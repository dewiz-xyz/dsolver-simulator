#![expect(
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests should fail immediately when a fixture or assertion is invalid"
)]

use std::sync::Arc;
use std::time::Duration;

use historical_quote::api::{JobProgress, JobState, JobType};
use historical_quote::EngineProgress;
use historical_quote_service::jobs::{
    JobLimits, JobRegistry, JobRequest, JobRunner, PollOutcome, ScheduledJob, SubmitOutcome,
};
use uuid::Uuid;

mod support;

use support::{consistency_request, quote_request, ControlledExecutor};

trait SubmitOutcomeExt {
    fn accepted_job_id(&self) -> Uuid;
}

impl SubmitOutcomeExt for SubmitOutcome {
    fn accepted_job_id(&self) -> Uuid {
        match self {
            SubmitOutcome::Accepted(submission) => submission.job_id,
            other => panic!("expected accepted job, got {other:?}"),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn byte_budget_preserves_fifo_head_blocking_and_one_based_positions() {
    let executor = Arc::new(ControlledExecutor::default());
    let registry = JobRegistry::new(JobLimits {
        max_running: 2,
        max_waiting: 16,
        decoded_byte_budget: 100,
        max_terminal_jobs: 20,
        terminal_ttl: Duration::from_secs(3_600),
    });
    let runner = JobRunner::new(registry.clone(), executor.clone());
    let run_task = tokio::spawn(runner.run());

    let first = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 60_000)),
            80,
        ))
        .await
        .accepted_job_id();
    let first_started = executor.next_started().await;
    assert_eq!(first_started.job_id, first);

    let head = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 60_000)),
            30,
        ))
        .await
        .accepted_job_id();
    let younger = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 60_000)),
            10,
        ))
        .await
        .accepted_job_id();
    tokio::task::yield_now().await;

    let head_snapshot = registry.inspect(head).await.expect("head job must exist");
    let younger_snapshot = registry
        .inspect(younger)
        .await
        .expect("younger job must exist");
    assert_eq!(head_snapshot.state, JobState::Queued);
    assert_eq!(head_snapshot.queue_position, Some(1));
    assert_eq!(younger_snapshot.state, JobState::Queued);
    assert_eq!(younger_snapshot.queue_position, Some(2));
    assert!(executor.try_next_started().is_none());

    first_started.complete_quote();
    let second = executor.next_started().await;
    let third = executor.next_started().await;
    assert_eq!((second.job_id, third.job_id), (head, younger));

    registry.begin_shutdown().await;
    run_task.await.expect("runner task must stop");
}

#[tokio::test(start_paused = true)]
async fn retained_request_id_reuses_same_content_and_conflicts_on_change() {
    let registry = JobRegistry::new(JobLimits::default());
    let request_id = Uuid::new_v4();
    let original = ScheduledJob::new(
        JobRequest::HistoricalQuote(quote_request(request_id, 60_000)),
        1,
    );
    let first = registry.submit(original.clone()).await.accepted_job_id();

    let reused = registry.submit(original).await;
    assert!(matches!(reused, SubmitOutcome::Reused(_)));
    assert_eq!(reused.job_id(), Some(first));

    let conflict = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(request_id, 30_000)),
            1,
        ))
        .await;
    assert!(matches!(conflict, SubmitOutcome::RequestIdConflict));
}

#[tokio::test(start_paused = true)]
async fn queue_time_counts_toward_the_deadline() {
    let executor = Arc::new(ControlledExecutor::default());
    let registry = JobRegistry::new(JobLimits {
        max_running: 1,
        ..JobLimits::default()
    });
    let runner = JobRunner::new(registry.clone(), executor.clone());
    let run_task = tokio::spawn(runner.run());

    registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 60_000)),
            1,
        ))
        .await;
    let _blocking = executor.next_started().await;
    let queued = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 1_000)),
            1,
        ))
        .await
        .accepted_job_id();

    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        registry
            .inspect(queued)
            .await
            .expect("queued job must exist")
            .state,
        JobState::Failed
    );

    registry.begin_shutdown().await;
    run_task.await.expect("runner task must stop");
}

#[tokio::test(start_paused = true)]
async fn terminal_delivery_is_leased_and_retryable_until_committed() {
    let executor = Arc::new(ControlledExecutor::default());
    let registry = JobRegistry::new(JobLimits::default());
    let runner = JobRunner::new(registry.clone(), executor.clone());
    let run_task = tokio::spawn(runner.run());
    let request_id = Uuid::new_v4();
    let job_id = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(request_id, 60_000)),
            1,
        ))
        .await
        .accepted_job_id();
    executor.next_started().await.complete_quote();
    tokio::task::yield_now().await;

    let PollOutcome::Terminal(first_delivery) = registry.poll(job_id).await else {
        panic!("first terminal poll must acquire delivery");
    };
    assert!(matches!(registry.poll(job_id).await, PollOutcome::NotFound));
    first_delivery.release().await;

    let PollOutcome::Terminal(second_delivery) = registry.poll(job_id).await else {
        panic!("released delivery must be retryable");
    };
    second_delivery.commit().await;
    assert!(matches!(registry.poll(job_id).await, PollOutcome::NotFound));
    let replacement = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(request_id, 60_000)),
            1,
        ))
        .await;
    assert!(matches!(replacement, SubmitOutcome::Accepted(_)));

    registry.begin_shutdown().await;
    run_task.await.expect("runner task must stop");
}

#[tokio::test(start_paused = true)]
async fn both_job_types_share_capacity_and_shutdown_cancels_them() {
    let executor = Arc::new(ControlledExecutor::default());
    let registry = JobRegistry::new(JobLimits {
        max_running: 1,
        ..JobLimits::default()
    });
    let runner = JobRunner::new(registry.clone(), executor.clone());
    let run_task = tokio::spawn(runner.run());
    let quote = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 60_000)),
            1,
        ))
        .await
        .accepted_job_id();
    let _running = executor.next_started().await;
    let check = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalStateConsistencyCheck(consistency_request(
                Uuid::new_v4(),
                60_000,
            )),
            1,
        ))
        .await
        .accepted_job_id();
    assert_eq!(
        registry
            .inspect(quote)
            .await
            .expect("quote must exist")
            .job_type,
        JobType::HistoricalQuote
    );
    assert_eq!(
        registry
            .inspect(check)
            .await
            .expect("check must exist")
            .job_type,
        JobType::HistoricalStateConsistencyCheck
    );

    registry.begin_shutdown().await;
    run_task.await.expect("runner task must stop");
    assert_eq!(
        registry
            .inspect(quote)
            .await
            .expect("quote must remain retained")
            .state,
        JobState::Cancelled
    );
    assert_eq!(
        registry
            .inspect(check)
            .await
            .expect("check must remain retained")
            .state,
        JobState::Cancelled
    );
}

#[tokio::test(start_paused = true)]
async fn default_concurrency_is_four_and_waiting_capacity_is_bounded() {
    let executor = Arc::new(ControlledExecutor::default());
    let registry = JobRegistry::new(JobLimits {
        max_waiting: 1,
        ..JobLimits::default()
    });
    let runner = JobRunner::new(registry.clone(), executor.clone());
    let run_task = tokio::spawn(runner.run());

    let mut running = Vec::new();
    for _ in 0..4 {
        registry
            .submit(ScheduledJob::new(
                JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 60_000)),
                1,
            ))
            .await;
        running.push(executor.next_started().await);
    }
    let waiting = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 60_000)),
            1,
        ))
        .await
        .accepted_job_id();
    let rejected = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 60_000)),
            1,
        ))
        .await;
    assert!(matches!(rejected, SubmitOutcome::QueueFull));
    assert_eq!(
        registry
            .inspect(waiting)
            .await
            .expect("waiting job must exist")
            .queue_position,
        Some(1)
    );

    registry.begin_shutdown().await;
    drop(running);
    run_task.await.expect("runner task must stop");
}

#[tokio::test(start_paused = true)]
async fn queued_and_running_cancellation_preserve_partial_progress() {
    let executor = Arc::new(ControlledExecutor::default());
    let registry = JobRegistry::new(JobLimits {
        max_running: 1,
        ..JobLimits::default()
    });
    let runner = JobRunner::new(registry.clone(), executor.clone());
    let run_task = tokio::spawn(runner.run());
    let running_id = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 60_000)),
            1,
        ))
        .await
        .accepted_job_id();
    let running = executor.next_started().await;
    running.progress.quote_comparison_completed();
    let queued_id = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 60_000)),
            1,
        ))
        .await
        .accepted_job_id();

    let _ = registry.cancel(queued_id).await;
    assert_eq!(
        registry
            .inspect(queued_id)
            .await
            .expect("queued job must be retained")
            .state,
        JobState::Cancelled
    );
    assert_eq!(
        registry
            .inspect(queued_id)
            .await
            .expect("queued job must be retained")
            .queue_position,
        None
    );
    let _ = registry.cancel(running_id).await;
    tokio::task::yield_now().await;
    let running = registry
        .inspect(running_id)
        .await
        .expect("running job must be retained");
    assert_eq!(running.state, JobState::Cancelled);
    let JobProgress::HistoricalQuote(progress) = running.progress else {
        panic!("quote job must report quote progress");
    };
    assert_eq!(progress.completed_comparisons, 1);

    registry.begin_shutdown().await;
    run_task.await.expect("runner task must stop");
}

#[tokio::test(start_paused = true)]
async fn running_deadline_cancels_work_and_keeps_partial_progress() {
    let executor = Arc::new(ControlledExecutor::default());
    let registry = JobRegistry::new(JobLimits::default());
    let runner = JobRunner::new(registry.clone(), executor.clone());
    let run_task = tokio::spawn(runner.run());
    let job_id = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 1_000)),
            1,
        ))
        .await
        .accepted_job_id();
    let running = executor.next_started().await;
    running.progress.quote_comparison_completed();

    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    let failed = registry
        .inspect(job_id)
        .await
        .expect("failed job must be retained");
    assert_eq!(failed.state, JobState::Failed);
    assert!(failed.failure.is_none());
    let JobProgress::HistoricalQuote(progress) = failed.progress else {
        panic!("quote job must report quote progress");
    };
    assert_eq!(progress.completed_comparisons, 1);

    registry.begin_shutdown().await;
    run_task.await.expect("runner task must stop");
}

#[tokio::test(start_paused = true)]
async fn terminal_retention_evicts_on_replacement_and_expires_unfetched_results() {
    let executor = Arc::new(ControlledExecutor::default());
    let registry = JobRegistry::new(JobLimits {
        max_running: 1,
        max_terminal_jobs: 2,
        terminal_ttl: Duration::from_secs(3_600),
        ..JobLimits::default()
    });
    let runner = JobRunner::new(registry.clone(), executor.clone());
    let run_task = tokio::spawn(runner.run());
    let mut completed = Vec::new();
    for _ in 0..3 {
        let job_id = registry
            .submit(ScheduledJob::new(
                JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 60_000)),
                1,
            ))
            .await
            .accepted_job_id();
        executor.next_started().await.complete_quote();
        tokio::task::yield_now().await;
        completed.push(job_id);
        if completed.len() == 2 {
            assert!(registry.inspect(completed[0]).await.is_some());
        }
    }
    assert!(registry.inspect(completed[0]).await.is_none());
    assert!(registry.inspect(completed[1]).await.is_some());
    assert!(registry.inspect(completed[2]).await.is_some());

    tokio::time::advance(Duration::from_secs(3_600)).await;
    assert!(registry.inspect(completed[1]).await.is_none());
    assert!(registry.inspect(completed[2]).await.is_none());
    registry.begin_shutdown().await;
    run_task.await.expect("runner task must stop");
}
