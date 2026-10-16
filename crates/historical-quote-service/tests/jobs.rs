#![expect(
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests should fail immediately when a fixture or assertion is invalid"
)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use historical_quote::api::{JobProgress, JobResult, JobState, JobType};
use historical_quote::EngineProgress;
use historical_quote_service::jobs::{
    CancelOutcome, ExecutionContext, JobExecutionError, JobExecutor, JobLimits, JobRegistry,
    JobRequest, JobRunner, PollOutcome, ScheduledJob, SubmitOutcome,
};
use tokio_util::sync::CancellationToken;
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
        max_terminal_bytes: u64::MAX,
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
    for (job_id, expected) in [(head, head_snapshot), (younger, younger_snapshot)] {
        let PollOutcome::Pending(polled) = registry.poll(job_id).await else {
            panic!("queued job must return a pending envelope");
        };
        assert_eq!(
            serde_json::to_value(*polled).expect("pending envelope must serialize"),
            serde_json::to_value(expected).expect("inspected envelope must serialize")
        );
    }
    assert_eq!(registry.snapshot().await.counts.queued, 2);
    assert!(executor.try_next_started().is_none());

    first_started.complete_quote();
    let second = executor.next_started().await;
    let third = executor.next_started().await;
    assert_eq!((second.job_id, third.job_id), (head, younger));

    registry.begin_shutdown().await;
    run_task.await.expect("runner task must stop");
}

#[tokio::test]
async fn job_one_byte_above_global_budget_is_rejected_before_execution() {
    let registry = JobRegistry::new(JobLimits {
        decoded_byte_budget: 100,
        ..JobLimits::default()
    });
    let outcome = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 60_000)),
            101,
        ))
        .await;

    assert!(matches!(outcome, SubmitOutcome::ServiceUnavailable));
    let snapshot = registry.snapshot().await;
    assert_eq!(snapshot.counts.queued, 0);
    assert_eq!(snapshot.counts.running, 0);
    assert_eq!(snapshot.reserved_decoded_bytes, 0);
}

#[tokio::test(start_paused = true)]
async fn job_at_the_exact_decoded_budget_is_admitted_and_scheduled() {
    let executor = Arc::new(ControlledExecutor::default());
    let registry = JobRegistry::new(JobLimits {
        decoded_byte_budget: 100,
        ..JobLimits::default()
    });
    let job_id = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 60_000)),
            100,
        ))
        .await
        .accepted_job_id();
    let runner = tokio::spawn(JobRunner::new(registry.clone(), executor.clone()).run());
    let started = tokio::time::timeout(Duration::from_secs(1), executor.next_started())
        .await
        .expect("exact-budget job must start before its deadline");
    assert_eq!(started.job_id, job_id);
    let snapshot = registry.snapshot().await;
    assert_eq!(snapshot.counts.running, 1);
    assert_eq!(snapshot.counts.queued, 0);
    assert_eq!(snapshot.reserved_decoded_bytes, 100);
    registry.begin_shutdown().await;
    runner.await.expect("runner must stop");
    assert_eq!(registry.snapshot().await.reserved_decoded_bytes, 0);
}

#[tokio::test(start_paused = true)]
async fn retained_request_id_reuses_same_content_and_conflicts_on_change() {
    let registry = JobRegistry::new(JobLimits::default());
    let request_id = Uuid::new_v4();
    let original = ScheduledJob::new(
        JobRequest::HistoricalQuote(quote_request(request_id, 60_000)),
        1,
    );
    let first_submission = registry.submit(original.clone()).await;
    let first = first_submission.accepted_job_id();
    let SubmitOutcome::Accepted(first_submission) = first_submission else {
        panic!("first submission must be accepted");
    };
    assert_eq!(first_submission.queue_position, Some(1));

    let reused = registry.submit(original).await;
    assert!(matches!(reused, SubmitOutcome::Reused(_)));
    assert_eq!(reused.job_id(), Some(first));
    let SubmitOutcome::Reused(reused) = reused else {
        panic!("retained request must reuse the submission");
    };
    assert_eq!(reused.queue_position, Some(1));

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
    settle().await;

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
        settle().await;
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

/// A cancelled job, or one past its deadline, ends at once, but its slot and
/// its reservation stay taken until its work stops, so the next job waits.
#[tokio::test(start_paused = true)]
async fn an_ended_job_keeps_its_slot_until_its_work_stops() {
    let executor = ControlledExecutor::default();
    let registry = JobRegistry::new(JobLimits {
        max_running: 1,
        ..JobLimits::default()
    });
    let runner = JobRunner::new(
        registry.clone(),
        Arc::new(IgnoringCancellation(executor.clone())),
    );
    let run_task = tokio::spawn(runner.run());
    let submit = |timeout_ms, reserved| {
        let registry = registry.clone();
        async move {
            registry
                .submit(ScheduledJob::new(
                    JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), timeout_ms)),
                    reserved,
                ))
                .await
                .accepted_job_id()
        }
    };
    let cancelled = submit(60_000, 5).await;
    let cancelled_work = executor.next_started().await;
    let timed_out = submit(10_000, 7).await;

    let _ = registry.cancel(cancelled).await;
    tokio::task::yield_now().await;
    let ended = registry.inspect(cancelled).await.expect("cancelled job");
    assert_eq!(ended.state, JobState::Cancelled);
    assert!(executor.try_next_started().is_none());
    assert_eq!(registry.snapshot().await.reserved_decoded_bytes, 5);

    cancelled_work.complete_quote();
    let timed_out_work = executor.next_started().await;
    assert_eq!(timed_out_work.job_id, timed_out);
    assert_eq!(registry.snapshot().await.reserved_decoded_bytes, 7);
    let next = submit(60_000, 3).await;
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    let ended = registry.inspect(timed_out).await.expect("timed out job");
    assert_eq!(ended.state, JobState::Failed);
    assert!(executor.try_next_started().is_none());
    assert_eq!(registry.snapshot().await.reserved_decoded_bytes, 7);

    timed_out_work.complete_quote();
    let next_work = executor.next_started().await;
    assert_eq!(next_work.job_id, next);
    registry.begin_shutdown().await;
    next_work.complete_quote();
    run_task.await.expect("runner task must stop");
}

/// Kept results are bounded by their bytes as well as their count, the oldest
/// dropped first, and a result larger than the service keeps fails its job.
#[tokio::test(start_paused = true)]
async fn kept_results_are_bounded_by_their_bytes() {
    let one = one_body_bytes().await;
    let limit = one * 2 + one / 2;
    let (registry, executor, run_task) = started(kept_bytes_limits(1, limit));
    let mut kept = Vec::new();
    for _ in 0..3 {
        kept.push(completed_quote(&registry, &executor).await);
    }
    assert!(registry.inspect(kept[0]).await.is_none());
    assert!(registry.inspect(kept[1]).await.is_some());
    assert!(registry.inspect(kept[2]).await.is_some());
    assert_eq!(registry.snapshot().await.counts.retained_terminal, 2);
    registry.begin_shutdown().await;
    run_task.await.expect("runner task must stop");

    let (registry, executor, run_task) = started(kept_bytes_limits(1, one - 1));
    let job_id = completed_quote(&registry, &executor).await;
    assert_eq!(failure_code(&registry, job_id).await, "read_limit_exceeded");
    registry.begin_shutdown().await;
    run_task.await.expect("runner task must stop");
}

/// A result being delivered is neither dropped for room nor expired, so a
/// failed delivery can be read again.
#[tokio::test(start_paused = true)]
async fn a_result_being_delivered_is_neither_dropped_nor_expired() {
    let one = one_body_bytes().await;
    let limit = one * 2 + one / 2;
    let (registry, executor, run_task) = started(kept_bytes_limits(1, limit));
    let delivered = completed_quote(&registry, &executor).await;
    let PollOutcome::Terminal(delivery) = registry.poll(delivered).await else {
        panic!("the job must have ended");
    };
    let older = completed_quote(&registry, &executor).await;
    let newer = completed_quote(&registry, &executor).await;

    assert!(registry.inspect(delivered).await.is_some());
    assert!(registry.inspect(older).await.is_none());
    assert!(registry.inspect(newer).await.is_some());
    let body = delivery.bytes();
    delivery.release().await;
    let PollOutcome::Terminal(retry) = registry.poll(delivered).await else {
        panic!("a released delivery must be readable again");
    };
    assert_eq!(retry.bytes(), body);

    tokio::time::advance(Duration::from_secs(3_600)).await;
    assert!(registry.inspect(delivered).await.is_some());
    assert!(registry.inspect(newer).await.is_none());
    retry.commit().await;
    assert!(registry.inspect(delivered).await.is_none());
    registry.begin_shutdown().await;
    run_task.await.expect("runner task must stop");
}

/// A result with no room beside a result being delivered, by bytes or by
/// count, waits, its job running and its slot taken, until the delivery ends,
/// and fails at its deadline or ends on cancellation if none does.
#[tokio::test(start_paused = true)]
async fn a_result_waits_for_the_room_a_delivery_holds() {
    let one = one_body_bytes().await;
    let by_count = JobLimits {
        max_running: 2,
        max_terminal_jobs: 1,
        ..JobLimits::default()
    };
    for limits in [kept_bytes_limits(2, one + one / 2), by_count] {
        let (registry, executor, run_task) = started(limits);
        let delivered = completed_quote(&registry, &executor).await;
        let PollOutcome::Terminal(delivery) = registry.poll(delivered).await else {
            panic!("the job must have ended");
        };

        let waiting = completed_quote(&registry, &executor).await;
        let job = registry.inspect(waiting).await.expect("waiting job");
        assert_eq!(job.state, JobState::Running);
        let snapshot = registry.snapshot().await;
        assert_eq!(snapshot.counts.running, 1);
        assert_eq!(snapshot.counts.retained_terminal, 1);
        delivery.commit().await;
        settle().await;
        let job = registry.inspect(waiting).await.expect("published job");
        assert_eq!(job.state, JobState::Completed);
        assert_eq!(registry.snapshot().await.counts.running, 0);

        let PollOutcome::Terminal(delivery) = registry.poll(waiting).await else {
            panic!("the job must have ended");
        };
        let late = registry
            .submit(ScheduledJob::new(
                JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 5_000)),
                1,
            ))
            .await
            .accepted_job_id();
        executor.next_started().await.complete_quote();
        settle().await;
        assert_eq!(
            registry.inspect(late).await.expect("late job").state,
            JobState::Running
        );
        tokio::time::advance(Duration::from_secs(5)).await;
        settle().await;
        assert_eq!(failure_code(&registry, late).await, "deadline_exceeded");

        let cancelled = completed_quote(&registry, &executor).await;
        assert_eq!(
            registry
                .inspect(cancelled)
                .await
                .expect("waiting job")
                .state,
            JobState::Running
        );
        let _ = registry.cancel(cancelled).await;
        settle().await;
        assert_eq!(
            registry
                .inspect(cancelled)
                .await
                .expect("cancelled job")
                .state,
            JobState::Cancelled
        );
        assert_eq!(registry.snapshot().await.counts.running, 0);
        delivery.commit().await;
        registry.begin_shutdown().await;
        run_task.await.expect("runner task must stop");
    }
}

/// Limits that run `max_running` jobs and keep `max_terminal_bytes` of their
/// bodies.
fn kept_bytes_limits(max_running: usize, max_terminal_bytes: u64) -> JobLimits {
    JobLimits {
        max_running,
        max_terminal_bytes,
        ..JobLimits::default()
    }
}

/// Cancelling a job that already failed changes nothing: the answer shows the
/// job as it is, without its terminal failure, and the failure still waits
/// for its one delivery.
#[tokio::test(start_paused = true)]
async fn cancelling_a_failed_job_leaves_its_failure_to_deliver() {
    let (registry, executor, run_task) = started(kept_bytes_limits(1, u64::MAX));
    let job_id = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 1_000)),
            1,
        ))
        .await
        .accepted_job_id();
    let _running = executor.next_started().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    settle().await;

    let CancelOutcome::Found(answer) = registry.cancel(job_id).await else {
        panic!("the failed job must be found");
    };
    assert_eq!(answer.state, JobState::Failed);
    assert!(answer.failure.is_none());
    assert_eq!(failure_code(&registry, job_id).await, "deadline_exceeded");
    registry.begin_shutdown().await;
    run_task.await.expect("runner task must stop");
}

/// Runs each job as the wrapped executor does, but its work never sees the
/// job's cancellation, so it runs on until the test ends it.
struct IgnoringCancellation(ControlledExecutor);

impl JobExecutor for IgnoringCancellation {
    fn execute(
        &self,
        context: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = Result<JobResult, JobExecutionError>> + Send + 'static>> {
        self.0.execute(ExecutionContext {
            cancellation: CancellationToken::new(),
            ..context
        })
    }
}

fn started(
    limits: JobLimits,
) -> (
    JobRegistry,
    Arc<ControlledExecutor>,
    tokio::task::JoinHandle<()>,
) {
    let executor = Arc::new(ControlledExecutor::default());
    let registry = JobRegistry::new(limits);
    let run_task = tokio::spawn(JobRunner::new(registry.clone(), executor.clone()).run());
    (registry, executor, run_task)
}

/// Runs one quote job to its end and returns its id.
async fn completed_quote(registry: &JobRegistry, executor: &ControlledExecutor) -> Uuid {
    let job_id = registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(quote_request(Uuid::new_v4(), 60_000)),
            1,
        ))
        .await
        .accepted_job_id();
    executor.next_started().await.complete_quote();
    settle().await;
    job_id
}

/// The bytes one completed quote keeps.
async fn one_body_bytes() -> u64 {
    let (registry, executor, run_task) = started(kept_bytes_limits(1, u64::MAX));
    let job_id = completed_quote(&registry, &executor).await;
    let PollOutcome::Terminal(delivery) = registry.poll(job_id).await else {
        panic!("the job must have ended");
    };
    let bytes = u64::try_from(delivery.bytes().len()).expect("a body length");
    registry.begin_shutdown().await;
    run_task.await.expect("runner task must stop");
    bytes
}

/// The failure code of the ended job `job_id`, read through its delivery.
async fn failure_code(registry: &JobRegistry, job_id: Uuid) -> String {
    let PollOutcome::Terminal(delivery) = registry.poll(job_id).await else {
        panic!("the job must have ended");
    };
    let body: serde_json::Value =
        serde_json::from_slice(&delivery.bytes()).expect("a JSON terminal body");
    assert_eq!(body["state"], "failed");
    assert!(body.get("result").is_none());
    body["failure"]["code"]
        .as_str()
        .expect("a failure code")
        .to_owned()
}

/// Lets the runner and its jobs act on what the test just did, the results
/// they write on the blocking pool included. A paused clock advances only once
/// the runtime is idle and no blocking task runs, so this short sleep ends
/// after all of it.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(1)).await;
}
