use std::future::Future;
use std::sync::Arc;

use historical_quote::api::{JobFailure, JobFailureCode, JobResult, JobState};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::registry::{internal_failure, Finish, FinishedJob, RunnableJob};
use super::{ExecutionContext, JobExecutionError, JobExecutor, JobRegistry};

pub struct JobRunner<E> {
    registry: JobRegistry,
    executor: Arc<E>,
}

impl<E> JobRunner<E>
where
    E: JobExecutor,
{
    #[must_use]
    pub fn new(registry: JobRegistry, executor: Arc<E>) -> Self {
        Self { registry, executor }
    }

    pub async fn run(self) {
        loop {
            let batch = self.registry.schedule().await;
            for job in batch.jobs {
                let registry = self.registry.clone();
                let executor = Arc::clone(&self.executor);
                tokio::spawn(run_one(registry, executor, job));
            }
            if batch.stopped {
                return;
            }
            if let Some(deadline) = batch.next_deadline {
                tokio::select! {
                    () = batch.notified => {}
                    () = tokio::time::sleep_until(deadline) => {}
                }
            } else {
                batch.notified.await;
            }
        }
    }

    pub async fn begin_shutdown(&self) {
        self.registry.begin_shutdown().await;
    }
}

async fn run_one<E>(registry: JobRegistry, executor: Arc<E>, job: RunnableJob)
where
    E: JobExecutor,
{
    let started = std::time::Instant::now();
    tracing::info!(
        job_id = %job.job_id,
        job_type = ?job.job_type,
        reserved_decoded_bytes = job.reserved_decoded_bytes,
        "historical job started"
    );
    let cancellation = job.cancellation.clone();
    let execution = executor.execute(ExecutionContext {
        job_id: job.job_id,
        request: job.request,
        cancellation: cancellation.clone(),
        progress: job.progress,
    });
    let work = execute_and_serialize(execution, &cancellation, registry.max_terminal_bytes());
    tokio::pin!(work);
    let mut work_stopped = false;
    let outcome = tokio::select! {
        biased;
        () = tokio::time::sleep_until(job.deadline) => {
            cancellation.cancel();
            FinishedJob::Failed(deadline_failure())
        }
        () = cancellation.cancelled() => FinishedJob::Cancelled,
        outcome = &mut work => {
            work_stopped = true;
            outcome
        }
    };
    let finish = publish(&registry, job.job_id, outcome, job.deadline, &cancellation).await;
    if let Finish::Ended {
        state,
        failure_code,
    } = finish
    {
        tracing::info!(
            job_id = %job.job_id,
            job_type = ?job.job_type,
            state = state_name(state),
            failure_code = ?failure_code,
            elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            reserved_decoded_bytes = job.reserved_decoded_bytes,
            "historical job finished"
        );
    }
    // Ended work holds its data until it stops, so its slot does too.
    if !work_stopped {
        work.await;
    }
    registry.release(job.reserved_decoded_bytes).await;
}

/// Runs the job and writes its result within `max_result_bytes` on the
/// blocking pool, so a large result holds no runtime thread and its writing
/// still answers to the deadline. A result of a job that already ended is not
/// written.
async fn execute_and_serialize<F>(
    execution: F,
    cancellation: &CancellationToken,
    max_result_bytes: u64,
) -> FinishedJob
where
    F: Future<Output = Result<JobResult, JobExecutionError>>,
{
    match execution.await {
        Ok(_) if cancellation.is_cancelled() => FinishedJob::Cancelled,
        Ok(result) => {
            tokio::task::spawn_blocking(move || FinishedJob::completed(&result, max_result_bytes))
                .await
                .unwrap_or_else(|_| FinishedJob::Failed(internal_failure()))
        }
        Err(JobExecutionError::Cancelled) => FinishedJob::Cancelled,
        Err(JobExecutionError::Failed(failure)) => FinishedJob::Failed(failure),
    }
}

/// Publishes the job's end. A result past the deadline or after cancellation
/// is not published as a success, and one that does not fit beside the
/// results being delivered waits until a delivery ends.
async fn publish<'a>(
    registry: &'a JobRegistry,
    job_id: Uuid,
    mut outcome: FinishedJob,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Finish<'a> {
    loop {
        if matches!(outcome, FinishedJob::Completed(_)) {
            if Instant::now() >= deadline {
                outcome = FinishedJob::Failed(deadline_failure());
            } else if cancellation.is_cancelled() {
                outcome = FinishedJob::Cancelled;
            }
        }
        let (result, room) = match registry.finish(job_id, outcome).await {
            Finish::NoRoom { result, room } => (result, room),
            finish => return finish,
        };
        tracing::info!(
            job_id = %job_id,
            "historical job result waits for room beside the results being delivered"
        );
        outcome = tokio::select! {
            biased;
            () = tokio::time::sleep_until(deadline) => FinishedJob::Failed(deadline_failure()),
            () = cancellation.cancelled() => FinishedJob::Cancelled,
            () = room => FinishedJob::Completed(result),
        };
    }
}

fn state_name(state: JobState) -> &'static str {
    match state {
        JobState::Queued => "queued",
        JobState::Running => "running",
        JobState::Completed => "completed",
        JobState::Failed => "failed",
        JobState::Cancelled => "cancelled",
    }
}

fn deadline_failure() -> JobFailure {
    JobFailure {
        code: JobFailureCode::DeadlineExceeded,
        message: "job deadline exceeded".to_owned(),
        retryable: false,
        details: None,
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use historical_quote::api::{JobFailureCode, JobState};
    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;

    use super::{publish, Finish, FinishedJob};
    use crate::jobs::{JobLimits, JobRegistry, JobRequest, ScheduledJob, SubmitOutcome};

    /// A result written past the job's deadline, or after it was cancelled,
    /// is not published as a success.
    #[tokio::test(start_paused = true)]
    async fn a_late_or_cancelled_result_is_not_a_success() -> anyhow::Result<()> {
        let registry = JobRegistry::new(JobLimits::default());
        let in_an_hour = Instant::now() + std::time::Duration::from_secs(3_600);
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        for (deadline, cancellation, ended, failure_code) in [
            (
                Instant::now(),
                CancellationToken::new(),
                JobState::Failed,
                Some(JobFailureCode::DeadlineExceeded),
            ),
            (in_an_hour, cancelled, JobState::Cancelled, None),
            (
                in_an_hour,
                CancellationToken::new(),
                JobState::Completed,
                None,
            ),
        ] {
            let job_id = running_job(&registry).await?;
            let finish = publish(
                &registry,
                job_id,
                FinishedJob::Completed(Bytes::from_static(b"{}")),
                deadline,
                &cancellation,
            )
            .await;
            assert!(matches!(
                finish,
                Finish::Ended { state, failure_code: code } if state == ended && code == failure_code
            ));
            registry.release(1).await;
        }
        Ok(())
    }

    async fn running_job(registry: &JobRegistry) -> anyhow::Result<uuid::Uuid> {
        let request = serde_json::from_value(serde_json::json!({
            "requestId": uuid::Uuid::new_v4(),
            "apiRevision": 1,
            "timeoutMs": 60000,
            "chainId": 8453,
            "pool": {
                "backend": "native",
                "protocol": "uniswap_v3",
                "componentId": "pool-1",
                "tokenIn": "0x01",
                "tokenOut": "0x02"
            },
            "blocks": {"start": 100, "endInclusive": 100, "step": 1},
            "lags": [1],
            "amountsIn": ["1"]
        }))?;
        let SubmitOutcome::Accepted(submission) = registry
            .submit(ScheduledJob::new(JobRequest::HistoricalQuote(request), 1))
            .await
        else {
            anyhow::bail!("the job must be accepted");
        };
        anyhow::ensure!(
            registry.schedule().await.jobs.len() == 1,
            "the job must run"
        );
        Ok(submission.job_id)
    }
}
