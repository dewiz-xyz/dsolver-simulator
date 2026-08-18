use std::sync::Arc;

use historical_quote::api::{JobFailure, JobFailureCode};

use super::registry::{FinishedJob, RunnableJob};
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
                    () = self.registry.notified() => {}
                    () = tokio::time::sleep_until(deadline) => {}
                }
            } else {
                self.registry.notified().await;
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
    tokio::pin!(execution);
    let outcome = tokio::select! {
        biased;
        () = tokio::time::sleep_until(job.deadline) => {
            cancellation.cancel();
            FinishedJob::Failed(JobFailure {
                code: JobFailureCode::DeadlineExceeded,
                message: "job deadline exceeded".to_owned(),
                retryable: false,
                details: None,
            })
        }
        () = cancellation.cancelled() => FinishedJob::Cancelled,
        result = &mut execution => match result {
            Ok(result) => FinishedJob::Completed(result),
            Err(JobExecutionError::Cancelled) => FinishedJob::Cancelled,
            Err(JobExecutionError::Failed(failure)) => FinishedJob::Failed(failure),
        },
    };
    let (state, failure_code) = match &outcome {
        FinishedJob::Completed(_) => ("completed", None),
        FinishedJob::Failed(failure) => ("failed", Some(failure.code)),
        FinishedJob::Cancelled => ("cancelled", None),
    };
    tracing::info!(
        job_id = %job.job_id,
        job_type = ?job.job_type,
        state,
        failure_code = ?failure_code,
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        reserved_decoded_bytes = job.reserved_decoded_bytes,
        "historical job finished"
    );
    registry.finish(job.job_id, outcome).await;
}
