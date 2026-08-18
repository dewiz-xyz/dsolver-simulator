mod registry;
mod result_body;
mod worker;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use historical_quote::api::{
    normalize_consistency_request, normalize_quote_request, ConsistencyCheckProgress,
    ConsistencyCheckRequest, ConsistencySelection, HistoricalQuoteProgress, JobFailure, JobResult,
    JobType, QuoteJobRequest,
};
use historical_quote::EngineProgress;
use uuid::Uuid;

pub use registry::{
    CancelOutcome, JobRegistry, JobSnapshot, PollOutcome, SubmitOutcome, TerminalDelivery,
};
pub use result_body::terminal_response_body;
pub use worker::JobRunner;

#[derive(Debug, Clone)]
pub struct JobLimits {
    pub max_running: usize,
    pub max_waiting: usize,
    pub decoded_byte_budget: u64,
    pub max_terminal_jobs: usize,
    pub terminal_ttl: Duration,
}

impl Default for JobLimits {
    fn default() -> Self {
        Self {
            max_running: 4,
            max_waiting: 16,
            decoded_byte_budget: u64::MAX,
            max_terminal_jobs: 20,
            terminal_ttl: Duration::from_secs(3_600),
        }
    }
}

#[derive(Debug, Clone)]
pub enum JobRequest {
    HistoricalQuote(QuoteJobRequest),
    HistoricalStateConsistencyCheck(ConsistencyCheckRequest),
}

impl JobRequest {
    #[must_use]
    pub fn request_id(&self) -> Uuid {
        match self {
            Self::HistoricalQuote(request) => request.request_id,
            Self::HistoricalStateConsistencyCheck(request) => request.request_id,
        }
    }

    #[must_use]
    pub const fn job_type(&self) -> JobType {
        match self {
            Self::HistoricalQuote(_) => JobType::HistoricalQuote,
            Self::HistoricalStateConsistencyCheck(_) => JobType::HistoricalStateConsistencyCheck,
        }
    }

    #[must_use]
    pub fn timeout_ms(&self) -> u64 {
        match self {
            Self::HistoricalQuote(request) => request.timeout_ms,
            Self::HistoricalStateConsistencyCheck(request) => request.timeout_ms,
        }
    }

    pub(crate) fn fingerprint(&self) -> Result<[u8; 32], serde_json::Error> {
        match self {
            Self::HistoricalQuote(request) => normalize_quote_request(request).fingerprint(),
            Self::HistoricalStateConsistencyCheck(request) => {
                normalize_consistency_request(request).fingerprint()
            }
        }
    }

    pub(crate) fn progress(&self) -> Arc<ProgressCounters> {
        match self {
            Self::HistoricalQuote(request) => Arc::new(ProgressCounters::quote(
                request
                    .blocks
                    .start_blocks()
                    .count()
                    .saturating_mul(request.lags.len())
                    .saturating_mul(request.amounts_in.len()),
            )),
            Self::HistoricalStateConsistencyCheck(request) => {
                let total = match &request.selection {
                    ConsistencySelection::CheckpointPairs { pairs } => pairs.len(),
                    ConsistencySelection::BlockRange { .. } => 0,
                };
                Arc::new(ProgressCounters::consistency(total))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScheduledJob {
    pub request: JobRequest,
    pub estimated_decoded_bytes: u64,
}

impl ScheduledJob {
    #[must_use]
    pub const fn new(request: JobRequest, estimated_decoded_bytes: u64) -> Self {
        Self {
            request,
            estimated_decoded_bytes,
        }
    }
}

pub struct ExecutionContext {
    pub job_id: Uuid,
    pub request: JobRequest,
    pub cancellation: tokio_util::sync::CancellationToken,
    pub progress: ProgressReporter,
}

pub trait JobExecutor: Send + Sync + 'static {
    fn execute(
        &self,
        context: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = Result<JobResult, JobExecutionError>> + Send + 'static>>;
}

#[derive(Debug)]
pub enum JobExecutionError {
    Cancelled,
    Failed(JobFailure),
}

#[derive(Clone)]
pub struct ProgressReporter {
    counters: Arc<ProgressCounters>,
}

impl ProgressReporter {
    pub(crate) fn new(counters: Arc<ProgressCounters>) -> Self {
        Self { counters }
    }
}

impl EngineProgress for ProgressReporter {
    fn quote_comparison_completed(&self) {
        self.counters.completed.fetch_add(1, Ordering::Relaxed);
    }

    fn checkpoint_pair_total(&self, total: u64) {
        self.counters.total.store(total, Ordering::Relaxed);
    }

    fn checkpoint_pair_completed(&self) {
        self.counters.completed.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy)]
enum ProgressKind {
    Quote,
    Consistency,
}

pub(crate) struct ProgressCounters {
    kind: ProgressKind,
    completed: AtomicU64,
    total: AtomicU64,
}

impl ProgressCounters {
    fn quote(total: usize) -> Self {
        Self::new(ProgressKind::Quote, total)
    }

    fn consistency(total: usize) -> Self {
        Self::new(ProgressKind::Consistency, total)
    }

    fn new(kind: ProgressKind, total: usize) -> Self {
        Self {
            kind,
            completed: AtomicU64::new(0),
            total: AtomicU64::new(u64::try_from(total).unwrap_or(u64::MAX)),
        }
    }

    pub(crate) fn snapshot(&self, completed_job: bool) -> historical_quote::api::JobProgress {
        let total = self.total.load(Ordering::Relaxed);
        let completed = self.completed.load(Ordering::Relaxed).min(total);
        let percent = if completed_job {
            100
        } else {
            completed
                .saturating_mul(100)
                .checked_div(total)
                .and_then(|percent| u8::try_from(percent).ok())
                .unwrap_or(0)
        };
        match self.kind {
            ProgressKind::Quote => {
                historical_quote::api::JobProgress::HistoricalQuote(HistoricalQuoteProgress {
                    completed_comparisons: if completed_job { total } else { completed },
                    total_comparisons: total,
                    percent_complete: percent,
                })
            }
            ProgressKind::Consistency => {
                historical_quote::api::JobProgress::HistoricalStateConsistencyCheck(
                    ConsistencyCheckProgress {
                        completed_checkpoint_pairs: if completed_job { total } else { completed },
                        total_checkpoint_pairs: total,
                        percent_complete: percent,
                    },
                )
            }
        }
    }
}
