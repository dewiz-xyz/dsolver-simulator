use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;
use uuid::Uuid;

use super::{deserialize_uuid_v4, Backend, ConsistencyCheckReport, HistoricalQuoteResult};

/// Work type stored by the shared in-memory scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum JobType {
    HistoricalQuote,
    HistoricalStateConsistencyCheck,
}

/// Lifecycle state of an admitted job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum JobState {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// Quote progress counts completed lag comparisons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HistoricalQuoteProgress {
    pub completed_comparisons: u64,
    pub total_comparisons: u64,
    #[schema(maximum = 100)]
    pub percent_complete: u8,
}

/// Consistency progress counts completed checkpoint pairs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConsistencyCheckProgress {
    pub completed_checkpoint_pairs: u64,
    pub total_checkpoint_pairs: u64,
    #[schema(maximum = 100)]
    pub percent_complete: u8,
}

/// Progress shape selected by the job type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum JobProgress {
    HistoricalQuote(HistoricalQuoteProgress),
    HistoricalStateConsistencyCheck(ConsistencyCheckProgress),
}

/// Response returned when a job is submitted or reused.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobSubmission {
    #[serde(deserialize_with = "deserialize_uuid_v4")]
    pub job_id: Uuid,
    #[serde(deserialize_with = "deserialize_uuid_v4")]
    pub request_id: Uuid,
    pub job_type: JobType,
    pub state: JobState,
    pub submitted_at: DateTime<Utc>,
    pub deadline_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 1)]
    pub queue_position: Option<u64>,
    pub cancellation_requested: bool,
    pub progress: JobProgress,
    pub reused: bool,
}

/// Terminal result selected by the job type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum JobResult {
    HistoricalQuote(HistoricalQuoteResult),
    HistoricalStateConsistencyCheck(ConsistencyCheckReport),
}

/// Stable terminal failure code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobFailureCode {
    DeadlineExceeded,
    HistoryReadFailed,
    HistoricalDataInvalid,
    StateReconstructionFailed,
    CheckBudgetExceeded,
    InternalError,
}

/// Safe failure stored in a terminal job envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobFailure {
    pub code: JobFailureCode,
    pub message: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

/// Current job state returned by polling or cancellation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobEnvelope {
    #[serde(deserialize_with = "deserialize_uuid_v4")]
    pub job_id: Uuid,
    #[serde(deserialize_with = "deserialize_uuid_v4")]
    pub request_id: Uuid,
    pub job_type: JobType,
    pub state: JobState,
    pub submitted_at: DateTime<Utc>,
    pub deadline_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 1)]
    pub queue_position: Option<u64>,
    pub cancellation_requested: bool,
    pub progress: JobProgress,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<JobResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<JobFailure>,
}

/// Readiness response used by the internal load balancer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReadyResponse {
    pub ready: bool,
}

/// Safe dependency state included in the protected status response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DependencyState {
    Healthy,
    Degraded,
    Unavailable,
}

/// Read replica and object-store health summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DependencyHealth {
    pub state: DependencyState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Counts of jobs currently retained by the service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobCounts {
    pub queued: u64,
    pub running: u64,
    pub retained_terminal: u64,
}

/// Configured bounds reported by the protected status endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceLimits {
    pub max_combination_count: u64,
    pub max_block_span: u64,
    pub default_timeout_ms: u64,
    pub max_timeout_ms: u64,
    pub max_running_jobs: u64,
    pub max_waiting_jobs: u64,
    pub decoded_byte_budget: u64,
    pub max_terminal_jobs: u64,
    pub terminal_retention_seconds: u64,
    pub max_consistency_pairs: u64,
    pub max_quote_selectors: u64,
    pub max_amounts_per_quote: u64,
    pub max_structured_differences: u64,
}

/// Protected service status without storage identifiers or full coverage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StatusResponse {
    pub service_revision: String,
    #[schema(minimum = 1, maximum = 1)]
    pub api_revision: u32,
    #[schema(minimum = 8453, maximum = 8453)]
    pub supported_chain_id: u64,
    pub enabled_backends: Vec<Backend>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visible_through_block: Option<u64>,
    pub database: DependencyHealth,
    pub object_store: DependencyHealth,
    pub jobs: JobCounts,
    pub reserved_decoded_bytes: u64,
    pub limits: ServiceLimits,
    pub draining: bool,
}
