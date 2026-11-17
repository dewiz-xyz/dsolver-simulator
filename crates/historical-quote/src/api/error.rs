use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

/// Stable code returned by an HTTP admission or polling error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiErrorCode {
    InvalidRequest,
    Unauthorized,
    JobNotFound,
    RequestIdConflict,
    ComparisonBudgetExceeded,
    QueueFull,
    ServiceUnavailable,
}

/// Safe error returned by the HTTP API.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApiError {
    pub code: ApiErrorCode,
    pub message: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

/// Details returned when a quote comparison budget is exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ComparisonBudgetDetails {
    pub start_block_count: u64,
    pub lag_count: u64,
    pub input_amount_count: u64,
    pub combination_count: u64,
    pub max_combination_count: u64,
}
