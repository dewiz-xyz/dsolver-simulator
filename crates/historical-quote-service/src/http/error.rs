use axum::http::header::RETRY_AFTER;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use historical_quote::api::{ApiError, ApiErrorCode};
use serde_json::Value;

pub struct HttpError {
    status: StatusCode,
    body: Box<ApiError>,
    retry_after_seconds: Option<u64>,
}

impl HttpError {
    pub fn invalid(message: impl Into<String>, details: Option<Value>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            message,
            false,
            details,
        )
    }

    pub fn job_not_found() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            ApiErrorCode::JobNotFound,
            "job was not found",
            false,
            None,
        )
    }

    pub fn request_id_conflict() -> Self {
        Self::new(
            StatusCode::CONFLICT,
            ApiErrorCode::RequestIdConflict,
            "requestId is retained with different request content",
            false,
            None,
        )
    }

    pub fn comparison_budget(details: Value) -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            ApiErrorCode::ComparisonBudgetExceeded,
            "quote comparison budget was exceeded",
            false,
            Some(details),
        )
    }

    pub fn queue_full(retry_after_seconds: u64) -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            ApiErrorCode::QueueFull,
            "job queue is full",
            true,
            None,
        )
        .with_retry_after(retry_after_seconds)
    }

    pub fn unavailable(retry_after_seconds: u64) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ApiErrorCode::ServiceUnavailable,
            "service cannot safely accept this request",
            true,
            None,
        )
        .with_retry_after(retry_after_seconds)
    }

    fn new(
        status: StatusCode,
        code: ApiErrorCode,
        message: impl Into<String>,
        retryable: bool,
        details: Option<Value>,
    ) -> Self {
        Self {
            status,
            body: Box::new(ApiError {
                code,
                message: message.into(),
                retryable,
                details,
            }),
            retry_after_seconds: None,
        }
    }

    fn with_retry_after(mut self, seconds: u64) -> Self {
        self.retry_after_seconds = Some(seconds);
        self
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let mut response = (self.status, axum::Json(*self.body)).into_response();
        if let Some(seconds) = self.retry_after_seconds {
            if let Ok(value) = HeaderValue::from_str(&seconds.to_string()) {
                response.headers_mut().insert(RETRY_AFTER, value);
            }
        }
        response
    }
}
