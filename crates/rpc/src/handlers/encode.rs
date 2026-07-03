use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use runtime::services::encode::{EncodeErrorKind, EncodeService, EncodeServiceError};

use crate::models::messages::{EncodeErrorResponse, RouteEncodeRequest, RouteEncodeResponse};
use crate::services::encode::{log_failure, log_handler_timeout, log_success};

pub async fn encode(
    State(encode_service): State<EncodeService>,
    Json(request): Json<RouteEncodeRequest>,
) -> Response {
    let request_for_logging = request.clone();
    match encode_service.encode(request).await {
        Ok(success) => {
            log_success(
                &request_for_logging,
                &success.computation,
                success.latency_ms,
            );
            Json::<RouteEncodeResponse>(success.computation.response).into_response()
        }
        Err(EncodeServiceError::Timeout {
            timeout_ms,
            latency_ms,
        }) => {
            log_handler_timeout(&request_for_logging, timeout_ms, latency_ms);
            let body = Json(EncodeErrorResponse {
                error: format!("Encode request timed out after {timeout_ms}ms"),
                request_id: request_for_logging.request_id.clone(),
            });
            (StatusCode::REQUEST_TIMEOUT, body).into_response()
        }
        Err(EncodeServiceError::Failed { error, latency_ms }) => {
            let status = encode_status_code(error.kind());
            let body = Json(EncodeErrorResponse {
                error: error.message().to_string(),
                request_id: request_for_logging.request_id.clone(),
            });
            log_failure(
                &request_for_logging,
                error.kind(),
                error.message(),
                latency_ms,
            );
            (status, body).into_response()
        }
    }
}

fn encode_status_code(kind: EncodeErrorKind) -> StatusCode {
    match kind {
        EncodeErrorKind::InvalidRequest => StatusCode::BAD_REQUEST,
        EncodeErrorKind::NotFound => StatusCode::NOT_FOUND,
        EncodeErrorKind::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        EncodeErrorKind::Simulation => StatusCode::UNPROCESSABLE_ENTITY,
        EncodeErrorKind::Encoding | EncodeErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::{encode_status_code, EncodeErrorKind, StatusCode};

    #[test]
    fn encode_status_mapping_stays_in_rpc_adapter() {
        assert_eq!(
            encode_status_code(EncodeErrorKind::InvalidRequest),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            encode_status_code(EncodeErrorKind::NotFound),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            encode_status_code(EncodeErrorKind::Unavailable),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            encode_status_code(EncodeErrorKind::Simulation),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            encode_status_code(EncodeErrorKind::Encoding),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            encode_status_code(EncodeErrorKind::Internal),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
