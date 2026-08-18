use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::header::{CONTENT_TYPE, LOCATION, RETRY_AFTER};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use historical_quote::api::{
    Backend, ComparisonBudgetDetails, ConsistencyCheckRequest, ConsistencySelection,
    CoverageRequest, DependencyHealth, DependencyState, QuoteJobRequest, ReadyResponse,
    StatusResponse, API_REVISION,
};
use uuid::Uuid;

use crate::app::{AppState, StatusDependencies};
use crate::http::error::HttpError;
use crate::jobs::{
    terminal_response_body, CancelOutcome, JobRequest, PollOutcome, ScheduledJob, SubmitOutcome,
};

const RETRY_AFTER_SECONDS: u64 = 2;
const REVISION_HEADER: &str = "x-dsolver-history-api-revision";

pub async fn submit_quote(
    State(state): State<AppState>,
    request: Result<Json<QuoteJobRequest>, JsonRejection>,
) -> Result<Response, HttpError> {
    let request = request
        .map_err(|_| HttpError::invalid("request body is invalid", None))?
        .0;
    validate_quote(&state, &request)?;
    let request_id = request.request_id;
    let dimensions = quote_dimensions(&request)?;
    let outcome = state
        .registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalQuote(request),
            state.config.job_decoded_byte_reservation,
        ))
        .await;
    tracing::info!(
        %request_id,
        start_block_count = dimensions.start_block_count,
        lag_count = dimensions.lag_count,
        input_amount_count = dimensions.input_amount_count,
        combination_count = dimensions.combination_count,
        "historical quote job submission resolved"
    );
    submission_response(outcome)
}

pub async fn submit_consistency_check(
    State(state): State<AppState>,
    request: Result<Json<ConsistencyCheckRequest>, JsonRejection>,
) -> Result<Response, HttpError> {
    let request = request
        .map_err(|_| HttpError::invalid("request body is invalid", None))?
        .0;
    validate_consistency(&state, &request)?;
    let request_id = request.request_id;
    let outcome = state
        .registry
        .submit(ScheduledJob::new(
            JobRequest::HistoricalStateConsistencyCheck(request),
            state.config.job_decoded_byte_reservation,
        ))
        .await;
    tracing::info!(%request_id, "historical state consistency check submission resolved");
    submission_response(outcome)
}

pub async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    require_revision_header(&headers)?;
    let job_id = parse_job_id(&job_id)?;
    match state.registry.poll(job_id).await {
        PollOutcome::Pending(envelope) => {
            let mut response = Json(envelope).into_response();
            insert_integer_header(&mut response, RETRY_AFTER, RETRY_AFTER_SECONDS);
            Ok(response)
        }
        PollOutcome::Terminal(delivery) => {
            let mut response = Response::new(terminal_response_body(delivery));
            response
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            Ok(response)
        }
        PollOutcome::NotFound => Err(HttpError::job_not_found()),
    }
}

pub async fn cancel_job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    require_revision_header(&headers)?;
    let job_id = parse_job_id(&job_id)?;
    match state.registry.cancel(job_id).await {
        CancelOutcome::Found(envelope) => Ok(Json(envelope).into_response()),
        CancelOutcome::NotFound => Err(HttpError::job_not_found()),
    }
}

pub async fn coverage(
    State(state): State<AppState>,
    request: Result<Json<CoverageRequest>, JsonRejection>,
) -> Result<Response, HttpError> {
    let request = request
        .map_err(|_| HttpError::invalid("request body is invalid", None))?
        .0;
    validate_backends(&state, &request.backends)?;
    state
        .coverage
        .resolve(request)
        .await
        .map(Json)
        .map(IntoResponse::into_response)
        .map_err(|_| HttpError::unavailable(RETRY_AFTER_SECONDS))
}

pub async fn ready(State(state): State<AppState>) -> Response {
    let ready = state.registry.is_ready().await;
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(ReadyResponse { ready })).into_response()
}

pub async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    require_revision_header(&headers)?;
    let scheduler = state.registry.snapshot().await;
    let dependencies = state
        .status
        .probe()
        .await
        .unwrap_or_else(|_| unavailable_dependencies());
    Ok(Json(StatusResponse {
        service_revision: state.config.service_revision.clone(),
        api_revision: API_REVISION,
        supported_chain_id: state.config.supported_chain_id,
        enabled_backends: state.config.enabled_backends.clone(),
        visible_through_block: dependencies.visible_through_block,
        database: dependencies.database,
        object_store: dependencies.object_store,
        jobs: scheduler.counts,
        reserved_decoded_bytes: scheduler.reserved_decoded_bytes,
        limits: state.config.public_limits(),
        draining: scheduler.draining,
    })
    .into_response())
}

fn validate_quote(state: &AppState, request: &QuoteJobRequest) -> Result<(), HttpError> {
    validate_timeout(state, request.timeout_ms)?;
    validate_backends(state, &[request.pool.backend])?;
    let span = request.blocks.end_inclusive - request.blocks.start;
    if span > state.config.max_block_span {
        return Err(HttpError::invalid(
            "requested block span exceeds the configured maximum",
            Some(serde_json::json!({"field": "blocks"})),
        ));
    }
    for lag in &request.lags {
        if request.blocks.end_inclusive.checked_add(*lag).is_none() {
            return Err(HttpError::invalid(
                "target block exceeds the block number range",
                Some(serde_json::json!({"field": "lags"})),
            ));
        }
    }
    let dimensions = quote_dimensions(request)?;
    if dimensions.combination_count > state.config.max_combination_count {
        let details = ComparisonBudgetDetails {
            start_block_count: dimensions.start_block_count,
            lag_count: dimensions.lag_count,
            input_amount_count: dimensions.input_amount_count,
            combination_count: dimensions.combination_count,
            max_combination_count: state.config.max_combination_count,
        };
        return Err(HttpError::comparison_budget(
            serde_json::to_value(details).unwrap_or(serde_json::Value::Null),
        ));
    }
    Ok(())
}

fn validate_consistency(
    state: &AppState,
    request: &ConsistencyCheckRequest,
) -> Result<(), HttpError> {
    validate_timeout(state, request.timeout_ms)?;
    validate_backends(state, &request.backends)?;
    match &request.selection {
        ConsistencySelection::CheckpointPairs { pairs } => {
            if pairs.len() > state.config.max_consistency_pairs {
                return Err(HttpError::invalid(
                    "checkpoint pair count exceeds the configured maximum",
                    Some(serde_json::json!({"field": "selection.pairs"})),
                ));
            }
        }
        ConsistencySelection::BlockRange {
            start,
            end_inclusive,
        } => {
            if end_inclusive - start > state.config.max_block_span {
                return Err(HttpError::invalid(
                    "requested block span exceeds the configured maximum",
                    Some(serde_json::json!({"field": "selection"})),
                ));
            }
        }
    }
    if request.quotes_to_compare.len() > state.config.max_quote_selectors {
        return Err(HttpError::invalid(
            "quote selector count exceeds the configured maximum",
            Some(serde_json::json!({"field": "quotesToCompare"})),
        ));
    }
    if request
        .quotes_to_compare
        .iter()
        .any(|quote| quote.amounts_in.len() > state.config.max_amounts_per_quote)
    {
        return Err(HttpError::invalid(
            "quote input amount count exceeds the configured maximum",
            Some(serde_json::json!({"field": "quotesToCompare.amountsIn"})),
        ));
    }
    Ok(())
}

fn validate_timeout(state: &AppState, timeout_ms: u64) -> Result<(), HttpError> {
    if timeout_ms > state.config.max_timeout_ms {
        return Err(HttpError::invalid(
            "timeoutMs exceeds the configured maximum",
            Some(serde_json::json!({"field": "timeoutMs"})),
        ));
    }
    Ok(())
}

fn validate_backends(state: &AppState, backends: &[Backend]) -> Result<(), HttpError> {
    if backends
        .iter()
        .any(|backend| !state.config.enabled_backends.contains(backend))
    {
        return Err(HttpError::invalid(
            "requested backend is not enabled",
            Some(serde_json::json!({"field": "backends"})),
        ));
    }
    Ok(())
}

fn quote_dimensions(request: &QuoteJobRequest) -> Result<QuoteDimensions, HttpError> {
    let start_block_count = u64::try_from(request.blocks.start_blocks().count())
        .map_err(|_| HttpError::invalid("requested block count is too large", None))?;
    let lag_count = u64::try_from(request.lags.len())
        .map_err(|_| HttpError::invalid("lag count is too large", None))?;
    let input_amount_count = u64::try_from(request.amounts_in.len())
        .map_err(|_| HttpError::invalid("input amount count is too large", None))?;
    let combination_count = start_block_count
        .checked_mul(lag_count)
        .and_then(|count| count.checked_mul(input_amount_count))
        .ok_or_else(|| HttpError::invalid("quote comparison count is too large", None))?;
    Ok(QuoteDimensions {
        start_block_count,
        lag_count,
        input_amount_count,
        combination_count,
    })
}

fn submission_response(outcome: SubmitOutcome) -> Result<Response, HttpError> {
    let (status, submission) = match outcome {
        SubmitOutcome::Accepted(submission) => (StatusCode::ACCEPTED, submission),
        SubmitOutcome::Reused(submission) => (StatusCode::OK, submission),
        SubmitOutcome::RequestIdConflict => return Err(HttpError::request_id_conflict()),
        SubmitOutcome::QueueFull => return Err(HttpError::queue_full(RETRY_AFTER_SECONDS)),
        SubmitOutcome::ServiceUnavailable | SubmitOutcome::InternalError => {
            return Err(HttpError::unavailable(RETRY_AFTER_SECONDS));
        }
    };
    tracing::info!(
        job_id = %submission.job_id,
        request_id = %submission.request_id,
        job_type = ?submission.job_type,
        state = ?submission.state,
        reused = submission.reused,
        "historical job submission accepted"
    );
    let location = format!("/jobs/{}", submission.job_id);
    let mut response = (status, Json(submission)).into_response();
    if let Ok(value) = HeaderValue::from_str(&location) {
        response.headers_mut().insert(LOCATION, value);
    }
    insert_integer_header(&mut response, RETRY_AFTER, RETRY_AFTER_SECONDS);
    Ok(response)
}

fn require_revision_header(headers: &HeaderMap) -> Result<(), HttpError> {
    let matches = headers
        .get(REVISION_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == API_REVISION.to_string());
    if matches {
        Ok(())
    } else {
        Err(HttpError::invalid(
            "X-DSolver-History-API-Revision must match the service revision",
            Some(serde_json::json!({"field": "X-DSolver-History-API-Revision"})),
        ))
    }
}

fn parse_job_id(value: &str) -> Result<Uuid, HttpError> {
    Uuid::parse_str(value).map_err(|_| HttpError::job_not_found())
}

fn insert_integer_header(response: &mut Response, name: axum::http::HeaderName, value: u64) {
    if let Ok(value) = HeaderValue::from_str(&value.to_string()) {
        response.headers_mut().insert(name, value);
    }
}

fn unavailable_dependencies() -> StatusDependencies {
    StatusDependencies {
        visible_through_block: None,
        database: DependencyHealth {
            state: DependencyState::Unavailable,
            message: Some("read replica status is unavailable".to_owned()),
        },
        object_store: DependencyHealth {
            state: DependencyState::Unavailable,
            message: Some("object-store status is unavailable".to_owned()),
        },
    }
}

struct QuoteDimensions {
    start_block_count: u64,
    lag_count: u64,
    input_amount_count: u64,
    combination_count: u64,
}
