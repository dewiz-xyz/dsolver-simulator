//! Generated OpenAPI 3.1 contract.

use utoipa::{
    openapi::{
        schema::{AdditionalProperties, Schema},
        RefOr,
    },
    Modify, OpenApi,
};

use crate::api::{
    ApiError, ApiErrorCode, Backend, BlockBounds, BlockRange, CanonicalStateSummary,
    CheckpointPair, ComparisonBudgetDetails, ConsistencyCheckProgress, ConsistencyCheckReport,
    ConsistencyCheckRequest, ConsistencyDifference, ConsistencyPairReport,
    ConsistencyQuoteComparison, ConsistencyQuoteOutcome, ConsistencySelection,
    ConsistencySelectionStatus, ConsistencySummary, CoverageRequest, CoverageResponse,
    DependencyHealth, DependencyState, GapCause, GapKind, GapReference, HistoricalQuoteEntry,
    HistoricalQuoteProgress, HistoricalQuoteResult, HistoricalQuoteTarget, JobCounts, JobEnvelope,
    JobFailure, JobFailureCode, JobProgress, JobResult, JobState, JobSubmission, JobType, KnownGap,
    PairStatus, PoolSelector, QuoteComparisonRequest, QuoteFailureReason, QuoteJobRequest,
    QuoteOutcome, QuoteProvenance, ReadyResponse, ServiceLimits, StatusResponse, StreamPosition,
    UnavailableReason, UnsignedAmount,
};

#[utoipa::path(
    post,
    path = "/jobs/quote",
    request_body = QuoteJobRequest,
    responses(
        (status = 200, description = "Existing retained job", body = JobSubmission,
            headers(("Retry-After" = u64, description = "Polling delay in seconds"))),
        (status = 202, description = "New quote job admitted", body = JobSubmission,
            headers(
                ("Location" = String, description = "Job polling path"),
                ("Retry-After" = u64, description = "Polling delay in seconds")
            )),
        (status = 400, description = "Invalid request", body = ApiError),
        (status = 401, description = "Bearer token is absent or wrong", body = ApiError),
        (status = 409, description = "Request ID conflicts with retained content", body = ApiError),
        (status = 422, description = "Comparison budget exceeded", body = ApiError),
        (status = 429, description = "Waiting queue is full", body = ApiError,
            headers(("Retry-After" = u64, description = "Retry delay in seconds"))),
        (status = 503, description = "Service cannot safely admit work", body = ApiError,
            headers(("Retry-After" = u64, description = "Retry delay in seconds")))
    ),
    security(("bearerAuth" = [])),
    tag = "jobs"
)]
#[expect(dead_code, reason = "Utoipa reads this contract-only path")]
pub(crate) fn post_quote_job() {}

#[utoipa::path(
    post,
    path = "/jobs/consistency-check",
    request_body = ConsistencyCheckRequest,
    responses(
        (status = 200, description = "Existing retained job", body = JobSubmission,
            headers(("Retry-After" = u64, description = "Polling delay in seconds"))),
        (status = 202, description = "New consistency check admitted", body = JobSubmission,
            headers(
                ("Location" = String, description = "Job polling path"),
                ("Retry-After" = u64, description = "Polling delay in seconds")
            )),
        (status = 400, description = "Invalid request", body = ApiError),
        (status = 401, description = "Bearer token is absent or wrong", body = ApiError),
        (status = 409, description = "Request ID conflicts with retained content", body = ApiError),
        (status = 422, description = "Admission budget exceeded", body = ApiError),
        (status = 429, description = "Waiting queue is full", body = ApiError,
            headers(("Retry-After" = u64, description = "Retry delay in seconds"))),
        (status = 503, description = "Service cannot safely admit work", body = ApiError,
            headers(("Retry-After" = u64, description = "Retry delay in seconds")))
    ),
    security(("bearerAuth" = [])),
    tag = "jobs"
)]
#[expect(dead_code, reason = "Utoipa reads this contract-only path")]
pub(crate) fn post_consistency_job() {}

#[utoipa::path(
    get,
    path = "/jobs/{jobId}",
    params(
        ("jobId" = uuid::Uuid, Path, description = "Opaque UUID version 4 job ID"),
        ("X-DSolver-History-API-Revision" = u32, Header, minimum = 1, maximum = 1,
            description = "Exact API revision")
    ),
    responses(
        (status = 200, description = "Current job envelope", body = JobEnvelope,
            headers(("Retry-After" = u64, description = "Present while work is incomplete"))),
        (status = 400, description = "Job ID or API revision is invalid", body = ApiError),
        (status = 401, description = "Bearer token is absent or wrong", body = ApiError),
        (status = 404, description = "Job is not retained", body = ApiError)
    ),
    security(("bearerAuth" = [])),
    tag = "jobs"
)]
#[expect(dead_code, reason = "Utoipa reads this contract-only path")]
pub(crate) fn get_job() {}

#[utoipa::path(
    post,
    path = "/jobs/{jobId}/cancel",
    params(
        ("jobId" = uuid::Uuid, Path, description = "Opaque UUID version 4 job ID"),
        ("X-DSolver-History-API-Revision" = u32, Header, minimum = 1, maximum = 1,
            description = "Exact API revision")
    ),
    responses(
        (status = 200, description = "Current state after the cancellation request", body = JobEnvelope),
        (status = 400, description = "Job ID or API revision is invalid", body = ApiError),
        (status = 401, description = "Bearer token is absent or wrong", body = ApiError),
        (status = 404, description = "Job is not retained", body = ApiError)
    ),
    security(("bearerAuth" = [])),
    tag = "jobs"
)]
#[expect(dead_code, reason = "Utoipa reads this contract-only path")]
pub(crate) fn cancel_job() {}

#[utoipa::path(
    post,
    path = "/coverage",
    request_body = CoverageRequest,
    responses(
        (status = 200, description = "Continuous reconstruction coverage", body = CoverageResponse),
        (status = 400, description = "Invalid request", body = ApiError),
        (status = 401, description = "Bearer token is absent or wrong", body = ApiError),
        (status = 503, description = "Coverage cannot be read safely", body = ApiError,
            headers(("Retry-After" = u64, description = "Retry delay in seconds")))
    ),
    security(("bearerAuth" = [])),
    tag = "coverage"
)]
#[expect(dead_code, reason = "Utoipa reads this contract-only path")]
pub(crate) fn post_coverage() {}

#[utoipa::path(
    get,
    path = "/status",
    params(
        ("X-DSolver-History-API-Revision" = u32, Header, minimum = 1, maximum = 1,
            description = "Exact API revision")
    ),
    responses(
        (status = 200, description = "Protected service status", body = StatusResponse),
        (status = 400, description = "API revision is invalid", body = ApiError),
        (status = 401, description = "Bearer token is absent or wrong", body = ApiError),
        (status = 503, description = "Status cannot be read safely", body = ApiError,
            headers(("Retry-After" = u64, description = "Retry delay in seconds")))
    ),
    security(("bearerAuth" = [])),
    tag = "service"
)]
#[expect(dead_code, reason = "Utoipa reads this contract-only path")]
pub(crate) fn get_status() {}

#[utoipa::path(
    get,
    path = "/ready",
    responses(
        (status = 200, description = "Service can admit work", body = ReadyResponse),
        (status = 503, description = "Service is not ready", body = ReadyResponse)
    ),
    tag = "service"
)]
#[expect(dead_code, reason = "Utoipa reads this contract-only path")]
pub(crate) fn get_ready() {}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Historical Pool Quote API",
        version = "1",
        description = "Authenticated historical directed pool quotes from reconstructed Base state."
    ),
    paths(
        post_quote_job,
        post_consistency_job,
        get_job,
        cancel_job,
        post_coverage,
        get_status,
        get_ready
    ),
    components(schemas(
        ApiError,
        ApiErrorCode,
        Backend,
        BlockBounds,
        BlockRange,
        CanonicalStateSummary,
        CheckpointPair,
        ComparisonBudgetDetails,
        ConsistencyCheckProgress,
        ConsistencyCheckReport,
        ConsistencyCheckRequest,
        ConsistencyDifference,
        ConsistencyPairReport,
        ConsistencyQuoteComparison,
        ConsistencyQuoteOutcome,
        ConsistencySelection,
        ConsistencySelectionStatus,
        ConsistencySummary,
        CoverageRequest,
        CoverageResponse,
        DependencyHealth,
        DependencyState,
        GapCause,
        GapKind,
        GapReference,
        HistoricalQuoteEntry,
        HistoricalQuoteProgress,
        HistoricalQuoteResult,
        HistoricalQuoteTarget,
        JobCounts,
        JobEnvelope,
        JobFailure,
        JobFailureCode,
        JobProgress,
        JobResult,
        JobState,
        JobSubmission,
        JobType,
        KnownGap,
        PairStatus,
        PoolSelector,
        QuoteComparisonRequest,
        QuoteFailureReason,
        QuoteJobRequest,
        QuoteOutcome,
        QuoteProvenance,
        ReadyResponse,
        ServiceLimits,
        StatusResponse,
        StreamPosition,
        UnavailableReason,
        UnsignedAmount
    )),
    modifiers(&BearerSecurity, &StrictTaggedUnions),
    tags(
        (name = "jobs", description = "Historical quote and consistency jobs"),
        (name = "coverage", description = "State reconstruction coverage"),
        (name = "service", description = "Readiness and protected status")
    )
)]
pub struct HistoricalPoolQuoteApi;

struct BearerSecurity;

impl Modify for BearerSecurity {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};

        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearerAuth",
                SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
            );
        }
    }
}

struct StrictTaggedUnions;

impl Modify for StrictTaggedUnions {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let Some(components) = openapi.components.as_mut() else {
            return;
        };

        for name in [
            "ConsistencyQuoteOutcome",
            "ConsistencySelection",
            "QuoteOutcome",
        ] {
            let Some(RefOr::T(Schema::OneOf(one_of))) = components.schemas.get_mut(name) else {
                continue;
            };

            // Utoipa does not carry Serde's deny_unknown_fields into inline enum variants.
            for item in &mut one_of.items {
                if let RefOr::T(Schema::Object(object)) = item {
                    object.additional_properties =
                        Some(Box::new(AdditionalProperties::FreeForm(false)));
                }
            }
        }
    }
}

/// Builds the OpenAPI 3.1 document from the Rust wire types.
#[must_use]
pub fn document() -> utoipa::openapi::OpenApi {
    HistoricalPoolQuoteApi::openapi()
}

/// Builds the JSON value used by the checked-in parity test.
#[must_use]
pub fn document_value() -> serde_json::Value {
    serde_json::json!(document())
}
