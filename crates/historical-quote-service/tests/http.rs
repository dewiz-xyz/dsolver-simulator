#![expect(
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests should fail immediately when a fixture or assertion is invalid"
)]

use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{Request, StatusCode};
use historical_quote::api::{
    Backend, CoverageRequest, CoverageResponse, DependencyHealth, DependencyState,
    HistoricalQuoteResult, JobResult, StatusResponse,
};
use historical_quote::EngineProgress;
use historical_quote_service::app::{
    router, AppState, CoverageResolver, ServiceDependencyError, StatusDependencies, StatusProbe,
};
use historical_quote_service::auth::{
    BearerKeySetError, BearerKeySetHandle, BearerKeySetReloader, KeySetSource, KeySetVersion,
};
use historical_quote_service::config::{
    ObjectStoreConfig, ReplicaDatabaseConfig, ServiceConfig, DEFAULT_DECODED_BYTE_BUDGET,
    DEFAULT_JOB_DECODED_BYTE_RESERVATION, DEFAULT_MAX_AMOUNTS_PER_QUOTE, DEFAULT_MAX_BLOCK_SPAN,
    DEFAULT_MAX_COMBINATION_COUNT, DEFAULT_MAX_CONSISTENCY_PAIRS, DEFAULT_MAX_QUOTE_SELECTORS,
    DEFAULT_MAX_STRUCTURED_DIFFERENCES, DEFAULT_MAX_TIMEOUT_MS, DEFAULT_TIMEOUT_MS,
};
use historical_quote_service::jobs::{
    ExecutionContext, JobExecutionError, JobExecutor, JobLimits, JobRegistry, JobRequest, JobRunner,
};
use sha2::{Digest, Sha256};
use tokio::sync::Notify;
use tower::ServiceExt;
use uuid::Uuid;

mod support;

use support::{consistency_request, quote_request, ControlledExecutor};

const EDSON_TOKEN: &str = "edson-test-token";
const PEDRO_TOKEN: &str = "pedro-test-token";
const REVISION_HEADER: &str = "x-dsolver-history-api-revision";
const INITIAL_KEY_VERSION: &str = "00000000-0000-4000-8000-000000000001";
const AUTH_VERSION_HEADER: &str = "x-dsolver-history-auth-version";
const AUTH_AGE_HEADER: &str = "x-dsolver-history-auth-refresh-age-seconds";
const AUTH_STATE_HEADER: &str = "x-dsolver-history-auth-refresh-state";

#[tokio::test]
async fn only_ready_is_open_and_bodyless_routes_require_exact_revision() {
    let (app, registry) = test_app(Arc::new(ImmediateExecutor)).await;
    assert_eq!(
        send(&app, request("GET", "/ready", None, None, false))
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        send(&app, request("GET", "/status", None, None, false))
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        send(
            &app,
            request("GET", "/status", None, Some(EDSON_TOKEN), false),
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send(
            &app,
            request("GET", "/status", None, Some(EDSON_TOKEN), true),
        )
        .await
        .status(),
        StatusCode::OK
    );
    registry.begin_shutdown().await;
}

#[tokio::test]
async fn job_paths_require_uuid_version_four_before_lookup() {
    let (app, registry) = test_app(Arc::new(ImmediateExecutor)).await;
    for job_id in [
        "not-a-uuid",
        "00000000-0000-1000-8000-000000000000",
        "00000000-0000-4000-0000-000000000000",
    ] {
        let path = format!("/jobs/{job_id}");
        let response = send(&app, request("GET", &path, None, Some(EDSON_TOKEN), true)).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(json_body(response).await["details"]["field"], "jobId");
    }

    let unknown = format!("/jobs/{}", Uuid::new_v4());
    assert_eq!(
        send(
            &app,
            request("GET", &unknown, None, Some(EDSON_TOKEN), true),
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    registry.begin_shutdown().await;
}

#[tokio::test]
async fn key_changes_take_effect_without_rebuilding_the_router() {
    let source = Arc::new(MutableKeySource::new());
    let (keys, mut reloader) = BearerKeySetReloader::load(source.clone())
        .await
        .expect("load keys");
    let (app, registry) = test_app_with_keys(
        Arc::new(ImmediateExecutor),
        test_job_limits(),
        keys,
        Arc::new(StaticStatus),
    );
    for token in [EDSON_TOKEN, PEDRO_TOKEN] {
        let response = send(&app, request("GET", "/status", None, Some(token), true)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }
    let unknown = send(
        &app,
        request("GET", "/status", None, Some("unknown-token"), true),
    )
    .await;
    assert_eq!(unknown.status(), StatusCode::UNAUTHORIZED);

    let base = [("edson", EDSON_TOKEN), ("pedro", PEDRO_TOKEN)];
    for entries in [
        [base.to_vec(), vec![("analytics", "analytics-old")]].concat(),
        [base.to_vec(), vec![("analytics", "analytics-new")]].concat(),
        base.to_vec(),
    ] {
        let version = Uuid::new_v4().to_string();
        source.replace(&version, &entries);
        reloader.refresh_once().await.expect("reload keys");
        for token in [EDSON_TOKEN, PEDRO_TOKEN, "analytics-old", "analytics-new"] {
            let response = send(&app, request("GET", "/status", None, Some(token), true)).await;
            if entries.iter().any(|(_, accepted)| *accepted == token) {
                assert_eq!(response.status(), StatusCode::OK);
                assert_eq!(response.headers()[AUTH_VERSION_HEADER], version);
                assert_eq!(response.headers()[AUTH_STATE_HEADER], "healthy");
                let status: StatusResponse = serde_json::from_value(json_body(response).await)
                    .expect("unchanged status body");
                assert_eq!(status.api_revision, 1);
            } else {
                assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
                assert_no_auth_metadata(&response);
            }
        }
    }
    assert_no_auth_metadata(&send(&app, request("GET", "/ready", None, None, false)).await);
    assert_no_auth_metadata(
        &send(
            &app,
            request("GET", "/status", None, Some(PEDRO_TOKEN), false),
        )
        .await,
    );
    registry.begin_shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn failed_refresh_keeps_authentication_and_exposes_degradation() {
    let source = Arc::new(MutableKeySource::new());
    let (keys, mut reloader) = BearerKeySetReloader::load(source.clone())
        .await
        .expect("load keys");
    let (app, registry) = test_app_with_keys(
        Arc::new(ImmediateExecutor),
        test_job_limits(),
        keys,
        Arc::new(StaticStatus),
    );
    source.replace(&Uuid::new_v4().to_string(), &[]);
    assert!(reloader.refresh_once().await.is_err());
    tokio::time::advance(Duration::from_secs(60)).await;
    let response = send(
        &app,
        request("GET", "/status", None, Some(PEDRO_TOKEN), true),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[AUTH_VERSION_HEADER], INITIAL_KEY_VERSION);
    assert_eq!(response.headers()[AUTH_STATE_HEADER], "degraded");
    assert_eq!(response.headers()[AUTH_AGE_HEADER], "60");
    source.replace(
        INITIAL_KEY_VERSION,
        &[("edson", EDSON_TOKEN), ("pedro", PEDRO_TOKEN)],
    );
    reloader.refresh_once().await.expect("recover refresh");
    let recovered = send(
        &app,
        request("GET", "/status", None, Some(PEDRO_TOKEN), true),
    )
    .await;
    assert_eq!(recovered.headers()[AUTH_STATE_HEADER], "healthy");
    assert_eq!(recovered.headers()[AUTH_AGE_HEADER], "0");
    registry.begin_shutdown().await;
}

#[tokio::test]
async fn status_headers_describe_the_snapshot_that_authenticated_the_request() {
    let source = Arc::new(MutableKeySource::new());
    let (keys, mut reloader) = BearerKeySetReloader::load(source.clone())
        .await
        .expect("load keys");
    let status = Arc::new(BlockedStatus::default());
    let (app, registry) = test_app_with_keys(
        Arc::new(ImmediateExecutor),
        test_job_limits(),
        keys,
        status.clone(),
    );
    let admitted_app = app.clone();
    let admitted = tokio::spawn(async move {
        send(
            &admitted_app,
            request("GET", "/status", None, Some(EDSON_TOKEN), true),
        )
        .await
    });
    status.entered.notified().await;
    source.replace(&Uuid::new_v4().to_string(), &[("pedro", PEDRO_TOKEN)]);
    reloader
        .refresh_once()
        .await
        .expect("revoke admitted token");
    status.release.notify_one();
    let response = admitted.await.expect("admitted request completes");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[AUTH_VERSION_HEADER], INITIAL_KEY_VERSION);
    let revoked = send(
        &app,
        request("GET", "/status", None, Some(EDSON_TOKEN), true),
    )
    .await;
    assert_eq!(revoked.status(), StatusCode::UNAUTHORIZED);
    assert_no_auth_metadata(&revoked);
    registry.begin_shutdown().await;
}

fn assert_no_auth_metadata(response: &axum::response::Response) {
    for name in [AUTH_VERSION_HEADER, AUTH_AGE_HEADER, AUTH_STATE_HEADER] {
        assert!(!response.headers().contains_key(name));
    }
}

#[derive(Default)]
struct BlockedStatus {
    entered: Notify,
    release: Notify,
}

impl StatusProbe for BlockedStatus {
    fn probe(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<StatusDependencies, ServiceDependencyError>> + Send + '_>>
    {
        Box::pin(async move {
            self.entered.notify_one();
            self.release.notified().await;
            StaticStatus.probe().await
        })
    }
}

#[tokio::test]
async fn quote_submission_reuses_retained_work_and_consumes_terminal_result_once() {
    let (app, registry) = test_app(Arc::new(ImmediateExecutor)).await;
    let request_id = Uuid::new_v4();
    let body = serde_json::to_value(quote_request(request_id, 60_000))
        .expect("quote request must serialize");
    let submitted = send(
        &app,
        request("POST", "/jobs/quote", Some(&body), Some(EDSON_TOKEN), false),
    )
    .await;
    assert_eq!(submitted.status(), StatusCode::ACCEPTED);
    assert_eq!(submitted.headers()["retry-after"], "2");
    let location = submitted.headers()["location"]
        .to_str()
        .expect("location must be text")
        .to_owned();

    wait_terminal(&registry, &location).await;
    let reused = send(
        &app,
        request("POST", "/jobs/quote", Some(&body), Some(EDSON_TOKEN), false),
    )
    .await;
    assert_eq!(reused.status(), StatusCode::OK);
    let reused_body = json_body(reused).await;
    assert_eq!(reused_body["reused"], true);
    assert!(reused_body.get("result").is_none());

    let terminal = send(
        &app,
        request("GET", &location, None, Some(EDSON_TOKEN), true),
    )
    .await;
    assert_eq!(terminal.status(), StatusCode::OK);
    let terminal_body = json_body(terminal).await;
    assert_eq!(terminal_body["state"], "completed");
    assert!(terminal_body.get("result").is_some());
    assert_eq!(
        send(
            &app,
            request("GET", &location, None, Some(EDSON_TOKEN), true),
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    registry.begin_shutdown().await;
}

#[tokio::test]
async fn admission_errors_use_safe_shapes_and_budget_details() {
    let (app, registry) = test_app(Arc::new(ImmediateExecutor)).await;
    let request_id = Uuid::new_v4();
    let mut body = serde_json::to_value(quote_request(request_id, 60_000))
        .expect("quote request must serialize");
    body["blocks"]["endInclusive"] = serde_json::json!(1_890);
    body["lags"] = serde_json::json!([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    body["amountsIn"] = serde_json::json!(["1", "2"]);
    let response = send(
        &app,
        request("POST", "/jobs/quote", Some(&body), Some(EDSON_TOKEN), false),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let error = json_body(response).await;
    assert_eq!(error["code"], "comparison_budget_exceeded");
    assert_eq!(error["details"]["startBlockCount"], 1_791);
    assert_eq!(error["details"]["lagCount"], 10);
    assert_eq!(error["details"]["inputAmountCount"], 2);
    assert_eq!(error["details"]["combinationCount"], 35_820);
    assert_eq!(error["details"]["maxCombinationCount"], 20_000);
    assert!(error.get("stack").is_none());

    let malformed = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/jobs/quote")
            .header(AUTHORIZATION, format!("Bearer {EDSON_TOKEN}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from("{not-json"))
            .expect("request must build"),
    )
    .await;
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        json_body(malformed).await["message"],
        "request body is invalid"
    );
    registry.begin_shutdown().await;
}

#[tokio::test]
async fn span_and_deadline_limits_fail_at_admission() {
    let (app, registry) = test_app(Arc::new(ImmediateExecutor)).await;
    let mut over_span = serde_json::to_value(quote_request(Uuid::new_v4(), DEFAULT_TIMEOUT_MS))
        .expect("quote request must serialize");
    over_span["blocks"]["endInclusive"] = serde_json::json!(1_901);
    let response = send(
        &app,
        request(
            "POST",
            "/jobs/quote",
            Some(&over_span),
            Some(EDSON_TOKEN),
            false,
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json_body(response).await["details"]["field"], "blocks");

    let mut exact_span = serde_json::to_value(quote_request(Uuid::new_v4(), DEFAULT_TIMEOUT_MS))
        .expect("quote request must serialize");
    exact_span["lags"] = serde_json::json!([DEFAULT_MAX_BLOCK_SPAN]);
    assert_eq!(
        send(
            &app,
            request(
                "POST",
                "/jobs/quote",
                Some(&exact_span),
                Some(EDSON_TOKEN),
                false,
            ),
        )
        .await
        .status(),
        StatusCode::ACCEPTED
    );

    let mut lagged_over_span =
        serde_json::to_value(quote_request(Uuid::new_v4(), DEFAULT_TIMEOUT_MS))
            .expect("quote request must serialize");
    lagged_over_span["lags"] = serde_json::json!([DEFAULT_MAX_BLOCK_SPAN + 1]);
    let response = send(
        &app,
        request(
            "POST",
            "/jobs/quote",
            Some(&lagged_over_span),
            Some(EDSON_TOKEN),
            false,
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json_body(response).await["details"]["field"], "blocks");

    let mut overflow = serde_json::to_value(quote_request(Uuid::new_v4(), DEFAULT_TIMEOUT_MS))
        .expect("quote request must serialize");
    overflow["blocks"]["start"] = serde_json::json!(u64::MAX);
    overflow["blocks"]["endInclusive"] = serde_json::json!(u64::MAX);
    let response = send(
        &app,
        request(
            "POST",
            "/jobs/quote",
            Some(&overflow),
            Some(EDSON_TOKEN),
            false,
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json_body(response).await["details"]["field"], "lags");

    let over_deadline =
        serde_json::to_value(quote_request(Uuid::new_v4(), DEFAULT_MAX_TIMEOUT_MS + 1))
            .expect("quote request must serialize");
    let response = send(
        &app,
        request(
            "POST",
            "/jobs/quote",
            Some(&over_deadline),
            Some(EDSON_TOKEN),
            false,
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json_body(response).await["details"]["field"], "timeoutMs");
    registry.begin_shutdown().await;
}

#[tokio::test]
async fn cancellation_is_retry_safe_and_does_not_consume_the_terminal_envelope() {
    let executor = Arc::new(ControlledExecutor::default());
    let (app, registry) = test_app(executor.clone()).await;
    let body = serde_json::to_value(quote_request(Uuid::new_v4(), 60_000))
        .expect("quote request must serialize");
    let submitted = send(
        &app,
        request("POST", "/jobs/quote", Some(&body), Some(EDSON_TOKEN), false),
    )
    .await;
    let location = submitted.headers()["location"]
        .to_str()
        .expect("location must be text")
        .to_owned();
    let started = executor.next_started().await;
    assert!(!started.job_id.is_nil());
    started.progress.quote_comparison_completed();
    let cancel_path = format!("{location}/cancel");
    let running = send(
        &app,
        request("POST", &cancel_path, None, Some(EDSON_TOKEN), true),
    )
    .await;
    assert_eq!(running.status(), StatusCode::OK);
    assert_eq!(json_body(running).await["cancellationRequested"], true);
    wait_terminal(&registry, &location).await;

    let cancelled = send(
        &app,
        request("POST", &cancel_path, None, Some(EDSON_TOKEN), true),
    )
    .await;
    assert_eq!(cancelled.status(), StatusCode::OK);
    assert_eq!(json_body(cancelled).await["state"], "cancelled");
    let poll = send(
        &app,
        request("GET", &location, None, Some(EDSON_TOKEN), true),
    )
    .await;
    assert_eq!(poll.status(), StatusCode::OK);
    assert_eq!(json_body(poll).await["state"], "cancelled");
    registry.begin_shutdown().await;
}

#[tokio::test]
async fn incomplete_terminal_body_delivery_releases_the_lease() {
    let executor = Arc::new(ControlledExecutor::default());
    let (app, registry) = test_app(executor.clone()).await;
    let body = serde_json::to_value(quote_request(Uuid::new_v4(), 60_000))
        .expect("quote request must serialize");
    let submitted = send(
        &app,
        request("POST", "/jobs/quote", Some(&body), Some(EDSON_TOKEN), false),
    )
    .await;
    let location = submitted.headers()["location"]
        .to_str()
        .expect("location must be text")
        .to_owned();
    executor.next_started().await.complete_quote();
    wait_terminal(&registry, &location).await;

    let selected = send(
        &app,
        request("GET", &location, None, Some(EDSON_TOKEN), true),
    )
    .await;
    assert_eq!(selected.status(), StatusCode::OK);
    drop(selected);
    tokio::task::yield_now().await;
    let retried = send(
        &app,
        request("GET", &location, None, Some(EDSON_TOKEN), true),
    )
    .await;
    assert_eq!(retried.status(), StatusCode::OK);
    assert_eq!(json_body(retried).await["state"], "completed");
    registry.begin_shutdown().await;
}

#[tokio::test]
async fn consistency_submission_uses_the_shared_job_lifecycle() {
    let executor = Arc::new(ControlledExecutor::default());
    let (app, registry) = test_app(executor.clone()).await;
    let consistency = consistency_request(Uuid::new_v4(), 60_000);
    let body = serde_json::to_value(consistency).expect("consistency request must serialize");
    let submitted = send(
        &app,
        request(
            "POST",
            "/jobs/consistency-check",
            Some(&body),
            Some(EDSON_TOKEN),
            false,
        ),
    )
    .await;
    assert_eq!(submitted.status(), StatusCode::ACCEPTED);
    assert_eq!(
        json_body(submitted).await["jobType"],
        "historicalStateConsistencyCheck"
    );
    let started = executor.next_started().await;
    assert!(!started.job_id.is_nil());
    registry.begin_shutdown().await;
}

#[tokio::test]
async fn full_queue_stays_ready_and_drain_turns_readiness_off() {
    let executor = Arc::new(ControlledExecutor::default());
    let (app, registry) = test_app_with_limits(
        executor.clone(),
        JobLimits {
            max_running: 1,
            max_waiting: 1,
            ..JobLimits::default()
        },
    )
    .await;
    for _ in 0..2 {
        let body = serde_json::to_value(quote_request(Uuid::new_v4(), 60_000))
            .expect("quote request must serialize");
        assert!(matches!(
            send(
                &app,
                request("POST", "/jobs/quote", Some(&body), Some(EDSON_TOKEN), false,)
            )
            .await
            .status(),
            StatusCode::ACCEPTED
        ));
        if executor.try_next_started().is_none() {
            tokio::task::yield_now().await;
        }
    }
    assert_eq!(
        send(&app, request("GET", "/ready", None, None, false))
            .await
            .status(),
        StatusCode::OK
    );
    registry.begin_shutdown().await;
    assert_eq!(
        send(&app, request("GET", "/ready", None, None, false))
            .await
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn protected_status_and_coverage_return_bounded_safe_data() {
    let (app, registry) = test_app(Arc::new(ImmediateExecutor)).await;
    let status = send(
        &app,
        request("GET", "/status", None, Some(EDSON_TOKEN), true),
    )
    .await;
    let status = json_body(status).await;
    assert_eq!(status["apiRevision"], 1);
    assert_eq!(status["visibleThroughBlock"], 123);
    assert_eq!(status["limits"]["maxRunningJobs"], 4);
    assert_eq!(status["limits"]["maxCombinationCount"], 20_000);
    assert_eq!(status["limits"]["maxBlockSpan"], 1_800);
    assert_eq!(status["limits"]["defaultTimeoutMs"], 600_000);
    assert_eq!(status["limits"]["maxTimeoutMs"], 900_000);
    assert_eq!(
        status["limits"]["decodedByteBudget"],
        4_u64 * 1024 * 1024 * 1024
    );
    assert!(status.get("databaseHost").is_none());

    let coverage_request = serde_json::json!({
        "apiRevision": 1,
        "chainId": 8453,
        "backends": ["native"]
    });
    let coverage = send(
        &app,
        request(
            "POST",
            "/coverage",
            Some(&coverage_request),
            Some(EDSON_TOKEN),
            false,
        ),
    )
    .await;
    assert_eq!(coverage.status(), StatusCode::OK);
    assert_eq!(json_body(coverage).await["visibleThroughBlock"], 123);
    registry.begin_shutdown().await;
}

async fn test_app<E>(executor: Arc<E>) -> (axum::Router, JobRegistry)
where
    E: JobExecutor,
{
    test_app_with_limits(executor, test_job_limits()).await
}

async fn test_app_with_limits<E>(executor: Arc<E>, limits: JobLimits) -> (axum::Router, JobRegistry)
where
    E: JobExecutor,
{
    let source = Arc::new(MutableKeySource::new());
    let (bearer_keys, _) = BearerKeySetReloader::load(source)
        .await
        .expect("initial key document must load");
    test_app_with_keys(executor, limits, bearer_keys, Arc::new(StaticStatus))
}

fn test_app_with_keys<E>(
    executor: Arc<E>,
    limits: JobLimits,
    bearer_keys: BearerKeySetHandle,
    status: Arc<dyn StatusProbe>,
) -> (axum::Router, JobRegistry)
where
    E: JobExecutor,
{
    let mut config = test_config();
    config.scheduler = limits.clone();
    let registry = JobRegistry::new(limits);
    let runner = JobRunner::new(registry.clone(), executor);
    tokio::spawn(runner.run());
    let app = router(AppState {
        config: Arc::new(config),
        bearer_keys,
        registry: registry.clone(),
        coverage: Arc::new(StaticCoverage),
        status,
    });
    (app, registry)
}

fn test_config() -> ServiceConfig {
    ServiceConfig {
        bind_address: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        supported_chain_id: 8_453,
        service_revision: "test-revision".to_owned(),
        enabled_backends: vec![Backend::Native, Backend::Rfq],
        bearer_key_set_secret_arn:
            "arn:aws:secretsmanager:eu-central-1:123456789012:secret:test-123456".to_owned(),
        scheduler: test_job_limits(),
        max_combination_count: DEFAULT_MAX_COMBINATION_COUNT,
        max_block_span: DEFAULT_MAX_BLOCK_SPAN,
        default_timeout_ms: DEFAULT_TIMEOUT_MS,
        max_timeout_ms: DEFAULT_MAX_TIMEOUT_MS,
        job_decoded_byte_reservation: DEFAULT_JOB_DECODED_BYTE_RESERVATION,
        max_consistency_pairs: DEFAULT_MAX_CONSISTENCY_PAIRS,
        max_quote_selectors: DEFAULT_MAX_QUOTE_SELECTORS,
        max_amounts_per_quote: DEFAULT_MAX_AMOUNTS_PER_QUOTE,
        max_structured_differences: DEFAULT_MAX_STRUCTURED_DIFFERENCES,
        database: ReplicaDatabaseConfig {
            host: "replica.invalid".to_owned(),
            port: 5_432,
            database: "state_history".to_owned(),
            username: "state_history_observer".to_owned(),
            max_connections: 1,
            max_connection_lifetime: Duration::from_secs(60),
        },
        object_store: ObjectStoreConfig {
            bucket: "history".to_owned(),
            prefix: "state".to_owned(),
            region: "eu-central-1".to_owned(),
            endpoint_url: None,
            force_path_style: false,
        },
        shutdown_timeout: Duration::from_secs(1),
    }
}

fn test_job_limits() -> JobLimits {
    JobLimits {
        max_running: 4,
        max_waiting: 16,
        decoded_byte_budget: DEFAULT_DECODED_BYTE_BUDGET,
        max_terminal_jobs: 20,
        terminal_ttl: Duration::from_secs(3_600),
    }
}

struct MutableKeySource {
    version: Mutex<KeySetVersion>,
}

impl MutableKeySource {
    fn new() -> Self {
        Self {
            version: Mutex::new(key_version(
                INITIAL_KEY_VERSION,
                &[("edson", EDSON_TOKEN), ("pedro", PEDRO_TOKEN)],
            )),
        }
    }

    fn replace(&self, version: &str, keys: &[(&str, &str)]) {
        *self
            .version
            .lock()
            .expect("key fixture lock must be available") = key_version(version, keys);
    }
}

impl KeySetSource for MutableKeySource {
    fn read_current(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<KeySetVersion, BearerKeySetError>> + Send + '_>> {
        let version = self
            .version
            .lock()
            .expect("key fixture lock must be available");
        let current = KeySetVersion {
            version_id: version.version_id.clone(),
            document: version.document.clone(),
        };
        Box::pin(async move { Ok(current) })
    }
}

fn key_version(version: &str, keys: &[(&str, &str)]) -> KeySetVersion {
    let entries: Vec<_> = keys
        .iter()
        .map(|(id, token)| {
            serde_json::json!({
                "id": id,
                "sha256": hex::encode(Sha256::digest(token.as_bytes())),
            })
        })
        .collect();
    KeySetVersion {
        version_id: version.to_owned(),
        document: serde_json::json!({"schemaVersion": 1, "keys": entries}).to_string(),
    }
}

#[derive(Clone, Copy)]
struct ImmediateExecutor;

impl JobExecutor for ImmediateExecutor {
    fn execute(
        &self,
        context: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = Result<JobResult, JobExecutionError>> + Send + 'static>> {
        Box::pin(async move {
            match context.request {
                JobRequest::HistoricalQuote(request) => {
                    let result: HistoricalQuoteResult = serde_json::from_value(serde_json::json!({
                        "requestId": request.request_id,
                        "chainId": 8453,
                        "pool": request.pool,
                        "visibleThroughBlock": 123,
                        "gaps": [],
                        "results": []
                    }))
                    .expect("quote result fixture must be valid");
                    Ok(JobResult::HistoricalQuote(result))
                }
                JobRequest::HistoricalStateConsistencyCheck(_) => Err(JobExecutionError::Cancelled),
            }
        })
    }
}

struct StaticCoverage;

impl CoverageResolver for StaticCoverage {
    fn resolve(
        &self,
        request: CoverageRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CoverageResponse, ServiceDependencyError>> + Send + '_>>
    {
        Box::pin(async move {
            Ok(CoverageResponse {
                api_revision: request.api_revision,
                chain_id: request.chain_id,
                backends: request.backends,
                visible_through_block: 123,
                continuous_intervals: Vec::new(),
                known_gaps: Vec::new(),
            })
        })
    }
}

struct StaticStatus;

impl StatusProbe for StaticStatus {
    fn probe(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<StatusDependencies, ServiceDependencyError>> + Send + '_>>
    {
        Box::pin(async {
            Ok(StatusDependencies {
                visible_through_block: Some(123),
                database: DependencyHealth {
                    state: DependencyState::Healthy,
                    message: None,
                },
                object_store: DependencyHealth {
                    state: DependencyState::Healthy,
                    message: None,
                },
            })
        })
    }
}

fn request(
    method: &str,
    uri: &str,
    body: Option<&serde_json::Value>,
    bearer_token: Option<&str>,
    revision: bool,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = bearer_token {
        builder = builder.header(AUTHORIZATION, format!("Bearer {token}"));
    }
    if revision {
        builder = builder.header(REVISION_HEADER, "1");
    }
    let payload = if let Some(body) = body {
        builder = builder.header(CONTENT_TYPE, "application/json");
        Body::from(serde_json::to_vec(body).expect("request body must serialize"))
    } else {
        Body::empty()
    };
    builder.body(payload).expect("request must build")
}

async fn send(app: &axum::Router, request: Request<Body>) -> axum::response::Response {
    app.clone()
        .oneshot(request)
        .await
        .expect("router must respond")
}

async fn json_body(response: axum::response::Response) -> serde_json::Value {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body must be readable");
    serde_json::from_slice(&bytes).expect("response body must be JSON")
}

async fn wait_terminal(registry: &JobRegistry, location: &str) {
    let job_id = Uuid::parse_str(location.trim_start_matches("/jobs/"))
        .expect("location must contain a UUID");
    for _ in 0..20 {
        if registry.inspect(job_id).await.is_some_and(|job| {
            matches!(
                job.state,
                historical_quote::api::JobState::Completed
                    | historical_quote::api::JobState::Cancelled
                    | historical_quote::api::JobState::Failed
            )
        }) {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("job did not become terminal");
}
