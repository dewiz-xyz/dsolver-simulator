#![expect(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "the CLI integration harness uses fixed process and HTTP fixtures"
)]

use std::collections::VecDeque;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::Router;
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;

const JOB_ONE: &str = "997b4517-2377-4c83-91a8-577e919ce72a";
const JOB_TWO: &str = "a87b4517-2377-4c83-91a8-577e919ce72a";
const QUOTE_REQUEST_ID: &str = "3bbd9a8c-d942-4f9d-b96d-7ee63b9308aa";
const VERIFY_REQUEST_ID: &str = "8021caa1-0257-4d42-9690-6d7ebd10b8f8";

fn binary() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dsolver-history"));
    command.env_remove("DSOLVER_HISTORY_URL");
    command.env_remove("DSOLVER_HISTORY_TOKEN");
    command
}

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

#[test]
fn token_is_not_a_command_line_option() {
    let output = binary()
        .args([
            "--url",
            "http://127.0.0.1:1",
            "quotes",
            "--token",
            "secret",
            "--request",
        ])
        .arg(fixture("quote_request.json"))
        .output()
        .expect("CLI process must start");

    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("--token"));
}

#[test]
fn quote_request_requires_url_and_token_configuration() {
    let output = binary()
        .args(["quotes", "--request"])
        .arg(fixture("quote_request.json"))
        .output()
        .expect("CLI process must start");

    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("DSOLVER_HISTORY_URL"));
}

#[tokio::test]
async fn request_files_cannot_be_mixed_with_direct_flags() {
    let api = MockApi::start(Vec::new()).await;
    let mut command = async_command(&api);
    command.args(["quotes", "--request"]);
    command.arg(fixture("quote_request.json"));
    command.args(["--chain-id", "8453"]);
    let output = run_command(command, None).await;

    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("cannot be mixed"));
    assert!(api.records().is_empty());

    let api = MockApi::start(Vec::new()).await;
    let output = run_cli_args(
        &api,
        [
            "quotes",
            "--submit-only",
            "--timeout-ms",
            "5000",
            "--backend",
            "native",
        ],
        None,
    )
    .await;
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("--chain-id"));
    assert!(api.records().is_empty());
}

#[tokio::test]
async fn quote_request_file_polls_and_keeps_json_on_stdout() {
    let api = MockApi::start(vec![
        Spec::json(
            StatusCode::ACCEPTED,
            submission(JOB_ONE, QUOTE_REQUEST_ID, "historicalQuote"),
        )
        .header("retry-after", "0"),
        Spec::json(StatusCode::OK, completed_quote(JOB_ONE)),
    ])
    .await;
    let output = run_cli(
        &api,
        ["quotes", "--request"],
        Some(fixture("quote_request.json")),
        None,
    )
    .await;

    assert_eq!(output.status.code(), Some(0));
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["requestId"], QUOTE_REQUEST_ID);
    assert_eq!(
        stdout["results"][0]["targets"][0]["outcome"]["outcome"],
        "unavailable"
    );
    assert!(stdout.get("jobId").is_none());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains(JOB_ONE));
    assert!(stderr.contains("100% complete"));

    let records = api.records();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].path, "/jobs/quote");
    assert_eq!(
        records[0].authorization.as_deref(),
        Some("Bearer test-token")
    );
    assert_eq!(records[1].revision.as_deref(), Some("1"));
}

#[tokio::test]
async fn direct_flags_build_the_typed_quote_request() {
    let api = MockApi::start(vec![Spec::json(
        StatusCode::ACCEPTED,
        submission(JOB_ONE, QUOTE_REQUEST_ID, "historicalQuote"),
    )])
    .await;
    let output = run_cli_args(
        &api,
        [
            "quotes",
            "--submit-only",
            "--request-id",
            QUOTE_REQUEST_ID,
            "--timeout-ms",
            "5000",
            "--chain-id",
            "8453",
            "--backend",
            "native",
            "--protocol",
            "uniswap_v3",
            "--component-id",
            "pool-1",
            "--token-in",
            "0x11",
            "--token-out",
            "0x22",
            "--start-block",
            "100",
            "--end-block-inclusive",
            "110",
            "--step",
            "5",
            "--lag",
            "1,3",
            "--amount-in",
            "100,200",
        ],
        None,
    )
    .await;

    assert_eq!(output.status.code(), Some(0));
    let request = &api.records()[0].body;
    assert_eq!(request["chainId"], 8453);
    assert_eq!(request["blocks"]["endInclusive"], 110);
    assert_eq!(request["lags"], json!([1, 3]));
    assert_eq!(request["amountsIn"], json!(["100", "200"]));
}

#[tokio::test]
async fn direct_verify_flags_build_the_typed_consistency_request() {
    let api = MockApi::start(vec![
        Spec::json(
            StatusCode::ACCEPTED,
            submission(
                JOB_ONE,
                VERIFY_REQUEST_ID,
                "historicalStateConsistencyCheck",
            ),
        ),
        Spec::json(StatusCode::OK, completed_verify(JOB_ONE, "pass")),
    ])
    .await;
    let output = run_cli_args(
        &api,
        [
            "verify",
            "--request-id",
            VERIFY_REQUEST_ID,
            "--timeout-ms",
            "5000",
            "--chain-id",
            "8453",
            "--backend",
            "native,rfq",
            "--checkpoint-pair",
            "12:900..12:1800",
            "--quote-backend",
            "native",
            "--protocol",
            "uniswap_v3",
            "--component-id",
            "pool-1",
            "--token-in",
            "0x11",
            "--token-out",
            "0x22",
            "--amount-in",
            "100,200",
        ],
        None,
    )
    .await;

    assert_eq!(output.status.code(), Some(0));
    let request = &api.records()[0].body;
    assert_eq!(request["chainId"], 8453);
    assert_eq!(request["backends"], json!(["native", "rfq"]));
    assert_eq!(request["selection"]["type"], "checkpointPairs");
    assert_eq!(
        request["quotesToCompare"][0]["amountsIn"],
        json!(["100", "200"])
    );
}

#[tokio::test]
async fn url_flag_overrides_the_environment() {
    let api = MockApi::completed_quote().await;
    let mut command = async_command(&api);
    command.env("DSOLVER_HISTORY_URL", "http://127.0.0.1:1");
    command.args(["--url", &api.url, "quotes", "--request"]);
    command.arg(fixture("quote_request.json"));
    let output = run_command(command, None).await;

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(api.records().len(), 2);
}

#[tokio::test]
async fn stdin_pretty_output_is_atomic_and_no_overwrite_by_default() {
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("quote.json");
    let request = std::fs::read(fixture("quote_request.json")).unwrap();

    let first = MockApi::completed_quote().await;
    let output = run_cli_args(
        &first,
        [
            "--pretty",
            "--output",
            destination.to_str().unwrap(),
            "quotes",
            "--request",
            "-",
        ],
        Some(request.clone()),
    )
    .await;
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stdout.is_empty());
    let original = std::fs::read(&destination).unwrap();
    assert!(original.starts_with(b"{\n"));

    let second = MockApi::start(Vec::new()).await;
    let output = run_cli_args(
        &second,
        [
            "--output",
            destination.to_str().unwrap(),
            "quotes",
            "--request",
            "-",
        ],
        Some(request.clone()),
    )
    .await;
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(std::fs::read(&destination).unwrap(), original);
    assert!(second.records().is_empty());

    let third = MockApi::completed_quote().await;
    let output = run_cli_args(
        &third,
        [
            "--output",
            destination.to_str().unwrap(),
            "--force",
            "quotes",
            "--request",
            "-",
        ],
        Some(request),
    )
    .await;
    assert_eq!(output.status.code(), Some(0));
    assert!(!std::fs::read(&destination).unwrap().starts_with(b"{\n"));
}

#[tokio::test]
async fn relative_output_path_publishes_in_the_current_directory() {
    let directory = tempfile::tempdir().unwrap();
    let api = MockApi::completed_quote().await;
    let mut command = async_command(&api);
    command.current_dir(directory.path()).args([
        "--output",
        "quote.json",
        "quotes",
        "--request",
        "-",
    ]);
    let request = std::fs::read(fixture("quote_request.json")).unwrap();
    let output = run_command(command, Some(request)).await;

    assert_eq!(output.status.code(), Some(0));
    let published: Value =
        serde_json::from_slice(&std::fs::read(directory.path().join("quote.json")).unwrap())
            .unwrap();
    assert_eq!(published["requestId"], QUOTE_REQUEST_ID);
}

#[tokio::test]
async fn uncertain_submit_retry_reuses_the_exact_request_body() {
    let api = MockApi::start(vec![
        Spec::json(
            StatusCode::SERVICE_UNAVAILABLE,
            api_error("service_unavailable"),
        )
        .header("retry-after", "0"),
        Spec::json(
            StatusCode::ACCEPTED,
            submission(JOB_ONE, QUOTE_REQUEST_ID, "historicalQuote"),
        ),
        Spec::json(StatusCode::OK, completed_quote(JOB_ONE)),
    ])
    .await;
    let output = run_cli(
        &api,
        ["quotes", "--request"],
        Some(fixture("quote_request.json")),
        None,
    )
    .await;

    assert_eq!(output.status.code(), Some(0));
    let records = api.records();
    assert_eq!(records[0].body, records[1].body);
    assert!(String::from_utf8_lossy(&output.stderr).contains("retrying"));
}

#[tokio::test]
async fn response_body_disconnect_retries_the_same_submission_and_terminal_poll() {
    let api = MockApi::start(vec![
        Spec::json(
            StatusCode::ACCEPTED,
            submission(JOB_ONE, QUOTE_REQUEST_ID, "historicalQuote"),
        )
        .disconnect_body(),
        Spec::json(
            StatusCode::ACCEPTED,
            submission(JOB_ONE, QUOTE_REQUEST_ID, "historicalQuote"),
        ),
        Spec::json(StatusCode::OK, completed_quote(JOB_ONE)).disconnect_body(),
        Spec::json(StatusCode::OK, completed_quote(JOB_ONE)),
    ])
    .await;
    let output = run_cli(
        &api,
        ["quotes", "--request"],
        Some(fixture("quote_request.json")),
        None,
    )
    .await;

    assert_eq!(output.status.code(), Some(0));
    let records = api.records();
    assert_eq!(records.len(), 4);
    assert_eq!(records[0].body, records[1].body);
    assert_eq!(records[2].path, records[3].path);
    assert!(String::from_utf8_lossy(&output.stderr).contains("uncertain"));
}

#[tokio::test]
async fn fallback_retry_delay_stays_inside_the_jitter_bounds() {
    let api = MockApi::start(vec![
        Spec::json(
            StatusCode::SERVICE_UNAVAILABLE,
            api_error("service_unavailable"),
        ),
        Spec::json(
            StatusCode::ACCEPTED,
            submission(JOB_ONE, QUOTE_REQUEST_ID, "historicalQuote"),
        ),
        Spec::json(StatusCode::OK, completed_quote(JOB_ONE)),
    ])
    .await;
    let started = tokio::time::Instant::now();
    let output = run_cli(
        &api,
        ["quotes", "--request"],
        Some(fixture("quote_request.json")),
        None,
    )
    .await;
    let elapsed = started.elapsed();

    assert_eq!(output.status.code(), Some(0));
    assert!(elapsed >= Duration::from_millis(180), "elapsed {elapsed:?}");
    assert!(elapsed < Duration::from_secs(2), "elapsed {elapsed:?}");
}

#[tokio::test]
async fn authentication_and_admission_errors_exit_two() {
    for (status, code) in [
        (StatusCode::UNAUTHORIZED, "unauthorized"),
        (StatusCode::CONFLICT, "request_id_conflict"),
    ] {
        let api = MockApi::start(vec![Spec::json(status, api_error(code))]).await;
        let output = run_cli(
            &api,
            ["quotes", "--request"],
            Some(fixture("quote_request.json")),
            None,
        )
        .await;

        assert_eq!(output.status.code(), Some(2));
        assert_eq!(api.records().len(), 1);
    }
}

#[tokio::test]
async fn job_not_found_recomputes_once_with_same_id_and_remaining_deadline() {
    let api = MockApi::start(vec![
        Spec::json(
            StatusCode::ACCEPTED,
            submission(JOB_ONE, QUOTE_REQUEST_ID, "historicalQuote"),
        ),
        Spec::json(StatusCode::NOT_FOUND, api_error("job_not_found")),
        Spec::json(
            StatusCode::ACCEPTED,
            submission(JOB_TWO, QUOTE_REQUEST_ID, "historicalQuote"),
        ),
        Spec::json(StatusCode::OK, completed_quote(JOB_TWO)),
    ])
    .await;
    let output = run_cli(
        &api,
        ["quotes", "--request"],
        Some(fixture("quote_request.json")),
        None,
    )
    .await;

    assert_eq!(output.status.code(), Some(0));
    let submissions = api
        .records()
        .into_iter()
        .filter(|record| record.path == "/jobs/quote")
        .collect::<Vec<_>>();
    assert_eq!(submissions.len(), 2);
    assert_eq!(
        submissions[0].body["requestId"],
        submissions[1].body["requestId"]
    );
    assert!(
        submissions[1].body["timeoutMs"].as_u64().unwrap()
            <= submissions[0].body["timeoutMs"].as_u64().unwrap()
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("recomputing once"));
}

#[tokio::test]
async fn terminal_failed_job_is_not_resubmitted() {
    let api = MockApi::start(vec![
        Spec::json(
            StatusCode::ACCEPTED,
            submission(JOB_ONE, QUOTE_REQUEST_ID, "historicalQuote"),
        ),
        Spec::json(StatusCode::OK, failed_quote(JOB_ONE)),
    ])
    .await;
    let output = run_cli(
        &api,
        ["quotes", "--request"],
        Some(fixture("quote_request.json")),
        None,
    )
    .await;

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        api.records()
            .iter()
            .filter(|record| record.path == "/jobs/quote")
            .count(),
        1
    );
}

#[tokio::test]
async fn verify_prints_human_stderr_and_uses_report_exit_status() {
    for (status, expected_exit) in [("pass", 0), ("mismatch", 1)] {
        let api = MockApi::start(vec![
            Spec::json(
                StatusCode::ACCEPTED,
                submission(
                    JOB_ONE,
                    VERIFY_REQUEST_ID,
                    "historicalStateConsistencyCheck",
                ),
            ),
            Spec::json(StatusCode::OK, completed_verify(JOB_ONE, status)),
        ])
        .await;
        let output = run_cli(
            &api,
            ["verify", "--request"],
            Some(fixture("verify_request.json")),
            None,
        )
        .await;

        assert_eq!(output.status.code(), Some(expected_exit));
        let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(stdout["pairs"][0]["status"], status);
        assert!(String::from_utf8_lossy(&output.stderr).contains("verification:"));
    }
}

#[tokio::test]
async fn coverage_status_and_job_commands_use_only_http_contracts() {
    let coverage_api = MockApi::start(vec![Spec::json(
        StatusCode::OK,
        json!({
            "apiRevision": 1,
            "chainId": 8453,
            "backends": ["native"],
            "visibleThroughBlock": 120,
            "continuousIntervals": [{"start": 100, "endInclusive": 120}],
            "knownGaps": []
        }),
    )])
    .await;
    let output = run_cli_args(
        &coverage_api,
        ["coverage", "--chain-id", "8453", "--backend", "native"],
        None,
    )
    .await;
    assert_eq!(output.status.code(), Some(0));

    let status_api = MockApi::start(vec![Spec::json(StatusCode::OK, status_response())]).await;
    let output = run_cli_args(&status_api, ["service", "status"], None).await;
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(status_api.records()[0].revision.as_deref(), Some("1"));

    let get_api = MockApi::start(vec![Spec::json(StatusCode::OK, completed_quote(JOB_ONE))]).await;
    let output = run_cli_args(&get_api, ["job", "get", JOB_ONE], None).await;
    assert_eq!(output.status.code(), Some(0));
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["requestId"], QUOTE_REQUEST_ID);

    let wait_api = MockApi::start(vec![
        Spec::json(StatusCode::OK, running_job(JOB_ONE)).header("retry-after", "0"),
        Spec::json(StatusCode::OK, completed_quote(JOB_ONE)),
    ])
    .await;
    let output = run_cli_args(&wait_api, ["job", "wait", JOB_ONE, "--job-envelope"], None).await;
    assert_eq!(output.status.code(), Some(0));
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["jobId"], JOB_ONE);

    let cancel_api = MockApi::start(vec![Spec::json(StatusCode::OK, cancelled_job(JOB_ONE))]).await;
    let output = run_cli_args(&cancel_api, ["job", "cancel", JOB_ONE], None).await;
    assert_eq!(output.status.code(), Some(0));
}

#[tokio::test]
async fn ctrl_c_requests_cancellation_and_exits_130() {
    let api = MockApi::start(vec![
        Spec::json(
            StatusCode::ACCEPTED,
            submission(JOB_ONE, QUOTE_REQUEST_ID, "historicalQuote"),
        ),
        Spec::json(StatusCode::OK, running_job(JOB_ONE)).header("retry-after", "1"),
        Spec::json(StatusCode::OK, cancelled_job(JOB_ONE)),
    ])
    .await;
    let mut command = async_command(&api);
    command.args(["quotes", "--request"]);
    command.arg(fixture("quote_request.json"));
    let child = command.spawn().expect("CLI process must start");
    let child_id = child.id().expect("child PID must exist");
    let child = child;

    tokio::time::timeout(Duration::from_secs(5), async {
        while api.records().len() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("CLI must begin polling");
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(i32::try_from(child_id).unwrap()),
        nix::sys::signal::Signal::SIGINT,
    )
    .expect("SIGINT must be delivered");
    let output = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output())
        .await
        .expect("CLI must exit after cancellation")
        .expect("CLI output must be readable");

    assert_eq!(
        output.status.code(),
        Some(130),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        api.records().last().unwrap().path,
        format!("/jobs/{JOB_ONE}/cancel")
    );
}

#[derive(Clone)]
struct MockState {
    specs: Arc<Mutex<VecDeque<Spec>>>,
    records: Arc<Mutex<Vec<Record>>>,
}

struct MockApi {
    url: String,
    records: Arc<Mutex<Vec<Record>>>,
    _task: tokio::task::JoinHandle<()>,
}

impl MockApi {
    async fn start(specs: Vec<Spec>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = MockState {
            specs: Arc::new(Mutex::new(specs.into())),
            records: Arc::new(Mutex::new(Vec::new())),
        };
        let records = Arc::clone(&state.records);
        let app = Router::new().fallback(handler).with_state(state);
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            url: format!("http://{address}"),
            records,
            _task: task,
        }
    }

    async fn completed_quote() -> Self {
        Self::start(vec![
            Spec::json(
                StatusCode::ACCEPTED,
                submission(JOB_ONE, QUOTE_REQUEST_ID, "historicalQuote"),
            ),
            Spec::json(StatusCode::OK, completed_quote(JOB_ONE)),
        ])
        .await
    }

    fn records(&self) -> Vec<Record> {
        self.records.lock().unwrap().clone()
    }
}

#[derive(Clone)]
struct Spec {
    status: StatusCode,
    headers: Vec<(String, String)>,
    body: Value,
    disconnect_body: bool,
}

impl Spec {
    fn json(status: StatusCode, body: Value) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body,
            disconnect_body: false,
        }
    }

    fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }

    fn disconnect_body(mut self) -> Self {
        self.disconnect_body = true;
        self
    }
}

#[derive(Clone)]
struct Record {
    path: String,
    authorization: Option<String>,
    revision: Option<String>,
    body: Value,
}

async fn handler(State(state): State<MockState>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    let headers = request.headers().clone();
    let body = to_bytes(request.into_body(), usize::MAX).await.unwrap();
    let parsed = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).unwrap()
    };
    state.records.lock().unwrap().push(Record {
        path,
        authorization: header(&headers, "authorization"),
        revision: header(&headers, "x-dsolver-history-api-revision"),
        body: parsed,
    });
    let spec = state
        .specs
        .lock()
        .unwrap()
        .pop_front()
        .expect("mock response must exist");
    let mut builder = Response::builder()
        .status(spec.status)
        .header("content-type", "application/json");
    for (name, value) in spec.headers {
        builder = builder.header(name, value);
    }
    let bytes = serde_json::to_vec(&spec.body).unwrap();
    let body = if spec.disconnect_body {
        let prefix = bytes[..bytes.len() / 2].to_vec();
        Body::from_stream(futures::stream::iter([
            Ok::<_, std::io::Error>(prefix),
            Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "mock response body disconnected",
            )),
        ]))
    } else {
        Body::from(bytes)
    };
    builder.body(body).unwrap()
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

async fn run_cli<const N: usize>(
    api: &MockApi,
    args: [&str; N],
    trailing_path: Option<PathBuf>,
    stdin: Option<Vec<u8>>,
) -> Output {
    let mut command = async_command(api);
    command.args(args);
    if let Some(path) = trailing_path {
        command.arg(path);
    }
    run_command(command, stdin).await
}

async fn run_cli_args<const N: usize>(
    api: &MockApi,
    args: [&str; N],
    stdin: Option<Vec<u8>>,
) -> Output {
    let mut command = async_command(api);
    command.args(args);
    run_command(command, stdin).await
}

fn async_command(api: &MockApi) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_dsolver-history"));
    command
        .env("DSOLVER_HISTORY_URL", &api.url)
        .env("DSOLVER_HISTORY_TOKEN", "test-token")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

async fn run_command(mut command: tokio::process::Command, stdin: Option<Vec<u8>>) -> Output {
    if stdin.is_some() {
        command.stdin(Stdio::piped());
    }
    let mut child = command.spawn().expect("CLI process must start");
    if let Some(stdin) = stdin {
        child.stdin.take().unwrap().write_all(&stdin).await.unwrap();
    }
    child.wait_with_output().await.expect("CLI must finish")
}

fn submission(job_id: &str, request_id: &str, job_type: &str) -> Value {
    json!({
        "jobId": job_id,
        "requestId": request_id,
        "jobType": job_type,
        "state": "queued",
        "submittedAt": "2026-08-13T15:00:00Z",
        "deadlineAt": "2026-08-13T15:05:00Z",
        "queuePosition": 1,
        "cancellationRequested": false,
        "progress": progress(job_type, 0),
        "reused": false
    })
}

fn running_job(job_id: &str) -> Value {
    json!({
        "jobId": job_id,
        "requestId": QUOTE_REQUEST_ID,
        "jobType": "historicalQuote",
        "state": "running",
        "submittedAt": "2026-08-13T15:00:00Z",
        "deadlineAt": "2026-08-13T15:05:00Z",
        "startedAt": "2026-08-13T15:00:01Z",
        "cancellationRequested": false,
        "progress": progress("historicalQuote", 50)
    })
}

fn completed_quote(job_id: &str) -> Value {
    terminal_envelope(
        job_id,
        QUOTE_REQUEST_ID,
        "historicalQuote",
        "completed",
        Some(quote_result()),
        None,
    )
}

fn failed_quote(job_id: &str) -> Value {
    terminal_envelope(
        job_id,
        QUOTE_REQUEST_ID,
        "historicalQuote",
        "failed",
        None,
        Some(json!({
            "code": "history_read_failed",
            "message": "history read failed",
            "retryable": false
        })),
    )
}

fn cancelled_job(job_id: &str) -> Value {
    terminal_envelope(
        job_id,
        QUOTE_REQUEST_ID,
        "historicalQuote",
        "cancelled",
        None,
        Some(json!({
            "code": "internal_error",
            "message": "job was cancelled",
            "retryable": false
        })),
    )
}

fn completed_verify(job_id: &str, status: &str) -> Value {
    terminal_envelope(
        job_id,
        VERIFY_REQUEST_ID,
        "historicalStateConsistencyCheck",
        "completed",
        Some(verify_report(status)),
        None,
    )
}

fn terminal_envelope(
    job_id: &str,
    request_id: &str,
    job_type: &str,
    state: &str,
    result: Option<Value>,
    failure: Option<Value>,
) -> Value {
    let mut value = json!({
        "jobId": job_id,
        "requestId": request_id,
        "jobType": job_type,
        "state": state,
        "submittedAt": "2026-08-13T15:00:00Z",
        "deadlineAt": "2026-08-13T15:05:00Z",
        "startedAt": "2026-08-13T15:00:01Z",
        "finishedAt": "2026-08-13T15:00:02Z",
        "cancellationRequested": false,
        "progress": progress(job_type, 100)
    });
    if let Some(result) = result {
        value["result"] = result;
    }
    if let Some(failure) = failure {
        value["failure"] = failure;
    }
    value
}

fn progress(job_type: &str, percent: u8) -> Value {
    if job_type == "historicalQuote" {
        json!({
            "completedComparisons": u64::from(percent > 0),
            "totalComparisons": 1,
            "percentComplete": percent
        })
    } else {
        json!({
            "completedCheckpointPairs": u64::from(percent > 0),
            "totalCheckpointPairs": 1,
            "percentComplete": percent
        })
    }
}

fn quote_result() -> Value {
    let request: Value = serde_json::from_str(include_str!("fixtures/quote_request.json")).unwrap();
    json!({
        "requestId": QUOTE_REQUEST_ID,
        "chainId": 8453,
        "pool": request["pool"].clone(),
        "visibleThroughBlock": 34100001,
        "gaps": [],
        "results": [{
            "startBlock": 34100000,
            "amountIn": "1000000",
            "startOutcome": {"outcome": "quote_failed", "reason": "component_not_found"},
            "targets": [{
                "lag": 1,
                "targetBlock": 34100001,
                "outcome": {"outcome": "unavailable", "reason": "gap", "gapRefs": []}
            }]
        }]
    })
}

fn verify_report(status: &str) -> Value {
    let is_pass = status == "pass";
    json!({
        "requestId": VERIFY_REQUEST_ID,
        "chainId": 8453,
        "backends": ["native"],
        "selection": {
            "type": "checkpointPairs",
            "pairs": [{
                "earlierPosition": {"generation": 12, "messageSeq": 900},
                "targetPosition": {"generation": 12, "messageSeq": 1800}
            }]
        },
        "selectionStatus": "compared",
        "storage": {
            "rangePlanValid": true,
            "checkpointObjectsValid": true,
            "tokenObjectsValid": true,
            "deltaOrderValid": true,
            "replayPlansVerified": 1,
            "checkpointObjectsVerified": 2,
            "tokenObjectsVerified": 2,
            "deltasVerified": 1
        },
        "summary": {
            "selectedCheckpointPairs": 1,
            "comparedCheckpointPairs": 1,
            "pass": u64::from(is_pass),
            "mismatch": u64::from(!is_pass),
            "gap": 0,
            "notComparable": 0
        },
        "pairs": [{
            "earlierPosition": {"generation": 12, "messageSeq": 900},
            "targetPosition": {"generation": 12, "messageSeq": 1800},
            "status": status,
            "directState": {"sha256": "sha256:a", "componentCount": 1, "backendCounts": {"native": 1}},
            "replayedState": {"sha256": if is_pass {"sha256:a"} else {"sha256:b"}, "componentCount": 1, "backendCounts": {"native": 1}},
            "affectedBackends": if is_pass {json!([])} else {json!(["native"])},
            "affectedComponentIds": [],
            "quoteComparisons": [],
            "totalDifferenceCount": u64::from(!is_pass),
            "differences": []
        }]
    })
}

fn api_error(code: &str) -> Value {
    json!({
        "code": code,
        "message": "mock error",
        "retryable": matches!(code, "service_unavailable" | "queue_full")
    })
}

fn status_response() -> Value {
    json!({
        "serviceRevision": "test",
        "apiRevision": 1,
        "supportedChainId": 8453,
        "enabledBackends": ["native"],
        "visibleThroughBlock": 120,
        "database": {"state": "healthy"},
        "objectStore": {"state": "degraded", "message": "checked during jobs"},
        "jobs": {"queued": 0, "running": 0, "retainedTerminal": 0},
        "reservedDecodedBytes": 0,
        "limits": {
            "maxCombinationCount": 20,
            "maxBlockSpan": 100,
            "defaultTimeoutMs": 5000,
            "maxTimeoutMs": 300000,
            "maxRunningJobs": 4,
            "maxWaitingJobs": 16,
            "decodedByteBudget": 1000000,
            "maxTerminalJobs": 20,
            "terminalRetentionSeconds": 3600,
            "maxConsistencyPairs": 10,
            "maxQuoteSelectors": 10,
            "maxAmountsPerQuote": 10,
            "maxStructuredDifferences": 100
        },
        "draining": false
    })
}
