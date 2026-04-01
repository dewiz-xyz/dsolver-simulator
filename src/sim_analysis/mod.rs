mod presets;
mod report;

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use num_bigint::BigUint;
use reqwest::{Client, StatusCode, Url};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::task::JoinSet;

use crate::models::messages::{
    AmountOutRequest, AmountOutResponse, EncodeErrorResponse, HopDraft, InteractionKind, PoolRef,
    PoolSwapDraft, QuoteResult, RouteEncodeRequest, RouteEncodeResponse, SegmentDraft, SwapKind,
};

use self::presets::{
    balanced_profile, chain_label, resolve_token, BalancedProfilePreset, SimulateScenarioPreset,
};
use self::report::{
    build_findings, relative_path, render_summary, AnalysisReport, BaselineComparison,
    LatencySummary, LogSummary, ReadinessReport, ReadinessSnapshot, RunMetadata, ScenarioDiff,
    ScenarioReport,
};

const DEFAULT_BASE_URL: &str = "http://localhost:3000";
const DEFAULT_PROFILE: &str = "balanced";
const DEFAULT_TIMEOUT_SECS: u64 = 20;
const DEFAULT_READY_TIMEOUT_SECS: u64 = 300;
const DEFAULT_VM_READY_TIMEOUT_SECS: u64 = 600;
const DEFAULT_SLIPPAGE_BPS: u32 = 25;
const SIMULATION_REPORT_SCHEMA_VERSION: u64 = 2;
const SAMPLE_LIMIT: usize = 4;
const LOG_SCAN_LINE_LIMIT: usize = 500;
const LOG_EXCERPT_LIMIT: usize = 40;

pub async fn run() -> Result<()> {
    let args = CliArgs::parse()?;
    let started_at_epoch_s = epoch_now_s()?;

    let repo = canonicalize_path(&args.repo)?;
    let report_dir = resolve_report_dir(&repo, &args)?;
    let evidence_dir = report_dir.join("evidence");
    fs::create_dir_all(&evidence_dir)
        .with_context(|| format!("failed to create {}", evidence_dir.display()))?;

    let client = Client::builder()
        .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
        .build()
        .context("failed to build HTTP client")?;
    let scripts = ScriptPaths::new(&repo);
    let lifecycle = ensure_server_ready(&client, &scripts, &repo, &args).await?;
    let log_boundary = capture_log_boundary(&repo);

    let analysis_result = analyze_run(
        &client,
        &repo,
        &report_dir,
        &evidence_dir,
        &args,
        &lifecycle,
        &log_boundary,
        started_at_epoch_s,
    )
    .await;

    let stop_result = if args.stop && lifecycle.started_server {
        stop_server(&scripts, &repo)
    } else {
        Ok(())
    };

    match (analysis_result, stop_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(err), Ok(())) | (Ok(()), Err(err)) => Err(err),
        (Err(err), Err(stop_err)) => Err(anyhow!("{err}; failed to stop server: {stop_err}")),
    }
}

struct CliArgs {
    repo: PathBuf,
    chain_id: u64,
    profile: String,
    base_url: String,
    report_dir: Option<PathBuf>,
    baseline: BaselineMode,
    stop: bool,
}

enum BaselineMode {
    Latest,
    None,
    Path(PathBuf),
}

struct ScriptPaths {
    start_server: PathBuf,
    stop_server: PathBuf,
    wait_ready: PathBuf,
}

struct LifecycleState {
    started_server: bool,
    initial_readiness: ReadinessSnapshot,
}

struct StartupBindConfig {
    host: String,
    port: u16,
}

struct LogBoundary {
    byte_offset: u64,
}

struct LoadProbePlan<'a> {
    kind: &'a str,
    label: &'a str,
    scenarios: &'a [SimulateScenarioPreset],
    request_count: usize,
    concurrency: usize,
}

#[derive(Clone)]
struct RequestObservation {
    status_label: Option<String>,
    result_quality: Option<String>,
    elapsed_ms: Option<f64>,
    classification: ObservationClass,
    protocols: Vec<String>,
    note: Option<String>,
    request: Value,
    response: Option<Value>,
    http_status: Option<u16>,
    error: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ObservationClass {
    Healthy,
    Degraded,
    Error,
}

#[derive(Clone)]
struct HttpJsonResponse {
    status_code: StatusCode,
    elapsed_ms: f64,
    body_text: String,
    json: Option<Value>,
}

#[derive(Default)]
struct BaselineLoad {
    baseline: Option<(PathBuf, AnalysisReport)>,
    warnings: Vec<String>,
}

struct EncodePrepOutcome {
    report: ScenarioReport,
    selected_pool: Option<SelectedPool>,
}

#[derive(Clone)]
struct SelectedPool {
    quote: AmountOutResponse,
    protocol: String,
}

impl CliArgs {
    fn parse() -> Result<Self> {
        let mut repo = PathBuf::from(".");
        let mut chain_id = env::var("CHAIN_ID")
            .ok()
            .map(|value| value.parse::<u64>())
            .transpose()
            .context("CHAIN_ID must be a valid u64 when set")?;
        let mut profile = DEFAULT_PROFILE.to_string();
        let mut base_url = DEFAULT_BASE_URL.to_string();
        let mut report_dir = None;
        let mut baseline = BaselineMode::Latest;
        let mut stop = false;

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--repo" => repo = PathBuf::from(next_arg(&mut args, "--repo")?),
                "--chain-id" => {
                    chain_id = Some(
                        next_arg(&mut args, "--chain-id")?
                            .parse()
                            .context("--chain-id must be a valid u64")?,
                    );
                }
                "--profile" => profile = next_arg(&mut args, "--profile")?,
                "--base-url" => base_url = next_arg(&mut args, "--base-url")?,
                "--out" => report_dir = Some(PathBuf::from(next_arg(&mut args, "--out")?)),
                "--baseline" => {
                    baseline = match next_arg(&mut args, "--baseline")?.as_str() {
                        "latest" => BaselineMode::Latest,
                        "none" => BaselineMode::None,
                        value => BaselineMode::Path(PathBuf::from(value)),
                    };
                }
                "--stop" => stop = true,
                "-h" | "--help" => {
                    print_help();
                    std::process::exit(0);
                }
                other => bail!("unknown option: {other}"),
            }
        }

        let Some(chain_id) = chain_id else {
            bail!("missing chain id: pass --chain-id or set CHAIN_ID in the environment");
        };

        if chain_id != 1 && chain_id != 8453 {
            bail!("unsupported chain id {chain_id}; supported values are 1 and 8453");
        }

        if profile != DEFAULT_PROFILE {
            bail!("unsupported profile {profile}; supported profile is {DEFAULT_PROFILE}");
        }

        Ok(Self {
            repo,
            chain_id,
            profile,
            base_url: base_url.trim_end_matches('/').to_string(),
            report_dir,
            baseline,
            stop,
        })
    }
}

impl ScriptPaths {
    fn new(repo: &Path) -> Self {
        let scripts_dir = repo.join("scripts");
        Self {
            start_server: scripts_dir.join("start_server.sh"),
            stop_server: scripts_dir.join("stop_server.sh"),
            wait_ready: scripts_dir.join("wait_ready.sh"),
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "Run orchestration needs explicit inputs for lifecycle, logs, and artifact roots."
)]
async fn analyze_run(
    client: &Client,
    repo: &Path,
    report_dir: &Path,
    evidence_dir: &Path,
    args: &CliArgs,
    lifecycle: &LifecycleState,
    log_boundary: &LogBoundary,
    started_at_epoch_s: u64,
) -> Result<()> {
    write_json(
        &evidence_dir.join("readiness-initial.json"),
        &lifecycle.initial_readiness,
    )?;

    let profile = balanced_profile(args.chain_id, lifecycle.initial_readiness.vm_enabled)?;
    let mut scenarios = Vec::new();

    for scenario in &profile.simulate_scenarios {
        scenarios.push(
            run_simulate_scenario(client, repo, evidence_dir, args, scenario)
                .await
                .with_context(|| format!("simulate scenario {} failed", scenario.label))?,
        );
    }

    scenarios.extend(run_encode_scenarios(client, repo, evidence_dir, args, &profile).await?);
    scenarios.push(
        run_load_probe(
            client,
            repo,
            evidence_dir,
            args,
            LoadProbePlan {
                kind: "latency",
                label: "latency sweep",
                scenarios: &profile.latency_scenarios,
                request_count: profile.latency_requests,
                concurrency: profile.latency_concurrency,
            },
        )
        .await?,
    );
    scenarios.push(
        run_load_probe(
            client,
            repo,
            evidence_dir,
            args,
            LoadProbePlan {
                kind: "stress",
                label: "light stress sweep",
                scenarios: &profile.stress_scenarios,
                request_count: profile.stress_requests,
                concurrency: profile.stress_concurrency,
            },
        )
        .await?,
    );

    let final_state = fetch_readiness_snapshot(client, &status_url(&args.base_url))
        .await
        .ok();
    if let Some(snapshot) = &final_state {
        write_json(&evidence_dir.join("readiness-final.json"), snapshot)?;
    }

    let baseline_load = maybe_load_baseline(report_dir, &args.baseline)?;
    let baseline_comparison =
        baseline_load
            .baseline
            .as_ref()
            .and_then(|(baseline_dir, baseline_report)| {
                compare_with_baseline(report_dir, baseline_dir, baseline_report, &scenarios)
            });

    let logs = collect_logs(repo, report_dir, log_boundary)?;
    let finished_at_epoch_s = epoch_now_s()?;
    let mut report = AnalysisReport {
        schema_version: SIMULATION_REPORT_SCHEMA_VERSION,
        run: RunMetadata {
            started_at_epoch_s,
            finished_at_epoch_s,
            chain_id: args.chain_id,
            chain_label: chain_label(args.chain_id).to_string(),
            profile: args.profile.clone(),
            repo: repo.display().to_string(),
            base_url: args.base_url.clone(),
            report_dir: report_dir.display().to_string(),
            started_server: lifecycle.started_server,
            stop_requested: args.stop,
        },
        readiness: ReadinessReport {
            initial: lifecycle.initial_readiness.clone(),
            final_state,
        },
        scenarios,
        logs,
        warnings: baseline_load.warnings,
        findings: Vec::new(),
        baseline: baseline_comparison,
    };
    report.findings = build_findings(&report);

    write_json(&report_dir.join("report.json"), &report)?;
    fs::write(report_dir.join("summary.md"), render_summary(&report)).with_context(|| {
        format!(
            "failed to write {}",
            report_dir.join("summary.md").display()
        )
    })?;

    Ok(())
}

async fn ensure_server_ready(
    client: &Client,
    scripts: &ScriptPaths,
    repo: &Path,
    args: &CliArgs,
) -> Result<LifecycleState> {
    let status_url = status_url(&args.base_url);
    let mut started_server = false;

    match fetch_readiness_snapshot(client, &status_url).await {
        Ok(snapshot) => {
            if snapshot.chain_id != args.chain_id {
                bail!(
                    "server already responding at {} but reports chain_id={} (expected {})",
                    status_url,
                    snapshot.chain_id,
                    args.chain_id
                );
            }
        }
        Err(_) => {
            start_server(scripts, repo, args.chain_id, &args.base_url)?;
            started_server = true;
        }
    }

    let readiness_result = async {
        wait_for_readiness(scripts, &status_url, args.chain_id, false)?;
        let mut snapshot = fetch_readiness_snapshot(client, &status_url)
            .await
            .context("failed to fetch readiness after wait_ready")?;

        if snapshot.vm_enabled && snapshot.vm_status.as_deref() != Some("ready") {
            wait_for_readiness(scripts, &status_url, args.chain_id, true)?;
            snapshot = fetch_readiness_snapshot(client, &status_url)
                .await
                .context("failed to fetch readiness after VM wait")?;
        }

        Ok(snapshot)
    }
    .await;

    let snapshot = match readiness_result {
        Ok(snapshot) => snapshot,
        Err(err) if started_server && args.stop => {
            return match stop_server(scripts, repo) {
                Ok(()) => Err(err),
                Err(stop_err) => Err(anyhow!(
                    "{err}; failed to stop server after readiness failure: {stop_err}"
                )),
            };
        }
        Err(err) => return Err(err),
    };

    Ok(LifecycleState {
        started_server,
        initial_readiness: snapshot,
    })
}

fn startup_bind_config(base_url: &str) -> Result<StartupBindConfig> {
    let url = Url::parse(base_url)
        .with_context(|| format!("failed to parse --base-url {base_url} for auto-start"))?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("--base-url must include a host for auto-start: {base_url}"))?;
    let port = url.port_or_known_default().ok_or_else(|| {
        anyhow!("--base-url must include a port or use http/https for auto-start: {base_url}")
    })?;

    Ok(StartupBindConfig {
        host: normalize_startup_host(host)?,
        port,
    })
}

fn normalize_startup_host(host: &str) -> Result<String> {
    if host.eq_ignore_ascii_case("localhost") {
        return Ok("127.0.0.1".to_string());
    }

    host.parse::<IpAddr>()
        .with_context(|| {
            format!("--base-url host must be an IP address or localhost for auto-start: {host}")
        })
        .map(|_| host.to_string())
}

async fn run_simulate_scenario(
    client: &Client,
    repo: &Path,
    evidence_dir: &Path,
    args: &CliArgs,
    scenario: &SimulateScenarioPreset,
) -> Result<ScenarioReport> {
    let request = simulate_request(
        args.chain_id,
        scenario,
        format!("simulate-{}", scenario.label),
    )?;
    let observation =
        execute_simulate_request(client, &simulate_url(&args.base_url), &request).await;
    let artifact = save_observation_artifact(
        repo,
        evidence_dir,
        &format!("simulate-{}", scenario.label),
        &observation,
    )?;
    let mut report = build_scenario_report(
        "simulate",
        scenario.label,
        "/simulate",
        &[observation],
        &[artifact],
    );
    if !scenario.tags.is_empty() {
        report
            .notes
            .push(format!("scenario tags: {}", scenario.tags.join(", ")));
    }
    Ok(report)
}

#[expect(
    clippy::too_many_lines,
    reason = "The encode probe is easier to audit as one linear workflow."
)]
async fn run_encode_scenarios(
    client: &Client,
    repo: &Path,
    evidence_dir: &Path,
    args: &CliArgs,
    profile: &BalancedProfilePreset,
) -> Result<Vec<ScenarioReport>> {
    let preset = &profile.encode;
    let token_in = resolve_token(args.chain_id, preset.token_in_symbol)?;
    let mid_token = resolve_token(args.chain_id, preset.mid_symbol)?;
    let token_out = resolve_token(args.chain_id, preset.token_out_symbol)?;
    let simulate_endpoint = simulate_url(&args.base_url);
    let mut reports = Vec::with_capacity(3);
    let mut notes = Vec::new();
    let mut protocols = BTreeMap::new();

    let first_request = AmountOutRequest {
        request_id: format!("encode-hop-1-{}", epoch_now_s()?),
        auction_id: None,
        token_in: token_in.to_string(),
        token_out: mid_token.to_string(),
        amounts: preset
            .amounts
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
    };
    let first_hop = run_encode_prep_hop(
        client,
        repo,
        evidence_dir,
        &simulate_endpoint,
        &first_request,
        "encode-hop-1",
        "encode prep hop 1",
    )
    .await?;
    let mut prep_evidence_files = first_hop.report.evidence_files.clone();
    let Some(first_pool) = first_hop.selected_pool.as_ref() else {
        reports.push(first_hop.report);
        reports.push(build_skipped_encode_report(
            "Skipping /encode because prep hop 1 did not produce a usable pool.",
            &prep_evidence_files,
            &protocols,
        ));
        return Ok(reports);
    };
    reports.push(first_hop.report);

    *protocols
        .entry(first_pool.protocol.clone())
        .or_insert(0usize) += 1;

    let hop_amounts = first_pool
        .quote
        .amounts_out
        .iter()
        .map(|amount| apply_slippage(amount, DEFAULT_SLIPPAGE_BPS))
        .collect::<Vec<_>>();
    let second_request = AmountOutRequest {
        request_id: format!("encode-hop-2-{}", epoch_now_s()?),
        auction_id: None,
        token_in: mid_token.to_string(),
        token_out: token_out.to_string(),
        amounts: hop_amounts,
    };
    let second_hop = run_encode_prep_hop(
        client,
        repo,
        evidence_dir,
        &simulate_endpoint,
        &second_request,
        "encode-hop-2",
        "encode prep hop 2",
    )
    .await?;
    prep_evidence_files.extend(second_hop.report.evidence_files.clone());
    let Some(second_pool) = second_hop.selected_pool.as_ref() else {
        reports.push(second_hop.report);
        reports.push(build_skipped_encode_report(
            "Skipping /encode because prep hop 2 did not produce a usable pool.",
            &prep_evidence_files,
            &protocols,
        ));
        return Ok(reports);
    };
    reports.push(second_hop.report);

    *protocols
        .entry(second_pool.protocol.clone())
        .or_insert(0usize) += 1;

    let min_amount_out = apply_slippage(
        second_pool
            .quote
            .amounts_out
            .first()
            .map(String::as_str)
            .unwrap_or("0"),
        DEFAULT_SLIPPAGE_BPS,
    );
    if min_amount_out == "0" {
        notes.push(
            "Computed route minAmountOut was zero, so encode is marked degraded.".to_string(),
        );
    }

    let encode_request = build_encode_route_request(
        repo,
        args.chain_id,
        preset,
        token_in,
        mid_token,
        token_out,
        first_pool,
        second_pool,
        &min_amount_out,
    )?;
    let encode_observation = execute_encode_request(
        client,
        &encode_url(&args.base_url),
        &encode_request,
        &protocols,
        &notes,
    )
    .await?;
    let encode_artifact =
        save_observation_artifact(repo, evidence_dir, "encode-route", &encode_observation)?;

    let mut report = build_scenario_report(
        "encode",
        "encode route probe",
        "/encode",
        &[encode_observation],
        &[encode_artifact],
    );
    for (protocol, count) in protocols {
        report.protocols_seen.insert(protocol, count);
    }
    report.notes.extend(notes);
    reports.push(report);
    Ok(reports)
}

async fn run_encode_prep_hop(
    client: &Client,
    repo: &Path,
    evidence_dir: &Path,
    endpoint: &str,
    request: &AmountOutRequest,
    artifact_stem: &str,
    scenario_label: &str,
) -> Result<EncodePrepOutcome> {
    let observation = execute_simulate_request(client, endpoint, request).await;
    let artifact = save_observation_artifact(repo, evidence_dir, artifact_stem, &observation)?;
    let mut report = build_scenario_report(
        "encode-prep",
        scenario_label,
        "/simulate",
        std::slice::from_ref(&observation),
        std::slice::from_ref(&artifact),
    );
    let selected_pool = match deserialize_quote_result(&observation) {
        Ok(quote) => match select_best_pool(&quote) {
            Some(pool) => {
                report
                    .notes
                    .push(format!("selected pool candidate: {}", pool.quote.pool_name));
                Some(pool)
            }
            None => {
                report
                    .notes
                    .push("no usable pool row was available for route assembly".to_string());
                None
            }
        },
        Err(error) => {
            report.notes.push(format!(
                "unable to decode hop response for route assembly: {error:#}"
            ));
            None
        }
    };
    Ok(EncodePrepOutcome {
        report,
        selected_pool,
    })
}

fn build_skipped_encode_report(
    reason: &str,
    evidence_files: &[String],
    protocols: &BTreeMap<String, usize>,
) -> ScenarioReport {
    let mut report = build_scenario_report(
        "encode",
        "encode route probe",
        "/encode",
        &[],
        evidence_files,
    );
    report.notes.push(reason.to_string());
    report.notes.push(
        "No /encode request was sent, so encode metrics are intentionally empty.".to_string(),
    );
    report.protocols_seen = protocols.clone();
    report
}

#[expect(
    clippy::too_many_arguments,
    reason = "Explicit route parts keep the encode request assembly obvious."
)]
fn build_encode_route_request(
    repo: &Path,
    chain_id: u64,
    preset: &crate::sim_analysis::presets::EncodePreset,
    token_in: &str,
    mid_token: &str,
    token_out: &str,
    first_pool: &SelectedPool,
    second_pool: &SelectedPool,
    min_amount_out: &str,
) -> Result<RouteEncodeRequest> {
    Ok(RouteEncodeRequest {
        chain_id,
        token_in: token_in.to_string(),
        token_out: token_out.to_string(),
        amount_in: preset.amounts.first().copied().unwrap_or("0").to_string(),
        min_amount_out: min_amount_out.to_string(),
        settlement_address: resolve_env_or_default(
            repo,
            "COW_SETTLEMENT_CONTRACT",
            preset.settlement_address,
        )?,
        tycho_router_address: resolve_env_or_default(
            repo,
            "TYCHO_ROUTER_ADDRESS",
            preset.tycho_router_address,
        )?,
        swap_kind: SwapKind::MultiSwap,
        segments: vec![SegmentDraft {
            kind: SwapKind::MultiSwap,
            share_bps: 0,
            hops: vec![
                HopDraft {
                    token_in: token_in.to_string(),
                    token_out: mid_token.to_string(),
                    swaps: vec![encode_pool_swap(first_pool, token_in, mid_token)],
                },
                HopDraft {
                    token_in: mid_token.to_string(),
                    token_out: token_out.to_string(),
                    swaps: vec![encode_pool_swap(second_pool, mid_token, token_out)],
                },
            ],
        }],
        request_id: Some(format!("encode-probe-{}", epoch_now_s()?)),
    })
}

fn encode_pool_swap(pool: &SelectedPool, token_in: &str, token_out: &str) -> PoolSwapDraft {
    PoolSwapDraft {
        pool: PoolRef {
            // Prefer quote metadata for canonical ids; the analyzer only falls back to known
            // pool_name prefixes when metadata is absent on an otherwise usable row.
            protocol: pool.protocol.clone(),
            component_id: pool.quote.pool.clone(),
            pool_address: Some(pool.quote.pool_address.clone()),
        },
        token_in: token_in.to_string(),
        token_out: token_out.to_string(),
        split_bps: 0,
    }
}

async fn run_load_probe(
    client: &Client,
    repo: &Path,
    evidence_dir: &Path,
    args: &CliArgs,
    plan: LoadProbePlan<'_>,
) -> Result<ScenarioReport> {
    let mut join_set = JoinSet::new();
    let simulate_endpoint = simulate_url(&args.base_url);
    let scenario_cycle = plan.scenarios.to_vec();
    let concurrency = plan.concurrency.max(1);
    let mut observations = Vec::with_capacity(plan.request_count);

    for index in 0..plan.request_count {
        let client = client.clone();
        let scenario = scenario_cycle[index % scenario_cycle.len()].clone();
        let endpoint = simulate_endpoint.clone();
        let request = simulate_request(
            args.chain_id,
            &scenario,
            format!("{}-{}-{index}", plan.kind, scenario.label),
        )?;
        join_set.spawn(async move { execute_simulate_request(&client, &endpoint, &request).await });

        if join_set.len() >= concurrency {
            if let Some(result) = join_set.join_next().await {
                observations.push(result.context("load probe task failed")?);
            }
        }
    }

    while let Some(result) = join_set.join_next().await {
        observations.push(result.context("load probe task failed")?);
    }

    let sample_observations = select_samples(&observations);
    let sample_path = evidence_dir.join(format!("{}-samples.json", plan.kind));
    write_json(
        &sample_path,
        &sample_observations
            .iter()
            .map(observation_artifact_payload)
            .collect::<Vec<_>>(),
    )?;

    let mut report = build_scenario_report(
        plan.kind,
        plan.label,
        "/simulate",
        &observations,
        &[relative_path(repo, &sample_path)],
    );
    report.notes.push(format!(
        "Executed {} request(s) with concurrency={concurrency} across {} preset pair(s).",
        plan.request_count,
        plan.scenarios.len()
    ));
    Ok(report)
}

async fn execute_simulate_request(
    client: &Client,
    url: &str,
    request: &AmountOutRequest,
) -> RequestObservation {
    let request_value = serde_json::to_value(request).unwrap_or_else(|_| json!({}));
    match post_json(client, url, request).await {
        Ok(response) => classify_simulate_response(request_value, response),
        Err(error) => RequestObservation {
            status_label: None,
            result_quality: None,
            elapsed_ms: None,
            classification: ObservationClass::Error,
            protocols: Vec::new(),
            note: Some("transport failure".to_string()),
            request: request_value,
            response: None,
            http_status: None,
            error: Some(error.to_string()),
        },
    }
}

async fn execute_encode_request(
    client: &Client,
    url: &str,
    request: &RouteEncodeRequest,
    protocols: &BTreeMap<String, usize>,
    notes: &[String],
) -> Result<RequestObservation> {
    let request_value =
        serde_json::to_value(request).context("failed to serialize encode request")?;
    match post_json(client, url, request).await {
        Ok(response) => {
            let mut observation = classify_encode_response(request_value, response);
            observation.protocols = protocols.keys().cloned().collect();
            if !notes.is_empty() {
                observation.note = Some(notes.join(" "));
            }
            Ok(observation)
        }
        Err(error) => Ok(RequestObservation {
            status_label: None,
            result_quality: None,
            elapsed_ms: None,
            classification: ObservationClass::Error,
            protocols: protocols.keys().cloned().collect(),
            note: Some("transport failure".to_string()),
            request: request_value,
            response: None,
            http_status: None,
            error: Some(error.to_string()),
        }),
    }
}

fn classify_simulate_response(request: Value, response: HttpJsonResponse) -> RequestObservation {
    let mut protocols = Vec::new();
    let mut note_parts = Vec::new();
    let http_status = Some(response.status_code.as_u16());
    let elapsed_ms = Some(response.elapsed_ms);
    let parsed = response
        .json
        .clone()
        .and_then(|value| serde_json::from_value::<QuoteResult>(value).ok());

    if response.status_code != StatusCode::OK {
        return RequestObservation {
            status_label: Some(format!("http_{}", response.status_code.as_u16())),
            result_quality: None,
            elapsed_ms,
            classification: ObservationClass::Error,
            protocols,
            note: Some("non-200 response".to_string()),
            request,
            response: response.json,
            http_status,
            error: Some(response.body_text),
        };
    }

    let Some(quote) = parsed else {
        return RequestObservation {
            status_label: Some("invalid_json".to_string()),
            result_quality: None,
            elapsed_ms,
            classification: ObservationClass::Error,
            protocols,
            note: Some("response did not match QuoteResult".to_string()),
            request,
            response: response.json,
            http_status,
            error: Some(response.body_text),
        };
    };

    protocols.extend(protocols_from_quote(&quote));
    if !quote.meta.failures.is_empty() {
        note_parts.push(format!("meta.failures={}", quote.meta.failures.len()));
    }
    if !quote.meta.pool_results.is_empty() {
        note_parts.push(format!("pool_results={}", quote.meta.pool_results.len()));
    }
    if quote.meta.partial_kind.is_some() {
        note_parts.push(format!(
            "partial_kind={}",
            quote
                .meta
                .partial_kind
                .map(enum_to_string)
                .unwrap_or_else(|| "unknown".to_string())
        ));
    }
    if quote.data.is_empty() {
        note_parts.push("data returned no pool rows".to_string());
    }
    if quote
        .data
        .iter()
        .any(|entry| entry.amounts_out.iter().all(|amount| amount == "0"))
    {
        note_parts.push("found fully zero amount ladder in data".to_string());
    }

    let status_label = Some(enum_to_string(quote.meta.status));
    let result_quality = Some(enum_to_string(quote.meta.result_quality));
    let classification = if quote.meta.status == crate::models::messages::QuoteStatus::Ready
        && quote.meta.result_quality == crate::models::messages::QuoteResultQuality::Complete
        && quote.meta.failures.is_empty()
        && !quote.meta.vm_unavailable
        && !quote.data.is_empty()
    {
        ObservationClass::Healthy
    } else if quote.meta.status == crate::models::messages::QuoteStatus::Ready
        || quote.meta.result_quality
            == crate::models::messages::QuoteResultQuality::RequestLevelFailure
        || quote.meta.result_quality == crate::models::messages::QuoteResultQuality::NoResults
    {
        ObservationClass::Degraded
    } else {
        ObservationClass::Error
    };

    RequestObservation {
        status_label,
        result_quality,
        elapsed_ms,
        classification,
        protocols,
        note: (!note_parts.is_empty()).then(|| note_parts.join("; ")),
        request,
        response: response.json,
        http_status,
        error: None,
    }
}

fn classify_encode_response(request: Value, response: HttpJsonResponse) -> RequestObservation {
    let http_status = Some(response.status_code.as_u16());
    let elapsed_ms = Some(response.elapsed_ms);

    if response.status_code != StatusCode::OK {
        let error_summary = response
            .json
            .clone()
            .and_then(|value| serde_json::from_value::<EncodeErrorResponse>(value).ok())
            .map(|error| error.error)
            .unwrap_or_else(|| response.body_text.clone());
        return RequestObservation {
            status_label: Some(format!("http_{}", response.status_code.as_u16())),
            result_quality: None,
            elapsed_ms,
            classification: ObservationClass::Error,
            protocols: Vec::new(),
            note: Some("encode returned non-200".to_string()),
            request,
            response: response.json,
            http_status,
            error: Some(error_summary),
        };
    }

    let parsed = response
        .json
        .clone()
        .and_then(|value| serde_json::from_value::<RouteEncodeResponse>(value).ok());
    let Some(encode_response) = parsed else {
        return RequestObservation {
            status_label: Some("invalid_json".to_string()),
            result_quality: None,
            elapsed_ms,
            classification: ObservationClass::Error,
            protocols: Vec::new(),
            note: Some("encode response did not match RouteEncodeResponse".to_string()),
            request,
            response: response.json,
            http_status,
            error: Some(response.body_text),
        };
    };

    let has_router_call = encode_response
        .interactions
        .iter()
        .any(|interaction| interaction.kind == InteractionKind::Call);
    let approval_count = encode_response
        .interactions
        .iter()
        .filter(|interaction| interaction.kind == InteractionKind::Erc20Approve)
        .count();
    let mut note_parts = vec![format!(
        "interactions={} approvals={approval_count}",
        encode_response.interactions.len()
    )];
    if let Some(debug) = &encode_response.debug {
        if let Some(resimulation) = &debug.resimulation {
            if let Some(block_number) = resimulation.block_number {
                note_parts.push(format!("resimulation_block={block_number}"));
            }
        }
    }

    RequestObservation {
        status_label: Some("encoded".to_string()),
        result_quality: None,
        elapsed_ms,
        classification: if has_router_call {
            ObservationClass::Healthy
        } else {
            ObservationClass::Degraded
        },
        protocols: Vec::new(),
        note: Some(note_parts.join("; ")),
        request,
        response: response.json,
        http_status,
        error: None,
    }
}

fn build_scenario_report(
    kind: &str,
    label: &str,
    endpoint: &str,
    observations: &[RequestObservation],
    evidence_files: &[String],
) -> ScenarioReport {
    let mut status_counts = BTreeMap::new();
    let mut result_quality_counts = BTreeMap::new();
    let mut protocols_seen = BTreeMap::new();
    let mut latencies = Vec::new();
    let mut healthy_count = 0;
    let mut degraded_count = 0;
    let mut error_count = 0;
    let mut notes = Vec::new();

    for observation in observations {
        if let Some(status) = &observation.status_label {
            *status_counts.entry(status.clone()).or_insert(0usize) += 1;
        }
        if let Some(result_quality) = &observation.result_quality {
            *result_quality_counts
                .entry(result_quality.clone())
                .or_insert(0usize) += 1;
        }
        for protocol in &observation.protocols {
            *protocols_seen.entry(protocol.clone()).or_insert(0usize) += 1;
        }
        if let Some(latency) = observation.elapsed_ms {
            latencies.push(latency);
        }
        if let Some(note) = &observation.note {
            if notes.len() < SAMPLE_LIMIT {
                notes.push(note.clone());
            }
        }
        match observation.classification {
            ObservationClass::Healthy => healthy_count += 1,
            ObservationClass::Degraded => degraded_count += 1,
            ObservationClass::Error => error_count += 1,
        }
    }

    ScenarioReport {
        kind: kind.to_string(),
        label: label.to_string(),
        endpoint: endpoint.to_string(),
        request_count: observations.len(),
        healthy_count,
        degraded_count,
        error_count,
        status_counts,
        result_quality_counts,
        protocols_seen,
        latency_ms: summarize_latencies(&latencies),
        notes,
        evidence_files: evidence_files.to_vec(),
    }
}

fn summarize_latencies(latencies: &[f64]) -> LatencySummary {
    if latencies.is_empty() {
        return LatencySummary::default();
    }

    let mut sorted = latencies.to_vec();
    sorted.sort_by(f64::total_cmp);
    let count = sorted.len();
    let sum = sorted.iter().copied().sum::<f64>();
    LatencySummary {
        count,
        min: sorted.first().copied(),
        average: Some(sum / count as f64),
        p50: Some(percentile(&sorted, 0.50)),
        p90: Some(percentile(&sorted, 0.90)),
        p99: Some(percentile(&sorted, 0.99)),
        max: sorted.last().copied(),
    }
}

fn percentile(sorted_values: &[f64], percentile: f64) -> f64 {
    if sorted_values.len() == 1 {
        return sorted_values[0];
    }

    let index = (sorted_values.len() - 1) as f64 * percentile;
    let lower = index.floor() as usize;
    let upper = index.ceil() as usize;
    if lower == upper {
        return sorted_values[lower];
    }

    let weight = index - lower as f64;
    sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * weight
}

fn deserialize_quote_result(observation: &RequestObservation) -> Result<QuoteResult> {
    let Some(response) = &observation.response else {
        bail!("simulate observation did not contain a JSON response");
    };
    serde_json::from_value(response.clone()).context("failed to deserialize quote result")
}

fn select_best_pool(quote: &QuoteResult) -> Option<SelectedPool> {
    let entry = quote
        .data
        .iter()
        .filter(|entry| {
            entry
                .amounts_out
                .first()
                .is_some_and(|amount| amount != "0")
                && entry.amounts_out.iter().any(|amount| amount != "0")
        })
        .max_by(|left, right| {
            compare_amount_strings(&left.amounts_out[0], &right.amounts_out[0])
        })?;

    let protocol = quote
        .meta
        .pool_results
        .iter()
        .find(|pool| pool.pool == entry.pool)
        .map(|pool| pool.protocol.clone())
        .unwrap_or_else(|| protocol_from_pool_name(&entry.pool_name));

    Some(SelectedPool {
        quote: entry.clone(),
        protocol,
    })
}

fn compare_amount_strings(left: &str, right: &str) -> std::cmp::Ordering {
    let left_value = BigUint::from_str(left).unwrap_or_else(|_| BigUint::from(0u8));
    let right_value = BigUint::from_str(right).unwrap_or_else(|_| BigUint::from(0u8));
    left_value.cmp(&right_value)
}

fn protocols_from_quote(quote: &QuoteResult) -> Vec<String> {
    let mut protocols = BTreeSet::new();
    for entry in &quote.data {
        protocols.insert(protocol_from_pool_name(&entry.pool_name));
    }
    for pool in &quote.meta.pool_results {
        protocols.insert(pool.protocol.clone());
    }
    protocols.into_iter().collect()
}

fn canonical_protocol_from_pool_name(pool_name: &str) -> Option<&'static str> {
    let protocol = pool_name
        .split_once("::")
        .map(|(protocol, _)| protocol)
        .unwrap_or(pool_name);
    match protocol {
        "uniswap_v2" | "UniswapV2" => Some("uniswap_v2"),
        "uniswap_v3" | "UniswapV3" => Some("uniswap_v3"),
        "uniswap_v4" | "UniswapV4" => Some("uniswap_v4"),
        "aerodrome_slipstreams" | "AerodromeSlipstreams" => Some("aerodrome_slipstreams"),
        "ekubo_v2" | "EkuboV2" => Some("ekubo_v2"),
        "ekubo_v3" | "EkuboV3" => Some("ekubo_v3"),
        "sushiswap_v2" | "SushiswapV2" => Some("sushiswap_v2"),
        "pancakeswap_v2" | "PancakeswapV2" | "PancakeSwapV2" => Some("pancakeswap_v2"),
        "pancakeswap_v3" | "PancakeswapV3" | "PancakeSwapV3" => Some("pancakeswap_v3"),
        "fluid_v1" | "FluidV1" => Some("fluid_v1"),
        "rocketpool" | "RocketPool" => Some("rocketpool"),
        "erc4626" | "ERC4626" => Some("erc4626"),
        "vm:curve" | "Curve" => Some("vm:curve"),
        "vm:balancer_v2" | "BalancerV2" => Some("vm:balancer_v2"),
        "vm:maverick_v2" | "MaverickV2" => Some("vm:maverick_v2"),
        _ => None,
    }
}

fn protocol_from_pool_name(pool_name: &str) -> String {
    canonical_protocol_from_pool_name(pool_name)
        .map(str::to_string)
        .unwrap_or_else(|| {
            pool_name
                .split_once("::")
                .map(|(protocol, _)| protocol)
                .unwrap_or(pool_name)
                .trim()
                .to_ascii_lowercase()
                .replace(['-', ' '], "_")
        })
}

fn simulate_request(
    chain_id: u64,
    scenario: &SimulateScenarioPreset,
    request_id: String,
) -> Result<AmountOutRequest> {
    Ok(AmountOutRequest {
        request_id,
        auction_id: None,
        token_in: resolve_token(chain_id, scenario.token_in_symbol)?.to_string(),
        token_out: resolve_token(chain_id, scenario.token_out_symbol)?.to_string(),
        amounts: scenario
            .amounts
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
    })
}

fn apply_slippage(amount: &str, bps: u32) -> String {
    let Ok(value) = BigUint::from_str(amount) else {
        return "0".to_string();
    };
    let safe_bps = BigUint::from(10_000u32.saturating_sub(bps));
    let scaled = value * safe_bps;
    (scaled / BigUint::from(10_000u32)).to_string()
}

async fn post_json<T: Serialize>(
    client: &Client,
    url: &str,
    payload: &T,
) -> Result<HttpJsonResponse> {
    let started_at = Instant::now();
    let response = client
        .post(url)
        .json(payload)
        .send()
        .await
        .with_context(|| format!("POST {url} failed"))?;
    let status_code = response.status();
    let body_text = response
        .text()
        .await
        .with_context(|| format!("failed to read response body from {url}"))?;
    let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;
    let json = serde_json::from_str(&body_text).ok();

    Ok(HttpJsonResponse {
        status_code,
        elapsed_ms,
        body_text,
        json,
    })
}

async fn fetch_readiness_snapshot(client: &Client, url: &str) -> Result<ReadinessSnapshot> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url} failed"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("failed to read {url} response body"))?;
    let mut value: Value = serde_json::from_str(&body)
        .with_context(|| format!("failed to parse readiness JSON from {url}: {body}"))?;
    if status != StatusCode::OK {
        value["status_http"] = json!(status.as_u16());
    }
    serde_json::from_value(value).context("failed to deserialize readiness snapshot")
}

fn start_server(scripts: &ScriptPaths, repo: &Path, chain_id: u64, base_url: &str) -> Result<()> {
    let bind = startup_bind_config(base_url)?;
    let args = vec![
        "--repo".to_string(),
        repo.display().to_string(),
        "--chain-id".to_string(),
        chain_id.to_string(),
        "--env".to_string(),
        format!("HOST={}", bind.host),
        "--env".to_string(),
        format!("PORT={}", bind.port),
    ];
    run_script(&scripts.start_server, &args).map(|_| ())
}

fn stop_server(scripts: &ScriptPaths, repo: &Path) -> Result<()> {
    run_script(
        &scripts.stop_server,
        &["--repo".to_string(), repo.display().to_string()],
    )
    .map(|_| ())
}

fn wait_for_readiness(
    scripts: &ScriptPaths,
    status_url: &str,
    chain_id: u64,
    require_vm: bool,
) -> Result<()> {
    let timeout_secs = if require_vm {
        DEFAULT_VM_READY_TIMEOUT_SECS
    } else {
        DEFAULT_READY_TIMEOUT_SECS
    };
    let mut args = vec![
        "--url".to_string(),
        status_url.to_string(),
        "--timeout".to_string(),
        timeout_secs.to_string(),
        "--expect-chain-id".to_string(),
        chain_id.to_string(),
    ];
    if require_vm {
        args.push("--require-vm-ready".to_string());
    }
    run_script(&scripts.wait_ready, &args).map(|_| ())
}

fn run_script(script: &Path, args: &[String]) -> Result<String> {
    let output = Command::new(script)
        .args(args)
        .output()
        .with_context(|| format!("failed to execute {}", script.display()))?;
    if !output.status.success() {
        bail!(
            "{} failed: {}",
            script.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn resolve_report_dir(repo: &Path, args: &CliArgs) -> Result<PathBuf> {
    if let Some(report_dir) = &args.report_dir {
        return Ok(canonicalize_output_path(repo, report_dir));
    }

    Ok(repo
        .join("logs")
        .join("simulation-reports")
        .join(args.chain_id.to_string())
        .join(&args.profile)
        .join(epoch_now_s()?.to_string()))
}

fn canonicalize_output_path(repo: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        repo.join(path)
    }
}

fn canonicalize_path(path: &Path) -> Result<PathBuf> {
    path.canonicalize()
        .with_context(|| format!("failed to canonicalize {}", path.display()))
}

fn maybe_load_baseline(report_dir: &Path, baseline_mode: &BaselineMode) -> Result<BaselineLoad> {
    match baseline_mode {
        BaselineMode::None => Ok(BaselineLoad::default()),
        BaselineMode::Path(path) => Ok(BaselineLoad {
            baseline: Some(load_baseline_report(path)?),
            warnings: Vec::new(),
        }),
        BaselineMode::Latest => {
            let Some(baseline_dir) = discover_latest_baseline(report_dir) else {
                return Ok(BaselineLoad::default());
            };
            match load_baseline_report(&baseline_dir) {
                Ok(baseline) => Ok(BaselineLoad {
                    baseline: Some(baseline),
                    warnings: Vec::new(),
                }),
                Err(error) => Ok(BaselineLoad {
                    baseline: None,
                    warnings: vec![format!(
                        "Skipped baseline comparison because {} could not be loaded: {error:#}",
                        baseline_dir.join("report.json").display()
                    )],
                }),
            }
        }
    }
}

fn load_baseline_report(baseline_dir: &Path) -> Result<(PathBuf, AnalysisReport)> {
    let report_path = baseline_dir.join("report.json");
    let contents = fs::read_to_string(&report_path)
        .with_context(|| format!("failed to read {}", report_path.display()))?;
    let report = serde_json::from_str(&contents)
        .with_context(|| format!("failed to parse {}", report_path.display()))?;
    Ok((baseline_dir.to_path_buf(), report))
}

fn discover_latest_baseline(report_dir: &Path) -> Option<PathBuf> {
    let parent = report_dir.parent()?;
    let current_name = report_dir.file_name()?.to_string_lossy().to_string();
    let mut candidates = fs::read_dir(parent)
        .ok()?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_type()
                .ok()
                .is_some_and(|file_type| file_type.is_dir())
        })
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            (name != current_name).then_some((name, entry.path()))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.0.cmp(&right.0));
    candidates.pop().map(|(_, path)| path)
}

fn compare_with_baseline(
    report_root: &Path,
    baseline_dir: &Path,
    baseline_report: &AnalysisReport,
    current_scenarios: &[ScenarioReport],
) -> Option<BaselineComparison> {
    let baseline_by_label = baseline_report
        .scenarios
        .iter()
        .map(|scenario| (scenario.label.as_str(), scenario))
        .collect::<BTreeMap<_, _>>();
    let mut diffs = Vec::new();

    for current in current_scenarios {
        let Some(baseline) = baseline_by_label.get(current.label.as_str()) else {
            continue;
        };

        let current_degraded_rate = rate(current.degraded_count, current.request_count);
        let baseline_degraded_rate = rate(baseline.degraded_count, baseline.request_count);
        let current_error_rate = rate(current.error_count, current.request_count);
        let baseline_error_rate = rate(baseline.error_count, baseline.request_count);
        let p90_delta_pct = match (current.latency_ms.p90, baseline.latency_ms.p90) {
            (Some(current_p90), Some(baseline_p90)) if baseline_p90 > 0.0 => {
                Some(((current_p90 - baseline_p90) / baseline_p90) * 100.0)
            }
            _ => None,
        };

        let mut notes = Vec::new();
        if let Some(delta) = p90_delta_pct {
            if delta > 25.0 {
                notes.push(format!("p90 increased by {:.1}%", delta));
            } else if delta < -25.0 {
                notes.push(format!("p90 improved by {:.1}%", delta.abs()));
            }
        }
        if current_degraded_rate > baseline_degraded_rate {
            notes.push(format!(
                "degraded rate {} -> {}",
                fmt_rate(baseline_degraded_rate),
                fmt_rate(current_degraded_rate)
            ));
        }
        if current_error_rate > baseline_error_rate {
            notes.push(format!(
                "error rate {} -> {}",
                fmt_rate(baseline_error_rate),
                fmt_rate(current_error_rate)
            ));
        }

        if notes.is_empty() {
            continue;
        }

        diffs.push(ScenarioDiff {
            label: current.label.clone(),
            degraded_rate_delta: Some(current_degraded_rate - baseline_degraded_rate),
            error_rate_delta: Some(current_error_rate - baseline_error_rate),
            p90_delta_pct,
            notes,
        });
    }

    (!diffs.is_empty()).then(|| BaselineComparison {
        compared_report_dir: relative_path(report_root, baseline_dir),
        scenario_diffs: diffs,
    })
}

fn capture_log_boundary(repo: &Path) -> LogBoundary {
    let log_path = repo.join("logs/tycho-sim-server.log");
    let byte_offset = fs::metadata(&log_path).map_or(0, |metadata| metadata.len());
    LogBoundary { byte_offset }
}

fn collect_logs(repo: &Path, report_dir: &Path, log_boundary: &LogBoundary) -> Result<LogSummary> {
    let log_path = repo.join("logs/tycho-sim-server.log");
    if !log_path.exists() {
        return Ok(LogSummary {
            log_file: log_path.display().to_string(),
            matched_lines: 0,
            excerpt_file: None,
            highlights: Vec::new(),
        });
    }

    let contents =
        fs::read(&log_path).with_context(|| format!("failed to read {}", log_path.display()))?;
    let start = usize::try_from(log_boundary.byte_offset).unwrap_or(usize::MAX);
    let scoped_contents = if start >= contents.len() {
        String::new()
    } else {
        String::from_utf8_lossy(&contents[start..]).into_owned()
    };
    let lines = scoped_contents.lines().collect::<Vec<_>>();
    let recent_lines = lines
        .iter()
        .rev()
        .take(LOG_SCAN_LINE_LIMIT)
        .copied()
        .collect::<Vec<_>>();
    let matched_lines = recent_lines
        .iter()
        .rev()
        .filter(|line| is_interesting_log_line(line))
        .map(|line| (*line).to_string())
        .collect::<Vec<_>>();

    let excerpt_file = if matched_lines.is_empty() {
        None
    } else {
        let excerpt_path = report_dir.join("evidence/log-excerpts.txt");
        let excerpt = matched_lines
            .iter()
            .rev()
            .take(LOG_EXCERPT_LIMIT)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&excerpt_path, format!("{excerpt}\n"))
            .with_context(|| format!("failed to write {}", excerpt_path.display()))?;
        Some(relative_path(repo, &excerpt_path))
    };

    Ok(LogSummary {
        log_file: log_path.display().to_string(),
        matched_lines: matched_lines.len(),
        excerpt_file,
        highlights: matched_lines.into_iter().take(6).collect(),
    })
}

fn is_interesting_log_line(line: &str) -> bool {
    let line = line.to_ascii_lowercase();
    line.contains("\"level\":\"error\"")
        || line.contains("\"level\":\"warn\"")
        || line.contains("panic")
        || line.contains("timed out")
        || line.contains("failed")
}

fn save_observation_artifact(
    repo: &Path,
    evidence_dir: &Path,
    stem: &str,
    observation: &RequestObservation,
) -> Result<String> {
    let path = evidence_dir.join(format!("{}.json", sanitize_filename(stem)));
    write_json(&path, &observation_artifact_payload(observation))?;
    Ok(relative_path(repo, &path))
}

fn observation_artifact_payload(observation: &RequestObservation) -> Value {
    json!({
        "http_status": observation.http_status,
        "status": observation.status_label,
        "result_quality": observation.result_quality,
        "elapsed_ms": observation.elapsed_ms,
        "classification": observation_class_label(observation.classification),
        "protocols": observation.protocols,
        "note": observation.note,
        "error": observation.error,
        "request": observation.request,
        "response": observation.response,
    })
}

fn observation_class_label(classification: ObservationClass) -> &'static str {
    match classification {
        ObservationClass::Healthy => "healthy",
        ObservationClass::Degraded => "degraded",
        ObservationClass::Error => "error",
    }
}

fn select_samples(observations: &[RequestObservation]) -> Vec<RequestObservation> {
    let mut selected = Vec::new();
    for wanted in [
        ObservationClass::Error,
        ObservationClass::Degraded,
        ObservationClass::Healthy,
    ] {
        for observation in observations
            .iter()
            .filter(|item| item.classification == wanted)
        {
            if selected.len() >= SAMPLE_LIMIT {
                return selected;
            }
            selected.push(observation.clone());
        }
    }
    selected
}

fn resolve_env_or_default(repo: &Path, key: &str, default_value: &str) -> Result<String> {
    if let Ok(value) = env::var(key) {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }
    let env_path = repo.join(".env");
    if env_path.exists() {
        let contents = fs::read_to_string(&env_path)
            .with_context(|| format!("failed to read {}", env_path.display()))?;
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || !line.contains('=') {
                continue;
            }
            let mut parts = line.splitn(2, '=');
            let raw_key = parts
                .next()
                .unwrap_or_default()
                .trim()
                .trim_start_matches("export ")
                .trim();
            let raw_value = parts.next().unwrap_or_default().trim();
            if raw_key == key {
                return Ok(raw_value.trim_matches('"').trim_matches('\'').to_string());
            }
        }
    }
    Ok(default_value.to_string())
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    args.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_help() {
    println!(
        "Usage: cargo run --bin sim-analysis -- --chain-id <1|8453> [options]\n\
         \n\
         Options:\n\
           --repo <path>        Repo root (default: .)\n\
           --chain-id <id>      Runtime chain id (or use CHAIN_ID env)\n\
           --profile balanced   Analysis profile (default: balanced)\n\
           --base-url <url>     Base URL for the local service (default: http://localhost:3000)\n\
           --out <path>         Custom report output directory\n\
           --baseline <mode>    latest, none, or a report directory path (default: latest)\n\
           --stop               Stop the server after the run if the analyzer started it\n"
    );
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let serialized =
        serde_json::to_string_pretty(value).context("failed to serialize JSON artifact")?;
    fs::write(path, format!("{serialized}\n"))
        .with_context(|| format!("failed to write {}", path.display()))
}

fn status_url(base_url: &str) -> String {
    format!("{base_url}/status")
}

fn simulate_url(base_url: &str) -> String {
    format!("{base_url}/simulate")
}

fn encode_url(base_url: &str) -> String {
    format!("{base_url}/encode")
}

fn epoch_now_s() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before UNIX_EPOCH")?
        .as_secs())
}

fn sanitize_filename(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect()
}

fn enum_to_string<T: Serialize>(value: T) -> String {
    serde_json::to_string(&value)
        .unwrap_or_else(|_| "\"unknown\"".to_string())
        .trim_matches('"')
        .to_string()
}

fn rate(count: usize, total: usize) -> f64 {
    if total == 0 {
        return 0.0;
    }
    count as f64 / total as f64
}

fn fmt_rate(rate: f64) -> String {
    format!("{:.2}%", rate * 100.0)
}

#[cfg(test)]
mod tests {
    use super::{
        capture_log_boundary, collect_logs, discover_latest_baseline, ensure_server_ready,
        maybe_load_baseline, percentile, sanitize_filename, select_best_pool, startup_bind_config,
        BaselineMode, CliArgs, ScriptPaths,
    };
    use crate::models::messages::{
        AmountOutResponse, PoolOutcomeKind, PoolSimulationOutcome, QuoteMeta, QuoteResult,
        QuoteResultQuality, QuoteStatus,
    };
    use reqwest::Client;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_test_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "sim-analysis-{name}-{}-{unique}",
            std::process::id()
        ))
    }

    fn write_executable(path: &std::path::Path, contents: &str) {
        assert!(fs::write(path, contents).is_ok());
        let metadata_result = fs::metadata(path);
        assert!(metadata_result.is_ok());
        let Some(metadata) = metadata_result.ok() else {
            return;
        };
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o755);
        assert!(fs::set_permissions(path, permissions).is_ok());
    }

    #[test]
    fn percentile_interpolates_between_points() {
        let values = [10.0, 20.0, 30.0, 40.0];
        assert_eq!(percentile(&values, 0.5), 25.0);
    }

    #[test]
    fn sanitize_filename_replaces_spaces() {
        assert_eq!(sanitize_filename("hello world"), "hello-world");
    }

    #[test]
    fn startup_bind_config_normalizes_localhost_and_preserves_explicit_port() {
        let bind_result = startup_bind_config("http://localhost:4100");
        assert!(bind_result.is_ok());
        let Some(bind) = bind_result.ok() else {
            return;
        };

        assert_eq!(bind.host, "127.0.0.1");
        assert_eq!(bind.port, 4100);
    }

    #[test]
    fn startup_bind_config_uses_scheme_default_port() {
        let bind_result = startup_bind_config("https://127.0.0.1");
        assert!(bind_result.is_ok());
        let Some(bind) = bind_result.ok() else {
            return;
        };

        assert_eq!(bind.host, "127.0.0.1");
        assert_eq!(bind.port, 443);
    }

    #[tokio::test]
    async fn ensure_server_ready_stops_started_server_when_wait_ready_fails_and_stop_is_set() {
        let root = temp_test_dir("ensure-ready-stop");
        let scripts_dir = root.join("scripts");
        assert!(fs::create_dir_all(&scripts_dir).is_ok());

        let start_marker = root.join("start-called");
        let stop_marker = root.join("stop-called");
        write_executable(
            &scripts_dir.join("start_server.sh"),
            &format!("#!/bin/sh\ntouch {}\n", start_marker.display()),
        );
        write_executable(
            &scripts_dir.join("stop_server.sh"),
            &format!("#!/bin/sh\ntouch {}\n", stop_marker.display()),
        );
        write_executable(
            &scripts_dir.join("wait_ready.sh"),
            "#!/bin/sh\necho wait-ready failed >&2\nexit 1\n",
        );

        let client_result = Client::builder()
            .timeout(Duration::from_millis(200))
            .build();
        assert!(client_result.is_ok());
        let Some(client) = client_result.ok() else {
            return;
        };

        let result = ensure_server_ready(
            &client,
            &ScriptPaths::new(&root),
            &root,
            &CliArgs {
                repo: root.clone(),
                chain_id: 1,
                profile: "balanced".to_string(),
                base_url: "http://127.0.0.1:1".to_string(),
                report_dir: None,
                baseline: BaselineMode::Latest,
                stop: true,
            },
        )
        .await;

        assert!(result.is_err());
        let Some(error) = result.err() else {
            return;
        };
        let message = error.to_string();
        assert!(message.contains("wait_ready.sh failed: wait-ready failed"));
        assert!(start_marker.exists());
        assert!(stop_marker.exists());
        assert!(fs::remove_dir_all(&root).is_ok());
    }

    #[tokio::test]
    async fn ensure_server_ready_keeps_readiness_error_when_cleanup_also_fails() {
        let root = temp_test_dir("ensure-ready-stop-fail");
        let scripts_dir = root.join("scripts");
        assert!(fs::create_dir_all(&scripts_dir).is_ok());

        let start_marker = root.join("start-called");
        let stop_marker = root.join("stop-called");
        write_executable(
            &scripts_dir.join("start_server.sh"),
            &format!("#!/bin/sh\ntouch {}\n", start_marker.display()),
        );
        write_executable(
            &scripts_dir.join("stop_server.sh"),
            &format!(
                "#!/bin/sh\ntouch {}\necho stop failed >&2\nexit 1\n",
                stop_marker.display()
            ),
        );
        write_executable(
            &scripts_dir.join("wait_ready.sh"),
            "#!/bin/sh\necho wait-ready failed >&2\nexit 1\n",
        );

        let client_result = Client::builder()
            .timeout(Duration::from_millis(200))
            .build();
        assert!(client_result.is_ok());
        let Some(client) = client_result.ok() else {
            return;
        };

        let result = ensure_server_ready(
            &client,
            &ScriptPaths::new(&root),
            &root,
            &CliArgs {
                repo: root.clone(),
                chain_id: 1,
                profile: "balanced".to_string(),
                base_url: "http://127.0.0.1:1".to_string(),
                report_dir: None,
                baseline: BaselineMode::Latest,
                stop: true,
            },
        )
        .await;

        assert!(result.is_err());
        let Some(error) = result.err() else {
            return;
        };
        let message = error.to_string();
        assert!(message.contains("wait_ready.sh failed: wait-ready failed"));
        assert!(message.contains("failed to stop server after readiness failure"));
        assert!(message.contains("stop_server.sh failed: stop failed"));
        assert!(start_marker.exists());
        assert!(stop_marker.exists());
        assert!(fs::remove_dir_all(&root).is_ok());
    }

    #[test]
    fn discover_latest_baseline_ignores_current_directory() {
        let root = temp_test_dir("discover-latest");
        let current = root.join("3");
        let older = root.join("1");
        let newer = root.join("2");
        assert!(fs::create_dir_all(&older).is_ok());
        assert!(fs::create_dir_all(&newer).is_ok());
        assert!(fs::create_dir_all(&current).is_ok());

        let discovered = discover_latest_baseline(&current);
        assert_eq!(
            discovered
                .as_ref()
                .and_then(|path| path.file_name())
                .and_then(|value| value.to_str()),
            Some("2")
        );

        assert!(fs::remove_dir_all(&root).is_ok());
    }

    #[test]
    fn latest_baseline_parse_failures_are_recorded_as_warnings() {
        let root = temp_test_dir("baseline-warning");
        let current = root.join("3");
        let latest = root.join("2");
        assert!(fs::create_dir_all(&current).is_ok());
        assert!(fs::create_dir_all(&latest).is_ok());
        assert!(fs::write(latest.join("report.json"), "{not-json").is_ok());

        let baseline_result = maybe_load_baseline(&current, &BaselineMode::Latest);
        assert!(baseline_result.is_ok());
        let Some(baseline) = baseline_result.ok() else {
            return;
        };

        assert!(baseline.baseline.is_none());
        assert_eq!(baseline.warnings.len(), 1);
        assert!(baseline.warnings[0].contains("Skipped baseline comparison"));

        assert!(fs::remove_dir_all(&root).is_ok());
    }

    #[test]
    fn collect_logs_only_reports_lines_added_after_boundary() {
        let root = temp_test_dir("log-boundary");
        let log_dir = root.join("logs");
        let report_dir = root.join("reports/run-1");
        assert!(fs::create_dir_all(report_dir.join("evidence")).is_ok());
        assert!(fs::create_dir_all(&log_dir).is_ok());

        let log_path = log_dir.join("tycho-sim-server.log");
        assert!(fs::write(&log_path, "{\"level\":\"warn\",\"msg\":\"old warning\"}\n").is_ok());
        let boundary = capture_log_boundary(&root);
        assert!(fs::write(
            &log_path,
            concat!(
                "{\"level\":\"warn\",\"msg\":\"old warning\"}\n",
                "{\"level\":\"info\",\"msg\":\"new info\"}\n",
                "{\"level\":\"error\",\"msg\":\"new error\"}\n"
            ),
        )
        .is_ok());

        let summary_result = collect_logs(&root, &report_dir, &boundary);
        assert!(summary_result.is_ok());
        let Some(summary) = summary_result.ok() else {
            return;
        };
        assert_eq!(summary.matched_lines, 1);
        assert_eq!(summary.highlights.len(), 1);
        assert!(summary.highlights[0].contains("new error"));

        assert!(fs::remove_dir_all(&root).is_ok());
    }

    #[test]
    fn select_best_pool_uses_canonical_protocol_from_quote_metadata() {
        let quote = QuoteResult {
            request_id: "req-1".to_string(),
            data: vec![AmountOutResponse {
                pool: "pool-1".to_string(),
                pool_name: "UniswapV3::DAI/USDC".to_string(),
                pool_address: "0x0000000000000000000000000000000000000001".to_string(),
                amounts_out: vec!["10".to_string()],
                gas_used: vec![1],
                block_number: 1,
            }],
            meta: QuoteMeta {
                status: QuoteStatus::Ready,
                result_quality: QuoteResultQuality::Complete,
                partial_kind: None,
                block_number: 1,
                vm_block_number: None,
                rfq_block_number: None,
                matching_pools: 1,
                candidate_pools: 1,
                total_pools: None,
                auction_id: None,
                pool_results: vec![PoolSimulationOutcome {
                    pool: "pool-1".to_string(),
                    pool_name: "UniswapV3::DAI/USDC".to_string(),
                    pool_address: "0x0000000000000000000000000000000000000001".to_string(),
                    protocol: "uniswap_v3".to_string(),
                    outcome: PoolOutcomeKind::PartialOutput,
                    reported_steps: 1,
                    expected_steps: 1,
                    reason: None,
                }],
                vm_unavailable: false,
                rfq_unavailable: false,
                failures: Vec::new(),
            },
        };

        let selected = select_best_pool(&quote);

        assert_eq!(
            selected.as_ref().map(|pool| pool.protocol.as_str()),
            Some("uniswap_v3")
        );
        assert_eq!(
            selected.as_ref().map(|pool| pool.quote.pool.as_str()),
            Some("pool-1")
        );
    }

    #[test]
    fn select_best_pool_falls_back_to_known_pool_name_prefixes() {
        let quote = QuoteResult {
            request_id: "req-1".to_string(),
            data: vec![AmountOutResponse {
                pool: "pool-1".to_string(),
                pool_name: "UniswapV3::DAI/USDC".to_string(),
                pool_address: "0x0000000000000000000000000000000000000001".to_string(),
                amounts_out: vec!["10".to_string()],
                gas_used: vec![1],
                block_number: 1,
            }],
            meta: QuoteMeta {
                status: QuoteStatus::Ready,
                result_quality: QuoteResultQuality::Complete,
                partial_kind: None,
                block_number: 1,
                vm_block_number: None,
                rfq_block_number: None,
                matching_pools: 1,
                candidate_pools: 1,
                total_pools: None,
                auction_id: None,
                pool_results: Vec::new(),
                vm_unavailable: false,
                rfq_unavailable: false,
                failures: Vec::new(),
            },
        };

        let selected = select_best_pool(&quote);

        assert_eq!(
            selected.as_ref().map(|pool| pool.protocol.as_str()),
            Some("uniswap_v3")
        );
    }

    #[test]
    fn select_best_pool_preserves_vm_protocol_from_quote_metadata() {
        let quote = QuoteResult {
            request_id: "req-1".to_string(),
            data: vec![AmountOutResponse {
                pool: "pool-1".to_string(),
                pool_name: "Curve::USDC/USDT".to_string(),
                pool_address: "0x0000000000000000000000000000000000000001".to_string(),
                amounts_out: vec!["10".to_string()],
                gas_used: vec![1],
                block_number: 1,
            }],
            meta: QuoteMeta {
                status: QuoteStatus::Ready,
                result_quality: QuoteResultQuality::Complete,
                partial_kind: None,
                block_number: 1,
                vm_block_number: Some(2),
                rfq_block_number: Some(2),
                matching_pools: 1,
                candidate_pools: 1,
                total_pools: None,
                auction_id: None,
                pool_results: vec![PoolSimulationOutcome {
                    pool: "pool-1".to_string(),
                    pool_name: "Curve::USDC/USDT".to_string(),
                    pool_address: "0x0000000000000000000000000000000000000001".to_string(),
                    protocol: "vm:curve".to_string(),
                    outcome: PoolOutcomeKind::PartialOutput,
                    reported_steps: 1,
                    expected_steps: 1,
                    reason: None,
                }],
                vm_unavailable: false,
                rfq_unavailable: false,
                failures: Vec::new(),
            },
        };

        let selected = select_best_pool(&quote);

        assert_eq!(
            selected.as_ref().map(|pool| pool.protocol.as_str()),
            Some("vm:curve")
        );
    }

    #[test]
    fn select_best_pool_falls_back_to_vm_protocol_from_pool_name() {
        let quote = QuoteResult {
            request_id: "req-1".to_string(),
            data: vec![AmountOutResponse {
                pool: "pool-1".to_string(),
                pool_name: "Curve::USDC/USDT".to_string(),
                pool_address: "0x0000000000000000000000000000000000000001".to_string(),
                amounts_out: vec!["10".to_string()],
                gas_used: vec![1],
                block_number: 1,
            }],
            meta: QuoteMeta {
                status: QuoteStatus::Ready,
                result_quality: QuoteResultQuality::Complete,
                partial_kind: None,
                block_number: 1,
                vm_block_number: Some(2),
                rfq_block_number: Some(2),
                matching_pools: 1,
                candidate_pools: 1,
                total_pools: None,
                auction_id: None,
                pool_results: Vec::new(),
                vm_unavailable: false,
                rfq_unavailable: false,
                failures: Vec::new(),
            },
        };

        let selected = select_best_pool(&quote);

        assert_eq!(
            selected.as_ref().map(|pool| pool.protocol.as_str()),
            Some("vm:curve")
        );
    }

    #[test]
    fn select_best_pool_falls_back_to_normalized_pool_name_prefix() {
        let quote = QuoteResult {
            request_id: "req-1".to_string(),
            data: vec![AmountOutResponse {
                pool: "pool-1".to_string(),
                pool_name: "UnknownProtocol::DAI/USDC".to_string(),
                pool_address: "0x0000000000000000000000000000000000000001".to_string(),
                amounts_out: vec!["10".to_string()],
                gas_used: vec![1],
                block_number: 1,
            }],
            meta: QuoteMeta {
                status: QuoteStatus::Ready,
                result_quality: QuoteResultQuality::Complete,
                partial_kind: None,
                block_number: 1,
                vm_block_number: None,
                rfq_block_number: None,
                matching_pools: 1,
                candidate_pools: 1,
                total_pools: None,
                auction_id: None,
                pool_results: Vec::new(),
                vm_unavailable: false,
                rfq_unavailable: false,
                failures: Vec::new(),
            },
        };

        let selected = select_best_pool(&quote);

        assert_eq!(
            selected.as_ref().map(|pool| pool.protocol.as_str()),
            Some("unknownprotocol")
        );
    }
}
