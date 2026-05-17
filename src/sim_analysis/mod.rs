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

use alloy_primitives::{hex, Keccak256};
use anyhow::{anyhow, bail, Context, Result};
use num_bigint::BigUint;
use reqwest::{Client, StatusCode, Url};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::models::messages::{
    AmountOutRequest, AmountOutResponse, EncodeErrorResponse, HopDraft, InteractionKind, PoolRef,
    PoolSwapDraft, QuoteResult, RouteEncodeRequest, RouteEncodeResponse, SegmentDraft, SwapKind,
};

use self::presets::{
    balanced_profile, chain_label, resolve_token, BalancedProfilePreset, EncodeRouteKind,
    EncodeRoutePreset, SimulateScenarioPreset,
};
use self::report::{
    build_findings, relative_path, render_summary, AnalysisReport, BaselineComparison,
    EncodeInspectionReport, LatencySummary, LogSummary, ReadinessReport, ReadinessSnapshot,
    RunMetadata, ScenarioDiff, ScenarioReport,
};

const DEFAULT_BASE_URL: &str = "http://localhost:3000";
const DEFAULT_PROFILE: &str = "balanced";
const DEFAULT_TIMEOUT_SECS: u64 = 20;
const DEFAULT_READY_TIMEOUT_SECS: u64 = 300;
const DEFAULT_VM_READY_TIMEOUT_SECS: u64 = 600;
const DEFAULT_RFQ_READY_TIMEOUT_SECS: u64 = 600;
const DEFAULT_SLIPPAGE_BPS: u32 = 25;
const SIMULATION_REPORT_SCHEMA_VERSION: u64 = 4;
const SAMPLE_LIMIT: usize = 4;
const LOG_SCAN_LINE_LIMIT: usize = 500;
const LOG_EXCERPT_LIMIT: usize = 40;
const EXPECTED_RFQ_PROTOCOL_VISIBILITY_NOTE: &str = "expected RFQ protocol visibility, saw none";
const BEBOP_SPLIT_BPS: u32 = 5_000;
const BPS_DENOMINATOR: u32 = 10_000;
const ABI_WORD_BYTES: usize = 32;
const FUNCTION_SELECTOR_BYTES: usize = 4;
const BEBOP_PACKED_PREFIX_BYTES: usize = 95;
const BEBOP_PARTIAL_FILL_OFFSET_INDEX: usize = 41;
const BEBOP_ORIGINAL_TAKER_AMOUNT_START: usize = 42;
const BEBOP_ORIGINAL_TAKER_AMOUNT_END: usize = 74;
const SPLIT_SWAP_SIGNATURE: &str =
    "splitSwap(uint256,address,address,uint256,bool,bool,uint256,address,bool,bytes)";
const SINGLE_SWAP_SIGNATURE: &str =
    "singleSwap(uint256,address,address,uint256,bool,bool,address,bool,bytes)";
const SEQUENTIAL_SWAP_SIGNATURE: &str =
    "sequentialSwap(uint256,address,address,uint256,bool,bool,address,bool,bytes)";

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
    encode_inspections: Vec<EncodeInspectionReport>,
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
    quote: Option<QuoteResult>,
}

#[derive(Clone)]
struct SelectedPool {
    quote: AmountOutResponse,
    protocol: String,
}

#[derive(Clone, Copy)]
enum PoolSelection<'a> {
    Best,
    RequiredProtocol(&'a str),
    ExcludeProtocolPrefix(&'a str),
}

#[derive(Clone)]
struct BebopInspectionExpectation {
    token_in: String,
    token_out: String,
    expected_taker_amount: String,
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

impl<'a> PoolSelection<'a> {
    fn label(self) -> String {
        match self {
            PoolSelection::Best => "best available pool".to_string(),
            PoolSelection::RequiredProtocol(protocol) => format!("required protocol {protocol}"),
            PoolSelection::ExcludeProtocolPrefix(prefix) => {
                format!("pool excluding protocol prefix {prefix}")
            }
        }
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
#[expect(
    clippy::too_many_lines,
    reason = "Run orchestration stays linear so report assembly remains easy to audit."
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

    scenarios.extend(
        run_encode_scenarios(
            client,
            repo,
            evidence_dir,
            args,
            &profile,
            &lifecycle.initial_readiness,
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
        wait_for_readiness(scripts, &status_url, args.chain_id, false, false)?;
        let mut snapshot = fetch_readiness_snapshot(client, &status_url)
            .await
            .context("failed to fetch readiness after wait_ready")?;

        if !snapshot_native_ready(&snapshot) {
            wait_for_native_readiness(client, &status_url, args.chain_id).await?;
            snapshot = fetch_readiness_snapshot(client, &status_url)
                .await
                .context("failed to fetch readiness after native wait")?;
        }

        let require_vm = snapshot.vm_enabled && snapshot.vm_status.as_deref() != Some("ready");
        let require_rfq = snapshot.rfq_enabled && snapshot.rfq_status.as_deref() != Some("ready");

        if require_vm || require_rfq {
            let wait_label = readiness_wait_label(require_vm, require_rfq);
            wait_for_readiness(scripts, &status_url, args.chain_id, require_vm, require_rfq)?;
            snapshot = fetch_readiness_snapshot(client, &status_url)
                .await
                .with_context(|| format!("failed to fetch readiness after {wait_label}"))?;
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
    if scenario.expect_rfq_visibility
        && !report
            .protocols_seen
            .keys()
            .any(|protocol| protocol.starts_with("rfq:"))
    {
        report
            .notes
            .push(EXPECTED_RFQ_PROTOCOL_VISIBILITY_NOTE.to_string());
    }
    Ok(report)
}

async fn run_encode_scenarios(
    client: &Client,
    repo: &Path,
    evidence_dir: &Path,
    args: &CliArgs,
    profile: &BalancedProfilePreset,
    readiness: &ReadinessSnapshot,
) -> Result<Vec<ScenarioReport>> {
    let mut reports = Vec::new();
    for preset in &profile.encode_routes {
        match preset.kind {
            EncodeRouteKind::GenericMultihop => {
                reports.extend(
                    run_generic_multihop_encode_route(client, repo, evidence_dir, args, preset)
                        .await?,
                );
            }
            EncodeRouteKind::BebopPartialFill => {
                reports.extend(
                    run_bebop_partial_fill_encode_route(
                        client,
                        repo,
                        evidence_dir,
                        args,
                        preset,
                        readiness,
                    )
                    .await?,
                );
            }
        }
    }
    Ok(reports)
}

#[expect(
    clippy::too_many_lines,
    reason = "The generic encode probe is easier to audit as one linear workflow."
)]
async fn run_generic_multihop_encode_route(
    client: &Client,
    repo: &Path,
    evidence_dir: &Path,
    args: &CliArgs,
    preset: &EncodeRoutePreset,
) -> Result<Vec<ScenarioReport>> {
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
        &format!("{}-hop-1", preset.label),
        &format!("{} prep hop 1", preset.label),
        PoolSelection::Best,
    )
    .await?;
    let mut prep_evidence_files = first_hop.report.evidence_files.clone();
    let Some(first_pool) = first_hop.selected_pool.clone() else {
        reports.push(first_hop.report);
        reports.push(build_skipped_encode_report(
            preset.label,
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
        &format!("{}-hop-2", preset.label),
        &format!("{} prep hop 2", preset.label),
        PoolSelection::Best,
    )
    .await?;
    prep_evidence_files.extend(second_hop.report.evidence_files.clone());
    let Some(second_pool) = second_hop.selected_pool.clone() else {
        reports.push(second_hop.report);
        reports.push(build_skipped_encode_report(
            preset.label,
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
        &first_pool,
        &second_pool,
        &min_amount_out,
    )?;
    let encode_observation = execute_encode_request(
        client,
        &encode_url(&args.base_url),
        &encode_request,
        preset.label,
        &protocols,
        &notes,
        None,
    )
    .await?;
    let encode_artifact =
        save_observation_artifact(repo, evidence_dir, preset.label, &encode_observation)?;

    let mut report = build_scenario_report(
        "encode",
        preset.label,
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

#[expect(
    clippy::too_many_lines,
    reason = "The Bebop encode diagnostic is easier to audit as one linear workflow."
)]
async fn run_bebop_partial_fill_encode_route(
    client: &Client,
    repo: &Path,
    evidence_dir: &Path,
    args: &CliArgs,
    preset: &EncodeRoutePreset,
    readiness: &ReadinessSnapshot,
) -> Result<Vec<ScenarioReport>> {
    if !rfq_required_probe_ready(readiness) {
        let mut report = build_skipped_encode_report(
            preset.label,
            "Bebop partial-fill encode probe is not required because RFQ is not enabled and ready.",
            &[],
            &BTreeMap::new(),
        );
        report.notes.push(format!(
            "rfq_enabled={} rfq_status={}",
            readiness.rfq_enabled,
            readiness.rfq_status.as_deref().unwrap_or("unknown")
        ));
        return Ok(vec![report]);
    }

    let token_in = resolve_token(args.chain_id, preset.token_in_symbol)?;
    let mid_token = resolve_token(args.chain_id, preset.mid_symbol)?;
    let token_out = resolve_token(args.chain_id, preset.token_out_symbol)?;
    let simulate_endpoint = simulate_url(&args.base_url);
    let mut reports = Vec::with_capacity(3);
    let notes = Vec::new();
    let mut protocols = BTreeMap::new();

    let first_request = AmountOutRequest {
        request_id: format!("{}-hop-1-{}", preset.label, epoch_now_s()?),
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
        &format!("{}-hop-1", preset.label),
        &format!("{} prep hop 1 non-RFQ", preset.label),
        PoolSelection::ExcludeProtocolPrefix("rfq:"),
    )
    .await?;
    let mut prep_evidence_files = first_hop.report.evidence_files.clone();
    let Some(first_pool) = first_hop.selected_pool.clone() else {
        reports.push(first_hop.report);
        reports.push(build_skipped_encode_report(
            preset.label,
            "Skipping /encode because Bebop prep hop 1 did not produce a non-RFQ pool.",
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
        request_id: format!("{}-hop-2-{}", preset.label, epoch_now_s()?),
        auction_id: None,
        token_in: mid_token.to_string(),
        token_out: token_out.to_string(),
        amounts: hop_amounts,
    };
    let mut second_hop = run_encode_prep_hop(
        client,
        repo,
        evidence_dir,
        &simulate_endpoint,
        &second_request,
        &format!("{}-hop-2", preset.label),
        &format!("{} prep hop 2 Bebop", preset.label),
        PoolSelection::RequiredProtocol("rfq:bebop"),
    )
    .await?;
    prep_evidence_files.extend(second_hop.report.evidence_files.clone());
    let comparison_pool = second_hop
        .quote
        .as_ref()
        .and_then(|quote| select_best_pool_excluding_prefix(quote, "rfq:"));
    if comparison_pool.is_none() {
        degrade_report_for_missing_required_pool(&mut second_hop.report, "non-RFQ comparison pool");
    }

    let Some(bebop_pool) = second_hop.selected_pool.clone() else {
        reports.push(second_hop.report);
        reports.push(build_skipped_encode_report(
            preset.label,
            "Skipping /encode because prep hop 2 did not surface rfq:bebop.",
            &prep_evidence_files,
            &protocols,
        ));
        return Ok(reports);
    };
    let Some(comparison_pool) = comparison_pool else {
        reports.push(second_hop.report);
        reports.push(build_skipped_encode_report(
            preset.label,
            "Skipping /encode because prep hop 2 did not surface a non-RFQ comparison pool.",
            &prep_evidence_files,
            &protocols,
        ));
        return Ok(reports);
    };
    reports.push(second_hop.report);

    for protocol in [
        bebop_pool.protocol.clone(),
        comparison_pool.protocol.clone(),
    ] {
        *protocols.entry(protocol).or_insert(0usize) += 1;
    }

    let min_amount_out = bebop_encode_probe_min_amount_out();

    let expected_taker_amount = expected_bebop_taker_amount_from_first_hop(
        first_pool
            .quote
            .amounts_out
            .first()
            .map(String::as_str)
            .unwrap_or("0"),
    );
    let encode_request = build_bebop_partial_fill_encode_request(
        repo,
        args.chain_id,
        preset,
        token_in,
        mid_token,
        token_out,
        &first_pool,
        &bebop_pool,
        &comparison_pool,
        &min_amount_out,
    )?;
    let expectation = BebopInspectionExpectation {
        token_in: mid_token.to_string(),
        token_out: token_out.to_string(),
        expected_taker_amount,
    };
    let encode_observation = execute_encode_request(
        client,
        &encode_url(&args.base_url),
        &encode_request,
        preset.label,
        &protocols,
        &notes,
        Some(&expectation),
    )
    .await?;
    let encode_artifact =
        save_observation_artifact(repo, evidence_dir, preset.label, &encode_observation)?;

    let mut report = build_scenario_report(
        "encode",
        preset.label,
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

#[expect(
    clippy::too_many_arguments,
    reason = "Prep-hop probing keeps HTTP, artifact, and selection inputs explicit."
)]
async fn run_encode_prep_hop(
    client: &Client,
    repo: &Path,
    evidence_dir: &Path,
    endpoint: &str,
    request: &AmountOutRequest,
    artifact_stem: &str,
    scenario_label: &str,
    selection: PoolSelection<'_>,
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
    let mut decoded_quote = None;
    let selected_pool = match deserialize_quote_result(&observation) {
        Ok(quote) => {
            let selected_pool = match selection {
                PoolSelection::Best => select_best_pool(&quote),
                PoolSelection::RequiredProtocol(protocol) => {
                    select_best_pool_by_protocol(&quote, protocol)
                }
                PoolSelection::ExcludeProtocolPrefix(prefix) => {
                    select_best_pool_excluding_prefix(&quote, prefix)
                }
            };
            decoded_quote = Some(quote);
            match selected_pool {
                Some(pool) => {
                    report
                        .notes
                        .push(format!("selected pool candidate: {}", pool.quote.pool_name));
                    Some(pool)
                }
                None => {
                    report.notes.push(format!(
                        "no usable pool row was available for {}",
                        selection.label()
                    ));
                    if let PoolSelection::RequiredProtocol(protocol) = selection {
                        degrade_report_for_missing_required_pool(&mut report, protocol);
                    }
                    None
                }
            }
        }
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
        quote: decoded_quote,
    })
}

fn build_skipped_encode_report(
    label: &str,
    reason: &str,
    evidence_files: &[String],
    protocols: &BTreeMap<String, usize>,
) -> ScenarioReport {
    let mut report = build_scenario_report("encode", label, "/encode", &[], evidence_files);
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
    preset: &EncodeRoutePreset,
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
        estimated_amount_in: None,
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "Explicit route parts keep the partial-fill probe easy to audit."
)]
fn build_bebop_partial_fill_encode_request(
    repo: &Path,
    chain_id: u64,
    preset: &EncodeRoutePreset,
    token_in: &str,
    mid_token: &str,
    token_out: &str,
    first_pool: &SelectedPool,
    bebop_pool: &SelectedPool,
    comparison_pool: &SelectedPool,
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
                    swaps: vec![
                        encode_pool_swap_with_split(
                            bebop_pool,
                            mid_token,
                            token_out,
                            BEBOP_SPLIT_BPS,
                        ),
                        encode_pool_swap(comparison_pool, mid_token, token_out),
                    ],
                },
            ],
        }],
        request_id: Some(format!("{}-{}", preset.label, epoch_now_s()?)),
        estimated_amount_in: None,
    })
}

fn encode_pool_swap(pool: &SelectedPool, token_in: &str, token_out: &str) -> PoolSwapDraft {
    encode_pool_swap_with_split(pool, token_in, token_out, 0)
}

fn encode_pool_swap_with_split(
    pool: &SelectedPool,
    token_in: &str,
    token_out: &str,
    split_bps: u32,
) -> PoolSwapDraft {
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
        split_bps,
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
            encode_inspections: Vec::new(),
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
    label: &str,
    protocols: &BTreeMap<String, usize>,
    notes: &[String],
    inspection: Option<&BebopInspectionExpectation>,
) -> Result<RequestObservation> {
    let request_value =
        serde_json::to_value(request).context("failed to serialize encode request")?;
    match post_json(client, url, request).await {
        Ok(response) => {
            let mut observation =
                classify_encode_response(label, request_value, response, inspection);
            observation.protocols = protocols.keys().cloned().collect();
            if !notes.is_empty() {
                observation.note = Some(match observation.note {
                    Some(existing_note) => format!("{existing_note}; {}", notes.join(" ")),
                    None => notes.join(" "),
                });
            }
            Ok(observation)
        }
        Err(error) => Ok(RequestObservation {
            status_label: None,
            result_quality: None,
            elapsed_ms: None,
            classification: ObservationClass::Error,
            protocols: protocols.keys().cloned().collect(),
            encode_inspections: Vec::new(),
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
            encode_inspections: Vec::new(),
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
            encode_inspections: Vec::new(),
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
        && !quote.meta.rfq_unavailable
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
        encode_inspections: Vec::new(),
        note: (!note_parts.is_empty()).then(|| note_parts.join("; ")),
        request,
        response: response.json,
        http_status,
        error: None,
    }
}

fn classify_encode_response(
    label: &str,
    request: Value,
    response: HttpJsonResponse,
    inspection: Option<&BebopInspectionExpectation>,
) -> RequestObservation {
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
            encode_inspections: Vec::new(),
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
            encode_inspections: Vec::new(),
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
    let encode_inspections = inspection.map_or_else(Vec::new, |expectation| {
        vec![inspect_bebop_calldata(label, &encode_response, expectation)]
    });
    if let Some(inspection) = encode_inspections.first() {
        note_parts.push(format!(
            "bebop_inspection={} inspected={}",
            inspection.status, inspection.inspected_bebop_payloads
        ));
    }
    let inspection_matched = encode_inspections
        .iter()
        .all(|inspection| inspection.status == "matched");

    RequestObservation {
        status_label: Some("encoded".to_string()),
        result_quality: None,
        elapsed_ms,
        classification: if has_router_call && inspection_matched {
            ObservationClass::Healthy
        } else {
            ObservationClass::Degraded
        },
        protocols: Vec::new(),
        encode_inspections,
        note: Some(note_parts.join("; ")),
        request,
        response: response.json,
        http_status,
        error: None,
    }
}

fn inspect_bebop_calldata(
    label: &str,
    response: &RouteEncodeResponse,
    expectation: &BebopInspectionExpectation,
) -> EncodeInspectionReport {
    let expected_token_in = match parse_hex_bytes(&expectation.token_in, 20) {
        Ok(bytes) => bytes,
        Err(error) => {
            return EncodeInspectionReport {
                label: label.to_string(),
                status: "missing".to_string(),
                inspected_bebop_payloads: 0,
                original_filled_taker_amount: None,
                expected_taker_amount: Some(expectation.expected_taker_amount.clone()),
                partial_fill_offset: None,
                detail: Some(format!("invalid expected token_in: {error:#}")),
            };
        }
    };
    let expected_token_out = match parse_hex_bytes(&expectation.token_out, 20) {
        Ok(bytes) => bytes,
        Err(error) => {
            return EncodeInspectionReport {
                label: label.to_string(),
                status: "missing".to_string(),
                inspected_bebop_payloads: 0,
                original_filled_taker_amount: None,
                expected_taker_amount: Some(expectation.expected_taker_amount.clone()),
                partial_fill_offset: None,
                detail: Some(format!("invalid expected token_out: {error:#}")),
            };
        }
    };

    let mut payloads = Vec::new();
    let mut errors = Vec::new();
    for interaction in response
        .interactions
        .iter()
        .filter(|interaction| interaction.kind == InteractionKind::Call)
    {
        match inspect_router_call_for_bebop(
            &interaction.calldata,
            &expected_token_in,
            &expected_token_out,
        ) {
            Ok(found) => payloads.extend(found),
            Err(error) => errors.push(error.to_string()),
        }
    }

    let Some(payload) = payloads.first() else {
        return EncodeInspectionReport {
            label: label.to_string(),
            status: "missing".to_string(),
            inspected_bebop_payloads: 0,
            original_filled_taker_amount: None,
            expected_taker_amount: Some(expectation.expected_taker_amount.clone()),
            partial_fill_offset: None,
            detail: Some(if errors.is_empty() {
                "no Bebop executor payload matched the expected token pair".to_string()
            } else {
                format!(
                    "no Bebop executor payload matched the expected token pair; decode errors: {}",
                    errors.join("; ")
                )
            }),
        };
    };

    let status = if payload.original_filled_taker_amount == expectation.expected_taker_amount {
        "matched"
    } else {
        "mismatch"
    };
    EncodeInspectionReport {
        label: label.to_string(),
        status: status.to_string(),
        inspected_bebop_payloads: payloads.len(),
        original_filled_taker_amount: Some(payload.original_filled_taker_amount.clone()),
        expected_taker_amount: Some(expectation.expected_taker_amount.clone()),
        partial_fill_offset: Some(payload.partial_fill_offset),
        detail: (status == "mismatch").then(|| {
            "originalFilledTakerAmount did not match expected token-in amount".to_string()
        }),
    }
}

struct BebopPayloadFields {
    original_filled_taker_amount: String,
    partial_fill_offset: u8,
}

fn inspect_router_call_for_bebop(
    calldata_hex: &str,
    token_in: &[u8],
    token_out: &[u8],
) -> Result<Vec<BebopPayloadFields>> {
    let calldata = parse_hex_bytes(calldata_hex, 0)?;
    let (swap_encoding, swaps) = extract_router_swaps(&calldata)?;
    let protocol_payloads = extract_protocol_payloads(swap_encoding, &swaps)?;
    Ok(protocol_payloads
        .iter()
        .filter_map(|payload| decode_bebop_payload(payload, token_in, token_out))
        .collect())
}

#[derive(Clone, Copy)]
enum RouterSwapEncoding {
    Single,
    Sequential,
    Split,
}

fn extract_router_swaps(calldata: &[u8]) -> Result<(RouterSwapEncoding, Vec<u8>)> {
    if calldata.len() < FUNCTION_SELECTOR_BYTES {
        bail!("router calldata is shorter than a function selector");
    }
    let selector = &calldata[..FUNCTION_SELECTOR_BYTES];
    let args = &calldata[FUNCTION_SELECTOR_BYTES..];
    if selector == function_selector(SPLIT_SWAP_SIGNATURE) {
        Ok((
            RouterSwapEncoding::Split,
            extract_final_dynamic_bytes_arg(args, 10)?,
        ))
    } else if selector == function_selector(SEQUENTIAL_SWAP_SIGNATURE) {
        Ok((
            RouterSwapEncoding::Sequential,
            extract_final_dynamic_bytes_arg(args, 9)?,
        ))
    } else if selector == function_selector(SINGLE_SWAP_SIGNATURE) {
        Ok((
            RouterSwapEncoding::Single,
            extract_final_dynamic_bytes_arg(args, 9)?,
        ))
    } else {
        bail!("router calldata selector did not match a supported swap function");
    }
}

fn extract_final_dynamic_bytes_arg(args: &[u8], arg_count: usize) -> Result<Vec<u8>> {
    let head_len = arg_count
        .checked_mul(ABI_WORD_BYTES)
        .context("ABI head length overflowed")?;
    if args.len() < head_len {
        bail!("router calldata args are shorter than the ABI head");
    }
    let offset_word_start = (arg_count - 1) * ABI_WORD_BYTES;
    let offset = abi_word_to_usize(&args[offset_word_start..offset_word_start + ABI_WORD_BYTES])?;
    let length_word_end = offset
        .checked_add(ABI_WORD_BYTES)
        .context("dynamic bytes offset overflowed")?;
    if args.len() < length_word_end {
        bail!("dynamic bytes offset points outside calldata");
    }
    let length = abi_word_to_usize(&args[offset..length_word_end])?;
    let bytes_start = length_word_end;
    let bytes_end = bytes_start
        .checked_add(length)
        .context("dynamic bytes length overflowed")?;
    if args.len() < bytes_end {
        bail!("dynamic bytes length points outside calldata");
    }
    Ok(args[bytes_start..bytes_end].to_vec())
}

fn extract_protocol_payloads(encoding: RouterSwapEncoding, swaps: &[u8]) -> Result<Vec<Vec<u8>>> {
    match encoding {
        RouterSwapEncoding::Single => {
            if swaps.len() < 20 {
                bail!("singleSwap payload is shorter than executor address");
            }
            Ok(vec![swaps[20..].to_vec()])
        }
        RouterSwapEncoding::Sequential => decode_ple_segments(swaps)?
            .into_iter()
            .map(|segment| {
                if segment.len() < 20 {
                    bail!("sequentialSwap PLE segment is shorter than executor address");
                }
                Ok(segment[20..].to_vec())
            })
            .collect(),
        RouterSwapEncoding::Split => decode_ple_segments(swaps)?
            .into_iter()
            .map(|segment| {
                if segment.len() < 25 {
                    bail!("splitSwap PLE segment is shorter than split executor header");
                }
                Ok(segment[25..].to_vec())
            })
            .collect(),
    }
}

fn decode_ple_segments(bytes: &[u8]) -> Result<Vec<&[u8]>> {
    let mut cursor = 0usize;
    let mut segments = Vec::new();
    while cursor < bytes.len() {
        if bytes.len() - cursor < 2 {
            bail!("PLE bytes ended inside a length prefix");
        }
        let len = u16::from_be_bytes([bytes[cursor], bytes[cursor + 1]]) as usize;
        cursor += 2;
        let end = cursor
            .checked_add(len)
            .context("PLE segment length overflowed")?;
        if end > bytes.len() {
            bail!("PLE segment length points outside swaps bytes");
        }
        segments.push(&bytes[cursor..end]);
        cursor = end;
    }
    Ok(segments)
}

fn decode_bebop_payload(
    protocol_data: &[u8],
    token_in: &[u8],
    token_out: &[u8],
) -> Option<BebopPayloadFields> {
    if protocol_data.len() < BEBOP_PACKED_PREFIX_BYTES {
        return None;
    }
    if &protocol_data[0..20] != token_in || &protocol_data[20..40] != token_out {
        return None;
    }
    Some(BebopPayloadFields {
        original_filled_taker_amount: BigUint::from_bytes_be(
            &protocol_data[BEBOP_ORIGINAL_TAKER_AMOUNT_START..BEBOP_ORIGINAL_TAKER_AMOUNT_END],
        )
        .to_string(),
        partial_fill_offset: protocol_data[BEBOP_PARTIAL_FILL_OFFSET_INDEX],
    })
}

fn parse_hex_bytes(value: &str, expected_len: usize) -> Result<Vec<u8>> {
    let trimmed = value.strip_prefix("0x").unwrap_or(value);
    let bytes = hex::decode(trimmed).with_context(|| format!("invalid hex value {value}"))?;
    if expected_len > 0 && bytes.len() != expected_len {
        bail!("expected {expected_len} byte(s), got {}", bytes.len());
    }
    Ok(bytes)
}

fn function_selector(signature: &str) -> [u8; 4] {
    let mut hasher = Keccak256::new();
    hasher.update(signature.as_bytes());
    let hash = hasher.finalize();
    [hash[0], hash[1], hash[2], hash[3]]
}

fn abi_word_to_usize(word: &[u8]) -> Result<usize> {
    let mut value = 0usize;
    for byte in word {
        value = value
            .checked_mul(256)
            .and_then(|current| current.checked_add(*byte as usize))
            .context("ABI word does not fit in usize")?;
    }
    Ok(value)
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
    let mut encode_inspections = Vec::new();

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
        encode_inspections.extend(observation.encode_inspections.clone());
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
        encode_inspections,
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
    select_best_pool_matching(quote, |_| true)
}

fn select_best_pool_by_protocol(quote: &QuoteResult, protocol: &str) -> Option<SelectedPool> {
    select_best_pool_matching(quote, |candidate| candidate == protocol)
}

fn select_best_pool_excluding_prefix(quote: &QuoteResult, prefix: &str) -> Option<SelectedPool> {
    select_best_pool_matching(quote, |candidate| !candidate.starts_with(prefix))
}

fn select_best_pool_matching(
    quote: &QuoteResult,
    accepts_protocol: impl Fn(&str) -> bool,
) -> Option<SelectedPool> {
    let (entry, protocol) = quote
        .data
        .iter()
        .filter(|entry| usable_pool_entry(entry))
        .filter_map(|entry| {
            let protocol = protocol_for_quote_entry(quote, entry);
            accepts_protocol(&protocol).then_some((entry, protocol))
        })
        .max_by(|left, right| {
            compare_amount_strings(&left.0.amounts_out[0], &right.0.amounts_out[0])
        })?;

    Some(SelectedPool {
        quote: entry.clone(),
        protocol,
    })
}

fn usable_pool_entry(entry: &AmountOutResponse) -> bool {
    entry
        .amounts_out
        .first()
        .is_some_and(|amount| amount != "0")
        && entry.amounts_out.iter().any(|amount| amount != "0")
}

fn protocol_for_quote_entry(quote: &QuoteResult, entry: &AmountOutResponse) -> String {
    quote
        .meta
        .pool_results
        .iter()
        .find(|pool| pool.pool == entry.pool)
        .map(|pool| pool.protocol.clone())
        .unwrap_or_else(|| protocol_from_pool_name(&entry.pool_name))
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
        "rfq:hashflow" | "Hashflow" | "hashflow" | "hashflow_pool" => Some("rfq:hashflow"),
        "rfq:bebop" | "Bebop" | "bebop" | "bebop_pool" => Some("rfq:bebop"),
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
    let safe_bps = BigUint::from(BPS_DENOMINATOR.saturating_sub(bps));
    let scaled = value * safe_bps;
    (scaled / BigUint::from(BPS_DENOMINATOR)).to_string()
}

fn split_amount_by_bps(amount: &str, bps: u32) -> String {
    let Ok(value) = BigUint::from_str(amount) else {
        return "0".to_string();
    };
    let scaled = value * BigUint::from(bps);
    (scaled / BigUint::from(BPS_DENOMINATOR)).to_string()
}

fn expected_bebop_taker_amount_from_first_hop(first_hop_amount_out: &str) -> String {
    split_amount_by_bps(first_hop_amount_out, BEBOP_SPLIT_BPS)
}

fn bebop_encode_probe_min_amount_out() -> String {
    // This diagnostic validates encoding, not quote quality. Keep the guard permissive
    // so a worse comparison AMM leg cannot block router calldata inspection.
    "1".to_string()
}

fn rfq_required_probe_ready(readiness: &ReadinessSnapshot) -> bool {
    readiness.rfq_enabled && readiness.rfq_status.as_deref() == Some("ready")
}

fn degrade_report_for_missing_required_pool(report: &mut ScenarioReport, required_pool: &str) {
    if report.error_count == 0 && report.degraded_count == 0 {
        report.healthy_count = report.healthy_count.saturating_sub(1);
        report.degraded_count = 1;
        report.request_count = report.request_count.max(1);
    }
    let descriptor = if required_pool.contains(':') {
        format!("required protocol {required_pool}")
    } else {
        format!("required {required_pool}")
    };
    report
        .notes
        .push(format!("{descriptor} was not available for route assembly"));
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
    require_rfq: bool,
) -> Result<()> {
    let timeout_secs = match (require_vm, require_rfq) {
        (true, true) => DEFAULT_VM_READY_TIMEOUT_SECS.max(DEFAULT_RFQ_READY_TIMEOUT_SECS),
        (true, false) => DEFAULT_VM_READY_TIMEOUT_SECS,
        (false, true) => DEFAULT_RFQ_READY_TIMEOUT_SECS,
        (false, false) => DEFAULT_READY_TIMEOUT_SECS,
    };
    let args = build_wait_ready_args(status_url, timeout_secs, chain_id, require_vm, require_rfq);
    run_script(&scripts.wait_ready, &args).map(|_| ())
}

async fn wait_for_native_readiness(client: &Client, status_url: &str, chain_id: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(DEFAULT_READY_TIMEOUT_SECS);
    #[expect(unused_assignments)]
    let mut last_observation = String::new();
    #[expect(unused_assignments)]
    let mut saw_observation = false;

    loop {
        match fetch_readiness_snapshot(client, status_url).await {
            Ok(snapshot) => {
                if snapshot.chain_id != chain_id {
                    bail!(
                        "server at {} reported chain_id={} while waiting for native readiness (expected {})",
                        status_url,
                        snapshot.chain_id,
                        chain_id
                    );
                }
                if snapshot_native_ready(&snapshot) {
                    return Ok(());
                }
                last_observation = format!(
                    "status={} native_status={}",
                    snapshot.status,
                    snapshot.native_status.as_deref().unwrap_or("unknown")
                );
                saw_observation = true;
            }
            Err(err) => {
                last_observation = err.to_string();
                saw_observation = true;
            }
        }

        if Instant::now() >= deadline {
            let suffix = if saw_observation {
                format!(": last observation {last_observation}")
            } else {
                String::new()
            };
            bail!(
                "timed out waiting for native readiness at {}{}",
                status_url,
                suffix
            );
        }

        sleep(Duration::from_secs(2)).await;
    }
}

fn snapshot_native_ready(snapshot: &ReadinessSnapshot) -> bool {
    snapshot
        .native_status
        .as_deref()
        .unwrap_or(snapshot.status.as_str())
        == "ready"
}

fn build_wait_ready_args(
    status_url: &str,
    timeout_secs: u64,
    chain_id: u64,
    require_vm: bool,
    require_rfq: bool,
) -> Vec<String> {
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
    if require_rfq {
        args.push("--require-rfq-ready".to_string());
    }
    args
}

fn readiness_wait_label(require_vm: bool, require_rfq: bool) -> &'static str {
    match (require_vm, require_rfq) {
        (true, true) => "VM and RFQ wait",
        (true, false) => "VM wait",
        (false, true) => "RFQ wait",
        (false, false) => "wait_ready",
    }
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
        "encode_inspections": observation.encode_inspections,
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
    use super::{apply_slippage, bebop_encode_probe_min_amount_out};
    use super::{
        build_wait_ready_args, capture_log_boundary, classify_simulate_response, collect_logs,
        degrade_report_for_missing_required_pool, discover_latest_baseline, ensure_server_ready,
        expected_bebop_taker_amount_from_first_hop, inspect_bebop_calldata, maybe_load_baseline,
        percentile, protocol_from_pool_name, sanitize_filename, select_best_pool,
        select_best_pool_by_protocol, select_best_pool_excluding_prefix, split_amount_by_bps,
        startup_bind_config, BaselineMode, BebopInspectionExpectation, CliArgs, HttpJsonResponse,
        ObservationClass, ScriptPaths, BEBOP_SPLIT_BPS, DEFAULT_SLIPPAGE_BPS,
    };
    use crate::models::messages::{
        AmountOutResponse, Interaction, InteractionKind, PoolOutcomeKind, PoolSimulationOutcome,
        QuoteMeta, QuoteResult, QuoteResultQuality, QuoteStatus, RouteEncodeResponse,
    };
    use crate::sim_analysis::presets::balanced_profile;
    use alloy_primitives::{Address, Keccak256, U256};
    use alloy_sol_types::SolValue;
    use reqwest::Client;
    use reqwest::StatusCode;
    use serde_json::json;
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

    fn quote_with_protocol_pools(pools: &[(&str, &str, &str)]) -> QuoteResult {
        QuoteResult {
            request_id: "req-1".to_string(),
            data: pools
                .iter()
                .enumerate()
                .map(|(index, (pool, protocol, amount_out))| AmountOutResponse {
                    pool: (*pool).to_string(),
                    pool_name: format!("{protocol}::WETH/USDC"),
                    pool_address: format!("0x{:040x}", index + 1),
                    amounts_out: vec![(*amount_out).to_string()],
                    gas_used: vec![1],
                    block_number: 1,
                    limit_max_in: None,
                    slippage: Vec::new(),
                })
                .collect(),
            meta: QuoteMeta {
                status: QuoteStatus::Ready,
                result_quality: QuoteResultQuality::Complete,
                partial_kind: None,
                block_number: 1,
                vm_block_number: None,
                rfq_block_number: Some(1),
                matching_pools: pools.len(),
                candidate_pools: pools.len(),
                total_pools: Some(pools.len()),
                auction_id: None,
                pool_results: pools
                    .iter()
                    .map(|(pool, protocol, _)| PoolSimulationOutcome {
                        pool: (*pool).to_string(),
                        pool_name: format!("{protocol}::WETH/USDC"),
                        pool_address: "0x0000000000000000000000000000000000000001".to_string(),
                        protocol: (*protocol).to_string(),
                        outcome: PoolOutcomeKind::PartialOutput,
                        reported_steps: 1,
                        expected_steps: 1,
                        reason: None,
                    })
                    .collect(),
                vm_unavailable: false,
                rfq_unavailable: false,
                failures: Vec::new(),
            },
        }
    }

    fn synthetic_split_swap_response(
        token_in: [u8; 20],
        token_out: [u8; 20],
        original_taker_amount: u128,
        partial_fill_offset: u8,
    ) -> RouteEncodeResponse {
        let mut protocol_data = Vec::new();
        protocol_data.extend_from_slice(&token_in);
        protocol_data.extend_from_slice(&token_out);
        protocol_data.push(1);
        protocol_data.push(partial_fill_offset);
        protocol_data.extend_from_slice(&U256::from(original_taker_amount).to_be_bytes::<32>());
        protocol_data.push(1);
        protocol_data.extend_from_slice(&[0x44; 20]);
        protocol_data.extend_from_slice(&[0xaa; 32]);

        let mut payload = Vec::new();
        payload.push(0);
        payload.push(1);
        payload.extend_from_slice(&5_000u32.to_be_bytes()[1..]);
        payload.extend_from_slice(&[0x33; 20]);
        payload.extend_from_slice(&protocol_data);

        let mut swaps = Vec::new();
        swaps.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        swaps.extend_from_slice(&payload);

        let signature =
            "splitSwap(uint256,address,address,uint256,bool,bool,uint256,address,bool,bytes)";
        let calldata_args = (
            U256::from(1u64),
            Address::from(token_in),
            Address::from(token_out),
            U256::from(1u64),
            false,
            false,
            U256::from(2u64),
            Address::from([0x55; 20]),
            false,
            swaps,
        )
            .abi_encode_params();
        let selector = function_selector(signature);
        let mut calldata = Vec::new();
        calldata.extend_from_slice(&selector);
        calldata.extend_from_slice(&calldata_args);

        RouteEncodeResponse {
            interactions: vec![Interaction {
                kind: InteractionKind::Call,
                target: "0x0000000000000000000000000000000000000001".to_string(),
                value: "0".to_string(),
                calldata: format!("0x{}", alloy_primitives::hex::encode(calldata)),
            }],
            debug: None,
        }
    }

    fn function_selector(signature: &str) -> [u8; 4] {
        let mut hasher = Keccak256::new();
        hasher.update(signature.as_bytes());
        let hash = hasher.finalize();
        [hash[0], hash[1], hash[2], hash[3]]
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

    #[test]
    fn build_wait_ready_args_supports_vm_and_rfq_requirements() {
        let rfq_only_args =
            build_wait_ready_args("http://localhost:3000/status", 600, 1, false, true);
        assert!(rfq_only_args.iter().any(|arg| arg == "--require-rfq-ready"));
        assert!(!rfq_only_args.iter().any(|arg| arg == "--require-vm-ready"));

        let vm_and_rfq_args =
            build_wait_ready_args("http://localhost:3000/status", 600, 1, true, true);
        assert!(vm_and_rfq_args
            .iter()
            .any(|arg| arg == "--require-vm-ready"));
        assert!(vm_and_rfq_args
            .iter()
            .any(|arg| arg == "--require-rfq-ready"));
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
    fn classify_simulate_response_marks_rfq_unavailable_as_degraded() {
        let quote = QuoteResult {
            request_id: "req-1".to_string(),
            data: vec![AmountOutResponse {
                pool: "pool-1".to_string(),
                pool_name: "UniswapV3::DAI/USDC".to_string(),
                pool_address: "0x0000000000000000000000000000000000000001".to_string(),
                amounts_out: vec!["10".to_string()],
                gas_used: vec![1],
                block_number: 1,
                limit_max_in: None,
                slippage: Vec::new(),
            }],
            meta: QuoteMeta {
                status: QuoteStatus::Ready,
                result_quality: QuoteResultQuality::Complete,
                partial_kind: None,
                block_number: 1,
                vm_block_number: None,
                rfq_block_number: Some(1),
                matching_pools: 1,
                candidate_pools: 1,
                total_pools: Some(1),
                auction_id: None,
                pool_results: Vec::new(),
                vm_unavailable: false,
                rfq_unavailable: true,
                failures: Vec::new(),
            },
        };
        let body = serde_json::to_string(&quote).unwrap_or_default();
        let response = HttpJsonResponse {
            status_code: StatusCode::OK,
            elapsed_ms: 1.0,
            body_text: body,
            json: serde_json::to_value(quote).ok(),
        };

        let observation = classify_simulate_response(json!({ "request_id": "req-1" }), response);

        assert!(matches!(
            observation.classification,
            ObservationClass::Degraded
        ));
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
                slippage: Vec::new(),
                limit_max_in: None,
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
                slippage: Vec::new(),
                limit_max_in: None,
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
                slippage: Vec::new(),
                limit_max_in: None,
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
    fn select_best_pool_preserves_rfq_protocol_from_quote_metadata() {
        let quote = QuoteResult {
            request_id: "req-1".to_string(),
            data: vec![AmountOutResponse {
                pool: "pool-1".to_string(),
                pool_name: "Hashflow::WETH/USDC".to_string(),
                pool_address: "0x0000000000000000000000000000000000000001".to_string(),
                amounts_out: vec!["10".to_string()],
                gas_used: vec![1],
                slippage: Vec::new(),
                limit_max_in: None,
                block_number: 1,
            }],
            meta: QuoteMeta {
                status: QuoteStatus::Ready,
                result_quality: QuoteResultQuality::Complete,
                partial_kind: None,
                block_number: 1,
                vm_block_number: None,
                rfq_block_number: Some(2),
                matching_pools: 1,
                candidate_pools: 1,
                total_pools: None,
                auction_id: None,
                pool_results: vec![PoolSimulationOutcome {
                    pool: "pool-1".to_string(),
                    pool_name: "Hashflow::WETH/USDC".to_string(),
                    pool_address: "0x0000000000000000000000000000000000000001".to_string(),
                    protocol: "rfq:hashflow".to_string(),
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
            Some("rfq:hashflow")
        );
    }

    #[test]
    fn select_best_pool_preserves_bebop_protocol_from_quote_metadata() {
        let quote = QuoteResult {
            request_id: "req-1".to_string(),
            data: vec![AmountOutResponse {
                pool: "pool-1".to_string(),
                pool_name: "Bebop::WETH/USDC".to_string(),
                pool_address: "0x0000000000000000000000000000000000000001".to_string(),
                amounts_out: vec!["10".to_string()],
                gas_used: vec![1],
                slippage: Vec::new(),
                limit_max_in: None,
                block_number: 1,
            }],
            meta: QuoteMeta {
                status: QuoteStatus::Ready,
                result_quality: QuoteResultQuality::Complete,
                partial_kind: None,
                block_number: 1,
                vm_block_number: None,
                rfq_block_number: Some(2),
                matching_pools: 1,
                candidate_pools: 1,
                total_pools: None,
                auction_id: None,
                pool_results: vec![PoolSimulationOutcome {
                    pool: "pool-1".to_string(),
                    pool_name: "Bebop::WETH/USDC".to_string(),
                    pool_address: "0x0000000000000000000000000000000000000001".to_string(),
                    protocol: "rfq:bebop".to_string(),
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
            Some("rfq:bebop")
        );
    }

    #[test]
    fn select_best_pool_by_protocol_prefers_bebop_over_other_protocols() {
        let quote = quote_with_protocol_pools(&[
            ("hashflow-pool", "rfq:hashflow", "120"),
            ("aero-pool", "aerodrome_slipstreams", "200"),
            ("bebop-pool", "rfq:bebop", "100"),
        ]);

        let selected = select_best_pool_by_protocol(&quote, "rfq:bebop");

        assert_eq!(
            selected.as_ref().map(|pool| pool.quote.pool.as_str()),
            Some("bebop-pool")
        );
        assert_eq!(
            selected.as_ref().map(|pool| pool.protocol.as_str()),
            Some("rfq:bebop")
        );
    }

    #[test]
    fn select_best_pool_excluding_prefix_skips_rfq_pools() {
        let quote = quote_with_protocol_pools(&[
            ("bebop-pool", "rfq:bebop", "300"),
            ("hashflow-pool", "rfq:hashflow", "250"),
            ("aero-pool", "aerodrome_slipstreams", "100"),
        ]);

        let selected = select_best_pool_excluding_prefix(&quote, "rfq:");

        assert_eq!(
            selected.as_ref().map(|pool| pool.quote.pool.as_str()),
            Some("aero-pool")
        );
        assert_eq!(
            selected.as_ref().map(|pool| pool.protocol.as_str()),
            Some("aerodrome_slipstreams")
        );
    }

    #[test]
    fn missing_required_bebop_pool_marks_encode_prep_degraded() {
        let mut report =
            super::build_scenario_report("encode-prep", "bebop prep hop 2", "/simulate", &[], &[]);

        degrade_report_for_missing_required_pool(&mut report, "rfq:bebop");

        assert_eq!(report.healthy_count, 0);
        assert_eq!(report.degraded_count, 1);
        assert!(report
            .notes
            .iter()
            .any(|note| note.contains("required protocol rfq:bebop was not available")));
    }

    #[test]
    fn bebop_probe_expects_raw_first_hop_split_amount() {
        let raw_first_hop_amount = "1000000000000000000";
        let slippage_reduced_amount = apply_slippage(raw_first_hop_amount, DEFAULT_SLIPPAGE_BPS);

        assert_eq!(
            expected_bebop_taker_amount_from_first_hop(raw_first_hop_amount),
            "500000000000000000"
        );
        assert_ne!(
            expected_bebop_taker_amount_from_first_hop(raw_first_hop_amount),
            split_amount_by_bps(&slippage_reduced_amount, BEBOP_SPLIT_BPS)
        );
    }

    #[test]
    fn bebop_probe_uses_permissive_min_amount_out() {
        assert_eq!(bebop_encode_probe_min_amount_out(), "1");
    }

    #[test]
    fn inspect_bebop_calldata_extracts_split_swap_packed_taker_amount() {
        let token_in = [0x11; 20];
        let token_out = [0x22; 20];
        let response =
            synthetic_split_swap_response(token_in, token_out, 37_364_114_182_137_972, 92);
        let expectation = BebopInspectionExpectation {
            token_in: format!("0x{}", alloy_primitives::hex::encode(token_in)),
            token_out: format!("0x{}", alloy_primitives::hex::encode(token_out)),
            expected_taker_amount: "37364114182137972".to_string(),
        };

        let inspection =
            inspect_bebop_calldata("bebop-partial-fill-encode", &response, &expectation);

        assert_eq!(inspection.status, "matched");
        assert_eq!(inspection.inspected_bebop_payloads, 1);
        assert_eq!(
            inspection.original_filled_taker_amount.as_deref(),
            Some("37364114182137972")
        );
        assert_eq!(inspection.partial_fill_offset, Some(92));
    }

    #[test]
    fn inspect_bebop_calldata_reports_output_unit_taker_amount_mismatch() {
        let token_in = [0x11; 20];
        let token_out = [0x22; 20];
        let response = synthetic_split_swap_response(token_in, token_out, 84_452_306, 92);
        let expectation = BebopInspectionExpectation {
            token_in: format!("0x{}", alloy_primitives::hex::encode(token_in)),
            token_out: format!("0x{}", alloy_primitives::hex::encode(token_out)),
            expected_taker_amount: "37364114182137972".to_string(),
        };

        let inspection =
            inspect_bebop_calldata("bebop-partial-fill-encode", &response, &expectation);

        assert_eq!(inspection.status, "mismatch");
        assert_eq!(
            inspection.original_filled_taker_amount.as_deref(),
            Some("84452306")
        );
        assert_eq!(
            inspection.expected_taker_amount.as_deref(),
            Some("37364114182137972")
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
                slippage: Vec::new(),
                limit_max_in: None,
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
                slippage: Vec::new(),
                limit_max_in: None,
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

    #[test]
    fn protocol_from_pool_name_normalizes_rfq_aliases() {
        assert_eq!(
            protocol_from_pool_name("Hashflow::WETH/USDC"),
            "rfq:hashflow"
        );
        assert_eq!(protocol_from_pool_name("Bebop::WETH/USDC"), "rfq:bebop");
        assert_eq!(protocol_from_pool_name("hashflow_pool"), "rfq:hashflow");
        assert_eq!(protocol_from_pool_name("bebop_pool"), "rfq:bebop");
    }

    #[test]
    fn balanced_profile_marks_base_and_ethereum_scenarios_as_rfq_targeted() {
        let ethereum_profile_result = balanced_profile(1, false);
        assert!(ethereum_profile_result.is_ok());
        let Some(ethereum_profile) = ethereum_profile_result.ok() else {
            return;
        };
        assert!(ethereum_profile
            .simulate_scenarios
            .iter()
            .any(|scenario| scenario.expect_rfq_visibility));

        let base_profile_result = balanced_profile(8453, false);
        assert!(base_profile_result.is_ok());
        let Some(base_profile) = base_profile_result.ok() else {
            return;
        };
        assert!(base_profile
            .simulate_scenarios
            .iter()
            .any(|scenario| scenario.expect_rfq_visibility));
    }
}
