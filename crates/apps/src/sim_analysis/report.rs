use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

const EXPECTED_RFQ_PROTOCOL_VISIBILITY_NOTE: &str = "expected RFQ protocol visibility, saw none";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReadinessSnapshot {
    pub status: String,
    pub chain_id: u64,
    pub backends: BTreeMap<String, ReadinessBackendSnapshot>,
}

impl ReadinessSnapshot {
    pub fn backend(&self, kind: &str) -> Option<&ReadinessBackendSnapshot> {
        self.backends.get(kind)
    }

    pub fn native_status(&self) -> Option<&str> {
        self.backend("native")
            .map(|backend| backend.status.as_str())
    }

    pub fn native_block_number(&self) -> Option<u64> {
        self.backend("native")
            .and_then(|backend| backend.block_number)
    }

    pub fn native_pool_count(&self) -> Option<u64> {
        self.backend("native")
            .and_then(|backend| backend.pool_count)
    }

    pub fn backend_enabled(&self, kind: &str) -> bool {
        self.backend(kind)
            .map(|backend| backend.enabled)
            .unwrap_or(false)
    }

    pub fn backend_status(&self, kind: &str) -> Option<&str> {
        self.backend(kind).map(|backend| backend.status.as_str())
    }

    pub fn backend_pool_count(&self, kind: &str) -> Option<u64> {
        self.backend(kind).and_then(|backend| backend.pool_count)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReadinessBackendSnapshot {
    pub enabled: bool,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_number: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_timestamp: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rebuild_duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_update_age_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription: Option<ReadinessSubscriptionSnapshot>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReadinessRedisReplayBoundarySnapshot {
    pub stream_key: String,
    pub stream_id: String,
    pub snapshot_id: String,
    pub generation: u64,
    pub exclusive_message_seq: u64,
}

impl ReadinessRedisReplayBoundarySnapshot {
    fn exclusive_entry_id(&self) -> String {
        format!("{}-{}", self.generation, self.exclusive_message_seq)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReadinessSubscriptionSnapshot {
    pub connected: bool,
    pub bootstrap_complete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redis_replay_boundary: Option<ReadinessRedisReplayBoundarySnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redis_catch_up_cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redis_replay_caught_up: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redis_gap_reason: Option<String>,
    pub restart_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AnalysisReport {
    pub schema_version: u64,
    pub run: RunMetadata,
    pub readiness: ReadinessReport,
    pub scenarios: Vec<ScenarioReport>,
    pub logs: LogSummary,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    pub findings: Vec<Finding>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub baseline: Option<BaselineComparison>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RunMetadata {
    pub started_at_epoch_s: u64,
    pub finished_at_epoch_s: u64,
    pub chain_id: u64,
    pub chain_label: String,
    pub profile: String,
    pub repo: String,
    pub base_url: String,
    pub report_dir: String,
    pub started_server: bool,
    pub stop_requested: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReadinessReport {
    pub initial: ReadinessSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_state: Option<ReadinessSnapshot>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ScenarioReport {
    pub kind: String,
    pub label: String,
    pub endpoint: String,
    pub request_count: usize,
    pub healthy_count: usize,
    pub degraded_count: usize,
    pub error_count: usize,
    pub status_counts: BTreeMap<String, usize>,
    pub result_quality_counts: BTreeMap<String, usize>,
    pub protocols_seen: BTreeMap<String, usize>,
    pub latency_ms: LatencySummary,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub evidence_files: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct LatencySummary {
    pub count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub average: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p50: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p90: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p99: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogSummary {
    pub sources: Vec<LogSourceSummary>,
    pub matched_lines: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub excerpt_file: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub highlights: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogSourceSummary {
    pub source: String,
    pub log_file: String,
    pub matched_lines: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub highlights: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Finding {
    pub severity: String,
    pub title: String,
    pub detail: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BaselineComparison {
    pub compared_report_dir: String,
    pub scenario_diffs: Vec<ScenarioDiff>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ScenarioDiff {
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub degraded_rate_delta: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_rate_delta: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p90_delta_pct: Option<f64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

pub fn build_findings(report: &AnalysisReport) -> Vec<Finding> {
    let mut findings = Vec::new();
    add_readiness_findings(report, &mut findings);
    add_scenario_findings(report, &mut findings);
    add_log_findings(report, &mut findings);
    add_baseline_findings(report, &mut findings);
    add_warning_findings(report, &mut findings);

    if findings.is_empty() {
        findings.push(Finding {
            severity: "healthy".to_string(),
            title: "No major anomalies detected".to_string(),
            detail: "The analyzer completed without request errors and did not spot standout regressions or noisy logs.".to_string(),
        });
    }

    findings
}

fn add_readiness_findings(report: &AnalysisReport, findings: &mut Vec<Finding>) {
    if report.readiness.initial.status != "ready" {
        findings.push(Finding {
            severity: "investigate".to_string(),
            title: "Initial service health was not ready".to_string(),
            detail: format!(
                "The analyzer began with /status={} and native={}. The run still proceeded, but overall service health looked unstable at the start.",
                report.readiness.initial.status,
                display_status(report.readiness.initial.native_status())
            ),
        });
    }

    if let Some(native_status) = report.readiness.initial.native_status() {
        if native_status != "ready" {
            findings.push(Finding {
                severity: "investigate".to_string(),
                title: "Initial native readiness was not ready".to_string(),
                detail: format!(
                    "The analyzer saw native={} at the start of the run.",
                    native_status
                ),
            });
        }
    }

    if report.readiness.initial.backend_enabled("vm")
        && report.readiness.initial.backend_status("vm") != Some("ready")
    {
        findings.push(Finding {
            severity: "attention".to_string(),
            title: "VM pools were enabled but not fully ready".to_string(),
            detail: format!(
                "Initial VM status was {} with vm_pools={}.",
                report
                    .readiness
                    .initial
                    .backend_status("vm")
                    .unwrap_or("unknown"),
                report
                    .readiness
                    .initial
                    .backend_pool_count("vm")
                    .map_or_else(|| "unknown".to_string(), |value| value.to_string())
            ),
        });
    }

    if report.readiness.initial.backend_enabled("rfq")
        && report.readiness.initial.backend_status("rfq") != Some("ready")
    {
        findings.push(Finding {
            severity: "attention".to_string(),
            title: "RFQ pools were enabled but not fully ready".to_string(),
            detail: format!(
                "Initial RFQ status was {} with rfq_pools={}.",
                report
                    .readiness
                    .initial
                    .backend_status("rfq")
                    .unwrap_or("unknown"),
                report
                    .readiness
                    .initial
                    .backend_pool_count("rfq")
                    .map_or_else(|| "unknown".to_string(), |value| value.to_string())
            ),
        });
    }
}

fn add_scenario_findings(report: &AnalysisReport, findings: &mut Vec<Finding>) {
    for scenario in &report.scenarios {
        if scenario.error_count > 0 {
            findings.push(Finding {
                severity: "investigate".to_string(),
                title: format!("{} had request errors", scenario.label),
                detail: format!(
                    "{} reported {} errored request(s) across {} total request(s).",
                    scenario.label, scenario.error_count, scenario.request_count
                ),
            });
        } else if scenario.degraded_count > 0 {
            findings.push(Finding {
                severity: "attention".to_string(),
                title: format!("{} surfaced degraded responses", scenario.label),
                detail: format!(
                    "{} reported {} degraded request(s) across {} total request(s).",
                    scenario.label, scenario.degraded_count, scenario.request_count
                ),
            });
        }

        if scenario
            .notes
            .iter()
            .any(|note| note == EXPECTED_RFQ_PROTOCOL_VISIBILITY_NOTE)
        {
            findings.push(Finding {
                severity: "attention".to_string(),
                title: format!("{} missed expected RFQ visibility", scenario.label),
                detail: format!(
                    "{} expected at least one RFQ protocol to surface, but none were observed in the scenario report.",
                    scenario.label
                ),
            });
        }
    }
}

fn add_log_findings(report: &AnalysisReport, findings: &mut Vec<Finding>) {
    for source in &report.logs.sources {
        if source.matched_lines == 0 {
            continue;
        }

        let title = if source.source == "broadcaster" {
            "Broadcaster log warnings or failures were captured"
        } else {
            "Simulator log warnings or failures were captured"
        };

        findings.push(Finding {
            severity: "attention".to_string(),
            title: title.to_string(),
            detail: format!(
                "The analyzer matched {} warning/error-like log line(s) in {}.",
                source.matched_lines, source.log_file
            ),
        });
    }
}

fn add_baseline_findings(report: &AnalysisReport, findings: &mut Vec<Finding>) {
    if let Some(baseline) = &report.baseline {
        for diff in &baseline.scenario_diffs {
            if let Some(delta) = diff.degraded_rate_delta {
                if delta > 0.05 {
                    findings.push(Finding {
                        severity: "attention".to_string(),
                        title: format!("{} degraded rate increased", diff.label),
                        detail: format!(
                            "Degraded rate increased by {:.2}% compared with the saved baseline.",
                            delta * 100.0
                        ),
                    });
                }
            }
            if let Some(delta) = diff.error_rate_delta {
                if delta > 0.0 {
                    findings.push(Finding {
                        severity: "investigate".to_string(),
                        title: format!("{} error rate increased", diff.label),
                        detail: format!(
                            "Error rate increased by {:.2}% compared with the saved baseline.",
                            delta * 100.0
                        ),
                    });
                }
            }
            if let Some(delta) = diff.p90_delta_pct {
                if delta > 25.0 {
                    findings.push(Finding {
                        severity: "attention".to_string(),
                        title: format!("{} latency regressed", diff.label),
                        detail: format!(
                            "p90 latency increased by {:.1}% compared with the saved baseline.",
                            delta
                        ),
                    });
                }
            }
        }
    }
}

fn add_warning_findings(report: &AnalysisReport, findings: &mut Vec<Finding>) {
    for warning in &report.warnings {
        findings.push(Finding {
            severity: "attention".to_string(),
            title: "Analyzer warning".to_string(),
            detail: warning.clone(),
        });
    }
}

pub fn render_summary(report: &AnalysisReport) -> String {
    let mut lines = Vec::new();
    append_summary_header(&mut lines, report);
    append_findings_section(&mut lines, report);
    append_warning_section(&mut lines, report);
    append_scenario_overview(&mut lines, report);
    append_readiness_section(&mut lines, report);
    append_evidence_section(&mut lines, report);

    format!("{}\n", lines.join("\n"))
}

fn append_summary_header(lines: &mut Vec<String>, report: &AnalysisReport) {
    lines.push("# Simulation analysis summary".to_string());
    lines.push(String::new());
    lines.push(format!(
        "- Chain: {} ({})",
        report.run.chain_label, report.run.chain_id
    ));
    lines.push(format!("- Profile: {}", report.run.profile));
    lines.push(format!("- Report dir: {}", report.run.report_dir));
    lines.push(format!(
        "- Server lifecycle: {}",
        if report.run.started_server {
            "started by analyzer"
        } else {
            "reused existing server"
        }
    ));
    lines.push(String::new());
}

fn append_findings_section(lines: &mut Vec<String>, report: &AnalysisReport) {
    lines.push("## Findings".to_string());
    for finding in &report.findings {
        lines.push(format!(
            "- [{}] {}: {}",
            finding.severity, finding.title, finding.detail
        ));
    }
}

fn append_warning_section(lines: &mut Vec<String>, report: &AnalysisReport) {
    if report.warnings.is_empty() {
        return;
    }

    lines.push(String::new());
    lines.push("## Analyzer warnings".to_string());
    for warning in &report.warnings {
        lines.push(format!("- {}", warning));
    }
}

fn append_scenario_overview(lines: &mut Vec<String>, report: &AnalysisReport) {
    lines.push(String::new());
    lines.push("## Scenario overview".to_string());
    for scenario in &report.scenarios {
        lines.push(format!(
            "- {} ({}) healthy={} degraded={} errors={} p90={}ms protocols={}",
            scenario.label,
            scenario.kind,
            scenario.healthy_count,
            scenario.degraded_count,
            scenario.error_count,
            fmt_latency(scenario.latency_ms.p90),
            fmt_protocols(&scenario.protocols_seen)
        ));
        for note in &scenario.notes {
            lines.push(format!("  note: {}", note));
        }
    }
}

fn append_readiness_section(lines: &mut Vec<String>, report: &AnalysisReport) {
    lines.push(String::new());
    lines.push("## Readiness".to_string());
    append_readiness_snapshot(lines, "Initial", &report.readiness.initial);
    if let Some(final_state) = &report.readiness.final_state {
        append_readiness_snapshot(lines, "Final", final_state);
    }
}

fn append_readiness_snapshot(lines: &mut Vec<String>, label: &str, snapshot: &ReadinessSnapshot) {
    lines.push(format!(
        "- {}: status={} native={} block={} pools={} vm={} rfq={}",
        label,
        snapshot.status,
        display_status(snapshot.native_status()),
        fmt_optional_u64(snapshot.native_block_number()),
        fmt_optional_u64(snapshot.native_pool_count()),
        readiness_component_status(
            snapshot.backend_enabled("vm"),
            snapshot.backend_status("vm")
        ),
        readiness_component_status(
            snapshot.backend_enabled("rfq"),
            snapshot.backend_status("rfq")
        )
    ));
    append_redis_replay_status(lines, snapshot);
}

fn append_redis_replay_status(lines: &mut Vec<String>, snapshot: &ReadinessSnapshot) {
    for (kind, backend) in &snapshot.backends {
        let Some(subscription) = &backend.subscription else {
            continue;
        };
        if !subscription.has_redis_replay_status() {
            continue;
        }
        lines.push(format!(
            "  redis {}: boundary={} checkpoint={} caught_up={} gap={}",
            kind,
            subscription.redis_boundary_label(),
            subscription
                .redis_catch_up_cursor
                .as_deref()
                .unwrap_or("unknown"),
            fmt_optional_bool(subscription.redis_replay_caught_up),
            subscription.redis_gap_reason.as_deref().unwrap_or("none")
        ));
    }
}

fn append_evidence_section(lines: &mut Vec<String>, report: &AnalysisReport) {
    lines.push(String::new());
    lines.push("## Evidence".to_string());
    for source in &report.logs.sources {
        lines.push(format!(
            "- Log file ({}): {} matched={}",
            source.source, source.log_file, source.matched_lines
        ));
    }
    if let Some(excerpt) = &report.logs.excerpt_file {
        lines.push(format!("- Log excerpts: {}", excerpt));
    }
    append_log_highlights(lines, report);
    if let Some(baseline) = &report.baseline {
        lines.push(format!(
            "- Compared against: {}",
            baseline.compared_report_dir
        ));
    }
    lines.push("- Primary artifact: report.json".to_string());
    lines.push("- Human summary: summary.md".to_string());
    lines.push(String::new());
}

fn append_log_highlights(lines: &mut Vec<String>, report: &AnalysisReport) {
    if report.logs.highlights.is_empty() {
        return;
    }

    lines.push("- Log highlights:".to_string());
    for highlight in report.logs.highlights.iter().take(6) {
        lines.push(format!("  {}", truncate_log_line(highlight)));
    }
}

fn truncate_log_line(line: &str) -> String {
    const MAX_LOG_HIGHLIGHT_CHARS: usize = 240;
    let mut chars = line.chars();
    let truncated = chars
        .by_ref()
        .take(MAX_LOG_HIGHLIGHT_CHARS)
        .collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

pub fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string()
}

fn fmt_latency(value: Option<f64>) -> String {
    value
        .map(|latency| format!("{latency:.1}"))
        .unwrap_or_else(|| "n/a".to_string())
}

fn fmt_optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "unknown".to_string(), |value| value.to_string())
}

fn fmt_optional_bool(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "true",
        Some(false) => "false",
        None => "unknown",
    }
}

fn display_status(value: Option<&str>) -> &str {
    value.unwrap_or("unknown")
}

fn fmt_protocols(protocols: &BTreeMap<String, usize>) -> String {
    if protocols.is_empty() {
        return "none".to_string();
    }

    protocols
        .iter()
        .take(4)
        .map(|(protocol, count)| format!("{protocol}:{count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn readiness_component_status(enabled: bool, status: Option<&str>) -> &str {
    match status {
        Some(status) => status,
        None if enabled => "unknown",
        None => "disabled",
    }
}

impl ReadinessSubscriptionSnapshot {
    fn has_redis_replay_status(&self) -> bool {
        self.redis_replay_boundary.is_some()
            || self.redis_catch_up_cursor.is_some()
            || self.redis_replay_caught_up.is_some()
            || self.redis_gap_reason.is_some()
    }

    fn redis_boundary_label(&self) -> String {
        self.redis_replay_boundary
            .as_ref()
            .map(ReadinessRedisReplayBoundarySnapshot::exclusive_entry_id)
            .unwrap_or_else(|| "unknown".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_findings, render_summary, AnalysisReport, Finding, LatencySummary, LogSourceSummary,
        LogSummary, ReadinessReport, EXPECTED_RFQ_PROTOCOL_VISIBILITY_NOTE,
    };
    use super::{
        ReadinessBackendSnapshot, ReadinessSnapshot, ReadinessSubscriptionSnapshot, RunMetadata,
        ScenarioReport,
    };
    use std::collections::BTreeMap;

    fn scenario(label: &str, degraded_count: usize, error_count: usize) -> ScenarioReport {
        ScenarioReport {
            kind: "simulate".to_string(),
            label: label.to_string(),
            endpoint: "/simulate".to_string(),
            request_count: 4,
            healthy_count: 4usize.saturating_sub(degraded_count + error_count),
            degraded_count,
            error_count,
            status_counts: BTreeMap::new(),
            result_quality_counts: BTreeMap::new(),
            protocols_seen: BTreeMap::new(),
            latency_ms: LatencySummary::default(),
            notes: Vec::new(),
            evidence_files: Vec::new(),
        }
    }

    fn readiness_backend(
        enabled: bool,
        status: &str,
        block_number: Option<u64>,
        pool_count: Option<u64>,
    ) -> ReadinessBackendSnapshot {
        ReadinessBackendSnapshot {
            enabled,
            status: status.to_string(),
            reason: None,
            block_number,
            update_timestamp: None,
            pool_count,
            restart_count: Some(0),
            last_error: None,
            rebuild_duration_ms: None,
            last_update_age_ms: None,
            subscription: None,
        }
    }

    fn report(findings: Vec<Finding>, scenarios: Vec<ScenarioReport>) -> AnalysisReport {
        AnalysisReport {
            schema_version: 6,
            run: RunMetadata {
                started_at_epoch_s: 1,
                finished_at_epoch_s: 2,
                chain_id: 1,
                chain_label: "ethereum".to_string(),
                profile: "balanced".to_string(),
                repo: ".".to_string(),
                base_url: "http://localhost:3000".to_string(),
                report_dir: "logs/simulation-reports/1/balanced/1".to_string(),
                started_server: true,
                stop_requested: true,
            },
            readiness: ReadinessReport {
                initial: ReadinessSnapshot {
                    status: "ready".to_string(),
                    chain_id: 1,
                    backends: BTreeMap::from([(
                        "native".to_string(),
                        readiness_backend(true, "ready", Some(1), Some(10)),
                    )]),
                },
                final_state: None,
            },
            scenarios,
            logs: LogSummary {
                sources: vec![
                    LogSourceSummary {
                        source: "simulator".to_string(),
                        log_file: "logs/tycho-sim-server.log".to_string(),
                        matched_lines: 0,
                        highlights: Vec::new(),
                    },
                    LogSourceSummary {
                        source: "broadcaster".to_string(),
                        log_file: "logs/tycho-broadcaster-service.log".to_string(),
                        matched_lines: 0,
                        highlights: Vec::new(),
                    },
                ],
                matched_lines: 0,
                excerpt_file: None,
                highlights: Vec::new(),
            },
            warnings: Vec::new(),
            findings,
            baseline: None,
        }
    }

    #[test]
    fn build_findings_marks_degraded_scenarios() {
        let analysis = report(Vec::new(), vec![scenario("core simulate", 1, 0)]);
        let findings = build_findings(&analysis);
        assert!(findings
            .iter()
            .any(|finding| finding.severity == "attention"));
    }

    #[test]
    fn build_findings_marks_healthy_runs() {
        let analysis = report(Vec::new(), vec![scenario("core simulate", 0, 0)]);
        let findings = build_findings(&analysis);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].severity, "healthy");
    }

    #[test]
    fn build_findings_marks_rfq_not_ready() {
        let mut analysis = report(Vec::new(), vec![scenario("core simulate", 0, 0)]);
        analysis.readiness.initial.backends.insert(
            "rfq".to_string(),
            readiness_backend(true, "warming_up", Some(0), Some(0)),
        );
        analysis.readiness.initial.status = "warming_up".to_string();

        let findings = build_findings(&analysis);

        assert!(findings.iter().any(|finding| {
            finding.title == "Initial service health was not ready"
                && finding.severity == "investigate"
        }));
        assert!(findings.iter().any(|finding| {
            finding.title == "RFQ pools were enabled but not fully ready"
                && finding.detail.contains("rfq_pools=0")
        }));
    }

    #[test]
    fn build_findings_marks_missing_rfq_visibility() {
        let mut scenario = scenario("rfq-usdc-weth", 0, 0);
        scenario
            .notes
            .push(EXPECTED_RFQ_PROTOCOL_VISIBILITY_NOTE.to_string());
        let analysis = report(Vec::new(), vec![scenario]);

        let findings = build_findings(&analysis);

        assert!(findings.iter().any(|finding| {
            finding.title == "rfq-usdc-weth missed expected RFQ visibility"
                && finding.severity == "attention"
        }));
    }

    #[test]
    fn readiness_backend_snapshot_preserves_update_timestamp() -> serde_json::Result<()> {
        let snapshot: ReadinessBackendSnapshot = serde_json::from_value(serde_json::json!({
            "enabled": true,
            "status": "ready",
            "update_timestamp": 1_710_000_000u64,
            "pool_count": 1
        }))?;

        let value = serde_json::to_value(snapshot)?;

        assert_eq!(value["update_timestamp"], 1_710_000_000u64);
        Ok(())
    }

    #[test]
    fn readiness_subscription_snapshot_preserves_redis_replay_fields() -> serde_json::Result<()> {
        let snapshot: ReadinessSubscriptionSnapshot = serde_json::from_value(serde_json::json!({
            "connected": true,
            "bootstrap_complete": true,
            "stream_id": "chain-8453-stream-2",
            "snapshot_id": "chain-8453-snapshot-2",
            "redis_replay_boundary": {
                "streamKey": "dsolver:broadcaster:local:8453:events",
                "streamId": "chain-8453-stream-2",
                "snapshotId": "chain-8453-snapshot-2",
                "generation": 2,
                "exclusiveMessageSeq": 14
            },
            "redis_catch_up_cursor": "2-16",
            "redis_replay_caught_up": true,
            "restart_count": 0
        }))?;

        let value = serde_json::to_value(snapshot)?;

        assert_eq!(
            value["redis_replay_boundary"]["streamKey"],
            "dsolver:broadcaster:local:8453:events"
        );
        assert_eq!(value["redis_replay_boundary"]["exclusiveMessageSeq"], 14);
        assert_eq!(value["redis_catch_up_cursor"], "2-16");
        assert!(value["redis_replay_caught_up"].as_bool().unwrap_or(false));
        assert!(value.get("redis_gap_reason").is_none());
        Ok(())
    }

    #[test]
    fn build_findings_marks_broadcaster_log_matches() {
        let mut analysis = report(Vec::new(), vec![scenario("core simulate", 0, 0)]);
        let broadcaster = analysis
            .logs
            .sources
            .iter_mut()
            .find(|source| source.source == "broadcaster");
        assert!(broadcaster.is_some());
        let Some(broadcaster) = broadcaster else {
            return;
        };
        broadcaster.matched_lines = 2;
        broadcaster
            .highlights
            .push("{\"level\":\"WARN\",\"message\":\"stream build failed\"}".to_string());
        analysis.logs.matched_lines = 2;
        analysis.logs.highlights = vec![
            "broadcaster: {\"level\":\"WARN\",\"message\":\"stream build failed\"}".to_string(),
        ];

        let findings = build_findings(&analysis);

        assert!(findings.iter().any(|finding| {
            finding.title == "Broadcaster log warnings or failures were captured"
                && finding
                    .detail
                    .contains("logs/tycho-broadcaster-service.log")
        }));
    }

    #[test]
    fn render_summary_includes_rfq_status() {
        let mut analysis = report(Vec::new(), vec![scenario("core simulate", 0, 0)]);
        analysis.readiness.initial.backends.insert(
            "rfq".to_string(),
            readiness_backend(true, "ready", Some(1), Some(1)),
        );
        analysis
            .readiness
            .initial
            .backends
            .entry("native".to_string())
            .or_insert_with(|| readiness_backend(true, "ready", Some(1), Some(10)))
            .status = "warming_up".to_string();

        let mut final_state = analysis.readiness.initial.clone();
        final_state.status = "ready".to_string();
        final_state
            .backends
            .entry("native".to_string())
            .or_insert_with(|| readiness_backend(true, "ready", Some(1), Some(10)))
            .status = "ready".to_string();
        final_state
            .backends
            .entry("rfq".to_string())
            .or_insert_with(|| readiness_backend(true, "ready", Some(1), Some(1)))
            .status = "rebuilding".to_string();
        analysis.readiness.final_state = Some(final_state);

        let summary = render_summary(&analysis);

        assert!(summary.contains("status=ready native=warming_up"));
        assert!(summary.contains("vm=disabled rfq=ready"));
        assert!(summary.contains("native=ready"));
        assert!(summary.contains("vm=disabled rfq=rebuilding"));
    }

    #[test]
    fn render_summary_includes_redis_replay_state() -> serde_json::Result<()> {
        let mut analysis = report(Vec::new(), vec![scenario("core simulate", 0, 0)]);
        let native = analysis
            .readiness
            .initial
            .backends
            .get_mut("native")
            .unwrap_or_else(|| unreachable!("test report includes native backend"));
        native.subscription = Some(serde_json::from_value(serde_json::json!({
            "connected": true,
            "bootstrap_complete": true,
            "stream_id": "chain-8453-stream-2",
            "snapshot_id": "chain-8453-snapshot-2",
            "redis_replay_boundary": {
                "streamKey": "dsolver:broadcaster:local:8453:events",
                "streamId": "chain-8453-stream-2",
                "snapshotId": "chain-8453-snapshot-2",
                "generation": 2,
                "exclusiveMessageSeq": 14
            },
            "redis_catch_up_cursor": "2-16",
            "redis_replay_caught_up": true,
            "restart_count": 0
        }))?);

        let summary = render_summary(&analysis);

        assert!(
            summary.contains("redis native: boundary=2-14 checkpoint=2-16 caught_up=true gap=none")
        );
        Ok(())
    }

    #[test]
    fn render_summary_includes_broadcaster_log_source_and_highlight() {
        let mut analysis = report(Vec::new(), vec![scenario("core simulate", 0, 0)]);
        let broadcaster = analysis
            .logs
            .sources
            .iter_mut()
            .find(|source| source.source == "broadcaster");
        assert!(broadcaster.is_some());
        let Some(broadcaster) = broadcaster else {
            return;
        };
        broadcaster.matched_lines = 1;
        broadcaster
            .highlights
            .push("{\"level\":\"ERROR\",\"message\":\"snapshot failed\"}".to_string());
        analysis.logs.matched_lines = 1;
        analysis.logs.excerpt_file =
            Some("logs/simulation-reports/1/balanced/1/evidence/log-excerpts.txt".to_string());
        analysis.logs.highlights =
            vec!["broadcaster: {\"level\":\"ERROR\",\"message\":\"snapshot failed\"}".to_string()];

        let summary = render_summary(&analysis);

        assert!(summary
            .contains("- Log file (broadcaster): logs/tycho-broadcaster-service.log matched=1"));
        assert!(summary.contains(
            "- Log excerpts: logs/simulation-reports/1/balanced/1/evidence/log-excerpts.txt"
        ));
        assert!(
            summary.contains("broadcaster: {\"level\":\"ERROR\",\"message\":\"snapshot failed\"}")
        );
    }
}
