use std::borrow::Cow;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Instant;

use axum::{extract::State, Json};
use num_bigint::BigUint;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    metrics::{
        emit_simulate_completion, emit_simulate_result_quality, emit_simulate_timeout, TimeoutKind,
    },
    models::factories::simulate_timeout_meta,
    models::{
        messages::{
            AmountOutRequest, AmountOutResponse, PoolOutcomeKind, PoolSimulationOutcome,
            QuoteFailure, QuoteFailureKind, QuotePartialKind, QuoteResult, QuoteResultQuality,
            QuoteStatus,
        },
        state::AppState,
    },
    services::quotes::{get_amounts_out, QuoteComputation},
};

pub async fn simulate(
    State(state): State<AppState>,
    Json(request): Json<AmountOutRequest>,
) -> Json<QuoteResult> {
    let started_at = Instant::now();
    log_received_request(&request);

    let computation = match run_quote_computation(&state, &request, started_at).await {
        Ok(computation) => computation,
        Err(timeout_result) => return Json(timeout_result),
    };

    let latency_ms = started_at.elapsed().as_millis() as u64;
    let timed_out = log_completion(&request, &computation, latency_ms);

    emit_simulate_completion(computation.meta.status, timed_out);
    emit_simulate_result_quality(computation.meta.result_quality);

    Json(QuoteResult {
        request_id: request.request_id,
        data: computation.responses,
        meta: computation.meta,
    })
}

fn log_received_request(request: &AmountOutRequest) {
    let token_in = canonicalize_token_for_log(request.token_in.as_str());
    let token_out = canonicalize_token_for_log(request.token_out.as_str());

    info!(
        request_id = request.request_id.as_str(),
        auction_id = request.auction_id.as_deref(),
        token_in = token_in.as_ref(),
        token_out = token_out.as_ref(),
        amounts = request.amounts.len(),
        "Received simulate request"
    );
}

async fn run_quote_computation(
    state: &AppState,
    request: &AmountOutRequest,
    started_at: Instant,
) -> Result<QuoteComputation, QuoteResult> {
    let cancel_token = CancellationToken::new();
    let mut cancel_guard = CancelOnDrop::new(cancel_token.clone());
    let request_timeout = state.request_timeout();
    let computation = get_amounts_out(state.clone(), request.clone(), Some(cancel_token));

    let Ok(computation) = tokio::time::timeout(request_timeout, computation).await else {
        return Err(build_request_guard_timeout_result(
            state,
            request,
            started_at,
            request_timeout,
        )
        .await);
    };

    cancel_guard.disarm();
    Ok(computation)
}

async fn build_request_guard_timeout_result(
    state: &AppState,
    request: &AmountOutRequest,
    started_at: Instant,
    request_timeout: std::time::Duration,
) -> QuoteResult {
    let block_number = state.current_block().await;
    let vm_block_number = state.current_vm_block().await;
    let rfq_block_number = state.current_rfq_block().await;
    let total_pools = state.total_pools().await;
    let timeout_ms = request_timeout.as_millis() as u64;

    warn!(
        scope = "handler_timeout",
        request_id = request.request_id.as_str(),
        auction_id = request.auction_id.as_deref(),
        latency_ms = started_at.elapsed().as_millis() as u64,
        timeout_ms,
        "Simulate request timed out at request-level guard"
    );

    emit_simulate_completion(QuoteStatus::Ready, true);
    emit_simulate_result_quality(QuoteResultQuality::RequestLevelFailure);
    emit_simulate_timeout(TimeoutKind::RequestGuard);

    QuoteResult {
        request_id: request.request_id.clone(),
        data: Vec::new(),
        meta: simulate_timeout_meta(
            block_number,
            vm_block_number,
            rfq_block_number,
            Some(total_pools),
            request.auction_id.clone(),
            format!("Simulate request timed out after {}ms", timeout_ms),
        ),
    }
}

fn log_completion(
    request: &AmountOutRequest,
    computation: &QuoteComputation,
    latency_ms: u64,
) -> bool {
    let timed_out = computation
        .meta
        .failures
        .iter()
        .any(|failure| matches!(failure.kind, QuoteFailureKind::Timeout));
    let failure_summary = summarize_failures(&computation.meta.failures);
    let pool_outcome_summary = summarize_pool_outcomes(&computation.meta.pool_results);
    let top_response = TopResponseSummary::from_best(&computation.responses);

    let scope = if timed_out {
        "handler_timeout"
    } else {
        "handler_complete"
    };
    let message = if timed_out {
        "Simulate computation completed with timeout"
    } else {
        "Simulate computation completed"
    };
    let event = CompletionEvent {
        timed_out,
        scope,
        message,
        request,
        computation,
        latency_ms,
        failure_summary: &failure_summary,
        pool_outcome_summary: &pool_outcome_summary,
        top_response: &top_response,
    };

    emit_completion_event(event);

    timed_out
}

struct CompletionEvent<'a> {
    timed_out: bool,
    scope: &'a str,
    message: &'a str,
    request: &'a AmountOutRequest,
    computation: &'a QuoteComputation,
    latency_ms: u64,
    failure_summary: &'a FailureSummary,
    pool_outcome_summary: &'a PoolOutcomeSummary,
    top_response: &'a TopResponseSummary<'a>,
}

fn emit_completion_event(event: CompletionEvent<'_>) {
    // Canonicalize address tokens so CloudWatch queries compare one stable representation.
    let token_in = canonicalize_token_for_log(event.request.token_in.as_str());
    let token_out = canonicalize_token_for_log(event.request.token_out.as_str());

    if event.timed_out {
        tracing::event!(
            tracing::Level::WARN,
            scope = event.scope,
            request_id = event.request.request_id.as_str(),
            auction_id = event.request.auction_id.as_deref(),
            latency_ms = event.latency_ms,
            quote_status = quote_status_label(event.computation.meta.status),
            quote_result_quality = quote_result_quality_label(event.computation.meta.result_quality),
            partial_kind = event.computation.meta.partial_kind.map(quote_partial_kind_label),
            vm_unavailable = event.computation.meta.vm_unavailable,
            responses = event.computation.responses.len(),
            failures = event.computation.meta.failures.len(),
            pool_results = event.computation.meta.pool_results.len(),
            scheduled_native_pools = event.computation.metrics.scheduled_native_pools,
            scheduled_vm_pools = event.computation.metrics.scheduled_vm_pools,
            simulation_runs = event.computation.metrics.scheduled_native_pools + event.computation.metrics.scheduled_vm_pools,
            skipped_vm_unavailable = event.computation.metrics.skipped_vm_unavailable,
            skipped_native_concurrency = event.computation.metrics.skipped_native_concurrency,
            skipped_vm_concurrency = event.computation.metrics.skipped_vm_concurrency,
            skipped_native_deadline = event.computation.metrics.skipped_native_deadline,
            skipped_vm_deadline = event.computation.metrics.skipped_vm_deadline,
            vm_completed_pools = event.computation.metrics.vm_completed_pools,
            vm_median_first_gas = event.computation.metrics.vm_median_first_gas,
            vm_low_first_gas_count = event.computation.metrics.vm_low_first_gas_count,
            vm_low_first_gas_ratio = event.computation.metrics.vm_low_first_gas_ratio,
            vm_low_first_gas_samples = ?event.computation.metrics.vm_low_first_gas_samples,
            token_in = token_in.as_ref(),
            token_out = token_out.as_ref(),
            amounts = event.request.amounts.len(),
            top_pool = event.top_response.pool,
            top_pool_name = event.top_response.pool_name,
            top_pool_address = event.top_response.pool_address,
            top_amount_out = event.top_response.amount_out,
            top_gas_used = event.top_response.gas_used,
            failure_kinds = ?event.failure_summary.kind_counts,
            failure_protocols = ?event.failure_summary.protocol_counts,
            failure_pool_kinds = ?event.failure_summary.pool_kind_counts,
            failure_samples = ?event.failure_summary.samples,
            outcome_kinds = ?event.pool_outcome_summary.kind_counts,
            outcome_protocols = ?event.pool_outcome_summary.protocol_counts,
            outcome_samples = ?event.pool_outcome_summary.samples,
            "{}",
            event.message
        );
    } else {
        tracing::event!(
            tracing::Level::INFO,
            scope = event.scope,
            request_id = event.request.request_id.as_str(),
            auction_id = event.request.auction_id.as_deref(),
            latency_ms = event.latency_ms,
            quote_status = quote_status_label(event.computation.meta.status),
            quote_result_quality = quote_result_quality_label(event.computation.meta.result_quality),
            partial_kind = event.computation.meta.partial_kind.map(quote_partial_kind_label),
            vm_unavailable = event.computation.meta.vm_unavailable,
            responses = event.computation.responses.len(),
            failures = event.computation.meta.failures.len(),
            pool_results = event.computation.meta.pool_results.len(),
            scheduled_native_pools = event.computation.metrics.scheduled_native_pools,
            scheduled_vm_pools = event.computation.metrics.scheduled_vm_pools,
            simulation_runs = event.computation.metrics.scheduled_native_pools + event.computation.metrics.scheduled_vm_pools,
            skipped_vm_unavailable = event.computation.metrics.skipped_vm_unavailable,
            skipped_native_concurrency = event.computation.metrics.skipped_native_concurrency,
            skipped_vm_concurrency = event.computation.metrics.skipped_vm_concurrency,
            skipped_native_deadline = event.computation.metrics.skipped_native_deadline,
            skipped_vm_deadline = event.computation.metrics.skipped_vm_deadline,
            vm_completed_pools = event.computation.metrics.vm_completed_pools,
            vm_median_first_gas = event.computation.metrics.vm_median_first_gas,
            vm_low_first_gas_count = event.computation.metrics.vm_low_first_gas_count,
            vm_low_first_gas_ratio = event.computation.metrics.vm_low_first_gas_ratio,
            vm_low_first_gas_samples = ?event.computation.metrics.vm_low_first_gas_samples,
            token_in = token_in.as_ref(),
            token_out = token_out.as_ref(),
            amounts = event.request.amounts.len(),
            top_pool = event.top_response.pool,
            top_pool_name = event.top_response.pool_name,
            top_pool_address = event.top_response.pool_address,
            top_amount_out = event.top_response.amount_out,
            top_gas_used = event.top_response.gas_used,
            failure_kinds = ?event.failure_summary.kind_counts,
            failure_protocols = ?event.failure_summary.protocol_counts,
            failure_pool_kinds = ?event.failure_summary.pool_kind_counts,
            failure_samples = ?event.failure_summary.samples,
            outcome_kinds = ?event.pool_outcome_summary.kind_counts,
            outcome_protocols = ?event.pool_outcome_summary.protocol_counts,
            outcome_samples = ?event.pool_outcome_summary.samples,
            "{}",
            event.message
        );
    }
}

fn canonicalize_token_for_log(token: &str) -> Cow<'_, str> {
    if is_hex_address(token) {
        Cow::Owned(token.to_ascii_lowercase())
    } else {
        Cow::Borrowed(token)
    }
}

fn is_hex_address(token: &str) -> bool {
    let bytes = token.as_bytes();
    bytes.len() == 42
        && bytes[0] == b'0'
        && matches!(bytes[1], b'x' | b'X')
        && bytes[2..].iter().all(u8::is_ascii_hexdigit)
}

#[derive(Debug, Default)]
struct TopResponseSummary<'a> {
    pool: Option<&'a str>,
    pool_name: Option<&'a str>,
    pool_address: Option<&'a str>,
    amount_out: Option<&'a str>,
    gas_used: Option<u64>,
}

impl<'a> TopResponseSummary<'a> {
    fn from_best(responses: &'a [AmountOutResponse]) -> Self {
        let Some(best_response) = best_response(responses) else {
            return Self::default();
        };

        Self::from(Some(best_response))
    }
}

impl<'a> From<Option<&'a crate::models::messages::AmountOutResponse>> for TopResponseSummary<'a> {
    fn from(response: Option<&'a crate::models::messages::AmountOutResponse>) -> Self {
        let Some(response) = response else {
            return Self::default();
        };

        Self {
            pool: Some(response.pool.as_str()),
            pool_name: Some(response.pool_name.as_str()),
            pool_address: Some(response.pool_address.as_str()),
            amount_out: response.amounts_out.first().map(String::as_str),
            gas_used: response.gas_used.first().copied(),
        }
    }
}

fn best_response(responses: &[AmountOutResponse]) -> Option<&AmountOutResponse> {
    let mut best_response = None;
    let mut best_amount = BigUint::default();

    for response in responses {
        let amount = first_amount_out_value(response);
        if best_response.is_none() || amount > best_amount {
            best_amount = amount;
            best_response = Some(response);
        }
    }

    best_response
}

fn first_amount_out_value(response: &AmountOutResponse) -> BigUint {
    response
        .amounts_out
        .first()
        .and_then(|amount| BigUint::from_str(amount).ok())
        .unwrap_or_default()
}

#[derive(Debug, Default)]
struct FailureSummary {
    kind_counts: Vec<(String, usize)>,
    protocol_counts: Vec<(String, usize)>,
    pool_kind_counts: Vec<(String, usize)>,
    samples: Vec<String>,
}

// Aggregate failure details for a single summary log entry per request.
fn summarize_failures(failures: &[QuoteFailure]) -> FailureSummary {
    if failures.is_empty() {
        return FailureSummary::default();
    }

    let mut kind_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut protocol_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut pool_kind_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut samples = Vec::new();

    for failure in failures {
        let kind_label = failure.kind.label();
        *kind_counts.entry(kind_label.to_string()).or_insert(0) += 1;

        if let Some(protocol) = failure.protocol.as_deref() {
            *protocol_counts.entry(protocol.to_string()).or_insert(0) += 1;
            let pool_kind = if protocol.starts_with("vm:") {
                "vm"
            } else {
                "native"
            };
            *pool_kind_counts.entry(pool_kind.to_string()).or_insert(0) += 1;
        } else {
            *pool_kind_counts.entry("unknown".to_string()).or_insert(0) += 1;
        }

        if samples.len() < 5 {
            let sample = format!(
                "kind={} protocol={:?} pool={:?} pool_name={:?} pool_address={:?} message={}",
                kind_label,
                failure.protocol,
                failure.pool,
                failure.pool_name,
                failure.pool_address,
                failure.message
            );
            samples.push(sample);
        }
    }

    FailureSummary {
        kind_counts: kind_counts.into_iter().collect(),
        protocol_counts: protocol_counts.into_iter().collect(),
        pool_kind_counts: pool_kind_counts.into_iter().collect(),
        samples,
    }
}

#[derive(Debug, Default)]
struct PoolOutcomeSummary {
    kind_counts: Vec<(String, usize)>,
    protocol_counts: Vec<(String, usize)>,
    samples: Vec<String>,
}

fn summarize_pool_outcomes(outcomes: &[PoolSimulationOutcome]) -> PoolOutcomeSummary {
    if outcomes.is_empty() {
        return PoolOutcomeSummary::default();
    }

    let mut kind_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut protocol_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut samples = Vec::new();

    for outcome in outcomes {
        *kind_counts
            .entry(pool_outcome_kind_label(outcome.outcome).to_string())
            .or_insert(0) += 1;
        *protocol_counts.entry(outcome.protocol.clone()).or_insert(0) += 1;

        if samples.len() < 5 {
            samples.push(format!(
                "outcome={:?} protocol={} pool={} pool_name={} reported_steps={} expected_steps={} reason={:?}",
                outcome.outcome,
                outcome.protocol,
                outcome.pool,
                outcome.pool_name,
                outcome.reported_steps,
                outcome.expected_steps,
                outcome.reason
            ));
        }
    }

    PoolOutcomeSummary {
        kind_counts: kind_counts.into_iter().collect(),
        protocol_counts: protocol_counts.into_iter().collect(),
        samples,
    }
}

fn pool_outcome_kind_label(kind: PoolOutcomeKind) -> &'static str {
    match kind {
        PoolOutcomeKind::PartialOutput => "partial_output",
        PoolOutcomeKind::ZeroOutput => "zero_output",
        PoolOutcomeKind::SkippedConcurrency => "skipped_concurrency",
        PoolOutcomeKind::SkippedDeadline => "skipped_deadline",
        PoolOutcomeKind::TimedOut => "timed_out",
        PoolOutcomeKind::SimulatorError => "simulator_error",
        PoolOutcomeKind::InternalError => "internal_error",
    }
}

fn serialized_enum_label<T: Serialize>(value: T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_string())
}

fn quote_status_label(status: QuoteStatus) -> String {
    serialized_enum_label(status)
}

fn quote_result_quality_label(result_quality: QuoteResultQuality) -> String {
    serialized_enum_label(result_quality)
}

fn quote_partial_kind_label(partial_kind: QuotePartialKind) -> String {
    serialized_enum_label(partial_kind)
}

struct CancelOnDrop {
    token: CancellationToken,
    armed: bool,
}

impl CancelOnDrop {
    fn new(token: CancellationToken) -> Self {
        Self { token, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.token.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::messages::{
        AmountOutResponse, PoolOutcomeKind, QuoteFailure, QuoteFailureKind,
    };

    fn make_failure(kind: QuoteFailureKind, protocol: Option<&str>) -> QuoteFailure {
        QuoteFailure {
            kind,
            message: format!("{} error", kind.label()),
            pool: Some("pool_id".to_string()),
            pool_name: Some("pool_name".to_string()),
            pool_address: Some("0xabc".to_string()),
            protocol: protocol.map(String::from),
        }
    }

    #[test]
    fn empty_failures_returns_default() {
        let summary = summarize_failures(&[]);
        assert!(summary.kind_counts.is_empty());
        assert!(summary.protocol_counts.is_empty());
        assert!(summary.pool_kind_counts.is_empty());
        assert!(summary.samples.is_empty());
    }

    #[test]
    fn single_failure_without_protocol() {
        let failures = vec![make_failure(QuoteFailureKind::Timeout, None)];
        let summary = summarize_failures(&failures);

        assert_eq!(summary.kind_counts, vec![("timeout".to_string(), 1)]);
        assert!(summary.protocol_counts.is_empty());
        assert_eq!(summary.pool_kind_counts, vec![("unknown".to_string(), 1)]);
        assert_eq!(summary.samples.len(), 1);
    }

    #[test]
    fn mixed_protocols_counted_correctly() {
        let failures = vec![
            make_failure(QuoteFailureKind::Simulator, Some("vm:uniswap_v2")),
            make_failure(QuoteFailureKind::Simulator, Some("vm:uniswap_v2")),
            make_failure(QuoteFailureKind::Timeout, Some("balancer_v2")),
            make_failure(QuoteFailureKind::Overflow, Some("balancer_v2")),
        ];
        let summary = summarize_failures(&failures);

        assert_eq!(
            summary.kind_counts,
            vec![
                ("overflow".to_string(), 1),
                ("simulator".to_string(), 2),
                ("timeout".to_string(), 1),
            ]
        );
        assert_eq!(
            summary.protocol_counts,
            vec![
                ("balancer_v2".to_string(), 2),
                ("vm:uniswap_v2".to_string(), 2),
            ]
        );
        assert_eq!(
            summary.pool_kind_counts,
            vec![("native".to_string(), 2), ("vm".to_string(), 2)]
        );
        assert_eq!(summary.samples.len(), 4);
    }

    #[test]
    fn samples_capped_at_five() {
        let failures: Vec<_> = (0..8)
            .map(|_| make_failure(QuoteFailureKind::Simulator, Some("uniswap_v3")))
            .collect();
        let summary = summarize_failures(&failures);

        assert_eq!(summary.kind_counts, vec![("simulator".to_string(), 8)]);
        assert_eq!(summary.samples.len(), 5);
    }

    #[test]
    fn counts_are_alphabetically_sorted() {
        let failures = vec![
            make_failure(QuoteFailureKind::Overflow, Some("vm:curve")),
            make_failure(QuoteFailureKind::Simulator, Some("aave_v3")),
            make_failure(QuoteFailureKind::Timeout, Some("balancer_v2")),
        ];
        let summary = summarize_failures(&failures);

        let kind_keys: Vec<_> = summary.kind_counts.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(kind_keys, vec!["overflow", "simulator", "timeout"]);

        let protocol_keys: Vec<_> = summary
            .protocol_counts
            .iter()
            .map(|(k, _)| k.clone())
            .collect();
        assert_eq!(protocol_keys, vec!["aave_v3", "balancer_v2", "vm:curve"]);
    }

    fn make_response(pool: &str, amount_out: &str, gas_used: u64) -> AmountOutResponse {
        AmountOutResponse {
            pool: pool.to_string(),
            pool_name: format!("{pool} name"),
            pool_address: format!("0x{gas_used:040x}"),
            amounts_out: vec![amount_out.to_string(), "0".to_string()],
            gas_used: vec![gas_used, 0],
            block_number: 1,
        }
    }

    #[test]
    fn top_response_summary_uses_highest_output_not_first_response() {
        let responses = vec![
            make_response("pool-a", "100", 1),
            make_response("pool-z", "250", 2),
        ];

        let summary = TopResponseSummary::from_best(&responses);

        assert_eq!(summary.pool, Some("pool-z"));
        assert_eq!(summary.pool_name, Some("pool-z name"));
        assert_eq!(
            summary.pool_address,
            Some("0x0000000000000000000000000000000000000002")
        );
        assert_eq!(summary.amount_out, Some("250"));
        assert_eq!(summary.gas_used, Some(2));
    }

    #[test]
    fn canonicalize_token_for_log_lowercases_hex_addresses() {
        let token = canonicalize_token_for_log("0xA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48");

        assert_eq!(token.as_ref(), "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");
    }

    #[test]
    fn canonicalize_token_for_log_leaves_non_address_tokens_unchanged() {
        let token = canonicalize_token_for_log("WETH");

        assert_eq!(token.as_ref(), "WETH");
    }

    #[test]
    fn request_guard_timeout_meta_uses_request_level_failure_quality() {
        let meta = simulate_timeout_meta(
            12,
            Some(11),
            Some(12),
            Some(42),
            Some("auction-1".to_string()),
            "Simulate request timed out after 1500ms".to_string(),
        );
        assert!(matches!(meta.status, QuoteStatus::Ready));
        assert_eq!(meta.result_quality, QuoteResultQuality::RequestLevelFailure);
        assert!(meta.partial_kind.is_none());
        assert_eq!(meta.block_number, 12);
        assert_eq!(meta.vm_block_number, Some(11));
        assert_eq!(meta.total_pools, Some(42));
        assert_eq!(meta.auction_id.as_deref(), Some("auction-1"));
        assert!(meta.pool_results.is_empty());
        assert!(!meta.vm_unavailable);
        assert_eq!(meta.failures.len(), 1);
        assert!(matches!(meta.failures[0].kind, QuoteFailureKind::Timeout));
    }

    #[test]
    fn summarize_pool_outcomes_groups_by_kind_and_protocol() {
        let outcomes = vec![
            PoolSimulationOutcome {
                pool: "pool-1".to_string(),
                pool_name: "pool one".to_string(),
                pool_address: "0x1".to_string(),
                protocol: "vm:curve".to_string(),
                outcome: PoolOutcomeKind::SkippedDeadline,
                reported_steps: 0,
                expected_steps: 2,
                reason: Some("deadline reached".to_string()),
            },
            PoolSimulationOutcome {
                pool: "pool-2".to_string(),
                pool_name: "pool two".to_string(),
                pool_address: "0x2".to_string(),
                protocol: "vm:curve".to_string(),
                outcome: PoolOutcomeKind::SkippedDeadline,
                reported_steps: 0,
                expected_steps: 2,
                reason: None,
            },
            PoolSimulationOutcome {
                pool: "pool-3".to_string(),
                pool_name: "pool three".to_string(),
                pool_address: "0x3".to_string(),
                protocol: "uniswap_v3".to_string(),
                outcome: PoolOutcomeKind::PartialOutput,
                reported_steps: 1,
                expected_steps: 2,
                reason: Some("partial amount coverage".to_string()),
            },
        ];

        let summary = summarize_pool_outcomes(&outcomes);
        assert_eq!(
            summary.kind_counts,
            vec![
                ("partial_output".to_string(), 1),
                ("skipped_deadline".to_string(), 2),
            ]
        );
        assert_eq!(
            summary.protocol_counts,
            vec![("uniswap_v3".to_string(), 1), ("vm:curve".to_string(), 2)]
        );
        assert_eq!(summary.samples.len(), 3);
    }
}
