use std::collections::BTreeSet;

use axum::http::StatusCode;
use num_bigint::BigUint;
use num_traits::Zero;
use tracing::{debug, info, warn};

use crate::models::messages::{
    ResimulationDebug, RouteDebug, RouteEncodeRequest, RouteEncodeResponse, SwapKind,
};
use crate::models::state::AppState;

use super::model::ResimulatedRouteInternal;
use super::wire::format_address;
use super::EncodeComputation;
use super::EncodeErrorKind;

const NATIVE_TOKEN_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EncodeFailureStage {
    Validation,
    Normalization,
    Readiness,
    Resimulation,
    MinAmountOutGuard,
    Encoding,
    InteractionBuild,
    Internal,
    HandlerTimeout,
}

impl EncodeFailureStage {
    const fn label(self) -> &'static str {
        match self {
            Self::Validation => "validation",
            Self::Normalization => "normalization",
            Self::Readiness => "readiness",
            Self::Resimulation => "resimulation",
            Self::MinAmountOutGuard => "min_amount_out_guard",
            Self::Encoding => "encoding",
            Self::InteractionBuild => "interaction_build",
            Self::Internal => "internal",
            Self::HandlerTimeout => "handler_timeout",
        }
    }
}

struct EncodeRequestSummary {
    request_id: Option<String>,
    chain_id: u64,
    swap_kind: &'static str,
    segments: usize,
    hops: usize,
    swaps: usize,
    route_protocols: String,
    route_uses_vm: bool,
    token_in: String,
    token_out: String,
    amount_in: String,
    min_amount_out: String,
    is_native_input: bool,
}

pub(super) fn compute_expected_total(resimulated: &ResimulatedRouteInternal) -> BigUint {
    let mut expected_total = BigUint::zero();
    for segment in &resimulated.segments {
        expected_total += segment.expected_amount_out.clone();
    }
    expected_total
}

pub(super) fn log_resimulation_amounts(
    request_id: Option<&str>,
    resimulated: &ResimulatedRouteInternal,
) {
    // Keep the hop-by-hop trace available for debugging without making it the primary signal.
    for (segment_index, segment) in resimulated.segments.iter().enumerate() {
        debug!(
            request_id,
            segment_index,
            share_bps = segment.share_bps,
            amount_in = %segment.amount_in,
            expected_amount_out = %segment.expected_amount_out,
            "Resimulated segment"
        );

        for (hop_index, hop) in segment.hops.iter().enumerate() {
            debug!(
                request_id,
                segment_index,
                hop_index,
                token_in = %format_address(&hop.token_in),
                token_out = %format_address(&hop.token_out),
                amount_in = %hop.amount_in,
                expected_amount_out = %hop.expected_amount_out,
                "Resimulated hop"
            );

            for (swap_index, swap) in hop.swaps.iter().enumerate() {
                debug!(
                    request_id,
                    segment_index,
                    hop_index,
                    swap_index,
                    split_bps = swap.split_bps,
                    pool_id = %swap.pool.component_id,
                    amount_in = %swap.amount_in,
                    expected_amount_out = %swap.expected_amount_out,
                    "Resimulated swap"
                );
            }
        }
    }
}

pub(crate) fn log_success(
    request: &RouteEncodeRequest,
    computation: &EncodeComputation,
    latency_ms: u64,
) {
    let summary = summarize_request(request);
    info!(
        scope = "handler_complete",
        request_id = summary.request_id.as_deref(),
        latency_ms,
        status_code = StatusCode::OK.as_u16(),
        chain_id = summary.chain_id,
        swap_kind = summary.swap_kind,
        segments = summary.segments,
        hops = summary.hops,
        swaps = summary.swaps,
        route_protocols = summary.route_protocols.as_str(),
        route_uses_vm = summary.route_uses_vm,
        token_in = summary.token_in.as_str(),
        token_out = summary.token_out.as_str(),
        amount_in = summary.amount_in.as_str(),
        min_amount_out = summary.min_amount_out.as_str(),
        is_native_input = summary.is_native_input,
        reset_approval = computation.reset_approval,
        expected_amount_out = computation.expected_amount_out.as_str(),
        amount_out_delta = computation.amount_out_delta.as_str(),
        interactions = computation.response.interactions.len(),
        block_number = response_block_number(&computation.response),
        "Encode request completed"
    );
}

pub(crate) fn log_received(request: &RouteEncodeRequest) {
    let summary = summarize_request(request);
    info!(
        request_id = summary.request_id.as_deref(),
        chain_id = summary.chain_id,
        swap_kind = summary.swap_kind,
        segments = summary.segments,
        hops = summary.hops,
        swaps = summary.swaps,
        route_protocols = summary.route_protocols.as_str(),
        route_uses_vm = summary.route_uses_vm,
        "Received encode request"
    );
}

pub(crate) fn log_failure(
    request: &RouteEncodeRequest,
    status_code: StatusCode,
    kind: EncodeErrorKind,
    message: &str,
    latency_ms: u64,
) {
    let summary = summarize_request(request);
    let stage = classify_failure_stage(kind, message);
    warn!(
        scope = "handler_complete",
        request_id = summary.request_id.as_deref(),
        latency_ms,
        status_code = status_code.as_u16(),
        encode_error_kind = encode_error_kind_label(kind),
        failure_stage = stage.label(),
        error = message,
        chain_id = summary.chain_id,
        swap_kind = summary.swap_kind,
        segments = summary.segments,
        hops = summary.hops,
        swaps = summary.swaps,
        route_protocols = summary.route_protocols.as_str(),
        route_uses_vm = summary.route_uses_vm,
        token_in = summary.token_in.as_str(),
        token_out = summary.token_out.as_str(),
        amount_in = summary.amount_in.as_str(),
        min_amount_out = summary.min_amount_out.as_str(),
        is_native_input = summary.is_native_input,
        "Encode request failed"
    );
}

pub(crate) fn log_handler_timeout(request: &RouteEncodeRequest, timeout_ms: u64, latency_ms: u64) {
    let summary = summarize_request(request);
    let error = format!("Encode request timed out after {timeout_ms}ms");
    warn!(
        scope = "handler_timeout",
        request_id = summary.request_id.as_deref(),
        latency_ms,
        timeout_ms,
        status_code = StatusCode::REQUEST_TIMEOUT.as_u16(),
        encode_error_kind = "timeout",
        failure_stage = EncodeFailureStage::HandlerTimeout.label(),
        error,
        chain_id = summary.chain_id,
        swap_kind = summary.swap_kind,
        segments = summary.segments,
        hops = summary.hops,
        swaps = summary.swaps,
        route_protocols = summary.route_protocols.as_str(),
        route_uses_vm = summary.route_uses_vm,
        token_in = summary.token_in.as_str(),
        token_out = summary.token_out.as_str(),
        amount_in = summary.amount_in.as_str(),
        min_amount_out = summary.min_amount_out.as_str(),
        is_native_input = summary.is_native_input,
        "Encode request timed out at request-level guard"
    );
}

pub(super) async fn build_debug(
    state: &AppState,
    request: &RouteEncodeRequest,
) -> Option<RouteDebug> {
    let request_id = request.request_id.clone()?;
    let block_number = state.current_block().await;
    Some(RouteDebug {
        request_id: Some(request_id),
        resimulation: Some(ResimulationDebug {
            block_number: Some(block_number),
            tycho_state_tag: None,
        }),
    })
}

fn summarize_request(request: &RouteEncodeRequest) -> EncodeRequestSummary {
    let hops = request
        .segments
        .iter()
        .map(|segment| segment.hops.len())
        .sum::<usize>();
    let swaps = request
        .segments
        .iter()
        .flat_map(|segment| segment.hops.iter())
        .map(|hop| hop.swaps.len())
        .sum::<usize>();
    let protocols = request
        .segments
        .iter()
        .flat_map(|segment| segment.hops.iter())
        .flat_map(|hop| hop.swaps.iter())
        .map(|swap| swap.pool.protocol.trim().to_ascii_lowercase())
        .collect::<BTreeSet<_>>();

    EncodeRequestSummary {
        request_id: request.request_id.clone(),
        chain_id: request.chain_id,
        swap_kind: swap_kind_label(request.swap_kind),
        segments: request.segments.len(),
        hops,
        swaps,
        route_protocols: protocols.into_iter().collect::<Vec<_>>().join(","),
        route_uses_vm: request
            .segments
            .iter()
            .flat_map(|segment| segment.hops.iter())
            .flat_map(|hop| hop.swaps.iter())
            .any(|swap| swap.pool.protocol.trim().starts_with("vm:")),
        token_in: request.token_in.clone(),
        token_out: request.token_out.clone(),
        amount_in: request.amount_in.clone(),
        min_amount_out: request.min_amount_out.clone(),
        is_native_input: is_native_address(&request.token_in),
    }
}

fn response_block_number(response: &RouteEncodeResponse) -> Option<u64> {
    response
        .debug
        .as_ref()
        .and_then(|debug| debug.resimulation.as_ref())
        .and_then(|resimulation| resimulation.block_number)
}

fn classify_failure_stage(kind: EncodeErrorKind, message: &str) -> EncodeFailureStage {
    let lowered = message.to_ascii_lowercase();
    match kind {
        EncodeErrorKind::InvalidRequest => {
            if lowered.contains("segment[")
                || lowered.contains("segment ")
                || lowered.contains("hop ")
                || lowered.contains("hop[")
                || lowered.contains("sharebps")
                || lowered.contains("splitbps")
                || lowered.contains("continuity")
                || lowered.contains("native tokenin/tokenout")
                || lowered.contains("erc4626")
            {
                EncodeFailureStage::Normalization
            } else {
                EncodeFailureStage::Validation
            }
        }
        EncodeErrorKind::Unavailable => EncodeFailureStage::Readiness,
        EncodeErrorKind::NotFound | EncodeErrorKind::Simulation => {
            if lowered.contains("below minamountout") {
                EncodeFailureStage::MinAmountOutGuard
            } else {
                EncodeFailureStage::Resimulation
            }
        }
        EncodeErrorKind::Encoding => {
            if lowered.contains("approve") || lowered.contains("interaction") {
                EncodeFailureStage::InteractionBuild
            } else {
                EncodeFailureStage::Encoding
            }
        }
        EncodeErrorKind::Internal => {
            if lowered.contains("approve") || lowered.contains("interaction") {
                EncodeFailureStage::InteractionBuild
            } else {
                EncodeFailureStage::Internal
            }
        }
    }
}

fn encode_error_kind_label(kind: EncodeErrorKind) -> &'static str {
    match kind {
        EncodeErrorKind::InvalidRequest => "invalid_request",
        EncodeErrorKind::NotFound => "not_found",
        EncodeErrorKind::Unavailable => "unavailable",
        EncodeErrorKind::Simulation => "simulation",
        EncodeErrorKind::Encoding => "encoding",
        EncodeErrorKind::Internal => "internal",
    }
}

const fn swap_kind_label(kind: SwapKind) -> &'static str {
    match kind {
        SwapKind::SimpleSwap => "simple_swap",
        SwapKind::MultiSwap => "multi_swap",
        SwapKind::MegaSwap => "mega_swap",
    }
}

fn is_native_address(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case(NATIVE_TOKEN_ADDRESS)
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    reason = "debug payload tests assert presence on deterministic fixtures"
)]
mod tests {
    use std::collections::HashMap;

    use tycho_simulation::protocol::models::Update;

    use super::*;
    use crate::models::messages::{PoolRef, PoolSwapDraft, SegmentDraft};
    use crate::services::encode::fixtures::{
        test_app_state, test_state_stores, token_store_with_tokens, TestAppStateConfig,
    };

    fn test_request(request_id: Option<&str>) -> RouteEncodeRequest {
        RouteEncodeRequest {
            chain_id: 1,
            token_in: "0x0000000000000000000000000000000000000001".to_string(),
            token_out: "0x0000000000000000000000000000000000000002".to_string(),
            amount_in: "10".to_string(),
            min_amount_out: "8".to_string(),
            settlement_address: "0x0000000000000000000000000000000000000003".to_string(),
            tycho_router_address: "0x0000000000000000000000000000000000000004".to_string(),
            swap_kind: SwapKind::SimpleSwap,
            segments: Vec::new(),
            request_id: request_id.map(str::to_string),
        }
    }

    fn route_request(protocols: &[&str]) -> RouteEncodeRequest {
        RouteEncodeRequest {
            chain_id: 1,
            token_in: "0x0000000000000000000000000000000000000000".to_string(),
            token_out: "0x0000000000000000000000000000000000000002".to_string(),
            amount_in: "10".to_string(),
            min_amount_out: "8".to_string(),
            settlement_address: "0x0000000000000000000000000000000000000003".to_string(),
            tycho_router_address: "0x0000000000000000000000000000000000000004".to_string(),
            swap_kind: SwapKind::MegaSwap,
            segments: vec![
                SegmentDraft {
                    kind: SwapKind::MultiSwap,
                    share_bps: 6_000,
                    hops: vec![
                        crate::models::messages::HopDraft {
                            token_in: "0x0000000000000000000000000000000000000000".to_string(),
                            token_out: "0x0000000000000000000000000000000000000005".to_string(),
                            swaps: vec![PoolSwapDraft {
                                pool: PoolRef {
                                    protocol: protocols[0].to_string(),
                                    component_id: "pool-a".to_string(),
                                    pool_address: None,
                                },
                                token_in: "0x0000000000000000000000000000000000000000".to_string(),
                                token_out: "0x0000000000000000000000000000000000000005".to_string(),
                                split_bps: 0,
                            }],
                        },
                        crate::models::messages::HopDraft {
                            token_in: "0x0000000000000000000000000000000000000005".to_string(),
                            token_out: "0x0000000000000000000000000000000000000002".to_string(),
                            swaps: vec![PoolSwapDraft {
                                pool: PoolRef {
                                    protocol: protocols[1].to_string(),
                                    component_id: "pool-b".to_string(),
                                    pool_address: None,
                                },
                                token_in: "0x0000000000000000000000000000000000000005".to_string(),
                                token_out: "0x0000000000000000000000000000000000000002".to_string(),
                                split_bps: 0,
                            }],
                        },
                    ],
                },
                SegmentDraft {
                    kind: SwapKind::SimpleSwap,
                    share_bps: 0,
                    hops: vec![crate::models::messages::HopDraft {
                        token_in: "0x0000000000000000000000000000000000000000".to_string(),
                        token_out: "0x0000000000000000000000000000000000000002".to_string(),
                        swaps: vec![PoolSwapDraft {
                            pool: PoolRef {
                                protocol: protocols[2].to_string(),
                                component_id: "pool-c".to_string(),
                                pool_address: None,
                            },
                            token_in: "0x0000000000000000000000000000000000000000".to_string(),
                            token_out: "0x0000000000000000000000000000000000000002".to_string(),
                            split_bps: 0,
                        }],
                    }],
                },
            ],
            request_id: Some("req-logging".to_string()),
        }
    }

    #[tokio::test]
    async fn build_debug_omits_when_request_id_missing() {
        let tokens_store = token_store_with_tokens(Vec::new());
        let (native_state_store, vm_state_store, rfq_state_store) =
            test_state_stores(&tokens_store);

        native_state_store
            .apply_update(Update::new(42, HashMap::new(), HashMap::new()))
            .await;

        let state = test_app_state(
            tokens_store,
            native_state_store,
            vm_state_store,
            rfq_state_store,
            TestAppStateConfig::default(),
        );

        let request = test_request(None);

        assert!(build_debug(&state, &request).await.is_none());
    }

    #[tokio::test]
    async fn build_debug_includes_block_when_request_id_present() {
        let tokens_store = token_store_with_tokens(Vec::new());
        let (native_state_store, vm_state_store, rfq_state_store) =
            test_state_stores(&tokens_store);

        native_state_store
            .apply_update(Update::new(42, HashMap::new(), HashMap::new()))
            .await;

        let state = test_app_state(
            tokens_store,
            native_state_store,
            vm_state_store,
            rfq_state_store,
            TestAppStateConfig::default(),
        );

        let request = test_request(Some("req-1"));

        let debug = build_debug(&state, &request).await.expect("debug present");
        assert_eq!(debug.request_id.as_deref(), Some("req-1"));
        assert_eq!(
            debug
                .resimulation
                .as_ref()
                .and_then(|resim| resim.block_number),
            Some(42)
        );
    }

    #[test]
    fn summarize_request_collects_route_shape_and_protocols() {
        let request = route_request(&["curve_v2", "vm:maverick_v2", "uniswap_v3"]);

        let summary = summarize_request(&request);

        assert_eq!(summary.chain_id, 1);
        assert_eq!(summary.swap_kind, "mega_swap");
        assert_eq!(summary.segments, 2);
        assert_eq!(summary.hops, 3);
        assert_eq!(summary.swaps, 3);
        assert_eq!(
            summary.route_protocols,
            "curve_v2,uniswap_v3,vm:maverick_v2"
        );
        assert!(summary.route_uses_vm);
        assert!(summary.is_native_input);
    }

    #[test]
    fn classify_failure_stage_keeps_min_amount_out_guard_separate() {
        let stage = classify_failure_stage(
            EncodeErrorKind::Simulation,
            "Route expectedAmountOut below minAmountOut",
        );

        assert_eq!(stage, EncodeFailureStage::MinAmountOutGuard);
    }

    #[test]
    fn classify_failure_stage_splits_validation_from_normalization() {
        let validation = classify_failure_stage(
            EncodeErrorKind::InvalidRequest,
            "Unsupported chainId: 8453 (this instance serves chain 1)",
        );
        let normalization = classify_failure_stage(
            EncodeErrorKind::InvalidRequest,
            "segment[0].hops must not be empty",
        );
        let readiness = classify_failure_stage(
            EncodeErrorKind::Unavailable,
            "Encode unavailable: native state stale",
        );

        assert_eq!(validation, EncodeFailureStage::Validation);
        assert_eq!(normalization, EncodeFailureStage::Normalization);
        assert_eq!(readiness, EncodeFailureStage::Readiness);
    }
}
