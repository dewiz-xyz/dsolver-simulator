use std::collections::{HashMap, HashSet};

use tycho_simulation::tycho_common::{models::Chain, Bytes};

use crate::models::messages::{RouteEncodeRequest, SegmentDraft, SwapKind};

use super::EncodeError;

pub(super) fn validate_chain(chain_id: u64, expected: Chain) -> Result<Chain, EncodeError> {
    if expected.id() != chain_id {
        return Err(EncodeError::invalid(format!(
            "Unsupported chainId: {} (this instance serves chain {})",
            chain_id,
            expected.id()
        )));
    }
    Ok(expected)
}

pub(super) fn normalize_protocol_id(protocol_id: &str) -> String {
    protocol_id
        .trim()
        .to_ascii_lowercase()
        .replace(['-', ' '], "_")
}

pub(super) fn is_native_protocol_allowlisted(protocol_id: &str, allowlist: &[String]) -> bool {
    let normalized = normalize_protocol_id(protocol_id);
    allowlist
        .iter()
        .any(|candidate| normalize_protocol_id(candidate) == normalized)
}

pub(super) fn format_native_protocol_allowlist(allowlist: &[String]) -> String {
    allowlist.join(", ")
}

pub(super) fn should_reset_allowance(
    reset_allowance_tokens: &HashMap<u64, HashSet<Bytes>>,
    chain_id: u64,
    token_in: &Bytes,
) -> bool {
    reset_allowance_tokens
        .get(&chain_id)
        .map(|tokens| tokens.contains(token_in))
        .unwrap_or(false)
}

fn infer_route_kind(segments: &[SegmentDraft]) -> SwapKind {
    if segments.len() > 1 {
        return SwapKind::MegaSwap;
    }
    let hops_len = segments
        .first()
        .map(|segment| segment.hops.len())
        .unwrap_or(0);
    if hops_len > 1 {
        SwapKind::MultiSwap
    } else {
        SwapKind::SimpleSwap
    }
}

fn infer_segment_kind(segment: &SegmentDraft) -> SwapKind {
    if segment.hops.len() > 1 {
        SwapKind::MultiSwap
    } else {
        SwapKind::SimpleSwap
    }
}

pub(super) fn validate_swap_kinds(request: &RouteEncodeRequest) -> Result<(), EncodeError> {
    if request.segments.is_empty() {
        return Err(EncodeError::invalid("segments must not be empty"));
    }

    let expected_route_kind = infer_route_kind(&request.segments);
    if request.swap_kind != expected_route_kind {
        return Err(EncodeError::invalid(format!(
            "swapKind mismatch: expected {:?}",
            expected_route_kind
        )));
    }

    for (index, segment) in request.segments.iter().enumerate() {
        let expected_segment_kind = infer_segment_kind(segment);
        if segment.kind != expected_segment_kind {
            return Err(EncodeError::invalid(format!(
                "segment[{}].kind mismatch: expected {:?}",
                index, expected_segment_kind
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
#[expect(
    clippy::panic,
    reason = "negative request tests use explicit invariant assertions"
)]
mod tests {
    use super::*;

    #[test]
    fn validate_swap_kinds_rejects_route_mismatch() {
        let request = RouteEncodeRequest {
            chain_id: 1,
            token_in: "0x0000000000000000000000000000000000000001".to_string(),
            token_out: "0x0000000000000000000000000000000000000002".to_string(),
            amount_in: "100".to_string(),
            min_amount_out: "90".to_string(),
            settlement_address: "0x0000000000000000000000000000000000000003".to_string(),
            tycho_router_address: "0x0000000000000000000000000000000000000004".to_string(),
            swap_kind: SwapKind::SimpleSwap,
            segments: vec![
                SegmentDraft {
                    kind: SwapKind::SimpleSwap,
                    share_bps: 5000,
                    hops: vec![crate::models::messages::HopDraft {
                        token_in: "0x0000000000000000000000000000000000000001".to_string(),
                        token_out: "0x0000000000000000000000000000000000000002".to_string(),
                        swaps: vec![crate::models::messages::PoolSwapDraft {
                            pool: crate::services::encode::fixtures::pool_ref("p1"),
                            token_in: "0x0000000000000000000000000000000000000001".to_string(),
                            token_out: "0x0000000000000000000000000000000000000002".to_string(),
                            split_bps: 0,
                        }],
                    }],
                },
                SegmentDraft {
                    kind: SwapKind::SimpleSwap,
                    share_bps: 0,
                    hops: vec![crate::models::messages::HopDraft {
                        token_in: "0x0000000000000000000000000000000000000001".to_string(),
                        token_out: "0x0000000000000000000000000000000000000002".to_string(),
                        swaps: vec![crate::models::messages::PoolSwapDraft {
                            pool: crate::services::encode::fixtures::pool_ref("p2"),
                            token_in: "0x0000000000000000000000000000000000000001".to_string(),
                            token_out: "0x0000000000000000000000000000000000000002".to_string(),
                            split_bps: 0,
                        }],
                    }],
                },
            ],
            request_id: None,
            estimated_amount_in: None,
        };

        match validate_swap_kinds(&request) {
            Err(err) => assert_eq!(
                err.kind(),
                crate::services::encode::EncodeErrorKind::InvalidRequest
            ),
            Ok(_) => panic!("swapKind mismatch should be rejected"),
        }
    }

    #[test]
    fn normalize_protocol_id_handles_whitespace_and_casing() {
        assert_eq!(normalize_protocol_id(" RocketPool "), "rocketpool");
        assert_eq!(normalize_protocol_id("ROCKET-POOL"), "rocket_pool");
    }

    #[test]
    fn native_protocol_allowlist_matches_rocketpool_only() {
        let allowlist = vec!["rocketpool".to_string()];
        assert!(is_native_protocol_allowlisted("rocketpool", &allowlist));
        assert!(is_native_protocol_allowlisted("ROCKETPOOL", &allowlist));
        assert!(!is_native_protocol_allowlisted("uniswap_v2", &allowlist));
    }

    #[test]
    fn empty_allowlist_rejects_everything() {
        let allowlist: Vec<String> = vec![];
        assert!(!is_native_protocol_allowlisted("rocketpool", &allowlist));
    }

    #[test]
    fn validate_chain_accepts_matching_chain() {
        let Ok(chain) = validate_chain(1, Chain::Ethereum) else {
            unreachable!("expected matching chain to validate");
        };
        assert_eq!(chain, Chain::Ethereum);
    }

    #[test]
    fn validate_chain_rejects_mismatched_chain() {
        let Err(err) = validate_chain(8453, Chain::Ethereum) else {
            unreachable!("expected mismatched chain to fail validation");
        };
        assert_eq!(
            err.kind(),
            crate::services::encode::EncodeErrorKind::InvalidRequest
        );
    }

    #[test]
    fn validate_chain_accepts_base() {
        let Ok(chain) = validate_chain(8453, Chain::Base) else {
            unreachable!("expected base chain to validate");
        };
        assert_eq!(chain, Chain::Base);
    }
}
