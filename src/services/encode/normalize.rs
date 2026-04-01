use num_bigint::BigUint;
use num_traits::Zero;
use tracing::info;
use tycho_simulation::tycho_common::Bytes;

use crate::models::erc4626::{
    is_erc4626_protocol, request_direction_supported, unsupported_direction_message,
};
use crate::models::messages::{HopDraft, PoolSwapDraft, RouteEncodeRequest, SegmentDraft};

use super::allocation::{allocate_amounts_by_bps, BPS_DENOMINATOR};
use super::model::{
    NormalizedHopInternal, NormalizedRouteInternal, NormalizedSegmentInternal,
    NormalizedSwapDraftInternal,
};
use super::request::{format_native_protocol_allowlist, is_native_protocol_allowlisted};
use super::wire::parse_address;
use super::EncodeError;

pub(super) fn normalize_route(
    request: &RouteEncodeRequest,
    request_token_in: &Bytes,
    request_token_out: &Bytes,
    total_amount_in: &BigUint,
    native_address: &Bytes,
    erc4626_deposits_enabled: bool,
    native_token_protocol_allowlist: &[String],
) -> Result<NormalizedRouteInternal, EncodeError> {
    if request.segments.is_empty() {
        return Err(EncodeError::invalid("segments must not be empty"));
    }
    if total_amount_in.is_zero() {
        return Err(EncodeError::invalid("amountIn must be > 0"));
    }

    let mut normalized_segments = Vec::with_capacity(request.segments.len());
    let segment_shares = request
        .segments
        .iter()
        .map(|segment| segment.share_bps)
        .collect::<Vec<_>>();
    let segment_amounts =
        allocate_amounts_by_bps(total_amount_in, &segment_shares, "segment", "shareBps")?;

    for (segment_index, segment) in request.segments.iter().enumerate() {
        validate_segment_shape(segment, segment_index)?;
        validate_hop_continuity(segment, request_token_in, request_token_out)?;

        let hops = segment
            .hops
            .iter()
            .map(|hop| {
                normalize_hop(
                    hop,
                    native_address,
                    erc4626_deposits_enabled,
                    native_token_protocol_allowlist,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        let segment_amount_in = segment_amounts
            .get(segment_index)
            .cloned()
            .ok_or_else(|| EncodeError::internal("Missing segment amount allocation"))?;
        if segment_amount_in.is_zero() {
            return Err(EncodeError::invalid(format!(
                "segment[{}] amountIn must be > 0",
                segment_index
            )));
        }

        normalized_segments.push(NormalizedSegmentInternal {
            share_bps: segment.share_bps,
            amount_in: segment_amount_in,
            hops,
        });
    }

    Ok(NormalizedRouteInternal {
        segments: normalized_segments,
    })
}

pub(super) fn route_backend_usage(normalized: &NormalizedRouteInternal) -> (bool, bool, bool) {
    let mut uses_native = false;
    let mut uses_vm = false;
    let mut uses_rfq = false;

    for swap in normalized
        .segments
        .iter()
        .flat_map(|segment| segment.hops.iter())
        .flat_map(|hop| hop.swaps.iter())
    {
        if swap.pool.protocol.starts_with("vm:") {
            uses_vm = true;
        } else if swap.pool.protocol.starts_with("rfq:") {
            uses_rfq = true;
        } else {
            uses_native = true;
        }

        if uses_native && uses_vm && uses_rfq {
            break;
        }
    }

    (uses_native, uses_vm, uses_rfq)
}

fn validate_segment_shape(segment: &SegmentDraft, segment_index: usize) -> Result<(), EncodeError> {
    if segment.hops.is_empty() {
        return Err(EncodeError::invalid(format!(
            "segment[{}].hops must not be empty",
            segment_index
        )));
    }
    Ok(())
}

fn validate_hop_continuity(
    segment: &SegmentDraft,
    request_token_in: &Bytes,
    request_token_out: &Bytes,
) -> Result<(), EncodeError> {
    let first = segment
        .hops
        .first()
        .ok_or_else(|| EncodeError::invalid("segment.hops must not be empty"))?;
    let last = segment
        .hops
        .last()
        .ok_or_else(|| EncodeError::invalid("segment.hops must not be empty"))?;

    let first_in = parse_address(&first.token_in)?;
    if &first_in != request_token_in {
        return Err(EncodeError::invalid(
            "segment first hop tokenIn does not match request tokenIn",
        ));
    }

    let last_out = parse_address(&last.token_out)?;
    if &last_out != request_token_out {
        return Err(EncodeError::invalid(
            "segment last hop tokenOut does not match request tokenOut",
        ));
    }

    for window in segment.hops.windows(2) {
        let prev_out = parse_address(&window[0].token_out)?;
        let next_in = parse_address(&window[1].token_in)?;
        if prev_out != next_in {
            return Err(EncodeError::invalid(
                "hop token continuity mismatch within segment",
            ));
        }
    }
    Ok(())
}

fn normalize_hop(
    hop: &HopDraft,
    native_address: &Bytes,
    erc4626_deposits_enabled: bool,
    native_token_protocol_allowlist: &[String],
) -> Result<NormalizedHopInternal, EncodeError> {
    if hop.swaps.is_empty() {
        return Err(EncodeError::invalid("hop.swaps must not be empty"));
    }

    let token_in = parse_address(&hop.token_in)?;
    let token_out = parse_address(&hop.token_out)?;
    if token_in == *native_address || token_out == *native_address {
        for swap in &hop.swaps {
            if !is_native_protocol_allowlisted(&swap.pool.protocol, native_token_protocol_allowlist)
            {
                let supported = format_native_protocol_allowlist(native_token_protocol_allowlist);
                return Err(EncodeError::invalid(format!(
                    "native tokenIn/tokenOut is only supported for protocols [{}]; got {}",
                    supported, swap.pool.protocol
                )));
            }
        }
    }
    let swaps = hop
        .swaps
        .iter()
        .map(|swap| normalize_swap(swap, &token_in, &token_out, erc4626_deposits_enabled))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(NormalizedHopInternal {
        token_in,
        token_out,
        swaps,
    })
}

fn normalize_swap(
    swap: &PoolSwapDraft,
    hop_token_in: &Bytes,
    hop_token_out: &Bytes,
    erc4626_deposits_enabled: bool,
) -> Result<NormalizedSwapDraftInternal, EncodeError> {
    let token_in = parse_address(&swap.token_in)?;
    let token_out = parse_address(&swap.token_out)?;
    if &token_in != hop_token_in {
        return Err(EncodeError::invalid(
            "swap tokenIn does not match hop tokenIn",
        ));
    }
    if &token_out != hop_token_out {
        return Err(EncodeError::invalid(
            "swap tokenOut does not match hop tokenOut",
        ));
    }
    validate_erc4626_swap_supported(swap, &token_in, &token_out, erc4626_deposits_enabled)?;

    if swap.split_bps > BPS_DENOMINATOR {
        return Err(EncodeError::invalid("swap splitBps must be <= 10000"));
    }

    Ok(NormalizedSwapDraftInternal {
        pool: swap.pool.clone(),
        token_in,
        token_out,
        split_bps: swap.split_bps,
    })
}

fn validate_erc4626_swap_supported(
    swap: &PoolSwapDraft,
    token_in: &Bytes,
    token_out: &Bytes,
    erc4626_deposits_enabled: bool,
) -> Result<(), EncodeError> {
    if !is_erc4626_protocol(&swap.pool.protocol) {
        return Ok(());
    }
    if request_direction_supported(
        &swap.pool.protocol,
        token_in,
        token_out,
        erc4626_deposits_enabled,
    ) {
        return Ok(());
    }

    info!(
        protocol = swap.pool.protocol.as_str(),
        component_id = swap.pool.component_id.as_str(),
        token_in = token_in.to_string(),
        token_out = token_out.to_string(),
        "Rejecting unsupported ERC4626 encode hop during normalization"
    );
    Err(EncodeError::invalid(unsupported_direction_message(
        token_in,
        token_out,
        erc4626_deposits_enabled,
    )))
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    reason = "deterministic normalize fixtures use hard-coded addresses and amounts"
)]
#[expect(
    clippy::expect_used,
    reason = "deterministic normalize fixtures use hard-coded response assertions"
)]
#[expect(
    clippy::panic,
    reason = "negative test branches are expressed as explicit test invariants"
)]
#[expect(
    clippy::manual_let_else,
    reason = "negative test assertions read more clearly in match form here"
)]
mod tests {
    use std::str::FromStr;

    use tycho_simulation::tycho_common::models::Chain;

    use super::*;
    use crate::models::messages::PoolRef;
    use crate::models::messages::{PoolSwapDraft, SwapKind};
    use crate::services::encode::fixtures::pool_ref;
    use crate::services::encode::wire::{format_address, parse_address, parse_amount};

    fn allowlist() -> Vec<String> {
        vec!["rocketpool".to_string()]
    }

    #[test]
    fn route_backend_usage_detects_mixed_routes() {
        let request = RouteEncodeRequest {
            chain_id: 1,
            token_in: "0x0000000000000000000000000000000000000001".to_string(),
            token_out: "0x0000000000000000000000000000000000000003".to_string(),
            amount_in: "100".to_string(),
            min_amount_out: "90".to_string(),
            settlement_address: "0x0000000000000000000000000000000000000004".to_string(),
            tycho_router_address: "0x0000000000000000000000000000000000000005".to_string(),
            swap_kind: SwapKind::MultiSwap,
            segments: vec![SegmentDraft {
                kind: SwapKind::MultiSwap,
                share_bps: 0,
                hops: vec![
                    HopDraft {
                        token_in: "0x0000000000000000000000000000000000000001".to_string(),
                        token_out: "0x0000000000000000000000000000000000000002".to_string(),
                        swaps: vec![PoolSwapDraft {
                            pool: PoolRef {
                                protocol: "uniswap_v2".to_string(),
                                component_id: "pool-native".to_string(),
                                pool_address: None,
                            },
                            token_in: "0x0000000000000000000000000000000000000001".to_string(),
                            token_out: "0x0000000000000000000000000000000000000002".to_string(),
                            split_bps: 0,
                        }],
                    },
                    HopDraft {
                        token_in: "0x0000000000000000000000000000000000000002".to_string(),
                        token_out: "0x0000000000000000000000000000000000000003".to_string(),
                        swaps: vec![PoolSwapDraft {
                            pool: PoolRef {
                                protocol: "vm:maverick_v2".to_string(),
                                component_id: "pool-vm".to_string(),
                                pool_address: None,
                            },
                            token_in: "0x0000000000000000000000000000000000000002".to_string(),
                            token_out: "0x0000000000000000000000000000000000000003".to_string(),
                            split_bps: 0,
                        }],
                    },
                ],
            }],
            request_id: None,
        };

        let normalized = normalize_route(
            &request,
            &parse_address(&request.token_in).unwrap(),
            &parse_address(&request.token_out).unwrap(),
            &BigUint::from(100u32),
            &Chain::Ethereum.native_token().address,
            false,
            &allowlist(),
        )
        .unwrap();

        assert_eq!(route_backend_usage(&normalized), (true, true, false));
    }

    #[test]
    fn normalize_route_computes_segment_amounts_from_share_bps() {
        let request = RouteEncodeRequest {
            chain_id: 1,
            token_in: "0x0000000000000000000000000000000000000001".to_string(),
            token_out: "0x0000000000000000000000000000000000000002".to_string(),
            amount_in: "100".to_string(),
            min_amount_out: "90".to_string(),
            settlement_address: "0x0000000000000000000000000000000000000003".to_string(),
            tycho_router_address: "0x0000000000000000000000000000000000000004".to_string(),
            swap_kind: SwapKind::MegaSwap,
            segments: vec![
                SegmentDraft {
                    kind: SwapKind::SimpleSwap,
                    share_bps: 2000,
                    hops: vec![HopDraft {
                        token_in: "0x0000000000000000000000000000000000000001".to_string(),
                        token_out: "0x0000000000000000000000000000000000000002".to_string(),
                        swaps: vec![PoolSwapDraft {
                            pool: pool_ref("p1"),
                            token_in: "0x0000000000000000000000000000000000000001".to_string(),
                            token_out: "0x0000000000000000000000000000000000000002".to_string(),
                            split_bps: 0,
                        }],
                    }],
                },
                SegmentDraft {
                    kind: SwapKind::SimpleSwap,
                    share_bps: 0,
                    hops: vec![HopDraft {
                        token_in: "0x0000000000000000000000000000000000000001".to_string(),
                        token_out: "0x0000000000000000000000000000000000000002".to_string(),
                        swaps: vec![PoolSwapDraft {
                            pool: pool_ref("p2"),
                            token_in: "0x0000000000000000000000000000000000000001".to_string(),
                            token_out: "0x0000000000000000000000000000000000000002".to_string(),
                            split_bps: 0,
                        }],
                    }],
                },
            ],
            request_id: None,
        };

        let token_in = parse_address(&request.token_in).unwrap();
        let token_out = parse_address(&request.token_out).unwrap();
        let amount_in = parse_amount(&request.amount_in).unwrap();
        let native_address = Chain::Ethereum.native_token().address;

        let normalized = normalize_route(
            &request,
            &token_in,
            &token_out,
            &amount_in,
            &native_address,
            false,
            &allowlist(),
        )
        .unwrap();
        assert_eq!(normalized.segments.len(), 2);
        assert_eq!(normalized.segments[0].share_bps, 2000);
        assert_eq!(normalized.segments[1].share_bps, 0);
        assert_eq!(normalized.segments[0].amount_in, BigUint::from(20u32));
        assert_eq!(normalized.segments[1].amount_in, BigUint::from(80u32));
    }

    #[test]
    fn normalize_route_rejects_segment_share_rounding_to_zero() {
        let request = RouteEncodeRequest {
            chain_id: 1,
            token_in: "0x0000000000000000000000000000000000000001".to_string(),
            token_out: "0x0000000000000000000000000000000000000002".to_string(),
            amount_in: "1".to_string(),
            min_amount_out: "1".to_string(),
            settlement_address: "0x0000000000000000000000000000000000000003".to_string(),
            tycho_router_address: "0x0000000000000000000000000000000000000004".to_string(),
            swap_kind: SwapKind::MegaSwap,
            segments: vec![
                SegmentDraft {
                    kind: SwapKind::SimpleSwap,
                    share_bps: 1,
                    hops: vec![HopDraft {
                        token_in: "0x0000000000000000000000000000000000000001".to_string(),
                        token_out: "0x0000000000000000000000000000000000000002".to_string(),
                        swaps: vec![PoolSwapDraft {
                            pool: pool_ref("p1"),
                            token_in: "0x0000000000000000000000000000000000000001".to_string(),
                            token_out: "0x0000000000000000000000000000000000000002".to_string(),
                            split_bps: 0,
                        }],
                    }],
                },
                SegmentDraft {
                    kind: SwapKind::SimpleSwap,
                    share_bps: 0,
                    hops: vec![HopDraft {
                        token_in: "0x0000000000000000000000000000000000000001".to_string(),
                        token_out: "0x0000000000000000000000000000000000000002".to_string(),
                        swaps: vec![PoolSwapDraft {
                            pool: pool_ref("p2"),
                            token_in: "0x0000000000000000000000000000000000000001".to_string(),
                            token_out: "0x0000000000000000000000000000000000000002".to_string(),
                            split_bps: 0,
                        }],
                    }],
                },
            ],
            request_id: None,
        };

        let token_in = Bytes::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let token_out = Bytes::from_str("0x0000000000000000000000000000000000000002").unwrap();
        let amount_in = BigUint::from(1u32);
        let native = Chain::Ethereum.native_token().address;

        let err = match normalize_route(
            &request,
            &token_in,
            &token_out,
            &amount_in,
            &native,
            false,
            &allowlist(),
        ) {
            Ok(_) => panic!("Expected segment share rounding to zero to be rejected"),
            Err(err) => err,
        };
        assert_eq!(
            err.kind(),
            crate::services::encode::EncodeErrorKind::InvalidRequest
        );
        assert!(
            err.message().contains("rounds down to zero"),
            "unexpected error: {}",
            err.message()
        );
    }

    #[test]
    fn normalize_route_rejects_non_zero_last_segment_share() {
        let request = RouteEncodeRequest {
            chain_id: 1,
            token_in: "0x0000000000000000000000000000000000000001".to_string(),
            token_out: "0x0000000000000000000000000000000000000002".to_string(),
            amount_in: "100".to_string(),
            min_amount_out: "90".to_string(),
            settlement_address: "0x0000000000000000000000000000000000000003".to_string(),
            tycho_router_address: "0x0000000000000000000000000000000000000004".to_string(),
            swap_kind: SwapKind::MegaSwap,
            segments: vec![
                SegmentDraft {
                    kind: SwapKind::SimpleSwap,
                    share_bps: 4000,
                    hops: vec![HopDraft {
                        token_in: "0x0000000000000000000000000000000000000001".to_string(),
                        token_out: "0x0000000000000000000000000000000000000002".to_string(),
                        swaps: vec![PoolSwapDraft {
                            pool: pool_ref("p1"),
                            token_in: "0x0000000000000000000000000000000000000001".to_string(),
                            token_out: "0x0000000000000000000000000000000000000002".to_string(),
                            split_bps: 0,
                        }],
                    }],
                },
                SegmentDraft {
                    kind: SwapKind::SimpleSwap,
                    share_bps: 6000,
                    hops: vec![HopDraft {
                        token_in: "0x0000000000000000000000000000000000000001".to_string(),
                        token_out: "0x0000000000000000000000000000000000000002".to_string(),
                        swaps: vec![PoolSwapDraft {
                            pool: pool_ref("p2"),
                            token_in: "0x0000000000000000000000000000000000000001".to_string(),
                            token_out: "0x0000000000000000000000000000000000000002".to_string(),
                            split_bps: 0,
                        }],
                    }],
                },
            ],
            request_id: None,
        };

        let token_in = parse_address(&request.token_in).unwrap();
        let token_out = parse_address(&request.token_out).unwrap();
        let amount_in = parse_amount(&request.amount_in).unwrap();
        let native_address = Chain::Ethereum.native_token().address;

        match normalize_route(
            &request,
            &token_in,
            &token_out,
            &amount_in,
            &native_address,
            false,
            &allowlist(),
        ) {
            Err(err) => assert_eq!(
                err.kind(),
                crate::services::encode::EncodeErrorKind::InvalidRequest
            ),
            Ok(_) => panic!("non-zero last segment share should be rejected"),
        }
    }

    #[test]
    fn normalize_route_rejects_native_hops_for_non_allowlisted_protocols() {
        let native = Chain::Ethereum.native_token().address;
        let request = RouteEncodeRequest {
            chain_id: 1,
            token_in: "0x0000000000000000000000000000000000000001".to_string(),
            token_out: "0x0000000000000000000000000000000000000002".to_string(),
            amount_in: "100".to_string(),
            min_amount_out: "90".to_string(),
            settlement_address: "0x0000000000000000000000000000000000000003".to_string(),
            tycho_router_address: "0x0000000000000000000000000000000000000004".to_string(),
            swap_kind: SwapKind::MultiSwap,
            segments: vec![SegmentDraft {
                kind: SwapKind::MultiSwap,
                share_bps: 0,
                hops: vec![
                    HopDraft {
                        token_in: "0x0000000000000000000000000000000000000001".to_string(),
                        token_out: format_address(&native),
                        swaps: vec![PoolSwapDraft {
                            pool: pool_ref("p1"),
                            token_in: "0x0000000000000000000000000000000000000001".to_string(),
                            token_out: format_address(&native),
                            split_bps: 0,
                        }],
                    },
                    HopDraft {
                        token_in: format_address(&native),
                        token_out: "0x0000000000000000000000000000000000000002".to_string(),
                        swaps: vec![PoolSwapDraft {
                            pool: pool_ref("p2"),
                            token_in: format_address(&native),
                            token_out: "0x0000000000000000000000000000000000000002".to_string(),
                            split_bps: 0,
                        }],
                    },
                ],
            }],
            request_id: None,
        };

        let token_in = parse_address(&request.token_in).unwrap();
        let token_out = parse_address(&request.token_out).unwrap();
        let amount_in = parse_amount(&request.amount_in).unwrap();

        match normalize_route(
            &request,
            &token_in,
            &token_out,
            &amount_in,
            &native,
            false,
            &allowlist(),
        ) {
            Err(err) => assert_eq!(
                err.kind(),
                crate::services::encode::EncodeErrorKind::InvalidRequest
            ),
            Ok(_) => panic!("native hop tokens should be rejected for non-allowlisted protocols"),
        }
    }

    #[test]
    fn normalize_route_allows_native_hops_for_rocketpool() {
        let native = Chain::Ethereum.native_token().address;
        let token_out = Bytes::from_str("0x0000000000000000000000000000000000000002").unwrap();
        let request = RouteEncodeRequest {
            chain_id: 1,
            token_in: format_address(&native),
            token_out: format_address(&token_out),
            amount_in: "100".to_string(),
            min_amount_out: "90".to_string(),
            settlement_address: "0x0000000000000000000000000000000000000003".to_string(),
            tycho_router_address: "0x0000000000000000000000000000000000000004".to_string(),
            swap_kind: SwapKind::SimpleSwap,
            segments: vec![SegmentDraft {
                kind: SwapKind::SimpleSwap,
                share_bps: 0,
                hops: vec![HopDraft {
                    token_in: format_address(&native),
                    token_out: format_address(&token_out),
                    swaps: vec![PoolSwapDraft {
                        pool: PoolRef {
                            protocol: "rocketpool".to_string(),
                            component_id: "p1".to_string(),
                            pool_address: None,
                        },
                        token_in: format_address(&native),
                        token_out: format_address(&token_out),
                        split_bps: 0,
                    }],
                }],
            }],
            request_id: None,
        };

        let token_in = parse_address(&request.token_in).unwrap();
        let request_token_out = parse_address(&request.token_out).unwrap();
        let amount_in = parse_amount(&request.amount_in).unwrap();

        let normalized = normalize_route(
            &request,
            &token_in,
            &request_token_out,
            &amount_in,
            &native,
            false,
            &allowlist(),
        )
        .expect("rocketpool native route should normalize");
        assert_eq!(normalized.segments.len(), 1);
        assert_eq!(normalized.segments[0].hops.len(), 1);
        assert_eq!(normalized.segments[0].hops[0].token_in, native);
        assert_eq!(normalized.segments[0].hops[0].token_out, token_out);
    }

    #[test]
    fn normalize_route_rejects_allowlisted_erc4626_deposit_when_deposits_disabled() {
        let request = RouteEncodeRequest {
            chain_id: 1,
            token_in: "0xdC035D45d973E3EC169d2276DDab16f1e407384F".to_string(),
            token_out: "0xa3931d71877c0e7a3148cb7eb4463524fec27fbd".to_string(),
            amount_in: "10".to_string(),
            min_amount_out: "8".to_string(),
            settlement_address: "0x0000000000000000000000000000000000000003".to_string(),
            tycho_router_address: "0x0000000000000000000000000000000000000004".to_string(),
            swap_kind: SwapKind::SimpleSwap,
            segments: vec![SegmentDraft {
                kind: SwapKind::SimpleSwap,
                share_bps: 0,
                hops: vec![HopDraft {
                    token_in: "0xdC035D45d973E3EC169d2276DDab16f1e407384F".to_string(),
                    token_out: "0xa3931d71877c0e7a3148cb7eb4463524fec27fbd".to_string(),
                    swaps: vec![PoolSwapDraft {
                        pool: PoolRef {
                            protocol: "erc4626".to_string(),
                            component_id: "p1".to_string(),
                            pool_address: None,
                        },
                        token_in: "0xdC035D45d973E3EC169d2276DDab16f1e407384F".to_string(),
                        token_out: "0xa3931d71877c0e7a3148cb7eb4463524fec27fbd".to_string(),
                        split_bps: 0,
                    }],
                }],
            }],
            request_id: None,
        };

        let token_in = parse_address(&request.token_in).unwrap();
        let token_out = parse_address(&request.token_out).unwrap();
        let amount_in = parse_amount(&request.amount_in).unwrap();
        let native = Chain::Ethereum.native_token().address;

        let err = match normalize_route(
            &request,
            &token_in,
            &token_out,
            &amount_in,
            &native,
            false,
            &allowlist(),
        ) {
            Ok(_) => panic!("allowlisted deposit should be rejected when deposits are disabled"),
            Err(err) => err,
        };

        assert_eq!(
            err.kind(),
            crate::services::encode::EncodeErrorKind::InvalidRequest
        );
        assert!(
            err.message().contains("sUSDS -> USDS"),
            "unexpected error: {}",
            err.message()
        );
    }

    #[test]
    fn normalize_route_allows_allowlisted_erc4626_deposit_when_deposits_enabled() {
        let request = RouteEncodeRequest {
            chain_id: 1,
            token_in: "0xdC035D45d973E3EC169d2276DDab16f1e407384F".to_string(),
            token_out: "0xa3931d71877c0e7a3148cb7eb4463524fec27fbd".to_string(),
            amount_in: "10".to_string(),
            min_amount_out: "8".to_string(),
            settlement_address: "0x0000000000000000000000000000000000000003".to_string(),
            tycho_router_address: "0x0000000000000000000000000000000000000004".to_string(),
            swap_kind: SwapKind::SimpleSwap,
            segments: vec![SegmentDraft {
                kind: SwapKind::SimpleSwap,
                share_bps: 0,
                hops: vec![HopDraft {
                    token_in: "0xdC035D45d973E3EC169d2276DDab16f1e407384F".to_string(),
                    token_out: "0xa3931d71877c0e7a3148cb7eb4463524fec27fbd".to_string(),
                    swaps: vec![PoolSwapDraft {
                        pool: PoolRef {
                            protocol: "erc4626".to_string(),
                            component_id: "p1".to_string(),
                            pool_address: None,
                        },
                        token_in: "0xdC035D45d973E3EC169d2276DDab16f1e407384F".to_string(),
                        token_out: "0xa3931d71877c0e7a3148cb7eb4463524fec27fbd".to_string(),
                        split_bps: 0,
                    }],
                }],
            }],
            request_id: None,
        };

        let token_in = parse_address(&request.token_in).unwrap();
        let token_out = parse_address(&request.token_out).unwrap();
        let amount_in = parse_amount(&request.amount_in).unwrap();
        let native = Chain::Ethereum.native_token().address;

        let normalized = normalize_route(
            &request,
            &token_in,
            &token_out,
            &amount_in,
            &native,
            true,
            &allowlist(),
        )
        .expect("allowlisted deposit should normalize when deposits are enabled");

        assert_eq!(normalized.segments.len(), 1);
        assert_eq!(normalized.segments[0].hops.len(), 1);
    }
}
