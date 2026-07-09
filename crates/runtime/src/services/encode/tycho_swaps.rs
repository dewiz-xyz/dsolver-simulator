use std::collections::HashMap;
use std::sync::Arc;

use num_bigint::BigUint;
use num_traits::{ToPrimitive, Zero};
use tycho_execution::encoding::models::Swap;
use tycho_simulation::tycho_common::{
    models::protocol::ProtocolComponent as CommonProtocolComponent, Bytes,
};

use super::model::ResimulatedRouteInternal;
use super::wire::format_address;
use super::EncodeError;

struct SplitStats {
    total_amount: BigUint,
    count: usize,
    last_index: usize,
}

pub(super) fn build_route_swaps(
    resimulated: &ResimulatedRouteInternal,
) -> Result<Vec<Swap>, EncodeError> {
    // Tycho split validation allows only one remainder per tokenIn, so depths must be consistent.
    ensure_token_in_single_depth(resimulated)?;

    let mut ordered_swaps = Vec::new();
    // Order swaps by hop depth across segments so split sets match token availability.
    let max_hops = resimulated
        .segments
        .iter()
        .map(|segment| segment.hops.len())
        .max()
        .unwrap_or(0);
    for hop_index in 0..max_hops {
        for segment in &resimulated.segments {
            if let Some(hop) = segment.hops.get(hop_index) {
                for swap in &hop.swaps {
                    ordered_swaps.push(swap);
                }
            }
        }
    }

    let mut split_stats: HashMap<Bytes, SplitStats> = HashMap::new();
    for (index, swap) in ordered_swaps.iter().enumerate() {
        let entry = split_stats
            .entry(swap.token_in.clone())
            .or_insert_with(|| SplitStats {
                total_amount: BigUint::zero(),
                count: 0,
                last_index: index,
            });
        entry.total_amount += swap.amount_in.clone();
        entry.count += 1;
        entry.last_index = index;
    }

    let mut swaps = Vec::with_capacity(ordered_swaps.len());
    for (index, swap) in ordered_swaps.iter().enumerate() {
        let stats = split_stats
            .get(&swap.token_in)
            .ok_or_else(|| EncodeError::internal("Missing split stats for swap tokenIn"))?;
        // Tycho split validation requires exactly one remainder (split=0) per tokenIn, and it must
        // be the last occurrence in order.
        let is_remainder = stats.count == 1 || stats.last_index == index;
        let split = compute_split_fraction(
            &swap.amount_in,
            &stats.total_amount,
            is_remainder,
            stats.count,
            &swap.token_in,
        )?;
        let common_component: CommonProtocolComponent = swap.component.as_ref().clone().into();
        let mut swap_data = Swap::new(
            common_component,
            swap.token_in.clone(),
            swap.token_out.clone(),
        )
        .split(split)
        .protocol_state(Arc::clone(&swap.pool_state));
        if swap.backend.is_rfq() {
            swap_data = swap_data.estimated_amount_in(swap.amount_in.clone());
        }
        swaps.push(swap_data);
    }

    Ok(swaps)
}

fn ensure_token_in_single_depth(resimulated: &ResimulatedRouteInternal) -> Result<(), EncodeError> {
    let mut token_depths: HashMap<Bytes, usize> = HashMap::new();
    for segment in &resimulated.segments {
        for (hop_index, hop) in segment.hops.iter().enumerate() {
            for swap in &hop.swaps {
                if let Some(existing_depth) = token_depths.get(&swap.token_in) {
                    if *existing_depth != hop_index {
                        return Err(EncodeError::invalid(format!(
                            "tokenIn {} appears at multiple hop depths",
                            format_address(&swap.token_in)
                        )));
                    }
                } else {
                    token_depths.insert(swap.token_in.clone(), hop_index);
                }
            }
        }
    }
    Ok(())
}

pub(super) fn compute_split_fraction(
    amount_in: &BigUint,
    total_in: &BigUint,
    is_remainder: bool,
    count: usize,
    token_in: &Bytes,
) -> Result<f64, EncodeError> {
    if count <= 1 || is_remainder {
        return Ok(0.0);
    }

    if total_in.is_zero() {
        return Err(EncodeError::invalid(
            "TokenIn total amount must be positive for split calculation",
        ));
    }
    if amount_in.is_zero() || amount_in >= total_in {
        return Err(EncodeError::invalid(format!(
            "Invalid split ratio for tokenIn {}",
            format_address(token_in)
        )));
    }

    let amount_in = amount_in.to_f64().ok_or_else(|| {
        EncodeError::invalid("Failed to convert swap amountIn for split calculation")
    })?;
    let total_in = total_in.to_f64().ok_or_else(|| {
        EncodeError::invalid("Failed to convert tokenIn total amount for split calculation")
    })?;
    let mut split = amount_in / total_in;
    if !split.is_finite() {
        return Err(EncodeError::invalid("Invalid split ratio for tokenIn"));
    }
    if split <= 0.0 {
        // Clamp away from 0 for very skewed splits that lose precision in f64.
        split = f64::MIN_POSITIVE;
    } else if split >= 1.0 {
        // Clamp away from 1 to preserve a non-remainder split.
        split = 1.0 - f64::EPSILON;
    }
    Ok(split)
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    reason = "deterministic swap fixtures use hard-coded addresses and JSON values"
)]
#[expect(
    clippy::expect_used,
    reason = "deterministic swap fixtures use hard-coded response assertions"
)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::services::encode::backend::PoolBackend;
    use crate::services::encode::fixtures::{
        component_with_protocol, dummy_component, dummy_token, fixture_bytes, pool_ref,
    };
    use crate::services::encode::mocks::MockProtocolSim;
    use crate::services::encode::model::{
        ResimulatedHopInternal, ResimulatedRouteInternal, ResimulatedSegmentInternal,
        ResimulatedSwapInternal,
    };
    use tycho_simulation::tycho_common::simulation::protocol_sim::ProtocolSim;

    struct RoutePoolFixture<'a> {
        pool_state: &'a Arc<dyn ProtocolSim>,
        component: &'a Arc<tycho_simulation::protocol::models::ProtocolComponent>,
    }

    struct RouteSwapSpec<'a> {
        pool: &'a str,
        token_in: &'a Bytes,
        token_out: &'a Bytes,
        split_bps: u32,
        amount_in: u32,
        expected_amount_out: u32,
    }

    fn swap_split(swap: &Swap) -> f64 {
        // `Swap`'s fields are private; use the serialized view to validate split behavior.
        let value = serde_json::to_value(swap).expect("swap should serialize");
        value
            .get("split")
            .and_then(serde_json::Value::as_f64)
            .expect("swap.split should be a JSON number")
    }

    fn route_swap(
        spec: RouteSwapSpec<'_>,
        fixture: &RoutePoolFixture<'_>,
    ) -> ResimulatedSwapInternal {
        ResimulatedSwapInternal {
            pool: pool_ref(spec.pool),
            backend: PoolBackend::from_component(fixture.component.as_ref()),
            token_in: spec.token_in.clone(),
            token_out: spec.token_out.clone(),
            split_bps: spec.split_bps,
            amount_in: BigUint::from(spec.amount_in),
            expected_amount_out: BigUint::from(spec.expected_amount_out),
            pool_state: Arc::clone(fixture.pool_state),
            component: Arc::clone(fixture.component),
        }
    }

    fn route_hop(
        token_in: &Bytes,
        token_out: &Bytes,
        amount_in: u32,
        expected_amount_out: u32,
        swaps: Vec<ResimulatedSwapInternal>,
    ) -> ResimulatedHopInternal {
        ResimulatedHopInternal {
            token_in: token_in.clone(),
            token_out: token_out.clone(),
            amount_in: BigUint::from(amount_in),
            expected_amount_out: BigUint::from(expected_amount_out),
            swaps,
        }
    }

    fn route_segment(
        share_bps: u32,
        amount_in: u32,
        expected_amount_out: u32,
        hops: Vec<ResimulatedHopInternal>,
    ) -> ResimulatedSegmentInternal {
        ResimulatedSegmentInternal {
            share_bps,
            amount_in: BigUint::from(amount_in),
            expected_amount_out: BigUint::from(expected_amount_out),
            hops,
        }
    }

    fn shared_intermediate_route(fixture: &RoutePoolFixture<'_>) -> ResimulatedRouteInternal {
        let token_a = fixture_bytes("0x0000000000000000000000000000000000000001");
        let token_b = fixture_bytes("0x0000000000000000000000000000000000000002");
        let token_c = fixture_bytes("0x0000000000000000000000000000000000000003");
        let token_d = fixture_bytes("0x0000000000000000000000000000000000000004");

        ResimulatedRouteInternal {
            segments: vec![
                route_segment(
                    6_000,
                    60,
                    55,
                    vec![
                        route_hop(
                            &token_a,
                            &token_b,
                            60,
                            58,
                            vec![route_swap(
                                RouteSwapSpec {
                                    pool: "p1",
                                    token_in: &token_a,
                                    token_out: &token_b,
                                    split_bps: 6_000,
                                    amount_in: 60,
                                    expected_amount_out: 58,
                                },
                                fixture,
                            )],
                        ),
                        route_hop(
                            &token_b,
                            &token_c,
                            60,
                            55,
                            vec![route_swap(
                                RouteSwapSpec {
                                    pool: "p2",
                                    token_in: &token_b,
                                    token_out: &token_c,
                                    split_bps: 6_000,
                                    amount_in: 60,
                                    expected_amount_out: 55,
                                },
                                fixture,
                            )],
                        ),
                    ],
                ),
                route_segment(
                    4_000,
                    40,
                    37,
                    vec![
                        route_hop(
                            &token_a,
                            &token_b,
                            40,
                            39,
                            vec![route_swap(
                                RouteSwapSpec {
                                    pool: "p3",
                                    token_in: &token_a,
                                    token_out: &token_b,
                                    split_bps: 4_000,
                                    amount_in: 40,
                                    expected_amount_out: 39,
                                },
                                fixture,
                            )],
                        ),
                        route_hop(
                            &token_b,
                            &token_d,
                            40,
                            37,
                            vec![route_swap(
                                RouteSwapSpec {
                                    pool: "p4",
                                    token_in: &token_b,
                                    token_out: &token_d,
                                    split_bps: 4_000,
                                    amount_in: 40,
                                    expected_amount_out: 37,
                                },
                                fixture,
                            )],
                        ),
                    ],
                ),
            ],
        }
    }

    #[test]
    fn compute_split_fraction_clamps_rounding() {
        let total_in = BigUint::from(1u64) << 60u32;
        let amount_in = total_in.clone() - BigUint::from(1u32);
        let token_in = Bytes::from_str("0x0000000000000000000000000000000000000001").unwrap();

        let split = compute_split_fraction(&amount_in, &total_in, false, 2, &token_in).unwrap();
        assert!(split > 0.0, "split should remain positive");
        assert!(split < 1.0, "split should remain less than 1");
    }

    #[test]
    fn build_route_swaps_sets_remainder_split_last() {
        let resimulated = ResimulatedRouteInternal {
            segments: vec![ResimulatedSegmentInternal {
                share_bps: 10_000,
                amount_in: BigUint::from(100u32),
                expected_amount_out: BigUint::from(90u32),
                hops: vec![ResimulatedHopInternal {
                    token_in: Bytes::from_str("0x0000000000000000000000000000000000000001")
                        .unwrap(),
                    token_out: Bytes::from_str("0x0000000000000000000000000000000000000002")
                        .unwrap(),
                    amount_in: BigUint::from(100u32),
                    expected_amount_out: BigUint::from(90u32),
                    swaps: vec![
                        ResimulatedSwapInternal {
                            pool: pool_ref("p1"),
                            backend: PoolBackend::Native,
                            token_in: Bytes::from_str("0x0000000000000000000000000000000000000001")
                                .unwrap(),
                            token_out: Bytes::from_str(
                                "0x0000000000000000000000000000000000000002",
                            )
                            .unwrap(),
                            split_bps: 3_000,
                            amount_in: BigUint::from(30u32),
                            expected_amount_out: BigUint::from(27u32),
                            pool_state: Arc::new(MockProtocolSim {}),
                            component: Arc::new(dummy_component()),
                        },
                        ResimulatedSwapInternal {
                            pool: pool_ref("p2"),
                            backend: PoolBackend::Native,
                            token_in: Bytes::from_str("0x0000000000000000000000000000000000000001")
                                .unwrap(),
                            token_out: Bytes::from_str(
                                "0x0000000000000000000000000000000000000002",
                            )
                            .unwrap(),
                            split_bps: 7_000,
                            amount_in: BigUint::from(70u32),
                            expected_amount_out: BigUint::from(63u32),
                            pool_state: Arc::new(MockProtocolSim {}),
                            component: Arc::new(dummy_component()),
                        },
                    ],
                }],
            }],
        };

        let swaps = build_route_swaps(&resimulated).unwrap();

        assert_eq!(swaps.len(), 2);
        assert!(swap_split(&swaps[0]) > 0.0);
        assert_eq!(swap_split(&swaps[1]), 0.0);
        assert_eq!(swaps[0].get_estimated_amount_in(), &None);
        assert_eq!(swaps[1].get_estimated_amount_in(), &None);
    }

    #[test]
    fn build_route_swaps_sets_estimated_amount_in_for_rfq_protocols() {
        let token_in = fixture_bytes("0x0000000000000000000000000000000000000001");
        let token_out = fixture_bytes("0x0000000000000000000000000000000000000002");
        let pool_state: Arc<dyn ProtocolSim> = Arc::new(MockProtocolSim {});
        let component = Arc::new(component_with_protocol(
            "0x0000000000000000000000000000000000000009",
            "rfq:bebop",
            "rfq:bebop",
            vec![
                dummy_token("0x0000000000000000000000000000000000000001"),
                dummy_token("0x0000000000000000000000000000000000000002"),
            ],
        ));
        let resimulated = ResimulatedRouteInternal {
            segments: vec![ResimulatedSegmentInternal {
                share_bps: 10_000,
                amount_in: BigUint::from(100u32),
                expected_amount_out: BigUint::from(99u32),
                hops: vec![ResimulatedHopInternal {
                    token_in: token_in.clone(),
                    token_out: token_out.clone(),
                    amount_in: BigUint::from(100u32),
                    expected_amount_out: BigUint::from(99u32),
                    swaps: vec![ResimulatedSwapInternal {
                        pool: pool_ref("p1"),
                        backend: PoolBackend::Rfq,
                        token_in: token_in.clone(),
                        token_out: token_out.clone(),
                        split_bps: 10_000,
                        amount_in: BigUint::from(100u32),
                        expected_amount_out: BigUint::from(99u32),
                        pool_state,
                        component,
                    }],
                }],
            }],
        };

        let swaps = build_route_swaps(&resimulated).unwrap();

        assert_eq!(swaps.len(), 1);
        assert_eq!(
            swaps[0].get_estimated_amount_in(),
            &Some(BigUint::from(100u32))
        );
    }

    #[test]
    fn build_route_swaps_sets_estimated_amount_in_for_type_name_only_rfq_components() {
        let token_in = fixture_bytes("0x0000000000000000000000000000000000000011");
        let token_out = fixture_bytes("0x0000000000000000000000000000000000000012");
        let pool_state: Arc<dyn ProtocolSim> = Arc::new(MockProtocolSim {});
        let component = Arc::new(component_with_protocol(
            "0x0000000000000000000000000000000000000019",
            "",
            "hashflow_pool",
            vec![
                dummy_token("0x0000000000000000000000000000000000000011"),
                dummy_token("0x0000000000000000000000000000000000000012"),
            ],
        ));
        let resimulated = ResimulatedRouteInternal {
            segments: vec![ResimulatedSegmentInternal {
                share_bps: 10_000,
                amount_in: BigUint::from(100u32),
                expected_amount_out: BigUint::from(99u32),
                hops: vec![ResimulatedHopInternal {
                    token_in: token_in.clone(),
                    token_out: token_out.clone(),
                    amount_in: BigUint::from(100u32),
                    expected_amount_out: BigUint::from(99u32),
                    swaps: vec![ResimulatedSwapInternal {
                        pool: pool_ref("p1"),
                        backend: PoolBackend::from_component(component.as_ref()),
                        token_in: token_in.clone(),
                        token_out: token_out.clone(),
                        split_bps: 10_000,
                        amount_in: BigUint::from(100u32),
                        expected_amount_out: BigUint::from(99u32),
                        pool_state,
                        component,
                    }],
                }],
            }],
        };

        let swaps = build_route_swaps(&resimulated).unwrap();

        assert_eq!(swaps.len(), 1);
        assert_eq!(
            swaps[0].get_estimated_amount_in(),
            &Some(BigUint::from(100u32))
        );
    }

    #[test]
    fn build_route_swaps_orders_by_hop_depth_with_shared_intermediate() {
        let pool_state: Arc<dyn ProtocolSim> = Arc::new(MockProtocolSim {});
        let component = Arc::new(dummy_component());
        let fixture = RoutePoolFixture {
            pool_state: &pool_state,
            component: &component,
        };
        let token_a = fixture_bytes("0x0000000000000000000000000000000000000001");
        let token_b = fixture_bytes("0x0000000000000000000000000000000000000002");
        let token_c = fixture_bytes("0x0000000000000000000000000000000000000003");
        let token_d = fixture_bytes("0x0000000000000000000000000000000000000004");
        let resimulated = shared_intermediate_route(&fixture);

        let swaps = build_route_swaps(&resimulated).unwrap();

        assert_eq!(swaps.len(), 4);
        assert_eq!(swaps[0].token_in(), &token_a);
        assert_eq!(swaps[0].token_out(), &token_b);
        assert_eq!(swaps[1].token_in(), &token_a);
        assert_eq!(swaps[1].token_out(), &token_b);
        assert_eq!(swaps[2].token_in(), &token_b);
        assert_eq!(swaps[2].token_out(), &token_c);
        assert_eq!(swaps[3].token_in(), &token_b);
        assert_eq!(swaps[3].token_out(), &token_d);
        assert!((swap_split(&swaps[0]) - 0.6).abs() < 1e-9);
        assert_eq!(swap_split(&swaps[1]), 0.0);
        assert!((swap_split(&swaps[2]) - 0.6).abs() < 1e-9);
        assert_eq!(swap_split(&swaps[3]), 0.0);
    }

    #[test]
    fn build_route_swaps_rejects_token_in_at_multiple_depths() {
        let token_a = Bytes::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let token_b = Bytes::from_str("0x0000000000000000000000000000000000000002").unwrap();
        let token_c = Bytes::from_str("0x0000000000000000000000000000000000000003").unwrap();
        let pool_state: Arc<dyn ProtocolSim> = Arc::new(MockProtocolSim {});
        let component = Arc::new(dummy_component());
        let resimulated = ResimulatedRouteInternal {
            segments: vec![ResimulatedSegmentInternal {
                share_bps: 10_000,
                amount_in: BigUint::from(100u32),
                expected_amount_out: BigUint::from(90u32),
                hops: vec![
                    ResimulatedHopInternal {
                        token_in: token_a.clone(),
                        token_out: token_b.clone(),
                        amount_in: BigUint::from(100u32),
                        expected_amount_out: BigUint::from(95u32),
                        swaps: vec![ResimulatedSwapInternal {
                            pool: pool_ref("p1"),
                            backend: PoolBackend::Native,
                            token_in: token_a.clone(),
                            token_out: token_b.clone(),
                            split_bps: 10_000,
                            amount_in: BigUint::from(100u32),
                            expected_amount_out: BigUint::from(95u32),
                            pool_state: Arc::clone(&pool_state),
                            component: Arc::clone(&component),
                        }],
                    },
                    ResimulatedHopInternal {
                        token_in: token_b.clone(),
                        token_out: token_a.clone(),
                        amount_in: BigUint::from(95u32),
                        expected_amount_out: BigUint::from(92u32),
                        swaps: vec![ResimulatedSwapInternal {
                            pool: pool_ref("p2"),
                            backend: PoolBackend::Native,
                            token_in: token_b.clone(),
                            token_out: token_a.clone(),
                            split_bps: 10_000,
                            amount_in: BigUint::from(95u32),
                            expected_amount_out: BigUint::from(92u32),
                            pool_state: Arc::clone(&pool_state),
                            component: Arc::clone(&component),
                        }],
                    },
                    ResimulatedHopInternal {
                        token_in: token_a.clone(),
                        token_out: token_c.clone(),
                        amount_in: BigUint::from(92u32),
                        expected_amount_out: BigUint::from(90u32),
                        swaps: vec![ResimulatedSwapInternal {
                            pool: pool_ref("p3"),
                            backend: PoolBackend::Native,
                            token_in: token_a.clone(),
                            token_out: token_c.clone(),
                            split_bps: 10_000,
                            amount_in: BigUint::from(92u32),
                            expected_amount_out: BigUint::from(90u32),
                            pool_state: Arc::clone(&pool_state),
                            component: Arc::clone(&component),
                        }],
                    },
                ],
            }],
        };

        let err = build_route_swaps(&resimulated).expect_err("rejects repeated token depth");
        assert!(
            err.message().contains("multiple hop depths"),
            "unexpected error: {err:?}"
        );
    }
}
