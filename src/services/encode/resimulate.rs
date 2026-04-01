use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use num_bigint::BigUint;
use num_traits::Zero;
use tokio::task::spawn_blocking;
use tokio::time::sleep;
use tracing::debug;
use tycho_simulation::{
    protocol::models::ProtocolComponent,
    tycho_common::{
        models::{token::Token, Chain},
        simulation::protocol_sim::ProtocolSim,
        Bytes,
    },
};

use crate::models::erc4626::{
    component_direction_supported, component_is_erc4626, unsupported_direction_message,
};
use crate::models::state::AppState;

use super::allocation::allocate_swaps_by_bps;
use super::model::{
    NormalizedRouteInternal, ResimulatedHopInternal, ResimulatedRouteInternal,
    ResimulatedSegmentInternal, ResimulatedSwapInternal,
};
use super::request::{format_native_protocol_allowlist, is_native_protocol_allowlisted};
use super::EncodeError;

type CachedPoolEntry = (Arc<dyn ProtocolSim>, Arc<ProtocolComponent>);

struct SegmentSimState {
    next_amount_in: BigUint,
    hops: Vec<Option<ResimulatedHopInternal>>,
}

struct RouteResimulator<'a> {
    state: &'a AppState,
    normalized: &'a NormalizedRouteInternal,
    chain: Chain,
    segment_states: Vec<SegmentSimState>,
    token_cache: TokenCache<'a>,
    pool_cache: HashMap<String, CachedPoolEntry>,
}

struct BlockingSwapRequest<'a> {
    state: &'a AppState,
    pool_state: Arc<dyn ProtocolSim>,
    amount_in_for_sim: BigUint,
    token_in: Token,
    token_out: Token,
    pool_timeout: Duration,
    pool_id: &'a str,
    component: &'a ProtocolComponent,
}

pub(super) async fn resimulate_route(
    state: &AppState,
    normalized: &NormalizedRouteInternal,
    chain: Chain,
    request_token_in: &Bytes,
    request_token_out: &Bytes,
    native_token_protocol_allowlist: &[String],
) -> Result<ResimulatedRouteInternal, EncodeError> {
    let mut resimulator = RouteResimulator::new(state, normalized, chain);

    // Resimulate in hop-depth order to match build_route_swaps execution order.
    for hop_index in 0..resimulator.max_hop_depth() {
        for segment_index in 0..normalized.segments.len() {
            resimulator
                .resimulate_segment_hop(segment_index, hop_index)
                .await?;
        }
    }

    validate_request_tokens(request_token_in, request_token_out)?;
    validate_native_request_tokens(normalized, chain, native_token_protocol_allowlist)?;
    let segments = resimulator.build_resimulated_segments()?;
    Ok(ResimulatedRouteInternal { segments })
}

impl<'a> RouteResimulator<'a> {
    fn new(state: &'a AppState, normalized: &'a NormalizedRouteInternal, chain: Chain) -> Self {
        Self {
            state,
            normalized,
            chain,
            segment_states: initialize_segment_states(normalized),
            token_cache: TokenCache::new(state),
            pool_cache: HashMap::new(),
        }
    }

    fn max_hop_depth(&self) -> usize {
        self.normalized
            .segments
            .iter()
            .map(|segment| segment.hops.len())
            .max()
            .unwrap_or(0)
    }

    async fn resimulate_segment_hop(
        &mut self,
        segment_index: usize,
        hop_index: usize,
    ) -> Result<(), EncodeError> {
        let Some(hop) = self
            .normalized
            .segments
            .get(segment_index)
            .and_then(|segment| segment.hops.get(hop_index))
        else {
            return Ok(());
        };
        let hop_amount_in = {
            let segment_state = self
                .segment_states
                .get_mut(segment_index)
                .ok_or_else(|| EncodeError::internal("Segment state missing after resimulation"))?;
            require_hop_amount_in(segment_state, segment_index, hop_index)?
        };
        let allocated_swaps =
            allocate_swaps_by_bps(hop_amount_in.clone(), &hop.swaps, segment_index, hop_index)?;
        let (swap_results, hop_expected) = self
            .simulate_allocated_swaps(allocated_swaps, &hop_amount_in)
            .await?;
        // We come back for the segment state here because the pool work above awaits.
        let segment_state = self
            .segment_states
            .get_mut(segment_index)
            .ok_or_else(|| EncodeError::internal("Segment state missing after resimulation"))?;
        segment_state.hops[hop_index] = Some(ResimulatedHopInternal {
            token_in: hop.token_in.clone(),
            token_out: hop.token_out.clone(),
            amount_in: hop_amount_in,
            expected_amount_out: hop_expected.clone(),
            swaps: swap_results,
        });
        segment_state.next_amount_in = hop_expected;
        Ok(())
    }

    async fn simulate_allocated_swaps(
        &mut self,
        allocated_swaps: Vec<super::allocation::AllocatedSwap>,
        hop_amount_in: &BigUint,
    ) -> Result<(Vec<ResimulatedSwapInternal>, BigUint), EncodeError> {
        let mut swap_results = Vec::with_capacity(allocated_swaps.len());
        let mut hop_expected = BigUint::zero();

        for allocated in allocated_swaps {
            let swap = self.simulate_allocated_swap(allocated).await?;
            hop_expected += swap.expected_amount_out.clone();
            swap_results.push(swap);
        }

        if hop_expected.is_zero() {
            return Err(EncodeError::simulation(format!(
                "hop amountIn {} produced zero amountOut",
                hop_amount_in
            )));
        }

        Ok((swap_results, hop_expected))
    }

    async fn simulate_allocated_swap(
        &mut self,
        allocated: super::allocation::AllocatedSwap,
    ) -> Result<ResimulatedSwapInternal, EncodeError> {
        let pool_entry = self.load_pool_entry(&allocated.pool.component_id).await?;
        ensure_erc4626_swap_supported(
            &pool_entry.1,
            &allocated.pool.component_id,
            &allocated.token_in,
            &allocated.token_out,
            self.state.erc4626_deposits_enabled,
        )?;
        let keep_native_unwrapped = ensure_native_swap_supported(
            self.chain,
            &allocated.token_in,
            &allocated.token_out,
            &pool_entry.1,
            &allocated.pool.component_id,
            &self.state.native_token_protocol_allowlist,
        )?;
        let sim_token_in = map_swap_token(&allocated.token_in, self.chain, keep_native_unwrapped);
        let sim_token_out = map_swap_token(&allocated.token_out, self.chain, keep_native_unwrapped);
        let token_in = self.token_cache.get(&sim_token_in).await?;
        let token_out = self.token_cache.get(&sim_token_out).await?;
        let pool_timeout = pool_timeout_for_component(self.state, pool_entry.1.as_ref());
        let (pre_state, result) = simulate_swap_blocking(BlockingSwapRequest {
            state: self.state,
            pool_state: Arc::clone(&pool_entry.0),
            amount_in_for_sim: allocated.amount_in.clone(),
            token_in,
            token_out,
            pool_timeout,
            pool_id: &allocated.pool.component_id,
            component: pool_entry.1.as_ref(),
        })
        .await?;
        let expected_out =
            require_non_zero_amount_out(&allocated.pool.component_id, result.amount)?;
        // Reusing the same pool in one route should see the updated state from the prior swap.
        self.pool_cache.insert(
            allocated.pool.component_id.clone(),
            (Arc::from(result.new_state), Arc::clone(&pool_entry.1)),
        );
        Ok(ResimulatedSwapInternal {
            pool: allocated.pool,
            token_in: allocated.token_in,
            token_out: allocated.token_out,
            split_bps: allocated.split_bps,
            amount_in: allocated.amount_in,
            expected_amount_out: expected_out,
            pool_state: pre_state,
            component: pool_entry.1,
        })
    }

    async fn load_pool_entry(&mut self, pool_id: &str) -> Result<CachedPoolEntry, EncodeError> {
        if let Some(entry) = self.pool_cache.get(pool_id) {
            return Ok((Arc::clone(&entry.0), Arc::clone(&entry.1)));
        }

        let entry = self
            .state
            .pool_by_id(pool_id)
            .await
            .ok_or_else(|| EncodeError::not_found(format!("Pool {} not found", pool_id)))?;
        self.pool_cache.insert(pool_id.to_string(), entry.clone());
        Ok(entry)
    }

    fn build_resimulated_segments(
        &mut self,
    ) -> Result<Vec<ResimulatedSegmentInternal>, EncodeError> {
        build_resimulated_segments(self.normalized, &mut self.segment_states)
    }
}

fn initialize_segment_states(normalized: &NormalizedRouteInternal) -> Vec<SegmentSimState> {
    normalized
        .segments
        .iter()
        .map(|segment| SegmentSimState {
            next_amount_in: segment.amount_in.clone(),
            hops: (0..segment.hops.len()).map(|_| None).collect(),
        })
        .collect()
}

fn require_hop_amount_in(
    segment_state: &SegmentSimState,
    segment_index: usize,
    hop_index: usize,
) -> Result<BigUint, EncodeError> {
    let hop_amount_in = segment_state.next_amount_in.clone();
    if hop_amount_in.is_zero() {
        return Err(EncodeError::invalid(format!(
            "segment[{}].hop[{}] amountIn is zero",
            segment_index, hop_index
        )));
    }

    Ok(hop_amount_in)
}

fn pool_timeout_for_component(state: &AppState, component: &ProtocolComponent) -> Duration {
    if component.protocol_system.starts_with("vm:") {
        state.pool_timeout_vm()
    } else {
        state.pool_timeout_native()
    }
}

async fn simulate_swap_blocking(
    request: BlockingSwapRequest<'_>,
) -> Result<
    (
        Arc<dyn ProtocolSim>,
        tycho_simulation::tycho_common::simulation::protocol_sim::GetAmountOutResult,
    ),
    EncodeError,
> {
    let BlockingSwapRequest {
        state,
        pool_state,
        amount_in_for_sim,
        token_in,
        token_out,
        pool_timeout,
        pool_id,
        component,
    } = request;
    let permit = acquire_pool_permit(state, component).await?;
    let handle = spawn_blocking(move || {
        let _permit = permit;
        let pre_state = pool_state;
        let result = pre_state.get_amount_out(amount_in_for_sim, &token_in, &token_out);
        (pre_state, result)
    });
    tokio::pin!(handle);

    let (pre_state, result) = tokio::select! {
        res = handle.as_mut() => {
            res.map_err(|join_err| join_error(pool_id, join_err))?
        }
        _ = sleep(pool_timeout) => {
            handle.as_mut().abort();
            return Err(timeout_error(pool_id, pool_timeout));
        }
    };
    let result = result.map_err(|err| {
        EncodeError::simulation(format!("Pool {} simulation failed: {}", pool_id, err))
    })?;
    Ok((pre_state, result))
}

async fn acquire_pool_permit(
    state: &AppState,
    component: &ProtocolComponent,
) -> Result<tokio::sync::OwnedSemaphorePermit, EncodeError> {
    if component.protocol_system.starts_with("vm:") {
        state
            .vm_sim_semaphore()
            .acquire_owned()
            .await
            .map_err(|_| EncodeError::internal("VM pool semaphore closed"))
    } else {
        state
            .native_sim_semaphore()
            .acquire_owned()
            .await
            .map_err(|_| EncodeError::internal("Native pool semaphore closed"))
    }
}

fn join_error(pool_id: &str, join_err: tokio::task::JoinError) -> EncodeError {
    let reason = if join_err.is_panic() {
        "panicked"
    } else if join_err.is_cancelled() {
        "was cancelled"
    } else {
        "failed"
    };
    EncodeError::internal(format!(
        "Pool {} simulation task {}: {}",
        pool_id, reason, join_err
    ))
}

fn timeout_error(pool_id: &str, pool_timeout: Duration) -> EncodeError {
    EncodeError::simulation(format!(
        "Pool {} simulation timed out after {}ms",
        pool_id,
        pool_timeout.as_millis()
    ))
}

fn require_non_zero_amount_out(
    pool_id: &str,
    expected_out: BigUint,
) -> Result<BigUint, EncodeError> {
    if expected_out.is_zero() {
        return Err(EncodeError::simulation(format!(
            "Pool {} returned zero amountOut",
            pool_id
        )));
    }

    Ok(expected_out)
}

fn build_resimulated_segments(
    normalized: &NormalizedRouteInternal,
    segment_states: &mut [SegmentSimState],
) -> Result<Vec<ResimulatedSegmentInternal>, EncodeError> {
    let mut resim_segments = Vec::with_capacity(normalized.segments.len());
    for (segment_index, segment) in normalized.segments.iter().enumerate() {
        let resim_hops = take_segment_hops(segment_states, segment_index, segment.hops.len())?;
        let last_hop = resim_hops
            .last()
            .ok_or_else(|| EncodeError::internal("Segment hops missing after resimulation"))?;
        resim_segments.push(ResimulatedSegmentInternal {
            share_bps: segment.share_bps,
            amount_in: segment.amount_in.clone(),
            expected_amount_out: last_hop.expected_amount_out.clone(),
            hops: resim_hops,
        });
    }
    Ok(resim_segments)
}

fn take_segment_hops(
    segment_states: &mut [SegmentSimState],
    segment_index: usize,
    hop_len: usize,
) -> Result<Vec<ResimulatedHopInternal>, EncodeError> {
    let segment_state = segment_states
        .get_mut(segment_index)
        .ok_or_else(|| EncodeError::internal("Segment state missing after resimulation"))?;
    let mut resim_hops = Vec::with_capacity(hop_len);
    for hop_index in 0..hop_len {
        let resim_hop = segment_state.hops[hop_index]
            .take()
            .ok_or_else(|| EncodeError::internal("Segment hops missing after resimulation"))?;
        resim_hops.push(resim_hop);
    }
    Ok(resim_hops)
}

fn validate_request_tokens(
    request_token_in: &Bytes,
    request_token_out: &Bytes,
) -> Result<(), EncodeError> {
    if request_token_in.is_empty() || request_token_out.is_empty() {
        return Err(EncodeError::internal(
            "Request tokens missing after resimulation",
        ));
    }

    Ok(())
}

fn ensure_native_swap_supported(
    chain: Chain,
    token_in: &Bytes,
    token_out: &Bytes,
    component: &ProtocolComponent,
    component_id: &str,
    native_token_protocol_allowlist: &[String],
) -> Result<bool, EncodeError> {
    let native_address = chain.native_token().address;
    let swap_uses_native = *token_in == native_address || *token_out == native_address;
    let protocol_supports_native =
        is_native_protocol_allowlisted(&component.protocol_system, native_token_protocol_allowlist);

    if swap_uses_native && !protocol_supports_native {
        let supported = format_native_protocol_allowlist(native_token_protocol_allowlist);
        return Err(EncodeError::invalid(format!(
            "native tokenIn/tokenOut is only supported for protocols [{}]; pool {} uses {}",
            supported, component_id, component.protocol_system
        )));
    }

    Ok(protocol_supports_native)
}

fn validate_native_request_tokens(
    normalized: &NormalizedRouteInternal,
    chain: Chain,
    native_token_protocol_allowlist: &[String],
) -> Result<(), EncodeError> {
    let native_address = chain.native_token().address;

    for segment in &normalized.segments {
        for hop in &segment.hops {
            let swap_uses_native =
                hop.token_in == native_address || hop.token_out == native_address;
            if !swap_uses_native {
                continue;
            }

            for swap in &hop.swaps {
                if !is_native_protocol_allowlisted(
                    &swap.pool.protocol,
                    native_token_protocol_allowlist,
                ) {
                    let supported =
                        format_native_protocol_allowlist(native_token_protocol_allowlist);
                    return Err(EncodeError::invalid(format!(
                        "native tokenIn/tokenOut is only supported for protocols [{}]; got {}",
                        supported, swap.pool.protocol
                    )));
                }
            }
        }
    }

    Ok(())
}

fn ensure_erc4626_swap_supported(
    component: &ProtocolComponent,
    pool_id: &str,
    token_in: &Bytes,
    token_out: &Bytes,
    erc4626_deposits_enabled: bool,
) -> Result<(), EncodeError> {
    if !component_is_erc4626(component) {
        return Ok(());
    }
    if component_direction_supported(component, token_in, token_out, erc4626_deposits_enabled) {
        return Ok(());
    }

    debug!(
        protocol = component.protocol_system.as_str(),
        pool_id,
        component_id = %component.id,
        token_in = token_in.to_string(),
        token_out = token_out.to_string(),
        "Rejecting unsupported ERC4626 encode hop during resimulation"
    );
    Err(EncodeError::invalid(unsupported_direction_message(
        token_in,
        token_out,
        erc4626_deposits_enabled,
    )))
}

fn map_swap_token(address: &Bytes, chain: Chain, keep_native_unwrapped: bool) -> Bytes {
    if !keep_native_unwrapped && *address == chain.native_token().address {
        chain.wrapped_native_token().address
    } else {
        address.clone()
    }
}

struct TokenCache<'a> {
    state: &'a AppState,
    cache: HashMap<Bytes, Token>,
}

impl<'a> TokenCache<'a> {
    fn new(state: &'a AppState) -> Self {
        Self {
            state,
            cache: HashMap::new(),
        }
    }

    async fn get(&mut self, address: &Bytes) -> Result<Token, EncodeError> {
        if let Some(token) = self.cache.get(address) {
            return Ok(token.clone());
        }
        let token = self
            .state
            .tokens
            .ensure(address)
            .await
            .map_err(|err| EncodeError::simulation(format!("Token lookup failed: {}", err)))?
            .ok_or_else(|| EncodeError::invalid("Token not found"))?;
        self.cache.insert(address.clone(), token.clone());
        Ok(token)
    }
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    reason = "resimulation fixtures use deterministic addresses and test doubles"
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
    use std::collections::HashMap;
    use std::time::Duration;

    use tycho_simulation::protocol::models::Update;

    use super::*;
    use crate::services::encode::fixtures::{
        component_with_protocol, component_with_tokens, dummy_token, pool_ref, test_app_state,
        test_state_stores, token_store_with_tokens, TestAppStateConfig,
    };
    use crate::services::encode::mocks::{step_multiplier, StepProtocolSim};
    use crate::services::encode::model::{
        NormalizedHopInternal, NormalizedSegmentInternal, NormalizedSwapDraftInternal,
    };

    #[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
    struct SlowProtocolSim {
        sleep_for: Duration,
    }

    #[test]
    fn map_swap_token_wraps_native_for_non_allowlisted_protocols() {
        let chain = Chain::Ethereum;
        let native = chain.native_token().address;
        let mapped = map_swap_token(&native, chain, false);
        assert_eq!(mapped, chain.wrapped_native_token().address);
    }

    #[test]
    fn map_swap_token_keeps_native_for_allowlisted_protocols() {
        let chain = Chain::Ethereum;
        let native = chain.native_token().address;
        let mapped = map_swap_token(&native, chain, true);
        assert_eq!(mapped, native);
    }

    #[typetag::serde]
    impl ProtocolSim for SlowProtocolSim {
        fn fee(&self) -> f64 {
            0.0
        }

        fn spot_price(
            &self,
            _base: &Token,
            _quote: &Token,
        ) -> Result<f64, tycho_simulation::tycho_common::simulation::errors::SimulationError>
        {
            Ok(0.0)
        }

        fn get_amount_out(
            &self,
            amount_in: BigUint,
            _token_in: &Token,
            _token_out: &Token,
        ) -> Result<
            tycho_simulation::tycho_common::simulation::protocol_sim::GetAmountOutResult,
            tycho_simulation::tycho_common::simulation::errors::SimulationError,
        > {
            std::thread::sleep(self.sleep_for);
            Ok(
                tycho_simulation::tycho_common::simulation::protocol_sim::GetAmountOutResult::new(
                    amount_in,
                    BigUint::zero(),
                    self.clone_box(),
                ),
            )
        }

        fn get_limits(
            &self,
            _sell_token: Bytes,
            _buy_token: Bytes,
        ) -> Result<
            (BigUint, BigUint),
            tycho_simulation::tycho_common::simulation::errors::SimulationError,
        > {
            Ok((BigUint::zero(), BigUint::zero()))
        }

        fn delta_transition(
            &mut self,
            _delta: tycho_simulation::tycho_common::dto::ProtocolStateDelta,
            _tokens: &HashMap<Bytes, Token>,
            _balances: &tycho_simulation::tycho_common::simulation::protocol_sim::Balances,
        ) -> Result<(), tycho_simulation::tycho_common::simulation::errors::TransitionError>
        {
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn ProtocolSim> {
            Box::new(Self {
                sleep_for: self.sleep_for,
            })
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }

        fn eq(&self, other: &dyn ProtocolSim) -> bool {
            other
                .as_any()
                .downcast_ref::<SlowProtocolSim>()
                .is_some_and(|rhs| rhs.sleep_for == self.sleep_for)
        }
    }

    fn timeout_test_config(
        enable_vm_pools: bool,
        enable_rfq_pools: bool,
        pool_timeout_native: Duration,
        pool_timeout_vm: Duration,
        pool_timeout_rfq: Duration,
    ) -> TestAppStateConfig {
        TestAppStateConfig {
            enable_vm_pools,
            enable_rfq_pools,
            erc4626_deposits_enabled: false,
            quote_timeout: Duration::from_millis(1000),
            pool_timeout_native,
            pool_timeout_vm,
            pool_timeout_rfq,
            request_timeout: Duration::from_millis(1000),
        }
    }

    fn assert_timeout_error(err: EncodeError, pool_id: &str) {
        assert_eq!(
            err.kind(),
            crate::services::encode::EncodeErrorKind::Simulation
        );
        assert!(
            err.message().contains(pool_id) && err.message().contains("timed out"),
            "unexpected error: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn resimulate_route_advances_pool_state_for_repeated_swaps() {
        let token_in = dummy_token("0x0000000000000000000000000000000000000001");
        let token_out = dummy_token("0x0000000000000000000000000000000000000002");
        let tokens_store = token_store_with_tokens(vec![token_in.clone(), token_out.clone()]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            test_state_stores(&tokens_store);

        let component = component_with_tokens(
            "0x0000000000000000000000000000000000000009",
            vec![token_in.clone(), token_out.clone()],
        );
        let mut states = HashMap::new();
        states.insert(
            "pool-1".to_string(),
            Box::new(StepProtocolSim { multiplier: 1 }) as Box<dyn ProtocolSim>,
        );
        let mut new_pairs = HashMap::new();
        new_pairs.insert("pool-1".to_string(), component);
        let update = Update::new(1, states, new_pairs);
        native_state_store.apply_update(update).await;

        let app_state = test_app_state(
            tokens_store,
            native_state_store,
            vm_state_store,
            rfq_state_store,
            TestAppStateConfig::default(),
        );

        let normalized = NormalizedRouteInternal {
            segments: vec![NormalizedSegmentInternal {
                share_bps: 10_000,
                amount_in: BigUint::from(10u32),
                hops: vec![NormalizedHopInternal {
                    token_in: token_in.address.clone(),
                    token_out: token_out.address.clone(),
                    swaps: vec![
                        NormalizedSwapDraftInternal {
                            pool: pool_ref("pool-1"),
                            token_in: token_in.address.clone(),
                            token_out: token_out.address.clone(),
                            split_bps: 5000,
                        },
                        NormalizedSwapDraftInternal {
                            pool: pool_ref("pool-1"),
                            token_in: token_in.address.clone(),
                            token_out: token_out.address.clone(),
                            split_bps: 0,
                        },
                    ],
                }],
            }],
        };

        let resimulated = resimulate_route(
            &app_state,
            &normalized,
            Chain::Ethereum,
            &token_in.address,
            &token_out.address,
            &app_state.native_token_protocol_allowlist,
        )
        .await
        .unwrap();

        let swaps = &resimulated.segments[0].hops[0].swaps;
        assert_eq!(swaps[0].expected_amount_out, BigUint::from(5u32));
        assert_eq!(swaps[1].expected_amount_out, BigUint::from(10u32));
        assert_eq!(step_multiplier(&swaps[0].pool_state), 1);
        assert_eq!(step_multiplier(&swaps[1].pool_state), 2);
    }

    #[tokio::test]
    async fn resimulate_route_requires_erc4626_deposit_capability_for_type_name_only_component() {
        let token_in = dummy_token("0xdC035D45d973E3EC169d2276DDab16f1e407384F");
        let token_out = dummy_token("0xa3931d71877c0e7a3148cb7eb4463524fec27fbd");
        let tokens_store = token_store_with_tokens(vec![token_in.clone(), token_out.clone()]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            test_state_stores(&tokens_store);

        let component = component_with_protocol(
            "0xa3931d71877c0e7a3148cb7eb4463524fec27fbd",
            "",
            "erc4626_pool",
            vec![token_in.clone(), token_out.clone()],
        );
        let mut states = HashMap::new();
        states.insert(
            "pool-erc4626".to_string(),
            Box::new(StepProtocolSim { multiplier: 1 }) as Box<dyn ProtocolSim>,
        );
        let mut new_pairs = HashMap::new();
        new_pairs.insert("pool-erc4626".to_string(), component);
        native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let normalized = NormalizedRouteInternal {
            segments: vec![NormalizedSegmentInternal {
                share_bps: 10_000,
                amount_in: BigUint::from(10u32),
                hops: vec![NormalizedHopInternal {
                    token_in: token_in.address.clone(),
                    token_out: token_out.address.clone(),
                    swaps: vec![NormalizedSwapDraftInternal {
                        pool: pool_ref("pool-erc4626"),
                        token_in: token_in.address.clone(),
                        token_out: token_out.address.clone(),
                        split_bps: 0,
                    }],
                }],
            }],
        };

        let disabled_state = test_app_state(
            Arc::clone(&tokens_store),
            Arc::clone(&native_state_store),
            Arc::clone(&vm_state_store),
            Arc::clone(&rfq_state_store),
            TestAppStateConfig::default(),
        );
        let err = match resimulate_route(
            &disabled_state,
            &normalized,
            Chain::Ethereum,
            &token_in.address,
            &token_out.address,
            &disabled_state.native_token_protocol_allowlist,
        )
        .await
        {
            Ok(_) => panic!("deposit should be rejected when deposits are disabled"),
            Err(err) => err,
        };
        assert_eq!(
            err.kind(),
            crate::services::encode::EncodeErrorKind::InvalidRequest
        );

        let enabled_state = test_app_state(
            tokens_store,
            native_state_store,
            vm_state_store,
            rfq_state_store,
            TestAppStateConfig {
                erc4626_deposits_enabled: true,
                ..TestAppStateConfig::default()
            },
        );
        let resimulated = match resimulate_route(
            &enabled_state,
            &normalized,
            Chain::Ethereum,
            &token_in.address,
            &token_out.address,
            &enabled_state.native_token_protocol_allowlist,
        )
        .await
        {
            Ok(resimulated) => resimulated,
            Err(err) => panic!(
                "deposit should resimulate when deposits are enabled: {}",
                err.message()
            ),
        };

        assert_eq!(
            resimulated.segments[0].hops[0].swaps[0].expected_amount_out,
            BigUint::from(10u32)
        );
    }

    #[tokio::test]
    async fn resimulate_route_orders_by_hop_depth_across_segments() {
        let token_a = dummy_token("0x0000000000000000000000000000000000000001");
        let token_b = dummy_token("0x0000000000000000000000000000000000000002");
        let token_c = dummy_token("0x0000000000000000000000000000000000000003");
        let tokens_store =
            token_store_with_tokens(vec![token_a.clone(), token_b.clone(), token_c.clone()]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            test_state_stores(&tokens_store);

        let component_a = component_with_tokens(
            "0x0000000000000000000000000000000000000009",
            vec![token_a.clone(), token_b.clone()],
        );
        let component_shared = component_with_tokens(
            "0x000000000000000000000000000000000000000a",
            vec![token_a.clone(), token_b.clone(), token_c.clone()],
        );

        let mut states = HashMap::new();
        states.insert(
            "pool-a".to_string(),
            Box::new(StepProtocolSim { multiplier: 1 }) as Box<dyn ProtocolSim>,
        );
        states.insert(
            "pool-shared".to_string(),
            Box::new(StepProtocolSim { multiplier: 1 }) as Box<dyn ProtocolSim>,
        );
        let mut new_pairs = HashMap::new();
        new_pairs.insert("pool-a".to_string(), component_a);
        new_pairs.insert("pool-shared".to_string(), component_shared);
        let update = Update::new(1, states, new_pairs);
        native_state_store.apply_update(update).await;

        let app_state = test_app_state(
            tokens_store,
            native_state_store,
            vm_state_store,
            rfq_state_store,
            TestAppStateConfig::default(),
        );

        let normalized = NormalizedRouteInternal {
            segments: vec![
                NormalizedSegmentInternal {
                    share_bps: 6_000,
                    amount_in: BigUint::from(100u32),
                    hops: vec![
                        NormalizedHopInternal {
                            token_in: token_a.address.clone(),
                            token_out: token_b.address.clone(),
                            swaps: vec![NormalizedSwapDraftInternal {
                                pool: pool_ref("pool-a"),
                                token_in: token_a.address.clone(),
                                token_out: token_b.address.clone(),
                                split_bps: 0,
                            }],
                        },
                        NormalizedHopInternal {
                            token_in: token_b.address.clone(),
                            token_out: token_c.address.clone(),
                            swaps: vec![NormalizedSwapDraftInternal {
                                pool: pool_ref("pool-shared"),
                                token_in: token_b.address.clone(),
                                token_out: token_c.address.clone(),
                                split_bps: 0,
                            }],
                        },
                    ],
                },
                NormalizedSegmentInternal {
                    share_bps: 4_000,
                    amount_in: BigUint::from(50u32),
                    hops: vec![NormalizedHopInternal {
                        token_in: token_a.address.clone(),
                        token_out: token_b.address.clone(),
                        swaps: vec![NormalizedSwapDraftInternal {
                            pool: pool_ref("pool-shared"),
                            token_in: token_a.address.clone(),
                            token_out: token_b.address.clone(),
                            split_bps: 0,
                        }],
                    }],
                },
            ],
        };

        let resimulated = resimulate_route(
            &app_state,
            &normalized,
            Chain::Ethereum,
            &token_a.address,
            &token_c.address,
            &app_state.native_token_protocol_allowlist,
        )
        .await
        .unwrap();

        let seg_a_hop1_swap = &resimulated.segments[0].hops[1].swaps[0];
        let seg_b_hop0_swap = &resimulated.segments[1].hops[0].swaps[0];

        assert_eq!(seg_b_hop0_swap.expected_amount_out, BigUint::from(50u32));
        assert_eq!(step_multiplier(&seg_b_hop0_swap.pool_state), 1);
        assert_eq!(seg_a_hop1_swap.expected_amount_out, BigUint::from(200u32));
        assert_eq!(step_multiplier(&seg_a_hop1_swap.pool_state), 2);
    }

    #[tokio::test]
    async fn resimulate_route_times_out_native_pool_simulation() {
        let token_in = dummy_token("0x0000000000000000000000000000000000000001");
        let token_out = dummy_token("0x0000000000000000000000000000000000000002");
        let tokens_store = token_store_with_tokens(vec![token_in.clone(), token_out.clone()]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            test_state_stores(&tokens_store);

        let component = component_with_tokens(
            "0x0000000000000000000000000000000000000009",
            vec![token_in.clone(), token_out.clone()],
        );
        let mut states = HashMap::new();
        states.insert(
            "pool-slow".to_string(),
            Box::new(SlowProtocolSim {
                sleep_for: Duration::from_millis(80),
            }) as Box<dyn ProtocolSim>,
        );
        let mut new_pairs = HashMap::new();
        new_pairs.insert("pool-slow".to_string(), component);
        native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = test_app_state(
            tokens_store,
            native_state_store,
            vm_state_store,
            rfq_state_store,
            timeout_test_config(
                false,
                false,
                Duration::from_millis(10),
                Duration::from_millis(1000),
                Duration::from_millis(1000),
            ),
        );

        let normalized = NormalizedRouteInternal {
            segments: vec![NormalizedSegmentInternal {
                share_bps: 10_000,
                amount_in: BigUint::from(10u32),
                hops: vec![NormalizedHopInternal {
                    token_in: token_in.address.clone(),
                    token_out: token_out.address.clone(),
                    swaps: vec![NormalizedSwapDraftInternal {
                        pool: pool_ref("pool-slow"),
                        token_in: token_in.address.clone(),
                        token_out: token_out.address.clone(),
                        split_bps: 0,
                    }],
                }],
            }],
        };

        let err = match resimulate_route(
            &app_state,
            &normalized,
            Chain::Ethereum,
            &token_in.address,
            &token_out.address,
            &app_state.native_token_protocol_allowlist,
        )
        .await
        {
            Ok(_) => panic!("Expected pool simulation timeout"),
            Err(err) => err,
        };

        assert_timeout_error(err, "pool-slow");
    }

    #[tokio::test]
    async fn resimulate_route_times_out_vm_pool_simulation() {
        let token_in = dummy_token("0x0000000000000000000000000000000000000001");
        let token_out = dummy_token("0x0000000000000000000000000000000000000002");
        let tokens_store = token_store_with_tokens(vec![token_in.clone(), token_out.clone()]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            test_state_stores(&tokens_store);

        let component = component_with_protocol(
            "0x0000000000000000000000000000000000000009",
            "vm:curve",
            "curve_pool",
            vec![token_in.clone(), token_out.clone()],
        );

        let mut states = HashMap::new();
        states.insert(
            "pool-vm-slow".to_string(),
            Box::new(SlowProtocolSim {
                sleep_for: Duration::from_millis(80),
            }) as Box<dyn ProtocolSim>,
        );
        let mut new_pairs = HashMap::new();
        new_pairs.insert("pool-vm-slow".to_string(), component);
        vm_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = test_app_state(
            tokens_store,
            native_state_store,
            vm_state_store,
            rfq_state_store,
            timeout_test_config(
                true,
                false,
                Duration::from_millis(1000),
                Duration::from_millis(10),
                Duration::from_millis(10),
            ),
        );

        let normalized = NormalizedRouteInternal {
            segments: vec![NormalizedSegmentInternal {
                share_bps: 10_000,
                amount_in: BigUint::from(10u32),
                hops: vec![NormalizedHopInternal {
                    token_in: token_in.address.clone(),
                    token_out: token_out.address.clone(),
                    swaps: vec![NormalizedSwapDraftInternal {
                        pool: pool_ref("pool-vm-slow"),
                        token_in: token_in.address.clone(),
                        token_out: token_out.address.clone(),
                        split_bps: 0,
                    }],
                }],
            }],
        };

        let err = match resimulate_route(
            &app_state,
            &normalized,
            Chain::Ethereum,
            &token_in.address,
            &token_out.address,
            &app_state.native_token_protocol_allowlist,
        )
        .await
        {
            Ok(_) => panic!("Expected VM pool simulation timeout"),
            Err(err) => err,
        };

        assert_timeout_error(err, "pool-vm-slow");
    }

    #[tokio::test]
    async fn token_cache_surfaces_lookup_request_failures() {
        let missing_token = dummy_token("0x0000000000000000000000000000000000000002");
        let tokens_store = Arc::new(crate::models::tokens::TokenStore::new(
            HashMap::new(),
            "http://127.0.0.1:9".to_string(),
            "test".to_string(),
            Chain::Ethereum,
            Duration::from_millis(10),
        ));
        let (native_state_store, vm_state_store, rfq_state_store) =
            test_state_stores(&tokens_store);
        let app_state = test_app_state(
            tokens_store,
            native_state_store,
            vm_state_store,
            rfq_state_store,
            TestAppStateConfig::default(),
        );
        let mut cache = TokenCache::new(&app_state);

        let err = match cache.get(&missing_token.address).await {
            Ok(_) => panic!("Expected token cache miss to fail when RPC fetch is unavailable"),
            Err(err) => err,
        };

        assert_eq!(
            err.kind(),
            crate::services::encode::EncodeErrorKind::Simulation
        );
        assert!(err.message().contains("Token lookup failed"));
    }
}
