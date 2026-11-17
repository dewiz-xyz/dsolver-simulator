use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use num_bigint::BigUint;
use num_traits::Zero;
use tracing::debug;
use tycho_simulation::{
    protocol::models::ProtocolComponent,
    rfq::protocols::{
        bebop::state::BebopState, hashflow::state::HashflowState, liquorice::state::LiquoriceState,
    },
    tycho_common::{
        models::{token::Token, Chain},
        simulation::protocol_sim::ProtocolSim,
        Bytes,
    },
};

use crate::models::erc4626::{
    component_direction_supported, component_is_erc4626, unsupported_direction_message,
};
use crate::models::state::{AppState, PublishedStatePin, RfqClientConfig, SimulationRebuildGuard};
use crate::services::simulation_executor::{SimulationExecutionError, SimulationExecutor};
use crate::services::stream_builder::ENCODE_RFQ_QUOTE_TIMEOUT;

use super::allocation::allocate_swaps_by_bps;
use super::backend::PoolBackend;
use super::model::{
    NormalizedRouteInternal, ResimulatedHopInternal, ResimulatedRouteInternal,
    ResimulatedSegmentInternal, ResimulatedSwapInternal,
};
use super::request::{format_native_protocol_allowlist, is_native_protocol_allowlisted};
use super::EncodeError;

#[derive(Clone)]
struct CachedPoolEntry {
    pool_state: Arc<dyn ProtocolSim>,
    component: Arc<ProtocolComponent>,
    backend: PoolBackend,
}

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
    rebuild_guard: Arc<SimulationRebuildGuard>,
    native_pin: Option<PublishedStatePin>,
}

struct SwapSimulationRequest {
    pool_state: Arc<dyn ProtocolSim>,
    amount_in_for_sim: BigUint,
    token_in: Token,
    token_out: Token,
    pool_id: String,
    rebuild_guard: Option<Arc<SimulationRebuildGuard>>,
}

#[cfg(test)]
async fn resimulate_route(
    state: &AppState,
    normalized: &NormalizedRouteInternal,
    chain: Chain,
    request_token_in: &Bytes,
    request_token_out: &Bytes,
    native_token_protocol_allowlist: &[String],
    rebuild_guard: Arc<SimulationRebuildGuard>,
) -> Result<ResimulatedRouteInternal, EncodeError> {
    let native_pin = Some(state.native_state_store.pin().await);
    resimulate_route_with_native_pin(
        state,
        normalized,
        chain,
        request_token_in,
        request_token_out,
        native_token_protocol_allowlist,
        rebuild_guard,
        native_pin,
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "route, chain, token, guard, and pinned-state inputs remain explicit at the request boundary"
)]
pub(super) async fn resimulate_route_with_native_pin(
    state: &AppState,
    normalized: &NormalizedRouteInternal,
    chain: Chain,
    request_token_in: &Bytes,
    request_token_out: &Bytes,
    native_token_protocol_allowlist: &[String],
    rebuild_guard: Arc<SimulationRebuildGuard>,
    native_pin: Option<PublishedStatePin>,
) -> Result<ResimulatedRouteInternal, EncodeError> {
    let mut resimulator =
        RouteResimulator::new(state, normalized, chain, rebuild_guard, native_pin);

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
    fn new(
        state: &'a AppState,
        normalized: &'a NormalizedRouteInternal,
        chain: Chain,
        rebuild_guard: Arc<SimulationRebuildGuard>,
        native_pin: Option<PublishedStatePin>,
    ) -> Self {
        Self {
            state,
            normalized,
            chain,
            segment_states: initialize_segment_states(normalized),
            token_cache: TokenCache::new(state),
            pool_cache: HashMap::new(),
            rebuild_guard,
            native_pin,
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
            self.state,
            &pool_entry.component,
            &allocated.pool.component_id,
            &allocated.token_in,
            &allocated.token_out,
            self.state.erc4626_deposits_enabled,
        )?;
        let keep_native_unwrapped = ensure_native_swap_supported(
            self.chain,
            &allocated.token_in,
            &allocated.token_out,
            &pool_entry.component,
            &allocated.pool.component_id,
            &self.state.native_token_protocol_allowlist,
        )?;
        let sim_token_in = map_swap_token(&allocated.token_in, self.chain, keep_native_unwrapped);
        let sim_token_out = map_swap_token(&allocated.token_out, self.chain, keep_native_unwrapped);
        let native_pin = pool_entry
            .backend
            .is_native()
            .then_some(self.native_pin.as_ref())
            .flatten();
        let token_in = self.token_cache.get(&sim_token_in, native_pin).await?;
        let token_out = self.token_cache.get(&sim_token_out, native_pin).await?;
        let (pre_state, result) = simulate_swap(SwapSimulationRequest {
            pool_state: Arc::clone(&pool_entry.pool_state),
            amount_in_for_sim: allocated.amount_in.clone(),
            token_in,
            token_out,
            pool_id: allocated.pool.component_id.clone(),
            rebuild_guard: pool_entry
                .backend
                .uses_rebuild_guard()
                .then(|| Arc::clone(&self.rebuild_guard)),
        })
        .await?;
        let expected_out =
            require_non_zero_amount_out(&allocated.pool.component_id, result.amount)?;
        // Reusing the same pool in one route should see the updated state from the prior swap.
        self.pool_cache.insert(
            allocated.pool.component_id.clone(),
            CachedPoolEntry {
                pool_state: Arc::from(result.new_state),
                component: Arc::clone(&pool_entry.component),
                backend: pool_entry.backend,
            },
        );
        Ok(ResimulatedSwapInternal {
            pool: allocated.pool,
            backend: pool_entry.backend,
            token_in: allocated.token_in,
            token_out: allocated.token_out,
            split_bps: allocated.split_bps,
            amount_in: allocated.amount_in,
            expected_amount_out: expected_out,
            pool_state: pre_state,
            component: pool_entry.component,
        })
    }

    async fn load_pool_entry(&mut self, pool_id: &str) -> Result<CachedPoolEntry, EncodeError> {
        if let Some(entry) = self.pool_cache.get(pool_id) {
            return Ok(entry.clone());
        }

        if let Some((pool_state, component)) = self
            .native_pin
            .as_ref()
            .and_then(|pin| pin.pool_by_id(pool_id))
        {
            let entry = CachedPoolEntry {
                backend: PoolBackend::from_component(component.as_ref()),
                pool_state,
                component,
            };
            self.pool_cache.insert(pool_id.to_string(), entry.clone());
            return Ok(entry);
        }

        if let Some((uses_vm, uses_rfq)) = self
            .state
            .pool_rebuild_guard_requirements(pool_id)
            .await
            .map_err(availability_error)?
        {
            if (uses_vm || uses_rfq)
                && !self
                    .rebuild_guard
                    .blocks_required_rebuilds(uses_vm, uses_rfq)
            {
                self.rebuild_guard = self
                    .state
                    .acquire_simulation_rebuild_guard(uses_vm, uses_rfq)
                    .await;
            }
        }

        let (pool_state, component) = self
            .state
            .pool_by_id(pool_id)
            .await
            .map_err(availability_error)?
            .ok_or_else(|| EncodeError::not_found(format!("Pool {} not found", pool_id)))?;
        let backend = PoolBackend::from_component(component.as_ref());
        let pool_state = if backend.is_rfq() {
            hydrate_rfq_pool_state(
                pool_id,
                pool_state.as_ref(),
                self.chain,
                &self.state.rfq_client_config,
                encode_rfq_quote_timeout(self.state.request_timeout()),
            )?
        } else {
            pool_state
        };

        let entry = CachedPoolEntry {
            backend,
            pool_state,
            component,
        };
        self.pool_cache.insert(pool_id.to_string(), entry.clone());
        Ok(entry)
    }

    fn build_resimulated_segments(
        &mut self,
    ) -> Result<Vec<ResimulatedSegmentInternal>, EncodeError> {
        build_resimulated_segments(self.normalized, &mut self.segment_states)
    }
}

// Kept below the firm-quote timeout so a clamped client still gets most of the request budget.
const ENCODE_QUOTE_TIMEOUT_HEADROOM: Duration = Duration::from_millis(200);

// The firm quote blocks the encode task through block_in_place, so the outer request guard cannot
// interrupt it. Clamp the quote timeout below the configured guard so the guard stays authoritative
// even if REQUEST_TIMEOUT_MS is lowered below the static cap.
fn encode_rfq_quote_timeout(request_timeout: Duration) -> Duration {
    ENCODE_RFQ_QUOTE_TIMEOUT.min(request_timeout.saturating_sub(ENCODE_QUOTE_TIMEOUT_HEADROOM))
}

fn hydrate_rfq_pool_state(
    pool_id: &str,
    pool_state: &dyn ProtocolSim,
    chain: Chain,
    config: &RfqClientConfig,
    quote_timeout: Duration,
) -> Result<Arc<dyn ProtocolSim>, EncodeError> {
    let bebop = pool_state.as_any().is::<BebopState>();
    if bebop || pool_state.as_any().is::<HashflowState>() {
        return super::rfq_hydration::hydrate(pool_state, chain, config, quote_timeout, bebop)
            .map_err(|error| {
                rfq_hydration_error(pool_id, if bebop { "Bebop" } else { "Hashflow" }, error)
            });
    }
    if pool_state.as_any().is::<LiquoriceState>() {
        return Err(EncodeError::invalid(
            "Liquorice encoding is not supported by the configured router",
        ));
    }
    if pool_state
        .as_any()
        .is::<tycho_simulation::rfq::protocols::native::state::NativeState>()
        || pool_state
            .as_any()
            .is::<tycho_simulation::pamm::protocols::metric::state::MetricState>()
    {
        return Err(EncodeError::invalid(
            "Native and Metric are broadcast-only providers; encoding is not enabled",
        ));
    }
    Err(EncodeError::internal(format!(
        "RFQ pool {pool_id} has unsupported state type"
    )))
}

fn rfq_hydration_error(
    pool_id: &str,
    provider: &str,
    error: impl std::fmt::Display,
) -> EncodeError {
    EncodeError::internal(format!(
        "Failed to hydrate {provider} client for RFQ pool {pool_id}: {error}"
    ))
}

fn availability_error(availability: crate::models::state::EncodeAvailability) -> EncodeError {
    EncodeError::unavailable(
        availability.availability_message().unwrap_or_else(|| {
            unreachable!("unready pool lookup must have an availability message")
        }),
    )
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

async fn simulate_swap(
    request: SwapSimulationRequest,
) -> Result<
    (
        Arc<dyn ProtocolSim>,
        tycho_simulation::tycho_common::simulation::protocol_sim::GetAmountOutResult,
    ),
    EncodeError,
> {
    let SwapSimulationRequest {
        pool_state,
        amount_in_for_sim,
        token_in,
        token_out,
        pool_id,
        rebuild_guard,
    } = request;
    let failure_pool_id = pool_id.clone();
    let (pre_state, result) = SimulationExecutor::new()
        .run(move || {
            let _rebuild_guard = rebuild_guard;
            let pre_state = pool_state;
            let result = pre_state.get_amount_out(amount_in_for_sim, &token_in, &token_out);
            (pre_state, result)
        })
        .await
        .map_err(|err| execution_error(&failure_pool_id, err))?;

    let result = result.map_err(|err| {
        EncodeError::simulation(format!("Pool {} simulation failed: {}", pool_id, err))
    })?;
    Ok((pre_state, result))
}

fn execution_error(pool_id: &str, err: SimulationExecutionError) -> EncodeError {
    match err {
        SimulationExecutionError::WorkerPanicked(_) => {
            EncodeError::internal(format!("Pool {} simulation worker panicked", pool_id))
        }
        SimulationExecutionError::ResultDropped => {
            EncodeError::internal(format!("Pool {} simulation result was dropped", pool_id))
        }
    }
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
    state: &AppState,
    component: &ProtocolComponent,
    pool_id: &str,
    token_in: &Bytes,
    token_out: &Bytes,
    erc4626_deposits_enabled: bool,
) -> Result<(), EncodeError> {
    if !component_is_erc4626(component) {
        return Ok(());
    }
    if component_direction_supported(
        component,
        token_in,
        token_out,
        erc4626_deposits_enabled,
        &state.erc4626_pair_policies,
    ) {
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
        &state.erc4626_pair_policies,
    )))
}

fn map_swap_token(address: &Bytes, chain: Chain, keep_native_unwrapped: bool) -> Bytes {
    if !keep_native_unwrapped && *address == chain.native_token().address {
        chain
            .wrapped_native_token()
            .map_or_else(|| address.clone(), |token| token.address)
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

    async fn get(
        &mut self,
        address: &Bytes,
        native_pin: Option<&PublishedStatePin>,
    ) -> Result<Token, EncodeError> {
        if let Some(token) = self.cache.get(address) {
            return Ok(token.clone());
        }
        if let Some(native_pin) = native_pin {
            let token = native_pin
                .token(address)
                .ok_or_else(|| EncodeError::invalid("Token not found"))?;
            self.cache.insert(address.clone(), token.clone());
            return Ok(token);
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
    use tycho_simulation::rfq::protocols::{
        bebop::state::BebopState, hashflow::state::HashflowState,
    };

    use super::*;
    use crate::models::state::RfqClientConfig;
    use crate::services::encode::fixtures::{
        component_with_protocol, component_with_tokens, dummy_token, pool_ref, test_app_state,
        test_state_stores, token_store_with_tokens, TestAppStateConfig,
    };
    use crate::services::encode::mocks::{step_multiplier, StepProtocolSim};
    use crate::services::encode::model::{
        NormalizedHopInternal, NormalizedSegmentInternal, NormalizedSwapDraftInternal,
    };

    async fn test_rebuild_guard(app_state: &AppState) -> Arc<SimulationRebuildGuard> {
        app_state
            .acquire_simulation_rebuild_guard(false, false)
            .await
    }

    fn rfq_client_config() -> RfqClientConfig {
        RfqClientConfig {
            tvl_threshold: 321.0,
            bebop_user: "runtime-bebop-user".to_string(),
            bebop_key: "runtime-bebop-key".to_string(),
            hashflow_user: "runtime-hashflow-user".to_string(),
            hashflow_key: "runtime-hashflow-key".to_string(),
            liquorice_user: "runtime-liquorice-user".to_string(),
            liquorice_key: "runtime-liquorice-key".to_string(),
        }
    }

    fn source_bebop_state(base_token: Token, quote_token: Token) -> BebopState {
        serde_json::from_value(serde_json::json!({
            "base_token": base_token,
            "quote_token": quote_token,
            "book": {
                "base": base_token.address,
                "quote": quote_token.address,
                "last_update_ts": 1,
                "bids": [{"price": 2.0, "quantity": 100.0}],
                "asks": [{"price": 2.0, "quantity": 100.0}],
            },
            "usd_per_quote_token": 13.0,
        }))
        .unwrap()
    }

    #[test]
    fn hydrate_rfq_pool_state_preserves_bebop_pricing_and_omits_credentials() {
        let base = dummy_token("0x0000000000000000000000000000000000000001");
        let quote = dummy_token("0x0000000000000000000000000000000000000002");
        let state = source_bebop_state(base.clone(), quote.clone());
        let hydrated = hydrate_rfq_pool_state(
            "pool-bebop",
            &state,
            Chain::Ethereum,
            &rfq_client_config(),
            ENCODE_RFQ_QUOTE_TIMEOUT,
        )
        .unwrap();
        let amount = BigUint::from(10_u64).pow(18);
        let expected = state.get_amount_out(amount.clone(), &base, &quote).unwrap();
        let actual = hydrated.get_amount_out(amount, &base, &quote).unwrap();
        assert_eq!(actual.amount, expected.amount);
        assert!(hydrated.as_indicatively_priced().is_ok());
        assert!(actual.new_state.as_indicatively_priced().is_ok());
        let serialized = serde_json::to_string(hydrated.as_ref()).unwrap();
        assert!(!serialized.contains("runtime-bebop-key"));
        assert!(serialized.contains("13.0"));
    }

    #[test]
    fn hydrate_rfq_pool_state_restores_hashflow_signing_without_changing_prices() {
        let base = dummy_token("0x0000000000000000000000000000000000000001");
        let quote = dummy_token("0x0000000000000000000000000000000000000002");
        let state: HashflowState = serde_json::from_value(serde_json::json!({
            "base_token": base,
            "quote_token": quote,
            "levels": {
                "pair": {"baseToken": base.address, "quoteToken": quote.address},
                "levels": [{"q": "0.1", "p": "2"}, {"q": "100", "p": "2"}],
            },
            "market_maker": "maker",
        }))
        .unwrap();
        let hydrated = hydrate_rfq_pool_state(
            "pool-hashflow",
            &state,
            Chain::Ethereum,
            &rfq_client_config(),
            ENCODE_RFQ_QUOTE_TIMEOUT,
        )
        .unwrap();
        let amount = BigUint::from(10_u64).pow(18);
        assert_eq!(
            hydrated
                .get_amount_out(amount.clone(), &base, &quote)
                .unwrap()
                .amount,
            state.get_amount_out(amount, &base, &quote).unwrap().amount
        );
        assert!(hydrated.as_indicatively_priced().is_ok());
        assert!(!serde_json::to_string(hydrated.as_ref())
            .unwrap()
            .contains("runtime-hashflow-key"));
    }

    #[test]
    fn hydrate_rfq_pool_state_rejects_unknown_state_type() {
        let err = match hydrate_rfq_pool_state(
            "pool-unknown",
            &StepProtocolSim { multiplier: 1 },
            Chain::Ethereum,
            &rfq_client_config(),
            ENCODE_RFQ_QUOTE_TIMEOUT,
        ) {
            Ok(_) => panic!("unknown RFQ state type should fail hydration"),
            Err(err) => err,
        };

        assert_eq!(
            err.kind(),
            crate::services::encode::EncodeErrorKind::Internal
        );
        assert_eq!(
            err.message(),
            "RFQ pool pool-unknown has unsupported state type"
        );
    }

    #[test]
    fn encode_rfq_quote_timeout_clamps_to_request_guard() {
        // Prod guard (4.5s) leaves room for the full cap.
        assert_eq!(
            encode_rfq_quote_timeout(Duration::from_millis(4500)),
            ENCODE_RFQ_QUOTE_TIMEOUT
        );
        // A guard below the cap wins, minus the headroom.
        assert_eq!(
            encode_rfq_quote_timeout(Duration::from_millis(2000)),
            Duration::from_millis(1800)
        );
        // A guard at or under the headroom leaves no quote budget.
        assert_eq!(
            encode_rfq_quote_timeout(Duration::from_millis(150)),
            Duration::ZERO
        );
    }

    #[test]
    fn map_swap_token_wraps_native_for_non_allowlisted_protocols() {
        let chain = Chain::Ethereum;
        let native = chain.native_token().address;
        let mapped = map_swap_token(&native, chain, false);
        assert_eq!(mapped, chain.wrapped_native_token().unwrap().address);
    }

    #[test]
    fn map_swap_token_keeps_native_for_allowlisted_protocols() {
        let chain = Chain::Ethereum;
        let native = chain.native_token().address;
        let mapped = map_swap_token(&native, chain, true);
        assert_eq!(mapped, native);
    }

    #[tokio::test]
    async fn resimulate_route_advances_pool_state_for_repeated_swaps() {
        let token_in = dummy_token("0x0000000000000000000000000000000000000001");
        let token_out = dummy_token("0x0000000000000000000000000000000000000002");
        let tokens_store = token_store_with_tokens(vec![token_in.clone(), token_out.clone()]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            test_state_stores(Arc::clone(&tokens_store));

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
            test_rebuild_guard(&app_state).await,
        )
        .await
        .unwrap();

        let swaps = &resimulated.segments[0].hops[0].swaps;
        assert_eq!(swaps[0].expected_amount_out, BigUint::from(5u32));
        assert_eq!(swaps[1].expected_amount_out, BigUint::from(10u32));
        assert_eq!(step_multiplier(swaps[0].pool_state.as_ref()), 1);
        assert_eq!(step_multiplier(swaps[1].pool_state.as_ref()), 2);
    }

    #[tokio::test]
    async fn resimulate_route_loads_vm_pool_after_waiting_for_rebuild_guard() {
        let token_in = dummy_token("0x0000000000000000000000000000000000000001");
        let token_out = dummy_token("0x0000000000000000000000000000000000000002");
        let tokens_store = token_store_with_tokens(vec![token_in.clone(), token_out.clone()]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            test_state_stores(Arc::clone(&tokens_store));

        let component = component_with_protocol(
            "0x0000000000000000000000000000000000000009",
            "vm:curve",
            "curve_pool",
            vec![token_in.clone(), token_out.clone()],
        );
        let mut states = HashMap::new();
        states.insert(
            "pool-vm".to_string(),
            Box::new(StepProtocolSim { multiplier: 1 }) as Box<dyn ProtocolSim>,
        );
        let mut new_pairs = HashMap::new();
        new_pairs.insert("pool-vm".to_string(), component);
        vm_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = test_app_state(
            tokens_store,
            native_state_store,
            Arc::clone(&vm_state_store),
            rfq_state_store,
            TestAppStateConfig {
                enable_vm_pools: true,
                ..TestAppStateConfig::default()
            },
        );
        let request_guard = app_state.vm_simulation_rebuild_gate().write_owned().await;
        let token_in_address = token_in.address.clone();
        let token_out_address = token_out.address.clone();
        let normalized = NormalizedRouteInternal {
            segments: vec![NormalizedSegmentInternal {
                share_bps: 10_000,
                amount_in: BigUint::from(10u32),
                hops: vec![NormalizedHopInternal {
                    token_in: token_in.address.clone(),
                    token_out: token_out.address.clone(),
                    swaps: vec![NormalizedSwapDraftInternal {
                        pool: pool_ref("pool-vm"),
                        token_in: token_in.address,
                        token_out: token_out.address,
                        split_bps: 0,
                    }],
                }],
            }],
        };
        let state_for_task = app_state.clone();
        let resimulation_task = tokio::spawn(async move {
            let guard = state_for_task
                .acquire_simulation_rebuild_guard(true, false)
                .await;
            resimulate_route(
                &state_for_task,
                &normalized,
                Chain::Ethereum,
                &token_in_address,
                &token_out_address,
                &state_for_task.native_token_protocol_allowlist,
                guard,
            )
            .await
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !resimulation_task.is_finished(),
            "resimulation should wait before loading VM pool state"
        );

        let mut replacement_states = HashMap::new();
        replacement_states.insert(
            "pool-vm".to_string(),
            Box::new(StepProtocolSim { multiplier: 2 }) as Box<dyn ProtocolSim>,
        );
        vm_state_store
            .apply_update(Update::new(2, replacement_states, HashMap::new()))
            .await;

        drop(request_guard);
        let resimulated =
            match tokio::time::timeout(Duration::from_millis(500), resimulation_task).await {
                Ok(join_result) => match join_result {
                    Ok(resimulation) => match resimulation {
                        Ok(resimulated) => resimulated,
                        Err(err) => panic!("resimulation should succeed: {}", err.message()),
                    },
                    Err(err) => panic!("resimulation task should not panic: {err}"),
                },
                Err(err) => panic!("resimulation should finish after guard release: {err}"),
            };

        let swap = &resimulated.segments[0].hops[0].swaps[0];
        assert_eq!(swap.expected_amount_out, BigUint::from(20u32));
        assert_eq!(step_multiplier(swap.pool_state.as_ref()), 2);
    }

    #[tokio::test]
    async fn resimulate_route_upgrades_rebuild_guard_for_resolved_vm_pool() {
        let token_in = dummy_token("0x0000000000000000000000000000000000000001");
        let token_out = dummy_token("0x0000000000000000000000000000000000000002");
        let tokens_store = token_store_with_tokens(vec![token_in.clone(), token_out.clone()]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            test_state_stores(Arc::clone(&tokens_store));

        let component = component_with_protocol(
            "0x0000000000000000000000000000000000000009",
            "vm:curve",
            "curve_pool",
            vec![token_in.clone(), token_out.clone()],
        );
        let mut states = HashMap::new();
        states.insert(
            "pool-vm".to_string(),
            Box::new(StepProtocolSim { multiplier: 1 }) as Box<dyn ProtocolSim>,
        );
        let mut new_pairs = HashMap::new();
        new_pairs.insert("pool-vm".to_string(), component);
        vm_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = test_app_state(
            tokens_store,
            native_state_store,
            Arc::clone(&vm_state_store),
            rfq_state_store,
            TestAppStateConfig {
                enable_vm_pools: true,
                ..TestAppStateConfig::default()
            },
        );
        let request_guard = app_state.vm_simulation_rebuild_gate().write_owned().await;
        let token_in_address = token_in.address.clone();
        let token_out_address = token_out.address.clone();
        let normalized = NormalizedRouteInternal {
            segments: vec![NormalizedSegmentInternal {
                share_bps: 10_000,
                amount_in: BigUint::from(10u32),
                hops: vec![NormalizedHopInternal {
                    token_in: token_in.address.clone(),
                    token_out: token_out.address.clone(),
                    swaps: vec![NormalizedSwapDraftInternal {
                        pool: pool_ref("pool-vm"),
                        token_in: token_in.address,
                        token_out: token_out.address,
                        split_bps: 0,
                    }],
                }],
            }],
        };
        let state_for_task = app_state.clone();
        let resimulation_task = tokio::spawn(async move {
            let guard = state_for_task
                .acquire_simulation_rebuild_guard(false, false)
                .await;
            resimulate_route(
                &state_for_task,
                &normalized,
                Chain::Ethereum,
                &token_in_address,
                &token_out_address,
                &state_for_task.native_token_protocol_allowlist,
                guard,
            )
            .await
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !resimulation_task.is_finished(),
            "resimulation should upgrade the no-op route guard before loading VM state"
        );

        let mut replacement_states = HashMap::new();
        replacement_states.insert(
            "pool-vm".to_string(),
            Box::new(StepProtocolSim { multiplier: 2 }) as Box<dyn ProtocolSim>,
        );
        vm_state_store
            .apply_update(Update::new(2, replacement_states, HashMap::new()))
            .await;

        drop(request_guard);
        let resimulated =
            match tokio::time::timeout(Duration::from_millis(500), resimulation_task).await {
                Ok(join_result) => match join_result {
                    Ok(resimulation) => match resimulation {
                        Ok(resimulated) => resimulated,
                        Err(err) => panic!("resimulation should succeed: {}", err.message()),
                    },
                    Err(err) => panic!("resimulation task should not panic: {err}"),
                },
                Err(err) => panic!("resimulation should finish after guard release: {err}"),
            };

        let swap = &resimulated.segments[0].hops[0].swaps[0];
        assert_eq!(swap.expected_amount_out, BigUint::from(20u32));
        assert_eq!(step_multiplier(swap.pool_state.as_ref()), 2);
    }

    #[tokio::test]
    async fn resimulate_route_requires_erc4626_deposit_capability_for_type_name_only_component() {
        let token_in = dummy_token("0xdC035D45d973E3EC169d2276DDab16f1e407384F");
        let token_out = dummy_token("0xa3931d71877c0e7a3148cb7eb4463524fec27fbd");
        let tokens_store = token_store_with_tokens(vec![token_in.clone(), token_out.clone()]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            test_state_stores(Arc::clone(&tokens_store));

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
            test_rebuild_guard(&disabled_state).await,
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
            test_rebuild_guard(&enabled_state).await,
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
            test_state_stores(Arc::clone(&tokens_store));

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
            test_rebuild_guard(&app_state).await,
        )
        .await
        .unwrap();

        let seg_a_hop1_swap = &resimulated.segments[0].hops[1].swaps[0];
        let seg_b_hop0_swap = &resimulated.segments[1].hops[0].swaps[0];

        assert_eq!(seg_b_hop0_swap.expected_amount_out, BigUint::from(50u32));
        assert_eq!(step_multiplier(seg_b_hop0_swap.pool_state.as_ref()), 1);
        assert_eq!(seg_a_hop1_swap.expected_amount_out, BigUint::from(200u32));
        assert_eq!(step_multiplier(seg_a_hop1_swap.pool_state.as_ref()), 2);
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
            test_state_stores(Arc::clone(&tokens_store));
        let app_state = test_app_state(
            tokens_store,
            native_state_store,
            vm_state_store,
            rfq_state_store,
            TestAppStateConfig::default(),
        );
        let mut cache = TokenCache::new(&app_state);

        let err = match cache.get(&missing_token.address, None).await {
            Ok(_) => panic!("Expected token cache miss to fail when RPC fetch is unavailable"),
            Err(err) => err,
        };

        assert_eq!(
            err.kind(),
            crate::services::encode::EncodeErrorKind::Simulation
        );
        assert!(err.message().contains("Token lookup failed"));
    }

    #[tokio::test]
    async fn native_token_cache_uses_metadata_from_the_pinned_version() {
        let address = "0x0000000000000000000000000000000000000001";
        let second_address = "0x0000000000000000000000000000000000000002";
        let address_bytes = dummy_token(address).address;
        let second_address_bytes = dummy_token(second_address).address;
        let pinned_token = Token::new(&address_bytes, "PINNED", 18, 0, &[], Chain::Ethereum, 100);
        let paired_token = Token::new(
            &second_address_bytes,
            "PAIR",
            18,
            0,
            &[],
            Chain::Ethereum,
            100,
        );
        let tokens_store = token_store_with_tokens(Vec::<Token>::new());
        let native_state_store = Arc::new(crate::models::state::StateStore::new_private(
            Arc::clone(&tokens_store),
        ));
        let vm_state_store = Arc::new(crate::models::state::StateStore::new(Arc::clone(
            &tokens_store,
        )));
        let rfq_state_store = Arc::new(crate::models::state::StateStore::new(Arc::clone(
            &tokens_store,
        )));
        let mut states = HashMap::new();
        states.insert(
            "pool-native".to_string(),
            Box::new(StepProtocolSim { multiplier: 1 }) as Box<dyn ProtocolSim>,
        );
        let mut pairs = HashMap::new();
        pairs.insert(
            "pool-native".to_string(),
            component_with_tokens(
                "0x0000000000000000000000000000000000000009",
                vec![pinned_token.clone(), paired_token],
            ),
        );
        native_state_store
            .apply_update(Update::new(1, states, pairs))
            .await;
        let native_pin = native_state_store.pin().await;
        let app_state = test_app_state(
            tokens_store,
            native_state_store,
            vm_state_store,
            rfq_state_store,
            TestAppStateConfig::default(),
        );
        let mut cache = TokenCache::new(&app_state);

        let from_pin = match cache.get(&address_bytes, Some(&native_pin)).await {
            Ok(token) => token,
            Err(error) => panic!("pinned token should exist: {}", error.message()),
        };

        assert_eq!(from_pin.symbol, "PINNED");
        assert_eq!(from_pin.decimals, 18);
        assert!(
            !app_state
                .tokens
                .snapshot()
                .await
                .contains_key(&address_bytes),
            "the proof requires token metadata to remain absent from the global store"
        );
    }
}
