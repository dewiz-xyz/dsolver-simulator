mod allocation;
mod backend;
mod calldata;
mod error;
mod model;
mod normalize;
mod request;
mod resimulate;
pub mod response;
mod tycho_swaps;
mod wire;

#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod mocks;

pub use error::{EncodeError, EncodeErrorKind};
pub use response::{log_failure, log_handler_timeout, log_received, log_success};

use std::sync::Arc;
use std::time::Instant;

use crate::models::messages::{RouteEncodeRequest, RouteEncodeResponse};
use crate::models::state::AppState;
use tycho_execution::encoding::tycho_encoder::TychoEncoder;
use tycho_simulation::tycho_common::{models::Chain, Bytes};

type EncoderFactory =
    Arc<dyn Fn(Chain, Bytes) -> Result<Arc<dyn TychoEncoder>, EncodeError> + Send + Sync>;

/// Transport-free runtime wrapper for `/encode` route encoding.
#[derive(Clone)]
pub struct EncodeService {
    state: AppState,
    encoder_factory: EncoderFactory,
}

pub struct EncodeServiceSuccess {
    pub computation: EncodeComputation,
    pub latency_ms: u64,
}

#[derive(Debug)]
pub enum EncodeServiceError {
    Timeout { timeout_ms: u64, latency_ms: u64 },
    Failed { error: EncodeError, latency_ms: u64 },
}

impl EncodeService {
    pub fn new(state: AppState) -> Self {
        Self {
            state,
            encoder_factory: Arc::new(calldata::build_encoder),
        }
    }

    pub fn with_encoder(state: AppState, encoder: Arc<dyn TychoEncoder>) -> Self {
        Self {
            state,
            encoder_factory: Arc::new(move |_, _| Ok(Arc::clone(&encoder))),
        }
    }

    pub async fn encode(
        &self,
        request: RouteEncodeRequest,
    ) -> Result<EncodeServiceSuccess, EncodeServiceError> {
        let started_at = Instant::now();
        response::log_received(&request);

        let request_timeout = self.state.request_timeout();
        let computation_future = encode_route(
            self.state.clone(),
            request,
            Arc::clone(&self.encoder_factory),
        );

        let Ok(computation) = tokio::time::timeout(request_timeout, computation_future).await
        else {
            return Err(EncodeServiceError::Timeout {
                timeout_ms: request_timeout.as_millis() as u64,
                latency_ms: started_at.elapsed().as_millis() as u64,
            });
        };

        let latency_ms = started_at.elapsed().as_millis() as u64;
        computation
            .map(|computation| EncodeServiceSuccess {
                computation,
                latency_ms,
            })
            .map_err(|error| EncodeServiceError::Failed { error, latency_ms })
    }
}

pub struct EncodeComputation {
    pub response: RouteEncodeResponse,
    pub expected_amount_out: String,
    pub amount_out_delta: String,
    pub reset_approval: bool,
}

async fn encode_route(
    state: AppState,
    request: RouteEncodeRequest,
    encoder_factory: EncoderFactory,
) -> Result<EncodeComputation, EncodeError> {
    let chain = request::validate_chain(request.chain_id, state.chain)?;
    request::validate_swap_kinds(&request)?;

    let token_in = wire::parse_address(&request.token_in)?;
    let token_out = wire::parse_address(&request.token_out)?;
    let amount_in = wire::parse_amount(&request.amount_in)?;
    let min_amount_out = wire::parse_amount(&request.min_amount_out)?;
    let router_address = wire::parse_address(&request.tycho_router_address)?;
    // Guard against panics in downstream EVM encoding (uint256 inputs).
    wire::biguint_to_u256_checked(&amount_in, "amountIn")?;
    wire::biguint_to_u256_checked(&min_amount_out, "minAmountOut")?;

    let native_address = chain.native_token().address;
    let is_native_input = token_in == native_address;
    let allowlist = &state.native_token_protocol_allowlist;
    let normalized = normalize::normalize_route(
        &request,
        &token_in,
        &token_out,
        &amount_in,
        &native_address,
        state.erc4626_deposits_enabled,
        &state.erc4626_pair_policies,
        allowlist,
    )?;
    let (uses_native, uses_vm, uses_rfq) = normalize::route_backend_usage(&normalized);
    let availability = state
        .encode_availability(uses_native, uses_vm, uses_rfq)
        .await;
    if let Some(message) = availability.availability_message() {
        return Err(EncodeError::unavailable(message));
    }
    let rebuild_guard = state
        .acquire_simulation_rebuild_guard(uses_vm, uses_rfq)
        .await;
    let availability = state
        .encode_availability(uses_native, uses_vm, uses_rfq)
        .await;
    if let Some(message) = availability.availability_message() {
        return Err(EncodeError::unavailable(message));
    }
    let resimulated = resimulate::resimulate_route(
        &state,
        &normalized,
        chain,
        &token_in,
        &token_out,
        allowlist,
        rebuild_guard,
    )
    .await?;
    response::log_resimulation_amounts(request.request_id.as_deref(), &resimulated);
    let expected_total = response::compute_expected_total(&resimulated);
    if expected_total < min_amount_out {
        return Err(EncodeError::simulation(
            "Route expectedAmountOut below minAmountOut",
        ));
    }
    let amount_out_delta = (&expected_total - &min_amount_out).to_string();
    let encoder = encoder_factory(chain, router_address.clone())?;
    let route_context = calldata::RouteContext {
        request: &request,
        token_in: &token_in,
        token_out: &token_out,
        amount_in: &amount_in,
        router_address: &router_address,
        is_native_input,
    };
    let router_call = calldata::build_route_calldata_tx(
        &route_context,
        &resimulated,
        encoder.as_ref(),
        &min_amount_out,
    )?;
    let reset_approval =
        request::should_reset_allowance(&state.reset_allowance_tokens, request.chain_id, &token_in);
    let interactions = calldata::build_settlement_interactions(
        &token_in,
        &amount_in,
        router_call,
        reset_approval,
        is_native_input,
    )?;

    let debug = response::build_debug(&state, &request).await;

    Ok(EncodeComputation {
        response: RouteEncodeResponse {
            interactions,
            debug,
        },
        expected_amount_out: expected_total.to_string(),
        amount_out_delta,
        reset_approval,
    })
}
