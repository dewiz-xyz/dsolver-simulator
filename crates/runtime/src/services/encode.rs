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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::models::messages::{RouteEncodeRequest, RouteEncodeResponse};
use crate::models::state::{AppState, NativeFenceStatus, PublishedStatePin};
use error::AttemptError;
use model::NormalizedRouteInternal;
use num_bigint::BigUint;
use tycho_execution::encoding::tycho_encoder::TychoEncoder;
use tycho_simulation::tycho_common::{models::Chain, Bytes};

use super::stream_builder::ENCODE_RFQ_QUOTE_TIMEOUT;

type EncoderFactory =
    Arc<dyn Fn(Chain, Bytes) -> Result<Arc<dyn TychoEncoder>, AttemptError> + Send + Sync>;

const MAX_ENCODE_ATTEMPTS: u8 = 2;
const RETRY_RFQ_BUDGET_MARGIN: Duration = Duration::from_millis(500);
const RETRY_NATIVE_BUDGET_FLOOR: Duration = Duration::from_millis(300);
const NATIVE_STATE_CHANGED_MESSAGE: &str =
    "Native state changed while the route was being encoded; retry against the current version";
const NATIVE_STATE_UNAVAILABLE_MESSAGE: &str =
    "Native state is unavailable for encoding; retry once the simulator is ready";

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
            started_at,
            request_timeout,
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
    pub attempts: u8,
    pub first_attempt_expected_amount_out: Option<String>,
}

struct PreparedEncodeRoute {
    request: RouteEncodeRequest,
    chain: Chain,
    token_in: Bytes,
    token_out: Bytes,
    amount_in: BigUint,
    min_amount_out: BigUint,
    router_address: Bytes,
    is_native_input: bool,
    normalized: NormalizedRouteInternal,
    backend_usage: RouteBackendUsage,
    reset_approval: bool,
    encoder: Arc<dyn TychoEncoder>,
}

#[derive(Clone, Copy)]
struct RouteBackendUsage {
    native: bool,
    vm: bool,
    rfq: bool,
}

async fn encode_route(
    state: AppState,
    request: RouteEncodeRequest,
    encoder_factory: EncoderFactory,
    started_at: Instant,
    request_timeout: Duration,
) -> Result<EncodeComputation, EncodeError> {
    let prepared = prepare_encode_route(&state, request, encoder_factory).await?;
    let mut first_attempt_expected_amount_out = None;

    for attempt_number in [AttemptNumber::First, AttemptNumber::Second] {
        let attempt = encode_attempt(&state, &prepared, attempt_number).await;
        let outcome_class = attempt.outcome.class();
        let fence_evaluation = if outcome_class == AttemptOutcomeClass::Deterministic {
            None
        } else {
            attempt.native_fence.evaluate(&state).await
        };
        let fence_status = fence_evaluation
            .as_ref()
            .map_or(NativeFenceStatus::Current, |evaluation| evaluation.status);

        match resolve_attempt(outcome_class, fence_status, attempt_number) {
            AttemptResolution::ReturnOutcome => {
                log_fence(&fence_evaluation, &prepared, attempt_number, "pass");
                return attempt.outcome.into_result().map(|mut computation| {
                    computation.attempts = attempt_number.value();
                    computation.first_attempt_expected_amount_out =
                        first_attempt_expected_amount_out;
                    computation
                });
            }
            AttemptResolution::Retry => {
                let first_error_kind = attempt.outcome.error().map(EncodeError::kind);
                let first_error_message = attempt
                    .outcome
                    .error()
                    .map(EncodeError::message)
                    .map(str::to_string);
                let first_expected_amount_out =
                    attempt.outcome.expected_amount_out().map(str::to_string);
                let budget = retry_budget(
                    request_timeout,
                    started_at.elapsed(),
                    attempt.route_uses_rfq,
                );
                if !budget.can_start {
                    log_fence(
                        &fence_evaluation,
                        &prepared,
                        attempt_number,
                        "retry_skipped_budget",
                    );
                    response::log_retry_skipped_budget(
                        prepared.request.request_id.as_deref(),
                        budget.remaining,
                        started_at.elapsed(),
                        attempt.route_uses_rfq,
                    );
                    return Err(native_state_changed_error());
                }

                log_fence(&fence_evaluation, &prepared, attempt_number, "retry");
                let trigger = if outcome_class == AttemptOutcomeClass::Success {
                    RetryTrigger::Fence
                } else {
                    RetryTrigger::StateDependentError
                };
                response::log_retry_fired(
                    prepared.request.request_id.as_deref(),
                    trigger.label(),
                    first_error_kind,
                    first_error_message.as_deref(),
                    first_expected_amount_out.as_deref(),
                    started_at.elapsed(),
                    attempt.route_uses_rfq,
                );
                first_attempt_expected_amount_out = first_expected_amount_out;
            }
            AttemptResolution::ReturnNativeStateChanged => {
                log_fence(
                    &fence_evaluation,
                    &prepared,
                    attempt_number,
                    "second_trip_abort",
                );
                response::log_second_trip_abort(
                    prepared.request.request_id.as_deref(),
                    outcome_class.label(),
                    attempt.outcome.error().map(EncodeError::message),
                    attempt.outcome.expected_amount_out(),
                    started_at.elapsed(),
                );
                return Err(native_state_changed_error());
            }
            AttemptResolution::ReturnNativeUnavailable => {
                log_fence(&fence_evaluation, &prepared, attempt_number, "unavailable");
                return Err(EncodeError::unavailable(NATIVE_STATE_UNAVAILABLE_MESSAGE));
            }
        }
    }

    unreachable!("encode attempt loop always returns after attempt two")
}

async fn prepare_encode_route(
    state: &AppState,
    request: RouteEncodeRequest,
    encoder_factory: EncoderFactory,
) -> Result<PreparedEncodeRoute, EncodeError> {
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
    let backend_usage = RouteBackendUsage {
        native: uses_native,
        vm: uses_vm,
        rfq: uses_rfq,
    };
    let availability = state
        .encode_availability(uses_native, uses_vm, uses_rfq)
        .await;
    if let Some(message) = availability.availability_message() {
        return Err(EncodeError::unavailable(message));
    }

    // Build the encoder once so retries repeat only state-dependent work.
    let encoder = encoder_factory(chain, router_address.clone()).map_err(EncodeError::from)?;
    let reset_approval =
        request::should_reset_allowance(&state.reset_allowance_tokens, request.chain_id, &token_in);

    Ok(PreparedEncodeRoute {
        request,
        chain,
        token_in,
        token_out,
        amount_in,
        min_amount_out,
        router_address,
        is_native_input,
        normalized,
        backend_usage,
        reset_approval,
        encoder,
    })
}

struct EncodeAttempt {
    outcome: AttemptOutcome,
    native_fence: NativeAttemptFence,
    route_uses_rfq: bool,
}

async fn encode_attempt(
    state: &AppState,
    prepared: &PreparedEncodeRoute,
    attempt_number: AttemptNumber,
) -> EncodeAttempt {
    // Protocol hints can label a native pool VM or RFQ, and the component-resolved lookup
    // still serves it, so neither the pin nor the fence can follow backend_usage. Pin
    // every attempt and key the fence on the pools the attempt resolved as native.
    let native_pin = state.native_state_store.pin().await;
    let execution =
        encode_attempt_with_pin(state, prepared, attempt_number, native_pin.clone()).await;
    let native_fence = NativeAttemptFence::new(native_pin, execution.native_pool_ids);

    EncodeAttempt {
        outcome: AttemptOutcome::from_result(execution.outcome),
        native_fence,
        route_uses_rfq: execution.route_uses_rfq,
    }
}

struct EncodeAttemptExecution {
    outcome: Result<EncodeComputation, AttemptError>,
    native_pool_ids: HashSet<String>,
    route_uses_rfq: bool,
}

async fn encode_attempt_with_pin(
    state: &AppState,
    prepared: &PreparedEncodeRoute,
    attempt_number: AttemptNumber,
    native_pin: PublishedStatePin,
) -> EncodeAttemptExecution {
    let rebuild_guard = state
        .acquire_simulation_rebuild_guard(prepared.backend_usage.vm, prepared.backend_usage.rfq)
        .await;
    let availability = state
        .encode_availability(
            prepared.backend_usage.native,
            prepared.backend_usage.vm,
            prepared.backend_usage.rfq,
        )
        .await;
    if let Some(message) = availability.availability_message() {
        return EncodeAttemptExecution {
            outcome: Err(AttemptError::deterministic(EncodeError::unavailable(
                message,
            ))),
            native_pool_ids: normalized_native_pool_ids(&prepared.normalized),
            route_uses_rfq: prepared.backend_usage.rfq,
        };
    }
    let resimulation = resimulate::resimulate_route_with_native_pin(
        state,
        &prepared.normalized,
        prepared.chain,
        rebuild_guard,
        native_pin,
    )
    .await;
    match resimulation.result {
        Ok(resimulated) => {
            let outcome =
                complete_encode_attempt(state, prepared, attempt_number, resimulated).await;
            EncodeAttemptExecution {
                outcome,
                native_pool_ids: resimulation.native_pool_ids,
                route_uses_rfq: resimulation.uses_rfq,
            }
        }
        Err(error) => {
            // Failure can strike before every pool resolves, so widen the resolved view
            // with the hinted pools. An id wrongly treated as native never registers as
            // changed, while a missed RFQ hop would let a retry approved under the native
            // floor run a firm quote inside block_in_place that the request timeout cannot
            // interrupt.
            let mut native_pool_ids = resimulation.native_pool_ids;
            native_pool_ids.extend(normalized_native_pool_ids(&prepared.normalized));
            EncodeAttemptExecution {
                outcome: Err(error),
                native_pool_ids,
                route_uses_rfq: resimulation.uses_rfq || prepared.backend_usage.rfq,
            }
        }
    }
}

async fn complete_encode_attempt(
    state: &AppState,
    prepared: &PreparedEncodeRoute,
    attempt_number: AttemptNumber,
    resimulated: model::ResimulatedRouteInternal,
) -> Result<EncodeComputation, AttemptError> {
    response::log_resimulation_amounts(
        prepared.request.request_id.as_deref(),
        attempt_number.value(),
        &resimulated,
    );
    let expected_total = response::compute_expected_total(&resimulated);
    if expected_total < prepared.min_amount_out {
        return Err(AttemptError::state_dependent(EncodeError::simulation(
            "Route expectedAmountOut below minAmountOut",
        )));
    }
    let amount_out_delta = (&expected_total - &prepared.min_amount_out).to_string();
    let route_context = calldata::RouteContext {
        request: &prepared.request,
        token_in: &prepared.token_in,
        token_out: &prepared.token_out,
        amount_in: &prepared.amount_in,
        router_address: &prepared.router_address,
        is_native_input: prepared.is_native_input,
    };
    let router_call = calldata::build_route_calldata_tx(
        &route_context,
        &resimulated,
        prepared.encoder.as_ref(),
        &prepared.min_amount_out,
    )?;
    let interactions = calldata::build_settlement_interactions(
        &prepared.token_in,
        &prepared.amount_in,
        router_call,
        prepared.reset_approval,
        prepared.is_native_input,
    )?;
    let debug = response::build_debug(state, &prepared.request).await;

    Ok(EncodeComputation {
        response: RouteEncodeResponse {
            interactions,
            debug,
        },
        expected_amount_out: expected_total.to_string(),
        amount_out_delta,
        reset_approval: prepared.reset_approval,
        attempts: attempt_number.value(),
        first_attempt_expected_amount_out: None,
    })
}

fn normalized_native_pool_ids(normalized: &NormalizedRouteInternal) -> HashSet<String> {
    normalized
        .segments
        .iter()
        .flat_map(|segment| segment.hops.iter())
        .flat_map(|hop| hop.swaps.iter())
        .filter(|swap| backend::PoolBackend::from_protocol_hint(&swap.pool.protocol).is_native())
        .map(|swap| swap.pool.component_id.clone())
        .collect()
}

struct NativeAttemptFence {
    pin: PublishedStatePin,
    native_pool_ids: HashSet<String>,
}

impl NativeAttemptFence {
    fn new(pin: PublishedStatePin, native_pool_ids: HashSet<String>) -> Self {
        Self {
            pin,
            native_pool_ids,
        }
    }

    async fn evaluate(&self, state: &AppState) -> Option<NativeFenceEvaluation> {
        // Every attempt holds a pin, but a route with no native pools must stay
        // independent of native readiness, so only native involvement arms the fence.
        if self.native_pool_ids.is_empty() {
            return None;
        }
        let check = state
            .native_route_fence_status(&self.pin, &self.native_pool_ids)
            .await;
        Some(NativeFenceEvaluation {
            status: check.status,
            generation_moved: check.current_generation != self.pin.request_generation(),
            identity_changed: check.status == NativeFenceStatus::Changed,
            native_pool_count: self.native_pool_ids.len(),
        })
    }
}

struct NativeFenceEvaluation {
    status: NativeFenceStatus,
    generation_moved: bool,
    identity_changed: bool,
    native_pool_count: usize,
}

fn log_fence(
    evaluation: &Option<NativeFenceEvaluation>,
    prepared: &PreparedEncodeRoute,
    attempt_number: AttemptNumber,
    outcome: &str,
) {
    let Some(evaluation) = evaluation.as_ref() else {
        return;
    };
    response::log_fence_evaluation(
        prepared.request.request_id.as_deref(),
        attempt_number.value(),
        evaluation.generation_moved,
        evaluation.identity_changed,
        evaluation.native_pool_count,
        outcome,
    );
}

enum AttemptOutcome {
    Success(EncodeComputation),
    StateDependent(EncodeError),
    Deterministic(EncodeError),
}

impl AttemptOutcome {
    fn from_result(result: Result<EncodeComputation, AttemptError>) -> Self {
        match result {
            Ok(computation) => Self::Success(computation),
            Err(AttemptError::StateDependent(error)) => Self::StateDependent(error),
            Err(AttemptError::Deterministic(error)) => Self::Deterministic(error),
        }
    }

    const fn class(&self) -> AttemptOutcomeClass {
        match self {
            Self::Success(_) => AttemptOutcomeClass::Success,
            Self::StateDependent(_) => AttemptOutcomeClass::StateDependent,
            Self::Deterministic(_) => AttemptOutcomeClass::Deterministic,
        }
    }

    fn error(&self) -> Option<&EncodeError> {
        match self {
            Self::Success(_) => None,
            Self::StateDependent(error) | Self::Deterministic(error) => Some(error),
        }
    }

    fn expected_amount_out(&self) -> Option<&str> {
        match self {
            Self::Success(computation) => Some(computation.expected_amount_out.as_str()),
            Self::StateDependent(_) | Self::Deterministic(_) => None,
        }
    }

    fn into_result(self) -> Result<EncodeComputation, EncodeError> {
        match self {
            Self::Success(computation) => Ok(computation),
            Self::StateDependent(error) | Self::Deterministic(error) => Err(error),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttemptOutcomeClass {
    Success,
    StateDependent,
    Deterministic,
}

impl AttemptOutcomeClass {
    const fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::StateDependent => "state_dependent_error",
            Self::Deterministic => "deterministic_error",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttemptNumber {
    First,
    Second,
}

impl AttemptNumber {
    const fn value(self) -> u8 {
        match self {
            Self::First => 1,
            Self::Second => MAX_ENCODE_ATTEMPTS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttemptResolution {
    ReturnOutcome,
    Retry,
    ReturnNativeStateChanged,
    ReturnNativeUnavailable,
}

const fn resolve_attempt(
    outcome_class: AttemptOutcomeClass,
    fence_status: NativeFenceStatus,
    attempt_number: AttemptNumber,
) -> AttemptResolution {
    match (outcome_class, fence_status, attempt_number) {
        (AttemptOutcomeClass::Deterministic, _, _)
        | (
            AttemptOutcomeClass::Success | AttemptOutcomeClass::StateDependent,
            NativeFenceStatus::Current,
            _,
        ) => AttemptResolution::ReturnOutcome,
        (_, NativeFenceStatus::Unavailable, _) => AttemptResolution::ReturnNativeUnavailable,
        (
            AttemptOutcomeClass::Success | AttemptOutcomeClass::StateDependent,
            NativeFenceStatus::Changed,
            AttemptNumber::First,
        ) => AttemptResolution::Retry,
        (
            AttemptOutcomeClass::Success | AttemptOutcomeClass::StateDependent,
            NativeFenceStatus::Changed,
            AttemptNumber::Second,
        ) => {
            // A second generation change invalidates success and state-dependent errors alike.
            AttemptResolution::ReturnNativeStateChanged
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RetryBudget {
    remaining: Duration,
    can_start: bool,
}

fn retry_budget(request_timeout: Duration, elapsed: Duration, route_uses_rfq: bool) -> RetryBudget {
    let remaining = request_timeout.saturating_sub(elapsed);
    // RFQ quotes block the task, so attempt two needs enough time to cover the quote and finish.
    let required = if route_uses_rfq {
        ENCODE_RFQ_QUOTE_TIMEOUT.saturating_add(RETRY_RFQ_BUDGET_MARGIN)
    } else {
        RETRY_NATIVE_BUDGET_FLOOR
    };
    RetryBudget {
        remaining,
        can_start: remaining >= required,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryTrigger {
    Fence,
    StateDependentError,
}

impl RetryTrigger {
    const fn label(self) -> &'static str {
        match self {
            Self::Fence => "fence",
            Self::StateDependentError => "state_dependent_error",
        }
    }
}

fn native_state_changed_error() -> EncodeError {
    EncodeError::unavailable(NATIVE_STATE_CHANGED_MESSAGE)
}

#[cfg(test)]
mod retry_tests {
    use super::{
        resolve_attempt, retry_budget, AttemptNumber, AttemptOutcomeClass, AttemptResolution,
        NativeFenceStatus,
    };
    use std::time::Duration;

    #[test]
    fn resolve_attempt_covers_every_outcome_fence_and_attempt_combination() {
        use AttemptNumber::{First, Second};
        use AttemptOutcomeClass::{Deterministic, StateDependent, Success};
        use AttemptResolution::{
            Retry, ReturnNativeStateChanged, ReturnNativeUnavailable, ReturnOutcome,
        };
        use NativeFenceStatus::{Changed, Current, Unavailable};

        let cases = [
            (Success, Current, First, ReturnOutcome),
            (Success, Current, Second, ReturnOutcome),
            (Success, Changed, First, Retry),
            (Success, Changed, Second, ReturnNativeStateChanged),
            (Success, Unavailable, First, ReturnNativeUnavailable),
            (Success, Unavailable, Second, ReturnNativeUnavailable),
            (StateDependent, Current, First, ReturnOutcome),
            (StateDependent, Current, Second, ReturnOutcome),
            (StateDependent, Changed, First, Retry),
            (StateDependent, Changed, Second, ReturnNativeStateChanged),
            (StateDependent, Unavailable, First, ReturnNativeUnavailable),
            (StateDependent, Unavailable, Second, ReturnNativeUnavailable),
            (Deterministic, Current, First, ReturnOutcome),
            (Deterministic, Current, Second, ReturnOutcome),
            (Deterministic, Changed, First, ReturnOutcome),
            (Deterministic, Changed, Second, ReturnOutcome),
            (Deterministic, Unavailable, First, ReturnOutcome),
            (Deterministic, Unavailable, Second, ReturnOutcome),
        ];

        assert_eq!(cases.len(), 18);
        for (outcome, fence, attempt, expected) in cases {
            assert_eq!(
                resolve_attempt(outcome, fence, attempt),
                expected,
                "{outcome:?} with {fence:?} on {attempt:?}"
            );
        }
    }

    #[test]
    fn retry_budget_requires_quote_timeout_and_margin_for_rfq_routes() {
        let timeout = Duration::from_millis(4_500);

        assert!(retry_budget(timeout, Duration::from_millis(1_000), true).can_start);
        let insufficient = retry_budget(timeout, Duration::from_millis(1_001), true);
        assert!(!insufficient.can_start);
        assert_eq!(insufficient.remaining, Duration::from_millis(3_499));
    }

    #[test]
    fn retry_budget_uses_native_floor_without_rfq_hops() {
        let timeout = Duration::from_millis(4_500);

        assert!(retry_budget(timeout, Duration::from_millis(4_200), false).can_start);
        let insufficient = retry_budget(timeout, Duration::from_millis(4_201), false);
        assert!(!insufficient.can_start);
        assert_eq!(insufficient.remaining, Duration::from_millis(299));
    }
}
