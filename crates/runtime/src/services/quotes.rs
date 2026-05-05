use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use num_bigint::BigUint;
use num_traits::{cast::ToPrimitive, Zero};
use rayon::prelude::*;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use tycho_simulation::{
    protocol::models::ProtocolComponent,
    tycho_common::{models::token::Token, simulation::protocol_sim::ProtocolSim, Bytes},
};

use crate::config::SlippageConfig;
use crate::models::erc4626::component_direction_supported;
use crate::models::messages::{
    AmountOutRequest, AmountOutResponse, PoolOutcomeKind, PoolSimulationOutcome, QuoteFailure,
    QuoteFailureKind, QuoteMeta, QuotePartialKind, QuoteResultQuality, QuoteStatus,
};
use crate::models::state::{AppState, SimulationRebuildGuard};
use crate::models::tokens::TokenStoreError;

use super::simulation_executor::{SimulationExecutionError, SimulationExecutor};

const VM_LOW_FIRST_GAS_THRESHOLD: u64 = 600_000;
const VM_LOW_FIRST_GAS_SAMPLE_CAP: usize = 3;
const NATIVE_TOKEN_ADDRESS_BYTES: [u8; 20] = [0u8; 20];
const BPS_DENOMINATOR: u32 = 10_000;
const STRESS_INPUT_DIVISOR: u32 = 100;
const MIN_STRESS_INPUT: u8 = 1;
const SATURATION_THRESHOLD_BPS: u32 = 9_999;
const SATURATION_RAMP_START_UTILIZATION_BPS: u32 = 9_500;
const SATURATION_MAX_UTILIZATION_BPS: u32 = 9_990;
const SLIPPAGE_SCALE: u32 = 1_000_000;
const SOFT_LADDER_ACTIVATION_DEGRADATION_BPS: u32 = 10;

fn native_token_address() -> Bytes {
    Bytes::from(NATIVE_TOKEN_ADDRESS_BYTES)
}

// Per-request scheduling metrics (logged once by the handler).
#[derive(Debug, Default, Clone)]
pub struct QuoteMetrics {
    pub scheduled_native_pools: usize,
    pub scheduled_vm_pools: usize,
    pub scheduled_rfq_pools: usize,
    pub skipped_vm_unavailable: bool,
    pub skipped_rfq_unavailable: bool,
    pub vm_completed_pools: usize,
    pub rfq_completed_pools: usize,
    pub vm_median_first_gas: Option<u64>,
    pub rfq_median_first_gas: Option<u64>,
    pub vm_low_first_gas_count: usize,
    pub rfq_low_first_gas_count: usize,
    pub vm_low_first_gas_ratio: Option<f64>,
    pub rfq_low_first_gas_ratio: Option<f64>,
    pub vm_low_first_gas_samples: Vec<String>,
    pub rfq_low_first_gas_samples: Vec<String>,
}

pub struct QuoteComputation {
    pub responses: Vec<AmountOutResponse>,
    pub meta: QuoteMeta,
    pub metrics: QuoteMetrics,
}

type CandidatePool = (String, Arc<dyn ProtocolSim>, Arc<ProtocolComponent>);
type PoolTask = Pin<Box<dyn Future<Output = Result<PoolSimOutcome, QuoteFailure>> + Send>>;

fn build_request_exit(
    responses: Vec<AmountOutResponse>,
    mut meta: QuoteMeta,
    metrics: QuoteMetrics,
    pool_results: Vec<PoolSimulationOutcome>,
    failures: Vec<QuoteFailure>,
    exit: RequestExit,
) -> QuoteComputation {
    meta.status = exit.status;
    meta.result_quality = exit.result_quality;
    meta.partial_kind = exit.partial_kind;
    meta.pool_results = pool_results;
    meta.vm_unavailable = metrics.skipped_vm_unavailable;
    meta.rfq_unavailable = metrics.skipped_rfq_unavailable;
    meta.failures = failures;
    QuoteComputation {
        responses,
        meta,
        metrics,
    }
}

#[derive(Debug, Clone, Copy)]
enum ScheduledPoolKind {
    Native,
    Vm,
    Rfq,
}

impl ScheduledPoolKind {
    const fn uses_rebuild_guard(self) -> bool {
        matches!(self, Self::Vm | Self::Rfq)
    }

    fn mark_scheduled(self, metrics: &mut QuoteMetrics) {
        match self {
            Self::Native => metrics.scheduled_native_pools += 1,
            Self::Vm => metrics.scheduled_vm_pools += 1,
            Self::Rfq => metrics.scheduled_rfq_pools += 1,
        }
    }
}

struct PreparedPoolSimulation {
    descriptor: PoolDescriptor,
    pool_state: Arc<dyn ProtocolSim>,
    sim_token_in: Arc<Token>,
    sim_token_out: Arc<Token>,
    pool_cancel: CancellationToken,
}

struct PreparePoolSimulationInput<'a> {
    kind: ScheduledPoolKind,
    pool_state: Arc<dyn ProtocolSim>,
    component: Arc<ProtocolComponent>,
    descriptor: PoolDescriptor,
    cancel_token: &'a CancellationToken,
    token_in: &'a Token,
    token_out: &'a Token,
    metrics: &'a mut QuoteMetrics,
}

#[derive(Clone)]
struct PoolDescriptor {
    id: String,
    name: String,
    address: String,
    protocol: String,
}

enum LadderStressQuote {
    Quote((BigUint, BigUint)),
    PartialTail,
    Missing,
}

struct SimulatePoolInput {
    descriptor: PoolDescriptor,
    pool_state: Arc<dyn ProtocolSim>,
    rebuild_guard: Option<Arc<SimulationRebuildGuard>>,
    token_in: Arc<Token>,
    token_out: Arc<Token>,
    amounts: Arc<Vec<BigUint>>,
    slippage: SlippageConfig,
    expected_len: usize,
    cancel_token: CancellationToken,
}

struct PoolOutcomeInput {
    descriptor: PoolDescriptor,
    outcome: PoolOutcomeKind,
    reported_steps: usize,
    expected_steps: usize,
    reason: Option<String>,
}

struct QuoteRunState {
    responses: Vec<AmountOutResponse>,
    failures: Vec<QuoteFailure>,
    pool_results: Vec<PoolSimulationOutcome>,
    metrics: QuoteMetrics,
    meta: QuoteMeta,
    classification: QuoteClassification,
}

impl QuoteRunState {
    fn finish(self, exit: RequestExit) -> QuoteComputation {
        build_request_exit(
            self.responses,
            self.meta,
            self.metrics,
            self.pool_results,
            self.failures,
            exit,
        )
    }
}

#[derive(Clone, Copy)]
struct RequestExit {
    status: QuoteStatus,
    result_quality: QuoteResultQuality,
    partial_kind: Option<QuotePartialKind>,
}

impl RequestExit {
    const fn new(status: QuoteStatus, result_quality: QuoteResultQuality) -> Self {
        Self {
            status,
            result_quality,
            partial_kind: None,
        }
    }

    const fn with_partial_kind(mut self, partial_kind: QuotePartialKind) -> Self {
        self.partial_kind = Some(partial_kind);
        self
    }
}

#[derive(Debug, Clone, Copy, Default)]
enum PartialClassification {
    #[default]
    None,
    AmountLadders,
    PoolCoverage,
    Mixed,
}

impl PartialClassification {
    fn note_partial_amount_coverage(&mut self) {
        *self = match self {
            Self::None | Self::AmountLadders => Self::AmountLadders,
            Self::PoolCoverage | Self::Mixed => Self::Mixed,
        };
    }

    fn note_pool_coverage_gap(&mut self) {
        *self = match self {
            Self::None | Self::PoolCoverage => Self::PoolCoverage,
            Self::AmountLadders | Self::Mixed => Self::Mixed,
        };
    }

    const fn as_partial_kind(self) -> Option<QuotePartialKind> {
        match self {
            Self::None => None,
            Self::AmountLadders => Some(QuotePartialKind::AmountLadders),
            Self::PoolCoverage => Some(QuotePartialKind::PoolCoverage),
            Self::Mixed => Some(QuotePartialKind::Mixed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
enum NoResultFailureClassification {
    #[default]
    LiquidityLike,
    OperationallyDegraded,
}

#[derive(Debug, Default)]
struct QuoteClassification {
    partial: PartialClassification,
    had_usable_response: bool,
    no_result_failures: NoResultFailureClassification,
}

impl QuoteClassification {
    fn new() -> Self {
        Self::default()
    }

    fn mark_request_degradation(&mut self) {
        self.no_result_failures = NoResultFailureClassification::OperationallyDegraded;
    }

    fn note_response(&mut self) {
        self.had_usable_response = true;
    }

    fn note_pool_outcome(&mut self, outcome: PoolOutcomeKind) {
        self.note_pool_outcome_with_steps(outcome, 0, 0);
    }

    fn note_pool_outcome_with_steps(
        &mut self,
        outcome: PoolOutcomeKind,
        reported_steps: usize,
        expected_steps: usize,
    ) {
        if matches!(outcome, PoolOutcomeKind::TimedOut)
            && reported_steps > 0
            && reported_steps < expected_steps
        {
            self.partial.note_partial_amount_coverage();
            return;
        }

        match outcome {
            PoolOutcomeKind::PartialOutput => self.partial.note_partial_amount_coverage(),
            PoolOutcomeKind::ZeroOutput
            | PoolOutcomeKind::TimedOut
            | PoolOutcomeKind::SimulatorError
            | PoolOutcomeKind::InternalError => self.partial.note_pool_coverage_gap(),
        }
    }

    fn note_failure(&mut self, failure: &QuoteFailure) {
        if !is_liquidity_like_failure(failure) {
            self.no_result_failures = NoResultFailureClassification::OperationallyDegraded;
        }
    }
}

struct RequestPair {
    token_in_address: String,
    token_out_address: String,
    token_in_bytes: Bytes,
    token_out_bytes: Bytes,
}

struct CandidateSets {
    native_candidates: Vec<CandidatePool>,
    vm_candidates: Vec<CandidatePool>,
    rfq_candidates: Vec<CandidatePool>,
}

struct PreparedQuoteExecution {
    rebuild_guard: Arc<SimulationRebuildGuard>,
    token_in: Arc<Token>,
    token_out: Arc<Token>,
    amounts_in: Arc<Vec<BigUint>>,
    expected_len: usize,
    native_candidates: Vec<CandidatePool>,
    vm_candidates: Vec<CandidatePool>,
    rfq_candidates: Vec<CandidatePool>,
    cancel_token: CancellationToken,
}

struct QuoteRequestRunner {
    state: AppState,
    request: AmountOutRequest,
    run: QuoteRunState,
    cancel: Option<CancellationToken>,
    readiness_wait: Duration,
}

fn pool_descriptor(id: String, component: &ProtocolComponent) -> PoolDescriptor {
    PoolDescriptor {
        name: derive_pool_name(component),
        address: component.id.to_string(),
        protocol: component.protocol_system.clone(),
        id,
    }
}

fn prepare_pool_simulation(input: PreparePoolSimulationInput<'_>) -> PreparedPoolSimulation {
    input.kind.mark_scheduled(input.metrics);
    let (sim_token_in, sim_token_out) = simulation_tokens_for_pool_with_remap_log(
        input.token_in,
        input.token_out,
        input.component.as_ref(),
        input.descriptor.id.as_str(),
        input.descriptor.protocol.as_str(),
    );

    PreparedPoolSimulation {
        descriptor: input.descriptor,
        pool_state: Arc::from(input.pool_state.as_ref().clone_box()),
        sim_token_in,
        sim_token_out,
        pool_cancel: input.cancel_token.child_token(),
    }
}

pub async fn get_amounts_out(
    state: AppState,
    request: AmountOutRequest,
    cancel: Option<CancellationToken>,
) -> QuoteComputation {
    QuoteRequestRunner::new(state, request, cancel)
        .await
        .run()
        .await
}

impl QuoteRequestRunner {
    async fn new(
        state: AppState,
        request: AmountOutRequest,
        cancel: Option<CancellationToken>,
    ) -> Self {
        let current_block = state.current_block().await;
        let current_vm_block = state.current_vm_block().await;
        let current_rfq_block = state.current_rfq_block().await;
        let total_pools = state.total_pools().await;
        let meta = QuoteMeta {
            status: QuoteStatus::Ready,
            result_quality: QuoteResultQuality::RequestLevelFailure,
            partial_kind: None,
            block_number: current_block,
            vm_block_number: current_vm_block,
            rfq_block_number: current_rfq_block,
            matching_pools: 0,
            candidate_pools: 0,
            total_pools: Some(total_pools),
            auction_id: request.auction_id.clone(),
            pool_results: Vec::new(),
            vm_unavailable: false,
            rfq_unavailable: false,
            failures: Vec::new(),
        };
        Self {
            state,
            request,
            run: QuoteRunState {
                responses: Vec::new(),
                failures: Vec::new(),
                pool_results: Vec::new(),
                metrics: QuoteMetrics::default(),
                meta,
                classification: QuoteClassification::new(),
            },
            cancel,
            readiness_wait: Duration::from_secs(2),
        }
    }

    async fn run(mut self) -> QuoteComputation {
        if self.is_cancelled() {
            self.run.classification.mark_request_degradation();
            self.push_failure(make_failure(
                QuoteFailureKind::Timeout,
                "Quote request cancelled before execution".to_string(),
                None,
            ));
            return self.finish(RequestExit::new(
                QuoteStatus::Ready,
                QuoteResultQuality::RequestLevelFailure,
            ));
        }
        let pair = match self.parse_pair() {
            Ok(pair) => pair,
            Err(exit) => return self.finish(exit),
        };
        if let Err(exit) = self.ensure_ready().await {
            return self.finish(exit);
        }
        let (token_in_ref, token_out_ref) = match self.load_tokens(&pair).await {
            Ok(tokens) => tokens,
            Err(exit) => return self.finish(exit),
        };
        let amounts_in = match self.parse_amounts() {
            Ok(amounts) => amounts,
            Err(exit) => return self.finish(exit),
        };
        let prepared = match self
            .prepare_execution(&pair, token_in_ref, token_out_ref, amounts_in)
            .await
        {
            Ok(prepared) => prepared,
            Err(exit) => return self.finish(exit),
        };
        self.execute_pool_quotes(prepared).await;
        self.finish_current_state()
    }

    fn finish(self, exit: RequestExit) -> QuoteComputation {
        self.run.finish(exit)
    }

    fn finish_current_state(mut self) -> QuoteComputation {
        self.finalize_responses();
        self.finalize_pool_results();
        let exit = self.classify_current_exit();
        self.run.finish(exit)
    }

    fn is_cancelled(&self) -> bool {
        self.cancel
            .as_ref()
            .map(CancellationToken::is_cancelled)
            .unwrap_or(false)
    }

    fn push_failure(&mut self, failure: QuoteFailure) {
        self.run.classification.note_failure(&failure);
        self.run.failures.push(failure);
    }

    fn classify_current_exit(&self) -> RequestExit {
        match self.run.meta.status {
            QuoteStatus::Ready => self.classify_ready_exit(),
            QuoteStatus::NoLiquidity => {
                RequestExit::new(QuoteStatus::NoLiquidity, QuoteResultQuality::NoResults)
            }
            QuoteStatus::WarmingUp
            | QuoteStatus::TokenMissing
            | QuoteStatus::InvalidRequest
            | QuoteStatus::InternalError => RequestExit::new(
                self.run.meta.status,
                QuoteResultQuality::RequestLevelFailure,
            ),
        }
    }

    fn classify_ready_exit(&self) -> RequestExit {
        if self.run.classification.had_usable_response {
            if self.run.pool_results.is_empty() && self.run.failures.is_empty() {
                return RequestExit::new(QuoteStatus::Ready, QuoteResultQuality::Complete);
            }

            let partial_kind = self
                .run
                .classification
                .partial
                .as_partial_kind()
                .unwrap_or(QuotePartialKind::PoolCoverage);

            return RequestExit::new(QuoteStatus::Ready, QuoteResultQuality::Partial)
                .with_partial_kind(partial_kind);
        }

        if matches!(
            self.run.classification.no_result_failures,
            NoResultFailureClassification::LiquidityLike
        ) {
            return RequestExit::new(QuoteStatus::NoLiquidity, QuoteResultQuality::NoResults);
        }

        // No usable quotes plus a non-liquidity degradation is a hard request failure.
        RequestExit::new(
            QuoteStatus::InternalError,
            QuoteResultQuality::RequestLevelFailure,
        )
    }

    fn parse_pair(&mut self) -> Result<RequestPair, RequestExit> {
        let token_in_raw = self.request.token_in.clone();
        let token_out_raw = self.request.token_out.clone();
        let (token_in_address, token_in_bytes) = self.parse_address(&token_in_raw, "token_in")?;
        let (token_out_address, token_out_bytes) =
            self.parse_address(&token_out_raw, "token_out")?;
        let wrapped_native = self.state.tokens.wrapped_native_token();
        if is_direct_native_wrapped_pair(&token_in_bytes, &token_out_bytes, wrapped_native.as_ref())
        {
            self.push_failure(make_failure(
                QuoteFailureKind::InvalidRequest,
                "Direct native/wrapped quotes are unsupported".to_string(),
                None,
            ));
            return Err(RequestExit::new(
                QuoteStatus::InvalidRequest,
                QuoteResultQuality::RequestLevelFailure,
            ));
        }
        Ok(RequestPair {
            token_in_address,
            token_out_address,
            token_in_bytes,
            token_out_bytes,
        })
    }

    fn parse_address(&mut self, raw: &str, field: &str) -> Result<(String, Bytes), RequestExit> {
        let address = raw.trim_start_matches("0x").to_lowercase();
        match Bytes::from_str(&address) {
            Ok(bytes) => Ok((address, bytes)),
            Err(err) => {
                self.push_failure(make_failure(
                    QuoteFailureKind::TokenValidation,
                    format!("Invalid {field} address: {err}"),
                    None,
                ));
                Err(RequestExit::new(
                    QuoteStatus::InvalidRequest,
                    QuoteResultQuality::RequestLevelFailure,
                ))
            }
        }
    }

    async fn ensure_ready(&mut self) -> Result<(), RequestExit> {
        if self.state.is_ready().await {
            return Ok(());
        }
        let ready = self.state.wait_for_readiness(self.readiness_wait).await;
        if !ready {
            self.push_failure(make_failure(
                QuoteFailureKind::WarmUp,
                format!(
                    "Service warming up: block={}, pools={}",
                    self.run.meta.block_number,
                    self.run.meta.total_pools.unwrap_or_default()
                ),
                None,
            ));
            return Err(RequestExit::new(
                QuoteStatus::WarmingUp,
                QuoteResultQuality::RequestLevelFailure,
            ));
        }
        self.refresh_meta_snapshot().await;
        Ok(())
    }

    async fn refresh_meta_snapshot(&mut self) {
        self.run.meta.block_number = self.state.current_block().await;
        self.run.meta.vm_block_number = self.state.current_vm_block().await;
        self.run.meta.rfq_block_number = self.state.current_rfq_block().await;
        self.run.meta.total_pools = Some(self.state.total_pools().await);
    }

    async fn load_tokens(&mut self, pair: &RequestPair) -> Result<(Token, Token), RequestExit> {
        let (token_in_res, token_out_res) = tokio::join!(
            self.state.tokens.ensure(&pair.token_in_bytes),
            self.state.tokens.ensure(&pair.token_out_bytes)
        );
        let token_in = self.handle_token_lookup(token_in_res, &pair.token_in_address)?;
        let token_out = self.handle_token_lookup(token_out_res, &pair.token_out_address)?;
        Ok((token_in, token_out))
    }

    fn handle_token_lookup(
        &mut self,
        lookup: Result<Option<Token>, TokenStoreError>,
        token_address: &str,
    ) -> Result<Token, RequestExit> {
        match lookup {
            Ok(Some(token)) => Ok(token),
            Ok(None) => {
                self.push_failure(make_failure(
                    QuoteFailureKind::TokenCoverage,
                    format!("Token not found: {token_address}"),
                    None,
                ));
                Err(RequestExit::new(
                    QuoteStatus::TokenMissing,
                    QuoteResultQuality::RequestLevelFailure,
                ))
            }
            Err(TokenStoreError::FetchTimeout(duration)) => {
                let timeout_ms = duration.as_millis() as u64;
                warn!(
                    request_id = self.request.request_id.as_str(),
                    auction_id = self.request.auction_id.as_deref(),
                    token = ?token_address,
                    timeout_ms,
                    "Token metadata fetch timed out"
                );
                self.push_failure(make_failure(
                    QuoteFailureKind::TokenCoverage,
                    format!(
                        "Token metadata fetch timed out after {}ms: {}",
                        timeout_ms, token_address
                    ),
                    None,
                ));
                Err(RequestExit::new(
                    QuoteStatus::TokenMissing,
                    QuoteResultQuality::RequestLevelFailure,
                ))
            }
            Err(TokenStoreError::RequestFailed(message)) => {
                warn!(
                    request_id = self.request.request_id.as_str(),
                    auction_id = self.request.auction_id.as_deref(),
                    token = ?token_address,
                    error = %message,
                    "Token metadata fetch failed"
                );
                self.push_failure(make_failure(
                    QuoteFailureKind::TokenCoverage,
                    format!("Token metadata fetch failed: {}", message),
                    None,
                ));
                Err(RequestExit::new(
                    QuoteStatus::TokenMissing,
                    QuoteResultQuality::RequestLevelFailure,
                ))
            }
        }
    }

    fn parse_amounts(&mut self) -> Result<Arc<Vec<BigUint>>, RequestExit> {
        let amounts_in: Vec<BigUint> = match self
            .request
            .amounts
            .iter()
            .map(|amount| BigUint::from_str(amount))
            .collect()
        {
            Ok(vec) => vec,
            Err(err) => {
                self.push_failure(make_failure(
                    QuoteFailureKind::InvalidRequest,
                    format!("Invalid amount: {}", err),
                    None,
                ));
                return Err(RequestExit::new(
                    QuoteStatus::InvalidRequest,
                    QuoteResultQuality::RequestLevelFailure,
                ));
            }
        };
        Ok(Arc::new(amounts_in))
    }

    async fn prepare_execution(
        &mut self,
        pair: &RequestPair,
        token_in_ref: Token,
        token_out_ref: Token,
        amounts_in: Arc<Vec<BigUint>>,
    ) -> Result<PreparedQuoteExecution, RequestExit> {
        debug!(
            "Processing quote: {} ({}) -> {} ({})",
            token_in_ref.symbol,
            pair.token_in_address,
            token_out_ref.symbol,
            pair.token_out_address
        );
        let expected_len = amounts_in.len();
        let initial_vm_ready = self.state.vm_ready().await;
        let initial_rfq_ready = self.state.rfq_ready().await;
        let rebuild_guard = self
            .state
            .acquire_simulation_rebuild_guard(initial_vm_ready, initial_rfq_ready)
            .await;
        let vm_ready = initial_vm_ready && self.state.vm_ready().await;
        let rfq_ready = initial_rfq_ready && self.state.rfq_ready().await;
        if self.state.enable_vm_pools && !vm_ready {
            self.run.metrics.skipped_vm_unavailable = true;
        }
        if self.state.enable_rfq_pools && !rfq_ready {
            self.run.metrics.skipped_rfq_unavailable = true;
        }
        self.refresh_meta_snapshot().await;
        let mut candidates = self.load_candidate_sets(pair, vm_ready, rfq_ready).await;
        candidates.rfq_candidates = filter_directional_rfq_candidates(
            candidates.rfq_candidates,
            &token_in_ref,
            &token_out_ref,
        );
        self.prepare_candidate_metadata(
            &pair.token_in_address,
            &pair.token_out_address,
            &token_in_ref,
            &token_out_ref,
            expected_len,
            &mut candidates,
        )?;
        Ok(PreparedQuoteExecution {
            token_in: Arc::new(token_in_ref),
            token_out: Arc::new(token_out_ref),
            amounts_in,
            expected_len,
            native_candidates: candidates.native_candidates,
            vm_candidates: candidates.vm_candidates,
            rfq_candidates: candidates.rfq_candidates,
            rebuild_guard,
            cancel_token: self
                .cancel
                .as_ref()
                .map(CancellationToken::child_token)
                .unwrap_or_default(),
        })
    }

    async fn load_candidate_sets(
        &mut self,
        pair: &RequestPair,
        vm_ready: bool,
        rfq_ready: bool,
    ) -> CandidateSets {
        let native_candidates = self
            .state
            .native_state_store
            .matching_pools_by_addresses(&pair.token_in_bytes, &pair.token_out_bytes)
            .await
            .into_iter()
            .map(|(id, (pool_state, component))| (id, pool_state, component))
            .collect();
        let native_candidates = filter_unsupported_erc4626_candidates(
            native_candidates,
            &pair.token_in_bytes,
            &pair.token_out_bytes,
            self.state.erc4626_deposits_enabled,
            &self.state.erc4626_pair_policies,
        );
        let vm_candidates = if vm_ready {
            self.state
                .vm_state_store
                .matching_pools_by_addresses(&pair.token_in_bytes, &pair.token_out_bytes)
                .await
                .into_iter()
                .map(|(id, (pool_state, component))| (id, pool_state, component))
                .collect()
        } else {
            Vec::new()
        };
        let rfq_candidates = if rfq_ready {
            self.state
                .rfq_state_store
                .matching_pools_by_addresses(&pair.token_in_bytes, &pair.token_out_bytes)
                .await
                .into_iter()
                .map(|(id, (pool_state, component))| (id, pool_state, component))
                .collect()
        } else {
            Vec::new()
        };
        CandidateSets {
            native_candidates,
            vm_candidates,
            rfq_candidates,
        }
    }

    fn prepare_candidate_metadata(
        &mut self,
        token_in_address: &str,
        token_out_address: &str,
        token_in_ref: &Token,
        token_out_ref: &Token,
        expected_len: usize,
        candidates: &mut CandidateSets,
    ) -> Result<(), RequestExit> {
        let total_candidates = candidates.native_candidates.len()
            + candidates.vm_candidates.len()
            + candidates.rfq_candidates.len();
        self.run.meta.matching_pools = total_candidates;
        self.run.meta.candidate_pools = total_candidates;
        self.run.failures.reserve(total_candidates);
        if total_candidates == 0 {
            self.push_failure(make_failure(
                QuoteFailureKind::NoPools,
                format!(
                    "No matching pools found for pair {}-{}",
                    token_in_address, token_out_address
                ),
                None,
            ));
            return Err(RequestExit::new(
                QuoteStatus::NoLiquidity,
                QuoteResultQuality::NoResults,
            ));
        }
        debug!(
            "Quote candidates prepared: matching_pools={} amounts_per_pool={}, {} ({}) -> {} ({})",
            self.run.meta.matching_pools,
            expected_len,
            token_in_ref.symbol,
            token_in_address,
            token_out_ref.symbol,
            token_out_address
        );
        candidates.native_candidates.sort_by(|a, b| a.0.cmp(&b.0));
        candidates.vm_candidates.sort_by(|a, b| a.0.cmp(&b.0));
        candidates.rfq_candidates.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(())
    }

    async fn execute_pool_quotes(&mut self, prepared: PreparedQuoteExecution) {
        let mut tasks = self.schedule_pool_tasks(&prepared);
        let mut vm_first_gases = Vec::new();
        let mut rfq_first_gases = Vec::new();
        if !tasks.is_empty() {
            self.collect_pool_task_results(
                &prepared,
                &mut tasks,
                &mut vm_first_gases,
                &mut rfq_first_gases,
            )
            .await;
        }
        drop(tasks);
        finalize_vm_first_gas_metrics(&mut self.run.metrics, &mut vm_first_gases);
        finalize_rfq_first_gas_metrics(&mut self.run.metrics, &mut rfq_first_gases);
        self.apply_empty_response_state();
    }

    fn schedule_pool_tasks(
        &mut self,
        prepared: &PreparedQuoteExecution,
    ) -> FuturesUnordered<PoolTask> {
        let mut tasks = FuturesUnordered::new();
        for (kind, candidates) in [
            (ScheduledPoolKind::Native, &prepared.native_candidates),
            (ScheduledPoolKind::Vm, &prepared.vm_candidates),
            (ScheduledPoolKind::Rfq, &prepared.rfq_candidates),
        ] {
            self.schedule_pool_group(kind, candidates, prepared, &mut tasks);
        }
        tasks
    }

    fn schedule_pool_group(
        &mut self,
        kind: ScheduledPoolKind,
        candidates: &[CandidatePool],
        prepared: &PreparedQuoteExecution,
        tasks: &mut FuturesUnordered<PoolTask>,
    ) {
        for (id, pool_state, component) in candidates.iter() {
            if prepared.cancel_token.is_cancelled() {
                break;
            }
            let descriptor = pool_descriptor(id.clone(), component.as_ref());
            let scheduled = prepare_pool_simulation(PreparePoolSimulationInput {
                kind,
                pool_state: Arc::clone(pool_state),
                component: Arc::clone(component),
                descriptor,
                cancel_token: &prepared.cancel_token,
                token_in: prepared.token_in.as_ref(),
                token_out: prepared.token_out.as_ref(),
                metrics: &mut self.run.metrics,
            });
            tasks.push(Box::pin(simulate_pool(SimulatePoolInput {
                descriptor: scheduled.descriptor,
                pool_state: scheduled.pool_state,
                rebuild_guard: kind
                    .uses_rebuild_guard()
                    .then(|| Arc::clone(&prepared.rebuild_guard)),
                token_in: scheduled.sim_token_in,
                token_out: scheduled.sim_token_out,
                amounts: Arc::clone(&prepared.amounts_in),
                slippage: self.state.slippage,
                expected_len: prepared.expected_len,
                cancel_token: scheduled.pool_cancel,
            })));
        }
    }

    async fn collect_pool_task_results(
        &mut self,
        prepared: &PreparedQuoteExecution,
        tasks: &mut FuturesUnordered<PoolTask>,
        vm_first_gases: &mut Vec<u64>,
        rfq_first_gases: &mut Vec<u64>,
    ) {
        while !tasks.is_empty() {
            tokio::select! {
                _ = prepared.cancel_token.cancelled() => {
                    self.run.classification.mark_request_degradation();
                    self.run
                        .classification
                        .note_pool_outcome(PoolOutcomeKind::TimedOut);
                    self.push_failure(make_failure(
                        QuoteFailureKind::Timeout,
                        "Quote computation cancelled before all pools completed".to_string(),
                        None,
                    ));
                    break;
                }
                maybe_outcome = tasks.next() => {
                    let Some(outcome) = maybe_outcome else {
                        break;
                    };
                    self.handle_pool_task_outcome(
                        outcome,
                        prepared,
                        vm_first_gases,
                        rfq_first_gases,
                    );
                }
            }
        }
    }

    fn handle_pool_task_outcome(
        &mut self,
        outcome: Result<PoolSimOutcome, QuoteFailure>,
        prepared: &PreparedQuoteExecution,
        vm_first_gases: &mut Vec<u64>,
        rfq_first_gases: &mut Vec<u64>,
    ) {
        match outcome {
            Ok(PoolSimOutcome::Simulated(result)) => {
                self.handle_simulated_pool_result(result, prepared, vm_first_gases, rfq_first_gases)
            }
            Err(failure) => self.handle_pool_failure(failure, prepared.expected_len),
        }
    }

    fn handle_simulated_pool_result(
        &mut self,
        mut result: PoolQuoteResult,
        prepared: &PreparedQuoteExecution,
        vm_first_gases: &mut Vec<u64>,
        rfq_first_gases: &mut Vec<u64>,
    ) {
        if result.successful_steps > 0 {
            while result.amounts_out.len() < prepared.expected_len {
                result.amounts_out.push("0".to_string());
                result.gas_used.push(0);
            }
        }
        let has_usable_output = amounts_out_include_positive_quote(&result.amounts_out);
        let had_timeout = is_timeout_like_outcome(&result);
        let is_partial = has_usable_output && result.successful_steps < prepared.expected_len;
        record_vm_first_gas_metrics(
            &mut self.run.metrics,
            vm_first_gases,
            result.protocol.as_str(),
            result.pool.as_str(),
            result.first_successful_gas_used,
        );
        record_rfq_first_gas_metrics(
            &mut self.run.metrics,
            rfq_first_gases,
            result.protocol.as_str(),
            result.pool.as_str(),
            result.first_successful_gas_used,
        );
        self.record_pool_outcome(&result, prepared.expected_len);
        self.record_pool_errors(&result, had_timeout, is_partial, prepared.expected_len);
        if has_usable_output {
            self.run.responses.push(AmountOutResponse {
                pool: result.pool,
                pool_name: result.pool_name,
                pool_address: result.pool_address,
                amounts_out: result.amounts_out,
                slippage: result.slippage,
                limit_max_in: result.limit_max_in.map(|value| value.to_string()),
                gas_used: result.gas_used,
                block_number: self.run.meta.block_number,
            });
            self.run.classification.note_response();
        }
    }

    fn record_pool_outcome(&mut self, result: &PoolQuoteResult, expected_len: usize) {
        if let Some((outcome, reason)) = classify_pool_outcome(result, expected_len) {
            self.run.classification.note_pool_outcome_with_steps(
                outcome,
                result.successful_steps,
                expected_len,
            );
            self.run
                .pool_results
                .push(make_pool_outcome(PoolOutcomeInput {
                    descriptor: PoolDescriptor {
                        id: result.pool.clone(),
                        name: result.pool_name.clone(),
                        address: result.pool_address.clone(),
                        protocol: result.protocol.clone(),
                    },
                    outcome,
                    reported_steps: result.successful_steps,
                    expected_steps: expected_len,
                    reason,
                }));
        }
    }

    fn record_pool_errors(
        &mut self,
        result: &PoolQuoteResult,
        had_timeout: bool,
        is_partial: bool,
        expected_len: usize,
    ) {
        if result.errors.is_empty() && !is_partial {
            return;
        }
        let context = FailureContext {
            pool_id: &result.pool,
            pool_name: Some(&result.pool_name),
            pool_address: Some(&result.pool_address),
            protocol: Some(&result.protocol),
        };
        let descriptor = format_pool_descriptor(&context);
        let message = if had_timeout {
            format!(
                "{}: Pool quote stopped after {} of {} requested amounts produced usable quotes",
                descriptor, result.successful_steps, expected_len
            )
        } else if result.successful_steps == 0 {
            let base_error = result
                .errors
                .first()
                .cloned()
                .unwrap_or_else(|| "Pool returned no quotes".to_string());
            format!("{}: {}", descriptor, base_error)
        } else if is_partial {
            let Some(base_error) = result.errors.first().cloned() else {
                return;
            };
            format!("{}: {}", descriptor, base_error)
        } else {
            let base_error = result
                .errors
                .first()
                .cloned()
                .unwrap_or_else(|| "Pool returned a malformed result".to_string());
            format!("{}: {}", descriptor, base_error)
        };
        let kind = if had_timeout {
            QuoteFailureKind::Timeout
        } else {
            classify_failure(&message, true)
        };
        self.push_failure(make_failure(kind, message, Some(context)));
    }

    fn handle_pool_failure(&mut self, failure: QuoteFailure, expected_len: usize) {
        if let Some(pool_outcome) = pool_outcome_from_failure(&failure, expected_len) {
            self.run
                .classification
                .note_pool_outcome(pool_outcome.outcome);
            self.run.pool_results.push(pool_outcome);
        }
        self.push_failure(failure);
    }

    fn apply_empty_response_state(&mut self) {
        if self.run.classification.had_usable_response {
            return;
        }
        if self.run.failures.is_empty() {
            self.push_failure(make_failure(
                QuoteFailureKind::Simulator,
                "All pools returned zero amounts".to_string(),
                None,
            ));
        }
    }

    fn finalize_responses(&mut self) {
        if self.run.responses.is_empty() {
            return;
        }
        self.run.responses.sort_by(|a, b| {
            a.pool
                .cmp(&b.pool)
                .then(a.pool_address.cmp(&b.pool_address))
        });
        let first = &self.run.responses[0];
        debug!(
            "Quote response sample: total_results={} first_pool={} address={} first_amount_out={} block={} vm_block={:?}",
            self.run.responses.len(),
            first.pool_name,
            first.pool_address,
            first.amounts_out.first().cloned().unwrap_or_else(|| "0".to_string()),
            first.block_number,
            self.run.meta.vm_block_number
        );
    }

    fn finalize_pool_results(&mut self) {
        self.run.pool_results.sort_by(|a, b| {
            a.protocol
                .cmp(&b.protocol)
                .then(a.pool.cmp(&b.pool))
                .then(a.pool_address.cmp(&b.pool_address))
                .then(outcome_kind_label(a.outcome).cmp(outcome_kind_label(b.outcome)))
        });
    }
}

fn filter_unsupported_erc4626_candidates(
    candidates: Vec<CandidatePool>,
    token_in: &Bytes,
    token_out: &Bytes,
    erc4626_deposits_enabled: bool,
    erc4626_pair_policies: &[crate::models::erc4626::Erc4626PairPolicy],
) -> Vec<CandidatePool> {
    candidates
        .into_iter()
        .filter(|(candidate_id, _, component)| {
            let supported = component_direction_supported(
                component.as_ref(),
                token_in,
                token_out,
                erc4626_deposits_enabled,
                erc4626_pair_policies,
            );
            if !supported {
                debug!(
                    scope = "erc4626_gate",
                    candidate_id,
                    component_id = %component.id,
                    protocol = component.protocol_system.as_str(),
                    token_in = %token_in,
                    token_out = %token_out,
                    "Skipping unsupported ERC4626 candidate"
                );
            }
            supported
        })
        .collect()
}

fn simulation_tokens_for_pool(
    request_token_in: &Token,
    request_token_out: &Token,
    component: &ProtocolComponent,
) -> (Token, Token) {
    (
        remap_request_token_for_pool(request_token_in, component),
        remap_request_token_for_pool(request_token_out, component),
    )
}

fn filter_directional_rfq_candidates(
    candidates: Vec<CandidatePool>,
    request_token_in: &Token,
    request_token_out: &Token,
) -> Vec<CandidatePool> {
    candidates
        .into_iter()
        .filter(|(pool_id, _, component)| {
            if !is_directional_rfq_component(component.as_ref()) {
                return true;
            }

            if directional_rfq_candidate_matches_direction(
                request_token_in,
                request_token_out,
                component,
            ) {
                return true;
            }

            let (sim_token_in, sim_token_out) =
                simulation_tokens_for_pool(request_token_in, request_token_out, component);
            let expected_token_in = component
                .tokens
                .first()
                .map(|token| token.address.to_string());
            let expected_token_out = component
                .tokens
                .get(1)
                .map(|token| token.address.to_string());
            debug!(
                scope = "directional_rfq_filter",
                pool_id = %pool_id,
                protocol = component.protocol_system.as_str(),
                requested_token_in = %request_token_in.address,
                requested_token_out = %request_token_out.address,
                simulation_token_in = %sim_token_in.address,
                simulation_token_out = %sim_token_out.address,
                expected_token_in = ?expected_token_in,
                expected_token_out = ?expected_token_out,
                "Skipping directional RFQ candidate whose stored direction does not match request"
            );
            false
        })
        .collect()
}

fn directional_rfq_candidate_matches_direction(
    request_token_in: &Token,
    request_token_out: &Token,
    component: &ProtocolComponent,
) -> bool {
    let Some(expected_token_in) = component.tokens.first() else {
        return false;
    };
    let Some(expected_token_out) = component.tokens.get(1) else {
        return false;
    };

    // Some RFQ integrations store directional pools instead of symmetric pairs.
    // Keep those candidates only when the ordered pool tokens line up with the
    // remapped simulation direction for this request.
    let (sim_token_in, sim_token_out) =
        simulation_tokens_for_pool(request_token_in, request_token_out, component);

    sim_token_in.address == expected_token_in.address
        && sim_token_out.address == expected_token_out.address
}

fn simulation_tokens_for_pool_with_remap_log(
    request_token_in: &Token,
    request_token_out: &Token,
    component: &ProtocolComponent,
    pool_id: &str,
    pool_protocol: &str,
) -> (Arc<Token>, Arc<Token>) {
    let (sim_token_in, sim_token_out) =
        simulation_tokens_for_pool(request_token_in, request_token_out, component);
    if sim_token_in.address != request_token_in.address
        || sim_token_out.address != request_token_out.address
    {
        debug!(
            scope = "native_wrapped_remap",
            pool_id = %pool_id,
            protocol = %pool_protocol,
            requested_token_in = %request_token_in.address,
            simulation_token_in = %sim_token_in.address,
            requested_token_out = %request_token_out.address,
            simulation_token_out = %sim_token_out.address,
            "Remapped request tokens for pool simulation"
        );
    }

    (Arc::new(sim_token_in), Arc::new(sim_token_out))
}

fn remap_request_token_for_pool(request_token: &Token, component: &ProtocolComponent) -> Token {
    let native = request_token.chain.native_token();
    let wrapped_native = request_token.chain.wrapped_native_token();

    if native.address == wrapped_native.address {
        return request_token.clone();
    }

    let pool_has_native = component
        .tokens
        .iter()
        .any(|token| token.address == native.address);
    let pool_has_wrapped = component
        .tokens
        .iter()
        .any(|token| token.address == wrapped_native.address);

    if request_token.address == wrapped_native.address && pool_has_native && !pool_has_wrapped {
        return native;
    }

    if request_token.address == native.address && pool_has_wrapped && !pool_has_native {
        return wrapped_native;
    }

    request_token.clone()
}

fn is_directional_rfq_component(component: &ProtocolComponent) -> bool {
    matches!(
        (
            component.protocol_system.as_str(),
            component.protocol_type_name.as_str()
        ),
        ("rfq:hashflow", _) | (_, "hashflow_pool") | ("rfq:liquorice", _) | (_, "liquorice_pool")
    )
}

fn is_direct_native_wrapped_pair(
    token_in: &Bytes,
    token_out: &Bytes,
    wrapped_native: Option<&Bytes>,
) -> bool {
    let native_address = native_token_address();
    let Some(wrapped_native) = wrapped_native else {
        return false;
    };

    (token_in == &native_address && token_out == wrapped_native)
        || (token_in == wrapped_native && token_out == &native_address)
}

struct PoolQuoteResult {
    pool: String,
    pool_name: String,
    pool_address: String,
    protocol: String,
    amounts_out: Vec<String>,
    slippage: Vec<u32>,
    limit_max_in: Option<BigUint>,
    gas_used: Vec<u64>,
    successful_steps: usize,
    first_successful_gas_used: Option<u64>,
    errors: Vec<String>,
    timed_out: bool,
}

enum PoolSimOutcome {
    Simulated(PoolQuoteResult),
}

struct PoolQuoteAccumulator {
    amounts_out: Vec<String>,
    gas_used: Vec<u64>,
    successful_steps: usize,
    first_successful_gas_used: Option<u64>,
    errors: Vec<String>,
    timed_out: bool,
}

impl PoolQuoteAccumulator {
    fn new(expected_len: usize) -> Self {
        Self {
            amounts_out: Vec::with_capacity(expected_len),
            gas_used: Vec::with_capacity(expected_len),
            successful_steps: 0,
            first_successful_gas_used: None,
            errors: Vec::new(),
            timed_out: false,
        }
    }
}

struct FailureContext<'a> {
    pool_id: &'a str,
    pool_name: Option<&'a str>,
    pool_address: Option<&'a str>,
    protocol: Option<&'a str>,
}

struct PoolSimulationRunner {
    descriptor: PoolDescriptor,
    pool_state: Arc<dyn ProtocolSim>,
    token_in: Arc<Token>,
    token_out: Arc<Token>,
    amounts: Arc<Vec<BigUint>>,
    slippage: SlippageConfig,
    expected_len: usize,
    cancel_token: CancellationToken,
}

struct AmountQuote {
    amount_in: BigUint,
    outcome: AmountQuoteOutcome,
}

enum AmountQuoteOutcome {
    Quoted { amount_out: BigUint, gas_used: u64 },
    Error(String),
    MissingGas,
    Cancelled,
}

impl PoolSimulationRunner {
    fn run(self) -> PoolSimOutcome {
        if let Some(outcome) = self.preflight_outcome() {
            return outcome;
        }
        self.quote_amounts()
    }

    fn preflight_outcome(&self) -> Option<PoolSimOutcome> {
        if self.cancel_token.is_cancelled() {
            return Some(self.immediate_outcome(
                "Cancelled",
                false,
                "Pool quote cancelled before completion",
            ));
        }
        None
    }

    fn immediate_outcome(&self, error: &str, timed_out: bool, log_message: &str) -> PoolSimOutcome {
        self.log_pool_debug("pool_simulation", log_message);
        PoolSimOutcome::Simulated(PoolQuoteResult {
            pool: self.descriptor.id.clone(),
            pool_name: self.descriptor.name.clone(),
            pool_address: self.descriptor.address.clone(),
            protocol: self.descriptor.protocol.clone(),
            amounts_out: Vec::new(),
            slippage: Vec::new(),
            limit_max_in: None,
            gas_used: Vec::new(),
            successful_steps: 0,
            first_successful_gas_used: None,
            errors: vec![error.to_string()],
            timed_out,
        })
    }

    fn quote_amounts(&self) -> PoolSimOutcome {
        let mut run = PoolQuoteAccumulator::new(self.expected_len);
        let (limit_max_in, limit_max_out) = self.pool_limits();
        let quotes = self.quote_requested_amounts();
        for quote in quotes {
            if self.record_amount_quote(quote, limit_max_in.as_ref(), &mut run) {
                break;
            }
        }
        self.finish_quote_amounts(run, limit_max_in, limit_max_out)
    }

    fn quote_requested_amounts(&self) -> Vec<AmountQuote> {
        if self.amounts.len() <= 1 {
            return self
                .amounts
                .iter()
                .map(|amount_in| self.quote_requested_amount(amount_in))
                .collect();
        }

        self.amounts
            .par_iter()
            .map(|amount_in| self.quote_requested_amount(amount_in))
            .collect()
    }

    fn quote_requested_amount(&self, amount_in: &BigUint) -> AmountQuote {
        if self.cancel_token.is_cancelled() {
            return AmountQuote {
                amount_in: amount_in.clone(),
                outcome: AmountQuoteOutcome::Cancelled,
            };
        }

        let outcome =
            match self
                .pool_state
                .get_amount_out(amount_in.clone(), &self.token_in, &self.token_out)
            {
                Ok(result) => {
                    let Some(gas_used) = result.gas.to_u64() else {
                        return AmountQuote {
                            amount_in: amount_in.clone(),
                            outcome: AmountQuoteOutcome::MissingGas,
                        };
                    };
                    AmountQuoteOutcome::Quoted {
                        amount_out: result.amount,
                        gas_used,
                    }
                }
                Err(error) => AmountQuoteOutcome::Error(error.to_string()),
            };

        AmountQuote {
            amount_in: amount_in.clone(),
            outcome,
        }
    }

    fn record_amount_quote(
        &self,
        quote: AmountQuote,
        limit_max_in: Option<&BigUint>,
        run: &mut PoolQuoteAccumulator,
    ) -> bool {
        if let Some(max_in) = limit_max_in {
            if quote.amount_in > *max_in {
                self.log_soft_limit_context(
                    &quote.amount_in,
                    max_in,
                    "Attempting quote above reported soft limit",
                );
            }
        }

        match quote.outcome {
            AmountQuoteOutcome::Quoted {
                amount_out,
                gas_used,
            } => {
                if amount_out.is_zero() {
                    // Zero outputs mean this requested amount did not produce a usable quote,
                    // even when the simulator returned Ok(...).
                    run.amounts_out.push("0".to_string());
                    run.gas_used.push(0);
                } else {
                    run.amounts_out.push(amount_out.to_string());
                    run.gas_used.push(gas_used);
                    run.successful_steps += 1;
                    run.first_successful_gas_used.get_or_insert(gas_used);
                }
                false
            }
            AmountQuoteOutcome::Error(message) => {
                if let Some(max_in) = limit_max_in {
                    self.log_soft_limit_failure(&quote.amount_in, max_in, &message);
                }
                self.log_pool_error(&message);
                run.amounts_out.push("0".to_string());
                run.gas_used.push(0);
                run.errors.push(message);
                false
            }
            AmountQuoteOutcome::MissingGas => {
                let message = "no gas reported".to_string();
                self.log_pool_error(&message);
                run.errors.push(message);
                run.amounts_out.clear();
                run.gas_used.clear();
                run.successful_steps = 0;
                run.first_successful_gas_used = None;
                true
            }
            AmountQuoteOutcome::Cancelled => {
                self.log_pool_debug("pool_simulation", "Pool quote cancelled before completion");
                run.errors.push("Cancelled".to_string());
                run.timed_out = true;
                true
            }
        }
    }

    fn pool_limits(&self) -> (Option<BigUint>, Option<BigUint>) {
        self.pool_state
            .get_limits(
                self.token_in.address.clone(),
                self.token_out.address.clone(),
            )
            .ok()
            .map(|(max_in, max_out)| (Some(max_in), Some(max_out)))
            .unwrap_or((None, None))
    }

    fn finish_quote_amounts(
        &self,
        mut run: PoolQuoteAccumulator,
        limit_max_in: Option<BigUint>,
        limit_max_out: Option<BigUint>,
    ) -> PoolSimOutcome {
        // Only pools with at least one usable amount output are emitted, but they still need
        // full requested-amount alignment.
        let (slippage, limit_max_in) = if run.successful_steps > 0 {
            let effective_limit_max_in = sanitize_limit_max_in(
                self.amounts.as_ref(),
                &run.amounts_out,
                limit_max_in.as_ref(),
                limit_max_out.as_ref(),
            );
            let mut slippage =
                self.compute_local_slippage_ladder(&run.amounts_out, limit_max_out.as_ref());
            while run.amounts_out.len() < self.expected_len {
                run.amounts_out.push("0".to_string());
                run.gas_used.push(0);
                slippage.push(0);
            }
            (slippage, effective_limit_max_in)
        } else {
            (Vec::new(), limit_max_in)
        };

        PoolSimOutcome::Simulated(PoolQuoteResult {
            pool: self.descriptor.id.clone(),
            pool_name: self.descriptor.name.clone(),
            pool_address: self.descriptor.address.clone(),
            protocol: self.descriptor.protocol.clone(),
            amounts_out: run.amounts_out,
            slippage,
            limit_max_in,
            gas_used: run.gas_used,
            successful_steps: run.successful_steps,
            first_successful_gas_used: run.first_successful_gas_used,
            errors: run.errors,
            timed_out: run.timed_out,
        })
    }

    fn compute_local_slippage_ladder(
        &self,
        amounts_out: &[String],
        limit_max_out: Option<&BigUint>,
    ) -> Vec<u32> {
        // When the ladder never approaches a trustworthy hard output limit, use the
        // observed ladder envelope as a softer utilization anchor instead.
        let soft_ladder_max_out = observed_soft_ladder_max_out(self.amounts.as_ref(), amounts_out);
        let first_usable_quote = amounts_out
            .iter()
            .enumerate()
            .find_map(|(index, amount_out)| {
                let amount_in = self.amounts.get(index)?;
                let amount_out = BigUint::from_str(amount_out).ok()?;
                (!amount_out.is_zero()).then(|| (amount_in.clone(), amount_out))
            });

        let mut slippage = amounts_out
            .iter()
            .enumerate()
            .map(|(index, amount_out)| {
                let Some(amount_in) = self.amounts.get(index) else {
                    return 0;
                };
                let Ok(amount_out) = BigUint::from_str(amount_out) else {
                    return 0;
                };
                if amount_out.is_zero() {
                    return 0;
                }

                let stress_quote = self.stress_quote_for_index(index, amount_in, amounts_out);
                let base_slippage = compute_dynamic_slippage_bps(
                    amount_in,
                    &amount_out,
                    stress_quote.as_ref().map(|(amount, _)| amount),
                    stress_quote.as_ref().map(|(_, amount_out)| amount_out),
                    limit_max_out,
                    soft_ladder_max_out.as_ref(),
                    &self.slippage,
                );
                let cumulative_penalty =
                    first_usable_quote
                        .as_ref()
                        .map_or(0, |(base_in, base_out)| {
                            cumulative_degradation_slippage_bps(
                                base_in,
                                base_out,
                                amount_in,
                                &amount_out,
                                &self.slippage,
                            )
                        });
                (base_slippage + cumulative_penalty).min(self.slippage.max_dynamic_slippage_bps)
            })
            .collect::<Vec<_>>();
        enforce_non_decreasing_slippage(amounts_out, &mut slippage);
        slippage
    }

    fn stress_quote_for_index(
        &self,
        index: usize,
        amount_in: &BigUint,
        amounts_out: &[String],
    ) -> Option<(BigUint, BigUint)> {
        match self.next_ladder_stress_quote(index, amounts_out) {
            LadderStressQuote::Quote(quote) => Some(quote),
            LadderStressQuote::PartialTail => None,
            LadderStressQuote::Missing => self.compute_local_stress_quote(amount_in),
        }
    }

    fn next_ladder_stress_quote(&self, index: usize, amounts_out: &[String]) -> LadderStressQuote {
        // TODO: Make this order-independent by selecting the next larger usable rung instead of
        // assuming the next request slot is the next larger trade.
        let Some(stressed_amount_in) = self.amounts.get(index + 1).cloned() else {
            return LadderStressQuote::Missing;
        };
        let Some(stressed_amount_out) = amounts_out
            .get(index + 1)
            .and_then(|amount| BigUint::from_str(amount).ok())
        else {
            return LadderStressQuote::Missing;
        };
        if stressed_amount_out.is_zero() {
            return LadderStressQuote::PartialTail;
        }

        LadderStressQuote::Quote((stressed_amount_in, stressed_amount_out))
    }

    fn compute_local_stress_quote(&self, amount_in: &BigUint) -> Option<(BigUint, BigUint)> {
        if self.cancel_token.is_cancelled() {
            return None;
        }

        let stress_in = compute_stress_input(amount_in);
        let stressed_amount_in = amount_in + &stress_in;
        let stressed_amount_out = self
            .pool_state
            .get_amount_out(stressed_amount_in.clone(), &self.token_in, &self.token_out)
            .ok()?
            .amount;
        if stressed_amount_out.is_zero() {
            return None;
        }

        Some((stressed_amount_in, stressed_amount_out))
    }

    fn log_pool_error(&self, message: &str) {
        self.log_pool_debug("pool_simulation", &format!("Pool quote error: {}", message));
    }

    fn log_soft_limit_context(&self, amount_in: &BigUint, max_in: &BigUint, message: &str) {
        debug!(
            scope = "limits_context",
            pool_id = %self.descriptor.id,
            protocol = %self.descriptor.protocol,
            pool_name = %self.descriptor.name,
            pool_address = %self.descriptor.address,
            amount_in = %amount_in,
            max_in = %max_in,
            "{}",
            message,
        );
    }

    fn log_soft_limit_failure(&self, amount_in: &BigUint, max_in: &BigUint, message: &str) {
        if amount_in > max_in {
            self.log_soft_limit_context(
                amount_in,
                max_in,
                "Quote failed above reported soft limit",
            );
            return;
        }
        if is_limit_like_error_message(message) {
            self.log_soft_limit_context(
                amount_in,
                max_in,
                "Quote failed within reported soft limit",
            );
        }
    }

    fn log_pool_debug(&self, scope: &'static str, message: &str) {
        debug!(
            scope,
            pool_id = %self.descriptor.id,
            protocol = %self.descriptor.protocol,
            pool_name = %self.descriptor.name,
            pool_address = %self.descriptor.address,
            "{}",
            message,
        );
    }
}

async fn simulate_pool(input: SimulatePoolInput) -> Result<PoolSimOutcome, QuoteFailure> {
    let SimulatePoolInput {
        descriptor,
        pool_state,
        rebuild_guard,
        token_in,
        token_out,
        amounts,
        slippage,
        expected_len,
        cancel_token,
    } = input;
    let failure_descriptor = descriptor.clone();
    let result = SimulationExecutor::new()
        .run(move || {
            let _rebuild_guard = rebuild_guard;
            PoolSimulationRunner {
                descriptor,
                pool_state,
                token_in,
                token_out,
                amounts,
                slippage,
                expected_len,
                cancel_token,
            }
            .run()
        })
        .await;

    match result {
        Ok(result) => Ok(result),
        Err(SimulationExecutionError::WorkerPanicked(panic)) => {
            let context = failure_context(&failure_descriptor);
            let descriptor = format_pool_descriptor(&context);
            let panic_message = panic_payload_message(&panic);
            let message = format!(
                "{}: Quote computation panicked: {}",
                descriptor, panic_message
            );
            Err(make_failure(
                QuoteFailureKind::Internal,
                message,
                Some(context),
            ))
        }
        Err(SimulationExecutionError::ResultDropped) => {
            let context = failure_context(&failure_descriptor);
            let descriptor = format_pool_descriptor(&context);
            let message = format!("{}: Quote computation result dropped", descriptor);
            Err(make_failure(
                QuoteFailureKind::Internal,
                message,
                Some(context),
            ))
        }
    }
}

fn panic_payload_message(payload: &(dyn std::any::Any + Send + 'static)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "unknown panic payload".to_string()
}

fn failure_context(descriptor: &PoolDescriptor) -> FailureContext<'_> {
    FailureContext {
        pool_id: &descriptor.id,
        pool_name: Some(descriptor.name.as_str()),
        pool_address: Some(descriptor.address.as_str()),
        protocol: Some(descriptor.protocol.as_str()),
    }
}

fn classify_failure(message: &str, from_pool: bool) -> QuoteFailureKind {
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("cancelled") || lowered.contains("canceled") {
        // Treat cancellations as timeout-equivalent for downstream semantics
        QuoteFailureKind::Timeout
    } else if lowered.contains("warm") {
        QuoteFailureKind::WarmUp
    } else if lowered.contains("overflow") {
        QuoteFailureKind::Overflow
    } else if lowered.contains("timeout") {
        QuoteFailureKind::Timeout
    } else if !from_pool && lowered.contains("token") {
        QuoteFailureKind::TokenCoverage
    } else {
        QuoteFailureKind::Simulator
    }
}

fn make_pool_outcome(input: PoolOutcomeInput) -> PoolSimulationOutcome {
    PoolSimulationOutcome {
        pool: input.descriptor.id,
        pool_name: input.descriptor.name,
        pool_address: input.descriptor.address,
        protocol: input.descriptor.protocol,
        outcome: input.outcome,
        reported_steps: input.reported_steps,
        expected_steps: input.expected_steps,
        reason: input.reason,
    }
}

fn outcome_kind_label(kind: PoolOutcomeKind) -> &'static str {
    match kind {
        PoolOutcomeKind::PartialOutput => "partial_output",
        PoolOutcomeKind::ZeroOutput => "zero_output",
        PoolOutcomeKind::TimedOut => "timed_out",
        PoolOutcomeKind::SimulatorError => "simulator_error",
        PoolOutcomeKind::InternalError => "internal_error",
    }
}

fn is_timeout_message(message: &str) -> bool {
    let lowered = message.to_ascii_lowercase();
    lowered.contains("timeout")
        || lowered.contains("timed out")
        || lowered.contains("cancelled")
        || lowered.contains("canceled")
}

fn is_timeout_like_outcome(result: &PoolQuoteResult) -> bool {
    result.timed_out
        || result
            .errors
            .iter()
            .any(|message| is_timeout_message(message))
}

fn classify_pool_outcome(
    result: &PoolQuoteResult,
    expected_len: usize,
) -> Option<(PoolOutcomeKind, Option<String>)> {
    if is_timeout_like_outcome(result) {
        return Some((
            PoolOutcomeKind::TimedOut,
            result.errors.first().cloned().or_else(|| {
                Some(format!(
                    "Pool simulation timed out after {} reported step(s)",
                    result.successful_steps
                ))
            }),
        ));
    }

    if result.successful_steps == 0 {
        if !result.errors.is_empty() {
            return Some((
                PoolOutcomeKind::SimulatorError,
                result.errors.first().cloned(),
            ));
        }
        return Some((PoolOutcomeKind::ZeroOutput, None));
    }

    if result.errors.is_empty() && !amounts_out_include_positive_quote(&result.amounts_out) {
        return Some((PoolOutcomeKind::ZeroOutput, None));
    }

    if result.successful_steps < expected_len {
        return Some((
            PoolOutcomeKind::PartialOutput,
            Some(format!(
                "Pool returned usable quotes for {} of {} requested amounts",
                result.successful_steps, expected_len
            )),
        ));
    }

    if !result.errors.is_empty() {
        return Some((
            PoolOutcomeKind::SimulatorError,
            result.errors.first().cloned(),
        ));
    }

    None
}

fn pool_outcome_from_failure(
    failure: &QuoteFailure,
    expected_len: usize,
) -> Option<PoolSimulationOutcome> {
    let pool = failure.pool.clone()?;
    let pool_name = failure
        .pool_name
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    let pool_address = failure
        .pool_address
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    let protocol = failure
        .protocol
        .clone()
        .unwrap_or_else(|| "unknown".to_string());

    let outcome = match failure.kind {
        QuoteFailureKind::Timeout => PoolOutcomeKind::TimedOut,
        QuoteFailureKind::Internal => PoolOutcomeKind::InternalError,
        QuoteFailureKind::Simulator
        | QuoteFailureKind::Overflow
        | QuoteFailureKind::InconsistentResult => PoolOutcomeKind::SimulatorError,
        _ => return None,
    };

    Some(make_pool_outcome(PoolOutcomeInput {
        descriptor: PoolDescriptor {
            id: pool,
            name: pool_name,
            address: pool_address,
            protocol,
        },
        outcome,
        reported_steps: 0,
        expected_steps: expected_len,
        reason: Some(failure.message.clone()),
    }))
}

fn amounts_out_include_positive_quote(amounts_out: &[String]) -> bool {
    amounts_out
        .iter()
        .filter_map(|amount| BigUint::from_str(amount).ok())
        .any(|amount| !amount.is_zero())
}

fn sanitize_limit_max_in(
    amounts_in: &[BigUint],
    amounts_out: &[String],
    limit_max_in: Option<&BigUint>,
    limit_max_out: Option<&BigUint>,
) -> Option<BigUint> {
    let raw_limit_max_in = limit_max_in?.clone();
    let Some(limit_max_out) = limit_max_out.filter(|value| !value.is_zero()) else {
        return Some(raw_limit_max_in);
    };

    let saturation_threshold_numerator = limit_max_out * BigUint::from(SATURATION_THRESHOLD_BPS);
    let saturation_threshold_denominator = BigUint::from(BPS_DENOMINATOR);
    let saturated_amount_in = amounts_in
        .iter()
        .zip(amounts_out.iter())
        // TODO: Make this order-independent by taking the smallest saturated amount_in
        // instead of the first saturated rung in request order.
        .find_map(|(amount_in, amount_out)| {
            let amount_out = BigUint::from_str(amount_out).ok()?;
            ((&amount_out * &saturation_threshold_denominator) >= saturation_threshold_numerator)
                .then(|| amount_in.clone())
        });

    Some(match saturated_amount_in {
        Some(observed_cap) => raw_limit_max_in.min(observed_cap),
        None => raw_limit_max_in,
    })
}

fn compute_stress_input(amount_in: &BigUint) -> BigUint {
    (amount_in / STRESS_INPUT_DIVISOR).max(BigUint::from(MIN_STRESS_INPUT))
}

fn observed_soft_ladder_max_out(amounts_in: &[BigUint], amounts_out: &[String]) -> Option<BigUint> {
    let mut usable_quotes = amounts_in
        .iter()
        .zip(amounts_out.iter())
        .filter_map(|(amount_in, amount_out)| {
            let amount_out = BigUint::from_str(amount_out).ok()?;
            (!amount_out.is_zero()).then_some((amount_in, amount_out))
        })
        .collect::<Vec<_>>();
    usable_quotes
        .sort_by(|(left_amount_in, _), (right_amount_in, _)| left_amount_in.cmp(right_amount_in));

    let (base_amount_in, base_amount_out) = usable_quotes.first()?;
    if usable_quotes.len() < 2
        || !usable_quotes.iter().skip(1).any(|(amount_in, amount_out)| {
            rate_degradation_bps(base_amount_in, base_amount_out, amount_in, amount_out)
                >= SOFT_LADDER_ACTIVATION_DEGRADATION_BPS
        })
    {
        return None;
    }

    usable_quotes
        .into_iter()
        .map(|(_, amount_out)| amount_out)
        .max()
}

fn rate_degradation_bps(
    base_amount_in: &BigUint,
    base_amount_out: &BigUint,
    amount_in: &BigUint,
    amount_out: &BigUint,
) -> u32 {
    if base_amount_in.is_zero()
        || base_amount_out.is_zero()
        || amount_in.is_zero()
        || amount_out.is_zero()
    {
        return 0;
    }

    let base_rate_numerator = base_amount_out * amount_in;
    let current_rate_numerator = amount_out * base_amount_in;
    if current_rate_numerator >= base_rate_numerator {
        return 0;
    }

    round_div_biguint(
        (base_rate_numerator.clone() - current_rate_numerator) * BigUint::from(BPS_DENOMINATOR),
        &base_rate_numerator,
    )
    .to_u32()
    .unwrap_or(BPS_DENOMINATOR)
}

fn compute_dynamic_slippage_bps(
    amount_in: &BigUint,
    amount_out: &BigUint,
    stressed_amount_in: Option<&BigUint>,
    stressed_amount_out: Option<&BigUint>,
    limit_max_out: Option<&BigUint>,
    soft_ladder_max_out: Option<&BigUint>,
    slippage: &SlippageConfig,
) -> u32 {
    if amount_out.is_zero() {
        return 0;
    }

    let scale = BigUint::from(SLIPPAGE_SCALE);
    let mut total_scaled = BigUint::from(slippage.min_dynamic_slippage_bps) * &scale;

    let effective_utilization_scaled = effective_output_utilization_scaled(
        amount_out,
        limit_max_out,
        soft_ladder_max_out,
        &scale,
        slippage,
    );
    total_scaled += utilization_term_scaled(&effective_utilization_scaled, &scale, slippage);

    if let (Some(stressed_amount_in), Some(stressed_amount_out)) =
        (stressed_amount_in, stressed_amount_out)
    {
        if !stressed_amount_in.is_zero() && !stressed_amount_out.is_zero() {
            total_scaled += sensitivity_term_scaled(
                amount_in,
                amount_out,
                stressed_amount_in,
                stressed_amount_out,
                &scale,
                slippage,
            );
        }
    }

    let total_bps = round_div_biguint(total_scaled, &scale)
        .to_u32()
        .unwrap_or(slippage.max_dynamic_slippage_bps);
    let saturation_floor_bps = saturation_slippage_floor_bps(amount_out, limit_max_out, slippage)
        .unwrap_or(slippage.min_dynamic_slippage_bps);
    total_bps.max(saturation_floor_bps).clamp(
        slippage.min_dynamic_slippage_bps,
        slippage.max_dynamic_slippage_bps,
    )
}

fn effective_output_utilization_scaled(
    amount_out: &BigUint,
    limit_max_out: Option<&BigUint>,
    soft_ladder_max_out: Option<&BigUint>,
    scale: &BigUint,
    slippage: &SlippageConfig,
) -> BigUint {
    let hard_utilization_scaled = limit_max_out
        .filter(|max_out| !max_out.is_zero())
        .map(|max_out| round_div_biguint(amount_out * scale, max_out))
        .unwrap_or_else(BigUint::zero);
    let soft_utilization_scaled = soft_ladder_max_out
        .filter(|max_out| !max_out.is_zero())
        .map(|max_out| {
            round_div_biguint(
                amount_out * BigUint::from(slippage.soft_ladder_utilization_cap_bps) * scale,
                &(max_out * BigUint::from(BPS_DENOMINATOR)),
            )
        })
        .unwrap_or_else(BigUint::zero);

    hard_utilization_scaled
        .max(soft_utilization_scaled)
        .min(scale.clone())
}

fn utilization_term_scaled(
    utilization_scaled: &BigUint,
    scale: &BigUint,
    slippage: &SlippageConfig,
) -> BigUint {
    let numerator = BigUint::from(slippage.utilization_coefficient_bps)
        * utilization_scaled
        * utilization_scaled;
    round_div_biguint(numerator, scale)
}

fn cumulative_degradation_slippage_bps(
    base_amount_in: &BigUint,
    base_amount_out: &BigUint,
    amount_in: &BigUint,
    amount_out: &BigUint,
    slippage: &SlippageConfig,
) -> u32 {
    if base_amount_in.is_zero()
        || base_amount_out.is_zero()
        || amount_in.is_zero()
        || amount_out.is_zero()
    {
        return 0;
    }

    let base_rate_numerator = base_amount_out * amount_in;
    let current_rate_numerator = amount_out * base_amount_in;
    if current_rate_numerator >= base_rate_numerator {
        return 0;
    }

    let penalty = round_div_biguint(
        (base_rate_numerator.clone() - current_rate_numerator)
            * BigUint::from(slippage.cumulative_degradation_coefficient_bps),
        &base_rate_numerator,
    );
    penalty
        .to_u32()
        .unwrap_or(slippage.max_dynamic_slippage_bps)
}

fn enforce_non_decreasing_slippage(amounts_out: &[String], slippage: &mut [u32]) {
    let mut running_max = 0u32;
    for (amount_out, slippage_bps) in amounts_out.iter().zip(slippage.iter_mut()) {
        let Ok(amount_out) = BigUint::from_str(amount_out) else {
            continue;
        };
        if amount_out.is_zero() {
            continue;
        }
        running_max = running_max.max(*slippage_bps);
        *slippage_bps = running_max;
    }
}

fn saturation_slippage_floor_bps(
    amount_out: &BigUint,
    limit_max_out: Option<&BigUint>,
    slippage: &SlippageConfig,
) -> Option<u32> {
    let limit_max_out = limit_max_out.filter(|value| !value.is_zero())?;
    let utilization_bps =
        round_div_biguint(amount_out * BigUint::from(BPS_DENOMINATOR), limit_max_out)
            .to_u32()
            .unwrap_or(BPS_DENOMINATOR)
            .min(BPS_DENOMINATOR);

    if utilization_bps >= SATURATION_MAX_UTILIZATION_BPS {
        return Some(slippage.max_dynamic_slippage_bps);
    }

    if utilization_bps <= SATURATION_RAMP_START_UTILIZATION_BPS {
        return None;
    }

    let ramp_progress = utilization_bps - SATURATION_RAMP_START_UTILIZATION_BPS;
    let ramp_width = SATURATION_MAX_UTILIZATION_BPS - SATURATION_RAMP_START_UTILIZATION_BPS;
    let ramp_span = slippage.max_dynamic_slippage_bps - slippage.saturation_ramp_start_slippage_bps;

    Some(
        slippage.saturation_ramp_start_slippage_bps
            + ((ramp_span * ramp_progress) + (ramp_width / 2)) / ramp_width,
    )
}

fn sensitivity_term_scaled(
    amount_in: &BigUint,
    amount_out: &BigUint,
    stressed_amount_in: &BigUint,
    stressed_amount_out: &BigUint,
    scale: &BigUint,
    slippage: &SlippageConfig,
) -> BigUint {
    let current_rate_numerator = amount_out * stressed_amount_in;
    let stressed_rate_numerator = stressed_amount_out * amount_in;
    if stressed_rate_numerator >= current_rate_numerator {
        return BigUint::zero();
    }

    let numerator = (current_rate_numerator.clone() - stressed_rate_numerator)
        * BigUint::from(slippage.sensitivity_coefficient_bps)
        * scale;
    round_div_biguint(numerator, &current_rate_numerator)
}

fn round_div_biguint(numerator: BigUint, denominator: &BigUint) -> BigUint {
    if denominator.is_zero() {
        return BigUint::zero();
    }

    (numerator + (denominator.clone() / 2u8)) / denominator
}

fn is_limit_like_error_message(message: &str) -> bool {
    let lowered = message.to_ascii_lowercase();
    lowered.contains("max_in")
        || lowered.contains("get_limits")
        || lowered.contains("sell amount exceeds limit")
        || lowered.contains("exceeds limit")
        || lowered.contains("exceeds available liquidity")
        || lowered.contains("borrowable limit")
        || lowered.contains("insufficient reserve")
        || lowered.contains("ticks exceeded")
        || lowered.contains("not enough liquidity")
        || lowered.contains("support complete swap")
}

fn is_liquidity_like_failure(failure: &QuoteFailure) -> bool {
    match failure.kind {
        QuoteFailureKind::NoPools => true,
        QuoteFailureKind::Simulator => {
            is_limit_like_error_message(&failure.message)
                || failure
                    .message
                    .to_ascii_lowercase()
                    .contains("all pools returned zero amounts")
        }
        QuoteFailureKind::WarmUp
        | QuoteFailureKind::TokenValidation
        | QuoteFailureKind::TokenCoverage
        | QuoteFailureKind::Timeout
        | QuoteFailureKind::Overflow
        | QuoteFailureKind::InconsistentResult
        | QuoteFailureKind::Internal
        | QuoteFailureKind::InvalidRequest => false,
    }
}

fn record_vm_first_gas_metrics(
    metrics: &mut QuoteMetrics,
    vm_first_gases: &mut Vec<u64>,
    protocol: &str,
    pool_id: &str,
    first_gas: Option<u64>,
) {
    if !protocol.starts_with("vm:") {
        return;
    }

    let Some(first_gas) = first_gas else {
        return;
    };

    metrics.vm_completed_pools += 1;
    vm_first_gases.push(first_gas);
    if first_gas < VM_LOW_FIRST_GAS_THRESHOLD {
        metrics.vm_low_first_gas_count += 1;
        if metrics.vm_low_first_gas_samples.len() < VM_LOW_FIRST_GAS_SAMPLE_CAP {
            metrics
                .vm_low_first_gas_samples
                .push(format!("{protocol}:{pool_id}:{first_gas}"));
        }
    }
}

fn finalize_vm_first_gas_metrics(metrics: &mut QuoteMetrics, vm_first_gases: &mut [u64]) {
    let Some(median_gas) = median_u64(vm_first_gases) else {
        return;
    };

    metrics.vm_median_first_gas = Some(median_gas);
    metrics.vm_low_first_gas_ratio =
        Some(metrics.vm_low_first_gas_count as f64 / metrics.vm_completed_pools as f64);
}

fn record_rfq_first_gas_metrics(
    metrics: &mut QuoteMetrics,
    rfq_first_gases: &mut Vec<u64>,
    protocol: &str,
    pool_id: &str,
    first_gas: Option<u64>,
) {
    if !protocol.starts_with("rfq:") {
        return;
    }

    let Some(first_gas) = first_gas else {
        return;
    };

    metrics.rfq_completed_pools += 1;
    rfq_first_gases.push(first_gas);
    if first_gas < VM_LOW_FIRST_GAS_THRESHOLD {
        metrics.rfq_low_first_gas_count += 1;
        if metrics.rfq_low_first_gas_samples.len() < VM_LOW_FIRST_GAS_SAMPLE_CAP {
            metrics
                .rfq_low_first_gas_samples
                .push(format!("{protocol}:{pool_id}:{first_gas}"));
        }
    }
}

fn finalize_rfq_first_gas_metrics(metrics: &mut QuoteMetrics, rfq_first_gases: &mut [u64]) {
    let Some(median_gas) = median_u64(rfq_first_gases) else {
        return;
    };

    metrics.rfq_median_first_gas = Some(median_gas);
    metrics.rfq_low_first_gas_ratio =
        Some(metrics.rfq_low_first_gas_count as f64 / metrics.rfq_completed_pools as f64);
}

fn median_u64(values: &mut [u64]) -> Option<u64> {
    if values.is_empty() {
        return None;
    }

    values.sort_unstable();
    let mid = values.len() / 2;
    if values.len() % 2 == 1 {
        Some(values[mid])
    } else {
        Some(((values[mid - 1] as u128 + values[mid] as u128) / 2) as u64)
    }
}

fn format_pool_descriptor(context: &FailureContext<'_>) -> String {
    let mut head = Vec::new();
    if let Some(protocol) = context.protocol {
        if !protocol.is_empty() {
            head.push(protocol);
        }
    }
    if let Some(name) = context.pool_name {
        if !name.is_empty() {
            head.push(name);
        }
    }

    let mut label = if !head.is_empty() {
        head.join("::")
    } else {
        context.pool_id.to_string()
    };

    if let Some(address) = context.pool_address {
        if !address.is_empty() {
            label = format!("{} [{}]", label, address);
        }
    }

    label
}

fn decode_attribute(value: &Bytes) -> Option<String> {
    if value.is_empty() {
        return None;
    }

    let mut bytes = value.to_vec();
    while matches!(bytes.last(), Some(&0)) {
        bytes.pop();
    }

    std::str::from_utf8(&bytes)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn derive_pool_name(component: &ProtocolComponent) -> String {
    for key in ["pool_name", "name", "label"] {
        if let Some(label) = component
            .static_attributes
            .get(key)
            .and_then(decode_attribute)
        {
            if !label.is_empty() {
                return label;
            }
        }
    }

    let symbols: Vec<&str> = component
        .tokens
        .iter()
        .map(|token| token.symbol.as_str())
        .filter(|symbol| !symbol.is_empty())
        .collect();

    if !symbols.is_empty() {
        return format!("{}::{}", component.protocol_system, symbols.join("/"));
    }

    if !component.protocol_type_name.is_empty() {
        return format!(
            "{}::{}",
            component.protocol_system, component.protocol_type_name
        );
    }

    component.protocol_system.clone()
}

fn make_failure(
    kind: QuoteFailureKind,
    message: String,
    pool: Option<FailureContext<'_>>,
) -> QuoteFailure {
    let (pool, pool_name, pool_address, protocol) = pool.map_or_else(
        || (None, None, None, None),
        |context| {
            (
                Some(context.pool_id.to_string()),
                context.pool_name.map(ToString::to_string),
                context.pool_address.map(ToString::to_string),
                context.protocol.map(ToString::to_string),
            )
        },
    );

    QuoteFailure {
        kind,
        message,
        pool,
        pool_name,
        pool_address,
        protocol,
    }
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    reason = "quote tests use deterministic fixture setup and direct invariant checks"
)]
mod tests {
    use super::*;

    use std::any::Any;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use chrono::NaiveDateTime;
    use num_traits::Zero;
    use tokio::sync::RwLock;
    use tokio::time::sleep;
    use tycho_simulation::protocol::models::Update;
    use tycho_simulation::tycho_common::dto::ProtocolStateDelta;
    use tycho_simulation::tycho_common::models::{token::Token, Chain};
    use tycho_simulation::tycho_common::simulation::errors::{SimulationError, TransitionError};
    use tycho_simulation::tycho_common::simulation::protocol_sim::{
        Balances, GetAmountOutResult, ProtocolSim,
    };
    use tycho_simulation::tycho_common::Bytes;

    use crate::models::state::{
        BroadcasterSubscriptionStatus, ConfiguredBackends, RfqStreamStatus, StateStore,
        VmStreamStatus,
    };
    use crate::models::stream_health::StreamHealth;
    use crate::models::tokens::TokenStore;

    fn default_calls() -> Arc<AtomicUsize> {
        Arc::new(AtomicUsize::new(0))
    }

    #[test]
    fn median_u64_handles_odd_and_even_inputs() {
        let mut odd = vec![9, 1, 3];
        assert_eq!(median_u64(&mut odd), Some(3));

        let mut even = vec![10, 2, 6, 4];
        assert_eq!(median_u64(&mut even), Some(5));
    }

    #[test]
    fn median_u64_returns_none_for_empty_input() {
        let mut values: Vec<u64> = Vec::new();
        assert_eq!(median_u64(&mut values), None);
    }

    #[test]
    fn remap_request_token_for_pool_maps_wrapped_to_native_for_native_only_pool() {
        let chain = Chain::Ethereum;
        let native = chain.native_token();
        let wrapped = chain.wrapped_native_token();
        let token_x = make_token(&Bytes::from([9u8; 20]), "TKX");
        let component = ProtocolComponent::new(
            Bytes::from([1u8; 20]),
            "rocketpool".to_string(),
            "rocketpool".to_string(),
            chain,
            vec![native.clone(), token_x],
            Vec::new(),
            HashMap::new(),
            Bytes::default(),
            NaiveDateTime::default(),
        );

        let mapped = remap_request_token_for_pool(&wrapped, &component);

        assert_eq!(mapped.address, native.address);
    }

    #[test]
    fn remap_request_token_for_pool_maps_native_to_wrapped_for_wrapped_only_pool() {
        let chain = Chain::Ethereum;
        let native = chain.native_token();
        let wrapped = chain.wrapped_native_token();
        let token_x = make_token(&Bytes::from([10u8; 20]), "TKX");
        let component = ProtocolComponent::new(
            Bytes::from([2u8; 20]),
            "uniswap_v2".to_string(),
            "uniswap_v2".to_string(),
            chain,
            vec![wrapped.clone(), token_x],
            Vec::new(),
            HashMap::new(),
            Bytes::default(),
            NaiveDateTime::default(),
        );

        let mapped = remap_request_token_for_pool(&native, &component);

        assert_eq!(mapped.address, wrapped.address);
    }

    #[test]
    fn remap_request_token_for_pool_keeps_non_native_tokens_unchanged() {
        let chain = Chain::Ethereum;
        let native = chain.native_token();
        let request_token = make_token(&Bytes::from([42u8; 20]), "USDC");
        let token_x = make_token(&Bytes::from([11u8; 20]), "TKX");
        let component = ProtocolComponent::new(
            Bytes::from([3u8; 20]),
            "uniswap_v2".to_string(),
            "uniswap_v2".to_string(),
            chain,
            vec![native, token_x],
            Vec::new(),
            HashMap::new(),
            Bytes::default(),
            NaiveDateTime::default(),
        );

        let mapped = remap_request_token_for_pool(&request_token, &component);

        assert_eq!(mapped.address, request_token.address);
    }

    #[test]
    fn remap_request_token_for_pool_keeps_native_and_wrapped_when_pool_has_both() {
        let chain = Chain::Ethereum;
        let native = chain.native_token();
        let wrapped = chain.wrapped_native_token();
        let token_x = make_token(&Bytes::from([12u8; 20]), "TKX");
        let component = ProtocolComponent::new(
            Bytes::from([4u8; 20]),
            "uniswap_v4".to_string(),
            "uniswap_v4_pool".to_string(),
            chain,
            vec![native.clone(), wrapped.clone(), token_x],
            Vec::new(),
            HashMap::new(),
            Bytes::default(),
            NaiveDateTime::default(),
        );

        let mapped_native = remap_request_token_for_pool(&native, &component);
        let mapped_wrapped = remap_request_token_for_pool(&wrapped, &component);

        assert_eq!(mapped_native.address, native.address);
        assert_eq!(mapped_wrapped.address, wrapped.address);
    }

    #[test]
    fn vm_first_gas_metrics_aggregate_median_ratio_and_samples() {
        let mut metrics = QuoteMetrics::default();
        let mut vm_first_gases = Vec::new();

        record_vm_first_gas_metrics(
            &mut metrics,
            &mut vm_first_gases,
            "vm:curve",
            "pool-a",
            Some(400_000),
        );
        record_vm_first_gas_metrics(
            &mut metrics,
            &mut vm_first_gases,
            "vm:curve",
            "pool-b",
            Some(900_000),
        );
        record_vm_first_gas_metrics(
            &mut metrics,
            &mut vm_first_gases,
            "vm:curve",
            "pool-c",
            Some(500_000),
        );
        record_vm_first_gas_metrics(
            &mut metrics,
            &mut vm_first_gases,
            "vm:curve",
            "pool-d",
            Some(550_000),
        );
        record_vm_first_gas_metrics(
            &mut metrics,
            &mut vm_first_gases,
            "vm:curve",
            "pool-e",
            Some(580_000),
        );

        finalize_vm_first_gas_metrics(&mut metrics, &mut vm_first_gases);

        assert_eq!(metrics.vm_completed_pools, 5);
        assert_eq!(metrics.vm_low_first_gas_count, 4);
        assert_eq!(metrics.vm_median_first_gas, Some(550_000));
        let ratio = metrics.vm_low_first_gas_ratio.expect("ratio should be set");
        assert!((ratio - 0.8).abs() < 1e-9);
        assert_eq!(
            metrics.vm_low_first_gas_samples.len(),
            VM_LOW_FIRST_GAS_SAMPLE_CAP
        );
        assert_eq!(
            metrics.vm_low_first_gas_samples[0],
            "vm:curve:pool-a:400000".to_string()
        );
    }

    #[test]
    fn vm_first_gas_metrics_ignore_non_vm_protocols() {
        let mut metrics = QuoteMetrics::default();
        let mut vm_first_gases = Vec::new();

        record_vm_first_gas_metrics(
            &mut metrics,
            &mut vm_first_gases,
            "uniswap_v3",
            "pool-native",
            Some(120_000),
        );
        finalize_vm_first_gas_metrics(&mut metrics, &mut vm_first_gases);

        assert_eq!(metrics.vm_completed_pools, 0);
        assert_eq!(metrics.vm_median_first_gas, None);
        assert_eq!(metrics.vm_low_first_gas_ratio, None);
        assert!(metrics.vm_low_first_gas_samples.is_empty());
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct LimitCountingSim {
        max_in: BigUint,
        #[serde(skip, default = "default_calls")]
        calls: Arc<AtomicUsize>,
    }

    #[typetag::serde]
    impl ProtocolSim for LimitCountingSim {
        fn fee(&self) -> f64 {
            0.0
        }

        fn spot_price(&self, _base: &Token, _quote: &Token) -> Result<f64, SimulationError> {
            Ok(0.0)
        }

        fn get_amount_out(
            &self,
            amount_in: BigUint,
            _token_in: &Token,
            _token_out: &Token,
        ) -> Result<GetAmountOutResult, SimulationError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if amount_in > self.max_in {
                return Err(SimulationError::InvalidInput(
                    "amount_in exceeds get_limits max_in".to_string(),
                    None,
                ));
            }
            Ok(GetAmountOutResult::new(
                BigUint::zero(),
                BigUint::zero(),
                self.clone_box(),
            ))
        }

        fn get_limits(
            &self,
            _sell_token: Bytes,
            _buy_token: Bytes,
        ) -> Result<(BigUint, BigUint), SimulationError> {
            Ok((self.max_in.clone(), BigUint::zero()))
        }

        fn delta_transition(
            &mut self,
            _delta: ProtocolStateDelta,
            _tokens: &HashMap<Bytes, Token>,
            _balances: &Balances,
        ) -> Result<(), TransitionError> {
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn ProtocolSim> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn eq(&self, other: &dyn ProtocolSim) -> bool {
            other.as_any().is::<Self>()
        }
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct ConservativeLimitSim {
        reported_max_in: BigUint,
        actual_max_in: BigUint,
        #[serde(skip, default = "default_calls")]
        calls: Arc<AtomicUsize>,
    }

    #[typetag::serde]
    impl ProtocolSim for ConservativeLimitSim {
        fn fee(&self) -> f64 {
            0.0
        }

        fn spot_price(&self, _base: &Token, _quote: &Token) -> Result<f64, SimulationError> {
            Ok(0.0)
        }

        fn get_amount_out(
            &self,
            amount_in: BigUint,
            _token_in: &Token,
            _token_out: &Token,
        ) -> Result<GetAmountOutResult, SimulationError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if amount_in > self.actual_max_in {
                return Err(SimulationError::InvalidInput(
                    format!("Sell amount exceeds limit {}", self.actual_max_in),
                    None,
                ));
            }
            Ok(GetAmountOutResult::new(
                BigUint::zero(),
                BigUint::zero(),
                self.clone_box(),
            ))
        }

        fn get_limits(
            &self,
            _sell_token: Bytes,
            _buy_token: Bytes,
        ) -> Result<(BigUint, BigUint), SimulationError> {
            Ok((self.reported_max_in.clone(), BigUint::zero()))
        }

        fn delta_transition(
            &mut self,
            _delta: ProtocolStateDelta,
            _tokens: &HashMap<Bytes, Token>,
            _balances: &Balances,
        ) -> Result<(), TransitionError> {
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn ProtocolSim> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn eq(&self, other: &dyn ProtocolSim) -> bool {
            other.as_any().is::<Self>()
        }
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct LinearAmountSim {
        multiplier: u32,
    }

    #[typetag::serde]
    impl ProtocolSim for LinearAmountSim {
        fn fee(&self) -> f64 {
            0.0
        }

        fn spot_price(&self, _base: &Token, _quote: &Token) -> Result<f64, SimulationError> {
            Ok(f64::from(self.multiplier))
        }

        fn get_amount_out(
            &self,
            amount_in: BigUint,
            _token_in: &Token,
            _token_out: &Token,
        ) -> Result<GetAmountOutResult, SimulationError> {
            Ok(GetAmountOutResult::new(
                amount_in * BigUint::from(self.multiplier),
                BigUint::zero(),
                self.clone_box(),
            ))
        }

        fn get_limits(
            &self,
            _sell_token: Bytes,
            _buy_token: Bytes,
        ) -> Result<(BigUint, BigUint), SimulationError> {
            Ok((BigUint::from(1_000_000u64), BigUint::zero()))
        }

        fn delta_transition(
            &mut self,
            _delta: ProtocolStateDelta,
            _tokens: &HashMap<Bytes, Token>,
            _balances: &Balances,
        ) -> Result<(), TransitionError> {
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn ProtocolSim> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn eq(&self, other: &dyn ProtocolSim) -> bool {
            other.as_any().is::<Self>()
        }
    }

    fn make_token(address: &Bytes, symbol: &str) -> Token {
        Token::new(address, symbol, 18, 0, &[], Chain::Ethereum, 100)
    }

    fn make_token_with_decimals(address: &Bytes, symbol: &str, decimals: u32) -> Token {
        Token::new(address, symbol, decimals, 0, &[], Chain::Ethereum, 100)
    }

    fn make_token_store(tokens: impl IntoIterator<Item = Token>) -> Arc<TokenStore> {
        let initial_tokens = tokens
            .into_iter()
            .map(|token| (token.address.clone(), token))
            .collect();
        Arc::new(TokenStore::new(
            initial_tokens,
            "http://localhost".to_string(),
            "test".to_string(),
            Chain::Ethereum,
            Duration::from_secs(60),
        ))
    }

    fn make_pair_component(
        address: &str,
        protocol_system: &str,
        protocol_type_name: &str,
        tokens: Vec<Token>,
    ) -> ProtocolComponent {
        ProtocolComponent::new(
            test_address(address),
            protocol_system.to_string(),
            protocol_type_name.to_string(),
            Chain::Ethereum,
            tokens,
            Vec::new(),
            HashMap::new(),
            Bytes::default(),
            NaiveDateTime::default(),
        )
    }

    #[expect(
        clippy::panic,
        reason = "quote test fixtures should fail immediately on invalid literal addresses"
    )]
    fn test_address(value: &str) -> Bytes {
        match Bytes::from_str(value) {
            Ok(address) => address,
            Err(err) => panic!("test address must parse: {err}"),
        }
    }

    fn erc4626_pair_policies() -> Vec<crate::models::erc4626::Erc4626PairPolicy> {
        vec![
            crate::models::erc4626::Erc4626PairPolicy {
                asset_symbol: "USDS".to_string(),
                share_symbol: "sUSDS".to_string(),
                asset: test_address("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
                share: test_address("0xa3931d71877c0e7a3148cb7eb4463524fec27fbd"),
                allow_asset_to_share: true,
                allow_share_to_asset: true,
            },
            crate::models::erc4626::Erc4626PairPolicy {
                asset_symbol: "USDC".to_string(),
                share_symbol: "sUSDC".to_string(),
                asset: test_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
                share: test_address("0xBc65ad17c5C0a2A4D159fa5a503f4992c7B545FE"),
                allow_asset_to_share: true,
                allow_share_to_asset: true,
            },
            crate::models::erc4626::Erc4626PairPolicy {
                asset_symbol: "PYUSD".to_string(),
                share_symbol: "spPYUSD".to_string(),
                asset: test_address("0x6c3ea9036406852006290770BEdFcAbA0e23A0e8"),
                share: test_address("0x80128DbB9f07b93DDE62A6daeadb69ED14a7D354"),
                allow_asset_to_share: true,
                allow_share_to_asset: true,
            },
        ]
    }

    struct TestAppStateConfig {
        enable_vm_pools: bool,
        enable_rfq_pools: bool,
        erc4626_deposits_enabled: bool,
        request_timeout: Duration,
    }

    impl Default for TestAppStateConfig {
        fn default() -> Self {
            Self {
                enable_vm_pools: false,
                enable_rfq_pools: false,
                erc4626_deposits_enabled: false,
                request_timeout: Duration::from_secs(2),
            }
        }
    }

    fn make_test_app_state(
        token_store: Arc<TokenStore>,
        native_state_store: Arc<StateStore>,
        vm_state_store: Arc<StateStore>,
        rfq_state_store: Arc<StateStore>,
        config: TestAppStateConfig,
    ) -> AppState {
        AppState {
            chain: Chain::Ethereum,
            native_token_protocol_allowlist: Arc::new(vec!["rocketpool".to_string()]),
            tokens: token_store,
            native_broadcaster_subscription: BroadcasterSubscriptionStatus::ready_for_test(),
            vm_broadcaster_subscription: BroadcasterSubscriptionStatus::ready_for_test(),
            native_state_store,
            vm_state_store,
            rfq_state_store,
            native_stream_health: Arc::new(StreamHealth::new()),
            vm_stream_health: Arc::new(StreamHealth::new()),
            rfq_stream_health: Arc::new(StreamHealth::new()),
            vm_stream: Arc::new(RwLock::new(VmStreamStatus::default())),
            rfq_stream: Arc::new(RwLock::new(RfqStreamStatus::default())),
            configured_backends: ConfiguredBackends {
                vm: config.enable_vm_pools,
                rfq: config.enable_rfq_pools,
            },
            enable_vm_pools: config.enable_vm_pools,
            enable_rfq_pools: config.enable_rfq_pools,
            readiness_stale: Duration::from_secs(120),
            request_timeout: config.request_timeout,
            simulation_rebuild_gate: Arc::new(RwLock::new(())),
            slippage: SlippageConfig::default(),
            erc4626_deposits_enabled: config.erc4626_deposits_enabled,
            erc4626_pair_policies: Arc::new(erc4626_pair_policies()),
            reset_allowance_tokens: Arc::new(HashMap::new()),
        }
    }

    fn make_test_state_stores(
        token_store: Arc<TokenStore>,
    ) -> (Arc<StateStore>, Arc<StateStore>, Arc<StateStore>) {
        (
            Arc::new(StateStore::new(Arc::clone(&token_store))),
            Arc::new(StateStore::new(Arc::clone(&token_store))),
            Arc::new(StateStore::new(token_store)),
        )
    }

    fn make_amount_out_request(
        request_id: &str,
        token_in: &str,
        token_out: &str,
        amounts: &[&str],
    ) -> AmountOutRequest {
        AmountOutRequest {
            request_id: request_id.to_string(),
            auction_id: None,
            token_in: token_in.to_string(),
            token_out: token_out.to_string(),
            amounts: amounts.iter().map(ToString::to_string).collect(),
        }
    }

    struct BasicQuoteFixture {
        token_in_hex: &'static str,
        token_out_hex: &'static str,
        token_in_meta: Token,
        token_out_meta: Token,
        token_store: Arc<TokenStore>,
        native_state_store: Arc<StateStore>,
        vm_state_store: Arc<StateStore>,
        rfq_state_store: Arc<StateStore>,
    }

    impl BasicQuoteFixture {
        fn new() -> Self {
            let token_in_hex = "0x0000000000000000000000000000000000000001";
            let token_out_hex = "0x0000000000000000000000000000000000000002";
            let token_in = Bytes::from_str(token_in_hex).expect("valid address");
            let token_out = Bytes::from_str(token_out_hex).expect("valid address");
            let token_in_meta = make_token(&token_in, "TK1");
            let token_out_meta = make_token(&token_out, "TK2");
            let token_store = make_token_store(vec![token_in_meta.clone(), token_out_meta.clone()]);
            let (native_state_store, vm_state_store, rfq_state_store) =
                make_test_state_stores(Arc::clone(&token_store));

            Self {
                token_in_hex,
                token_out_hex,
                token_in_meta,
                token_out_meta,
                token_store,
                native_state_store,
                vm_state_store,
                rfq_state_store,
            }
        }

        fn pair_tokens(&self) -> Vec<Token> {
            vec![self.token_in_meta.clone(), self.token_out_meta.clone()]
        }

        fn request(&self, request_id: &str, amounts: &[&str]) -> AmountOutRequest {
            make_amount_out_request(request_id, self.token_in_hex, self.token_out_hex, amounts)
        }
    }

    struct Erc4626QuoteFixture {
        token_in_hex: &'static str,
        token_out_hex: &'static str,
        token_in_meta: Token,
        token_out_meta: Token,
        token_store: Arc<TokenStore>,
        native_state_store: Arc<StateStore>,
        vm_state_store: Arc<StateStore>,
        rfq_state_store: Arc<StateStore>,
    }

    impl Erc4626QuoteFixture {
        fn new(
            token_in_hex: &'static str,
            token_in_symbol: &str,
            token_in_decimals: u32,
            token_out_hex: &'static str,
            token_out_symbol: &str,
            token_out_decimals: u32,
        ) -> Self {
            let token_in = Bytes::from_str(token_in_hex).expect("valid address");
            let token_out = Bytes::from_str(token_out_hex).expect("valid address");
            let token_in_meta =
                make_token_with_decimals(&token_in, token_in_symbol, token_in_decimals);
            let token_out_meta =
                make_token_with_decimals(&token_out, token_out_symbol, token_out_decimals);
            let token_store = make_token_store(vec![token_in_meta.clone(), token_out_meta.clone()]);
            let (native_state_store, vm_state_store, rfq_state_store) =
                make_test_state_stores(Arc::clone(&token_store));

            Self {
                token_in_hex,
                token_out_hex,
                token_in_meta,
                token_out_meta,
                token_store,
                native_state_store,
                vm_state_store,
                rfq_state_store,
            }
        }

        fn pair_tokens(&self) -> Vec<Token> {
            vec![self.token_in_meta.clone(), self.token_out_meta.clone()]
        }

        fn request(&self, request_id: &str, amounts: &[&str]) -> AmountOutRequest {
            make_amount_out_request(request_id, self.token_in_hex, self.token_out_hex, amounts)
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "test fixture setup stays clearer with explicit pool metadata inputs"
    )]
    fn insert_pool_state(
        states: &mut HashMap<String, Box<dyn ProtocolSim>>,
        new_pairs: &mut HashMap<String, ProtocolComponent>,
        pool_id: &str,
        pool_address: &str,
        protocol_system: &str,
        protocol_type_name: &str,
        tokens: Vec<Token>,
        sim: Box<dyn ProtocolSim>,
    ) {
        states.insert(pool_id.to_string(), sim);
        new_pairs.insert(
            pool_id.to_string(),
            make_pair_component(pool_address, protocol_system, protocol_type_name, tokens),
        );
    }

    async fn make_vm_unavailable_fixture(enable_vm_pools: bool) -> (AppState, AmountOutRequest) {
        let fixture = BasicQuoteFixture::new();
        prime_ready_native_store(&fixture.native_state_store).await;

        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig {
                enable_vm_pools,
                ..TestAppStateConfig::default()
            },
        );

        let request = fixture.request(
            if enable_vm_pools {
                "req-vm-enabled-not-ready"
            } else {
                "req-vm-disabled"
            },
            &["1"],
        );

        (app_state, request)
    }

    async fn prime_ready_native_store(state_store: &Arc<StateStore>) {
        let ready_token_a =
            Bytes::from_str("0x0000000000000000000000000000000000000003").expect("valid address");
        let ready_token_b =
            Bytes::from_str("0x0000000000000000000000000000000000000004").expect("valid address");
        let mut ready_states = HashMap::new();
        let mut ready_pairs = HashMap::new();
        insert_pool_state(
            &mut ready_states,
            &mut ready_pairs,
            "pool-ready",
            "0x0000000000000000000000000000000000000010",
            "uniswap_v2",
            "uniswap_v2",
            vec![
                make_token(&ready_token_a, "RDY1"),
                make_token(&ready_token_b, "RDY2"),
            ],
            Box::new(LimitCountingSim {
                max_in: BigUint::from(1_000u32),
                calls: default_calls(),
            }),
        );
        state_store
            .apply_update(Update::new(1, ready_states, ready_pairs))
            .await;
    }

    async fn install_basic_native_quote_pool(
        fixture: &BasicQuoteFixture,
    ) -> (Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let token_in = Bytes::from_str(fixture.token_in_hex).expect("valid address");
        let token_out = Bytes::from_str(fixture.token_out_hex).expect("valid address");
        let limit_calls = Arc::new(AtomicUsize::new(0));
        let quote_calls = Arc::new(AtomicUsize::new(0));
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-basic-ready",
            "0x0000000000000000000000000000000000000020",
            "uniswap_v2",
            "uniswap_v2_pool",
            fixture.pair_tokens(),
            Box::new(StrictPairTokenSim {
                expected_sell: token_in,
                expected_buy: token_out,
                limit_calls: Arc::clone(&limit_calls),
                quote_calls: Arc::clone(&quote_calls),
            }),
        );
        fixture
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        (limit_calls, quote_calls)
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct StrictPairTokenSim {
        expected_sell: Bytes,
        expected_buy: Bytes,
        #[serde(skip, default = "default_calls")]
        limit_calls: Arc<AtomicUsize>,
        #[serde(skip, default = "default_calls")]
        quote_calls: Arc<AtomicUsize>,
    }

    #[typetag::serde]
    impl ProtocolSim for StrictPairTokenSim {
        fn fee(&self) -> f64 {
            0.0
        }

        fn spot_price(&self, _base: &Token, _quote: &Token) -> Result<f64, SimulationError> {
            Err(SimulationError::FatalError("spot unavailable".to_string()))
        }

        fn get_amount_out(
            &self,
            amount_in: BigUint,
            token_in: &Token,
            token_out: &Token,
        ) -> Result<GetAmountOutResult, SimulationError> {
            self.quote_calls.fetch_add(1, Ordering::SeqCst);
            if token_in.address != self.expected_sell || token_out.address != self.expected_buy {
                return Err(SimulationError::InvalidInput(
                    format!(
                        "unexpected token pair for quote: {} -> {}",
                        token_in.address, token_out.address
                    ),
                    None,
                ));
            }

            Ok(GetAmountOutResult::new(
                amount_in.clone(),
                BigUint::from(21_000u64),
                self.clone_box(),
            ))
        }

        fn get_limits(
            &self,
            sell_token: Bytes,
            buy_token: Bytes,
        ) -> Result<(BigUint, BigUint), SimulationError> {
            self.limit_calls.fetch_add(1, Ordering::SeqCst);
            if sell_token != self.expected_sell || buy_token != self.expected_buy {
                return Err(SimulationError::InvalidInput(
                    format!(
                        "unexpected token pair for limits: {} -> {}",
                        sell_token, buy_token
                    ),
                    None,
                ));
            }

            Ok((BigUint::from(1_000_000u64), BigUint::zero()))
        }

        fn delta_transition(
            &mut self,
            _delta: ProtocolStateDelta,
            _tokens: &HashMap<Bytes, Token>,
            _balances: &Balances,
        ) -> Result<(), TransitionError> {
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn ProtocolSim> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn eq(&self, other: &dyn ProtocolSim) -> bool {
            other.as_any().is::<Self>()
        }
    }

    #[tokio::test]
    async fn get_amounts_out_rejects_native_wrapped_pair_invalid_request() {
        let token_store = make_token_store(Vec::new());
        let (native_state_store, vm_state_store, rfq_state_store) =
            make_test_state_stores(Arc::clone(&token_store));
        let app_state = make_test_app_state(
            token_store,
            native_state_store,
            vm_state_store,
            rfq_state_store,
            TestAppStateConfig::default(),
        );

        let native = Chain::Ethereum.native_token().address.to_string();
        let wrapped = Chain::Ethereum.wrapped_native_token().address.to_string();
        for (request_id, token_in, token_out) in [
            ("req-native-to-wrapped", native.clone(), wrapped.clone()),
            ("req-wrapped-to-native", wrapped.clone(), native.clone()),
        ] {
            let request = make_amount_out_request(request_id, &token_in, &token_out, &["1"]);

            let computation = get_amounts_out(app_state.clone(), request, None).await;

            assert!(computation.responses.is_empty());
            assert!(matches!(
                computation.meta.status,
                QuoteStatus::InvalidRequest
            ));
            assert_eq!(
                computation.meta.result_quality,
                QuoteResultQuality::RequestLevelFailure
            );
            assert_eq!(computation.meta.matching_pools, 0);
            assert_eq!(computation.meta.candidate_pools, 0);
            assert!(computation
                .meta
                .failures
                .iter()
                .any(|failure| matches!(failure.kind, QuoteFailureKind::InvalidRequest)));
            assert_eq!(computation.meta.failures.len(), 1);
            assert_eq!(
                computation.meta.failures[0].message,
                "Direct native/wrapped quotes are unsupported"
            );
        }
    }

    #[tokio::test]
    async fn get_amounts_out_returns_warming_up_until_broadcaster_bootstrap_completes() {
        let fixture = BasicQuoteFixture::new();
        let (_limit_calls, quote_calls) = install_basic_native_quote_pool(&fixture).await;
        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        app_state
            .native_broadcaster_subscription
            .mark_connected()
            .await;

        let warming_up = get_amounts_out(
            app_state.clone(),
            fixture.request("req-broadcaster-bootstrap-warming", &["2"]),
            None,
        )
        .await;

        assert!(warming_up.responses.is_empty());
        assert!(matches!(warming_up.meta.status, QuoteStatus::WarmingUp));
        assert_eq!(warming_up.meta.failures.len(), 1);
        assert!(matches!(
            warming_up.meta.failures[0].kind,
            QuoteFailureKind::WarmUp
        ));
        assert_eq!(quote_calls.load(Ordering::SeqCst), 0);

        app_state
            .native_broadcaster_subscription
            .mark_bootstrap_complete()
            .await;

        let ready = get_amounts_out(
            app_state,
            fixture.request("req-broadcaster-bootstrap-ready", &["2"]),
            None,
        )
        .await;

        assert_eq!(ready.responses.len(), 1);
        assert_eq!(ready.responses[0].amounts_out, vec!["2".to_string()]);
        assert!(matches!(ready.meta.status, QuoteStatus::Ready));
        assert!(ready.meta.failures.is_empty());
        assert_eq!(ready.meta.matching_pools, 1);
        assert_eq!(ready.meta.candidate_pools, 1);
        assert_eq!(quote_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn get_amounts_out_waits_for_broadcaster_bootstrap_before_returning_ready() {
        let fixture = BasicQuoteFixture::new();
        let (_limit_calls, quote_calls) = install_basic_native_quote_pool(&fixture).await;
        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        app_state
            .native_broadcaster_subscription
            .mark_connected()
            .await;

        let bootstrap_state = app_state.clone();
        let bootstrap_task = tokio::spawn(async move {
            sleep(Duration::from_millis(60)).await;
            bootstrap_state
                .native_broadcaster_subscription
                .mark_bootstrap_complete()
                .await;
        });

        let ready = get_amounts_out(
            app_state,
            fixture.request("req-broadcaster-bootstrap-awaits-ready", &["2"]),
            None,
        )
        .await;

        bootstrap_task
            .await
            .expect("bootstrap task should complete");

        assert_eq!(ready.responses.len(), 1);
        assert_eq!(ready.responses[0].amounts_out, vec!["2".to_string()]);
        assert!(matches!(ready.meta.status, QuoteStatus::Ready));
        assert!(ready.meta.failures.is_empty());
        assert_eq!(ready.meta.matching_pools, 1);
        assert_eq!(ready.meta.candidate_pools, 1);
        assert_eq!(quote_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn get_amounts_out_preserves_request_amount_order() {
        let fixture = BasicQuoteFixture::new();
        let (_limit_calls, quote_calls) = install_basic_native_quote_pool(&fixture).await;
        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );

        let computation = get_amounts_out(
            app_state,
            fixture.request("req-preserve-amount-order", &["30", "10", "20"]),
            None,
        )
        .await;

        assert_eq!(computation.responses.len(), 1);
        assert_eq!(
            computation.responses[0].amounts_out,
            vec!["30".to_string(), "10".to_string(), "20".to_string()]
        );
        assert_eq!(
            computation.responses[0].gas_used,
            vec![21_000, 21_000, 21_000]
        );
        assert!(matches!(computation.meta.status, QuoteStatus::Ready));
        assert_eq!(
            computation.meta.result_quality,
            QuoteResultQuality::Complete
        );
        assert!(computation.meta.failures.is_empty());
        assert_eq!(quote_calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn get_amounts_out_quotes_allowlisted_erc4626_direction() {
        let fixture = Erc4626QuoteFixture::new(
            "0xdC035D45d973E3EC169d2276DDab16f1e407384F",
            "USDS",
            18,
            "0xa3931d71877c0e7a3148cb7eb4463524fec27fbd",
            "sUSDS",
            18,
        );
        let token_in = Bytes::from_str(fixture.token_in_hex).expect("valid address");
        let token_out = Bytes::from_str(fixture.token_out_hex).expect("valid address");
        let limit_calls = Arc::new(AtomicUsize::new(0));
        let quote_calls = Arc::new(AtomicUsize::new(0));
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-erc4626",
            fixture.token_out_hex,
            "erc4626",
            "erc4626_pool",
            fixture.pair_tokens(),
            Box::new(StrictPairTokenSim {
                expected_sell: token_in.clone(),
                expected_buy: token_out.clone(),
                limit_calls: Arc::clone(&limit_calls),
                quote_calls: Arc::clone(&quote_calls),
            }),
        );
        fixture
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig {
                erc4626_deposits_enabled: true,
                ..TestAppStateConfig::default()
            },
        );
        let request = fixture.request("req-erc4626-allowlisted", &["2"]);

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(computation.responses.len(), 1);
        assert_eq!(computation.responses[0].amounts_out, vec!["2".to_string()]);
        assert_eq!(computation.responses[0].slippage, vec![1]);
        assert!(matches!(computation.meta.status, QuoteStatus::Ready));
        assert!(computation.meta.failures.is_empty());
        assert_eq!(computation.meta.matching_pools, 1);
        assert_eq!(computation.meta.candidate_pools, 1);
        assert_eq!(quote_calls.load(Ordering::SeqCst), 2);
        assert!(limit_calls.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn get_amounts_out_filters_non_allowlisted_erc4626_before_simulation() {
        let fixture = Erc4626QuoteFixture::new(
            "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3",
            "USDe",
            18,
            "0x9d39a5de30e57443bff2a8307a4256c8797a3497",
            "sUSDe",
            18,
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-susde",
            fixture.token_out_hex,
            "",
            "erc4626_pool",
            fixture.pair_tokens(),
            Box::new(LimitCountingSim {
                max_in: BigUint::from(10u8),
                calls: Arc::clone(&calls),
            }),
        );
        fixture
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request("req-erc4626-filtered", &["2"]);

        let computation = get_amounts_out(app_state, request, None).await;

        assert!(computation.responses.is_empty());
        assert!(matches!(computation.meta.status, QuoteStatus::NoLiquidity));
        assert_eq!(computation.meta.matching_pools, 0);
        assert_eq!(computation.meta.candidate_pools, 0);
        assert_eq!(computation.meta.failures.len(), 1);
        assert!(matches!(
            computation.meta.failures[0].kind,
            QuoteFailureKind::NoPools
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn get_amounts_out_filters_allowlisted_erc4626_deposit_when_deposits_disabled() {
        let fixture = Erc4626QuoteFixture::new(
            "0xdC035D45d973E3EC169d2276DDab16f1e407384F",
            "USDS",
            18,
            "0xa3931d71877c0e7a3148cb7eb4463524fec27fbd",
            "sUSDS",
            18,
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-susds",
            fixture.token_out_hex,
            "erc4626",
            "erc4626_pool",
            fixture.pair_tokens(),
            Box::new(LimitCountingSim {
                max_in: BigUint::from(10u8),
                calls: Arc::clone(&calls),
            }),
        );
        fixture
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request("req-erc4626-deposit-disabled", &["2"]);

        let computation = get_amounts_out(app_state, request, None).await;

        assert!(computation.responses.is_empty());
        assert!(matches!(computation.meta.status, QuoteStatus::NoLiquidity));
        assert_eq!(computation.meta.matching_pools, 0);
        assert_eq!(computation.meta.candidate_pools, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn get_amounts_out_keeps_allowlisted_erc4626_redeem_when_deposits_disabled() {
        let fixture = Erc4626QuoteFixture::new(
            "0xa3931d71877c0e7a3148cb7eb4463524fec27fbd",
            "sUSDS",
            18,
            "0xdC035D45d973E3EC169d2276DDab16f1e407384F",
            "USDS",
            18,
        );
        let token_in = Bytes::from_str(fixture.token_in_hex).expect("valid address");
        let token_out = Bytes::from_str(fixture.token_out_hex).expect("valid address");
        let limit_calls = Arc::new(AtomicUsize::new(0));
        let quote_calls = Arc::new(AtomicUsize::new(0));
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-erc4626-redeem",
            fixture.token_in_hex,
            "",
            "erc4626_pool",
            fixture.pair_tokens(),
            Box::new(StrictPairTokenSim {
                expected_sell: token_in,
                expected_buy: token_out,
                limit_calls: Arc::clone(&limit_calls),
                quote_calls: Arc::clone(&quote_calls),
            }),
        );
        fixture
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request("req-erc4626-redeem-disabled", &["2"]);

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(computation.responses.len(), 1);
        assert!(matches!(computation.meta.status, QuoteStatus::Ready));
        assert_eq!(computation.meta.matching_pools, 1);
        assert_eq!(computation.meta.candidate_pools, 1);
        assert_eq!(quote_calls.load(Ordering::SeqCst), 2);
        assert!(limit_calls.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn get_amounts_out_keeps_non_erc4626_candidates_when_unsupported_erc4626_is_filtered() {
        let fixture = Erc4626QuoteFixture::new(
            "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3",
            "USDe",
            18,
            "0x9d39a5de30e57443bff2a8307a4256c8797a3497",
            "sUSDe",
            18,
        );
        let token_in = Bytes::from_str(fixture.token_in_hex).expect("valid address");
        let token_out = Bytes::from_str(fixture.token_out_hex).expect("valid address");
        let erc4626_calls = Arc::new(AtomicUsize::new(0));
        let limit_calls = Arc::new(AtomicUsize::new(0));
        let quote_calls = Arc::new(AtomicUsize::new(0));
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-susde",
            fixture.token_out_hex,
            "erc4626",
            "erc4626_pool",
            fixture.pair_tokens(),
            Box::new(LimitCountingSim {
                max_in: BigUint::from(10u8),
                calls: Arc::clone(&erc4626_calls),
            }),
        );
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-uniswap",
            "0x0000000000000000000000000000000000000011",
            "uniswap_v2",
            "uniswap_v2",
            fixture.pair_tokens(),
            Box::new(StrictPairTokenSim {
                expected_sell: token_in,
                expected_buy: token_out,
                limit_calls: Arc::clone(&limit_calls),
                quote_calls: Arc::clone(&quote_calls),
            }),
        );
        fixture
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request("req-erc4626-mixed", &["2"]);

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(computation.responses.len(), 1);
        assert_eq!(computation.responses[0].pool, "pool-uniswap");
        assert!(matches!(computation.meta.status, QuoteStatus::Ready));
        assert_eq!(computation.meta.matching_pools, 1);
        assert_eq!(quote_calls.load(Ordering::SeqCst), 2);
        assert_eq!(erc4626_calls.load(Ordering::SeqCst), 0);
        assert!(limit_calls.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn get_amounts_out_remaps_wrapped_request_tokens_for_native_only_pool() {
        let wrapped_native = Chain::Ethereum.wrapped_native_token().address;
        let native = Chain::Ethereum.native_token().address;
        let token_out =
            Bytes::from_str("0x0000000000000000000000000000000000000002").expect("valid address");

        let wrapped_meta = make_token_with_decimals(&wrapped_native, "WETH", 18);
        let native_meta = make_token_with_decimals(&native, "ETH", 18);
        let token_out_meta = make_token_with_decimals(&token_out, "TK2", 6);
        let token_store = make_token_store(vec![
            wrapped_meta.clone(),
            native_meta.clone(),
            token_out_meta.clone(),
        ]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            make_test_state_stores(Arc::clone(&token_store));

        let limit_calls = Arc::new(AtomicUsize::new(0));
        let quote_calls = Arc::new(AtomicUsize::new(0));
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-native",
            "0x0000000000000000000000000000000000000011",
            "rocketpool",
            "rocketpool",
            vec![native_meta, token_out_meta],
            Box::new(StrictPairTokenSim {
                expected_sell: native.clone(),
                expected_buy: token_out.clone(),
                limit_calls: Arc::clone(&limit_calls),
                quote_calls: Arc::clone(&quote_calls),
            }),
        );

        native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            token_store,
            Arc::clone(&native_state_store),
            Arc::clone(&vm_state_store),
            Arc::clone(&rfq_state_store),
            TestAppStateConfig::default(),
        );

        let request = make_amount_out_request(
            "req-remap-native-only",
            &wrapped_native.to_string(),
            &token_out.to_string(),
            &["2"],
        );

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(quote_calls.load(Ordering::SeqCst), 2);
        assert!(!computation.responses.is_empty());
        assert_eq!(computation.responses[0].amounts_out, vec!["2".to_string()]);
        assert!(computation.meta.failures.is_empty());
        assert!(matches!(computation.meta.status, QuoteStatus::Ready));
        assert!(limit_calls.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn get_amounts_out_prefers_exact_wrapped_pool_over_native_alias_pool() {
        let wrapped_native = Chain::Ethereum.wrapped_native_token().address;
        let native = Chain::Ethereum.native_token().address;
        let token_out =
            Bytes::from_str("0x0000000000000000000000000000000000000006").expect("valid address");

        let wrapped_meta = make_token_with_decimals(&wrapped_native, "WETH", 18);
        let native_meta = make_token_with_decimals(&native, "ETH", 18);
        let token_out_meta = make_token_with_decimals(&token_out, "TK6", 6);
        let token_store = make_token_store(vec![
            wrapped_meta.clone(),
            native_meta.clone(),
            token_out_meta.clone(),
        ]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            make_test_state_stores(Arc::clone(&token_store));

        let wrapped_limit_calls = Arc::new(AtomicUsize::new(0));
        let wrapped_quote_calls = Arc::new(AtomicUsize::new(0));
        let native_limit_calls = Arc::new(AtomicUsize::new(0));
        let native_quote_calls = Arc::new(AtomicUsize::new(0));

        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-weth",
            "0x0000000000000000000000000000000000000015",
            "uniswap_v2",
            "uniswap_v2_pool",
            vec![wrapped_meta, token_out_meta.clone()],
            Box::new(StrictPairTokenSim {
                expected_sell: wrapped_native.clone(),
                expected_buy: token_out.clone(),
                limit_calls: Arc::clone(&wrapped_limit_calls),
                quote_calls: Arc::clone(&wrapped_quote_calls),
            }),
        );
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-native",
            "0x0000000000000000000000000000000000000016",
            "rocketpool",
            "rocketpool",
            vec![native_meta, token_out_meta],
            Box::new(StrictPairTokenSim {
                expected_sell: native.clone(),
                expected_buy: token_out.clone(),
                limit_calls: Arc::clone(&native_limit_calls),
                quote_calls: Arc::clone(&native_quote_calls),
            }),
        );

        native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            token_store,
            Arc::clone(&native_state_store),
            Arc::clone(&vm_state_store),
            Arc::clone(&rfq_state_store),
            TestAppStateConfig::default(),
        );

        let request = make_amount_out_request(
            "req-prefer-exact-wrapped",
            &wrapped_native.to_string(),
            &token_out.to_string(),
            &["2"],
        );

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(computation.meta.matching_pools, 1);
        assert_eq!(computation.meta.candidate_pools, 1);
        assert_eq!(wrapped_quote_calls.load(Ordering::SeqCst), 2);
        assert!(wrapped_limit_calls.load(Ordering::SeqCst) >= 1);
        assert_eq!(native_quote_calls.load(Ordering::SeqCst), 0);
        assert_eq!(native_limit_calls.load(Ordering::SeqCst), 0);
        assert_eq!(computation.responses.len(), 1);
        assert!(computation.meta.failures.is_empty());
        assert!(matches!(computation.meta.status, QuoteStatus::Ready));
    }

    #[tokio::test]
    async fn get_amounts_out_prefers_exact_native_pool_over_wrapped_alias_pool() {
        let wrapped_native = Chain::Ethereum.wrapped_native_token().address;
        let native = Chain::Ethereum.native_token().address;
        let token_out =
            Bytes::from_str("0x0000000000000000000000000000000000000007").expect("valid address");

        let wrapped_meta = make_token_with_decimals(&wrapped_native, "WETH", 18);
        let native_meta = make_token_with_decimals(&native, "ETH", 18);
        let token_out_meta = make_token_with_decimals(&token_out, "TK7", 6);
        let token_store = make_token_store(vec![
            wrapped_meta.clone(),
            native_meta.clone(),
            token_out_meta.clone(),
        ]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            make_test_state_stores(Arc::clone(&token_store));

        let wrapped_limit_calls = Arc::new(AtomicUsize::new(0));
        let wrapped_quote_calls = Arc::new(AtomicUsize::new(0));
        let native_limit_calls = Arc::new(AtomicUsize::new(0));
        let native_quote_calls = Arc::new(AtomicUsize::new(0));

        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-weth",
            "0x0000000000000000000000000000000000000017",
            "uniswap_v2",
            "uniswap_v2_pool",
            vec![wrapped_meta, token_out_meta.clone()],
            Box::new(StrictPairTokenSim {
                expected_sell: wrapped_native.clone(),
                expected_buy: token_out.clone(),
                limit_calls: Arc::clone(&wrapped_limit_calls),
                quote_calls: Arc::clone(&wrapped_quote_calls),
            }),
        );
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-native",
            "0x0000000000000000000000000000000000000018",
            "rocketpool",
            "rocketpool",
            vec![native_meta, token_out_meta],
            Box::new(StrictPairTokenSim {
                expected_sell: native.clone(),
                expected_buy: token_out.clone(),
                limit_calls: Arc::clone(&native_limit_calls),
                quote_calls: Arc::clone(&native_quote_calls),
            }),
        );

        native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            token_store,
            Arc::clone(&native_state_store),
            Arc::clone(&vm_state_store),
            Arc::clone(&rfq_state_store),
            TestAppStateConfig::default(),
        );

        let request = make_amount_out_request(
            "req-prefer-exact-native",
            &native.to_string(),
            &token_out.to_string(),
            &["2"],
        );

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(computation.meta.matching_pools, 1);
        assert_eq!(computation.meta.candidate_pools, 1);
        assert_eq!(native_quote_calls.load(Ordering::SeqCst), 2);
        assert!(native_limit_calls.load(Ordering::SeqCst) >= 1);
        assert_eq!(wrapped_quote_calls.load(Ordering::SeqCst), 0);
        assert_eq!(wrapped_limit_calls.load(Ordering::SeqCst), 0);
        assert_eq!(computation.responses.len(), 1);
        assert!(computation.meta.failures.is_empty());
        assert!(matches!(computation.meta.status, QuoteStatus::Ready));
    }

    #[tokio::test]
    async fn get_amounts_out_remaps_wrapped_token_out_for_native_only_pool() {
        let wrapped_native = Chain::Ethereum.wrapped_native_token().address;
        let native = Chain::Ethereum.native_token().address;
        let token_in =
            Bytes::from_str("0x0000000000000000000000000000000000000008").expect("valid address");

        let wrapped_meta = make_token_with_decimals(&wrapped_native, "WETH", 18);
        let native_meta = make_token_with_decimals(&native, "ETH", 18);
        let token_in_meta = make_token_with_decimals(&token_in, "TK8", 6);
        let token_store = make_token_store(vec![
            wrapped_meta.clone(),
            native_meta.clone(),
            token_in_meta.clone(),
        ]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            make_test_state_stores(Arc::clone(&token_store));

        let limit_calls = Arc::new(AtomicUsize::new(0));
        let quote_calls = Arc::new(AtomicUsize::new(0));
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-native-buy",
            "0x0000000000000000000000000000000000000019",
            "rocketpool",
            "rocketpool",
            vec![token_in_meta, native_meta],
            Box::new(StrictPairTokenSim {
                expected_sell: token_in.clone(),
                expected_buy: native.clone(),
                limit_calls: Arc::clone(&limit_calls),
                quote_calls: Arc::clone(&quote_calls),
            }),
        );

        native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            token_store,
            Arc::clone(&native_state_store),
            Arc::clone(&vm_state_store),
            Arc::clone(&rfq_state_store),
            TestAppStateConfig::default(),
        );

        let request = make_amount_out_request(
            "req-remap-native-buy",
            &token_in.to_string(),
            &wrapped_native.to_string(),
            &["2"],
        );

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(quote_calls.load(Ordering::SeqCst), 2);
        assert!(limit_calls.load(Ordering::SeqCst) >= 1);
        assert_eq!(computation.responses.len(), 1);
        assert_eq!(computation.responses[0].amounts_out, vec!["2".to_string()]);
        assert!(computation.meta.failures.is_empty());
        assert!(matches!(computation.meta.status, QuoteStatus::Ready));
    }

    #[tokio::test]
    async fn get_amounts_out_remaps_wrapped_request_tokens_for_native_only_vm_pool() {
        let wrapped_native = Chain::Ethereum.wrapped_native_token().address;
        let native = Chain::Ethereum.native_token().address;
        let token_out =
            Bytes::from_str("0x0000000000000000000000000000000000000004").expect("valid address");

        let wrapped_meta = make_token_with_decimals(&wrapped_native, "WETH", 18);
        let native_meta = make_token_with_decimals(&native, "ETH", 18);
        let token_out_meta = make_token_with_decimals(&token_out, "TK4", 6);
        let token_store = make_token_store(vec![
            wrapped_meta.clone(),
            native_meta.clone(),
            token_out_meta.clone(),
        ]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            make_test_state_stores(Arc::clone(&token_store));

        // Keep native store ready without creating any matching pools for the request pair.
        let native_dummy_token_a =
            Bytes::from_str("0x0000000000000000000000000000000000000010").expect("valid address");
        let native_dummy_token_b =
            Bytes::from_str("0x0000000000000000000000000000000000000011").expect("valid address");
        let native_dummy_meta_a = make_token_with_decimals(&native_dummy_token_a, "NTA", 18);
        let native_dummy_meta_b = make_token_with_decimals(&native_dummy_token_b, "NTB", 18);
        let mut native_states = HashMap::new();
        let mut native_pairs = HashMap::new();
        insert_pool_state(
            &mut native_states,
            &mut native_pairs,
            "native-dummy",
            "0x0000000000000000000000000000000000000013",
            "uniswap_v2",
            "uniswap_v2",
            vec![native_dummy_meta_a, native_dummy_meta_b],
            Box::new(LimitCountingSim {
                max_in: BigUint::from(1_000_000u64),
                calls: default_calls(),
            }),
        );
        native_state_store
            .apply_update(Update::new(1, native_states, native_pairs))
            .await;

        let vm_limit_calls = Arc::new(AtomicUsize::new(0));
        let vm_quote_calls = Arc::new(AtomicUsize::new(0));
        let mut vm_states = HashMap::new();
        let mut vm_pairs = HashMap::new();
        insert_pool_state(
            &mut vm_states,
            &mut vm_pairs,
            "vm-native",
            "0x0000000000000000000000000000000000000014",
            "vm:curve",
            "curve_pool",
            vec![native_meta, token_out_meta],
            Box::new(StrictPairTokenSim {
                expected_sell: native.clone(),
                expected_buy: token_out.clone(),
                limit_calls: Arc::clone(&vm_limit_calls),
                quote_calls: Arc::clone(&vm_quote_calls),
            }),
        );
        vm_state_store
            .apply_update(Update::new(2, vm_states, vm_pairs))
            .await;

        let app_state = make_test_app_state(
            token_store,
            Arc::clone(&native_state_store),
            Arc::clone(&vm_state_store),
            Arc::clone(&rfq_state_store),
            TestAppStateConfig {
                enable_vm_pools: true,
                ..TestAppStateConfig::default()
            },
        );

        let request = make_amount_out_request(
            "req-remap-vm-native-only",
            &wrapped_native.to_string(),
            &token_out.to_string(),
            &["2"],
        );

        let computation = get_amounts_out(app_state, request, None).await;

        assert!(vm_limit_calls.load(Ordering::SeqCst) >= 1);
        assert_eq!(vm_quote_calls.load(Ordering::SeqCst), 2);
        assert_eq!(computation.metrics.scheduled_vm_pools, 1);
        assert_eq!(computation.responses.len(), 1);
        assert_eq!(computation.responses[0].amounts_out, vec!["2".to_string()]);
        assert!(computation.meta.failures.is_empty());
        assert!(matches!(computation.meta.status, QuoteStatus::Ready));
    }

    #[tokio::test]
    async fn get_amounts_out_loads_vm_candidates_after_waiting_for_rebuild_guard() {
        let fixture = BasicQuoteFixture::new();
        prime_ready_native_store(&fixture.native_state_store).await;
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-vm",
            "0x0000000000000000000000000000000000000015",
            "vm:curve",
            "curve_pool",
            fixture.pair_tokens(),
            Box::new(LinearAmountSim { multiplier: 1 }),
        );
        fixture
            .vm_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;
        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig {
                enable_vm_pools: true,
                ..TestAppStateConfig::default()
            },
        );
        let request_guard = app_state.simulation_rebuild_gate().write_owned().await;
        let request = fixture.request("req-vm-rebuild-guard", &["10"]);
        let quote_task = tokio::spawn(get_amounts_out(app_state, request, None));

        sleep(Duration::from_millis(20)).await;
        assert!(
            !quote_task.is_finished(),
            "quote should wait before reading VM candidates"
        );

        let mut replacement_states = HashMap::new();
        replacement_states.insert(
            "pool-vm".to_string(),
            Box::new(LinearAmountSim { multiplier: 2 }) as Box<dyn ProtocolSim>,
        );
        fixture
            .vm_state_store
            .apply_update(Update::new(2, replacement_states, HashMap::new()))
            .await;

        drop(request_guard);
        let computation = tokio::time::timeout(Duration::from_millis(500), quote_task)
            .await
            .expect("quote should finish after guard is released")
            .expect("quote task should not panic");

        assert_eq!(computation.responses.len(), 1);
        assert_eq!(computation.responses[0].amounts_out, vec!["20".to_string()]);
        assert_eq!(computation.meta.vm_block_number, Some(2));
        assert_eq!(computation.meta.total_pools, Some(2));
        assert!(computation.meta.failures.is_empty());
    }

    #[tokio::test]
    async fn get_amounts_out_filters_wrong_direction_hashflow_candidate_before_simulation() {
        let wrapped_native = Chain::Ethereum.wrapped_native_token().address;
        let token_in =
            Bytes::from_str("0x0000000000000000000000000000000000000101").expect("valid address");
        let token_out = wrapped_native.clone();

        let token_in_meta = make_token_with_decimals(&token_in, "USDC", 6);
        let token_out_meta = make_token_with_decimals(&token_out, "WETH", 18);
        let token_store = make_token_store(vec![token_in_meta.clone(), token_out_meta.clone()]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            make_test_state_stores(Arc::clone(&token_store));
        prime_ready_native_store(&native_state_store).await;

        let hashflow_limit_calls = Arc::new(AtomicUsize::new(0));
        let hashflow_quote_calls = Arc::new(AtomicUsize::new(0));
        let bebop_limit_calls = Arc::new(AtomicUsize::new(0));
        let bebop_quote_calls = Arc::new(AtomicUsize::new(0));
        let mut rfq_states = HashMap::new();
        let mut rfq_pairs = HashMap::new();
        insert_pool_state(
            &mut rfq_states,
            &mut rfq_pairs,
            "rfq-hashflow-reversed",
            "0x0000000000000000000000000000000000000105",
            "rfq:hashflow",
            "hashflow_pool",
            vec![token_out_meta.clone(), token_in_meta.clone()],
            Box::new(StrictPairTokenSim {
                expected_sell: token_out.clone(),
                expected_buy: token_in.clone(),
                limit_calls: Arc::clone(&hashflow_limit_calls),
                quote_calls: Arc::clone(&hashflow_quote_calls),
            }),
        );
        insert_pool_state(
            &mut rfq_states,
            &mut rfq_pairs,
            "rfq-bebop",
            "0x0000000000000000000000000000000000000106",
            "rfq:bebop",
            "bebop_pool",
            vec![token_in_meta.clone(), token_out_meta.clone()],
            Box::new(StrictPairTokenSim {
                expected_sell: token_in.clone(),
                expected_buy: token_out.clone(),
                limit_calls: Arc::clone(&bebop_limit_calls),
                quote_calls: Arc::clone(&bebop_quote_calls),
            }),
        );
        rfq_state_store
            .apply_update(Update::new(2, rfq_states, rfq_pairs))
            .await;

        let app_state = make_test_app_state(
            token_store,
            Arc::clone(&native_state_store),
            Arc::clone(&vm_state_store),
            Arc::clone(&rfq_state_store),
            TestAppStateConfig {
                enable_rfq_pools: true,
                ..TestAppStateConfig::default()
            },
        );
        let request = make_amount_out_request(
            "req-rfq-hashflow-direction-filter",
            &token_in.to_string(),
            &token_out.to_string(),
            &["2"],
        );

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(computation.meta.matching_pools, 1);
        assert_eq!(computation.meta.candidate_pools, 1);
        assert_eq!(computation.metrics.scheduled_rfq_pools, 1);
        assert_eq!(hashflow_limit_calls.load(Ordering::SeqCst), 0);
        assert_eq!(hashflow_quote_calls.load(Ordering::SeqCst), 0);
        assert!(bebop_limit_calls.load(Ordering::SeqCst) >= 1);
        assert_eq!(bebop_quote_calls.load(Ordering::SeqCst), 2);
        assert_eq!(computation.responses.len(), 1);
        assert_eq!(computation.responses[0].pool, "rfq-bebop");
        assert_eq!(computation.responses[0].amounts_out, vec!["2".to_string()]);
        assert!(computation.meta.failures.is_empty());
        assert!(matches!(computation.meta.status, QuoteStatus::Ready));
    }

    #[tokio::test]
    async fn get_amounts_out_filters_wrong_direction_liquorice_candidate_before_simulation() {
        let wrapped_native = Chain::Ethereum.wrapped_native_token().address;
        let token_in =
            Bytes::from_str("0x0000000000000000000000000000000000000107").expect("valid address");
        let token_out = wrapped_native.clone();

        let token_in_meta = make_token_with_decimals(&token_in, "USDC", 6);
        let token_out_meta = make_token_with_decimals(&token_out, "WETH", 18);
        let token_store = make_token_store(vec![token_in_meta.clone(), token_out_meta.clone()]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            make_test_state_stores(Arc::clone(&token_store));
        prime_ready_native_store(&native_state_store).await;

        let liquorice_limit_calls = Arc::new(AtomicUsize::new(0));
        let liquorice_quote_calls = Arc::new(AtomicUsize::new(0));
        let bebop_limit_calls = Arc::new(AtomicUsize::new(0));
        let bebop_quote_calls = Arc::new(AtomicUsize::new(0));
        let mut rfq_states = HashMap::new();
        let mut rfq_pairs = HashMap::new();
        insert_pool_state(
            &mut rfq_states,
            &mut rfq_pairs,
            "rfq-liquorice-reversed",
            "0x0000000000000000000000000000000000000108",
            "rfq:liquorice",
            "liquorice_pool",
            vec![token_out_meta.clone(), token_in_meta.clone()],
            Box::new(StrictPairTokenSim {
                expected_sell: token_out.clone(),
                expected_buy: token_in.clone(),
                limit_calls: Arc::clone(&liquorice_limit_calls),
                quote_calls: Arc::clone(&liquorice_quote_calls),
            }),
        );
        insert_pool_state(
            &mut rfq_states,
            &mut rfq_pairs,
            "rfq-bebop-liquorice-filter",
            "0x0000000000000000000000000000000000000109",
            "rfq:bebop",
            "bebop_pool",
            vec![token_in_meta.clone(), token_out_meta.clone()],
            Box::new(StrictPairTokenSim {
                expected_sell: token_in.clone(),
                expected_buy: token_out.clone(),
                limit_calls: Arc::clone(&bebop_limit_calls),
                quote_calls: Arc::clone(&bebop_quote_calls),
            }),
        );
        rfq_state_store
            .apply_update(Update::new(2, rfq_states, rfq_pairs))
            .await;

        let app_state = make_test_app_state(
            token_store,
            Arc::clone(&native_state_store),
            Arc::clone(&vm_state_store),
            Arc::clone(&rfq_state_store),
            TestAppStateConfig {
                enable_rfq_pools: true,
                ..TestAppStateConfig::default()
            },
        );
        let request = make_amount_out_request(
            "req-rfq-liquorice-direction-filter",
            &token_in.to_string(),
            &token_out.to_string(),
            &["2"],
        );

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(computation.meta.matching_pools, 1);
        assert_eq!(computation.meta.candidate_pools, 1);
        assert_eq!(computation.metrics.scheduled_rfq_pools, 1);
        assert_eq!(liquorice_limit_calls.load(Ordering::SeqCst), 0);
        assert_eq!(liquorice_quote_calls.load(Ordering::SeqCst), 0);
        assert!(bebop_limit_calls.load(Ordering::SeqCst) >= 1);
        assert_eq!(bebop_quote_calls.load(Ordering::SeqCst), 2);
        assert_eq!(computation.responses.len(), 1);
        assert_eq!(computation.responses[0].pool, "rfq-bebop-liquorice-filter");
        assert_eq!(computation.responses[0].amounts_out, vec!["2".to_string()]);
        assert!(computation.meta.failures.is_empty());
        assert!(matches!(computation.meta.status, QuoteStatus::Ready));
    }

    #[tokio::test]
    async fn get_amounts_out_keeps_correct_direction_hashflow_candidate() {
        let token_in =
            Bytes::from_str("0x0000000000000000000000000000000000000111").expect("valid address");
        let token_out =
            Bytes::from_str("0x0000000000000000000000000000000000000112").expect("valid address");

        let token_in_meta = make_token_with_decimals(&token_in, "WETH", 18);
        let token_out_meta = make_token_with_decimals(&token_out, "USDC", 6);
        let token_store = make_token_store(vec![token_in_meta.clone(), token_out_meta.clone()]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            make_test_state_stores(Arc::clone(&token_store));
        prime_ready_native_store(&native_state_store).await;

        let limit_calls = Arc::new(AtomicUsize::new(0));
        let quote_calls = Arc::new(AtomicUsize::new(0));
        let mut rfq_states = HashMap::new();
        let mut rfq_pairs = HashMap::new();
        insert_pool_state(
            &mut rfq_states,
            &mut rfq_pairs,
            "rfq-hashflow",
            "0x0000000000000000000000000000000000000116",
            "rfq:hashflow",
            "hashflow_pool",
            vec![token_in_meta.clone(), token_out_meta.clone()],
            Box::new(StrictPairTokenSim {
                expected_sell: token_in.clone(),
                expected_buy: token_out.clone(),
                limit_calls: Arc::clone(&limit_calls),
                quote_calls: Arc::clone(&quote_calls),
            }),
        );
        rfq_state_store
            .apply_update(Update::new(2, rfq_states, rfq_pairs))
            .await;

        let app_state = make_test_app_state(
            token_store,
            Arc::clone(&native_state_store),
            Arc::clone(&vm_state_store),
            Arc::clone(&rfq_state_store),
            TestAppStateConfig {
                enable_rfq_pools: true,
                ..TestAppStateConfig::default()
            },
        );
        let request = make_amount_out_request(
            "req-rfq-hashflow-correct-direction",
            &token_in.to_string(),
            &token_out.to_string(),
            &["2"],
        );

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(computation.meta.matching_pools, 1);
        assert_eq!(computation.meta.candidate_pools, 1);
        assert_eq!(computation.metrics.scheduled_rfq_pools, 1);
        assert!(limit_calls.load(Ordering::SeqCst) >= 1);
        assert_eq!(quote_calls.load(Ordering::SeqCst), 2);
        assert_eq!(computation.responses.len(), 1);
        assert_eq!(computation.responses[0].pool, "rfq-hashflow");
        assert_eq!(computation.responses[0].amounts_out, vec!["2".to_string()]);
        assert!(computation.meta.failures.is_empty());
        assert!(matches!(computation.meta.status, QuoteStatus::Ready));
    }

    #[tokio::test]
    async fn get_amounts_out_keeps_hashflow_candidate_after_native_wrapped_remap() {
        let native = Chain::Ethereum.native_token().address;
        let wrapped = Chain::Ethereum.wrapped_native_token().address;
        let token_out =
            Bytes::from_str("0x0000000000000000000000000000000000000121").expect("valid address");

        let native_meta = make_token_with_decimals(&native, "ETH", 18);
        let wrapped_meta = make_token_with_decimals(&wrapped, "WETH", 18);
        let token_out_meta = make_token_with_decimals(&token_out, "USDC", 6);
        let token_store = make_token_store(vec![
            native_meta.clone(),
            wrapped_meta.clone(),
            token_out_meta.clone(),
        ]);
        let (native_state_store, vm_state_store, rfq_state_store) =
            make_test_state_stores(Arc::clone(&token_store));
        prime_ready_native_store(&native_state_store).await;

        let limit_calls = Arc::new(AtomicUsize::new(0));
        let quote_calls = Arc::new(AtomicUsize::new(0));
        let mut rfq_states = HashMap::new();
        let mut rfq_pairs = HashMap::new();
        insert_pool_state(
            &mut rfq_states,
            &mut rfq_pairs,
            "rfq-hashflow-native-remap",
            "0x0000000000000000000000000000000000000125",
            "rfq:hashflow",
            "hashflow_pool",
            vec![wrapped_meta.clone(), token_out_meta.clone()],
            Box::new(StrictPairTokenSim {
                expected_sell: wrapped.clone(),
                expected_buy: token_out.clone(),
                limit_calls: Arc::clone(&limit_calls),
                quote_calls: Arc::clone(&quote_calls),
            }),
        );
        rfq_state_store
            .apply_update(Update::new(2, rfq_states, rfq_pairs))
            .await;

        let app_state = make_test_app_state(
            token_store,
            Arc::clone(&native_state_store),
            Arc::clone(&vm_state_store),
            Arc::clone(&rfq_state_store),
            TestAppStateConfig {
                enable_rfq_pools: true,
                ..TestAppStateConfig::default()
            },
        );
        let request = make_amount_out_request(
            "req-rfq-hashflow-native-remap",
            &native.to_string(),
            &token_out.to_string(),
            &["2"],
        );

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(computation.meta.matching_pools, 1);
        assert_eq!(computation.meta.candidate_pools, 1);
        assert_eq!(computation.metrics.scheduled_rfq_pools, 1);
        assert!(limit_calls.load(Ordering::SeqCst) >= 1);
        assert_eq!(quote_calls.load(Ordering::SeqCst), 2);
        assert_eq!(computation.responses.len(), 1);
        assert_eq!(computation.responses[0].pool, "rfq-hashflow-native-remap");
        assert_eq!(computation.responses[0].amounts_out, vec!["2".to_string()]);
        assert!(computation.meta.failures.is_empty());
        assert!(matches!(computation.meta.status, QuoteStatus::Ready));
    }

    #[tokio::test]
    async fn vm_unavailable_is_false_when_vm_pools_disabled() {
        let (app_state, request) = make_vm_unavailable_fixture(false).await;

        let computation = get_amounts_out(app_state, request, None).await;
        assert!(matches!(computation.meta.status, QuoteStatus::NoLiquidity));
        assert!(!computation.meta.vm_unavailable);
    }

    #[tokio::test]
    async fn vm_unavailable_is_true_when_vm_pools_enabled_but_not_ready() {
        let (app_state, request) = make_vm_unavailable_fixture(true).await;

        let computation = get_amounts_out(app_state, request, None).await;
        assert!(matches!(computation.meta.status, QuoteStatus::NoLiquidity));
        assert!(computation.meta.vm_unavailable);
    }

    #[tokio::test]
    async fn vm_and_rfq_unavailable_are_true_when_enabled_backends_are_not_ready() {
        let fixture = BasicQuoteFixture::new();
        prime_ready_native_store(&fixture.native_state_store).await;
        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig {
                enable_vm_pools: true,
                enable_rfq_pools: true,
                ..TestAppStateConfig::default()
            },
        );

        let computation = get_amounts_out(
            app_state,
            fixture.request("req-enabled-backends-not-ready", &["1"]),
            None,
        )
        .await;

        assert!(matches!(computation.meta.status, QuoteStatus::NoLiquidity));
        assert!(computation.meta.vm_unavailable);
        assert!(computation.meta.rfq_unavailable);
        assert!(computation.metrics.skipped_vm_unavailable);
        assert!(computation.metrics.skipped_rfq_unavailable);
        assert_eq!(computation.metrics.scheduled_vm_pools, 0);
        assert_eq!(computation.metrics.scheduled_rfq_pools, 0);
    }

    #[tokio::test]
    async fn skips_pool_when_request_exceeds_limits_and_max_probe_fails() {
        let fixture = BasicQuoteFixture::new();

        let calls = Arc::new(AtomicUsize::new(0));
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-1",
            "0x0000000000000000000000000000000000000009",
            "uniswap_v2",
            "uniswap_v2",
            fixture.pair_tokens(),
            Box::new(LimitCountingSim {
                max_in: BigUint::zero(),
                calls: Arc::clone(&calls),
            }),
        );

        fixture
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request("req-1", &["1", "5", "11"]);

        let computation = get_amounts_out(app_state, request, None).await;
        assert!(calls.load(Ordering::SeqCst) >= 2);
        assert!(computation.responses.is_empty());
        assert!(matches!(computation.meta.status, QuoteStatus::NoLiquidity));
        assert_eq!(
            computation.meta.result_quality,
            QuoteResultQuality::NoResults
        );
        assert!(computation.meta.partial_kind.is_none());
        assert_eq!(computation.meta.failures.len(), 1);
        assert!(matches!(
            computation.meta.failures[0].kind,
            QuoteFailureKind::Simulator
        ));
        assert_eq!(computation.meta.pool_results.len(), 1);
        assert_eq!(
            computation.meta.pool_results[0].outcome,
            PoolOutcomeKind::SimulatorError
        );
    }

    #[tokio::test]
    async fn executes_mixed_amount_series_when_some_steps_exceed_reported_limit() {
        let token_in_hex = "0x0000000000000000000000000000000000000001";
        let token_out_hex = "0x0000000000000000000000000000000000000002";
        let token_in = Bytes::from_str(token_in_hex).expect("valid address");
        let token_out = Bytes::from_str(token_out_hex).expect("valid address");

        let calls = Arc::new(AtomicUsize::new(0));
        let sim = LimitCountingSim {
            max_in: BigUint::from(10u8),
            calls: Arc::clone(&calls),
        };

        let outcome = simulate_pool(SimulatePoolInput {
            descriptor: PoolDescriptor {
                id: "pool-1".to_string(),
                name: "uniswap_v2".to_string(),
                address: "0x0000000000000000000000000000000000000009".to_string(),
                protocol: "uniswap_v2".to_string(),
            },
            pool_state: Arc::new(sim),
            rebuild_guard: None,
            token_in: Arc::new(make_token(&token_in, "TK1")),
            token_out: Arc::new(make_token(&token_out, "TK2")),
            amounts: Arc::new(vec![BigUint::from(1u8), BigUint::from(11u8)]),
            slippage: SlippageConfig::default(),
            expected_len: 2,
            cancel_token: CancellationToken::new(),
        })
        .await
        .expect("simulate_pool should return outcome");

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        match outcome {
            PoolSimOutcome::Simulated(result) => {
                assert_eq!(result.amounts_out.len(), 2);
                assert_eq!(result.gas_used.len(), 2);
                assert_eq!(result.amounts_out, vec!["0".to_string(), "0".to_string()]);
                assert_eq!(result.gas_used, vec![0, 0]);
                assert_eq!(result.successful_steps, 0);
                assert_eq!(result.errors.len(), 1);
                assert!(result.errors[0].contains("amount_in exceeds get_limits max_in"));
            }
        }
    }

    #[tokio::test]
    async fn conservative_soft_limit_zero_output_does_not_count_as_successful() {
        let token_in_hex = "0x0000000000000000000000000000000000000001";
        let token_out_hex = "0x0000000000000000000000000000000000000002";
        let token_in = Bytes::from_str(token_in_hex).expect("valid address");
        let token_out = Bytes::from_str(token_out_hex).expect("valid address");

        let calls = Arc::new(AtomicUsize::new(0));
        let sim = ConservativeLimitSim {
            reported_max_in: BigUint::from(10u8),
            actual_max_in: BigUint::from(15u8),
            calls: Arc::clone(&calls),
        };

        let outcome = simulate_pool(SimulatePoolInput {
            descriptor: PoolDescriptor {
                id: "pool-1".to_string(),
                name: "uniswap_v2".to_string(),
                address: "0x0000000000000000000000000000000000000009".to_string(),
                protocol: "uniswap_v2".to_string(),
            },
            pool_state: Arc::new(sim),
            rebuild_guard: None,
            token_in: Arc::new(make_token(&token_in, "TK1")),
            token_out: Arc::new(make_token(&token_out, "TK2")),
            amounts: Arc::new(vec![BigUint::from(11u8), BigUint::from(20u8)]),
            slippage: SlippageConfig::default(),
            expected_len: 2,
            cancel_token: CancellationToken::new(),
        })
        .await
        .expect("simulate_pool should return outcome");

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        match outcome {
            PoolSimOutcome::Simulated(result) => {
                assert_eq!(result.amounts_out.len(), 2);
                assert_eq!(result.gas_used.len(), 2);
                assert_eq!(result.amounts_out, vec!["0".to_string(), "0".to_string()]);
                assert_eq!(result.gas_used, vec![0, 0]);
                assert_eq!(result.successful_steps, 0);
                assert_eq!(result.errors.len(), 1);
                assert!(result.errors[0]
                    .to_ascii_lowercase()
                    .contains("sell amount exceeds limit"));
            }
        }
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct SoftLimitSim {
        max_in: BigUint,
        #[serde(skip, default = "default_calls")]
        calls: Arc<AtomicUsize>,
    }

    #[typetag::serde]
    impl ProtocolSim for SoftLimitSim {
        fn fee(&self) -> f64 {
            0.0
        }

        fn spot_price(&self, _base: &Token, _quote: &Token) -> Result<f64, SimulationError> {
            Ok(0.0)
        }

        fn get_amount_out(
            &self,
            _amount_in: BigUint,
            _token_in: &Token,
            _token_out: &Token,
        ) -> Result<GetAmountOutResult, SimulationError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(GetAmountOutResult::new(
                BigUint::zero(),
                BigUint::zero(),
                self.clone_box(),
            ))
        }

        fn get_limits(
            &self,
            _sell_token: Bytes,
            _buy_token: Bytes,
        ) -> Result<(BigUint, BigUint), SimulationError> {
            Ok((self.max_in.clone(), BigUint::zero()))
        }

        fn delta_transition(
            &mut self,
            _delta: ProtocolStateDelta,
            _tokens: &HashMap<Bytes, Token>,
            _balances: &Balances,
        ) -> Result<(), TransitionError> {
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn ProtocolSim> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn eq(&self, other: &dyn ProtocolSim) -> bool {
            other.as_any().is::<Self>()
        }
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct PositiveOutputSim {
        max_in: BigUint,
        #[serde(skip, default = "default_calls")]
        calls: Arc<AtomicUsize>,
    }

    #[typetag::serde]
    impl ProtocolSim for PositiveOutputSim {
        fn fee(&self) -> f64 {
            0.0
        }

        fn spot_price(&self, _base: &Token, _quote: &Token) -> Result<f64, SimulationError> {
            Ok(0.0)
        }

        fn get_amount_out(
            &self,
            amount_in: BigUint,
            _token_in: &Token,
            _token_out: &Token,
        ) -> Result<GetAmountOutResult, SimulationError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(GetAmountOutResult::new(
                amount_in.clone(),
                BigUint::zero(),
                self.clone_box(),
            ))
        }

        fn get_limits(
            &self,
            _sell_token: Bytes,
            _buy_token: Bytes,
        ) -> Result<(BigUint, BigUint), SimulationError> {
            Ok((self.max_in.clone(), BigUint::zero()))
        }

        fn delta_transition(
            &mut self,
            _delta: ProtocolStateDelta,
            _tokens: &HashMap<Bytes, Token>,
            _balances: &Balances,
        ) -> Result<(), TransitionError> {
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn ProtocolSim> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn eq(&self, other: &dyn ProtocolSim) -> bool {
            other.as_any().is::<Self>()
        }
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct DelayedAmountOrderSim {
        #[serde(skip, default = "default_calls")]
        calls: Arc<AtomicUsize>,
    }

    #[typetag::serde]
    impl ProtocolSim for DelayedAmountOrderSim {
        fn fee(&self) -> f64 {
            0.0
        }

        fn spot_price(&self, _base: &Token, _quote: &Token) -> Result<f64, SimulationError> {
            Ok(0.0)
        }

        fn get_amount_out(
            &self,
            amount_in: BigUint,
            _token_in: &Token,
            _token_out: &Token,
        ) -> Result<GetAmountOutResult, SimulationError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let sleep_for = if amount_in == BigUint::from(10u8) {
                Duration::from_millis(30)
            } else if amount_in == BigUint::from(20u8) {
                Duration::from_millis(15)
            } else {
                Duration::from_millis(1)
            };
            std::thread::sleep(sleep_for);
            Ok(GetAmountOutResult::new(
                &amount_in * BigUint::from(10u8),
                &amount_in + BigUint::from(1000u16),
                self.clone_box(),
            ))
        }

        fn get_limits(
            &self,
            _sell_token: Bytes,
            _buy_token: Bytes,
        ) -> Result<(BigUint, BigUint), SimulationError> {
            Ok((BigUint::from(1_000_000u64), BigUint::zero()))
        }

        fn delta_transition(
            &mut self,
            _delta: ProtocolStateDelta,
            _tokens: &HashMap<Bytes, Token>,
            _balances: &Balances,
        ) -> Result<(), TransitionError> {
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn ProtocolSim> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn eq(&self, other: &dyn ProtocolSim) -> bool {
            other.as_any().is::<Self>()
        }
    }

    #[tokio::test]
    async fn simulate_pool_preserves_request_amount_order_when_amounts_finish_out_of_order() {
        let fixture = BasicQuoteFixture::new();
        let calls = Arc::new(AtomicUsize::new(0));

        let outcome = simulate_pool(SimulatePoolInput {
            descriptor: PoolDescriptor {
                id: "pool-delayed-amounts".to_string(),
                name: "Pool Delayed Amounts".to_string(),
                address: "0x0000000000000000000000000000000000000025".to_string(),
                protocol: "uniswap_v2".to_string(),
            },
            pool_state: Arc::new(DelayedAmountOrderSim {
                calls: Arc::clone(&calls),
            }),
            rebuild_guard: None,
            token_in: Arc::new(fixture.token_in_meta.clone()),
            token_out: Arc::new(fixture.token_out_meta.clone()),
            amounts: Arc::new(vec![
                BigUint::from(30u8),
                BigUint::from(10u8),
                BigUint::from(20u8),
            ]),
            slippage: SlippageConfig::default(),
            expected_len: 3,
            cancel_token: CancellationToken::new(),
        })
        .await
        .expect("simulate_pool should return outcome");

        match outcome {
            PoolSimOutcome::Simulated(result) => {
                assert_eq!(
                    result.amounts_out,
                    vec!["300".to_string(), "100".to_string(), "200".to_string()]
                );
                assert_eq!(result.gas_used, vec![1030, 1010, 1020]);
                assert_eq!(result.successful_steps, 3);
            }
        }
        assert!(
            calls.load(Ordering::SeqCst) >= 3,
            "requested amount quotes should run before any slippage probes"
        );
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct QuadraticImpactSim {
        max_in: BigUint,
        divisor: BigUint,
        #[serde(skip, default = "default_calls")]
        calls: Arc<AtomicUsize>,
    }

    #[typetag::serde]
    impl ProtocolSim for QuadraticImpactSim {
        fn fee(&self) -> f64 {
            0.0
        }

        fn spot_price(&self, _base: &Token, _quote: &Token) -> Result<f64, SimulationError> {
            Ok(0.0)
        }

        fn get_amount_out(
            &self,
            amount_in: BigUint,
            _token_in: &Token,
            _token_out: &Token,
        ) -> Result<GetAmountOutResult, SimulationError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if amount_in > self.max_in {
                return Err(SimulationError::InvalidInput(
                    "amount_in exceeds get_limits max_in".to_string(),
                    None,
                ));
            }
            let penalty = (&amount_in * &amount_in) / &self.divisor;
            let amount_out = if penalty >= amount_in {
                BigUint::zero()
            } else {
                amount_in - penalty
            };
            Ok(GetAmountOutResult::new(
                amount_out,
                BigUint::from(21_000u64),
                self.clone_box(),
            ))
        }

        fn get_limits(
            &self,
            _sell_token: Bytes,
            _buy_token: Bytes,
        ) -> Result<(BigUint, BigUint), SimulationError> {
            Ok((self.max_in.clone(), BigUint::zero()))
        }

        fn delta_transition(
            &mut self,
            _delta: ProtocolStateDelta,
            _tokens: &HashMap<Bytes, Token>,
            _balances: &Balances,
        ) -> Result<(), TransitionError> {
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn ProtocolSim> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn eq(&self, other: &dyn ProtocolSim) -> bool {
            other.as_any().is::<Self>()
        }
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct PlateauLimitSim {
        raw_max_in: BigUint,
        max_out: BigUint,
        #[serde(skip, default = "default_calls")]
        calls: Arc<AtomicUsize>,
    }

    #[typetag::serde]
    impl ProtocolSim for PlateauLimitSim {
        fn fee(&self) -> f64 {
            0.0
        }

        fn spot_price(&self, _base: &Token, _quote: &Token) -> Result<f64, SimulationError> {
            Ok(0.0)
        }

        fn get_amount_out(
            &self,
            amount_in: BigUint,
            _token_in: &Token,
            _token_out: &Token,
        ) -> Result<GetAmountOutResult, SimulationError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(GetAmountOutResult::new(
                amount_in.min(self.max_out.clone()),
                BigUint::from(21_000u64),
                self.clone_box(),
            ))
        }

        fn get_limits(
            &self,
            _sell_token: Bytes,
            _buy_token: Bytes,
        ) -> Result<(BigUint, BigUint), SimulationError> {
            Ok((self.raw_max_in.clone(), self.max_out.clone()))
        }

        fn delta_transition(
            &mut self,
            _delta: ProtocolStateDelta,
            _tokens: &HashMap<Bytes, Token>,
            _balances: &Balances,
        ) -> Result<(), TransitionError> {
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn ProtocolSim> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn eq(&self, other: &dyn ProtocolSim) -> bool {
            other.as_any().is::<Self>()
        }
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct ZeroThenPositiveSim {
        #[serde(skip, default = "default_calls")]
        calls: Arc<AtomicUsize>,
    }

    #[typetag::serde]
    impl ProtocolSim for ZeroThenPositiveSim {
        fn fee(&self) -> f64 {
            0.0
        }

        fn spot_price(&self, _base: &Token, _quote: &Token) -> Result<f64, SimulationError> {
            Ok(0.0)
        }

        fn get_amount_out(
            &self,
            amount_in: BigUint,
            _token_in: &Token,
            _token_out: &Token,
        ) -> Result<GetAmountOutResult, SimulationError> {
            let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
            if call_index == 0 {
                return Ok(GetAmountOutResult::new(
                    BigUint::zero(),
                    BigUint::from(7u8),
                    self.clone_box(),
                ));
            }

            Ok(GetAmountOutResult::new(
                amount_in,
                BigUint::from(11u8),
                self.clone_box(),
            ))
        }

        fn get_limits(
            &self,
            _sell_token: Bytes,
            _buy_token: Bytes,
        ) -> Result<(BigUint, BigUint), SimulationError> {
            Ok((BigUint::from(u64::MAX), BigUint::zero()))
        }

        fn delta_transition(
            &mut self,
            _delta: ProtocolStateDelta,
            _tokens: &HashMap<Bytes, Token>,
            _balances: &Balances,
        ) -> Result<(), TransitionError> {
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn ProtocolSim> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn eq(&self, other: &dyn ProtocolSim) -> bool {
            other.as_any().is::<Self>()
        }
    }

    #[tokio::test]
    async fn all_zero_soft_limit_quotes_are_filtered_from_responses() {
        let fixture = BasicQuoteFixture::new();

        let calls = Arc::new(AtomicUsize::new(0));
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-1",
            "0x0000000000000000000000000000000000000009",
            "uniswap_v2",
            "uniswap_v2",
            fixture.pair_tokens(),
            Box::new(SoftLimitSim {
                max_in: BigUint::from(10u8),
                calls: Arc::clone(&calls),
            }),
        );

        fixture
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request("req-2", &["1", "11"]);

        let computation = get_amounts_out(app_state, request, None).await;
        assert!(matches!(computation.meta.status, QuoteStatus::NoLiquidity));
        assert_eq!(
            computation.meta.result_quality,
            QuoteResultQuality::NoResults
        );
        assert_eq!(computation.meta.failures.len(), 1);
        assert!(computation.meta.failures[0]
            .message
            .contains("All pools returned zero amounts"));
        assert_eq!(computation.meta.pool_results.len(), 1);
        assert_eq!(
            computation.meta.pool_results[0].outcome,
            PoolOutcomeKind::ZeroOutput
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(computation.responses.is_empty());
    }

    #[tokio::test]
    async fn get_amounts_out_slippage_uses_next_ladder_quote_before_tail_probe() {
        let fixture = BasicQuoteFixture::new();

        let calls = Arc::new(AtomicUsize::new(0));
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-1",
            "0x0000000000000000000000000000000000000009",
            "uniswap_v2",
            "uniswap_v2",
            fixture.pair_tokens(),
            Box::new(QuadraticImpactSim {
                max_in: BigUint::from(100_000u32),
                divisor: BigUint::from(1_000_000u32),
                calls: Arc::clone(&calls),
            }),
        );

        fixture
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request("req-next-ladder-stress-slippage", &["10000", "50000"]);

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(computation.responses.len(), 1);
        assert_eq!(
            computation.responses[0].amounts_out,
            vec!["9900".to_string(), "47500".to_string()]
        );
        assert_eq!(computation.responses[0].slippage, vec![102, 102]);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn get_amounts_out_slippage_skips_soft_ladder_utilization_for_linear_quotes() {
        let fixture = BasicQuoteFixture::new();

        let calls = Arc::new(AtomicUsize::new(0));
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-1",
            "0x0000000000000000000000000000000000000009",
            "uniswap_v2",
            "uniswap_v2",
            fixture.pair_tokens(),
            Box::new(PositiveOutputSim {
                max_in: BigUint::from(100_000u32),
                calls: Arc::clone(&calls),
            }),
        );

        fixture
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request("req-linear-soft-ladder-slippage", &["100", "200", "300"]);

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(computation.responses.len(), 1);
        assert_eq!(
            computation.responses[0].amounts_out,
            vec!["100".to_string(), "200".to_string(), "300".to_string()]
        );
        assert_eq!(computation.responses[0].slippage, vec![1, 1, 1]);
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn get_amounts_out_skips_tail_probe_when_next_ladder_quote_is_zero() {
        let fixture = BasicQuoteFixture::new();

        let calls = Arc::new(AtomicUsize::new(0));
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-1",
            "0x0000000000000000000000000000000000000046",
            "uniswap_v2",
            "uniswap_v2",
            fixture.pair_tokens(),
            Box::new(StepSelectiveFailureSim {
                fail_on_calls: vec![1],
                calls: Arc::clone(&calls),
            }),
        );

        fixture
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request("req-partial-tail-no-probe", &["1", "2"]);

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(computation.responses.len(), 1);
        assert_eq!(
            computation.responses[0].amounts_out,
            vec!["1".to_string(), "0".to_string()]
        );
        assert_eq!(computation.responses[0].slippage, vec![1, 0]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn get_amounts_out_caps_limit_max_in_when_limit_max_out_is_binding() {
        let fixture = BasicQuoteFixture::new();

        let calls = Arc::new(AtomicUsize::new(0));
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-1",
            "0x0000000000000000000000000000000000000009",
            "uniswap_v3",
            "uniswap_v3",
            fixture.pair_tokens(),
            Box::new(PlateauLimitSim {
                raw_max_in: BigUint::from(80_108_715_832_839_134u64)
                    * BigUint::from(1_000_000_000u64),
                max_out: BigUint::from(29_053_011_332u64),
                calls: Arc::clone(&calls),
            }),
        );

        fixture
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request(
            "req-cap-limit-max-in",
            &["2000000000", "10000000000", "30000000000", "50000000000"],
        );

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(computation.responses.len(), 1);
        assert_eq!(
            computation.responses[0].amounts_out,
            vec![
                "2000000000".to_string(),
                "10000000000".to_string(),
                "29053011332".to_string(),
                "29053011332".to_string()
            ]
        );
        assert_eq!(
            computation.responses[0].limit_max_in.as_deref(),
            Some("30000000000")
        );
        assert!(calls.load(Ordering::SeqCst) >= 4);
    }

    #[tokio::test]
    async fn mixed_zero_and_positive_outputs_are_classified_as_partial() {
        let fixture = BasicQuoteFixture::new();

        let calls = Arc::new(AtomicUsize::new(0));
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-zero-then-positive",
            "0x0000000000000000000000000000000000000049",
            "uniswap_v2",
            "uniswap_v2",
            fixture.pair_tokens(),
            Box::new(ZeroThenPositiveSim {
                calls: Arc::clone(&calls),
            }),
        );

        fixture
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request("req-zero-then-positive", &["1", "2"]);

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(matches!(computation.meta.status, QuoteStatus::Ready));
        assert_eq!(computation.meta.result_quality, QuoteResultQuality::Partial);
        assert_eq!(
            computation.meta.partial_kind,
            Some(QuotePartialKind::AmountLadders)
        );
        assert!(computation.meta.failures.is_empty());
        assert_eq!(computation.responses.len(), 1);
        assert_eq!(
            computation.responses[0].amounts_out,
            vec!["0".to_string(), "2".to_string()]
        );
        assert_eq!(computation.responses[0].gas_used, vec![0, 11]);
        assert_eq!(computation.meta.pool_results.len(), 1);
        assert_eq!(
            computation.meta.pool_results[0].outcome,
            PoolOutcomeKind::PartialOutput
        );
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct StepSelectiveFailureSim {
        fail_on_calls: Vec<usize>,
        #[serde(skip, default = "default_calls")]
        calls: Arc<AtomicUsize>,
    }

    #[typetag::serde]
    impl ProtocolSim for StepSelectiveFailureSim {
        fn fee(&self) -> f64 {
            0.0
        }

        fn spot_price(&self, _base: &Token, _quote: &Token) -> Result<f64, SimulationError> {
            Ok(0.0)
        }

        fn get_amount_out(
            &self,
            amount_in: BigUint,
            _token_in: &Token,
            _token_out: &Token,
        ) -> Result<GetAmountOutResult, SimulationError> {
            let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_on_calls.contains(&call_index) {
                return Err(SimulationError::FatalError(format!(
                    "simulated requested amount {call_index} failure"
                )));
            }
            Ok(GetAmountOutResult::new(
                amount_in.clone(),
                BigUint::from((call_index + 1) as u64),
                self.clone_box(),
            ))
        }

        fn get_limits(
            &self,
            _sell_token: Bytes,
            _buy_token: Bytes,
        ) -> Result<(BigUint, BigUint), SimulationError> {
            Ok((BigUint::from(u64::MAX), BigUint::zero()))
        }

        fn delta_transition(
            &mut self,
            _delta: ProtocolStateDelta,
            _tokens: &HashMap<Bytes, Token>,
            _balances: &Balances,
        ) -> Result<(), TransitionError> {
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn ProtocolSim> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn eq(&self, other: &dyn ProtocolSim) -> bool {
            other.as_any().is::<Self>()
        }
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct ProbeLimitMessageSim {
        max_in: BigUint,
        message: String,
        #[serde(skip, default = "default_calls")]
        calls: Arc<AtomicUsize>,
    }

    #[typetag::serde]
    impl ProtocolSim for ProbeLimitMessageSim {
        fn fee(&self) -> f64 {
            0.0
        }

        fn spot_price(&self, _base: &Token, _quote: &Token) -> Result<f64, SimulationError> {
            Ok(0.0)
        }

        fn get_amount_out(
            &self,
            amount_in: BigUint,
            _token_in: &Token,
            _token_out: &Token,
        ) -> Result<GetAmountOutResult, SimulationError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if amount_in > self.max_in {
                return Err(SimulationError::InvalidInput(self.message.clone(), None));
            }
            Ok(GetAmountOutResult::new(
                amount_in,
                BigUint::zero(),
                self.clone_box(),
            ))
        }

        fn get_limits(
            &self,
            _sell_token: Bytes,
            _buy_token: Bytes,
        ) -> Result<(BigUint, BigUint), SimulationError> {
            Ok((self.max_in.clone(), BigUint::zero()))
        }

        fn delta_transition(
            &mut self,
            _delta: ProtocolStateDelta,
            _tokens: &HashMap<Bytes, Token>,
            _balances: &Balances,
        ) -> Result<(), TransitionError> {
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn ProtocolSim> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn eq(&self, other: &dyn ProtocolSim) -> bool {
            other.as_any().is::<Self>()
        }
    }

    #[tokio::test]
    async fn get_amounts_out_keeps_limit_like_partial_amount_failure_visible() {
        let fixture = BasicQuoteFixture::new();

        let calls = Arc::new(AtomicUsize::new(0));
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-1",
            "0x0000000000000000000000000000000000000016",
            "fluid_v1",
            "fluid_v1",
            fixture.pair_tokens(),
            Box::new(ProbeLimitMessageSim {
                max_in: BigUint::from(10u8),
                message: "Fatal error: tokenOut amount exceeds borrowable limit".to_string(),
                calls: Arc::clone(&calls),
            }),
        );

        fixture
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request("req-limit-partial-amounts", &["1", "11"]);

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(computation.responses.len(), 1);
        assert_eq!(computation.responses[0].amounts_out.len(), 2);
        assert!(matches!(computation.meta.status, QuoteStatus::Ready));
        assert_eq!(computation.meta.result_quality, QuoteResultQuality::Partial);
        assert_eq!(
            computation.meta.partial_kind,
            Some(QuotePartialKind::AmountLadders)
        );
        assert_eq!(computation.meta.failures.len(), 1);
        assert!(matches!(
            computation.meta.failures[0].kind,
            QuoteFailureKind::Simulator
        ));
        assert!(computation.meta.failures[0]
            .message
            .contains("borrowable limit"));
        assert_eq!(computation.meta.pool_results.len(), 1);
        assert_eq!(
            computation.meta.pool_results[0].outcome,
            PoolOutcomeKind::PartialOutput
        );
    }

    #[tokio::test]
    async fn get_amounts_out_keeps_partial_amount_coverage_and_filters_zero_only_row() {
        let fixture = BasicQuoteFixture::new();

        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-partial",
            "0x0000000000000000000000000000000000000046",
            "uniswap_v2",
            "uniswap_v2",
            fixture.pair_tokens(),
            Box::new(StepSelectiveFailureSim {
                fail_on_calls: vec![1],
                calls: default_calls(),
            }),
        );
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-zero",
            "0x0000000000000000000000000000000000000047",
            "uniswap_v2",
            "uniswap_v2",
            fixture.pair_tokens(),
            Box::new(SoftLimitSim {
                max_in: BigUint::from(100u8),
                calls: default_calls(),
            }),
        );

        fixture
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request("req-partial-and-zero-only", &["1", "2"]);

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(computation.responses.len(), 1);
        assert_eq!(computation.responses[0].pool, "pool-partial");
        assert_eq!(
            computation.responses[0].amounts_out,
            vec!["1".to_string(), "0".to_string()]
        );
        assert!(matches!(computation.meta.status, QuoteStatus::Ready));
        assert_eq!(computation.meta.result_quality, QuoteResultQuality::Partial);
        assert_eq!(computation.meta.partial_kind, Some(QuotePartialKind::Mixed));
        assert!(computation
            .meta
            .pool_results
            .iter()
            .any(|outcome| outcome.outcome == PoolOutcomeKind::PartialOutput));
        assert!(computation
            .meta
            .pool_results
            .iter()
            .any(|outcome| outcome.outcome == PoolOutcomeKind::ZeroOutput));
    }

    #[test]
    fn timed_out_pool_outcome_with_partial_steps_counts_as_amount_ladders() {
        let mut classification = QuoteClassification::new();

        classification.note_pool_outcome_with_steps(PoolOutcomeKind::TimedOut, 1, 2);

        assert_eq!(
            classification.partial.as_partial_kind(),
            Some(QuotePartialKind::AmountLadders)
        );
    }
    #[tokio::test]
    async fn candidate_pools_all_run_without_capacity_skip_outcomes() {
        let fixture = BasicQuoteFixture::new();

        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-1",
            "0x0000000000000000000000000000000000000023",
            "uniswap_v2",
            "uniswap_v2",
            fixture.pair_tokens(),
            Box::new(PositiveOutputSim {
                max_in: BigUint::from(100u8),
                calls: default_calls(),
            }),
        );
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-2",
            "0x0000000000000000000000000000000000000024",
            "uniswap_v2",
            "uniswap_v2",
            fixture.pair_tokens(),
            Box::new(PositiveOutputSim {
                max_in: BigUint::from(100u8),
                calls: default_calls(),
            }),
        );

        fixture
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request("req-candidate-pools-all-run", &["1"]);

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(computation.responses.len(), 2);
        assert!(matches!(computation.meta.status, QuoteStatus::Ready));
        assert_eq!(
            computation.meta.result_quality,
            QuoteResultQuality::Complete
        );
        assert_eq!(computation.meta.partial_kind, None);
        assert!(computation.meta.failures.is_empty());
        assert!(computation.meta.pool_results.is_empty());
        assert_eq!(computation.meta.matching_pools, 2);
        assert_eq!(computation.meta.candidate_pools, 2);
        assert_eq!(computation.metrics.scheduled_native_pools, 2);
    }

    #[tokio::test]
    async fn limit_like_failures_on_all_steps_return_real_no_results() {
        let fixture = BasicQuoteFixture::new();

        let insufficient_calls = Arc::new(AtomicUsize::new(0));
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        insert_pool_state(
            &mut states,
            &mut new_pairs,
            "pool-gho",
            "0x0000000000000000000000000000000000000013",
            "fluid_v1",
            "fluid_v1",
            fixture.pair_tokens(),
            Box::new(ProbeLimitMessageSim {
                max_in: BigUint::zero(),
                message:
                    "Fatal error: Insufficient reserve: tokenOut amount exceeds withdrawable limit"
                        .to_string(),
                calls: Arc::clone(&insufficient_calls),
            }),
        );

        fixture
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;

        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request("req-insufficient-reserve", &["1", "11"]);

        let computation = get_amounts_out(app_state, request, None).await;

        assert_eq!(insufficient_calls.load(Ordering::SeqCst), 2);
        assert!(computation.responses.is_empty());
        assert!(matches!(computation.meta.status, QuoteStatus::NoLiquidity));
        assert_eq!(
            computation.meta.result_quality,
            QuoteResultQuality::NoResults
        );
        assert!(computation.meta.partial_kind.is_none());
        assert_eq!(computation.meta.failures.len(), 1);
        assert!(matches!(
            computation.meta.failures[0].kind,
            QuoteFailureKind::Simulator
        ));
        assert!(computation
            .meta
            .pool_results
            .iter()
            .any(|outcome| outcome.outcome == PoolOutcomeKind::SimulatorError));
    }

    #[tokio::test]
    async fn internal_pool_failure_without_usable_quotes_falls_back_to_internal_error() {
        let fixture = BasicQuoteFixture::new();
        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request("req-internal-fallback", &["1"]);
        let mut runner = QuoteRequestRunner::new(app_state, request, None).await;

        runner.handle_pool_failure(
            make_failure(
                QuoteFailureKind::Internal,
                "pool-1: Quote computation panicked".to_string(),
                Some(FailureContext {
                    pool_id: "pool-1",
                    pool_name: Some("Pool 1"),
                    pool_address: Some("0x0000000000000000000000000000000000000001"),
                    protocol: Some("uniswap_v2"),
                }),
            ),
            1,
        );

        let exit = runner.classify_current_exit();
        assert!(matches!(exit.status, QuoteStatus::InternalError));
        assert_eq!(exit.result_quality, QuoteResultQuality::RequestLevelFailure);
        assert!(exit.partial_kind.is_none());
        assert_eq!(runner.run.failures.len(), 1);
        assert_eq!(runner.run.pool_results.len(), 1);
        assert!(matches!(
            runner.run.pool_results[0].outcome,
            PoolOutcomeKind::InternalError
        ));
    }

    #[tokio::test]
    async fn finalize_responses_orders_by_pool_identity_even_when_first_amount_is_zero() {
        let fixture = BasicQuoteFixture::new();
        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request("req-finalize-order", &["1", "2"]);
        let mut runner = QuoteRequestRunner::new(app_state, request, None).await;
        runner.run.responses = vec![
            AmountOutResponse {
                pool: "pool-z".to_string(),
                pool_name: "Pool Z".to_string(),
                pool_address: "0x0000000000000000000000000000000000000010".to_string(),
                amounts_out: vec!["9".to_string(), "10".to_string()],
                slippage: vec![1, 1],
                limit_max_in: None,
                gas_used: vec![1, 1],
                block_number: 1,
            },
            AmountOutResponse {
                pool: "pool-a".to_string(),
                pool_name: "Pool A".to_string(),
                pool_address: "0x0000000000000000000000000000000000000001".to_string(),
                amounts_out: vec!["0".to_string(), "200".to_string()],
                slippage: vec![0, 1],
                limit_max_in: None,
                gas_used: vec![0, 2],
                block_number: 1,
            },
        ];

        runner.finalize_responses();

        assert_eq!(
            runner
                .run
                .responses
                .iter()
                .map(|response| response.pool.as_str())
                .collect::<Vec<_>>(),
            vec!["pool-a", "pool-z"]
        );
        assert_eq!(runner.run.responses[0].amounts_out[0], "0");
    }

    #[tokio::test]
    async fn finalize_responses_uses_pool_address_as_identity_tiebreaker() {
        let fixture = BasicQuoteFixture::new();
        let app_state = make_test_app_state(
            Arc::clone(&fixture.token_store),
            Arc::clone(&fixture.native_state_store),
            Arc::clone(&fixture.vm_state_store),
            Arc::clone(&fixture.rfq_state_store),
            TestAppStateConfig::default(),
        );
        let request = fixture.request("req-finalize-tiebreak", &["1"]);
        let mut runner = QuoteRequestRunner::new(app_state, request, None).await;
        runner.run.responses = vec![
            AmountOutResponse {
                pool: "pool-shared".to_string(),
                pool_name: "Pool Shared".to_string(),
                pool_address: "0x0000000000000000000000000000000000000002".to_string(),
                amounts_out: vec!["7".to_string()],
                slippage: vec![1],
                limit_max_in: None,
                gas_used: vec![1],
                block_number: 1,
            },
            AmountOutResponse {
                pool: "pool-shared".to_string(),
                pool_name: "Pool Shared".to_string(),
                pool_address: "0x0000000000000000000000000000000000000001".to_string(),
                amounts_out: vec!["8".to_string()],
                slippage: vec![1],
                limit_max_in: None,
                gas_used: vec![1],
                block_number: 1,
            },
        ];

        runner.finalize_responses();

        assert_eq!(
            runner
                .run
                .responses
                .iter()
                .map(|response| response.pool_address.as_str())
                .collect::<Vec<_>>(),
            vec![
                "0x0000000000000000000000000000000000000001",
                "0x0000000000000000000000000000000000000002",
            ]
        );
    }

    #[test]
    fn compute_stress_input_uses_one_percent_with_minimum_floor() {
        assert_eq!(
            compute_stress_input(&BigUint::from(995u32)),
            BigUint::from(9u32)
        );
        assert_eq!(
            compute_stress_input(&BigUint::from(50u32)),
            BigUint::from(1u32)
        );
    }

    #[test]
    fn compute_dynamic_slippage_bps_uses_local_stress_quote() {
        let slippage = compute_dynamic_slippage_bps(
            &BigUint::from(10_000u32),
            &BigUint::from(9_900u32),
            Some(&BigUint::from(10_100u32)),
            Some(&BigUint::from(9_997u32)),
            Some(&BigUint::from(100_000u32)),
            None,
            &SlippageConfig::default(),
        );

        assert_eq!(slippage, 2);
    }

    #[test]
    fn compute_dynamic_slippage_bps_uses_soft_ladder_utilization_when_limit_is_unreached() {
        let slippage = compute_dynamic_slippage_bps(
            &BigUint::from(100u32),
            &BigUint::from(100u32),
            None,
            None,
            Some(&BigUint::from(10_000u32)),
            Some(&BigUint::from(100u32)),
            &SlippageConfig::default(),
        );

        assert_eq!(slippage, 8);
    }

    #[test]
    fn soft_ladder_max_out_requires_meaningful_ladder_degradation() {
        let amounts_in = vec![
            BigUint::from(100u32),
            BigUint::from(200u32),
            BigUint::from(300u32),
        ];
        let amounts_out = vec!["100".to_string(), "200".to_string(), "300".to_string()];

        let soft_ladder_max_out = observed_soft_ladder_max_out(&amounts_in, &amounts_out);

        assert_eq!(soft_ladder_max_out, None);
    }

    #[test]
    fn soft_ladder_max_out_activates_when_ladder_rate_degrades() {
        let amounts_in = vec![
            BigUint::from(100u32),
            BigUint::from(200u32),
            BigUint::from(300u32),
        ];
        let amounts_out = vec!["100".to_string(), "190".to_string(), "270".to_string()];

        let soft_ladder_max_out = observed_soft_ladder_max_out(&amounts_in, &amounts_out);

        assert_eq!(soft_ladder_max_out, Some(BigUint::from(270u32)));
    }

    #[test]
    fn soft_ladder_max_out_is_order_independent() {
        let amounts_in = vec![
            BigUint::from(300u32),
            BigUint::from(100u32),
            BigUint::from(200u32),
        ];
        let amounts_out = vec!["270".to_string(), "100".to_string(), "190".to_string()];

        let soft_ladder_max_out = observed_soft_ladder_max_out(&amounts_in, &amounts_out);

        assert_eq!(soft_ladder_max_out, Some(BigUint::from(270u32)));
    }

    #[test]
    fn compute_dynamic_slippage_bps_uses_limit_max_out_when_available() {
        let slippage = compute_dynamic_slippage_bps(
            &BigUint::from(100u32),
            &BigUint::from(100u32),
            None,
            None,
            Some(&BigUint::from(300u32)),
            None,
            &SlippageConfig::default(),
        );

        assert_eq!(slippage, 8);
    }

    #[test]
    fn compute_dynamic_slippage_bps_ramps_aggressively_near_limit_max_out() {
        let slippage = compute_dynamic_slippage_bps(
            &BigUint::from(9_700u32),
            &BigUint::from(9_700u32),
            None,
            None,
            Some(&BigUint::from(10_000u32)),
            None,
            &SlippageConfig::default(),
        );

        assert_eq!(slippage, 108);
    }

    #[test]
    fn compute_dynamic_slippage_bps_hits_max_when_effectively_at_limit_max_out() {
        let slippage = compute_dynamic_slippage_bps(
            &BigUint::from(9_990u32),
            &BigUint::from(9_990u32),
            None,
            None,
            Some(&BigUint::from(10_000u32)),
            None,
            &SlippageConfig::default(),
        );

        assert_eq!(slippage, SlippageConfig::default().max_dynamic_slippage_bps);
    }

    #[test]
    fn cumulative_degradation_slippage_bps_penalizes_rate_decay_from_first_rung() {
        let penalty = cumulative_degradation_slippage_bps(
            &BigUint::from(100u32),
            &BigUint::from(100u32),
            &BigUint::from(200u32),
            &BigUint::from(160u32),
            &SlippageConfig::default(),
        );

        assert_eq!(penalty, 60);
    }

    #[test]
    fn enforce_non_decreasing_slippage_keeps_usable_ladder_monotonic() {
        let amounts_out = vec![
            "100".to_string(),
            "200".to_string(),
            "300".to_string(),
            "0".to_string(),
            "400".to_string(),
        ];
        let mut slippage = vec![27, 25, 26, 0, 24];

        enforce_non_decreasing_slippage(&amounts_out, &mut slippage);

        assert_eq!(slippage, vec![27, 27, 27, 0, 27]);
    }

    #[test]
    fn limit_like_error_message_matches_known_liquidity_signals() {
        assert!(!is_limit_like_error_message("token_in invalid for probe"));
        assert!(is_limit_like_error_message(
            "tokenOut amount exceeds borrowable limit"
        ));
        assert!(is_limit_like_error_message(
            "Fatal error: Insufficient reserve: tokenOut amount exceeds withdrawable limit"
        ));
        assert!(is_limit_like_error_message(
            "Recoverable error: Withdrawal 57960015764091093503191 exceeds available liquidity 3543179769736901794933"
        ));
        assert!(is_limit_like_error_message("Sell amount exceeds limit 42"));
        assert!(is_limit_like_error_message("Ticks exceeded"));
    }
}
