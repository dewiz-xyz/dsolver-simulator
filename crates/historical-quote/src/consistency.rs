use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;

use alloy_primitives::U256;
use sha2::{Digest, Sha256};
use simulator_replay::{CanonicalState, ExactPoolQuote};
use state_history::{
    BlockInterval, CheckpointPairQuery, CheckpointPairSelection, ExplicitCheckpointPair, ReadLimits,
};
use tokio_util::sync::CancellationToken;
use tycho_simulation::tycho_common::Bytes;

use crate::api::{
    Backend, CanonicalStateSummary, ConsistencyCheckReport, ConsistencyCheckRequest,
    ConsistencyDifference, ConsistencyPairReport, ConsistencyQuoteComparison,
    ConsistencyQuoteOutcome, ConsistencySelection, ConsistencySelectionStatus, ConsistencySummary,
    PairStatus, PoolSelector, QuoteComparisonRequest, QuoteFailureReason, UnsignedAmount,
};
use crate::quote_job::quote_failure_reason;
use crate::replay::{
    apply_all_pair_deltas, check_cancelled, history_backend, history_position, public_position,
    replay_backend, restore_checkpoint,
};
use crate::{EngineProgress, HistoricalError, HistorySource};

pub const DEFAULT_MAX_CONSISTENCY_DIFFERENCES: usize = 100;

pub struct ConsistencyChecker<S> {
    source: Arc<S>,
    read_limits: ReadLimits,
    max_differences: usize,
}

impl<S> ConsistencyChecker<S> {
    pub fn new(source: Arc<S>, read_limits: ReadLimits, max_differences: usize) -> Self {
        Self {
            source,
            read_limits,
            max_differences,
        }
    }

    pub fn with_default_limit(source: Arc<S>, read_limits: ReadLimits) -> Self {
        Self::new(source, read_limits, DEFAULT_MAX_CONSISTENCY_DIFFERENCES)
    }
}

impl<S: HistorySource> ConsistencyChecker<S> {
    pub async fn execute(
        &self,
        request: &ConsistencyCheckRequest,
        cancellation: &CancellationToken,
        progress: &impl EngineProgress,
    ) -> Result<ConsistencyCheckReport, HistoricalError> {
        check_cancelled(cancellation)?;
        if request.backends.contains(&Backend::Vm) {
            return Err(HistoricalError::UnsupportedCanonicalVm);
        }
        let query = checkpoint_pair_query(request)?;
        let pairs = self.source.resolve_checkpoint_pairs(query).await?;
        if pairs.is_empty() {
            return Ok(empty_report(request));
        }

        let mut remaining_differences = self.max_differences;
        let mut reports = Vec::with_capacity(pairs.len());
        for pair in pairs {
            check_cancelled(cancellation)?;
            let plan = self
                .source
                .plan_checkpoint_pair(
                    pair,
                    request
                        .backends
                        .iter()
                        .copied()
                        .map(history_backend)
                        .collect(),
                    self.read_limits,
                )
                .await?;
            let report = if plan.gaps.is_empty() {
                self.compare_pair(request, plan, cancellation, remaining_differences)
                    .await?
            } else {
                gap_report(request, &plan.pair)
            };
            remaining_differences = remaining_differences.saturating_sub(report.differences.len());
            reports.push(report);
            progress.checkpoint_pair_completed();
        }
        let summary = summarize(&reports);
        let mut backends = request.backends.clone();
        backends.sort_unstable();
        Ok(ConsistencyCheckReport {
            request_id: request.request_id,
            chain_id: request.chain_id,
            backends,
            selection: request.selection.clone(),
            selection_status: ConsistencySelectionStatus::Compared,
            summary,
            pairs: reports,
        })
    }

    async fn compare_pair(
        &self,
        request: &ConsistencyCheckRequest,
        plan: state_history::CheckpointPairReplayPlan,
        cancellation: &CancellationToken,
        difference_limit: usize,
    ) -> Result<ConsistencyPairReport, HistoricalError> {
        let backends = request.backends.iter().copied().collect::<BTreeSet<_>>();
        let direct = restore_checkpoint(
            self.source.as_ref(),
            &plan.pair.target,
            &backends,
            self.read_limits,
            cancellation,
        )
        .await?;
        let replayed = restore_checkpoint(
            self.source.as_ref(),
            &plan.pair.earlier,
            &backends,
            self.read_limits,
            cancellation,
        )
        .await?;
        let (Some(direct), Some(mut replayed)) = (direct, replayed) else {
            return Ok(gap_report(request, &plan.pair));
        };
        if direct.checkpoint_application_invalid
            || apply_all_pair_deltas(&mut replayed, &plan.deltas, &backends, cancellation).await?
        {
            return Err(HistoricalError::StateReconstructionFailed(
                "checkpoint pair produced a deterministic apply anomaly".to_owned(),
            ));
        }
        replayed
            .world
            .set_current_block(target_world_marker(&plan.pair.target, &request.backends));

        let replay_backends = request
            .backends
            .iter()
            .copied()
            .map(replay_backend)
            .collect::<Vec<_>>();
        let direct_state = direct
            .world
            .pin()
            .canonical_state(&replay_backends)
            .map_err(canonical_error)?;
        let replayed_state = replayed
            .world
            .pin()
            .canonical_state(&replay_backends)
            .map_err(canonical_error)?;
        let state_matches = direct_state.as_bytes() == replayed_state.as_bytes();
        let difference_set = canonical_differences(
            &direct_state,
            &replayed_state,
            &request.backends,
            difference_limit,
        );

        let quote_comparisons = compare_quotes(
            &direct.world.pin(),
            &replayed.world.pin(),
            &request.quotes_to_compare,
            cancellation,
        )?;
        let status = compared_pair_status(state_matches, &quote_comparisons);
        let mut affected_backends = difference_set.affected_backends;
        let mut affected_component_ids = difference_set.affected_component_ids;
        for comparison in &quote_comparisons {
            if comparison.status == PairStatus::Mismatch {
                affected_backends.insert(comparison.pool.backend);
                affected_component_ids.insert(comparison.pool.component_id.clone());
            }
        }
        Ok(ConsistencyPairReport {
            earlier_position: public_position(plan.pair.earlier.position),
            target_position: public_position(plan.pair.target.position),
            status,
            direct_state: Some(canonical_summary(&direct_state)),
            replayed_state: Some(canonical_summary(&replayed_state)),
            affected_backends: affected_backends.into_iter().collect(),
            affected_component_ids: affected_component_ids.into_iter().collect(),
            quote_comparisons,
            total_difference_count: difference_set.total_count,
            differences: difference_set.differences,
        })
    }
}

fn compare_quotes(
    direct: &simulator_replay::StatePoint,
    replayed: &simulator_replay::StatePoint,
    requests: &[QuoteComparisonRequest],
    cancellation: &CancellationToken,
) -> Result<Vec<ConsistencyQuoteComparison>, HistoricalError> {
    let mut comparisons = Vec::new();
    for quote in sorted_quote_requests(requests) {
        for amount in quote.amounts_in {
            check_cancelled(cancellation)?;
            let direct_outcome = consistency_quote(direct, &quote.pool, &amount);
            check_cancelled(cancellation)?;
            let replayed_outcome = consistency_quote(replayed, &quote.pool, &amount);
            let status = quote_status(&direct_outcome, &replayed_outcome);
            comparisons.push(ConsistencyQuoteComparison {
                pool: quote.pool.clone(),
                amount_in: amount,
                direct_outcome,
                replayed_outcome,
                status,
            });
        }
    }
    Ok(comparisons)
}

fn compared_pair_status(
    state_matches: bool,
    comparisons: &[ConsistencyQuoteComparison],
) -> PairStatus {
    if !state_matches
        || comparisons
            .iter()
            .any(|comparison| comparison.status == PairStatus::Mismatch)
    {
        PairStatus::Mismatch
    } else if comparisons
        .iter()
        .any(|comparison| comparison.status == PairStatus::NotComparable)
    {
        PairStatus::NotComparable
    } else {
        PairStatus::Pass
    }
}

fn checkpoint_pair_query(
    request: &ConsistencyCheckRequest,
) -> Result<CheckpointPairQuery, HistoricalError> {
    let selection = match &request.selection {
        ConsistencySelection::CheckpointPairs { pairs } => CheckpointPairSelection::Explicit(
            pairs
                .iter()
                .map(|pair| ExplicitCheckpointPair {
                    earlier_position: history_position(pair.earlier_position),
                    target_position: history_position(pair.target_position),
                })
                .collect(),
        ),
        ConsistencySelection::BlockRange {
            start,
            end_inclusive,
        } => CheckpointPairSelection::BlockRange(
            BlockInterval::new(*start, *end_inclusive)
                .map_err(|error| HistoricalError::InvalidSelector(error.to_string()))?,
        ),
    };
    Ok(CheckpointPairQuery::for_backends(
        request.chain_id,
        selection,
        request
            .backends
            .iter()
            .copied()
            .map(history_backend)
            .collect(),
    ))
}

fn empty_report(request: &ConsistencyCheckRequest) -> ConsistencyCheckReport {
    let mut backends = request.backends.clone();
    backends.sort_unstable();
    ConsistencyCheckReport {
        request_id: request.request_id,
        chain_id: request.chain_id,
        backends,
        selection: request.selection.clone(),
        selection_status: ConsistencySelectionStatus::NotComparable,
        summary: ConsistencySummary {
            selected_checkpoint_pairs: 0,
            compared_checkpoint_pairs: 0,
            pass: 0,
            mismatch: 0,
            gap: 0,
            not_comparable: 0,
        },
        pairs: Vec::new(),
    }
}

fn gap_report(
    request: &ConsistencyCheckRequest,
    pair: &state_history::CheckpointPair,
) -> ConsistencyPairReport {
    let mut affected_backends = request.backends.clone();
    affected_backends.sort_unstable();
    ConsistencyPairReport {
        earlier_position: public_position(pair.earlier.position),
        target_position: public_position(pair.target.position),
        status: PairStatus::Gap,
        direct_state: None,
        replayed_state: None,
        affected_backends,
        affected_component_ids: Vec::new(),
        quote_comparisons: Vec::new(),
        total_difference_count: 0,
        differences: Vec::new(),
    }
}

fn summarize(reports: &[ConsistencyPairReport]) -> ConsistencySummary {
    let mut summary = ConsistencySummary {
        selected_checkpoint_pairs: u64::try_from(reports.len()).unwrap_or(u64::MAX),
        compared_checkpoint_pairs: 0,
        pass: 0,
        mismatch: 0,
        gap: 0,
        not_comparable: 0,
    };
    for report in reports {
        match report.status {
            PairStatus::Pass => {
                summary.pass += 1;
                summary.compared_checkpoint_pairs += 1;
            }
            PairStatus::Mismatch => {
                summary.mismatch += 1;
                summary.compared_checkpoint_pairs += 1;
            }
            PairStatus::Gap => summary.gap += 1,
            PairStatus::NotComparable => {
                summary.not_comparable += 1;
                summary.compared_checkpoint_pairs += 1;
            }
        }
    }
    summary
}

fn sorted_quote_requests(requests: &[QuoteComparisonRequest]) -> Vec<QuoteComparisonRequest> {
    let mut requests = requests.to_vec();
    for request in &mut requests {
        request.amounts_in.sort_unstable();
    }
    requests.sort_unstable();
    requests
}

fn consistency_quote(
    point: &simulator_replay::StatePoint,
    pool: &PoolSelector,
    amount: &UnsignedAmount,
) -> ConsistencyQuoteOutcome {
    let request = Bytes::from_str(&pool.token_in)
        .and_then(|token_in| {
            Bytes::from_str(&pool.token_out).map(|token_out| (token_in, token_out))
        })
        .ok()
        .and_then(|(token_in, token_out)| {
            U256::from_str_radix(amount.as_str(), 10)
                .ok()
                .map(|amount_in| ExactPoolQuote {
                    backend: replay_backend(pool.backend),
                    protocol: pool.protocol.clone(),
                    component_id: pool.component_id.clone(),
                    token_in,
                    token_out,
                    amount_in,
                })
        });
    let Some(request) = request else {
        return ConsistencyQuoteOutcome::QuoteFailed {
            reason: QuoteFailureReason::SimulationFailed,
        };
    };
    match point.quote(&request) {
        Ok(amount_out) => match amount_out.to_string().parse() {
            Ok(amount_out) => ConsistencyQuoteOutcome::Ok { amount_out },
            Err(_) => ConsistencyQuoteOutcome::QuoteFailed {
                reason: QuoteFailureReason::SimulationFailed,
            },
        },
        Err(error) => ConsistencyQuoteOutcome::QuoteFailed {
            reason: quote_failure_reason(&error),
        },
    }
}

fn quote_status(
    direct: &ConsistencyQuoteOutcome,
    replayed: &ConsistencyQuoteOutcome,
) -> PairStatus {
    match (direct, replayed) {
        (
            ConsistencyQuoteOutcome::Ok { amount_out: direct },
            ConsistencyQuoteOutcome::Ok {
                amount_out: replayed,
            },
        ) if direct == replayed => PairStatus::Pass,
        (
            ConsistencyQuoteOutcome::QuoteFailed { reason: direct },
            ConsistencyQuoteOutcome::QuoteFailed { reason: replayed },
        ) if direct == replayed => PairStatus::NotComparable,
        _ => PairStatus::Mismatch,
    }
}

fn target_world_marker(manifest: &state_history::CheckpointManifest, backends: &[Backend]) -> u64 {
    let mut marker = 0;
    if backends
        .iter()
        .any(|backend| matches!(backend, Backend::Native | Backend::Vm))
    {
        marker = marker.max(manifest.block_number);
    }
    if backends.contains(&Backend::Rfq) {
        marker = marker.max(manifest.rfq_observed_at_ms.unwrap_or_default() / 1_000);
    }
    marker
}

fn canonical_error(error: simulator_replay::CanonicalStateError) -> HistoricalError {
    match error {
        simulator_replay::CanonicalStateError::UnsupportedBackend(_) => {
            HistoricalError::UnsupportedCanonicalVm
        }
        simulator_replay::CanonicalStateError::Serialize(error) => {
            HistoricalError::StateReconstructionFailed(error.to_string())
        }
    }
}

fn canonical_summary(state: &CanonicalState) -> CanonicalStateSummary {
    CanonicalStateSummary {
        sha256: digest(state.as_bytes()),
        component_count: u64::try_from(state.component_count).unwrap_or(u64::MAX),
        backend_counts: state
            .backend_counts
            .iter()
            .map(|(backend, count)| {
                (
                    match backend {
                        simulator_replay::ReplayBackend::Native => Backend::Native,
                        simulator_replay::ReplayBackend::Vm => Backend::Vm,
                        simulator_replay::ReplayBackend::Rfq => Backend::Rfq,
                    },
                    u64::try_from(*count).unwrap_or(u64::MAX),
                )
            })
            .collect(),
    }
}

struct DifferenceSet {
    total_count: u64,
    differences: Vec<ConsistencyDifference>,
    affected_backends: BTreeSet<Backend>,
    affected_component_ids: BTreeSet<String>,
}

fn canonical_differences(
    direct: &CanonicalState,
    replayed: &CanonicalState,
    requested_backends: &[Backend],
    limit: usize,
) -> DifferenceSet {
    let mut candidates = Vec::new();
    let mut affected_backends = BTreeSet::new();
    let mut affected_component_ids = BTreeSet::new();
    collect_component_differences(
        direct,
        replayed,
        &mut candidates,
        &mut affected_backends,
        &mut affected_component_ids,
    );
    collect_token_differences(
        direct,
        replayed,
        requested_backends,
        &mut candidates,
        &mut affected_backends,
    );
    if direct.as_bytes() != replayed.as_bytes() && candidates.is_empty() {
        for backend in requested_backends {
            candidates.push(difference(
                *backend,
                None,
                "canonical_state",
                Some(direct.as_bytes()),
                Some(replayed.as_bytes()),
            ));
            affected_backends.insert(*backend);
        }
    }
    candidates.sort_by(|left, right| {
        (
            left.backend,
            left.component_id.as_deref(),
            left.field.as_str(),
        )
            .cmp(&(
                right.backend,
                right.component_id.as_deref(),
                right.field.as_str(),
            ))
    });
    let total_count = u64::try_from(candidates.len()).unwrap_or(u64::MAX);
    candidates.truncate(limit);
    DifferenceSet {
        total_count,
        differences: candidates,
        affected_backends,
        affected_component_ids,
    }
}

fn collect_component_differences(
    direct: &CanonicalState,
    replayed: &CanonicalState,
    candidates: &mut Vec<ConsistencyDifference>,
    affected_backends: &mut BTreeSet<Backend>,
    affected_component_ids: &mut BTreeSet<String>,
) {
    let direct_components = direct
        .components()
        .iter()
        .map(|component| {
            (
                (
                    public_replay_backend(component.backend),
                    component.component_id.clone(),
                ),
                component,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let replayed_components = replayed
        .components()
        .iter()
        .map(|component| {
            (
                (
                    public_replay_backend(component.backend),
                    component.component_id.clone(),
                ),
                component,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let component_keys = direct_components
        .keys()
        .chain(replayed_components.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for (backend, component_id) in component_keys {
        let component_difference_start = candidates.len();
        let direct = direct_components.get(&(backend, component_id.clone()));
        let replayed = replayed_components.get(&(backend, component_id.clone()));
        if direct.map(|value| value.component_bytes())
            != replayed.map(|value| value.component_bytes())
        {
            candidates.push(difference(
                backend,
                Some(component_id.clone()),
                "component",
                direct.map(|value| value.component_bytes()),
                replayed.map(|value| value.component_bytes()),
            ));
        }
        if direct.map(|value| value.state_bytes()) != replayed.map(|value| value.state_bytes()) {
            candidates.push(difference(
                backend,
                Some(component_id.clone()),
                "state",
                direct.map(|value| value.state_bytes()),
                replayed.map(|value| value.state_bytes()),
            ));
        }
        if candidates.len() != component_difference_start {
            affected_backends.insert(backend);
            affected_component_ids.insert(component_id);
        }
    }
}

fn collect_token_differences(
    direct: &CanonicalState,
    replayed: &CanonicalState,
    requested_backends: &[Backend],
    candidates: &mut Vec<ConsistencyDifference>,
    affected_backends: &mut BTreeSet<Backend>,
) {
    let direct_tokens = direct
        .tokens()
        .iter()
        .map(|token| (token.address.clone(), token.token_bytes()))
        .collect::<BTreeMap<_, _>>();
    let replayed_tokens = replayed
        .tokens()
        .iter()
        .map(|token| (token.address.clone(), token.token_bytes()))
        .collect::<BTreeMap<_, _>>();
    let token_keys = direct_tokens
        .keys()
        .chain(replayed_tokens.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for address in token_keys {
        let direct = direct_tokens.get(&address).copied();
        let replayed = replayed_tokens.get(&address).copied();
        if direct != replayed {
            for backend in requested_backends {
                candidates.push(difference(
                    *backend,
                    None,
                    &format!("token:{address}"),
                    direct,
                    replayed,
                ));
                affected_backends.insert(*backend);
            }
        }
    }
}

fn difference(
    backend: Backend,
    component_id: Option<String>,
    field: &str,
    direct: Option<&[u8]>,
    replayed: Option<&[u8]>,
) -> ConsistencyDifference {
    ConsistencyDifference {
        backend,
        component_id,
        field: field.to_owned(),
        direct_sha256: direct.map(digest),
        replayed_sha256: replayed.map(digest),
    }
}

fn digest(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn public_replay_backend(backend: simulator_replay::ReplayBackend) -> Backend {
    match backend {
        simulator_replay::ReplayBackend::Native => Backend::Native,
        simulator_replay::ReplayBackend::Vm => Backend::Vm,
        simulator_replay::ReplayBackend::Rfq => Backend::Rfq,
    }
}
