use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use alloy_primitives::U256;
use simulator_replay::{ExactPoolQuote, ReplayQuoteError};
use tokio_util::sync::CancellationToken;
use tycho_simulation::tycho_common::Bytes;

use crate::api::{
    normalize_quote_request, HistoricalQuoteEntry, HistoricalQuoteResult, HistoricalQuoteTarget,
    PoolSelector, QuoteFailureReason, QuoteJobRequest, QuoteOutcome, QuoteProvenance,
    UnavailableReason, UnsignedAmount,
};
use crate::replay::{
    check_cancelled, gap_affects_interval, history_position, replay_backend,
    HistoricalReconstructor, ReconstructedState, ReconstructedTimeline, ReconstructionRequest,
};
use crate::{EngineProgress, HistoricalError, HistorySource};

pub struct HistoricalQuoteExecutor<S> {
    reconstructor: HistoricalReconstructor<S>,
    service_revision: String,
}

impl<S> HistoricalQuoteExecutor<S> {
    pub fn new(
        reconstructor: HistoricalReconstructor<S>,
        service_revision: impl Into<String>,
    ) -> Self {
        Self {
            reconstructor,
            service_revision: service_revision.into(),
        }
    }
}

impl<S: HistorySource> HistoricalQuoteExecutor<S> {
    pub async fn execute(
        &self,
        request: &QuoteJobRequest,
        cancellation: &CancellationToken,
        progress: &impl EngineProgress,
    ) -> Result<HistoricalQuoteResult, HistoricalError> {
        let normalized = normalize_quote_request(request);
        let start_blocks = normalized.blocks.start_blocks().collect::<Vec<_>>();
        let mut required_blocks = start_blocks.iter().copied().collect::<BTreeSet<_>>();
        for start in &start_blocks {
            for lag in &normalized.lags {
                required_blocks.insert(start.checked_add(*lag).ok_or_else(|| {
                    HistoricalError::InvalidSelector(
                        "start block plus lag exceeds the block number range".to_owned(),
                    )
                })?);
            }
        }
        let timeline = self
            .reconstructor
            .reconstruct(
                &ReconstructionRequest {
                    chain_id: normalized.chain_id,
                    backends: BTreeSet::from([normalized.pool.backend]),
                    required_blocks,
                },
                cancellation,
            )
            .await?;
        let mut quote_cache = BTreeMap::new();
        let mut results = Vec::with_capacity(start_blocks.len() * normalized.amounts_in.len());
        for start_block in start_blocks {
            for amount_in in &normalized.amounts_in {
                check_cancelled(cancellation)?;
                let start_outcome = quote_at(
                    &timeline,
                    start_block,
                    amount_in,
                    &normalized.pool,
                    &self.service_revision,
                    &mut quote_cache,
                );
                let start_state_available =
                    timeline.states.get(&start_block).is_some_and(Result::is_ok);
                let mut targets = Vec::with_capacity(normalized.lags.len());
                for lag in &normalized.lags {
                    check_cancelled(cancellation)?;
                    let target_block = start_block.checked_add(*lag).ok_or_else(|| {
                        HistoricalError::InvalidSelector(
                            "start block plus lag exceeds the block number range".to_owned(),
                        )
                    })?;
                    let outcome = if !start_state_available {
                        unavailable_from_start(&timeline, start_block)
                    } else if let Some(gap_refs) =
                        relative_gap_refs(&timeline, start_block, target_block)
                    {
                        QuoteOutcome::Unavailable {
                            reason: UnavailableReason::Gap,
                            gap_refs,
                        }
                    } else {
                        quote_at(
                            &timeline,
                            target_block,
                            amount_in,
                            &normalized.pool,
                            &self.service_revision,
                            &mut quote_cache,
                        )
                    };
                    targets.push(HistoricalQuoteTarget {
                        lag: *lag,
                        target_block,
                        outcome,
                    });
                    progress.quote_comparison_completed();
                }
                results.push(HistoricalQuoteEntry {
                    start_block,
                    amount_in: amount_in.clone(),
                    start_outcome,
                    targets,
                });
            }
        }
        Ok(HistoricalQuoteResult {
            request_id: request.request_id,
            chain_id: request.chain_id,
            pool: request.pool.clone(),
            visible_through_block: timeline.visible_through_block,
            gaps: timeline.gaps,
            results,
        })
    }
}

fn quote_at(
    timeline: &ReconstructedTimeline,
    block: u64,
    amount_in: &UnsignedAmount,
    pool: &PoolSelector,
    service_revision: &str,
    cache: &mut BTreeMap<(u64, UnsignedAmount), QuoteOutcome>,
) -> QuoteOutcome {
    let key = (block, amount_in.clone());
    if let Some(outcome) = cache.get(&key) {
        return outcome.clone();
    }
    let outcome = match timeline.states.get(&block) {
        Some(Ok(state)) => quote_state(state, amount_in, pool, service_revision),
        Some(Err(unavailable)) => QuoteOutcome::Unavailable {
            reason: unavailable.reason,
            gap_refs: unavailable.gap_refs.clone(),
        },
        None => QuoteOutcome::Unavailable {
            reason: UnavailableReason::StateApplicationInvalid,
            gap_refs: Vec::new(),
        },
    };
    cache.insert(key, outcome.clone());
    outcome
}

fn quote_state(
    state: &ReconstructedState,
    amount_in: &UnsignedAmount,
    pool: &PoolSelector,
    service_revision: &str,
) -> QuoteOutcome {
    let Ok(token_in) = Bytes::from_str(&pool.token_in) else {
        return QuoteOutcome::QuoteFailed {
            reason: QuoteFailureReason::DirectionUnsupported,
        };
    };
    let Ok(token_out) = Bytes::from_str(&pool.token_out) else {
        return QuoteOutcome::QuoteFailed {
            reason: QuoteFailureReason::DirectionUnsupported,
        };
    };
    let Ok(amount) = U256::from_str_radix(amount_in.as_str(), 10) else {
        return QuoteOutcome::QuoteFailed {
            reason: QuoteFailureReason::SimulationFailed,
        };
    };
    match state.point.quote(&ExactPoolQuote {
        backend: replay_backend(pool.backend),
        protocol: pool.protocol.clone(),
        component_id: pool.component_id.clone(),
        token_in,
        token_out,
        amount_in: amount,
    }) {
        Ok(amount_out) => {
            let Ok(amount_out) = amount_out.to_string().parse() else {
                return QuoteOutcome::QuoteFailed {
                    reason: QuoteFailureReason::SimulationFailed,
                };
            };
            QuoteOutcome::Ok {
                amount_out,
                provenance: QuoteProvenance {
                    block_hash: state.provenance.block_hash.clone(),
                    final_position: state.provenance.final_position,
                    anchor_checkpoint_position: state.provenance.anchor_checkpoint_position,
                    token_snapshot_digest: state.provenance.token_snapshot_digest.clone(),
                    service_revision: service_revision.to_owned(),
                    rfq_observation_cursor: state.provenance.rfq_observation_cursor,
                    rfq_observed_at: state.provenance.rfq_observed_at,
                    rfq_observation_age_ms: state.provenance.rfq_observation_age_ms,
                },
            }
        }
        Err(error) => QuoteOutcome::QuoteFailed {
            reason: quote_failure_reason(&error),
        },
    }
}

pub(crate) fn quote_failure_reason(error: &ReplayQuoteError) -> QuoteFailureReason {
    match error {
        ReplayQuoteError::ComponentNotFound(_) => QuoteFailureReason::ComponentNotFound,
        ReplayQuoteError::BackendMismatch { .. }
        | ReplayQuoteError::ProtocolMismatch { .. }
        | ReplayQuoteError::DirectionUnsupported(_)
        | ReplayQuoteError::TokenMissing(_) => QuoteFailureReason::DirectionUnsupported,
        ReplayQuoteError::ZeroOutput => QuoteFailureReason::ZeroOutput,
        ReplayQuoteError::SimulationFailed(message)
            if message.to_ascii_lowercase().contains("insufficient") =>
        {
            QuoteFailureReason::InsufficientLiquidity
        }
        ReplayQuoteError::SimulationFailed(_)
        | ReplayQuoteError::SimulationPanicked(_)
        | ReplayQuoteError::AmountOverflow => QuoteFailureReason::SimulationFailed,
    }
}

fn unavailable_from_start(timeline: &ReconstructedTimeline, start_block: u64) -> QuoteOutcome {
    match timeline.states.get(&start_block) {
        Some(Err(unavailable)) => QuoteOutcome::Unavailable {
            reason: unavailable.reason,
            gap_refs: unavailable.gap_refs.clone(),
        },
        _ => QuoteOutcome::Unavailable {
            reason: UnavailableReason::StateApplicationInvalid,
            gap_refs: Vec::new(),
        },
    }
}

fn relative_gap_refs(
    timeline: &ReconstructedTimeline,
    start_block: u64,
    target_block: u64,
) -> Option<Vec<String>> {
    let start = timeline.states.get(&start_block)?.as_ref().ok()?;
    let target = timeline.states.get(&target_block)?.as_ref().ok()?;
    let gap_refs = timeline
        .range_gaps
        .iter()
        .enumerate()
        .filter(|(_, gap)| {
            gap_affects_interval(
                gap,
                history_position(start.provenance.final_position),
                history_position(target.provenance.final_position),
                start_block,
                target_block,
                timeline.block_boundaries_ms.get(&start_block).copied(),
                timeline.block_boundaries_ms.get(&target_block).copied(),
            )
        })
        .map(|(index, _)| format!("gap-{}", index + 1))
        .collect::<Vec<_>>();
    (!gap_refs.is_empty()).then_some(gap_refs)
}
