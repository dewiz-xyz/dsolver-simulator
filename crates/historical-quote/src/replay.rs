use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use simulator_replay::{
    acquire_vm_world_lease, decode_token_map, DecoderConfig, ReplayBackend, ReplayDecoder,
    ReplayWorld, StatePoint, VmWorldLease,
};
use state_history::{
    Backend as HistoryBackend, BlockInterval, CheckpointManifest, RangeGap, RangeGapKind, RangeLeg,
    ReadLimits, StoredDelta, StreamPosition as HistoryPosition, TargetPlan, TargetPlanQuery,
};
use tokio_util::sync::CancellationToken;

use crate::api::{Backend, GapCause, GapKind, GapReference, StreamPosition, UnavailableReason};
use crate::{HistoricalError, HistorySource};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconstructionRequest {
    pub chain_id: u64,
    pub backends: BTreeSet<Backend>,
    pub required_blocks: BTreeSet<u64>,
}

pub struct ReconstructedTimeline {
    pub visible_through_block: u64,
    pub states: BTreeMap<u64, Result<ReconstructedState, UnavailableState>>,
    pub gaps: Vec<GapReference>,
    pub estimated_decoded_bytes: u64,
    pub(crate) range_gaps: Vec<RangeGap>,
    pub(crate) block_boundaries_ms: BTreeMap<u64, u64>,
    _vm_lease: Option<VmWorldLease>,
}

#[derive(Clone)]
pub struct ReconstructedState {
    pub point: StatePoint,
    pub provenance: StateProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateProvenance {
    pub block_hash: String,
    pub final_position: StreamPosition,
    pub anchor_checkpoint_position: StreamPosition,
    pub token_snapshot_digest: Option<String>,
    pub rfq_observation_cursor: Option<StreamPosition>,
    pub rfq_observed_at: Option<DateTime<Utc>>,
    pub rfq_observation_age_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnavailableState {
    pub reason: UnavailableReason,
    pub gap_refs: Vec<String>,
}

pub struct HistoricalReconstructor<S> {
    source: Arc<S>,
    read_limits: ReadLimits,
}

impl<S> HistoricalReconstructor<S> {
    pub fn new(source: Arc<S>, read_limits: ReadLimits) -> Self {
        Self {
            source,
            read_limits,
        }
    }
}

impl<S: HistorySource> HistoricalReconstructor<S> {
    pub async fn reconstruct(
        &self,
        request: &ReconstructionRequest,
        cancellation: &CancellationToken,
    ) -> Result<ReconstructedTimeline, HistoricalError> {
        check_cancelled(cancellation)?;
        let history_backends = request
            .backends
            .iter()
            .copied()
            .map(history_backend)
            .collect::<Vec<_>>();
        let query = TargetPlanQuery::new(
            request.chain_id,
            history_backends,
            request.required_blocks.clone(),
            self.read_limits,
        )
        .map_err(|error| HistoricalError::InvalidSelector(error.to_string()))?;
        let plan = self.source.plan_targets(query).await?;
        check_cancelled(cancellation)?;
        let vm_lease = if request.backends.contains(&Backend::Vm) {
            Some(acquire_vm_world_lease().await)
        } else {
            None
        };
        let mut timeline = self.reconstruct_plan(request, plan, cancellation).await?;
        timeline._vm_lease = vm_lease;
        Ok(timeline)
    }

    async fn reconstruct_plan(
        &self,
        request: &ReconstructionRequest,
        plan: TargetPlan,
        cancellation: &CancellationToken,
    ) -> Result<ReconstructedTimeline, HistoricalError> {
        let gaps = plan
            .gaps
            .iter()
            .enumerate()
            .map(|(index, gap)| public_gap(index, gap))
            .collect::<Result<Vec<_>, _>>()?;
        let mut states = early_unavailable_states(request, &plan);
        let mut assigned = vec![Vec::new(); plan.legs.len()];
        for block in &request.required_blocks {
            if states.contains_key(block) {
                continue;
            }
            if let Some(index) = leg_for_block(&plan, &request.backends, *block) {
                assigned[index].push(*block);
            }
        }

        for (index, leg) in plan.legs.iter().enumerate() {
            if assigned[index].is_empty() {
                continue;
            }
            for (block, state) in self
                .reconstruct_leg(request, &plan, leg, &assigned[index], cancellation)
                .await?
            {
                states.insert(block, state);
            }
        }

        for block in &request.required_blocks {
            states.entry(*block).or_insert_with(|| {
                let gap_refs = matching_gap_refs(&plan.gaps, *block);
                Err(UnavailableState {
                    reason: if gap_refs.is_empty() {
                        if request.backends.contains(&Backend::Rfq) {
                            UnavailableReason::RfqObservationMissing
                        } else {
                            UnavailableReason::StateApplicationInvalid
                        }
                    } else {
                        UnavailableReason::Gap
                    },
                    gap_refs,
                })
            });
        }
        Ok(ReconstructedTimeline {
            visible_through_block: plan.visible_through_block,
            states,
            gaps,
            estimated_decoded_bytes: plan.estimated_decoded_bytes,
            range_gaps: plan.gaps,
            block_boundaries_ms: request
                .required_blocks
                .iter()
                .filter_map(|block| {
                    block.checked_add(1).and_then(|next| {
                        plan.block_times
                            .get(&next)
                            .map(|observation| (*block, observation.timestamp_ms))
                    })
                })
                .collect(),
            _vm_lease: None,
        })
    }

    async fn reconstruct_leg(
        &self,
        request: &ReconstructionRequest,
        plan: &TargetPlan,
        leg: &RangeLeg,
        blocks: &[u64],
        cancellation: &CancellationToken,
    ) -> Result<Vec<(u64, Result<ReconstructedState, UnavailableState>)>, HistoricalError> {
        check_cancelled(cancellation)?;
        let restored = restore_checkpoint(
            self.source.as_ref(),
            &leg.checkpoint,
            &request.backends,
            self.read_limits,
            cancellation,
        )
        .await?;
        let Some(mut restored) = restored else {
            return Ok(blocks
                .iter()
                .map(|block| {
                    (
                        *block,
                        Err(unavailable(UnavailableReason::TokenSnapshotMissing)),
                    )
                })
                .collect());
        };
        let mut applied = BTreeSet::new();
        let mut application_invalid = restored.checkpoint_application_invalid;
        let mut blocks = blocks.to_vec();
        blocks.sort_unstable();
        let mut states = Vec::with_capacity(blocks.len());
        for block in blocks {
            check_cancelled(cancellation)?;
            let next_block_timestamp = block
                .checked_add(1)
                .and_then(|next| plan.block_times.get(&next))
                .map(|observation| observation.timestamp_ms);
            let mut application = DeltaApplication {
                backends: &request.backends,
                target_block: block,
                next_block_timestamp,
                applied: &mut applied,
                application_invalid: &mut application_invalid,
                cancellation,
            };
            apply_deltas_through(&mut restored, &leg.deltas, &mut application).await?;
            states.push((
                block,
                reconstructed_state(
                    request,
                    plan,
                    &restored,
                    block,
                    next_block_timestamp,
                    application_invalid,
                )?,
            ));
        }
        Ok(states)
    }
}

pub(crate) struct RestoredWorld {
    pub(crate) world: ReplayWorld,
    pub(crate) decoder: ReplayDecoder,
    pub(crate) final_position: HistoryPosition,
    pub(crate) anchor_checkpoint_position: HistoryPosition,
    pub(crate) anchor_block: u64,
    pub(crate) token_snapshot_digest: Option<String>,
    pub(crate) anchor_rfq_observed_at_ms: Option<u64>,
    pub(crate) rfq_observed_at_ms: Option<u64>,
    pub(crate) rfq_observation_position: Option<HistoryPosition>,
    pub(crate) checkpoint_application_invalid: bool,
}

pub(crate) async fn restore_checkpoint<S: HistorySource>(
    source: &S,
    manifest: &CheckpointManifest,
    backends: &BTreeSet<Backend>,
    limits: ReadLimits,
    cancellation: &CancellationToken,
) -> Result<Option<RestoredWorld>, HistoricalError> {
    check_cancelled(cancellation)?;
    let needs_tokens = backends
        .iter()
        .any(|backend| matches!(backend, Backend::Native | Backend::Vm));
    let (tokens, token_snapshot_digest) = if needs_tokens {
        if manifest.token_reference.is_none() {
            return Ok(None);
        }
        let snapshot = source
            .fetch_checkpoint_token_snapshot(manifest.clone(), limits)
            .await?
            .ok_or_else(|| {
                HistoricalError::HistoricalDataInvalid(
                    "checkpoint token reference resolved without a token snapshot".to_owned(),
                )
            })?;
        let tokens = decode_token_map(snapshot.chain_id, snapshot.tokens.as_ref())
            .map_err(|error| HistoricalError::HistoricalDataInvalid(error.to_string()))?;
        let digest = manifest
            .token_reference
            .as_ref()
            .map(|reference| format!("sha256:{}", reference.sha256));
        (tokens, digest)
    } else {
        (simulator_replay::TokenMap::new(), None)
    };
    check_cancelled(cancellation)?;
    let archive = source.fetch_checkpoint(manifest.clone(), limits).await?;
    let decoder = ReplayDecoder::new(DecoderConfig::retained_v1(), tokens.clone())
        .await
        .map_err(|error| HistoricalError::HistoricalDataInvalid(error.to_string()))?;
    let replay_backends = backends
        .iter()
        .copied()
        .map(replay_backend)
        .collect::<Vec<_>>();
    let decoded = decoder
        .decode_checkpoint_payloads(&archive.payloads_json, &replay_backends)
        .await
        .map_err(|error| HistoricalError::HistoricalDataInvalid(error.to_string()))?;
    let update = decoded.update.ok_or_else(|| {
        HistoricalError::HistoricalDataInvalid(
            "checkpoint contains no state for the selected backends".to_owned(),
        )
    })?;
    let mut world = ReplayWorld::new(tokens, None);
    let report = world.restore(update);
    Ok(Some(RestoredWorld {
        world,
        decoder,
        final_position: manifest.position,
        anchor_checkpoint_position: manifest.position,
        anchor_block: manifest.block_number,
        token_snapshot_digest,
        anchor_rfq_observed_at_ms: manifest.rfq_observed_at_ms,
        rfq_observed_at_ms: manifest.rfq_observed_at_ms,
        rfq_observation_position: manifest.rfq_observed_at_ms.map(|_| manifest.position),
        checkpoint_application_invalid: report.has_anomalies(),
    }))
}

pub(crate) async fn apply_all_pair_deltas(
    restored: &mut RestoredWorld,
    deltas: &[StoredDelta],
    backends: &BTreeSet<Backend>,
    cancellation: &CancellationToken,
) -> Result<bool, HistoricalError> {
    let mut invalid = restored.checkpoint_application_invalid;
    for delta in deltas {
        check_cancelled(cancellation)?;
        let replay_backends = delta
            .applicable_backends
            .iter()
            .filter_map(|cursor| {
                let backend = public_backend(cursor.backend);
                backends
                    .contains(&backend)
                    .then_some(replay_backend(backend))
            })
            .collect::<Vec<_>>();
        if replay_backends.is_empty() {
            continue;
        }
        let decoded = restored
            .decoder
            .decode_delta(
                delta.payload_format_version,
                delta.raw_payload.as_ref(),
                &replay_backends,
            )
            .await
            .map_err(|error| HistoricalError::HistoricalDataInvalid(error.to_string()))?;
        if let Some(update) = decoded.update {
            invalid |= restored.world.apply(update).has_anomalies();
        }
        if decoded.had_applicable_partition {
            restored.final_position = delta.position;
        }
    }
    Ok(invalid)
}

struct DeltaApplication<'a> {
    backends: &'a BTreeSet<Backend>,
    target_block: u64,
    next_block_timestamp: Option<u64>,
    applied: &'a mut BTreeSet<(HistoryPosition, Backend)>,
    application_invalid: &'a mut bool,
    cancellation: &'a CancellationToken,
}

async fn apply_deltas_through(
    restored: &mut RestoredWorld,
    deltas: &[StoredDelta],
    application: &mut DeltaApplication<'_>,
) -> Result<(), HistoricalError> {
    for delta in deltas {
        check_cancelled(application.cancellation)?;
        let eligible = delta
            .applicable_backends
            .iter()
            .filter_map(|cursor| {
                let backend = public_backend(cursor.backend);
                if !application.backends.contains(&backend)
                    || application.applied.contains(&(delta.position, backend))
                {
                    return None;
                }
                let ready = match backend {
                    Backend::Native | Backend::Vm => cursor
                        .block_number
                        .is_some_and(|block| block <= application.target_block),
                    Backend::Rfq => application.next_block_timestamp.is_some_and(|boundary| {
                        cursor.observed_at_ms.is_some_and(|observed| {
                            observed < boundary
                                && restored
                                    .rfq_observed_at_ms
                                    .is_none_or(|selected| observed >= selected)
                        })
                    }),
                };
                ready.then_some((backend, cursor.observed_at_ms))
            })
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            continue;
        }
        let replay_backends = eligible
            .iter()
            .map(|(backend, _)| replay_backend(*backend))
            .collect::<Vec<_>>();
        let decoded = restored
            .decoder
            .decode_delta(
                delta.payload_format_version,
                delta.raw_payload.as_ref(),
                &replay_backends,
            )
            .await
            .map_err(|error| HistoricalError::HistoricalDataInvalid(error.to_string()))?;
        if let Some(update) = decoded.update {
            *application.application_invalid |= restored.world.apply(update).has_anomalies();
        }
        if decoded.had_applicable_partition {
            restored.final_position = delta.position;
            for (backend, observed_at_ms) in eligible {
                application.applied.insert((delta.position, backend));
                if backend == Backend::Rfq {
                    restored.rfq_observed_at_ms = observed_at_ms;
                    restored.rfq_observation_position = Some(delta.position);
                }
            }
        }
    }
    Ok(())
}

fn reconstructed_state(
    request: &ReconstructionRequest,
    plan: &TargetPlan,
    restored: &RestoredWorld,
    block: u64,
    next_block_timestamp: Option<u64>,
    application_invalid: bool,
) -> Result<Result<ReconstructedState, UnavailableState>, HistoricalError> {
    if application_invalid {
        return Ok(Err(unavailable(UnavailableReason::StateApplicationInvalid)));
    }
    let gap_refs = state_gap_refs(&plan.gaps, restored, block, next_block_timestamp);
    if !gap_refs.is_empty() {
        return Ok(Err(UnavailableState {
            reason: UnavailableReason::Gap,
            gap_refs,
        }));
    }
    if request.backends.contains(&Backend::Rfq) && restored.rfq_observed_at_ms.is_none() {
        return Ok(Err(unavailable(UnavailableReason::RfqObservationMissing)));
    }
    let observation = plan.block_times.get(&block).ok_or_else(|| {
        HistoricalError::HistoricalDataInvalid(format!("block {block} lost its planned metadata"))
    })?;
    let block_hash = observation.block_hash.clone().ok_or_else(|| {
        HistoricalError::HistoricalDataInvalid(format!("block {block} has no canonical hash"))
    })?;
    let (rfq_observation_cursor, rfq_observed_at, rfq_observation_age_ms) =
        if request.backends.contains(&Backend::Rfq) {
            let (observed_at, observation_age_ms) = rfq_provenance(restored, next_block_timestamp)?;
            (
                restored.rfq_observation_position.map(public_position),
                observed_at,
                observation_age_ms,
            )
        } else {
            (None, None, None)
        };
    Ok(Ok(ReconstructedState {
        point: restored.world.pin(),
        provenance: StateProvenance {
            block_hash,
            final_position: public_position(restored.final_position),
            anchor_checkpoint_position: public_position(restored.anchor_checkpoint_position),
            token_snapshot_digest: restored.token_snapshot_digest.clone(),
            rfq_observation_cursor,
            rfq_observed_at,
            rfq_observation_age_ms,
        },
    }))
}

fn unavailable(reason: UnavailableReason) -> UnavailableState {
    UnavailableState {
        reason,
        gap_refs: Vec::new(),
    }
}

fn early_unavailable_states(
    request: &ReconstructionRequest,
    plan: &TargetPlan,
) -> BTreeMap<u64, Result<ReconstructedState, UnavailableState>> {
    let mut states = BTreeMap::new();
    for block in &request.required_blocks {
        let reason = if *block > plan.visible_through_block {
            Some(UnavailableReason::ReplicaCutoff)
        } else if plan.missing_block_times.contains(block)
            || request.backends.contains(&Backend::Rfq)
                && block
                    .checked_add(1)
                    .is_none_or(|next| plan.missing_block_times.contains(&next))
        {
            Some(UnavailableReason::BlockBoundaryMissing)
        } else if plan
            .lineage_invalid_intervals
            .iter()
            .any(|interval| interval_contains(*interval, *block))
        {
            Some(UnavailableReason::CanonicalLineageInvalid)
        } else {
            None
        };
        if let Some(reason) = reason {
            states.insert(
                *block,
                Err(UnavailableState {
                    reason,
                    gap_refs: Vec::new(),
                }),
            );
        }
    }
    states
}

fn leg_for_block(plan: &TargetPlan, backends: &BTreeSet<Backend>, block: u64) -> Option<usize> {
    let next_timestamp = block
        .checked_add(1)
        .and_then(|next| plan.block_times.get(&next))
        .map(|observation| observation.timestamp_ms);
    plan.legs
        .iter()
        .enumerate()
        .filter(|(_, leg)| {
            backends.iter().all(|backend| match backend {
                Backend::Native | Backend::Vm => leg.checkpoint.block_number <= block,
                Backend::Rfq => leg
                    .checkpoint
                    .rfq_observed_at_ms
                    .zip(next_timestamp)
                    .is_some_and(|(observed, boundary)| observed < boundary),
            })
        })
        .max_by_key(|(_, leg)| leg.checkpoint.position)
        .map(|(index, _)| index)
}

fn state_gap_refs(
    gaps: &[RangeGap],
    restored: &RestoredWorld,
    block: u64,
    next_block_timestamp: Option<u64>,
) -> Vec<String> {
    gaps.iter()
        .enumerate()
        .filter(|(_, gap)| {
            gap_affects_interval(
                gap,
                restored.anchor_checkpoint_position,
                restored.final_position,
                restored.anchor_block,
                block,
                restored.anchor_rfq_observed_at_ms,
                next_block_timestamp,
            )
        })
        .map(|(index, _)| format!("gap-{}", index + 1))
        .collect()
}

pub(crate) fn gap_affects_interval(
    gap: &RangeGap,
    start_position: HistoryPosition,
    end_position: HistoryPosition,
    start_block: u64,
    end_block: u64,
    start_observed_at_ms: Option<u64>,
    end_observed_at_ms: Option<u64>,
) -> bool {
    let position_overlap = gap
        .from_position
        .zip(gap.to_position)
        .is_some_and(|(from, to)| to > start_position && from <= end_position);
    let block_overlap = gap
        .from_block
        .zip(gap.to_block_inclusive)
        .is_some_and(|(from, to)| to > start_block && from <= end_block);
    let time_overlap = gap
        .from_observed_at_ms
        .zip(gap.to_observed_at_ms)
        .zip(start_observed_at_ms.zip(end_observed_at_ms))
        .is_some_and(|((from, to), (start, end))| to > start && from < end);
    let no_public_bounds = gap.from_block.is_none()
        && gap.to_block_inclusive.is_none()
        && gap.from_observed_at_ms.is_none()
        && gap.to_observed_at_ms.is_none();
    position_overlap
        || block_overlap
        || time_overlap
        || no_public_bounds
            && gap
                .from_position
                .is_some_and(|from| from > start_position && from <= end_position)
}

fn matching_gap_refs(gaps: &[RangeGap], block: u64) -> Vec<String> {
    gaps.iter()
        .enumerate()
        .filter(|(_, gap)| {
            gap.from_block
                .zip(gap.to_block_inclusive)
                .is_some_and(|(from, to)| from <= block && block <= to)
                || gap.kind == RangeGapKind::MissingCheckpoint
        })
        .map(|(index, _)| format!("gap-{}", index + 1))
        .collect()
}

fn public_gap(index: usize, gap: &RangeGap) -> Result<GapReference, HistoricalError> {
    Ok(GapReference {
        reference: format!("gap-{}", index + 1),
        kind: match gap.kind {
            RangeGapKind::MissingCheckpoint => GapKind::MissingCheckpoint,
            RangeGapKind::Recorded => GapKind::Recorded,
            RangeGapKind::IncompleteTail => GapKind::IncompleteTail,
        },
        cause: (gap.kind == RangeGapKind::Recorded)
            .then(|| gap_cause(&gap.reason))
            .transpose()?,
        from_position: gap.from_position.map(public_position),
        to_position: gap.to_position.map(public_position),
        from_block: gap.from_block,
        to_block_inclusive: gap.to_block_inclusive,
        from_observed_at_ms: gap.from_observed_at_ms,
        to_observed_at_ms: gap.to_observed_at_ms,
    })
}

fn gap_cause(reason: &str) -> Result<GapCause, HistoricalError> {
    match reason {
        "queue_overflow" => Ok(GapCause::QueueOverflow),
        "write_failed" => Ok(GapCause::WriteFailed),
        "writer_stopped" => Ok(GapCause::WriterStopped),
        "checkpoint_failed" => Ok(GapCause::CheckpointFailed),
        "boundary_slip" => Ok(GapCause::BoundarySlip),
        other => Err(HistoricalError::HistoricalDataInvalid(format!(
            "unknown recorded gap cause {other}"
        ))),
    }
}

fn rfq_provenance(
    restored: &RestoredWorld,
    next_block_timestamp: Option<u64>,
) -> Result<(Option<DateTime<Utc>>, Option<u64>), HistoricalError> {
    let Some(observed_at_ms) = restored.rfq_observed_at_ms else {
        return Ok((None, None));
    };
    let observed_at =
        DateTime::from_timestamp_millis(i64::try_from(observed_at_ms).map_err(|_| {
            HistoricalError::HistoricalDataInvalid(
                "RFQ observation timestamp exceeds RFC 3339 range".to_owned(),
            )
        })?)
        .ok_or_else(|| {
            HistoricalError::HistoricalDataInvalid(
                "RFQ observation timestamp exceeds RFC 3339 range".to_owned(),
            )
        })?;
    let age = next_block_timestamp
        .and_then(|boundary| boundary.checked_sub(observed_at_ms))
        .ok_or_else(|| {
            HistoricalError::HistoricalDataInvalid(
                "RFQ observation is not before the next block boundary".to_owned(),
            )
        })?;
    Ok((Some(observed_at), Some(age)))
}

pub(crate) fn history_backend(backend: Backend) -> HistoryBackend {
    match backend {
        Backend::Native => HistoryBackend::Native,
        Backend::Vm => HistoryBackend::Vm,
        Backend::Rfq => HistoryBackend::Rfq,
    }
}

pub(crate) fn public_backend(backend: HistoryBackend) -> Backend {
    match backend {
        HistoryBackend::Native => Backend::Native,
        HistoryBackend::Vm => Backend::Vm,
        HistoryBackend::Rfq => Backend::Rfq,
    }
}

pub(crate) fn replay_backend(backend: Backend) -> ReplayBackend {
    match backend {
        Backend::Native => ReplayBackend::Native,
        Backend::Vm => ReplayBackend::Vm,
        Backend::Rfq => ReplayBackend::Rfq,
    }
}

pub(crate) fn public_position(position: HistoryPosition) -> StreamPosition {
    StreamPosition {
        generation: position.generation,
        message_seq: position.message_seq,
    }
}

pub(crate) fn history_position(position: StreamPosition) -> HistoryPosition {
    HistoryPosition {
        generation: position.generation,
        message_seq: position.message_seq,
    }
}

fn interval_contains(interval: BlockInterval, block: u64) -> bool {
    interval.start <= block && block <= interval.end_inclusive
}

pub(crate) fn check_cancelled(cancellation: &CancellationToken) -> Result<(), HistoricalError> {
    if cancellation.is_cancelled() {
        Err(HistoricalError::Cancelled)
    } else {
        Ok(())
    }
}
