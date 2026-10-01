//! The raw snapshot at a stream position, rebuilt from the last checkpoint
//! before it and the stored updates up to it, with the broadcaster's own merge
//! rules.

use std::collections::BTreeMap;

use anyhow::{anyhow, bail, ensure, Context};
use simulator_core::broadcaster::{
    apply_raw_protocol_messages, parse_snapshot_payloads, BroadcasterBackend, BroadcasterEnvelope,
    BroadcasterPayload, BroadcasterProtocolSyncStatus, BroadcasterSnapshotPartition,
    RawSnapshotReassembly,
};
use thiserror::Error;

use super::{
    begin_range_transaction, checkpoints::segment_boundary_position, database_backends,
    database_i64, manifest_from_row, PositionRangeQuery, RangeGap, ReadConnectionProvider,
    ReadLimits, StateHistoryReader, StoredDelta,
};
use crate::{Backend, CheckpointArchive, CheckpointManifest, StreamPosition};

/// The snapshot of one chain at `position`, for backends kept as raw messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawSnapshotQuery {
    pub chain_id: u64,
    pub position: StreamPosition,
    pub backends: Vec<Backend>,
}

impl RawSnapshotQuery {
    pub fn new(chain_id: u64, position: StreamPosition, mut backends: Vec<Backend>) -> Self {
        backends.sort_unstable();
        backends.dedup();
        Self {
            chain_id,
            position,
            backends,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RawSnapshot {
    pub position: StreamPosition,
    /// The checkpoint the rebuild started from.
    pub checkpoint: CheckpointManifest,
    pub partitions: Vec<BroadcasterSnapshotPartition>,
}

#[derive(Debug, Error)]
pub enum RawSnapshotError {
    #[error("backend {} is not kept as raw messages", .0.as_str())]
    NotRaw(Backend),
    #[error(
        "no complete checkpoint with the requested backends at or before {0:?} in its segment"
    )]
    NoCheckpoint(StreamPosition),
    /// The writer has not stored through the position, with no promise that it will.
    #[error("history is stored through {last_stored:?}, not yet through {position:?}")]
    NotYetStored {
        position: StreamPosition,
        last_stored: Option<StreamPosition>,
    },
    #[error("{} recorded gaps lie between the checkpoint and {position:?}", .gaps.len())]
    Gaps {
        position: StreamPosition,
        gaps: Vec<RangeGap>,
    },
    #[error("{} boundary checkpoints lie between the checkpoint and {position:?}", .boundaries.len())]
    Boundaries {
        position: StreamPosition,
        boundaries: Vec<CheckpointManifest>,
    },
    #[error(transparent)]
    Read(#[from] anyhow::Error),
}

impl<P: ReadConnectionProvider> StateHistoryReader<P> {
    /// Rebuilds the raw state from the history visible now. Success does not prove
    /// the history is complete. A gap or a recovery boundary stored late or never
    /// can make the result differ from the snapshot the broadcaster served.
    pub async fn read_raw_snapshot(
        &self,
        query: &RawSnapshotQuery,
        limits: ReadLimits,
    ) -> Result<RawSnapshot, RawSnapshotError> {
        if query.backends.is_empty() {
            return Err(anyhow!("raw snapshot backends must not be empty").into());
        }
        let backends = query
            .backends
            .iter()
            .map(|backend| raw_backend(*backend).ok_or(RawSnapshotError::NotRaw(*backend)))
            .collect::<Result<Vec<_>, _>>()?;
        let checkpoint = self
            .checkpoint_before(query)
            .await?
            .ok_or(RawSnapshotError::NoCheckpoint(query.position))?;
        let archive = self
            .fetch_checkpoint_with_limit(&checkpoint, limits)
            .await
            .context("failed to fetch the raw snapshot checkpoint")?;
        let deltas = if checkpoint.position == query.position {
            Vec::new()
        } else {
            self.stored_updates_after(&checkpoint, query, remaining(limits, &checkpoint))
                .await?
        };
        let partitions =
            tokio::task::spawn_blocking(move || rebuild_partitions(&archive, &deltas, &backends))
                .await
                .context("raw snapshot rebuild task failed")??;
        Ok(RawSnapshot {
            position: query.position,
            checkpoint,
            partitions,
        })
    }

    /// The latest complete checkpoint with the query's backends at or before its
    /// position, when it lies in the position's segment.
    async fn checkpoint_before(
        &self,
        query: &RawSnapshotQuery,
    ) -> anyhow::Result<Option<CheckpointManifest>> {
        let mut connection = self
            .connections
            .acquire()
            .await
            .context("failed to acquire raw snapshot connection")?;
        let mut transaction = begin_range_transaction(&mut connection).await?;
        let row = sqlx::query(
            "SELECT id, chain_id, generation, message_seq, state_version, kind, block_number,
                    rfq_observed_at_ms, backends, s3_key, archive_sha256, archive_bytes,
                    compressed_bytes, token_s3_key, token_sha256, token_count, token_bytes,
                    status, error
             FROM state_history.checkpoints
             WHERE chain_id = $1
               AND status = 'complete'
               AND (generation, message_seq) <= ($2, $3)
               AND backends @> $4::text[]
             ORDER BY generation DESC, message_seq DESC, kind
             LIMIT 1",
        )
        .bind(database_i64(query.chain_id, "raw snapshot chain_id")?)
        .bind(database_i64(
            query.position.generation,
            "raw snapshot generation",
        )?)
        .bind(database_i64(
            query.position.message_seq,
            "raw snapshot message_seq",
        )?)
        .bind(database_backends(&query.backends))
        .fetch_optional(&mut *transaction)
        .await
        .context("failed to select the raw snapshot checkpoint")?;
        let checkpoint = row.as_ref().map(manifest_from_row).transpose()?;
        let segment_start =
            segment_boundary_position(&mut transaction, query.chain_id, query.position).await?;
        transaction
            .commit()
            .await
            .context("failed to commit raw snapshot checkpoint transaction")?;
        Ok(checkpoint.filter(|checkpoint| {
            segment_start.is_some_and(|segment_start| checkpoint.position >= segment_start)
        }))
    }

    async fn stored_updates_after(
        &self,
        checkpoint: &CheckpointManifest,
        query: &RawSnapshotQuery,
        limits: ReadLimits,
    ) -> Result<Vec<StoredDelta>, RawSnapshotError> {
        let range = self
            .read_position_range(
                &PositionRangeQuery::new(
                    query.chain_id,
                    checkpoint.position,
                    query.position,
                    query.backends.clone(),
                ),
                limits,
            )
            .await?;
        let gaps = range
            .gaps
            .into_iter()
            .filter(|gap| !is_rfq_only(gap))
            .collect::<Vec<_>>();
        if !gaps.is_empty() {
            return Err(RawSnapshotError::Gaps {
                position: query.position,
                gaps,
            });
        }
        if !range.boundaries.is_empty() {
            return Err(RawSnapshotError::Boundaries {
                position: query.position,
                boundaries: range.boundaries,
            });
        }
        if range
            .last_stored
            .is_none_or(|last_stored| last_stored < query.position)
        {
            return Err(RawSnapshotError::NotYetStored {
                position: query.position,
                last_stored: range.last_stored,
            });
        }
        Ok(range.deltas)
    }
}

/// A gap with RFQ time bounds and no block bounds lost only RFQ updates, the same
/// rule coverage applies to a block backend.
fn is_rfq_only(gap: &RangeGap) -> bool {
    gap.from_block.is_none()
        && gap.to_block_inclusive.is_none()
        && gap
            .from_observed_at_ms
            .zip(gap.to_observed_at_ms)
            .is_some_and(|(from, to)| from <= to)
}

fn raw_backend(backend: Backend) -> Option<BroadcasterBackend> {
    match backend {
        Backend::Native => Some(BroadcasterBackend::Native),
        Backend::Vm => Some(BroadcasterBackend::Vm),
        Backend::Rfq => None,
    }
}

/// What `limits` leaves for the updates once the checkpoint took its declared bytes.
fn remaining(limits: ReadLimits, checkpoint: &CheckpointManifest) -> ReadLimits {
    ReadLimits {
        max_compressed_bytes: limits
            .max_compressed_bytes
            .saturating_sub(checkpoint.compressed_bytes.unwrap_or_default()),
        max_decoded_bytes: limits
            .max_decoded_bytes
            .saturating_sub(checkpoint.archive_bytes.unwrap_or_default()),
    }
}

#[derive(Default)]
struct RawPartition {
    block_number: Option<u64>,
    sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
    reassembly: RawSnapshotReassembly,
}

fn rebuild_partitions(
    archive: &CheckpointArchive,
    deltas: &[StoredDelta],
    backends: &[BroadcasterBackend],
) -> anyhow::Result<Vec<BroadcasterSnapshotPartition>> {
    let (start, chunks) = parse_snapshot_payloads(&archive.payloads_json)?;
    for backend in backends {
        ensure!(
            start.backends.contains(backend),
            "checkpoint snapshot has no {backend} partition"
        );
    }
    let mut partitions = backends
        .iter()
        .map(|backend| (*backend, RawPartition::default()))
        .collect::<BTreeMap<_, _>>();
    for chunk in chunks.into_values() {
        for partition in chunk.partitions {
            let Some(raw) = partitions.get_mut(&partition.backend) else {
                continue;
            };
            ensure!(
                partition.states.is_empty(),
                "checkpoint {} partition carries decoded states",
                partition.backend
            );
            raw.block_number = Some(partition.block_number);
            raw.sync_statuses.extend(partition.sync_statuses);
            for message in partition.messages {
                raw.reassembly.push(message)?;
            }
        }
    }
    let mut partitions = partitions
        .into_iter()
        .map(|(backend, mut raw)| {
            let block_number = raw
                .block_number
                .with_context(|| format!("checkpoint snapshot has no {backend} partition"))?;
            Ok(BroadcasterSnapshotPartition::with_messages(
                backend,
                block_number,
                raw.reassembly.take_messages(),
                raw.sync_statuses,
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    for delta in deltas {
        let envelope: BroadcasterEnvelope = serde_json::from_str(delta.raw_payload.get())
            .with_context(|| format!("stored delta {:?} is not an envelope", delta.position))?;
        let BroadcasterPayload::Update(update) = envelope.payload else {
            bail!("stored delta {:?} is not an update", delta.position);
        };
        for incoming in update.partitions {
            let Some(partition) = partitions
                .iter_mut()
                .find(|partition| partition.backend == incoming.backend)
            else {
                continue;
            };
            ensure!(
                incoming.new_pairs.is_empty()
                    && incoming.updated_states.is_empty()
                    && incoming.removed_pairs.is_empty(),
                "stored delta {:?} carries decoded state for the {} partition",
                delta.position,
                incoming.backend
            );
            partition.block_number = incoming.block_number;
            partition.sync_statuses = incoming.sync_statuses;
            apply_raw_protocol_messages(&mut partition.messages, &incoming.messages).with_context(
                || {
                    format!(
                        "stored delta {:?} does not apply to the {} partition",
                        delta.position, incoming.backend
                    )
                },
            )?;
        }
    }
    Ok(partitions)
}
