use anyhow::{ensure, Context};
use futures::TryStreamExt;
use sqlx::Row;

use super::{
    begin_range_transaction, database_backends, database_i64, database_u64,
    decode_delta_row_with_limit, encoded_delta_from_row, fetch_delta_backends, gap_from_row,
    manifest_from_row, BlockInterval, RangeGap, RangeGapKind, ReadConnectionProvider, ReadLimits,
    StateHistoryReader, StoredDelta,
};
use crate::{Backend, CheckpointKind, CheckpointManifest, CheckpointStatus, StreamPosition};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenAnchor {
    Available {
        checkpoint: Box<CheckpointManifest>,
        used_fallback: bool,
    },
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExplicitCheckpointPair {
    pub earlier_position: StreamPosition,
    pub target_position: StreamPosition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointPairSelection {
    BlockRange(BlockInterval),
    Explicit(Vec<ExplicitCheckpointPair>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointPairQuery {
    pub chain_id: u64,
    pub selection: CheckpointPairSelection,
    pub backends: Vec<Backend>,
    pub max_pairs: Option<usize>,
}

impl CheckpointPairQuery {
    pub const fn new(chain_id: u64, selection: CheckpointPairSelection) -> Self {
        Self {
            chain_id,
            selection,
            backends: Vec::new(),
            max_pairs: None,
        }
    }

    pub fn for_backends(
        chain_id: u64,
        selection: CheckpointPairSelection,
        mut backends: Vec<Backend>,
    ) -> Self {
        backends.sort_unstable();
        backends.dedup();
        Self {
            chain_id,
            selection,
            backends,
            max_pairs: None,
        }
    }

    #[must_use]
    pub const fn with_max_pairs(mut self, max_pairs: usize) -> Self {
        self.max_pairs = Some(max_pairs);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointPair {
    pub earlier: CheckpointManifest,
    pub target: CheckpointManifest,
}

#[derive(Debug, Clone)]
pub struct CheckpointPairReplayPlan {
    pub pair: CheckpointPair,
    pub deltas: Vec<StoredDelta>,
    pub gaps: Vec<RangeGap>,
    pub estimated_decoded_bytes: u64,
}

impl<P: ReadConnectionProvider> StateHistoryReader<P> {
    pub async fn select_token_anchor(
        &self,
        selected: &CheckpointManifest,
    ) -> anyhow::Result<TokenAnchor> {
        ensure!(
            selected.status == CheckpointStatus::Complete,
            "token anchor selection requires a complete checkpoint"
        );
        let mut connection = self
            .connections
            .acquire()
            .await
            .context("failed to acquire token anchor connection")?;
        let mut transaction = begin_range_transaction(&mut connection).await?;
        let stored = exact_checkpoint(&mut transaction, selected.chain_id, selected.position)
            .await?
            .context("selected checkpoint does not exist as a complete retained checkpoint")?;
        ensure!(
            stored.id == selected.id,
            "selected checkpoint does not match the retained checkpoint at its position"
        );

        let anchor = if stored.token_reference.is_some() {
            TokenAnchor::Available {
                checkpoint: Box::new(stored),
                used_fallback: false,
            }
        } else {
            match token_fallback_in_segment(&mut transaction, &stored).await? {
                Some(checkpoint) => TokenAnchor::Available {
                    checkpoint: Box::new(checkpoint),
                    used_fallback: true,
                },
                None => TokenAnchor::Missing,
            }
        };
        transaction
            .commit()
            .await
            .context("failed to commit token anchor transaction")?;
        Ok(anchor)
    }

    pub async fn resolve_checkpoint_pairs(
        &self,
        query: &CheckpointPairQuery,
    ) -> anyhow::Result<Vec<CheckpointPair>> {
        let mut connection = self
            .connections
            .acquire()
            .await
            .context("failed to acquire checkpoint pair connection")?;
        let mut transaction = begin_range_transaction(&mut connection).await?;
        let pairs = match &query.selection {
            CheckpointPairSelection::BlockRange(bounds) => {
                range_checkpoint_pairs(
                    &mut transaction,
                    query.chain_id,
                    *bounds,
                    &query.backends,
                    query.max_pairs,
                )
                .await?
            }
            CheckpointPairSelection::Explicit(selections) => {
                explicit_checkpoint_pairs(
                    &mut transaction,
                    query.chain_id,
                    selections,
                    &query.backends,
                    query.max_pairs,
                )
                .await?
            }
        };
        transaction
            .commit()
            .await
            .context("failed to commit checkpoint pair transaction")?;
        Ok(pairs)
    }

    pub async fn plan_checkpoint_pair(
        &self,
        pair: &CheckpointPair,
        mut backends: Vec<Backend>,
        limits: ReadLimits,
    ) -> anyhow::Result<CheckpointPairReplayPlan> {
        ensure!(
            !backends.is_empty(),
            "checkpoint pair backends must not be empty"
        );
        ensure!(
            pair.earlier.chain_id == pair.target.chain_id,
            "checkpoint pair chain ids differ"
        );
        ensure!(
            pair.earlier.position < pair.target.position,
            "checkpoint pair must be ordered"
        );
        backends.sort_unstable();
        backends.dedup();

        let mut connection = self
            .connections
            .acquire()
            .await
            .context("failed to acquire checkpoint pair plan connection")?;
        let mut transaction = begin_range_transaction(&mut connection).await?;
        let earlier = exact_checkpoint(
            &mut transaction,
            pair.earlier.chain_id,
            pair.earlier.position,
        )
        .await?
        .context("earlier checkpoint is not retained as complete")?;
        let target = exact_checkpoint(&mut transaction, pair.target.chain_id, pair.target.position)
            .await?
            .context("target checkpoint is not retained as complete")?;
        ensure!(
            earlier.id == pair.earlier.id && target.id == pair.target.id,
            "checkpoint pair does not match retained checkpoint identity"
        );
        let segment_start =
            segment_boundary_position(&mut transaction, target.chain_id, target.position)
                .await?
                .context("target checkpoint has no complete segment boundary")?;
        ensure!(
            earlier.position >= segment_start,
            "checkpoint pair must stay in the same segment"
        );

        let (checkpoint_compressed_bytes, checkpoint_decoded_bytes) =
            declared_pair_checkpoint_bytes(&earlier, &target)?;
        limits.check_compressed(checkpoint_compressed_bytes)?;
        limits.check_decoded(checkpoint_decoded_bytes)?;
        let remaining_compressed_bytes = limits
            .max_compressed_bytes
            .saturating_sub(checkpoint_compressed_bytes);
        let remaining_decoded_bytes = limits
            .max_decoded_bytes
            .saturating_sub(checkpoint_decoded_bytes);
        let encoded_rows = fetch_pair_delta_rows(
            &mut transaction,
            &earlier,
            &target,
            &backends,
            remaining_compressed_bytes,
        )
        .await?;
        let delta_ids = encoded_rows.iter().map(|row| row.id).collect::<Vec<_>>();
        let backend_cursors = fetch_delta_backends(&mut transaction, &delta_ids).await?;
        let mut delta_decoded_bytes = 0_u64;
        let mut deltas = Vec::with_capacity(encoded_rows.len());
        for row in encoded_rows {
            let remaining = remaining_decoded_bytes.saturating_sub(delta_decoded_bytes);
            let (row, decoded_bytes) =
                decode_delta_row_with_limit(row, &backend_cursors, remaining)?;
            delta_decoded_bytes = delta_decoded_bytes
                .checked_add(decoded_bytes)
                .context("checkpoint pair decoded byte estimate overflowed")?;
            deltas.push(StoredDelta {
                position: row.position,
                observed_at_ms: row.observed_at_ms,
                payload_format_version: row.payload_format_version,
                raw_payload: row.payload,
                applicable_backends: row
                    .backends
                    .into_iter()
                    .filter(|cursor| backends.binary_search(&cursor.backend).is_ok())
                    .collect(),
            });
        }
        let gaps = fetch_pair_gaps(&mut transaction, &earlier, &target).await?;
        transaction
            .commit()
            .await
            .context("failed to commit checkpoint pair plan transaction")?;
        Ok(CheckpointPairReplayPlan {
            pair: CheckpointPair { earlier, target },
            deltas,
            gaps,
            estimated_decoded_bytes: checkpoint_decoded_bytes
                .checked_add(delta_decoded_bytes)
                .context("checkpoint pair decoded byte estimate overflowed")?,
        })
    }
}

fn declared_pair_checkpoint_bytes(
    earlier: &CheckpointManifest,
    target: &CheckpointManifest,
) -> anyhow::Result<(u64, u64)> {
    let mut compressed_bytes = 0_u64;
    let mut decoded_bytes = 0_u64;
    let mut token_digests = std::collections::BTreeSet::new();
    for manifest in [earlier, target] {
        compressed_bytes = compressed_bytes
            .checked_add(
                manifest
                    .compressed_bytes
                    .context("complete checkpoint is missing its compressed byte length")?,
            )
            .context("checkpoint pair compressed byte estimate overflowed")?;
        decoded_bytes = decoded_bytes
            .checked_add(
                manifest
                    .archive_bytes
                    .context("complete checkpoint is missing its archive byte length")?,
            )
            .context("checkpoint pair decoded byte estimate overflowed")?;
        if let Some(reference) = manifest
            .token_reference
            .as_ref()
            .filter(|reference| token_digests.insert(reference.sha256.clone()))
        {
            decoded_bytes = decoded_bytes
                .checked_add(reference.token_bytes)
                .context("checkpoint pair token byte estimate overflowed")?;
        }
    }
    Ok((compressed_bytes, decoded_bytes))
}

async fn fetch_pair_delta_rows(
    connection: &mut sqlx::PgConnection,
    earlier: &CheckpointManifest,
    target: &CheckpointManifest,
    backends: &[Backend],
    max_compressed_bytes: u64,
) -> anyhow::Result<Vec<super::EncodedDeltaRow>> {
    let compressed_bytes: i64 = sqlx::query_scalar(
        "SELECT COALESCE(sum(octet_length(d.payload)), 0)::bigint
         FROM state_history.deltas d
         WHERE d.chain_id = $1
           AND (d.generation, d.message_seq) > ($2, $3)
           AND (d.generation, d.message_seq) <= ($4, $5)
           AND EXISTS (
               SELECT 1 FROM state_history.delta_backends matched
               WHERE matched.delta_id = d.id AND matched.backend = ANY($6::text[])
           )",
    )
    .bind(database_i64(earlier.chain_id, "checkpoint pair chain_id")?)
    .bind(database_i64(
        earlier.position.generation,
        "earlier generation",
    )?)
    .bind(database_i64(
        earlier.position.message_seq,
        "earlier message_seq",
    )?)
    .bind(database_i64(
        target.position.generation,
        "target generation",
    )?)
    .bind(database_i64(
        target.position.message_seq,
        "target message_seq",
    )?)
    .bind(database_backends(backends))
    .fetch_one(&mut *connection)
    .await
    .context("failed to preflight checkpoint pair delta bytes")?;
    let compressed_bytes = database_u64(compressed_bytes, "checkpoint pair delta bytes")?;
    ensure!(
        compressed_bytes <= max_compressed_bytes,
        "compressed bytes {compressed_bytes} exceed limit {max_compressed_bytes}"
    );

    let rows = sqlx::query(
        "SELECT d.id, d.generation, d.message_seq, d.observed_at_ms,
                d.payload_format_version, d.payload, d.payload_sha256
         FROM state_history.deltas d
         WHERE d.chain_id = $1
           AND (d.generation, d.message_seq) > ($2, $3)
           AND (d.generation, d.message_seq) <= ($4, $5)
           AND EXISTS (
               SELECT 1 FROM state_history.delta_backends matched
               WHERE matched.delta_id = d.id AND matched.backend = ANY($6::text[])
           )
         ORDER BY d.generation, d.message_seq",
    )
    .bind(database_i64(earlier.chain_id, "checkpoint pair chain_id")?)
    .bind(database_i64(
        earlier.position.generation,
        "earlier generation",
    )?)
    .bind(database_i64(
        earlier.position.message_seq,
        "earlier message_seq",
    )?)
    .bind(database_i64(
        target.position.generation,
        "target generation",
    )?)
    .bind(database_i64(
        target.position.message_seq,
        "target message_seq",
    )?)
    .bind(database_backends(backends))
    .fetch_all(connection)
    .await
    .context("failed to select checkpoint pair deltas")?;
    rows.iter().map(encoded_delta_from_row).collect()
}

async fn fetch_pair_gaps(
    connection: &mut sqlx::PgConnection,
    earlier: &CheckpointManifest,
    target: &CheckpointManifest,
) -> anyhow::Result<Vec<RangeGap>> {
    let rows = sqlx::query(
        "SELECT generation, from_message_seq, to_message_seq, reason,
                from_block_number, to_block_number, from_observed_at_ms, to_observed_at_ms
         FROM state_history.gaps
         WHERE chain_id = $1
           AND (generation > $2 OR (generation = $2 AND to_message_seq > $3))
           AND (generation < $4 OR (generation = $4 AND from_message_seq <= $5))
         ORDER BY generation, from_message_seq, to_message_seq, id",
    )
    .bind(database_i64(earlier.chain_id, "checkpoint pair chain_id")?)
    .bind(database_i64(
        earlier.position.generation,
        "earlier generation",
    )?)
    .bind(database_i64(
        earlier.position.message_seq,
        "earlier message_seq",
    )?)
    .bind(database_i64(
        target.position.generation,
        "target generation",
    )?)
    .bind(database_i64(
        target.position.message_seq,
        "target message_seq",
    )?)
    .fetch_all(connection)
    .await
    .context("failed to select checkpoint pair gaps")?;
    rows.iter()
        .map(|row| {
            let gap = gap_from_row(row)?;
            Ok(RangeGap {
                kind: RangeGapKind::Recorded,
                generation: Some(gap.generation),
                from_message_seq: Some(gap.from_message_seq),
                to_message_seq: Some(gap.to_message_seq),
                from_position: Some(StreamPosition {
                    generation: gap.generation,
                    message_seq: gap.from_message_seq,
                }),
                to_position: Some(StreamPosition {
                    generation: gap.generation,
                    message_seq: gap.to_message_seq,
                }),
                from_block: gap.from_block_number,
                to_block_inclusive: gap.to_block_number,
                from_observed_at_ms: gap.from_observed_at_ms,
                to_observed_at_ms: gap.to_observed_at_ms,
                reason: gap.reason,
            })
        })
        .collect()
}

pub(super) async fn token_fallback_in_segment(
    connection: &mut sqlx::PgConnection,
    selected: &CheckpointManifest,
) -> anyhow::Result<Option<CheckpointManifest>> {
    let boundary = sqlx::query(
        "SELECT id, chain_id, generation, message_seq, state_version, kind, block_number,
                rfq_observed_at_ms, backends, s3_key, archive_sha256, archive_bytes,
                compressed_bytes, token_s3_key, token_sha256, token_count, token_bytes,
                status, error
         FROM state_history.checkpoints
         WHERE chain_id = $1 AND status = 'complete' AND kind = 'boundary'
           AND (generation, message_seq) <= ($2, $3)
         ORDER BY generation DESC, message_seq DESC
         LIMIT 1",
    )
    .bind(database_i64(selected.chain_id, "token anchor chain_id")?)
    .bind(database_i64(
        selected.position.generation,
        "token anchor generation",
    )?)
    .bind(database_i64(
        selected.position.message_seq,
        "token anchor message_seq",
    )?)
    .fetch_optional(&mut *connection)
    .await
    .context("failed to select token anchor segment boundary")?
    .as_ref()
    .map(manifest_from_row)
    .transpose()?;
    let Some(boundary) = boundary else {
        return Ok(None);
    };

    let row = sqlx::query(
        "SELECT id, chain_id, generation, message_seq, state_version, kind, block_number,
                rfq_observed_at_ms, backends, s3_key, archive_sha256, archive_bytes,
                compressed_bytes, token_s3_key, token_sha256, token_count, token_bytes,
                status, error
         FROM state_history.checkpoints
         WHERE chain_id = $1 AND status = 'complete' AND token_s3_key IS NOT NULL
           AND (generation, message_seq) >= ($2, $3)
           AND (generation, message_seq) < ($4, $5)
         ORDER BY generation DESC, message_seq DESC
         LIMIT 1",
    )
    .bind(database_i64(selected.chain_id, "token anchor chain_id")?)
    .bind(database_i64(
        boundary.position.generation,
        "token segment generation",
    )?)
    .bind(database_i64(
        boundary.position.message_seq,
        "token segment message_seq",
    )?)
    .bind(database_i64(
        selected.position.generation,
        "selected generation",
    )?)
    .bind(database_i64(
        selected.position.message_seq,
        "selected message_seq",
    )?)
    .fetch_optional(connection)
    .await
    .context("failed to select same-segment token fallback")?;
    row.as_ref().map(manifest_from_row).transpose()
}

async fn range_checkpoint_pairs(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    bounds: BlockInterval,
    backends: &[Backend],
    max_pairs: Option<usize>,
) -> anyhow::Result<Vec<CheckpointPair>> {
    let mut rows = sqlx::query(
        "SELECT id, chain_id, generation, message_seq, state_version, kind, block_number,
                rfq_observed_at_ms, backends, s3_key, archive_sha256, archive_bytes,
                compressed_bytes, token_s3_key, token_sha256, token_count, token_bytes,
                status, error
         FROM state_history.checkpoints
         WHERE chain_id = $1 AND status = 'complete'
           AND block_number BETWEEN $2 AND $3
         ORDER BY generation, message_seq, kind",
    )
    .bind(database_i64(chain_id, "checkpoint pair chain_id")?)
    .bind(database_i64(bounds.start, "checkpoint pair range start")?)
    .bind(database_i64(
        bounds.end_inclusive,
        "checkpoint pair range end",
    )?)
    .fetch(&mut *connection);
    let sentinel_limit = max_pairs.and_then(|limit| limit.checked_add(1));
    let mut pairs = Vec::new();
    let mut previous: Option<CheckpointManifest> = None;
    let mut previous_position = None;
    while let Some(row) = rows
        .try_next()
        .await
        .context("failed to select checkpoint pair range")?
    {
        let manifest = manifest_from_row(&row)?;
        ensure!(
            previous_position != Some(manifest.position),
            "checkpoint position is ambiguous because multiple complete kinds exist"
        );
        previous_position = Some(manifest.position);
        if manifest.kind == CheckpointKind::Boundary {
            previous = checkpoint_supports(&manifest, backends).then_some(manifest);
            continue;
        }
        if !checkpoint_supports(&manifest, backends) {
            continue;
        }
        if let Some(earlier) = previous.replace(manifest.clone()) {
            pairs.push(CheckpointPair {
                earlier,
                target: manifest,
            });
            if sentinel_limit == Some(pairs.len()) {
                break;
            }
        }
    }
    Ok(pairs)
}

async fn explicit_checkpoint_pairs(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    selections: &[ExplicitCheckpointPair],
    backends: &[Backend],
    max_pairs: Option<usize>,
) -> anyhow::Result<Vec<CheckpointPair>> {
    let sentinel_limit = max_pairs
        .and_then(|limit| limit.checked_add(1))
        .unwrap_or(usize::MAX);
    let mut pairs = Vec::with_capacity(selections.len().min(sentinel_limit));
    for selection in selections.iter().take(sentinel_limit) {
        ensure!(
            selection.earlier_position < selection.target_position,
            "explicit checkpoint pair must be ordered"
        );
        let earlier = exact_checkpoint(connection, chain_id, selection.earlier_position)
            .await?
            .with_context(|| {
                format!(
                    "missing complete checkpoint at generation {} message sequence {}",
                    selection.earlier_position.generation, selection.earlier_position.message_seq
                )
            })?;
        let target = exact_checkpoint(connection, chain_id, selection.target_position)
            .await?
            .with_context(|| {
                format!(
                    "missing complete checkpoint at generation {} message sequence {}",
                    selection.target_position.generation, selection.target_position.message_seq
                )
            })?;
        let segment_start = segment_boundary_position(connection, chain_id, target.position)
            .await?
            .context("target checkpoint has no complete segment boundary")?;
        ensure!(
            earlier.position >= segment_start,
            "explicit checkpoint pair must stay in the same segment"
        );
        ensure!(
            checkpoint_supports(&earlier, backends) && checkpoint_supports(&target, backends),
            "explicit checkpoint pair does not contain every requested backend"
        );
        pairs.push(CheckpointPair { earlier, target });
    }
    Ok(pairs)
}

fn checkpoint_supports(manifest: &CheckpointManifest, backends: &[Backend]) -> bool {
    backends
        .iter()
        .all(|backend| manifest.backends.binary_search(backend).is_ok())
}

async fn exact_checkpoint(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    position: StreamPosition,
) -> anyhow::Result<Option<CheckpointManifest>> {
    let rows = sqlx::query(
        "SELECT id, chain_id, generation, message_seq, state_version, kind, block_number,
                rfq_observed_at_ms, backends, s3_key, archive_sha256, archive_bytes,
                compressed_bytes, token_s3_key, token_sha256, token_count, token_bytes,
                status, error
         FROM state_history.checkpoints
         WHERE chain_id = $1 AND status = 'complete'
           AND generation = $2 AND message_seq = $3
         ORDER BY kind",
    )
    .bind(database_i64(chain_id, "exact checkpoint chain_id")?)
    .bind(database_i64(
        position.generation,
        "exact checkpoint generation",
    )?)
    .bind(database_i64(
        position.message_seq,
        "exact checkpoint message_seq",
    )?)
    .fetch_all(connection)
    .await
    .context("failed to select exact checkpoint")?;
    ensure!(
        rows.len() <= 1,
        "checkpoint position is ambiguous because multiple complete kinds exist"
    );
    rows.first().map(manifest_from_row).transpose()
}

async fn segment_boundary_position(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    position: StreamPosition,
) -> anyhow::Result<Option<StreamPosition>> {
    let row = sqlx::query(
        "SELECT generation, message_seq
         FROM state_history.checkpoints
         WHERE chain_id = $1 AND status = 'complete' AND kind = 'boundary'
           AND (generation, message_seq) <= ($2, $3)
         ORDER BY generation DESC, message_seq DESC
         LIMIT 1",
    )
    .bind(database_i64(chain_id, "segment chain_id")?)
    .bind(database_i64(position.generation, "segment generation")?)
    .bind(database_i64(position.message_seq, "segment message_seq")?)
    .fetch_optional(connection)
    .await
    .context("failed to select checkpoint segment boundary")?;
    row.map(|row| {
        Ok(StreamPosition {
            generation: super::database_u64(row.try_get("generation")?, "segment generation")?,
            message_seq: super::database_u64(row.try_get("message_seq")?, "segment message_seq")?,
        })
    })
    .transpose()
}
