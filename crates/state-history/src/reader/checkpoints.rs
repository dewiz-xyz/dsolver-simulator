use anyhow::{ensure, Context};
use sqlx::Row;

use super::{
    begin_range_transaction, database_i64, manifest_from_row, BlockInterval,
    ReadConnectionProvider, StateHistoryReader,
};
use crate::{CheckpointKind, CheckpointManifest, CheckpointStatus, StreamPosition};

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
}

impl CheckpointPairQuery {
    pub const fn new(chain_id: u64, selection: CheckpointPairSelection) -> Self {
        Self {
            chain_id,
            selection,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointPair {
    pub earlier: CheckpointManifest,
    pub target: CheckpointManifest,
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
                range_checkpoint_pairs(&mut transaction, query.chain_id, *bounds).await?
            }
            CheckpointPairSelection::Explicit(selections) => {
                explicit_checkpoint_pairs(&mut transaction, query.chain_id, selections).await?
            }
        };
        transaction
            .commit()
            .await
            .context("failed to commit checkpoint pair transaction")?;
        Ok(pairs)
    }
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
) -> anyhow::Result<Vec<CheckpointPair>> {
    let rows = sqlx::query(
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
    .fetch_all(connection)
    .await
    .context("failed to select checkpoint pair range")?;
    let manifests = rows
        .iter()
        .map(manifest_from_row)
        .collect::<anyhow::Result<Vec<_>>>()?;
    ensure_unique_positions(&manifests)?;

    let mut pairs = Vec::new();
    let mut previous: Option<CheckpointManifest> = None;
    for manifest in manifests {
        if manifest.kind == CheckpointKind::Boundary {
            previous = Some(manifest);
            continue;
        }
        if let Some(earlier) = previous.replace(manifest.clone()) {
            pairs.push(CheckpointPair {
                earlier,
                target: manifest,
            });
        }
    }
    Ok(pairs)
}

async fn explicit_checkpoint_pairs(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    selections: &[ExplicitCheckpointPair],
) -> anyhow::Result<Vec<CheckpointPair>> {
    let mut pairs = Vec::with_capacity(selections.len());
    for selection in selections {
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
        pairs.push(CheckpointPair { earlier, target });
    }
    Ok(pairs)
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

fn ensure_unique_positions(manifests: &[CheckpointManifest]) -> anyhow::Result<()> {
    for pair in manifests.windows(2) {
        ensure!(
            pair[0].position != pair[1].position,
            "checkpoint position is ambiguous because multiple complete kinds exist"
        );
    }
    Ok(())
}
