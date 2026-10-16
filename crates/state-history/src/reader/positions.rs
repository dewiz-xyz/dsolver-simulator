//! Reads of the stored stream between two positions, for a consumer that starts
//! from its own state instead of a checkpoint.

use std::collections::BTreeMap;

use anyhow::{ensure, Context};
use sqlx::Row;

use super::{
    begin_range_transaction, database_backends, database_i64, database_u64,
    decode_delta_row_with_limit, encoded_delta_from_row, fetch_delta_backends, gap_from_row,
    manifest_from_row, EncodedDeltaRow, RangeGap, ReadConnectionProvider, ReadLimitError,
    ReadLimits, StateHistoryReader, StoredDelta,
};
use crate::{Backend, CheckpointManifest, DeltaBackendCursor, StreamPosition};

/// The stored stream of one chain after `after`, up to and including `through`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionRangeQuery {
    pub chain_id: u64,
    pub after: StreamPosition,
    pub through: StreamPosition,
    pub backends: Vec<Backend>,
}

impl PositionRangeQuery {
    pub fn new(
        chain_id: u64,
        after: StreamPosition,
        through: StreamPosition,
        mut backends: Vec<Backend>,
    ) -> Self {
        backends.sort_unstable();
        backends.dedup();
        Self {
            chain_id,
            after,
            through,
            backends,
        }
    }
}

/// What the store holds for a position range, read in one snapshot.
#[derive(Debug, Clone)]
pub struct PositionRange {
    /// The stored updates with a partition of a requested backend, in stream
    /// order. Positions between them may belong to kinds that are never stored.
    pub deltas: Vec<StoredDelta>,
    /// The recorded gaps that overlap the range.
    pub gaps: Vec<RangeGap>,
    /// The boundary checkpoints inside the range, of any status. Stored deltas
    /// do not carry state across one.
    pub boundaries: Vec<CheckpointManifest>,
    /// The latest delta stored in `through`'s generation, whatever its
    /// backends. It says how far the writer has come, not whether `deltas` is
    /// complete. One writer stores a generation's deltas in stream order, so a
    /// position above it may still be on its way. A lost delta's gap can be
    /// stored after later deltas or not at all, so a position at or below it that
    /// is neither in `deltas` nor in a gap is no proof that no update was lost
    /// there. Another generation's deltas prove nothing about this one, since a
    /// new writer can store while the old one drains.
    pub last_stored: Option<StreamPosition>,
    pub estimated_decoded_bytes: u64,
}

impl<P: ReadConnectionProvider> StateHistoryReader<P> {
    pub async fn read_position_range(
        &self,
        query: &PositionRangeQuery,
        limits: ReadLimits,
    ) -> anyhow::Result<PositionRange> {
        ensure!(
            !query.backends.is_empty(),
            "position range backends must not be empty"
        );
        ensure!(
            query.after < query.through,
            "position range must be ordered"
        );
        let mut connection = self
            .connections
            .acquire()
            .await
            .context("failed to acquire position range connection")?;
        let mut transaction = begin_range_transaction(&mut connection).await?;
        let encoded_rows = fetch_delta_rows_between(
            &mut transaction,
            query.chain_id,
            query.after,
            query.through,
            &query.backends,
            limits.max_compressed_bytes,
        )
        .await?;
        let backend_cursors = fetch_backend_cursors(&mut transaction, &encoded_rows).await?;
        let gaps = fetch_gaps_between(&mut transaction, query.chain_id, query.after, query.through)
            .await?;
        let boundaries =
            fetch_boundaries_between(&mut transaction, query.chain_id, query.after, query.through)
                .await?;
        let last_stored =
            fetch_last_stored(&mut transaction, query.chain_id, query.through.generation).await?;
        transaction
            .commit()
            .await
            .context("failed to commit position range transaction")?;
        drop(connection);
        let (deltas, estimated_decoded_bytes) = decode_stored_deltas(
            encoded_rows,
            &backend_cursors,
            &query.backends,
            limits.max_decoded_bytes,
        )?;
        Ok(PositionRange {
            deltas,
            gaps,
            boundaries,
            last_stored,
            estimated_decoded_bytes,
        })
    }
}

/// The encoded deltas after `after` through `through` with a partition of one
/// of `backends`, refusing more than `max_compressed_bytes` before any payload
/// is read.
pub(super) async fn fetch_delta_rows_between(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    after: StreamPosition,
    through: StreamPosition,
    backends: &[Backend],
    max_compressed_bytes: u64,
) -> anyhow::Result<Vec<EncodedDeltaRow>> {
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
    .bind(database_i64(chain_id, "position range chain_id")?)
    .bind(database_i64(after.generation, "range start generation")?)
    .bind(database_i64(after.message_seq, "range start message_seq")?)
    .bind(database_i64(through.generation, "range end generation")?)
    .bind(database_i64(through.message_seq, "range end message_seq")?)
    .bind(database_backends(backends))
    .fetch_one(&mut *connection)
    .await
    .context("failed to preflight position range delta bytes")?;
    let compressed_bytes = database_u64(compressed_bytes, "position range delta bytes")?;
    if compressed_bytes > max_compressed_bytes {
        return Err(ReadLimitError::CompressedBytesExceeded {
            declared: compressed_bytes,
            limit: max_compressed_bytes,
        }
        .into());
    }

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
    .bind(database_i64(chain_id, "position range chain_id")?)
    .bind(database_i64(after.generation, "range start generation")?)
    .bind(database_i64(after.message_seq, "range start message_seq")?)
    .bind(database_i64(through.generation, "range end generation")?)
    .bind(database_i64(through.message_seq, "range end message_seq")?)
    .bind(database_backends(backends))
    .fetch_all(connection)
    .await
    .context("failed to select position range deltas")?;
    rows.iter().map(encoded_delta_from_row).collect()
}

/// The backend cursors of `rows`, by delta id.
pub(super) async fn fetch_backend_cursors(
    connection: &mut sqlx::PgConnection,
    rows: &[EncodedDeltaRow],
) -> anyhow::Result<BTreeMap<i64, Vec<DeltaBackendCursor>>> {
    let delta_ids = rows.iter().map(|row| row.id).collect::<Vec<_>>();
    fetch_delta_backends(connection, &delta_ids).await
}

/// Decodes `rows` within `max_decoded_bytes` and returns them with their
/// decoded size, after the connection that read them is back in its pool. Each keeps
/// its whole payload, and its `applicable_backends` lists only the partitions
/// of `backends`.
pub(super) fn decode_stored_deltas(
    rows: Vec<EncodedDeltaRow>,
    backend_cursors: &BTreeMap<i64, Vec<DeltaBackendCursor>>,
    backends: &[Backend],
    max_decoded_bytes: u64,
) -> anyhow::Result<(Vec<StoredDelta>, u64)> {
    let mut decoded_bytes = 0_u64;
    let mut deltas = Vec::with_capacity(rows.len());
    for row in rows {
        let remaining = max_decoded_bytes.saturating_sub(decoded_bytes);
        let (row, row_bytes) = decode_delta_row_with_limit(row, backend_cursors, remaining)?;
        decoded_bytes = decoded_bytes
            .checked_add(row_bytes)
            .context("delta decoded byte estimate overflowed")?;
        deltas.push(StoredDelta {
            position: row.position,
            observed_at_ms: row.observed_at_ms,
            payload_format_version: row.payload_format_version,
            raw_payload: row.payload,
            applicable_backends: row
                .backends
                .into_iter()
                .filter(|cursor| backends.contains(&cursor.backend))
                .collect(),
        });
    }
    Ok((deltas, decoded_bytes))
}

/// The recorded gaps that overlap the positions after `after` through `through`.
pub(super) async fn fetch_gaps_between(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    after: StreamPosition,
    through: StreamPosition,
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
    .bind(database_i64(chain_id, "position range chain_id")?)
    .bind(database_i64(after.generation, "range start generation")?)
    .bind(database_i64(after.message_seq, "range start message_seq")?)
    .bind(database_i64(through.generation, "range end generation")?)
    .bind(database_i64(through.message_seq, "range end message_seq")?)
    .fetch_all(connection)
    .await
    .context("failed to select position range gaps")?;
    rows.iter()
        .map(|row| gap_from_row(row).map(RangeGap::from))
        .collect()
}

async fn fetch_boundaries_between(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    after: StreamPosition,
    through: StreamPosition,
) -> anyhow::Result<Vec<CheckpointManifest>> {
    let rows = sqlx::query(
        "SELECT id, chain_id, generation, message_seq, state_version, kind, block_number,
                rfq_observed_at_ms, backends, s3_key, archive_sha256, archive_bytes,
                compressed_bytes, token_s3_key, token_sha256, token_count, token_bytes,
                status, error
         FROM state_history.checkpoints
         WHERE chain_id = $1
           AND kind = 'boundary'
           AND (generation, message_seq) > ($2, $3)
           AND (generation, message_seq) <= ($4, $5)
         ORDER BY generation, message_seq",
    )
    .bind(database_i64(chain_id, "position range chain_id")?)
    .bind(database_i64(after.generation, "range start generation")?)
    .bind(database_i64(after.message_seq, "range start message_seq")?)
    .bind(database_i64(through.generation, "range end generation")?)
    .bind(database_i64(through.message_seq, "range end message_seq")?)
    .fetch_all(connection)
    .await
    .context("failed to select position range boundary checkpoints")?;
    rows.iter().map(manifest_from_row).collect()
}

async fn fetch_last_stored(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    generation: u64,
) -> anyhow::Result<Option<StreamPosition>> {
    let row = sqlx::query(
        "SELECT generation, message_seq FROM state_history.deltas
         WHERE chain_id = $1 AND generation = $2
         ORDER BY message_seq DESC LIMIT 1",
    )
    .bind(database_i64(chain_id, "position range chain_id")?)
    .bind(database_i64(generation, "range end generation")?)
    .fetch_optional(connection)
    .await
    .context("failed to select the last stored delta")?;
    row.map(|row| {
        Ok(StreamPosition {
            generation: database_u64(row.try_get("generation")?, "stored generation")?,
            message_seq: database_u64(row.try_get("message_seq")?, "stored message_seq")?,
        })
    })
    .transpose()
}
