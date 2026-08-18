use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context;
use sqlx::Row;

use super::{
    begin_range_transaction, database_backends, database_i64, database_u64, gap_from_row,
    manifest_from_row, resolve_range_in_transaction, GapRow, RangeGap, RangeGapKind, RangeLeg,
    RangeRequest, ReadConnectionProvider, ReadLimits, StateHistoryReader,
};
use crate::{Backend, BlockTimeObservation, ModelError, StreamPosition};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockInterval {
    pub start: u64,
    pub end_inclusive: u64,
}

impl BlockInterval {
    pub fn new(start: u64, end_inclusive: u64) -> Result<Self, ModelError> {
        if start > end_inclusive {
            return Err(ModelError::InvalidBlockRange {
                start,
                end: end_inclusive,
            });
        }
        Ok(Self {
            start,
            end_inclusive,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageQuery {
    pub chain_id: u64,
    pub backends: Vec<Backend>,
    pub bounds: Option<BlockInterval>,
}

impl CoverageQuery {
    pub fn new(
        chain_id: u64,
        mut backends: Vec<Backend>,
        bounds: Option<BlockInterval>,
    ) -> Result<Self, ModelError> {
        if backends.is_empty() {
            return Err(ModelError::EmptyBackends);
        }
        backends.sort_unstable();
        backends.dedup();
        Ok(Self {
            chain_id,
            backends,
            bounds,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageSnapshot {
    pub visible_through_block: u64,
    pub continuous_intervals: Vec<BlockInterval>,
    pub known_gaps: Vec<RangeGap>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetPlanQuery {
    pub chain_id: u64,
    pub backends: Vec<Backend>,
    pub target_blocks: BTreeSet<u64>,
    pub limits: ReadLimits,
}

impl TargetPlanQuery {
    pub fn new(
        chain_id: u64,
        mut backends: Vec<Backend>,
        target_blocks: BTreeSet<u64>,
        limits: ReadLimits,
    ) -> Result<Self, ModelError> {
        if backends.is_empty() {
            return Err(ModelError::EmptyBackends);
        }
        if target_blocks.is_empty() {
            return Err(ModelError::EmptyTargetBlocks);
        }
        backends.sort_unstable();
        backends.dedup();
        if backends.binary_search(&Backend::Rfq).is_ok()
            && target_blocks.last().is_some_and(|block| *block == u64::MAX)
        {
            return Err(ModelError::RfqTargetHasNoSuccessor(u64::MAX));
        }
        Ok(Self {
            chain_id,
            backends,
            target_blocks,
            limits,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TargetPlan {
    pub visible_through_block: u64,
    pub block_times: BTreeMap<u64, BlockTimeObservation>,
    pub missing_block_times: BTreeSet<u64>,
    pub legs: Vec<RangeLeg>,
    pub gaps: Vec<RangeGap>,
    pub lineage_invalid_intervals: Vec<BlockInterval>,
    pub estimated_decoded_bytes: u64,
}

impl<P: ReadConnectionProvider> StateHistoryReader<P> {
    pub async fn coverage(&self, query: &CoverageQuery) -> anyhow::Result<CoverageSnapshot> {
        let mut connection = self
            .connections
            .acquire()
            .await
            .context("failed to acquire state history coverage connection")?;
        let mut transaction = begin_range_transaction(&mut connection).await?;

        let visible_through_block =
            visible_through_block(&mut transaction, query.chain_id, &query.backends).await?;
        let retained_floor =
            earliest_compatible_checkpoint(&mut transaction, query.chain_id, &query.backends)
                .await?
                .context("state history has no complete checkpoint for the requested backends")?;
        let retained_start = query
            .bounds
            .map_or(retained_floor, |bounds| bounds.start.max(retained_floor));
        let requested_end = query
            .bounds
            .map_or(visible_through_block, |bounds| bounds.end_inclusive);
        let interval_end = requested_end.min(visible_through_block);

        let gap_rows = coverage_gap_rows(&mut transaction, query.chain_id).await?;
        let known_gaps = gap_rows.iter().map(range_gap).collect::<Vec<_>>();
        let mut unsafe_intervals = project_gap_intervals(
            &mut transaction,
            query,
            &gap_rows,
            retained_start,
            interval_end,
        )
        .await?;
        unsafe_intervals.extend(
            lineage_invalid_intervals(
                &mut transaction,
                query.chain_id,
                retained_start,
                interval_end,
            )
            .await?,
        );
        if query.backends.binary_search(&Backend::Rfq).is_ok() {
            unsafe_intervals.extend(
                missing_rfq_boundary_intervals(
                    &mut transaction,
                    query.chain_id,
                    retained_start,
                    interval_end,
                )
                .await?,
            );
        }
        let continuous_intervals = if retained_start > interval_end {
            Vec::new()
        } else {
            subtract_intervals(
                BlockInterval::new(retained_start, interval_end)?,
                &unsafe_intervals,
            )
        };

        transaction
            .commit()
            .await
            .context("failed to commit state history coverage transaction")?;
        Ok(CoverageSnapshot {
            visible_through_block,
            continuous_intervals,
            known_gaps,
        })
    }

    pub async fn plan_targets(&self, query: &TargetPlanQuery) -> anyhow::Result<TargetPlan> {
        let start_block = *query
            .target_blocks
            .first()
            .context("target plan has no start block")?;
        let end_block = *query
            .target_blocks
            .last()
            .context("target plan has no end block")?;
        let includes_rfq = query.backends.binary_search(&Backend::Rfq).is_ok();
        let mut required_blocks = query.target_blocks.clone();
        if includes_rfq {
            for target in &query.target_blocks {
                required_blocks.insert(
                    target
                        .checked_add(1)
                        .ok_or(ModelError::RfqTargetHasNoSuccessor(*target))?,
                );
            }
        }

        let mut connection = self
            .connections
            .acquire()
            .await
            .context("failed to acquire target planning connection")?;
        let mut transaction = begin_range_transaction(&mut connection).await?;
        let visible_through_block =
            visible_through_block(&mut transaction, query.chain_id, &query.backends).await?;
        let (block_times, missing_block_times) =
            required_block_times(&mut transaction, query.chain_id, &required_blocks).await?;
        let lineage_end = required_blocks.last().copied().unwrap_or(end_block);
        let lineage_invalid_intervals =
            lineage_invalid_intervals(&mut transaction, query.chain_id, start_block, lineage_end)
                .await?;

        let Some((range_start, range_end)) = replay_target_range(query, &block_times) else {
            transaction
                .commit()
                .await
                .context("failed to commit target planning transaction")?;
            return Ok(TargetPlan {
                visible_through_block,
                block_times,
                missing_block_times,
                legs: Vec::new(),
                gaps: Vec::new(),
                lineage_invalid_intervals,
                estimated_decoded_bytes: 0,
            });
        };
        let mut range_request = RangeRequest::new(
            query.chain_id,
            range_start,
            range_end,
            query.backends.clone(),
        )?;
        let rfq_bounds = replay_rfq_bounds(includes_rfq, range_start, range_end, &block_times)?;
        if let Some((start_timestamp, end_timestamp)) = rfq_bounds {
            range_request = range_request.with_rfq_bounds(start_timestamp, end_timestamp)?;
        }
        let resolution = resolve_range_in_transaction(
            &mut transaction,
            &range_request,
            rfq_bounds,
            query.limits,
        )
        .await?;
        transaction
            .commit()
            .await
            .context("failed to commit target planning transaction")?;
        Ok(TargetPlan {
            visible_through_block,
            block_times,
            missing_block_times,
            legs: resolution.plan.legs,
            gaps: resolution.plan.gaps,
            lineage_invalid_intervals,
            estimated_decoded_bytes: resolution.estimated_decoded_bytes,
        })
    }
}

fn replay_target_range(
    query: &TargetPlanQuery,
    block_times: &BTreeMap<u64, BlockTimeObservation>,
) -> Option<(u64, u64)> {
    if query.backends.binary_search(&Backend::Rfq).is_err() {
        return query
            .target_blocks
            .first()
            .copied()
            .zip(query.target_blocks.last().copied());
    }
    let mut safe = query.target_blocks.iter().copied().filter(|target| {
        target
            .checked_add(1)
            .is_some_and(|next| block_times.contains_key(target) && block_times.contains_key(&next))
    });
    let start = safe.next()?;
    Some((start, safe.next_back().unwrap_or(start)))
}

fn replay_rfq_bounds(
    includes_rfq: bool,
    range_start: u64,
    range_end: u64,
    block_times: &BTreeMap<u64, BlockTimeObservation>,
) -> anyhow::Result<Option<(u64, u64)>> {
    if !includes_rfq {
        return Ok(None);
    }
    let start_plus_one = range_start
        .checked_add(1)
        .ok_or(ModelError::RfqTargetHasNoSuccessor(range_start))?;
    let start_timestamp = block_times
        .get(&start_plus_one)
        .context("safe RFQ target is missing its next-block timestamp")?
        .timestamp_ms
        .checked_sub(1)
        .context("target next-block timestamp must be positive")?;
    let end_plus_one = range_end
        .checked_add(1)
        .ok_or(ModelError::RfqTargetHasNoSuccessor(range_end))?;
    let end_timestamp = block_times
        .get(&end_plus_one)
        .context("safe RFQ target is missing its next-block timestamp")?
        .timestamp_ms
        .checked_sub(1)
        .context("target next-block timestamp must be positive")?;
    Ok(Some((start_timestamp, end_timestamp)))
}

async fn required_block_times(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    required_blocks: &BTreeSet<u64>,
) -> anyhow::Result<(BTreeMap<u64, BlockTimeObservation>, BTreeSet<u64>)> {
    let database_blocks = required_blocks
        .iter()
        .map(|block| database_i64(*block, "required target block"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let rows = sqlx::query(
        "SELECT block_number, timestamp_ms, block_hash, parent_hash
         FROM state_history.block_times
         WHERE chain_id = $1 AND block_number = ANY($2::bigint[])
         ORDER BY block_number",
    )
    .bind(database_i64(chain_id, "target plan chain_id")?)
    .bind(database_blocks)
    .fetch_all(connection)
    .await
    .context("failed to select target block times")?;
    let mut observations = BTreeMap::new();
    for row in rows {
        let observation = BlockTimeObservation {
            block_number: database_u64(row.try_get("block_number")?, "target block number")?,
            timestamp_ms: database_u64(row.try_get("timestamp_ms")?, "target timestamp")?,
            block_hash: row.try_get("block_hash")?,
            parent_hash: row.try_get("parent_hash")?,
        };
        observations.insert(observation.block_number, observation);
    }
    let missing = required_blocks
        .iter()
        .filter(|block| !observations.contains_key(block))
        .copied()
        .collect::<BTreeSet<_>>();
    Ok((observations, missing))
}

pub(super) async fn visible_through_block(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    backends: &[Backend],
) -> anyhow::Result<u64> {
    let mut cutoffs = Vec::with_capacity(backends.len());
    for backend in backends {
        let cutoff = match backend {
            Backend::Native | Backend::Vm => {
                let checkpoint: Option<i64> = sqlx::query_scalar(
                    "SELECT max(block_number)
                     FROM state_history.checkpoints
                     WHERE chain_id = $1 AND status = 'complete' AND $2 = ANY(backends)",
                )
                .bind(database_i64(chain_id, "coverage chain_id")?)
                .bind(backend.as_str())
                .fetch_one(&mut *connection)
                .await?;
                let delta: Option<i64> = sqlx::query_scalar(
                    "SELECT max(block_number)
                     FROM state_history.delta_backends
                     WHERE chain_id = $1 AND backend = $2",
                )
                .bind(database_i64(chain_id, "coverage chain_id")?)
                .bind(backend.as_str())
                .fetch_one(&mut *connection)
                .await?;
                checkpoint
                    .into_iter()
                    .chain(delta)
                    .max()
                    .map(|value| database_u64(value, "coverage backend cutoff"))
                    .transpose()?
            }
            Backend::Rfq => {
                let has_anchor: bool = sqlx::query_scalar(
                    "SELECT EXISTS (
                        SELECT 1 FROM state_history.checkpoints
                        WHERE chain_id = $1 AND status = 'complete' AND 'rfq' = ANY(backends)
                    )",
                )
                .bind(database_i64(chain_id, "coverage chain_id")?)
                .fetch_one(&mut *connection)
                .await?;
                if !has_anchor {
                    None
                } else {
                    let block: Option<i64> = sqlx::query_scalar(
                        "SELECT max(block_number) - 1
                         FROM state_history.block_times
                         WHERE chain_id = $1",
                    )
                    .bind(database_i64(chain_id, "coverage chain_id")?)
                    .fetch_one(&mut *connection)
                    .await?;
                    block
                        .map(|value| database_u64(value, "coverage RFQ cutoff"))
                        .transpose()?
                }
            }
        };
        cutoffs.push(cutoff.with_context(|| {
            format!("state history has no visible {} cutoff", backend.as_str())
        })?);
    }
    cutoffs
        .into_iter()
        .min()
        .context("coverage backend set is empty")
}

async fn earliest_compatible_checkpoint(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    backends: &[Backend],
) -> anyhow::Result<Option<u64>> {
    let value: Option<i64> = sqlx::query_scalar(
        "SELECT min(block_number)
         FROM state_history.checkpoints
         WHERE chain_id = $1 AND status = 'complete' AND backends @> $2::text[]",
    )
    .bind(database_i64(chain_id, "coverage chain_id")?)
    .bind(database_backends(backends))
    .fetch_one(connection)
    .await?;
    value
        .map(|value| database_u64(value, "coverage retained start"))
        .transpose()
}

async fn coverage_gap_rows(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
) -> anyhow::Result<Vec<GapRow>> {
    let rows = sqlx::query(
        "SELECT generation, from_message_seq, to_message_seq, reason,
                from_block_number, to_block_number, from_observed_at_ms, to_observed_at_ms
         FROM state_history.gaps
         WHERE chain_id = $1
         ORDER BY generation, from_message_seq, to_message_seq, id",
    )
    .bind(database_i64(chain_id, "coverage chain_id")?)
    .fetch_all(connection)
    .await?;
    rows.iter().map(gap_from_row).collect()
}

fn range_gap(gap: &GapRow) -> RangeGap {
    RangeGap {
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
        reason: gap.reason.clone(),
    }
}

async fn project_gap_intervals(
    connection: &mut sqlx::PgConnection,
    query: &CoverageQuery,
    gaps: &[GapRow],
    start: u64,
    end: u64,
) -> anyhow::Result<Vec<BlockInterval>> {
    let mut projected = Vec::new();
    for gap in gaps {
        let includes_block_backend = query
            .backends
            .iter()
            .any(|backend| matches!(backend, Backend::Native | Backend::Vm));
        let includes_rfq = query.backends.binary_search(&Backend::Rfq).is_ok();
        let no_bounds = gap.from_block_number.is_none()
            && gap.to_block_number.is_none()
            && gap.from_observed_at_ms.is_none()
            && gap.to_observed_at_ms.is_none();
        let mut needs_conservative_projection = no_bounds;

        if includes_block_backend {
            match (gap.from_block_number, gap.to_block_number) {
                (Some(from), Some(to)) => push_clipped(&mut projected, from, to, start, end)?,
                (None, None) => {}
                _ => needs_conservative_projection = true,
            }
        }
        if includes_rfq {
            match (gap.from_observed_at_ms, gap.to_observed_at_ms) {
                (Some(from), Some(to)) => {
                    if let Some(interval) =
                        rfq_time_gap_to_blocks(connection, query.chain_id, from, to).await?
                    {
                        push_clipped(
                            &mut projected,
                            interval.start,
                            interval.end_inclusive,
                            start,
                            end,
                        )?;
                    }
                }
                (None, None) => {}
                _ => needs_conservative_projection = true,
            }
        }
        if needs_conservative_projection {
            let (prior, next) = surrounding_boundaries(connection, query, gap).await?;
            let unsafe_start = prior.unwrap_or(start).max(start);
            let unsafe_end = next.map_or(end, |block| block.saturating_sub(1)).min(end);
            if unsafe_start <= unsafe_end {
                projected.push(BlockInterval::new(unsafe_start, unsafe_end)?);
            }
        }
    }
    Ok(projected)
}

async fn surrounding_boundaries(
    connection: &mut sqlx::PgConnection,
    query: &CoverageQuery,
    gap: &GapRow,
) -> anyhow::Result<(Option<u64>, Option<u64>)> {
    let rows = sqlx::query(
        "SELECT id, chain_id, generation, message_seq, state_version, kind, block_number,
                rfq_observed_at_ms, backends, s3_key, archive_sha256, archive_bytes,
                compressed_bytes, token_s3_key, token_sha256, token_count, token_bytes,
                status, error
         FROM state_history.checkpoints
         WHERE chain_id = $1 AND status = 'complete' AND kind = 'boundary'
           AND backends @> $2::text[]
         ORDER BY generation, message_seq",
    )
    .bind(database_i64(query.chain_id, "coverage chain_id")?)
    .bind(database_backends(&query.backends))
    .fetch_all(connection)
    .await?;
    let from_position = StreamPosition {
        generation: gap.generation,
        message_seq: gap.from_message_seq,
    };
    let to_position = StreamPosition {
        generation: gap.generation,
        message_seq: gap.to_message_seq,
    };
    let manifests = rows
        .iter()
        .map(manifest_from_row)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let prior = manifests
        .iter()
        .filter(|manifest| manifest.position < from_position)
        .map(|manifest| manifest.block_number)
        .next_back();
    let next = manifests
        .iter()
        .find(|manifest| manifest.position > to_position)
        .map(|manifest| manifest.block_number);
    Ok((prior, next))
}

async fn rfq_time_gap_to_blocks(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    from_ms: u64,
    to_ms: u64,
) -> anyhow::Result<Option<BlockInterval>> {
    let row: (Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT min(current.block_number), max(current.block_number)
         FROM state_history.block_times current
         JOIN state_history.block_times next
           ON next.chain_id = current.chain_id AND next.block_number = current.block_number + 1
         WHERE current.chain_id = $1
           AND next.timestamp_ms > $2
           AND current.timestamp_ms <= $3",
    )
    .bind(database_i64(chain_id, "coverage chain_id")?)
    .bind(database_i64(from_ms, "coverage RFQ gap start")?)
    .bind(database_i64(to_ms, "coverage RFQ gap end")?)
    .fetch_one(connection)
    .await?;
    match row {
        (Some(start), Some(end)) => Ok(Some(BlockInterval::new(
            database_u64(start, "coverage RFQ gap block start")?,
            database_u64(end, "coverage RFQ gap block end")?,
        )?)),
        (None, None) => Ok(None),
        _ => anyhow::bail!("RFQ gap projection returned incomplete bounds"),
    }
}

async fn missing_rfq_boundary_intervals(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    start: u64,
    end: u64,
) -> anyhow::Result<Vec<BlockInterval>> {
    if start > end {
        return Ok(Vec::new());
    }
    let rows: Vec<i64> = sqlx::query_scalar(
        "SELECT current.block_number
         FROM state_history.block_times current
         LEFT JOIN state_history.block_times successor
           ON successor.chain_id = current.chain_id
          AND successor.block_number = current.block_number + 1
         WHERE current.chain_id = $1
           AND current.block_number BETWEEN $2 AND $3
           AND successor.block_number IS NULL
         ORDER BY current.block_number",
    )
    .bind(database_i64(chain_id, "coverage chain_id")?)
    .bind(database_i64(start, "coverage RFQ boundary start")?)
    .bind(database_i64(end, "coverage RFQ boundary end")?)
    .fetch_all(connection)
    .await
    .context("failed to select missing RFQ next-block boundaries")?;
    let intervals = rows
        .into_iter()
        .map(|block| {
            let block = database_u64(block, "coverage RFQ missing boundary block")?;
            BlockInterval::new(block, block).map_err(anyhow::Error::from)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(merge_intervals(intervals))
}

pub(super) async fn lineage_invalid_intervals(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    start: u64,
    end: u64,
) -> anyhow::Result<Vec<BlockInterval>> {
    if start > end {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        "SELECT block_number, block_hash, parent_hash
         FROM state_history.block_times
         WHERE chain_id = $1 AND block_number BETWEEN $2 AND $3
         ORDER BY block_number",
    )
    .bind(database_i64(chain_id, "lineage chain_id")?)
    .bind(database_i64(start, "lineage start")?)
    .bind(database_i64(end, "lineage end")?)
    .fetch_all(connection)
    .await?;
    let mut invalid = Vec::new();
    let mut previous: Option<(u64, String)> = None;
    let mut expected_block = start;
    for row in rows {
        let block = database_u64(row.try_get("block_number")?, "lineage block")?;
        let hash: Option<String> = row.try_get("block_hash")?;
        let parent: Option<String> = row.try_get("parent_hash")?;
        let had_missing_predecessor = block > expected_block;
        if had_missing_predecessor {
            invalid.push(BlockInterval::new(expected_block, block - 1)?);
            previous = None;
        }
        let is_invalid = match (&previous, hash.as_ref(), parent.as_ref()) {
            (_, None, _) | (_, _, None) => true,
            (Some((previous_block, previous_hash)), Some(_), Some(parent_hash)) => {
                block != previous_block.saturating_add(1) || parent_hash != previous_hash
            }
            (None, Some(_), Some(_)) => block != start || had_missing_predecessor,
        };
        if is_invalid {
            invalid.push(BlockInterval::new(block, block)?);
        }
        if let Some(hash) = hash {
            previous = Some((block, hash));
        }
        expected_block = block.saturating_add(1);
    }
    if expected_block <= end {
        invalid.push(BlockInterval::new(expected_block, end)?);
    }
    Ok(merge_intervals(invalid))
}

fn push_clipped(
    intervals: &mut Vec<BlockInterval>,
    from: u64,
    to: u64,
    start: u64,
    end: u64,
) -> anyhow::Result<()> {
    let clipped_start = from.max(start);
    let clipped_end = to.min(end);
    if clipped_start <= clipped_end {
        intervals.push(BlockInterval::new(clipped_start, clipped_end)?);
    }
    Ok(())
}

pub(super) fn subtract_intervals(
    base: BlockInterval,
    unsafe_intervals: &[BlockInterval],
) -> Vec<BlockInterval> {
    let unsafe_intervals = merge_intervals(unsafe_intervals.to_vec());
    let mut safe = Vec::new();
    let mut cursor = base.start;
    for interval in unsafe_intervals {
        if interval.end_inclusive < base.start || interval.start > base.end_inclusive {
            continue;
        }
        let start = interval.start.max(base.start);
        let end = interval.end_inclusive.min(base.end_inclusive);
        if cursor < start {
            safe.push(BlockInterval {
                start: cursor,
                end_inclusive: start - 1,
            });
        }
        cursor = cursor.max(end.saturating_add(1));
        if cursor > base.end_inclusive {
            break;
        }
    }
    if cursor <= base.end_inclusive {
        safe.push(BlockInterval {
            start: cursor,
            end_inclusive: base.end_inclusive,
        });
    }
    safe
}

fn merge_intervals(mut intervals: Vec<BlockInterval>) -> Vec<BlockInterval> {
    intervals.sort_unstable();
    let mut merged: Vec<BlockInterval> = Vec::new();
    for interval in intervals {
        if let Some(last) = merged.last_mut() {
            if interval.start <= last.end_inclusive.saturating_add(1) {
                last.end_inclusive = last.end_inclusive.max(interval.end_inclusive);
                continue;
            }
        }
        merged.push(interval);
    }
    merged
}
