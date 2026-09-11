use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context;
use sqlx::Row;

use super::{
    begin_range_transaction, database_backends, database_i64, database_u64, gap_from_row,
    resolve_range_in_transaction, GapRow, RangeGap, RangeLeg, RangeRequest, ReadConnectionProvider,
    ReadLimits, StateHistoryReader,
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

#[derive(Debug)]
struct GapProjection {
    gap_index: usize,
    unsafe_intervals: Vec<BlockInterval>,
}

#[derive(Debug, Default)]
struct PendingGapProjection {
    unsafe_intervals: Vec<BlockInterval>,
    rfq_time_bounds: Option<(u64, u64)>,
    needs_conservative_projection: bool,
}

#[derive(Debug, Clone, Copy)]
struct BoundaryPoint {
    position: StreamPosition,
    block_number: u64,
}

#[derive(Debug)]
struct CoverageBlockRow {
    block_number: u64,
    block_hash: Option<String>,
    parent_hash: Option<String>,
}

#[derive(Debug, Default)]
struct CoverageBlockAnalysis {
    lineage_invalid_intervals: Vec<BlockInterval>,
    missing_rfq_boundary_intervals: Vec<BlockInterval>,
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
        let gap_projections = project_gap_intervals(
            &mut transaction,
            query,
            &gap_rows,
            retained_start,
            interval_end,
        )
        .await?;
        let known_gaps = gap_projections
            .iter()
            .map(|projection| RangeGap::from(gap_rows[projection.gap_index].clone()))
            .collect::<Vec<_>>();
        let mut unsafe_intervals = gap_projections
            .into_iter()
            .flat_map(|projection| projection.unsafe_intervals)
            .collect::<Vec<_>>();
        let includes_rfq = query.backends.binary_search(&Backend::Rfq).is_ok();
        let block_analysis = coverage_block_analysis(
            &mut transaction,
            query.chain_id,
            retained_start,
            interval_end,
            includes_rfq,
        )
        .await?;
        unsafe_intervals.extend(block_analysis.lineage_invalid_intervals);
        unsafe_intervals.extend(block_analysis.missing_rfq_boundary_intervals);
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

pub(crate) async fn visible_through_block(
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

async fn project_gap_intervals(
    connection: &mut sqlx::PgConnection,
    query: &CoverageQuery,
    gaps: &[GapRow],
    start: u64,
    end: u64,
) -> anyhow::Result<Vec<GapProjection>> {
    if start > end || gaps.is_empty() {
        return Ok(Vec::new());
    }

    let includes_block_backend = query
        .backends
        .iter()
        .any(|backend| matches!(backend, Backend::Native | Backend::Vm));
    let includes_rfq = query.backends.binary_search(&Backend::Rfq).is_ok();
    let mut pending = gaps
        .iter()
        .map(|gap| pending_gap_projection(gap, start, end, includes_block_backend, includes_rfq))
        .collect::<anyhow::Result<Vec<_>>>()?;

    let rfq_bounds = pending
        .iter()
        .enumerate()
        .filter_map(|(gap_index, projection)| {
            projection
                .rfq_time_bounds
                .map(|(from_ms, to_ms)| (gap_index, from_ms, to_ms))
        })
        .collect::<Vec<_>>();
    for (gap_index, interval) in
        batch_rfq_gap_intervals(connection, query.chain_id, &rfq_bounds, start, end).await?
    {
        pending
            .get_mut(gap_index)
            .context("RFQ projection returned an unknown gap index")?
            .unsafe_intervals
            .push(interval);
    }

    if pending
        .iter()
        .any(|projection| projection.needs_conservative_projection)
    {
        let boundaries = compatible_boundary_points(connection, query).await?;
        for (gap, projection) in gaps.iter().zip(&mut pending) {
            if !projection.needs_conservative_projection {
                continue;
            }
            let (prior, next) = surrounding_boundary_blocks(&boundaries, gap);
            if let Some(interval) = conservative_gap_interval(prior, next, start, end)? {
                projection.unsafe_intervals.push(interval);
            }
        }
    }

    Ok(pending
        .into_iter()
        .enumerate()
        .filter_map(|(gap_index, projection)| {
            (!projection.unsafe_intervals.is_empty()).then_some(GapProjection {
                gap_index,
                unsafe_intervals: merge_intervals(projection.unsafe_intervals),
            })
        })
        .collect())
}

fn pending_gap_projection(
    gap: &GapRow,
    start: u64,
    end: u64,
    includes_block_backend: bool,
    includes_rfq: bool,
) -> anyhow::Result<PendingGapProjection> {
    let block_bounds = ordered_bounds(gap.from_block_number, gap.to_block_number);
    let rfq_time_bounds = ordered_bounds(gap.from_observed_at_ms, gap.to_observed_at_ms);
    let mut projection = PendingGapProjection::default();

    if includes_block_backend {
        match (gap.from_block_number, gap.to_block_number) {
            (Some(from), Some(to)) if from <= to => {
                push_clipped(&mut projection.unsafe_intervals, from, to, start, end)?;
            }
            (None, None) if rfq_time_bounds.is_some() => {}
            _ => projection.needs_conservative_projection = true,
        }
    }
    if includes_rfq {
        match (gap.from_observed_at_ms, gap.to_observed_at_ms) {
            (Some(from), Some(to)) if from <= to => {
                projection.rfq_time_bounds = Some((from, to));
            }
            (None, None) if block_bounds.is_some() => {}
            _ => projection.needs_conservative_projection = true,
        }
    }

    Ok(projection)
}

fn ordered_bounds(from: Option<u64>, to: Option<u64>) -> Option<(u64, u64)> {
    from.zip(to).filter(|(from, to)| from <= to)
}

async fn compatible_boundary_points(
    connection: &mut sqlx::PgConnection,
    query: &CoverageQuery,
) -> anyhow::Result<Vec<BoundaryPoint>> {
    let rows = sqlx::query(
        "SELECT generation, message_seq, block_number, backends
         FROM state_history.checkpoints
         WHERE chain_id = $1 AND status = 'complete' AND kind = 'boundary'
           AND backends @> $2::text[]
         ORDER BY generation, message_seq",
    )
    .bind(database_i64(query.chain_id, "coverage chain_id")?)
    .bind(database_backends(&query.backends))
    .fetch_all(connection)
    .await?;

    rows.into_iter()
        .map(|row| {
            let backend_values: Vec<String> = row.try_get("backends")?;
            backend_values
                .into_iter()
                .map(|backend| backend.parse::<Backend>())
                .collect::<Result<Vec<_>, _>>()?;
            Ok(BoundaryPoint {
                position: StreamPosition {
                    generation: database_u64(row.try_get("generation")?, "boundary generation")?,
                    message_seq: database_u64(row.try_get("message_seq")?, "boundary message_seq")?,
                },
                block_number: database_u64(row.try_get("block_number")?, "boundary block")?,
            })
        })
        .collect()
}

fn surrounding_boundary_blocks(
    boundaries: &[BoundaryPoint],
    gap: &GapRow,
) -> (Option<u64>, Option<u64>) {
    let from = StreamPosition {
        generation: gap.generation,
        message_seq: gap.from_message_seq,
    };
    let to = StreamPosition {
        generation: gap.generation,
        message_seq: gap.to_message_seq,
    };
    let prior_index = boundaries.partition_point(|point| point.position < from);
    let next_index = boundaries.partition_point(|point| point.position <= to);
    let prior = prior_index
        .checked_sub(1)
        .map(|index| boundaries[index].block_number);
    let next = boundaries.get(next_index).map(|point| point.block_number);
    (prior, next)
}

fn conservative_gap_interval(
    prior: Option<u64>,
    next: Option<u64>,
    start: u64,
    end: u64,
) -> anyhow::Result<Option<BlockInterval>> {
    if matches!((prior, next), (Some(prior), Some(next)) if prior > next) {
        // Stream and block order disagree, so the gap's block reach is unknowable.
        return Ok(Some(BlockInterval::new(start, end)?));
    }

    let unsafe_start = prior.unwrap_or(start).max(start);
    let unsafe_end = next.map_or(end, |block| block.saturating_sub(1)).min(end);
    Ok((unsafe_start <= unsafe_end)
        .then(|| BlockInterval::new(unsafe_start, unsafe_end))
        .transpose()?)
}

async fn batch_rfq_gap_intervals(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    bounds: &[(usize, u64, u64)],
    start: u64,
    end: u64,
) -> anyhow::Result<BTreeMap<usize, BlockInterval>> {
    if bounds.is_empty() || start > end {
        return Ok(BTreeMap::new());
    }

    let gap_indices = bounds
        .iter()
        .map(|(gap_index, _, _)| {
            i64::try_from(*gap_index).context("coverage RFQ gap index exceeds PostgreSQL bigint")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let from_ms = bounds
        .iter()
        .map(|(_, from_ms, _)| database_i64(*from_ms, "coverage RFQ gap start"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let to_ms = bounds
        .iter()
        .map(|(_, _, to_ms)| database_i64(*to_ms, "coverage RFQ gap end"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    // Aggregate the global matching hull before clipping, matching the legacy fail-closed behavior
    // when block-time pairs are missing or non-contiguous.
    let rows = sqlx::query(
        "WITH requested_gap(gap_index, from_ms, to_ms) AS (
             SELECT * FROM unnest($2::bigint[], $3::bigint[], $4::bigint[])
         )
         SELECT requested_gap.gap_index,
                min(current.block_number) AS start_block,
                max(current.block_number) AS end_block
         FROM requested_gap
         JOIN state_history.block_times current
           ON current.chain_id = $1
         JOIN state_history.block_times successor
           ON successor.chain_id = current.chain_id
          AND successor.block_number = current.block_number + 1
          AND successor.timestamp_ms > requested_gap.from_ms
         WHERE current.timestamp_ms <= requested_gap.to_ms
         GROUP BY requested_gap.gap_index
         ORDER BY requested_gap.gap_index",
    )
    .bind(database_i64(chain_id, "coverage chain_id")?)
    .bind(gap_indices)
    .bind(from_ms)
    .bind(to_ms)
    .fetch_all(connection)
    .await
    .context("failed to batch project coverage RFQ gaps")?;

    let mut projected = BTreeMap::new();
    for row in rows {
        let gap_index = usize::try_from(database_u64(
            row.try_get("gap_index")?,
            "coverage RFQ gap index",
        )?)
        .context("coverage RFQ gap index exceeds usize")?;
        let projected_start =
            database_u64(row.try_get("start_block")?, "coverage RFQ gap block start")?;
        let projected_end = database_u64(row.try_get("end_block")?, "coverage RFQ gap block end")?;
        let clipped_start = projected_start.max(start);
        let clipped_end = projected_end.min(end);
        if clipped_start > clipped_end {
            continue;
        }
        let interval = BlockInterval::new(clipped_start, clipped_end)?;
        if projected.insert(gap_index, interval).is_some() {
            anyhow::bail!("RFQ gap projection returned a duplicate gap index");
        }
    }
    Ok(projected)
}

async fn coverage_block_analysis(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    start: u64,
    end: u64,
    includes_rfq: bool,
) -> anyhow::Result<CoverageBlockAnalysis> {
    if start > end {
        return Ok(CoverageBlockAnalysis::default());
    }
    let fetch_end = if includes_rfq {
        end.saturating_add(1)
    } else {
        end
    };
    let rows = coverage_block_rows(connection, chain_id, start, fetch_end).await?;
    analyze_coverage_blocks(&rows, start, end, includes_rfq)
}

async fn coverage_block_rows(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    start: u64,
    end: u64,
) -> anyhow::Result<Vec<CoverageBlockRow>> {
    let rows = sqlx::query(
        "SELECT block_number, block_hash, parent_hash
         FROM state_history.block_times
         WHERE chain_id = $1 AND block_number BETWEEN $2 AND $3
         ORDER BY block_number",
    )
    .bind(database_i64(chain_id, "coverage block chain_id")?)
    .bind(database_i64(start, "coverage block start")?)
    .bind(database_i64(end, "coverage block end")?)
    .fetch_all(connection)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(CoverageBlockRow {
                block_number: database_u64(row.try_get("block_number")?, "coverage block")?,
                block_hash: row.try_get("block_hash")?,
                parent_hash: row.try_get("parent_hash")?,
            })
        })
        .collect()
}

fn analyze_coverage_blocks(
    rows: &[CoverageBlockRow],
    start: u64,
    end: u64,
    includes_rfq: bool,
) -> anyhow::Result<CoverageBlockAnalysis> {
    let missing_rfq_boundary_intervals = if includes_rfq {
        missing_rfq_intervals_from_rows(rows, start, end)
    } else {
        Vec::new()
    };
    Ok(CoverageBlockAnalysis {
        lineage_invalid_intervals: lineage_intervals_from_rows(rows, start, end)?,
        missing_rfq_boundary_intervals,
    })
}

fn lineage_intervals_from_rows(
    rows: &[CoverageBlockRow],
    start: u64,
    end: u64,
) -> anyhow::Result<Vec<BlockInterval>> {
    let mut invalid = Vec::new();
    let mut previous: Option<(u64, &str)> = None;
    let mut expected_block = start;
    for row in rows {
        let block = row.block_number;
        if block > end {
            break;
        }
        let hash = row.block_hash.as_deref();
        let parent = row.parent_hash.as_deref();
        let had_missing_predecessor = block > expected_block;
        if had_missing_predecessor {
            invalid.push(BlockInterval::new(expected_block, block - 1)?);
            previous = None;
        }
        let is_invalid = match (&previous, hash, parent) {
            (_, None, _) | (_, _, None) => true,
            (Some((previous_block, previous_hash)), Some(_), Some(parent_hash)) => {
                block != previous_block.saturating_add(1) || parent_hash != *previous_hash
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

fn missing_rfq_intervals_from_rows(
    rows: &[CoverageBlockRow],
    start: u64,
    end: u64,
) -> Vec<BlockInterval> {
    let missing = rows
        .iter()
        .enumerate()
        .filter(|(_, row)| row.block_number >= start && row.block_number <= end)
        .filter(|(index, row)| {
            row.block_number.checked_add(1).is_none_or(|successor| {
                rows.get(index + 1)
                    .is_none_or(|next| next.block_number != successor)
            })
        })
        .map(|(_, row)| BlockInterval {
            start: row.block_number,
            end_inclusive: row.block_number,
        })
        .collect();
    merge_intervals(missing)
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
    let rows = coverage_block_rows(connection, chain_id, start, end).await?;
    lineage_intervals_from_rows(&rows, start, end)
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

#[cfg(test)]
mod tests {
    use super::{
        analyze_coverage_blocks, conservative_gap_interval, pending_gap_projection,
        surrounding_boundary_blocks, BlockInterval, BoundaryPoint, CoverageBlockRow, GapRow,
        StreamPosition,
    };

    fn block(block_number: u64) -> CoverageBlockRow {
        CoverageBlockRow {
            block_number,
            block_hash: Some(format!("hash-{block_number}")),
            parent_hash: Some(format!("hash-{}", block_number.saturating_sub(1))),
        }
    }

    fn gap(from_message_seq: u64, to_message_seq: u64) -> GapRow {
        GapRow {
            generation: 1,
            from_message_seq,
            to_message_seq,
            reason: "write_failed".to_string(),
            from_block_number: None,
            to_block_number: None,
            from_observed_at_ms: None,
            to_observed_at_ms: None,
        }
    }

    #[test]
    fn combined_analysis_marks_lineage_hole_and_missing_rfq_successor() -> anyhow::Result<()> {
        let rows = vec![block(100), block(102), block(103)];
        let analysis = analyze_coverage_blocks(&rows, 100, 102, true)?;

        assert_eq!(
            analysis.lineage_invalid_intervals,
            vec![BlockInterval::new(101, 102)?]
        );
        assert_eq!(
            analysis.missing_rfq_boundary_intervals,
            vec![BlockInterval::new(100, 100)?]
        );
        Ok(())
    }

    #[test]
    fn combined_analysis_checks_successor_beyond_requested_end() -> anyhow::Result<()> {
        let rows = vec![block(100), block(101)];
        let analysis = analyze_coverage_blocks(&rows, 100, 101, true)?;

        assert!(analysis.lineage_invalid_intervals.is_empty());
        assert_eq!(
            analysis.missing_rfq_boundary_intervals,
            vec![BlockInterval::new(101, 101)?]
        );
        Ok(())
    }

    #[test]
    fn combined_analysis_accepts_successor_beyond_requested_end() -> anyhow::Result<()> {
        let mut successor = block(102);
        successor.block_hash = None;
        successor.parent_hash = None;
        let rows = vec![block(100), block(101), successor];
        let analysis = analyze_coverage_blocks(&rows, 100, 101, true)?;

        assert!(analysis.lineage_invalid_intervals.is_empty());
        assert!(analysis.missing_rfq_boundary_intervals.is_empty());
        Ok(())
    }

    #[test]
    fn combined_analysis_skips_successor_checks_without_rfq() -> anyhow::Result<()> {
        let rows = vec![block(100), block(101)];
        let analysis = analyze_coverage_blocks(&rows, 100, 101, false)?;

        assert!(analysis.lineage_invalid_intervals.is_empty());
        assert!(analysis.missing_rfq_boundary_intervals.is_empty());
        Ok(())
    }

    #[test]
    fn reversed_gap_bounds_require_conservative_projection() -> anyhow::Result<()> {
        let mut gap = gap(10, 20);
        gap.from_block_number = Some(110);
        gap.to_block_number = Some(105);
        gap.from_observed_at_ms = Some(110_000);
        gap.to_observed_at_ms = Some(105_000);

        let projection = pending_gap_projection(&gap, 100, 120, true, true)?;

        assert!(projection.unsafe_intervals.is_empty());
        assert!(projection.rfq_time_bounds.is_none());
        assert!(projection.needs_conservative_projection);
        Ok(())
    }

    #[test]
    fn inconsistent_boundary_order_falls_back_to_the_full_range() -> anyhow::Result<()> {
        assert_eq!(
            conservative_gap_interval(Some(115), Some(105), 100, 120)?,
            Some(BlockInterval::new(100, 120)?)
        );
        Ok(())
    }

    #[test]
    fn equal_boundary_blocks_preserve_the_empty_legacy_span() -> anyhow::Result<()> {
        assert_eq!(
            conservative_gap_interval(Some(110), Some(110), 100, 120)?,
            None
        );
        Ok(())
    }

    #[test]
    fn surrounding_boundaries_are_strict_and_reusable() {
        let boundaries = [
            BoundaryPoint {
                position: StreamPosition {
                    generation: 1,
                    message_seq: 9,
                },
                block_number: 104,
            },
            BoundaryPoint {
                position: StreamPosition {
                    generation: 1,
                    message_seq: 10,
                },
                block_number: 105,
            },
            BoundaryPoint {
                position: StreamPosition {
                    generation: 1,
                    message_seq: 20,
                },
                block_number: 115,
            },
            BoundaryPoint {
                position: StreamPosition {
                    generation: 1,
                    message_seq: 21,
                },
                block_number: 116,
            },
            BoundaryPoint {
                position: StreamPosition {
                    generation: 1,
                    message_seq: 30,
                },
                block_number: 125,
            },
            BoundaryPoint {
                position: StreamPosition {
                    generation: 1,
                    message_seq: 31,
                },
                block_number: 126,
            },
        ];

        assert_eq!(
            surrounding_boundary_blocks(&boundaries, &gap(10, 20)),
            (Some(104), Some(116))
        );
        assert_eq!(
            surrounding_boundary_blocks(&boundaries, &gap(20, 30)),
            (Some(105), Some(126))
        );
    }
}
