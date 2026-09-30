use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::Context;
use serde_json::value::RawValue;
use sha2::Digest;
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterEnvelope, BroadcasterPayload, BroadcasterProtocolMessage,
    BroadcasterProtocolSyncStatus, BroadcasterSnapshotChunk, BroadcasterSnapshotEnd,
    BroadcasterSnapshotPartition, BroadcasterSnapshotStart, BroadcasterUpdateMessage,
    BroadcasterUpdatePartition,
};
use sqlx::PgPool;
use state_history::{
    encode_archive, ArchiveMetadata, Backend, BlockInterval, CheckpointArchive, CheckpointKind,
    CheckpointManifest, CheckpointPairQuery, CheckpointPairSelection, CheckpointStatus,
    CoverageQuery, ExplicitCheckpointPair, PositionRangeQuery, RawSnapshotError, RawSnapshotQuery,
    ReadLimitError, ReadLimits, StateHistoryReader, StoredPayloadError, StreamPosition,
    TargetPlanQuery, TokenAnchor, ARCHIVE_SCHEMA_VERSION,
};
use tycho_simulation::{
    tycho_client::feed::{
        synchronizer::{ComponentWithState, Snapshot, StateSyncMessage},
        BlockHeader, SynchronizerState,
    },
    tycho_common::{
        models::{
            blockchain::BlockAggregatedChanges,
            protocol::{ProtocolComponent, ProtocolComponentState, ProtocolComponentStateDelta},
            Chain,
        },
        Bytes,
    },
};

const CHAIN_ID: u64 = 8453;

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn existing_delta_rows_are_format_one(pool: PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO state_history.deltas
            (chain_id, generation, message_seq, block_number, observed_at_ms, payload, payload_sha256)
         VALUES ($1, 1, 1, 100, 100000, $2, $3)",
    )
    .bind(database_i64(CHAIN_ID)?)
    .bind(zstd::stream::encode_all(br#"{}"#.as_slice(), 3)?)
    .bind(hex::encode(sha2::Sha256::digest(br#"{}"#)))
    .execute(&pool)
    .await?;

    let (schema_version, payload_format_version): (i32, i16) = sqlx::query_as(
        "SELECT
            (SELECT version FROM state_history.schema_meta),
            (SELECT payload_format_version FROM state_history.deltas LIMIT 1)",
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(schema_version, 2);
    assert_eq!(payload_format_version, 1);
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn coverage_splits_on_gap_and_reports_cutoff(pool: PgPool) -> anyhow::Result<()> {
    seed_block_times(&pool, 100, 121).await?;
    seed_checkpoint(&pool, 1, 0, 100, CheckpointKind::Boundary, true).await?;
    seed_delta(&pool, 1, 1, 120, 1, &[Backend::Native]).await?;
    seed_gap(&pool, 1, 2, 3, Some((108, 110)), None).await?;

    let reader = StateHistoryReader::new(pool, object_store().await);
    let snapshot = reader
        .coverage(&CoverageQuery::new(
            CHAIN_ID,
            vec![Backend::Native],
            Some(BlockInterval::new(100, 120)?),
        )?)
        .await?;

    assert_eq!(snapshot.visible_through_block, 120);
    assert_eq!(
        snapshot.continuous_intervals,
        vec![BlockInterval::new(100, 107)?, BlockInterval::new(111, 120)?]
    );
    assert_eq!(snapshot.known_gaps.len(), 1);
    assert_eq!(snapshot.known_gaps[0].from_block, Some(108));
    assert_eq!(snapshot.known_gaps[0].to_block_inclusive, Some(110));
    assert_eq!(snapshot.known_gaps[0].from_position, Some(position(1, 2)));
    assert_eq!(snapshot.known_gaps[0].to_position, Some(position(1, 3)));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn bounded_native_coverage_reports_only_intersecting_unclipped_gaps(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_block_times(&pool, 100, 121).await?;
    seed_checkpoint(&pool, 1, 0, 100, CheckpointKind::Boundary, true).await?;
    seed_delta(&pool, 1, 40, 120, 1, &[Backend::Native]).await?;
    seed_gap(&pool, 1, 2, 3, Some((90, 110)), None).await?;
    seed_gap(&pool, 1, 4, 5, Some((1_000, 1_010)), None).await?;

    let snapshot = StateHistoryReader::new(pool, object_store().await)
        .coverage(&CoverageQuery::new(
            CHAIN_ID,
            vec![Backend::Native],
            Some(BlockInterval::new(100, 120)?),
        )?)
        .await?;

    assert_eq!(snapshot.known_gaps.len(), 1);
    assert_eq!(snapshot.known_gaps[0].from_block, Some(90));
    assert_eq!(snapshot.known_gaps[0].to_block_inclusive, Some(110));
    assert_eq!(
        snapshot.continuous_intervals,
        vec![BlockInterval::new(111, 120)?]
    );
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn bounded_rfq_coverage_reports_only_time_gaps_mapped_into_the_range(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_block_times(&pool, 100, 121).await?;
    seed_checkpoint_with_backends(
        &pool,
        1,
        0,
        100,
        CheckpointKind::Boundary,
        false,
        &[Backend::Rfq],
    )
    .await?;
    seed_delta(&pool, 1, 40, 120, 1, &[Backend::Rfq]).await?;
    seed_gap(&pool, 1, 2, 3, None, Some((108_000, 110_000))).await?;
    seed_gap(&pool, 1, 4, 5, None, Some((1_000_000, 1_010_000))).await?;

    let snapshot = StateHistoryReader::new(pool, object_store().await)
        .coverage(&CoverageQuery::new(
            CHAIN_ID,
            vec![Backend::Rfq],
            Some(BlockInterval::new(100, 120)?),
        )?)
        .await?;

    assert_eq!(snapshot.known_gaps.len(), 1);
    assert_eq!(snapshot.known_gaps[0].from_observed_at_ms, Some(108_000));
    assert_eq!(snapshot.known_gaps[0].to_observed_at_ms, Some(110_000));
    assert_eq!(
        snapshot.continuous_intervals,
        vec![BlockInterval::new(100, 107)?, BlockInterval::new(111, 120)?]
    );
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn bounded_rfq_coverage_preserves_the_global_projection_hull(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_block_times(&pool, 90, 131).await?;
    sqlx::query(
        "UPDATE state_history.block_times
         SET timestamp_ms = CASE block_number
             WHEN 99 THEN 1500
             WHEN 100 THEN 3000
             WHEN 121 THEN 1500
             WHEN 122 THEN 3000
         END
         WHERE chain_id = $1 AND block_number IN (99, 100, 121, 122)",
    )
    .bind(database_i64(CHAIN_ID)?)
    .execute(&pool)
    .await?;
    seed_checkpoint_with_backends(
        &pool,
        1,
        0,
        100,
        CheckpointKind::Boundary,
        false,
        &[Backend::Rfq],
    )
    .await?;
    seed_gap(&pool, 1, 2, 3, None, Some((1_000, 2_000))).await?;

    let snapshot = StateHistoryReader::new(pool, object_store().await)
        .coverage(&CoverageQuery::new(
            CHAIN_ID,
            vec![Backend::Rfq],
            Some(BlockInterval::new(100, 120)?),
        )?)
        .await?;

    assert!(snapshot.continuous_intervals.is_empty());
    assert_eq!(snapshot.known_gaps.len(), 1);
    assert_eq!(snapshot.known_gaps[0].from_observed_at_ms, Some(1_000));
    assert_eq!(snapshot.known_gaps[0].to_observed_at_ms, Some(2_000));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn mixed_coverage_reports_each_intersecting_gap_once(pool: PgPool) -> anyhow::Result<()> {
    seed_block_times(&pool, 100, 121).await?;
    seed_checkpoint_with_backends(
        &pool,
        1,
        0,
        100,
        CheckpointKind::Boundary,
        true,
        &[Backend::Native, Backend::Rfq],
    )
    .await?;
    seed_delta(&pool, 1, 40, 120, 1, &[Backend::Native, Backend::Rfq]).await?;
    seed_gap(&pool, 1, 2, 2, Some((104, 105)), Some((104_000, 105_000))).await?;
    seed_gap(&pool, 1, 4, 4, Some((108, 108)), None).await?;
    seed_gap(&pool, 1, 6, 6, None, Some((112_000, 112_000))).await?;
    seed_gap(&pool, 1, 8, 8, Some((1_000, 1_001)), None).await?;

    let snapshot = StateHistoryReader::new(pool, object_store().await)
        .coverage(&CoverageQuery::new(
            CHAIN_ID,
            vec![Backend::Native, Backend::Rfq],
            Some(BlockInterval::new(100, 120)?),
        )?)
        .await?;

    assert_eq!(snapshot.known_gaps.len(), 3);
    assert_eq!(
        snapshot
            .known_gaps
            .iter()
            .map(|gap| gap.from_position)
            .collect::<Vec<_>>(),
        vec![
            Some(position(1, 2)),
            Some(position(1, 4)),
            Some(position(1, 6))
        ]
    );
    assert_eq!(
        snapshot.continuous_intervals,
        vec![
            BlockInterval::new(100, 103)?,
            BlockInterval::new(106, 107)?,
            BlockInterval::new(109, 111)?,
            BlockInterval::new(113, 120)?,
        ]
    );
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn coverage_starts_at_the_first_retained_checkpoint(pool: PgPool) -> anyhow::Result<()> {
    seed_block_times(&pool, 90, 110).await?;
    seed_checkpoint(&pool, 1, 0, 100, CheckpointKind::Boundary, true).await?;
    seed_delta(&pool, 1, 1, 110, 1, &[Backend::Native]).await?;

    let reader = StateHistoryReader::new(pool, object_store().await);
    let snapshot = reader
        .coverage(&CoverageQuery::new(
            CHAIN_ID,
            vec![Backend::Native],
            Some(BlockInterval::new(90, 110)?),
        )?)
        .await?;

    assert_eq!(
        snapshot.continuous_intervals,
        vec![BlockInterval::new(100, 110)?]
    );
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn rfq_coverage_excludes_block_without_next_boundary(pool: PgPool) -> anyhow::Result<()> {
    seed_block_times(&pool, 100, 102).await?;
    sqlx::query(
        "DELETE FROM state_history.block_times
         WHERE chain_id = $1 AND block_number = 101",
    )
    .bind(database_i64(CHAIN_ID)?)
    .execute(&pool)
    .await?;
    seed_checkpoint_with_backends(
        &pool,
        1,
        0,
        100,
        CheckpointKind::Boundary,
        false,
        &[Backend::Rfq],
    )
    .await?;

    let reader = StateHistoryReader::new(pool, object_store().await);
    let snapshot = reader
        .coverage(&CoverageQuery::new(
            CHAIN_ID,
            vec![Backend::Rfq],
            Some(BlockInterval::new(100, 101)?),
        )?)
        .await?;

    assert!(snapshot.continuous_intervals.is_empty());
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn mixed_coverage_excludes_a_lineage_hole_and_its_missing_rfq_successor(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_block_times(&pool, 100, 103).await?;
    sqlx::query(
        "DELETE FROM state_history.block_times
         WHERE chain_id = $1 AND block_number = 101",
    )
    .bind(database_i64(CHAIN_ID)?)
    .execute(&pool)
    .await?;
    seed_checkpoint_with_backends(
        &pool,
        1,
        0,
        100,
        CheckpointKind::Boundary,
        true,
        &[Backend::Native, Backend::Rfq],
    )
    .await?;
    seed_delta(&pool, 1, 1, 102, 1, &[Backend::Native, Backend::Rfq]).await?;

    let snapshot = StateHistoryReader::new(pool, object_store().await)
        .coverage(&CoverageQuery::new(
            CHAIN_ID,
            vec![Backend::Native, Backend::Rfq],
            Some(BlockInterval::new(100, 102)?),
        )?)
        .await?;

    assert!(snapshot.continuous_intervals.is_empty());
    assert!(snapshot.known_gaps.is_empty());
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn cursorless_gap_uses_its_full_span_for_boundary_projection(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_block_times(&pool, 100, 120).await?;
    seed_checkpoint(&pool, 1, 0, 100, CheckpointKind::Boundary, true).await?;
    seed_checkpoint(&pool, 1, 10, 105, CheckpointKind::Boundary, true).await?;
    seed_checkpoint(&pool, 1, 30, 115, CheckpointKind::Boundary, true).await?;
    seed_delta(&pool, 1, 31, 120, 1, &[Backend::Native]).await?;
    seed_gap(&pool, 1, 2, 20, None, None).await?;

    let reader = StateHistoryReader::new(pool, object_store().await);
    let snapshot = reader
        .coverage(&CoverageQuery::new(
            CHAIN_ID,
            vec![Backend::Native],
            Some(BlockInterval::new(100, 120)?),
        )?)
        .await?;

    assert_eq!(
        snapshot.continuous_intervals,
        vec![BlockInterval::new(115, 120)?]
    );
    assert_eq!(snapshot.known_gaps.len(), 1);
    assert_eq!(snapshot.known_gaps[0].from_block, None);
    assert_eq!(snapshot.known_gaps[0].from_observed_at_ms, None);
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn one_sided_gap_uses_conservative_boundary_projection(pool: PgPool) -> anyhow::Result<()> {
    seed_block_times(&pool, 100, 120).await?;
    seed_checkpoint(&pool, 1, 0, 100, CheckpointKind::Boundary, true).await?;
    seed_checkpoint(&pool, 1, 10, 105, CheckpointKind::Boundary, true).await?;
    seed_checkpoint(&pool, 1, 30, 115, CheckpointKind::Boundary, true).await?;
    seed_delta(&pool, 1, 40, 120, 1, &[Backend::Native]).await?;
    seed_gap_with_cursors(
        &pool,
        1,
        12,
        20,
        GapCursors {
            from_block: Some(106),
            ..GapCursors::default()
        },
    )
    .await?;
    seed_gap(&pool, 1, 32, 32, None, Some((118_000, 118_000))).await?;

    let snapshot = StateHistoryReader::new(pool, object_store().await)
        .coverage(&CoverageQuery::new(
            CHAIN_ID,
            vec![Backend::Native],
            Some(BlockInterval::new(100, 120)?),
        )?)
        .await?;

    assert_eq!(snapshot.known_gaps.len(), 1);
    assert_eq!(snapshot.known_gaps[0].from_block, Some(106));
    assert_eq!(snapshot.known_gaps[0].to_block_inclusive, None);
    assert_eq!(
        snapshot.continuous_intervals,
        vec![BlockInterval::new(100, 104)?, BlockInterval::new(115, 120)?]
    );
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn coverage_ignores_gaps_fully_identified_for_another_backend(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_block_times(&pool, 100, 121).await?;
    seed_checkpoint_with_backends(
        &pool,
        1,
        0,
        100,
        CheckpointKind::Boundary,
        true,
        &[Backend::Native, Backend::Rfq],
    )
    .await?;
    seed_delta(&pool, 1, 40, 120, 1, &[Backend::Native, Backend::Rfq]).await?;
    seed_gap(&pool, 1, 2, 2, Some((108, 108)), None).await?;
    seed_gap(&pool, 1, 4, 4, None, Some((112_000, 112_000))).await?;

    let reader = StateHistoryReader::new(pool, object_store().await);
    let native = reader
        .coverage(&CoverageQuery::new(
            CHAIN_ID,
            vec![Backend::Native],
            Some(BlockInterval::new(100, 120)?),
        )?)
        .await?;
    let rfq = reader
        .coverage(&CoverageQuery::new(
            CHAIN_ID,
            vec![Backend::Rfq],
            Some(BlockInterval::new(100, 120)?),
        )?)
        .await?;

    assert_eq!(native.known_gaps.len(), 1);
    assert_eq!(native.known_gaps[0].from_position, Some(position(1, 2)));
    assert_eq!(rfq.known_gaps.len(), 1);
    assert_eq!(rfq.known_gaps[0].from_position, Some(position(1, 4)));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn target_plan_returns_every_required_next_block_time(pool: PgPool) -> anyhow::Result<()> {
    seed_block_times(&pool, 100, 111).await?;
    seed_checkpoint_with_backends(
        &pool,
        1,
        0,
        100,
        CheckpointKind::Boundary,
        true,
        &[Backend::Native, Backend::Rfq],
    )
    .await?;
    seed_delta(&pool, 1, 1, 110, 1, &[Backend::Native, Backend::Rfq]).await?;

    let reader = StateHistoryReader::new(pool, object_store().await);
    let plan = reader
        .plan_targets(&TargetPlanQuery::new(
            CHAIN_ID,
            vec![Backend::Native, Backend::Rfq],
            BTreeSet::from([100, 110]),
            ReadLimits::new(1_000_000, 1_000_000)?,
        )?)
        .await?;

    assert_eq!(
        plan.block_times.keys().copied().collect::<Vec<_>>(),
        vec![100, 101, 110, 111]
    );
    assert!(plan.lineage_invalid_intervals.is_empty());
    assert_eq!(plan.visible_through_block, 110);
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn target_plan_accepts_rfq_checkpoint_before_next_block(pool: PgPool) -> anyhow::Result<()> {
    seed_block_times(&pool, 100, 101).await?;
    let checkpoint = seed_checkpoint_with_backends(
        &pool,
        1,
        1,
        100,
        CheckpointKind::Boundary,
        false,
        &[Backend::Rfq],
    )
    .await?;
    sqlx::query(
        "UPDATE state_history.checkpoints
         SET rfq_observed_at_ms = 100500
         WHERE id = $1",
    )
    .bind(checkpoint.id)
    .execute(&pool)
    .await?;

    let reader = StateHistoryReader::new(pool, object_store().await);
    let plan = reader
        .plan_targets(&TargetPlanQuery::new(
            CHAIN_ID,
            vec![Backend::Rfq],
            BTreeSet::from([100]),
            ReadLimits::new(1_000_000, 1_000_000)?,
        )?)
        .await?;

    assert_eq!(plan.legs.len(), 1);
    assert_eq!(plan.legs[0].checkpoint.id, checkpoint.id);
    assert_eq!(plan.legs[0].checkpoint.rfq_observed_at_ms, Some(100_500));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn target_plan_keeps_safe_cells_when_one_block_time_is_missing(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_block_times(&pool, 100, 102).await?;
    sqlx::query(
        "DELETE FROM state_history.block_times
         WHERE chain_id = $1 AND block_number = 101",
    )
    .bind(database_i64(CHAIN_ID)?)
    .execute(&pool)
    .await?;
    seed_checkpoint(&pool, 1, 0, 100, CheckpointKind::Boundary, true).await?;
    seed_delta(&pool, 1, 1, 102, 1, &[Backend::Native]).await?;

    let reader = StateHistoryReader::new(pool, object_store().await);
    let plan = reader
        .plan_targets(&TargetPlanQuery::new(
            CHAIN_ID,
            vec![Backend::Native],
            BTreeSet::from([100, 101, 102]),
            ReadLimits::new(1_000_000, 1_000_000)?,
        )?)
        .await?;

    assert_eq!(plan.missing_block_times, BTreeSet::from([101]));
    assert!(plan.block_times.contains_key(&100));
    assert!(plan.block_times.contains_key(&102));
    assert!(!plan.legs.is_empty());
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn target_plan_marks_broken_canonical_lineage(pool: PgPool) -> anyhow::Result<()> {
    seed_block_times(&pool, 100, 111).await?;
    sqlx::query(
        "UPDATE state_history.block_times
         SET parent_hash = 'wrong-parent'
         WHERE chain_id = $1 AND block_number = 110",
    )
    .bind(database_i64(CHAIN_ID)?)
    .execute(&pool)
    .await?;
    seed_checkpoint(&pool, 1, 0, 100, CheckpointKind::Boundary, true).await?;
    seed_delta(&pool, 1, 1, 110, 1, &[Backend::Native]).await?;

    let reader = StateHistoryReader::new(pool, object_store().await);
    let plan = reader
        .plan_targets(&TargetPlanQuery::new(
            CHAIN_ID,
            vec![Backend::Native],
            BTreeSet::from([100, 110]),
            ReadLimits::new(1_000_000, 1_000_000)?,
        )?)
        .await?;

    assert_eq!(
        plan.lineage_invalid_intervals,
        vec![BlockInterval::new(110, 110)?]
    );
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn token_anchor_stays_inside_the_selected_segment(pool: PgPool) -> anyhow::Result<()> {
    let first_boundary = seed_checkpoint(&pool, 1, 0, 100, CheckpointKind::Boundary, true).await?;
    let interval = seed_checkpoint(&pool, 1, 10, 110, CheckpointKind::Interval, false).await?;
    let second_boundary =
        seed_checkpoint(&pool, 2, 0, 120, CheckpointKind::Boundary, false).await?;

    let reader = StateHistoryReader::new(pool, object_store().await);
    assert_eq!(
        reader.select_token_anchor(&interval).await?,
        TokenAnchor::Available {
            checkpoint: Box::new(first_boundary),
            used_fallback: true,
        }
    );
    assert_eq!(
        reader.select_token_anchor(&second_boundary).await?,
        TokenAnchor::Missing
    );
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
#[expect(
    clippy::expect_used,
    reason = "the rejected explicit pair is the fixture outcome"
)]
async fn checkpoint_pairs_are_adjacent_and_never_cross_segments(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_checkpoint(&pool, 1, 0, 100, CheckpointKind::Boundary, true).await?;
    seed_checkpoint(&pool, 1, 10, 110, CheckpointKind::Interval, true).await?;
    seed_checkpoint(&pool, 1, 20, 120, CheckpointKind::Interval, true).await?;
    seed_checkpoint(&pool, 2, 0, 200, CheckpointKind::Boundary, true).await?;
    seed_checkpoint(&pool, 2, 10, 210, CheckpointKind::Interval, true).await?;

    let reader = StateHistoryReader::new(pool, object_store().await);
    let pairs = reader
        .resolve_checkpoint_pairs(&CheckpointPairQuery::new(
            CHAIN_ID,
            CheckpointPairSelection::BlockRange(BlockInterval::new(100, 210)?),
        ))
        .await?;
    assert_eq!(pairs.len(), 3);
    assert_eq!(pairs[0].earlier.position, position(1, 0));
    assert_eq!(pairs[0].target.position, position(1, 10));
    assert_eq!(pairs[1].earlier.position, position(1, 10));
    assert_eq!(pairs[1].target.position, position(1, 20));
    assert_eq!(pairs[2].earlier.position, position(2, 0));
    assert_eq!(pairs[2].target.position, position(2, 10));

    let narrow_pairs = reader
        .resolve_checkpoint_pairs(&CheckpointPairQuery::new(
            CHAIN_ID,
            CheckpointPairSelection::BlockRange(BlockInterval::new(110, 120)?),
        ))
        .await?;
    assert_eq!(narrow_pairs.len(), 2);
    assert_eq!(narrow_pairs[0].earlier.position, position(1, 0));
    assert_eq!(narrow_pairs[0].target.position, position(1, 10));
    assert_eq!(narrow_pairs[1].earlier.position, position(1, 10));
    assert_eq!(narrow_pairs[1].target.position, position(1, 20));

    let bounded_pairs = reader
        .resolve_checkpoint_pairs(
            &CheckpointPairQuery::new(
                CHAIN_ID,
                CheckpointPairSelection::BlockRange(BlockInterval::new(100, 210)?),
            )
            .with_max_pairs(1),
        )
        .await?;
    assert_eq!(bounded_pairs.len(), 2);

    let error = reader
        .resolve_checkpoint_pairs(&CheckpointPairQuery::new(
            CHAIN_ID,
            CheckpointPairSelection::Explicit(vec![ExplicitCheckpointPair {
                earlier_position: position(1, 20),
                target_position: position(2, 0),
            }]),
        ))
        .await
        .expect_err("an explicit pair must not cross a segment boundary");
    assert!(error.to_string().contains("same segment"));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn backend_filter_keeps_incompatible_boundaries_as_segment_breaks(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_checkpoint(&pool, 1, 0, 100, CheckpointKind::Boundary, true).await?;
    seed_checkpoint(&pool, 1, 10, 110, CheckpointKind::Interval, true).await?;
    seed_checkpoint_with_backends(
        &pool,
        2,
        0,
        200,
        CheckpointKind::Boundary,
        false,
        &[Backend::Rfq],
    )
    .await?;
    seed_checkpoint(&pool, 2, 10, 210, CheckpointKind::Interval, true).await?;

    let reader = StateHistoryReader::new(pool, object_store().await);
    let pairs = reader
        .resolve_checkpoint_pairs(&CheckpointPairQuery::for_backends(
            CHAIN_ID,
            CheckpointPairSelection::BlockRange(BlockInterval::new(100, 210)?),
            vec![Backend::Native],
        ))
        .await?;

    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0].earlier.position, position(1, 0));
    assert_eq!(pairs[0].target.position, position(1, 10));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn backend_filter_projects_out_incompatible_intervals_before_pairing(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_checkpoint(&pool, 1, 0, 100, CheckpointKind::Boundary, true).await?;
    seed_checkpoint(&pool, 1, 10, 110, CheckpointKind::Interval, true).await?;
    seed_checkpoint_with_backends(
        &pool,
        1,
        20,
        120,
        CheckpointKind::Interval,
        false,
        &[Backend::Rfq],
    )
    .await?;
    seed_checkpoint(&pool, 1, 30, 130, CheckpointKind::Interval, true).await?;
    seed_checkpoint(&pool, 1, 40, 140, CheckpointKind::Interval, true).await?;

    let reader = StateHistoryReader::new(pool, object_store().await);
    let pairs = reader
        .resolve_checkpoint_pairs(&CheckpointPairQuery::for_backends(
            CHAIN_ID,
            CheckpointPairSelection::BlockRange(BlockInterval::new(130, 140)?),
            vec![Backend::Native],
        ))
        .await?;

    assert_eq!(pairs.len(), 2);
    assert_eq!(pairs[0].earlier.position, position(1, 10));
    assert_eq!(pairs[0].target.position, position(1, 30));
    assert_eq!(pairs[1].earlier.position, position(1, 30));
    assert_eq!(pairs[1].target.position, position(1, 40));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn range_pairs_remain_globally_adjacent_across_nonmonotonic_blocks(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_checkpoint(&pool, 1, 0, 100, CheckpointKind::Boundary, true).await?;
    seed_checkpoint(&pool, 1, 10, 110, CheckpointKind::Interval, true).await?;
    seed_checkpoint(&pool, 1, 15, 999, CheckpointKind::Interval, true).await?;
    seed_checkpoint(&pool, 1, 20, 120, CheckpointKind::Interval, true).await?;

    let reader = StateHistoryReader::new(pool, object_store().await);
    let pairs = reader
        .resolve_checkpoint_pairs(&CheckpointPairQuery::new(
            CHAIN_ID,
            CheckpointPairSelection::BlockRange(BlockInterval::new(110, 120)?),
        ))
        .await?;

    assert_eq!(pairs.len(), 2);
    assert_eq!(pairs[0].earlier.position, position(1, 0));
    assert_eq!(pairs[0].target.position, position(1, 10));
    assert_eq!(pairs[1].earlier.position, position(1, 15));
    assert_eq!(pairs[1].target.position, position(1, 20));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn checkpoint_pair_plan_accounts_for_fallback_token_anchor_once(
    pool: PgPool,
) -> anyhow::Result<()> {
    let fallback = seed_checkpoint(&pool, 1, 0, 100, CheckpointKind::Boundary, true).await?;
    let earlier = seed_checkpoint(&pool, 1, 10, 110, CheckpointKind::Interval, false).await?;
    let target = seed_checkpoint(&pool, 1, 20, 120, CheckpointKind::Interval, false).await?;

    let reader = StateHistoryReader::new(pool, object_store().await);
    let plan = reader
        .plan_checkpoint_pair(
            &state_history::CheckpointPair { earlier, target },
            vec![Backend::Native],
            ReadLimits::new(1_000, 2_100)?,
        )
        .await?;

    assert_eq!(
        plan.earlier_token_anchor,
        TokenAnchor::Available {
            checkpoint: Box::new(fallback.clone()),
            used_fallback: true,
        }
    );
    assert_eq!(
        plan.target_token_anchor,
        TokenAnchor::Available {
            checkpoint: Box::new(fallback),
            used_fallback: true,
        }
    );
    assert_eq!(plan.estimated_decoded_bytes, 2_100);
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
#[expect(
    clippy::expect_used,
    reason = "the preflight rejection is the fixture outcome"
)]
async fn checkpoint_limit_fails_before_object_download(pool: PgPool) -> anyhow::Result<()> {
    let mut manifest = manifest(1, 0, 100, CheckpointKind::Boundary, true);
    manifest.compressed_bytes = Some(101);
    manifest.archive_bytes = Some(1_000);
    let reader = StateHistoryReader::new(pool, object_store().await);

    let error = reader
        .fetch_checkpoint_with_limit(&manifest, ReadLimits::new(100, 2_000)?)
        .await
        .expect_err("the declared compressed size exceeds the limit");
    assert!(matches!(
        error,
        ReadLimitError::CompressedBytesExceeded {
            declared: 101,
            limit: 100
        }
    ));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
#[expect(
    clippy::expect_used,
    reason = "the unknown-format rejection is the fixture outcome"
)]
async fn unknown_delta_format_fails_closed(pool: PgPool) -> anyhow::Result<()> {
    seed_block_times(&pool, 100, 101).await?;
    seed_checkpoint(&pool, 1, 0, 100, CheckpointKind::Boundary, true).await?;
    seed_delta(&pool, 1, 1, 101, 99, &[Backend::Native]).await?;
    let reader = StateHistoryReader::new(pool, object_store().await);

    let error = reader
        .plan_targets(&TargetPlanQuery::new(
            CHAIN_ID,
            vec![Backend::Native],
            BTreeSet::from([100, 101]),
            ReadLimits::new(1_000_000, 1_000_000)?,
        )?)
        .await
        .expect_err("unknown delta formats must fail closed");
    assert!(error
        .to_string()
        .contains("unsupported delta payload format 99"));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn a_position_range_returns_the_stored_updates_between_its_ends(
    pool: PgPool,
) -> anyhow::Result<()> {
    // Stored out of order, and the latest one carries only RFQ.
    seed_delta(&pool, 1, 9, 109, 1, &[Backend::Native]).await?;
    seed_delta(&pool, 1, 12, 112, 1, &[Backend::Rfq]).await?;
    seed_delta(&pool, 1, 5, 105, 1, &[Backend::Native]).await?;
    seed_delta(&pool, 1, 8, 108, 1, &[Backend::Rfq]).await?;
    seed_delta(&pool, 1, 7, 107, 1, &[Backend::Native, Backend::Rfq]).await?;
    let reader = StateHistoryReader::new(pool, object_store().await);

    let range = reader
        .read_position_range(
            &PositionRangeQuery::new(
                CHAIN_ID,
                position(1, 5),
                position(1, 9),
                vec![Backend::Native],
            ),
            ReadLimits::unbounded(),
        )
        .await?;

    let positions = range
        .deltas
        .iter()
        .map(|delta| delta.position)
        .collect::<Vec<_>>();
    assert_eq!(positions, [position(1, 7), position(1, 9)]);
    let backends = range
        .deltas
        .iter()
        .map(|delta| {
            delta
                .applicable_backends
                .iter()
                .map(|cursor| cursor.backend)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(backends, [vec![Backend::Native], vec![Backend::Native]]);
    assert_eq!(
        range.deltas[0].raw_payload.get(),
        r#"{"generation":1,"messageSeq":7}"#
    );
    assert_eq!(range.last_stored, Some(position(1, 12)));
    assert!(range.gaps.is_empty() && range.boundaries.is_empty());
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn a_position_range_reports_the_gaps_and_boundaries_inside_it(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_delta(&pool, 1, 9, 109, 1, &[Backend::Native]).await?;
    seed_gap(&pool, 1, 3, 4, None, None).await?;
    seed_gap(&pool, 1, 6, 9, None, None).await?;
    seed_gap(&pool, 1, 10, 11, None, None).await?;
    seed_checkpoint(&pool, 1, 8, 108, CheckpointKind::Boundary, false).await?;
    seed_checkpoint(&pool, 1, 14, 114, CheckpointKind::Boundary, false).await?;
    sqlx::query(
        "UPDATE state_history.checkpoints SET status = 'failed', error = 'export failed'
         WHERE generation = 1 AND message_seq = 14",
    )
    .execute(&pool)
    .await?;
    seed_checkpoint(&pool, 2, 0, 120, CheckpointKind::Boundary, false).await?;
    seed_checkpoint(&pool, 2, 5, 125, CheckpointKind::Interval, false).await?;
    seed_delta(&pool, 2, 6, 126, 1, &[Backend::Native]).await?;
    seed_checkpoint(&pool, 2, 6, 126, CheckpointKind::Boundary, false).await?;
    seed_checkpoint(&pool, 3, 0, 130, CheckpointKind::Boundary, false).await?;
    let reader = StateHistoryReader::new(pool, object_store().await);

    let range = reader
        .read_position_range(
            &PositionRangeQuery::new(
                CHAIN_ID,
                position(1, 8),
                position(2, 6),
                vec![Backend::Native],
            ),
            ReadLimits::unbounded(),
        )
        .await?;

    let gaps = range
        .gaps
        .iter()
        .map(|gap| (gap.from_position, gap.to_position))
        .collect::<Vec<_>>();
    assert_eq!(
        gaps,
        [
            (Some(position(1, 6)), Some(position(1, 9))),
            (Some(position(1, 10)), Some(position(1, 11)))
        ]
    );
    let boundaries = range
        .boundaries
        .iter()
        .map(|checkpoint| checkpoint.position)
        .collect::<Vec<_>>();
    assert_eq!(
        boundaries,
        [position(1, 14), position(2, 0), position(2, 6)]
    );
    assert_eq!(range.boundaries[0].status, CheckpointStatus::Failed);
    assert_eq!(range.deltas.len(), 2);
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn only_a_stored_delta_bounds_what_is_stored_and_a_later_gap_does_not(
    pool: PgPool,
) -> anyhow::Result<()> {
    let reader = StateHistoryReader::new(pool.clone(), object_store().await);
    let query = PositionRangeQuery::new(
        CHAIN_ID,
        position(1, 100),
        position(1, 111),
        vec![Backend::Native],
    );

    let empty = reader
        .read_position_range(&query, ReadLimits::unbounded())
        .await?;
    // The writer stored 101, while 102 to 110 still wait in its queue and the
    // overflow of 111 is already recorded as a gap.
    seed_delta(&pool, 1, 101, 201, 1, &[Backend::Native]).await?;
    seed_gap(&pool, 1, 111, 111, None, None).await?;
    let queued = reader
        .read_position_range(&query, ReadLimits::unbounded())
        .await?;

    assert_eq!(empty.last_stored, None);
    assert_eq!(queued.last_stored, Some(position(1, 101)));
    assert_eq!(queued.gaps.len(), 1);
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn a_newer_generation_does_not_bound_what_an_older_one_stored(
    pool: PgPool,
) -> anyhow::Result<()> {
    // The new writer of generation 469 already stored while the old writer of
    // 468 may still drain its queue.
    seed_delta(&pool, 468, 5, 105, 1, &[Backend::Native]).await?;
    seed_delta(&pool, 469, 1, 110, 1, &[Backend::Native]).await?;
    let reader = StateHistoryReader::new(pool, object_store().await);

    let range = reader
        .read_position_range(
            &PositionRangeQuery::new(
                CHAIN_ID,
                position(468, 0),
                position(468, 10),
                vec![Backend::Native],
            ),
            ReadLimits::unbounded(),
        )
        .await?;

    assert_eq!(range.last_stored, Some(position(468, 5)));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn a_query_built_with_unsorted_backends_keeps_each_requested_partition(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_delta(&pool, 1, 7, 107, 1, &[Backend::Native, Backend::Rfq]).await?;
    let reader = StateHistoryReader::new(pool, object_store().await);
    let query = PositionRangeQuery {
        chain_id: CHAIN_ID,
        after: position(1, 5),
        through: position(1, 9),
        backends: vec![Backend::Rfq, Backend::Native],
    };

    let range = reader
        .read_position_range(&query, ReadLimits::unbounded())
        .await?;

    let mut backends = range.deltas[0]
        .applicable_backends
        .iter()
        .map(|cursor| cursor.backend)
        .collect::<Vec<_>>();
    backends.sort_unstable();
    assert_eq!(backends, [Backend::Native, Backend::Rfq]);
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
#[expect(
    clippy::expect_used,
    reason = "the limit rejections are the fixture outcome"
)]
async fn a_position_range_holds_its_deltas_to_both_byte_limits(pool: PgPool) -> anyhow::Result<()> {
    seed_delta(&pool, 1, 7, 107, 1, &[Backend::Native]).await?;
    seed_delta(&pool, 1, 9, 109, 1, &[Backend::Native]).await?;
    let (compressed, largest_compressed): (i64, i64) = sqlx::query_as(
        "SELECT sum(octet_length(payload))::bigint, max(octet_length(payload))::bigint
         FROM state_history.deltas",
    )
    .fetch_one(&pool)
    .await?;
    let compressed = u64::try_from(compressed)?;
    let decoded = [7, 9]
        .map(|seq| format!(r#"{{"generation":1,"messageSeq":{seq}}}"#).len() as u64)
        .iter()
        .sum::<u64>();
    assert!(u64::try_from(largest_compressed)? < compressed);
    let reader = StateHistoryReader::new(pool, object_store().await);
    let query = PositionRangeQuery::new(
        CHAIN_ID,
        position(1, 5),
        position(1, 9),
        vec![Backend::Native],
    );

    let exact = reader
        .read_position_range(&query, ReadLimits::new(compressed, decoded)?)
        .await?;
    let compressed_short = reader
        .read_position_range(&query, ReadLimits::new(compressed - 1, decoded)?)
        .await
        .expect_err("the two deltas exceed the compressed limit together");
    let decoded_short = reader
        .read_position_range(&query, ReadLimits::new(compressed, decoded - 1)?)
        .await
        .expect_err("the two deltas exceed the decoded limit together");

    assert_eq!(exact.deltas.len(), 2);
    assert_eq!(exact.estimated_decoded_bytes, decoded);
    assert!(compressed_short.to_string().contains("exceed limit"));
    assert!(decoded_short.to_string().contains("exceed"));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn a_corrupt_delta_fails_the_whole_position_range(pool: PgPool) -> anyhow::Result<()> {
    seed_delta(&pool, 1, 7, 107, 1, &[Backend::Native]).await?;
    seed_delta(&pool, 1, 9, 109, 1, &[Backend::Native]).await?;
    sqlx::query(
        "UPDATE state_history.deltas SET payload_sha256 = repeat('0', 64)
         WHERE generation = 1 AND message_seq = 9",
    )
    .execute(&pool)
    .await?;
    let reader = StateHistoryReader::new(pool, object_store().await);

    let read = reader
        .read_position_range(
            &PositionRangeQuery::new(
                CHAIN_ID,
                position(1, 5),
                position(1, 9),
                vec![Backend::Native],
            ),
            ReadLimits::unbounded(),
        )
        .await;

    let error = read
        .err()
        .context("a corrupt delta returns no partial range")?;
    assert!(matches!(
        error.downcast_ref::<StoredPayloadError>(),
        Some(StoredPayloadError::Sha256Mismatch(at)) if *at == position(1, 9)
    ));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL and MinIO"]
async fn a_raw_snapshot_folds_the_stored_updates_into_its_checkpoint(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_checkpoint(&pool, 21, 1, 100, CheckpointKind::Boundary, false).await?;
    seed_raw_checkpoint(
        &pool,
        position(21, 10),
        110,
        vec![vec![raw_message(
            110,
            vec![component("a", 0), component("b", 5)],
            None,
            &[],
        )]],
    )
    .await?;
    seed_raw_update(
        &pool,
        position(21, 11),
        raw_message(111, Vec::new(), Some(liquidity_change("a", 9)), &[]),
    )
    .await?;
    seed_raw_update(
        &pool,
        position(21, 12),
        raw_message(112, vec![component("c", 3)], None, &[]),
    )
    .await?;
    seed_raw_update(
        &pool,
        position(21, 13),
        raw_message(113, Vec::new(), None, &["b"]),
    )
    .await?;
    let reader = StateHistoryReader::new(pool, object_store().await);

    let snapshot = reader
        .read_raw_snapshot(
            &RawSnapshotQuery::new(CHAIN_ID, position(21, 13), vec![Backend::Native]),
            ReadLimits::unbounded(),
        )
        .await?;

    assert_eq!(snapshot.checkpoint.position, position(21, 10));
    let [partition] = snapshot.partitions.as_slice() else {
        anyhow::bail!("expected one native partition");
    };
    assert_eq!(partition.backend, BroadcasterBackend::Native);
    assert_eq!(partition.block_number, 113);
    let [message] = partition.messages.as_slice() else {
        anyhow::bail!("expected one protocol message");
    };
    assert_eq!(message.message.header.number, 113);
    assert!(message.message.deltas.is_none());
    assert_eq!(
        liquidities(message),
        [("a".to_string(), 9), ("c".to_string(), 3)]
    );
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL and MinIO"]
async fn a_raw_snapshot_at_a_checkpoint_is_that_checkpoint(pool: PgPool) -> anyhow::Result<()> {
    seed_checkpoint(&pool, 22, 1, 100, CheckpointKind::Boundary, false).await?;
    seed_raw_checkpoint(
        &pool,
        position(22, 10),
        110,
        vec![vec![raw_message(110, vec![component("a", 4)], None, &[])]],
    )
    .await?;
    let reader = StateHistoryReader::new(pool, object_store().await);

    let snapshot = reader
        .read_raw_snapshot(
            &RawSnapshotQuery::new(CHAIN_ID, position(22, 10), vec![Backend::Native]),
            ReadLimits::unbounded(),
        )
        .await?;

    let [partition] = snapshot.partitions.as_slice() else {
        anyhow::bail!("expected one native partition");
    };
    assert_eq!(partition.block_number, 110);
    assert_eq!(liquidities(&partition.messages[0]), [("a".to_string(), 4)]);
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL and MinIO"]
async fn a_raw_snapshot_waits_until_its_position_is_stored(pool: PgPool) -> anyhow::Result<()> {
    seed_checkpoint(&pool, 23, 1, 100, CheckpointKind::Boundary, false).await?;
    seed_raw_checkpoint(
        &pool,
        position(23, 10),
        110,
        vec![vec![raw_message(110, vec![component("a", 1)], None, &[])]],
    )
    .await?;
    seed_raw_update(
        &pool,
        position(23, 11),
        raw_message(111, Vec::new(), Some(liquidity_change("a", 2)), &[]),
    )
    .await?;
    let reader = StateHistoryReader::new(pool, object_store().await);

    let error = reader
        .read_raw_snapshot(
            &RawSnapshotQuery::new(CHAIN_ID, position(23, 12), vec![Backend::Native]),
            ReadLimits::unbounded(),
        )
        .await
        .err()
        .context("a position past the stored history must be refused")?;

    assert!(matches!(
        error,
        RawSnapshotError::NotYetStored { last_stored: Some(last), .. } if last == position(23, 11)
    ));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL and MinIO"]
async fn a_raw_snapshot_refuses_a_recorded_gap_before_waiting_for_the_rest(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_checkpoint(&pool, 24, 1, 100, CheckpointKind::Boundary, false).await?;
    seed_raw_checkpoint(
        &pool,
        position(24, 10),
        110,
        vec![vec![raw_message(110, vec![component("a", 1)], None, &[])]],
    )
    .await?;
    seed_raw_update(
        &pool,
        position(24, 11),
        raw_message(111, Vec::new(), Some(liquidity_change("a", 2)), &[]),
    )
    .await?;
    seed_gap(&pool, 24, 12, 12, Some((112, 112)), None).await?;
    let reader = StateHistoryReader::new(pool, object_store().await);

    let error = reader
        .read_raw_snapshot(
            &RawSnapshotQuery::new(CHAIN_ID, position(24, 12), vec![Backend::Native]),
            ReadLimits::unbounded(),
        )
        .await
        .err()
        .context("a gap before the position must be refused")?;

    assert!(matches!(error, RawSnapshotError::Gaps { ref gaps, .. } if gaps.len() == 1));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL and MinIO"]
async fn a_raw_snapshot_refuses_a_boundary_before_waiting_for_the_rest(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_checkpoint(&pool, 25, 1, 100, CheckpointKind::Boundary, false).await?;
    seed_raw_checkpoint(
        &pool,
        position(25, 10),
        110,
        vec![vec![raw_message(110, vec![component("a", 1)], None, &[])]],
    )
    .await?;
    sqlx::query(
        "INSERT INTO state_history.checkpoints
            (chain_id, generation, message_seq, kind, block_number, backends)
         VALUES ($1, 25, 12, 'boundary', 112, ARRAY['native'])",
    )
    .bind(database_i64(CHAIN_ID)?)
    .execute(&pool)
    .await?;
    seed_raw_update(
        &pool,
        position(25, 11),
        raw_message(111, Vec::new(), Some(liquidity_change("a", 2)), &[]),
    )
    .await?;
    let reader = StateHistoryReader::new(pool, object_store().await);

    let error = reader
        .read_raw_snapshot(
            &RawSnapshotQuery::new(CHAIN_ID, position(25, 12), vec![Backend::Native]),
            ReadLimits::unbounded(),
        )
        .await
        .err()
        .context("updates never carry state across a boundary")?;

    assert!(matches!(
        error,
        RawSnapshotError::Boundaries { ref boundaries, .. } if boundaries.len() == 1
    ));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL and MinIO"]
async fn a_raw_snapshot_never_starts_before_its_segment(pool: PgPool) -> anyhow::Result<()> {
    seed_checkpoint(&pool, 26, 1, 100, CheckpointKind::Boundary, false).await?;
    seed_raw_checkpoint(
        &pool,
        position(26, 10),
        110,
        vec![vec![raw_message(110, vec![component("a", 1)], None, &[])]],
    )
    .await?;
    seed_checkpoint_with_backends(
        &pool,
        26,
        12,
        112,
        CheckpointKind::Boundary,
        false,
        &[Backend::Rfq],
    )
    .await?;
    let reader = StateHistoryReader::new(pool, object_store().await);

    let error = reader
        .read_raw_snapshot(
            &RawSnapshotQuery::new(CHAIN_ID, position(26, 13), vec![Backend::Native]),
            ReadLimits::unbounded(),
        )
        .await
        .err()
        .context("a checkpoint before the segment boundary must not be used")?;

    assert!(matches!(error, RawSnapshotError::NoCheckpoint(at) if at == position(26, 13)));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL"]
async fn a_raw_snapshot_refuses_a_backend_kept_as_decoded_state(
    pool: PgPool,
) -> anyhow::Result<()> {
    let reader = StateHistoryReader::new(pool, object_store().await);

    let error = reader
        .read_raw_snapshot(
            &RawSnapshotQuery::new(
                CHAIN_ID,
                position(27, 10),
                vec![Backend::Native, Backend::Rfq],
            ),
            ReadLimits::unbounded(),
        )
        .await
        .err()
        .context("RFQ is not kept as raw messages")?;

    assert!(matches!(error, RawSnapshotError::NotRaw(Backend::Rfq)));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL and MinIO"]
async fn a_raw_snapshot_joins_split_protocols_and_updates_only_the_changed_one(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_checkpoint(&pool, 28, 1, 100, CheckpointKind::Boundary, false).await?;
    seed_raw_checkpoint(
        &pool,
        position(28, 10),
        110,
        vec![
            vec![
                raw_message(110, vec![component("a", 1), component("b", 2)], None, &[]),
                raw_protocol_message("uniswap_v3", 109, vec![component("x", 7)], None, &[]),
            ],
            vec![raw_message(110, vec![component("c", 3)], None, &[])],
        ],
    )
    .await?;
    let changed = raw_message(111, Vec::new(), Some(liquidity_change("a", 9)), &[]);
    let mut statuses = sync_statuses(std::slice::from_ref(&changed));
    statuses.insert(
        "uniswap_v3".to_string(),
        BroadcasterProtocolSyncStatus::from_synchronizer_state(&SynchronizerState::Ready(header(
            109,
        ))),
    );
    seed_raw_update_partition(
        &pool,
        position(28, 11),
        BroadcasterUpdatePartition::with_messages(
            BroadcasterBackend::Native,
            111,
            vec![changed],
            statuses.clone(),
        ),
    )
    .await?;
    let reader = StateHistoryReader::new(pool, object_store().await);

    let snapshot = reader
        .read_raw_snapshot(
            &RawSnapshotQuery::new(CHAIN_ID, position(28, 11), vec![Backend::Native]),
            ReadLimits::unbounded(),
        )
        .await?;

    let [partition] = snapshot.partitions.as_slice() else {
        anyhow::bail!("expected one native partition");
    };
    assert_eq!(partition.block_number, 111);
    assert_eq!(partition.sync_statuses, statuses);
    let [v2, v3] = partition.messages.as_slice() else {
        anyhow::bail!("expected two protocol messages");
    };
    assert_eq!(v2.protocol, "uniswap_v2");
    assert_eq!(v2.message.header, header(111));
    assert_eq!(
        liquidities(v2),
        [
            ("a".to_string(), 9),
            ("b".to_string(), 2),
            ("c".to_string(), 3)
        ]
    );
    assert_eq!(v3.protocol, "uniswap_v3");
    assert_eq!(v3.message.header, header(109));
    assert_eq!(liquidities(v3), [("x".to_string(), 7)]);
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL and MinIO"]
async fn a_raw_snapshot_refuses_a_checkpoint_without_the_declared_partition(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_checkpoint(&pool, 29, 1, 100, CheckpointKind::Boundary, false).await?;
    seed_raw_checkpoint(&pool, position(29, 10), 110, Vec::new()).await?;
    let reader = StateHistoryReader::new(pool, object_store().await);

    let error = reader
        .read_raw_snapshot(
            &RawSnapshotQuery::new(CHAIN_ID, position(29, 10), vec![Backend::Native]),
            ReadLimits::unbounded(),
        )
        .await
        .err()
        .context("a declared partition missing from the chunks must be refused")?;

    assert!(matches!(
        error,
        RawSnapshotError::Read(ref error) if error.to_string().contains("has no native partition")
    ));
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL and MinIO"]
async fn a_raw_snapshot_takes_a_sync_only_update_and_keeps_the_market(
    pool: PgPool,
) -> anyhow::Result<()> {
    seed_checkpoint(&pool, 30, 1, 100, CheckpointKind::Boundary, false).await?;
    seed_raw_checkpoint(
        &pool,
        position(30, 10),
        110,
        vec![vec![raw_message(110, vec![component("a", 1)], None, &[])]],
    )
    .await?;
    let statuses = BTreeMap::from([(
        "uniswap_v2".to_string(),
        BroadcasterProtocolSyncStatus::from_synchronizer_state(&SynchronizerState::Ready(header(
            111,
        ))),
    )]);
    seed_raw_update_partition(
        &pool,
        position(30, 11),
        BroadcasterUpdatePartition::with_messages(
            BroadcasterBackend::Native,
            111,
            Vec::new(),
            statuses.clone(),
        ),
    )
    .await?;
    let reader = StateHistoryReader::new(pool, object_store().await);

    let snapshot = reader
        .read_raw_snapshot(
            &RawSnapshotQuery::new(CHAIN_ID, position(30, 11), vec![Backend::Native]),
            ReadLimits::unbounded(),
        )
        .await?;

    let [partition] = snapshot.partitions.as_slice() else {
        anyhow::bail!("expected one native partition");
    };
    assert_eq!(partition.block_number, 111);
    assert_eq!(partition.sync_statuses, statuses);
    let [message] = partition.messages.as_slice() else {
        anyhow::bail!("expected one protocol message");
    };
    assert_eq!(message.message.header, header(110));
    assert_eq!(liquidities(message), [("a".to_string(), 1)]);
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires the state-history integration PostgreSQL and MinIO"]
async fn a_raw_snapshot_passes_a_gap_that_lost_only_rfq(pool: PgPool) -> anyhow::Result<()> {
    seed_checkpoint(&pool, 31, 1, 100, CheckpointKind::Boundary, false).await?;
    seed_raw_checkpoint(
        &pool,
        position(31, 10),
        110,
        vec![vec![raw_message(110, vec![component("a", 1)], None, &[])]],
    )
    .await?;
    seed_gap(&pool, 31, 11, 11, None, Some((111_000, 111_000))).await?;
    seed_raw_update(
        &pool,
        position(31, 12),
        raw_message(112, Vec::new(), Some(liquidity_change("a", 2)), &[]),
    )
    .await?;
    let reader = StateHistoryReader::new(pool, object_store().await);

    let snapshot = reader
        .read_raw_snapshot(
            &RawSnapshotQuery::new(CHAIN_ID, position(31, 12), vec![Backend::Native]),
            ReadLimits::unbounded(),
        )
        .await?;

    let [partition] = snapshot.partitions.as_slice() else {
        anyhow::bail!("expected one native partition");
    };
    assert_eq!(partition.block_number, 112);
    assert_eq!(liquidities(&partition.messages[0]), [("a".to_string(), 2)]);
    Ok(())
}

async fn seed_checkpoint(
    pool: &PgPool,
    generation: u64,
    message_seq: u64,
    block_number: u64,
    kind: CheckpointKind,
    with_tokens: bool,
) -> anyhow::Result<CheckpointManifest> {
    seed_checkpoint_with_backends(
        pool,
        generation,
        message_seq,
        block_number,
        kind,
        with_tokens,
        &[Backend::Native],
    )
    .await
}

async fn seed_checkpoint_with_backends(
    pool: &PgPool,
    generation: u64,
    message_seq: u64,
    block_number: u64,
    kind: CheckpointKind,
    with_tokens: bool,
    backends: &[Backend],
) -> anyhow::Result<CheckpointManifest> {
    insert_complete_checkpoint(
        pool,
        manifest(generation, message_seq, block_number, kind, with_tokens),
        backends,
    )
    .await
}

async fn insert_complete_checkpoint(
    pool: &PgPool,
    checkpoint: CheckpointManifest,
    backends: &[Backend],
) -> anyhow::Result<CheckpointManifest> {
    let backend_values = backends
        .iter()
        .map(|backend| backend.as_str())
        .collect::<Vec<_>>();
    let token = checkpoint.token_reference.as_ref();
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO state_history.checkpoints
            (chain_id, generation, message_seq, kind, block_number, rfq_observed_at_ms,
             backends, s3_key, archive_sha256, archive_bytes, compressed_bytes,
             token_s3_key, token_sha256, token_count, token_bytes, status, completed_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                 'complete', now())
         RETURNING id",
    )
    .bind(database_i64(CHAIN_ID)?)
    .bind(database_i64(checkpoint.position.generation)?)
    .bind(database_i64(checkpoint.position.message_seq)?)
    .bind(checkpoint.kind.as_str())
    .bind(database_i64(checkpoint.block_number)?)
    .bind(
        backends
            .contains(&Backend::Rfq)
            .then(|| database_i64(checkpoint.block_number * 1_000))
            .transpose()?,
    )
    .bind(backend_values)
    .bind(checkpoint.s3_key.as_deref())
    .bind(checkpoint.archive_sha256.as_deref())
    .bind(checkpoint.archive_bytes.map(database_i64).transpose()?)
    .bind(checkpoint.compressed_bytes.map(database_i64).transpose()?)
    .bind(token.map(|reference| reference.s3_key.as_str()))
    .bind(token.map(|reference| reference.sha256.as_str()))
    .bind(
        token
            .map(|reference| database_i64(reference.token_count))
            .transpose()?,
    )
    .bind(
        token
            .map(|reference| database_i64(reference.token_bytes))
            .transpose()?,
    )
    .fetch_one(pool)
    .await?;
    Ok(CheckpointManifest {
        id,
        state_version: None,
        backends: backends.to_vec(),
        ..checkpoint
    })
}

async fn seed_delta(
    pool: &PgPool,
    generation: u64,
    message_seq: u64,
    block_number: u64,
    payload_format_version: i16,
    backends: &[Backend],
) -> anyhow::Result<()> {
    let raw = RawValue::from_string(format!(
        r#"{{"generation":{generation},"messageSeq":{message_seq}}}"#
    ))?;
    seed_delta_payload(
        pool,
        position(generation, message_seq),
        block_number,
        payload_format_version,
        backends,
        &raw,
    )
    .await
}

async fn seed_delta_payload(
    pool: &PgPool,
    position: StreamPosition,
    block_number: u64,
    payload_format_version: i16,
    backends: &[Backend],
    raw: &RawValue,
) -> anyhow::Result<()> {
    let StreamPosition {
        generation,
        message_seq,
    } = position;
    let canonical = raw.get().as_bytes();
    let payload = zstd::stream::encode_all(canonical, 3)?;
    let sha256 = hex::encode(sha2::Sha256::digest(canonical));
    let delta_id: i64 = sqlx::query_scalar(
        "INSERT INTO state_history.deltas
            (chain_id, generation, message_seq, block_number, observed_at_ms, payload,
             payload_sha256, payload_format_version)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         RETURNING id",
    )
    .bind(database_i64(CHAIN_ID)?)
    .bind(database_i64(generation)?)
    .bind(database_i64(message_seq)?)
    .bind(database_i64(block_number)?)
    .bind(database_i64(block_number * 1_000)?)
    .bind(payload)
    .bind(sha256)
    .bind(payload_format_version)
    .fetch_one(pool)
    .await?;
    for backend in backends {
        let (block_cursor, time_cursor) = match backend {
            Backend::Native | Backend::Vm => (Some(block_number), None),
            Backend::Rfq => (None, Some(block_number * 1_000)),
        };
        sqlx::query(
            "INSERT INTO state_history.delta_backends
                (delta_id, chain_id, backend, generation, message_seq, block_number, observed_at_ms)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(delta_id)
        .bind(database_i64(CHAIN_ID)?)
        .bind(backend.as_str())
        .bind(database_i64(generation)?)
        .bind(database_i64(message_seq)?)
        .bind(block_cursor.map(database_i64).transpose()?)
        .bind(time_cursor.map(database_i64).transpose()?)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn seed_gap(
    pool: &PgPool,
    generation: u64,
    from_message_seq: u64,
    to_message_seq: u64,
    block_bounds: Option<(u64, u64)>,
    time_bounds: Option<(u64, u64)>,
) -> anyhow::Result<()> {
    seed_gap_with_cursors(
        pool,
        generation,
        from_message_seq,
        to_message_seq,
        GapCursors {
            from_block: block_bounds.map(|bounds| bounds.0),
            to_block: block_bounds.map(|bounds| bounds.1),
            from_observed_at_ms: time_bounds.map(|bounds| bounds.0),
            to_observed_at_ms: time_bounds.map(|bounds| bounds.1),
        },
    )
    .await
}

#[derive(Clone, Copy, Default)]
struct GapCursors {
    from_block: Option<u64>,
    to_block: Option<u64>,
    from_observed_at_ms: Option<u64>,
    to_observed_at_ms: Option<u64>,
}

async fn seed_gap_with_cursors(
    pool: &PgPool,
    generation: u64,
    from_message_seq: u64,
    to_message_seq: u64,
    cursors: GapCursors,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO state_history.gaps
            (chain_id, generation, from_message_seq, to_message_seq, reason,
             from_block_number, to_block_number, from_observed_at_ms, to_observed_at_ms)
         VALUES ($1, $2, $3, $4, 'write_failed', $5, $6, $7, $8)",
    )
    .bind(database_i64(CHAIN_ID)?)
    .bind(database_i64(generation)?)
    .bind(database_i64(from_message_seq)?)
    .bind(database_i64(to_message_seq)?)
    .bind(cursors.from_block.map(database_i64).transpose()?)
    .bind(cursors.to_block.map(database_i64).transpose()?)
    .bind(cursors.from_observed_at_ms.map(database_i64).transpose()?)
    .bind(cursors.to_observed_at_ms.map(database_i64).transpose()?)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_block_times(pool: &PgPool, start: u64, end: u64) -> anyhow::Result<()> {
    for block_number in start..=end {
        sqlx::query(
            "INSERT INTO state_history.block_times
                (chain_id, block_number, timestamp_ms, block_hash, parent_hash,
                 source_generation, source_message_seq)
             VALUES ($1, $2, $3, $4, $5, 1, $6)",
        )
        .bind(database_i64(CHAIN_ID)?)
        .bind(database_i64(block_number)?)
        .bind(database_i64(block_number * 1_000)?)
        .bind(format!("hash-{block_number}"))
        .bind(format!("hash-{}", block_number.saturating_sub(1)))
        .bind(database_i64(block_number - start + 1)?)
        .execute(pool)
        .await?;
    }
    Ok(())
}

fn manifest(
    generation: u64,
    message_seq: u64,
    block_number: u64,
    kind: CheckpointKind,
    with_tokens: bool,
) -> CheckpointManifest {
    CheckpointManifest {
        id: 0,
        chain_id: CHAIN_ID,
        position: position(generation, message_seq),
        state_version: Some(message_seq),
        kind,
        block_number,
        rfq_observed_at_ms: None,
        backends: vec![Backend::Native],
        s3_key: Some(format!(
            "reader/chain={CHAIN_ID}/gen={generation}/seq={message_seq}/kind={}/checkpoint.zst",
            kind.as_str()
        )),
        archive_sha256: Some(format!("archive-{generation}-{message_seq}")),
        archive_bytes: Some(1_000),
        compressed_bytes: Some(100),
        token_reference: with_tokens.then(|| state_history::TokenSnapshotRef {
            s3_key: format!("reader/chain={CHAIN_ID}/tokens/token-{generation}-{message_seq}.zst"),
            sha256: format!("token-{generation}-{message_seq}"),
            token_count: 1,
            token_bytes: 100,
        }),
        status: CheckpointStatus::Complete,
        error: None,
    }
}

async fn seed_raw_checkpoint(
    pool: &PgPool,
    at: StreamPosition,
    block_number: u64,
    chunks: Vec<Vec<BroadcasterProtocolMessage>>,
) -> anyhow::Result<CheckpointManifest> {
    let snapshot_id = format!("snapshot-{}-{}", at.generation, at.message_seq);
    let mut payloads = vec![BroadcasterPayload::SnapshotStart(
        BroadcasterSnapshotStart::new(
            snapshot_id.clone(),
            CHAIN_ID,
            vec![BroadcasterBackend::Native],
            u32::try_from(chunks.len())?,
        )?,
    )];
    for (index, messages) in chunks.into_iter().enumerate() {
        payloads.push(BroadcasterPayload::SnapshotChunk(
            BroadcasterSnapshotChunk::new(
                snapshot_id.clone(),
                u32::try_from(index)?,
                vec![BroadcasterSnapshotPartition::with_messages(
                    BroadcasterBackend::Native,
                    block_number,
                    messages.clone(),
                    sync_statuses(&messages),
                )],
            )?,
        ));
    }
    payloads.push(BroadcasterPayload::SnapshotEnd(
        BroadcasterSnapshotEnd::new(snapshot_id),
    ));
    let encoded = encode_archive(CheckpointArchive {
        metadata: ArchiveMetadata {
            schema_version: ARCHIVE_SCHEMA_VERSION,
            chain_id: CHAIN_ID,
            position: at,
            state_version: None,
            kind: CheckpointKind::Interval,
            block_number,
            rfq_observed_at_ms: None,
            backends: vec![Backend::Native],
        },
        payloads_json: payloads
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<_, _>>()?,
    })?;
    let objects = object_store().await;
    let key = objects.key_for(CHAIN_ID, at, CheckpointKind::Interval);
    objects.put(&key, encoded.bytes).await?;
    insert_complete_checkpoint(
        pool,
        CheckpointManifest {
            s3_key: Some(key),
            archive_sha256: Some(encoded.info.sha256),
            archive_bytes: Some(encoded.info.archive_bytes),
            compressed_bytes: Some(encoded.info.compressed_bytes),
            ..manifest(
                at.generation,
                at.message_seq,
                block_number,
                CheckpointKind::Interval,
                false,
            )
        },
        &[Backend::Native],
    )
    .await
}

async fn seed_raw_update(
    pool: &PgPool,
    at: StreamPosition,
    message: BroadcasterProtocolMessage,
) -> anyhow::Result<()> {
    let statuses = sync_statuses(std::slice::from_ref(&message));
    seed_raw_update_partition(
        pool,
        at,
        BroadcasterUpdatePartition::with_messages(
            BroadcasterBackend::Native,
            message.message.header.number,
            vec![message],
            statuses,
        ),
    )
    .await
}

async fn seed_raw_update_partition(
    pool: &PgPool,
    at: StreamPosition,
    partition: BroadcasterUpdatePartition,
) -> anyhow::Result<()> {
    let block_number = partition.block_number;
    let update = BroadcasterUpdateMessage::new(vec![partition])?;
    let envelope = BroadcasterEnvelope::new(
        format!("stream-{}", at.generation),
        at.message_seq,
        BroadcasterPayload::Update(update),
    );
    let raw = RawValue::from_string(serde_json::to_string(&envelope)?)?;
    seed_delta_payload(pool, at, block_number, 1, &[Backend::Native], &raw).await
}

fn raw_message(
    block_number: u64,
    states: Vec<ComponentWithState>,
    deltas: Option<BlockAggregatedChanges>,
    removed: &[&str],
) -> BroadcasterProtocolMessage {
    raw_protocol_message("uniswap_v2", block_number, states, deltas, removed)
}

fn raw_protocol_message(
    protocol: &str,
    block_number: u64,
    states: Vec<ComponentWithState>,
    deltas: Option<BlockAggregatedChanges>,
    removed: &[&str],
) -> BroadcasterProtocolMessage {
    let header = header(block_number);
    BroadcasterProtocolMessage::new(
        protocol,
        SynchronizerState::Ready(header.clone()),
        StateSyncMessage {
            header,
            snapshots: Snapshot {
                states: states
                    .into_iter()
                    .map(|state| (state.state.component_id.clone(), state))
                    .collect(),
                vm_storage: HashMap::new(),
            },
            deltas,
            removed_components: removed
                .iter()
                .map(|id| (id.to_string(), protocol_component(id)))
                .collect(),
        },
    )
}

fn header(block_number: u64) -> BlockHeader {
    BlockHeader {
        hash: Bytes::from([block_number as u8; 32]),
        number: block_number,
        parent_hash: Bytes::from([block_number.saturating_sub(1) as u8; 32]),
        revert: false,
        timestamp: block_number * 2,
        partial_block_index: None,
    }
}

fn sync_statuses(
    messages: &[BroadcasterProtocolMessage],
) -> BTreeMap<String, BroadcasterProtocolSyncStatus> {
    messages
        .iter()
        .map(|message| {
            (
                message.protocol.clone(),
                BroadcasterProtocolSyncStatus::from_synchronizer_state(&message.sync_state),
            )
        })
        .collect()
}

fn component(id: &str, liquidity: u8) -> ComponentWithState {
    ComponentWithState {
        state: ProtocolComponentState {
            component_id: id.to_string(),
            attributes: HashMap::from([("liquidity".to_string(), Bytes::from([liquidity]))]),
            balances: HashMap::new(),
        },
        component: protocol_component(id),
        component_tvl: None,
        entrypoints: Vec::new(),
    }
}

fn protocol_component(id: &str) -> ProtocolComponent {
    ProtocolComponent {
        id: id.to_string(),
        protocol_system: "uniswap_v2".to_string(),
        protocol_type_name: "uniswap_v2".to_string(),
        chain: Chain::Base,
        tokens: vec![Bytes::from([1u8; 20]), Bytes::from([2u8; 20])],
        contract_addresses: Vec::new(),
        static_attributes: HashMap::new(),
        change: Default::default(),
        creation_tx: Bytes::from([3u8; 32]),
        created_at: chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
            .unwrap_or_default()
            .naive_utc(),
    }
}

fn liquidity_change(id: &str, liquidity: u8) -> BlockAggregatedChanges {
    let mut changes = BlockAggregatedChanges::default();
    changes.state_deltas.insert(
        id.to_string(),
        ProtocolComponentStateDelta::new(
            id,
            HashMap::from([("liquidity".to_string(), Bytes::from([liquidity]))]),
            HashSet::new(),
        ),
    );
    changes
}

fn liquidities(message: &BroadcasterProtocolMessage) -> Vec<(String, u8)> {
    let mut liquidities = message
        .message
        .snapshots
        .states
        .iter()
        .map(|(id, state)| (id.clone(), state.state.attributes["liquidity"][0]))
        .collect::<Vec<_>>();
    liquidities.sort();
    liquidities
}

async fn object_store() -> state_history::CheckpointObjectStore {
    state_history::CheckpointObjectStore::from_env_config(
        "state-history-analysis".to_owned(),
        "reader".to_owned(),
        "eu-central-1".to_owned(),
        Some("http://127.0.0.1:59000".to_owned()),
        true,
    )
    .await
}

const fn position(generation: u64, message_seq: u64) -> StreamPosition {
    StreamPosition {
        generation,
        message_seq,
    }
}

fn database_i64(value: u64) -> anyhow::Result<i64> {
    i64::try_from(value).context("fixture value exceeds PostgreSQL BIGINT")
}
