use std::collections::BTreeSet;

use anyhow::Context;
use serde_json::value::RawValue;
use sha2::Digest;
use sqlx::PgPool;
use state_history::{
    Backend, BlockInterval, CheckpointKind, CheckpointManifest, CheckpointPairQuery,
    CheckpointPairSelection, CheckpointStatus, CoverageQuery, ExplicitCheckpointPair,
    PositionRangeQuery, ReadLimitError, ReadLimits, StateHistoryReader, StoredPayloadError,
    StreamPosition, TargetPlanQuery, TokenAnchor,
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
    let checkpoint = manifest(generation, message_seq, block_number, kind, with_tokens);
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
    .bind(database_i64(generation)?)
    .bind(database_i64(message_seq)?)
    .bind(kind.as_str())
    .bind(database_i64(block_number)?)
    .bind(
        backends
            .contains(&Backend::Rfq)
            .then(|| database_i64(block_number * 1_000))
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
