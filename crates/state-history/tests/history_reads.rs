use std::collections::BTreeSet;

use anyhow::Context;
use serde_json::value::RawValue;
use sha2::Digest;
use sqlx::PgPool;
use state_history::{
    Backend, BlockInterval, CheckpointKind, CheckpointManifest, CheckpointPairQuery,
    CheckpointPairSelection, CheckpointStatus, CoverageQuery, ExplicitCheckpointPair,
    ReadLimitError, ReadLimits, StateHistoryReader, StreamPosition, TargetPlanQuery, TokenAnchor,
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
    .bind(
        block_bounds
            .map(|bounds| database_i64(bounds.0))
            .transpose()?,
    )
    .bind(
        block_bounds
            .map(|bounds| database_i64(bounds.1))
            .transpose()?,
    )
    .bind(
        time_bounds
            .map(|bounds| database_i64(bounds.0))
            .transpose()?,
    )
    .bind(
        time_bounds
            .map(|bounds| database_i64(bounds.1))
            .transpose()?,
    )
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
