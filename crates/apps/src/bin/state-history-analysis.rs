use std::collections::BTreeMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Region, Client as S3Client};
use serde::Serialize;
use tokio::time::{sleep, Duration, Instant};
use tracing_subscriber::EnvFilter;

use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterBlockRef, BroadcasterEnvelope,
    BroadcasterGenerationHandoff, BroadcasterHeartbeat, BroadcasterPayload, BroadcasterProgress,
    BroadcasterProtocolSyncStatus, BroadcasterProtocolSyncStatusKind, BroadcasterRedisStreamEntry,
    BroadcasterSnapshotChunk, BroadcasterSnapshotEnd, BroadcasterSnapshotPartition,
    BroadcasterSnapshotStart, BroadcasterUpdateMessage, BroadcasterUpdatePartition,
};
use state_history::{
    rfq_timestamp_ms_from_seconds, BacktestRangePlan, BacktestRangeRequest, BlockTimestampRecord,
    CheckpointArchive, CheckpointArchiveMetadata, CheckpointWriteOutcome, DecodedCheckpointArchive,
    HistoryRangeGapSource, HistoryRangePlan, HistoryRangeRequest, IngestionGap, PersistedDelta,
    S3CheckpointStore, StateHistoryCheckpointWriter, StateHistoryPgStore, StateHistoryReader,
    StateHistoryWriter, StateHistoryWriterConfig,
};

const DEFAULT_DATABASE_URL: &str = "postgres://postgres:postgres@127.0.0.1:55432/state_history";
const DEFAULT_S3_BUCKET: &str = "state-history";
const DEFAULT_S3_PREFIX: &str = "state-history/local-analysis";
const DEFAULT_S3_REGION: &str = "us-east-1";
const DEFAULT_S3_ENDPOINT_URL: &str = "http://127.0.0.1:59000";
const DEFAULT_S3_FORCE_PATH_STYLE: bool = true;

const START_BLOCK_NUMBER: u64 = 100;
const VM_BLOCK_NUMBER: u64 = 101;
const END_BLOCK_NUMBER: u64 = 110;
const NEXT_BLOCK_NUMBER: u64 = 111;

const NATIVE_HASH_SEED: u8 = 1;
const VM_HASH_SEED: u8 = 2;
const RFQ_HASH_SEED: u8 = 3;
const INITIAL_HEAD_HASH_SEED: u8 = 0x0A;
const REORG_HASH_SEED: u8 = 0x0B;
const STALE_WRITER_HASH_SEED: u8 = 0x0C;
const CROSS_STREAM_HASH_SEED: u8 = 0x0D;
const OLDER_GENERATION_HASH_SEED: u8 = 0x0E;
const CONFLICT_NATIVE_HASH_SEED: u8 = 0x20;
const CONFLICT_VM_HASH_SEED: u8 = 0x21;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    let args = CliArgs::parse()?;
    let report = run(args).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "analysis harness keeps the branch and backtest report assembly together"
)]
async fn run(args: CliArgs) -> Result<StateHistoryAnalysisReport> {
    StateHistoryPgStore::run_migrations(&args.database_url).await?;
    ensure_bucket_exists(&args).await?;

    let pg_store = StateHistoryPgStore::connect(&args.database_url).await?;
    pg_store.validate_schema().await?;
    let checkpoint_store = S3CheckpointStore::from_env_config(
        &args.s3_region,
        args.s3_bucket.clone(),
        args.s3_endpoint_url.as_deref(),
        args.s3_force_path_style,
    )
    .await?;

    let run_id = current_timestamp_ms()?;
    let chain_id = 9_000_000_000u64.saturating_add(run_id % 1_000_000);
    let stream_id = format!("chain-{chain_id}-stream-1");
    let rfq_cursor_timestamp_seconds = 1_700_000_000u64.saturating_add(run_id % 1_000);
    let rfq_cursor_timestamp_ms = rfq_timestamp_ms_from_seconds(rfq_cursor_timestamp_seconds)?;
    let (inserted_deltas, handoff_stream_id) = insert_synthetic_delta_fixtures(
        &pg_store,
        chain_id,
        &stream_id,
        rfq_cursor_timestamp_seconds,
    )
    .await?;

    let checkpoint_archive =
        synthetic_checkpoint_archive(chain_id, &stream_id, run_id, rfq_cursor_timestamp_ms)?;
    let checkpoint_writer = StateHistoryCheckpointWriter::new(
        pg_store.clone(),
        checkpoint_store.clone(),
        args.s3_prefix.clone(),
    );
    let checkpoint = checkpoint_writer
        .write_checkpoint(checkpoint_archive)
        .await
        .context("failed to write synthetic checkpoint")?;

    let reader = StateHistoryReader::new(pg_store.clone(), checkpoint_store.clone());
    let request = synthetic_history_request(chain_id, rfq_cursor_timestamp_ms)?;
    record_pre_checkpoint_gap(&pg_store, chain_id, &stream_id).await?;
    let plan = reader.resolve_range(request.clone()).await?;
    let valid_generation_switch_gaps = assert_no_generation_switch_gap(&plan)?;
    let unproven_ingestion_gaps = assert_unproven_ingestion_gap(&plan)?;
    let selected_checkpoint = plan
        .checkpoint
        .as_ref()
        .ok_or_else(|| anyhow!("expected a complete checkpoint for the synthetic range"))?;
    let decoded = reader.fetch_checkpoint(selected_checkpoint).await?;

    let replayed_message_sequences = assert_replay_plan(&plan, &stream_id, &handoff_stream_id)?;
    anyhow::ensure!(
        decoded.archive.payloads.len() == 3,
        "expected checkpoint archive start/chunk/end payloads"
    );

    let post_handoff_checkpoint_generation_switch_gaps =
        assert_post_handoff_checkpoint_does_not_report_generation_switch(
            &checkpoint_writer,
            &reader,
            &request,
            chain_id,
            &handoff_stream_id,
            run_id.saturating_add(1),
            rfq_cursor_timestamp_ms,
        )
        .await?;

    record_unseen_generation_gap(&pg_store, chain_id).await?;
    let unseen_generation_gap_plan = reader.resolve_range(request.clone()).await?;
    let unseen_generation_gap_switch_gaps =
        assert_generation_switch_gap(&unseen_generation_gap_plan)?;

    insert_old_generation_continuation_delta(&pg_store, chain_id, &stream_id).await?;
    let old_generation_plan = reader.resolve_range(request.clone()).await?;
    assert_generation_switch_gap(&old_generation_plan)?;

    let stale_stream_id = insert_stale_generation_delta(&pg_store, chain_id).await?;
    let stale_plan = reader.resolve_range(request.clone()).await?;
    let generation_switch_gaps = assert_generation_switch_gap(&stale_plan)?;

    record_visible_gap_fixtures(&pg_store, chain_id, &handoff_stream_id, &stale_stream_id).await?;
    let gap_plan = reader.resolve_range(request).await?;
    let recorded_gaps = gap_plan
        .gaps
        .iter()
        .filter(|gap| gap.source == HistoryRangeGapSource::RecordedGap)
        .count();
    anyhow::ensure!(
        recorded_gaps == 1,
        "expected one recorded synthetic gap, got {}",
        recorded_gaps
    );

    let backtest = run_backtest_scenario(
        &pg_store,
        checkpoint_store,
        &args.s3_prefix,
        chain_id.saturating_add(1),
        run_id,
    )
    .await?;

    Ok(StateHistoryAnalysisReport {
        status: "passed",
        chain_id,
        stream_id,
        inserted_deltas,
        replayed_message_sequences,
        checkpoint_manifest_id: checkpoint.manifest_id,
        checkpoint_s3_key: checkpoint.s3_key,
        checkpoint_payload_hash: checkpoint.payload.hash,
        decoded_checkpoint_payloads: decoded.archive.payloads.len(),
        recorded_gaps,
        unproven_ingestion_gaps,
        valid_generation_switch_gaps,
        post_handoff_checkpoint_generation_switch_gaps,
        unseen_generation_gap_switch_gaps,
        generation_switch_gaps,
        backtest_chain_id: backtest.chain_id,
        backtest_stream_id: backtest.stream_id,
        backtest_inserted_deltas: backtest.inserted_deltas,
        backtest_replayed_message_sequences: backtest.replayed_message_sequences,
        backtest_checkpoint_manifest_id: backtest.checkpoint_manifest_id,
        backtest_checkpoint_s3_key: backtest.checkpoint_s3_key,
        backtest_checkpoint_payload_hash: backtest.checkpoint_payload_hash,
        backtest_decoded_checkpoint_payloads: backtest.decoded_checkpoint_payloads,
        backtest_start_block_timestamp_ms: backtest.backtest_start_block_timestamp_ms,
        backtest_end_block_timestamp_ms: backtest.backtest_end_block_timestamp_ms,
        rfq_end_timestamp_ms: backtest.rfq_end_timestamp_ms,
        reorg_superseded: backtest.reorg_superseded,
        stale_write_kept: backtest.stale_write_kept,
        duplicate_write_left_updated_at_untouched: backtest
            .duplicate_write_left_updated_at_untouched,
        cross_stream_superseded: backtest.cross_stream_superseded,
        older_generation_write_kept: backtest.older_generation_write_kept,
        source_advanced_without_churn: backtest.source_advanced_without_churn,
        snapshot_seeded_start_boundary: backtest.snapshot_seeded_start_boundary,
        head_range_unresolvable: backtest.head_range_unresolvable,
        conflicted_checkpoint_rejected: backtest.conflicted_checkpoint_rejected,
        backtest_recorded_gaps: backtest.recorded_gaps,
        backtest_generation_switch_gaps: backtest.generation_switch_gaps,
        backtest_unproven_ingestion_gaps: backtest.unproven_ingestion_gaps,
    })
}

async fn insert_synthetic_delta_fixtures(
    pg_store: &StateHistoryPgStore,
    chain_id: u64,
    stream_id: &str,
    rfq_cursor_timestamp_seconds: u64,
) -> Result<(usize, String)> {
    let redis_entries = synthetic_delta_entries(chain_id, stream_id, rfq_cursor_timestamp_seconds)?;
    let mut inserted_deltas = 0usize;
    for (index, entry) in redis_entries.iter().enumerate() {
        let redis_entry_id = format!("1-{}", index + 1);
        pg_store
            .insert_entry(entry, Some(&redis_entry_id))
            .await
            .with_context(|| format!("failed to insert synthetic delta {}", entry.message_seq))?;
        inserted_deltas = inserted_deltas.saturating_add(1);
    }

    let handoff_stream_id = format!("chain-{chain_id}-stream-2");
    let handoff_marker = synthetic_handoff_marker(
        chain_id,
        stream_id,
        "1-4",
        &handoff_stream_id,
        rfq_cursor_timestamp_seconds,
    )?;
    assert_duplicate_handoff_enqueue_is_healthy(pg_store, handoff_marker).await?;
    inserted_deltas = inserted_deltas.saturating_add(1);

    let post_handoff_entries = synthetic_post_handoff_delta_entries(chain_id, &handoff_stream_id)?;
    for (index, entry) in post_handoff_entries.iter().enumerate() {
        let redis_entry_id = format!("2-{}", index + 2);
        pg_store
            .insert_entry(entry, Some(&redis_entry_id))
            .await
            .with_context(|| {
                format!(
                    "failed to insert post-handoff synthetic delta {}",
                    entry.message_seq
                )
            })?;
        inserted_deltas = inserted_deltas.saturating_add(1);
    }

    Ok((inserted_deltas, handoff_stream_id))
}

async fn insert_stale_generation_delta(
    pg_store: &StateHistoryPgStore,
    chain_id: u64,
) -> Result<String> {
    let stale_stream_id = format!("chain-{chain_id}-stream-9");
    let stale_generation_delta =
        synthetic_delta_entry(chain_id, &stale_stream_id, 2, BroadcasterBackend::Vm, 101)?;
    pg_store
        .insert_entry(&stale_generation_delta, Some("9-2"))
        .await
        .context("failed to insert stale-generation synthetic delta")?;
    Ok(stale_stream_id)
}

async fn insert_old_generation_continuation_delta(
    pg_store: &StateHistoryPgStore,
    chain_id: u64,
    stream_id: &str,
) -> Result<()> {
    let old_generation_delta =
        synthetic_delta_entry(chain_id, stream_id, 5, BroadcasterBackend::Native, 102)?;
    pg_store
        .insert_entry(&old_generation_delta, Some("1-5"))
        .await
        .context("failed to insert old-generation continuation delta")?;
    Ok(())
}

fn synthetic_history_request(
    chain_id: u64,
    rfq_cursor_timestamp_ms: u64,
) -> Result<HistoryRangeRequest> {
    HistoryRangeRequest::new(
        chain_id,
        100,
        110,
        vec![
            BroadcasterBackend::Native,
            BroadcasterBackend::Vm,
            BroadcasterBackend::Rfq,
        ],
    )?
    .with_rfq_timestamp_range(
        rfq_cursor_timestamp_ms + 1,
        rfq_cursor_timestamp_ms + 100_000,
    )
}

fn assert_replay_plan(
    plan: &HistoryRangePlan,
    stream_id: &str,
    handoff_stream_id: &str,
) -> Result<Vec<u64>> {
    let replayed_streams_and_sequences = plan
        .deltas
        .iter()
        .map(|delta| (delta.entry.stream_id.as_str(), delta.entry.message_seq))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        replayed_streams_and_sequences
            == vec![
                (stream_id, 2),
                (stream_id, 3),
                (stream_id, 4),
                (handoff_stream_id, 2),
                (handoff_stream_id, 3),
            ],
        "expected replay to follow stream-1 before stream-2 even when message_seq is reused, got {replayed_streams_and_sequences:?}"
    );
    let replayed_message_sequences = plan
        .deltas
        .iter()
        .map(|delta| delta.entry.message_seq)
        .collect::<Vec<_>>();
    Ok(replayed_message_sequences)
}

fn assert_no_generation_switch_gap(plan: &HistoryRangePlan) -> Result<usize> {
    let generation_switch_gaps = generation_switch_gap_count(plan);
    anyhow::ensure!(
        generation_switch_gaps == 0,
        "expected valid handoff replay to have no generation switch gaps, got {generation_switch_gaps}"
    );
    Ok(generation_switch_gaps)
}

fn assert_generation_switch_gap(plan: &HistoryRangePlan) -> Result<usize> {
    let generation_switch_gaps = generation_switch_gap_count(plan);
    anyhow::ensure!(
        generation_switch_gaps == 1,
        "expected one generation switch gap, got {generation_switch_gaps}"
    );
    Ok(generation_switch_gaps)
}

async fn assert_post_handoff_checkpoint_does_not_report_generation_switch(
    checkpoint_writer: &StateHistoryCheckpointWriter,
    reader: &StateHistoryReader,
    request: &HistoryRangeRequest,
    chain_id: u64,
    handoff_stream_id: &str,
    captured_at_timestamp_ms: u64,
    rfq_cursor_timestamp_ms: u64,
) -> Result<usize> {
    let post_handoff_checkpoint = checkpoint_writer
        .write_checkpoint(synthetic_checkpoint_archive_with_cursor(
            chain_id,
            handoff_stream_id,
            captured_at_timestamp_ms,
            rfq_cursor_timestamp_ms,
            100,
            2,
        )?)
        .await
        .context("failed to write post-handoff synthetic checkpoint")?;
    let post_handoff_checkpoint_plan = reader.resolve_range(request.clone()).await?;
    let generation_switch_gaps = assert_no_generation_switch_gap(&post_handoff_checkpoint_plan)?;
    let selected_post_handoff_checkpoint = post_handoff_checkpoint_plan
        .checkpoint
        .as_ref()
        .ok_or_else(|| anyhow!("expected post-handoff checkpoint to cover synthetic range"))?;
    anyhow::ensure!(
        selected_post_handoff_checkpoint.id == post_handoff_checkpoint.manifest_id,
        "expected post-handoff checkpoint to be selected, got {}",
        selected_post_handoff_checkpoint.id
    );
    Ok(generation_switch_gaps)
}

fn assert_unproven_ingestion_gap(plan: &HistoryRangePlan) -> Result<usize> {
    let unproven_ingestion_gaps = unproven_ingestion_gap_count(plan);
    anyhow::ensure!(
        unproven_ingestion_gaps >= 1,
        "expected synthetic open stream to report unproven ingestion coverage"
    );
    Ok(unproven_ingestion_gaps)
}

fn generation_switch_gap_count(plan: &HistoryRangePlan) -> usize {
    plan.gaps
        .iter()
        .filter(|gap| gap.source == HistoryRangeGapSource::GenerationSwitch)
        .count()
}

fn unproven_ingestion_gap_count(plan: &HistoryRangePlan) -> usize {
    plan.gaps
        .iter()
        .filter(|gap| gap.source == HistoryRangeGapSource::UnprovenIngestion)
        .count()
}

async fn record_pre_checkpoint_gap(
    pg_store: &StateHistoryPgStore,
    chain_id: u64,
    stream_id: &str,
) -> Result<()> {
    pg_store
        .record_gap(&IngestionGap {
            chain_id,
            stream_id: stream_id.to_string(),
            from_message_seq: 1,
            to_message_seq: 1,
            prev_persistable_message_seq: None,
            backend_scope: vec![BroadcasterBackend::Native],
            from_block_number: Some(100),
            to_block_number: Some(100),
            from_timestamp_ms: None,
            to_timestamp_ms: None,
            reason: "state history analysis pre-checkpoint gap".to_string(),
        })
        .await?;
    Ok(())
}

async fn record_visible_gap_fixtures(
    pg_store: &StateHistoryPgStore,
    chain_id: u64,
    handoff_stream_id: &str,
    stale_stream_id: &str,
) -> Result<()> {
    for (stream_id, reason) in [
        (
            handoff_stream_id,
            "state history analysis post-handoff synthetic gap",
        ),
        (stale_stream_id, "state history analysis stale-stream gap"),
    ] {
        pg_store
            .record_gap(&IngestionGap {
                chain_id,
                stream_id: stream_id.to_string(),
                from_message_seq: 9,
                to_message_seq: 9,
                prev_persistable_message_seq: Some(8),
                backend_scope: vec![BroadcasterBackend::Native],
                from_block_number: Some(108),
                to_block_number: Some(109),
                from_timestamp_ms: None,
                to_timestamp_ms: None,
                reason: reason.to_string(),
            })
            .await?;
    }
    Ok(())
}

async fn record_unseen_generation_gap(pg_store: &StateHistoryPgStore, chain_id: u64) -> Result<()> {
    pg_store
        .record_gap(&IngestionGap {
            chain_id,
            stream_id: format!("chain-{chain_id}-stream-3"),
            from_message_seq: 1,
            to_message_seq: 1,
            prev_persistable_message_seq: None,
            backend_scope: vec![BroadcasterBackend::Native],
            from_block_number: Some(108),
            to_block_number: Some(109),
            from_timestamp_ms: None,
            to_timestamp_ms: None,
            reason: "state history analysis unseen-generation gap".to_string(),
        })
        .await?;
    Ok(())
}

async fn ensure_bucket_exists(args: &CliArgs) -> Result<()> {
    let client = s3_client(args).await;
    if client
        .head_bucket()
        .bucket(&args.s3_bucket)
        .send()
        .await
        .is_ok()
    {
        return Ok(());
    }

    client
        .create_bucket()
        .bucket(&args.s3_bucket)
        .send()
        .await
        .with_context(|| format!("failed to create S3 bucket {}", args.s3_bucket))?;
    Ok(())
}

async fn s3_client(args: &CliArgs) -> S3Client {
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(args.s3_region.clone()))
        .load()
        .await;
    let mut builder =
        aws_sdk_s3::config::Builder::from(&sdk_config).force_path_style(args.s3_force_path_style);
    if let Some(endpoint_url) = &args.s3_endpoint_url {
        builder = builder.endpoint_url(endpoint_url);
    }
    S3Client::from_conf(builder.build())
}

fn synthetic_delta_entries(
    chain_id: u64,
    stream_id: &str,
    rfq_cursor_timestamp_seconds: u64,
) -> Result<Vec<BroadcasterRedisStreamEntry>> {
    [
        (1, BroadcasterBackend::Native, 100),
        (2, BroadcasterBackend::Native, 100),
        (3, BroadcasterBackend::Vm, 101),
        (
            4,
            BroadcasterBackend::Rfq,
            rfq_cursor_timestamp_seconds + 10,
        ),
    ]
    .into_iter()
    .map(|(message_seq, backend, cursor)| {
        synthetic_delta_entry(chain_id, stream_id, message_seq, backend, cursor)
    })
    .collect()
}

fn synthetic_post_handoff_delta_entries(
    chain_id: u64,
    stream_id: &str,
) -> Result<Vec<BroadcasterRedisStreamEntry>> {
    [
        (2, BroadcasterBackend::Native, 102),
        (3, BroadcasterBackend::Vm, 103),
    ]
    .into_iter()
    .map(|(message_seq, backend, cursor)| {
        synthetic_delta_entry(chain_id, stream_id, message_seq, backend, cursor)
    })
    .collect()
}

fn synthetic_handoff_marker(
    chain_id: u64,
    previous_stream_id: &str,
    previous_entry_id: &str,
    next_stream_id: &str,
    rfq_cursor_timestamp_seconds: u64,
) -> Result<BroadcasterRedisStreamEntry> {
    let backends = vec![
        BroadcasterBackend::Native,
        BroadcasterBackend::Vm,
        BroadcasterBackend::Rfq,
    ];
    let handoff = BroadcasterGenerationHandoff::new(
        previous_stream_id,
        previous_entry_id,
        vec![
            BroadcasterBackendHead::new(BroadcasterBackend::Native, 100),
            BroadcasterBackendHead::new(BroadcasterBackend::Vm, 101),
            BroadcasterBackendHead::new(BroadcasterBackend::Rfq, rfq_cursor_timestamp_seconds),
        ],
    )?;
    let progress = BroadcasterProgress::new_with_handoff(
        chain_id,
        format!("chain-{chain_id}-snapshot-2"),
        backends,
        "state history analysis handoff".to_string(),
        handoff,
    )?;
    let envelope =
        BroadcasterEnvelope::new(next_stream_id, 1, BroadcasterPayload::Progress(progress));
    BroadcasterRedisStreamEntry::from_envelope(chain_id, &envelope).map_err(Into::into)
}

async fn assert_duplicate_handoff_enqueue_is_healthy(
    pg_store: &StateHistoryPgStore,
    marker: BroadcasterRedisStreamEntry,
) -> Result<()> {
    let writer = StateHistoryWriter::spawn(
        pg_store.clone(),
        StateHistoryWriterConfig {
            queue_capacity: 4,
            retry_window: Duration::from_millis(200),
        },
    )?;
    writer
        .enqueue_entry(marker.clone(), "2-1".to_string())
        .await?;
    writer.enqueue_entry(marker, "2-1".to_string()).await?;

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let snapshot = writer.snapshot().await;
        if snapshot.persisted_deltas >= 2 || Instant::now() >= deadline {
            anyhow::ensure!(
                snapshot.healthy,
                "duplicate handoff marker enqueue left writer unhealthy: {:?}",
                snapshot.last_error
            );
            anyhow::ensure!(
                snapshot.persisted_deltas >= 2,
                "duplicate handoff marker enqueue was not persisted before timeout"
            );
            return Ok(());
        }
        sleep(Duration::from_millis(10)).await;
    }
}

fn synthetic_delta_entry(
    chain_id: u64,
    stream_id: &str,
    message_seq: u64,
    backend: BroadcasterBackend,
    cursor: u64,
) -> Result<BroadcasterRedisStreamEntry> {
    let partition = BroadcasterUpdatePartition::new(
        backend,
        cursor,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        sync_statuses(backend),
    );
    let payload = BroadcasterPayload::Update(BroadcasterUpdateMessage::new(vec![partition])?);
    let envelope = BroadcasterEnvelope::new(stream_id, message_seq, payload);
    BroadcasterRedisStreamEntry::from_envelope(chain_id, &envelope).map_err(Into::into)
}

fn synthetic_checkpoint_archive(
    chain_id: u64,
    stream_id: &str,
    captured_at_timestamp_ms: u64,
    rfq_update_timestamp_ms: u64,
) -> Result<CheckpointArchive> {
    synthetic_checkpoint_archive_with_cursor(
        chain_id,
        stream_id,
        captured_at_timestamp_ms,
        rfq_update_timestamp_ms,
        100,
        1,
    )
}

fn synthetic_checkpoint_archive_with_cursor(
    chain_id: u64,
    stream_id: &str,
    captured_at_timestamp_ms: u64,
    rfq_update_timestamp_ms: u64,
    block_number: u64,
    source_message_seq: u64,
) -> Result<CheckpointArchive> {
    let backends = vec![
        BroadcasterBackend::Native,
        BroadcasterBackend::Vm,
        BroadcasterBackend::Rfq,
    ];
    let snapshot_id = "state-history-analysis-snapshot";
    Ok(CheckpointArchive {
        metadata: CheckpointArchiveMetadata {
            chain_id,
            block_number,
            captured_at_timestamp_ms,
            rfq_update_timestamp_ms: Some(rfq_update_timestamp_ms),
            stream_id: stream_id.to_string(),
            source_message_seq,
            backends: backends.clone(),
        },
        payloads: vec![
            BroadcasterEnvelope::new(
                stream_id,
                1,
                BroadcasterPayload::SnapshotStart(BroadcasterSnapshotStart::new(
                    snapshot_id,
                    chain_id,
                    backends,
                    0,
                )?),
            ),
            branch_checkpoint_chunk_envelope(stream_id, snapshot_id)?,
            BroadcasterEnvelope::new(
                stream_id,
                3,
                BroadcasterPayload::SnapshotEnd(BroadcasterSnapshotEnd::new(snapshot_id)),
            ),
        ],
    })
}

fn branch_checkpoint_chunk_envelope(
    stream_id: &str,
    snapshot_id: &str,
) -> Result<BroadcasterEnvelope> {
    let partition = BroadcasterSnapshotPartition::new(
        BroadcasterBackend::Native,
        START_BLOCK_NUMBER,
        Vec::new(),
        branch_sync_statuses_with_block(
            "uniswap_v2",
            BroadcasterBlockRef {
                hash: vec![NATIVE_HASH_SEED; 32].into(),
                number: START_BLOCK_NUMBER,
                parent_hash: vec![NATIVE_HASH_SEED.saturating_add(1); 32].into(),
                revert: false,
                timestamp: 1_700_000_000,
                partial_block_index: None,
            },
        ),
    );
    let chunk = BroadcasterSnapshotChunk::new(snapshot_id, 0, vec![partition])?;
    Ok(BroadcasterEnvelope::new(
        stream_id,
        2,
        BroadcasterPayload::SnapshotChunk(chunk),
    ))
}

fn branch_sync_statuses_with_block(
    protocol: &str,
    block: BroadcasterBlockRef,
) -> BTreeMap<String, BroadcasterProtocolSyncStatus> {
    BTreeMap::from([(
        protocol.to_string(),
        BroadcasterProtocolSyncStatus {
            kind: BroadcasterProtocolSyncStatusKind::Ready,
            block: Some(block),
            reason: None,
        },
    )])
}

fn sync_statuses(backend: BroadcasterBackend) -> BTreeMap<String, BroadcasterProtocolSyncStatus> {
    let protocol = match backend {
        BroadcasterBackend::Native => "uniswap_v2",
        BroadcasterBackend::Vm => "vm:curve",
        BroadcasterBackend::Rfq => "rfq:hashflow",
    };
    BTreeMap::from([(
        protocol.to_string(),
        BroadcasterProtocolSyncStatus {
            kind: BroadcasterProtocolSyncStatusKind::Ready,
            block: None,
            reason: None,
        },
    )])
}

#[derive(Debug)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "one independent pass flag per harness data check"
)]
struct BacktestScenarioReport {
    chain_id: u64,
    stream_id: String,
    inserted_deltas: usize,
    replayed_message_sequences: Vec<u64>,
    checkpoint_manifest_id: i64,
    checkpoint_s3_key: String,
    checkpoint_payload_hash: String,
    decoded_checkpoint_payloads: usize,
    backtest_start_block_timestamp_ms: Option<u64>,
    backtest_end_block_timestamp_ms: Option<u64>,
    rfq_end_timestamp_ms: u64,
    reorg_superseded: bool,
    stale_write_kept: bool,
    duplicate_write_left_updated_at_untouched: bool,
    cross_stream_superseded: bool,
    older_generation_write_kept: bool,
    source_advanced_without_churn: bool,
    snapshot_seeded_start_boundary: bool,
    head_range_unresolvable: bool,
    conflicted_checkpoint_rejected: bool,
    recorded_gaps: usize,
    generation_switch_gaps: usize,
    unproven_ingestion_gaps: usize,
}

async fn run_backtest_scenario(
    pg_store: &StateHistoryPgStore,
    checkpoint_store: S3CheckpointStore,
    s3_prefix: &str,
    chain_id: u64,
    run_id: u64,
) -> Result<BacktestScenarioReport> {
    let stream_id = format!("chain-{chain_id}-stream-1");
    let stale_stream_id = format!("chain-{chain_id}-stream-9");
    let times = FixtureTimestamps::for_run(run_id);
    let mut inserted_deltas = 0usize;
    insert_backtest_live_stream_fixtures(
        pg_store,
        chain_id,
        &stream_id,
        &times,
        &mut inserted_deltas,
    )
    .await?;
    let duplicate_write_left_updated_at_untouched =
        assert_idempotent_duplicate(pg_store, chain_id, &stream_id, &times, &mut inserted_deltas)
            .await?;

    let checkpoint_writer = StateHistoryCheckpointWriter::new(
        pg_store.clone(),
        checkpoint_store.clone(),
        s3_prefix.to_string(),
    );
    let (checkpoint, snapshot_seeded_start_boundary) = write_and_verify_checkpoint(
        pg_store,
        &checkpoint_writer,
        chain_id,
        &stream_id,
        run_id,
        &times,
    )
    .await?;
    let supersession = run_supersession_checks(
        pg_store,
        &checkpoint_writer,
        chain_id,
        &stream_id,
        &stale_stream_id,
        &times,
        run_id,
        &mut inserted_deltas,
    )
    .await?;
    seed_backtest_lineage_checkpoints(&checkpoint_writer, chain_id, &stream_id, run_id, &times)
        .await?;
    advance_backtest_cursor_heads(pg_store, chain_id, &stream_id, &times).await?;

    let reader = StateHistoryReader::new(pg_store.clone(), checkpoint_store);
    record_pre_checkpoint_gap(pg_store, chain_id, &stream_id).await?;
    let request = backtest_history_request(chain_id, &times)?;
    let plan = reader.resolve_range(request.clone()).await?;
    let replayed_message_sequences = assert_backtest_replay_plan(&plan, &stream_id)?;
    let backtest_plan = reader
        .resolve_backtest_range(synthetic_backtest_request(chain_id, END_BLOCK_NUMBER)?)
        .await?;
    let rfq_end_timestamp_ms = assert_backtest_plan(&backtest_plan, &plan, &times)?;
    let head_range_unresolvable = assert_head_range_unresolvable(&reader, chain_id).await?;
    let conflicted_checkpoint_rejected = assert_conflicted_checkpoint_rejected(
        pg_store,
        &checkpoint_writer,
        chain_id,
        &stream_id,
        run_id,
        &times,
    )
    .await?;
    let decoded = fetch_decoded_checkpoint(&reader, &plan).await?;
    let (recorded_gaps, generation_switch_gaps, unproven_ingestion_gaps) =
        assert_visible_recorded_gap(
            &reader,
            pg_store,
            chain_id,
            &stream_id,
            &stale_stream_id,
            request,
        )
        .await?;

    Ok(BacktestScenarioReport {
        chain_id,
        stream_id,
        inserted_deltas,
        replayed_message_sequences,
        checkpoint_manifest_id: checkpoint.manifest_id,
        checkpoint_s3_key: checkpoint.s3_key,
        checkpoint_payload_hash: checkpoint.payload.hash,
        decoded_checkpoint_payloads: decoded.archive.payloads.len(),
        backtest_start_block_timestamp_ms: backtest_plan.start_block_timestamp_ms,
        backtest_end_block_timestamp_ms: backtest_plan.end_block_timestamp_ms,
        rfq_end_timestamp_ms,
        reorg_superseded: supersession.reorg_superseded,
        stale_write_kept: supersession.stale_write_kept,
        duplicate_write_left_updated_at_untouched,
        cross_stream_superseded: supersession.cross_stream_superseded,
        older_generation_write_kept: supersession.older_generation_write_kept,
        source_advanced_without_churn: supersession.source_advanced_without_churn,
        snapshot_seeded_start_boundary,
        head_range_unresolvable,
        conflicted_checkpoint_rejected,
        recorded_gaps,
        generation_switch_gaps,
        unproven_ingestion_gaps,
    })
}

async fn assert_checkpoint_reorg_supersession(
    pg_store: &StateHistoryPgStore,
    checkpoint_writer: &StateHistoryCheckpointWriter,
    chain_id: u64,
    stream_id: &str,
    times: &FixtureTimestamps,
    run_id: u64,
) -> Result<(bool, BlockTimestampRecord)> {
    let archive = backtest_checkpoint_archive_with_cursor(
        chain_id,
        stream_id,
        run_id.saturating_add(2),
        times.rfq_cursor_timestamp_ms,
        NEXT_BLOCK_NUMBER,
        9,
        times.next_block_timestamp_ms,
        REORG_HASH_SEED,
    )?;
    checkpoint_writer.write_checkpoint(archive).await?;
    let record = expect_block_timestamp_record(
        pg_store,
        chain_id,
        NEXT_BLOCK_NUMBER,
        times.next_block_timestamp_ms,
        REORG_HASH_SEED,
        stream_id,
        9,
    )
    .await?;
    anyhow::ensure!(
        record.updated_at > record.created_at,
        "checkpoint reorg supersession must bump updated_at on block {NEXT_BLOCK_NUMBER}"
    );
    Ok((true, record))
}

async fn assert_checkpoint_stale_writer_kept(
    pg_store: &StateHistoryPgStore,
    checkpoint_writer: &StateHistoryCheckpointWriter,
    chain_id: u64,
    stream_id: &str,
    times: &FixtureTimestamps,
    run_id: u64,
    reorg_record: &BlockTimestampRecord,
) -> Result<bool> {
    let archive = backtest_checkpoint_archive_with_cursor(
        chain_id,
        stream_id,
        run_id.saturating_add(3),
        times.rfq_cursor_timestamp_ms,
        NEXT_BLOCK_NUMBER,
        7,
        times.next_block_timestamp_ms,
        STALE_WRITER_HASH_SEED,
    )?;
    checkpoint_writer.write_checkpoint(archive).await?;
    let record = fetch_block_timestamp_record(pg_store, chain_id, NEXT_BLOCK_NUMBER).await?;
    anyhow::ensure!(
        &record == reorg_record,
        "stale same-stream checkpoint must leave the superseded row untouched"
    );
    Ok(true)
}

async fn advance_backtest_cursor_heads(
    pg_store: &StateHistoryPgStore,
    chain_id: u64,
    stream_id: &str,
    times: &FixtureTimestamps,
) -> Result<()> {
    let writer = StateHistoryWriter::spawn(
        pg_store.clone(),
        StateHistoryWriterConfig {
            queue_capacity: 4,
            retry_window: Duration::from_millis(200),
        },
    )?;
    let heartbeat = BroadcasterRedisStreamEntry::from_envelope(
        chain_id,
        &BroadcasterEnvelope::new(
            stream_id,
            7,
            BroadcasterPayload::Heartbeat(BroadcasterHeartbeat::new(
                chain_id,
                format!("chain-{chain_id}-snapshot-1"),
                vec![
                    BroadcasterBackendHead::new(BroadcasterBackend::Native, NEXT_BLOCK_NUMBER),
                    BroadcasterBackendHead::new(BroadcasterBackend::Vm, NEXT_BLOCK_NUMBER),
                    BroadcasterBackendHead::new(
                        BroadcasterBackend::Rfq,
                        times.next_block_timestamp_seconds(),
                    ),
                ],
            )?),
        ),
    )?;
    writer.enqueue_entry(heartbeat, "1-7".to_string()).await?;
    sleep(Duration::from_millis(100)).await;
    let snapshot = writer.snapshot().await;
    anyhow::ensure!(
        snapshot.healthy,
        "heartbeat observation left writer unhealthy: {:?}",
        snapshot.last_error
    );
    Ok(())
}

struct FixtureTimestamps {
    rfq_cursor_timestamp_ms: u64,
    start_block_timestamp_ms: u64,
    end_block_timestamp_ms: u64,
    next_block_timestamp_ms: u64,
}

impl FixtureTimestamps {
    fn for_run(run_id: u64) -> Self {
        let rfq_cursor_timestamp_ms = 1_700_000_000_000u64.saturating_add((run_id % 1_000) * 1_000);
        let end_block_timestamp_ms = rfq_cursor_timestamp_ms + 100_000;
        Self {
            rfq_cursor_timestamp_ms,
            start_block_timestamp_ms: rfq_cursor_timestamp_ms + 1_000,
            end_block_timestamp_ms,
            next_block_timestamp_ms: end_block_timestamp_ms + 2_000,
        }
    }

    fn rfq_end_timestamp_ms(&self) -> u64 {
        self.next_block_timestamp_ms - 1
    }

    fn rfq_cursor_timestamp_seconds(&self) -> u64 {
        self.rfq_cursor_timestamp_ms / 1_000
    }

    fn next_block_timestamp_seconds(&self) -> u64 {
        self.next_block_timestamp_ms / 1_000
    }
}

#[expect(
    clippy::struct_excessive_bools,
    reason = "one independent pass flag per harness data check"
)]
struct SupersessionCheckOutcomes {
    reorg_superseded: bool,
    stale_write_kept: bool,
    source_advanced_without_churn: bool,
    cross_stream_superseded: bool,
    older_generation_write_kept: bool,
}

#[expect(
    clippy::too_many_arguments,
    reason = "fixture helper keeps each proof input explicit"
)]
async fn run_supersession_checks(
    pg_store: &StateHistoryPgStore,
    checkpoint_writer: &StateHistoryCheckpointWriter,
    chain_id: u64,
    stream_id: &str,
    stale_stream_id: &str,
    times: &FixtureTimestamps,
    run_id: u64,
    inserted_deltas: &mut usize,
) -> Result<SupersessionCheckOutcomes> {
    let (reorg_superseded, reorg_record) = assert_checkpoint_reorg_supersession(
        pg_store,
        checkpoint_writer,
        chain_id,
        stream_id,
        times,
        run_id,
    )
    .await?;
    let stale_write_kept = assert_checkpoint_stale_writer_kept(
        pg_store,
        checkpoint_writer,
        chain_id,
        stream_id,
        times,
        run_id,
        &reorg_record,
    )
    .await?;
    let (source_advanced_without_churn, cross_stream_superseded) =
        assert_stale_stream_writes(pg_store, chain_id, stale_stream_id, times, inserted_deltas)
            .await?;
    let older_generation_write_kept = assert_older_generation_write_kept(
        pg_store,
        chain_id,
        stale_stream_id,
        times,
        inserted_deltas,
    )
    .await?;
    Ok(SupersessionCheckOutcomes {
        reorg_superseded,
        stale_write_kept,
        source_advanced_without_churn,
        cross_stream_superseded,
        older_generation_write_kept,
    })
}

async fn assert_older_generation_write_kept(
    pg_store: &StateHistoryPgStore,
    chain_id: u64,
    newer_stream_id: &str,
    times: &FixtureTimestamps,
    inserted_deltas: &mut usize,
) -> Result<bool> {
    let older_stream_id = format!("chain-{chain_id}-stream-8");
    let older = backtest_delta_entry(
        chain_id,
        &older_stream_id,
        100,
        BroadcasterBackend::Native,
        NEXT_BLOCK_NUMBER,
        Some(times.next_block_timestamp_ms),
        Some(OLDER_GENERATION_HASH_SEED),
    )?;
    insert_delta_checked(pg_store, &older, "8-100", inserted_deltas).await?;
    let retained = expect_block_timestamp_record(
        pg_store,
        chain_id,
        NEXT_BLOCK_NUMBER,
        times.next_block_timestamp_ms,
        CROSS_STREAM_HASH_SEED,
        newer_stream_id,
        3,
    )
    .await?;
    anyhow::ensure!(
        retained.updated_at > retained.created_at,
        "older generation write changed the retained generation-9 row"
    );
    Ok(true)
}

async fn insert_backtest_live_stream_fixtures(
    pg_store: &StateHistoryPgStore,
    chain_id: u64,
    stream_id: &str,
    times: &FixtureTimestamps,
    inserted_deltas: &mut usize,
) -> Result<()> {
    for entry in backtest_delta_entries(chain_id, stream_id, times)? {
        let redis_entry_id = format!("1-{}", entry.message_seq);
        let persisted =
            insert_delta_checked(pg_store, &entry, &redis_entry_id, inserted_deltas).await?;
        anyhow::ensure!(
            persisted.inserted,
            "synthetic fixture delta {} was already present",
            entry.message_seq
        );
    }
    Ok(())
}

async fn insert_delta_checked(
    pg_store: &StateHistoryPgStore,
    entry: &BroadcasterRedisStreamEntry,
    redis_entry_id: &str,
    inserted_deltas: &mut usize,
) -> Result<PersistedDelta> {
    let persisted = pg_store
        .insert_entry(entry, Some(redis_entry_id))
        .await
        .with_context(|| {
            format!(
                "failed to insert synthetic delta {} on stream {}",
                entry.message_seq, entry.stream_id
            )
        })?;
    // Every clean fixture write must extract its timestamps without skips.
    anyhow::ensure!(
        persisted.skipped_block_timestamp_records == 0,
        "synthetic delta {} on stream {} skipped {} block timestamp records",
        entry.message_seq,
        entry.stream_id,
        persisted.skipped_block_timestamp_records
    );
    if persisted.inserted {
        *inserted_deltas = inserted_deltas.saturating_add(1);
    }
    Ok(persisted)
}

// Seqs 1-2 carry no block refs, so the start boundary must stay absent until
// the checkpoint seeds it, and a verbatim seq-5 redelivery must leave the
// end-boundary row byte-identical with updated_at untouched.
async fn assert_idempotent_duplicate(
    pg_store: &StateHistoryPgStore,
    chain_id: u64,
    stream_id: &str,
    times: &FixtureTimestamps,
    inserted_deltas: &mut usize,
) -> Result<bool> {
    anyhow::ensure!(
        pg_store
            .block_timestamp_record(chain_id, START_BLOCK_NUMBER)
            .await?
            .is_none(),
        "start boundary block {START_BLOCK_NUMBER} must stay absent until the checkpoint seeds it"
    );
    let duplicate = backtest_delta_entry_with_partitions(
        chain_id,
        stream_id,
        5,
        vec![
            (
                BroadcasterBackend::Native,
                END_BLOCK_NUMBER,
                Some(times.end_block_timestamp_ms),
                None,
            ),
            (
                BroadcasterBackend::Rfq,
                times.next_block_timestamp_seconds() + 1,
                None,
                None,
            ),
        ],
    )?;
    let persisted = insert_delta_checked(pg_store, &duplicate, "1-5", inserted_deltas).await?;
    anyhow::ensure!(
        !persisted.inserted,
        "verbatim duplicate delta must be deduplicated"
    );
    let record = expect_block_timestamp_record(
        pg_store,
        chain_id,
        END_BLOCK_NUMBER,
        times.end_block_timestamp_ms,
        NATIVE_HASH_SEED,
        stream_id,
        5,
    )
    .await?;
    anyhow::ensure!(
        record.updated_at == record.created_at,
        "verbatim duplicate write must not touch updated_at on block {END_BLOCK_NUMBER}"
    );
    Ok(true)
}

// The stale-generation writes stay one envelope per block: a shared envelope
// only keeps its block cursor when every chain partition agrees on the height,
// and a NULL cursor would hide the delta from generation-switch detection. The
// content-identical block-101 duplicate advances provenance without churn, the
// conflicting block-111 head overwrites cross-stream despite the lower seq.
async fn assert_stale_stream_writes(
    pg_store: &StateHistoryPgStore,
    chain_id: u64,
    stale_stream_id: &str,
    times: &FixtureTimestamps,
    inserted_deltas: &mut usize,
) -> Result<(bool, bool)> {
    let duplicate = backtest_delta_entry(
        chain_id,
        stale_stream_id,
        2,
        BroadcasterBackend::Vm,
        VM_BLOCK_NUMBER,
        Some(times.start_block_timestamp_ms + 1_000),
        None,
    )?;
    insert_delta_checked(pg_store, &duplicate, "stale-2", inserted_deltas).await?;
    let conflicting = backtest_delta_entry(
        chain_id,
        stale_stream_id,
        3,
        BroadcasterBackend::Native,
        NEXT_BLOCK_NUMBER,
        Some(times.next_block_timestamp_ms),
        Some(CROSS_STREAM_HASH_SEED),
    )?;
    insert_delta_checked(pg_store, &conflicting, "stale-3", inserted_deltas).await?;

    let advanced = expect_block_timestamp_record(
        pg_store,
        chain_id,
        VM_BLOCK_NUMBER,
        times.start_block_timestamp_ms + 1_000,
        VM_HASH_SEED,
        stale_stream_id,
        2,
    )
    .await?;
    anyhow::ensure!(
        advanced.updated_at == advanced.created_at,
        "content-identical cross-stream write must not touch updated_at on block {VM_BLOCK_NUMBER}"
    );

    let superseded = expect_block_timestamp_record(
        pg_store,
        chain_id,
        NEXT_BLOCK_NUMBER,
        times.next_block_timestamp_ms,
        CROSS_STREAM_HASH_SEED,
        stale_stream_id,
        3,
    )
    .await?;
    anyhow::ensure!(
        superseded.updated_at > superseded.created_at,
        "conflicting cross-stream write must bump updated_at on block {NEXT_BLOCK_NUMBER}"
    );
    Ok((true, true))
}

async fn write_and_verify_checkpoint(
    pg_store: &StateHistoryPgStore,
    checkpoint_writer: &StateHistoryCheckpointWriter,
    chain_id: u64,
    stream_id: &str,
    run_id: u64,
    times: &FixtureTimestamps,
) -> Result<(CheckpointWriteOutcome, bool)> {
    let checkpoint_archive = backtest_checkpoint_archive(chain_id, stream_id, run_id, times)?;
    let checkpoint_source_message_seq = checkpoint_archive.metadata.source_message_seq;
    let checkpoint = checkpoint_writer
        .write_checkpoint(checkpoint_archive)
        .await
        .context("failed to write synthetic checkpoint")?;
    let snapshot_seeded_start_boundary = assert_snapshot_seeded_start_boundary(
        pg_store,
        chain_id,
        stream_id,
        checkpoint_source_message_seq,
        times,
    )
    .await?;
    Ok((checkpoint, snapshot_seeded_start_boundary))
}

async fn seed_backtest_lineage_checkpoints(
    checkpoint_writer: &StateHistoryCheckpointWriter,
    chain_id: u64,
    stream_id: &str,
    run_id: u64,
    times: &FixtureTimestamps,
) -> Result<()> {
    for block_number in (VM_BLOCK_NUMBER + 1)..END_BLOCK_NUMBER {
        let archive = backtest_checkpoint_archive_with_cursor(
            chain_id,
            stream_id,
            run_id.saturating_add(block_number),
            times.rfq_cursor_timestamp_ms,
            block_number,
            block_number,
            times
                .start_block_timestamp_ms
                .saturating_add((block_number - START_BLOCK_NUMBER) * 1_000),
            NATIVE_HASH_SEED,
        )?;
        checkpoint_writer.write_checkpoint(archive).await?;
    }
    Ok(())
}

async fn fetch_decoded_checkpoint(
    reader: &StateHistoryReader,
    plan: &HistoryRangePlan,
) -> Result<DecodedCheckpointArchive> {
    let selected_checkpoint = plan
        .checkpoint
        .as_ref()
        .ok_or_else(|| anyhow!("expected a complete checkpoint for the synthetic range"))?;
    let decoded = reader.fetch_checkpoint(selected_checkpoint).await?;
    anyhow::ensure!(
        decoded.archive.payloads.len() == 3,
        "expected checkpoint archive start/chunk/end payloads, got {}",
        decoded.archive.payloads.len()
    );
    Ok(decoded)
}

// Only the completed checkpoint may seed the start boundary, stamped with the
// archive's replay-boundary cursor.
async fn assert_snapshot_seeded_start_boundary(
    pg_store: &StateHistoryPgStore,
    chain_id: u64,
    stream_id: &str,
    checkpoint_source_message_seq: u64,
    times: &FixtureTimestamps,
) -> Result<bool> {
    let record = expect_block_timestamp_record(
        pg_store,
        chain_id,
        START_BLOCK_NUMBER,
        times.start_block_timestamp_ms,
        NATIVE_HASH_SEED,
        stream_id,
        checkpoint_source_message_seq,
    )
    .await?;
    anyhow::ensure!(
        record.updated_at == record.created_at,
        "snapshot-seeded boundary row must be a fresh insert"
    );
    Ok(true)
}

// Block 111 is the recorded head (no block-112 row), so an RFQ range ending
// there must hard-error instead of guessing a bound.
async fn assert_head_range_unresolvable(
    reader: &StateHistoryReader,
    chain_id: u64,
) -> Result<bool> {
    let request = synthetic_backtest_request(chain_id, NEXT_BLOCK_NUMBER)?;
    let Err(error) = reader.resolve_backtest_range(request).await else {
        anyhow::bail!("backtest range ending at the recorded head must be unresolvable")
    };
    let message = format!("{error:#}");
    anyhow::ensure!(
        message.contains(&format!(
            "missing state history block timestamp for block {}",
            NEXT_BLOCK_NUMBER + 1
        )),
        "unexpected head-range error: {message}"
    );
    Ok(true)
}

// A mid-reorg archive that disagrees about the boundary height must be
// rejected before any manifest or S3 object exists, leaving the boundary row
// exactly as the previous checkpoint wrote it.
async fn assert_conflicted_checkpoint_rejected(
    pg_store: &StateHistoryPgStore,
    checkpoint_writer: &StateHistoryCheckpointWriter,
    chain_id: u64,
    stream_id: &str,
    run_id: u64,
    times: &FixtureTimestamps,
) -> Result<bool> {
    let before = fetch_block_timestamp_record(pg_store, chain_id, START_BLOCK_NUMBER).await?;
    let status_before = checkpoint_writer.snapshot().await;
    let archive = conflicted_checkpoint_archive(chain_id, stream_id, run_id, times)?;
    let Err(error) = checkpoint_writer.write_checkpoint(archive).await else {
        anyhow::bail!("conflicted-boundary checkpoint must be rejected")
    };
    let message = format!("{error:#}");
    anyhow::ensure!(
        message.contains(&format!(
            "no usable block timestamp for boundary block {START_BLOCK_NUMBER}"
        )),
        "unexpected conflicted-checkpoint error: {message}"
    );
    let after = fetch_block_timestamp_record(pg_store, chain_id, START_BLOCK_NUMBER).await?;
    anyhow::ensure!(
        after == before,
        "rejected checkpoint must not touch the boundary timestamp row"
    );
    let snapshot = checkpoint_writer.snapshot().await;
    anyhow::ensure!(
        snapshot.failed_checkpoints == status_before.failed_checkpoints.saturating_add(1),
        "conflicted checkpoint did not increment the failed count to {}",
        status_before.failed_checkpoints.saturating_add(1)
    );
    anyhow::ensure!(
        snapshot.attempted_checkpoints == status_before.attempted_checkpoints.saturating_add(1)
            && snapshot.completed_checkpoints == status_before.completed_checkpoints,
        "conflicted checkpoint must increment attempts without completing"
    );
    Ok(true)
}

async fn assert_visible_recorded_gap(
    reader: &StateHistoryReader,
    pg_store: &StateHistoryPgStore,
    chain_id: u64,
    stream_id: &str,
    stale_stream_id: &str,
    request: HistoryRangeRequest,
) -> Result<(usize, usize, usize)> {
    record_backtest_visible_gap_fixtures(pg_store, chain_id, stream_id, stale_stream_id).await?;
    let gap_plan = reader.resolve_range(request).await?;
    let recorded_gaps = gap_plan
        .gaps
        .iter()
        .filter(|gap| gap.source == HistoryRangeGapSource::RecordedGap)
        .count();
    anyhow::ensure!(
        recorded_gaps == 1,
        "expected one recorded synthetic gap, got {recorded_gaps}"
    );
    let generation_switch_gaps = generation_switch_gap_count(&gap_plan);
    anyhow::ensure!(
        generation_switch_gaps == 1,
        "expected one generation-switch gap in backtest scenario, got {generation_switch_gaps}"
    );
    let unproven_ingestion_gaps = unproven_ingestion_gap_count(&gap_plan);
    anyhow::ensure!(
        unproven_ingestion_gaps == 0,
        "backtest scenario should have no unproven ingestion gaps, got {unproven_ingestion_gaps}"
    );
    Ok((
        recorded_gaps,
        generation_switch_gaps,
        unproven_ingestion_gaps,
    ))
}

async fn record_backtest_visible_gap_fixtures(
    pg_store: &StateHistoryPgStore,
    chain_id: u64,
    stream_id: &str,
    stale_stream_id: &str,
) -> Result<()> {
    for (stream_id, reason) in [
        (stream_id, "state history analysis backtest synthetic gap"),
        (
            stale_stream_id,
            "state history analysis backtest stale-stream gap",
        ),
    ] {
        pg_store
            .record_gap(&IngestionGap {
                chain_id,
                stream_id: stream_id.to_string(),
                from_message_seq: 9,
                to_message_seq: 9,
                prev_persistable_message_seq: Some(8),
                backend_scope: vec![BroadcasterBackend::Native],
                from_block_number: Some(108),
                to_block_number: Some(109),
                from_timestamp_ms: None,
                to_timestamp_ms: None,
                reason: reason.to_string(),
            })
            .await?;
    }
    Ok(())
}

async fn fetch_block_timestamp_record(
    pg_store: &StateHistoryPgStore,
    chain_id: u64,
    block_number: u64,
) -> Result<BlockTimestampRecord> {
    pg_store
        .block_timestamp_record(chain_id, block_number)
        .await?
        .ok_or_else(|| {
            anyhow!("missing state history block timestamp record for block {block_number}")
        })
}

async fn expect_block_timestamp_record(
    pg_store: &StateHistoryPgStore,
    chain_id: u64,
    block_number: u64,
    timestamp_ms: u64,
    hash_seed: u8,
    source_stream_id: &str,
    source_message_seq: u64,
) -> Result<BlockTimestampRecord> {
    let record = fetch_block_timestamp_record(pg_store, chain_id, block_number).await?;
    anyhow::ensure!(
        record.timestamp_ms == timestamp_ms,
        "block {block_number} stored timestamp {} instead of {timestamp_ms}",
        record.timestamp_ms
    );
    anyhow::ensure!(
        record.block_hash == expected_hash(hash_seed),
        "block {block_number} stored an unexpected block hash"
    );
    anyhow::ensure!(
        record.source_stream_id == source_stream_id
            && record.source_message_seq == source_message_seq,
        "block {block_number} has provenance ({}, {}) instead of ({source_stream_id}, {source_message_seq})",
        record.source_stream_id,
        record.source_message_seq
    );
    Ok(record)
}

fn expected_hash(seed: u8) -> Vec<u8> {
    vec![seed; 32]
}

fn backtest_history_request(
    chain_id: u64,
    times: &FixtureTimestamps,
) -> Result<HistoryRangeRequest> {
    HistoryRangeRequest::new(
        chain_id,
        START_BLOCK_NUMBER,
        END_BLOCK_NUMBER,
        vec![
            BroadcasterBackend::Native,
            BroadcasterBackend::Vm,
            BroadcasterBackend::Rfq,
        ],
    )?
    .with_rfq_timestamp_range(times.start_block_timestamp_ms, times.rfq_end_timestamp_ms())
}

fn synthetic_backtest_request(
    chain_id: u64,
    end_block_number: u64,
) -> Result<BacktestRangeRequest> {
    BacktestRangeRequest::new(
        chain_id,
        START_BLOCK_NUMBER,
        end_block_number,
        vec![
            BroadcasterBackend::Native,
            BroadcasterBackend::Vm,
            BroadcasterBackend::Rfq,
        ],
    )
}

fn assert_backtest_replay_plan(plan: &HistoryRangePlan, stream_id: &str) -> Result<Vec<u64>> {
    let replayed_message_sequences = plan
        .deltas
        .iter()
        .map(|delta| delta.entry.message_seq)
        .collect::<Vec<_>>();
    anyhow::ensure!(
        replayed_message_sequences == vec![2, 3, 4, 5],
        "expected native, VM, RFQ, and end-boundary native deltas after checkpoint, got {replayed_message_sequences:?}"
    );
    anyhow::ensure!(
        plan.deltas
            .iter()
            .all(|delta| delta.entry.stream_id == stream_id),
        "reader replayed a delta from outside the checkpoint stream"
    );
    assert_projected_backends(&plan.deltas[1], &[BroadcasterBackend::Vm])?;
    assert_projected_backends(
        &plan.deltas[2],
        &[BroadcasterBackend::Native, BroadcasterBackend::Rfq],
    )?;
    assert_projected_backends(&plan.deltas[3], &[BroadcasterBackend::Native])?;
    Ok(replayed_message_sequences)
}

fn assert_projected_backends(
    delta: &state_history::StoredDeltaEntry,
    expected: &[BroadcasterBackend],
) -> Result<()> {
    let envelope: BroadcasterEnvelope = serde_json::from_str(&delta.entry.payload_json)
        .context("failed to decode projected replay envelope")?;
    let BroadcasterPayload::Update(update) = envelope.payload else {
        anyhow::bail!("expected projected replay entry to remain an update")
    };
    let actual = update
        .partitions
        .iter()
        .map(|partition| partition.backend)
        .collect::<Vec<_>>();
    anyhow::ensure!(
        actual == expected,
        "replay projection kept backends {actual:?}, expected {expected:?}"
    );
    Ok(())
}

fn assert_backtest_plan(
    backtest_plan: &BacktestRangePlan,
    explicit_plan: &HistoryRangePlan,
    times: &FixtureTimestamps,
) -> Result<u64> {
    anyhow::ensure!(
        backtest_plan.start_block_timestamp_ms == Some(times.start_block_timestamp_ms),
        "backtest resolver picked the wrong start block timestamp"
    );
    anyhow::ensure!(
        backtest_plan.end_block_timestamp_ms == Some(times.end_block_timestamp_ms),
        "backtest resolver picked the wrong end block timestamp"
    );
    anyhow::ensure!(
        backtest_plan.history.request.rfq_start_timestamp_ms
            == Some(times.start_block_timestamp_ms),
        "backtest resolver picked the wrong RFQ start timestamp"
    );
    let rfq_end_timestamp_ms = backtest_plan
        .history
        .request
        .rfq_end_timestamp_ms
        .ok_or_else(|| anyhow!("backtest resolver dropped the RFQ end timestamp"))?;
    anyhow::ensure!(
        rfq_end_timestamp_ms == times.rfq_end_timestamp_ms(),
        "backtest RFQ end bound must be the next block timestamp minus 1ms"
    );
    anyhow::ensure!(
        &backtest_plan.history == explicit_plan,
        "block-range backtest resolver produced a different replay plan than the explicit RFQ timestamp request"
    );
    Ok(rfq_end_timestamp_ms)
}

fn backtest_delta_entries(
    chain_id: u64,
    stream_id: &str,
    times: &FixtureTimestamps,
) -> Result<Vec<BroadcasterRedisStreamEntry>> {
    // Seqs 1-2 carry no refs on purpose: the start boundary must come from the
    // checkpoint, not from update harvesting. Seq 6 records the end+1 head
    // that bounds the RFQ range and anchors the reorg fixtures.
    vec![
        backtest_delta_entry(
            chain_id,
            stream_id,
            1,
            BroadcasterBackend::Native,
            START_BLOCK_NUMBER,
            None,
            None,
        ),
        backtest_delta_entry(
            chain_id,
            stream_id,
            2,
            BroadcasterBackend::Native,
            START_BLOCK_NUMBER,
            None,
            None,
        ),
        backtest_delta_entry(
            chain_id,
            stream_id,
            3,
            BroadcasterBackend::Vm,
            VM_BLOCK_NUMBER,
            Some(times.start_block_timestamp_ms + 1_000),
            None,
        ),
        backtest_delta_entry_with_partitions(
            chain_id,
            stream_id,
            4,
            vec![
                (
                    BroadcasterBackend::Native,
                    START_BLOCK_NUMBER - 1,
                    None,
                    None,
                ),
                (
                    BroadcasterBackend::Rfq,
                    times.rfq_cursor_timestamp_seconds() + 10,
                    None,
                    None,
                ),
            ],
        ),
        backtest_delta_entry_with_partitions(
            chain_id,
            stream_id,
            5,
            vec![
                (
                    BroadcasterBackend::Native,
                    END_BLOCK_NUMBER,
                    Some(times.end_block_timestamp_ms),
                    None,
                ),
                (
                    BroadcasterBackend::Rfq,
                    times.next_block_timestamp_seconds() + 1,
                    None,
                    None,
                ),
            ],
        ),
        backtest_delta_entry(
            chain_id,
            stream_id,
            6,
            BroadcasterBackend::Native,
            NEXT_BLOCK_NUMBER,
            Some(times.next_block_timestamp_ms),
            Some(INITIAL_HEAD_HASH_SEED),
        ),
    ]
    .into_iter()
    .collect()
}

fn backtest_delta_entry(
    chain_id: u64,
    stream_id: &str,
    message_seq: u64,
    backend: BroadcasterBackend,
    cursor: u64,
    block_timestamp_ms: Option<u64>,
    hash_seed: Option<u8>,
) -> Result<BroadcasterRedisStreamEntry> {
    backtest_delta_entry_with_partitions(
        chain_id,
        stream_id,
        message_seq,
        vec![(backend, cursor, block_timestamp_ms, hash_seed)],
    )
}

fn backtest_delta_entry_with_partitions(
    chain_id: u64,
    stream_id: &str,
    message_seq: u64,
    partitions: Vec<(BroadcasterBackend, u64, Option<u64>, Option<u8>)>,
) -> Result<BroadcasterRedisStreamEntry> {
    let partitions = partitions
        .into_iter()
        .map(|(backend, cursor, block_timestamp_ms, hash_seed)| {
            Ok(BroadcasterUpdatePartition::new(
                backend,
                cursor,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                backtest_sync_statuses(backend, cursor, block_timestamp_ms, hash_seed)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let payload = BroadcasterPayload::Update(BroadcasterUpdateMessage::new(partitions)?);
    let envelope = BroadcasterEnvelope::new(stream_id, message_seq, payload);
    BroadcasterRedisStreamEntry::from_envelope(chain_id, &envelope).map_err(Into::into)
}

fn backtest_checkpoint_archive(
    chain_id: u64,
    stream_id: &str,
    captured_at_timestamp_ms: u64,
    times: &FixtureTimestamps,
) -> Result<CheckpointArchive> {
    backtest_checkpoint_archive_with_cursor(
        chain_id,
        stream_id,
        captured_at_timestamp_ms,
        times.rfq_cursor_timestamp_ms,
        START_BLOCK_NUMBER,
        1,
        times.start_block_timestamp_ms,
        NATIVE_HASH_SEED,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "checkpoint fixture spells out cursor and block provenance"
)]
fn backtest_checkpoint_archive_with_cursor(
    chain_id: u64,
    stream_id: &str,
    captured_at_timestamp_ms: u64,
    rfq_update_timestamp_ms: u64,
    block_number: u64,
    source_message_seq: u64,
    block_timestamp_ms: u64,
    hash_seed: u8,
) -> Result<CheckpointArchive> {
    let backends = vec![
        BroadcasterBackend::Native,
        BroadcasterBackend::Vm,
        BroadcasterBackend::Rfq,
    ];
    let snapshot_id = "state-history-analysis-snapshot";
    // The chunk's sync-status ref is the only source for the block-100 row, so
    // a completed checkpoint provably seeds the backtest start boundary.
    let partition = BroadcasterSnapshotPartition::new(
        BroadcasterBackend::Native,
        START_BLOCK_NUMBER,
        Vec::new(),
        backtest_sync_statuses(
            BroadcasterBackend::Native,
            block_number,
            Some(block_timestamp_ms),
            Some(hash_seed),
        )?,
    );
    Ok(CheckpointArchive {
        metadata: CheckpointArchiveMetadata {
            chain_id,
            block_number,
            captured_at_timestamp_ms,
            rfq_update_timestamp_ms: Some(rfq_update_timestamp_ms),
            stream_id: stream_id.to_string(),
            source_message_seq,
            backends: backends.clone(),
        },
        payloads: vec![
            BroadcasterEnvelope::new(
                stream_id,
                1,
                BroadcasterPayload::SnapshotStart(BroadcasterSnapshotStart::new(
                    snapshot_id,
                    chain_id,
                    backends,
                    1,
                )?),
            ),
            BroadcasterEnvelope::new(
                stream_id,
                2,
                BroadcasterPayload::SnapshotChunk(BroadcasterSnapshotChunk::new(
                    snapshot_id,
                    0,
                    vec![partition],
                )?),
            ),
            BroadcasterEnvelope::new(
                stream_id,
                3,
                BroadcasterPayload::SnapshotEnd(BroadcasterSnapshotEnd::new(snapshot_id)),
            ),
        ],
    })
}

fn conflicted_checkpoint_archive(
    chain_id: u64,
    stream_id: &str,
    captured_at_timestamp_ms: u64,
    times: &FixtureTimestamps,
) -> Result<CheckpointArchive> {
    let backends = vec![BroadcasterBackend::Native, BroadcasterBackend::Vm];
    let snapshot_id = "state-history-analysis-conflicted-snapshot";
    // Native and VM disagree about the boundary hash, mimicking a mid-reorg
    // capture. The collector must poison height 100 and the boundary guard
    // must reject the archive before manifest creation.
    let partitions = vec![
        BroadcasterSnapshotPartition::new(
            BroadcasterBackend::Native,
            START_BLOCK_NUMBER,
            Vec::new(),
            backtest_sync_statuses(
                BroadcasterBackend::Native,
                START_BLOCK_NUMBER,
                Some(times.start_block_timestamp_ms),
                Some(CONFLICT_NATIVE_HASH_SEED),
            )?,
        ),
        BroadcasterSnapshotPartition::new(
            BroadcasterBackend::Vm,
            START_BLOCK_NUMBER,
            Vec::new(),
            backtest_sync_statuses(
                BroadcasterBackend::Vm,
                START_BLOCK_NUMBER,
                Some(times.start_block_timestamp_ms),
                Some(CONFLICT_VM_HASH_SEED),
            )?,
        ),
    ];
    Ok(CheckpointArchive {
        metadata: CheckpointArchiveMetadata {
            chain_id,
            block_number: START_BLOCK_NUMBER,
            captured_at_timestamp_ms: captured_at_timestamp_ms + 1,
            rfq_update_timestamp_ms: None,
            stream_id: stream_id.to_string(),
            source_message_seq: 1,
            backends: backends.clone(),
        },
        payloads: vec![
            BroadcasterEnvelope::new(
                stream_id,
                1,
                BroadcasterPayload::SnapshotStart(BroadcasterSnapshotStart::new(
                    snapshot_id,
                    chain_id,
                    backends,
                    1,
                )?),
            ),
            BroadcasterEnvelope::new(
                stream_id,
                2,
                BroadcasterPayload::SnapshotChunk(BroadcasterSnapshotChunk::new(
                    snapshot_id,
                    0,
                    partitions,
                )?),
            ),
            BroadcasterEnvelope::new(
                stream_id,
                3,
                BroadcasterPayload::SnapshotEnd(BroadcasterSnapshotEnd::new(snapshot_id)),
            ),
        ],
    })
}

fn backtest_sync_statuses(
    backend: BroadcasterBackend,
    block_number: u64,
    block_timestamp_ms: Option<u64>,
    hash_seed: Option<u8>,
) -> Result<BTreeMap<String, BroadcasterProtocolSyncStatus>> {
    let protocol = match backend {
        BroadcasterBackend::Native => "uniswap_v2",
        BroadcasterBackend::Vm => "vm:curve",
        BroadcasterBackend::Rfq => "rfq:hashflow",
    };
    let block = block_timestamp_ms
        .map(|timestamp_ms| {
            let mut block = backtest_block_ref(block_number, backend, timestamp_ms, hash_seed)?;
            if backend == BroadcasterBackend::Vm && block_number == VM_BLOCK_NUMBER {
                block.parent_hash = vec![NATIVE_HASH_SEED; 32].into();
            } else if backend == BroadcasterBackend::Native && block_number > VM_BLOCK_NUMBER {
                // The first native child follows the VM fixture; later children follow native.
                let parent_seed = if block_number == VM_BLOCK_NUMBER + 1 {
                    VM_HASH_SEED
                } else {
                    NATIVE_HASH_SEED
                };
                block.parent_hash = vec![parent_seed; 32].into();
            }
            Ok::<_, anyhow::Error>(block)
        })
        .transpose()?;
    Ok(BTreeMap::from([(
        protocol.to_string(),
        BroadcasterProtocolSyncStatus {
            kind: BroadcasterProtocolSyncStatusKind::Ready,
            block,
            reason: None,
        },
    )]))
}

fn backtest_block_ref(
    number: u64,
    backend: BroadcasterBackend,
    timestamp_ms: u64,
    hash_seed: Option<u8>,
) -> Result<BroadcasterBlockRef> {
    anyhow::ensure!(
        timestamp_ms.is_multiple_of(1_000),
        "synthetic block timestamps must be whole seconds"
    );
    // Hashes derive from the seed alone, so overriding it is how fixtures fork
    // the chain at one height while default hashes stay content-identical
    // across streams.
    let seed = hash_seed.unwrap_or(match backend {
        BroadcasterBackend::Native => NATIVE_HASH_SEED,
        BroadcasterBackend::Vm => VM_HASH_SEED,
        BroadcasterBackend::Rfq => RFQ_HASH_SEED,
    });
    Ok(BroadcasterBlockRef {
        hash: vec![seed; 32].into(),
        number,
        parent_hash: vec![seed + 1; 32].into(),
        revert: false,
        timestamp: timestamp_ms / 1_000,
        partial_block_index: None,
    })
}

fn current_timestamp_ms() -> Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before UNIX_EPOCH")?;
    Ok(duration.as_millis().try_into().unwrap_or(u64::MAX))
}

#[derive(Debug)]
struct CliArgs {
    database_url: String,
    s3_bucket: String,
    s3_prefix: String,
    s3_region: String,
    s3_endpoint_url: Option<String>,
    s3_force_path_style: bool,
}

impl CliArgs {
    fn parse() -> Result<Self> {
        let mut args = Self {
            database_url: env_or_default("STATE_HISTORY_DATABASE_URL", DEFAULT_DATABASE_URL),
            s3_bucket: env_or_default("STATE_HISTORY_S3_BUCKET", DEFAULT_S3_BUCKET),
            s3_prefix: env_or_default("STATE_HISTORY_S3_PREFIX", DEFAULT_S3_PREFIX),
            s3_region: env_or_default("STATE_HISTORY_S3_REGION", DEFAULT_S3_REGION),
            s3_endpoint_url: Some(env_or_default(
                "STATE_HISTORY_S3_ENDPOINT_URL",
                DEFAULT_S3_ENDPOINT_URL,
            )),
            s3_force_path_style: env::var("STATE_HISTORY_S3_FORCE_PATH_STYLE")
                .ok()
                .map(|value| parse_bool(&value))
                .transpose()?
                .unwrap_or(DEFAULT_S3_FORCE_PATH_STYLE),
        };

        let mut cli = env::args().skip(1);
        while let Some(arg) = cli.next() {
            match arg.as_str() {
                "--database-url" => args.database_url = next_arg(&mut cli, "--database-url")?,
                "--s3-bucket" => args.s3_bucket = next_arg(&mut cli, "--s3-bucket")?,
                "--s3-prefix" => args.s3_prefix = next_arg(&mut cli, "--s3-prefix")?,
                "--s3-region" => args.s3_region = next_arg(&mut cli, "--s3-region")?,
                "--s3-endpoint-url" => {
                    args.s3_endpoint_url = Some(next_arg(&mut cli, "--s3-endpoint-url")?);
                }
                "--no-s3-endpoint-url" => args.s3_endpoint_url = None,
                "--s3-force-path-style" => args.s3_force_path_style = true,
                "--no-s3-force-path-style" => args.s3_force_path_style = false,
                "-h" | "--help" => {
                    print_help();
                    std::process::exit(0);
                }
                _ => return Err(anyhow!("unknown option {arg}")),
            }
        }
        Ok(args)
    }
}

fn env_or_default(key: &str, default: &str) -> String {
    env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(anyhow!("invalid boolean value {value}")),
    }
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    args.next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("{flag} requires a value"))
}

fn print_help() {
    println!(
        "Usage: state-history-analysis [--database-url <url>] [--s3-bucket <bucket>] [--s3-prefix <prefix>] [--s3-region <region>] [--s3-endpoint-url <url>] [--s3-force-path-style]"
    );
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[expect(
    clippy::struct_excessive_bools,
    reason = "JSON report exposes independent harness pass flags"
)]
struct StateHistoryAnalysisReport {
    status: &'static str,
    chain_id: u64,
    stream_id: String,
    inserted_deltas: usize,
    replayed_message_sequences: Vec<u64>,
    checkpoint_manifest_id: i64,
    checkpoint_s3_key: String,
    checkpoint_payload_hash: String,
    decoded_checkpoint_payloads: usize,
    recorded_gaps: usize,
    unproven_ingestion_gaps: usize,
    valid_generation_switch_gaps: usize,
    post_handoff_checkpoint_generation_switch_gaps: usize,
    unseen_generation_gap_switch_gaps: usize,
    generation_switch_gaps: usize,
    backtest_chain_id: u64,
    backtest_stream_id: String,
    backtest_inserted_deltas: usize,
    backtest_replayed_message_sequences: Vec<u64>,
    backtest_checkpoint_manifest_id: i64,
    backtest_checkpoint_s3_key: String,
    backtest_checkpoint_payload_hash: String,
    backtest_decoded_checkpoint_payloads: usize,
    backtest_start_block_timestamp_ms: Option<u64>,
    backtest_end_block_timestamp_ms: Option<u64>,
    rfq_end_timestamp_ms: u64,
    reorg_superseded: bool,
    stale_write_kept: bool,
    duplicate_write_left_updated_at_untouched: bool,
    cross_stream_superseded: bool,
    older_generation_write_kept: bool,
    source_advanced_without_churn: bool,
    snapshot_seeded_start_boundary: bool,
    head_range_unresolvable: bool,
    conflicted_checkpoint_rejected: bool,
    backtest_recorded_gaps: usize,
    backtest_generation_switch_gaps: usize,
    backtest_unproven_ingestion_gaps: usize,
}
