use std::collections::BTreeMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Region, Client as S3Client};
use serde::Serialize;
use tokio::time::{sleep, Duration, Instant};

use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterGenerationHandoff,
    BroadcasterPayload, BroadcasterProgress, BroadcasterProtocolSyncStatus,
    BroadcasterProtocolSyncStatusKind, BroadcasterRedisStreamEntry, BroadcasterSnapshotEnd,
    BroadcasterSnapshotStart, BroadcasterUpdateMessage, BroadcasterUpdatePartition,
};
use state_history::{
    CheckpointArchive, CheckpointArchiveMetadata, HistoryRangeGapSource, HistoryRangePlan,
    HistoryRangeRequest, IngestionGap, S3CheckpointStore, StateHistoryCheckpointWriter,
    StateHistoryPgStore, StateHistoryReader, StateHistoryWriter, StateHistoryWriterConfig,
};

const DEFAULT_DATABASE_URL: &str = "postgres://postgres:postgres@127.0.0.1:55432/state_history";
const DEFAULT_S3_BUCKET: &str = "state-history";
const DEFAULT_S3_PREFIX: &str = "state-history/local-analysis";
const DEFAULT_S3_REGION: &str = "us-east-1";
const DEFAULT_S3_ENDPOINT_URL: &str = "http://127.0.0.1:59000";
const DEFAULT_S3_FORCE_PATH_STYLE: bool = true;

#[tokio::main]
async fn main() -> Result<()> {
    let args = CliArgs::parse()?;
    let report = run(args).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

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
    let rfq_cursor_timestamp_ms = 1_700_000_000_000u64.saturating_add(run_id % 1_000_000);
    let (inserted_deltas, handoff_stream_id) =
        insert_synthetic_delta_fixtures(&pg_store, chain_id, &stream_id, rfq_cursor_timestamp_ms)
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

    let reader = StateHistoryReader::new(pg_store.clone(), checkpoint_store);
    let request = synthetic_history_request(chain_id, rfq_cursor_timestamp_ms)?;
    record_pre_checkpoint_gap(&pg_store, chain_id, &stream_id).await?;
    let plan = reader.resolve_range(request.clone()).await?;
    let valid_generation_switch_gaps = assert_no_generation_switch_gap(&plan)?;
    let selected_checkpoint = plan
        .checkpoint
        .as_ref()
        .ok_or_else(|| anyhow!("expected a complete checkpoint for the synthetic range"))?;
    let decoded = reader.fetch_checkpoint(selected_checkpoint).await?;

    let replayed_message_sequences = assert_replay_plan(&plan, &stream_id, &handoff_stream_id)?;
    anyhow::ensure!(
        decoded.archive.payloads.len() == 2,
        "expected checkpoint archive start/end payloads"
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
        valid_generation_switch_gaps,
        post_handoff_checkpoint_generation_switch_gaps,
        generation_switch_gaps,
    })
}

async fn insert_synthetic_delta_fixtures(
    pg_store: &StateHistoryPgStore,
    chain_id: u64,
    stream_id: &str,
    rfq_cursor_timestamp_ms: u64,
) -> Result<(usize, String)> {
    let redis_entries = synthetic_delta_entries(chain_id, stream_id, rfq_cursor_timestamp_ms)?;
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
    let handoff_marker = synthetic_handoff_marker(chain_id, stream_id, "1-4", &handoff_stream_id)?;
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
    .with_rfq_timestamp_range(rfq_cursor_timestamp_ms + 1, rfq_cursor_timestamp_ms + 100)
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

fn generation_switch_gap_count(plan: &HistoryRangePlan) -> usize {
    plan.gaps
        .iter()
        .filter(|gap| gap.source == HistoryRangeGapSource::GenerationSwitch)
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
    rfq_cursor_timestamp_ms: u64,
) -> Result<Vec<BroadcasterRedisStreamEntry>> {
    [
        (1, BroadcasterBackend::Native, 100),
        (2, BroadcasterBackend::Native, 100),
        (3, BroadcasterBackend::Vm, 101),
        (4, BroadcasterBackend::Rfq, rfq_cursor_timestamp_ms + 10),
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
            BroadcasterBackendHead::new(BroadcasterBackend::Rfq, 1_700_000_000_000),
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
            BroadcasterEnvelope::new(
                stream_id,
                2,
                BroadcasterPayload::SnapshotEnd(BroadcasterSnapshotEnd::new(snapshot_id)),
            ),
        ],
    })
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
    valid_generation_switch_gaps: usize,
    post_handoff_checkpoint_generation_switch_gaps: usize,
    generation_switch_gaps: usize,
}
