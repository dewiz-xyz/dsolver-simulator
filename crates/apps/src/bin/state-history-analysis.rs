use std::collections::BTreeMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Region, Client as S3Client};
use serde::Serialize;

use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterEnvelope, BroadcasterPayload, BroadcasterProtocolSyncStatus,
    BroadcasterProtocolSyncStatusKind, BroadcasterRedisStreamEntry, BroadcasterSnapshotEnd,
    BroadcasterSnapshotStart, BroadcasterUpdateMessage, BroadcasterUpdatePartition,
};
use state_history::{
    indexed_backends_for_entry, CheckpointArchive, CheckpointArchiveMetadata, CheckpointPayload,
    GasPriceMetadata, GasPriceSource, HistoryRangePlan, HistoryRangeRequest, IngestionGap,
    S3CheckpointStore, StateHistoryCheckpointWriter, StateHistoryPgStore, StateHistoryReader,
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
    let stream_id = format!("state-history-analysis-{run_id}");
    let rfq_cursor_timestamp_ms = 1_700_000_000_000u64.saturating_add(run_id % 1_000_000);
    let (inserted_deltas, stale_stream_id) =
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
    let ineligible_checkpoint = checkpoint_writer
        .write_checkpoint(synthetic_divergent_checkpoint_archive(
            chain_id,
            &stream_id,
            run_id.saturating_add(1),
        )?)
        .await
        .context("failed to write divergent backend checkpoint")?;

    let reader = StateHistoryReader::new(pg_store.clone(), checkpoint_store);
    assert_checkpoint_selection_respects_backend_blocks(
        &reader,
        chain_id,
        ineligible_checkpoint.manifest_id,
    )
    .await?;
    let request = synthetic_history_request(chain_id, rfq_cursor_timestamp_ms)?;
    record_pre_checkpoint_gap(&pg_store, chain_id, &stream_id).await?;
    let plan = reader.resolve_range(request.clone()).await?;
    plan.ensure_gap_free()?;
    let selected_checkpoint = plan
        .checkpoint
        .as_ref()
        .ok_or_else(|| anyhow!("expected a complete checkpoint for the synthetic range"))?;
    let decoded = reader.fetch_checkpoint(selected_checkpoint).await?;

    let replay_assertions = assert_replay_plan(&plan, &stream_id)?;
    anyhow::ensure!(
        decoded.archive.payloads.len() == 2,
        "expected checkpoint archive start/end payloads"
    );
    let checkpoint_manifest_gas_metadata =
        assert_checkpoint_backend_index(&selected_checkpoint.metadata.backend_index)?;
    let decoded_checkpoint_gas_metadata =
        assert_checkpoint_backend_index(&decoded.archive.metadata.backend_index)?;

    record_visible_gap_fixtures(&pg_store, chain_id, &stream_id, &stale_stream_id).await?;
    let gap_plan = reader.resolve_range(request).await?;
    anyhow::ensure!(
        gap_plan.gaps.len() == 1,
        "expected one recorded synthetic gap, got {}",
        gap_plan.gaps.len()
    );

    Ok(StateHistoryAnalysisReport {
        status: "passed",
        chain_id,
        stream_id,
        inserted_deltas,
        replayed_message_sequences: replay_assertions.message_sequences,
        replayed_gas_metadata: replay_assertions.gas_metadata,
        replayed_null_gas_deltas: replay_assertions.null_gas_deltas,
        checkpoint_manifest_id: checkpoint.manifest_id,
        checkpoint_s3_key: checkpoint.s3_key,
        checkpoint_payload_hash: checkpoint.payload.hash,
        decoded_checkpoint_payloads: decoded.archive.payloads.len(),
        checkpoint_manifest_gas_metadata,
        decoded_checkpoint_gas_metadata,
        recorded_gaps: gap_plan.gaps.len(),
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
        let gas_prices = synthetic_delta_gas_prices(entry)?;
        pg_store
            .insert_delta_with_gas_prices(entry, Some(&redis_entry_id), &gas_prices)
            .await
            .with_context(|| format!("failed to insert synthetic delta {}", entry.message_seq))?;
        inserted_deltas = inserted_deltas.saturating_add(1);
    }

    let stale_stream_id = format!("{stream_id}-stale");
    let stale_generation_delta =
        synthetic_delta_entry(chain_id, &stale_stream_id, 2, BroadcasterBackend::Vm, 101)?;
    pg_store
        .insert_delta(&stale_generation_delta, Some("stale-2"))
        .await
        .context("failed to insert stale-generation synthetic delta")?;
    inserted_deltas = inserted_deltas.saturating_add(1);

    Ok((inserted_deltas, stale_stream_id))
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

fn assert_replay_plan(plan: &HistoryRangePlan, stream_id: &str) -> Result<ReplayAssertions> {
    let replayed_message_sequences = plan
        .deltas
        .iter()
        .map(|delta| delta.entry.message_seq)
        .collect::<Vec<_>>();
    anyhow::ensure!(
        replayed_message_sequences == vec![2, 3, 4, 5],
        "expected native, VM, RFQ, and missing-gas deltas after checkpoint, got {replayed_message_sequences:?}"
    );
    anyhow::ensure!(
        plan.deltas
            .iter()
            .all(|delta| delta.entry.stream_id == stream_id),
        "reader replayed a delta from outside the checkpoint stream"
    );
    let replayed_gas_metadata = plan
        .deltas
        .iter()
        .flat_map(|delta| delta.backend_index.iter())
        .filter(|index| index.gas_price.is_some())
        .count();
    anyhow::ensure!(
        replayed_gas_metadata == 2,
        "expected two replayed native/VM gas metadata rows, got {replayed_gas_metadata}"
    );
    let null_gas_deltas = plan
        .deltas
        .iter()
        .filter(|delta| {
            delta.backend_index.iter().any(|index| {
                matches!(
                    index.backend,
                    BroadcasterBackend::Native | BroadcasterBackend::Vm
                ) && index.gas_price.is_none()
            })
        })
        .count();
    anyhow::ensure!(
        null_gas_deltas == 1,
        "expected one replayed native delta with null gas metadata, got {null_gas_deltas}"
    );
    Ok(ReplayAssertions {
        message_sequences: replayed_message_sequences,
        gas_metadata: replayed_gas_metadata,
        null_gas_deltas,
    })
}

fn assert_checkpoint_backend_index(backend_index: &[CheckpointPayload]) -> Result<usize> {
    anyhow::ensure!(
        backend_index.len() == 3,
        "expected checkpoint backend index rows for native, VM, and RFQ, got {}",
        backend_index.len()
    );
    let gas_metadata = backend_index
        .iter()
        .filter(|index| index.gas_price.is_some())
        .count();
    anyhow::ensure!(
        gas_metadata == 2,
        "expected checkpoint native/VM gas metadata, got {gas_metadata}"
    );
    Ok(gas_metadata)
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
    stream_id: &str,
    stale_stream_id: &str,
) -> Result<()> {
    for (stream_id, reason) in [
        (stream_id, "state history analysis synthetic gap"),
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

async fn assert_checkpoint_selection_respects_backend_blocks(
    reader: &StateHistoryReader,
    chain_id: u64,
    ineligible_manifest_id: i64,
) -> Result<()> {
    let request = HistoryRangeRequest::new(chain_id, 150, 160, vec![BroadcasterBackend::Native])?;
    let plan = reader.resolve_range(request).await?;
    let checkpoint = plan
        .checkpoint
        .as_ref()
        .ok_or_else(|| anyhow!("expected native checkpoint fallback"))?;
    anyhow::ensure!(
        checkpoint.id != ineligible_manifest_id,
        "reader selected checkpoint {ineligible_manifest_id} even though native backend is ahead of the request"
    );
    let native_cursor = checkpoint
        .metadata
        .backend_index
        .iter()
        .find(|index| index.backend == BroadcasterBackend::Native)
        .and_then(|index| index.block_number)
        .ok_or_else(|| anyhow!("selected checkpoint is missing native backend cursor"))?;
    anyhow::ensure!(
        native_cursor <= 150,
        "selected checkpoint native cursor {native_cursor} is ahead of the request"
    );
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
        (5, BroadcasterBackend::Native, 107),
    ]
    .into_iter()
    .map(|(message_seq, backend, cursor)| {
        synthetic_delta_entry(chain_id, stream_id, message_seq, backend, cursor)
    })
    .collect()
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
    let backends = vec![
        BroadcasterBackend::Native,
        BroadcasterBackend::Vm,
        BroadcasterBackend::Rfq,
    ];
    let snapshot_id = "state-history-analysis-snapshot";
    Ok(CheckpointArchive {
        metadata: CheckpointArchiveMetadata {
            chain_id,
            block_number: 100,
            captured_at_timestamp_ms,
            rfq_update_timestamp_ms: Some(rfq_update_timestamp_ms),
            stream_id: stream_id.to_string(),
            source_message_seq: 1,
            backends: backends.clone(),
            backend_index: vec![
                CheckpointPayload {
                    backend: BroadcasterBackend::Native,
                    block_number: Some(100),
                    observed_timestamp_ms: None,
                    gas_price: Some(synthetic_gas_price(
                        BroadcasterBackend::Native,
                        100,
                        "1400000000",
                    )),
                },
                CheckpointPayload {
                    backend: BroadcasterBackend::Vm,
                    block_number: Some(100),
                    observed_timestamp_ms: None,
                    gas_price: Some(synthetic_gas_price(
                        BroadcasterBackend::Vm,
                        100,
                        "1450000000",
                    )),
                },
                CheckpointPayload {
                    backend: BroadcasterBackend::Rfq,
                    block_number: None,
                    observed_timestamp_ms: Some(rfq_update_timestamp_ms),
                    gas_price: None,
                },
            ],
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

fn synthetic_divergent_checkpoint_archive(
    chain_id: u64,
    stream_id: &str,
    captured_at_timestamp_ms: u64,
) -> Result<CheckpointArchive> {
    let backends = vec![
        BroadcasterBackend::Native,
        BroadcasterBackend::Vm,
        BroadcasterBackend::Rfq,
    ];
    let snapshot_id = "state-history-analysis-divergent-snapshot";
    Ok(CheckpointArchive {
        metadata: CheckpointArchiveMetadata {
            chain_id,
            block_number: 100,
            captured_at_timestamp_ms,
            rfq_update_timestamp_ms: Some(captured_at_timestamp_ms),
            stream_id: stream_id.to_string(),
            source_message_seq: 1,
            backends: backends.clone(),
            backend_index: vec![
                CheckpointPayload {
                    backend: BroadcasterBackend::Native,
                    block_number: Some(200),
                    observed_timestamp_ms: None,
                    gas_price: Some(synthetic_gas_price(
                        BroadcasterBackend::Native,
                        200,
                        "1550000000",
                    )),
                },
                CheckpointPayload {
                    backend: BroadcasterBackend::Vm,
                    block_number: Some(100),
                    observed_timestamp_ms: None,
                    gas_price: Some(synthetic_gas_price(
                        BroadcasterBackend::Vm,
                        100,
                        "1450000000",
                    )),
                },
                CheckpointPayload {
                    backend: BroadcasterBackend::Rfq,
                    block_number: None,
                    observed_timestamp_ms: Some(captured_at_timestamp_ms),
                    gas_price: None,
                },
            ],
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

fn synthetic_delta_gas_prices(
    entry: &BroadcasterRedisStreamEntry,
) -> Result<Vec<GasPriceMetadata>> {
    let mut gas_prices = Vec::new();
    for index in indexed_backends_for_entry(entry)? {
        let Some(block_number) = index.block_number else {
            continue;
        };
        match (entry.message_seq, index.backend) {
            (5, BroadcasterBackend::Native) | (_, BroadcasterBackend::Rfq) => {}
            (_, BroadcasterBackend::Native) => gas_prices.push(synthetic_gas_price(
                BroadcasterBackend::Native,
                block_number,
                "1500000000",
            )),
            (_, BroadcasterBackend::Vm) => gas_prices.push(synthetic_gas_price(
                BroadcasterBackend::Vm,
                block_number,
                "1600000000",
            )),
        }
    }
    Ok(gas_prices)
}

fn synthetic_gas_price(
    backend: BroadcasterBackend,
    block_number: u64,
    price_wei: &str,
) -> GasPriceMetadata {
    GasPriceMetadata {
        backend,
        block_number,
        price_wei: price_wei.to_string(),
        block_hash: Some(format!("0x{block_number:064x}")),
        block_timestamp_secs: Some(1_720_000_000u64.saturating_add(block_number)),
        source: GasPriceSource::RpcBlock,
    }
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
    replayed_gas_metadata: usize,
    replayed_null_gas_deltas: usize,
    checkpoint_manifest_id: i64,
    checkpoint_s3_key: String,
    checkpoint_payload_hash: String,
    decoded_checkpoint_payloads: usize,
    checkpoint_manifest_gas_metadata: usize,
    decoded_checkpoint_gas_metadata: usize,
    recorded_gaps: usize,
}

struct ReplayAssertions {
    message_sequences: Vec<u64>,
    gas_metadata: usize,
    null_gas_deltas: usize,
}
