use std::collections::{btree_map, BTreeMap, BTreeSet};
use std::fmt;
use std::io::Cursor;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use anyhow::{anyhow, Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Region, primitives::ByteStream, Client as S3Client};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterMessageKind,
    BroadcasterPayload, BroadcasterProtocolMessage, BroadcasterProtocolSyncStatus,
    BroadcasterRedisStreamEntry,
};
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, Row};
use tokio::sync::{mpsc, Mutex, Notify, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration, Instant};
use tracing::{debug, warn};

const CHECKPOINT_ARCHIVE_SCHEMA_VERSION: u32 = 1;
const ZSTD_LEVEL: i32 = 3;
const DEFAULT_WRITER_QUEUE_CAPACITY: usize = 8_192;
const DEFAULT_WRITER_RETRY_WINDOW: Duration = Duration::from_secs(30);
const DEFAULT_GAP_RECORD_TASK_LIMIT: usize = 16;
const PERSISTABLE_PREV_CACHE_LIMIT: usize = 1_024;
const WRITER_RETRY_BACKOFF_BASE: Duration = Duration::from_millis(100);
const WRITER_RETRY_BACKOFF_CAP: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointArchive {
    pub metadata: CheckpointArchiveMetadata,
    pub payloads: Vec<BroadcasterEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointArchiveMetadata {
    pub chain_id: u64,
    pub block_number: u64,
    pub captured_at_timestamp_ms: u64,
    pub rfq_update_timestamp_ms: Option<u64>,
    pub stream_id: String,
    pub source_message_seq: u64,
    pub backends: Vec<BroadcasterBackend>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedCheckpointArchive {
    pub bytes: Vec<u8>,
    pub payload: EncodedPayload,
}

#[derive(Debug, Clone)]
pub struct DecodedCheckpointArchive {
    pub archive: CheckpointArchive,
    pub payload: EncodedPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedPayload {
    pub encoding: PayloadEncoding,
    pub hash: String,
    pub uncompressed_bytes: usize,
    pub compressed_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadEncoding {
    JsonZstd,
}

impl PayloadEncoding {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::JsonZstd => "json+zstd",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointPayload {
    pub backend: BroadcasterBackend,
    pub block_number: Option<u64>,
    pub observed_timestamp_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedDeltaMessage {
    pub chain_id: u64,
    pub stream_id: String,
    pub snapshot_id: Option<String>,
    pub message_seq: u64,
    pub prev_persistable_message_seq: Option<u64>,
    pub redis_entry_id: Option<String>,
    pub kind: BroadcasterMessageKind,
    pub backend_scope: Vec<BroadcasterBackend>,
    pub block_number: Option<u64>,
    pub observed_timestamp_ms: Option<u64>,
    pub payload: EncodedPayload,
    pub payload_compressed: Vec<u8>,
    pub backend_index: Vec<CheckpointPayload>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedDelta {
    pub id: i64,
    pub inserted: bool,
    pub skipped_block_timestamp_records: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestionGap {
    pub chain_id: u64,
    pub stream_id: String,
    pub from_message_seq: u64,
    pub to_message_seq: u64,
    pub backend_scope: Vec<BroadcasterBackend>,
    pub from_block_number: Option<u64>,
    pub to_block_number: Option<u64>,
    pub from_timestamp_ms: Option<u64>,
    pub to_timestamp_ms: Option<u64>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointManifestInput {
    pub metadata: CheckpointArchiveMetadata,
    pub s3_bucket: String,
    pub s3_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointCompletion {
    pub payload_hash: String,
    pub payload_bytes: usize,
    pub compressed_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointManifest {
    pub id: i64,
    pub metadata: CheckpointArchiveMetadata,
    pub s3_bucket: String,
    pub s3_key: String,
    pub payload_hash: Option<String>,
    pub payload_bytes: Option<i64>,
    pub compressed_bytes: Option<i64>,
    pub status: CheckpointStatus,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredDeltaEntry {
    pub id: i64,
    pub redis_entry_id: Option<String>,
    pub payload: EncodedPayload,
    pub entry: BroadcasterRedisStreamEntry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredGenerationHandoff {
    previous_stream_id: String,
    previous_entry_id: String,
    next_stream_id: String,
    next_entry_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct StoredGenerationHandoffs {
    by_previous_stream: BTreeMap<String, StoredGenerationHandoff>,
    by_next_stream: BTreeMap<String, StoredGenerationHandoff>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreparedGenerationHandoff {
    handoff: StoredGenerationHandoff,
    snapshot_id: String,
    backend_scope: Vec<BroadcasterBackend>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HistoryReplaySegment {
    ordinal: i64,
    stream_id: String,
    from_message_seq: u64,
    to_message_seq: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ValidatedHistorySegments {
    replay_segments: Vec<HistoryReplaySegment>,
    generation_switch_exempt_segments: Vec<HistoryReplaySegment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HistoryStreamCursor {
    stream_id: String,
    last_observed_seq: u64,
    last_persistable_seq: u64,
    last_persisted_seq: u64,
    native_head_block: Option<u64>,
    vm_head_block: Option<u64>,
    rfq_head_timestamp_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRangeRequest {
    pub chain_id: u64,
    pub start_block_number: u64,
    pub end_block_number: u64,
    pub rfq_start_timestamp_ms: Option<u64>,
    pub rfq_end_timestamp_ms: Option<u64>,
    pub backends: Vec<BroadcasterBackend>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRangePlan {
    pub request: HistoryRangeRequest,
    pub checkpoint: Option<CheckpointManifest>,
    pub replay_from_message_seq: Option<u64>,
    pub replay_from_block_number: u64,
    pub rfq_replay_from_timestamp_ms: Option<u64>,
    pub deltas: Vec<StoredDeltaEntry>,
    pub gaps: Vec<HistoryRangeGap>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestRangeRequest {
    pub chain_id: u64,
    pub start_block_number: u64,
    pub end_block_number: u64,
    pub backends: Vec<BroadcasterBackend>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestRangePlan {
    pub request: BacktestRangeRequest,
    pub start_block_timestamp_ms: Option<u64>,
    pub end_block_timestamp_ms: Option<u64>,
    pub history: HistoryRangePlan,
}

// Chain timestamps for start, end, and end + 1, fetched in one query. The next
// block timestamp bounds the RFQ range because RFQ updates observed during the
// end block's head tenure carry cursors past the end block's own timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BacktestBoundaryTimestamps {
    start_block_timestamp_ms: u64,
    end_block_timestamp_ms: u64,
    next_block_timestamp_ms: u64,
}

impl BacktestBoundaryTimestamps {
    fn rfq_end_timestamp_ms(&self) -> u64 {
        // Safe: assembly rejects next <= end, so next is at least 1.
        self.next_block_timestamp_ms - 1
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRangeGap {
    pub source: HistoryRangeGapSource,
    pub backend_scope: Vec<BroadcasterBackend>,
    pub from_block_number: Option<u64>,
    pub to_block_number: Option<u64>,
    pub from_timestamp_ms: Option<u64>,
    pub to_timestamp_ms: Option<u64>,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryRangeGapSource {
    MissingCheckpoint,
    RecordedGap,
    GenerationSwitch,
    UnprovenIngestion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointStatus {
    Writing,
    Complete,
    Failed,
}

impl CheckpointStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Writing => "writing",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "writing" => Ok(Self::Writing),
            "complete" => Ok(Self::Complete),
            "failed" => Ok(Self::Failed),
            _ => Err(anyhow!("unknown checkpoint status {value}")),
        }
    }
}

#[derive(Clone)]
pub struct StateHistoryPgStore {
    pool: PgPool,
}

#[derive(Clone)]
pub struct S3CheckpointStore {
    client: S3Client,
    bucket: String,
}

#[derive(Clone)]
pub struct StateHistoryReader {
    pg_store: StateHistoryPgStore,
    checkpoint_store: S3CheckpointStore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockTimestampMetadata {
    chain_id: u64,
    block_number: u64,
    timestamp_ms: u64,
    block_hash: Vec<u8>,
    parent_hash: Vec<u8>,
    source_stream_id: String,
    source_message_seq: u64,
    source_backend: BroadcasterBackend,
    source_protocol: String,
}

// Full readback of a stored block timestamp row, timestamps at PG precision so
// harness checks can compare created_at and updated_at without ms truncation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockTimestampRecord {
    pub chain_id: u64,
    pub block_number: u64,
    pub timestamp_ms: u64,
    pub block_hash: Vec<u8>,
    pub parent_hash: Vec<u8>,
    pub source_stream_id: String,
    pub source_message_seq: u64,
    pub source_backend: BroadcasterBackend,
    pub source_protocol: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateHistoryWriterConfig {
    pub queue_capacity: usize,
    pub retry_window: Duration,
}

impl Default for StateHistoryWriterConfig {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_WRITER_QUEUE_CAPACITY,
            retry_window: DEFAULT_WRITER_RETRY_WINDOW,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateHistoryWriterSnapshot {
    pub healthy: bool,
    pub queue_capacity: usize,
    pub retry_window_ms: u64,
    pub enqueued_deltas: u64,
    pub persisted_deltas: u64,
    pub recorded_gaps: u64,
    pub dropped_deltas: u64,
    pub failed_deltas: u64,
    pub skipped_block_timestamp_records: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_persisted_stream_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_persisted_redis_entry_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_persisted_message_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateHistoryCheckpointWriterSnapshot {
    pub healthy: bool,
    pub attempted_checkpoints: u64,
    pub completed_checkpoints: u64,
    pub failed_checkpoints: u64,
    pub skipped_block_timestamp_records: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_checkpoint_block_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_checkpoint_s3_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointWriteOutcome {
    pub manifest_id: i64,
    pub s3_key: String,
    pub payload: EncodedPayload,
}

#[derive(Clone)]
pub struct StateHistoryWriter {
    sender: mpsc::Sender<StateHistoryWriteCommand>,
    pg_store: StateHistoryPgStore,
    status: Arc<RwLock<StateHistoryWriterSnapshot>>,
    persistable_by_stream: Arc<Mutex<BTreeMap<(u64, String), PersistableStreamCursor>>>,
    gap_record_permits: Arc<Semaphore>,
    shutdown: Arc<WriterShutdown>,
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl fmt::Debug for StateHistoryWriter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StateHistoryWriter")
            .field("queue_capacity", &self.sender.max_capacity())
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct StateHistoryCheckpointWriter {
    pg_store: StateHistoryPgStore,
    checkpoint_store: S3CheckpointStore,
    s3_prefix: String,
    status: Arc<RwLock<StateHistoryCheckpointWriterSnapshot>>,
}

#[derive(Debug)]
enum StateHistoryWriteCommand {
    Persist {
        entry: Box<BroadcasterRedisStreamEntry>,
        redis_entry_id: String,
        prev_persistable_message_seq: Option<u64>,
        observation: StreamObservation,
    },
    Observe(StreamObservation),
}

impl StateHistoryWriteCommand {
    fn is_persist(&self) -> bool {
        matches!(self, Self::Persist { .. })
    }

    fn stream_id(&self) -> &str {
        match self {
            Self::Persist { entry, .. } => &entry.stream_id,
            Self::Observe(observation) => &observation.stream_id,
        }
    }

    fn message_seq(&self) -> u64 {
        match self {
            Self::Persist { entry, .. } => entry.message_seq,
            Self::Observe(observation) => observation.message_seq,
        }
    }

    fn into_persist_entry(self) -> Option<BroadcasterRedisStreamEntry> {
        match self {
            Self::Persist { entry, .. } => Some(*entry),
            Self::Observe(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamObservation {
    chain_id: u64,
    stream_id: String,
    message_seq: u64,
    last_persistable_seq: u64,
    heads: BackendHeadObservation,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct BackendHeadObservation {
    native_head_block: Option<u64>,
    vm_head_block: Option<u64>,
    rfq_head_timestamp_ms: Option<u64>,
}

#[derive(Debug, Default)]
struct PersistableStreamCursor {
    last_message_seq: u64,
    prev_by_message_seq: BTreeMap<u64, Option<u64>>,
}

struct WriterShutdown {
    cancelled: AtomicBool,
    notify: Notify,
}

impl Default for WriterShutdown {
    fn default() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }
}

impl WriterShutdown {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    async fn cancelled(&self) {
        let notified = self.notify.notified();
        tokio::pin!(notified);
        if self.cancelled.load(Ordering::SeqCst) {
            return;
        }
        notified.await;
    }
}

impl PersistableStreamCursor {
    fn trim_prev_cache(&mut self) {
        if self.prev_by_message_seq.len() <= PERSISTABLE_PREV_CACHE_LIMIT {
            return;
        }
        let retain_from = self
            .last_message_seq
            .saturating_sub(PERSISTABLE_PREV_CACHE_LIMIT as u64);
        self.prev_by_message_seq
            .retain(|message_seq, _| *message_seq >= retain_from);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointArchiveWire {
    schema_version: u32,
    metadata: CheckpointArchiveMetadata,
    payloads: Vec<BroadcasterEnvelope>,
}

impl EncodedCheckpointArchive {
    pub fn decode(&self) -> Result<DecodedCheckpointArchive> {
        decode_checkpoint_archive_bytes(self.bytes.clone(), Some(&self.payload.hash))
    }
}

pub fn encode_checkpoint_archive(archive: &CheckpointArchive) -> Result<EncodedCheckpointArchive> {
    let wire = CheckpointArchiveWire {
        schema_version: CHECKPOINT_ARCHIVE_SCHEMA_VERSION,
        metadata: archive.metadata.clone(),
        payloads: archive.payloads.clone(),
    };
    let json =
        serde_json::to_vec(&wire).context("failed to serialize checkpoint archive as JSON")?;
    let hash = sha256_hex(&json);
    let compressed = zstd::stream::encode_all(Cursor::new(&json), ZSTD_LEVEL)
        .context("failed to compress checkpoint archive")?;
    let compressed_bytes = compressed.len();
    Ok(EncodedCheckpointArchive {
        bytes: compressed,
        payload: EncodedPayload {
            encoding: PayloadEncoding::JsonZstd,
            hash,
            uncompressed_bytes: json.len(),
            compressed_bytes,
        },
    })
}

pub fn decode_checkpoint_archive_bytes(
    bytes: Vec<u8>,
    expected_hash: Option<&str>,
) -> Result<DecodedCheckpointArchive> {
    let uncompressed = zstd::stream::decode_all(Cursor::new(&bytes))
        .context("failed to decompress checkpoint archive")?;
    let hash = sha256_hex(&uncompressed);
    if let Some(expected_hash) = expected_hash {
        anyhow::ensure!(
            hash == expected_hash,
            "checkpoint archive hash mismatch: expected {expected_hash}, decoded {hash}"
        );
    }
    let wire: CheckpointArchiveWire = serde_json::from_slice(&uncompressed)
        .context("failed to deserialize checkpoint archive")?;
    anyhow::ensure!(
        wire.schema_version == CHECKPOINT_ARCHIVE_SCHEMA_VERSION,
        "unsupported checkpoint archive schema version {}",
        wire.schema_version
    );
    Ok(DecodedCheckpointArchive {
        archive: CheckpointArchive {
            metadata: wire.metadata,
            payloads: wire.payloads,
        },
        payload: EncodedPayload {
            encoding: PayloadEncoding::JsonZstd,
            hash,
            uncompressed_bytes: uncompressed.len(),
            compressed_bytes: bytes.len(),
        },
    })
}

pub fn indexed_backends_for_entry(
    entry: &BroadcasterRedisStreamEntry,
) -> Result<Vec<CheckpointPayload>> {
    let backends = parse_backend_scope(&entry.backend_scope)?;
    let envelope = decode_entry_envelope(entry)?;
    match envelope.payload {
        BroadcasterPayload::Update(update) => {
            let partition_backends = update
                .partitions
                .iter()
                .map(|partition| partition.backend)
                .collect::<Vec<_>>();
            anyhow::ensure!(
                partition_backends == backends,
                "state history update partition backends do not match Redis backend_scope"
            );
            Ok(update
                .partitions
                .into_iter()
                .map(|partition| match partition.backend {
                    BroadcasterBackend::Native | BroadcasterBackend::Vm => CheckpointPayload {
                        backend: partition.backend,
                        block_number: Some(partition.block_number),
                        observed_timestamp_ms: None,
                    },
                    BroadcasterBackend::Rfq => CheckpointPayload {
                        backend: partition.backend,
                        block_number: None,
                        observed_timestamp_ms: Some(partition.block_number),
                    },
                })
                .collect())
        }
        BroadcasterPayload::Progress(_) => Ok(backends
            .into_iter()
            .map(|backend| CheckpointPayload {
                backend,
                block_number: None,
                observed_timestamp_ms: None,
            })
            .collect()),
        _ => Err(anyhow!(
            "state history only indexes update entries and progress markers"
        )),
    }
}

pub fn prepare_delta_message(
    entry: &BroadcasterRedisStreamEntry,
    redis_entry_id: Option<&str>,
) -> Result<PreparedDeltaMessage> {
    prepare_delta_message_with_prev(entry, redis_entry_id, None)
}

fn prepare_delta_message_with_prev(
    entry: &BroadcasterRedisStreamEntry,
    redis_entry_id: Option<&str>,
    prev_persistable_message_seq: Option<u64>,
) -> Result<PreparedDeltaMessage> {
    let backend_index = indexed_backends_for_entry(entry)?;
    let json = serde_json::to_vec(entry).context("failed to serialize Redis stream entry")?;
    let hash = sha256_hex(&json);
    let compressed = zstd::stream::encode_all(Cursor::new(&json), ZSTD_LEVEL)
        .context("failed to compress Redis stream entry")?;
    let compressed_bytes = compressed.len();
    Ok(PreparedDeltaMessage {
        chain_id: entry.chain_id,
        stream_id: entry.stream_id.clone(),
        snapshot_id: entry.snapshot_id.clone(),
        message_seq: entry.message_seq,
        prev_persistable_message_seq,
        redis_entry_id: redis_entry_id.map(str::to_string),
        kind: entry.kind,
        backend_scope: backend_index.iter().map(|index| index.backend).collect(),
        block_number: entry.block_number,
        observed_timestamp_ms: entry.observed_timestamp_ms,
        payload: EncodedPayload {
            encoding: PayloadEncoding::JsonZstd,
            hash,
            uncompressed_bytes: json.len(),
            compressed_bytes,
        },
        payload_compressed: compressed,
        backend_index,
    })
}

fn prepare_generation_handoff(
    entry: &BroadcasterRedisStreamEntry,
    redis_entry_id: Option<&str>,
) -> Result<Option<PreparedGenerationHandoff>> {
    if entry.kind != BroadcasterMessageKind::Progress {
        return Ok(None);
    }
    let envelope: BroadcasterEnvelope = serde_json::from_str(&entry.payload_json)
        .context("failed to decode Redis progress payload for state history handoff")?;
    let BroadcasterPayload::Progress(progress) = envelope.payload else {
        return Ok(None);
    };
    let Some(handoff) = progress.handoff else {
        return Ok(None);
    };
    let next_entry_id = redis_entry_id
        .ok_or_else(|| anyhow!("state history handoff marker requires Redis entry id"))?;
    let Some((previous_generation, _previous_message_seq)) =
        valid_redis_stream_entry_pair(&handoff.previous_stream_id, &handoff.previous_entry_id)
    else {
        return Err(anyhow!(
            "state history handoff previous stream and entry id are not aligned"
        ));
    };
    let Some((next_generation, next_message_seq)) =
        valid_redis_stream_entry_pair(&entry.stream_id, next_entry_id)
    else {
        return Err(anyhow!(
            "state history handoff next stream and entry id are not aligned"
        ));
    };
    anyhow::ensure!(
        next_generation == previous_generation.saturating_add(1),
        "state history handoff generation must advance exactly once"
    );
    anyhow::ensure!(
        next_message_seq == entry.message_seq && entry.message_seq == 1,
        "state history handoff marker must be the first entry in the next generation"
    );
    Ok(Some(PreparedGenerationHandoff {
        handoff: StoredGenerationHandoff {
            previous_stream_id: handoff.previous_stream_id,
            previous_entry_id: handoff.previous_entry_id,
            next_stream_id: entry.stream_id.clone(),
            next_entry_id: next_entry_id.to_string(),
        },
        snapshot_id: progress.snapshot_id,
        backend_scope: progress.backends,
    }))
}

impl StreamObservation {
    fn for_entry(entry: &BroadcasterRedisStreamEntry, last_persistable_seq: u64) -> Result<Self> {
        let envelope = decode_entry_envelope(entry)?;
        let heads = match envelope.payload {
            BroadcasterPayload::Update(update) => {
                BackendHeadObservation::from_update_partitions(update.partitions.iter())
            }
            BroadcasterPayload::Heartbeat(heartbeat) => {
                BackendHeadObservation::from_backend_heads(heartbeat.backend_heads.iter())
            }
            BroadcasterPayload::Progress(progress) => progress
                .handoff
                .map(|handoff| {
                    BackendHeadObservation::from_backend_heads(handoff.base_heads.iter())
                })
                .unwrap_or_default(),
            _ => BackendHeadObservation::default(),
        };
        Ok(Self {
            chain_id: entry.chain_id,
            stream_id: entry.stream_id.clone(),
            message_seq: entry.message_seq,
            last_persistable_seq,
            heads,
        })
    }
}

impl BackendHeadObservation {
    fn from_update_partitions<'a>(
        partitions: impl IntoIterator<
            Item = &'a simulator_core::broadcaster::BroadcasterUpdatePartition,
        >,
    ) -> Self {
        let mut observation = Self::default();
        for partition in partitions {
            observation.observe_backend_head(partition.backend, partition.block_number);
        }
        observation
    }

    fn from_backend_heads<'a>(heads: impl IntoIterator<Item = &'a BroadcasterBackendHead>) -> Self {
        let mut observation = Self::default();
        for head in heads {
            observation.observe_backend_head(head.backend, head.block_number);
        }
        observation
    }

    fn observe_backend_head(&mut self, backend: BroadcasterBackend, cursor: u64) {
        match backend {
            BroadcasterBackend::Native => {
                self.native_head_block = max_optional_u64(self.native_head_block, Some(cursor));
            }
            BroadcasterBackend::Vm => {
                self.vm_head_block = max_optional_u64(self.vm_head_block, Some(cursor));
            }
            BroadcasterBackend::Rfq => {
                self.rfq_head_timestamp_ms =
                    max_optional_u64(self.rfq_head_timestamp_ms, Some(cursor));
            }
        }
    }
}

// Shared block-timestamp extraction over live deltas and checkpoint archives. Rows are
// keyed by height so one collection window yields at most one upsert per block, and
// conflicting same-height content poisons that height for the rest of the window.
#[derive(Debug, Default)]
struct BlockTimestampCollector {
    rows: BTreeMap<u64, BlockTimestampMetadata>,
    conflicted: BTreeSet<u64>,
    skipped_records: u64,
}

#[derive(Debug, Default)]
struct CollectedBlockTimestamps {
    // Ascending block_number so multi-row transactions share one lock order.
    rows: Vec<BlockTimestampMetadata>,
    skipped_records: u64,
}

impl BlockTimestampCollector {
    fn record_skip(&mut self) {
        self.skipped_records = self.skipped_records.saturating_add(1);
    }

    fn stage(&mut self, candidate: BlockTimestampMetadata) {
        if self.conflicted.contains(&candidate.block_number) {
            return;
        }
        match self.rows.entry(candidate.block_number) {
            btree_map::Entry::Vacant(slot) => {
                slot.insert(candidate);
            }
            btree_map::Entry::Occupied(slot) => {
                let staged = slot.get();
                if staged.timestamp_ms == candidate.timestamp_ms
                    && staged.block_hash == candidate.block_hash
                    && staged.parent_hash == candidate.parent_hash
                {
                    // Identical content, first-seen candidate keeps provenance.
                    return;
                }
                // The envelope disagrees with itself about this height, likely a mid-reorg
                // capture. Persisting either version would pin arbitrary data, so drop the
                // height and let a later envelope supply it.
                warn!(
                    event = "state_history_block_timestamp_conflict_skipped",
                    chain_id = candidate.chain_id,
                    block_number = candidate.block_number,
                    "Skipping block timestamp height with conflicting records in one collection window"
                );
                slot.remove();
                self.conflicted.insert(candidate.block_number);
                self.record_skip();
            }
        }
    }

    fn finish(self) -> CollectedBlockTimestamps {
        CollectedBlockTimestamps {
            rows: self.rows.into_values().collect(),
            skipped_records: self.skipped_records,
        }
    }
}

fn collect_block_timestamps_from_payload(
    collector: &mut BlockTimestampCollector,
    chain_id: u64,
    source_stream_id: &str,
    source_message_seq: u64,
    payload: &BroadcasterPayload,
) {
    match payload {
        BroadcasterPayload::Update(update) => {
            for partition in &update.partitions {
                collect_block_timestamps_from_partition(
                    collector,
                    chain_id,
                    source_stream_id,
                    source_message_seq,
                    partition.backend,
                    &partition.messages,
                    &partition.sync_statuses,
                );
            }
        }
        BroadcasterPayload::SnapshotChunk(chunk) => {
            for partition in &chunk.partitions {
                collect_block_timestamps_from_partition(
                    collector,
                    chain_id,
                    source_stream_id,
                    source_message_seq,
                    partition.backend,
                    &partition.messages,
                    &partition.sync_statuses,
                );
            }
        }
        _ => {}
    }
}

fn collect_block_timestamps_from_partition(
    collector: &mut BlockTimestampCollector,
    chain_id: u64,
    source_stream_id: &str,
    source_message_seq: u64,
    backend: BroadcasterBackend,
    messages: &[BroadcasterProtocolMessage],
    sync_statuses: &BTreeMap<String, BroadcasterProtocolSyncStatus>,
) {
    // Only chain backends carry block headers. This filter is load-bearing on the
    // snapshot path: source_backend has a CHECK IN ('native','vm') constraint, so an
    // RFQ row would abort the surrounding transaction.
    if !matches!(backend, BroadcasterBackend::Native | BroadcasterBackend::Vm) {
        return;
    }
    for message in messages {
        let header = &message.message.header;
        let Some(timestamp_ms) = block_timestamp_ms_from_seconds(header.timestamp) else {
            warn_block_timestamp_overflow(chain_id, header.number, header.timestamp);
            collector.record_skip();
            continue;
        };
        collector.stage(BlockTimestampMetadata {
            chain_id,
            block_number: header.number,
            timestamp_ms,
            block_hash: header.hash.as_ref().to_vec(),
            parent_hash: header.parent_hash.as_ref().to_vec(),
            source_stream_id: source_stream_id.to_string(),
            source_message_seq,
            source_backend: backend,
            source_protocol: message.protocol.clone(),
        });
    }
    for (protocol, status) in sync_statuses {
        let Some(block) = &status.block else {
            continue;
        };
        let Some(timestamp_ms) = block_timestamp_ms_from_seconds(block.timestamp) else {
            warn_block_timestamp_overflow(chain_id, block.number, block.timestamp);
            collector.record_skip();
            continue;
        };
        collector.stage(BlockTimestampMetadata {
            chain_id,
            block_number: block.number,
            timestamp_ms,
            block_hash: block.hash.as_ref().to_vec(),
            parent_hash: block.parent_hash.as_ref().to_vec(),
            source_stream_id: source_stream_id.to_string(),
            source_message_seq,
            source_backend: backend,
            source_protocol: protocol.clone(),
        });
    }
}

fn warn_block_timestamp_overflow(chain_id: u64, block_number: u64, timestamp_seconds: u64) {
    warn!(
        event = "state_history_block_timestamp_overflow_skipped",
        chain_id,
        block_number,
        timestamp_seconds,
        "Skipping block timestamp that does not fit the millisecond BIGINT range"
    );
}

fn block_timestamps_for_entry(
    entry: &BroadcasterRedisStreamEntry,
    backend_scope: &[BroadcasterBackend],
) -> CollectedBlockTimestamps {
    // RFQ-only entries cannot carry block headers, skip the payload decode entirely.
    if !backend_scope
        .iter()
        .any(|backend| matches!(backend, BroadcasterBackend::Native | BroadcasterBackend::Vm))
    {
        return CollectedBlockTimestamps::default();
    }
    let envelope: BroadcasterEnvelope = match serde_json::from_str(&entry.payload_json) {
        Ok(envelope) => envelope,
        Err(error) => {
            // Timestamp metadata is best effort, a bad payload must never fail the delta write.
            warn!(
                event = "state_history_block_timestamp_payload_undecodable",
                stream_id = %entry.stream_id,
                message_seq = entry.message_seq,
                error = %error,
                "Skipping block timestamp extraction for undecodable delta payload"
            );
            return CollectedBlockTimestamps {
                rows: Vec::new(),
                skipped_records: 1,
            };
        }
    };
    let mut collector = BlockTimestampCollector::default();
    collect_block_timestamps_from_payload(
        &mut collector,
        entry.chain_id,
        &entry.stream_id,
        entry.message_seq,
        &envelope.payload,
    );
    collector.finish()
}

fn block_timestamps_from_checkpoint_archive(
    archive: &CheckpointArchive,
) -> Result<CollectedBlockTimestamps> {
    // One collector across every envelope so the conflict-skip rule is archive-wide,
    // a mid-reorg capture that disagrees across chunks poisons the height, not just
    // one chunk. Provenance uses the metadata replay-boundary cursor instead of the
    // synthetic per-archive seqs so supersession stays ordered against live deltas
    // of the same stream.
    let mut collector = BlockTimestampCollector::default();
    for envelope in &archive.payloads {
        collect_block_timestamps_from_payload(
            &mut collector,
            archive.metadata.chain_id,
            &archive.metadata.stream_id,
            archive.metadata.source_message_seq,
            &envelope.payload,
        );
    }
    let collected = collector.finish();
    // Every complete checkpoint must anchor a block_timestamps row at its boundary
    // height, otherwise backtest ranges starting there are unresolvable. Failing
    // here keeps the checkpoint retryable on the next poll.
    anyhow::ensure!(
        collected
            .rows
            .iter()
            .any(|row| row.block_number == archive.metadata.block_number),
        "checkpoint archive carries no usable block timestamp for boundary block {}",
        archive.metadata.block_number
    );
    Ok(collected)
}

fn block_timestamp_ms_from_seconds(timestamp_seconds: u64) -> Option<u64> {
    let timestamp_ms = timestamp_seconds.checked_mul(1_000)?;
    // The stored column is BIGINT, values that cannot round-trip through i64 are unusable.
    i64::try_from(timestamp_ms).ok()?;
    Some(timestamp_ms)
}

impl HistoryRangeRequest {
    pub fn new(
        chain_id: u64,
        start_block_number: u64,
        end_block_number: u64,
        backends: Vec<BroadcasterBackend>,
    ) -> Result<Self> {
        let request = Self {
            chain_id,
            start_block_number,
            end_block_number,
            rfq_start_timestamp_ms: None,
            rfq_end_timestamp_ms: None,
            backends,
        };
        request.validate_shape()?;
        Ok(request)
    }

    pub fn with_rfq_timestamp_range(
        mut self,
        start_timestamp_ms: u64,
        end_timestamp_ms: u64,
    ) -> Result<Self> {
        self.rfq_start_timestamp_ms = Some(start_timestamp_ms);
        self.rfq_end_timestamp_ms = Some(end_timestamp_ms);
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_shape()?;
        if self.backends.contains(&BroadcasterBackend::Rfq) {
            let start = self
                .rfq_start_timestamp_ms
                .ok_or_else(|| anyhow!("RFQ history ranges require a start timestamp"))?;
            let end = self
                .rfq_end_timestamp_ms
                .ok_or_else(|| anyhow!("RFQ history ranges require an end timestamp"))?;
            anyhow::ensure!(
                start <= end,
                "RFQ history range start timestamp must be <= end timestamp"
            );
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<()> {
        anyhow::ensure!(
            self.start_block_number <= self.end_block_number,
            "history range start block must be <= end block"
        );
        anyhow::ensure!(
            !self.backends.is_empty(),
            "history range backend scope must not be empty"
        );
        let mut sorted = self.backends.clone();
        sorted.sort();
        sorted.dedup();
        anyhow::ensure!(
            sorted == self.backends,
            "history range backend scope must be sorted and unique"
        );
        Ok(())
    }

    fn block_backends(&self) -> Vec<BroadcasterBackend> {
        self.backends
            .iter()
            .copied()
            .filter(|backend| {
                matches!(backend, BroadcasterBackend::Native | BroadcasterBackend::Vm)
            })
            .collect()
    }

    fn includes_rfq(&self) -> bool {
        self.backends.contains(&BroadcasterBackend::Rfq)
    }

    fn replay_from_block_number(&self, checkpoint: Option<&CheckpointManifest>) -> u64 {
        checkpoint
            .map(|checkpoint| checkpoint.metadata.block_number)
            .unwrap_or(self.start_block_number)
    }

    fn rfq_replay_from_timestamp_ms(&self, checkpoint: Option<&CheckpointManifest>) -> Option<u64> {
        if !self.includes_rfq() {
            return None;
        }
        Some(
            checkpoint
                .and_then(|checkpoint| checkpoint.metadata.rfq_update_timestamp_ms)
                .unwrap_or(self.rfq_start_timestamp_ms.unwrap_or_default()),
        )
    }

    fn replay_from_message_seq(&self, checkpoint: Option<&CheckpointManifest>) -> Option<u64> {
        checkpoint.map(|checkpoint| checkpoint.metadata.source_message_seq.saturating_add(1))
    }
}

impl BacktestRangeRequest {
    pub fn new(
        chain_id: u64,
        start_block_number: u64,
        end_block_number: u64,
        backends: Vec<BroadcasterBackend>,
    ) -> Result<Self> {
        let request = Self {
            chain_id,
            start_block_number,
            end_block_number,
            backends,
        };
        request.validate()?;
        Ok(request)
    }

    // Fields are pub, so resolve_backtest_range re-runs this on literally built requests.
    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.end_block_number < u64::MAX,
            "backtest range end block must be below u64::MAX to bound the RFQ range with the next block"
        );
        // HistoryRangeRequest owns the shared shape rules, keep a single validator.
        HistoryRangeRequest::new(
            self.chain_id,
            self.start_block_number,
            self.end_block_number,
            self.backends.clone(),
        )
        .map(drop)
    }

    fn includes_rfq(&self) -> bool {
        self.backends.contains(&BroadcasterBackend::Rfq)
    }

    fn to_history_range_request(
        &self,
        boundary: Option<&BacktestBoundaryTimestamps>,
    ) -> Result<HistoryRangeRequest> {
        let request = HistoryRangeRequest::new(
            self.chain_id,
            self.start_block_number,
            self.end_block_number,
            self.backends.clone(),
        )?;
        if !self.includes_rfq() {
            return Ok(request);
        }

        let boundary = boundary.ok_or_else(|| {
            anyhow!("RFQ backtest ranges require resolved boundary block timestamps")
        })?;
        request.with_rfq_timestamp_range(
            boundary.start_block_timestamp_ms,
            boundary.rfq_end_timestamp_ms(),
        )
    }
}

// Pure so the boundary error taxonomy is unit-testable without Postgres.
fn backtest_boundary_from_rows(
    request: &BacktestRangeRequest,
    rows: &[(u64, u64)],
) -> Result<BacktestBoundaryTimestamps> {
    let next_block_number = request.end_block_number.checked_add(1).ok_or_else(|| {
        anyhow!("backtest range end block must be below u64::MAX to bound the RFQ range with the next block")
    })?;
    let timestamp_for = |block_number: u64| {
        rows.iter()
            .find(|(row_block_number, _)| *row_block_number == block_number)
            .map(|(_, timestamp_ms)| *timestamp_ms)
    };
    let start_block_timestamp_ms = timestamp_for(request.start_block_number).ok_or_else(|| {
        anyhow!(
            "missing state history block timestamp for start block {}",
            request.start_block_number
        )
    })?;
    let end_block_timestamp_ms = timestamp_for(request.end_block_number).ok_or_else(|| {
        anyhow!(
            "missing state history block timestamp for end block {}",
            request.end_block_number
        )
    })?;
    let next_block_timestamp_ms = timestamp_for(next_block_number).ok_or_else(|| {
        anyhow!(
            "missing state history block timestamp for block {next_block_number} needed to bound \
             the RFQ range for end block {}; ranges ending at the recorded head are unresolvable \
             until the next block is stored",
            request.end_block_number
        )
    })?;
    // Also guards the rfq_end_timestamp_ms subtraction against next_ts == 0.
    anyhow::ensure!(
        next_block_timestamp_ms > end_block_timestamp_ms,
        "state history block timestamp for block {next_block_number} ({next_block_timestamp_ms}) \
         must be greater than the end block {} timestamp ({end_block_timestamp_ms})",
        request.end_block_number
    );
    Ok(BacktestBoundaryTimestamps {
        start_block_timestamp_ms,
        end_block_timestamp_ms,
        next_block_timestamp_ms,
    })
}

impl HistoryRangePlan {
    pub fn ensure_gap_free(&self) -> Result<()> {
        if self.gaps.is_empty() {
            return Ok(());
        }
        let reasons = self
            .gaps
            .iter()
            .map(|gap| gap.reason.as_str())
            .collect::<Vec<_>>()
            .join("; ");
        Err(anyhow!(
            "state history range has {} gap(s): {reasons}",
            self.gaps.len()
        ))
    }
}

impl StoredGenerationHandoffs {
    fn new(handoffs: impl IntoIterator<Item = StoredGenerationHandoff>) -> Self {
        let mut stored = Self::default();
        for handoff in handoffs {
            stored
                .by_previous_stream
                .insert(handoff.previous_stream_id.clone(), handoff.clone());
            stored
                .by_next_stream
                .insert(handoff.next_stream_id.clone(), handoff);
        }
        stored
    }
}

fn build_validated_history_segments(
    checkpoint: &CheckpointManifest,
    handoffs: &StoredGenerationHandoffs,
) -> ValidatedHistorySegments {
    let replay_segments = build_validated_replay_segments(checkpoint, handoffs);
    let generation_switch_exempt_segments =
        build_generation_switch_exempt_segments(checkpoint, handoffs);
    ValidatedHistorySegments {
        replay_segments,
        generation_switch_exempt_segments,
    }
}

fn build_validated_replay_segments(
    checkpoint: &CheckpointManifest,
    handoffs: &StoredGenerationHandoffs,
) -> Vec<HistoryReplaySegment> {
    let mut segments = Vec::new();
    let mut current_stream_id = checkpoint.metadata.stream_id.clone();
    let mut from_message_seq = checkpoint.metadata.source_message_seq.saturating_add(1);

    loop {
        if let Some((handoff, previous_message_seq, next_message_seq)) =
            validated_forward_handoff(&current_stream_id, handoffs)
        {
            if replay_cursor_is_beyond_handoff_tail(from_message_seq, previous_message_seq) {
                break;
            }
            if from_message_seq <= previous_message_seq {
                segments.push(HistoryReplaySegment {
                    ordinal: segments.len() as i64,
                    stream_id: current_stream_id,
                    from_message_seq,
                    to_message_seq: Some(previous_message_seq),
                });
            }
            current_stream_id = handoff.next_stream_id.clone();
            from_message_seq = next_message_seq.saturating_add(1);
            continue;
        }

        segments.push(HistoryReplaySegment {
            ordinal: segments.len() as i64,
            stream_id: current_stream_id,
            from_message_seq,
            to_message_seq: None,
        });
        break;
    }

    segments
}

fn build_generation_switch_exempt_segments(
    checkpoint: &CheckpointManifest,
    handoffs: &StoredGenerationHandoffs,
) -> Vec<HistoryReplaySegment> {
    let mut ancestor_segments = Vec::new();
    let mut current_stream_id = checkpoint.metadata.stream_id.clone();
    while let Some((handoff, previous_message_seq, _next_message_seq)) =
        validated_backward_handoff(&current_stream_id, handoffs)
    {
        ancestor_segments.push(HistoryReplaySegment {
            ordinal: 0,
            stream_id: handoff.previous_stream_id.clone(),
            from_message_seq: 1,
            to_message_seq: Some(previous_message_seq),
        });
        current_stream_id = handoff.previous_stream_id.clone();
    }
    ancestor_segments.reverse();

    let mut current_and_descendants = Vec::new();
    let mut current_stream_id = checkpoint.metadata.stream_id.clone();
    let mut from_message_seq = checkpoint.metadata.source_message_seq.saturating_add(1);
    loop {
        if let Some((handoff, previous_message_seq, next_message_seq)) =
            validated_forward_handoff(&current_stream_id, handoffs)
        {
            current_and_descendants.push(HistoryReplaySegment {
                ordinal: 0,
                stream_id: current_stream_id,
                from_message_seq: 1,
                to_message_seq: Some(previous_message_seq),
            });
            if replay_cursor_is_beyond_handoff_tail(from_message_seq, previous_message_seq) {
                break;
            }
            current_stream_id = handoff.next_stream_id.clone();
            from_message_seq = next_message_seq.saturating_add(1);
            continue;
        }

        current_and_descendants.push(HistoryReplaySegment {
            ordinal: 0,
            stream_id: current_stream_id,
            from_message_seq: 1,
            to_message_seq: None,
        });
        break;
    }

    ancestor_segments
        .into_iter()
        .chain(current_and_descendants)
        .enumerate()
        .map(|(ordinal, mut segment)| {
            segment.ordinal = ordinal as i64;
            segment
        })
        .collect()
}

fn replay_cursor_is_beyond_handoff_tail(
    from_message_seq: u64,
    handoff_tail_message_seq: u64,
) -> bool {
    from_message_seq > handoff_tail_message_seq.saturating_add(1)
}

fn validated_forward_handoff<'a>(
    current_stream_id: &str,
    handoffs: &'a StoredGenerationHandoffs,
) -> Option<(&'a StoredGenerationHandoff, u64, u64)> {
    let handoff = handoffs.by_previous_stream.get(current_stream_id)?;
    let (previous_generation, previous_message_seq, next_generation, next_message_seq) =
        validated_handoff_parts(handoff)?;
    (handoff.previous_stream_id == current_stream_id
        && next_generation == previous_generation.saturating_add(1)
        && next_message_seq == 1)
        .then_some((handoff, previous_message_seq, next_message_seq))
}

fn validated_backward_handoff<'a>(
    current_stream_id: &str,
    handoffs: &'a StoredGenerationHandoffs,
) -> Option<(&'a StoredGenerationHandoff, u64, u64)> {
    let handoff = handoffs.by_next_stream.get(current_stream_id)?;
    let (previous_generation, previous_message_seq, next_generation, next_message_seq) =
        validated_handoff_parts(handoff)?;
    (handoff.next_stream_id == current_stream_id
        && next_generation == previous_generation.saturating_add(1)
        && next_message_seq == 1)
        .then_some((handoff, previous_message_seq, next_message_seq))
}

fn validated_handoff_parts(handoff: &StoredGenerationHandoff) -> Option<(u64, u64, u64, u64)> {
    let (previous_generation, previous_message_seq) =
        valid_redis_stream_entry_pair(&handoff.previous_stream_id, &handoff.previous_entry_id)?;
    let (next_generation, next_message_seq) =
        valid_redis_stream_entry_pair(&handoff.next_stream_id, &handoff.next_entry_id)?;
    Some((
        previous_generation,
        previous_message_seq,
        next_generation,
        next_message_seq,
    ))
}

fn segment_ordinals(segments: &[HistoryReplaySegment]) -> Vec<i64> {
    segments.iter().map(|segment| segment.ordinal).collect()
}

fn segment_stream_ids(segments: &[HistoryReplaySegment]) -> Vec<String> {
    segments
        .iter()
        .map(|segment| segment.stream_id.clone())
        .collect()
}

fn segment_from_message_seq(segments: &[HistoryReplaySegment]) -> Result<Vec<i64>> {
    segments
        .iter()
        .map(|segment| u64_to_i64("segment.from_message_seq", segment.from_message_seq))
        .collect()
}

fn segment_to_message_seq(segments: &[HistoryReplaySegment]) -> Result<Vec<i64>> {
    segments
        .iter()
        .map(|segment| {
            segment.to_message_seq.map_or(Ok(i64::MAX), |value| {
                u64_to_i64("segment.to_message_seq", value)
            })
        })
        .collect()
}

fn ingestion_gap_within_segments(
    segments: &[HistoryReplaySegment],
    stream_id: &str,
    from_message_seq: u64,
    to_message_seq: u64,
) -> bool {
    segments.iter().any(|segment| {
        segment.stream_id == stream_id
            && from_message_seq >= segment.from_message_seq
            && segment
                .to_message_seq
                .is_none_or(|tail| to_message_seq <= tail)
    })
}

fn verify_ingestion_coverage_from_cursors(
    request: &HistoryRangeRequest,
    segments: &[HistoryReplaySegment],
    cursors: &[HistoryStreamCursor],
) -> Vec<HistoryRangeGap> {
    if segments.is_empty() {
        return vec![unproven_ingestion_gap(
            request,
            "state history has no replay segment to prove ingestion coverage",
        )];
    }

    let cursors_by_stream = cursors
        .iter()
        .map(|cursor| (cursor.stream_id.as_str(), cursor))
        .collect::<BTreeMap<_, _>>();
    for segment in segments {
        let Some(cursor) = cursors_by_stream.get(segment.stream_id.as_str()).copied() else {
            return vec![unproven_ingestion_gap(
                request,
                format!(
                    "state history cursor is missing for stream {}",
                    segment.stream_id
                ),
            )];
        };
        if cursor.last_persistable_seq != cursor.last_persisted_seq {
            return vec![unproven_ingestion_gap(
                request,
                format!(
                    "state history cursor for stream {} has persistable seq {} but persisted seq {}",
                    cursor.stream_id, cursor.last_persistable_seq, cursor.last_persisted_seq
                ),
            )];
        }
        if let Some(tail) = segment.to_message_seq {
            if cursor.last_observed_seq < tail {
                return vec![unproven_ingestion_gap(
                    request,
                    format!(
                        "state history closed stream {} is only observed through seq {}, below handoff tail {}",
                        cursor.stream_id, cursor.last_observed_seq, tail
                    ),
                )];
            }
        } else if let Some(reason) = cursor.unproven_head_reason(request) {
            return vec![unproven_ingestion_gap(request, reason)];
        }
    }

    Vec::new()
}

impl HistoryStreamCursor {
    fn unproven_head_reason(&self, request: &HistoryRangeRequest) -> Option<String> {
        for backend in &request.backends {
            match backend {
                BroadcasterBackend::Native => {
                    if self
                        .native_head_block
                        .is_none_or(|head| head < request.end_block_number)
                    {
                        return Some(format!(
                            "state history native head for stream {} is not proven through block {}",
                            self.stream_id, request.end_block_number
                        ));
                    }
                }
                BroadcasterBackend::Vm => {
                    if self
                        .vm_head_block
                        .is_none_or(|head| head < request.end_block_number)
                    {
                        return Some(format!(
                            "state history VM head for stream {} is not proven through block {}",
                            self.stream_id, request.end_block_number
                        ));
                    }
                }
                BroadcasterBackend::Rfq => {
                    let Some(rfq_end_timestamp_ms) = request.rfq_end_timestamp_ms else {
                        return Some(format!(
                            "state history RFQ head for stream {} cannot be proven without an RFQ end timestamp",
                            self.stream_id
                        ));
                    };
                    if self
                        .rfq_head_timestamp_ms
                        .is_none_or(|head| head < rfq_end_timestamp_ms)
                    {
                        return Some(format!(
                            "state history RFQ head for stream {} is not proven through timestamp {}",
                            self.stream_id, rfq_end_timestamp_ms
                        ));
                    }
                }
            }
        }
        None
    }
}

fn unproven_ingestion_gap(
    request: &HistoryRangeRequest,
    reason: impl Into<String>,
) -> HistoryRangeGap {
    HistoryRangeGap {
        source: HistoryRangeGapSource::UnprovenIngestion,
        backend_scope: request.backends.clone(),
        from_block_number: Some(request.start_block_number),
        to_block_number: Some(request.end_block_number),
        from_timestamp_ms: request.rfq_start_timestamp_ms,
        to_timestamp_ms: request.rfq_end_timestamp_ms,
        reason: reason.into(),
    }
}

fn valid_redis_stream_entry_pair(stream_id: &str, entry_id: &str) -> Option<(u64, u64)> {
    let stream_generation = redis_stream_generation(stream_id)?;
    let (entry_generation, message_seq) = redis_entry_id_parts(entry_id)?;
    (stream_generation == entry_generation).then_some((stream_generation, message_seq))
}

fn redis_stream_generation(stream_id: &str) -> Option<u64> {
    stream_id.rsplit_once("-stream-")?.1.parse().ok()
}

fn redis_entry_id_parts(entry_id: &str) -> Option<(u64, u64)> {
    let (generation, message_seq) = entry_id.split_once('-')?;
    Some((generation.parse().ok()?, message_seq.parse().ok()?))
}

impl StateHistoryPgStore {
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .context("failed to connect to state history PostgreSQL")?;
        Ok(Self::from_pool(pool))
    }

    pub async fn run_migrations(database_url: &str) -> Result<()> {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(database_url)
            .await
            .context("failed to connect to state history PostgreSQL for migrations")?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .context("failed to run state history migrations")?;
        Ok(())
    }

    pub async fn validate_schema(&self) -> Result<()> {
        for table in [
            "state_history.delta_messages",
            "state_history.delta_backend_index",
            "state_history.generation_handoffs",
            "state_history.block_timestamps",
            "state_history.checkpoints",
            "state_history.ingestion_gaps",
            "state_history.stream_cursors",
        ] {
            let exists: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
                .bind(table)
                .fetch_one(&self.pool)
                .await
                .with_context(|| format!("failed to validate state history table {table}"))?;
            anyhow::ensure!(
                exists.as_deref() == Some(table),
                "state history schema is missing table {table}"
            );
        }
        Ok(())
    }

    async fn backtest_boundary_timestamps(
        &self,
        request: &BacktestRangeRequest,
    ) -> Result<BacktestBoundaryTimestamps> {
        // Defensive even though validate() rejects u64::MAX, request fields are pub.
        let next_block_number = request.end_block_number.checked_add(1).ok_or_else(|| {
            anyhow!("backtest range end block must be below u64::MAX to bound the RFQ range with the next block")
        })?;
        let block_numbers = vec![
            u64_to_i64("start_block_number", request.start_block_number)?,
            u64_to_i64("end_block_number", request.end_block_number)?,
            u64_to_i64("next_block_number", next_block_number)?,
        ];
        let rows = sqlx::query_as::<_, (i64, i64)>(
            r#"
            SELECT block_number, timestamp_ms
            FROM state_history.block_timestamps
            WHERE chain_id = $1 AND block_number = ANY($2::BIGINT[])
            "#,
        )
        .bind(u64_to_i64("chain_id", request.chain_id)?)
        .bind(&block_numbers)
        .fetch_all(&self.pool)
        .await
        .context("failed to resolve state history backtest boundary timestamps")?;
        let rows = rows
            .into_iter()
            .map(|(block_number, timestamp_ms)| {
                Ok((
                    i64_to_u64("block_number", block_number)?,
                    i64_to_u64("timestamp_ms", timestamp_ms)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        backtest_boundary_from_rows(request, &rows)
    }

    pub async fn block_timestamp_record(
        &self,
        chain_id: u64,
        block_number: u64,
    ) -> Result<Option<BlockTimestampRecord>> {
        let row = sqlx::query(
            r#"
            SELECT chain_id, block_number, timestamp_ms, block_hash, parent_hash,
                source_stream_id, source_message_seq, source_backend, source_protocol,
                created_at, updated_at
            FROM state_history.block_timestamps
            WHERE chain_id = $1 AND block_number = $2
            "#,
        )
        .bind(u64_to_i64("chain_id", chain_id)?)
        .bind(u64_to_i64("block_number", block_number)?)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load state history block timestamp record")?;
        row.map(block_timestamp_record_from_row).transpose()
    }

    pub async fn insert_entry(
        &self,
        entry: &BroadcasterRedisStreamEntry,
        redis_entry_id: Option<&str>,
    ) -> Result<PersistedDelta> {
        let prev_persistable_message_seq = self.latest_persisted_message_seq_before(entry).await?;
        self.insert_entry_with_prev(
            entry,
            redis_entry_id,
            prev_persistable_message_seq,
            StreamObservation::for_entry(entry, entry.message_seq)?,
        )
        .await
    }

    async fn latest_persisted_message_seq_before(
        &self,
        entry: &BroadcasterRedisStreamEntry,
    ) -> Result<Option<u64>> {
        let value = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT message_seq
            FROM state_history.delta_messages
            WHERE chain_id = $1
                AND stream_id = $2
                AND message_seq < $3
            ORDER BY message_seq DESC
            LIMIT 1
            "#,
        )
        .bind(u64_to_i64("chain_id", entry.chain_id)?)
        .bind(&entry.stream_id)
        .bind(u64_to_i64("message_seq", entry.message_seq)?)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load previous state history delta")?;
        value
            .map(|value| i64_to_u64("message_seq", value))
            .transpose()
    }

    async fn insert_entry_with_prev(
        &self,
        entry: &BroadcasterRedisStreamEntry,
        redis_entry_id: Option<&str>,
        prev_persistable_message_seq: Option<u64>,
        observation: StreamObservation,
    ) -> Result<PersistedDelta> {
        let handoff = prepare_generation_handoff(entry, redis_entry_id)?;
        anyhow::ensure!(
            matches!(
                entry.kind,
                BroadcasterMessageKind::Update | BroadcasterMessageKind::Progress
            ),
            "state history only persists update entries and progress markers"
        );
        let prepared =
            prepare_delta_message_with_prev(entry, redis_entry_id, prev_persistable_message_seq)?;
        let collected = block_timestamps_for_entry(entry, &prepared.backend_scope);
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin state history delta transaction")?;
        let inserted_id = Self::insert_delta_message_row(&mut tx, &prepared).await?;

        let (id, inserted) = match inserted_id {
            Some(id) => (id, true),
            None => (Self::existing_delta_id(&mut tx, &prepared).await?, false),
        };

        Self::insert_backend_index_rows(&mut tx, id, &prepared).await?;

        if let (true, Some(handoff)) = (inserted, handoff) {
            Self::insert_generation_handoff_row(&mut tx, prepared.chain_id, id, &handoff).await?;
        }
        Self::upsert_stream_cursor(&mut tx, &observation, Some(prepared.message_seq)).await?;
        for timestamp in &collected.rows {
            insert_block_timestamp(&mut tx, timestamp).await?;
        }

        tx.commit()
            .await
            .context("failed to commit state history delta transaction")?;
        Ok(PersistedDelta {
            id,
            inserted,
            skipped_block_timestamp_records: collected.skipped_records,
        })
    }

    async fn insert_delta_message_row(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        prepared: &PreparedDeltaMessage,
    ) -> Result<Option<i64>> {
        sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO state_history.delta_messages (
                chain_id, stream_id, snapshot_id, message_seq, prev_persistable_message_seq,
                redis_entry_id, kind, backend_scope, block_number, observed_timestamp_ms,
                payload_encoding, payload_compressed, payload_hash, runtime_published_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, now())
            ON CONFLICT (chain_id, stream_id, message_seq) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(u64_to_i64("chain_id", prepared.chain_id)?)
        .bind(&prepared.stream_id)
        .bind(&prepared.snapshot_id)
        .bind(u64_to_i64("message_seq", prepared.message_seq)?)
        .bind(optional_u64_to_i64(
            "prev_persistable_message_seq",
            prepared.prev_persistable_message_seq,
        )?)
        .bind(&prepared.redis_entry_id)
        .bind(prepared.kind.as_str())
        .bind(backend_scope_strings(&prepared.backend_scope))
        .bind(optional_u64_to_i64("block_number", prepared.block_number)?)
        .bind(optional_u64_to_i64(
            "observed_timestamp_ms",
            prepared.observed_timestamp_ms,
        )?)
        .bind(prepared.payload.encoding.as_str())
        .bind(&prepared.payload_compressed)
        .bind(&prepared.payload.hash)
        .fetch_optional(&mut **tx)
        .await
        .context("failed to insert state history delta")
    }

    async fn existing_delta_id(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        prepared: &PreparedDeltaMessage,
    ) -> Result<i64> {
        let row = sqlx::query(
            r#"
            SELECT id, payload_hash, prev_persistable_message_seq
            FROM state_history.delta_messages
            WHERE chain_id = $1 AND stream_id = $2 AND message_seq = $3
            "#,
        )
        .bind(u64_to_i64("chain_id", prepared.chain_id)?)
        .bind(&prepared.stream_id)
        .bind(u64_to_i64("message_seq", prepared.message_seq)?)
        .fetch_one(&mut **tx)
        .await
        .context("failed to load existing state history delta")?;

        let id: i64 = row.get("id");
        let payload_hash: String = row.get("payload_hash");
        let prev_persistable_message_seq = optional_i64_to_u64(
            "prev_persistable_message_seq",
            row.get("prev_persistable_message_seq"),
        )?;
        anyhow::ensure!(
            payload_hash == prepared.payload.hash,
            "state history delta idempotency conflict for stream {} message_seq {}",
            prepared.stream_id,
            prepared.message_seq
        );
        anyhow::ensure!(
            prev_persistable_message_seq == prepared.prev_persistable_message_seq,
            "state history persisted chain conflict for stream {} message_seq {}",
            prepared.stream_id,
            prepared.message_seq
        );
        Ok(id)
    }

    async fn insert_backend_index_rows(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        delta_id: i64,
        prepared: &PreparedDeltaMessage,
    ) -> Result<()> {
        for index in &prepared.backend_index {
            sqlx::query(
                r#"
                INSERT INTO state_history.delta_backend_index (
                    delta_id, chain_id, backend, block_number, observed_timestamp_ms, message_seq
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (delta_id, backend) DO NOTHING
                "#,
            )
            .bind(delta_id)
            .bind(u64_to_i64("chain_id", prepared.chain_id)?)
            .bind(index.backend.as_str())
            .bind(optional_u64_to_i64("block_number", index.block_number)?)
            .bind(optional_u64_to_i64(
                "observed_timestamp_ms",
                index.observed_timestamp_ms,
            )?)
            .bind(u64_to_i64("message_seq", prepared.message_seq)?)
            .execute(&mut **tx)
            .await
            .context("failed to insert state history delta backend index")?;
        }
        Ok(())
    }

    async fn insert_generation_handoff_row(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        chain_id: u64,
        delta_id: i64,
        handoff: &PreparedGenerationHandoff,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO state_history.generation_handoffs (
                chain_id, handoff_delta_id, previous_stream_id, previous_entry_id,
                next_stream_id, next_entry_id, snapshot_id, backend_scope
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(u64_to_i64("chain_id", chain_id)?)
        .bind(delta_id)
        .bind(&handoff.handoff.previous_stream_id)
        .bind(&handoff.handoff.previous_entry_id)
        .bind(&handoff.handoff.next_stream_id)
        .bind(&handoff.handoff.next_entry_id)
        .bind(&handoff.snapshot_id)
        .bind(backend_scope_strings(&handoff.backend_scope))
        .execute(&mut **tx)
        .await
        .context("failed to insert state history generation handoff")?;
        Ok(())
    }

    async fn observe_stream(&self, observation: &StreamObservation) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin state history cursor transaction")?;
        Self::upsert_stream_cursor(&mut tx, observation, None).await?;
        tx.commit()
            .await
            .context("failed to commit state history cursor transaction")?;
        Ok(())
    }

    async fn upsert_stream_cursor(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        observation: &StreamObservation,
        persisted_seq: Option<u64>,
    ) -> Result<()> {
        let persisted_seq = persisted_seq.unwrap_or(0);
        sqlx::query(
            r#"
            INSERT INTO state_history.stream_cursors (
                chain_id, stream_id, last_observed_seq, last_persistable_seq,
                last_persisted_seq, native_head_block, vm_head_block,
                rfq_head_timestamp_ms, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
            ON CONFLICT (chain_id, stream_id) DO UPDATE
            SET last_observed_seq = GREATEST(
                    state_history.stream_cursors.last_observed_seq,
                    EXCLUDED.last_observed_seq
                ),
                last_persistable_seq = GREATEST(
                    state_history.stream_cursors.last_persistable_seq,
                    EXCLUDED.last_persistable_seq
                ),
                last_persisted_seq = GREATEST(
                    state_history.stream_cursors.last_persisted_seq,
                    EXCLUDED.last_persisted_seq
                ),
                native_head_block = CASE
                    WHEN EXCLUDED.native_head_block IS NULL
                        THEN state_history.stream_cursors.native_head_block
                    WHEN state_history.stream_cursors.native_head_block IS NULL
                        THEN EXCLUDED.native_head_block
                    ELSE GREATEST(
                        state_history.stream_cursors.native_head_block,
                        EXCLUDED.native_head_block
                    )
                END,
                vm_head_block = CASE
                    WHEN EXCLUDED.vm_head_block IS NULL
                        THEN state_history.stream_cursors.vm_head_block
                    WHEN state_history.stream_cursors.vm_head_block IS NULL
                        THEN EXCLUDED.vm_head_block
                    ELSE GREATEST(
                        state_history.stream_cursors.vm_head_block,
                        EXCLUDED.vm_head_block
                    )
                END,
                rfq_head_timestamp_ms = CASE
                    WHEN EXCLUDED.rfq_head_timestamp_ms IS NULL
                        THEN state_history.stream_cursors.rfq_head_timestamp_ms
                    WHEN state_history.stream_cursors.rfq_head_timestamp_ms IS NULL
                        THEN EXCLUDED.rfq_head_timestamp_ms
                    ELSE GREATEST(
                        state_history.stream_cursors.rfq_head_timestamp_ms,
                        EXCLUDED.rfq_head_timestamp_ms
                    )
                END,
                updated_at = now()
            "#,
        )
        .bind(u64_to_i64("chain_id", observation.chain_id)?)
        .bind(&observation.stream_id)
        .bind(u64_to_i64("last_observed_seq", observation.message_seq)?)
        .bind(u64_to_i64(
            "last_persistable_seq",
            observation.last_persistable_seq,
        )?)
        .bind(u64_to_i64("last_persisted_seq", persisted_seq)?)
        .bind(optional_u64_to_i64(
            "native_head_block",
            observation.heads.native_head_block,
        )?)
        .bind(optional_u64_to_i64(
            "vm_head_block",
            observation.heads.vm_head_block,
        )?)
        .bind(optional_u64_to_i64(
            "rfq_head_timestamp_ms",
            observation.heads.rfq_head_timestamp_ms,
        )?)
        .execute(&mut **tx)
        .await
        .context("failed to update state history stream cursor")?;
        Ok(())
    }

    pub async fn record_gap(&self, gap: &IngestionGap) -> Result<i64> {
        anyhow::ensure!(
            gap.from_message_seq <= gap.to_message_seq,
            "gap start message_seq must be <= end message_seq"
        );
        anyhow::ensure!(
            !gap.backend_scope.is_empty(),
            "gap backend scope must not be empty"
        );
        let id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO state_history.ingestion_gaps (
                chain_id, stream_id, from_message_seq, to_message_seq, backend_scope,
                from_block_number, to_block_number, from_timestamp_ms, to_timestamp_ms, reason
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING id
            "#,
        )
        .bind(u64_to_i64("chain_id", gap.chain_id)?)
        .bind(&gap.stream_id)
        .bind(u64_to_i64("from_message_seq", gap.from_message_seq)?)
        .bind(u64_to_i64("to_message_seq", gap.to_message_seq)?)
        .bind(backend_scope_strings(&gap.backend_scope))
        .bind(optional_u64_to_i64(
            "from_block_number",
            gap.from_block_number,
        )?)
        .bind(optional_u64_to_i64("to_block_number", gap.to_block_number)?)
        .bind(optional_u64_to_i64(
            "from_timestamp_ms",
            gap.from_timestamp_ms,
        )?)
        .bind(optional_u64_to_i64("to_timestamp_ms", gap.to_timestamp_ms)?)
        .bind(&gap.reason)
        .fetch_one(&self.pool)
        .await
        .context("failed to insert state history ingestion gap")?;
        Ok(id)
    }

    pub async fn create_checkpoint_manifest(&self, input: &CheckpointManifestInput) -> Result<i64> {
        let id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO state_history.checkpoints (
                chain_id, block_number, captured_at_timestamp_ms, rfq_update_timestamp_ms,
                stream_id, source_message_seq, backend_scope, s3_bucket, s3_key,
                payload_encoding, status
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'writing')
            RETURNING id
            "#,
        )
        .bind(u64_to_i64("chain_id", input.metadata.chain_id)?)
        .bind(u64_to_i64("block_number", input.metadata.block_number)?)
        .bind(u64_to_i64(
            "captured_at_timestamp_ms",
            input.metadata.captured_at_timestamp_ms,
        )?)
        .bind(optional_u64_to_i64(
            "rfq_update_timestamp_ms",
            input.metadata.rfq_update_timestamp_ms,
        )?)
        .bind(&input.metadata.stream_id)
        .bind(u64_to_i64(
            "source_message_seq",
            input.metadata.source_message_seq,
        )?)
        .bind(backend_scope_strings(&input.metadata.backends))
        .bind(&input.s3_bucket)
        .bind(&input.s3_key)
        .bind(PayloadEncoding::JsonZstd.as_str())
        .fetch_one(&self.pool)
        .await
        .context("failed to create state history checkpoint manifest")?;
        Ok(id)
    }

    async fn mark_checkpoint_complete_with_block_timestamps(
        &self,
        checkpoint_id: i64,
        completion: &CheckpointCompletion,
        block_timestamps: &[BlockTimestampMetadata],
    ) -> Result<()> {
        // Completion and timestamp rows share one transaction so snapshot-derived
        // rows become visible iff the checkpoint is complete. A failed upsert rolls
        // the manifest back to 'writing' and the caller marks it failed.
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin state history checkpoint completion transaction")?;
        let updated = sqlx::query(
            r#"
            UPDATE state_history.checkpoints
            SET status = 'complete',
                payload_hash = $2,
                payload_bytes = $3,
                compressed_bytes = $4,
                error = NULL,
                completed_at = now()
            WHERE id = $1
            "#,
        )
        .bind(checkpoint_id)
        .bind(&completion.payload_hash)
        .bind(usize_to_i64("payload_bytes", completion.payload_bytes)?)
        .bind(usize_to_i64(
            "compressed_bytes",
            completion.compressed_bytes,
        )?)
        .execute(&mut *tx)
        .await
        .context("failed to mark state history checkpoint complete")?;
        anyhow::ensure!(
            updated.rows_affected() == 1,
            "checkpoint manifest {checkpoint_id} not found"
        );
        for timestamp in block_timestamps {
            insert_block_timestamp(&mut tx, timestamp).await?;
        }
        tx.commit()
            .await
            .context("failed to commit state history checkpoint completion transaction")
    }

    pub async fn mark_checkpoint_failed(&self, checkpoint_id: i64, error: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE state_history.checkpoints
            SET status = 'failed',
                error = $2,
                completed_at = now()
            WHERE id = $1
            "#,
        )
        .bind(checkpoint_id)
        .bind(error)
        .execute(&self.pool)
        .await
        .context("failed to mark state history checkpoint failed")?;
        Ok(())
    }

    async fn latest_checkpoint_for_request(
        &self,
        request: &HistoryRangeRequest,
    ) -> Result<Option<CheckpointManifest>> {
        let max_rfq_update_timestamp_ms = request
            .includes_rfq()
            .then_some(request.rfq_start_timestamp_ms)
            .flatten();
        let row = sqlx::query(
            r#"
            SELECT id, chain_id, block_number, captured_at_timestamp_ms,
                rfq_update_timestamp_ms, stream_id, source_message_seq, backend_scope,
                s3_bucket, s3_key, payload_hash, payload_bytes, compressed_bytes, status, error
            FROM state_history.checkpoints
            WHERE chain_id = $1
                AND block_number <= $2
                AND status = 'complete'
                AND ($3::text[] = '{}'::text[] OR backend_scope @> $3::text[])
                AND (
                    $4::bigint IS NULL
                    OR (
                        rfq_update_timestamp_ms IS NOT NULL
                        AND rfq_update_timestamp_ms <= $4
                    )
                )
            ORDER BY block_number DESC, rfq_update_timestamp_ms DESC NULLS LAST,
                captured_at_timestamp_ms DESC
            LIMIT 1
            "#,
        )
        .bind(u64_to_i64("chain_id", request.chain_id)?)
        .bind(u64_to_i64("block_number", request.start_block_number)?)
        .bind(backend_scope_strings(&request.backends))
        .bind(optional_u64_to_i64(
            "max_rfq_update_timestamp_ms",
            max_rfq_update_timestamp_ms,
        )?)
        .fetch_optional(&self.pool)
        .await
        .context("failed to resolve latest state history checkpoint")?;

        row.map(checkpoint_manifest_from_row).transpose()
    }

    pub async fn resolve_history_range(
        &self,
        request: HistoryRangeRequest,
    ) -> Result<HistoryRangePlan> {
        request.validate()?;
        let checkpoint = self.latest_checkpoint_for_request(&request).await?;
        let replay_from_message_seq = request.replay_from_message_seq(checkpoint.as_ref());
        let replay_from_block_number = request.replay_from_block_number(checkpoint.as_ref());
        let rfq_replay_from_timestamp_ms =
            request.rfq_replay_from_timestamp_ms(checkpoint.as_ref());
        let history_segments = match checkpoint.as_ref() {
            Some(checkpoint) => Some(self.validated_history_segments(checkpoint).await?),
            None => None,
        };
        let mut gaps = Vec::new();
        let deltas;
        if let Some(checkpoint) = checkpoint.as_ref() {
            let Some(segments) = history_segments.as_ref() else {
                return Err(anyhow!(
                    "checkpointed state history range is missing validated replay segments"
                ));
            };
            gaps.extend(
                self.recorded_gaps_for_range(
                    &request,
                    Some(segments.replay_segments.as_slice()),
                    replay_from_block_number,
                    rfq_replay_from_timestamp_ms,
                )
                .await
                .context("failed to load state history gaps for range")?,
            );
            if let Some(gap) = self
                .generation_switch_gap_for_range(
                    &request,
                    checkpoint,
                    segments.generation_switch_exempt_segments.as_slice(),
                    replay_from_block_number,
                    rfq_replay_from_timestamp_ms,
                )
                .await
                .context("failed to detect state history generation switches")?
            {
                gaps.push(gap);
            }
            gaps.extend(
                self.ingestion_coverage_gaps_for_range(&request, &segments.replay_segments)
                    .await
                    .context("failed to verify state history ingestion coverage")?,
            );
            deltas = self
                .deltas_for_range(
                    &request,
                    Some(segments.replay_segments.as_slice()),
                    replay_from_block_number,
                    rfq_replay_from_timestamp_ms,
                )
                .await
                .context("failed to load state history deltas for range")?;
        } else {
            gaps.push(HistoryRangeGap {
                source: HistoryRangeGapSource::MissingCheckpoint,
                backend_scope: request.backends.clone(),
                from_block_number: Some(request.start_block_number),
                to_block_number: Some(request.end_block_number),
                from_timestamp_ms: request.rfq_start_timestamp_ms,
                to_timestamp_ms: request.rfq_end_timestamp_ms,
                reason: "no complete checkpoint covers the requested range".to_string(),
            });
            deltas = Vec::new();
        }
        Ok(HistoryRangePlan {
            request,
            checkpoint,
            replay_from_message_seq,
            replay_from_block_number,
            rfq_replay_from_timestamp_ms,
            deltas,
            gaps,
        })
    }

    async fn validated_history_segments(
        &self,
        checkpoint: &CheckpointManifest,
    ) -> Result<ValidatedHistorySegments> {
        let handoffs = self
            .generation_handoffs_for_chain(checkpoint.metadata.chain_id)
            .await?;
        Ok(build_validated_history_segments(checkpoint, &handoffs))
    }

    async fn ingestion_coverage_gaps_for_range(
        &self,
        request: &HistoryRangeRequest,
        replay_segments: &[HistoryReplaySegment],
    ) -> Result<Vec<HistoryRangeGap>> {
        let mut gaps = Vec::new();
        if self
            .persisted_chain_gap_exists(request.chain_id, replay_segments)
            .await?
        {
            gaps.push(unproven_ingestion_gap(
                request,
                "state history persisted delta chain is incomplete for the requested range",
            ));
        }
        let cursors = self
            .stream_cursors_for_segments(request.chain_id, replay_segments)
            .await?;
        gaps.extend(verify_ingestion_coverage_from_cursors(
            request,
            replay_segments,
            &cursors,
        ));
        Ok(gaps)
    }

    async fn persisted_chain_gap_exists(
        &self,
        chain_id: u64,
        replay_segments: &[HistoryReplaySegment],
    ) -> Result<bool> {
        if replay_segments.is_empty() {
            return Ok(false);
        }
        let exists = sqlx::query_scalar::<_, i32>(
            r#"
            WITH replay_segments AS (
                SELECT *
                FROM UNNEST($2::bigint[], $3::text[], $4::bigint[], $5::bigint[])
                    AS segment(ordinal, stream_id, from_message_seq, to_message_seq)
            ),
            scoped_deltas AS (
                SELECT
                    segment.ordinal,
                    segment.from_message_seq,
                    d.message_seq,
                    d.prev_persistable_message_seq,
                    LAG(d.message_seq) OVER (
                        PARTITION BY segment.ordinal
                        ORDER BY d.message_seq ASC
                    ) AS previous_persisted_seq
                FROM replay_segments segment
                JOIN state_history.delta_messages d
                    ON d.chain_id = $1
                    AND d.stream_id = segment.stream_id
                    AND d.message_seq >= segment.from_message_seq
                    AND d.message_seq <= segment.to_message_seq
            )
            SELECT 1
            FROM scoped_deltas
            WHERE (
                    previous_persisted_seq IS NULL
                    AND prev_persistable_message_seq IS NOT NULL
                    AND prev_persistable_message_seq >= from_message_seq
                )
                OR (
                    previous_persisted_seq IS NOT NULL
                    AND prev_persistable_message_seq IS DISTINCT FROM previous_persisted_seq
                )
            LIMIT 1
            "#,
        )
        .bind(u64_to_i64("chain_id", chain_id)?)
        .bind(segment_ordinals(replay_segments))
        .bind(segment_stream_ids(replay_segments))
        .bind(segment_from_message_seq(replay_segments)?)
        .bind(segment_to_message_seq(replay_segments)?)
        .fetch_optional(&self.pool)
        .await
        .context("failed to verify state history persisted chain")?;
        Ok(exists.is_some())
    }

    async fn stream_cursors_for_segments(
        &self,
        chain_id: u64,
        replay_segments: &[HistoryReplaySegment],
    ) -> Result<Vec<HistoryStreamCursor>> {
        let stream_ids = segment_stream_ids(replay_segments);
        if stream_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            r#"
            SELECT stream_id, last_observed_seq, last_persistable_seq, last_persisted_seq,
                native_head_block, vm_head_block, rfq_head_timestamp_ms
            FROM state_history.stream_cursors
            WHERE chain_id = $1
                AND stream_id = ANY($2::text[])
            "#,
        )
        .bind(u64_to_i64("chain_id", chain_id)?)
        .bind(stream_ids)
        .fetch_all(&self.pool)
        .await
        .context("failed to load state history stream cursors")?;

        rows.into_iter()
            .map(|row| {
                Ok(HistoryStreamCursor {
                    stream_id: row.get("stream_id"),
                    last_observed_seq: i64_to_u64(
                        "last_observed_seq",
                        row.get("last_observed_seq"),
                    )?,
                    last_persistable_seq: i64_to_u64(
                        "last_persistable_seq",
                        row.get("last_persistable_seq"),
                    )?,
                    last_persisted_seq: i64_to_u64(
                        "last_persisted_seq",
                        row.get("last_persisted_seq"),
                    )?,
                    native_head_block: optional_i64_to_u64(
                        "native_head_block",
                        row.get("native_head_block"),
                    )?,
                    vm_head_block: optional_i64_to_u64("vm_head_block", row.get("vm_head_block"))?,
                    rfq_head_timestamp_ms: optional_i64_to_u64(
                        "rfq_head_timestamp_ms",
                        row.get("rfq_head_timestamp_ms"),
                    )?,
                })
            })
            .collect()
    }

    async fn generation_handoffs_for_chain(
        &self,
        chain_id: u64,
    ) -> Result<StoredGenerationHandoffs> {
        let rows = sqlx::query(
            r#"
            SELECT previous_stream_id, previous_entry_id, next_stream_id, next_entry_id
            FROM state_history.generation_handoffs
            WHERE chain_id = $1
            ORDER BY id ASC
            "#,
        )
        .bind(u64_to_i64("chain_id", chain_id)?)
        .fetch_all(&self.pool)
        .await
        .context("failed to load state history generation handoffs")?;

        let mut handoffs = Vec::new();
        for row in rows {
            let handoff = StoredGenerationHandoff {
                previous_stream_id: row.get("previous_stream_id"),
                previous_entry_id: row.get("previous_entry_id"),
                next_stream_id: row.get("next_stream_id"),
                next_entry_id: row.get("next_entry_id"),
            };
            handoffs.push(handoff);
        }
        Ok(StoredGenerationHandoffs::new(handoffs))
    }

    pub async fn resolve_backtest_range(
        &self,
        request: BacktestRangeRequest,
    ) -> Result<BacktestRangePlan> {
        request.validate()?;
        let boundary = if request.includes_rfq() {
            Some(self.backtest_boundary_timestamps(&request).await?)
        } else {
            None
        };
        let history_request = request.to_history_range_request(boundary.as_ref())?;
        let history = self.resolve_history_range(history_request).await?;
        Ok(BacktestRangePlan {
            request,
            start_block_timestamp_ms: boundary.map(|boundary| boundary.start_block_timestamp_ms),
            end_block_timestamp_ms: boundary.map(|boundary| boundary.end_block_timestamp_ms),
            history,
        })
    }

    async fn deltas_for_range(
        &self,
        request: &HistoryRangeRequest,
        replay_segments: Option<&[HistoryReplaySegment]>,
        replay_from_block_number: u64,
        rfq_replay_from_timestamp_ms: Option<u64>,
    ) -> Result<Vec<StoredDeltaEntry>> {
        let rows = match replay_segments {
            Some(segments) => {
                self.deltas_for_segmented_range(
                    request,
                    segments,
                    replay_from_block_number,
                    rfq_replay_from_timestamp_ms,
                )
                .await?
            }
            None => {
                self.deltas_for_single_stream_range(
                    request,
                    replay_from_block_number,
                    rfq_replay_from_timestamp_ms,
                )
                .await?
            }
        };

        rows.into_iter()
            .map(stored_delta_from_row)
            .collect::<Result<Vec<_>>>()
    }

    async fn deltas_for_segmented_range(
        &self,
        request: &HistoryRangeRequest,
        segments: &[HistoryReplaySegment],
        replay_from_block_number: u64,
        rfq_replay_from_timestamp_ms: Option<u64>,
    ) -> Result<Vec<sqlx::postgres::PgRow>> {
        let block_backends = request.block_backends();
        sqlx::query(
            r#"
            WITH replay_segments AS (
                SELECT *
                FROM UNNEST($8::bigint[], $9::text[], $10::bigint[], $11::bigint[])
                    AS segment(ordinal, stream_id, from_message_seq, to_message_seq)
            ),
            selected_deltas AS (
                SELECT DISTINCT segment.ordinal, idx.delta_id
                FROM replay_segments segment
                JOIN state_history.delta_messages d
                    ON d.stream_id = segment.stream_id
                    AND d.message_seq >= segment.from_message_seq
                    AND d.message_seq <= segment.to_message_seq
                JOIN state_history.delta_backend_index idx ON idx.delta_id = d.id
                WHERE idx.chain_id = $1
                    AND (
                        (
                            idx.backend = ANY($2::text[])
                            AND idx.block_number IS NOT NULL
                            AND idx.block_number >= $3
                            AND idx.block_number <= $4
                        )
                        OR (
                            $5::boolean
                            AND idx.backend = 'rfq'
                            AND idx.observed_timestamp_ms IS NOT NULL
                            AND idx.observed_timestamp_ms >= $6
                            AND idx.observed_timestamp_ms <= $7
                        )
                    )
            )
            SELECT d.id, d.redis_entry_id, d.payload_encoding, d.payload_compressed,
                d.payload_hash
            FROM selected_deltas selected
            JOIN state_history.delta_messages d ON d.id = selected.delta_id
            ORDER BY selected.ordinal ASC, d.message_seq ASC, d.id ASC
            "#,
        )
        .bind(u64_to_i64("chain_id", request.chain_id)?)
        .bind(backend_scope_strings(&block_backends))
        .bind(u64_to_i64(
            "replay_from_block_number",
            replay_from_block_number,
        )?)
        .bind(u64_to_i64("end_block_number", request.end_block_number)?)
        .bind(request.includes_rfq())
        .bind(
            optional_u64_to_i64("rfq_replay_from_timestamp_ms", rfq_replay_from_timestamp_ms)?
                .unwrap_or(0),
        )
        .bind(
            optional_u64_to_i64("rfq_end_timestamp_ms", request.rfq_end_timestamp_ms)?.unwrap_or(0),
        )
        .bind(segment_ordinals(segments))
        .bind(segment_stream_ids(segments))
        .bind(segment_from_message_seq(segments)?)
        .bind(segment_to_message_seq(segments)?)
        .fetch_all(&self.pool)
        .await
        .context("failed to load segmented state history deltas")
    }

    async fn deltas_for_single_stream_range(
        &self,
        request: &HistoryRangeRequest,
        replay_from_block_number: u64,
        rfq_replay_from_timestamp_ms: Option<u64>,
    ) -> Result<Vec<sqlx::postgres::PgRow>> {
        let block_backends = request.block_backends();
        sqlx::query(
            r#"
            WITH selected_deltas AS (
                SELECT DISTINCT idx.delta_id
                FROM state_history.delta_backend_index idx
                JOIN state_history.delta_messages d ON d.id = idx.delta_id
                WHERE idx.chain_id = $1
                    AND (
                        (
                            idx.backend = ANY($2::text[])
                            AND idx.block_number IS NOT NULL
                            AND idx.block_number >= $3
                            AND idx.block_number <= $4
                        )
                        OR (
                            $5::boolean
                            AND idx.backend = 'rfq'
                            AND idx.observed_timestamp_ms IS NOT NULL
                            AND idx.observed_timestamp_ms >= $6
                            AND idx.observed_timestamp_ms <= $7
                        )
                    )
            )
            SELECT d.id, d.redis_entry_id, d.payload_encoding, d.payload_compressed,
                d.payload_hash
            FROM selected_deltas selected
            JOIN state_history.delta_messages d ON d.id = selected.delta_id
            ORDER BY d.message_seq ASC, d.id ASC
            "#,
        )
        .bind(u64_to_i64("chain_id", request.chain_id)?)
        .bind(backend_scope_strings(&block_backends))
        .bind(u64_to_i64(
            "replay_from_block_number",
            replay_from_block_number,
        )?)
        .bind(u64_to_i64("end_block_number", request.end_block_number)?)
        .bind(request.includes_rfq())
        .bind(
            optional_u64_to_i64("rfq_replay_from_timestamp_ms", rfq_replay_from_timestamp_ms)?
                .unwrap_or(0),
        )
        .bind(
            optional_u64_to_i64("rfq_end_timestamp_ms", request.rfq_end_timestamp_ms)?.unwrap_or(0),
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to load state history deltas")
    }

    async fn recorded_gaps_for_range(
        &self,
        request: &HistoryRangeRequest,
        replay_segments: Option<&[HistoryReplaySegment]>,
        replay_from_block_number: u64,
        rfq_replay_from_timestamp_ms: Option<u64>,
    ) -> Result<Vec<HistoryRangeGap>> {
        let rows = match replay_segments {
            Some(segments) => {
                self.recorded_gaps_for_segmented_range(
                    request,
                    segments,
                    replay_from_block_number,
                    rfq_replay_from_timestamp_ms,
                )
                .await?
            }
            None => {
                self.recorded_gaps_for_single_stream_range(
                    request,
                    replay_from_block_number,
                    rfq_replay_from_timestamp_ms,
                )
                .await?
            }
        };

        rows.into_iter()
            .map(Self::recorded_gap_from_row)
            .collect::<Result<Vec<_>>>()
    }

    async fn recorded_gaps_for_segmented_range(
        &self,
        request: &HistoryRangeRequest,
        segments: &[HistoryReplaySegment],
        replay_from_block_number: u64,
        rfq_replay_from_timestamp_ms: Option<u64>,
    ) -> Result<Vec<sqlx::postgres::PgRow>> {
        sqlx::query(
            r#"
            WITH replay_segments AS (
                SELECT *
                FROM UNNEST($8::bigint[], $9::text[], $10::bigint[], $11::bigint[])
                    AS segment(ordinal, stream_id, from_message_seq, to_message_seq)
            )
            SELECT DISTINCT segment.ordinal, gap.backend_scope, gap.from_block_number,
                gap.to_block_number, gap.from_timestamp_ms, gap.to_timestamp_ms, gap.reason
            FROM state_history.ingestion_gaps gap
            JOIN replay_segments segment
                ON gap.stream_id = segment.stream_id
                AND gap.to_message_seq >= segment.from_message_seq
                AND gap.from_message_seq <= segment.to_message_seq
            WHERE gap.chain_id = $1
                AND gap.backend_scope && $2::text[]
                AND (
                    (
                        gap.from_block_number IS NOT NULL
                        AND gap.to_block_number IS NOT NULL
                        AND gap.from_block_number <= $3
                        AND gap.to_block_number >= $4
                    )
                    OR (
                        $5::boolean
                        AND gap.from_timestamp_ms IS NOT NULL
                        AND gap.to_timestamp_ms IS NOT NULL
                        AND gap.from_timestamp_ms <= $6
                        AND gap.to_timestamp_ms >= $7
                    )
                    OR (
                        gap.from_block_number IS NULL
                        AND gap.to_block_number IS NULL
                        AND gap.from_timestamp_ms IS NULL
                        AND gap.to_timestamp_ms IS NULL
                    )
                )
            ORDER BY segment.ordinal, gap.from_block_number NULLS LAST,
                gap.from_timestamp_ms NULLS LAST, gap.reason ASC
            "#,
        )
        .bind(u64_to_i64("chain_id", request.chain_id)?)
        .bind(backend_scope_strings(&request.backends))
        .bind(u64_to_i64("end_block_number", request.end_block_number)?)
        .bind(u64_to_i64(
            "replay_from_block_number",
            replay_from_block_number,
        )?)
        .bind(request.includes_rfq())
        .bind(
            optional_u64_to_i64("rfq_end_timestamp_ms", request.rfq_end_timestamp_ms)?.unwrap_or(0),
        )
        .bind(
            optional_u64_to_i64("rfq_replay_from_timestamp_ms", rfq_replay_from_timestamp_ms)?
                .unwrap_or(0),
        )
        .bind(segment_ordinals(segments))
        .bind(segment_stream_ids(segments))
        .bind(segment_from_message_seq(segments)?)
        .bind(segment_to_message_seq(segments)?)
        .fetch_all(&self.pool)
        .await
        .context("failed to load segmented state history gaps")
    }

    async fn recorded_gaps_for_single_stream_range(
        &self,
        request: &HistoryRangeRequest,
        replay_from_block_number: u64,
        rfq_replay_from_timestamp_ms: Option<u64>,
    ) -> Result<Vec<sqlx::postgres::PgRow>> {
        sqlx::query(
            r#"
            SELECT backend_scope, from_block_number, to_block_number, from_timestamp_ms,
                to_timestamp_ms, reason
            FROM state_history.ingestion_gaps
            WHERE chain_id = $1
                AND backend_scope && $2::text[]
                AND (
                    (
                        from_block_number IS NOT NULL
                        AND to_block_number IS NOT NULL
                        AND from_block_number <= $3
                        AND to_block_number >= $4
                    )
                    OR (
                        $5::boolean
                        AND from_timestamp_ms IS NOT NULL
                        AND to_timestamp_ms IS NOT NULL
                        AND from_timestamp_ms <= $6
                        AND to_timestamp_ms >= $7
                    )
                    OR (
                        from_block_number IS NULL
                        AND to_block_number IS NULL
                        AND from_timestamp_ms IS NULL
                        AND to_timestamp_ms IS NULL
                    )
                )
            ORDER BY from_block_number NULLS LAST, from_timestamp_ms NULLS LAST, reason ASC
            "#,
        )
        .bind(u64_to_i64("chain_id", request.chain_id)?)
        .bind(backend_scope_strings(&request.backends))
        .bind(u64_to_i64("end_block_number", request.end_block_number)?)
        .bind(u64_to_i64(
            "replay_from_block_number",
            replay_from_block_number,
        )?)
        .bind(request.includes_rfq())
        .bind(
            optional_u64_to_i64("rfq_end_timestamp_ms", request.rfq_end_timestamp_ms)?.unwrap_or(0),
        )
        .bind(
            optional_u64_to_i64("rfq_replay_from_timestamp_ms", rfq_replay_from_timestamp_ms)?
                .unwrap_or(0),
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to load state history gaps")
    }

    fn recorded_gap_from_row(row: sqlx::postgres::PgRow) -> Result<HistoryRangeGap> {
        let backends: Vec<String> = row.get("backend_scope");
        Ok(HistoryRangeGap {
            source: HistoryRangeGapSource::RecordedGap,
            backend_scope: parse_backend_strings(&backends)?,
            from_block_number: optional_i64_to_u64(
                "from_block_number",
                row.get("from_block_number"),
            )?,
            to_block_number: optional_i64_to_u64("to_block_number", row.get("to_block_number"))?,
            from_timestamp_ms: optional_i64_to_u64(
                "from_timestamp_ms",
                row.get("from_timestamp_ms"),
            )?,
            to_timestamp_ms: optional_i64_to_u64("to_timestamp_ms", row.get("to_timestamp_ms"))?,
            reason: row.get("reason"),
        })
    }

    async fn generation_switch_gap_for_range(
        &self,
        request: &HistoryRangeRequest,
        checkpoint: &CheckpointManifest,
        switch_exempt_segments: &[HistoryReplaySegment],
        replay_from_block_number: u64,
        rfq_replay_from_timestamp_ms: Option<u64>,
    ) -> Result<Option<HistoryRangeGap>> {
        let block_backends = request.block_backends();
        let segment_ordinals = segment_ordinals(switch_exempt_segments);
        let segment_stream_ids = segment_stream_ids(switch_exempt_segments);
        let segment_from_message_seq = segment_from_message_seq(switch_exempt_segments)?;
        let segment_to_message_seq = segment_to_message_seq(switch_exempt_segments)?;
        let switched_stream_exists = sqlx::query_scalar::<_, i32>(
            r#"
            WITH switch_exempt_segments AS (
                SELECT *
                FROM UNNEST($8::bigint[], $9::text[], $10::bigint[], $11::bigint[])
                    AS segment(ordinal, stream_id, from_message_seq, to_message_seq)
            )
            SELECT 1
            FROM state_history.delta_backend_index idx
            JOIN state_history.delta_messages d ON d.id = idx.delta_id
            WHERE idx.chain_id = $1
                AND NOT EXISTS (
                    SELECT 1
                    FROM switch_exempt_segments segment
                    WHERE d.stream_id = segment.stream_id
                        AND d.message_seq >= segment.from_message_seq
                        AND d.message_seq <= segment.to_message_seq
                )
                AND (
                    (
                        idx.backend = ANY($2::text[])
                        AND idx.block_number IS NOT NULL
                        AND idx.block_number >= $3
                        AND idx.block_number <= $4
                    )
                    OR (
                        $5::boolean
                        AND idx.backend = 'rfq'
                        AND idx.observed_timestamp_ms IS NOT NULL
                        AND idx.observed_timestamp_ms >= $6
                        AND idx.observed_timestamp_ms <= $7
                    )
                    OR (
                        d.kind = 'progress'
                        AND idx.backend = ANY($12::text[])
                    )
                )
            LIMIT 1
            "#,
        )
        .bind(u64_to_i64("chain_id", request.chain_id)?)
        .bind(backend_scope_strings(&block_backends))
        .bind(u64_to_i64(
            "replay_from_block_number",
            replay_from_block_number,
        )?)
        .bind(u64_to_i64("end_block_number", request.end_block_number)?)
        .bind(request.includes_rfq())
        .bind(
            optional_u64_to_i64("rfq_replay_from_timestamp_ms", rfq_replay_from_timestamp_ms)?
                .unwrap_or(0),
        )
        .bind(
            optional_u64_to_i64("rfq_end_timestamp_ms", request.rfq_end_timestamp_ms)?.unwrap_or(0),
        )
        .bind(segment_ordinals)
        .bind(segment_stream_ids)
        .bind(segment_from_message_seq)
        .bind(segment_to_message_seq)
        .bind(backend_scope_strings(&request.backends))
        .fetch_optional(&self.pool)
        .await?;

        let switched = switched_stream_exists.is_some()
            || self
                .unexempt_ingestion_gap_exists(
                    request,
                    switch_exempt_segments,
                    replay_from_block_number,
                    rfq_replay_from_timestamp_ms,
                )
                .await?;

        Ok(switched.then(|| HistoryRangeGap {
            source: HistoryRangeGapSource::GenerationSwitch,
            backend_scope: request.backends.clone(),
            from_block_number: Some(request.start_block_number),
            to_block_number: Some(request.end_block_number),
            from_timestamp_ms: request.rfq_start_timestamp_ms,
            to_timestamp_ms: request.rfq_end_timestamp_ms,
            reason: format!(
                "Redis stream generation changed after checkpoint stream {}; replay needs a new checkpoint",
                checkpoint.metadata.stream_id
            ),
        }))
    }

    async fn unexempt_ingestion_gap_exists(
        &self,
        request: &HistoryRangeRequest,
        switch_exempt_segments: &[HistoryReplaySegment],
        replay_from_block_number: u64,
        rfq_replay_from_timestamp_ms: Option<u64>,
    ) -> Result<bool> {
        let rows = sqlx::query(
            r#"
            SELECT stream_id, from_message_seq, to_message_seq
            FROM state_history.ingestion_gaps
            WHERE chain_id = $1
                AND backend_scope && $2::text[]
                AND (
                    (
                        from_block_number IS NOT NULL
                        AND to_block_number IS NOT NULL
                        AND from_block_number <= $3
                        AND to_block_number >= $4
                    )
                    OR (
                        $5::boolean
                        AND from_timestamp_ms IS NOT NULL
                        AND to_timestamp_ms IS NOT NULL
                        AND from_timestamp_ms <= $6
                        AND to_timestamp_ms >= $7
                    )
                    OR (
                        from_block_number IS NULL
                        AND to_block_number IS NULL
                        AND from_timestamp_ms IS NULL
                        AND to_timestamp_ms IS NULL
                    )
                )
            "#,
        )
        .bind(u64_to_i64("chain_id", request.chain_id)?)
        .bind(backend_scope_strings(&request.backends))
        .bind(u64_to_i64("end_block_number", request.end_block_number)?)
        .bind(u64_to_i64(
            "replay_from_block_number",
            replay_from_block_number,
        )?)
        .bind(request.includes_rfq())
        .bind(
            optional_u64_to_i64("rfq_end_timestamp_ms", request.rfq_end_timestamp_ms)?.unwrap_or(0),
        )
        .bind(
            optional_u64_to_i64("rfq_replay_from_timestamp_ms", rfq_replay_from_timestamp_ms)?
                .unwrap_or(0),
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to load state history gaps for generation switch detection")?;

        for row in rows {
            let stream_id: String = row.get("stream_id");
            let from_message_seq = i64_to_u64("from_message_seq", row.get("from_message_seq"))?;
            let to_message_seq = i64_to_u64("to_message_seq", row.get("to_message_seq"))?;
            if !ingestion_gap_within_segments(
                switch_exempt_segments,
                &stream_id,
                from_message_seq,
                to_message_seq,
            ) {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

impl S3CheckpointStore {
    pub fn new(client: S3Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
        }
    }

    pub async fn from_env_config(
        region: &str,
        bucket: impl Into<String>,
        endpoint_url: Option<&str>,
        force_path_style: bool,
    ) -> Result<Self> {
        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .load()
            .await;
        let mut builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        if let Some(endpoint_url) = endpoint_url {
            builder = builder.endpoint_url(endpoint_url);
        }
        builder = builder.force_path_style(force_path_style);
        Ok(Self::new(S3Client::from_conf(builder.build()), bucket))
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub async fn put_checkpoint_archive(
        &self,
        key: &str,
        archive: &CheckpointArchive,
    ) -> Result<EncodedCheckpointArchive> {
        let encoded = encode_checkpoint_archive(archive)?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(encoded.bytes.clone()))
            .send()
            .await
            .with_context(|| format!("failed to upload state history checkpoint {key}"))?;
        Ok(encoded)
    }

    pub async fn get_checkpoint_archive(
        &self,
        key: &str,
        expected_hash: Option<&str>,
    ) -> Result<DecodedCheckpointArchive> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("failed to fetch state history checkpoint {key}"))?;
        let bytes = output
            .body
            .collect()
            .await
            .with_context(|| format!("failed to read state history checkpoint {key}"))?
            .into_bytes()
            .to_vec();
        decode_checkpoint_archive_bytes(bytes, expected_hash)
    }
}

impl StateHistoryReader {
    pub fn new(pg_store: StateHistoryPgStore, checkpoint_store: S3CheckpointStore) -> Self {
        Self {
            pg_store,
            checkpoint_store,
        }
    }

    pub async fn resolve_range(&self, request: HistoryRangeRequest) -> Result<HistoryRangePlan> {
        self.pg_store.resolve_history_range(request).await
    }

    pub async fn resolve_backtest_range(
        &self,
        request: BacktestRangeRequest,
    ) -> Result<BacktestRangePlan> {
        self.pg_store.resolve_backtest_range(request).await
    }

    pub async fn fetch_checkpoint(
        &self,
        manifest: &CheckpointManifest,
    ) -> Result<DecodedCheckpointArchive> {
        anyhow::ensure!(
            manifest.status == CheckpointStatus::Complete,
            "only complete state history checkpoints can be fetched"
        );
        anyhow::ensure!(
            manifest.s3_bucket == self.checkpoint_store.bucket(),
            "checkpoint bucket {} does not match configured bucket {}",
            manifest.s3_bucket,
            self.checkpoint_store.bucket()
        );
        self.checkpoint_store
            .get_checkpoint_archive(&manifest.s3_key, manifest.payload_hash.as_deref())
            .await
    }
}

impl StateHistoryCheckpointWriter {
    pub fn new(
        pg_store: StateHistoryPgStore,
        checkpoint_store: S3CheckpointStore,
        s3_prefix: impl Into<String>,
    ) -> Self {
        Self {
            pg_store,
            checkpoint_store,
            s3_prefix: s3_prefix.into(),
            status: Arc::new(RwLock::new(StateHistoryCheckpointWriterSnapshot {
                healthy: true,
                ..StateHistoryCheckpointWriterSnapshot::default()
            })),
        }
    }

    pub async fn write_checkpoint(
        &self,
        archive: CheckpointArchive,
    ) -> Result<CheckpointWriteOutcome> {
        anyhow::ensure!(
            !archive.metadata.backends.is_empty(),
            "state history checkpoint backend scope must not be empty"
        );
        anyhow::ensure!(
            !archive.metadata.backends.contains(&BroadcasterBackend::Rfq)
                || archive.metadata.rfq_update_timestamp_ms.is_some(),
            "state history RFQ checkpoints require an RFQ update timestamp"
        );
        let boundary_block = archive.metadata.block_number;
        {
            let mut status = self.status.write().await;
            status.attempted_checkpoints = status.attempted_checkpoints.saturating_add(1);
        }
        // Collect while the payloads are still the in-memory typed vec, and fail the
        // boundary guard before the manifest exists so nothing is orphaned in PG or
        // S3. The checkpoint task retries on the next poll.
        let collected = match block_timestamps_from_checkpoint_archive(&archive) {
            Ok(collected) => collected,
            Err(error) => return Err(self.fail_checkpoint(None, boundary_block, error).await),
        };
        let s3_key = checkpoint_s3_key(
            &self.s3_prefix,
            archive.metadata.chain_id,
            archive.metadata.block_number,
            archive.metadata.captured_at_timestamp_ms,
            &archive.metadata.stream_id,
        );
        let input = CheckpointManifestInput {
            metadata: archive.metadata.clone(),
            s3_bucket: self.checkpoint_store.bucket().to_string(),
            s3_key: s3_key.clone(),
        };
        let manifest_id = match self.pg_store.create_checkpoint_manifest(&input).await {
            Ok(manifest_id) => manifest_id,
            Err(error) => return Err(self.fail_checkpoint(None, boundary_block, error).await),
        };

        let encoded = match self
            .checkpoint_store
            .put_checkpoint_archive(&s3_key, &archive)
            .await
        {
            Ok(encoded) => encoded,
            Err(error) => {
                return Err(self
                    .fail_checkpoint(Some(manifest_id), boundary_block, error)
                    .await)
            }
        };
        let completion = CheckpointCompletion {
            payload_hash: encoded.payload.hash.clone(),
            payload_bytes: encoded.payload.uncompressed_bytes,
            compressed_bytes: encoded.payload.compressed_bytes,
        };
        if let Err(error) = self
            .pg_store
            .mark_checkpoint_complete_with_block_timestamps(
                manifest_id,
                &completion,
                &collected.rows,
            )
            .await
        {
            return Err(self
                .fail_checkpoint(Some(manifest_id), boundary_block, error)
                .await);
        }

        let mut status = self.status.write().await;
        status.healthy = true;
        status.completed_checkpoints = status.completed_checkpoints.saturating_add(1);
        status.skipped_block_timestamp_records = status
            .skipped_block_timestamp_records
            .saturating_add(collected.skipped_records);
        status.last_checkpoint_block_number = Some(boundary_block);
        status.last_checkpoint_s3_key = Some(s3_key.clone());
        status.last_error = None;

        Ok(CheckpointWriteOutcome {
            manifest_id,
            s3_key,
            payload: encoded.payload,
        })
    }

    pub async fn snapshot(&self) -> StateHistoryCheckpointWriterSnapshot {
        self.status.read().await.clone()
    }

    // Shared failure arm for write_checkpoint. Marks the manifest failed when one
    // exists, records the status failure, and hands the error back for propagation.
    async fn fail_checkpoint(
        &self,
        manifest_id: Option<i64>,
        block_number: u64,
        error: anyhow::Error,
    ) -> anyhow::Error {
        let message = format!("{error:#}");
        if let Some(manifest_id) = manifest_id {
            let _ = self
                .pg_store
                .mark_checkpoint_failed(manifest_id, &message)
                .await;
        }
        self.record_checkpoint_failure(Some(block_number), message)
            .await;
        error
    }

    async fn record_checkpoint_failure(&self, block_number: Option<u64>, message: String) {
        let mut status = self.status.write().await;
        status.healthy = false;
        status.failed_checkpoints = status.failed_checkpoints.saturating_add(1);
        status.last_error = Some(match block_number {
            Some(block_number) => format!("checkpoint block {block_number} failed: {message}"),
            None => message,
        });
    }
}

impl StateHistoryWriter {
    pub fn spawn(pg_store: StateHistoryPgStore, config: StateHistoryWriterConfig) -> Result<Self> {
        anyhow::ensure!(
            config.queue_capacity > 0,
            "state history writer queue capacity must be greater than zero"
        );
        anyhow::ensure!(
            !config.retry_window.is_zero(),
            "state history writer retry window must be greater than zero"
        );
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let status = Arc::new(RwLock::new(StateHistoryWriterSnapshot {
            healthy: true,
            queue_capacity: config.queue_capacity,
            retry_window_ms: config.retry_window.as_millis() as u64,
            ..StateHistoryWriterSnapshot::default()
        }));
        let shutdown = Arc::new(WriterShutdown::default());
        let task = tokio::spawn(run_state_history_writer(
            pg_store.clone(),
            receiver,
            status.clone(),
            config.retry_window,
            shutdown.clone(),
        ));
        Ok(Self {
            sender,
            pg_store,
            status,
            persistable_by_stream: Arc::new(Mutex::new(BTreeMap::new())),
            gap_record_permits: Arc::new(Semaphore::new(DEFAULT_GAP_RECORD_TASK_LIMIT)),
            shutdown,
            task: Arc::new(Mutex::new(Some(task))),
        })
    }

    pub async fn enqueue_entry(
        &self,
        entry: BroadcasterRedisStreamEntry,
        redis_entry_id: String,
    ) -> Result<()> {
        let command = match entry.kind {
            BroadcasterMessageKind::Update | BroadcasterMessageKind::Progress => {
                let observation = StreamObservation::for_entry(&entry, entry.message_seq)?;
                let key = (entry.chain_id, entry.stream_id.clone());
                let mut persistable_by_stream = self.persistable_by_stream.lock().await;
                let cursor = persistable_by_stream.entry(key).or_default();
                let prev_persistable_message_seq = if let Some(prev) =
                    cursor.prev_by_message_seq.get(&entry.message_seq)
                {
                    *prev
                } else {
                    anyhow::ensure!(
                            cursor.last_message_seq < entry.message_seq,
                            "state history persistable messages must be enqueued in stream order: stream {} prev {} next {}",
                            entry.stream_id,
                            cursor.last_message_seq,
                            entry.message_seq
                        );
                    let prev = (cursor.last_message_seq > 0).then_some(cursor.last_message_seq);
                    cursor.last_message_seq = entry.message_seq;
                    cursor.prev_by_message_seq.insert(entry.message_seq, prev);
                    cursor.trim_prev_cache();
                    prev
                };
                StateHistoryWriteCommand::Persist {
                    entry: Box::new(entry),
                    redis_entry_id: redis_entry_id.clone(),
                    prev_persistable_message_seq,
                    observation,
                }
            }
            BroadcasterMessageKind::Heartbeat => {
                let key = (entry.chain_id, entry.stream_id.clone());
                let last_persistable_seq = self
                    .persistable_by_stream
                    .lock()
                    .await
                    .get(&key)
                    .map(|cursor| cursor.last_message_seq)
                    .unwrap_or_default();
                StateHistoryWriteCommand::Observe(StreamObservation::for_entry(
                    &entry,
                    last_persistable_seq,
                )?)
            }
            BroadcasterMessageKind::SnapshotStart
            | BroadcasterMessageKind::SnapshotChunk
            | BroadcasterMessageKind::SnapshotEnd => return Ok(()),
        };
        let is_persist = command.is_persist();
        let stream_id = command.stream_id().to_string();
        let message_seq = command.message_seq();
        match self.sender.try_send(command) {
            Ok(()) => {
                if is_persist {
                    let mut status = self.status.write().await;
                    status.enqueued_deltas = status.enqueued_deltas.saturating_add(1);
                }
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(command)) => {
                let mut status = self.status.write().await;
                let message = format!(
                    "state history writer queue full at stream {stream_id} message_seq {message_seq}"
                );
                if command.is_persist() {
                    status.record_enqueue_failure(message);
                } else {
                    status.record_observe_enqueue_failure(message);
                }
                drop(status);
                if let Some(entry) = command.into_persist_entry() {
                    self.record_gap_detached(entry, "state history writer queue full");
                }
                Err(anyhow!("state history writer queue full"))
            }
            Err(mpsc::error::TrySendError::Closed(command)) => {
                let mut status = self.status.write().await;
                let message = format!(
                    "state history writer task stopped at stream {stream_id} message_seq {message_seq}"
                );
                if command.is_persist() {
                    status.record_enqueue_failure(message);
                } else {
                    status.record_observe_enqueue_failure(message);
                }
                drop(status);
                if let Some(entry) = command.into_persist_entry() {
                    self.record_gap_detached(entry, "state history writer task stopped");
                }
                Err(anyhow!("state history writer task stopped"))
            }
        }
    }

    pub async fn snapshot(&self) -> StateHistoryWriterSnapshot {
        self.status.read().await.clone()
    }

    pub async fn shutdown(&self, drain_timeout: Duration) -> Result<()> {
        self.shutdown.cancel();
        let Some(mut task) = self.task.lock().await.take() else {
            return Ok(());
        };
        tokio::select! {
            result = &mut task => match result {
                Ok(()) => Ok(()),
                Err(error) => Err(anyhow!("state history writer task failed: {error}")),
            },
            _ = sleep(drain_timeout) => {
                task.abort();
                Err(anyhow!(
                "state history writer shutdown drain timed out after {} ms",
                drain_timeout.as_millis()
                ))
            }
        }
    }

    fn record_gap_detached(&self, entry: BroadcasterRedisStreamEntry, reason: &'static str) {
        let Ok(permit) = self.gap_record_permits.clone().try_acquire_owned() else {
            warn!(
                event = "state_history_gap_record_dropped",
                stream_id = entry.stream_id.as_str(),
                message_seq = entry.message_seq,
                "State history gap recorder is saturated"
            );
            return;
        };
        let pg_store = self.pg_store.clone();
        let status = self.status.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(error) = record_gap_for_entry(&pg_store, &entry, reason).await {
                let message = format!("{error:#}");
                warn!(
                    event = "state_history_gap_record_failed",
                    error = %message,
                    "Failed to record state history gap"
                );
                let mut status = status.write().await;
                status.last_error = Some(message);
            } else {
                let mut status = status.write().await;
                status.recorded_gaps = status.recorded_gaps.saturating_add(1);
            }
        });
    }
}

impl StateHistoryWriterSnapshot {
    fn record_enqueue_failure(&mut self, message: String) {
        self.healthy = false;
        self.dropped_deltas = self.dropped_deltas.saturating_add(1);
        self.last_error = Some(message);
    }

    fn record_observe_enqueue_failure(&mut self, message: String) {
        self.healthy = false;
        self.last_error = Some(message);
    }
}

async fn run_state_history_writer(
    pg_store: StateHistoryPgStore,
    mut receiver: mpsc::Receiver<StateHistoryWriteCommand>,
    status: Arc<RwLock<StateHistoryWriterSnapshot>>,
    retry_window: Duration,
    shutdown: Arc<WriterShutdown>,
) {
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                receiver.close();
                while let Some(command) = receiver.recv().await {
                    process_write_command_with_retry(&pg_store, command, status.clone(), retry_window).await;
                }
                return;
            }
            command = receiver.recv() => {
                let Some(command) = command else {
                    return;
                };
                process_write_command_with_retry(&pg_store, command, status.clone(), retry_window).await;
            }
        }
    }
}

async fn process_write_command_with_retry(
    pg_store: &StateHistoryPgStore,
    command: StateHistoryWriteCommand,
    status: Arc<RwLock<StateHistoryWriterSnapshot>>,
    retry_window: Duration,
) {
    match command {
        StateHistoryWriteCommand::Persist {
            entry,
            redis_entry_id,
            prev_persistable_message_seq,
            observation,
        } => {
            persist_delta_with_retry(
                pg_store,
                *entry,
                redis_entry_id,
                prev_persistable_message_seq,
                observation,
                status,
                retry_window,
            )
            .await;
        }
        StateHistoryWriteCommand::Observe(observation) => {
            observe_stream_with_retry(pg_store, observation, status, retry_window).await;
        }
    }
}

async fn persist_delta_with_retry(
    pg_store: &StateHistoryPgStore,
    entry: BroadcasterRedisStreamEntry,
    redis_entry_id: String,
    prev_persistable_message_seq: Option<u64>,
    observation: StreamObservation,
    status: Arc<RwLock<StateHistoryWriterSnapshot>>,
    retry_window: Duration,
) {
    let started_at = Instant::now();
    let mut attempts = 0u64;
    loop {
        attempts = attempts.saturating_add(1);
        match pg_store
            .insert_entry_with_prev(
                &entry,
                Some(&redis_entry_id),
                prev_persistable_message_seq,
                observation.clone(),
            )
            .await
        {
            Ok(persisted) => {
                let mut status = status.write().await;
                status.healthy = true;
                status.persisted_deltas = status.persisted_deltas.saturating_add(1);
                status.skipped_block_timestamp_records = status
                    .skipped_block_timestamp_records
                    .saturating_add(persisted.skipped_block_timestamp_records);
                status.last_persisted_stream_id = Some(entry.stream_id.clone());
                status.last_persisted_redis_entry_id = Some(redis_entry_id.clone());
                status.last_persisted_message_seq = Some(entry.message_seq);
                status.last_error = None;
                return;
            }
            Err(error) => {
                let message = format!("{error:#}");
                let elapsed = started_at.elapsed();
                warn!(
                    event = "state_history_delta_write_failed",
                    message_seq = entry.message_seq,
                    attempt = attempts,
                    error = %message,
                    "State history delta write failed"
                );
                if is_permanent_persistence_error(&error) || elapsed >= retry_window {
                    match record_gap_for_entry(pg_store, &entry, &message).await {
                        Ok(_) => {
                            let mut status = status.write().await;
                            status.healthy = false;
                            status.failed_deltas = status.failed_deltas.saturating_add(1);
                            status.recorded_gaps = status.recorded_gaps.saturating_add(1);
                            status.last_error = Some(message);
                        }
                        Err(error) => {
                            let message = format!("{error:#}");
                            warn!(
                                event = "state_history_gap_record_failed",
                                message_seq = entry.message_seq,
                                error = %message,
                                "Failed to record state history gap"
                            );
                            let mut status = status.write().await;
                            status.healthy = false;
                            status.failed_deltas = status.failed_deltas.saturating_add(1);
                            status.last_error = Some(message);
                        }
                    }
                    return;
                }
                sleep(writer_retry_backoff(attempts, retry_window - elapsed)).await;
            }
        }
    }
}

async fn observe_stream_with_retry(
    pg_store: &StateHistoryPgStore,
    observation: StreamObservation,
    status: Arc<RwLock<StateHistoryWriterSnapshot>>,
    retry_window: Duration,
) {
    let started_at = Instant::now();
    let mut attempts = 0u64;
    loop {
        attempts = attempts.saturating_add(1);
        match pg_store.observe_stream(&observation).await {
            Ok(()) => {
                let mut status = status.write().await;
                status.healthy = true;
                status.last_error = None;
                return;
            }
            Err(error) => {
                let message = format!("{error:#}");
                let elapsed = started_at.elapsed();
                warn!(
                    event = "state_history_cursor_observe_failed",
                    stream_id = observation.stream_id.as_str(),
                    message_seq = observation.message_seq,
                    attempt = attempts,
                    error = %message,
                    "State history cursor observe failed"
                );
                if is_permanent_persistence_error(&error) || elapsed >= retry_window {
                    let mut status = status.write().await;
                    status.healthy = false;
                    status.last_error = Some(message);
                    return;
                }
                sleep(writer_retry_backoff(attempts, retry_window - elapsed)).await;
            }
        }
    }
}

fn is_permanent_persistence_error(error: &anyhow::Error) -> bool {
    let mut saw_sqlx_error = false;
    for cause in error.chain() {
        let Some(sqlx_error) = cause.downcast_ref::<sqlx::Error>() else {
            continue;
        };
        saw_sqlx_error = true;
        if let sqlx::Error::Database(database_error) = sqlx_error {
            if matches!(
                database_error.kind(),
                sqlx::error::ErrorKind::UniqueViolation
                    | sqlx::error::ErrorKind::ForeignKeyViolation
                    | sqlx::error::ErrorKind::NotNullViolation
                    | sqlx::error::ErrorKind::CheckViolation
            ) {
                return true;
            }
        }
    }
    !saw_sqlx_error
}

async fn record_gap_for_entry(
    pg_store: &StateHistoryPgStore,
    entry: &BroadcasterRedisStreamEntry,
    reason: &str,
) -> Result<i64> {
    let gap = ingestion_gap_for_entry(entry, reason)?;
    pg_store.record_gap(&gap).await
}

// Pre-image of the stored row as of the statement-start snapshot, used only to
// classify and log the write outcome.
#[derive(Debug, Clone)]
struct StoredBlockTimestamp {
    timestamp_ms: u64,
    block_hash: Vec<u8>,
    parent_hash: Vec<u8>,
    source_stream_id: String,
    source_message_seq: u64,
}

// Diagnostic classification of one block timestamp upsert. The SQL predicate already
// enforced source order, so outcomes never change what was written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockTimestampWriteOutcome {
    // Applied with no pre-image. Under READ COMMITTED a row committed after the
    // statement snapshot can still reach the conflict arm and pass the source check,
    // so this can also be a concurrent overwrite. Diagnostics only, never acted on.
    Inserted,
    // Verbatim redelivery from a stale or equal source, no row version written.
    IdenticalNoop,
    // Stale source carried different content, the stored row wins.
    StaleSourceKept,
    // Newer or different source with identical content, provenance advanced with
    // updated_at preserved.
    SourceAdvanced,
    // Newer or different source with different content, full overwrite. This is the
    // reorg audit trail.
    Superseded,
    // Not applied with no pre-image. The insert race was lost to a concurrent writer
    // whose row then won the source-order check.
    ConcurrentSkip,
}

fn classify_block_timestamp_write(
    incoming: &BlockTimestampMetadata,
    applied: bool,
    stored: Option<&StoredBlockTimestamp>,
) -> BlockTimestampWriteOutcome {
    let Some(stored) = stored else {
        return if applied {
            BlockTimestampWriteOutcome::Inserted
        } else {
            BlockTimestampWriteOutcome::ConcurrentSkip
        };
    };
    let content_identical = stored.timestamp_ms == incoming.timestamp_ms
        && stored.block_hash == incoming.block_hash
        && stored.parent_hash == incoming.parent_hash;
    match (applied, content_identical) {
        (true, true) => BlockTimestampWriteOutcome::SourceAdvanced,
        (true, false) => BlockTimestampWriteOutcome::Superseded,
        (false, true) => BlockTimestampWriteOutcome::IdenticalNoop,
        (false, false) => BlockTimestampWriteOutcome::StaleSourceKept,
    }
}

fn hex_string(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[usize::from(byte >> 4)] as char);
        output.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    output
}

async fn insert_block_timestamp(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    timestamp: &BlockTimestampMetadata,
) -> Result<()> {
    // Single statement so the pre-image read, the source-order check, and the write
    // are atomic under the two in-process writers plus handoff-era peers. The existing
    // CTE reads the statement-start snapshot and executes because the final SELECT
    // references it, which gives the classifier a pre-image for free. updated_at only
    // moves when content changes, so provenance-only advances stay HOT-eligible.
    let row = sqlx::query(
        r#"
        WITH incoming AS (
            SELECT $1::BIGINT AS chain_id, $2::BIGINT AS block_number, $3::BIGINT AS timestamp_ms,
                   $4::BYTEA AS block_hash, $5::BYTEA AS parent_hash,
                   $6::TEXT AS source_stream_id, $7::BIGINT AS source_message_seq,
                   $8::TEXT AS source_backend, $9::TEXT AS source_protocol
        ),
        existing AS (
            SELECT stored.timestamp_ms, stored.block_hash, stored.parent_hash,
                   stored.source_stream_id, stored.source_message_seq
            FROM state_history.block_timestamps stored
            JOIN incoming USING (chain_id, block_number)
        ),
        upsert AS (
            INSERT INTO state_history.block_timestamps AS stored (
                chain_id, block_number, timestamp_ms, block_hash, parent_hash,
                source_stream_id, source_message_seq, source_backend, source_protocol
            )
            SELECT chain_id, block_number, timestamp_ms, block_hash, parent_hash,
                   source_stream_id, source_message_seq, source_backend, source_protocol
            FROM incoming
            ON CONFLICT (chain_id, block_number) DO UPDATE SET
                timestamp_ms = EXCLUDED.timestamp_ms,
                block_hash = EXCLUDED.block_hash,
                parent_hash = EXCLUDED.parent_hash,
                source_stream_id = EXCLUDED.source_stream_id,
                source_message_seq = EXCLUDED.source_message_seq,
                source_backend = EXCLUDED.source_backend,
                source_protocol = EXCLUDED.source_protocol,
                updated_at = CASE
                    WHEN (stored.timestamp_ms, stored.block_hash, stored.parent_hash)
                         IS DISTINCT FROM (EXCLUDED.timestamp_ms, EXCLUDED.block_hash, EXCLUDED.parent_hash)
                    THEN now()
                    ELSE stored.updated_at
                END
            WHERE stored.source_stream_id <> EXCLUDED.source_stream_id
               OR stored.source_message_seq < EXCLUDED.source_message_seq
            RETURNING 1 AS applied
        )
        SELECT EXISTS (SELECT 1 FROM upsert) AS applied,
               existing.timestamp_ms        AS stored_timestamp_ms,
               existing.block_hash          AS stored_block_hash,
               existing.parent_hash         AS stored_parent_hash,
               existing.source_stream_id    AS stored_source_stream_id,
               existing.source_message_seq  AS stored_source_message_seq
        FROM (SELECT 1) AS one
        LEFT JOIN existing ON TRUE
        "#,
    )
    .bind(u64_to_i64("chain_id", timestamp.chain_id)?)
    .bind(u64_to_i64("block_number", timestamp.block_number)?)
    .bind(u64_to_i64("timestamp_ms", timestamp.timestamp_ms)?)
    .bind(&timestamp.block_hash)
    .bind(&timestamp.parent_hash)
    .bind(&timestamp.source_stream_id)
    .bind(u64_to_i64(
        "source_message_seq",
        timestamp.source_message_seq,
    )?)
    .bind(timestamp.source_backend.as_str())
    .bind(&timestamp.source_protocol)
    .fetch_one(&mut **tx)
    .await
    .context("failed to upsert state history block timestamp")?;
    let applied: bool = row.get("applied");
    let stored = stored_block_timestamp_from_row(&row)?;
    let outcome = classify_block_timestamp_write(timestamp, applied, stored.as_ref());
    log_block_timestamp_write_outcome(timestamp, stored.as_ref(), outcome);
    Ok(())
}

fn stored_block_timestamp_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<Option<StoredBlockTimestamp>> {
    row.get::<Option<i64>, _>("stored_timestamp_ms")
        .map(|stored_timestamp_ms| -> Result<StoredBlockTimestamp> {
            Ok(StoredBlockTimestamp {
                timestamp_ms: i64_to_u64("stored_timestamp_ms", stored_timestamp_ms)?,
                block_hash: row.get("stored_block_hash"),
                parent_hash: row.get("stored_parent_hash"),
                source_stream_id: row.get("stored_source_stream_id"),
                source_message_seq: i64_to_u64(
                    "stored_source_message_seq",
                    row.get("stored_source_message_seq"),
                )?,
            })
        })
        .transpose()
}

fn log_block_timestamp_write_outcome(
    timestamp: &BlockTimestampMetadata,
    stored: Option<&StoredBlockTimestamp>,
    outcome: BlockTimestampWriteOutcome,
) {
    match outcome {
        BlockTimestampWriteOutcome::Inserted | BlockTimestampWriteOutcome::IdenticalNoop => {}
        BlockTimestampWriteOutcome::SourceAdvanced => {
            debug!(
                event = "state_history_block_timestamp_source_advanced",
                chain_id = timestamp.chain_id,
                block_number = timestamp.block_number,
                source_stream_id = %timestamp.source_stream_id,
                source_message_seq = timestamp.source_message_seq,
                "Advancing block timestamp provenance for identical content"
            );
        }
        BlockTimestampWriteOutcome::StaleSourceKept => {
            if let Some(stored) = stored {
                warn!(
                    event = "state_history_block_timestamp_stale_source_kept",
                    chain_id = timestamp.chain_id,
                    block_number = timestamp.block_number,
                    stored_source_stream_id = %stored.source_stream_id,
                    stored_source_message_seq = stored.source_message_seq,
                    incoming_source_stream_id = %timestamp.source_stream_id,
                    incoming_source_message_seq = timestamp.source_message_seq,
                    "Keeping stored block timestamp over conflicting content from a stale source"
                );
            }
        }
        BlockTimestampWriteOutcome::Superseded => {
            if let Some(stored) = stored {
                warn!(
                    event = "state_history_block_timestamp_superseded",
                    chain_id = timestamp.chain_id,
                    block_number = timestamp.block_number,
                    old_timestamp_ms = stored.timestamp_ms,
                    old_block_hash = %hex_string(&stored.block_hash),
                    old_source_stream_id = %stored.source_stream_id,
                    old_source_message_seq = stored.source_message_seq,
                    new_timestamp_ms = timestamp.timestamp_ms,
                    new_block_hash = %hex_string(&timestamp.block_hash),
                    new_source_stream_id = %timestamp.source_stream_id,
                    new_source_message_seq = timestamp.source_message_seq,
                    "Superseding stored block timestamp with newer source content"
                );
            }
        }
        BlockTimestampWriteOutcome::ConcurrentSkip => {
            warn!(
                event = "state_history_block_timestamp_concurrent_skip",
                chain_id = timestamp.chain_id,
                block_number = timestamp.block_number,
                source_stream_id = %timestamp.source_stream_id,
                source_message_seq = timestamp.source_message_seq,
                "Skipping block timestamp write that lost an insert race to a newer source"
            );
        }
    }
}

fn ingestion_gap_for_entry(
    entry: &BroadcasterRedisStreamEntry,
    reason: &str,
) -> Result<IngestionGap> {
    let backend_scope = parse_backend_scope(&entry.backend_scope)?;
    Ok(IngestionGap {
        chain_id: entry.chain_id,
        stream_id: entry.stream_id.clone(),
        from_message_seq: entry.message_seq,
        to_message_seq: entry.message_seq,
        backend_scope,
        from_block_number: entry.block_number,
        to_block_number: entry.block_number,
        from_timestamp_ms: entry.observed_timestamp_ms,
        to_timestamp_ms: entry.observed_timestamp_ms,
        reason: reason.to_string(),
    })
}

fn writer_retry_backoff(attempts: u64, remaining: Duration) -> Duration {
    let multiplier = 1u32.checked_shl(attempts.min(8) as u32).unwrap_or(u32::MAX);
    let backoff = WRITER_RETRY_BACKOFF_BASE
        .saturating_mul(multiplier)
        .min(WRITER_RETRY_BACKOFF_CAP);
    backoff.min(remaining)
}

fn parse_backend_scope(scope: &str) -> Result<Vec<BroadcasterBackend>> {
    anyhow::ensure!(!scope.trim().is_empty(), "backend scope must not be empty");
    let mut backends = Vec::new();
    for value in scope.split(',') {
        let backend = match value {
            "native" => BroadcasterBackend::Native,
            "vm" => BroadcasterBackend::Vm,
            "rfq" => BroadcasterBackend::Rfq,
            _ => return Err(anyhow!("unsupported backend scope value {value}")),
        };
        if backends.contains(&backend) {
            return Err(anyhow!("duplicate backend scope value {value}"));
        }
        backends.push(backend);
    }
    let mut sorted = backends.clone();
    sorted.sort();
    anyhow::ensure!(sorted == backends, "backend scope must be sorted");
    Ok(backends)
}

fn backend_scope_strings(backends: &[BroadcasterBackend]) -> Vec<String> {
    backends
        .iter()
        .map(|backend| backend.as_str().to_string())
        .collect()
}

fn u64_to_i64(field: &str, value: u64) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{field} exceeds PostgreSQL BIGINT range"))
}

fn optional_u64_to_i64(field: &str, value: Option<u64>) -> Result<Option<i64>> {
    value.map(|value| u64_to_i64(field, value)).transpose()
}

fn usize_to_i64(field: &str, value: usize) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{field} exceeds PostgreSQL BIGINT range"))
}

fn i64_to_u64(field: &str, value: i64) -> Result<u64> {
    u64::try_from(value).with_context(|| format!("{field} is negative"))
}

fn optional_i64_to_u64(field: &str, value: Option<i64>) -> Result<Option<u64>> {
    value.map(|value| i64_to_u64(field, value)).transpose()
}

fn max_optional_u64(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn block_timestamp_record_from_row(row: sqlx::postgres::PgRow) -> Result<BlockTimestampRecord> {
    let source_backend: String = row.get("source_backend");
    let source_backend = match source_backend.as_str() {
        "native" => BroadcasterBackend::Native,
        "vm" => BroadcasterBackend::Vm,
        other => {
            return Err(anyhow!(
                "unsupported block timestamp source backend {other}"
            ))
        }
    };
    Ok(BlockTimestampRecord {
        chain_id: i64_to_u64("chain_id", row.get("chain_id"))?,
        block_number: i64_to_u64("block_number", row.get("block_number"))?,
        timestamp_ms: i64_to_u64("timestamp_ms", row.get("timestamp_ms"))?,
        block_hash: row.get("block_hash"),
        parent_hash: row.get("parent_hash"),
        source_stream_id: row.get("source_stream_id"),
        source_message_seq: i64_to_u64("source_message_seq", row.get("source_message_seq"))?,
        source_backend,
        source_protocol: row.get("source_protocol"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn checkpoint_manifest_from_row(row: sqlx::postgres::PgRow) -> Result<CheckpointManifest> {
    let status: String = row.get("status");
    let backends: Vec<String> = row.get("backend_scope");
    Ok(CheckpointManifest {
        id: row.get("id"),
        metadata: CheckpointArchiveMetadata {
            chain_id: i64_to_u64("chain_id", row.get("chain_id"))?,
            block_number: i64_to_u64("block_number", row.get("block_number"))?,
            captured_at_timestamp_ms: i64_to_u64(
                "captured_at_timestamp_ms",
                row.get("captured_at_timestamp_ms"),
            )?,
            rfq_update_timestamp_ms: optional_i64_to_u64(
                "rfq_update_timestamp_ms",
                row.get("rfq_update_timestamp_ms"),
            )?,
            stream_id: row.get("stream_id"),
            source_message_seq: i64_to_u64("source_message_seq", row.get("source_message_seq"))?,
            backends: parse_backend_strings(&backends)?,
        },
        s3_bucket: row.get("s3_bucket"),
        s3_key: row.get("s3_key"),
        payload_hash: row.get("payload_hash"),
        payload_bytes: row.get("payload_bytes"),
        compressed_bytes: row.get("compressed_bytes"),
        status: CheckpointStatus::from_str(&status)?,
        error: row.get("error"),
    })
}

fn parse_backend_strings(values: &[String]) -> Result<Vec<BroadcasterBackend>> {
    let scope = values.join(",");
    parse_backend_scope(&scope)
}

fn decode_entry_envelope(entry: &BroadcasterRedisStreamEntry) -> Result<BroadcasterEnvelope> {
    serde_json::from_str(&entry.payload_json).context("failed to decode Redis stream entry payload")
}

fn stored_delta_from_row(row: sqlx::postgres::PgRow) -> Result<StoredDeltaEntry> {
    let encoding: String = row.get("payload_encoding");
    anyhow::ensure!(
        encoding == PayloadEncoding::JsonZstd.as_str(),
        "unsupported state history delta payload encoding {encoding}"
    );
    let payload_compressed: Vec<u8> = row.get("payload_compressed");
    let uncompressed = zstd::stream::decode_all(Cursor::new(&payload_compressed))
        .context("failed to decompress state history delta payload")?;
    let hash = sha256_hex(&uncompressed);
    let expected_hash: String = row.get("payload_hash");
    anyhow::ensure!(
        hash == expected_hash,
        "state history delta payload hash mismatch: expected {expected_hash}, decoded {hash}"
    );
    let entry = serde_json::from_slice(&uncompressed)
        .context("failed to deserialize state history delta payload")?;
    Ok(StoredDeltaEntry {
        id: row.get("id"),
        redis_entry_id: row.get("redis_entry_id"),
        payload: EncodedPayload {
            encoding: PayloadEncoding::JsonZstd,
            hash,
            uncompressed_bytes: uncompressed.len(),
            compressed_bytes: payload_compressed.len(),
        },
        entry,
    })
}

pub fn checkpoint_s3_key(
    prefix: &str,
    chain_id: u64,
    block_number: u64,
    captured_at_timestamp_ms: u64,
    stream_id: &str,
) -> String {
    let prefix = prefix.trim_matches('/');
    let key = format!(
        "chain={chain_id}/block={block_number}/timestamp={captured_at_timestamp_ms}/stream={stream_id}/checkpoint.zst"
    );
    if prefix.is_empty() {
        key
    } else {
        format!("{prefix}/{key}")
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        output.push(HEX[usize::from(byte >> 4)] as char);
        output.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use anyhow::Result;
    use simulator_core::broadcaster::{
        BroadcasterBackend, BroadcasterBlockRef, BroadcasterEnvelope, BroadcasterMessageKind,
        BroadcasterPayload, BroadcasterProtocolMessage, BroadcasterProtocolSyncStatus,
        BroadcasterProtocolSyncStatusKind, BroadcasterRedisStreamEntry, BroadcasterSnapshotChunk,
        BroadcasterSnapshotPartition, BroadcasterUpdateMessage, BroadcasterUpdatePartition,
    };
    use tycho_simulation::{
        tycho_client::feed::{
            synchronizer::{Snapshot, StateSyncMessage},
            BlockHeader, SynchronizerState,
        },
        tycho_common::Bytes,
    };

    use super::{
        backtest_boundary_from_rows, block_timestamp_ms_from_seconds, block_timestamps_for_entry,
        block_timestamps_from_checkpoint_archive, build_validated_history_segments,
        build_validated_replay_segments, classify_block_timestamp_write,
        collect_block_timestamps_from_payload, decode_checkpoint_archive_bytes,
        encode_checkpoint_archive, indexed_backends_for_entry, ingestion_gap_for_entry,
        ingestion_gap_within_segments, optional_u64_to_i64, prepare_delta_message,
        run_state_history_writer, u64_to_i64, verify_ingestion_coverage_from_cursors,
        writer_retry_backoff, BacktestBoundaryTimestamps, BacktestRangeRequest,
        BlockTimestampCollector, BlockTimestampMetadata, BlockTimestampWriteOutcome,
        CheckpointArchive, CheckpointArchiveMetadata, CheckpointManifest, CheckpointPayload,
        CheckpointStatus, DecodedCheckpointArchive, HistoryRangeGap, HistoryRangeGapSource,
        HistoryRangePlan, HistoryRangeRequest, HistoryReplaySegment, HistoryStreamCursor,
        StateHistoryPgStore, StateHistoryWriteCommand, StateHistoryWriter,
        StateHistoryWriterSnapshot, StoredBlockTimestamp, StoredGenerationHandoff,
        StoredGenerationHandoffs, WriterShutdown, DEFAULT_GAP_RECORD_TASK_LIMIT,
        WRITER_RETRY_BACKOFF_CAP,
    };

    #[test]
    fn checkpoint_archive_round_trips_with_hash_and_payload_order() -> Result<()> {
        let first = update_envelope("stream-1", 7, BroadcasterBackend::Native, 123)?;
        let second = update_envelope("stream-1", 8, BroadcasterBackend::Rfq, 456)?;
        let archive = CheckpointArchive {
            metadata: CheckpointArchiveMetadata {
                chain_id: 8453,
                block_number: 123,
                captured_at_timestamp_ms: 1_720_000_000_000,
                rfq_update_timestamp_ms: Some(456),
                stream_id: "stream-1".to_string(),
                source_message_seq: 8,
                backends: vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq],
            },
            payloads: vec![first.clone(), second.clone()],
        };

        let encoded = encode_checkpoint_archive(&archive)?;
        let decoded: DecodedCheckpointArchive = encoded.decode()?;

        assert_eq!(decoded.archive.metadata, archive.metadata);
        assert_eq!(decoded.archive.payloads.len(), 2);
        assert_eq!(decoded.archive.payloads[0].message_seq, first.message_seq);
        assert_eq!(decoded.archive.payloads[1].message_seq, second.message_seq);
        assert_eq!(decoded.payload.hash, encoded.payload.hash);
        assert_eq!(decoded.payload.compressed_bytes, encoded.bytes.len());
        assert!(decoded.payload.uncompressed_bytes > 0);

        Ok(())
    }

    #[test]
    fn checkpoint_decode_rejects_hash_mismatch() -> Result<()> {
        let envelope = update_envelope("stream-1", 7, BroadcasterBackend::Native, 123)?;
        let archive = CheckpointArchive {
            metadata: CheckpointArchiveMetadata {
                chain_id: 8453,
                block_number: 123,
                captured_at_timestamp_ms: 1_720_000_000_000,
                rfq_update_timestamp_ms: None,
                stream_id: "stream-1".to_string(),
                source_message_seq: 7,
                backends: vec![BroadcasterBackend::Native],
            },
            payloads: vec![envelope],
        };
        let encoded = encode_checkpoint_archive(&archive)?;

        let error = decode_checkpoint_archive_bytes(encoded.bytes, Some("wrong-hash"))
            .err()
            .ok_or_else(|| anyhow::anyhow!("hash mismatch should fail"))?;

        assert!(error.to_string().contains("hash mismatch"));
        Ok(())
    }

    #[test]
    fn indexed_backends_are_derived_from_redis_backend_scope() -> Result<()> {
        let native = update_envelope("stream-1", 9, BroadcasterBackend::Native, 124)?;
        let rfq = update_envelope("stream-1", 10, BroadcasterBackend::Rfq, 789)?;
        let native_entry = BroadcasterRedisStreamEntry::from_envelope(8453, &native)?;
        let rfq_entry = BroadcasterRedisStreamEntry::from_envelope(8453, &rfq)?;

        assert_eq!(
            indexed_backends_for_entry(&native_entry)?,
            vec![CheckpointPayload {
                backend: BroadcasterBackend::Native,
                block_number: Some(124),
                observed_timestamp_ms: None,
            }]
        );
        assert_eq!(
            indexed_backends_for_entry(&rfq_entry)?,
            vec![CheckpointPayload {
                backend: BroadcasterBackend::Rfq,
                block_number: None,
                observed_timestamp_ms: Some(789),
            }]
        );

        Ok(())
    }

    #[test]
    fn indexed_backends_use_each_update_partition_cursor() -> Result<()> {
        let envelope = multi_backend_update_envelope(
            "stream-1",
            12,
            [
                (BroadcasterBackend::Native, 124),
                (BroadcasterBackend::Vm, 127),
                (BroadcasterBackend::Rfq, 1_720_000_000_000),
            ],
        )?;
        let entry = BroadcasterRedisStreamEntry::from_envelope(8453, &envelope)?;

        assert_eq!(
            indexed_backends_for_entry(&entry)?,
            vec![
                CheckpointPayload {
                    backend: BroadcasterBackend::Native,
                    block_number: Some(124),
                    observed_timestamp_ms: None,
                },
                CheckpointPayload {
                    backend: BroadcasterBackend::Vm,
                    block_number: Some(127),
                    observed_timestamp_ms: None,
                },
                CheckpointPayload {
                    backend: BroadcasterBackend::Rfq,
                    block_number: None,
                    observed_timestamp_ms: Some(1_720_000_000_000),
                },
            ]
        );

        Ok(())
    }

    #[test]
    fn indexed_backends_reject_update_scope_partition_mismatch() -> Result<()> {
        let envelope = update_envelope("stream-1", 12, BroadcasterBackend::Native, 124)?;
        let mut entry = BroadcasterRedisStreamEntry::from_envelope(8453, &envelope)?;
        entry.backend_scope = "native,rfq".to_string();

        let error = indexed_backends_for_entry(&entry)
            .err()
            .ok_or_else(|| anyhow::anyhow!("scope mismatch should fail"))?;

        assert!(error.to_string().contains("partition backends"));
        Ok(())
    }

    #[test]
    fn block_timestamps_are_extracted_from_native_and_vm_update_blocks() -> Result<()> {
        let native_block = block_ref(124, 1, 1_720_000_001);
        let vm_block = block_ref(125, 2, 1_720_000_002);
        let update = BroadcasterUpdateMessage::new(vec![
            BroadcasterUpdatePartition::new(
                BroadcasterBackend::Native,
                native_block.number,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                sync_statuses_with_block("uniswap_v2", native_block.clone()),
            ),
            BroadcasterUpdatePartition::new(
                BroadcasterBackend::Vm,
                vm_block.number,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                sync_statuses_with_block("vm:curve", vm_block.clone()),
            ),
        ])?;
        let envelope = BroadcasterEnvelope::new(
            "stream-1",
            12,
            simulator_core::broadcaster::BroadcasterPayload::Update(update),
        );
        let entry = BroadcasterRedisStreamEntry::from_envelope(8453, &envelope)?;

        let collected = block_timestamps_for_entry(
            &entry,
            &[BroadcasterBackend::Native, BroadcasterBackend::Vm],
        );
        let timestamps = collected.rows;

        assert_eq!(collected.skipped_records, 0);
        assert_eq!(timestamps.len(), 2);
        assert_eq!(timestamps[0].chain_id, 8453);
        assert_eq!(timestamps[0].block_number, 124);
        assert_eq!(timestamps[0].timestamp_ms, 1_720_000_001_000);
        assert_eq!(timestamps[0].block_hash, vec![1u8; 32]);
        assert_eq!(timestamps[0].parent_hash, vec![2u8; 32]);
        assert_eq!(timestamps[0].source_stream_id, "stream-1");
        assert_eq!(timestamps[0].source_message_seq, 12);
        assert_eq!(timestamps[0].source_backend, BroadcasterBackend::Native);
        assert_eq!(timestamps[0].source_protocol, "uniswap_v2");
        assert_eq!(timestamps[1].block_number, 125);
        assert_eq!(timestamps[1].timestamp_ms, 1_720_000_002_000);
        assert_eq!(timestamps[1].source_backend, BroadcasterBackend::Vm);
        assert_eq!(timestamps[1].source_protocol, "vm:curve");

        Ok(())
    }

    #[test]
    fn block_timestamps_are_extracted_from_raw_protocol_message_headers() -> Result<()> {
        let update =
            BroadcasterUpdateMessage::new(vec![BroadcasterUpdatePartition::with_messages(
                BroadcasterBackend::Native,
                126,
                vec![raw_protocol_message("uniswap_v2", 126, 3, 1_720_000_003)],
                BTreeMap::new(),
            )])?;
        let envelope = BroadcasterEnvelope::new(
            "stream-1",
            13,
            simulator_core::broadcaster::BroadcasterPayload::Update(update),
        );
        let entry = BroadcasterRedisStreamEntry::from_envelope(8453, &envelope)?;

        let timestamps = block_timestamps_for_entry(&entry, &[BroadcasterBackend::Native]).rows;

        assert_eq!(timestamps.len(), 1);
        assert_eq!(timestamps[0].block_number, 126);
        assert_eq!(timestamps[0].timestamp_ms, 1_720_000_003_000);
        assert_eq!(timestamps[0].block_hash, vec![3u8; 32]);
        assert_eq!(timestamps[0].parent_hash, vec![4u8; 32]);
        assert_eq!(timestamps[0].source_backend, BroadcasterBackend::Native);
        assert_eq!(timestamps[0].source_protocol, "uniswap_v2");

        Ok(())
    }

    #[test]
    fn block_timestamps_are_extracted_from_snapshot_chunk_partitions() -> Result<()> {
        let native_partition = BroadcasterSnapshotPartition::with_messages(
            BroadcasterBackend::Native,
            130,
            vec![raw_protocol_message("uniswap_v2", 130, 5, 1_720_000_005)],
            sync_statuses_with_block("uniswap_v3", block_ref(131, 7, 1_720_000_006)),
        );
        let rfq_partition = BroadcasterSnapshotPartition::new(
            BroadcasterBackend::Rfq,
            132,
            Vec::new(),
            sync_statuses_with_block("rfq:bebop", block_ref(132, 9, 1_720_000_007)),
        );
        let chunk =
            BroadcasterSnapshotChunk::new("snapshot-1", 0, vec![native_partition, rfq_partition])?;

        let mut collector = BlockTimestampCollector::default();
        collect_block_timestamps_from_payload(
            &mut collector,
            8453,
            "checkpoint-stream",
            42,
            &BroadcasterPayload::SnapshotChunk(chunk),
        );
        let collected = collector.finish();

        assert_eq!(collected.skipped_records, 0);
        assert_eq!(collected.rows.len(), 2);
        assert_eq!(collected.rows[0].block_number, 130);
        assert_eq!(collected.rows[0].timestamp_ms, 1_720_000_005_000);
        assert_eq!(collected.rows[0].source_stream_id, "checkpoint-stream");
        assert_eq!(collected.rows[0].source_message_seq, 42);
        assert_eq!(collected.rows[0].source_backend, BroadcasterBackend::Native);
        assert_eq!(collected.rows[0].source_protocol, "uniswap_v2");
        assert_eq!(collected.rows[1].block_number, 131);
        assert_eq!(collected.rows[1].source_protocol, "uniswap_v3");

        Ok(())
    }

    #[test]
    fn block_timestamps_dedup_identical_records_per_height() -> Result<()> {
        // Message header and sync-status ref carry identical content for block 124.
        let update =
            BroadcasterUpdateMessage::new(vec![BroadcasterUpdatePartition::with_messages(
                BroadcasterBackend::Native,
                124,
                vec![raw_protocol_message("uniswap_v2", 124, 1, 1_720_000_001)],
                sync_statuses_with_block("uniswap_v3", block_ref(124, 1, 1_720_000_001)),
            )])?;
        let envelope = BroadcasterEnvelope::new(
            "stream-1",
            12,
            simulator_core::broadcaster::BroadcasterPayload::Update(update),
        );
        let entry = BroadcasterRedisStreamEntry::from_envelope(8453, &envelope)?;

        let collected = block_timestamps_for_entry(&entry, &[BroadcasterBackend::Native]);

        assert_eq!(collected.skipped_records, 0);
        assert_eq!(collected.rows.len(), 1);
        assert_eq!(collected.rows[0].block_number, 124);
        assert_eq!(collected.rows[0].timestamp_ms, 1_720_000_001_000);
        // First-seen candidate keeps provenance, message headers stage before sync statuses.
        assert_eq!(collected.rows[0].source_protocol, "uniswap_v2");

        Ok(())
    }

    #[test]
    fn block_timestamps_conflicting_same_height_records_skip_height() -> Result<()> {
        let update = BroadcasterUpdateMessage::new(vec![
            BroadcasterUpdatePartition::with_messages(
                BroadcasterBackend::Native,
                124,
                vec![raw_protocol_message("uniswap_v2", 124, 1, 1_720_000_001)],
                sync_statuses_with_block("uniswap_v3", block_ref(124, 9, 1_720_000_001)),
            ),
            BroadcasterUpdatePartition::new(
                BroadcasterBackend::Vm,
                125,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                sync_statuses_with_block("vm:curve", block_ref(125, 2, 1_720_000_002)),
            ),
        ])?;
        let envelope = BroadcasterEnvelope::new(
            "stream-1",
            12,
            simulator_core::broadcaster::BroadcasterPayload::Update(update),
        );
        let entry = BroadcasterRedisStreamEntry::from_envelope(8453, &envelope)?;

        let collected = block_timestamps_for_entry(
            &entry,
            &[BroadcasterBackend::Native, BroadcasterBackend::Vm],
        );

        assert_eq!(collected.skipped_records, 1);
        assert_eq!(collected.rows.len(), 1);
        assert_eq!(collected.rows[0].block_number, 125);
        assert_eq!(collected.rows[0].source_backend, BroadcasterBackend::Vm);

        Ok(())
    }

    #[test]
    fn checkpoint_archive_conflict_skip_spans_chunks_and_fails_missing_boundary() -> Result<()> {
        let metadata = CheckpointArchiveMetadata {
            chain_id: 8453,
            block_number: 130,
            captured_at_timestamp_ms: 1_720_000_000_000,
            rfq_update_timestamp_ms: None,
            stream_id: "stream-live".to_string(),
            source_message_seq: 42,
            backends: vec![BroadcasterBackend::Native],
        };

        // The chunks disagree about the boundary height, so the archive-wide skip
        // rule leaves no boundary row and the guard hard-errors.
        let conflicted_boundary = CheckpointArchive {
            metadata: metadata.clone(),
            payloads: vec![
                snapshot_chunk_envelope(1, 0, vec![block_ref(130, 1, 1_720_000_005)])?,
                snapshot_chunk_envelope(2, 1, vec![block_ref(130, 2, 1_720_000_005)])?,
            ],
        };
        let error = block_timestamps_from_checkpoint_archive(&conflicted_boundary)
            .err()
            .ok_or_else(|| anyhow::anyhow!("conflicted boundary height should fail"))?;
        assert!(error
            .to_string()
            .contains("no usable block timestamp for boundary block 130"));

        // A cross-chunk conflict away from the boundary only poisons that height.
        let conflicted_sibling = CheckpointArchive {
            metadata,
            payloads: vec![
                snapshot_chunk_envelope(
                    1,
                    0,
                    vec![
                        block_ref(130, 1, 1_720_000_005),
                        block_ref(131, 3, 1_720_000_006),
                    ],
                )?,
                snapshot_chunk_envelope(2, 1, vec![block_ref(131, 4, 1_720_000_006)])?,
            ],
        };
        let collected = block_timestamps_from_checkpoint_archive(&conflicted_sibling)?;

        assert_eq!(collected.skipped_records, 1);
        assert_eq!(collected.rows.len(), 1);
        assert_eq!(collected.rows[0].block_number, 130);
        assert_eq!(collected.rows[0].timestamp_ms, 1_720_000_005_000);
        // Provenance comes from the metadata replay-boundary cursor, not the
        // synthetic per-archive envelope stream or seqs.
        assert_eq!(collected.rows[0].source_stream_id, "stream-live");
        assert_eq!(collected.rows[0].source_message_seq, 42);

        Ok(())
    }

    #[test]
    fn block_timestamps_skip_headers_with_overflowing_timestamps() -> Result<()> {
        let mut sync_statuses =
            sync_statuses_with_block("uniswap_v2", block_ref(124, 1, 1_720_000_001));
        sync_statuses.extend(sync_statuses_with_block(
            "uniswap_v3",
            block_ref(125, 2, u64::MAX),
        ));
        let update = BroadcasterUpdateMessage::new(vec![BroadcasterUpdatePartition::new(
            BroadcasterBackend::Native,
            125,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            sync_statuses,
        )])?;
        let envelope = BroadcasterEnvelope::new(
            "stream-1",
            12,
            simulator_core::broadcaster::BroadcasterPayload::Update(update),
        );
        let entry = BroadcasterRedisStreamEntry::from_envelope(8453, &envelope)?;

        let collected = block_timestamps_for_entry(&entry, &[BroadcasterBackend::Native]);

        assert_eq!(collected.skipped_records, 1);
        assert_eq!(collected.rows.len(), 1);
        assert_eq!(collected.rows[0].block_number, 124);
        assert_eq!(collected.rows[0].timestamp_ms, 1_720_000_001_000);

        Ok(())
    }

    #[test]
    fn block_timestamp_extraction_skips_payload_for_rfq_only_scope() -> Result<()> {
        let envelope = update_envelope("stream-1", 14, BroadcasterBackend::Rfq, 789)?;
        let mut entry = BroadcasterRedisStreamEntry::from_envelope(8453, &envelope)?;
        // An undecodable payload proves the RFQ-only path never touches payload_json.
        entry.payload_json = "not json".to_string();

        let collected = block_timestamps_for_entry(&entry, &[BroadcasterBackend::Rfq]);

        assert!(collected.rows.is_empty());
        assert_eq!(collected.skipped_records, 0);

        Ok(())
    }

    #[test]
    fn block_timestamp_extraction_survives_undecodable_payload() -> Result<()> {
        let envelope = update_envelope("stream-1", 15, BroadcasterBackend::Native, 124)?;
        let mut entry = BroadcasterRedisStreamEntry::from_envelope(8453, &envelope)?;
        entry.payload_json = "not json".to_string();

        let collected = block_timestamps_for_entry(&entry, &[BroadcasterBackend::Native]);

        assert!(collected.rows.is_empty());
        assert_eq!(collected.skipped_records, 1);

        Ok(())
    }

    #[test]
    fn block_timestamp_seconds_normalize_to_milliseconds_with_overflow_check() {
        assert_eq!(
            block_timestamp_ms_from_seconds(1_720_000_001),
            Some(1_720_000_001_000)
        );
        // Multiplication overflows u64.
        assert_eq!(block_timestamp_ms_from_seconds(u64::MAX), None);
        // Fits u64 after the multiply but not the BIGINT column.
        assert_eq!(block_timestamp_ms_from_seconds(u64::MAX / 1_000), None);
    }

    #[test]
    fn block_timestamp_write_classification_covers_supersession_matrix() {
        let incoming = BlockTimestampMetadata {
            chain_id: 8453,
            block_number: 111,
            timestamp_ms: 1_720_000_010_000,
            block_hash: vec![0xaa; 32],
            parent_hash: vec![0xbb; 32],
            source_stream_id: "stream-live".to_string(),
            source_message_seq: 8,
            source_backend: BroadcasterBackend::Native,
            source_protocol: "uniswap_v2".to_string(),
        };
        // Same stream at a newer seq, the source-order predicate skips these writes.
        let same_stream_newer_identical = StoredBlockTimestamp {
            timestamp_ms: incoming.timestamp_ms,
            block_hash: incoming.block_hash.clone(),
            parent_hash: incoming.parent_hash.clone(),
            source_stream_id: incoming.source_stream_id.clone(),
            source_message_seq: 10,
        };
        let same_stream_newer_conflicting = StoredBlockTimestamp {
            block_hash: vec![0xcc; 32],
            ..same_stream_newer_identical.clone()
        };
        // Different stream at a higher seq, the predicate still applies these writes.
        let cross_stream_identical = StoredBlockTimestamp {
            source_stream_id: "stream-checkpoint".to_string(),
            source_message_seq: 42,
            ..same_stream_newer_identical.clone()
        };
        let cross_stream_conflicting = StoredBlockTimestamp {
            timestamp_ms: incoming.timestamp_ms + 1_000,
            block_hash: vec![0xdd; 32],
            ..cross_stream_identical.clone()
        };

        // Applied without a pre-image is a fresh insert. Under READ COMMITTED it can
        // also be a concurrent overwrite, the documented Inserted caveat.
        assert_eq!(
            classify_block_timestamp_write(&incoming, true, None),
            BlockTimestampWriteOutcome::Inserted
        );
        // Not applied without a pre-image means the insert race was lost.
        assert_eq!(
            classify_block_timestamp_write(&incoming, false, None),
            BlockTimestampWriteOutcome::ConcurrentSkip
        );
        // Verbatim redelivery leaves the row and updated_at untouched.
        assert_eq!(
            classify_block_timestamp_write(&incoming, false, Some(&same_stream_newer_identical)),
            BlockTimestampWriteOutcome::IdenticalNoop
        );
        // A stale source with different content never displaces the stored row.
        assert_eq!(
            classify_block_timestamp_write(&incoming, false, Some(&same_stream_newer_conflicting)),
            BlockTimestampWriteOutcome::StaleSourceKept
        );
        // Identical content accepted from another source only advances provenance.
        assert_eq!(
            classify_block_timestamp_write(&incoming, true, Some(&cross_stream_identical)),
            BlockTimestampWriteOutcome::SourceAdvanced
        );
        // Cross-stream writes apply regardless of the stored seq, so an unfenced late
        // writer on another stream overwrites. This pins the accepted residual.
        assert_eq!(
            classify_block_timestamp_write(&incoming, true, Some(&cross_stream_conflicting)),
            BlockTimestampWriteOutcome::Superseded
        );
    }

    #[test]
    fn backtest_range_request_builds_history_request_with_rfq_block_timestamp_bounds() -> Result<()>
    {
        let request = BacktestRangeRequest::new(
            8453,
            100,
            200,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq],
        )?;
        let boundary = BacktestBoundaryTimestamps {
            start_block_timestamp_ms: 1_720_000_000_000,
            end_block_timestamp_ms: 1_720_000_060_000,
            next_block_timestamp_ms: 1_720_000_062_000,
        };

        let history_request = request.to_history_range_request(Some(&boundary))?;

        assert_eq!(history_request.chain_id, 8453);
        assert_eq!(history_request.start_block_number, 100);
        assert_eq!(history_request.end_block_number, 200);
        assert_eq!(
            history_request.rfq_start_timestamp_ms,
            Some(1_720_000_000_000)
        );
        // The RFQ end bound is the next block timestamp minus 1ms, not the end
        // block's own timestamp, so end-block head-tenure RFQ updates are kept.
        assert_eq!(
            history_request.rfq_end_timestamp_ms,
            Some(1_720_000_061_999)
        );
        assert_eq!(
            history_request.backends,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq]
        );

        Ok(())
    }

    #[test]
    fn backtest_range_request_requires_boundary_timestamps_for_rfq() -> Result<()> {
        let request = BacktestRangeRequest::new(8453, 100, 200, vec![BroadcasterBackend::Rfq])?;

        let error = request
            .to_history_range_request(None)
            .err()
            .ok_or_else(|| anyhow::anyhow!("missing boundary timestamps should fail"))?;

        assert!(error
            .to_string()
            .contains("RFQ backtest ranges require resolved boundary block timestamps"));

        Ok(())
    }

    #[test]
    fn backtest_range_request_rejects_end_block_u64_max() -> Result<()> {
        let error =
            BacktestRangeRequest::new(8453, 100, u64::MAX, vec![BroadcasterBackend::Native])
                .err()
                .ok_or_else(|| anyhow::anyhow!("u64::MAX end block should fail"))?;
        assert!(error.to_string().contains("below u64::MAX"));

        // Fields are pub, so a literally built request must be caught by validate too.
        let literal = BacktestRangeRequest {
            chain_id: 8453,
            start_block_number: 100,
            end_block_number: u64::MAX,
            backends: vec![BroadcasterBackend::Native],
        };
        let error = literal
            .validate()
            .err()
            .ok_or_else(|| anyhow::anyhow!("literal u64::MAX end block should fail validation"))?;
        assert!(error.to_string().contains("below u64::MAX"));

        Ok(())
    }

    #[test]
    fn backtest_boundary_assembly_reports_missing_and_non_increasing_rows() -> Result<()> {
        let request = BacktestRangeRequest::new(8453, 100, 200, vec![BroadcasterBackend::Rfq])?;
        let start_row = (100u64, 1_720_000_000_000u64);
        let end_row = (200u64, 1_720_000_060_000u64);
        let next_row = (201u64, 1_720_000_062_000u64);

        let error = backtest_boundary_from_rows(&request, &[end_row, next_row])
            .err()
            .ok_or_else(|| anyhow::anyhow!("missing start row should fail"))?;
        assert!(error
            .to_string()
            .contains("missing state history block timestamp for start block 100"));

        let error = backtest_boundary_from_rows(&request, &[start_row, next_row])
            .err()
            .ok_or_else(|| anyhow::anyhow!("missing end row should fail"))?;
        assert!(error
            .to_string()
            .contains("missing state history block timestamp for end block 200"));

        let error = backtest_boundary_from_rows(&request, &[start_row, end_row])
            .err()
            .ok_or_else(|| anyhow::anyhow!("missing end+1 row should fail"))?;
        assert!(error
            .to_string()
            .contains("missing state history block timestamp for block 201"));
        assert!(error
            .to_string()
            .contains("ranges ending at the recorded head are unresolvable"));

        let error = backtest_boundary_from_rows(&request, &[start_row, end_row, (201, end_row.1)])
            .err()
            .ok_or_else(|| anyhow::anyhow!("non-increasing next timestamp should fail"))?;
        assert!(error.to_string().contains("must be greater than"));

        // start == end shares one row across both bounds.
        let single_block =
            BacktestRangeRequest::new(8453, 200, 200, vec![BroadcasterBackend::Rfq])?;
        let boundary = backtest_boundary_from_rows(&single_block, &[end_row, next_row])?;
        assert_eq!(boundary.start_block_timestamp_ms, 1_720_000_060_000);
        assert_eq!(boundary.end_block_timestamp_ms, 1_720_000_060_000);
        assert_eq!(boundary.next_block_timestamp_ms, 1_720_000_062_000);
        assert_eq!(boundary.rfq_end_timestamp_ms(), 1_720_000_061_999);

        Ok(())
    }

    #[test]
    fn prepared_delta_message_uses_compressed_redis_entry_json() -> Result<()> {
        let envelope = update_envelope("stream-1", 11, BroadcasterBackend::Native, 125)?;
        let entry = BroadcasterRedisStreamEntry::from_envelope(8453, &envelope)?;

        let prepared = prepare_delta_message(&entry, Some("7-11"))?;
        let decoded = zstd::stream::decode_all(std::io::Cursor::new(&prepared.payload_compressed))?;
        let round_trip: BroadcasterRedisStreamEntry = serde_json::from_slice(&decoded)?;

        assert_eq!(round_trip, entry);
        assert_eq!(prepared.redis_entry_id.as_deref(), Some("7-11"));
        assert_eq!(prepared.payload.encoding.as_str(), "json+zstd");
        assert_eq!(
            prepared.payload.compressed_bytes,
            prepared.payload_compressed.len()
        );
        assert_eq!(prepared.backend_scope, vec![BroadcasterBackend::Native]);
        assert_eq!(prepared.backend_index[0].block_number, Some(125));

        Ok(())
    }

    #[test]
    fn history_range_request_validates_blocks_backends_and_rfq_bounds() -> Result<()> {
        let request = HistoryRangeRequest::new(8453, 100, 200, vec![BroadcasterBackend::Native])?;
        assert!(request.validate().is_ok());

        let error = HistoryRangeRequest::new(8453, 200, 100, vec![BroadcasterBackend::Native])
            .err()
            .ok_or_else(|| anyhow::anyhow!("reversed block range should fail"))?;
        assert!(error.to_string().contains("start block"));

        let error = HistoryRangeRequest::new(
            8453,
            100,
            200,
            vec![BroadcasterBackend::Vm, BroadcasterBackend::Native],
        )
        .err()
        .ok_or_else(|| anyhow::anyhow!("unsorted backends should fail"))?;
        assert!(error.to_string().contains("sorted and unique"));

        let rfq_without_timestamps =
            HistoryRangeRequest::new(8453, 100, 200, vec![BroadcasterBackend::Rfq])?;
        let error = rfq_without_timestamps
            .validate()
            .err()
            .ok_or_else(|| anyhow::anyhow!("RFQ range without timestamps should fail"))?;
        assert!(error.to_string().contains("start timestamp"));

        let rfq_request = HistoryRangeRequest::new(
            8453,
            100,
            200,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq],
        )?
        .with_rfq_timestamp_range(1_720_000_000_000, 1_720_000_060_000)?;
        assert!(rfq_request.validate().is_ok());

        let error = HistoryRangeRequest::new(8453, 100, 200, vec![BroadcasterBackend::Rfq])?
            .with_rfq_timestamp_range(20, 10)
            .err()
            .ok_or_else(|| anyhow::anyhow!("reversed RFQ timestamps should fail"))?;
        assert!(error.to_string().contains("start timestamp"));

        Ok(())
    }

    #[test]
    fn history_range_plan_reports_gaps_without_failing_construction() -> Result<()> {
        let request = HistoryRangeRequest::new(8453, 100, 200, vec![BroadcasterBackend::Native])?;
        let replay_from_block_number = request.start_block_number;
        let plan = HistoryRangePlan {
            request,
            checkpoint: None,
            replay_from_message_seq: None,
            replay_from_block_number,
            rfq_replay_from_timestamp_ms: None,
            deltas: Vec::new(),
            gaps: vec![HistoryRangeGap {
                source: HistoryRangeGapSource::MissingCheckpoint,
                backend_scope: vec![BroadcasterBackend::Native],
                from_block_number: Some(100),
                to_block_number: Some(200),
                from_timestamp_ms: None,
                to_timestamp_ms: None,
                reason: "no complete checkpoint covers the requested range".to_string(),
            }],
        };

        let error = plan.ensure_gap_free().err().ok_or_else(|| {
            anyhow::anyhow!("gap-free enforcement should reject a plan with gaps")
        })?;
        assert!(error.to_string().contains("1 gap"));
        assert!(error.to_string().contains("no complete checkpoint"));

        Ok(())
    }

    #[test]
    fn history_range_uses_checkpoint_as_replay_start() -> Result<()> {
        let request = HistoryRangeRequest::new(
            8453,
            100,
            200,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq],
        )?
        .with_rfq_timestamp_range(1_720_000_000_000, 1_720_000_060_000)?;
        let checkpoint = checkpoint_manifest(90, 1_719_999_990_000, Some(1_719_999_995_000));

        assert_eq!(request.replay_from_message_seq(Some(&checkpoint)), Some(43));
        assert_eq!(request.replay_from_message_seq(None), None);
        assert_eq!(request.replay_from_block_number(Some(&checkpoint)), 90);
        assert_eq!(
            request.rfq_replay_from_timestamp_ms(Some(&checkpoint)),
            Some(1_719_999_995_000)
        );
        assert_eq!(request.replay_from_block_number(None), 100);
        assert_eq!(
            request.rfq_replay_from_timestamp_ms(None),
            Some(1_720_000_000_000)
        );

        Ok(())
    }

    #[test]
    fn replay_segments_follow_valid_multi_hop_handoffs() {
        let checkpoint = checkpoint_manifest_with_stream("chain-8453-stream-1", 1);
        let handoffs = stored_handoffs([
            stored_handoff("chain-8453-stream-1", "1-3", "chain-8453-stream-2", "2-1"),
            stored_handoff("chain-8453-stream-2", "2-3", "chain-8453-stream-3", "3-1"),
        ]);

        let segments = build_validated_replay_segments(&checkpoint, &handoffs);

        assert_eq!(
            segments,
            vec![
                HistoryReplaySegment {
                    ordinal: 0,
                    stream_id: "chain-8453-stream-1".to_string(),
                    from_message_seq: 2,
                    to_message_seq: Some(3),
                },
                HistoryReplaySegment {
                    ordinal: 1,
                    stream_id: "chain-8453-stream-2".to_string(),
                    from_message_seq: 2,
                    to_message_seq: Some(3),
                },
                HistoryReplaySegment {
                    ordinal: 2,
                    stream_id: "chain-8453-stream-3".to_string(),
                    from_message_seq: 2,
                    to_message_seq: None,
                },
            ]
        );
    }

    #[test]
    fn replay_segments_stop_before_malformed_handoff_generation() {
        let checkpoint = checkpoint_manifest_with_stream("chain-8453-stream-1", 1);
        let handoffs = stored_handoffs([stored_handoff(
            "chain-8453-stream-1",
            "1-3",
            "chain-8453-stream-3",
            "3-1",
        )]);

        let segments = build_validated_replay_segments(&checkpoint, &handoffs);

        assert_eq!(
            segments,
            vec![HistoryReplaySegment {
                ordinal: 0,
                stream_id: "chain-8453-stream-1".to_string(),
                from_message_seq: 2,
                to_message_seq: None,
            }]
        );
    }

    #[test]
    fn history_segments_exempt_validated_ancestors_for_switch_detection() {
        let checkpoint = checkpoint_manifest_with_stream("chain-8453-stream-2", 2);
        let handoffs = stored_handoffs([stored_handoff(
            "chain-8453-stream-1",
            "1-4",
            "chain-8453-stream-2",
            "2-1",
        )]);

        let segments = build_validated_history_segments(&checkpoint, &handoffs);

        assert_eq!(
            segments.replay_segments,
            vec![HistoryReplaySegment {
                ordinal: 0,
                stream_id: "chain-8453-stream-2".to_string(),
                from_message_seq: 3,
                to_message_seq: None,
            }]
        );
        assert_eq!(
            segments.generation_switch_exempt_segments,
            vec![
                HistoryReplaySegment {
                    ordinal: 0,
                    stream_id: "chain-8453-stream-1".to_string(),
                    from_message_seq: 1,
                    to_message_seq: Some(4),
                },
                HistoryReplaySegment {
                    ordinal: 1,
                    stream_id: "chain-8453-stream-2".to_string(),
                    from_message_seq: 1,
                    to_message_seq: None,
                },
            ]
        );
    }

    #[test]
    fn unseen_generation_gap_rows_are_not_exempt_from_switch_detection() {
        let checkpoint = checkpoint_manifest_with_stream("chain-8453-stream-2", 2);
        let handoffs = stored_handoffs([stored_handoff(
            "chain-8453-stream-1",
            "1-4",
            "chain-8453-stream-2",
            "2-1",
        )]);
        let segments = build_validated_history_segments(&checkpoint, &handoffs);
        let exempt = segments.generation_switch_exempt_segments.as_slice();

        assert!(!ingestion_gap_within_segments(
            exempt,
            "chain-8453-stream-3",
            1,
            1
        ));
        assert!(!ingestion_gap_within_segments(
            exempt,
            "chain-8453-stream-1",
            3,
            6
        ));
        assert!(ingestion_gap_within_segments(
            exempt,
            "chain-8453-stream-1",
            2,
            4
        ));
        assert!(ingestion_gap_within_segments(
            exempt,
            "chain-8453-stream-2",
            5,
            9
        ));
    }

    #[test]
    fn history_segments_continue_when_checkpoint_sits_on_handoff_tail() {
        let checkpoint = checkpoint_manifest_with_stream("chain-8453-stream-1", 3);
        let handoffs = stored_handoffs([stored_handoff(
            "chain-8453-stream-1",
            "1-3",
            "chain-8453-stream-2",
            "2-1",
        )]);

        let segments = build_validated_history_segments(&checkpoint, &handoffs);

        assert_eq!(
            segments.replay_segments,
            vec![HistoryReplaySegment {
                ordinal: 0,
                stream_id: "chain-8453-stream-2".to_string(),
                from_message_seq: 2,
                to_message_seq: None,
            }]
        );
        assert_eq!(
            segments.generation_switch_exempt_segments,
            vec![
                HistoryReplaySegment {
                    ordinal: 0,
                    stream_id: "chain-8453-stream-1".to_string(),
                    from_message_seq: 1,
                    to_message_seq: Some(3),
                },
                HistoryReplaySegment {
                    ordinal: 1,
                    stream_id: "chain-8453-stream-2".to_string(),
                    from_message_seq: 1,
                    to_message_seq: None,
                },
            ]
        );
    }

    #[test]
    fn history_segments_cap_checkpoint_stream_at_forward_handoff_tail() {
        let checkpoint = checkpoint_manifest_with_stream("chain-8453-stream-1", 5);
        let handoffs = stored_handoffs([stored_handoff(
            "chain-8453-stream-1",
            "1-3",
            "chain-8453-stream-2",
            "2-1",
        )]);

        let segments = build_validated_history_segments(&checkpoint, &handoffs);

        assert_eq!(segments.replay_segments, Vec::new());
        assert_eq!(
            segments.generation_switch_exempt_segments,
            vec![HistoryReplaySegment {
                ordinal: 0,
                stream_id: "chain-8453-stream-1".to_string(),
                from_message_seq: 1,
                to_message_seq: Some(3),
            }]
        );
    }

    #[test]
    fn ingestion_coverage_rejects_cursor_lag() -> Result<()> {
        let request = HistoryRangeRequest::new(8453, 100, 110, vec![BroadcasterBackend::Native])?;
        let segments = vec![HistoryReplaySegment {
            ordinal: 0,
            stream_id: "chain-8453-stream-1".to_string(),
            from_message_seq: 2,
            to_message_seq: None,
        }];
        let cursors = vec![HistoryStreamCursor {
            stream_id: "chain-8453-stream-1".to_string(),
            last_observed_seq: 5,
            last_persistable_seq: 5,
            last_persisted_seq: 4,
            native_head_block: Some(110),
            vm_head_block: None,
            rfq_head_timestamp_ms: None,
        }];

        let gaps = verify_ingestion_coverage_from_cursors(&request, &segments, &cursors);

        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].source, HistoryRangeGapSource::UnprovenIngestion);
        assert!(gaps[0].reason.contains("persistable"));
        Ok(())
    }

    #[test]
    fn ingestion_coverage_rejects_unobserved_closed_stream_tail() -> Result<()> {
        let request = HistoryRangeRequest::new(8453, 100, 110, vec![BroadcasterBackend::Native])?;
        let segments = vec![
            HistoryReplaySegment {
                ordinal: 0,
                stream_id: "chain-8453-stream-1".to_string(),
                from_message_seq: 2,
                to_message_seq: Some(7),
            },
            HistoryReplaySegment {
                ordinal: 1,
                stream_id: "chain-8453-stream-2".to_string(),
                from_message_seq: 2,
                to_message_seq: None,
            },
        ];
        let cursors = vec![
            HistoryStreamCursor {
                stream_id: "chain-8453-stream-1".to_string(),
                last_observed_seq: 6,
                last_persistable_seq: 6,
                last_persisted_seq: 6,
                native_head_block: Some(107),
                vm_head_block: None,
                rfq_head_timestamp_ms: None,
            },
            HistoryStreamCursor {
                stream_id: "chain-8453-stream-2".to_string(),
                last_observed_seq: 3,
                last_persistable_seq: 3,
                last_persisted_seq: 3,
                native_head_block: Some(110),
                vm_head_block: None,
                rfq_head_timestamp_ms: None,
            },
        ];

        let gaps = verify_ingestion_coverage_from_cursors(&request, &segments, &cursors);

        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].source, HistoryRangeGapSource::UnprovenIngestion);
        assert!(gaps[0].reason.contains("closed stream"));
        Ok(())
    }

    #[test]
    fn ingestion_coverage_rejects_open_stream_before_requested_head() -> Result<()> {
        let request = HistoryRangeRequest::new(
            8453,
            100,
            110,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq],
        )?
        .with_rfq_timestamp_range(1_720_000_000_000, 1_720_000_000_500)?;
        let segments = vec![HistoryReplaySegment {
            ordinal: 0,
            stream_id: "chain-8453-stream-1".to_string(),
            from_message_seq: 2,
            to_message_seq: None,
        }];
        let cursors = vec![HistoryStreamCursor {
            stream_id: "chain-8453-stream-1".to_string(),
            last_observed_seq: 5,
            last_persistable_seq: 5,
            last_persisted_seq: 5,
            native_head_block: Some(109),
            vm_head_block: None,
            rfq_head_timestamp_ms: Some(1_720_000_000_500),
        }];

        let gaps = verify_ingestion_coverage_from_cursors(&request, &segments, &cursors);

        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].source, HistoryRangeGapSource::UnprovenIngestion);
        assert!(gaps[0].reason.contains("native"));
        Ok(())
    }

    #[test]
    fn ingestion_coverage_accepts_heartbeat_proven_heads() -> Result<()> {
        let request = HistoryRangeRequest::new(
            8453,
            100,
            110,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq],
        )?
        .with_rfq_timestamp_range(1_720_000_000_000, 1_720_000_000_500)?;
        let segments = vec![HistoryReplaySegment {
            ordinal: 0,
            stream_id: "chain-8453-stream-1".to_string(),
            from_message_seq: 2,
            to_message_seq: None,
        }];
        let cursors = vec![HistoryStreamCursor {
            stream_id: "chain-8453-stream-1".to_string(),
            last_observed_seq: 6,
            last_persistable_seq: 5,
            last_persisted_seq: 5,
            native_head_block: Some(110),
            vm_head_block: None,
            rfq_head_timestamp_ms: Some(1_720_000_000_500),
        }];

        let gaps = verify_ingestion_coverage_from_cursors(&request, &segments, &cursors);

        assert!(gaps.is_empty(), "unexpected ingestion gaps: {gaps:?}");
        Ok(())
    }

    #[test]
    fn writer_gap_rows_keep_entry_cursors() -> Result<()> {
        let native = update_envelope("stream-1", 9, BroadcasterBackend::Native, 124)?;
        let rfq = update_envelope("stream-1", 10, BroadcasterBackend::Rfq, 789)?;
        let native_gap = ingestion_gap_for_entry(
            &BroadcasterRedisStreamEntry::from_envelope(8453, &native)?,
            "storage unavailable",
        )?;
        let rfq_gap = ingestion_gap_for_entry(
            &BroadcasterRedisStreamEntry::from_envelope(8453, &rfq)?,
            "storage unavailable",
        )?;

        assert_eq!(native_gap.backend_scope, vec![BroadcasterBackend::Native]);
        assert_eq!(native_gap.from_block_number, Some(124));
        assert_eq!(native_gap.from_timestamp_ms, None);
        assert_eq!(rfq_gap.backend_scope, vec![BroadcasterBackend::Rfq]);
        assert_eq!(rfq_gap.from_block_number, None);
        assert_eq!(rfq_gap.from_timestamp_ms, Some(789));
        assert_eq!(rfq_gap.reason, "storage unavailable");

        Ok(())
    }

    #[test]
    fn writer_retry_backoff_is_bounded_by_cap_and_remaining_window() {
        assert!(
            writer_retry_backoff(1, std::time::Duration::from_secs(30)) > std::time::Duration::ZERO
        );
        assert_eq!(
            writer_retry_backoff(100, std::time::Duration::from_secs(30)),
            WRITER_RETRY_BACKOFF_CAP
        );
        assert_eq!(
            writer_retry_backoff(100, std::time::Duration::from_millis(25)),
            std::time::Duration::from_millis(25)
        );
    }

    #[tokio::test]
    async fn writer_stamps_dropped_persistable_before_try_send() -> Result<()> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let writer = test_state_history_writer(sender, 0)?;
        let first = BroadcasterRedisStreamEntry::from_envelope(
            8453,
            &update_envelope("chain-8453-stream-1", 1, BroadcasterBackend::Native, 100)?,
        )?;
        let dropped = BroadcasterRedisStreamEntry::from_envelope(
            8453,
            &update_envelope("chain-8453-stream-1", 2, BroadcasterBackend::Native, 101)?,
        )?;
        let third = BroadcasterRedisStreamEntry::from_envelope(
            8453,
            &update_envelope("chain-8453-stream-1", 3, BroadcasterBackend::Native, 102)?,
        )?;

        writer.enqueue_entry(first, "1-1".to_string()).await?;
        let error = writer
            .enqueue_entry(dropped, "1-2".to_string())
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("full queue should reject second persistable entry"))?;
        assert!(error.to_string().contains("queue full"));

        let first_command = receiver
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("first command should be queued"))?;
        match first_command {
            StateHistoryWriteCommand::Persist {
                prev_persistable_message_seq,
                observation,
                ..
            } => {
                assert_eq!(prev_persistable_message_seq, None);
                assert_eq!(observation.last_persistable_seq, 1);
            }
            StateHistoryWriteCommand::Observe(_) => {
                anyhow::bail!("first update should persist")
            }
        }

        writer.enqueue_entry(third, "1-3".to_string()).await?;
        let third_command = receiver
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("third command should be queued"))?;
        match third_command {
            StateHistoryWriteCommand::Persist {
                prev_persistable_message_seq,
                observation,
                ..
            } => {
                assert_eq!(prev_persistable_message_seq, Some(2));
                assert_eq!(observation.last_persistable_seq, 3);
            }
            StateHistoryWriteCommand::Observe(_) => {
                anyhow::bail!("third update should persist")
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn writer_heartbeats_observe_latest_persistable_cursor() -> Result<()> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
        let writer = test_state_history_writer(sender, DEFAULT_GAP_RECORD_TASK_LIMIT)?;
        let update = BroadcasterRedisStreamEntry::from_envelope(
            8453,
            &update_envelope("chain-8453-stream-1", 1, BroadcasterBackend::Native, 100)?,
        )?;
        let heartbeat = BroadcasterRedisStreamEntry::from_envelope(
            8453,
            &BroadcasterEnvelope::new(
                "chain-8453-stream-1",
                2,
                simulator_core::broadcaster::BroadcasterPayload::Heartbeat(
                    simulator_core::broadcaster::BroadcasterHeartbeat::new(
                        8453,
                        "snapshot-1",
                        vec![simulator_core::broadcaster::BroadcasterBackendHead::new(
                            BroadcasterBackend::Native,
                            101,
                        )],
                    )?,
                ),
            ),
        )?;

        writer.enqueue_entry(update, "1-1".to_string()).await?;
        writer.enqueue_entry(heartbeat, "1-2".to_string()).await?;
        let _update_command = receiver
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("update command should be queued"))?;
        let heartbeat_command = receiver
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("heartbeat command should be queued"))?;

        match heartbeat_command {
            StateHistoryWriteCommand::Observe(observation) => {
                assert_eq!(observation.last_persistable_seq, 1);
                assert_eq!(observation.heads.native_head_block, Some(101));
            }
            StateHistoryWriteCommand::Persist { .. } => {
                anyhow::bail!("heartbeat should only observe")
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn writer_shutdown_signal_is_sticky_and_stops_idle_task() -> Result<()> {
        let shutdown = std::sync::Arc::new(WriterShutdown::default());
        shutdown.cancel();
        tokio::time::timeout(std::time::Duration::from_millis(50), shutdown.cancelled()).await?;

        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@127.0.0.1/state_history")?;
        let pg_store = StateHistoryPgStore::from_pool(pool);
        let status = std::sync::Arc::new(tokio::sync::RwLock::new(StateHistoryWriterSnapshot {
            healthy: true,
            queue_capacity: 1,
            retry_window_ms: 1,
            ..StateHistoryWriterSnapshot::default()
        }));
        let shutdown = std::sync::Arc::new(WriterShutdown::default());
        let task = tokio::spawn(run_state_history_writer(
            pg_store.clone(),
            receiver,
            status.clone(),
            std::time::Duration::from_millis(1),
            shutdown.clone(),
        ));
        let writer = StateHistoryWriter {
            sender,
            pg_store,
            status,
            persistable_by_stream: std::sync::Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
            gap_record_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
                DEFAULT_GAP_RECORD_TASK_LIMIT,
            )),
            shutdown,
            task: std::sync::Arc::new(tokio::sync::Mutex::new(Some(task))),
        };

        writer.shutdown(std::time::Duration::from_secs(1)).await?;

        Ok(())
    }

    fn checkpoint_manifest(
        block_number: u64,
        captured_at_timestamp_ms: u64,
        rfq_update_timestamp_ms: Option<u64>,
    ) -> CheckpointManifest {
        checkpoint_manifest_with_stream_and_cursor(
            "stream-1",
            42,
            block_number,
            captured_at_timestamp_ms,
            rfq_update_timestamp_ms,
        )
    }

    fn checkpoint_manifest_with_stream(
        stream_id: &str,
        source_message_seq: u64,
    ) -> CheckpointManifest {
        checkpoint_manifest_with_stream_and_cursor(stream_id, source_message_seq, 90, 1, None)
    }

    fn checkpoint_manifest_with_stream_and_cursor(
        stream_id: &str,
        source_message_seq: u64,
        block_number: u64,
        captured_at_timestamp_ms: u64,
        rfq_update_timestamp_ms: Option<u64>,
    ) -> CheckpointManifest {
        CheckpointManifest {
            id: 1,
            metadata: CheckpointArchiveMetadata {
                chain_id: 8453,
                block_number,
                captured_at_timestamp_ms,
                rfq_update_timestamp_ms,
                stream_id: stream_id.to_string(),
                source_message_seq,
                backends: vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq],
            },
            s3_bucket: "state-history".to_string(),
            s3_key: "checkpoint.zst".to_string(),
            payload_hash: Some("hash".to_string()),
            payload_bytes: Some(10),
            compressed_bytes: Some(8),
            status: CheckpointStatus::Complete,
            error: None,
        }
    }

    fn stored_handoff(
        previous_stream_id: &str,
        previous_entry_id: &str,
        next_stream_id: &str,
        next_entry_id: &str,
    ) -> StoredGenerationHandoff {
        StoredGenerationHandoff {
            previous_stream_id: previous_stream_id.to_string(),
            previous_entry_id: previous_entry_id.to_string(),
            next_stream_id: next_stream_id.to_string(),
            next_entry_id: next_entry_id.to_string(),
        }
    }

    fn stored_handoffs(
        handoffs: impl IntoIterator<Item = StoredGenerationHandoff>,
    ) -> StoredGenerationHandoffs {
        StoredGenerationHandoffs::new(handoffs)
    }

    #[test]
    fn postgres_bigint_conversion_rejects_u64_overflow() -> Result<()> {
        assert_eq!(u64_to_i64("message_seq", i64::MAX as u64)?, i64::MAX);
        let Err(error) = u64_to_i64("message_seq", i64::MAX as u64 + 1) else {
            anyhow::bail!("overflow should fail before binding SQL parameters");
        };

        assert!(error.to_string().contains("message_seq"));
        assert_eq!(optional_u64_to_i64("block_number", None)?, None);

        Ok(())
    }

    #[test]
    fn checkpoint_s3_key_is_searchable_by_chain_block_timestamp_and_stream() {
        assert_eq!(
            super::checkpoint_s3_key("state-history", 8453, 123, 1_720_000_000_000, "stream-1"),
            "state-history/chain=8453/block=123/timestamp=1720000000000/stream=stream-1/checkpoint.zst"
        );
        assert_eq!(
            super::checkpoint_s3_key("/state-history/", 8453, 123, 1, "stream-1"),
            "state-history/chain=8453/block=123/timestamp=1/stream=stream-1/checkpoint.zst"
        );
    }

    fn update_envelope(
        stream_id: &str,
        message_seq: u64,
        backend: BroadcasterBackend,
        cursor: u64,
    ) -> Result<BroadcasterEnvelope> {
        let mut sync_statuses = BTreeMap::new();
        let protocol = match backend {
            BroadcasterBackend::Native => "uniswap_v2",
            BroadcasterBackend::Vm => "vm:curve",
            BroadcasterBackend::Rfq => "rfq:bebop",
        };
        sync_statuses.insert(
            protocol.to_string(),
            BroadcasterProtocolSyncStatus {
                kind: BroadcasterProtocolSyncStatusKind::Started,
                block: None,
                reason: None,
            },
        );
        let update = BroadcasterUpdateMessage::new(vec![BroadcasterUpdatePartition::new(
            backend,
            cursor,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            sync_statuses,
        )])?;
        Ok(BroadcasterEnvelope::new(
            stream_id,
            message_seq,
            simulator_core::broadcaster::BroadcasterPayload::Update(update),
        ))
    }

    fn multi_backend_update_envelope(
        stream_id: &str,
        message_seq: u64,
        partitions: impl IntoIterator<Item = (BroadcasterBackend, u64)>,
    ) -> Result<BroadcasterEnvelope> {
        let update = BroadcasterUpdateMessage::new(
            partitions
                .into_iter()
                .map(|(backend, cursor)| {
                    BroadcasterUpdatePartition::new(
                        backend,
                        cursor,
                        Vec::new(),
                        Vec::new(),
                        Vec::new(),
                        sync_statuses_for_started_protocol(backend),
                    )
                })
                .collect(),
        )?;
        Ok(BroadcasterEnvelope::new(
            stream_id,
            message_seq,
            simulator_core::broadcaster::BroadcasterPayload::Update(update),
        ))
    }

    fn sync_statuses_for_started_protocol(
        backend: BroadcasterBackend,
    ) -> BTreeMap<String, BroadcasterProtocolSyncStatus> {
        let protocol = match backend {
            BroadcasterBackend::Native => "uniswap_v2",
            BroadcasterBackend::Vm => "vm:curve",
            BroadcasterBackend::Rfq => "rfq:bebop",
        };
        BTreeMap::from([(
            protocol.to_string(),
            BroadcasterProtocolSyncStatus {
                kind: BroadcasterProtocolSyncStatusKind::Started,
                block: None,
                reason: None,
            },
        )])
    }

    fn test_state_history_writer(
        sender: tokio::sync::mpsc::Sender<StateHistoryWriteCommand>,
        gap_record_task_limit: usize,
    ) -> Result<StateHistoryWriter> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@127.0.0.1/state_history")?;
        Ok(StateHistoryWriter {
            sender,
            pg_store: StateHistoryPgStore::from_pool(pool),
            status: std::sync::Arc::new(tokio::sync::RwLock::new(StateHistoryWriterSnapshot {
                healthy: true,
                queue_capacity: 1,
                retry_window_ms: 1,
                ..StateHistoryWriterSnapshot::default()
            })),
            persistable_by_stream: std::sync::Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
            gap_record_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
                gap_record_task_limit,
            )),
            shutdown: std::sync::Arc::new(WriterShutdown::default()),
            task: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
        })
    }

    fn sync_statuses_with_block(
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

    fn snapshot_chunk_envelope(
        message_seq: u64,
        chunk_index: u32,
        blocks: Vec<BroadcasterBlockRef>,
    ) -> Result<BroadcasterEnvelope> {
        // Protocol keys are contract-validated against known native protocols.
        let protocols = ["uniswap_v2", "uniswap_v3"];
        let mut sync_statuses = BTreeMap::new();
        for (protocol, block) in protocols.into_iter().zip(blocks) {
            sync_statuses.extend(sync_statuses_with_block(protocol, block));
        }
        let partition = BroadcasterSnapshotPartition::new(
            BroadcasterBackend::Native,
            130,
            Vec::new(),
            sync_statuses,
        );
        let chunk = BroadcasterSnapshotChunk::new("snapshot-1", chunk_index, vec![partition])?;
        // A stream and seq distinct from the archive metadata prove that collected
        // provenance is fixed from the metadata cursor.
        Ok(BroadcasterEnvelope::new(
            "archive-internal",
            message_seq,
            BroadcasterPayload::SnapshotChunk(chunk),
        ))
    }

    fn block_ref(number: u64, seed: u8, timestamp: u64) -> BroadcasterBlockRef {
        BroadcasterBlockRef {
            hash: vec![seed; 32].into(),
            number,
            parent_hash: vec![seed.saturating_add(1); 32].into(),
            revert: false,
            timestamp,
            partial_block_index: None,
        }
    }

    fn raw_protocol_message(
        protocol: &str,
        number: u64,
        seed: u8,
        timestamp: u64,
    ) -> BroadcasterProtocolMessage {
        BroadcasterProtocolMessage::new(
            protocol,
            SynchronizerState::Ready(raw_block_header(number, seed, timestamp)),
            StateSyncMessage {
                header: raw_block_header(number, seed, timestamp),
                snapshots: Snapshot {
                    states: HashMap::new(),
                    vm_storage: HashMap::new(),
                },
                deltas: None,
                removed_components: HashMap::new(),
            },
        )
    }

    fn raw_block_header(number: u64, seed: u8, timestamp: u64) -> BlockHeader {
        BlockHeader {
            hash: Bytes::from(vec![seed; 32]),
            number,
            parent_hash: Bytes::from(vec![seed.saturating_add(1); 32]),
            revert: false,
            timestamp,
            partial_block_index: None,
        }
    }

    #[test]
    fn update_envelope_fixture_is_state_changing() -> Result<()> {
        let envelope = update_envelope("stream-1", 1, BroadcasterBackend::Native, 10)?;
        assert_eq!(envelope.kind(), BroadcasterMessageKind::Update);
        Ok(())
    }
}
