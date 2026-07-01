use std::io::Cursor;

use anyhow::{anyhow, Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Region, primitives::ByteStream, Client as S3Client};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterEnvelope, BroadcasterMessageKind, BroadcasterRedisStreamEntry,
};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

const CHECKPOINT_ARCHIVE_SCHEMA_VERSION: u32 = 1;
const ZSTD_LEVEL: i32 = 3;

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
    pub first_delta_id_after: Option<i64>,
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
    pub first_delta_id_after: Option<i64>,
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
pub struct HistoryRangeRequest {
    pub chain_id: u64,
    pub start_block_number: u64,
    pub end_block_number: u64,
    pub rfq_start_timestamp_ms: Option<u64>,
    pub rfq_end_timestamp_ms: Option<u64>,
    pub backends: Vec<BroadcasterBackend>,
    pub require_checkpoint: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRangePlan {
    pub request: HistoryRangeRequest,
    pub checkpoint: Option<CheckpointManifest>,
    pub replay_from_block_number: u64,
    pub rfq_replay_from_timestamp_ms: Option<u64>,
    pub deltas: Vec<StoredDeltaEntry>,
    pub gaps: Vec<HistoryRangeGap>,
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
    Ok(backends
        .into_iter()
        .map(|backend| CheckpointPayload {
            backend,
            block_number: entry.block_number_for_backend(backend),
            observed_timestamp_ms: entry.observed_timestamp_for_backend(backend),
        })
        .collect())
}

pub fn prepare_delta_message(
    entry: &BroadcasterRedisStreamEntry,
    redis_entry_id: Option<&str>,
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
            require_checkpoint: true,
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

    pub fn without_checkpoint_requirement(mut self) -> Result<Self> {
        self.require_checkpoint = false;
        self.validate_shape()?;
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
            .map(|checkpoint| checkpoint.metadata.block_number.saturating_add(1))
            .unwrap_or(self.start_block_number)
    }

    fn rfq_replay_from_timestamp_ms(&self, checkpoint: Option<&CheckpointManifest>) -> Option<u64> {
        if !self.includes_rfq() {
            return None;
        }
        Some(
            checkpoint
                .map(|checkpoint| {
                    checkpoint
                        .metadata
                        .captured_at_timestamp_ms
                        .saturating_add(1)
                })
                .unwrap_or(self.rfq_start_timestamp_ms.unwrap_or_default()),
        )
    }
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

    pub async fn validate_schema(&self) -> Result<()> {
        for table in [
            "state_history.delta_messages",
            "state_history.delta_backend_index",
            "state_history.checkpoints",
            "state_history.ingestion_gaps",
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

    pub async fn insert_delta(
        &self,
        entry: &BroadcasterRedisStreamEntry,
        redis_entry_id: Option<&str>,
    ) -> Result<PersistedDelta> {
        let prepared = prepare_delta_message(entry, redis_entry_id)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin state history delta transaction")?;
        let id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO state_history.delta_messages (
                chain_id, stream_id, snapshot_id, message_seq, redis_entry_id, kind,
                backend_scope, block_number, observed_timestamp_ms, payload_encoding,
                payload_compressed, payload_hash, runtime_published_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now())
            ON CONFLICT (chain_id, stream_id, message_seq) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(u64_to_i64("chain_id", prepared.chain_id)?)
        .bind(&prepared.stream_id)
        .bind(&prepared.snapshot_id)
        .bind(u64_to_i64("message_seq", prepared.message_seq)?)
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
        .fetch_optional(&mut *tx)
        .await
        .context("failed to insert state history delta")?;

        let (id, inserted) = match id {
            Some(id) => (id, true),
            None => {
                let row = sqlx::query(
                    r#"
                    SELECT id, payload_hash
                    FROM state_history.delta_messages
                    WHERE chain_id = $1 AND stream_id = $2 AND message_seq = $3
                    "#,
                )
                .bind(u64_to_i64("chain_id", prepared.chain_id)?)
                .bind(&prepared.stream_id)
                .bind(u64_to_i64("message_seq", prepared.message_seq)?)
                .fetch_one(&mut *tx)
                .await
                .context("failed to load existing state history delta")?;
                let id: i64 = row.get("id");
                let payload_hash: String = row.get("payload_hash");
                anyhow::ensure!(
                    payload_hash == prepared.payload.hash,
                    "state history delta idempotency conflict for stream {} message_seq {}",
                    prepared.stream_id,
                    prepared.message_seq
                );
                (id, false)
            }
        };

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
            .bind(id)
            .bind(u64_to_i64("chain_id", prepared.chain_id)?)
            .bind(index.backend.as_str())
            .bind(optional_u64_to_i64("block_number", index.block_number)?)
            .bind(optional_u64_to_i64(
                "observed_timestamp_ms",
                index.observed_timestamp_ms,
            )?)
            .bind(u64_to_i64("message_seq", prepared.message_seq)?)
            .execute(&mut *tx)
            .await
            .context("failed to insert state history delta backend index")?;
        }

        tx.commit()
            .await
            .context("failed to commit state history delta transaction")?;
        Ok(PersistedDelta { id, inserted })
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
                chain_id, block_number, captured_at_timestamp_ms, stream_id, source_message_seq,
                backend_scope, s3_bucket, s3_key, payload_encoding, status
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'writing')
            RETURNING id
            "#,
        )
        .bind(u64_to_i64("chain_id", input.metadata.chain_id)?)
        .bind(u64_to_i64("block_number", input.metadata.block_number)?)
        .bind(u64_to_i64(
            "captured_at_timestamp_ms",
            input.metadata.captured_at_timestamp_ms,
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

    pub async fn mark_checkpoint_complete(
        &self,
        checkpoint_id: i64,
        completion: &CheckpointCompletion,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE state_history.checkpoints
            SET status = 'complete',
                payload_hash = $2,
                payload_bytes = $3,
                compressed_bytes = $4,
                first_delta_id_after = $5,
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
        .bind(completion.first_delta_id_after)
        .execute(&self.pool)
        .await
        .context("failed to mark state history checkpoint complete")?;
        Ok(())
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

    pub async fn latest_checkpoint_before(
        &self,
        chain_id: u64,
        block_number: u64,
    ) -> Result<Option<CheckpointManifest>> {
        self.latest_checkpoint_covering_before(chain_id, block_number, &[])
            .await
    }

    pub async fn latest_checkpoint_covering_before(
        &self,
        chain_id: u64,
        block_number: u64,
        backends: &[BroadcasterBackend],
    ) -> Result<Option<CheckpointManifest>> {
        let row = sqlx::query(
            r#"
            SELECT id, chain_id, block_number, captured_at_timestamp_ms, stream_id,
                source_message_seq, backend_scope, s3_bucket, s3_key, payload_hash,
                payload_bytes, compressed_bytes, status, first_delta_id_after, error
            FROM state_history.checkpoints
            WHERE chain_id = $1
                AND block_number <= $2
                AND status = 'complete'
                AND ($3::text[] = '{}'::text[] OR backend_scope @> $3::text[])
            ORDER BY block_number DESC, captured_at_timestamp_ms DESC
            LIMIT 1
            "#,
        )
        .bind(u64_to_i64("chain_id", chain_id)?)
        .bind(u64_to_i64("block_number", block_number)?)
        .bind(backend_scope_strings(backends))
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
        let checkpoint = self
            .latest_checkpoint_covering_before(
                request.chain_id,
                request.start_block_number,
                &request.backends,
            )
            .await?;
        let replay_from_block_number = request.replay_from_block_number(checkpoint.as_ref());
        let rfq_replay_from_timestamp_ms =
            request.rfq_replay_from_timestamp_ms(checkpoint.as_ref());
        let mut gaps = self
            .recorded_gaps_for_range(
                &request,
                replay_from_block_number,
                rfq_replay_from_timestamp_ms,
            )
            .await
            .context("failed to load state history gaps for range")?;
        if request.require_checkpoint && checkpoint.is_none() {
            gaps.push(HistoryRangeGap {
                source: HistoryRangeGapSource::MissingCheckpoint,
                backend_scope: request.backends.clone(),
                from_block_number: Some(request.start_block_number),
                to_block_number: Some(request.end_block_number),
                from_timestamp_ms: request.rfq_start_timestamp_ms,
                to_timestamp_ms: request.rfq_end_timestamp_ms,
                reason: "no complete checkpoint covers the requested range".to_string(),
            });
        }
        let deltas = self
            .deltas_for_range(
                &request,
                checkpoint.as_ref(),
                replay_from_block_number,
                rfq_replay_from_timestamp_ms,
            )
            .await
            .context("failed to load state history deltas for range")?;
        Ok(HistoryRangePlan {
            request,
            checkpoint,
            replay_from_block_number,
            rfq_replay_from_timestamp_ms,
            deltas,
            gaps,
        })
    }

    async fn deltas_for_range(
        &self,
        request: &HistoryRangeRequest,
        checkpoint: Option<&CheckpointManifest>,
        replay_from_block_number: u64,
        rfq_replay_from_timestamp_ms: Option<u64>,
    ) -> Result<Vec<StoredDeltaEntry>> {
        let block_backends = request.block_backends();
        let first_delta_id_after =
            checkpoint.and_then(|checkpoint| checkpoint.first_delta_id_after);
        let rows = sqlx::query(
            r#"
            WITH selected_deltas AS (
                SELECT DISTINCT delta_id
                FROM state_history.delta_backend_index
                WHERE chain_id = $1
                    AND (
                        (
                            backend = ANY($2::text[])
                            AND block_number IS NOT NULL
                            AND block_number >= $3
                            AND block_number <= $4
                        )
                        OR (
                            $5::boolean
                            AND backend = 'rfq'
                            AND observed_timestamp_ms IS NOT NULL
                            AND observed_timestamp_ms >= $6
                            AND observed_timestamp_ms <= $7
                        )
                    )
            )
            SELECT d.id, d.redis_entry_id, d.payload_encoding, d.payload_compressed,
                d.payload_hash
            FROM selected_deltas selected
            JOIN state_history.delta_messages d ON d.id = selected.delta_id
            WHERE ($8::bigint IS NULL OR d.id >= $8)
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
        .bind(first_delta_id_after)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(stored_delta_from_row)
            .collect::<Result<Vec<_>>>()
    }

    async fn recorded_gaps_for_range(
        &self,
        request: &HistoryRangeRequest,
        replay_from_block_number: u64,
        rfq_replay_from_timestamp_ms: Option<u64>,
    ) -> Result<Vec<HistoryRangeGap>> {
        let rows = sqlx::query(
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
        .await?;

        rows.into_iter()
            .map(|row| {
                let backends: Vec<String> = row.get("backend_scope");
                Ok(HistoryRangeGap {
                    source: HistoryRangeGapSource::RecordedGap,
                    backend_scope: parse_backend_strings(&backends)?,
                    from_block_number: optional_i64_to_u64(
                        "from_block_number",
                        row.get("from_block_number"),
                    )?,
                    to_block_number: optional_i64_to_u64(
                        "to_block_number",
                        row.get("to_block_number"),
                    )?,
                    from_timestamp_ms: optional_i64_to_u64(
                        "from_timestamp_ms",
                        row.get("from_timestamp_ms"),
                    )?,
                    to_timestamp_ms: optional_i64_to_u64(
                        "to_timestamp_ms",
                        row.get("to_timestamp_ms"),
                    )?,
                    reason: row.get("reason"),
                })
            })
            .collect::<Result<Vec<_>>>()
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
        first_delta_id_after: row.get("first_delta_id_after"),
        error: row.get("error"),
    })
}

fn parse_backend_strings(values: &[String]) -> Result<Vec<BroadcasterBackend>> {
    let scope = values.join(",");
    parse_backend_scope(&scope)
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

trait EntryCursorExt {
    fn block_number_for_backend(&self, backend: BroadcasterBackend) -> Option<u64>;
    fn observed_timestamp_for_backend(&self, backend: BroadcasterBackend) -> Option<u64>;
}

impl EntryCursorExt for BroadcasterRedisStreamEntry {
    fn block_number_for_backend(&self, backend: BroadcasterBackend) -> Option<u64> {
        matches!(backend, BroadcasterBackend::Native | BroadcasterBackend::Vm)
            .then_some(self.block_number)
            .flatten()
    }

    fn observed_timestamp_for_backend(&self, backend: BroadcasterBackend) -> Option<u64> {
        (backend == BroadcasterBackend::Rfq)
            .then_some(self.observed_timestamp_ms)
            .flatten()
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
    use std::collections::BTreeMap;

    use anyhow::Result;
    use simulator_core::broadcaster::{
        BroadcasterBackend, BroadcasterEnvelope, BroadcasterMessageKind,
        BroadcasterProtocolSyncStatus, BroadcasterProtocolSyncStatusKind,
        BroadcasterRedisStreamEntry, BroadcasterUpdateMessage, BroadcasterUpdatePartition,
    };

    use super::{
        decode_checkpoint_archive_bytes, encode_checkpoint_archive, indexed_backends_for_entry,
        optional_u64_to_i64, prepare_delta_message, u64_to_i64, CheckpointArchive,
        CheckpointArchiveMetadata, CheckpointPayload, DecodedCheckpointArchive, HistoryRangeGap,
        HistoryRangeGapSource, HistoryRangePlan, HistoryRangeRequest,
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
    fn history_range_plan_accepts_gap_free_ranges() -> Result<()> {
        let request = HistoryRangeRequest::new(8453, 100, 200, vec![BroadcasterBackend::Native])?;
        let replay_from_block_number = request.start_block_number;
        let plan = HistoryRangePlan {
            request,
            checkpoint: None,
            replay_from_block_number,
            rfq_replay_from_timestamp_ms: None,
            deltas: Vec::new(),
            gaps: Vec::new(),
        };

        plan.ensure_gap_free()
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

    #[test]
    fn update_envelope_fixture_is_state_changing() -> Result<()> {
        let envelope = update_envelope("stream-1", 1, BroadcasterBackend::Native, 10)?;
        assert_eq!(envelope.kind(), BroadcasterMessageKind::Update);
        Ok(())
    }
}
