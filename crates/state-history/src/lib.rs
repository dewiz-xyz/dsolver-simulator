use std::io::Cursor;

use anyhow::{anyhow, Context, Result};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointArchiveWire {
    schema_version: u32,
    metadata: CheckpointArchiveMetadata,
    payloads: Vec<BroadcasterEnvelope>,
}

impl EncodedCheckpointArchive {
    pub fn decode(&self) -> Result<DecodedCheckpointArchive> {
        let uncompressed = zstd::stream::decode_all(Cursor::new(&self.bytes))
            .context("failed to decompress checkpoint archive")?;
        let hash = sha256_hex(&uncompressed);
        anyhow::ensure!(
            hash == self.payload.hash,
            "checkpoint archive hash mismatch: expected {}, decoded {}",
            self.payload.hash,
            hash
        );
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
                compressed_bytes: self.bytes.len(),
            },
        })
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
        let row = sqlx::query(
            r#"
            SELECT id, chain_id, block_number, captured_at_timestamp_ms, stream_id,
                source_message_seq, backend_scope, s3_bucket, s3_key, payload_hash,
                payload_bytes, compressed_bytes, status, first_delta_id_after, error
            FROM state_history.checkpoints
            WHERE chain_id = $1 AND block_number <= $2 AND status = 'complete'
            ORDER BY block_number DESC, captured_at_timestamp_ms DESC
            LIMIT 1
            "#,
        )
        .bind(u64_to_i64("chain_id", chain_id)?)
        .bind(u64_to_i64("block_number", block_number)?)
        .fetch_optional(&self.pool)
        .await
        .context("failed to resolve latest state history checkpoint")?;

        row.map(checkpoint_manifest_from_row).transpose()
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
        encode_checkpoint_archive, indexed_backends_for_entry, optional_u64_to_i64,
        prepare_delta_message, u64_to_i64, CheckpointArchive, CheckpointArchiveMetadata,
        CheckpointPayload, DecodedCheckpointArchive,
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
