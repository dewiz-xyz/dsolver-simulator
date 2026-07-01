use std::io::Cursor;

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterEnvelope, BroadcasterRedisStreamEntry,
};

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
        encode_checkpoint_archive, indexed_backends_for_entry, CheckpointArchive,
        CheckpointArchiveMetadata, CheckpointPayload, DecodedCheckpointArchive,
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
