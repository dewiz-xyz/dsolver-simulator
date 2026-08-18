use anyhow::{ensure, Context};
use aws_sdk_s3::primitives::ByteStream;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

use crate::reader::{decode_zstd_with_limit, ReadLimitError};
use crate::{Backend, CheckpointKind, EncodedArchiveInfo, StreamPosition};

pub const ARCHIVE_SCHEMA_VERSION: u32 = 1;
const ZSTD_COMPRESSION_LEVEL: i32 = 3;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArchiveCodecError {
    #[error("unsupported checkpoint archive schema version {0}")]
    UnsupportedSchemaVersion(u32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointArchive {
    pub metadata: ArchiveMetadata,
    pub payloads_json: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveMetadata {
    pub schema_version: u32,
    pub chain_id: u64,
    pub position: StreamPosition,
    pub state_version: Option<u64>,
    pub kind: CheckpointKind,
    pub block_number: u64,
    pub rfq_observed_at_ms: Option<u64>,
    pub backends: Vec<Backend>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedArchive {
    pub bytes: Vec<u8>,
    pub info: EncodedArchiveInfo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedArchive {
    pub archive: CheckpointArchive,
    pub archive_bytes: u64,
    pub compressed_bytes: u64,
}

#[derive(Serialize)]
struct ArchiveEnvelope {
    schema_version: u32,
    metadata: ArchiveMetadata,
    payloads: Vec<Box<RawValue>>,
}

#[derive(Deserialize)]
struct DecodedArchiveEnvelope {
    schema_version: u32,
    metadata: ArchiveMetadata,
    payloads: Vec<Box<RawValue>>,
}

pub fn encode_archive(archive: CheckpointArchive) -> anyhow::Result<EncodedArchive> {
    ensure!(
        archive.metadata.schema_version == ARCHIVE_SCHEMA_VERSION,
        "unsupported checkpoint archive schema version {}; expected {ARCHIVE_SCHEMA_VERSION}",
        archive.metadata.schema_version
    );

    let payloads = archive
        .payloads_json
        .into_iter()
        .map(RawValue::from_string)
        .collect::<serde_json::Result<Vec<_>>>()
        .context("checkpoint archive contains invalid JSON payload")?;
    let envelope = ArchiveEnvelope {
        schema_version: archive.metadata.schema_version,
        metadata: archive.metadata,
        payloads,
    };
    let archive_json =
        serde_json::to_vec(&envelope).context("failed to serialize checkpoint archive")?;
    let sha256 = hex::encode(Sha256::digest(&archive_json));
    let archive_bytes =
        u64::try_from(archive_json.len()).context("checkpoint archive length exceeds u64")?;
    let bytes = zstd::stream::encode_all(archive_json.as_slice(), ZSTD_COMPRESSION_LEVEL)
        .context("failed to compress checkpoint archive")?;
    let compressed_bytes =
        u64::try_from(bytes.len()).context("compressed checkpoint archive length exceeds u64")?;

    Ok(EncodedArchive {
        bytes,
        info: EncodedArchiveInfo {
            sha256,
            archive_bytes,
            compressed_bytes,
        },
    })
}

pub fn decode_archive(bytes: &[u8], expected_sha256: &str) -> anyhow::Result<CheckpointArchive> {
    Ok(decode_archive_with_info(bytes, expected_sha256)?.archive)
}

pub(crate) fn decode_archive_with_info(
    bytes: &[u8],
    expected_sha256: &str,
) -> anyhow::Result<DecodedArchive> {
    let archive_json =
        zstd::stream::decode_all(bytes).context("failed to decompress checkpoint archive")?;
    let actual_sha256 = hex::encode(Sha256::digest(&archive_json));
    ensure!(
        actual_sha256 == expected_sha256,
        "checkpoint archive sha256 mismatch: expected {expected_sha256}, got {actual_sha256}"
    );
    let envelope: DecodedArchiveEnvelope =
        serde_json::from_slice(&archive_json).context("failed to parse checkpoint archive")?;
    match envelope.schema_version {
        ARCHIVE_SCHEMA_VERSION => {
            decode_archive_v1(envelope, archive_json, bytes).map_err(anyhow::Error::from)
        }
        other => Err(ArchiveCodecError::UnsupportedSchemaVersion(other).into()),
    }
}

pub(crate) fn decode_archive_with_info_and_limit(
    bytes: &[u8],
    expected_sha256: &str,
    max_decoded_bytes: u64,
) -> Result<DecodedArchive, ReadLimitError> {
    let archive_json = decode_zstd_with_limit(bytes, max_decoded_bytes, "checkpoint archive")?;
    let actual_sha256 = hex::encode(Sha256::digest(&archive_json));
    if actual_sha256 != expected_sha256 {
        return Err(ReadLimitError::Read(anyhow::anyhow!(
            "checkpoint archive sha256 mismatch: expected {expected_sha256}, got {actual_sha256}"
        )));
    }

    let envelope: DecodedArchiveEnvelope =
        serde_json::from_slice(&archive_json).context("failed to parse checkpoint archive")?;
    match envelope.schema_version {
        ARCHIVE_SCHEMA_VERSION => decode_archive_v1(envelope, archive_json, bytes),
        other => Err(ReadLimitError::Read(
            ArchiveCodecError::UnsupportedSchemaVersion(other).into(),
        )),
    }
}

fn decode_archive_v1(
    envelope: DecodedArchiveEnvelope,
    archive_json: Vec<u8>,
    compressed: &[u8],
) -> Result<DecodedArchive, ReadLimitError> {
    if envelope.metadata.schema_version != envelope.schema_version {
        return Err(ReadLimitError::Read(anyhow::anyhow!(
            "checkpoint archive metadata schema version {} does not match envelope version {}",
            envelope.metadata.schema_version,
            envelope.schema_version
        )));
    }

    Ok(DecodedArchive {
        archive_bytes: u64::try_from(archive_json.len())
            .context("checkpoint archive length exceeds u64")?,
        compressed_bytes: u64::try_from(compressed.len())
            .context("compressed checkpoint archive length exceeds u64")?,
        archive: CheckpointArchive {
            metadata: envelope.metadata,
            payloads_json: envelope
                .payloads
                .into_iter()
                .map(|payload| payload.get().to_owned())
                .collect(),
        },
    })
}

pub fn checkpoint_s3_key(
    prefix: &str,
    chain_id: u64,
    position: StreamPosition,
    kind: CheckpointKind,
) -> String {
    let suffix = format!(
        "chain={chain_id}/gen={}/seq={}/kind={}/checkpoint.zst",
        position.generation,
        position.message_seq,
        kind.as_str()
    );
    if prefix.is_empty() {
        suffix
    } else {
        format!("{prefix}/{suffix}")
    }
}

pub struct CheckpointObjectStore {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}

impl CheckpointObjectStore {
    pub async fn from_env_config(
        bucket: String,
        prefix: String,
        region: String,
        endpoint_url: Option<String>,
        force_path_style: bool,
    ) -> Self {
        let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region))
            .load()
            .await;
        let mut s3_config =
            aws_sdk_s3::config::Builder::from(&shared_config).force_path_style(force_path_style);
        if let Some(endpoint_url) = endpoint_url {
            s3_config = s3_config.endpoint_url(endpoint_url);
        }

        Self {
            client: aws_sdk_s3::Client::from_conf(s3_config.build()),
            bucket,
            prefix,
        }
    }

    pub fn key_for(&self, chain_id: u64, position: StreamPosition, kind: CheckpointKind) -> String {
        checkpoint_s3_key(&self.prefix, chain_id, position, kind)
    }

    pub fn token_key_for(&self, chain_id: u64, sha256: &str) -> String {
        crate::token_snapshot_s3_key(&self.prefix, chain_id, sha256)
    }

    pub async fn put(&self, key: &str, bytes: Vec<u8>) -> anyhow::Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes))
            .send()
            .await
            .with_context(|| {
                format!("failed to put checkpoint object s3://{}/{key}", self.bucket)
            })?;
        Ok(())
    }

    pub async fn fetch(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        self.fetch_with_limit(key, u64::MAX)
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn fetch_with_limit(
        &self,
        key: &str,
        max_compressed_bytes: u64,
    ) -> Result<Vec<u8>, ReadLimitError> {
        use tokio::io::AsyncReadExt;

        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| {
                format!(
                    "failed to fetch checkpoint object s3://{}/{key}",
                    self.bucket
                )
            })
            .map_err(ReadLimitError::ObjectRead)?;
        if let Some(content_length) = output.content_length() {
            let content_length = u64::try_from(content_length)
                .context("checkpoint object content length is negative")
                .map_err(ReadLimitError::ObjectRead)?;
            if content_length > max_compressed_bytes {
                return Err(ReadLimitError::CompressedBytesExceeded {
                    declared: content_length,
                    limit: max_compressed_bytes,
                });
            }
        }
        let mut reader = output
            .body
            .into_async_read()
            .take(max_compressed_bytes.saturating_add(1));
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .await
            .with_context(|| {
                format!(
                    "failed to read checkpoint object s3://{}/{key}",
                    self.bucket
                )
            })
            .map_err(ReadLimitError::ObjectRead)?;
        let compressed_bytes = u64::try_from(bytes.len())
            .context("checkpoint object length exceeds u64")
            .map_err(ReadLimitError::ObjectRead)?;
        if compressed_bytes > max_compressed_bytes {
            return Err(ReadLimitError::CompressedBytesExceeded {
                declared: compressed_bytes,
                limit: max_compressed_bytes,
            });
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[expect(clippy::unwrap_used)]
    #[test]
    fn archive_round_trips_and_verifies_hash() {
        let archive = CheckpointArchive {
            metadata: test_metadata(),
            payloads_json: vec![r#"{"k":1}"#.into()],
        };
        let encoded = encode_archive(archive.clone()).unwrap();
        let decoded = decode_archive(&encoded.bytes, &encoded.info.sha256).unwrap();
        assert_eq!(decoded.payloads_json, archive.payloads_json);
        assert!(decode_archive(&encoded.bytes, "deadbeef").is_err());
    }

    #[expect(
        clippy::expect_used,
        clippy::unwrap_used,
        reason = "the generated unknown-version fixture must stay valid"
    )]
    #[test]
    fn unknown_archive_schema_version_fails_closed() {
        let archive = CheckpointArchive {
            metadata: test_metadata(),
            payloads_json: vec![r#"{"k":1}"#.into()],
        };
        let encoded = encode_archive(archive).unwrap();
        let json = zstd::stream::decode_all(encoded.bytes.as_slice()).unwrap();
        let mut envelope: serde_json::Value = serde_json::from_slice(&json).unwrap();
        envelope["schema_version"] = serde_json::json!(99);
        envelope["metadata"]["schema_version"] = serde_json::json!(99);
        let json = serde_json::to_vec(&envelope).unwrap();
        let sha256 = hex::encode(Sha256::digest(&json));
        let bytes = zstd::stream::encode_all(json.as_slice(), ZSTD_COMPRESSION_LEVEL).unwrap();

        let error = decode_archive(&bytes, &sha256)
            .expect_err("unknown checkpoint archive schemas must fail closed");

        assert!(error
            .to_string()
            .contains("unsupported checkpoint archive schema version 99"));
    }

    #[test]
    fn s3_key_shape_matches_contract() {
        let pos = StreamPosition {
            generation: 7,
            message_seq: 4242,
        };
        assert_eq!(
            checkpoint_s3_key("state-history", 8453, pos, CheckpointKind::Boundary),
            "state-history/chain=8453/gen=7/seq=4242/kind=boundary/checkpoint.zst"
        );
        assert_eq!(
            checkpoint_s3_key("", 8453, pos, CheckpointKind::Interval),
            "chain=8453/gen=7/seq=4242/kind=interval/checkpoint.zst"
        );
    }

    #[expect(clippy::unwrap_used)]
    #[tokio::test]
    #[ignore] // needs MinIO from docker-compose.state-history.yml
    async fn minio_put_fetch_round_trip() {
        let store = test_minio_store().await; // endpoint http://127.0.0.1:59000, force_path_style
        store
            .put("test/checkpoint.zst", b"payload".to_vec())
            .await
            .unwrap();
        assert_eq!(
            store.fetch("test/checkpoint.zst").await.unwrap(),
            b"payload"
        );
        assert!(store.fetch("test/missing.zst").await.is_err());
    }

    fn test_metadata() -> ArchiveMetadata {
        ArchiveMetadata {
            schema_version: ARCHIVE_SCHEMA_VERSION,
            chain_id: 8453,
            position: StreamPosition {
                generation: 7,
                message_seq: 4242,
            },
            state_version: Some(100),
            kind: CheckpointKind::Interval,
            block_number: 20_000_000,
            rfq_observed_at_ms: None,
            backends: vec![Backend::Native, Backend::Vm],
        }
    }

    async fn test_minio_store() -> CheckpointObjectStore {
        const TEST_BUCKET: &str = "state-history-test";

        std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
        std::env::set_var("AWS_REGION", "eu-central-1");

        let store = CheckpointObjectStore::from_env_config(
            TEST_BUCKET.to_owned(),
            String::new(),
            "eu-central-1".to_owned(),
            Some("http://127.0.0.1:59000".to_owned()),
            true,
        )
        .await;
        if let Err(error) = store
            .client
            .create_bucket()
            .bucket(TEST_BUCKET)
            .send()
            .await
        {
            let already_exists = error.as_service_error().is_some_and(|service_error| {
                service_error.is_bucket_already_exists()
                    || service_error.is_bucket_already_owned_by_you()
            });
            assert!(
                already_exists,
                "failed to create MinIO test bucket: {error}"
            );
        }

        store
    }
}
