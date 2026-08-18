use std::io::Read;

use anyhow::Context;
use simulator_core::broadcaster::BroadcasterTokenDto;

use super::{ReadConnectionProvider, StateHistoryReader};
use crate::{
    archive::decode_archive_with_info_and_limit,
    tokens::decode_token_snapshot_raw_with_info_and_limit, CheckpointArchive, CheckpointManifest,
    CheckpointStatus, RawTokenSnapshot, TokenSnapshotRef,
};

macro_rules! read_ensure {
    ($condition:expr, $($argument:tt)*) => {
        if !$condition {
            return Err(ReadLimitError::Read(anyhow::anyhow!($($argument)*)));
        }
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadLimits {
    pub max_compressed_bytes: u64,
    pub max_decoded_bytes: u64,
}

impl ReadLimits {
    pub fn new(max_compressed_bytes: u64, max_decoded_bytes: u64) -> Result<Self, ReadLimitError> {
        if max_compressed_bytes == 0 || max_decoded_bytes == 0 {
            return Err(ReadLimitError::ZeroLimit);
        }
        Ok(Self {
            max_compressed_bytes,
            max_decoded_bytes,
        })
    }

    pub const fn unbounded() -> Self {
        Self {
            max_compressed_bytes: u64::MAX,
            max_decoded_bytes: u64::MAX,
        }
    }

    pub(crate) fn check_compressed(self, declared: u64) -> Result<(), ReadLimitError> {
        if declared > self.max_compressed_bytes {
            return Err(ReadLimitError::CompressedBytesExceeded {
                declared,
                limit: self.max_compressed_bytes,
            });
        }
        Ok(())
    }

    pub(crate) fn check_decoded(self, declared: u64) -> Result<(), ReadLimitError> {
        if declared > self.max_decoded_bytes {
            return Err(ReadLimitError::DecodedBytesExceeded {
                declared,
                limit: self.max_decoded_bytes,
            });
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadLimitError {
    #[error("read limits must be positive")]
    ZeroLimit,
    #[error("compressed bytes {declared} exceed limit {limit}")]
    CompressedBytesExceeded { declared: u64, limit: u64 },
    #[error("decoded bytes {declared} exceed limit {limit}")]
    DecodedBytesExceeded { declared: u64, limit: u64 },
    #[error(transparent)]
    Read(#[from] anyhow::Error),
}

pub(crate) fn decode_zstd_with_limit(
    compressed: &[u8],
    max_decoded_bytes: u64,
    object_name: &'static str,
) -> Result<Vec<u8>, ReadLimitError> {
    let decoder = zstd::stream::read::Decoder::new(compressed)
        .with_context(|| format!("failed to create {object_name} decoder"))?;
    let read_limit = max_decoded_bytes.saturating_add(1);
    let mut limited = decoder.take(read_limit);
    let mut decoded = Vec::new();
    limited
        .read_to_end(&mut decoded)
        .with_context(|| format!("failed to decompress {object_name}"))?;
    let decoded_bytes = u64::try_from(decoded.len()).context("decoded byte length exceeds u64")?;
    if decoded_bytes > max_decoded_bytes {
        return Err(ReadLimitError::DecodedBytesExceeded {
            declared: decoded_bytes,
            limit: max_decoded_bytes,
        });
    }
    Ok(decoded)
}

impl<P: ReadConnectionProvider> StateHistoryReader<P> {
    pub async fn fetch_checkpoint_with_limit(
        &self,
        manifest: &CheckpointManifest,
        limits: ReadLimits,
    ) -> Result<CheckpointArchive, ReadLimitError> {
        read_ensure!(
            manifest.status == CheckpointStatus::Complete,
            "checkpoint manifest {} is not complete",
            manifest.id
        );
        let key = manifest
            .s3_key
            .as_deref()
            .context("complete checkpoint manifest is missing its S3 key")?;
        let expected_sha256 = manifest
            .archive_sha256
            .as_deref()
            .context("complete checkpoint manifest is missing its archive sha256")?;
        let expected_archive_bytes = manifest
            .archive_bytes
            .context("complete checkpoint manifest is missing its archive byte length")?;
        let expected_compressed_bytes = manifest
            .compressed_bytes
            .context("complete checkpoint manifest is missing its compressed byte length")?;
        limits.check_compressed(expected_compressed_bytes)?;
        limits.check_decoded(expected_archive_bytes)?;

        let expected_key =
            self.objects
                .key_for(manifest.chain_id, manifest.position, manifest.kind);
        read_ensure!(
            key == expected_key,
            "checkpoint manifest {} object key mismatch: expected {expected_key}, got {key}",
            manifest.id
        );
        let bytes = self
            .objects
            .fetch_with_limit(key, limits.max_compressed_bytes)
            .await?;
        let decoded =
            decode_archive_with_info_and_limit(&bytes, expected_sha256, limits.max_decoded_bytes)?;
        read_ensure!(
            decoded.archive_bytes == expected_archive_bytes,
            "checkpoint manifest {} archive length mismatch: expected {expected_archive_bytes}, got {}",
            manifest.id,
            decoded.archive_bytes
        );
        read_ensure!(
            decoded.compressed_bytes == expected_compressed_bytes,
            "checkpoint manifest {} compressed length mismatch: expected {expected_compressed_bytes}, got {}",
            manifest.id,
            decoded.compressed_bytes
        );
        let metadata = &decoded.archive.metadata;
        read_ensure!(
            metadata.chain_id == manifest.chain_id
                && metadata.position == manifest.position
                && metadata.state_version == manifest.state_version
                && metadata.kind == manifest.kind
                && metadata.block_number == manifest.block_number
                && metadata.rfq_observed_at_ms == manifest.rfq_observed_at_ms
                && metadata.backends == manifest.backends,
            "checkpoint manifest {} metadata does not match its archive",
            manifest.id
        );
        read_ensure!(
            !decoded.archive.payloads_json.is_empty(),
            "checkpoint manifest {} archive contains no payloads",
            manifest.id
        );
        Ok(decoded.archive)
    }

    pub async fn fetch_checkpoint_token_snapshot_with_limit(
        &self,
        manifest: &CheckpointManifest,
        limits: ReadLimits,
    ) -> Result<Option<RawTokenSnapshot>, ReadLimitError> {
        read_ensure!(
            manifest.status == CheckpointStatus::Complete,
            "checkpoint manifest {} is not complete",
            manifest.id
        );
        let Some(reference) = manifest.token_reference.as_ref() else {
            return Ok(None);
        };
        let expected_key = self
            .objects
            .token_key_for(manifest.chain_id, &reference.sha256);
        read_ensure!(
            reference.s3_key == expected_key,
            "checkpoint manifest {} token object key mismatch: expected {expected_key}, got {}",
            manifest.id,
            reference.s3_key
        );
        let snapshot = self
            .fetch_token_snapshot_with_limit(reference, limits)
            .await?;
        read_ensure!(
            snapshot.chain_id == manifest.chain_id,
            "checkpoint manifest {} token chain mismatch: expected {}, got {}",
            manifest.id,
            manifest.chain_id,
            snapshot.chain_id
        );
        Ok(Some(snapshot))
    }

    pub async fn fetch_token_snapshot_with_limit(
        &self,
        reference: &TokenSnapshotRef,
        limits: ReadLimits,
    ) -> Result<RawTokenSnapshot, ReadLimitError> {
        limits.check_decoded(reference.token_bytes)?;
        let bytes = self
            .objects
            .fetch_with_limit(&reference.s3_key, limits.max_compressed_bytes)
            .await
            .with_context(|| format!("failed to fetch token snapshot {}", reference.s3_key))?;
        let decoded = decode_token_snapshot_raw_with_info_and_limit(
            &bytes,
            &reference.sha256,
            limits.max_decoded_bytes,
        )?;
        let expected_key = self
            .objects
            .token_key_for(decoded.snapshot.chain_id, &reference.sha256);
        read_ensure!(
            reference.s3_key == expected_key,
            "token snapshot object key mismatch: expected {expected_key}, got {}",
            reference.s3_key
        );
        read_ensure!(
            decoded.token_bytes == reference.token_bytes,
            "token snapshot byte length mismatch: expected {}, got {}",
            reference.token_bytes,
            decoded.token_bytes
        );
        read_ensure!(
            decoded.snapshot.token_count == reference.token_count,
            "token snapshot count mismatch: expected {}, got {}",
            reference.token_count,
            decoded.snapshot.token_count
        );
        let tokens: Vec<BroadcasterTokenDto> = serde_json::from_str(decoded.snapshot.tokens.get())
            .context("failed to parse token snapshot token array")?;
        let actual_token_count =
            u64::try_from(tokens.len()).context("token snapshot token count exceeds u64")?;
        read_ensure!(
            actual_token_count == decoded.snapshot.token_count,
            "token snapshot envelope count mismatch: expected {}, got {actual_token_count}",
            decoded.snapshot.token_count
        );
        read_ensure!(
            tokens
                .iter()
                .all(|token| token.chain_id == decoded.snapshot.chain_id),
            "token snapshot contains a token from a different chain"
        );
        read_ensure!(
            tokens
                .windows(2)
                .all(|pair| pair[0].address < pair[1].address),
            "token snapshot addresses are not strictly sorted and unique"
        );
        Ok(decoded.snapshot)
    }
}
