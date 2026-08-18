use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use simulator_core::broadcaster::BroadcasterTokenDto;

use crate::reader::{decode_zstd_with_limit, ReadLimitError};

pub const TOKEN_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
const ZSTD_COMPRESSION_LEVEL: i32 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenSnapshot {
    pub chain_id: u64,
    pub tokens: Vec<BroadcasterTokenDto>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RawTokenSnapshot {
    pub schema_version: u32,
    pub chain_id: u64,
    pub token_count: u64,
    pub tokens: Box<RawValue>,
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedRawTokenSnapshot {
    pub snapshot: RawTokenSnapshot,
    pub token_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedTokenSnapshot {
    pub bytes: Vec<u8>,
    pub info: EncodedTokenSnapshotInfo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedTokenSnapshotInfo {
    pub sha256: String,
    pub token_count: u64,
    pub token_bytes: u64,
    pub compressed_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TokenSnapshotCodecError {
    #[error("token snapshot sha256 mismatch: expected {expected}, got {actual}")]
    Sha256Mismatch { expected: String, actual: String },
    #[error("unsupported token snapshot schema version {found}; expected {expected}")]
    UnsupportedSchemaVersion { found: u32, expected: u32 },
    #[error("token snapshot contains duplicate token address {address}")]
    DuplicateTokenAddress { address: String },
}

#[derive(Serialize)]
struct TokenSnapshotEnvelope {
    schema_version: u32,
    chain_id: u64,
    token_count: u64,
    tokens: Vec<BroadcasterTokenDto>,
}

#[derive(Deserialize)]
struct DecodedTokenSnapshotEnvelope {
    schema_version: u32,
    chain_id: u64,
    tokens: Vec<BroadcasterTokenDto>,
}

trait DecodedTokenSnapshot {
    fn schema_version(&self) -> u32;
}

impl DecodedTokenSnapshot for DecodedTokenSnapshotEnvelope {
    fn schema_version(&self) -> u32 {
        self.schema_version
    }
}

impl DecodedTokenSnapshot for RawTokenSnapshot {
    fn schema_version(&self) -> u32 {
        self.schema_version
    }
}

pub fn encode_token_snapshot(snapshot: TokenSnapshot) -> anyhow::Result<EncodedTokenSnapshot> {
    let TokenSnapshot {
        chain_id,
        mut tokens,
    } = snapshot;
    tokens.sort_by(|left, right| left.address.cmp(&right.address));
    // The catalog source is keyed by address, so a duplicate here is a caller bug. Stable sort
    // would order duplicates by input position and break content identity, so fail loud instead
    // of picking a survivor.
    if let Some(pair) = tokens
        .windows(2)
        .find(|pair| pair[0].address == pair[1].address)
    {
        return Err(TokenSnapshotCodecError::DuplicateTokenAddress {
            address: pair[0].address.to_string(),
        }
        .into());
    }
    let token_count =
        u64::try_from(tokens.len()).context("token snapshot token count exceeds u64")?;
    let envelope = TokenSnapshotEnvelope {
        schema_version: TOKEN_SNAPSHOT_SCHEMA_VERSION,
        chain_id,
        token_count,
        tokens,
    };
    let snapshot_json =
        serde_json::to_vec(&envelope).context("failed to serialize token snapshot")?;
    let sha256 = hex::encode(Sha256::digest(&snapshot_json));
    let token_bytes =
        u64::try_from(snapshot_json.len()).context("token snapshot length exceeds u64")?;
    let bytes = zstd::stream::encode_all(snapshot_json.as_slice(), ZSTD_COMPRESSION_LEVEL)
        .context("failed to compress token snapshot")?;
    let compressed_bytes =
        u64::try_from(bytes.len()).context("compressed token snapshot length exceeds u64")?;

    Ok(EncodedTokenSnapshot {
        bytes,
        info: EncodedTokenSnapshotInfo {
            sha256,
            token_count,
            token_bytes,
            compressed_bytes,
        },
    })
}

pub fn decode_token_snapshot(bytes: &[u8], expected_sha256: &str) -> anyhow::Result<TokenSnapshot> {
    let envelope: DecodedTokenSnapshotEnvelope =
        decode_verified_token_snapshot(bytes, expected_sha256)?;

    Ok(TokenSnapshot {
        chain_id: envelope.chain_id,
        tokens: envelope.tokens,
    })
}

/// Decodes a token snapshot while preserving the token array as raw JSON.
///
/// # Errors
///
/// Returns an error when decompression, sha256 verification, envelope parsing,
/// or schema validation fails.
pub fn decode_token_snapshot_raw(
    bytes: &[u8],
    expected_sha256: &str,
) -> anyhow::Result<RawTokenSnapshot> {
    Ok(decode_token_snapshot_raw_with_info(bytes, expected_sha256)?.snapshot)
}

pub(crate) fn decode_token_snapshot_raw_with_info(
    bytes: &[u8],
    expected_sha256: &str,
) -> anyhow::Result<DecodedRawTokenSnapshot> {
    let snapshot_json = decode_verified_token_snapshot_bytes(bytes, expected_sha256)?;
    let snapshot: RawTokenSnapshot =
        serde_json::from_slice(&snapshot_json).context("failed to parse token snapshot")?;
    validate_schema_version(&snapshot)?;

    Ok(DecodedRawTokenSnapshot {
        snapshot,
        token_bytes: u64::try_from(snapshot_json.len())
            .context("token snapshot length exceeds u64")?,
    })
}

pub(crate) fn decode_token_snapshot_raw_with_info_and_limit(
    bytes: &[u8],
    expected_sha256: &str,
    max_decoded_bytes: u64,
) -> Result<DecodedRawTokenSnapshot, ReadLimitError> {
    let snapshot_json =
        decode_verified_token_snapshot_bytes_with_limit(bytes, expected_sha256, max_decoded_bytes)?;
    let snapshot: RawTokenSnapshot =
        serde_json::from_slice(&snapshot_json).context("failed to parse token snapshot")?;
    validate_schema_version(&snapshot)?;

    Ok(DecodedRawTokenSnapshot {
        snapshot,
        token_bytes: u64::try_from(snapshot_json.len())
            .context("token snapshot length exceeds u64")?,
    })
}

fn decode_verified_token_snapshot<T>(bytes: &[u8], expected_sha256: &str) -> anyhow::Result<T>
where
    T: DeserializeOwned + DecodedTokenSnapshot,
{
    let snapshot_json = decode_verified_token_snapshot_bytes(bytes, expected_sha256)?;
    let envelope: T =
        serde_json::from_slice(&snapshot_json).context("failed to parse token snapshot")?;
    decode_token_snapshot_version(envelope)
}

fn decode_verified_token_snapshot_bytes(
    bytes: &[u8],
    expected_sha256: &str,
) -> anyhow::Result<Vec<u8>> {
    let snapshot_json =
        zstd::stream::decode_all(bytes).context("failed to decompress token snapshot")?;
    let actual_sha256 = hex::encode(Sha256::digest(&snapshot_json));
    if actual_sha256 != expected_sha256 {
        return Err(TokenSnapshotCodecError::Sha256Mismatch {
            expected: expected_sha256.to_owned(),
            actual: actual_sha256,
        }
        .into());
    }
    Ok(snapshot_json)
}

fn decode_verified_token_snapshot_bytes_with_limit(
    bytes: &[u8],
    expected_sha256: &str,
    max_decoded_bytes: u64,
) -> Result<Vec<u8>, ReadLimitError> {
    let snapshot_json = decode_zstd_with_limit(bytes, max_decoded_bytes, "token snapshot")?;
    let actual_sha256 = hex::encode(Sha256::digest(&snapshot_json));
    if actual_sha256 != expected_sha256 {
        return Err(ReadLimitError::Read(
            TokenSnapshotCodecError::Sha256Mismatch {
                expected: expected_sha256.to_owned(),
                actual: actual_sha256,
            }
            .into(),
        ));
    }

    Ok(snapshot_json)
}

fn validate_schema_version<T: DecodedTokenSnapshot>(envelope: &T) -> anyhow::Result<()> {
    match envelope.schema_version() {
        TOKEN_SNAPSHOT_SCHEMA_VERSION => Ok(()),
        found => Err(TokenSnapshotCodecError::UnsupportedSchemaVersion {
            found,
            expected: TOKEN_SNAPSHOT_SCHEMA_VERSION,
        }
        .into()),
    }
}

fn decode_token_snapshot_version<T: DecodedTokenSnapshot>(envelope: T) -> anyhow::Result<T> {
    match envelope.schema_version() {
        TOKEN_SNAPSHOT_SCHEMA_VERSION => Ok(envelope),
        found => Err(TokenSnapshotCodecError::UnsupportedSchemaVersion {
            found,
            expected: TOKEN_SNAPSHOT_SCHEMA_VERSION,
        }
        .into()),
    }
}

pub fn token_snapshot_s3_key(prefix: &str, chain_id: u64, sha256: &str) -> String {
    let suffix = format!("chain={chain_id}/tokens/{sha256}.zst");
    if prefix.is_empty() {
        suffix
    } else {
        format!("{prefix}/{suffix}")
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{anyhow, Result};
    use serde_json::json;

    use super::*;

    #[test]
    fn token_snapshot_round_trips() -> Result<()> {
        let snapshot = test_snapshot()?;
        let encoded = encode_token_snapshot(snapshot.clone())?;
        let decoded = decode_token_snapshot(&encoded.bytes, &encoded.info.sha256)?;

        assert_eq!(decoded, snapshot);
        assert_eq!(encoded.info.token_count, 2);
        Ok(())
    }

    #[test]
    fn raw_token_snapshot_round_trips() -> Result<()> {
        let snapshot = test_snapshot()?;
        let encoded = encode_token_snapshot(snapshot)?;
        let decoded = decode_token_snapshot_raw(&encoded.bytes, &encoded.info.sha256)?;
        let tokens: serde_json::Value = serde_json::from_str(decoded.tokens.get())?;

        // The raw token array must be a verbatim slice of the canonical bytes, not a
        // re-serialization: consumers cache by the sha computed over those bytes.
        let canonical = String::from_utf8(zstd::stream::decode_all(encoded.bytes.as_slice())?)?;
        assert!(canonical.contains(decoded.tokens.get()));

        assert_eq!(decoded.schema_version, TOKEN_SNAPSHOT_SCHEMA_VERSION);
        assert_eq!(decoded.chain_id, 8453);
        assert_eq!(decoded.token_count, 2);
        assert_eq!(
            tokens,
            json!([
                {
                    "address": "0x1111111111111111111111111111111111111111",
                    "symbol": "ONE",
                    "decimals": 6,
                    "tax": 7,
                    "gas": [21_000, null],
                    "chainId": 8453,
                    "quality": 80,
                },
                {
                    "address": "0x2222222222222222222222222222222222222222",
                    "symbol": "TWO",
                    "decimals": 18,
                    "tax": 7,
                    "gas": [21_000, null],
                    "chainId": 8453,
                    "quality": 90,
                }
            ])
        );
        Ok(())
    }

    #[test]
    fn token_snapshot_encoding_is_deterministic() -> Result<()> {
        let snapshot = test_snapshot()?;
        let repeated = encode_token_snapshot(snapshot.clone())?;
        let repeated_again = encode_token_snapshot(snapshot.clone())?;

        let mut reversed = snapshot;
        reversed.tokens.reverse();
        let differently_ordered = encode_token_snapshot(reversed)?;

        let canonical = zstd::stream::decode_all(repeated.bytes.as_slice())?;
        let canonical_again = zstd::stream::decode_all(repeated_again.bytes.as_slice())?;
        let differently_ordered_canonical =
            zstd::stream::decode_all(differently_ordered.bytes.as_slice())?;
        let envelope: serde_json::Value = serde_json::from_slice(&canonical)?;

        assert_eq!(canonical, canonical_again);
        assert_eq!(canonical, differently_ordered_canonical);
        assert_eq!(repeated.bytes, repeated_again.bytes);
        assert_eq!(repeated.bytes, differently_ordered.bytes);
        assert_eq!(repeated.info.sha256, repeated_again.info.sha256);
        assert_eq!(repeated.info.sha256, differently_ordered.info.sha256);
        assert_eq!(envelope["schema_version"], TOKEN_SNAPSHOT_SCHEMA_VERSION);
        assert_eq!(envelope["chain_id"], 8453);
        assert_eq!(envelope["token_count"], 2);
        assert_eq!(
            envelope["tokens"][0],
            json!({
                "address": "0x1111111111111111111111111111111111111111",
                "symbol": "ONE",
                "decimals": 6,
                "tax": 7,
                "gas": [21_000, null],
                "chainId": 8453,
                "quality": 80,
            })
        );
        Ok(())
    }

    #[test]
    fn distinct_token_snapshots_have_distinct_hashes_and_keys() -> Result<()> {
        let first = encode_token_snapshot(test_snapshot()?)?;
        let mut changed = test_snapshot()?;
        changed.tokens[0].quality += 1;
        let second = encode_token_snapshot(changed)?;

        assert_ne!(first.info.sha256, second.info.sha256);
        assert_ne!(
            token_snapshot_s3_key("state-history", 8453, &first.info.sha256),
            token_snapshot_s3_key("state-history", 8453, &second.info.sha256)
        );
        assert_eq!(
            token_snapshot_s3_key("state-history", 8453, &first.info.sha256),
            format!("state-history/chain=8453/tokens/{}.zst", first.info.sha256)
        );
        assert_eq!(
            token_snapshot_s3_key("", 8453, &first.info.sha256),
            format!("chain=8453/tokens/{}.zst", first.info.sha256)
        );
        Ok(())
    }

    #[test]
    fn schema_version_mismatch_is_typed() -> Result<()> {
        let encoded = encode_token_snapshot(test_snapshot()?)?;
        let canonical = zstd::stream::decode_all(encoded.bytes.as_slice())?;
        let mut envelope: serde_json::Value = serde_json::from_slice(&canonical)?;
        envelope["schema_version"] = json!(TOKEN_SNAPSHOT_SCHEMA_VERSION + 1);
        let changed_canonical = serde_json::to_vec(&envelope)?;
        let changed_sha256 = hex::encode(Sha256::digest(&changed_canonical));
        let changed_bytes =
            zstd::stream::encode_all(changed_canonical.as_slice(), ZSTD_COMPRESSION_LEVEL)?;

        let error = decode_token_snapshot(&changed_bytes, &changed_sha256)
            .err()
            .ok_or_else(|| anyhow!("schema mismatch should fail"))?;
        assert_eq!(
            error.downcast_ref::<TokenSnapshotCodecError>(),
            Some(&TokenSnapshotCodecError::UnsupportedSchemaVersion {
                found: TOKEN_SNAPSHOT_SCHEMA_VERSION + 1,
                expected: TOKEN_SNAPSHOT_SCHEMA_VERSION,
            })
        );
        Ok(())
    }

    #[test]
    fn raw_schema_version_mismatch_is_typed() -> Result<()> {
        let encoded = encode_token_snapshot(test_snapshot()?)?;
        let canonical = zstd::stream::decode_all(encoded.bytes.as_slice())?;
        let mut envelope: serde_json::Value = serde_json::from_slice(&canonical)?;
        envelope["schema_version"] = json!(TOKEN_SNAPSHOT_SCHEMA_VERSION + 1);
        let changed_canonical = serde_json::to_vec(&envelope)?;
        let changed_sha256 = hex::encode(Sha256::digest(&changed_canonical));
        let changed_bytes =
            zstd::stream::encode_all(changed_canonical.as_slice(), ZSTD_COMPRESSION_LEVEL)?;

        let error = decode_token_snapshot_raw(&changed_bytes, &changed_sha256)
            .err()
            .ok_or_else(|| anyhow!("schema mismatch should fail"))?;
        assert_eq!(
            error.downcast_ref::<TokenSnapshotCodecError>(),
            Some(&TokenSnapshotCodecError::UnsupportedSchemaVersion {
                found: TOKEN_SNAPSHOT_SCHEMA_VERSION + 1,
                expected: TOKEN_SNAPSHOT_SCHEMA_VERSION,
            })
        );
        Ok(())
    }

    #[test]
    fn duplicate_token_address_is_typed() -> Result<()> {
        let mut snapshot = test_snapshot()?;
        let mut duplicate = snapshot.tokens[0].clone();
        duplicate.quality += 1;
        snapshot.tokens.push(duplicate);

        let error = encode_token_snapshot(snapshot)
            .err()
            .ok_or_else(|| anyhow!("duplicate address should fail"))?;
        assert!(matches!(
            error.downcast_ref::<TokenSnapshotCodecError>(),
            Some(TokenSnapshotCodecError::DuplicateTokenAddress { address })
                if address == "0x1111111111111111111111111111111111111111"
        ));
        Ok(())
    }

    #[test]
    fn sha256_mismatch_is_typed() -> Result<()> {
        let encoded = encode_token_snapshot(test_snapshot()?)?;

        let error = decode_token_snapshot(&encoded.bytes, "deadbeef")
            .err()
            .ok_or_else(|| anyhow!("sha256 mismatch should fail"))?;
        assert!(matches!(
            error.downcast_ref::<TokenSnapshotCodecError>(),
            Some(TokenSnapshotCodecError::Sha256Mismatch { expected, .. })
                if expected == "deadbeef"
        ));
        Ok(())
    }

    #[test]
    fn raw_sha256_mismatch_is_typed() -> Result<()> {
        let encoded = encode_token_snapshot(test_snapshot()?)?;

        let error = decode_token_snapshot_raw(&encoded.bytes, "deadbeef")
            .err()
            .ok_or_else(|| anyhow!("sha256 mismatch should fail"))?;
        assert!(matches!(
            error.downcast_ref::<TokenSnapshotCodecError>(),
            Some(TokenSnapshotCodecError::Sha256Mismatch { expected, .. })
                if expected == "deadbeef"
        ));
        Ok(())
    }

    #[test]
    fn raw_corrupt_bytes_return_decompression_error() -> Result<()> {
        let error = decode_token_snapshot_raw(b"not a zstd object", "unused")
            .err()
            .ok_or_else(|| anyhow!("corrupt token snapshot should fail"))?;

        assert!(error
            .to_string()
            .contains("failed to decompress token snapshot"));
        assert!(error.downcast_ref::<std::io::Error>().is_some());
        Ok(())
    }

    fn test_snapshot() -> Result<TokenSnapshot> {
        Ok(TokenSnapshot {
            chain_id: 8453,
            tokens: vec![
                token("0x1111111111111111111111111111111111111111", "ONE", 6, 80)?,
                token("0x2222222222222222222222222222222222222222", "TWO", 18, 90)?,
            ],
        })
    }

    fn token(
        address: &str,
        symbol: &str,
        decimals: u32,
        quality: u32,
    ) -> Result<BroadcasterTokenDto> {
        Ok(BroadcasterTokenDto {
            address: serde_json::from_value(json!(address))?,
            symbol: symbol.to_owned(),
            decimals,
            tax: 7,
            gas: vec![Some(21_000), None],
            chain_id: 8453,
            quality,
        })
    }
}
