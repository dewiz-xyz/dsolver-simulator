use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use utoipa::ToSchema;
use uuid::Uuid;

use super::{
    deserialize_uuid_v4, validate_api_revision, validate_backends, validate_chain_id,
    validate_timeout, Backend, StreamPosition,
};

/// Request for the raw snapshot the broadcaster served at a stream position,
/// for backends kept as raw Tycho messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(
    rename_all = "camelCase",
    deny_unknown_fields,
    try_from = "UncheckedRawSnapshotRequest"
)]
pub struct RawSnapshotRequest {
    #[serde(deserialize_with = "deserialize_uuid_v4")]
    #[schema(schema_with = crate::api::common::uuid_v4_schema)]
    pub request_id: Uuid,
    #[schema(minimum = 1, maximum = 1)]
    pub api_revision: u32,
    #[schema(minimum = 1)]
    pub timeout_ms: u64,
    #[schema(minimum = 8453, maximum = 8453)]
    pub chain_id: u64,
    #[schema(schema_with = crate::api::common::unique_backends_schema)]
    pub backends: Vec<Backend>,
    pub position: StreamPosition,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UncheckedRawSnapshotRequest {
    #[serde(deserialize_with = "deserialize_uuid_v4")]
    request_id: Uuid,
    api_revision: u32,
    timeout_ms: u64,
    chain_id: u64,
    backends: Vec<Backend>,
    position: StreamPosition,
}

impl TryFrom<UncheckedRawSnapshotRequest> for RawSnapshotRequest {
    type Error = String;

    fn try_from(value: UncheckedRawSnapshotRequest) -> Result<Self, Self::Error> {
        validate_api_revision(value.api_revision)?;
        validate_timeout(value.timeout_ms)?;
        validate_chain_id(value.chain_id)?;
        validate_raw_backends(&value.backends)?;
        Ok(Self {
            request_id: value.request_id,
            api_revision: value.api_revision,
            timeout_ms: value.timeout_ms,
            chain_id: value.chain_id,
            backends: value.backends,
            position: value.position,
        })
    }
}

/// Request for the stored messages after one stream position through another,
/// as the broadcaster stored them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(
    rename_all = "camelCase",
    deny_unknown_fields,
    try_from = "UncheckedStoredMessagesRequest"
)]
pub struct StoredMessagesRequest {
    #[serde(deserialize_with = "deserialize_uuid_v4")]
    #[schema(schema_with = crate::api::common::uuid_v4_schema)]
    pub request_id: Uuid,
    #[schema(minimum = 1, maximum = 1)]
    pub api_revision: u32,
    #[schema(minimum = 1)]
    pub timeout_ms: u64,
    #[schema(minimum = 8453, maximum = 8453)]
    pub chain_id: u64,
    #[schema(schema_with = crate::api::common::unique_backends_schema)]
    pub backends: Vec<Backend>,
    /// The position before the first message returned.
    pub after: StreamPosition,
    /// The position of the last message returned, which must be stored.
    pub through: StreamPosition,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UncheckedStoredMessagesRequest {
    #[serde(deserialize_with = "deserialize_uuid_v4")]
    request_id: Uuid,
    api_revision: u32,
    timeout_ms: u64,
    chain_id: u64,
    backends: Vec<Backend>,
    after: StreamPosition,
    through: StreamPosition,
}

impl TryFrom<UncheckedStoredMessagesRequest> for StoredMessagesRequest {
    type Error = String;

    fn try_from(value: UncheckedStoredMessagesRequest) -> Result<Self, Self::Error> {
        validate_api_revision(value.api_revision)?;
        validate_timeout(value.timeout_ms)?;
        validate_chain_id(value.chain_id)?;
        validate_raw_backends(&value.backends)?;
        if value.after >= value.through {
            return Err("after must precede through".to_owned());
        }
        // Each generation has its own writer, so no stored order runs across two.
        if value.after.generation != value.through.generation {
            return Err("after and through must be in one generation".to_owned());
        }
        Ok(Self {
            request_id: value.request_id,
            api_revision: value.api_revision,
            timeout_ms: value.timeout_ms,
            chain_id: value.chain_id,
            backends: value.backends,
            after: value.after,
            through: value.through,
        })
    }
}

/// The raw snapshot at a stream position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RawSnapshotResult {
    pub position: StreamPosition,
    /// The checkpoint the snapshot was rebuilt from.
    pub checkpoint_position: StreamPosition,
    /// One snapshot partition per requested backend, in the broadcaster's
    /// snapshot wire form.
    #[schema(value_type = Vec<Object>)]
    pub partitions: Vec<RawJson>,
}

/// The stored messages in a range, in stream order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StoredMessagesResult {
    pub after: StreamPosition,
    pub through: StreamPosition,
    pub messages: Vec<StoredMessage>,
}

/// One stored message, the envelope exactly as the broadcaster stored it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StoredMessage {
    pub position: StreamPosition,
    #[schema(value_type = Object)]
    pub envelope: RawJson,
}

/// Raw-history progress, a single read that is either pending or done.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RawHistoryProgress {
    #[schema(maximum = 100)]
    pub percent_complete: u8,
}

/// JSON kept as the bytes it was written in.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RawJson(Box<RawValue>);

impl RawJson {
    /// The JSON text.
    #[must_use]
    pub fn get(&self) -> &str {
        self.0.get()
    }
}

impl From<Box<RawValue>> for RawJson {
    fn from(value: Box<RawValue>) -> Self {
        Self(value)
    }
}

impl PartialEq for RawJson {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}

impl Eq for RawJson {}

/// Canonical raw snapshot request content used for idempotency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedRawSnapshotRequest {
    pub api_revision: u32,
    pub timeout_ms: u64,
    pub chain_id: u64,
    pub backends: Vec<Backend>,
    pub position: StreamPosition,
}

impl NormalizedRawSnapshotRequest {
    /// Returns a SHA-256 fingerprint of the canonical request JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if canonical JSON serialization fails.
    pub fn fingerprint(&self) -> Result<[u8; 32], serde_json::Error> {
        let json = serde_json::to_vec(self)?;
        Ok(Sha256::digest(json).into())
    }
}

/// Canonicalizes a validated raw snapshot request for idempotency comparison.
#[must_use]
pub fn normalize_raw_snapshot_request(
    request: &RawSnapshotRequest,
) -> NormalizedRawSnapshotRequest {
    let mut backends = request.backends.clone();
    backends.sort_unstable();
    NormalizedRawSnapshotRequest {
        api_revision: request.api_revision,
        timeout_ms: request.timeout_ms,
        chain_id: request.chain_id,
        backends,
        position: request.position,
    }
}

/// Canonical stored messages request content used for idempotency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedStoredMessagesRequest {
    pub api_revision: u32,
    pub timeout_ms: u64,
    pub chain_id: u64,
    pub backends: Vec<Backend>,
    pub after: StreamPosition,
    pub through: StreamPosition,
}

impl NormalizedStoredMessagesRequest {
    /// Returns a SHA-256 fingerprint of the canonical request JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if canonical JSON serialization fails.
    pub fn fingerprint(&self) -> Result<[u8; 32], serde_json::Error> {
        let json = serde_json::to_vec(self)?;
        Ok(Sha256::digest(json).into())
    }
}

/// Canonicalizes a validated stored messages request for idempotency
/// comparison.
#[must_use]
pub fn normalize_stored_messages_request(
    request: &StoredMessagesRequest,
) -> NormalizedStoredMessagesRequest {
    let mut backends = request.backends.clone();
    backends.sort_unstable();
    NormalizedStoredMessagesRequest {
        api_revision: request.api_revision,
        timeout_ms: request.timeout_ms,
        chain_id: request.chain_id,
        backends,
        after: request.after,
        through: request.through,
    }
}

fn validate_raw_backends(backends: &[Backend]) -> Result<(), String> {
    validate_backends(backends)?;
    if backends.contains(&Backend::Rfq) {
        return Err("backends must be kept as raw messages, native or vm".to_owned());
    }
    Ok(())
}
