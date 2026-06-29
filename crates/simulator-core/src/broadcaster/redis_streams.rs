use std::collections::BTreeSet;

use serde::{
    de::{self, Deserializer},
    Deserialize, Serialize,
};

use super::{
    ensure_chain_id, ensure_message_seq, ensure_snapshot_id, ensure_stream_id, BroadcasterBackend,
    BroadcasterContractError, BroadcasterEnvelope, BroadcasterMessageKind, BroadcasterPayload,
    BroadcasterUpdatePartition,
};

const REDIS_STREAM_SCHEMA_VERSION: &str = "1";

/// Redis Streams field model for one serialized broadcaster envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BroadcasterRedisStreamEntry {
    pub schema_version: String,
    #[serde(with = "u64_string")]
    pub chain_id: u64,
    pub stream_id: String,
    #[serde(with = "u64_string")]
    pub message_seq: u64,
    pub kind: BroadcasterMessageKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
    pub backend_scope: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "optional_u64_string"
    )]
    pub block_number: Option<u64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "optional_u64_string"
    )]
    pub observed_timestamp_ms: Option<u64>,
    #[serde(with = "u64_string")]
    pub event_time_ms: u64,
    /// Serialized `BroadcasterEnvelope` for the Redis delta payload contract.
    pub payload_json: String,
}

/// Redis replay checkpoint captured with an HTTP snapshot session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterRedisReplayBoundary {
    pub stream_key: String,
    pub stream_id: String,
    pub snapshot_id: String,
    pub generation: u64,
    pub exclusive_message_seq: u64,
}

impl BroadcasterRedisReplayBoundary {
    pub fn new(
        stream_key: impl Into<String>,
        stream_id: impl Into<String>,
        snapshot_id: impl Into<String>,
        generation: u64,
        exclusive_message_seq: u64,
    ) -> Result<Self, BroadcasterContractError> {
        Ok(Self {
            stream_key: required_redis_field("stream_key", stream_key.into())?,
            stream_id: required_redis_field("stream_id", stream_id.into())?,
            snapshot_id: required_redis_field("snapshot_id", snapshot_id.into())?,
            generation: redis_boundary_generation(generation)?,
            exclusive_message_seq,
        })
    }

    pub fn exclusive_entry_id(&self) -> String {
        format!("{}-{}", self.generation, self.exclusive_message_seq)
    }
}

impl<'de> Deserialize<'de> for BroadcasterRedisReplayBoundary {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        #[serde(deny_unknown_fields)]
        struct WireBoundary {
            stream_key: String,
            stream_id: String,
            snapshot_id: String,
            generation: u64,
            exclusive_message_seq: u64,
        }

        let wire = WireBoundary::deserialize(deserializer)?;
        let boundary = Self::new(
            wire.stream_key,
            wire.stream_id,
            wire.snapshot_id,
            wire.generation,
            wire.exclusive_message_seq,
        )
        .map_err(de::Error::custom)?;
        Ok(boundary)
    }
}

impl BroadcasterRedisStreamEntry {
    pub fn from_envelope(
        chain_id: u64,
        event_time_ms: u64,
        envelope: &BroadcasterEnvelope,
    ) -> Result<Self, BroadcasterContractError> {
        ensure_redis_delta_kind(envelope.kind())?;
        let backends = redis_payload_backend_scope(&envelope.payload)?.unwrap_or_default();
        let payload_json = serde_json::to_string(envelope).map_err(|error| {
            BroadcasterContractError::RedisPayloadJsonInvalid {
                message: error.to_string(),
            }
        })?;
        let entry = Self {
            schema_version: REDIS_STREAM_SCHEMA_VERSION.to_string(),
            chain_id,
            stream_id: required_redis_field("stream_id", envelope.stream_id.clone())?,
            message_seq: envelope.message_seq,
            kind: envelope.kind(),
            snapshot_id: redis_payload_snapshot_id(&envelope.payload).map(str::to_string),
            backend_scope: redis_backend_scope(backends)?,
            block_number: redis_entry_block_number(&envelope.payload),
            observed_timestamp_ms: redis_entry_observed_timestamp_ms(&envelope.payload),
            event_time_ms,
            payload_json,
        };
        entry.validate()?;
        Ok(entry)
    }

    fn validate(&self) -> Result<(), BroadcasterContractError> {
        ensure_redis_schema_version(&self.schema_version)?;
        required_redis_field("stream_id", self.stream_id.clone())?;
        required_redis_field("payload_json", self.payload_json.clone())?;
        ensure_redis_message_seq(self.message_seq)?;
        ensure_redis_delta_kind(self.kind)?;
        let backends = parse_redis_backend_scope(&self.backend_scope)?;
        if let Some(snapshot_id) = &self.snapshot_id {
            required_redis_field("snapshot_id", snapshot_id.clone())?;
        }
        if self.snapshot_id.is_none() && redis_entry_requires_snapshot_id(self.kind) {
            return Err(BroadcasterContractError::RedisEntryMissingSnapshotId { kind: self.kind });
        }
        self.validate_payload(&backends)
    }

    fn validate_payload(
        &self,
        backends: &[BroadcasterBackend],
    ) -> Result<(), BroadcasterContractError> {
        let envelope = parse_redis_payload_json(&self.payload_json)?;
        ensure_stream_id(&self.stream_id, &envelope.stream_id)?;
        ensure_message_seq(Some(self.message_seq), envelope.message_seq)?;
        if envelope.kind() != self.kind {
            return Err(BroadcasterContractError::RedisPayloadKindMismatch {
                expected: self.kind,
                found: envelope.kind(),
            });
        }
        if let Some(payload_snapshot_id) = redis_payload_snapshot_id(&envelope.payload) {
            ensure_redis_snapshot_id(self.snapshot_id.as_deref(), payload_snapshot_id)?;
        }
        if let Some(payload_chain_id) = redis_payload_chain_id(&envelope.payload) {
            ensure_chain_id(self.chain_id, payload_chain_id)?;
        }
        if let Some(payload_backends) = redis_payload_backend_scope(&envelope.payload)? {
            ensure_redis_backend_scope(backends, &payload_backends)?;
        }
        ensure_redis_payload_block_number(self.block_number, &envelope.payload)?;
        ensure_redis_payload_observed_timestamp_ms(self.observed_timestamp_ms, &envelope.payload)?;
        Ok(())
    }
}

impl<'de> Deserialize<'de> for BroadcasterRedisStreamEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireEntry {
            schema_version: String,
            #[serde(with = "u64_string")]
            chain_id: u64,
            stream_id: String,
            #[serde(with = "u64_string")]
            message_seq: u64,
            kind: BroadcasterMessageKind,
            #[serde(default)]
            snapshot_id: Option<String>,
            backend_scope: String,
            #[serde(default, with = "optional_u64_string")]
            block_number: Option<u64>,
            #[serde(default, with = "optional_u64_string")]
            observed_timestamp_ms: Option<u64>,
            #[serde(with = "u64_string")]
            event_time_ms: u64,
            payload_json: String,
        }

        let wire = WireEntry::deserialize(deserializer)?;
        let entry = Self {
            schema_version: wire.schema_version,
            chain_id: wire.chain_id,
            stream_id: wire.stream_id,
            message_seq: wire.message_seq,
            kind: wire.kind,
            snapshot_id: wire.snapshot_id,
            backend_scope: wire.backend_scope,
            block_number: wire.block_number,
            observed_timestamp_ms: wire.observed_timestamp_ms,
            event_time_ms: wire.event_time_ms,
            payload_json: wire.payload_json,
        };
        entry.validate().map_err(de::Error::custom)?;
        Ok(entry)
    }
}

fn redis_entry_requires_snapshot_id(kind: BroadcasterMessageKind) -> bool {
    matches!(
        kind,
        BroadcasterMessageKind::Heartbeat | BroadcasterMessageKind::Progress
    )
}

fn required_redis_field(
    field: &'static str,
    value: String,
) -> Result<String, BroadcasterContractError> {
    if value.trim().is_empty() {
        Err(BroadcasterContractError::RedisEntryEmptyField { field })
    } else {
        Ok(value)
    }
}

fn ensure_redis_schema_version(schema_version: &str) -> Result<(), BroadcasterContractError> {
    if schema_version == REDIS_STREAM_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(BroadcasterContractError::RedisUnsupportedSchemaVersion {
            found: schema_version.to_string(),
        })
    }
}

fn ensure_redis_message_seq(message_seq: u64) -> Result<(), BroadcasterContractError> {
    if message_seq == 0 {
        Err(BroadcasterContractError::RedisMessageSequenceZero)
    } else {
        Ok(())
    }
}

fn ensure_redis_delta_kind(kind: BroadcasterMessageKind) -> Result<(), BroadcasterContractError> {
    match kind {
        BroadcasterMessageKind::Update
        | BroadcasterMessageKind::Heartbeat
        | BroadcasterMessageKind::Progress => Ok(()),
        BroadcasterMessageKind::SnapshotStart
        | BroadcasterMessageKind::SnapshotChunk
        | BroadcasterMessageKind::SnapshotEnd => {
            Err(BroadcasterContractError::RedisSnapshotPayloadUnsupported { kind })
        }
    }
}

fn redis_backend_scope(
    backends: Vec<BroadcasterBackend>,
) -> Result<String, BroadcasterContractError> {
    if backends.is_empty() {
        return Err(BroadcasterContractError::RedisEntryEmptyField {
            field: "backend_scope",
        });
    }

    let mut unique = BTreeSet::new();
    for backend in backends {
        if !unique.insert(backend) {
            return Err(BroadcasterContractError::DuplicateBackendEntry {
                context: "redis_entry.backend_scope",
                backend,
            });
        }
    }

    let mut scope = String::new();
    for backend in unique {
        if !scope.is_empty() {
            scope.push(',');
        }
        scope.push_str(backend.as_str());
    }
    Ok(scope)
}

fn parse_redis_backend_scope(
    backend_scope: &str,
) -> Result<Vec<BroadcasterBackend>, BroadcasterContractError> {
    required_redis_field("backend_scope", backend_scope.to_string())?;
    let mut backends = Vec::new();
    for value in backend_scope.split(',') {
        let backend = parse_redis_backend(value)?;
        if backends.contains(&backend) {
            return Err(BroadcasterContractError::DuplicateBackendEntry {
                context: "redis_entry.backend_scope",
                backend,
            });
        }
        backends.push(backend);
    }
    let expected = redis_backend_scope(backends.clone())?;
    if backend_scope != expected {
        return Err(BroadcasterContractError::RedisBackendScopeInvalid {
            backend_scope: backend_scope.to_string(),
        });
    }
    Ok(backends)
}

fn parse_redis_backend(value: &str) -> Result<BroadcasterBackend, BroadcasterContractError> {
    match value {
        "native" => Ok(BroadcasterBackend::Native),
        "vm" => Ok(BroadcasterBackend::Vm),
        "rfq" => Ok(BroadcasterBackend::Rfq),
        _ => Err(BroadcasterContractError::RedisBackendScopeInvalid {
            backend_scope: value.to_string(),
        }),
    }
}

fn ensure_redis_payload_block_number(
    entry_block_number: Option<u64>,
    payload: &BroadcasterPayload,
) -> Result<(), BroadcasterContractError> {
    let payload_block_number = redis_entry_block_number(payload);
    match (entry_block_number, payload_block_number) {
        (Some(entry_block_number), Some(payload_block_number))
            if entry_block_number != payload_block_number =>
        {
            return Err(BroadcasterContractError::RedisBlockNumberMismatch {
                entry_block_number,
                payload_block_number,
            });
        }
        (Some(_), None) => {
            return Err(BroadcasterContractError::RedisEntryUnexpectedField {
                field: "block_number",
            });
        }
        (None, Some(_)) => {
            return Err(BroadcasterContractError::RedisEntryEmptyField {
                field: "block_number",
            });
        }
        (Some(_), Some(_)) | (None, None) => {}
    }
    Ok(())
}

fn redis_entry_block_number(payload: &BroadcasterPayload) -> Option<u64> {
    redis_payload_global_block_number(&redis_payload_chain_block_numbers(payload))
}

fn ensure_redis_payload_observed_timestamp_ms(
    entry_observed_timestamp_ms: Option<u64>,
    payload: &BroadcasterPayload,
) -> Result<(), BroadcasterContractError> {
    let payload_observed_timestamp_ms = redis_entry_observed_timestamp_ms(payload);
    match (entry_observed_timestamp_ms, payload_observed_timestamp_ms) {
        (Some(entry_observed_timestamp_ms), Some(payload_observed_timestamp_ms))
            if entry_observed_timestamp_ms != payload_observed_timestamp_ms =>
        {
            return Err(BroadcasterContractError::RedisObservedTimestampMismatch {
                entry_observed_timestamp_ms,
                payload_observed_timestamp_ms,
            });
        }
        (Some(_), None) => {
            return Err(BroadcasterContractError::RedisEntryUnexpectedField {
                field: "observed_timestamp_ms",
            });
        }
        (None, Some(_)) => {
            return Err(BroadcasterContractError::RedisEntryEmptyField {
                field: "observed_timestamp_ms",
            });
        }
        (Some(_), Some(_)) | (None, None) => {}
    }
    Ok(())
}

fn redis_entry_observed_timestamp_ms(payload: &BroadcasterPayload) -> Option<u64> {
    match payload {
        BroadcasterPayload::Update(update) => update
            .partitions
            .iter()
            .find(|partition| partition.backend == BroadcasterBackend::Rfq)
            .map(|partition| partition.block_number),
        BroadcasterPayload::Heartbeat(heartbeat) => heartbeat
            .backend_heads
            .iter()
            .find(|head| head.backend == BroadcasterBackend::Rfq)
            .map(|head| head.block_number),
        BroadcasterPayload::Progress(_)
        | BroadcasterPayload::SnapshotStart(_)
        | BroadcasterPayload::SnapshotChunk(_)
        | BroadcasterPayload::SnapshotEnd(_) => None,
    }
}

fn redis_payload_global_block_number(block_numbers: &[u64]) -> Option<u64> {
    let (&first_block_number, remaining) = block_numbers.split_first()?;
    remaining
        .iter()
        .all(|block_number| *block_number == first_block_number)
        .then_some(first_block_number)
}

fn redis_payload_chain_block_numbers(payload: &BroadcasterPayload) -> Vec<u64> {
    match payload {
        BroadcasterPayload::Update(update) => update
            .partitions
            .iter()
            .filter(|partition| {
                matches!(
                    partition.backend,
                    BroadcasterBackend::Native | BroadcasterBackend::Vm
                )
            })
            .map(|partition| partition.block_number)
            .collect(),
        BroadcasterPayload::Heartbeat(_)
        | BroadcasterPayload::Progress(_)
        | BroadcasterPayload::SnapshotStart(_)
        | BroadcasterPayload::SnapshotChunk(_)
        | BroadcasterPayload::SnapshotEnd(_) => Vec::new(),
    }
}

fn parse_redis_payload_json(
    payload_json: &str,
) -> Result<BroadcasterEnvelope, BroadcasterContractError> {
    serde_json::from_str(payload_json).map_err(|error| {
        BroadcasterContractError::RedisPayloadJsonInvalid {
            message: error.to_string(),
        }
    })
}

fn redis_payload_snapshot_id(payload: &BroadcasterPayload) -> Option<&str> {
    match payload {
        BroadcasterPayload::Heartbeat(heartbeat) => Some(&heartbeat.snapshot_id),
        BroadcasterPayload::Progress(progress) => Some(&progress.snapshot_id),
        BroadcasterPayload::Update(_)
        | BroadcasterPayload::SnapshotStart(_)
        | BroadcasterPayload::SnapshotChunk(_)
        | BroadcasterPayload::SnapshotEnd(_) => None,
    }
}

fn redis_payload_chain_id(payload: &BroadcasterPayload) -> Option<u64> {
    match payload {
        BroadcasterPayload::Heartbeat(heartbeat) => Some(heartbeat.chain_id),
        BroadcasterPayload::Progress(progress) => Some(progress.chain_id),
        BroadcasterPayload::Update(_)
        | BroadcasterPayload::SnapshotStart(_)
        | BroadcasterPayload::SnapshotChunk(_)
        | BroadcasterPayload::SnapshotEnd(_) => None,
    }
}

fn redis_payload_backend_scope(
    payload: &BroadcasterPayload,
) -> Result<Option<Vec<BroadcasterBackend>>, BroadcasterContractError> {
    match payload {
        BroadcasterPayload::Update(update) => redis_update_backend_scope(&update.partitions),
        BroadcasterPayload::Heartbeat(heartbeat) => Ok(Some(
            heartbeat
                .backend_heads
                .iter()
                .map(|head| head.backend)
                .collect(),
        )),
        BroadcasterPayload::Progress(progress) => Ok(Some(progress.backends.clone())),
        BroadcasterPayload::SnapshotStart(_)
        | BroadcasterPayload::SnapshotChunk(_)
        | BroadcasterPayload::SnapshotEnd(_) => Ok(None),
    }
}

fn redis_update_backend_scope(
    partitions: &[BroadcasterUpdatePartition],
) -> Result<Option<Vec<BroadcasterBackend>>, BroadcasterContractError> {
    let backends: Vec<_> = partitions
        .iter()
        .map(|partition| partition.backend)
        .collect();
    if backends.is_empty() {
        return Ok(Some(backends));
    }
    parse_redis_backend_scope(&redis_backend_scope(backends)?).map(Some)
}

fn ensure_redis_snapshot_id(
    entry_snapshot_id: Option<&str>,
    payload_snapshot_id: &str,
) -> Result<(), BroadcasterContractError> {
    let Some(entry_snapshot_id) = entry_snapshot_id else {
        return Err(BroadcasterContractError::RedisEntryMissingSnapshotId {
            kind: BroadcasterMessageKind::SnapshotStart,
        });
    };
    ensure_snapshot_id(entry_snapshot_id, payload_snapshot_id)
}

fn ensure_redis_backend_scope(
    entry_backends: &[BroadcasterBackend],
    payload_backends: &[BroadcasterBackend],
) -> Result<(), BroadcasterContractError> {
    let entry_scope = redis_backend_scope(entry_backends.to_vec())?;
    let payload_scope = redis_backend_scope(payload_backends.to_vec()).unwrap_or_default();
    if entry_scope == payload_scope {
        Ok(())
    } else {
        Err(BroadcasterContractError::RedisBackendScopeInvalid {
            backend_scope: entry_scope,
        })
    }
}

fn redis_boundary_generation(generation: u64) -> Result<u64, BroadcasterContractError> {
    if generation == 0 {
        return Err(BroadcasterContractError::InvalidRedisEntryId {
            entry_id: format!("{generation}-0"),
        });
    }
    Ok(generation)
}

mod u64_string {
    use serde::{de, Deserialize, Deserializer, Serializer};

    #[expect(
        clippy::trivially_copy_pass_by_ref,
        reason = "serde serializer helpers receive values by reference"
    )]
    pub fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value
            .parse()
            .map_err(|_| de::Error::custom("expected u64 string"))
    }
}

mod optional_u64_string {
    use serde::{de, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Option<u64>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(value) => serializer.serialize_some(&value.to_string()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let Some(value) = Option::<String>::deserialize(deserializer)? else {
            return Ok(None);
        };
        value
            .parse()
            .map(Some)
            .map_err(|_| de::Error::custom("expected optional u64 string"))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use anyhow::{anyhow, Result};

    use super::{BroadcasterRedisReplayBoundary, BroadcasterRedisStreamEntry};
    use crate::broadcaster::test_support::{
        dummy_state, heartbeat_envelope, protocol_component, snapshot_end_envelope,
        snapshot_start_envelope, update_envelope,
    };
    use crate::broadcaster::{
        BroadcasterBackend, BroadcasterBackendHead, BroadcasterContractError, BroadcasterEnvelope,
        BroadcasterGenerationHandoff, BroadcasterHeartbeat, BroadcasterMessageKind,
        BroadcasterPayload, BroadcasterProgress, BroadcasterStateEntry, BroadcasterUpdateMessage,
        BroadcasterUpdatePartition,
    };

    #[test]
    fn redis_stream_entry_derives_fields_from_envelope() -> Result<()> {
        let envelope = update_envelope("stream-1", 4)?;
        let entry = redis_entry(&envelope)?;

        assert_eq!(entry.stream_id, "stream-1");
        assert_eq!(entry.message_seq, 4);
        assert_eq!(entry.kind, BroadcasterMessageKind::Update);
        assert_eq!(entry.snapshot_id, None);
        assert_eq!(entry.backend_scope, "native");
        assert_eq!(entry.block_number, Some(124));
        assert_eq!(entry.payload_json, serde_json::to_string(&envelope)?);

        Ok(())
    }

    #[test]
    fn redis_stream_entry_accepts_delta_at_first_message_sequence() -> Result<()> {
        let envelope = update_envelope("stream-1", 1)?;
        let entry = redis_entry(&envelope)?;

        assert_eq!(entry.message_seq, 1);
        assert_eq!(entry.kind, BroadcasterMessageKind::Update);
        Ok(())
    }

    #[test]
    fn redis_stream_entry_omits_block_number_for_rfq_only_update() -> Result<()> {
        let envelope = rfq_update_envelope("stream-1", 4, 321)?;
        let entry = redis_entry(&envelope)?;

        assert_eq!(entry.backend_scope, "rfq");
        assert_eq!(entry.block_number, None);
        assert_eq!(entry.observed_timestamp_ms, Some(321));

        let value = serde_json::to_value(&entry)?;
        assert!(value.get("block_number").is_none());
        assert_eq!(value["observed_timestamp_ms"], "321");

        let decoded: BroadcasterRedisStreamEntry = serde_json::from_value(value)?;
        assert_eq!(decoded, entry);

        Ok(())
    }

    #[test]
    fn redis_stream_entry_omits_block_number_for_mixed_backend_update_blocks() -> Result<()> {
        let envelope = mixed_backend_update_envelope("stream-1", 4, 124, 125)?;
        let entry = redis_entry(&envelope)?;

        assert_eq!(entry.backend_scope, "native,vm");
        assert_eq!(entry.block_number, None);

        let value = serde_json::to_value(&entry)?;
        assert!(value.get("block_number").is_none());

        let decoded: BroadcasterRedisStreamEntry = serde_json::from_value(value)?;
        assert_eq!(decoded, entry);

        Ok(())
    }

    #[test]
    fn redis_stream_entry_uses_native_block_number_for_native_and_rfq_update() -> Result<()> {
        let envelope = native_and_rfq_update_envelope("stream-1", 4, 124, 321)?;
        let entry = redis_entry(&envelope)?;

        assert_eq!(entry.backend_scope, "native,rfq");
        assert_eq!(entry.block_number, Some(124));
        assert_eq!(entry.observed_timestamp_ms, Some(321));

        let value = serde_json::to_value(&entry)?;
        assert_eq!(value["observed_timestamp_ms"], "321");

        Ok(())
    }

    #[test]
    fn redis_stream_entry_round_trips_with_stable_field_shape() -> Result<()> {
        let envelope = update_envelope("stream-1", 4)?;
        let entry = redis_entry(&envelope)?;

        let value = serde_json::to_value(&entry)?;

        assert_eq!(value["schema_version"], "1");
        assert_eq!(value["chain_id"], "8453");
        assert_eq!(value["stream_id"], "stream-1");
        assert_eq!(value["message_seq"], "4");
        assert_eq!(value["kind"], "update");
        assert!(value.get("snapshot_id").is_none());
        assert_eq!(value["backend_scope"], "native");
        assert_eq!(value["block_number"], "124");
        assert!(value.get("observed_timestamp_ms").is_none());
        assert_eq!(value["event_time_ms"], "1710000000123");
        assert_eq!(value["payload_json"], serde_json::to_string(&envelope)?);

        let decoded: BroadcasterRedisStreamEntry = serde_json::from_value(value)?;
        assert_eq!(decoded, entry);

        Ok(())
    }

    #[test]
    fn redis_stream_entry_deserialization_requires_payload_json() -> Result<()> {
        let error = serde_json::from_value::<BroadcasterRedisStreamEntry>(serde_json::json!({
            "schema_version": "1",
            "chain_id": "8453",
            "stream_id": "stream-1",
            "message_seq": "1",
            "kind": "update",
            "backend_scope": "native",
            "event_time_ms": "1710000000000"
        }))
        .err()
        .ok_or_else(|| anyhow!("missing payload_json should fail deserialization"))?;

        assert!(error.to_string().contains("missing field `payload_json`"));

        Ok(())
    }

    #[test]
    fn redis_stream_entry_deserialization_validates_contract_invariants() -> Result<()> {
        let mut value = redis_entry_value(&heartbeat_envelope("stream-1", 8453, "snapshot-1", 2)?)?;
        value
            .as_object_mut()
            .ok_or_else(|| anyhow!("redis entry should encode as object"))?
            .remove("snapshot_id");

        let error = redis_entry_decode_error(value, "redis entry without snapshot_id")?;
        assert!(error.to_string().contains("requires snapshot_id"));

        let mut value = redis_entry_value(&update_envelope("stream-1", 2)?)?;
        value["schema_version"] = serde_json::json!("2");

        let error = redis_entry_decode_error(value, "unsupported schema version")?;
        assert!(error
            .to_string()
            .contains("unsupported redis schema version"));

        Ok(())
    }

    #[test]
    fn redis_stream_entry_rejects_zero_message_sequence() -> Result<()> {
        let envelope = update_envelope("stream-1", 0)?;

        let Err(error) = redis_entry(&envelope) else {
            return Err(anyhow!("zero message_seq should fail"));
        };

        assert_eq!(error, BroadcasterContractError::RedisMessageSequenceZero);

        Ok(())
    }

    #[test]
    fn redis_stream_entry_rejects_snapshot_payloads() -> Result<()> {
        let envelopes = [
            snapshot_start_envelope("stream-1", 8453, "snapshot-1", 2, 1)?,
            snapshot_end_envelope("stream-1", 2),
        ];

        for envelope in envelopes {
            let kind = envelope.kind();
            let Err(error) = redis_entry(&envelope) else {
                return Err(anyhow!("{kind} should not be accepted as a Redis delta"));
            };

            assert_eq!(
                error,
                BroadcasterContractError::RedisSnapshotPayloadUnsupported { kind }
            );
        }

        Ok(())
    }

    #[test]
    fn redis_stream_entry_deserialization_rejects_empty_snapshot_id() -> Result<()> {
        let mut value = redis_entry_value(&update_envelope("stream-1", 4)?)?;
        value["snapshot_id"] = serde_json::json!("");

        let error = redis_entry_decode_error(value, "empty snapshot_id")?;
        assert!(error.to_string().contains("snapshot_id must not be empty"));

        Ok(())
    }

    #[test]
    fn redis_stream_entry_requires_block_number_for_native_or_vm_state_entries() -> Result<()> {
        let mut value = redis_entry_value(&update_envelope("stream-1", 4)?)?;
        value
            .as_object_mut()
            .ok_or_else(|| anyhow!("redis entry should encode as object"))?
            .remove("block_number");

        let error = redis_entry_decode_error(value, "native update without block_number")?;

        assert!(error.to_string().contains("block_number must not be empty"));

        Ok(())
    }

    #[test]
    fn redis_stream_entry_rejects_payload_block_number_mismatch() -> Result<()> {
        let mut value = redis_entry_value(&update_envelope("stream-1", 4)?)?;
        value["block_number"] = serde_json::json!("125");

        let error = redis_entry_decode_error(value, "mismatched block_number")?;

        assert!(error
            .to_string()
            .contains("redis block_number mismatch: entry 125, payload 124"));

        Ok(())
    }

    #[test]
    fn redis_stream_entry_rejects_block_number_for_rfq_only_payload() -> Result<()> {
        let mut value = redis_entry_value(&rfq_update_envelope("stream-1", 4, 321)?)?;
        value["block_number"] = serde_json::json!("322");

        let error = redis_entry_decode_error(value, "unexpected RFQ block_number")?;

        assert!(error
            .to_string()
            .contains("redis stream field block_number is not allowed for this payload"));

        Ok(())
    }

    #[test]
    fn redis_stream_entry_rejects_observed_timestamp_mismatch() -> Result<()> {
        let mut value = redis_entry_value(&rfq_update_envelope("stream-1", 4, 321)?)?;
        value["observed_timestamp_ms"] = serde_json::json!("322");

        let error = redis_entry_decode_error(value, "mismatched RFQ observed_timestamp_ms")?;

        assert!(error
            .to_string()
            .contains("redis observed_timestamp_ms mismatch: entry 322, payload 321"));

        Ok(())
    }

    #[test]
    fn redis_stream_entry_requires_observed_timestamp_for_rfq_payload() -> Result<()> {
        let mut value = redis_entry_value(&rfq_update_envelope("stream-1", 4, 321)?)?;
        value
            .as_object_mut()
            .ok_or_else(|| anyhow!("redis entry should encode as object"))?
            .remove("observed_timestamp_ms");

        let error = redis_entry_decode_error(value, "RFQ update without observed_timestamp_ms")?;

        assert!(error
            .to_string()
            .contains("observed_timestamp_ms must not be empty"));

        Ok(())
    }

    #[test]
    fn redis_stream_entry_rejects_block_number_for_divergent_backend_blocks() -> Result<()> {
        let mut value =
            redis_entry_value(&mixed_backend_update_envelope("stream-1", 4, 124, 125)?)?;
        value["block_number"] = serde_json::json!("125");

        let error = redis_entry_decode_error(value, "unexpected divergent block_number")?;

        assert!(error
            .to_string()
            .contains("redis stream field block_number is not allowed for this payload"));

        Ok(())
    }

    #[test]
    fn redis_stream_entry_omits_block_number_for_heartbeat_with_backend_heads() -> Result<()> {
        let envelope = heartbeat_envelope("stream-1", 8453, "snapshot-1", 5)?;
        let entry = redis_entry(&envelope)?;

        assert_eq!(entry.kind, BroadcasterMessageKind::Heartbeat);
        assert_eq!(entry.block_number, None);
        assert_eq!(entry.observed_timestamp_ms, None);

        let value = serde_json::to_value(&entry)?;
        assert!(value.get("block_number").is_none());
        assert!(value.get("observed_timestamp_ms").is_none());

        let decoded: BroadcasterRedisStreamEntry = serde_json::from_value(value)?;
        assert_eq!(decoded, entry);

        Ok(())
    }

    #[test]
    fn redis_stream_entry_uses_observed_timestamp_for_rfq_heartbeat() -> Result<()> {
        let envelope = rfq_heartbeat_envelope("stream-1", 8453, "snapshot-1", 5, 321)?;
        let entry = redis_entry(&envelope)?;

        assert_eq!(entry.kind, BroadcasterMessageKind::Heartbeat);
        assert_eq!(entry.backend_scope, "rfq");
        assert_eq!(entry.block_number, None);
        assert_eq!(entry.observed_timestamp_ms, Some(321));

        let value = serde_json::to_value(&entry)?;
        assert!(value.get("block_number").is_none());
        assert_eq!(value["observed_timestamp_ms"], "321");

        Ok(())
    }

    #[test]
    fn redis_stream_entry_omits_handoff_for_normal_progress() -> Result<()> {
        let envelope = progress_envelope("stream-1", 8453, "snapshot-1", 6)?;
        let entry = redis_entry(&envelope)?;

        assert_eq!(entry.kind, BroadcasterMessageKind::Progress);
        assert_eq!(entry.snapshot_id, Some("snapshot-1".to_string()));
        assert_eq!(entry.backend_scope, "native,vm");
        assert_eq!(entry.block_number, None);
        assert_eq!(entry.observed_timestamp_ms, None);

        let payload: serde_json::Value = serde_json::from_str(&entry.payload_json)?;
        assert!(payload.get("handoff").is_none());

        let value = serde_json::to_value(&entry)?;
        assert!(value.get("block_number").is_none());
        assert!(value.get("observed_timestamp_ms").is_none());

        let decoded: BroadcasterRedisStreamEntry = serde_json::from_value(value)?;
        assert_eq!(decoded, entry);

        Ok(())
    }

    #[test]
    fn redis_stream_entry_round_trips_progress_with_handoff() -> Result<()> {
        let handoff = generation_handoff()?;
        let progress = BroadcasterProgress::new_with_handoff(
            8453,
            "snapshot-2",
            vec![BroadcasterBackend::Vm, BroadcasterBackend::Native],
            "generation_reset",
            handoff.clone(),
        )?;
        let envelope =
            BroadcasterEnvelope::new("stream-2", 9, BroadcasterPayload::Progress(progress));
        let entry = redis_entry(&envelope)?;

        assert_eq!(entry.kind, BroadcasterMessageKind::Progress);
        assert_eq!(entry.snapshot_id, Some("snapshot-2".to_string()));
        assert_eq!(entry.backend_scope, "native,vm");

        let payload: serde_json::Value = serde_json::from_str(&entry.payload_json)?;
        assert_eq!(
            payload["handoff"],
            serde_json::json!({
                "previousStreamId": "stream-1",
                "previousEntryId": "7-42",
                "baseHeads": [
                    {"backend": "native", "blockNumber": 124},
                    {"backend": "vm", "blockNumber": 125}
                ]
            })
        );

        let decoded_entry: BroadcasterRedisStreamEntry =
            serde_json::from_value(serde_json::to_value(&entry)?)?;
        let decoded_envelope: BroadcasterEnvelope =
            serde_json::from_str(&decoded_entry.payload_json)?;
        let BroadcasterPayload::Progress(decoded_progress) = decoded_envelope.payload else {
            return Err(anyhow!("decoded payload should remain progress"));
        };
        assert_eq!(decoded_progress.handoff, Some(handoff));

        Ok(())
    }

    #[test]
    fn generation_handoff_rejects_empty_ids_and_duplicate_base_heads() -> Result<()> {
        let error = BroadcasterGenerationHandoff::new("", "7-42", base_heads())
            .err()
            .ok_or_else(|| anyhow!("empty previous stream id should fail handoff construction"))?;
        assert_eq!(
            error,
            BroadcasterContractError::RedisEntryEmptyField {
                field: "handoff.previousStreamId",
            }
        );

        let error = BroadcasterGenerationHandoff::new("stream-1", " ", base_heads())
            .err()
            .ok_or_else(|| anyhow!("empty previous entry id should fail handoff construction"))?;
        assert_eq!(
            error,
            BroadcasterContractError::RedisEntryEmptyField {
                field: "handoff.previousEntryId",
            }
        );

        let duplicate_heads = vec![
            BroadcasterBackendHead::new(BroadcasterBackend::Native, 124),
            BroadcasterBackendHead::new(BroadcasterBackend::Native, 125),
        ];
        let error = BroadcasterGenerationHandoff::new("stream-1", "7-42", duplicate_heads)
            .err()
            .ok_or_else(|| anyhow!("duplicate handoff base heads should fail construction"))?;
        assert_eq!(
            error,
            BroadcasterContractError::DuplicateBackendEntry {
                context: "progress.handoff.baseHeads",
                backend: BroadcasterBackend::Native,
            }
        );

        Ok(())
    }

    #[test]
    fn generation_handoff_deserialization_rejects_empty_ids_and_duplicate_base_heads() -> Result<()>
    {
        let empty_stream_error = handoff_decode_error(
            serde_json::json!({
                "previousStreamId": "",
                "previousEntryId": "7-42",
                "baseHeads": [
                    {"backend": "native", "blockNumber": 124}
                ]
            }),
            "empty previousStreamId",
        )?;
        assert!(empty_stream_error
            .to_string()
            .contains("handoff.previousStreamId must not be empty"));

        let empty_entry_error = handoff_decode_error(
            serde_json::json!({
                "previousStreamId": "stream-1",
                "previousEntryId": "",
                "baseHeads": [
                    {"backend": "native", "blockNumber": 124}
                ]
            }),
            "empty previousEntryId",
        )?;
        assert!(empty_entry_error
            .to_string()
            .contains("handoff.previousEntryId must not be empty"));

        let duplicate_heads_error = handoff_decode_error(
            serde_json::json!({
                "previousStreamId": "stream-1",
                "previousEntryId": "7-42",
                "baseHeads": [
                    {"backend": "native", "blockNumber": 124},
                    {"backend": "native", "blockNumber": 125}
                ]
            }),
            "duplicate baseHeads",
        )?;
        assert!(duplicate_heads_error
            .to_string()
            .contains("duplicate backend entry Native"));

        Ok(())
    }

    #[test]
    fn redis_replay_boundary_uses_camel_case_shape_and_deterministic_checkpoint() -> Result<()> {
        let boundary = BroadcasterRedisReplayBoundary::new(
            "dsolver:broadcaster:prod-base:8453:events",
            "chain-8453-stream-7",
            "chain-8453-snapshot-7",
            7,
            42,
        )?;

        let value = serde_json::to_value(&boundary)?;

        assert_eq!(
            value,
            serde_json::json!({
                "streamKey": "dsolver:broadcaster:prod-base:8453:events",
                "streamId": "chain-8453-stream-7",
                "snapshotId": "chain-8453-snapshot-7",
                "generation": 7,
                "exclusiveMessageSeq": 42,
            })
        );
        assert_eq!(boundary.exclusive_entry_id(), "7-42");
        let decoded: BroadcasterRedisReplayBoundary = serde_json::from_value(value)?;
        assert_eq!(decoded, boundary);

        Ok(())
    }

    #[test]
    fn redis_replay_boundary_rejects_stale_entry_id_field() -> Result<()> {
        let value = serde_json::json!({
            "streamKey": "dsolver:broadcaster:prod-base:8453:events",
            "streamId": "chain-8453-stream-7",
            "snapshotId": "chain-8453-snapshot-7",
            "generation": 7,
            "exclusiveEntryId": "7-99",
            "exclusiveMessageSeq": 42,
        });

        let Err(error) = serde_json::from_value::<BroadcasterRedisReplayBoundary>(value) else {
            return Err(anyhow!(
                "stale replay boundary entry id field should fail under the sequence boundary contract"
            ));
        };

        assert!(error.to_string().contains("exclusiveEntryId"));
        Ok(())
    }

    fn redis_entry(
        envelope: &BroadcasterEnvelope,
    ) -> Result<BroadcasterRedisStreamEntry, BroadcasterContractError> {
        BroadcasterRedisStreamEntry::from_envelope(8453, 1_710_000_000_123, envelope)
    }

    fn redis_entry_value(envelope: &BroadcasterEnvelope) -> Result<serde_json::Value> {
        serde_json::to_value(redis_entry(envelope)?).map_err(Into::into)
    }

    fn redis_entry_decode_error(
        value: serde_json::Value,
        context: &'static str,
    ) -> Result<serde_json::Error> {
        serde_json::from_value::<BroadcasterRedisStreamEntry>(value)
            .err()
            .ok_or_else(|| anyhow!("{context} should fail"))
    }

    fn rfq_update_envelope(
        stream_id: &str,
        message_seq: u64,
        block_number: u64,
    ) -> Result<BroadcasterEnvelope> {
        Ok(BroadcasterEnvelope::new(
            stream_id,
            message_seq,
            BroadcasterPayload::Update(BroadcasterUpdateMessage::new(vec![
                BroadcasterUpdatePartition::new(
                    BroadcasterBackend::Rfq,
                    block_number,
                    vec![BroadcasterStateEntry::new(
                        "pool-rfq",
                        protocol_component("pool-rfq", "rfq:hashflow"),
                        dummy_state("rfq-update"),
                    )],
                    Vec::new(),
                    Vec::new(),
                    BTreeMap::new(),
                ),
            ])?),
        ))
    }

    fn rfq_heartbeat_envelope(
        stream_id: &str,
        chain_id: u64,
        snapshot_id: &str,
        message_seq: u64,
        observed_timestamp_ms: u64,
    ) -> Result<BroadcasterEnvelope> {
        Ok(BroadcasterEnvelope::new(
            stream_id,
            message_seq,
            BroadcasterPayload::Heartbeat(BroadcasterHeartbeat::new(
                chain_id,
                snapshot_id,
                vec![BroadcasterBackendHead::new(
                    BroadcasterBackend::Rfq,
                    observed_timestamp_ms,
                )],
            )?),
        ))
    }

    fn progress_envelope(
        stream_id: &str,
        chain_id: u64,
        snapshot_id: &str,
        message_seq: u64,
    ) -> Result<BroadcasterEnvelope> {
        Ok(BroadcasterEnvelope::new(
            stream_id,
            message_seq,
            BroadcasterPayload::Progress(BroadcasterProgress::new(
                chain_id,
                snapshot_id,
                vec![BroadcasterBackend::Vm, BroadcasterBackend::Native],
                "generation_reset",
            )?),
        ))
    }

    fn generation_handoff() -> Result<BroadcasterGenerationHandoff, BroadcasterContractError> {
        BroadcasterGenerationHandoff::new("stream-1", "7-42", base_heads())
    }

    fn base_heads() -> Vec<BroadcasterBackendHead> {
        vec![
            BroadcasterBackendHead::new(BroadcasterBackend::Vm, 125),
            BroadcasterBackendHead::new(BroadcasterBackend::Native, 124),
        ]
    }

    fn handoff_decode_error(
        value: serde_json::Value,
        context: &'static str,
    ) -> Result<serde_json::Error> {
        serde_json::from_value::<BroadcasterGenerationHandoff>(value)
            .err()
            .ok_or_else(|| anyhow!("{context} should fail"))
    }

    fn mixed_backend_update_envelope(
        stream_id: &str,
        message_seq: u64,
        native_block_number: u64,
        vm_block_number: u64,
    ) -> Result<BroadcasterEnvelope> {
        Ok(BroadcasterEnvelope::new(
            stream_id,
            message_seq,
            BroadcasterPayload::Update(BroadcasterUpdateMessage::new(vec![
                BroadcasterUpdatePartition::new(
                    BroadcasterBackend::Native,
                    native_block_number,
                    vec![BroadcasterStateEntry::new(
                        "pool-native",
                        protocol_component("pool-native", "uniswap_v2"),
                        dummy_state("native-update"),
                    )],
                    Vec::new(),
                    Vec::new(),
                    BTreeMap::new(),
                ),
                BroadcasterUpdatePartition::new(
                    BroadcasterBackend::Vm,
                    vm_block_number,
                    vec![BroadcasterStateEntry::new(
                        "pool-vm",
                        protocol_component("pool-vm", "vm:curve"),
                        dummy_state("vm-update"),
                    )],
                    Vec::new(),
                    Vec::new(),
                    BTreeMap::new(),
                ),
            ])?),
        ))
    }

    fn native_and_rfq_update_envelope(
        stream_id: &str,
        message_seq: u64,
        native_block_number: u64,
        rfq_timestamp: u64,
    ) -> Result<BroadcasterEnvelope> {
        Ok(BroadcasterEnvelope::new(
            stream_id,
            message_seq,
            BroadcasterPayload::Update(BroadcasterUpdateMessage::new(vec![
                BroadcasterUpdatePartition::new(
                    BroadcasterBackend::Native,
                    native_block_number,
                    vec![BroadcasterStateEntry::new(
                        "pool-native",
                        protocol_component("pool-native", "uniswap_v2"),
                        dummy_state("native-update"),
                    )],
                    Vec::new(),
                    Vec::new(),
                    BTreeMap::new(),
                ),
                BroadcasterUpdatePartition::new(
                    BroadcasterBackend::Rfq,
                    rfq_timestamp,
                    vec![BroadcasterStateEntry::new(
                        "pool-rfq",
                        protocol_component("pool-rfq", "rfq:hashflow"),
                        dummy_state("rfq-update"),
                    )],
                    Vec::new(),
                    Vec::new(),
                    BTreeMap::new(),
                ),
            ])?),
        ))
    }
}
