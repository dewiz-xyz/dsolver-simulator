use std::collections::BTreeSet;

use serde::{
    de::{self, Deserializer},
    Deserialize, Serialize,
};

use super::{
    ensure_chain_id, ensure_message_seq, ensure_snapshot_id, ensure_stream_id, BroadcasterBackend,
    BroadcasterContractError, BroadcasterEnvelope, BroadcasterMessageKind, BroadcasterPayload,
    BroadcasterSnapshotPartition, BroadcasterUpdatePartition,
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
    #[serde(with = "u64_string")]
    pub event_time_ms: u64,
    /// Serialized `BroadcasterEnvelope` for the current HTTP/websocket payload contract.
    ///
    /// Snapshot chunks and live updates can still carry `Box<dyn ProtocolSim>`
    /// state. Keep that coupling visible until Redis gets a stable typed payload
    /// before live consumers rely on this stream.
    pub payload_json: String,
}

impl BroadcasterRedisStreamEntry {
    pub fn from_envelope(
        chain_id: u64,
        event_time_ms: u64,
        envelope: &BroadcasterEnvelope,
        backends: Vec<BroadcasterBackend>,
    ) -> Result<Self, BroadcasterContractError> {
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
        ensure_redis_snapshot_start_message_seq(self.kind, self.message_seq)?;
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
            event_time_ms: wire.event_time_ms,
            payload_json: wire.payload_json,
        };
        entry.validate().map_err(de::Error::custom)?;
        Ok(entry)
    }
}

/// Pointer to the latest complete snapshot segment in a Redis stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BroadcasterRedisSnapshotPointer {
    pub schema_version: String,
    pub chain_id: u64,
    pub stream_key: String,
    pub stream_id: String,
    pub snapshot_id: String,
    pub snapshot_start_entry_id: String,
    pub snapshot_end_entry_id: String,
    pub live_cursor_entry_id: String,
    pub completed_at_ms: u64,
}

impl BroadcasterRedisSnapshotPointer {
    #[expect(
        clippy::too_many_arguments,
        reason = "constructor mirrors the snapshot pointer wire contract"
    )]
    pub fn new(
        chain_id: u64,
        stream_key: impl Into<String>,
        stream_id: impl Into<String>,
        snapshot_id: impl Into<String>,
        snapshot_start_entry_id: impl Into<String>,
        snapshot_end_entry_id: impl Into<String>,
        live_cursor_entry_id: impl Into<String>,
        completed_at_ms: u64,
    ) -> Result<Self, BroadcasterContractError> {
        let snapshot_start_entry_id =
            required_redis_field("snapshot_start_entry_id", snapshot_start_entry_id.into())?;
        let snapshot_end_entry_id =
            required_redis_field("snapshot_end_entry_id", snapshot_end_entry_id.into())?;
        let live_cursor_entry_id =
            required_redis_field("live_cursor_entry_id", live_cursor_entry_id.into())?;

        ensure_redis_entry_range(&snapshot_start_entry_id, &snapshot_end_entry_id)?;
        if live_cursor_entry_id != snapshot_end_entry_id {
            return Err(
                BroadcasterContractError::RedisSnapshotPointerLiveCursorMismatch {
                    snapshot_end_entry_id,
                    live_cursor_entry_id,
                },
            );
        }

        let pointer = Self {
            schema_version: REDIS_STREAM_SCHEMA_VERSION.to_string(),
            chain_id,
            stream_key: required_redis_field("stream_key", stream_key.into())?,
            stream_id: required_redis_field("stream_id", stream_id.into())?,
            snapshot_id: required_redis_field("snapshot_id", snapshot_id.into())?,
            snapshot_start_entry_id,
            snapshot_end_entry_id,
            live_cursor_entry_id,
            completed_at_ms,
        };
        pointer.validate()?;
        Ok(pointer)
    }

    pub fn ensure_snapshot_retained(
        &self,
        oldest_retained_entry_id: &str,
    ) -> Result<(), BroadcasterContractError> {
        let oldest_retained = parse_redis_entry_id(oldest_retained_entry_id)?;
        let snapshot_start = parse_redis_entry_id(&self.snapshot_start_entry_id)?;
        if oldest_retained <= snapshot_start {
            return Ok(());
        }

        Err(BroadcasterContractError::RedisSnapshotRetentionGap {
            oldest_retained_entry_id: oldest_retained_entry_id.to_string(),
            snapshot_start_entry_id: self.snapshot_start_entry_id.clone(),
        })
    }

    fn validate(&self) -> Result<(), BroadcasterContractError> {
        ensure_redis_schema_version(&self.schema_version)?;
        required_redis_field("stream_key", self.stream_key.clone())?;
        required_redis_field("stream_id", self.stream_id.clone())?;
        required_redis_field("snapshot_id", self.snapshot_id.clone())?;
        required_redis_field(
            "snapshot_start_entry_id",
            self.snapshot_start_entry_id.clone(),
        )?;
        required_redis_field("snapshot_end_entry_id", self.snapshot_end_entry_id.clone())?;
        required_redis_field("live_cursor_entry_id", self.live_cursor_entry_id.clone())?;
        ensure_redis_entry_range(&self.snapshot_start_entry_id, &self.snapshot_end_entry_id)?;
        if self.live_cursor_entry_id != self.snapshot_end_entry_id {
            return Err(
                BroadcasterContractError::RedisSnapshotPointerLiveCursorMismatch {
                    snapshot_end_entry_id: self.snapshot_end_entry_id.clone(),
                    live_cursor_entry_id: self.live_cursor_entry_id.clone(),
                },
            );
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for BroadcasterRedisSnapshotPointer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WirePointer {
            schema_version: String,
            chain_id: u64,
            stream_key: String,
            stream_id: String,
            snapshot_id: String,
            snapshot_start_entry_id: String,
            snapshot_end_entry_id: String,
            live_cursor_entry_id: String,
            completed_at_ms: u64,
        }

        let wire = WirePointer::deserialize(deserializer)?;
        let pointer = Self {
            schema_version: wire.schema_version,
            chain_id: wire.chain_id,
            stream_key: wire.stream_key,
            stream_id: wire.stream_id,
            snapshot_id: wire.snapshot_id,
            snapshot_start_entry_id: wire.snapshot_start_entry_id,
            snapshot_end_entry_id: wire.snapshot_end_entry_id,
            live_cursor_entry_id: wire.live_cursor_entry_id,
            completed_at_ms: wire.completed_at_ms,
        };
        pointer.validate().map_err(de::Error::custom)?;
        Ok(pointer)
    }
}

fn redis_entry_requires_snapshot_id(kind: BroadcasterMessageKind) -> bool {
    matches!(
        kind,
        BroadcasterMessageKind::SnapshotStart
            | BroadcasterMessageKind::SnapshotChunk
            | BroadcasterMessageKind::SnapshotEnd
            | BroadcasterMessageKind::Heartbeat
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

fn ensure_redis_snapshot_start_message_seq(
    kind: BroadcasterMessageKind,
    message_seq: u64,
) -> Result<(), BroadcasterContractError> {
    if kind == BroadcasterMessageKind::SnapshotStart {
        return ensure_message_seq(Some(1), message_seq);
    }
    if message_seq == 1 {
        return Err(BroadcasterContractError::RedisFirstMessageNotSnapshotStart { kind });
    }
    Ok(())
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

fn redis_payload_global_block_number(block_numbers: &[u64]) -> Option<u64> {
    let (&first_block_number, remaining) = block_numbers.split_first()?;
    remaining
        .iter()
        .all(|block_number| *block_number == first_block_number)
        .then_some(first_block_number)
}

fn redis_payload_chain_block_numbers(payload: &BroadcasterPayload) -> Vec<u64> {
    match payload {
        BroadcasterPayload::SnapshotChunk(chunk) => chunk
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
        BroadcasterPayload::SnapshotStart(_)
        | BroadcasterPayload::SnapshotEnd(_)
        | BroadcasterPayload::Heartbeat(_) => Vec::new(),
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
        BroadcasterPayload::SnapshotStart(start) => Some(&start.snapshot_id),
        BroadcasterPayload::SnapshotChunk(chunk) => Some(&chunk.snapshot_id),
        BroadcasterPayload::SnapshotEnd(end) => Some(&end.snapshot_id),
        BroadcasterPayload::Update(_) => None,
        BroadcasterPayload::Heartbeat(heartbeat) => Some(&heartbeat.snapshot_id),
    }
}

fn redis_payload_chain_id(payload: &BroadcasterPayload) -> Option<u64> {
    match payload {
        BroadcasterPayload::SnapshotStart(start) => Some(start.chain_id),
        BroadcasterPayload::Heartbeat(heartbeat) => Some(heartbeat.chain_id),
        BroadcasterPayload::SnapshotChunk(_)
        | BroadcasterPayload::SnapshotEnd(_)
        | BroadcasterPayload::Update(_) => None,
    }
}

fn redis_payload_backend_scope(
    payload: &BroadcasterPayload,
) -> Result<Option<Vec<BroadcasterBackend>>, BroadcasterContractError> {
    match payload {
        BroadcasterPayload::SnapshotStart(start) => Ok(Some(start.backends.clone())),
        BroadcasterPayload::SnapshotChunk(chunk) => {
            redis_partition_backend_scope(&chunk.partitions)
        }
        BroadcasterPayload::SnapshotEnd(_) => Ok(None),
        BroadcasterPayload::Update(update) => redis_partition_backend_scope(&update.partitions),
        BroadcasterPayload::Heartbeat(heartbeat) => Ok(Some(
            heartbeat
                .backend_heads
                .iter()
                .map(|head| head.backend)
                .collect(),
        )),
    }
}

fn redis_partition_backend_scope<T: RedisPartitionBackend>(
    partitions: &[T],
) -> Result<Option<Vec<BroadcasterBackend>>, BroadcasterContractError> {
    let backends: Vec<_> = partitions
        .iter()
        .map(RedisPartitionBackend::backend)
        .collect();
    if backends.is_empty() {
        return Ok(Some(backends));
    }
    parse_redis_backend_scope(&redis_backend_scope(backends)?).map(Some)
}

trait RedisPartitionBackend {
    fn backend(&self) -> BroadcasterBackend;
}

impl RedisPartitionBackend for BroadcasterSnapshotPartition {
    fn backend(&self) -> BroadcasterBackend {
        self.backend
    }
}

impl RedisPartitionBackend for BroadcasterUpdatePartition {
    fn backend(&self) -> BroadcasterBackend {
        self.backend
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RedisEntryIdParts {
    millis: u64,
    sequence: u64,
}

fn parse_redis_entry_id(entry_id: &str) -> Result<RedisEntryIdParts, BroadcasterContractError> {
    let Some((millis, sequence)) = entry_id.split_once('-') else {
        return Err(BroadcasterContractError::InvalidRedisEntryId {
            entry_id: entry_id.to_string(),
        });
    };
    let Ok(millis) = millis.parse() else {
        return Err(BroadcasterContractError::InvalidRedisEntryId {
            entry_id: entry_id.to_string(),
        });
    };
    let Ok(sequence) = sequence.parse() else {
        return Err(BroadcasterContractError::InvalidRedisEntryId {
            entry_id: entry_id.to_string(),
        });
    };
    Ok(RedisEntryIdParts { millis, sequence })
}

fn ensure_redis_entry_range(
    snapshot_start_entry_id: &str,
    snapshot_end_entry_id: &str,
) -> Result<(), BroadcasterContractError> {
    let snapshot_start = parse_redis_entry_id(snapshot_start_entry_id)?;
    let snapshot_end = parse_redis_entry_id(snapshot_end_entry_id)?;
    if snapshot_start < snapshot_end {
        return Ok(());
    }

    Err(BroadcasterContractError::RedisSnapshotEntryRangeInvalid {
        snapshot_start_entry_id: snapshot_start_entry_id.to_string(),
        snapshot_end_entry_id: snapshot_end_entry_id.to_string(),
    })
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
    use std::any::Any;
    use std::collections::{BTreeMap, HashMap};

    use anyhow::{anyhow, Result};
    use chrono::NaiveDateTime;
    use num_bigint::BigUint;
    use tycho_simulation::{
        protocol::models::ProtocolComponent,
        tycho_common::{
            dto::ProtocolStateDelta,
            models::{token::Token, Chain},
            simulation::{
                errors::{SimulationError, TransitionError},
                protocol_sim::{Balances, GetAmountOutResult, ProtocolSim},
            },
            Bytes,
        },
    };

    use super::{BroadcasterRedisSnapshotPointer, BroadcasterRedisStreamEntry};
    use crate::broadcaster::{
        BroadcasterBackend, BroadcasterBackendHead, BroadcasterContractError, BroadcasterEnvelope,
        BroadcasterHeartbeat, BroadcasterMessageKind, BroadcasterPayload, BroadcasterRemovedPair,
        BroadcasterSnapshotChunk, BroadcasterSnapshotEnd, BroadcasterSnapshotPartition,
        BroadcasterSnapshotStart, BroadcasterStateDelta, BroadcasterStateEntry,
        BroadcasterUpdateMessage, BroadcasterUpdatePartition,
    };

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct RedisDummySim {
        label: String,
    }

    #[typetag::serde]
    impl ProtocolSim for RedisDummySim {
        fn fee(&self) -> f64 {
            0.0
        }

        fn spot_price(&self, _base: &Token, _quote: &Token) -> Result<f64, SimulationError> {
            Ok(1.0)
        }

        fn get_amount_out(
            &self,
            amount_in: BigUint,
            _token_in: &Token,
            _token_out: &Token,
        ) -> Result<GetAmountOutResult, SimulationError> {
            Ok(GetAmountOutResult::new(
                amount_in,
                BigUint::default(),
                Box::new(self.clone()),
            ))
        }

        fn get_limits(
            &self,
            _sell_token: Bytes,
            _buy_token: Bytes,
        ) -> Result<(BigUint, BigUint), SimulationError> {
            Ok((BigUint::from(10u32), BigUint::from(20u32)))
        }

        fn delta_transition(
            &mut self,
            _delta: ProtocolStateDelta,
            _tokens: &HashMap<Bytes, Token>,
            _balances: &Balances,
        ) -> Result<(), TransitionError> {
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn ProtocolSim> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn eq(&self, other: &dyn ProtocolSim) -> bool {
            other
                .as_any()
                .downcast_ref::<Self>()
                .is_some_and(|state| state == self)
        }
    }

    #[test]
    fn redis_stream_entry_derives_fields_from_envelope() -> Result<()> {
        let envelope = update_envelope("stream-1", 4)?;
        let entry = redis_entry(&envelope, vec![BroadcasterBackend::Native])?;

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
    fn redis_stream_entry_omits_block_number_for_rfq_only_update() -> Result<()> {
        let envelope = rfq_update_envelope("stream-1", 4, 321)?;
        let entry = redis_entry(&envelope, vec![BroadcasterBackend::Rfq])?;

        assert_eq!(entry.backend_scope, "rfq");
        assert_eq!(entry.block_number, None);

        let value = serde_json::to_value(&entry)?;
        assert!(value.get("block_number").is_none());

        let decoded: BroadcasterRedisStreamEntry = serde_json::from_value(value)?;
        assert_eq!(decoded, entry);

        Ok(())
    }

    #[test]
    fn redis_stream_entry_omits_block_number_for_rfq_only_snapshot_chunk() -> Result<()> {
        let envelope = rfq_snapshot_chunk_envelope("stream-1", 2, 321)?;
        let entry = redis_entry(&envelope, vec![BroadcasterBackend::Rfq])?;

        assert_eq!(entry.backend_scope, "rfq");
        assert_eq!(entry.block_number, None);

        let value = serde_json::to_value(&entry)?;
        assert!(value.get("block_number").is_none());

        let decoded: BroadcasterRedisStreamEntry = serde_json::from_value(value)?;
        assert_eq!(decoded, entry);

        Ok(())
    }

    #[test]
    fn redis_stream_entry_omits_block_number_for_mixed_backend_update_blocks() -> Result<()> {
        let envelope = mixed_backend_update_envelope("stream-1", 4, 124, 125)?;
        let entry = redis_entry(
            &envelope,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Vm],
        )?;

        assert_eq!(entry.backend_scope, "native,vm");
        assert_eq!(entry.block_number, None);

        let value = serde_json::to_value(&entry)?;
        assert!(value.get("block_number").is_none());

        let decoded: BroadcasterRedisStreamEntry = serde_json::from_value(value)?;
        assert_eq!(decoded, entry);

        Ok(())
    }

    #[test]
    fn redis_stream_entry_omits_block_number_for_mixed_backend_snapshot_blocks() -> Result<()> {
        let envelope = snapshot_chunk_envelope_with_partitions(
            "stream-1",
            2,
            0,
            vec![
                BroadcasterSnapshotPartition::new(
                    BroadcasterBackend::Native,
                    124,
                    vec![BroadcasterStateEntry::new(
                        "pool-native",
                        protocol_component("pool-native", "uniswap_v2"),
                        dummy_state("native-snapshot"),
                    )],
                    BTreeMap::new(),
                ),
                BroadcasterSnapshotPartition::new(
                    BroadcasterBackend::Vm,
                    125,
                    vec![BroadcasterStateEntry::new(
                        "pool-vm",
                        protocol_component("pool-vm", "vm:curve"),
                        dummy_state("vm-snapshot"),
                    )],
                    BTreeMap::new(),
                ),
            ],
        )?;
        let entry = redis_entry(
            &envelope,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Vm],
        )?;

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
        let entry = redis_entry(
            &envelope,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq],
        )?;

        assert_eq!(entry.backend_scope, "native,rfq");
        assert_eq!(entry.block_number, Some(124));

        Ok(())
    }

    #[test]
    fn redis_stream_entry_round_trips_with_stable_field_shape() -> Result<()> {
        let envelope = update_envelope("stream-1", 4)?;
        let entry = redis_entry(&envelope, vec![BroadcasterBackend::Native])?;

        let value = serde_json::to_value(&entry)?;

        assert_eq!(value["schema_version"], "1");
        assert_eq!(value["chain_id"], "8453");
        assert_eq!(value["stream_id"], "stream-1");
        assert_eq!(value["message_seq"], "4");
        assert_eq!(value["kind"], "update");
        assert!(value.get("snapshot_id").is_none());
        assert_eq!(value["backend_scope"], "native");
        assert_eq!(value["block_number"], "124");
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
            "kind": "snapshot_start",
            "snapshot_id": "snapshot-1",
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
        let mut value = redis_entry_value(
            &snapshot_end_envelope("stream-1", 2),
            vec![BroadcasterBackend::Native],
        )?;
        value
            .as_object_mut()
            .ok_or_else(|| anyhow!("redis entry should encode as object"))?
            .remove("snapshot_id");

        let error = redis_entry_decode_error(value, "snapshot entry without snapshot_id")?;
        assert!(error.to_string().contains("requires snapshot_id"));

        let mut value = redis_entry_value(
            &snapshot_end_envelope("stream-1", 2),
            vec![BroadcasterBackend::Native],
        )?;
        value["schema_version"] = serde_json::json!("2");

        let error = redis_entry_decode_error(value, "unsupported schema version")?;
        assert!(error
            .to_string()
            .contains("unsupported redis schema version"));

        Ok(())
    }

    #[test]
    fn redis_stream_entry_rejects_zero_message_sequence() -> Result<()> {
        let envelope = BroadcasterEnvelope::new(
            "stream-1",
            0,
            BroadcasterPayload::SnapshotEnd(BroadcasterSnapshotEnd::new("snapshot-1")),
        );

        let Err(error) = redis_entry(&envelope, vec![BroadcasterBackend::Native]) else {
            return Err(anyhow!("zero message_seq should fail"));
        };

        assert_eq!(error, BroadcasterContractError::RedisMessageSequenceZero);

        Ok(())
    }

    #[test]
    fn redis_stream_entry_requires_snapshot_start_to_begin_at_one() -> Result<()> {
        let envelope = snapshot_start_envelope("stream-1", 8453, "snapshot-1", 2, 1)?;

        let Err(error) = redis_entry(
            &envelope,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Vm],
        ) else {
            return Err(anyhow!("snapshot_start message_seq must start at one"));
        };

        assert_eq!(
            error,
            BroadcasterContractError::UnexpectedMessageSeq {
                expected: 1,
                found: 2,
            }
        );

        Ok(())
    }

    #[test]
    fn redis_stream_entry_rejects_non_snapshot_start_at_first_message_seq() -> Result<()> {
        let envelopes = [
            update_envelope("stream-1", 1)?,
            snapshot_end_envelope("stream-1", 1),
            heartbeat_envelope("stream-1", 8453, "snapshot-1", 1)?,
        ];

        for envelope in &envelopes {
            let kind = envelope.kind();
            let Err(error) = redis_entry(envelope, vec![BroadcasterBackend::Native]) else {
                return Err(anyhow!("{kind} at message_seq 1 should fail"));
            };

            assert_eq!(
                error,
                BroadcasterContractError::RedisFirstMessageNotSnapshotStart { kind }
            );
        }

        let mut value = redis_entry_value(
            &snapshot_end_envelope("stream-1", 2),
            vec![BroadcasterBackend::Native],
        )?;
        value["message_seq"] = serde_json::json!("1");

        let error = redis_entry_decode_error(value, "snapshot_end at message_seq 1")?;
        assert!(error
            .to_string()
            .contains("redis message_seq 1 must be snapshot_start"));

        Ok(())
    }

    #[test]
    fn redis_stream_entry_deserialization_rejects_empty_snapshot_id() -> Result<()> {
        let mut value = redis_entry_value(
            &update_envelope("stream-1", 4)?,
            vec![BroadcasterBackend::Native],
        )?;
        value["snapshot_id"] = serde_json::json!("");

        let error = redis_entry_decode_error(value, "empty snapshot_id")?;
        assert!(error.to_string().contains("snapshot_id must not be empty"));

        Ok(())
    }

    #[test]
    fn redis_stream_entry_requires_block_number_for_native_or_vm_state_entries() -> Result<()> {
        let mut value = redis_entry_value(
            &update_envelope("stream-1", 4)?,
            vec![BroadcasterBackend::Native],
        )?;
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
        let mut value = redis_entry_value(
            &update_envelope("stream-1", 4)?,
            vec![BroadcasterBackend::Native],
        )?;
        value["block_number"] = serde_json::json!("125");

        let error = redis_entry_decode_error(value, "mismatched block_number")?;

        assert!(error
            .to_string()
            .contains("redis block_number mismatch: entry 125, payload 124"));

        Ok(())
    }

    #[test]
    fn redis_stream_entry_rejects_block_number_for_rfq_only_payload() -> Result<()> {
        let mut value = redis_entry_value(
            &rfq_update_envelope("stream-1", 4, 321)?,
            vec![BroadcasterBackend::Rfq],
        )?;
        value["block_number"] = serde_json::json!("322");

        let error = redis_entry_decode_error(value, "unexpected RFQ block_number")?;

        assert!(error
            .to_string()
            .contains("redis stream field block_number is not allowed for this payload"));

        Ok(())
    }

    #[test]
    fn redis_stream_entry_rejects_block_number_for_divergent_backend_blocks() -> Result<()> {
        let mut value = redis_entry_value(
            &mixed_backend_update_envelope("stream-1", 4, 124, 125)?,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Vm],
        )?;
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
        let entry = redis_entry(&envelope, vec![BroadcasterBackend::Native])?;

        assert_eq!(entry.kind, BroadcasterMessageKind::Heartbeat);
        assert_eq!(entry.block_number, None);

        let value = serde_json::to_value(&entry)?;
        assert!(value.get("block_number").is_none());

        let decoded: BroadcasterRedisStreamEntry = serde_json::from_value(value)?;
        assert_eq!(decoded, entry);

        Ok(())
    }

    #[test]
    fn redis_stream_entry_rejects_snapshot_chunk_empty_payload_scope() -> Result<()> {
        let payload_json = serde_json::to_string(&BroadcasterEnvelope::new(
            "stream-1",
            2,
            BroadcasterPayload::SnapshotChunk(BroadcasterSnapshotChunk::new(
                "snapshot-1",
                0,
                Vec::new(),
            )?),
        ))?;

        let error = redis_entry_decode_error(
            serde_json::json!({
                "schema_version": "1",
                "chain_id": "8453",
                "stream_id": "stream-1",
                "message_seq": "2",
                "kind": "snapshot_chunk",
                "snapshot_id": "snapshot-1",
                "backend_scope": "native",
                "block_number": "124",
                "event_time_ms": "1710000000123",
                "payload_json": payload_json
            }),
            "empty snapshot chunk payload scope",
        )?;

        assert!(error
            .to_string()
            .contains("invalid redis backend_scope: native"));

        Ok(())
    }

    #[test]
    fn redis_snapshot_pointer_uses_snake_case_shape_and_validates_live_cursor() -> Result<()> {
        let pointer = BroadcasterRedisSnapshotPointer::new(
            8453,
            "dsolver:broadcaster:prod-base:8453:events",
            "chain-8453-stream-7",
            "chain-8453-snapshot-7",
            "1710000000000-0",
            "1710000000123-0",
            "1710000000123-0",
            1_710_000_000_123,
        )?;

        let value = serde_json::to_value(&pointer)?;

        assert_eq!(
            value,
            serde_json::json!({
                "schema_version": "1",
                "chain_id": 8453,
                "stream_key": "dsolver:broadcaster:prod-base:8453:events",
                "stream_id": "chain-8453-stream-7",
                "snapshot_id": "chain-8453-snapshot-7",
                "snapshot_start_entry_id": "1710000000000-0",
                "snapshot_end_entry_id": "1710000000123-0",
                "live_cursor_entry_id": "1710000000123-0",
                "completed_at_ms": 1710000000123u64
            })
        );
        let decoded: BroadcasterRedisSnapshotPointer = serde_json::from_value(value)?;
        assert_eq!(decoded, pointer);

        let Err(error) = BroadcasterRedisSnapshotPointer::new(
            8453,
            "dsolver:broadcaster:prod-base:8453:events",
            "chain-8453-stream-7",
            "chain-8453-snapshot-7",
            "1710000000000-0",
            "1710000000123-0",
            "1710000000999-0",
            1_710_000_000_123,
        ) else {
            return Err(anyhow!("pointer with mismatched live cursor should fail"));
        };

        assert_eq!(
            error,
            BroadcasterContractError::RedisSnapshotPointerLiveCursorMismatch {
                snapshot_end_entry_id: "1710000000123-0".to_string(),
                live_cursor_entry_id: "1710000000999-0".to_string(),
            }
        );

        Ok(())
    }

    #[test]
    fn redis_snapshot_pointer_deserialization_validates_invariants() -> Result<()> {
        let error = serde_json::from_value::<BroadcasterRedisSnapshotPointer>(serde_json::json!({
            "schema_version": "1",
            "chain_id": 8453,
            "stream_key": "dsolver:broadcaster:prod-base:8453:events",
            "stream_id": "chain-8453-stream-7",
            "snapshot_id": "chain-8453-snapshot-7",
            "snapshot_start_entry_id": "1710000000000-0",
            "snapshot_end_entry_id": "1710000000123-0",
            "live_cursor_entry_id": "1710000000999-0",
            "completed_at_ms": 1710000000123u64
        }))
        .err()
        .ok_or_else(|| anyhow!("mismatched live cursor should fail"))?;

        assert!(error
            .to_string()
            .contains("live cursor 1710000000999-0 must match snapshot end"));

        Ok(())
    }

    #[test]
    fn redis_snapshot_pointer_rejects_single_entry_range() -> Result<()> {
        let Err(error) = BroadcasterRedisSnapshotPointer::new(
            8453,
            "dsolver:broadcaster:prod-base:8453:events",
            "chain-8453-stream-7",
            "chain-8453-snapshot-7",
            "1710000000000-0",
            "1710000000000-0",
            "1710000000000-0",
            1_710_000_000_123,
        ) else {
            return Err(anyhow!("single-entry snapshot range should fail"));
        };

        assert_eq!(
            error,
            BroadcasterContractError::RedisSnapshotEntryRangeInvalid {
                snapshot_start_entry_id: "1710000000000-0".to_string(),
                snapshot_end_entry_id: "1710000000000-0".to_string(),
            }
        );

        Ok(())
    }

    #[test]
    fn redis_snapshot_pointer_detects_retention_gap() -> Result<()> {
        let pointer = BroadcasterRedisSnapshotPointer::new(
            8453,
            "dsolver:broadcaster:prod-base:8453:events",
            "chain-8453-stream-7",
            "chain-8453-snapshot-7",
            "1710000000000-0",
            "1710000000123-0",
            "1710000000123-0",
            1_710_000_000_123,
        )?;

        pointer.ensure_snapshot_retained("1709999999999-0")?;
        pointer.ensure_snapshot_retained("1710000000000-0")?;
        let Err(error) = pointer.ensure_snapshot_retained("1710000000001-0") else {
            return Err(anyhow!("retention past snapshot start should fail"));
        };

        assert_eq!(
            error,
            BroadcasterContractError::RedisSnapshotRetentionGap {
                oldest_retained_entry_id: "1710000000001-0".to_string(),
                snapshot_start_entry_id: "1710000000000-0".to_string(),
            }
        );

        Ok(())
    }

    fn redis_entry(
        envelope: &BroadcasterEnvelope,
        backends: Vec<BroadcasterBackend>,
    ) -> Result<BroadcasterRedisStreamEntry, BroadcasterContractError> {
        BroadcasterRedisStreamEntry::from_envelope(8453, 1_710_000_000_123, envelope, backends)
    }

    fn redis_entry_value(
        envelope: &BroadcasterEnvelope,
        backends: Vec<BroadcasterBackend>,
    ) -> Result<serde_json::Value> {
        serde_json::to_value(redis_entry(envelope, backends)?).map_err(Into::into)
    }

    fn redis_entry_decode_error(
        value: serde_json::Value,
        context: &'static str,
    ) -> Result<serde_json::Error> {
        serde_json::from_value::<BroadcasterRedisStreamEntry>(value)
            .err()
            .ok_or_else(|| anyhow!("{context} should fail"))
    }

    fn snapshot_start_envelope(
        stream_id: &str,
        chain_id: u64,
        snapshot_id: &str,
        message_seq: u64,
        total_chunks: u32,
    ) -> Result<BroadcasterEnvelope> {
        Ok(BroadcasterEnvelope::new(
            stream_id,
            message_seq,
            BroadcasterPayload::SnapshotStart(BroadcasterSnapshotStart::new(
                snapshot_id,
                chain_id,
                vec![BroadcasterBackend::Vm, BroadcasterBackend::Native],
                total_chunks,
            )?),
        ))
    }

    fn snapshot_chunk_envelope_with_partitions(
        stream_id: &str,
        message_seq: u64,
        chunk_index: u32,
        partitions: Vec<BroadcasterSnapshotPartition>,
    ) -> Result<BroadcasterEnvelope> {
        Ok(BroadcasterEnvelope::new(
            stream_id,
            message_seq,
            BroadcasterPayload::SnapshotChunk(BroadcasterSnapshotChunk::new(
                "snapshot-1",
                chunk_index,
                partitions,
            )?),
        ))
    }

    fn snapshot_end_envelope(stream_id: &str, message_seq: u64) -> BroadcasterEnvelope {
        BroadcasterEnvelope::new(
            stream_id,
            message_seq,
            BroadcasterPayload::SnapshotEnd(BroadcasterSnapshotEnd::new("snapshot-1")),
        )
    }

    fn update_envelope(stream_id: &str, message_seq: u64) -> Result<BroadcasterEnvelope> {
        Ok(BroadcasterEnvelope::new(
            stream_id,
            message_seq,
            BroadcasterPayload::Update(BroadcasterUpdateMessage::new(vec![
                BroadcasterUpdatePartition::new(
                    BroadcasterBackend::Native,
                    124,
                    vec![BroadcasterStateEntry::new(
                        "pool-new",
                        protocol_component("pool-new", "uniswap_v2"),
                        dummy_state("update-new"),
                    )],
                    vec![BroadcasterStateDelta::new(
                        "pool-existing",
                        BroadcasterBackend::Native,
                        dummy_state("update-existing"),
                    )],
                    vec![BroadcasterRemovedPair::new(
                        "pool-removed",
                        protocol_component("pool-removed", "uniswap_v2"),
                    )],
                    BTreeMap::new(),
                ),
            ])?),
        ))
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

    fn rfq_snapshot_chunk_envelope(
        stream_id: &str,
        message_seq: u64,
        block_number: u64,
    ) -> Result<BroadcasterEnvelope> {
        snapshot_chunk_envelope_with_partitions(
            stream_id,
            message_seq,
            0,
            vec![BroadcasterSnapshotPartition::new(
                BroadcasterBackend::Rfq,
                block_number,
                vec![BroadcasterStateEntry::new(
                    "pool-rfq",
                    protocol_component("pool-rfq", "rfq:hashflow"),
                    dummy_state("rfq-snapshot"),
                )],
                BTreeMap::new(),
            )],
        )
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

    fn heartbeat_envelope(
        stream_id: &str,
        chain_id: u64,
        snapshot_id: &str,
        message_seq: u64,
    ) -> Result<BroadcasterEnvelope> {
        Ok(BroadcasterEnvelope::new(
            stream_id,
            message_seq,
            BroadcasterPayload::Heartbeat(BroadcasterHeartbeat::new(
                chain_id,
                snapshot_id,
                vec![BroadcasterBackendHead::new(BroadcasterBackend::Native, 124)],
            )?),
        ))
    }

    fn protocol_component(component_id: &str, protocol_system: &str) -> ProtocolComponent {
        let token_a = token(1, "USDC");
        let token_b = token(2, "WETH");
        ProtocolComponent::new(
            Bytes::from(component_id.as_bytes().to_vec()),
            protocol_system.to_string(),
            "pool".to_string(),
            Chain::Ethereum,
            vec![token_a, token_b],
            Vec::new(),
            HashMap::new(),
            Bytes::from(vec![0u8; 32]),
            NaiveDateTime::default(),
        )
    }

    fn token(seed: u8, symbol: &str) -> Token {
        let address = Bytes::from(vec![seed; 20]);
        Token::new(&address, symbol, 18, 0, &[Some(0)], Chain::Ethereum, 100)
    }

    fn dummy_state(label: &str) -> Box<dyn ProtocolSim> {
        Box::new(RedisDummySim {
            label: label.to_string(),
        })
    }
}
