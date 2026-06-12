use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;

use serde::{
    de::{self, Deserializer},
    Deserialize, Serialize,
};
use tycho_simulation::{
    protocol::models::{ProtocolComponent, Update as TychoUpdate},
    tycho_client::feed::{
        synchronizer::StateSyncMessage, BlockHeader, FeedMessage, HeaderLike, SynchronizerState,
    },
    tycho_common::{models::token::Token, simulation::protocol_sim::ProtocolSim, Bytes},
};

use crate::models::protocol::ProtocolKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BroadcasterBackend {
    Native,
    Vm,
    Rfq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterTokenLookupRequest {
    pub chain_id: u64,
    pub addresses: Vec<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterTokenLookupResponse {
    pub tokens: Vec<BroadcasterTokenDto>,
    pub missing: Vec<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterTokenSnapshotResponse {
    pub chain_id: u64,
    pub tokens: Vec<BroadcasterTokenDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterSnapshotSessionResponse {
    pub chain_id: u64,
    pub session_id: u64,
    pub stream_id: String,
    pub snapshot_id: String,
    pub payload_count: u32,
    pub snapshot_chunk_count: u32,
    pub expires_in_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterTokenDto {
    pub address: Bytes,
    pub symbol: String,
    pub decimals: u32,
    pub tax: u64,
    pub gas: Vec<Option<u64>>,
    pub chain_id: u64,
    pub quality: u32,
}

impl BroadcasterTokenDto {
    pub fn into_token(
        self,
        chain: tycho_simulation::tycho_common::models::Chain,
    ) -> Result<Token, BroadcasterContractError> {
        if self.chain_id != chain.id() {
            return Err(BroadcasterContractError::TokenChainMismatch {
                expected: chain.id(),
                actual: self.chain_id,
            });
        }

        Ok(Token::new(
            &self.address,
            &self.symbol,
            self.decimals,
            self.tax,
            &self.gas,
            chain,
            self.quality,
        ))
    }
}

impl From<Token> for BroadcasterTokenDto {
    fn from(token: Token) -> Self {
        Self {
            address: token.address,
            symbol: token.symbol,
            decimals: token.decimals,
            tax: token.tax,
            gas: token.gas,
            chain_id: token.chain.id(),
            quality: token.quality,
        }
    }
}

impl BroadcasterBackend {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Vm => "vm",
            Self::Rfq => "rfq",
        }
    }
}

impl fmt::Display for BroadcasterBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BroadcasterMessageKind {
    SnapshotStart,
    SnapshotChunk,
    SnapshotEnd,
    Update,
    Heartbeat,
}

impl BroadcasterMessageKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SnapshotStart => "snapshot_start",
            Self::SnapshotChunk => "snapshot_chunk",
            Self::SnapshotEnd => "snapshot_end",
            Self::Update => "update",
            Self::Heartbeat => "heartbeat",
        }
    }
}

impl fmt::Display for BroadcasterMessageKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

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
            block_number: redis_entry_block_number(&envelope.payload)?,
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
        ensure_redis_block_number(self.kind, &backends, self.block_number)?;
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
#[serde(rename_all = "camelCase")]
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
        #[serde(rename_all = "camelCase")]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcasterEnvelope {
    pub stream_id: String,
    pub message_seq: u64,
    #[serde(flatten)]
    pub payload: BroadcasterPayload,
}

impl BroadcasterEnvelope {
    pub fn new(
        stream_id: impl Into<String>,
        message_seq: u64,
        payload: BroadcasterPayload,
    ) -> Self {
        Self {
            stream_id: stream_id.into(),
            message_seq,
            payload,
        }
    }

    pub const fn kind(&self) -> BroadcasterMessageKind {
        self.payload.kind()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BroadcasterPayload {
    SnapshotStart(BroadcasterSnapshotStart),
    SnapshotChunk(BroadcasterSnapshotChunk),
    SnapshotEnd(BroadcasterSnapshotEnd),
    Update(BroadcasterUpdateMessage),
    Heartbeat(BroadcasterHeartbeat),
}

impl BroadcasterPayload {
    pub const fn kind(&self) -> BroadcasterMessageKind {
        match self {
            Self::SnapshotStart(_) => BroadcasterMessageKind::SnapshotStart,
            Self::SnapshotChunk(_) => BroadcasterMessageKind::SnapshotChunk,
            Self::SnapshotEnd(_) => BroadcasterMessageKind::SnapshotEnd,
            Self::Update(_) => BroadcasterMessageKind::Update,
            Self::Heartbeat(_) => BroadcasterMessageKind::Heartbeat,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterSnapshotStart {
    pub snapshot_id: String,
    pub chain_id: u64,
    #[serde(deserialize_with = "deserialize_unique_backends")]
    pub backends: Vec<BroadcasterBackend>,
    pub total_chunks: u32,
}

impl BroadcasterSnapshotStart {
    pub fn new(
        snapshot_id: impl Into<String>,
        chain_id: u64,
        mut backends: Vec<BroadcasterBackend>,
        total_chunks: u32,
    ) -> Result<Self, BroadcasterContractError> {
        backends.sort();
        validate_snapshot_start_backends(&backends)?;
        Ok(Self {
            snapshot_id: snapshot_id.into(),
            chain_id,
            backends,
            total_chunks,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterSnapshotChunk {
    pub snapshot_id: String,
    pub chunk_index: u32,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_unique_snapshot_partitions"
    )]
    pub partitions: Vec<BroadcasterSnapshotPartition>,
}

impl BroadcasterSnapshotChunk {
    pub fn new(
        snapshot_id: impl Into<String>,
        chunk_index: u32,
        mut partitions: Vec<BroadcasterSnapshotPartition>,
    ) -> Result<Self, BroadcasterContractError> {
        partitions.sort_by_key(|partition| partition.backend);
        validate_snapshot_chunk_partitions(&partitions)?;
        Ok(Self {
            snapshot_id: snapshot_id.into(),
            chunk_index,
            partitions,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterSnapshotPartition {
    pub backend: BroadcasterBackend,
    pub block_number: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<BroadcasterProtocolMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub states: Vec<BroadcasterStateEntry>,
    // BTreeMap keeps the wire output deterministic for snapshots, deltas, and golden tests.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
}

impl BroadcasterSnapshotPartition {
    pub fn new(
        backend: BroadcasterBackend,
        block_number: u64,
        mut states: Vec<BroadcasterStateEntry>,
        sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
    ) -> Self {
        states.sort_by(|left, right| left.component_id.cmp(&right.component_id));
        Self {
            backend,
            block_number,
            messages: Vec::new(),
            states,
            sync_statuses,
        }
    }

    pub fn with_messages(
        backend: BroadcasterBackend,
        block_number: u64,
        messages: Vec<BroadcasterProtocolMessage>,
        sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
    ) -> Self {
        Self {
            backend,
            block_number,
            messages,
            states: Vec::new(),
            sync_statuses,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterSnapshotEnd {
    pub snapshot_id: String,
}

impl BroadcasterSnapshotEnd {
    pub fn new(snapshot_id: impl Into<String>) -> Self {
        Self {
            snapshot_id: snapshot_id.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterUpdateMessage {
    #[serde(
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_unique_update_partitions"
    )]
    pub partitions: Vec<BroadcasterUpdatePartition>,
}

impl BroadcasterUpdateMessage {
    pub fn new(
        mut partitions: Vec<BroadcasterUpdatePartition>,
    ) -> Result<Self, BroadcasterContractError> {
        partitions.sort_by_key(|partition| partition.backend);
        validate_update_partitions(&partitions)?;
        Ok(Self { partitions })
    }

    pub fn from_tycho_update(
        update: &TychoUpdate,
        known_backends: &HashMap<String, BroadcasterBackend>,
    ) -> Result<Self, BroadcasterContractError> {
        let mut partitions = BTreeMap::<BroadcasterBackend, UpdatePartitionBuilder>::new();

        for (component_id, component) in &update.new_pairs {
            let Some(state) = update.states.get(component_id) else {
                return Err(BroadcasterContractError::NewPairMissingState {
                    component_id: component_id.clone(),
                });
            };
            let backend = backend_for_component(component_id, component)?;
            partitions
                .entry(backend)
                .or_default()
                .new_pairs
                .push(BroadcasterStateEntry::new(
                    component_id.clone(),
                    component.clone(),
                    state.clone(),
                ));
        }

        for (component_id, state) in &update.states {
            if update.new_pairs.contains_key(component_id) {
                continue;
            }
            let Some(backend) = known_backends.get(component_id).copied() else {
                return Err(BroadcasterContractError::StateBackendMissing {
                    component_id: component_id.clone(),
                });
            };
            partitions
                .entry(backend)
                .or_default()
                .updated_states
                .push(BroadcasterStateDelta::new(
                    component_id.clone(),
                    backend,
                    state.clone(),
                ));
        }

        for (component_id, component) in &update.removed_pairs {
            let backend = backend_for_component(component_id, component)?;
            partitions
                .entry(backend)
                .or_default()
                .removed_pairs
                .push(BroadcasterRemovedPair::new(
                    component_id.clone(),
                    component.clone(),
                ));
        }

        for (protocol, status) in &update.sync_states {
            let backend = backend_for_sync_state(protocol)?;
            partitions.entry(backend).or_default().sync_statuses.insert(
                protocol.clone(),
                BroadcasterProtocolSyncStatus::from_synchronizer_state(status),
            );
        }

        let partitions = partitions
            .into_iter()
            .filter_map(|(backend, partition)| {
                if partition.is_empty() {
                    return None;
                }
                Some(BroadcasterUpdatePartition::new(
                    backend,
                    update.block_number_or_timestamp,
                    partition.new_pairs,
                    partition.updated_states,
                    partition.removed_pairs,
                    partition.sync_statuses,
                ))
            })
            .collect();

        Self::new(partitions)
    }

    pub fn from_tycho_feed_message(
        feed: &FeedMessage<BlockHeader>,
    ) -> Result<Self, BroadcasterContractError> {
        let mut partitions = BTreeMap::<BroadcasterBackend, Vec<BroadcasterProtocolMessage>>::new();
        let mut sync_statuses =
            BTreeMap::<BroadcasterBackend, BTreeMap<String, BroadcasterProtocolSyncStatus>>::new();
        let mut block_numbers = BTreeMap::<BroadcasterBackend, u64>::new();

        for (protocol, status) in &feed.sync_states {
            let backend = backend_for_sync_state(protocol)?;
            if let Some(block_number) = sync_state_block_number(status) {
                block_numbers
                    .entry(backend)
                    .and_modify(|current| *current = (*current).max(block_number))
                    .or_insert(block_number);
            }
            sync_statuses.entry(backend).or_default().insert(
                protocol.clone(),
                BroadcasterProtocolSyncStatus::from_synchronizer_state(status),
            );
        }

        for (protocol, message) in &feed.state_msgs {
            let backend = backend_for_sync_state(protocol)?;
            let sync_state = feed
                .sync_states
                .get(protocol)
                .cloned()
                .unwrap_or(SynchronizerState::Started);
            partitions
                .entry(backend)
                .or_default()
                .push(BroadcasterProtocolMessage::new(
                    protocol.clone(),
                    sync_state,
                    message.clone(),
                ));
        }

        let partition_backends = partitions
            .keys()
            .chain(sync_statuses.keys())
            .copied()
            .collect::<BTreeSet<_>>();
        let partitions = partition_backends
            .into_iter()
            .filter_map(|backend| {
                let messages = partitions.remove(&backend).unwrap_or_default();
                let statuses = sync_statuses.remove(&backend).unwrap_or_default();
                if messages.is_empty() && statuses.is_empty() {
                    return None;
                }
                let block_number = messages
                    .iter()
                    .map(|message| message.message.header.clone().block_number_or_timestamp())
                    .max()
                    .or_else(|| block_numbers.get(&backend).copied())
                    .unwrap_or_default();
                Some(BroadcasterUpdatePartition::with_messages(
                    backend,
                    block_number,
                    messages,
                    statuses,
                ))
            })
            .collect();

        Self::new(partitions)
    }
}

fn sync_state_block_number(state: &SynchronizerState) -> Option<u64> {
    match state {
        SynchronizerState::Ready(header)
        | SynchronizerState::Delayed(header)
        | SynchronizerState::Stale(header)
        | SynchronizerState::Advanced(header) => Some(header.clone().block_number_or_timestamp()),
        SynchronizerState::Started | SynchronizerState::Ended(_) => None,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterUpdatePartition {
    pub backend: BroadcasterBackend,
    pub block_number: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<BroadcasterProtocolMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub new_pairs: Vec<BroadcasterStateEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub updated_states: Vec<BroadcasterStateDelta>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_pairs: Vec<BroadcasterRemovedPair>,
    // BTreeMap keeps the wire output deterministic for snapshots, deltas, and golden tests.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
}

impl BroadcasterUpdatePartition {
    pub fn new(
        backend: BroadcasterBackend,
        block_number: u64,
        mut new_pairs: Vec<BroadcasterStateEntry>,
        mut updated_states: Vec<BroadcasterStateDelta>,
        mut removed_pairs: Vec<BroadcasterRemovedPair>,
        sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
    ) -> Self {
        new_pairs.sort_by(|left, right| left.component_id.cmp(&right.component_id));
        updated_states.sort_by(|left, right| left.component_id.cmp(&right.component_id));
        removed_pairs.sort_by(|left, right| left.component_id.cmp(&right.component_id));
        Self {
            backend,
            block_number,
            messages: Vec::new(),
            new_pairs,
            updated_states,
            removed_pairs,
            sync_statuses,
        }
    }

    pub fn with_messages(
        backend: BroadcasterBackend,
        block_number: u64,
        messages: Vec<BroadcasterProtocolMessage>,
        sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
    ) -> Self {
        Self {
            backend,
            block_number,
            messages,
            new_pairs: Vec::new(),
            updated_states: Vec::new(),
            removed_pairs: Vec::new(),
            sync_statuses,
        }
    }

    fn is_empty(&self) -> bool {
        self.messages.is_empty()
            && self.new_pairs.is_empty()
            && self.updated_states.is_empty()
            && self.removed_pairs.is_empty()
            && self.sync_statuses.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterProtocolMessage {
    pub protocol: String,
    pub sync_state: SynchronizerState,
    pub message: StateSyncMessage<BlockHeader>,
}

impl BroadcasterProtocolMessage {
    pub fn new(
        protocol: impl Into<String>,
        sync_state: SynchronizerState,
        message: StateSyncMessage<BlockHeader>,
    ) -> Self {
        Self {
            protocol: protocol.into(),
            sync_state,
            message,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterHeartbeat {
    pub chain_id: u64,
    pub snapshot_id: String,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_unique_backend_heads"
    )]
    pub backend_heads: Vec<BroadcasterBackendHead>,
}

impl BroadcasterHeartbeat {
    pub fn new(
        chain_id: u64,
        snapshot_id: impl Into<String>,
        mut backend_heads: Vec<BroadcasterBackendHead>,
    ) -> Result<Self, BroadcasterContractError> {
        backend_heads.sort_by_key(|head| head.backend);
        validate_heartbeat_backend_heads(&backend_heads)?;
        Ok(Self {
            chain_id,
            snapshot_id: snapshot_id.into(),
            backend_heads,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterBackendHead {
    pub backend: BroadcasterBackend,
    pub block_number: u64,
}

impl BroadcasterBackendHead {
    pub const fn new(backend: BroadcasterBackend, block_number: u64) -> Self {
        Self {
            backend,
            block_number,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterStateEntry {
    pub component_id: String,
    pub component: ProtocolComponent,
    pub state: Box<dyn ProtocolSim>,
}

impl BroadcasterStateEntry {
    pub fn new(
        component_id: impl Into<String>,
        component: ProtocolComponent,
        state: Box<dyn ProtocolSim>,
    ) -> Self {
        Self {
            component_id: component_id.into(),
            component,
            state,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterStateDelta {
    pub component_id: String,
    pub backend: BroadcasterBackend,
    pub state: Box<dyn ProtocolSim>,
}

impl BroadcasterStateDelta {
    pub fn new(
        component_id: impl Into<String>,
        backend: BroadcasterBackend,
        state: Box<dyn ProtocolSim>,
    ) -> Self {
        Self {
            component_id: component_id.into(),
            backend,
            state,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterRemovedPair {
    pub component_id: String,
    pub component: ProtocolComponent,
}

impl BroadcasterRemovedPair {
    pub fn new(component_id: impl Into<String>, component: ProtocolComponent) -> Self {
        Self {
            component_id: component_id.into(),
            component,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterProtocolSyncStatus {
    pub kind: BroadcasterProtocolSyncStatusKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block: Option<BroadcasterBlockRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl BroadcasterProtocolSyncStatus {
    pub fn from_synchronizer_state(state: &SynchronizerState) -> Self {
        match state {
            SynchronizerState::Started => Self {
                kind: BroadcasterProtocolSyncStatusKind::Started,
                block: None,
                reason: None,
            },
            SynchronizerState::Ready(block) => Self {
                kind: BroadcasterProtocolSyncStatusKind::Ready,
                block: Some(BroadcasterBlockRef::from(block)),
                reason: None,
            },
            SynchronizerState::Delayed(block) => Self {
                kind: BroadcasterProtocolSyncStatusKind::Delayed,
                block: Some(BroadcasterBlockRef::from(block)),
                reason: None,
            },
            SynchronizerState::Stale(block) => Self {
                kind: BroadcasterProtocolSyncStatusKind::Stale,
                block: Some(BroadcasterBlockRef::from(block)),
                reason: None,
            },
            SynchronizerState::Advanced(block) => Self {
                kind: BroadcasterProtocolSyncStatusKind::Advanced,
                block: Some(BroadcasterBlockRef::from(block)),
                reason: None,
            },
            SynchronizerState::Ended(reason) => Self {
                kind: BroadcasterProtocolSyncStatusKind::Ended,
                block: None,
                reason: Some(reason.clone()),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BroadcasterProtocolSyncStatusKind {
    Started,
    Ready,
    Delayed,
    Stale,
    Advanced,
    Ended,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterBlockRef {
    pub hash: Bytes,
    pub number: u64,
    pub parent_hash: Bytes,
    pub revert: bool,
    pub timestamp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial_block_index: Option<u32>,
}

impl From<&BlockHeader> for BroadcasterBlockRef {
    fn from(block: &BlockHeader) -> Self {
        Self {
            hash: block.hash.clone(),
            number: block.number,
            parent_hash: block.parent_hash.clone(),
            revert: block.revert,
            timestamp: block.timestamp,
            partial_block_index: block.partial_block_index,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum BroadcasterSubscriptionState {
    #[default]
    AwaitingSnapshot,
    Snapshot {
        stream_id: String,
        chain_id: u64,
        snapshot_id: String,
        declared_backends: HashSet<BroadcasterBackend>,
        observed_backends: HashSet<BroadcasterBackend>,
        next_chunk_index: u32,
        total_chunks: u32,
    },
    Live {
        stream_id: String,
        chain_id: u64,
        snapshot_id: String,
        declared_backends: HashSet<BroadcasterBackend>,
    },
}

impl BroadcasterSubscriptionState {
    const fn label(&self) -> &'static str {
        match self {
            Self::AwaitingSnapshot => "awaiting_snapshot",
            Self::Snapshot { .. } => "snapshot",
            Self::Live { .. } => "live",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BroadcasterSubscriptionEvent {
    SnapshotStarted {
        snapshot_id: String,
    },
    SnapshotChunkAccepted {
        snapshot_id: String,
        chunk_index: u32,
    },
    SnapshotCompleted {
        snapshot_id: String,
    },
    UpdateAccepted,
    HeartbeatAccepted,
}

#[derive(Debug, Clone)]
struct SnapshotObservationState {
    stream_id: String,
    chain_id: u64,
    snapshot_id: String,
    declared_backends: HashSet<BroadcasterBackend>,
    observed_backends: HashSet<BroadcasterBackend>,
    next_chunk_index: u32,
    total_chunks: u32,
}

#[derive(Debug, Clone, Default)]
pub struct BroadcasterSubscriptionTracker {
    state: BroadcasterSubscriptionState,
    next_message_seq: Option<u64>,
}

impl BroadcasterSubscriptionTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn state(&self) -> &BroadcasterSubscriptionState {
        &self.state
    }

    pub fn next_message_seq(&self) -> Option<u64> {
        self.next_message_seq
    }

    pub fn reset_for_reconnect(&mut self) {
        self.state = BroadcasterSubscriptionState::AwaitingSnapshot;
        self.next_message_seq = None;
    }

    pub fn observe(
        &mut self,
        envelope: &BroadcasterEnvelope,
    ) -> Result<BroadcasterSubscriptionEvent, BroadcasterContractError> {
        match self.state.clone() {
            BroadcasterSubscriptionState::AwaitingSnapshot => {
                self.observe_awaiting_snapshot(envelope)
            }
            BroadcasterSubscriptionState::Snapshot {
                stream_id,
                chain_id,
                snapshot_id,
                declared_backends,
                observed_backends,
                next_chunk_index,
                total_chunks,
            } => self.observe_snapshot(
                envelope,
                SnapshotObservationState {
                    stream_id,
                    chain_id,
                    snapshot_id,
                    declared_backends,
                    observed_backends,
                    next_chunk_index,
                    total_chunks,
                },
            ),
            BroadcasterSubscriptionState::Live {
                stream_id,
                chain_id,
                snapshot_id,
                declared_backends,
            } => self.observe_live(
                envelope,
                &stream_id,
                chain_id,
                &snapshot_id,
                declared_backends,
            ),
        }
    }

    fn observe_awaiting_snapshot(
        &mut self,
        envelope: &BroadcasterEnvelope,
    ) -> Result<BroadcasterSubscriptionEvent, BroadcasterContractError> {
        let BroadcasterPayload::SnapshotStart(start) = &envelope.payload else {
            return Err(BroadcasterContractError::ExpectedSnapshotStart {
                found: envelope.kind(),
            });
        };

        validate_snapshot_start_backends(&start.backends)?;
        let next_seq = next_message_seq(envelope.message_seq)?;
        self.state = BroadcasterSubscriptionState::Snapshot {
            stream_id: envelope.stream_id.clone(),
            chain_id: start.chain_id,
            snapshot_id: start.snapshot_id.clone(),
            declared_backends: start.backends.iter().copied().collect(),
            observed_backends: HashSet::new(),
            next_chunk_index: 0,
            total_chunks: start.total_chunks,
        };
        self.next_message_seq = Some(next_seq);
        Ok(BroadcasterSubscriptionEvent::SnapshotStarted {
            snapshot_id: start.snapshot_id.clone(),
        })
    }

    fn observe_snapshot(
        &mut self,
        envelope: &BroadcasterEnvelope,
        snapshot: SnapshotObservationState,
    ) -> Result<BroadcasterSubscriptionEvent, BroadcasterContractError> {
        let SnapshotObservationState {
            stream_id,
            chain_id,
            snapshot_id,
            declared_backends,
            observed_backends,
            next_chunk_index,
            total_chunks,
        } = snapshot;

        ensure_stream_id(&stream_id, &envelope.stream_id)?;
        ensure_message_seq(self.next_message_seq, envelope.message_seq)?;
        let next_seq = next_message_seq(envelope.message_seq)?;

        match &envelope.payload {
            BroadcasterPayload::SnapshotStart(_) => {
                Err(BroadcasterContractError::UnexpectedSnapshotStart {
                    state: self.state.label(),
                })
            }
            BroadcasterPayload::SnapshotChunk(chunk) => {
                if chunk.snapshot_id != snapshot_id {
                    return Err(BroadcasterContractError::UnexpectedSnapshotId {
                        expected: snapshot_id.clone(),
                        found: chunk.snapshot_id.clone(),
                    });
                }
                if next_chunk_index >= total_chunks {
                    return Err(BroadcasterContractError::ExtraSnapshotChunk {
                        total_chunks,
                        found: chunk.chunk_index,
                    });
                }
                validate_snapshot_chunk_partitions(&chunk.partitions)?;
                validate_declared_snapshot_chunk_backends(&chunk.partitions, &declared_backends)?;
                if chunk.chunk_index != next_chunk_index {
                    return Err(BroadcasterContractError::UnexpectedChunkIndex {
                        expected: next_chunk_index,
                        found: chunk.chunk_index,
                    });
                }
                let mut observed_backends = observed_backends;
                observed_backends
                    .extend(chunk.partitions.iter().map(|partition| partition.backend));
                self.state = BroadcasterSubscriptionState::Snapshot {
                    stream_id: stream_id.clone(),
                    chain_id,
                    snapshot_id: snapshot_id.clone(),
                    declared_backends,
                    observed_backends,
                    next_chunk_index: next_chunk_index + 1,
                    total_chunks,
                };
                self.next_message_seq = Some(next_seq);
                Ok(BroadcasterSubscriptionEvent::SnapshotChunkAccepted {
                    snapshot_id: chunk.snapshot_id.clone(),
                    chunk_index: chunk.chunk_index,
                })
            }
            BroadcasterPayload::SnapshotEnd(end) => {
                if end.snapshot_id != snapshot_id {
                    return Err(BroadcasterContractError::UnexpectedSnapshotId {
                        expected: snapshot_id.clone(),
                        found: end.snapshot_id.clone(),
                    });
                }
                if next_chunk_index != total_chunks {
                    return Err(BroadcasterContractError::SnapshotIncomplete {
                        expected_chunks: total_chunks,
                        observed_chunks: next_chunk_index,
                    });
                }
                ensure_all_declared_backends_observed(&declared_backends, &observed_backends)?;
                self.state = BroadcasterSubscriptionState::Live {
                    stream_id,
                    chain_id,
                    snapshot_id,
                    declared_backends,
                };
                self.next_message_seq = Some(next_seq);
                Ok(BroadcasterSubscriptionEvent::SnapshotCompleted {
                    snapshot_id: end.snapshot_id.clone(),
                })
            }
            BroadcasterPayload::Update(_) => {
                Err(BroadcasterContractError::UpdateBeforeSnapshotComplete)
            }
            BroadcasterPayload::Heartbeat(_) => {
                Err(BroadcasterContractError::HeartbeatBeforeSnapshotComplete)
            }
        }
    }

    fn observe_live(
        &mut self,
        envelope: &BroadcasterEnvelope,
        stream_id: &str,
        chain_id: u64,
        snapshot_id: &str,
        declared_backends: HashSet<BroadcasterBackend>,
    ) -> Result<BroadcasterSubscriptionEvent, BroadcasterContractError> {
        ensure_stream_id(stream_id, &envelope.stream_id)?;
        ensure_message_seq(self.next_message_seq, envelope.message_seq)?;
        let next_seq = next_message_seq(envelope.message_seq)?;

        match &envelope.payload {
            BroadcasterPayload::SnapshotStart(_) => {
                Err(BroadcasterContractError::UnexpectedSnapshotStart {
                    state: self.state.label(),
                })
            }
            BroadcasterPayload::SnapshotChunk(_) => {
                Err(BroadcasterContractError::UnexpectedSnapshotChunk)
            }
            BroadcasterPayload::SnapshotEnd(_) => {
                Err(BroadcasterContractError::UnexpectedSnapshotEnd)
            }
            BroadcasterPayload::Update(update) => {
                validate_update_partitions(&update.partitions)?;
                validate_declared_update_backends(&update.partitions, &declared_backends)?;
                self.next_message_seq = Some(next_seq);
                Ok(BroadcasterSubscriptionEvent::UpdateAccepted)
            }
            BroadcasterPayload::Heartbeat(heartbeat) => {
                validate_heartbeat_backend_heads(&heartbeat.backend_heads)?;
                validate_declared_heartbeat_backends(&heartbeat.backend_heads, &declared_backends)?;
                ensure_chain_id(chain_id, heartbeat.chain_id)?;
                ensure_snapshot_id(snapshot_id, &heartbeat.snapshot_id)?;
                self.next_message_seq = Some(next_seq);
                Ok(BroadcasterSubscriptionEvent::HeartbeatAccepted)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BroadcasterContractError {
    ExpectedSnapshotStart {
        found: BroadcasterMessageKind,
    },
    UnexpectedStreamId {
        expected: String,
        found: String,
    },
    UnexpectedChainId {
        expected: u64,
        found: u64,
    },
    UnexpectedMessageSeq {
        expected: u64,
        found: u64,
    },
    MessageSequenceOverflow {
        message_seq: u64,
    },
    UnexpectedSnapshotStart {
        state: &'static str,
    },
    UnexpectedSnapshotChunk,
    UnexpectedSnapshotEnd,
    UnexpectedSnapshotId {
        expected: String,
        found: String,
    },
    UnexpectedChunkIndex {
        expected: u32,
        found: u32,
    },
    ExtraSnapshotChunk {
        total_chunks: u32,
        found: u32,
    },
    SnapshotIncomplete {
        expected_chunks: u32,
        observed_chunks: u32,
    },
    MissingDeclaredSnapshotBackends {
        missing: Vec<BroadcasterBackend>,
    },
    UpdateBeforeSnapshotComplete,
    HeartbeatBeforeSnapshotComplete,
    EmptyUpdate,
    EmptyUpdatePartition {
        backend: BroadcasterBackend,
    },
    DuplicateBackendEntry {
        context: &'static str,
        backend: BroadcasterBackend,
    },
    UndeclaredBackendEntry {
        context: &'static str,
        backend: BroadcasterBackend,
    },
    NewPairMissingState {
        component_id: String,
    },
    UnknownComponentProtocol {
        component_id: String,
    },
    UnsupportedComponentProtocol {
        component_id: String,
        protocol: String,
    },
    StateBackendMissing {
        component_id: String,
    },
    UnknownSyncStateProtocol {
        protocol: String,
    },
    UnsupportedSyncStateProtocol {
        protocol: String,
    },
    PartitionContentBackendMismatch {
        context: &'static str,
        entry: String,
        partition_backend: BroadcasterBackend,
        entry_backend: BroadcasterBackend,
    },
    RedisEntryEmptyField {
        field: &'static str,
    },
    RedisUnsupportedSchemaVersion {
        found: String,
    },
    RedisEntryMissingSnapshotId {
        kind: BroadcasterMessageKind,
    },
    RedisMessageSequenceZero,
    RedisBackendScopeInvalid {
        backend_scope: String,
    },
    RedisPayloadJsonInvalid {
        message: String,
    },
    RedisPayloadKindMismatch {
        expected: BroadcasterMessageKind,
        found: BroadcasterMessageKind,
    },
    RedisBlockNumberMismatch {
        entry_block_number: u64,
        payload_block_number: u64,
    },
    InvalidRedisEntryId {
        entry_id: String,
    },
    RedisSnapshotEntryRangeInvalid {
        snapshot_start_entry_id: String,
        snapshot_end_entry_id: String,
    },
    RedisSnapshotPointerLiveCursorMismatch {
        snapshot_end_entry_id: String,
        live_cursor_entry_id: String,
    },
    RedisSnapshotRetentionGap {
        oldest_retained_entry_id: String,
        snapshot_start_entry_id: String,
    },
    TokenChainMismatch {
        expected: u64,
        actual: u64,
    },
}

fn fmt_unexpected_message_seq(
    f: &mut fmt::Formatter<'_>,
    expected: u64,
    found: u64,
) -> fmt::Result {
    write!(
        f,
        "unexpected message sequence: expected {expected}, found {found}"
    )
}

fn fmt_unexpected_snapshot_id(
    f: &mut fmt::Formatter<'_>,
    expected: &str,
    found: &str,
) -> fmt::Result {
    write!(
        f,
        "unexpected snapshot id: expected {expected}, found {found}"
    )
}

fn fmt_unexpected_chunk_index(
    f: &mut fmt::Formatter<'_>,
    expected: u32,
    found: u32,
) -> fmt::Result {
    write!(
        f,
        "unexpected snapshot chunk index: expected {expected}, found {found}"
    )
}

fn fmt_snapshot_incomplete(
    f: &mut fmt::Formatter<'_>,
    expected_chunks: u32,
    observed_chunks: u32,
) -> fmt::Result {
    write!(
        f,
        "snapshot incomplete: expected {expected_chunks} chunks, observed {observed_chunks}"
    )
}

fn fmt_unsupported_component_protocol(
    f: &mut fmt::Formatter<'_>,
    component_id: &str,
    protocol: &str,
) -> fmt::Result {
    write!(
        f,
        "component {component_id} uses unsupported broadcaster protocol {protocol}"
    )
}

fn fmt_missing_declared_snapshot_backends(
    f: &mut fmt::Formatter<'_>,
    missing: &[BroadcasterBackend],
) -> fmt::Result {
    write!(
        f,
        "snapshot is missing declared backends: {}",
        missing
            .iter()
            .map(|backend| backend.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn fmt_duplicate_backend_entry(
    f: &mut fmt::Formatter<'_>,
    context: &str,
    backend: BroadcasterBackend,
) -> fmt::Result {
    write!(f, "duplicate backend entry {backend:?} in {context}")
}

fn fmt_undeclared_backend_entry(
    f: &mut fmt::Formatter<'_>,
    context: &str,
    backend: BroadcasterBackend,
) -> fmt::Result {
    write!(
        f,
        "backend entry {backend:?} in {context} was not declared in snapshot_start.backends"
    )
}

fn fmt_partition_content_backend_mismatch(
    f: &mut fmt::Formatter<'_>,
    context: &str,
    entry: &str,
    partition_backend: BroadcasterBackend,
    entry_backend: BroadcasterBackend,
) -> fmt::Result {
    write!(
        f,
        "{context} entry {entry} belongs to {entry_backend:?} but partition backend is {partition_backend:?}"
    )
}

fn fmt_unknown_component_protocol(f: &mut fmt::Formatter<'_>, component_id: &str) -> fmt::Result {
    write!(
        f,
        "component {component_id} could not be classified for the broadcaster"
    )
}

fn fmt_state_backend_missing(f: &mut fmt::Formatter<'_>, component_id: &str) -> fmt::Result {
    write!(
        f,
        "state {component_id} is missing a known broadcaster backend"
    )
}

fn fmt_unknown_sync_state_protocol(f: &mut fmt::Formatter<'_>, protocol: &str) -> fmt::Result {
    write!(
        f,
        "sync state {protocol} could not be classified for the broadcaster"
    )
}

fn fmt_unsupported_sync_state_protocol(f: &mut fmt::Formatter<'_>, protocol: &str) -> fmt::Result {
    write!(
        f,
        "sync state {protocol} uses an unsupported broadcaster backend"
    )
}

fn fmt_redis_contract_error(
    f: &mut fmt::Formatter<'_>,
    error: &BroadcasterContractError,
) -> fmt::Result {
    match error {
        BroadcasterContractError::RedisEntryEmptyField { field } => {
            write!(f, "redis stream field {field} must not be empty")
        }
        BroadcasterContractError::RedisUnsupportedSchemaVersion { found } => {
            write!(f, "unsupported redis schema version: {found}")
        }
        BroadcasterContractError::RedisEntryMissingSnapshotId { kind } => {
            write!(f, "redis {kind} entry requires snapshot_id")
        }
        BroadcasterContractError::RedisMessageSequenceZero => {
            write!(f, "redis message_seq must start at 1")
        }
        BroadcasterContractError::RedisBackendScopeInvalid { backend_scope } => {
            write!(f, "invalid redis backend_scope: {backend_scope}")
        }
        BroadcasterContractError::RedisPayloadJsonInvalid { message } => {
            write!(f, "redis payload_json is invalid: {message}")
        }
        BroadcasterContractError::RedisPayloadKindMismatch { expected, found } => {
            write!(
                f,
                "redis payload kind mismatch: expected {expected}, found {found}"
            )
        }
        BroadcasterContractError::RedisBlockNumberMismatch {
            entry_block_number,
            payload_block_number,
        } => write!(
            f,
            "redis block_number mismatch: entry {entry_block_number}, payload {payload_block_number}"
        ),
        BroadcasterContractError::InvalidRedisEntryId { entry_id } => {
            write!(f, "invalid redis stream entry id: {entry_id}")
        }
        BroadcasterContractError::RedisSnapshotEntryRangeInvalid {
            snapshot_start_entry_id,
            snapshot_end_entry_id,
        } => write!(
            f,
            "redis snapshot range is invalid: start {snapshot_start_entry_id}, end {snapshot_end_entry_id}"
        ),
        BroadcasterContractError::RedisSnapshotPointerLiveCursorMismatch {
            snapshot_end_entry_id,
            live_cursor_entry_id,
        } => write!(
            f,
            "redis snapshot pointer live cursor {live_cursor_entry_id} must match snapshot end {snapshot_end_entry_id}"
        ),
        BroadcasterContractError::RedisSnapshotRetentionGap {
            oldest_retained_entry_id,
            snapshot_start_entry_id,
        } => write!(
            f,
            "redis retention starts at {oldest_retained_entry_id}, after latest snapshot start {snapshot_start_entry_id}"
        ),
        _ => f.write_str("redis contract error"),
    }
}

fn fmt_protocol_classification_error(
    f: &mut fmt::Formatter<'_>,
    error: &BroadcasterContractError,
) -> fmt::Result {
    match error {
        BroadcasterContractError::UnknownComponentProtocol { component_id } => {
            fmt_unknown_component_protocol(f, component_id)
        }
        BroadcasterContractError::UnsupportedComponentProtocol {
            component_id,
            protocol,
        } => fmt_unsupported_component_protocol(f, component_id, protocol),
        BroadcasterContractError::StateBackendMissing { component_id } => {
            fmt_state_backend_missing(f, component_id)
        }
        BroadcasterContractError::UnknownSyncStateProtocol { protocol } => {
            fmt_unknown_sync_state_protocol(f, protocol)
        }
        BroadcasterContractError::UnsupportedSyncStateProtocol { protocol } => {
            fmt_unsupported_sync_state_protocol(f, protocol)
        }
        _ => f.write_str("protocol classification error"),
    }
}

fn fmt_snapshot_flow_error(
    f: &mut fmt::Formatter<'_>,
    error: &BroadcasterContractError,
) -> fmt::Result {
    match error {
        BroadcasterContractError::UnexpectedSnapshotStart { state } => {
            write!(f, "unexpected snapshot_start while in {state}")
        }
        BroadcasterContractError::UnexpectedSnapshotChunk => {
            write!(f, "unexpected snapshot_chunk while live")
        }
        BroadcasterContractError::UnexpectedSnapshotEnd => {
            write!(f, "unexpected snapshot_end while live")
        }
        BroadcasterContractError::UnexpectedSnapshotId { expected, found } => {
            fmt_unexpected_snapshot_id(f, expected, found)
        }
        BroadcasterContractError::UnexpectedChunkIndex { expected, found } => {
            fmt_unexpected_chunk_index(f, *expected, *found)
        }
        BroadcasterContractError::ExtraSnapshotChunk {
            total_chunks,
            found,
        } => write!(
            f,
            "received extra snapshot chunk {found} after declared total of {total_chunks}"
        ),
        BroadcasterContractError::SnapshotIncomplete {
            expected_chunks,
            observed_chunks,
        } => fmt_snapshot_incomplete(f, *expected_chunks, *observed_chunks),
        BroadcasterContractError::MissingDeclaredSnapshotBackends { missing } => {
            fmt_missing_declared_snapshot_backends(f, missing)
        }
        _ => f.write_str("snapshot flow error"),
    }
}

impl fmt::Display for BroadcasterContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExpectedSnapshotStart { found } => {
                write!(f, "expected snapshot_start, found {found}")
            }
            Self::UnexpectedStreamId { expected, found } => write!(
                f,
                "unexpected stream id: expected {expected}, found {found}"
            ),
            Self::UnexpectedChainId { expected, found } => {
                write!(f, "unexpected chain id: expected {expected}, found {found}")
            }
            Self::UnexpectedMessageSeq { expected, found } => {
                fmt_unexpected_message_seq(f, *expected, *found)
            }
            Self::MessageSequenceOverflow { message_seq } => {
                write!(f, "message sequence overflow at {message_seq}")
            }
            Self::UnexpectedSnapshotStart { .. }
            | Self::UnexpectedSnapshotChunk
            | Self::UnexpectedSnapshotEnd
            | Self::UnexpectedSnapshotId { .. }
            | Self::UnexpectedChunkIndex { .. }
            | Self::ExtraSnapshotChunk { .. }
            | Self::SnapshotIncomplete { .. }
            | Self::MissingDeclaredSnapshotBackends { .. } => fmt_snapshot_flow_error(f, self),
            Self::UpdateBeforeSnapshotComplete => {
                write!(f, "received update before snapshot bootstrap completed")
            }
            Self::HeartbeatBeforeSnapshotComplete => {
                write!(f, "received heartbeat before snapshot bootstrap completed")
            }
            Self::EmptyUpdate => write!(f, "update message must contain at least one partition"),
            Self::EmptyUpdatePartition { backend } => {
                write!(
                    f,
                    "update partition for {backend:?} must contain state or sync data"
                )
            }
            Self::DuplicateBackendEntry { context, backend } => {
                fmt_duplicate_backend_entry(f, context, *backend)
            }
            Self::UndeclaredBackendEntry { context, backend } => {
                fmt_undeclared_backend_entry(f, context, *backend)
            }
            Self::NewPairMissingState { component_id } => {
                write!(f, "new pair {component_id} is missing its state payload")
            }
            Self::UnknownComponentProtocol { .. }
            | Self::UnsupportedComponentProtocol { .. }
            | Self::StateBackendMissing { .. }
            | Self::UnknownSyncStateProtocol { .. }
            | Self::UnsupportedSyncStateProtocol { .. } => {
                fmt_protocol_classification_error(f, self)
            }
            Self::PartitionContentBackendMismatch {
                context,
                entry,
                partition_backend,
                entry_backend,
            } => fmt_partition_content_backend_mismatch(
                f,
                context,
                entry,
                *partition_backend,
                *entry_backend,
            ),
            Self::RedisEntryEmptyField { .. }
            | Self::RedisUnsupportedSchemaVersion { .. }
            | Self::RedisEntryMissingSnapshotId { .. }
            | Self::RedisMessageSequenceZero
            | Self::RedisBackendScopeInvalid { .. }
            | Self::RedisPayloadJsonInvalid { .. }
            | Self::RedisPayloadKindMismatch { .. }
            | Self::RedisBlockNumberMismatch { .. }
            | Self::InvalidRedisEntryId { .. }
            | Self::RedisSnapshotEntryRangeInvalid { .. }
            | Self::RedisSnapshotPointerLiveCursorMismatch { .. }
            | Self::RedisSnapshotRetentionGap { .. } => fmt_redis_contract_error(f, self),
            Self::TokenChainMismatch { expected, actual } => {
                write!(
                    f,
                    "token chain id mismatch: expected {expected}, got {actual}"
                )
            }
        }
    }
}

impl std::error::Error for BroadcasterContractError {}

#[derive(Default)]
struct UpdatePartitionBuilder {
    new_pairs: Vec<BroadcasterStateEntry>,
    updated_states: Vec<BroadcasterStateDelta>,
    removed_pairs: Vec<BroadcasterRemovedPair>,
    sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
}

impl UpdatePartitionBuilder {
    fn is_empty(&self) -> bool {
        self.new_pairs.is_empty()
            && self.updated_states.is_empty()
            && self.removed_pairs.is_empty()
            && self.sync_statuses.is_empty()
    }
}

fn ensure_stream_id(expected: &str, found: &str) -> Result<(), BroadcasterContractError> {
    if expected == found {
        Ok(())
    } else {
        Err(BroadcasterContractError::UnexpectedStreamId {
            expected: expected.to_string(),
            found: found.to_string(),
        })
    }
}

fn ensure_chain_id(expected: u64, found: u64) -> Result<(), BroadcasterContractError> {
    if expected == found {
        Ok(())
    } else {
        Err(BroadcasterContractError::UnexpectedChainId { expected, found })
    }
}

fn ensure_snapshot_id(expected: &str, found: &str) -> Result<(), BroadcasterContractError> {
    if expected == found {
        Ok(())
    } else {
        Err(BroadcasterContractError::UnexpectedSnapshotId {
            expected: expected.to_string(),
            found: found.to_string(),
        })
    }
}

fn ensure_message_seq(expected: Option<u64>, found: u64) -> Result<(), BroadcasterContractError> {
    if expected == Some(found) {
        Ok(())
    } else {
        Err(BroadcasterContractError::UnexpectedMessageSeq {
            expected: expected.unwrap_or(found),
            found,
        })
    }
}

fn next_message_seq(message_seq: u64) -> Result<u64, BroadcasterContractError> {
    message_seq
        .checked_add(1)
        .ok_or(BroadcasterContractError::MessageSequenceOverflow { message_seq })
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
        ensure_message_seq(Some(1), message_seq)
    } else {
        Ok(())
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

fn ensure_redis_block_number(
    kind: BroadcasterMessageKind,
    backends: &[BroadcasterBackend],
    block_number: Option<u64>,
) -> Result<(), BroadcasterContractError> {
    let requires_block = matches!(
        kind,
        BroadcasterMessageKind::SnapshotChunk | BroadcasterMessageKind::Update
    ) && backends
        .iter()
        .any(|backend| matches!(backend, BroadcasterBackend::Native | BroadcasterBackend::Vm));
    if requires_block && block_number.is_none() {
        return Err(BroadcasterContractError::RedisEntryEmptyField {
            field: "block_number",
        });
    }
    Ok(())
}

fn ensure_redis_payload_block_number(
    entry_block_number: Option<u64>,
    payload: &BroadcasterPayload,
) -> Result<(), BroadcasterContractError> {
    let Some(entry_block_number) = entry_block_number else {
        return Ok(());
    };
    for payload_block_number in redis_payload_block_numbers(payload) {
        if payload_block_number != entry_block_number {
            return Err(BroadcasterContractError::RedisBlockNumberMismatch {
                entry_block_number,
                payload_block_number,
            });
        }
    }
    Ok(())
}

fn redis_entry_block_number(
    payload: &BroadcasterPayload,
) -> Result<Option<u64>, BroadcasterContractError> {
    let mut block_numbers = redis_payload_block_numbers(payload).into_iter();
    let Some(first_block_number) = block_numbers.next() else {
        return Ok(None);
    };
    for payload_block_number in block_numbers {
        if payload_block_number != first_block_number {
            return Err(BroadcasterContractError::RedisBlockNumberMismatch {
                entry_block_number: first_block_number,
                payload_block_number,
            });
        }
    }
    Ok(Some(first_block_number))
}

fn redis_payload_block_numbers(payload: &BroadcasterPayload) -> Vec<u64> {
    match payload {
        BroadcasterPayload::SnapshotChunk(chunk) => chunk
            .partitions
            .iter()
            .map(|partition| partition.block_number)
            .collect(),
        BroadcasterPayload::Update(update) => update
            .partitions
            .iter()
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

fn backend_for_component(
    component_id: &str,
    component: &ProtocolComponent,
) -> Result<BroadcasterBackend, BroadcasterContractError> {
    let Some(kind) = ProtocolKind::from_component(component) else {
        return Err(BroadcasterContractError::UnknownComponentProtocol {
            component_id: component_id.to_string(),
        });
    };
    Ok(backend_for_kind(kind))
}

fn backend_for_sync_state(protocol: &str) -> Result<BroadcasterBackend, BroadcasterContractError> {
    let Some(kind) = ProtocolKind::from_sync_state_key(protocol) else {
        return Err(BroadcasterContractError::UnknownSyncStateProtocol {
            protocol: protocol.to_string(),
        });
    };
    Ok(backend_for_kind(kind))
}

fn backend_for_kind(kind: ProtocolKind) -> BroadcasterBackend {
    match kind {
        ProtocolKind::Curve | ProtocolKind::BalancerV2 | ProtocolKind::MaverickV2 => {
            BroadcasterBackend::Vm
        }
        ProtocolKind::Hashflow | ProtocolKind::Bebop | ProtocolKind::Liquorice => {
            BroadcasterBackend::Rfq
        }
        _ => BroadcasterBackend::Native,
    }
}

fn deserialize_unique_backends<'de, D>(deserializer: D) -> Result<Vec<BroadcasterBackend>, D::Error>
where
    D: Deserializer<'de>,
{
    let mut backends = Vec::<BroadcasterBackend>::deserialize(deserializer)?;
    backends.sort();
    validate_snapshot_start_backends(&backends).map_err(de::Error::custom)?;
    Ok(backends)
}

fn deserialize_unique_snapshot_partitions<'de, D>(
    deserializer: D,
) -> Result<Vec<BroadcasterSnapshotPartition>, D::Error>
where
    D: Deserializer<'de>,
{
    let mut partitions = Vec::<BroadcasterSnapshotPartition>::deserialize(deserializer)?;
    partitions.sort_by_key(|partition| partition.backend);
    validate_snapshot_chunk_partitions(&partitions).map_err(de::Error::custom)?;
    Ok(partitions)
}

fn deserialize_unique_update_partitions<'de, D>(
    deserializer: D,
) -> Result<Vec<BroadcasterUpdatePartition>, D::Error>
where
    D: Deserializer<'de>,
{
    let mut partitions = Vec::<BroadcasterUpdatePartition>::deserialize(deserializer)?;
    partitions.sort_by_key(|partition| partition.backend);
    validate_update_partitions(&partitions).map_err(de::Error::custom)?;
    Ok(partitions)
}

fn deserialize_unique_backend_heads<'de, D>(
    deserializer: D,
) -> Result<Vec<BroadcasterBackendHead>, D::Error>
where
    D: Deserializer<'de>,
{
    let mut heads = Vec::<BroadcasterBackendHead>::deserialize(deserializer)?;
    heads.sort_by_key(|head| head.backend);
    validate_heartbeat_backend_heads(&heads).map_err(de::Error::custom)?;
    Ok(heads)
}

fn validate_snapshot_start_backends(
    backends: &[BroadcasterBackend],
) -> Result<(), BroadcasterContractError> {
    validate_unique_backends("snapshot_start.backends", backends)
}

fn validate_snapshot_chunk_partitions(
    partitions: &[BroadcasterSnapshotPartition],
) -> Result<(), BroadcasterContractError> {
    validate_unique_partition_backends("snapshot_chunk.partitions", partitions)?;
    validate_snapshot_partition_contents(partitions)
}

fn validate_update_partitions(
    partitions: &[BroadcasterUpdatePartition],
) -> Result<(), BroadcasterContractError> {
    if partitions.is_empty() {
        return Err(BroadcasterContractError::EmptyUpdate);
    }
    validate_unique_update_backends("update.partitions", partitions)?;
    validate_non_empty_update_partitions(partitions)?;
    validate_update_partition_contents(partitions)
}

fn validate_heartbeat_backend_heads(
    heads: &[BroadcasterBackendHead],
) -> Result<(), BroadcasterContractError> {
    validate_unique_backend_heads("heartbeat.backend_heads", heads)
}

fn validate_declared_snapshot_chunk_backends(
    partitions: &[BroadcasterSnapshotPartition],
    declared_backends: &HashSet<BroadcasterBackend>,
) -> Result<(), BroadcasterContractError> {
    validate_declared_backend_entries(
        "snapshot_chunk.partitions",
        partitions.iter().map(|partition| partition.backend),
        declared_backends,
    )
}

fn validate_declared_update_backends(
    partitions: &[BroadcasterUpdatePartition],
    declared_backends: &HashSet<BroadcasterBackend>,
) -> Result<(), BroadcasterContractError> {
    validate_declared_backend_entries(
        "update.partitions",
        partitions.iter().map(|partition| partition.backend),
        declared_backends,
    )
}

fn validate_declared_heartbeat_backends(
    heads: &[BroadcasterBackendHead],
    declared_backends: &HashSet<BroadcasterBackend>,
) -> Result<(), BroadcasterContractError> {
    validate_declared_backend_entries(
        "heartbeat.backend_heads",
        heads.iter().map(|head| head.backend),
        declared_backends,
    )
}

fn validate_unique_backends(
    context: &'static str,
    backends: &[BroadcasterBackend],
) -> Result<(), BroadcasterContractError> {
    validate_unique_backend_entries(context, backends.iter().copied())
}

fn validate_unique_partition_backends(
    context: &'static str,
    partitions: &[BroadcasterSnapshotPartition],
) -> Result<(), BroadcasterContractError> {
    validate_unique_backend_entries(
        context,
        partitions.iter().map(|partition| partition.backend),
    )
}

fn validate_unique_update_backends(
    context: &'static str,
    partitions: &[BroadcasterUpdatePartition],
) -> Result<(), BroadcasterContractError> {
    validate_unique_backend_entries(
        context,
        partitions.iter().map(|partition| partition.backend),
    )
}

fn validate_non_empty_update_partitions(
    partitions: &[BroadcasterUpdatePartition],
) -> Result<(), BroadcasterContractError> {
    for partition in partitions {
        if partition.is_empty() {
            return Err(BroadcasterContractError::EmptyUpdatePartition {
                backend: partition.backend,
            });
        }
    }
    Ok(())
}

fn validate_unique_backend_heads(
    context: &'static str,
    heads: &[BroadcasterBackendHead],
) -> Result<(), BroadcasterContractError> {
    validate_unique_backend_entries(context, heads.iter().map(|head| head.backend))
}

fn validate_unique_backend_entries(
    context: &'static str,
    backends: impl IntoIterator<Item = BroadcasterBackend>,
) -> Result<(), BroadcasterContractError> {
    let mut seen = HashSet::new();
    for backend in backends {
        if !seen.insert(backend) {
            return Err(BroadcasterContractError::DuplicateBackendEntry { context, backend });
        }
    }
    Ok(())
}

fn validate_declared_backend_entries(
    context: &'static str,
    backends: impl IntoIterator<Item = BroadcasterBackend>,
    declared_backends: &HashSet<BroadcasterBackend>,
) -> Result<(), BroadcasterContractError> {
    for backend in backends {
        if !declared_backends.contains(&backend) {
            return Err(BroadcasterContractError::UndeclaredBackendEntry { context, backend });
        }
    }
    Ok(())
}

fn ensure_all_declared_backends_observed(
    declared_backends: &HashSet<BroadcasterBackend>,
    observed_backends: &HashSet<BroadcasterBackend>,
) -> Result<(), BroadcasterContractError> {
    let mut missing = declared_backends
        .difference(observed_backends)
        .copied()
        .collect::<Vec<_>>();
    missing.sort();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(BroadcasterContractError::MissingDeclaredSnapshotBackends { missing })
    }
}

fn validate_snapshot_partition_contents(
    partitions: &[BroadcasterSnapshotPartition],
) -> Result<(), BroadcasterContractError> {
    for partition in partitions {
        for message in &partition.messages {
            let backend = backend_for_sync_state(&message.protocol)?;
            validate_partition_content_backend(
                "snapshot_chunk.partitions.messages",
                &message.protocol,
                partition.backend,
                backend,
            )?;
        }
        for state in &partition.states {
            let backend = backend_for_component(&state.component_id, &state.component)?;
            validate_partition_content_backend(
                "snapshot_chunk.partitions.states",
                &state.component_id,
                partition.backend,
                backend,
            )?;
        }
        for protocol in partition.sync_statuses.keys() {
            let backend = backend_for_sync_state(protocol)?;
            validate_partition_content_backend(
                "snapshot_chunk.partitions.sync_statuses",
                protocol,
                partition.backend,
                backend,
            )?;
        }
    }
    Ok(())
}

fn validate_update_partition_contents(
    partitions: &[BroadcasterUpdatePartition],
) -> Result<(), BroadcasterContractError> {
    for partition in partitions {
        for message in &partition.messages {
            let backend = backend_for_sync_state(&message.protocol)?;
            validate_partition_content_backend(
                "update.partitions.messages",
                &message.protocol,
                partition.backend,
                backend,
            )?;
        }
        for state in &partition.new_pairs {
            let backend = backend_for_component(&state.component_id, &state.component)?;
            validate_partition_content_backend(
                "update.partitions.new_pairs",
                &state.component_id,
                partition.backend,
                backend,
            )?;
        }
        for state in &partition.updated_states {
            validate_partition_content_backend(
                "update.partitions.updated_states",
                &state.component_id,
                partition.backend,
                state.backend,
            )?;
        }
        for removed in &partition.removed_pairs {
            let backend = backend_for_component(&removed.component_id, &removed.component)?;
            validate_partition_content_backend(
                "update.partitions.removed_pairs",
                &removed.component_id,
                partition.backend,
                backend,
            )?;
        }
        for protocol in partition.sync_statuses.keys() {
            let backend = backend_for_sync_state(protocol)?;
            validate_partition_content_backend(
                "update.partitions.sync_statuses",
                protocol,
                partition.backend,
                backend,
            )?;
        }
    }
    Ok(())
}

fn validate_partition_content_backend(
    context: &'static str,
    entry: &str,
    partition_backend: BroadcasterBackend,
    entry_backend: BroadcasterBackend,
) -> Result<(), BroadcasterContractError> {
    if partition_backend == entry_backend {
        Ok(())
    } else {
        Err(BroadcasterContractError::PartitionContentBackendMismatch {
            context,
            entry: entry.to_string(),
            partition_backend,
            entry_backend,
        })
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
        protocol::models::{ProtocolComponent, Update as TychoUpdate},
        tycho_client::feed::{
            synchronizer::{Snapshot, StateSyncMessage},
            BlockHeader, SynchronizerState,
        },
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

    use super::{
        BroadcasterBackend, BroadcasterBackendHead, BroadcasterContractError, BroadcasterEnvelope,
        BroadcasterHeartbeat, BroadcasterMessageKind, BroadcasterPayload,
        BroadcasterProtocolMessage, BroadcasterProtocolSyncStatus,
        BroadcasterProtocolSyncStatusKind, BroadcasterRedisSnapshotPointer,
        BroadcasterRedisStreamEntry, BroadcasterRemovedPair, BroadcasterSnapshotChunk,
        BroadcasterSnapshotEnd, BroadcasterSnapshotPartition, BroadcasterSnapshotSessionResponse,
        BroadcasterSnapshotStart, BroadcasterStateDelta, BroadcasterStateEntry,
        BroadcasterSubscriptionEvent, BroadcasterSubscriptionState, BroadcasterSubscriptionTracker,
        BroadcasterTokenDto, BroadcasterTokenLookupRequest, BroadcasterTokenLookupResponse,
        BroadcasterTokenSnapshotResponse, BroadcasterUpdateMessage, BroadcasterUpdatePartition,
    };

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct DummySim {
        label: String,
    }

    #[typetag::serde]
    impl ProtocolSim for DummySim {
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
    fn token_lookup_contract_uses_camel_case_shape() -> Result<()> {
        let address = Bytes::from([0x11_u8; 20]);
        let request = BroadcasterTokenLookupRequest {
            chain_id: 1,
            addresses: vec![address.clone()],
        };
        let json = serde_json::to_value(&request)?;

        assert_eq!(json["chainId"], 1);
        assert_eq!(
            json["addresses"][0],
            "0x1111111111111111111111111111111111111111"
        );
        assert!(json.get("chain_id").is_none());

        let response = BroadcasterTokenLookupResponse {
            tokens: vec![BroadcasterTokenDto::from(Token::new(
                &address,
                "TKN",
                18,
                7,
                &[Some(21_000)],
                Chain::Ethereum,
                75,
            ))],
            missing: vec![Bytes::from([0x22_u8; 20])],
        };
        let json = serde_json::to_value(&response)?;

        assert!(json["tokens"].is_array());
        assert_eq!(
            json["missing"][0],
            "0x2222222222222222222222222222222222222222"
        );
        assert_eq!(json["tokens"][0]["chainId"], 1);
        Ok(())
    }

    #[test]
    fn token_snapshot_contract_uses_camel_case_shape() -> Result<()> {
        let address = Bytes::from([0x12_u8; 20]);
        let response = BroadcasterTokenSnapshotResponse {
            chain_id: 1,
            tokens: vec![BroadcasterTokenDto::from(Token::new(
                &address,
                "TKN",
                18,
                7,
                &[Some(21_000)],
                Chain::Ethereum,
                75,
            ))],
        };
        let json = serde_json::to_value(&response)?;

        assert_eq!(json["chainId"], 1);
        assert_eq!(json["tokens"][0]["symbol"], "TKN");
        assert!(json.get("chain_id").is_none());
        Ok(())
    }

    #[test]
    fn snapshot_session_contract_uses_camel_case_shape() -> Result<()> {
        let response = BroadcasterSnapshotSessionResponse {
            chain_id: 1,
            session_id: 9,
            stream_id: "stream-1".to_string(),
            snapshot_id: "snapshot-1".to_string(),
            payload_count: 4,
            snapshot_chunk_count: 2,
            expires_in_ms: 300_000,
        };
        let json = serde_json::to_value(&response)?;

        assert_eq!(json["chainId"], 1);
        assert_eq!(json["sessionId"], 9);
        assert_eq!(json["streamId"], "stream-1");
        assert_eq!(json["snapshotId"], "snapshot-1");
        assert_eq!(json["payloadCount"], 4);
        assert_eq!(json["snapshotChunkCount"], 2);
        assert_eq!(json["expiresInMs"], 300_000);
        assert!(json.get("session_id").is_none());
        Ok(())
    }

    #[test]
    fn token_dto_round_trips_tycho_token_fields() -> Result<()> {
        let address = Bytes::from([0x33_u8; 20]);
        let token = Token::new(
            &address,
            "ROUND",
            6,
            15,
            &[Some(45_000), None],
            Chain::Ethereum,
            50,
        );

        let round_tripped = BroadcasterTokenDto::from(token.clone()).into_token(Chain::Ethereum)?;

        assert_eq!(round_tripped, token);
        Ok(())
    }

    #[test]
    fn snapshot_start_round_trips_with_sorted_backends() -> Result<()> {
        let envelope = BroadcasterEnvelope::new(
            "stream-1",
            10,
            BroadcasterPayload::SnapshotStart(BroadcasterSnapshotStart::new(
                "snapshot-1",
                8453,
                vec![BroadcasterBackend::Vm, BroadcasterBackend::Native],
                2,
            )?),
        );

        let value = serde_json::to_value(&envelope)?;
        assert_eq!(value["kind"], "snapshot_start");
        assert_eq!(value["backends"], serde_json::json!(["native", "vm"]));

        let decoded: BroadcasterEnvelope = serde_json::from_value(value)?;
        let BroadcasterPayload::SnapshotStart(start) = decoded.payload else {
            return Err(anyhow!("expected snapshot_start payload"));
        };
        assert_eq!(start.snapshot_id, "snapshot-1");
        assert_eq!(
            start.backends,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Vm]
        );
        assert_eq!(start.total_chunks, 2);
        Ok(())
    }

    #[test]
    fn broadcaster_backend_rfq_round_trips() -> Result<()> {
        let json = serde_json::to_value(BroadcasterBackend::Rfq)?;
        assert_eq!(json, serde_json::json!("rfq"));

        let decoded: BroadcasterBackend = serde_json::from_value(json)?;
        assert_eq!(decoded, BroadcasterBackend::Rfq);
        assert_eq!(BroadcasterBackend::Rfq.as_str(), "rfq");
        Ok(())
    }

    #[test]
    fn snapshot_start_accepts_declared_rfq_backend() -> Result<()> {
        let start = BroadcasterSnapshotStart::new(
            "snapshot-1",
            8453,
            vec![BroadcasterBackend::Rfq, BroadcasterBackend::Native],
            2,
        )?;

        assert_eq!(
            start.backends,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq]
        );
        Ok(())
    }

    #[test]
    fn snapshot_start_constructor_rejects_duplicate_backends() -> Result<()> {
        let Err(error) = BroadcasterSnapshotStart::new(
            "snapshot-1",
            8453,
            vec![
                BroadcasterBackend::Native,
                BroadcasterBackend::Vm,
                BroadcasterBackend::Native,
            ],
            2,
        ) else {
            return Err(anyhow!("duplicate backends should fail"));
        };

        assert_eq!(
            error,
            BroadcasterContractError::DuplicateBackendEntry {
                context: "snapshot_start.backends",
                backend: BroadcasterBackend::Native,
            }
        );
        Ok(())
    }

    #[test]
    fn snapshot_chunk_round_trips_protocol_states() -> Result<()> {
        let sync_statuses = BTreeMap::from([(
            "uniswap_v2".to_string(),
            BroadcasterProtocolSyncStatus::from_synchronizer_state(&SynchronizerState::Ready(
                block_header(123, 7),
            )),
        )]);
        let chunk = BroadcasterSnapshotChunk::new(
            "snapshot-1",
            0,
            vec![BroadcasterSnapshotPartition::new(
                BroadcasterBackend::Native,
                123,
                vec![BroadcasterStateEntry::new(
                    "pool-1",
                    protocol_component("pool-1", "uniswap_v2"),
                    dummy_state("native-new"),
                )],
                sync_statuses,
            )],
        )?;
        let envelope =
            BroadcasterEnvelope::new("stream-1", 11, BroadcasterPayload::SnapshotChunk(chunk));

        let decoded: BroadcasterEnvelope =
            serde_json::from_str(&serde_json::to_string(&envelope)?)?;

        let BroadcasterPayload::SnapshotChunk(chunk) = decoded.payload else {
            return Err(anyhow!("expected snapshot_chunk payload"));
        };
        assert_eq!(chunk.snapshot_id, "snapshot-1");
        assert_eq!(chunk.chunk_index, 0);
        assert_eq!(chunk.partitions.len(), 1);
        let partition = &chunk.partitions[0];
        assert_eq!(partition.backend, BroadcasterBackend::Native);
        assert_eq!(partition.block_number, 123);
        assert_eq!(partition.states.len(), 1);
        assert_dummy_state(partition.states[0].state.as_ref(), "native-new");
        assert_eq!(
            partition.sync_statuses["uniswap_v2"].kind,
            BroadcasterProtocolSyncStatusKind::Ready
        );
        Ok(())
    }

    #[test]
    fn snapshot_end_round_trips() -> Result<()> {
        let envelope = BroadcasterEnvelope::new(
            "stream-1",
            12,
            BroadcasterPayload::SnapshotEnd(BroadcasterSnapshotEnd::new("snapshot-1")),
        );

        let decoded: BroadcasterEnvelope =
            serde_json::from_str(&serde_json::to_string(&envelope)?)?;

        let BroadcasterPayload::SnapshotEnd(end) = decoded.payload else {
            return Err(anyhow!("expected snapshot_end payload"));
        };
        assert_eq!(end.snapshot_id, "snapshot-1");
        Ok(())
    }

    #[test]
    fn update_from_tycho_update_splits_mixed_native_and_vm_content() -> Result<()> {
        let update = tycho_update();
        let message = BroadcasterUpdateMessage::from_tycho_update(&update, &known_backends())?;
        let envelope =
            BroadcasterEnvelope::new("stream-1", 13, BroadcasterPayload::Update(message));

        let decoded: BroadcasterEnvelope =
            serde_json::from_str(&serde_json::to_string(&envelope)?)?;

        let BroadcasterPayload::Update(update) = decoded.payload else {
            return Err(anyhow!("expected update payload"));
        };
        assert_eq!(update.partitions.len(), 2);

        let native = &update.partitions[0];
        assert_eq!(native.backend, BroadcasterBackend::Native);
        assert_eq!(native.new_pairs.len(), 1);
        assert_eq!(native.new_pairs[0].component_id, "pool-new");
        assert_eq!(native.updated_states.len(), 1);
        assert_eq!(
            native.updated_states[0].component_id,
            "pool-existing-native"
        );
        assert_eq!(native.updated_states[0].backend, BroadcasterBackend::Native);
        assert!(native.removed_pairs.is_empty());
        assert_eq!(
            native.sync_statuses["uniswap_v2"].kind,
            BroadcasterProtocolSyncStatusKind::Ready
        );

        let vm = &update.partitions[1];
        assert_eq!(vm.backend, BroadcasterBackend::Vm);
        assert!(vm.new_pairs.is_empty());
        assert_eq!(vm.updated_states.len(), 1);
        assert_eq!(vm.updated_states[0].component_id, "pool-existing-vm");
        assert_eq!(vm.updated_states[0].backend, BroadcasterBackend::Vm);
        assert_eq!(vm.removed_pairs.len(), 1);
        assert_eq!(vm.removed_pairs[0].component_id, "pool-removed");
        assert_eq!(
            vm.sync_statuses["vm:curve"].kind,
            BroadcasterProtocolSyncStatusKind::Advanced
        );
        Ok(())
    }

    #[test]
    fn update_from_tycho_update_accepts_rfq_content() -> Result<()> {
        let mut update = tycho_update();
        update.new_pairs.insert(
            "pool-rfq".to_string(),
            protocol_component("pool-rfq", "rfq:hashflow"),
        );
        update
            .states
            .insert("pool-rfq".to_string(), dummy_state("rfq-state"));

        let message = BroadcasterUpdateMessage::from_tycho_update(&update, &known_backends())?;
        let rfq = message
            .partitions
            .iter()
            .find(|partition| partition.backend == BroadcasterBackend::Rfq)
            .ok_or_else(|| anyhow!("expected RFQ partition"))?;

        assert_eq!(rfq.new_pairs.len(), 1);
        assert_eq!(rfq.new_pairs[0].component_id, "pool-rfq");
        assert_dummy_state(rfq.new_pairs[0].state.as_ref(), "rfq-state");
        Ok(())
    }

    #[test]
    fn update_from_tycho_update_rejects_state_without_known_backend() -> Result<()> {
        let update = tycho_update();

        let Err(error) = BroadcasterUpdateMessage::from_tycho_update(&update, &HashMap::new())
        else {
            return Err(anyhow!("unknown state backend should fail"));
        };

        assert!(matches!(
            error,
            BroadcasterContractError::StateBackendMissing { .. }
        ));
        Ok(())
    }

    #[test]
    fn update_from_tycho_update_rejects_non_canonical_native_sync_state_keys() -> Result<()> {
        let mut update = tycho_update();
        update.sync_states.remove("uniswap_v2");
        update.sync_states.insert(
            "native:uniswap_v2".to_string(),
            SynchronizerState::Ready(block_header(123, 9)),
        );

        let Err(error) = BroadcasterUpdateMessage::from_tycho_update(&update, &known_backends())
        else {
            return Err(anyhow!("non-canonical sync state key should fail"));
        };

        assert_eq!(
            error,
            BroadcasterContractError::UnknownSyncStateProtocol {
                protocol: "native:uniswap_v2".to_string(),
            }
        );
        Ok(())
    }

    #[test]
    fn heartbeat_round_trips_without_mutating_payload_state() -> Result<()> {
        let envelope = BroadcasterEnvelope::new(
            "stream-1",
            14,
            BroadcasterPayload::Heartbeat(BroadcasterHeartbeat::new(
                8453,
                "snapshot-1",
                vec![
                    BroadcasterBackendHead::new(BroadcasterBackend::Vm, 101),
                    BroadcasterBackendHead::new(BroadcasterBackend::Native, 100),
                ],
            )?),
        );

        let decoded: BroadcasterEnvelope =
            serde_json::from_str(&serde_json::to_string(&envelope)?)?;

        let BroadcasterPayload::Heartbeat(heartbeat) = decoded.payload else {
            return Err(anyhow!("expected heartbeat payload"));
        };
        assert_eq!(heartbeat.chain_id, 8453);
        assert_eq!(heartbeat.snapshot_id, "snapshot-1");
        assert_eq!(heartbeat.backend_heads.len(), 2);
        assert_eq!(
            heartbeat.backend_heads[0].backend,
            BroadcasterBackend::Native
        );
        assert_eq!(heartbeat.backend_heads[1].backend, BroadcasterBackend::Vm);
        Ok(())
    }

    #[test]
    fn heartbeat_accepts_rfq_backend_head() -> Result<()> {
        let heartbeat = BroadcasterHeartbeat::new(
            8453,
            "snapshot-1",
            vec![
                BroadcasterBackendHead::new(BroadcasterBackend::Rfq, 102),
                BroadcasterBackendHead::new(BroadcasterBackend::Native, 100),
            ],
        )?;

        assert_eq!(heartbeat.backend_heads.len(), 2);
        assert_eq!(
            heartbeat.backend_heads[0].backend,
            BroadcasterBackend::Native
        );
        assert_eq!(heartbeat.backend_heads[0].block_number, 100);
        assert_eq!(heartbeat.backend_heads[1].backend, BroadcasterBackend::Rfq);
        assert_eq!(heartbeat.backend_heads[1].block_number, 102);
        Ok(())
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
    fn redis_stream_entry_derives_rfq_block_number_from_envelope() -> Result<()> {
        let envelope = rfq_update_envelope("stream-1", 4, 321)?;
        let entry = redis_entry(&envelope, vec![BroadcasterBackend::Rfq])?;

        assert_eq!(entry.backend_scope, "rfq");
        assert_eq!(entry.block_number, Some(321));

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
            &snapshot_end_envelope("stream-1", 1),
            vec![BroadcasterBackend::Native],
        )?;
        value
            .as_object_mut()
            .ok_or_else(|| anyhow!("redis entry should encode as object"))?
            .remove("snapshot_id");

        let error = redis_entry_decode_error(value, "snapshot entry without snapshot_id")?;
        assert!(error.to_string().contains("requires snapshot_id"));

        let mut value = redis_entry_value(
            &snapshot_end_envelope("stream-1", 1),
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
    fn redis_stream_entry_rejects_rfq_payload_block_number_mismatch() -> Result<()> {
        let mut value = redis_entry_value(
            &rfq_update_envelope("stream-1", 4, 321)?,
            vec![BroadcasterBackend::Rfq],
        )?;
        value["block_number"] = serde_json::json!("322");

        let error = redis_entry_decode_error(value, "mismatched RFQ block_number")?;

        assert!(error
            .to_string()
            .contains("redis block_number mismatch: entry 322, payload 321"));

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
    fn redis_snapshot_pointer_uses_camel_case_shape_and_validates_live_cursor() -> Result<()> {
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
                "schemaVersion": "1",
                "chainId": 8453,
                "streamKey": "dsolver:broadcaster:prod-base:8453:events",
                "streamId": "chain-8453-stream-7",
                "snapshotId": "chain-8453-snapshot-7",
                "snapshotStartEntryId": "1710000000000-0",
                "snapshotEndEntryId": "1710000000123-0",
                "liveCursorEntryId": "1710000000123-0",
                "completedAtMs": 1710000000123u64
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
            "schemaVersion": "1",
            "chainId": 8453,
            "streamKey": "dsolver:broadcaster:prod-base:8453:events",
            "streamId": "chain-8453-stream-7",
            "snapshotId": "chain-8453-snapshot-7",
            "snapshotStartEntryId": "1710000000000-0",
            "snapshotEndEntryId": "1710000000123-0",
            "liveCursorEntryId": "1710000000999-0",
            "completedAtMs": 1710000000123u64
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

    #[test]
    fn tracker_accepts_snapshot_bootstrap_then_live_messages() -> Result<()> {
        let mut tracker = BroadcasterSubscriptionTracker::new();

        let event = tracker.observe(&snapshot_start_envelope(
            "stream-1",
            8453,
            "snapshot-1",
            50,
            1,
        )?)?;
        assert_eq!(
            event,
            BroadcasterSubscriptionEvent::SnapshotStarted {
                snapshot_id: "snapshot-1".to_string(),
            }
        );

        let event = tracker.observe(&snapshot_chunk_envelope("stream-1", 51, 0)?)?;
        assert_eq!(
            event,
            BroadcasterSubscriptionEvent::SnapshotChunkAccepted {
                snapshot_id: "snapshot-1".to_string(),
                chunk_index: 0,
            }
        );

        let event = tracker.observe(&snapshot_end_envelope("stream-1", 52))?;
        assert_eq!(
            event,
            BroadcasterSubscriptionEvent::SnapshotCompleted {
                snapshot_id: "snapshot-1".to_string(),
            }
        );
        assert!(matches!(
            tracker.state(),
            BroadcasterSubscriptionState::Live { .. }
        ));

        assert_eq!(
            tracker.observe(&update_envelope("stream-1", 53)?)?,
            BroadcasterSubscriptionEvent::UpdateAccepted
        );
        assert_eq!(
            tracker.observe(&heartbeat_envelope("stream-1", 8453, "snapshot-1", 54)?)?,
            BroadcasterSubscriptionEvent::HeartbeatAccepted
        );
        assert_eq!(tracker.next_message_seq(), Some(55));

        Ok(())
    }

    #[test]
    fn tracker_rejects_duplicate_or_out_of_order_message_sequences() -> Result<()> {
        let mut tracker = BroadcasterSubscriptionTracker::new();
        tracker.observe(&snapshot_start_envelope(
            "stream-1",
            8453,
            "snapshot-1",
            10,
            1,
        )?)?;

        let Err(error) = tracker.observe(&snapshot_chunk_envelope("stream-1", 12, 0)?) else {
            return Err(anyhow!("skipping a message sequence should fail"));
        };
        assert_eq!(
            error,
            BroadcasterContractError::UnexpectedMessageSeq {
                expected: 11,
                found: 12,
            }
        );

        Ok(())
    }

    #[test]
    fn tracker_rejects_snapshot_end_before_all_chunks_arrive() -> Result<()> {
        let mut tracker = BroadcasterSubscriptionTracker::new();
        tracker.observe(&snapshot_start_envelope(
            "stream-1",
            8453,
            "snapshot-1",
            20,
            2,
        )?)?;
        tracker.observe(&snapshot_chunk_envelope("stream-1", 21, 0)?)?;

        let Err(error) = tracker.observe(&snapshot_end_envelope("stream-1", 22)) else {
            return Err(anyhow!("snapshot should stay incomplete"));
        };

        assert_eq!(
            error,
            BroadcasterContractError::SnapshotIncomplete {
                expected_chunks: 2,
                observed_chunks: 1,
            }
        );
        assert_eq!(tracker.next_message_seq(), Some(22));
        Ok(())
    }

    #[test]
    fn tracker_rejects_out_of_order_snapshot_chunks_without_advancing_sequence() -> Result<()> {
        let mut tracker = BroadcasterSubscriptionTracker::new();
        tracker.observe(&snapshot_start_envelope(
            "stream-1",
            8453,
            "snapshot-1",
            30,
            1,
        )?)?;

        let Err(error) = tracker.observe(&snapshot_chunk_envelope("stream-1", 31, 1)?) else {
            return Err(anyhow!("chunk index must advance in order"));
        };

        assert_eq!(
            error,
            BroadcasterContractError::UnexpectedChunkIndex {
                expected: 0,
                found: 1,
            }
        );
        assert_eq!(tracker.next_message_seq(), Some(31));
        assert_eq!(
            tracker.observe(&snapshot_chunk_envelope("stream-1", 31, 0)?)?,
            BroadcasterSubscriptionEvent::SnapshotChunkAccepted {
                snapshot_id: "snapshot-1".to_string(),
                chunk_index: 0,
            }
        );
        let Err(error) = tracker.observe(&snapshot_chunk_envelope("stream-1", 32, 1)?) else {
            return Err(anyhow!("extra snapshot chunk should fail immediately"));
        };
        assert_eq!(
            error,
            BroadcasterContractError::ExtraSnapshotChunk {
                total_chunks: 1,
                found: 1,
            }
        );
        assert_eq!(tracker.next_message_seq(), Some(32));
        assert_eq!(
            tracker.observe(&snapshot_end_envelope("stream-1", 32))?,
            BroadcasterSubscriptionEvent::SnapshotCompleted {
                snapshot_id: "snapshot-1".to_string(),
            }
        );
        Ok(())
    }

    #[test]
    fn tracker_rejects_empty_update_without_advancing_sequence() -> Result<()> {
        let Err(error) = BroadcasterUpdateMessage::new(Vec::new()) else {
            return Err(anyhow!("constructor should reject empty update"));
        };
        assert_eq!(error, BroadcasterContractError::EmptyUpdate);

        let serde_error = serde_json::from_value::<BroadcasterEnvelope>(serde_json::json!({
            "stream_id": "stream-1",
            "message_seq": 1,
            "kind": "update",
            "partitions": []
        }))
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
        assert!(
            serde_error.contains("update message must contain at least one partition"),
            "serde should reject empty updates"
        );

        let mut tracker = ready_tracker()?;

        let Err(error) = tracker.observe(&BroadcasterEnvelope::new(
            "stream-1",
            53,
            BroadcasterPayload::Update(BroadcasterUpdateMessage {
                partitions: Vec::new(),
            }),
        )) else {
            return Err(anyhow!("empty update should fail"));
        };

        assert_eq!(error, BroadcasterContractError::EmptyUpdate);
        assert_eq!(tracker.next_message_seq(), Some(53));
        assert_eq!(
            tracker.observe(&update_envelope("stream-1", 53)?)?,
            BroadcasterSubscriptionEvent::UpdateAccepted
        );
        Ok(())
    }

    #[test]
    fn serde_rejects_update_without_partitions() {
        let serde_error = serde_json::from_value::<BroadcasterEnvelope>(serde_json::json!({
            "stream_id": "stream-1",
            "message_seq": 1,
            "kind": "update"
        }))
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();

        assert!(
            serde_error.contains("missing field `partitions`"),
            "serde should reject updates that omit partitions"
        );
    }

    #[test]
    fn update_constructor_rejects_semantic_empty_partition() -> Result<()> {
        let Err(error) = BroadcasterUpdateMessage::new(vec![BroadcasterUpdatePartition::new(
            BroadcasterBackend::Native,
            123,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            BTreeMap::new(),
        )]) else {
            return Err(anyhow!("semantic-empty partition should fail"));
        };

        assert_eq!(
            error,
            BroadcasterContractError::EmptyUpdatePartition {
                backend: BroadcasterBackend::Native,
            }
        );
        Ok(())
    }

    #[test]
    fn update_from_tycho_update_rejects_no_op_update() -> Result<()> {
        let update = TychoUpdate::new(123, HashMap::new(), HashMap::new());

        let Err(error) = BroadcasterUpdateMessage::from_tycho_update(&update, &known_backends())
        else {
            return Err(anyhow!("no-op update should fail"));
        };

        assert_eq!(error, BroadcasterContractError::EmptyUpdate);
        Ok(())
    }

    #[test]
    fn serde_rejects_semantic_empty_update_partition() {
        let value = serde_json::json!({
            "stream_id": "stream-1",
            "message_seq": 1,
            "kind": "update",
            "partitions": [
                {"backend": "native", "blockNumber": 1}
            ]
        });

        let error = serde_json::from_value::<BroadcasterEnvelope>(value).err();
        assert!(
            error.is_some(),
            "semantic-empty update partition should fail"
        );
        let error = error.map(|error| error.to_string()).unwrap_or_default();
        assert!(error.contains("update partition for Native must contain state or sync data"));
    }

    #[test]
    fn tracker_rejects_semantic_empty_update_partition_without_advancing_sequence() -> Result<()> {
        let mut tracker = ready_tracker()?;

        let Err(error) = tracker.observe(&BroadcasterEnvelope::new(
            "stream-1",
            53,
            BroadcasterPayload::Update(BroadcasterUpdateMessage {
                partitions: vec![BroadcasterUpdatePartition::new(
                    BroadcasterBackend::Native,
                    124,
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    BTreeMap::new(),
                )],
            }),
        )) else {
            return Err(anyhow!("semantic-empty update should fail"));
        };

        assert_eq!(
            error,
            BroadcasterContractError::EmptyUpdatePartition {
                backend: BroadcasterBackend::Native,
            }
        );
        assert_eq!(tracker.next_message_seq(), Some(53));
        assert_eq!(
            tracker.observe(&update_envelope("stream-1", 53)?)?,
            BroadcasterSubscriptionEvent::UpdateAccepted
        );
        Ok(())
    }

    #[test]
    fn tracker_requires_reconnect_reset_before_a_fresh_snapshot() -> Result<()> {
        let mut tracker = BroadcasterSubscriptionTracker::new();
        tracker.observe(&snapshot_start_envelope_with_backends(
            "stream-1",
            8453,
            "snapshot-1",
            40,
            0,
            Vec::new(),
        )?)?;
        tracker.observe(&snapshot_end_envelope("stream-1", 41))?;

        let Err(error) = tracker.observe(&snapshot_start_envelope_with_backends(
            "stream-1",
            8453,
            "snapshot-1",
            42,
            0,
            Vec::new(),
        )?) else {
            return Err(anyhow!("fresh snapshot should require reconnect reset"));
        };
        assert_eq!(
            error,
            BroadcasterContractError::UnexpectedSnapshotStart { state: "live" }
        );

        tracker.reset_for_reconnect();
        assert!(matches!(
            tracker.state(),
            BroadcasterSubscriptionState::AwaitingSnapshot
        ));
        assert_eq!(
            tracker.observe(&snapshot_start_envelope_with_backends(
                "stream-1",
                8453,
                "snapshot-1",
                42,
                0,
                Vec::new(),
            )?)?,
            BroadcasterSubscriptionEvent::SnapshotStarted {
                snapshot_id: "snapshot-1".to_string(),
            }
        );

        Ok(())
    }

    #[test]
    fn tracker_rejects_heartbeat_before_snapshot_complete() -> Result<()> {
        let mut tracker = BroadcasterSubscriptionTracker::new();
        tracker.observe(&snapshot_start_envelope(
            "stream-1",
            8453,
            "snapshot-1",
            60,
            1,
        )?)?;

        let Err(error) = tracker.observe(&heartbeat_envelope("stream-1", 8453, "snapshot-1", 61)?)
        else {
            return Err(anyhow!("heartbeat should not arrive during bootstrap"));
        };

        assert_eq!(
            error,
            BroadcasterContractError::HeartbeatBeforeSnapshotComplete
        );
        Ok(())
    }

    #[test]
    fn tracker_rejects_live_heartbeat_with_wrong_snapshot_id() -> Result<()> {
        let mut tracker = ready_tracker()?;

        let Err(error) = tracker.observe(&heartbeat_envelope("stream-1", 8453, "snapshot-2", 53)?)
        else {
            return Err(anyhow!("heartbeat snapshot should match"));
        };

        assert_eq!(
            error,
            BroadcasterContractError::UnexpectedSnapshotId {
                expected: "snapshot-1".to_string(),
                found: "snapshot-2".to_string(),
            }
        );
        assert_eq!(tracker.next_message_seq(), Some(53));
        Ok(())
    }

    #[test]
    fn tracker_rejects_live_heartbeat_with_wrong_chain_id() -> Result<()> {
        let mut tracker = ready_tracker()?;

        let Err(error) = tracker.observe(&heartbeat_envelope("stream-1", 1, "snapshot-1", 53)?)
        else {
            return Err(anyhow!("heartbeat chain should match"));
        };

        assert_eq!(
            error,
            BroadcasterContractError::UnexpectedChainId {
                expected: 8453,
                found: 1,
            }
        );
        assert_eq!(tracker.next_message_seq(), Some(53));
        Ok(())
    }

    #[test]
    fn tracker_rejects_invalid_backend_contracts_without_advancing_sequence() -> Result<()> {
        assert_missing_declared_snapshot_backends_rejected()?;
        assert_undeclared_backend_entries_rejected()?;
        Ok(())
    }

    #[test]
    fn serde_rejects_duplicate_snapshot_start_backends() -> Result<()> {
        let value = serde_json::json!({
            "stream_id": "stream-1",
            "message_seq": 1,
            "kind": "snapshot_start",
            "snapshotId": "snapshot-1",
            "chainId": 8453,
            "backends": ["native", "native"],
            "totalChunks": 1
        });

        let error = serde_json::from_value::<BroadcasterEnvelope>(value).err();
        assert!(error.is_some(), "duplicate backends should fail");
        let error = error.map(|error| error.to_string()).unwrap_or_default();
        assert!(error.contains("duplicate backend entry Native"));
        let mut tracker = BroadcasterSubscriptionTracker::new();
        let Err(error) = tracker.observe(&BroadcasterEnvelope::new(
            "stream-1",
            1,
            BroadcasterPayload::SnapshotStart(BroadcasterSnapshotStart {
                snapshot_id: "snapshot-1".to_string(),
                chain_id: 8453,
                backends: vec![BroadcasterBackend::Native, BroadcasterBackend::Native],
                total_chunks: 1,
            }),
        )) else {
            return Err(anyhow!("tracker should reject duplicate snapshot backends"));
        };
        assert_eq!(
            error,
            BroadcasterContractError::DuplicateBackendEntry {
                context: "snapshot_start.backends",
                backend: BroadcasterBackend::Native,
            }
        );
        assert_eq!(tracker.next_message_seq(), None);
        assert!(matches!(
            tracker.state(),
            BroadcasterSubscriptionState::AwaitingSnapshot
        ));
        Ok(())
    }

    #[test]
    fn serde_rejects_duplicate_snapshot_chunk_partitions() -> Result<()> {
        let value = serde_json::json!({
            "stream_id": "stream-1",
            "message_seq": 1,
            "kind": "snapshot_chunk",
            "snapshotId": "snapshot-1",
            "chunkIndex": 0,
            "partitions": [
                {"backend": "native", "blockNumber": 1, "states": [], "syncStatuses": {}},
                {"backend": "native", "blockNumber": 2, "states": [], "syncStatuses": {}}
            ]
        });

        let error = serde_json::from_value::<BroadcasterEnvelope>(value).err();
        assert!(error.is_some(), "duplicate partitions should fail");
        let error = error.map(|error| error.to_string()).unwrap_or_default();
        assert!(error.contains("duplicate backend entry Native"));
        let mut tracker = BroadcasterSubscriptionTracker::new();
        tracker.observe(&snapshot_start_envelope(
            "stream-1",
            8453,
            "snapshot-1",
            1,
            1,
        )?)?;
        let Err(error) = tracker.observe(&BroadcasterEnvelope::new(
            "stream-1",
            2,
            BroadcasterPayload::SnapshotChunk(BroadcasterSnapshotChunk {
                snapshot_id: "snapshot-1".to_string(),
                chunk_index: 0,
                partitions: vec![
                    BroadcasterSnapshotPartition::new(
                        BroadcasterBackend::Native,
                        1,
                        Vec::new(),
                        BTreeMap::new(),
                    ),
                    BroadcasterSnapshotPartition::new(
                        BroadcasterBackend::Native,
                        2,
                        Vec::new(),
                        BTreeMap::new(),
                    ),
                ],
            }),
        )) else {
            return Err(anyhow!(
                "tracker should reject duplicate snapshot partitions"
            ));
        };
        assert_eq!(
            error,
            BroadcasterContractError::DuplicateBackendEntry {
                context: "snapshot_chunk.partitions",
                backend: BroadcasterBackend::Native,
            }
        );
        assert_eq!(tracker.next_message_seq(), Some(2));
        assert_eq!(
            tracker.observe(&snapshot_chunk_envelope("stream-1", 2, 0)?)?,
            BroadcasterSubscriptionEvent::SnapshotChunkAccepted {
                snapshot_id: "snapshot-1".to_string(),
                chunk_index: 0,
            }
        );
        assert_snapshot_partition_content_mismatch_rejected()?;
        Ok(())
    }

    #[test]
    fn serde_rejects_duplicate_update_partitions() -> Result<()> {
        let value = serde_json::json!({
            "stream_id": "stream-1",
            "message_seq": 1,
            "kind": "update",
            "partitions": [
                {"backend": "vm", "blockNumber": 1, "updatedStates": []},
                {"backend": "vm", "blockNumber": 2, "updatedStates": []}
            ]
        });

        let error = serde_json::from_value::<BroadcasterEnvelope>(value).err();
        assert!(error.is_some(), "duplicate update partitions should fail");
        let error = error.map(|error| error.to_string()).unwrap_or_default();
        assert!(error.contains("duplicate backend entry Vm"));
        let mut tracker = ready_tracker()?;
        let Err(error) = tracker.observe(&BroadcasterEnvelope::new(
            "stream-1",
            53,
            BroadcasterPayload::Update(BroadcasterUpdateMessage {
                partitions: vec![
                    BroadcasterUpdatePartition::new(
                        BroadcasterBackend::Vm,
                        1,
                        Vec::new(),
                        vec![BroadcasterStateDelta::new(
                            "pool-vm-1",
                            BroadcasterBackend::Vm,
                            dummy_state("vm-1"),
                        )],
                        Vec::new(),
                        BTreeMap::new(),
                    ),
                    BroadcasterUpdatePartition::new(
                        BroadcasterBackend::Vm,
                        2,
                        Vec::new(),
                        vec![BroadcasterStateDelta::new(
                            "pool-vm-2",
                            BroadcasterBackend::Vm,
                            dummy_state("vm-2"),
                        )],
                        Vec::new(),
                        BTreeMap::new(),
                    ),
                ],
            }),
        )) else {
            return Err(anyhow!("tracker should reject duplicate update partitions"));
        };
        assert_eq!(
            error,
            BroadcasterContractError::DuplicateBackendEntry {
                context: "update.partitions",
                backend: BroadcasterBackend::Vm,
            }
        );
        assert_eq!(tracker.next_message_seq(), Some(53));
        assert_eq!(
            tracker.observe(&update_envelope("stream-1", 53)?)?,
            BroadcasterSubscriptionEvent::UpdateAccepted
        );
        assert_update_partition_content_mismatches_rejected(&mut tracker)?;
        Ok(())
    }

    #[test]
    fn serde_rejects_duplicate_heartbeat_heads() -> Result<()> {
        let value = serde_json::json!({
            "stream_id": "stream-1",
            "message_seq": 1,
            "kind": "heartbeat",
            "chainId": 8453,
            "snapshotId": "snapshot-1",
            "backendHeads": [
                {"backend": "native", "blockNumber": 1},
                {"backend": "native", "blockNumber": 2}
            ]
        });

        let error = serde_json::from_value::<BroadcasterEnvelope>(value).err();
        assert!(error.is_some(), "duplicate heartbeat heads should fail");
        let error = error.map(|error| error.to_string()).unwrap_or_default();
        assert!(error.contains("duplicate backend entry Native"));
        let mut tracker = ready_tracker()?;
        let Err(error) = tracker.observe(&BroadcasterEnvelope::new(
            "stream-1",
            53,
            BroadcasterPayload::Heartbeat(BroadcasterHeartbeat {
                chain_id: 8453,
                snapshot_id: "snapshot-1".to_string(),
                backend_heads: vec![
                    BroadcasterBackendHead::new(BroadcasterBackend::Native, 1),
                    BroadcasterBackendHead::new(BroadcasterBackend::Native, 2),
                ],
            }),
        )) else {
            return Err(anyhow!("tracker should reject duplicate heartbeat heads"));
        };
        assert_eq!(
            error,
            BroadcasterContractError::DuplicateBackendEntry {
                context: "heartbeat.backend_heads",
                backend: BroadcasterBackend::Native,
            }
        );
        assert_eq!(tracker.next_message_seq(), Some(53));
        assert_eq!(
            tracker.observe(&heartbeat_envelope("stream-1", 8453, "snapshot-1", 53)?)?,
            BroadcasterSubscriptionEvent::HeartbeatAccepted
        );
        Ok(())
    }

    fn assert_missing_declared_snapshot_backends_rejected() -> Result<()> {
        let mut tracker = BroadcasterSubscriptionTracker::new();
        tracker.observe(&snapshot_start_envelope(
            "stream-1",
            8453,
            "snapshot-1",
            70,
            1,
        )?)?;
        tracker.observe(&snapshot_chunk_envelope_with_partitions(
            "stream-1",
            71,
            0,
            vec![native_snapshot_partition()],
        )?)?;

        let Err(error) = tracker.observe(&snapshot_end_envelope("stream-1", 72)) else {
            return Err(anyhow!("snapshot_end should require all declared backends"));
        };
        assert_eq!(
            error,
            BroadcasterContractError::MissingDeclaredSnapshotBackends {
                missing: vec![BroadcasterBackend::Vm],
            }
        );
        assert_eq!(tracker.next_message_seq(), Some(72));
        Ok(())
    }

    fn assert_undeclared_backend_entries_rejected() -> Result<()> {
        let mut tracker = BroadcasterSubscriptionTracker::new();
        tracker.observe(&snapshot_start_envelope_with_backends(
            "stream-1",
            8453,
            "snapshot-1",
            70,
            1,
            vec![BroadcasterBackend::Native],
        )?)?;

        let Err(error) = tracker.observe(&BroadcasterEnvelope::new(
            "stream-1",
            71,
            BroadcasterPayload::SnapshotChunk(BroadcasterSnapshotChunk {
                snapshot_id: "snapshot-1".to_string(),
                chunk_index: 0,
                partitions: vec![BroadcasterSnapshotPartition::new(
                    BroadcasterBackend::Vm,
                    123,
                    vec![BroadcasterStateEntry::new(
                        "pool-vm",
                        protocol_component("pool-vm", "vm:curve"),
                        dummy_state("vm-state"),
                    )],
                    BTreeMap::new(),
                )],
            }),
        )) else {
            return Err(anyhow!("snapshot chunk backend should be declared"));
        };
        assert_eq!(
            error,
            BroadcasterContractError::UndeclaredBackendEntry {
                context: "snapshot_chunk.partitions",
                backend: BroadcasterBackend::Vm,
            }
        );
        assert_eq!(tracker.next_message_seq(), Some(71));
        tracker.observe(&snapshot_chunk_envelope_with_partitions(
            "stream-1",
            71,
            0,
            vec![native_snapshot_partition()],
        )?)?;
        tracker.observe(&snapshot_end_envelope("stream-1", 72))?;

        let Err(error) = tracker.observe(&BroadcasterEnvelope::new(
            "stream-1",
            73,
            BroadcasterPayload::Update(BroadcasterUpdateMessage {
                partitions: vec![BroadcasterUpdatePartition::new(
                    BroadcasterBackend::Vm,
                    124,
                    Vec::new(),
                    vec![BroadcasterStateDelta::new(
                        "pool-vm",
                        BroadcasterBackend::Vm,
                        dummy_state("vm-update"),
                    )],
                    Vec::new(),
                    BTreeMap::new(),
                )],
            }),
        )) else {
            return Err(anyhow!("update backend should be declared"));
        };
        assert_eq!(
            error,
            BroadcasterContractError::UndeclaredBackendEntry {
                context: "update.partitions",
                backend: BroadcasterBackend::Vm,
            }
        );
        assert_eq!(tracker.next_message_seq(), Some(73));
        tracker.observe(&update_envelope("stream-1", 73)?)?;

        let Err(error) = tracker.observe(&BroadcasterEnvelope::new(
            "stream-1",
            74,
            BroadcasterPayload::Heartbeat(BroadcasterHeartbeat {
                chain_id: 8453,
                snapshot_id: "snapshot-1".to_string(),
                backend_heads: vec![BroadcasterBackendHead::new(BroadcasterBackend::Vm, 124)],
            }),
        )) else {
            return Err(anyhow!("heartbeat backend should be declared"));
        };
        assert_eq!(
            error,
            BroadcasterContractError::UndeclaredBackendEntry {
                context: "heartbeat.backend_heads",
                backend: BroadcasterBackend::Vm,
            }
        );
        assert_eq!(tracker.next_message_seq(), Some(74));
        assert_eq!(
            tracker.observe(&heartbeat_envelope("stream-1", 8453, "snapshot-1", 74)?)?,
            BroadcasterSubscriptionEvent::HeartbeatAccepted
        );
        Ok(())
    }

    fn assert_snapshot_partition_content_mismatch_rejected() -> Result<()> {
        let mut tracker = BroadcasterSubscriptionTracker::new();
        tracker.observe(&snapshot_start_envelope_with_backends(
            "stream-1",
            8453,
            "snapshot-1",
            1,
            1,
            vec![BroadcasterBackend::Native],
        )?)?;
        let Err(error) = tracker.observe(&BroadcasterEnvelope::new(
            "stream-1",
            2,
            BroadcasterPayload::SnapshotChunk(BroadcasterSnapshotChunk {
                snapshot_id: "snapshot-1".to_string(),
                chunk_index: 0,
                partitions: vec![BroadcasterSnapshotPartition::new(
                    BroadcasterBackend::Native,
                    1,
                    vec![BroadcasterStateEntry::new(
                        "pool-vm",
                        protocol_component("pool-vm", "vm:curve"),
                        dummy_state("vm-state"),
                    )],
                    BTreeMap::new(),
                )],
            }),
        )) else {
            return Err(anyhow!(
                "tracker should reject snapshot partition content from the wrong backend"
            ));
        };
        assert_eq!(
            error,
            BroadcasterContractError::PartitionContentBackendMismatch {
                context: "snapshot_chunk.partitions.states",
                entry: "pool-vm".to_string(),
                partition_backend: BroadcasterBackend::Native,
                entry_backend: BroadcasterBackend::Vm,
            }
        );
        assert_eq!(tracker.next_message_seq(), Some(2));
        assert_eq!(
            tracker.observe(&snapshot_chunk_envelope_with_partitions(
                "stream-1",
                2,
                0,
                vec![native_snapshot_partition()],
            )?)?,
            BroadcasterSubscriptionEvent::SnapshotChunkAccepted {
                snapshot_id: "snapshot-1".to_string(),
                chunk_index: 0,
            }
        );
        Ok(())
    }

    fn assert_update_partition_content_mismatches_rejected(
        tracker: &mut BroadcasterSubscriptionTracker,
    ) -> Result<()> {
        let Err(error) = tracker.observe(&BroadcasterEnvelope::new(
            "stream-1",
            54,
            BroadcasterPayload::Update(BroadcasterUpdateMessage {
                partitions: vec![BroadcasterUpdatePartition::new(
                    BroadcasterBackend::Native,
                    124,
                    Vec::new(),
                    Vec::new(),
                    vec![BroadcasterRemovedPair::new(
                        "pool-vm",
                        protocol_component("pool-vm", "vm:curve"),
                    )],
                    BTreeMap::new(),
                )],
            }),
        )) else {
            return Err(anyhow!(
                "tracker should reject update partition content from the wrong backend"
            ));
        };
        assert_eq!(
            error,
            BroadcasterContractError::PartitionContentBackendMismatch {
                context: "update.partitions.removed_pairs",
                entry: "pool-vm".to_string(),
                partition_backend: BroadcasterBackend::Native,
                entry_backend: BroadcasterBackend::Vm,
            }
        );
        assert_eq!(tracker.next_message_seq(), Some(54));
        assert_eq!(
            tracker.observe(&update_envelope("stream-1", 54)?)?,
            BroadcasterSubscriptionEvent::UpdateAccepted
        );

        let Err(error) = tracker.observe(&BroadcasterEnvelope::new(
            "stream-1",
            55,
            BroadcasterPayload::Update(BroadcasterUpdateMessage {
                partitions: vec![BroadcasterUpdatePartition::new(
                    BroadcasterBackend::Native,
                    124,
                    Vec::new(),
                    vec![BroadcasterStateDelta::new(
                        "pool-vm",
                        BroadcasterBackend::Vm,
                        dummy_state("vm-update"),
                    )],
                    Vec::new(),
                    BTreeMap::new(),
                )],
            }),
        )) else {
            return Err(anyhow!(
                "tracker should reject updated state content from the wrong backend"
            ));
        };
        assert_eq!(
            error,
            BroadcasterContractError::PartitionContentBackendMismatch {
                context: "update.partitions.updated_states",
                entry: "pool-vm".to_string(),
                partition_backend: BroadcasterBackend::Native,
                entry_backend: BroadcasterBackend::Vm,
            }
        );
        assert_eq!(tracker.next_message_seq(), Some(55));
        assert_eq!(
            tracker.observe(&update_envelope("stream-1", 55)?)?,
            BroadcasterSubscriptionEvent::UpdateAccepted
        );
        Ok(())
    }

    #[test]
    fn constructors_reject_raw_message_partition_backend_mismatch() -> Result<()> {
        let Err(error) = BroadcasterSnapshotChunk::new(
            "snapshot-1",
            0,
            vec![BroadcasterSnapshotPartition::with_messages(
                BroadcasterBackend::Native,
                123,
                vec![raw_protocol_message("vm:curve")],
                BTreeMap::new(),
            )],
        ) else {
            return Err(anyhow!(
                "snapshot chunk should reject raw messages from the wrong backend"
            ));
        };
        assert_eq!(
            error,
            BroadcasterContractError::PartitionContentBackendMismatch {
                context: "snapshot_chunk.partitions.messages",
                entry: "vm:curve".to_string(),
                partition_backend: BroadcasterBackend::Native,
                entry_backend: BroadcasterBackend::Vm,
            }
        );

        let Err(error) =
            BroadcasterUpdateMessage::new(vec![BroadcasterUpdatePartition::with_messages(
                BroadcasterBackend::Native,
                123,
                vec![raw_protocol_message("vm:curve")],
                BTreeMap::new(),
            )])
        else {
            return Err(anyhow!(
                "update should reject raw messages from the wrong backend"
            ));
        };
        assert_eq!(
            error,
            BroadcasterContractError::PartitionContentBackendMismatch {
                context: "update.partitions.messages",
                entry: "vm:curve".to_string(),
                partition_backend: BroadcasterBackend::Native,
                entry_backend: BroadcasterBackend::Vm,
            }
        );
        Ok(())
    }

    fn ready_tracker() -> Result<BroadcasterSubscriptionTracker> {
        let mut tracker = BroadcasterSubscriptionTracker::new();
        tracker.observe(&snapshot_start_envelope(
            "stream-1",
            8453,
            "snapshot-1",
            50,
            1,
        )?)?;
        tracker.observe(&snapshot_chunk_envelope("stream-1", 51, 0)?)?;
        tracker.observe(&snapshot_end_envelope("stream-1", 52))?;
        Ok(tracker)
    }

    fn snapshot_start_envelope(
        stream_id: &str,
        chain_id: u64,
        snapshot_id: &str,
        message_seq: u64,
        total_chunks: u32,
    ) -> Result<BroadcasterEnvelope> {
        snapshot_start_envelope_with_backends(
            stream_id,
            chain_id,
            snapshot_id,
            message_seq,
            total_chunks,
            vec![BroadcasterBackend::Vm, BroadcasterBackend::Native],
        )
    }

    fn snapshot_start_envelope_with_backends(
        stream_id: &str,
        chain_id: u64,
        snapshot_id: &str,
        message_seq: u64,
        total_chunks: u32,
        backends: Vec<BroadcasterBackend>,
    ) -> Result<BroadcasterEnvelope> {
        Ok(BroadcasterEnvelope::new(
            stream_id,
            message_seq,
            BroadcasterPayload::SnapshotStart(BroadcasterSnapshotStart::new(
                snapshot_id,
                chain_id,
                backends,
                total_chunks,
            )?),
        ))
    }

    fn snapshot_chunk_envelope(
        stream_id: &str,
        message_seq: u64,
        chunk_index: u32,
    ) -> Result<BroadcasterEnvelope> {
        snapshot_chunk_envelope_with_partitions(
            stream_id,
            message_seq,
            chunk_index,
            vec![native_snapshot_partition(), vm_snapshot_partition()],
        )
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

    fn known_backends() -> HashMap<String, BroadcasterBackend> {
        HashMap::from([
            (
                "pool-existing-native".to_string(),
                BroadcasterBackend::Native,
            ),
            ("pool-existing-vm".to_string(), BroadcasterBackend::Vm),
        ])
    }

    fn native_snapshot_partition() -> BroadcasterSnapshotPartition {
        BroadcasterSnapshotPartition::new(
            BroadcasterBackend::Native,
            123,
            vec![BroadcasterStateEntry::new(
                "pool-1",
                protocol_component("pool-1", "uniswap_v2"),
                dummy_state("snapshot-state"),
            )],
            BTreeMap::new(),
        )
    }

    fn vm_snapshot_partition() -> BroadcasterSnapshotPartition {
        BroadcasterSnapshotPartition::new(
            BroadcasterBackend::Vm,
            123,
            vec![BroadcasterStateEntry::new(
                "pool-vm",
                protocol_component("pool-vm", "vm:curve"),
                dummy_state("snapshot-vm-state"),
            )],
            BTreeMap::new(),
        )
    }

    fn tycho_update() -> TychoUpdate {
        let mut states = HashMap::new();
        states.insert("pool-new".to_string(), dummy_state("new-state"));
        states.insert(
            "pool-existing-native".to_string(),
            dummy_state("existing-native-state"),
        );
        states.insert(
            "pool-existing-vm".to_string(),
            dummy_state("existing-vm-state"),
        );

        let mut new_pairs = HashMap::new();
        new_pairs.insert(
            "pool-new".to_string(),
            protocol_component("pool-new", "uniswap_v2"),
        );

        let mut removed_pairs = HashMap::new();
        removed_pairs.insert(
            "pool-removed".to_string(),
            protocol_component("pool-removed", "vm:curve"),
        );

        let mut sync_states = HashMap::new();
        sync_states.insert(
            "uniswap_v2".to_string(),
            SynchronizerState::Ready(block_header(123, 1)),
        );
        sync_states.insert(
            "vm:curve".to_string(),
            SynchronizerState::Advanced(block_header(122, 2)),
        );

        TychoUpdate::new(123, states, new_pairs)
            .set_removed_pairs(removed_pairs)
            .set_sync_states(sync_states)
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
        Box::new(DummySim {
            label: label.to_string(),
        })
    }

    fn block_header(number: u64, seed: u8) -> BlockHeader {
        BlockHeader {
            hash: Bytes::from(vec![seed; 32]),
            number,
            parent_hash: Bytes::from(vec![seed.saturating_add(1); 32]),
            revert: false,
            timestamp: number * 10,
            partial_block_index: None,
        }
    }

    fn raw_protocol_message(protocol: &str) -> BroadcasterProtocolMessage {
        BroadcasterProtocolMessage::new(
            protocol,
            SynchronizerState::Ready(block_header(123, 1)),
            StateSyncMessage {
                header: block_header(123, 1),
                snapshots: Snapshot {
                    states: HashMap::new(),
                    vm_storage: HashMap::new(),
                },
                deltas: None,
                removed_components: HashMap::new(),
            },
        )
    }

    fn assert_dummy_state(state: &dyn ProtocolSim, expected_label: &str) {
        let dummy = state.as_any().downcast_ref::<DummySim>();
        assert!(dummy.is_some(), "expected DummySim state");
        let dummy = dummy.unwrap_or_else(|| unreachable!());
        assert_eq!(dummy.label, expected_label);
    }
}
