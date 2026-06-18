use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{anyhow, ensure, Context, Result};
use tokio::sync::RwLock;
use tokio::time::Instant;
use tycho_simulation::{
    protocol::models::Update as TychoUpdate,
    tycho_client::feed::{
        synchronizer::{Snapshot, StateSyncMessage},
        BlockHeader, FeedMessage,
    },
    tycho_common::{dto::ResponseAccount, Bytes},
};

use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterHeartbeat, BroadcasterPayload,
    BroadcasterProtocolMessage, BroadcasterProtocolSyncStatus, BroadcasterSnapshotChunk,
    BroadcasterSnapshotEnd, BroadcasterSnapshotPartition, BroadcasterSnapshotStart,
    BroadcasterStateDelta, BroadcasterStateEntry, BroadcasterUpdateMessage,
};

use super::redis_publisher::BroadcasterRedisPublisherStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BroadcasterReadiness {
    Ready,
    RedisPublisherUnhealthy,
    SnapshotWarmingUp,
    UpstreamDisconnected,
}

impl BroadcasterReadiness {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UpstreamDisconnected => "upstream_disconnected",
            Self::SnapshotWarmingUp => "snapshot_warming_up",
            Self::RedisPublisherUnhealthy => "redis_publisher_unhealthy",
            Self::Ready => "ready",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BroadcasterStatusSnapshot {
    pub readiness: BroadcasterReadiness,
    pub chain_id: u64,
    pub upstream: BroadcasterUpstreamSnapshot,
    pub snapshot: BroadcasterSnapshotStatus,
    pub subscribers: BroadcasterSubscriberSnapshot,
    pub backends: BTreeMap<BroadcasterBackend, BroadcasterBackendStatus>,
    pub redis_publisher: Option<BroadcasterRedisPublisherStatus>,
}

#[derive(Debug, Clone)]
pub struct BroadcasterUpstreamSnapshot {
    pub connected: bool,
    pub restart_count: u64,
    pub last_error: Option<String>,
    pub last_disconnect_reason: Option<String>,
    pub last_update_age_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct BroadcasterSnapshotStatus {
    pub ready: bool,
    pub stream_id: String,
    pub snapshot_id: String,
    pub configured_backends: Vec<BroadcasterBackend>,
    pub total_states: usize,
    pub max_payload_bytes: usize,
}

#[derive(Debug, Clone, Default)]
pub struct BroadcasterSubscriberSnapshot {
    pub active: usize,
    pub lag_disconnects: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BroadcasterBackendStatus {
    pub block_number: Option<u64>,
    pub pool_count: usize,
    pub sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
}

#[derive(Debug, Clone)]
pub struct BroadcasterSnapshotExport {
    pub stream_id: String,
    pub snapshot_id: String,
    pub max_payload_bytes: usize,
    pub payloads: Vec<BroadcasterPayload>,
}

#[derive(Debug, Clone)]
pub struct BroadcasterLiveState {
    pub stream_id: String,
    pub snapshot_id: String,
}

#[derive(Debug, Clone, Default)]
pub struct BroadcasterUpstreamState {
    inner: Arc<RwLock<BroadcasterUpstreamStateData>>,
}

#[derive(Debug, Default)]
struct BroadcasterUpstreamStateData {
    connected: bool,
    restart_count: u64,
    last_error: Option<String>,
    last_disconnect_reason: Option<String>,
    last_update_at: Option<Instant>,
}

impl BroadcasterUpstreamState {
    pub async fn mark_connected(&self) {
        let mut guard = self.inner.write().await;
        guard.connected = true;
        guard.last_disconnect_reason = None;
    }

    pub async fn record_update(&self) {
        let mut guard = self.inner.write().await;
        guard.connected = true;
        guard.last_update_at = Some(Instant::now());
    }

    pub async fn mark_disconnected(&self, reason: impl Into<String>, last_error: Option<String>) {
        let mut guard = self.inner.write().await;
        guard.connected = false;
        guard.restart_count = guard.restart_count.saturating_add(1);
        guard.last_disconnect_reason = Some(reason.into());
        guard.last_error = last_error;
    }

    pub async fn mark_build_failed(&self, error: impl Into<String>) {
        let error = error.into();
        let mut guard = self.inner.write().await;
        guard.connected = false;
        guard.last_error = Some(error.clone());
        guard.last_disconnect_reason = Some("build_failed".to_string());
    }

    pub async fn snapshot(&self) -> BroadcasterUpstreamSnapshot {
        let guard = self.inner.read().await;
        BroadcasterUpstreamSnapshot {
            connected: guard.connected,
            restart_count: guard.restart_count,
            last_error: guard.last_error.clone(),
            last_disconnect_reason: guard.last_disconnect_reason.clone(),
            last_update_age_ms: guard.last_update_at.map(|instant| {
                Instant::now()
                    .saturating_duration_since(instant)
                    .as_millis() as u64
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BroadcasterSnapshotCache {
    chain_id: u64,
    configured_backends: Vec<BroadcasterBackend>,
    inner: Arc<RwLock<BroadcasterSnapshotCacheData>>,
}

#[derive(Debug)]
struct BroadcasterSnapshotCacheData {
    generation: u64,
    stream_id: String,
    snapshot_id: String,
    partitions: BTreeMap<BroadcasterBackend, BroadcasterPartitionState>,
    known_backends: HashMap<String, BroadcasterBackend>,
}

#[derive(Debug, Clone, Default)]
struct BroadcasterPartitionState {
    block_number: Option<u64>,
    sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
    messages: Vec<BroadcasterProtocolMessage>,
    states: BTreeMap<String, BroadcasterStateEntry>,
}

impl BroadcasterSnapshotCache {
    pub fn new(chain_id: u64, mut configured_backends: Vec<BroadcasterBackend>) -> Self {
        configured_backends.sort();
        configured_backends.dedup();
        let generation = 1;

        Self {
            chain_id,
            configured_backends,
            inner: Arc::new(RwLock::new(BroadcasterSnapshotCacheData {
                generation,
                stream_id: format_stream_id(chain_id, generation),
                snapshot_id: format_snapshot_id(chain_id, generation),
                partitions: BTreeMap::new(),
                known_backends: HashMap::new(),
            })),
        }
    }

    pub async fn reset_generation(&self) -> BroadcasterLiveState {
        let mut guard = self.inner.write().await;
        guard.generation = guard.generation.saturating_add(1);
        guard.stream_id = format_stream_id(self.chain_id, guard.generation);
        guard.snapshot_id = format_snapshot_id(self.chain_id, guard.generation);
        guard.partitions.clear();
        guard.known_backends.clear();

        BroadcasterLiveState {
            stream_id: guard.stream_id.clone(),
            snapshot_id: guard.snapshot_id.clone(),
        }
    }

    pub async fn apply_update(&self, update: &TychoUpdate) -> Result<BroadcasterUpdateMessage> {
        let known_backends = {
            let guard = self.inner.read().await;
            guard.known_backends.clone()
        };
        let message = BroadcasterUpdateMessage::from_tycho_update(update, &known_backends)?;
        ensure_configured_update_backends(&message, &self.configured_backends)?;
        let mut guard = self.inner.write().await;
        apply_update_message(&mut guard, &message)?;
        Ok(message)
    }

    pub async fn apply_feed_message(
        &self,
        feed: &FeedMessage<BlockHeader>,
    ) -> Result<BroadcasterUpdateMessage> {
        let message = BroadcasterUpdateMessage::from_tycho_feed_message(feed)?;
        ensure_configured_update_backends(&message, &self.configured_backends)?;
        let mut guard = self.inner.write().await;
        apply_raw_update_message(&mut guard, &message);
        Ok(message)
    }

    pub async fn export_snapshot(
        &self,
        max_payload_bytes: usize,
    ) -> Result<BroadcasterSnapshotExport> {
        let guard = self.inner.read().await;
        let snapshot_id = guard.snapshot_id.clone();
        let stream_id = guard.stream_id.clone();
        let chunks = build_snapshot_chunks(
            &stream_id,
            &snapshot_id,
            &self.configured_backends,
            &guard.partitions,
            max_payload_bytes,
        )?;
        let total_chunks = chunks.len() as u32;
        let mut payloads = Vec::with_capacity(chunks.len().saturating_add(2));
        payloads.push(BroadcasterPayload::SnapshotStart(
            BroadcasterSnapshotStart::new(
                snapshot_id.clone(),
                self.chain_id,
                self.configured_backends.clone(),
                total_chunks,
            )?,
        ));

        payloads.extend(chunks.into_iter().map(BroadcasterPayload::SnapshotChunk));

        payloads.push(BroadcasterPayload::SnapshotEnd(
            BroadcasterSnapshotEnd::new(snapshot_id),
        ));
        for (index, payload) in payloads.iter().enumerate() {
            ensure_payload_fits(&stream_id, index as u64 + 1, payload, max_payload_bytes)
                .with_context(|| format!("snapshot payload {index} exceeds byte cap"))?;
        }

        Ok(BroadcasterSnapshotExport {
            stream_id,
            snapshot_id: guard.snapshot_id.clone(),
            max_payload_bytes,
            payloads,
        })
    }

    pub async fn heartbeat(&self) -> Result<Option<BroadcasterPayload>> {
        let guard = self.inner.read().await;
        if !self.is_ready_locked(&guard) {
            return Ok(None);
        }

        let backend_heads = self
            .configured_backends
            .iter()
            .filter_map(|backend| {
                guard
                    .partitions
                    .get(backend)
                    .and_then(|partition| partition.block_number)
                    .map(|block_number| BroadcasterBackendHead::new(*backend, block_number))
            })
            .collect();

        Ok(Some(BroadcasterPayload::Heartbeat(
            BroadcasterHeartbeat::new(self.chain_id, guard.snapshot_id.clone(), backend_heads)?,
        )))
    }

    pub async fn live_state(&self) -> BroadcasterLiveState {
        let guard = self.inner.read().await;
        BroadcasterLiveState {
            stream_id: guard.stream_id.clone(),
            snapshot_id: guard.snapshot_id.clone(),
        }
    }

    pub async fn is_ready(&self) -> bool {
        let guard = self.inner.read().await;
        self.is_ready_locked(&guard)
    }

    pub async fn status_snapshot(
        &self,
        max_payload_bytes: usize,
        upstream: BroadcasterUpstreamSnapshot,
        subscribers: BroadcasterSubscriberSnapshot,
    ) -> BroadcasterStatusSnapshot {
        let guard = self.inner.read().await;
        let ready = self.is_ready_locked(&guard);
        let readiness = if !upstream.connected {
            BroadcasterReadiness::UpstreamDisconnected
        } else if ready {
            BroadcasterReadiness::Ready
        } else {
            BroadcasterReadiness::SnapshotWarmingUp
        };

        let backends = self
            .configured_backends
            .iter()
            .map(|backend| {
                let status = guard.partitions.get(backend).cloned().unwrap_or_default();
                (
                    *backend,
                    BroadcasterBackendStatus {
                        block_number: status.block_number,
                        pool_count: status.entry_count(),
                        sync_statuses: status.sync_statuses,
                    },
                )
            })
            .collect();

        BroadcasterStatusSnapshot {
            readiness,
            chain_id: self.chain_id,
            upstream,
            snapshot: BroadcasterSnapshotStatus {
                ready,
                stream_id: guard.stream_id.clone(),
                snapshot_id: guard.snapshot_id.clone(),
                configured_backends: self.configured_backends.clone(),
                total_states: guard
                    .partitions
                    .values()
                    .map(BroadcasterPartitionState::entry_count)
                    .sum(),
                max_payload_bytes,
            },
            subscribers,
            backends,
            redis_publisher: None,
        }
    }

    fn is_ready_locked(&self, guard: &BroadcasterSnapshotCacheData) -> bool {
        self.configured_backends.iter().all(|backend| {
            guard
                .partitions
                .get(backend)
                .and_then(|partition| partition.block_number)
                .is_some()
        })
    }
}

impl BroadcasterPartitionState {
    fn entry_count(&self) -> usize {
        if self.messages.is_empty() {
            self.states.len()
        } else {
            self.messages
                .iter()
                .map(|message| message.message.snapshots.states.len())
                .sum()
        }
    }
}

fn apply_raw_update_message(
    guard: &mut BroadcasterSnapshotCacheData,
    message: &BroadcasterUpdateMessage,
) {
    for partition in &message.partitions {
        let partition_state = guard.partitions.entry(partition.backend).or_default();
        partition_state.block_number = Some(partition.block_number);
        partition_state.sync_statuses = partition.sync_statuses.clone();
        for message in &partition.messages {
            merge_raw_message(&mut partition_state.messages, message.clone());
        }
    }
}

fn ensure_configured_update_backends(
    message: &BroadcasterUpdateMessage,
    configured_backends: &[BroadcasterBackend],
) -> Result<()> {
    for partition in &message.partitions {
        ensure!(
            configured_backends.contains(&partition.backend),
            "update partition backend {} is not configured",
            partition.backend.as_str()
        );
    }
    Ok(())
}

fn merge_raw_message(
    messages: &mut Vec<BroadcasterProtocolMessage>,
    incoming: BroadcasterProtocolMessage,
) {
    if let Some(existing) = messages
        .iter_mut()
        .find(|message| message.protocol == incoming.protocol)
    {
        existing.sync_state = incoming.sync_state;
        existing.message = existing.message.clone().merge(incoming.message);
    } else {
        messages.push(incoming);
    }
    messages.sort_by(|left, right| left.protocol.cmp(&right.protocol));
}

fn apply_update_message(
    guard: &mut BroadcasterSnapshotCacheData,
    message: &BroadcasterUpdateMessage,
) -> Result<()> {
    for partition in &message.partitions {
        let partition_state = guard.partitions.entry(partition.backend).or_default();
        partition_state.block_number = Some(partition.block_number);
        partition_state.sync_statuses = partition.sync_statuses.clone();

        for entry in &partition.new_pairs {
            guard
                .known_backends
                .insert(entry.component_id.clone(), partition.backend);
            partition_state
                .states
                .insert(entry.component_id.clone(), entry.clone());
        }

        for delta in &partition.updated_states {
            apply_state_delta(partition.backend, partition_state, delta)?;
        }

        for removed in &partition.removed_pairs {
            guard.known_backends.remove(&removed.component_id);
            partition_state.states.remove(&removed.component_id);
        }
    }

    Ok(())
}

fn apply_state_delta(
    backend: BroadcasterBackend,
    partition_state: &mut BroadcasterPartitionState,
    delta: &BroadcasterStateDelta,
) -> Result<()> {
    let Some(existing) = partition_state.states.get_mut(&delta.component_id) else {
        return Err(anyhow!(
            "missing tracked broadcaster state for {} on backend {}",
            delta.component_id,
            backend
        ));
    };
    if delta.backend != backend {
        return Err(anyhow!(
            "backend mismatch for {}: expected {}, found {}",
            delta.component_id,
            backend,
            delta.backend
        ));
    }
    existing.state = delta.state.clone();
    Ok(())
}

fn build_snapshot_chunks(
    stream_id: &str,
    snapshot_id: &str,
    configured_backends: &[BroadcasterBackend],
    partitions: &BTreeMap<BroadcasterBackend, BroadcasterPartitionState>,
    max_payload_bytes: usize,
) -> Result<Vec<BroadcasterSnapshotChunk>> {
    let mut chunks = Vec::new();
    for backend in configured_backends {
        let Some(partition) = partitions.get(backend) else {
            continue;
        };
        let partitions = build_partition_snapshot_chunks(
            &SnapshotChunkBuildContext {
                stream_id,
                snapshot_id,
                backend: *backend,
                block_number: partition.block_number.unwrap_or_default(),
                max_payload_bytes,
            },
            partition,
            chunks.len(),
        )?;
        chunks.extend(partitions);
    }
    Ok(chunks)
}

fn build_partition_snapshot_chunks(
    ctx: &SnapshotChunkBuildContext<'_>,
    partition: &BroadcasterPartitionState,
    base_chunk_index: usize,
) -> Result<Vec<BroadcasterSnapshotChunk>> {
    let mut chunks = Vec::new();
    let mut sync_statuses = partition.sync_statuses.clone();

    if !partition.messages.is_empty() {
        let mut messages = Vec::new();
        for message in &partition.messages {
            let fragments = split_protocol_message_for_snapshot(ctx, message, &sync_statuses)?;
            for fragment in fragments {
                let mut candidate = messages.clone();
                candidate.push(fragment.clone());
                if ctx.messages_fit(
                    base_chunk_index + chunks.len(),
                    candidate.clone(),
                    sync_statuses.clone(),
                )? {
                    messages = candidate;
                    continue;
                }

                if messages.is_empty() {
                    return Err(anyhow!(
                        "broadcaster snapshot message for protocol {} exceeds {} bytes",
                        fragment.protocol,
                        ctx.max_payload_bytes
                    ));
                }
                chunks.push(ctx.messages_chunk(
                    base_chunk_index + chunks.len(),
                    std::mem::take(&mut messages),
                    std::mem::take(&mut sync_statuses),
                )?);
                messages.push(fragment);
            }
        }
        if !messages.is_empty() || !sync_statuses.is_empty() {
            chunks.push(ctx.messages_chunk(
                base_chunk_index + chunks.len(),
                messages,
                sync_statuses,
            )?);
        }
        return Ok(chunks);
    }

    let mut states = Vec::new();
    for state in partition.states.values() {
        let mut candidate = states.clone();
        candidate.push(state.clone());
        if ctx.states_fit(
            base_chunk_index + chunks.len(),
            candidate.clone(),
            sync_statuses.clone(),
        )? {
            states = candidate;
            continue;
        }

        if states.is_empty() {
            return Err(anyhow!(
                "broadcaster snapshot state {} exceeds {} bytes",
                state.component_id,
                ctx.max_payload_bytes
            ));
        }
        chunks.push(ctx.states_chunk(
            base_chunk_index + chunks.len(),
            std::mem::take(&mut states),
            std::mem::take(&mut sync_statuses),
        )?);
        states.push(state.clone());
    }
    if !states.is_empty() || !sync_statuses.is_empty() {
        chunks.push(ctx.states_chunk(base_chunk_index + chunks.len(), states, sync_statuses)?);
    }
    Ok(chunks)
}

fn split_protocol_message_for_snapshot(
    ctx: &SnapshotChunkBuildContext<'_>,
    message: &BroadcasterProtocolMessage,
    sync_statuses: &BTreeMap<String, BroadcasterProtocolSyncStatus>,
) -> Result<Vec<BroadcasterProtocolMessage>> {
    if message.message.snapshots.vm_storage.is_empty()
        && ctx.messages_fit(
            WORST_CASE_SNAPSHOT_CHUNK_INDEX,
            vec![message.clone()],
            sync_statuses.clone(),
        )?
    {
        return Ok(vec![message.clone()]);
    }

    let mut states = message
        .message
        .snapshots
        .states
        .iter()
        .map(|(component_id, state)| (component_id.clone(), state.clone()))
        .collect::<Vec<_>>();
    states.sort_by(|left, right| left.0.cmp(&right.0));
    let mut vm_storage = message
        .message
        .snapshots
        .vm_storage
        .iter()
        .collect::<Vec<_>>();
    vm_storage.sort_by(|left, right| left.0.cmp(right.0));

    let mut fragments = Vec::new();
    let mut current = empty_protocol_fragment(message, false);
    let mut current_has_payload = false;

    for (component_id, state) in states {
        let mut candidate = current.clone();
        candidate
            .message
            .snapshots
            .states
            .insert(component_id.clone(), state.clone());
        if ctx.raw_fragment_fits(candidate.clone(), sync_statuses, fragments.is_empty())? {
            current = candidate;
            current_has_payload = true;
            continue;
        }
        ensure!(
            current_has_payload,
            "broadcaster snapshot state fragment for protocol {} exceeds {} bytes",
            message.protocol,
            ctx.max_payload_bytes
        );
        fragments.push(current);
        current = empty_protocol_fragment(message, false);
        current.message.snapshots.states.insert(component_id, state);
        ensure!(
            ctx.raw_fragment_fits(current.clone(), sync_statuses, fragments.is_empty(),)?,
            "broadcaster snapshot state fragment for protocol {} exceeds {} bytes",
            message.protocol,
            ctx.max_payload_bytes
        );
        current_has_payload = true;
    }

    for (address, account) in vm_storage {
        if current_has_payload {
            fragments.push(current);
            current = empty_protocol_fragment(message, false);
            current_has_payload = false;
        }
        let account_fragments = split_vm_storage_account_for_snapshot(
            ctx,
            message,
            sync_statuses,
            address.clone(),
            account,
            fragments.is_empty(),
        )?;
        fragments.extend(account_fragments);
    }

    let has_tail =
        message.message.deltas.is_some() || !message.message.removed_components.is_empty();
    if !current_has_payload && !has_tail {
        return Ok(fragments);
    }

    let mut final_fragment = if current_has_payload {
        current.clone()
    } else {
        empty_protocol_fragment(message, false)
    };
    final_fragment.message.deltas = message.message.deltas.clone();
    final_fragment.message.removed_components = message.message.removed_components.clone();
    if ctx.raw_fragment_fits(final_fragment.clone(), sync_statuses, fragments.is_empty())? {
        fragments.push(final_fragment);
    } else {
        if current_has_payload {
            fragments.push(current);
        }
        let tail = empty_protocol_fragment(message, true);
        ensure!(
            ctx.raw_fragment_fits(tail.clone(), sync_statuses, fragments.is_empty(),)?,
            "broadcaster snapshot delta/removal fragment for protocol {} exceeds {} bytes",
            message.protocol,
            ctx.max_payload_bytes
        );
        fragments.push(tail);
    }

    Ok(fragments)
}

fn empty_protocol_fragment(
    message: &BroadcasterProtocolMessage,
    include_tail: bool,
) -> BroadcasterProtocolMessage {
    let (deltas, removed_components) = if include_tail {
        (
            message.message.deltas.clone(),
            message.message.removed_components.clone(),
        )
    } else {
        (None, HashMap::new())
    };

    // Build this structurally so large raw snapshots are never cloned just to be cleared.
    BroadcasterProtocolMessage::new(
        message.protocol.clone(),
        message.sync_state.clone(),
        StateSyncMessage {
            header: message.message.header.clone(),
            snapshots: Snapshot::default(),
            deltas,
            removed_components,
        },
    )
}

fn split_vm_storage_account_for_snapshot(
    ctx: &SnapshotChunkBuildContext<'_>,
    message: &BroadcasterProtocolMessage,
    sync_statuses: &BTreeMap<String, BroadcasterProtocolSyncStatus>,
    address: Bytes,
    account: &ResponseAccount,
    include_sync_statuses: bool,
) -> Result<Vec<BroadcasterProtocolMessage>> {
    let mut slots = account
        .slots
        .iter()
        .map(|(slot, value)| (slot.clone(), value.clone()))
        .collect::<Vec<_>>();
    slots.sort_by(|left, right| left.0.cmp(&right.0));

    let mut fragments = Vec::new();
    if slots.is_empty() {
        let fragment =
            vm_storage_account_fragment_for_slot_range(message, address.clone(), account, &[]);
        ensure!(
            ctx.raw_fragment_fits(fragment.clone(), sync_statuses, include_sync_statuses)?,
            "broadcaster snapshot VM storage account metadata for protocol {} account {} exceeds {} bytes",
            message.protocol,
            address,
            ctx.max_payload_bytes
        );
        return Ok(vec![fragment]);
    }

    let mut start = 0usize;
    let mut include_sync_for_fragment = include_sync_statuses;
    while start < slots.len() {
        let metadata_fragment =
            vm_storage_account_fragment_for_slot_range(message, address.clone(), account, &[]);
        let metadata_size =
            ctx.raw_fragment_size(metadata_fragment, sync_statuses, include_sync_for_fragment)?;
        ensure!(
            metadata_size < ctx.max_payload_bytes,
            "broadcaster snapshot VM storage account metadata for protocol {} account {} exceeds {} bytes",
            message.protocol,
            address,
            ctx.max_payload_bytes
        );

        let mut estimated_size = metadata_size;
        let mut end = start;
        while end < slots.len() {
            let next_size = estimated_size.saturating_add(estimated_slot_entry_size(&slots[end]));
            if next_size > ctx.max_payload_bytes {
                break;
            }
            estimated_size = next_size;
            end += 1;
        }

        if end == start {
            return Err(anyhow!(
                "broadcaster snapshot VM storage slot fragment for protocol {} account {} exceeds {} bytes",
                message.protocol,
                address,
                ctx.max_payload_bytes
            ));
        }

        let mut fragment = vm_storage_account_fragment_for_slot_range(
            message,
            address.clone(),
            account,
            &slots[start..end],
        );
        while !ctx.raw_fragment_fits(fragment.clone(), sync_statuses, include_sync_for_fragment)? {
            end = end.saturating_sub(1);
            if end == start {
                return Err(anyhow!(
                    "broadcaster snapshot VM storage slot fragment for protocol {} account {} exceeds {} bytes",
                    message.protocol,
                    address,
                    ctx.max_payload_bytes
                ));
            }
            fragment = vm_storage_account_fragment_for_slot_range(
                message,
                address.clone(),
                account,
                &slots[start..end],
            );
        }
        fragments.push(fragment);
        include_sync_for_fragment = false;
        start = end;
    }

    Ok(fragments)
}

fn estimated_slot_entry_size((slot, value): &(Bytes, Bytes)) -> usize {
    // Pessimistic JSON size for one `"0x...":"0x..."` storage entry plus a comma.
    hex_json_string_size(slot)
        .saturating_add(1)
        .saturating_add(hex_json_string_size(value))
        .saturating_add(1)
}

fn hex_json_string_size(bytes: &Bytes) -> usize {
    4usize.saturating_add(bytes.len().saturating_mul(2))
}

fn vm_storage_account_fragment_for_slot_range(
    message: &BroadcasterProtocolMessage,
    address: Bytes,
    account: &ResponseAccount,
    slots: &[(Bytes, Bytes)],
) -> BroadcasterProtocolMessage {
    let account = response_account_with_slots(account, slots.iter().cloned().collect());
    vm_storage_account_fragment(message, address, account)
}

#[expect(
    deprecated,
    reason = "creation_tx is deprecated but still part of the broadcaster wire DTO"
)]
fn response_account_with_slots(
    account: &ResponseAccount,
    slots: HashMap<Bytes, Bytes>,
) -> ResponseAccount {
    ResponseAccount::new(
        account.chain,
        account.address.clone(),
        account.title.clone(),
        slots,
        account.native_balance.clone(),
        account.token_balances.clone(),
        account.code.clone(),
        account.code_hash.clone(),
        account.balance_modify_tx.clone(),
        account.code_modify_tx.clone(),
        account.creation_tx.clone(),
    )
}

fn vm_storage_account_fragment(
    message: &BroadcasterProtocolMessage,
    address: Bytes,
    account: ResponseAccount,
) -> BroadcasterProtocolMessage {
    let mut fragment = empty_protocol_fragment(message, false);
    fragment
        .message
        .snapshots
        .vm_storage
        .insert(address, account);
    fragment
}

const WORST_CASE_SNAPSHOT_CHUNK_INDEX: usize = u32::MAX as usize;

struct SnapshotChunkBuildContext<'a> {
    stream_id: &'a str,
    snapshot_id: &'a str,
    backend: BroadcasterBackend,
    block_number: u64,
    max_payload_bytes: usize,
}

impl SnapshotChunkBuildContext<'_> {
    fn raw_fragment_fits(
        &self,
        message: BroadcasterProtocolMessage,
        sync_statuses: &BTreeMap<String, BroadcasterProtocolSyncStatus>,
        include_sync_statuses: bool,
    ) -> Result<bool> {
        self.raw_fragment_size(message, sync_statuses, include_sync_statuses)
            .map(|size| size <= self.max_payload_bytes)
    }

    fn raw_fragment_size(
        &self,
        message: BroadcasterProtocolMessage,
        sync_statuses: &BTreeMap<String, BroadcasterProtocolSyncStatus>,
        include_sync_statuses: bool,
    ) -> Result<usize> {
        self.messages_size(
            WORST_CASE_SNAPSHOT_CHUNK_INDEX,
            vec![message],
            if include_sync_statuses {
                sync_statuses.clone()
            } else {
                BTreeMap::new()
            },
        )
    }

    fn messages_fit(
        &self,
        chunk_index: usize,
        messages: Vec<BroadcasterProtocolMessage>,
        sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
    ) -> Result<bool> {
        self.messages_size(chunk_index, messages, sync_statuses)
            .map(|size| size <= self.max_payload_bytes)
    }

    fn messages_size(
        &self,
        chunk_index: usize,
        messages: Vec<BroadcasterProtocolMessage>,
        sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
    ) -> Result<usize> {
        self.chunk_size(self.messages_chunk(chunk_index, messages, sync_statuses)?)
    }

    fn states_fit(
        &self,
        chunk_index: usize,
        states: Vec<BroadcasterStateEntry>,
        sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
    ) -> Result<bool> {
        self.chunk_fits(self.states_chunk(chunk_index, states, sync_statuses)?)
    }

    fn messages_chunk(
        &self,
        chunk_index: usize,
        messages: Vec<BroadcasterProtocolMessage>,
        sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
    ) -> Result<BroadcasterSnapshotChunk> {
        self.snapshot_chunk(
            chunk_index,
            BroadcasterSnapshotPartition::with_messages(
                self.backend,
                self.block_number,
                messages,
                sync_statuses,
            ),
        )
    }

    fn states_chunk(
        &self,
        chunk_index: usize,
        states: Vec<BroadcasterStateEntry>,
        sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
    ) -> Result<BroadcasterSnapshotChunk> {
        self.snapshot_chunk(
            chunk_index,
            BroadcasterSnapshotPartition::new(
                self.backend,
                self.block_number,
                states,
                sync_statuses,
            ),
        )
    }

    fn snapshot_chunk(
        &self,
        chunk_index: usize,
        partition: BroadcasterSnapshotPartition,
    ) -> Result<BroadcasterSnapshotChunk> {
        let chunk_index =
            u32::try_from(chunk_index).context("snapshot chunk index exceeds u32 range")?;
        BroadcasterSnapshotChunk::new(self.snapshot_id.to_string(), chunk_index, vec![partition])
            .map_err(Into::into)
    }

    fn chunk_fits(&self, chunk: BroadcasterSnapshotChunk) -> Result<bool> {
        self.chunk_size(chunk)
            .map(|size| size <= self.max_payload_bytes)
    }

    fn chunk_size(&self, chunk: BroadcasterSnapshotChunk) -> Result<usize> {
        payload_size(self.stream_id, &BroadcasterPayload::SnapshotChunk(chunk))
    }
}

fn ensure_payload_fits(
    stream_id: &str,
    message_seq: u64,
    payload: &BroadcasterPayload,
    max_payload_bytes: usize,
) -> Result<()> {
    let envelope = simulator_core::broadcaster::BroadcasterEnvelope::new(
        stream_id.to_string(),
        message_seq,
        payload.clone(),
    );
    let size = serde_json::to_vec(&envelope)?.len();
    ensure!(
        size <= max_payload_bytes,
        "serialized broadcaster snapshot payload is {size} bytes, above configured max {max_payload_bytes}"
    );
    Ok(())
}

fn payload_size(stream_id: &str, payload: &BroadcasterPayload) -> Result<usize> {
    let envelope = simulator_core::broadcaster::BroadcasterEnvelope::new(
        stream_id.to_string(),
        u64::MAX,
        payload.clone(),
    );
    Ok(serde_json::to_vec(&envelope)?.len())
}

fn format_stream_id(chain_id: u64, generation: u64) -> String {
    format!("chain-{chain_id}-stream-{generation}")
}

fn format_snapshot_id(chain_id: u64, generation: u64) -> String {
    format!("chain-{chain_id}-snapshot-{generation}")
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use super::{
        BroadcasterReadiness, BroadcasterSnapshotCache, BroadcasterSnapshotExport,
        BroadcasterSubscriberSnapshot, BroadcasterUpstreamState,
    };
    use anyhow::{anyhow, Result};
    use num_bigint::BigUint;
    use simulator_core::broadcaster::{
        BroadcasterBackend, BroadcasterEnvelope, BroadcasterPayload, BroadcasterProtocolMessage,
        BroadcasterProtocolSyncStatus, BroadcasterProtocolSyncStatusKind, BroadcasterSnapshotChunk,
        BroadcasterSubscriptionEvent, BroadcasterSubscriptionTracker,
    };
    use tycho_common::{
        dto::{ProtocolComponent as DtoProtocolComponent, ResponseAccount, ResponseProtocolState},
        models::Chain as DtoChain,
        Bytes as DtoBytes,
    };
    use tycho_simulation::tycho_common::dto::ProtocolStateDelta;
    use tycho_simulation::tycho_common::simulation::errors::{SimulationError, TransitionError};
    use tycho_simulation::{
        protocol::models::{ProtocolComponent, Update},
        tycho_client::feed::{
            synchronizer::{ComponentWithState, Snapshot, StateSyncMessage},
            BlockHeader, FeedMessage, SynchronizerState,
        },
        tycho_common::{
            models::{token::Token, Chain},
            simulation::protocol_sim::{Balances, GetAmountOutResult, ProtocolSim},
            Bytes,
        },
    };

    #[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
    struct DummySim(u8);

    #[typetag::serde]
    impl ProtocolSim for DummySim {
        fn fee(&self) -> f64 {
            0.0
        }

        fn spot_price(&self, _base: &Token, _quote: &Token) -> Result<f64, SimulationError> {
            Ok(0.0)
        }

        fn get_amount_out(
            &self,
            amount_in: BigUint,
            _token_in: &Token,
            _token_out: &Token,
        ) -> Result<GetAmountOutResult, SimulationError> {
            Ok(GetAmountOutResult::new(
                amount_in,
                BigUint::from(0u8),
                self.clone_box(),
            ))
        }

        fn get_limits(
            &self,
            _sell_token: Bytes,
            _buy_token: Bytes,
        ) -> Result<(BigUint, BigUint), SimulationError> {
            Ok((BigUint::from(0u8), BigUint::from(0u8)))
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

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }

        fn eq(&self, other: &dyn ProtocolSim) -> bool {
            other
                .as_any()
                .downcast_ref::<DummySim>()
                .map(|value| value.0 == self.0)
                .unwrap_or(false)
        }
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

    fn snapshot_chunk_build_context(
        max_payload_bytes: usize,
    ) -> super::SnapshotChunkBuildContext<'static> {
        snapshot_chunk_build_context_for_backend(max_payload_bytes, BroadcasterBackend::Native)
    }

    fn snapshot_chunk_build_context_for_backend(
        max_payload_bytes: usize,
        backend: BroadcasterBackend,
    ) -> super::SnapshotChunkBuildContext<'static> {
        super::SnapshotChunkBuildContext {
            stream_id: "chain-1-stream-1",
            snapshot_id: "chain-1-snapshot-1",
            backend,
            block_number: 10,
            max_payload_bytes,
        }
    }

    fn snapshot_chunks(export: &BroadcasterSnapshotExport) -> Vec<&BroadcasterSnapshotChunk> {
        export
            .payloads
            .iter()
            .filter_map(|payload| match payload {
                BroadcasterPayload::SnapshotChunk(chunk) => Some(chunk),
                _ => None,
            })
            .collect()
    }

    fn first_snapshot_chunk_size(export: &BroadcasterSnapshotExport) -> Result<usize> {
        snapshot_chunks(export)
            .first()
            .map(|chunk| {
                super::payload_size(
                    &export.stream_id,
                    &BroadcasterPayload::SnapshotChunk((*chunk).clone()),
                )
            })
            .ok_or_else(|| anyhow!("expected snapshot chunk"))?
    }

    fn assert_payloads_smaller_than(
        export: &BroadcasterSnapshotExport,
        max_size: usize,
    ) -> Result<()> {
        for (message_seq, payload) in export.payloads.iter().cloned().enumerate() {
            let size = serde_json::to_vec(&BroadcasterEnvelope::new(
                export.stream_id.clone(),
                message_seq as u64 + 1,
                payload,
            ))?
            .len();
            assert!(size < max_size);
        }
        Ok(())
    }

    fn assert_payloads_at_most(export: &BroadcasterSnapshotExport, max_size: usize) -> Result<()> {
        for (message_seq, payload) in export.payloads.iter().cloned().enumerate() {
            let size = serde_json::to_vec(&BroadcasterEnvelope::new(
                export.stream_id.clone(),
                message_seq as u64 + 1,
                payload,
            ))?
            .len();
            assert!(size <= max_size);
        }
        Ok(())
    }

    fn vm_account_fragments<'a>(
        export: &'a BroadcasterSnapshotExport,
        account_address: &DtoBytes,
    ) -> Vec<&'a ResponseAccount> {
        snapshot_chunks(export)
            .into_iter()
            .flat_map(|chunk| &chunk.partitions)
            .flat_map(|partition| &partition.messages)
            .filter_map(|message| message.message.snapshots.vm_storage.get(account_address))
            .collect()
    }

    fn vm_account_slot_key_batches(
        export: &BroadcasterSnapshotExport,
        account_address: &DtoBytes,
    ) -> Vec<Vec<DtoBytes>> {
        vm_account_fragments(export, account_address)
            .into_iter()
            .map(|account| {
                let mut slot_keys = account.slots.keys().cloned().collect::<Vec<_>>();
                slot_keys.sort();
                slot_keys
            })
            .collect()
    }

    fn account_metadata_without_slots(account: &ResponseAccount) -> ResponseAccount {
        let mut metadata = account.clone();
        metadata.slots.clear();
        metadata
    }

    fn assert_vm_storage_account_fragments_match(
        export: &BroadcasterSnapshotExport,
        account_address: &DtoBytes,
        expected_account: &ResponseAccount,
    ) {
        let fragments = vm_account_fragments(export, account_address);
        assert!(fragments.len() > 1);

        let expected_metadata = account_metadata_without_slots(expected_account);
        let mut observed_slots = HashMap::new();
        let mut emitted_slot_count = 0usize;

        for fragment in fragments {
            assert_eq!(
                account_metadata_without_slots(fragment),
                expected_metadata,
                "VM account fragment metadata changed"
            );
            emitted_slot_count += fragment.slots.len();

            for (slot, value) in &fragment.slots {
                assert!(
                    observed_slots.insert(slot.clone(), value.clone()).is_none(),
                    "duplicate VM storage slot emitted: {slot:?}"
                );
            }
        }

        assert_eq!(emitted_slot_count, expected_account.slots.len());
        assert_eq!(observed_slots, expected_account.slots);
    }

    #[tokio::test]
    async fn cache_applies_updates_and_exports_snapshot() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(
            1,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Vm],
        );
        let update = mixed_update();
        cache.apply_update(&update).await?;

        let export = cache.export_snapshot(8_388_608).await?;
        assert_eq!(export.stream_id, "chain-1-stream-1");
        assert!(matches!(
            export.payloads.first(),
            Some(BroadcasterPayload::SnapshotStart(_))
        ));
        assert!(matches!(
            export.payloads.last(),
            Some(BroadcasterPayload::SnapshotEnd(_))
        ));
        assert_eq!(
            export
                .payloads
                .iter()
                .filter(|payload| matches!(payload, BroadcasterPayload::SnapshotChunk(_)))
                .count(),
            2
        );

        let heartbeat = cache.heartbeat().await?;
        assert!(matches!(heartbeat, Some(BroadcasterPayload::Heartbeat(_))));
        Ok(())
    }

    #[tokio::test]
    async fn cache_exports_decoded_snapshot_by_serialized_payload_bytes() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        cache.apply_update(&multi_native_update()).await?;

        let full_export = cache.export_snapshot(8_388_608).await?;
        let full_chunk_size = first_snapshot_chunk_size(&full_export)?;

        let export = cache.export_snapshot(full_chunk_size - 1).await?;
        let chunks = snapshot_chunks(&export);

        assert!(chunks.len() > 1);
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.partitions[0].states.len())
                .sum::<usize>(),
            3
        );
        assert_payloads_smaller_than(&export, full_chunk_size)?;
        Ok(())
    }

    #[tokio::test]
    async fn cache_splits_raw_snapshot_message_by_serialized_payload_bytes() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let feed = FeedMessage {
            state_msgs: HashMap::from([(
                "uniswap_v2".to_string(),
                StateSyncMessage {
                    header: block_header(10, 1),
                    snapshots: Snapshot {
                        states: HashMap::from([
                            ("raw-1".to_string(), raw_component_with_state("raw-1", 1)),
                            ("raw-2".to_string(), raw_component_with_state("raw-2", 2)),
                        ]),
                        vm_storage: HashMap::new(),
                    },
                    deltas: None,
                    removed_components: HashMap::new(),
                },
            )]),
            sync_states: HashMap::from([(
                "uniswap_v2".to_string(),
                SynchronizerState::Ready(block_header(10, 1)),
            )]),
        };
        cache.apply_feed_message(&feed).await?;

        let full_export = cache.export_snapshot(8_388_608).await?;
        let full_chunk_size = first_snapshot_chunk_size(&full_export)?;

        let export = cache.export_snapshot(full_chunk_size - 1).await?;
        let chunks = snapshot_chunks(&export);

        assert!(chunks.len() > 1);
        assert_eq!(
            chunks
                .iter()
                .flat_map(|chunk| &chunk.partitions)
                .flat_map(|partition| &partition.messages)
                .map(|message| message.message.snapshots.states.len())
                .sum::<usize>(),
            2
        );
        assert_payloads_smaller_than(&export, full_chunk_size)?;
        Ok(())
    }

    #[test]
    fn raw_fragment_sizing_reserves_chunk_index_digit_growth() -> Result<()> {
        let message = raw_protocol_message_with_states(HashMap::from([(
            "raw-1".to_string(),
            raw_component_with_state("raw-1", 1),
        )]));
        let sync_statuses = BTreeMap::new();
        let ctx = snapshot_chunk_build_context(usize::MAX);
        let size_at_zero = ctx.messages_size(0, vec![message.clone()], sync_statuses.clone())?;
        let size_at_worst = ctx.messages_size(
            super::WORST_CASE_SNAPSHOT_CHUNK_INDEX,
            vec![message.clone()],
            sync_statuses.clone(),
        )?;
        assert!(size_at_worst > size_at_zero);

        let capped_ctx = snapshot_chunk_build_context(size_at_zero);
        assert!(capped_ctx.messages_fit(0, vec![message.clone()], BTreeMap::new())?);
        assert!(!capped_ctx.raw_fragment_fits(message, &sync_statuses, false)?);
        Ok(())
    }

    #[tokio::test]
    async fn raw_vm_slot_split_handles_chunk_index_digit_boundary() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Vm]);
        let account_address = DtoBytes::from([44u8; 20]);
        let account = raw_response_account(account_address.clone(), 12, 256);
        let message = raw_vm_protocol_message(account_address.clone(), account.clone());
        let mut slots = account.slots.iter().collect::<Vec<_>>();
        slots.sort_by(|left, right| left.0.cmp(right.0));
        let first_slot = vec![((*slots[0].0).clone(), (*slots[0].1).clone())];
        let ctx = snapshot_chunk_build_context_for_backend(usize::MAX, BroadcasterBackend::Vm);
        let metadata_fragment = super::vm_storage_account_fragment_for_slot_range(
            &message,
            account_address.clone(),
            &account,
            &[],
        );
        let single_slot_fragment = super::vm_storage_account_fragment_for_slot_range(
            &message,
            account_address,
            &account,
            &first_slot,
        );
        let metadata_size = ctx.raw_fragment_size(metadata_fragment, &BTreeMap::new(), false)?;
        let max_payload_bytes =
            metadata_size.saturating_add(super::estimated_slot_entry_size(&first_slot[0]));
        assert!(
            ctx.raw_fragment_size(single_slot_fragment, &BTreeMap::new(), false)?
                <= max_payload_bytes
        );

        let feed = FeedMessage {
            state_msgs: HashMap::from([("vm:balancer_v2".to_string(), message.message)]),
            sync_states: HashMap::new(),
        };
        cache.apply_feed_message(&feed).await?;

        let export = cache.export_snapshot(max_payload_bytes).await?;
        let chunks = snapshot_chunks(&export);
        assert!(chunks.iter().any(|chunk| chunk.chunk_index == 9));
        assert!(chunks.iter().any(|chunk| chunk.chunk_index == 10));
        assert_payloads_at_most(&export, max_payload_bytes)?;
        Ok(())
    }

    #[test]
    fn empty_protocol_fragment_preserves_metadata_and_tail() {
        let account_address = DtoBytes::from([42u8; 20]);
        let message = BroadcasterProtocolMessage::new(
            "vm:balancer_v2",
            SynchronizerState::Ready(block_header(20, 2)),
            StateSyncMessage {
                header: block_header(20, 2),
                snapshots: Snapshot {
                    states: HashMap::from([(
                        "raw-1".to_string(),
                        raw_component_with_state("raw-1", 1),
                    )]),
                    vm_storage: HashMap::from([(
                        account_address.clone(),
                        raw_response_account(account_address, 1, 32),
                    )]),
                },
                deltas: Some(Default::default()),
                removed_components: HashMap::from([(
                    "removed-1".to_string(),
                    raw_component("removed-1", "uniswap_v2", 3),
                )]),
            },
        );

        let without_tail = super::empty_protocol_fragment(&message, false);
        assert_eq!(without_tail.protocol, message.protocol);
        assert_eq!(without_tail.sync_state, message.sync_state);
        assert_eq!(without_tail.message.header, message.message.header);
        assert!(without_tail.message.snapshots.states.is_empty());
        assert!(without_tail.message.snapshots.vm_storage.is_empty());
        assert!(without_tail.message.deltas.is_none());
        assert!(without_tail.message.removed_components.is_empty());

        let with_tail = super::empty_protocol_fragment(&message, true);
        assert_eq!(with_tail.protocol, message.protocol);
        assert_eq!(with_tail.sync_state, message.sync_state);
        assert_eq!(with_tail.message.header, message.message.header);
        assert!(with_tail.message.snapshots.states.is_empty());
        assert!(with_tail.message.snapshots.vm_storage.is_empty());
        assert_eq!(with_tail.message.deltas, message.message.deltas);
        assert_eq!(
            with_tail.message.removed_components,
            message.message.removed_components
        );
    }

    #[tokio::test]
    async fn cache_splits_oversized_raw_vm_account_by_storage_slots() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Vm]);
        let account_address = DtoBytes::from([42u8; 20]);
        let account = raw_response_account(account_address.clone(), 8, 512);
        let expected_account = account.clone();
        let feed = FeedMessage {
            state_msgs: HashMap::from([(
                "vm:balancer_v2".to_string(),
                StateSyncMessage {
                    header: block_header(10, 1),
                    snapshots: Snapshot {
                        states: HashMap::new(),
                        vm_storage: HashMap::from([(account_address.clone(), account)]),
                    },
                    deltas: None,
                    removed_components: HashMap::new(),
                },
            )]),
            sync_states: HashMap::from([(
                "vm:balancer_v2".to_string(),
                SynchronizerState::Ready(block_header(10, 1)),
            )]),
        };
        cache.apply_feed_message(&feed).await?;

        let full_export = cache.export_snapshot(8_388_608).await?;
        let full_chunk_size = first_snapshot_chunk_size(&full_export)?;
        let max_payload_bytes = full_chunk_size / 2;
        let export = cache.export_snapshot(max_payload_bytes).await?;
        let slot_key_batches = vm_account_slot_key_batches(&export, &account_address);
        let repeated_export = cache.export_snapshot(max_payload_bytes).await?;

        assert_eq!(
            slot_key_batches,
            vm_account_slot_key_batches(&repeated_export, &account_address)
        );
        assert_vm_storage_account_fragments_match(&export, &account_address, &expected_account);
        assert_payloads_at_most(&export, max_payload_bytes)?;
        Ok(())
    }

    #[tokio::test]
    async fn cache_splits_raw_vm_account_above_default_payload_cap() -> Result<()> {
        const MAX_PAYLOAD_BYTES: usize = 8_388_608;

        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Vm]);
        let account_address = DtoBytes::from([43u8; 20]);
        let account = raw_response_account(account_address.clone(), 10_000, 1_000);
        let expected_account = account.clone();
        let feed = FeedMessage {
            state_msgs: HashMap::from([(
                "vm:balancer_v2".to_string(),
                StateSyncMessage {
                    header: block_header(10, 1),
                    snapshots: Snapshot {
                        states: HashMap::new(),
                        vm_storage: HashMap::from([(account_address.clone(), account)]),
                    },
                    deltas: None,
                    removed_components: HashMap::new(),
                },
            )]),
            sync_states: HashMap::from([(
                "vm:balancer_v2".to_string(),
                SynchronizerState::Ready(block_header(10, 1)),
            )]),
        };
        cache.apply_feed_message(&feed).await?;

        let unsplit_export = cache.export_snapshot(usize::MAX).await?;
        assert!(first_snapshot_chunk_size(&unsplit_export)? > MAX_PAYLOAD_BYTES);

        let export = cache.export_snapshot(MAX_PAYLOAD_BYTES).await?;
        assert_vm_storage_account_fragments_match(&export, &account_address, &expected_account);
        assert_payloads_at_most(&export, MAX_PAYLOAD_BYTES)?;
        Ok(())
    }

    #[tokio::test]
    async fn cache_exports_empty_backend_partition_in_first_snapshot_chunk() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(
            1,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Vm],
        );
        cache.apply_update(&mixed_update()).await?;
        cache.apply_update(&vm_sync_only_update()).await?;

        let export = cache.export_snapshot(8_388_608).await?;
        let Some(BroadcasterPayload::SnapshotChunk(chunk)) =
            export.payloads.iter().find(|payload| {
                matches!(
                    payload,
                    BroadcasterPayload::SnapshotChunk(chunk)
                        if chunk
                            .partitions
                            .iter()
                            .any(|partition| partition.backend == BroadcasterBackend::Vm)
                )
            })
        else {
            return Err(anyhow!("expected vm snapshot_chunk payload"));
        };

        let Some(vm_partition) = chunk
            .partitions
            .iter()
            .find(|partition| partition.backend == BroadcasterBackend::Vm)
        else {
            return Err(anyhow!("expected vm snapshot partition"));
        };
        assert!(vm_partition.states.is_empty());
        assert_eq!(vm_partition.block_number, 11);
        assert_eq!(
            vm_partition.sync_statuses["vm:curve"].kind,
            BroadcasterProtocolSyncStatusKind::Ready
        );

        let mut tracker = BroadcasterSubscriptionTracker::new();
        let mut observed_events = Vec::new();
        for (message_seq, payload) in export.payloads.iter().cloned().enumerate() {
            let envelope =
                BroadcasterEnvelope::new(export.stream_id.clone(), message_seq as u64 + 1, payload);
            observed_events.push(tracker.observe(&envelope)?);
        }
        assert_eq!(
            observed_events,
            vec![
                BroadcasterSubscriptionEvent::SnapshotStarted {
                    snapshot_id: "chain-1-snapshot-1".to_string(),
                },
                BroadcasterSubscriptionEvent::SnapshotChunkAccepted {
                    snapshot_id: "chain-1-snapshot-1".to_string(),
                    chunk_index: 0,
                },
                BroadcasterSubscriptionEvent::SnapshotChunkAccepted {
                    snapshot_id: "chain-1-snapshot-1".to_string(),
                    chunk_index: 1,
                },
                BroadcasterSubscriptionEvent::SnapshotCompleted {
                    snapshot_id: "chain-1-snapshot-1".to_string(),
                },
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn cache_exports_empty_rfq_backend_partition_in_snapshot() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(
            1,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq],
        );
        cache.apply_update(&native_only_update()).await?;
        cache.apply_update(&rfq_sync_only_update()).await?;

        let export = cache.export_snapshot(8_388_608).await?;
        let Some(BroadcasterPayload::SnapshotChunk(chunk)) =
            export.payloads.iter().find(|payload| {
                matches!(
                    payload,
                    BroadcasterPayload::SnapshotChunk(chunk)
                        if chunk
                            .partitions
                            .iter()
                            .any(|partition| partition.backend == BroadcasterBackend::Rfq)
                )
            })
        else {
            return Err(anyhow!("expected rfq snapshot_chunk payload"));
        };

        let Some(rfq_partition) = chunk
            .partitions
            .iter()
            .find(|partition| partition.backend == BroadcasterBackend::Rfq)
        else {
            return Err(anyhow!("expected rfq snapshot partition"));
        };
        assert!(rfq_partition.states.is_empty());
        assert_eq!(rfq_partition.block_number, 12);
        assert_eq!(
            rfq_partition.sync_statuses["rfq:hashflow"].kind,
            BroadcasterProtocolSyncStatusKind::Ready
        );

        Ok(())
    }

    #[tokio::test]
    async fn cache_applies_rfq_update_to_rfq_partition() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(
            1,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq],
        );
        cache.apply_update(&native_only_update()).await?;
        let message = cache.apply_update(&rfq_only_update(12, "rfq-1", 3)).await?;

        let rfq = message
            .partitions
            .iter()
            .find(|partition| partition.backend == BroadcasterBackend::Rfq)
            .ok_or_else(|| anyhow!("expected rfq update partition"))?;
        assert_eq!(rfq.block_number, 12);
        assert_eq!(rfq.new_pairs.len(), 1);
        assert_eq!(rfq.new_pairs[0].component_id, "rfq-1");

        let status = cache
            .status_snapshot(
                8_388_608,
                connected_upstream().await,
                BroadcasterSubscriberSnapshot::default(),
            )
            .await;
        let rfq_status = status
            .backends
            .get(&BroadcasterBackend::Rfq)
            .ok_or_else(|| anyhow!("expected rfq backend status"))?;
        assert_eq!(rfq_status.block_number, Some(12));
        assert_eq!(rfq_status.pool_count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn status_snapshot_and_heartbeat_report_rfq_backend() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(
            1,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq],
        );
        cache.apply_update(&native_only_update()).await?;
        cache.apply_update(&rfq_only_update(12, "rfq-1", 3)).await?;

        let status = cache
            .status_snapshot(
                8_388_608,
                connected_upstream().await,
                BroadcasterSubscriberSnapshot::default(),
            )
            .await;
        assert_eq!(status.readiness, BroadcasterReadiness::Ready);
        assert_eq!(status.snapshot.configured_backends.len(), 2);
        assert!(status.backends.contains_key(&BroadcasterBackend::Rfq));

        let Some(BroadcasterPayload::Heartbeat(heartbeat)) = cache.heartbeat().await? else {
            return Err(anyhow!("expected ready heartbeat"));
        };
        assert!(heartbeat
            .backend_heads
            .iter()
            .any(|head| head.backend == BroadcasterBackend::Rfq && head.block_number == 12));
        Ok(())
    }

    #[tokio::test]
    async fn cache_rejects_undeclared_rfq_partition() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);

        let Err(error) = cache.apply_update(&rfq_only_update(12, "rfq-1", 3)).await else {
            return Err(anyhow!("undeclared rfq update should fail"));
        };

        assert!(
            error
                .to_string()
                .contains("update partition backend rfq is not configured"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cache_resets_generation_on_reset() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        cache.apply_update(&native_only_update()).await?;
        let live_before = cache.live_state().await;
        assert_eq!(live_before.snapshot_id, "chain-1-snapshot-1");

        let live_after = cache.reset_generation().await;
        assert_eq!(live_after.stream_id, "chain-1-stream-2");
        assert_eq!(live_after.snapshot_id, "chain-1-snapshot-2");
        assert!(cache.heartbeat().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn status_snapshot_distinguishes_warming_and_ready() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let upstream_state = BroadcasterUpstreamState::default();
        let disconnected = cache
            .status_snapshot(
                500,
                upstream_state.snapshot().await,
                BroadcasterSubscriberSnapshot::default(),
            )
            .await;
        assert_eq!(
            disconnected.readiness,
            BroadcasterReadiness::UpstreamDisconnected
        );

        upstream_state.mark_connected().await;
        let warming = cache
            .status_snapshot(
                500,
                upstream_state.snapshot().await,
                BroadcasterSubscriberSnapshot::default(),
            )
            .await;
        assert_eq!(warming.readiness, BroadcasterReadiness::SnapshotWarmingUp);

        cache.apply_update(&native_only_update()).await?;
        upstream_state.record_update().await;
        let ready = cache
            .status_snapshot(
                500,
                upstream_state.snapshot().await,
                BroadcasterSubscriberSnapshot::default(),
            )
            .await;
        assert_eq!(ready.readiness, BroadcasterReadiness::Ready);
        Ok(())
    }

    async fn connected_upstream() -> super::BroadcasterUpstreamSnapshot {
        let upstream = BroadcasterUpstreamState::default();
        upstream.mark_connected().await;
        upstream.record_update().await;
        upstream.snapshot().await
    }

    fn mixed_update() -> Update {
        let mut new_pairs = HashMap::new();
        new_pairs.insert(
            "native-1".to_string(),
            native_component("native-1", "uniswap_v2"),
        );
        new_pairs.insert("vm-1".to_string(), vm_component("vm-1", "vm:curve"));

        let mut states = HashMap::new();
        states.insert(
            "native-1".to_string(),
            Box::new(DummySim(1)) as Box<dyn ProtocolSim>,
        );
        states.insert(
            "vm-1".to_string(),
            Box::new(DummySim(2)) as Box<dyn ProtocolSim>,
        );

        Update::new(10, states, new_pairs).set_sync_states(HashMap::from([
            (
                "uniswap_v2".to_string(),
                SynchronizerState::Ready(block_header(10, 1)),
            ),
            (
                "vm:curve".to_string(),
                SynchronizerState::Ready(block_header(10, 2)),
            ),
        ]))
    }

    fn native_only_update() -> Update {
        let mut new_pairs = HashMap::new();
        new_pairs.insert(
            "native-1".to_string(),
            native_component("native-1", "uniswap_v2"),
        );

        let mut states = HashMap::new();
        states.insert(
            "native-1".to_string(),
            Box::new(DummySim(1)) as Box<dyn ProtocolSim>,
        );

        Update::new(10, states, new_pairs).set_sync_states(HashMap::from([(
            "uniswap_v2".to_string(),
            SynchronizerState::Ready(block_header(10, 1)),
        )]))
    }

    fn multi_native_update() -> Update {
        let mut new_pairs = HashMap::new();
        let mut states = HashMap::new();
        for index in 0u8..3 {
            let component_id = format!("native-{index}");
            new_pairs.insert(
                component_id.clone(),
                native_component(&component_id, "uniswap_v2"),
            );
            states.insert(
                component_id,
                Box::new(DummySim(index)) as Box<dyn ProtocolSim>,
            );
        }

        Update::new(10, states, new_pairs).set_sync_states(HashMap::from([(
            "uniswap_v2".to_string(),
            SynchronizerState::Ready(block_header(10, 1)),
        )]))
    }

    fn vm_sync_only_update() -> Update {
        Update::new(11, HashMap::new(), HashMap::new())
            .set_removed_pairs(HashMap::from([(
                "vm-1".to_string(),
                vm_component("vm-1", "vm:curve"),
            )]))
            .set_sync_states(HashMap::from([(
                "vm:curve".to_string(),
                SynchronizerState::Ready(block_header(11, 3)),
            )]))
    }

    fn rfq_sync_only_update() -> Update {
        Update::new(12, HashMap::new(), HashMap::new()).set_sync_states(HashMap::from([(
            "rfq:hashflow".to_string(),
            SynchronizerState::Ready(block_header(12, 4)),
        )]))
    }

    fn rfq_only_update(block_number: u64, component_id: &str, seed: u8) -> Update {
        let mut new_pairs = HashMap::new();
        new_pairs.insert(
            component_id.to_string(),
            rfq_component(component_id, "rfq:hashflow", seed),
        );

        let mut states = HashMap::new();
        states.insert(
            component_id.to_string(),
            Box::new(DummySim(seed)) as Box<dyn ProtocolSim>,
        );

        Update::new(block_number, states, new_pairs).set_sync_states(HashMap::from([(
            "rfq:hashflow".to_string(),
            SynchronizerState::Ready(block_header(block_number, seed)),
        )]))
    }

    fn raw_protocol_message_with_states(
        states: HashMap<String, ComponentWithState>,
    ) -> BroadcasterProtocolMessage {
        BroadcasterProtocolMessage::new(
            "uniswap_v2",
            SynchronizerState::Started,
            StateSyncMessage {
                header: block_header(10, 1),
                snapshots: Snapshot {
                    states,
                    vm_storage: HashMap::new(),
                },
                deltas: None,
                removed_components: HashMap::new(),
            },
        )
    }

    fn raw_vm_protocol_message(
        account_address: DtoBytes,
        account: ResponseAccount,
    ) -> BroadcasterProtocolMessage {
        BroadcasterProtocolMessage::new(
            "vm:balancer_v2",
            SynchronizerState::Started,
            StateSyncMessage {
                header: block_header(10, 1),
                snapshots: Snapshot {
                    states: HashMap::new(),
                    vm_storage: HashMap::from([(account_address, account)]),
                },
                deltas: None,
                removed_components: HashMap::new(),
            },
        )
    }

    fn native_component(_id: &str, protocol: &str) -> ProtocolComponent {
        ProtocolComponent::new(
            Bytes::from([3u8; 20]),
            protocol.to_string(),
            protocol.to_string(),
            Chain::Ethereum,
            vec![dummy_token(1, "TKNA"), dummy_token(2, "TKNB")],
            Vec::new(),
            HashMap::new(),
            Bytes::from([9u8; 32]),
            chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
                .unwrap_or_else(|| unreachable!("unix epoch"))
                .naive_utc(),
        )
    }

    fn vm_component(_id: &str, protocol: &str) -> ProtocolComponent {
        ProtocolComponent::new(
            Bytes::from([4u8; 20]),
            protocol.to_string(),
            protocol.to_string(),
            Chain::Ethereum,
            vec![dummy_token(3, "TKNC"), dummy_token(4, "TKND")],
            Vec::new(),
            HashMap::new(),
            Bytes::from([8u8; 32]),
            chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
                .unwrap_or_else(|| unreachable!("unix epoch"))
                .naive_utc(),
        )
    }

    fn rfq_component(_id: &str, protocol: &str, seed: u8) -> ProtocolComponent {
        ProtocolComponent::new(
            Bytes::from([seed; 20]),
            protocol.to_string(),
            "hashflow_pool".to_string(),
            Chain::Ethereum,
            vec![dummy_token(seed, "RFQA"), dummy_token(seed + 1, "RFQB")],
            Vec::new(),
            HashMap::new(),
            Bytes::from([seed; 32]),
            chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
                .unwrap_or_else(|| unreachable!("unix epoch"))
                .naive_utc(),
        )
    }

    fn raw_component_with_state(component_id: &str, seed: u8) -> ComponentWithState {
        ComponentWithState {
            state: ResponseProtocolState {
                component_id: component_id.to_string(),
                attributes: HashMap::from([(
                    "large".to_string(),
                    DtoBytes::from(vec![seed; 1024]),
                )]),
                balances: HashMap::new(),
            },
            component: raw_component(component_id, "uniswap_v2", seed),
            component_tvl: Some(seed as f64),
            entrypoints: Vec::new(),
        }
    }

    fn raw_component(component_id: &str, protocol: &str, seed: u8) -> DtoProtocolComponent {
        DtoProtocolComponent {
            id: component_id.to_string(),
            protocol_system: protocol.to_string(),
            protocol_type_name: protocol.to_string(),
            chain: DtoChain::Ethereum.into(),
            tokens: vec![DtoBytes::from([seed; 20]), DtoBytes::from([seed + 1; 20])],
            contract_ids: Vec::new(),
            static_attributes: HashMap::new(),
            change: Default::default(),
            creation_tx: DtoBytes::from([seed; 32]),
            created_at: chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
                .unwrap_or_else(|| unreachable!("unix epoch"))
                .naive_utc(),
        }
    }

    fn raw_response_account(
        address: DtoBytes,
        slot_count: usize,
        slot_value_size: usize,
    ) -> ResponseAccount {
        let mut slots = HashMap::new();
        for index in 0..slot_count {
            let seed = index as u8;
            let mut slot_key = vec![0u8; 32];
            slot_key[24..].copy_from_slice(&(index as u64).to_be_bytes());
            slots.insert(
                DtoBytes::from(slot_key),
                DtoBytes::from(vec![seed.saturating_add(1); slot_value_size]),
            );
        }

        ResponseAccount::new(
            DtoChain::Ethereum.into(),
            address,
            "vm-account".to_string(),
            slots,
            DtoBytes::from([0u8; 32]),
            HashMap::new(),
            DtoBytes::from(vec![7u8; 128]),
            DtoBytes::from([8u8; 32]),
            DtoBytes::from([9u8; 32]),
            DtoBytes::from([10u8; 32]),
            None,
        )
    }

    fn dummy_token(seed: u8, symbol: &str) -> Token {
        Token::new(
            &Bytes::from([seed; 20]),
            symbol,
            18,
            0,
            &[],
            Chain::Ethereum,
            1,
        )
    }

    #[test]
    fn upstream_state_reports_disconnect_details() {
        let runtime = tokio::runtime::Runtime::new().unwrap_or_else(|_| unreachable!("runtime"));
        runtime.block_on(async {
            let upstream = BroadcasterUpstreamState::default();
            upstream.mark_build_failed("boom").await;
            let snapshot = upstream.snapshot().await;
            assert!(!snapshot.connected);
            assert_eq!(snapshot.last_error.as_deref(), Some("boom"));
            assert_eq!(
                snapshot.last_disconnect_reason.as_deref(),
                Some("build_failed")
            );
        });
    }

    #[test]
    fn sync_status_clone_keeps_repo_owned_shape() {
        let status = BroadcasterProtocolSyncStatus {
            kind: BroadcasterProtocolSyncStatusKind::Ready,
            block: None,
            reason: None,
        };
        assert_eq!(status.kind, BroadcasterProtocolSyncStatusKind::Ready);
    }
}
