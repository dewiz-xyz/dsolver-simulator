use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::sync::OwnedRwLockWriteGuard;
use tokio::time::Instant;
use tracing::warn;
use tycho_simulation::{
    evm::decoder::TychoStreamDecoder,
    evm::engine_db::SHARED_TYCHO_DB,
    protocol::models::{ProtocolComponent, Update},
    tycho_client::feed::{BlockHeader, FeedMessage},
    tycho_common::simulation::protocol_sim::ProtocolSim,
};

use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterPayload,
    BroadcasterProtocolMessage, BroadcasterRedisReplayBoundary, BroadcasterRedisStreamEntry,
    BroadcasterSnapshotPartition, BroadcasterSnapshotStart, BroadcasterSubscriptionTracker,
    BroadcasterUpdatePartition,
};

use super::snapshot::RawSnapshotReassembly;
use super::{
    BroadcasterSubscriptionControls, RfqBroadcasterSubscriptionControls,
    VmBroadcasterSubscriptionControls,
};

fn redis_entry_scope_contains(
    entry: &BroadcasterRedisStreamEntry,
    backend: BroadcasterBackend,
) -> bool {
    entry
        .backend_scope
        .split(',')
        .any(|scope| scope == backend.as_str())
}

pub(super) struct PreparedRedisProcessor {
    pub(super) index: usize,
    pub(super) processor: BroadcasterSubscriptionProcessor,
    pub(super) replay_boundary: BroadcasterRedisReplayBoundary,
}

pub(super) struct BroadcasterSubscriptionProcessor {
    expected_chain_id: u64,
    pub(super) controls: BroadcasterSubscriptionControls,
    decoder: Arc<TychoStreamDecoder<BlockHeader>>,
    tracker: BroadcasterSubscriptionTracker,
    raw_snapshot: RawSnapshotReassembly,
    bootstrap_block: Option<u64>,
    bootstrap_redis_replay_boundary: Option<BroadcasterRedisReplayBoundary>,
    pub(super) rebuild: Option<SubscriptionRebuildState>,
}

impl BroadcasterSubscriptionProcessor {
    #[cfg(test)]
    pub(super) fn new(
        expected_chain_id: u64,
        controls: BroadcasterSubscriptionControls,
        rebuild: Option<SubscriptionRebuildState>,
    ) -> Self {
        let mut processor = Self::with_decoder(
            expected_chain_id,
            controls,
            Arc::new(TychoStreamDecoder::new()),
            rebuild,
        );
        processor.set_bootstrap_redis_replay_boundary(default_test_redis_replay_boundary());
        processor
    }

    pub(super) fn with_decoder(
        expected_chain_id: u64,
        controls: BroadcasterSubscriptionControls,
        decoder: Arc<TychoStreamDecoder<BlockHeader>>,
        rebuild: Option<SubscriptionRebuildState>,
    ) -> Self {
        Self {
            expected_chain_id,
            controls,
            decoder,
            tracker: BroadcasterSubscriptionTracker::new(),
            raw_snapshot: RawSnapshotReassembly::default(),
            bootstrap_block: None,
            bootstrap_redis_replay_boundary: None,
            rebuild,
        }
    }

    pub(super) fn set_bootstrap_redis_replay_boundary(
        &mut self,
        boundary: BroadcasterRedisReplayBoundary,
    ) {
        self.bootstrap_redis_replay_boundary = Some(boundary);
    }

    pub(super) fn bootstrap_complete(&self) -> bool {
        matches!(
            self.tracker.state(),
            simulator_core::broadcaster::BroadcasterSubscriptionState::Live { .. }
        )
    }

    #[cfg(test)]
    pub(super) fn next_message_seq(&self) -> Option<u64> {
        self.tracker.next_message_seq()
    }

    pub(super) fn align_redis_replay_boundary(
        &mut self,
        boundary: &BroadcasterRedisReplayBoundary,
    ) -> Result<()> {
        self.tracker
            .align_live_replay_boundary(boundary)
            .map_err(|error| anyhow!("invalid broadcaster Redis replay boundary: {error}"))
    }

    pub(super) fn continue_redis_generation_handoff(
        &mut self,
        boundary: &BroadcasterRedisReplayBoundary,
    ) -> Result<()> {
        self.tracker
            .continue_live_generation(boundary)
            .map_err(|error| anyhow!("invalid broadcaster Redis generation handoff: {error}"))
    }

    pub(super) async fn observe(&mut self, envelope: BroadcasterEnvelope) -> Result<()> {
        if let BroadcasterPayload::SnapshotStart(start) = &envelope.payload {
            self.ensure_snapshot_chain_id(start.chain_id)?;
            self.ensure_snapshot_includes_backend(start)?;
        }

        self.tracker
            .observe(&envelope)
            .map_err(|error| anyhow!("invalid broadcaster envelope: {error}"))?;

        match envelope.payload {
            BroadcasterPayload::SnapshotStart(start) => {
                self.bootstrap_block = None;
                self.raw_snapshot.reset();
                self.controls
                    .broadcaster_subscription()
                    .mark_snapshot_started(envelope.stream_id, start.snapshot_id)
                    .await;
            }
            BroadcasterPayload::SnapshotChunk(chunk) => {
                for partition in chunk.partitions {
                    if partition.backend == self.controls.backend() {
                        self.bootstrap_block = Some(partition.block_number);
                        self.buffer_snapshot_partition(partition).await?;
                    }
                }
            }
            BroadcasterPayload::SnapshotEnd(_end) => {
                self.apply_reassembled_snapshot_messages().await?;
                self.refresh_bootstrap_health().await;
                let boundary = self
                    .bootstrap_redis_replay_boundary
                    .clone()
                    .ok_or_else(|| {
                        anyhow!("HTTP snapshot completed without Redis replay boundary")
                    })?;
                self.controls
                    .broadcaster_subscription()
                    .mark_bootstrap_complete_with_redis_boundary(boundary)
                    .await;
                self.finish_rebuild().await;
            }
            BroadcasterPayload::Update(update) => {
                for partition in update.partitions {
                    if partition.backend == self.controls.backend() {
                        self.apply_live_update_partition(partition).await?;
                    }
                }
            }
            BroadcasterPayload::Heartbeat(heartbeat) => {
                for head in heartbeat.backend_heads {
                    if head.backend == self.controls.backend() {
                        self.apply_heartbeat(head).await;
                    }
                }
            }
            BroadcasterPayload::Progress(_progress) => {}
        }

        Ok(())
    }

    pub(super) async fn observe_redis_delta(
        &mut self,
        entry: &BroadcasterRedisStreamEntry,
        envelope: &BroadcasterEnvelope,
    ) -> Result<()> {
        if redis_entry_scope_contains(entry, self.controls.backend()) {
            return self.observe(envelope.clone()).await;
        }

        self.tracker
            .skip_live_delta(envelope)
            .map_err(|error| anyhow!("invalid skipped broadcaster Redis envelope: {error}"))
    }

    fn ensure_snapshot_includes_backend(&self, start: &BroadcasterSnapshotStart) -> Result<()> {
        let backend = self.controls.backend();
        if !start.backends.contains(&backend) {
            return Err(anyhow!(
                "broadcaster snapshot start {} does not include {} backend",
                start.snapshot_id,
                backend
            ));
        }
        Ok(())
    }

    fn ensure_snapshot_chain_id(&self, chain_id: u64) -> Result<()> {
        if chain_id != self.expected_chain_id {
            return Err(anyhow!(
                "broadcaster chain id mismatch for {} subscription: expected {}, got {}",
                self.controls.backend_label(),
                self.expected_chain_id,
                chain_id
            ));
        }
        Ok(())
    }

    async fn finish_rebuild(&mut self) {
        let Some(rebuild) = self.rebuild.take() else {
            return;
        };

        drop(rebuild.guard);

        if let BroadcasterSubscriptionControls::Vm(controls) = &self.controls {
            let mut vm_stream = controls.vm_stream.write().await;
            vm_stream.rebuilding = false;
            vm_stream.rebuild_started_at = None;
        }
    }

    async fn apply_snapshot_partition(
        &self,
        partition: BroadcasterSnapshotPartition,
    ) -> Result<()> {
        if !partition.messages.is_empty() {
            return Err(anyhow!(
                "raw broadcaster snapshot messages cannot be applied without reassembly"
            ));
        }

        let update = snapshot_partition_update(partition);
        self.controls.state_store().apply_update(update).await;
        Ok(())
    }

    async fn buffer_snapshot_partition(
        &mut self,
        partition: BroadcasterSnapshotPartition,
    ) -> Result<()> {
        if partition.messages.is_empty() {
            return self.apply_snapshot_partition(partition).await;
        }
        self.ensure_raw_messages_supported()?;

        for message in partition.messages {
            self.raw_snapshot.push(message)?;
        }
        Ok(())
    }

    async fn apply_reassembled_snapshot_messages(&mut self) -> Result<()> {
        let messages = self.raw_snapshot.take_messages();
        self.apply_protocol_messages(messages).await
    }

    async fn apply_live_update_partition(
        &self,
        partition: BroadcasterUpdatePartition,
    ) -> Result<()> {
        let block_number = partition.block_number;
        if !partition.messages.is_empty() {
            self.ensure_raw_messages_supported()?;
            self.apply_protocol_messages(partition.messages).await?;
            self.controls
                .stream_health()
                .record_update(block_number)
                .await;
            return Ok(());
        }

        let update = live_partition_update(partition);
        self.controls.state_store().apply_update(update).await;
        self.controls
            .stream_health()
            .record_update(block_number)
            .await;
        Ok(())
    }

    async fn apply_protocol_messages(
        &self,
        messages: Vec<BroadcasterProtocolMessage>,
    ) -> Result<()> {
        for message in messages {
            self.apply_protocol_message(message).await?;
        }
        Ok(())
    }

    async fn apply_protocol_message(&self, raw: BroadcasterProtocolMessage) -> Result<()> {
        let mut state_msgs = HashMap::new();
        state_msgs.insert(raw.protocol.clone(), raw.message);
        let mut sync_states = HashMap::new();
        sync_states.insert(raw.protocol, raw.sync_state);
        let feed = FeedMessage {
            state_msgs,
            sync_states,
        };
        let update = self
            .decoder
            .decode(&feed)
            .await
            .map_err(|error| anyhow!("failed to decode broadcaster raw payload: {error}"))?;
        self.controls.state_store().apply_update(update).await;
        Ok(())
    }

    async fn apply_heartbeat(&self, head: BroadcasterBackendHead) {
        self.controls
            .state_store()
            .apply_update(Update::new(
                head.block_number,
                HashMap::new(),
                HashMap::new(),
            ))
            .await;
        self.controls
            .stream_health()
            .record_update(head.block_number)
            .await;
    }

    async fn refresh_bootstrap_health(&self) {
        if let Some(block_number) = self.bootstrap_block {
            self.controls
                .stream_health()
                .record_update(block_number)
                .await;
        }
    }

    fn ensure_raw_messages_supported(&self) -> Result<()> {
        if self.controls.backend() == BroadcasterBackend::Rfq {
            return Err(anyhow!(
                "raw RFQ broadcaster messages are unsupported; expected decoded RFQ state partitions"
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
pub(super) fn default_test_redis_replay_boundary() -> BroadcasterRedisReplayBoundary {
    match BroadcasterRedisReplayBoundary::new(
        "stream:test".to_string(),
        "stream-1".to_string(),
        "snapshot-1".to_string(),
        1,
        0,
    ) {
        Ok(boundary) => boundary,
        Err(error) => unreachable!("test Redis replay boundary should be valid: {error}"),
    }
}

pub(super) async fn handle_subscription_reset(
    controls: &BroadcasterSubscriptionControls,
    last_error: Option<String>,
    rebuild: Option<SubscriptionRebuildState>,
) -> Option<SubscriptionRebuildState> {
    controls
        .broadcaster_subscription()
        .mark_disconnected(last_error.clone())
        .await;
    controls.stream_health().increment_restart().await;
    controls.stream_health().reset_bursts().await;
    controls
        .stream_health()
        .set_last_error(last_error.clone())
        .await;

    match controls {
        BroadcasterSubscriptionControls::Native(_) => {
            controls.state_store().reset().await;
            None
        }
        BroadcasterSubscriptionControls::Vm(vm_controls) => {
            {
                let mut vm_stream = vm_controls.vm_stream.write().await;
                vm_stream.last_error = last_error;
            }
            let rebuild = begin_or_continue_vm_rebuild(vm_controls, rebuild).await;
            controls.state_store().reset().await;
            Some(rebuild)
        }
        BroadcasterSubscriptionControls::Rfq(rfq_controls) => {
            let rebuild = begin_or_continue_rfq_rebuild(rfq_controls, rebuild).await;
            controls.state_store().reset().await;
            Some(rebuild)
        }
    }
}

pub(super) struct SubscriptionRebuildState {
    pub(super) guard: OwnedRwLockWriteGuard<()>,
}

async fn begin_or_continue_vm_rebuild(
    controls: &VmBroadcasterSubscriptionControls,
    rebuild: Option<SubscriptionRebuildState>,
) -> SubscriptionRebuildState {
    {
        let mut vm_stream = controls.vm_stream.write().await;
        vm_stream.rebuilding = true;
        vm_stream.restart_count = vm_stream.restart_count.saturating_add(1);
        if vm_stream.rebuild_started_at.is_none() {
            vm_stream.rebuild_started_at = Some(Instant::now());
        }
    }

    if let Some(rebuild) = rebuild {
        return rebuild;
    }

    let guard = controls.simulation_rebuild_gate.clone().write_owned().await;

    if let Err(err) = SHARED_TYCHO_DB.clear() {
        warn!(
            error = %err,
            "Failed clearing TychoDB during broadcaster-driven VM rebuild"
        );
    }

    SubscriptionRebuildState { guard }
}

async fn begin_or_continue_rfq_rebuild(
    controls: &RfqBroadcasterSubscriptionControls,
    rebuild: Option<SubscriptionRebuildState>,
) -> SubscriptionRebuildState {
    if let Some(rebuild) = rebuild {
        return rebuild;
    }

    let guard = controls.simulation_rebuild_gate.clone().write_owned().await;

    SubscriptionRebuildState { guard }
}

fn snapshot_partition_update(partition: BroadcasterSnapshotPartition) -> Update {
    let mut states = HashMap::new();
    let mut new_pairs = HashMap::new();

    for entry in partition.states {
        states.insert(entry.component_id.clone(), entry.state);
        new_pairs.insert(entry.component_id, entry.component);
    }

    Update::new(partition.block_number, states, new_pairs)
}

fn live_partition_update(partition: BroadcasterUpdatePartition) -> Update {
    let block_number = partition.block_number;
    let mut states: HashMap<String, Box<dyn ProtocolSim>> = HashMap::new();
    let mut new_pairs: HashMap<String, ProtocolComponent> = HashMap::new();
    let mut removed_pairs = HashMap::new();

    for entry in partition.new_pairs {
        states.insert(entry.component_id.clone(), entry.state);
        new_pairs.insert(entry.component_id, entry.component);
    }

    for delta in partition.updated_states {
        states.insert(delta.component_id, delta.state);
    }

    for removed in partition.removed_pairs {
        removed_pairs.insert(removed.component_id, removed.component);
    }

    Update::new(block_number, states, new_pairs).set_removed_pairs(removed_pairs)
}
