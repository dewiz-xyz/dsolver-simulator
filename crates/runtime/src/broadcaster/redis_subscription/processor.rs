use std::sync::Arc;

use anyhow::{anyhow, Result};
use simulator_replay::{ReplayBackend, ReplayDecoder};
use tokio::sync::OwnedRwLockWriteGuard;
use tokio::time::Instant;
use tracing::{info, warn};
use tycho_simulation::evm::engine_db::SHARED_TYCHO_DB;

#[cfg(test)]
use simulator_replay::DecoderConfig;
#[cfg(test)]
use tycho_simulation::{evm::decoder::TychoStreamDecoder, tycho_client::feed::BlockHeader};

use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterPayload,
    BroadcasterRedisReplayBoundary, BroadcasterRedisStreamEntry, BroadcasterSnapshotPartition,
    BroadcasterSnapshotStart, BroadcasterSubscriptionTracker, BroadcasterUpdateMessage,
};

use crate::broadcaster::redis_publisher::current_time_ms;

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
    decoder: Arc<ReplayDecoder>,
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
        let decoder = ReplayDecoder::with_decoder(
            DecoderConfig::for_backend(
                ReplayBackend::from(controls.backend()),
                controls.protocols().to_vec(),
                0,
            ),
            Arc::new(TychoStreamDecoder::<BlockHeader>::new()),
        );
        let mut processor =
            Self::with_decoder(expected_chain_id, controls, Arc::new(decoder), rebuild);
        processor.set_bootstrap_redis_replay_boundary(default_test_redis_replay_boundary());
        processor
    }

    pub(super) fn with_decoder(
        expected_chain_id: u64,
        controls: BroadcasterSubscriptionControls,
        decoder: Arc<ReplayDecoder>,
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

    pub(super) async fn recovery_copy(
        &self,
        replay_boundary: BroadcasterRedisReplayBoundary,
    ) -> Self {
        Self {
            expected_chain_id: self.expected_chain_id,
            controls: self.controls.recovery_copy().await,
            decoder: Arc::clone(&self.decoder),
            tracker: BroadcasterSubscriptionTracker::new(),
            raw_snapshot: RawSnapshotReassembly::default(),
            bootstrap_block: None,
            bootstrap_redis_replay_boundary: Some(replay_boundary),
            rebuild: None,
        }
    }

    pub(super) fn bootstrap_complete(&self) -> bool {
        matches!(
            self.tracker.state(),
            simulator_core::broadcaster::BroadcasterSubscriptionState::Live { .. }
        )
    }

    pub(super) fn align_redis_replay_boundary(
        &mut self,
        boundary: &BroadcasterRedisReplayBoundary,
    ) -> Result<()> {
        self.tracker
            .align_live_replay_boundary(boundary)
            .map_err(|error| anyhow!("invalid broadcaster Redis replay boundary: {error}"))
    }

    pub(super) async fn observe(&mut self, envelope: BroadcasterEnvelope) -> Result<()> {
        self.observe_with_state_version(envelope, None).await
    }

    async fn observe_with_state_version(
        &mut self,
        envelope: BroadcasterEnvelope,
        state_version: Option<u64>,
    ) -> Result<()> {
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
                let boundary = self
                    .bootstrap_redis_replay_boundary
                    .clone()
                    .ok_or_else(|| {
                        anyhow!("HTTP snapshot completed without Redis replay boundary")
                    })?;
                self.controls
                    .state_store()
                    .align_bootstrap_state_version(boundary.state_version)
                    .await;
                self.controls
                    .broadcaster_subscription()
                    .mark_bootstrap_complete_with_redis_boundary(boundary)
                    .await;
                self.finish_rebuild().await;
            }
            BroadcasterPayload::Update(update) => {
                self.apply_live_update(update, state_version).await?;
            }
            BroadcasterPayload::Heartbeat(heartbeat) => {
                for head in heartbeat.backend_heads {
                    if head.backend == self.controls.backend() {
                        self.apply_heartbeat(head);
                    }
                }
            }
            BroadcasterPayload::Progress(_progress) => {}
            BroadcasterPayload::RecoveryStart(_)
            | BroadcasterPayload::RecoveryChunk(_)
            | BroadcasterPayload::RecoveryCatchUp(_)
            | BroadcasterPayload::RecoveryCommit(_) => {
                return Err(anyhow!(
                    "Redis recovery payload reached a backend processor before commit"
                ));
            }
        }

        Ok(())
    }

    pub(super) async fn observe_redis_delta(
        &mut self,
        entry: &BroadcasterRedisStreamEntry,
        envelope: &BroadcasterEnvelope,
    ) -> Result<()> {
        if redis_entry_scope_contains(entry, self.controls.backend()) {
            let native_progress_block = if self.controls.backend() == BroadcasterBackend::Native {
                match &envelope.payload {
                    BroadcasterPayload::Update(update) => update.complete_native_block(),
                    _ => None,
                }
            } else {
                None
            };
            let consumer_started_at = Instant::now();
            let consumer_started_at_ms = current_time_ms();
            self.observe_with_state_version(envelope.clone(), Some(entry.state_version))
                .await?;
            let consumer_processing_ms = consumer_started_at.elapsed().as_millis() as u64;
            self.controls
                .broadcaster_subscription()
                .record_publisher_to_consumer_delay(entry.published_at_ms)
                .await;
            if let Some(block_number) = native_progress_block {
                let publisher_to_consumer_start_ms =
                    consumer_started_at_ms.saturating_sub(entry.published_at_ms);
                info!(
                    event = "broadcaster_native_update_applied",
                    block = block_number,
                    stream_id = entry.stream_id.as_str(),
                    message_seq = entry.message_seq,
                    state_version = entry.state_version,
                    published_at_ms = entry.published_at_ms,
                    consumer_started_at_ms,
                    publisher_to_consumer_start_ms,
                    consumer_processing_ms,
                    "Broadcaster native update applied"
                );
            }
            return Ok(());
        }

        self.tracker
            .skip_live_delta(envelope)
            .map_err(|error| anyhow!("invalid skipped broadcaster Redis envelope: {error}"))
    }

    pub(super) async fn apply_recovery_update(
        &self,
        update: BroadcasterUpdateMessage,
    ) -> Result<()> {
        self.apply_live_update(update, None).await
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

        let decoded = self.decoder.decode_snapshot_partition(partition).await?;
        if let Some(update) = decoded.update {
            self.controls.state_store().apply_update(update).await;
        }
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
        if let Some(update) = self
            .decoder
            .decode_snapshot_messages(ReplayBackend::from(self.controls.backend()), messages)
            .await?
        {
            self.controls.state_store().apply_update(update).await;
        }
        Ok(())
    }

    async fn apply_live_update(
        &self,
        update: BroadcasterUpdateMessage,
        state_version: Option<u64>,
    ) -> Result<()> {
        let _shared_db_guard = match &self.controls {
            BroadcasterSubscriptionControls::Vm(controls) if !controls.recovery_guard_held => {
                Some(controls.simulation_rebuild_gate.write().await)
            }
            _ => None,
        };

        let backend = ReplayBackend::from(self.controls.backend());
        let decoded = self.decoder.decode_live_delta(update, &[backend]).await?;
        if !decoded.had_applicable_partition {
            return Ok(());
        }
        if let Some(update) = decoded.update {
            match state_version {
                Some(state_version) => {
                    self.controls
                        .state_store()
                        .apply_update_at_version(update, state_version)
                        .await?;
                }
                None => {
                    self.controls.state_store().apply_update(update).await;
                }
            }
        }
        match self.controls.backend() {
            BroadcasterBackend::Rfq => {
                self.controls
                    .stream_health()
                    .record_update(decoded.block_number)
                    .await;
            }
            BroadcasterBackend::Native => {
                if let Some(complete_block) = decoded.complete_native_block {
                    self.controls
                        .stream_health()
                        .record_progress(complete_block)
                        .await;
                }
            }
            BroadcasterBackend::Vm => {
                self.controls
                    .stream_health()
                    .record_progress(decoded.block_number)
                    .await;
            }
        }
        Ok(())
    }

    fn apply_heartbeat(&self, _head: BroadcasterBackendHead) {}

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
    // Advance the request fence before exposing the disconnected subscription state.
    controls.state_store().fence_requests().await;
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

pub(super) async fn begin_vm_recovery(
    controls: &BroadcasterSubscriptionControls,
) -> Option<SubscriptionRebuildState> {
    let BroadcasterSubscriptionControls::Vm(vm_controls) = controls else {
        return None;
    };
    Some(begin_or_continue_vm_rebuild(vm_controls, None).await)
}

pub(super) async fn finish_vm_recovery(
    controls: &BroadcasterSubscriptionControls,
    rebuild: SubscriptionRebuildState,
) {
    drop(rebuild.guard);
    if let BroadcasterSubscriptionControls::Vm(vm_controls) = controls {
        let mut vm_stream = vm_controls.vm_stream.write().await;
        vm_stream.rebuilding = false;
        vm_stream.rebuild_started_at = None;
    }
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
