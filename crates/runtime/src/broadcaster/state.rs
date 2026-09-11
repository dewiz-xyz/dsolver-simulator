use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, ensure, Context, Result};
use state_history::WriterStatus;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::{info, warn};
use tycho_simulation::{
    protocol::models::Update as TychoUpdate,
    tycho_client::feed::{
        synchronizer::{Snapshot, StateSyncMessage},
        BlockHeader, FeedMessage, SynchronizerState,
    },
    tycho_common::{
        models::{
            blockchain::BlockAggregatedChanges,
            contract::{Account, AccountBalance, AccountDelta},
            protocol::ProtocolComponentStateDelta,
            token::Token,
            ChangeType,
        },
        Bytes,
    },
};

use crate::models::tokens::TokenStore;

use simulator_core::broadcaster::{
    complete_broadcaster_partition_block, BroadcasterBackend, BroadcasterBackendHead,
    BroadcasterEnvelope, BroadcasterHeartbeat, BroadcasterPayload, BroadcasterProtocolMessage,
    BroadcasterProtocolSyncStatus, BroadcasterSnapshotChunk, BroadcasterSnapshotEnd,
    BroadcasterSnapshotPartition, BroadcasterSnapshotStart, BroadcasterStateDelta,
    BroadcasterStateEntry, BroadcasterUpdateMessage,
};

use super::redis_publisher::{
    BroadcasterDeploymentAdmissionSnapshot, BroadcasterDeploymentPhase,
    BroadcasterRedisPublisherStatus,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BroadcasterReadiness {
    Ready,
    RedisPublisherPassive,
    RedisPublisherRetired,
    RedisPublisherUnhealthy,
    SnapshotWarmingUp,
    UpstreamRecovering,
    SnapshotUnexportable,
    UpstreamDisconnected,
    NativeProgressStale,
}

impl BroadcasterReadiness {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UpstreamDisconnected => "upstream_disconnected",
            Self::NativeProgressStale => "native_progress_stale",
            Self::SnapshotWarmingUp => "snapshot_warming_up",
            Self::UpstreamRecovering | Self::SnapshotUnexportable => "degraded",
            Self::RedisPublisherPassive => "redis_publisher_passive",
            Self::RedisPublisherRetired => "redis_publisher_retired",
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
    pub recovery: BroadcasterRecoveryStatus,
    pub snapshot: BroadcasterSnapshotStatus,
    pub snapshot_sessions: BroadcasterSnapshotSessionsSnapshot,
    pub backends: BTreeMap<BroadcasterBackend, BroadcasterBackendStatus>,
    pub redis_publisher: Option<BroadcasterRedisPublisherStatus>,
    pub deployment_admission: BroadcasterDeploymentAdmissionSnapshot,
    pub state_history: Option<WriterStatus>,
}

#[derive(Debug, Clone)]
pub struct BroadcasterRecoveryStatus {
    pub active: bool,
    pub phase: &'static str,
    pub age_ms: Option<u64>,
    pub attempt: u8,
    pub buffer_a_count: usize,
    pub buffer_b_count: usize,
    pub oldest_buffered_age_ms: Option<u64>,
    pub last_outcome: Option<&'static str>,
    pub last_duration_ms: Option<u64>,
    pub last_encoded_bytes: Option<usize>,
    pub last_chunk_count: Option<usize>,
    pub last_error: Option<String>,
}

impl Default for BroadcasterRecoveryStatus {
    fn default() -> Self {
        Self {
            active: false,
            phase: "idle",
            age_ms: None,
            attempt: 0,
            buffer_a_count: 0,
            buffer_b_count: 0,
            oldest_buffered_age_ms: None,
            last_outcome: None,
            last_duration_ms: None,
            last_encoded_bytes: None,
            last_chunk_count: None,
            last_error: None,
        }
    }
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
    pub exportable: bool,
    pub last_export_check_age_ms: Option<u64>,
    pub last_export_success_age_ms: Option<u64>,
    pub last_export_duration_ms: Option<u64>,
    pub last_export_payload_count: Option<usize>,
    pub largest_payload_bytes: Option<usize>,
    pub payload_limit_utilization_bps: Option<u16>,
    pub last_export_error: Option<String>,
    pub recovery_pending: bool,
    pub recovery_id: Option<u64>,
    pub recovery_error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct BroadcasterSnapshotSessionsSnapshot {
    pub active: usize,
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
pub(crate) struct BroadcasterSnapshotSource {
    stream_id: String,
    snapshot_id: String,
    partitions: BTreeMap<BroadcasterBackend, BroadcasterPartitionState>,
}

#[derive(Debug, Clone)]
pub(crate) struct BroadcasterStagedUpdate {
    message: Option<BroadcasterUpdateMessage>,
    apply_mode: BroadcasterStagedUpdateApplyMode,
    new_tokens: Vec<Token>,
    replacement_ready: bool,
    recovery_id: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct BroadcasterRecoverySource {
    recovery_id: u64,
    partitions: BTreeMap<BroadcasterBackend, BroadcasterPartitionState>,
}

impl BroadcasterRecoverySource {
    pub(crate) fn complete_native_block(&self) -> Option<u64> {
        let partition = self.partitions.get(&BroadcasterBackend::Native)?;
        complete_broadcaster_partition_block(
            partition.block_number?,
            &partition.messages,
            &partition.sync_statuses,
        )
    }
}

impl BroadcasterSnapshotSource {
    pub(crate) fn relabel_generation(&mut self, chain_id: u64, generation: u64) {
        self.stream_id = format_stream_id(chain_id, generation);
        self.snapshot_id = format_snapshot_id(chain_id, generation);
    }

    pub(crate) fn backend_heads(&self) -> Vec<BroadcasterBackendHead> {
        self.partitions
            .iter()
            .filter_map(|(backend, partition)| {
                partition
                    .block_number
                    .map(|block_number| BroadcasterBackendHead::new(*backend, block_number))
            })
            .collect()
    }
}

impl BroadcasterStagedUpdate {
    pub(crate) fn message(&self) -> Option<&BroadcasterUpdateMessage> {
        self.message.as_ref()
    }

    pub(crate) const fn publishes_update(&self) -> bool {
        self.message.is_some() || self.replacement_ready
    }

    pub(crate) const fn has_replacement_ready(&self) -> bool {
        self.replacement_ready
    }

    pub(crate) const fn recovery_id(&self) -> Option<u64> {
        self.recovery_id
    }

    fn recovery_partitions(
        &self,
    ) -> Option<&BTreeMap<BroadcasterBackend, BroadcasterPartitionState>> {
        match &self.apply_mode {
            BroadcasterStagedUpdateApplyMode::Replacement {
                partitions,
                pending: None,
                ..
            } if self.replacement_ready => Some(partitions),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
enum BroadcasterStagedUpdateApplyMode {
    Decoded,
    RawNormal {
        expected_protocols: BTreeSet<String>,
        next_recovery_id: u64,
    },
    Replacement {
        partitions: BTreeMap<BroadcasterBackend, BroadcasterPartitionState>,
        expected_protocols: BTreeSet<String>,
        pending: Option<UpstreamReplacementState>,
        next_recovery_id: u64,
    },
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
    last_native_block: Option<u64>,
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

    pub async fn record_native_progress(&self, block_number: u64) -> bool {
        let mut guard = self.inner.write().await;
        if guard
            .last_native_block
            .is_some_and(|last_block| block_number <= last_block)
        {
            return false;
        }
        guard.connected = true;
        guard.last_native_block = Some(block_number);
        guard.last_update_at = Some(Instant::now());
        true
    }

    #[cfg(test)]
    pub(crate) async fn last_native_block(&self) -> Option<u64> {
        self.inner.read().await.last_native_block
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
    token_catalog: Option<BroadcasterTokenCatalog>,
}

#[derive(Debug, Clone)]
struct BroadcasterTokenCatalog {
    tokens: Arc<TokenStore>,
    min_quality: u32,
}

#[derive(Debug, Clone)]
struct BroadcasterSnapshotCacheData {
    generation: u64,
    stream_id: String,
    snapshot_id: String,
    partitions: BTreeMap<BroadcasterBackend, BroadcasterPartitionState>,
    known_backends: HashMap<String, BroadcasterBackend>,
    expected_protocols: BTreeSet<String>,
    replacement: Option<UpstreamReplacementState>,
    next_recovery_id: u64,
}

#[derive(Debug, Clone)]
struct UpstreamReplacementState {
    id: u64,
    started_at: Instant,
    candidates: BTreeMap<BroadcasterBackend, BroadcasterPartitionState>,
}

#[derive(Debug, Clone, Default)]
struct BroadcasterPartitionState {
    block_number: Option<u64>,
    sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
    messages: Vec<BroadcasterProtocolMessage>,
    states: BTreeMap<String, BroadcasterStateEntry>,
}

impl BroadcasterSnapshotCache {
    pub fn new(chain_id: u64, configured_backends: Vec<BroadcasterBackend>) -> Self {
        Self::new_with_initial_generation_and_token_catalog(chain_id, configured_backends, 1, None)
    }

    pub fn with_token_catalog(
        chain_id: u64,
        configured_backends: Vec<BroadcasterBackend>,
        tokens: Arc<TokenStore>,
        min_quality: u32,
    ) -> Self {
        Self::new_with_initial_generation_and_token_catalog(
            chain_id,
            configured_backends,
            1,
            Some(BroadcasterTokenCatalog {
                tokens,
                min_quality,
            }),
        )
    }

    /// The store stream folds write into. Checkpoint captures must clone this
    /// same store, or the snapshot superset guarantee silently splits in two.
    pub fn token_catalog_store(&self) -> Option<Arc<TokenStore>> {
        self.token_catalog
            .as_ref()
            .map(|catalog| Arc::clone(&catalog.tokens))
    }

    #[cfg(test)]
    pub(crate) fn new_with_initial_generation(
        chain_id: u64,
        configured_backends: Vec<BroadcasterBackend>,
        generation: u64,
    ) -> Self {
        Self::new_with_initial_generation_and_token_catalog(
            chain_id,
            configured_backends,
            generation,
            None,
        )
    }

    fn new_with_initial_generation_and_token_catalog(
        chain_id: u64,
        mut configured_backends: Vec<BroadcasterBackend>,
        generation: u64,
        token_catalog: Option<BroadcasterTokenCatalog>,
    ) -> Self {
        configured_backends.sort();
        configured_backends.dedup();
        let generation = generation.max(1);

        Self {
            chain_id,
            configured_backends,
            inner: Arc::new(RwLock::new(BroadcasterSnapshotCacheData {
                generation,
                stream_id: format_stream_id(chain_id, generation),
                snapshot_id: format_snapshot_id(chain_id, generation),
                partitions: BTreeMap::new(),
                known_backends: HashMap::new(),
                expected_protocols: BTreeSet::new(),
                replacement: None,
                next_recovery_id: 1,
            })),
            token_catalog,
        }
    }

    pub async fn reset_to_generation(&self, generation: u64) {
        let mut guard = self.inner.write().await;
        Self::relabel_generation_locked(self.chain_id, &mut guard, generation);
        guard.partitions.clear();
        guard.known_backends.clear();
        guard.expected_protocols.clear();
        guard.replacement = None;
    }

    pub async fn relabel_generation(&self, generation: u64) {
        let mut guard = self.inner.write().await;
        Self::relabel_generation_locked(self.chain_id, &mut guard, generation);
    }

    pub async fn begin_same_generation_recovery(&self) {
        let mut guard = self.inner.write().await;
        // Pending candidates chain from the previous connection; the reconnect
        // stream re-delivers full snapshots, so stale candidates must not survive
        // into the next alignment attempt.
        let superseded_recovery_id = guard.replacement.take().map(|pending| pending.id);
        let id = guard.next_recovery_id;
        guard.next_recovery_id = guard.next_recovery_id.saturating_add(1);
        guard.replacement = Some(UpstreamReplacementState {
            id,
            started_at: Instant::now(),
            candidates: BTreeMap::new(),
        });
        info!(
            event = "broadcaster_upstream_recovery_started",
            recovery_id = id,
            superseded_recovery_id = ?superseded_recovery_id,
            reason = "stream_discontinuity",
            "Starting same-generation recovery after the upstream stream became invalid"
        );
    }

    pub async fn begin_same_generation_recovery_from_current(&self) {
        let mut guard = self.inner.write().await;
        if guard.replacement.is_some() {
            return;
        }
        let id = guard.next_recovery_id;
        guard.next_recovery_id = guard.next_recovery_id.saturating_add(1);
        guard.replacement = Some(UpstreamReplacementState {
            id,
            started_at: Instant::now(),
            candidates: guard.partitions.clone(),
        });
        info!(
            event = "broadcaster_upstream_recovery_started",
            recovery_id = id,
            reason = "oversized_live_update",
            "Starting same-generation recovery from the current authoritative state"
        );
    }

    pub async fn reset_same_generation(&self) {
        let mut guard = self.inner.write().await;
        guard.partitions.clear();
        guard.known_backends.clear();
        guard.expected_protocols.clear();
        guard.replacement = None;
    }

    #[cfg(test)]
    pub(crate) async fn replacement_pending(&self) -> bool {
        self.inner.read().await.replacement.is_some()
    }

    pub async fn apply_update(&self, update: &TychoUpdate) -> Result<BroadcasterUpdateMessage> {
        let staged = self.stage_update(update).await?;
        let message = staged
            .message
            .clone()
            .ok_or_else(|| anyhow!("decoded staged update is missing its Redis payload"))?;
        self.commit_staged_update(staged).await?;
        Ok(message)
    }

    pub async fn apply_feed_message(
        &self,
        feed: &FeedMessage<BlockHeader>,
    ) -> Result<Option<BroadcasterUpdateMessage>> {
        let staged = self.stage_feed_message(feed).await?;
        let message = staged.message.clone();
        self.commit_staged_update(staged).await?;
        Ok(message)
    }

    pub(crate) async fn stage_update(
        &self,
        update: &TychoUpdate,
    ) -> Result<BroadcasterStagedUpdate> {
        let guard = self.inner.read().await;
        let message = BroadcasterUpdateMessage::from_tycho_update(update, &guard.known_backends)?;
        ensure_configured_update_backends(&message, &self.configured_backends)?;
        validate_update_message_applicable(&guard, &message)?;
        Ok(BroadcasterStagedUpdate {
            message: Some(message),
            apply_mode: BroadcasterStagedUpdateApplyMode::Decoded,
            new_tokens: Vec::new(),
            replacement_ready: false,
            recovery_id: None,
        })
    }

    pub(crate) async fn stage_feed_message(
        &self,
        feed: &FeedMessage<BlockHeader>,
    ) -> Result<BroadcasterStagedUpdate> {
        let message = BroadcasterUpdateMessage::from_tycho_feed_message(feed)?;
        ensure_configured_update_backends(&message, &self.configured_backends)?;
        // Replacement compaction clears delta tokens, so retain them from the accepted input.
        let new_tokens = self
            .token_catalog
            .as_ref()
            .map_or_else(Vec::new, |_| raw_update_new_tokens(&message));
        let guard = self.inner.read().await;
        let mut expected_protocols = guard.expected_protocols.clone();
        expected_protocols.extend(feed.sync_states.keys().cloned());
        let has_gap = raw_message_has_header_gap(&guard.partitions, &message);
        let replacement = guard.replacement.clone();
        let next_recovery_id = guard.next_recovery_id;

        if replacement.is_none() && has_gap {
            return Err(anyhow!(
                "Tycho header gap requires reconnect and a fresh authoritative replacement"
            ));
        }

        if let Some(mut pending) = replacement {
            apply_replacement_feed_message(&mut pending.candidates, &message, feed)?;
            if replacement_is_aligned(&pending.candidates, &expected_protocols) {
                let elapsed_ms = pending.started_at.elapsed().as_millis() as u64;
                info!(
                    event = "broadcaster_upstream_recovery_aligned",
                    recovery_id = pending.id,
                    elapsed_ms,
                    "Fresh authoritative Tycho replacement is ready to publish"
                );
                return Ok(BroadcasterStagedUpdate {
                    message: None,
                    apply_mode: BroadcasterStagedUpdateApplyMode::Replacement {
                        partitions: pending.candidates,
                        expected_protocols,
                        pending: None,
                        next_recovery_id,
                    },
                    new_tokens,
                    replacement_ready: true,
                    recovery_id: Some(pending.id),
                });
            } else {
                info!(
                    event = "broadcaster_upstream_recovery_waiting",
                    recovery_id = pending.id,
                    expected_protocols = ?expected_protocols,
                    "Waiting for Tycho protocol candidates to align"
                );
                return Ok(BroadcasterStagedUpdate {
                    message: None,
                    apply_mode: BroadcasterStagedUpdateApplyMode::Replacement {
                        partitions: guard.partitions.clone(),
                        expected_protocols,
                        pending: Some(pending),
                        next_recovery_id,
                    },
                    new_tokens,
                    replacement_ready: false,
                    recovery_id: None,
                });
            }
        }

        validate_raw_update_message(&guard.partitions, &message)?;
        Ok(BroadcasterStagedUpdate {
            message: Some(message),
            apply_mode: BroadcasterStagedUpdateApplyMode::RawNormal {
                expected_protocols,
                next_recovery_id,
            },
            new_tokens,
            replacement_ready: false,
            recovery_id: None,
        })
    }

    pub(crate) async fn commit_staged_update(&self, staged: BroadcasterStagedUpdate) -> Result<()> {
        let BroadcasterStagedUpdate {
            message,
            apply_mode,
            new_tokens: accepted_new_tokens,
            ..
        } = staged;
        let mut guard = self.inner.write().await;
        match apply_mode {
            BroadcasterStagedUpdateApplyMode::Decoded => {
                if let Some(message) = message.as_ref() {
                    apply_update_message(&mut guard, message);
                }
            }
            BroadcasterStagedUpdateApplyMode::RawNormal {
                expected_protocols,
                next_recovery_id,
            } => {
                if let Some(message) = message.as_ref() {
                    apply_raw_update_message(&mut guard.partitions, message)?;
                }
                guard.expected_protocols = expected_protocols;
                guard.next_recovery_id = next_recovery_id;
            }
            BroadcasterStagedUpdateApplyMode::Replacement {
                partitions,
                expected_protocols,
                pending,
                next_recovery_id,
            } => {
                guard.partitions = partitions;
                guard.expected_protocols = expected_protocols;
                guard.replacement = pending;
                guard.next_recovery_id = next_recovery_id;
            }
        }
        drop(guard);

        if accepted_new_tokens.is_empty() {
            return Ok(());
        }
        let Some(token_catalog) = &self.token_catalog else {
            return Ok(());
        };
        // Match startup admission before exposing stream tokens through catalog snapshots.
        let admitted_tokens = accepted_new_tokens
            .into_iter()
            .filter(|token| token.quality >= token_catalog.min_quality);
        // Live publication releases the publisher lock before cache commit reaches this write.
        token_catalog.tokens.insert_batch(admitted_tokens).await;
        Ok(())
    }

    pub async fn export_snapshot(
        &self,
        max_payload_bytes: usize,
    ) -> Result<BroadcasterSnapshotExport> {
        let guard = self.inner.read().await;
        let source = BroadcasterSnapshotSource {
            stream_id: guard.stream_id.clone(),
            snapshot_id: guard.snapshot_id.clone(),
            partitions: guard.partitions.clone(),
        };
        drop(guard);
        self.export_snapshot_source(source, max_payload_bytes)
    }

    pub(crate) async fn pin_snapshot_source(&self) -> Option<BroadcasterSnapshotSource> {
        let guard = self.inner.read().await;
        self.is_ready_locked(&guard)
            .then(|| BroadcasterSnapshotSource {
                stream_id: guard.stream_id.clone(),
                snapshot_id: guard.snapshot_id.clone(),
                partitions: guard.partitions.clone(),
            })
    }

    pub(crate) fn export_snapshot_source(
        &self,
        source: BroadcasterSnapshotSource,
        max_payload_bytes: usize,
    ) -> Result<BroadcasterSnapshotExport> {
        let snapshot_id = source.snapshot_id;
        let stream_id = source.stream_id;
        let chunks = build_snapshot_chunks(
            &stream_id,
            &snapshot_id,
            &self.configured_backends,
            &source.partitions,
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
            BroadcasterSnapshotEnd::new(snapshot_id.clone()),
        ));
        for (index, payload) in payloads.iter().enumerate() {
            ensure_payload_fits(&stream_id, index as u64 + 1, payload, max_payload_bytes)
                .with_context(|| format!("snapshot payload {index} exceeds byte cap"))?;
        }

        Ok(BroadcasterSnapshotExport {
            stream_id,
            snapshot_id,
            max_payload_bytes,
            payloads,
        })
    }

    pub(crate) fn recovery_source(
        &self,
        staged: &BroadcasterStagedUpdate,
    ) -> Result<BroadcasterRecoverySource> {
        let partitions = staged
            .recovery_partitions()
            .ok_or_else(|| anyhow!("staged update does not contain a ready replacement"))?;
        let recovery_id = staged
            .recovery_id()
            .ok_or_else(|| anyhow!("ready replacement is missing its recovery identity"))?;
        Ok(BroadcasterRecoverySource {
            recovery_id,
            partitions: partitions.clone(),
        })
    }

    pub(crate) async fn pin_recovery_source(&self, recovery_id: u64) -> BroadcasterRecoverySource {
        BroadcasterRecoverySource {
            recovery_id,
            partitions: self.inner.read().await.partitions.clone(),
        }
    }

    pub(crate) fn serialize_recovery_source(
        &self,
        source: BroadcasterRecoverySource,
        max_payload_bytes: usize,
    ) -> Result<String> {
        let BroadcasterRecoverySource {
            recovery_id,
            partitions,
        } = source;
        let stream_id = format!("chain-{}-recovery-{recovery_id}", self.chain_id);
        let snapshot_id = format!("chain-{}-recovery-snapshot-{recovery_id}", self.chain_id);
        let chunks = build_snapshot_chunks(
            &stream_id,
            &snapshot_id,
            &self.configured_backends,
            &partitions,
            max_payload_bytes,
        )?;
        let total_chunks = u32::try_from(chunks.len())?;
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

        let envelopes = payloads
            .into_iter()
            .enumerate()
            .map(|(index, payload)| {
                BroadcasterEnvelope::new(
                    stream_id.clone(),
                    u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1),
                    payload,
                )
            })
            .collect::<Vec<_>>();
        serde_json::to_string(&envelopes)
            .context("failed to serialize broadcaster recovery replacement")
    }

    pub async fn heartbeat(&self) -> Result<Option<BroadcasterPayload>> {
        let guard = self.inner.read().await;
        if !self.is_ready_locked(&guard) {
            return Ok(None);
        }

        let backend_heads = self.backend_heads_locked(&guard);

        Ok(Some(BroadcasterPayload::Heartbeat(
            BroadcasterHeartbeat::new(self.chain_id, guard.snapshot_id.clone(), backend_heads)?,
        )))
    }

    pub fn configured_backends(&self) -> Vec<BroadcasterBackend> {
        self.configured_backends.clone()
    }

    pub const fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub async fn backend_heads(&self) -> Vec<BroadcasterBackendHead> {
        let guard = self.inner.read().await;
        self.backend_heads_locked(&guard)
    }

    pub async fn is_ready(&self) -> bool {
        let guard = self.inner.read().await;
        self.is_ready_locked(&guard)
    }

    pub async fn status_snapshot(
        &self,
        max_payload_bytes: usize,
        upstream: BroadcasterUpstreamSnapshot,
        snapshot_sessions: BroadcasterSnapshotSessionsSnapshot,
        native_progress_lease: Duration,
    ) -> BroadcasterStatusSnapshot {
        let guard = self.inner.read().await;
        let ready = self.is_ready_locked(&guard);
        let native_progress_stale = self
            .configured_backends
            .contains(&BroadcasterBackend::Native)
            && upstream
                .last_update_age_ms
                .is_none_or(|age_ms| age_ms >= native_progress_lease.as_millis() as u64);
        let readiness = if !upstream.connected {
            BroadcasterReadiness::UpstreamDisconnected
        } else if guard.replacement.is_some() {
            BroadcasterReadiness::UpstreamRecovering
        } else if !ready {
            BroadcasterReadiness::SnapshotWarmingUp
        } else if native_progress_stale {
            BroadcasterReadiness::NativeProgressStale
        } else {
            BroadcasterReadiness::Ready
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
            recovery: BroadcasterRecoveryStatus::default(),
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
                exportable: true,
                last_export_check_age_ms: None,
                last_export_success_age_ms: None,
                last_export_duration_ms: None,
                last_export_payload_count: None,
                largest_payload_bytes: None,
                payload_limit_utilization_bps: None,
                last_export_error: None,
                recovery_pending: guard.replacement.is_some(),
                recovery_id: guard.replacement.as_ref().map(|replacement| replacement.id),
                recovery_error: None,
            },
            snapshot_sessions,
            backends,
            redis_publisher: None,
            deployment_admission: BroadcasterDeploymentAdmissionSnapshot {
                admitted: false,
                phase: BroadcasterDeploymentPhase::Warming,
                last_error: None,
            },
            state_history: None,
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

    fn backend_heads_locked(
        &self,
        guard: &BroadcasterSnapshotCacheData,
    ) -> Vec<BroadcasterBackendHead> {
        self.configured_backends
            .iter()
            .filter_map(|backend| {
                guard
                    .partitions
                    .get(backend)
                    .and_then(|partition| partition.block_number)
                    .map(|block_number| BroadcasterBackendHead::new(*backend, block_number))
            })
            .collect()
    }

    fn relabel_generation_locked(
        chain_id: u64,
        guard: &mut BroadcasterSnapshotCacheData,
        generation: u64,
    ) {
        let generation = generation.max(1);
        guard.generation = generation;
        guard.stream_id = format_stream_id(chain_id, generation);
        guard.snapshot_id = format_snapshot_id(chain_id, generation);
    }
}

pub(crate) fn combine_snapshot_exports(
    chain_id: u64,
    exports: Vec<BroadcasterSnapshotExport>,
) -> Result<BroadcasterSnapshotExport> {
    let mut exports = exports.into_iter();
    let first = exports
        .next()
        .ok_or_else(|| anyhow!("cannot combine empty broadcaster snapshot export set"))?;
    let stream_id = first.stream_id.clone();
    let snapshot_id = first.snapshot_id.clone();
    let max_payload_bytes = first.max_payload_bytes;
    let mut backends = Vec::new();
    let mut chunks = Vec::new();

    for export in std::iter::once(first).chain(exports) {
        collect_snapshot_export_parts(
            chain_id,
            &stream_id,
            &snapshot_id,
            max_payload_bytes,
            export,
            &mut backends,
            &mut chunks,
        )?;
    }

    backends.sort();
    backends.dedup();
    let total_chunks = chunks.len() as u32;
    let mut payloads = Vec::with_capacity(chunks.len().saturating_add(2));
    payloads.push(BroadcasterPayload::SnapshotStart(
        BroadcasterSnapshotStart::new(snapshot_id.clone(), chain_id, backends, total_chunks)?,
    ));
    for (chunk_index, chunk) in chunks.into_iter().enumerate() {
        payloads.push(BroadcasterPayload::SnapshotChunk(
            BroadcasterSnapshotChunk::new(
                snapshot_id.clone(),
                chunk_index as u32,
                chunk.partitions,
            )?,
        ));
    }
    payloads.push(BroadcasterPayload::SnapshotEnd(
        BroadcasterSnapshotEnd::new(snapshot_id.clone()),
    ));

    Ok(BroadcasterSnapshotExport {
        stream_id,
        snapshot_id,
        max_payload_bytes,
        payloads,
    })
}

fn collect_snapshot_export_parts(
    chain_id: u64,
    stream_id: &str,
    snapshot_id: &str,
    max_payload_bytes: usize,
    export: BroadcasterSnapshotExport,
    backends: &mut Vec<BroadcasterBackend>,
    chunks: &mut Vec<BroadcasterSnapshotChunk>,
) -> Result<()> {
    ensure!(
        export.stream_id == stream_id,
        "snapshot export stream_id mismatch: expected {stream_id}, found {}",
        export.stream_id
    );
    ensure!(
        export.snapshot_id == snapshot_id,
        "snapshot export snapshot_id mismatch: expected {snapshot_id}, found {}",
        export.snapshot_id
    );
    ensure!(
        export.max_payload_bytes == max_payload_bytes,
        "snapshot export max_payload_bytes mismatch: expected {max_payload_bytes}, found {}",
        export.max_payload_bytes
    );

    for payload in export.payloads {
        match payload {
            BroadcasterPayload::SnapshotStart(start) => {
                ensure!(
                    start.snapshot_id == snapshot_id,
                    "snapshot_start id mismatch: expected {snapshot_id}, found {}",
                    start.snapshot_id
                );
                ensure!(
                    start.chain_id == chain_id,
                    "snapshot_start chain_id mismatch: expected {chain_id}, found {}",
                    start.chain_id
                );
                backends.extend(start.backends);
            }
            BroadcasterPayload::SnapshotChunk(chunk) => {
                ensure!(
                    chunk.snapshot_id == snapshot_id,
                    "snapshot_chunk id mismatch: expected {snapshot_id}, found {}",
                    chunk.snapshot_id
                );
                chunks.push(chunk);
            }
            BroadcasterPayload::SnapshotEnd(end) => {
                ensure!(
                    end.snapshot_id == snapshot_id,
                    "snapshot_end id mismatch: expected {snapshot_id}, found {}",
                    end.snapshot_id
                );
            }
            BroadcasterPayload::Update(_)
            | BroadcasterPayload::Heartbeat(_)
            | BroadcasterPayload::Progress(_)
            | BroadcasterPayload::RecoveryStart(_)
            | BroadcasterPayload::RecoveryChunk(_)
            | BroadcasterPayload::RecoveryCatchUp(_)
            | BroadcasterPayload::RecoveryCommit(_) => {
                return Err(anyhow!(
                    "snapshot export contains non-snapshot payload {}",
                    payload.kind().as_str()
                ));
            }
        }
    }

    Ok(())
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
    partitions: &mut BTreeMap<BroadcasterBackend, BroadcasterPartitionState>,
    message: &BroadcasterUpdateMessage,
) -> Result<()> {
    for partition in &message.partitions {
        let partition_state = partitions.entry(partition.backend).or_default();
        partition_state.block_number = Some(partition.block_number);
        partition_state.sync_statuses = partition.sync_statuses.clone();
        let mut messages = partition.messages.iter().collect::<Vec<_>>();
        messages.sort_by(|left, right| left.protocol.cmp(&right.protocol));
        for message in messages {
            merge_raw_message(&mut partition_state.messages, message.clone());
            propagate_touched_vm_accounts(&mut partition_state.messages, message)?;
        }
        canonicalize_shared_vm_accounts(&mut partition_state.messages)?;
    }
    Ok(())
}

#[expect(
    clippy::excessive_nesting,
    reason = "Backend, protocol and account reconciliation retains the conflicting peer identity in errors"
)]
fn validate_raw_update_message(
    partitions: &BTreeMap<BroadcasterBackend, BroadcasterPartitionState>,
    update: &BroadcasterUpdateMessage,
) -> Result<()> {
    for partition in &update.partitions {
        let mut projected_by_block_and_address: BTreeMap<
            (u64, Bytes),
            (String, ProjectedVmAccount),
        > = BTreeMap::new();
        let mut messages = partition.messages.iter().collect::<Vec<_>>();
        messages.sort_by(|left, right| left.protocol.cmp(&right.protocol));
        for incoming in messages {
            for address in touched_vm_addresses(incoming) {
                let mut projected =
                    project_vm_account(partitions, partition.backend, incoming, &address);
                for existing in partitions
                    .get(&partition.backend)
                    .into_iter()
                    .flat_map(|partition| &partition.messages)
                    .chain(partition.messages.iter())
                    .filter(|message| {
                        message.protocol != incoming.protocol
                            && message.message.header.number == incoming.message.header.number
                    })
                {
                    let Some(existing_account) =
                        existing.message.snapshots.vm_storage.get(&address)
                    else {
                        continue;
                    };
                    projected = merge_projected_vm_accounts(
                        projected,
                        ProjectedVmAccount::Materialized(Some(existing_account.clone())),
                        &address,
                        incoming.message.header.number,
                    )
                    .map_err(|error| {
                        anyhow!(
                            "while reconciling VM protocols {} and {}: {error:#}",
                            incoming.protocol,
                            existing.protocol
                        )
                    })?;
                }
                let key = (incoming.message.header.number, address.clone());
                if let Some((protocol, previous)) = projected_by_block_and_address.get_mut(&key) {
                    *previous = merge_projected_vm_accounts(
                        previous.clone(),
                        projected,
                        &address,
                        incoming.message.header.number,
                    )
                    .with_context(|| {
                        format!(
                            "conflicting VM account {address} values from protocols {protocol} and {}",
                            incoming.protocol
                        )
                    })?;
                } else {
                    projected_by_block_and_address
                        .insert(key, (incoming.protocol.clone(), projected));
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
enum ProjectedVmAccount {
    Materialized(Option<Account>),
    Residual {
        update: Option<AccountDelta>,
        balances: HashMap<Bytes, AccountBalance>,
    },
}

fn merge_projected_vm_accounts(
    left: ProjectedVmAccount,
    right: ProjectedVmAccount,
    address: &Bytes,
    block_number: u64,
) -> Result<ProjectedVmAccount> {
    match (left, right) {
        (
            ProjectedVmAccount::Materialized(Some(left)),
            ProjectedVmAccount::Materialized(Some(right)),
        ) => Ok(ProjectedVmAccount::Materialized(Some(
            merge_shared_vm_accounts(left, right, block_number)?,
        ))),
        (ProjectedVmAccount::Materialized(None), ProjectedVmAccount::Materialized(None)) => {
            Ok(ProjectedVmAccount::Materialized(None))
        }
        (
            left @ ProjectedVmAccount::Residual { .. },
            right @ ProjectedVmAccount::Residual { .. },
        ) if left == right => Ok(left),
        _ => Err(anyhow!(
            "conflicting VM account {address} values at block {block_number}"
        )),
    }
}

fn merge_shared_vm_accounts(
    mut left: Account,
    right: Account,
    block_number: u64,
) -> Result<Account> {
    let address = left.address.clone();
    ensure!(
        left.chain == right.chain
            && left.address == right.address
            && left.native_balance == right.native_balance
            && left.code == right.code
            && left.code_hash == right.code_hash
            && left.balance_modify_tx == right.balance_modify_tx
            && left.code_modify_tx == right.code_modify_tx
            && left.creation_tx == right.creation_tx,
        "conflicting VM account {address} values at block {block_number}"
    );
    merge_vm_account_map(&mut left.slots, right.slots);
    merge_vm_account_map(&mut left.token_balances, right.token_balances);
    left.title = deterministic_account_title(left.title, right.title);
    Ok(left)
}

fn merge_vm_account_map<V>(target: &mut HashMap<Bytes, V>, incoming: HashMap<Bytes, V>) {
    // Tycho's protocol-local account views can disagree on an overlapping value.
    // Sorted protocol order gives the shared VM database one stable winner.
    for (key, value) in incoming {
        target.entry(key).or_insert(value);
    }
}

fn deterministic_account_title(left: String, right: String) -> String {
    match (left.is_empty(), right.is_empty()) {
        (true, false) => right,
        (false, true) | (true, true) => left,
        (false, false) => left.min(right),
    }
}

fn touched_vm_addresses(message: &BroadcasterProtocolMessage) -> BTreeSet<Bytes> {
    let mut touched = message
        .message
        .snapshots
        .vm_storage
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    if let Some(changes) = &message.message.deltas {
        touched.extend(changes.account_deltas.keys().cloned());
        touched.extend(changes.account_balances.keys().cloned());
    }
    touched
}

fn project_vm_account(
    partitions: &BTreeMap<BroadcasterBackend, BroadcasterPartitionState>,
    backend: BroadcasterBackend,
    incoming: &BroadcasterProtocolMessage,
    address: &Bytes,
) -> ProjectedVmAccount {
    let changes = incoming.message.deltas.as_ref();
    let update = changes.and_then(|changes| changes.account_deltas.get(address));
    let balances = changes
        .and_then(|changes| changes.account_balances.get(address))
        .cloned()
        .unwrap_or_default();
    if update.is_some_and(|update| matches!(update.change_type(), ChangeType::Deletion)) {
        return ProjectedVmAccount::Materialized(None);
    }
    if update.is_some_and(|update| matches!(update.change_type(), ChangeType::Creation)) {
        return ProjectedVmAccount::Residual {
            update: update.cloned(),
            balances,
        };
    }

    let account = incoming
        .message
        .snapshots
        .vm_storage
        .get(address)
        .cloned()
        .or_else(|| {
            partitions.get(&backend).and_then(|partition| {
                partition
                    .messages
                    .iter()
                    .find(|message| message.protocol == incoming.protocol)
                    .and_then(|message| message.message.snapshots.vm_storage.get(address))
                    .cloned()
                    .or_else(|| {
                        partition.messages.iter().find_map(|message| {
                            message.message.snapshots.vm_storage.get(address).cloned()
                        })
                    })
            })
        });
    if let Some(mut account) = account {
        if let Some(update) = update.cloned() {
            fold_account_update_into_snapshot(&mut account, update);
        }
        account.token_balances.extend(balances);
        ProjectedVmAccount::Materialized(Some(account))
    } else {
        ProjectedVmAccount::Residual {
            update: update.cloned(),
            balances,
        }
    }
}

fn raw_message_has_header_gap(
    partitions: &BTreeMap<BroadcasterBackend, BroadcasterPartitionState>,
    update: &BroadcasterUpdateMessage,
) -> bool {
    update.partitions.iter().any(|partition| {
        partition.messages.iter().any(|incoming| {
            find_raw_protocol_message(partitions, &incoming.protocol).is_some_and(|previous| {
                raw_header_is_discontinuous(&previous.message.header, &incoming.message.header)
                    && (!incoming.message.snapshots.states.is_empty()
                        || !incoming.message.snapshots.vm_storage.is_empty())
            })
        })
    })
}

fn raw_header_is_discontinuous(previous: &BlockHeader, incoming: &BlockHeader) -> bool {
    incoming.hash != previous.hash && incoming.parent_hash != previous.hash
}

fn find_raw_protocol_message<'a>(
    partitions: &'a BTreeMap<BroadcasterBackend, BroadcasterPartitionState>,
    protocol: &str,
) -> Option<&'a BroadcasterProtocolMessage> {
    partitions
        .values()
        .flat_map(|partition| &partition.messages)
        .find(|message| message.protocol == protocol)
}

fn find_raw_protocol_message_mut<'a>(
    partitions: &'a mut BTreeMap<BroadcasterBackend, BroadcasterPartitionState>,
    protocol: &str,
) -> Option<&'a mut BroadcasterProtocolMessage> {
    partitions
        .values_mut()
        .flat_map(|partition| &mut partition.messages)
        .find(|message| message.protocol == protocol)
}

fn apply_replacement_feed_message(
    candidates: &mut BTreeMap<BroadcasterBackend, BroadcasterPartitionState>,
    update: &BroadcasterUpdateMessage,
    feed: &FeedMessage<BlockHeader>,
) -> Result<()> {
    ensure!(
        !raw_message_has_header_gap(candidates, update),
        "fresh Tycho replacement became discontinuous before it aligned"
    );
    validate_raw_update_message(candidates, update)?;
    for partition in &update.partitions {
        let candidate_partition = candidates.entry(partition.backend).or_default();
        candidate_partition.block_number = Some(partition.block_number);
        candidate_partition.sync_statuses = partition.sync_statuses.clone();
        let mut messages = partition.messages.iter().collect::<Vec<_>>();
        messages.sort_by(|left, right| left.protocol.cmp(&right.protocol));
        for incoming in messages {
            merge_raw_message(&mut candidate_partition.messages, incoming.clone());
            propagate_touched_vm_accounts(&mut candidate_partition.messages, incoming)?;
        }
        canonicalize_shared_vm_accounts(&mut candidate_partition.messages)?;
    }

    for (protocol, sync_state) in &feed.sync_states {
        if let Some(candidate) = find_raw_protocol_message_mut(candidates, protocol) {
            candidate.sync_state = sync_state.clone();
            if !feed.state_msgs.contains_key(protocol) {
                if let SynchronizerState::Ready(header) = sync_state {
                    candidate.message.header = header.clone();
                }
            }
        }
    }
    for partition in candidates.values_mut() {
        partition
            .messages
            .sort_by(|left, right| left.protocol.cmp(&right.protocol));
        partition.block_number = partition
            .messages
            .iter()
            .map(|message| message.message.header.number)
            .max();
    }
    Ok(())
}

fn propagate_touched_vm_accounts(
    messages: &mut [BroadcasterProtocolMessage],
    incoming: &BroadcasterProtocolMessage,
) -> Result<()> {
    let mut touched = incoming
        .message
        .snapshots
        .vm_storage
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut deleted = BTreeSet::new();
    if let Some(changes) = &incoming.message.deltas {
        touched.extend(changes.account_deltas.keys().cloned());
        touched.extend(changes.account_balances.keys().cloned());
        deleted.extend(
            changes
                .account_deltas
                .iter()
                .filter_map(|(address, update)| {
                    matches!(update.change_type(), ChangeType::Deletion).then_some(address.clone())
                }),
        );
    }

    for address in touched {
        if deleted.contains(&address) {
            for message in messages.iter_mut() {
                message.message.snapshots.vm_storage.remove(&address);
            }
            continue;
        }
        let mut same_block = messages
            .iter()
            .filter(|message| message.message.header.number == incoming.message.header.number)
            .filter_map(|message| {
                message
                    .message
                    .snapshots
                    .vm_storage
                    .get(&address)
                    .cloned()
                    .map(|account| (message.protocol.as_str(), account))
            })
            .collect::<Vec<_>>();
        same_block.sort_by_key(|(protocol, _)| *protocol);
        let canonical = same_block
            .into_iter()
            .map(|(_, account)| account)
            .try_fold(None, |canonical, account| match canonical {
                Some(canonical) => {
                    merge_shared_vm_accounts(canonical, account, incoming.message.header.number)
                        .map(Some)
                }
                None => Ok(Some(account)),
            })?;
        if let Some(canonical) = canonical {
            for message in messages
                .iter_mut()
                .filter(|message| message.message.snapshots.vm_storage.contains_key(&address))
            {
                message
                    .message
                    .snapshots
                    .vm_storage
                    .insert(address.clone(), canonical.clone());
            }
        }
    }
    Ok(())
}

fn canonicalize_shared_vm_accounts(messages: &mut [BroadcasterProtocolMessage]) -> Result<()> {
    let addresses = messages
        .iter()
        .flat_map(|message| message.message.snapshots.vm_storage.keys().cloned())
        .collect::<BTreeSet<_>>();
    for address in addresses {
        let latest_block = messages
            .iter()
            .filter(|message| message.message.snapshots.vm_storage.contains_key(&address))
            .map(|message| message.message.header.number)
            .max()
            .unwrap_or_default();
        let mut latest = messages
            .iter()
            .filter(|message| message.message.header.number == latest_block)
            .filter_map(|message| {
                message
                    .message
                    .snapshots
                    .vm_storage
                    .get(&address)
                    .cloned()
                    .map(|account| (message.protocol.as_str(), account))
            })
            .collect::<Vec<_>>();
        latest.sort_by_key(|(protocol, _)| *protocol);
        let canonical = latest.into_iter().map(|(_, account)| account).try_fold(
            None,
            |canonical, account| match canonical {
                Some(canonical) => {
                    merge_shared_vm_accounts(canonical, account, latest_block).map(Some)
                }
                None => Ok(Some(account)),
            },
        )?;
        let Some(canonical) = canonical else {
            continue;
        };
        for message in messages
            .iter_mut()
            .filter(|message| message.message.snapshots.vm_storage.contains_key(&address))
        {
            message
                .message
                .snapshots
                .vm_storage
                .insert(address.clone(), canonical.clone());
        }
    }
    Ok(())
}

fn replacement_is_aligned(
    candidates: &BTreeMap<BroadcasterBackend, BroadcasterPartitionState>,
    expected_protocols: &BTreeSet<String>,
) -> bool {
    if expected_protocols.is_empty() {
        return false;
    }
    let messages = candidates
        .values()
        .flat_map(|partition| &partition.messages)
        .collect::<Vec<_>>();
    if messages.len() != expected_protocols.len()
        || messages
            .iter()
            .any(|message| !expected_protocols.contains(&message.protocol))
        || candidates.values().any(|partition| {
            partition.block_number.is_none_or(|block_number| {
                complete_broadcaster_partition_block(
                    block_number,
                    &partition.messages,
                    &partition.sync_statuses,
                ) != Some(block_number)
            })
        })
    {
        return false;
    }
    let Some(first) = messages.first() else {
        return false;
    };
    messages.iter().all(|message| {
        message.message.header.number == first.message.header.number
            && message.message.header.hash == first.message.header.hash
    })
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

fn validate_update_message_applicable(
    guard: &BroadcasterSnapshotCacheData,
    message: &BroadcasterUpdateMessage,
) -> Result<()> {
    for partition in &message.partitions {
        let new_pair_ids = partition
            .new_pairs
            .iter()
            .map(|entry| entry.component_id.as_str())
            .collect::<HashSet<_>>();
        let partition_state = guard.partitions.get(&partition.backend);
        for delta in &partition.updated_states {
            if delta.backend != partition.backend {
                return Err(anyhow!(
                    "backend mismatch for {}: expected {}, found {}",
                    delta.component_id,
                    partition.backend,
                    delta.backend
                ));
            }

            let is_known = partition_state
                .is_some_and(|state| state.states.contains_key(&delta.component_id))
                || new_pair_ids.contains(delta.component_id.as_str());
            if !is_known {
                return Err(anyhow!(
                    "missing tracked broadcaster state for {} on backend {}",
                    delta.component_id,
                    partition.backend
                ));
            }
        }
    }

    Ok(())
}

fn raw_update_new_tokens(message: &BroadcasterUpdateMessage) -> Vec<Token> {
    let mut new_tokens = Vec::new();
    for partition in &message.partitions {
        let mut messages = partition.messages.iter().collect::<Vec<_>>();
        messages.sort_by(|left, right| left.protocol.cmp(&right.protocol));
        for message in messages {
            new_tokens.extend(
                message
                    .message
                    .deltas
                    .iter()
                    .flat_map(|deltas| deltas.new_tokens.values())
                    .cloned(),
            );
        }
    }
    new_tokens
}

fn merge_raw_message(
    messages: &mut Vec<BroadcasterProtocolMessage>,
    mut incoming: BroadcasterProtocolMessage,
) {
    if let Some(existing_index) = messages
        .iter_mut()
        .position(|message| message.protocol == incoming.protocol)
    {
        // Moving the affected protocol out avoids cloning its full materialized state per block.
        let mut existing = messages.remove(existing_index);
        let previous_residue_count = raw_residue_entry_count(&existing.message);
        existing.sync_state = incoming.sync_state;
        let (incoming_new_tokens, incoming_dci_update) = incoming
            .message
            .deltas
            .as_mut()
            .map(|deltas| {
                (
                    std::mem::take(&mut deltas.new_tokens),
                    std::mem::take(&mut deltas.dci_update),
                )
            })
            .unwrap_or_default();
        let incoming_account_lifecycle = incoming
            .message
            .deltas
            .as_ref()
            .map(|deltas| {
                deltas
                    .account_deltas
                    .iter()
                    .filter(|(_, update)| {
                        matches!(
                            update.change_type(),
                            ChangeType::Creation | ChangeType::Deletion
                        )
                    })
                    .map(|(address, update)| (address.clone(), update.clone()))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();
        existing.message = existing.message.merge(incoming.message);
        if let Some(deltas) = existing.message.deltas.as_mut() {
            // BlockAggregatedChanges::merge omits new_tokens and dci_update, so preserve them explicitly.
            deltas.new_tokens.extend(incoming_new_tokens);
            for (component_id, entrypoints) in incoming_dci_update.new_entrypoints {
                deltas
                    .dci_update
                    .new_entrypoints
                    .entry(component_id)
                    .or_default()
                    .extend(entrypoints);
            }
            for (entrypoint_id, params) in incoming_dci_update.new_entrypoint_params {
                deltas
                    .dci_update
                    .new_entrypoint_params
                    .entry(entrypoint_id)
                    .or_default()
                    .extend(params);
            }
            deltas
                .dci_update
                .trace_results
                .extend(incoming_dci_update.trace_results);
            // Creation and deletion are lifecycle edges. The dependency's merge keeps an old
            // creation forever, so the newest edge has to win before materialization.
            deltas.account_deltas.extend(incoming_account_lifecycle);
        }
        let stats = compact_raw_state_sync_message(&mut existing.message);
        if stats.residual_entries > previous_residue_count {
            warn!(
                event = "broadcaster_raw_cache_residue_growth",
                protocol = %existing.protocol,
                previous_count = previous_residue_count,
                current_count = stats.residual_entries,
                folded_state_updates = stats.folded_state_updates,
                folded_component_balances = stats.folded_component_balances,
                folded_component_tvl = stats.folded_component_tvl,
                folded_account_updates = stats.folded_account_updates,
                folded_account_balances = stats.folded_account_balances,
                "Broadcaster raw cache residue grew after compaction"
            );
        }
        messages.push(existing);
    } else {
        let previous_residue_count = 0;
        let stats = compact_raw_state_sync_message(&mut incoming.message);
        if stats.residual_entries > previous_residue_count {
            warn!(
                event = "broadcaster_raw_cache_residue_growth",
                protocol = %incoming.protocol,
                previous_count = previous_residue_count,
                current_count = stats.residual_entries,
                folded_state_updates = stats.folded_state_updates,
                folded_component_balances = stats.folded_component_balances,
                folded_component_tvl = stats.folded_component_tvl,
                folded_account_updates = stats.folded_account_updates,
                folded_account_balances = stats.folded_account_balances,
                "Broadcaster raw cache residue grew after compaction"
            );
        }
        messages.push(incoming);
    }
    messages.sort_by(|left, right| left.protocol.cmp(&right.protocol));
}

#[derive(Debug, Default, PartialEq, Eq)]
struct RawCompactionStats {
    folded_state_updates: usize,
    folded_component_balances: usize,
    folded_component_tvl: usize,
    folded_account_updates: usize,
    folded_account_balances: usize,
    residual_entries: usize,
}

fn compact_raw_state_sync_message(
    message: &mut StateSyncMessage<BlockHeader>,
) -> RawCompactionStats {
    let removed_components = std::mem::take(&mut message.removed_components);
    for component_id in removed_components.keys() {
        message.snapshots.states.remove(component_id);
        if let Some(deltas) = message.deltas.as_mut() {
            deltas.state_deltas.remove(component_id);
            deltas.component_balances.remove(component_id);
            deltas.component_tvl.remove(component_id);
            deltas.new_protocol_components.remove(component_id);
            deltas.deleted_protocol_components.remove(component_id);
        }
    }

    let mut stats = message
        .deltas
        .as_mut()
        .map(|deltas| fold_block_changes_into_snapshot(&mut message.snapshots, deltas))
        .unwrap_or_default();
    if message
        .deltas
        .as_ref()
        .is_some_and(|deltas| !block_changes_has_bootstrap_residue(deltas))
    {
        message.deltas = None;
    }
    stats.residual_entries = raw_residue_entry_count(message);
    stats
}

fn fold_block_changes_into_snapshot(
    snapshots: &mut Snapshot,
    deltas: &mut BlockAggregatedChanges,
) -> RawCompactionStats {
    let mut stats = RawCompactionStats::default();

    // Tokens come from /tokens/snapshot before the consumer builds its decoder.
    deltas.new_tokens.clear();

    for component_id in std::mem::take(&mut deltas.deleted_protocol_components).into_keys() {
        snapshots.states.remove(&component_id);
        deltas.state_deltas.remove(&component_id);
        deltas.component_balances.remove(&component_id);
        deltas.component_tvl.remove(&component_id);
        deltas.new_protocol_components.remove(&component_id);
        deltas.dci_update.new_entrypoints.remove(&component_id);
    }
    deltas
        .new_protocol_components
        .retain(|component_id, _| !snapshots.states.contains_key(component_id));

    deltas.state_deltas.retain(|component_id, delta| {
        let Some(component) = snapshots.states.get_mut(component_id) else {
            return true;
        };
        apply_protocol_delta_to_snapshot(component, std::mem::take(delta));
        stats.folded_state_updates += 1;
        false
    });

    deltas.component_balances.retain(|component_id, balances| {
        let Some(component) = snapshots.states.get_mut(component_id) else {
            return true;
        };
        component.state.balances.extend(
            std::mem::take(balances)
                .into_iter()
                .map(|(token, balance)| (token, balance.balance)),
        );
        stats.folded_component_balances += 1;
        false
    });

    deltas.component_tvl.retain(|component_id, tvl| {
        let Some(component) = snapshots.states.get_mut(component_id) else {
            return true;
        };
        component.component_tvl = Some(*tvl);
        stats.folded_component_tvl += 1;
        false
    });

    deltas.account_deltas.retain(|address, update| {
        if matches!(update.change_type(), ChangeType::Deletion) {
            snapshots.vm_storage.remove(address);
            deltas.account_balances.remove(address);
            stats.folded_account_updates += 1;
            return false;
        }
        if matches!(update.change_type(), ChangeType::Creation) {
            // A creation has no title or code hashes, so keep the latest operation for bootstrap.
            snapshots.vm_storage.remove(address);
            return true;
        }
        let Some(account) = snapshots.vm_storage.get_mut(address) else {
            return true;
        };
        fold_account_update_into_snapshot(account, update.clone());
        stats.folded_account_updates += 1;
        false
    });

    deltas.account_balances.retain(|address, balances| {
        let Some(account) = snapshots.vm_storage.get_mut(address) else {
            return true;
        };
        account.token_balances.extend(std::mem::take(balances));
        stats.folded_account_balances += 1;
        false
    });

    let active_entrypoint_ids = deltas
        .dci_update
        .new_entrypoints
        .values()
        .flat_map(|entrypoints| entrypoints.iter().map(|entrypoint| &entrypoint.external_id))
        .collect::<HashSet<_>>();
    deltas
        .dci_update
        .new_entrypoint_params
        .retain(|entrypoint_id, _| active_entrypoint_ids.contains(entrypoint_id));
    deltas
        .dci_update
        .trace_results
        .retain(|entrypoint_id, _| active_entrypoint_ids.contains(entrypoint_id));

    stats
}

fn apply_protocol_delta_to_snapshot(
    component: &mut tycho_simulation::tycho_client::feed::synchronizer::ComponentWithState,
    delta: ProtocolComponentStateDelta,
) {
    for attribute in delta.deleted_attributes {
        component.state.attributes.remove(&attribute);
    }
    component.state.attributes.extend(delta.updated_attributes);
}

fn fold_account_update_into_snapshot(account: &mut Account, update: AccountDelta) {
    let code = update.code().clone();
    account.slots.extend(
        update
            .slots
            .into_iter()
            .map(|(slot, value)| (slot, value.unwrap_or_default())),
    );
    if let Some(balance) = update.balance {
        account.native_balance = balance;
    }
    if let Some(code) = code {
        account.code = code;
    }
}

fn block_changes_has_bootstrap_residue(deltas: &BlockAggregatedChanges) -> bool {
    !deltas.new_tokens.is_empty()
        || !deltas.account_deltas.is_empty()
        || !deltas.state_deltas.is_empty()
        || !deltas.new_protocol_components.is_empty()
        || !deltas.deleted_protocol_components.is_empty()
        || !deltas.component_balances.is_empty()
        || !deltas.account_balances.is_empty()
        || !deltas.component_tvl.is_empty()
        || !deltas.dci_update.new_entrypoints.is_empty()
        || !deltas.dci_update.new_entrypoint_params.is_empty()
        || !deltas.dci_update.trace_results.is_empty()
}

fn raw_residue_entry_count(message: &StateSyncMessage<BlockHeader>) -> usize {
    let delta_entries = message.deltas.as_ref().map_or(0, |deltas| {
        deltas.new_tokens.len()
            + deltas.account_deltas.len()
            + deltas.state_deltas.len()
            + deltas.new_protocol_components.len()
            + deltas.deleted_protocol_components.len()
            + deltas.component_balances.len()
            + deltas.account_balances.len()
            + deltas.component_tvl.len()
            + deltas.dci_update.new_entrypoints.len()
            + deltas.dci_update.new_entrypoint_params.len()
            + deltas.dci_update.trace_results.len()
    });
    delta_entries + message.removed_components.len()
}

fn apply_update_message(
    guard: &mut BroadcasterSnapshotCacheData,
    message: &BroadcasterUpdateMessage,
) {
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
            apply_state_delta(partition.backend, partition_state, delta);
        }

        for removed in &partition.removed_pairs {
            guard.known_backends.remove(&removed.component_id);
            partition_state.states.remove(&removed.component_id);
        }
    }
}

fn apply_state_delta(
    backend: BroadcasterBackend,
    partition_state: &mut BroadcasterPartitionState,
    delta: &BroadcasterStateDelta,
) {
    assert_eq!(
        delta.backend, backend,
        "staged broadcaster delta backend mismatch for {}",
        delta.component_id
    );
    assert!(
        partition_state.states.contains_key(&delta.component_id),
        "staged broadcaster delta missing tracked state for {} on backend {}",
        delta.component_id,
        backend
    );
    if let Some(existing) = partition_state.states.get_mut(&delta.component_id) {
        existing.state = delta.state.clone();
    }
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
        let fragments = vec![message.clone()];
        log_protocol_snapshot_export(ctx, message, sync_statuses, &fragments)?;
        return Ok(fragments);
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

    if current_has_payload {
        fragments.push(current);
    }

    let tail_fragments =
        split_protocol_tail_for_snapshot(ctx, message, sync_statuses, fragments.is_empty())?;
    fragments.extend(tail_fragments);

    log_protocol_snapshot_export(ctx, message, sync_statuses, &fragments)?;

    Ok(fragments)
}

fn split_protocol_tail_for_snapshot(
    ctx: &SnapshotChunkBuildContext<'_>,
    message: &BroadcasterProtocolMessage,
    sync_statuses: &BTreeMap<String, BroadcasterProtocolSyncStatus>,
    include_sync_statuses: bool,
) -> Result<Vec<BroadcasterProtocolMessage>> {
    if message.message.deltas.is_none() && message.message.removed_components.is_empty() {
        return Ok(Vec::new());
    }

    let mut builder =
        ProtocolTailFragmentBuilder::new(ctx, message, sync_statuses, include_sync_statuses);

    if let Some(source) = message.message.deltas.as_ref() {
        builder.append_delta_entries("new_tokens", &source.new_tokens, |deltas, key, value| {
            deltas.new_tokens.insert(key, value);
        })?;
        builder.append_delta_entries(
            "state_updates",
            &source.state_deltas,
            |deltas, key, value| {
                deltas.state_deltas.insert(key, value);
            },
        )?;
        builder.append_delta_entries(
            "account_updates",
            &source.account_deltas,
            |deltas, key, value| {
                deltas.account_deltas.insert(key, value);
            },
        )?;
        builder.append_delta_entries(
            "component_balances",
            &source.component_balances,
            |deltas, key, value| {
                deltas.component_balances.insert(key, value);
            },
        )?;
        builder.append_delta_entries(
            "account_balances",
            &source.account_balances,
            |deltas, key, value| {
                deltas.account_balances.insert(key, value);
            },
        )?;
        builder.append_delta_entries(
            "component_tvl",
            &source.component_tvl,
            |deltas, key, value| {
                deltas.component_tvl.insert(key, value);
            },
        )?;
        builder.append_delta_entries(
            "new_protocol_components",
            &source.new_protocol_components,
            |deltas, key, value| {
                deltas.new_protocol_components.insert(key, value);
            },
        )?;
        builder.append_delta_entries(
            "deleted_protocol_components",
            &source.deleted_protocol_components,
            |deltas, key, value| {
                deltas.deleted_protocol_components.insert(key, value);
            },
        )?;
        builder.append_delta_entries(
            "dci.new_entrypoints",
            &source.dci_update.new_entrypoints,
            |deltas, key, value| {
                deltas.dci_update.new_entrypoints.insert(key, value);
            },
        )?;
        builder.append_delta_entries(
            "dci.new_entrypoint_params",
            &source.dci_update.new_entrypoint_params,
            |deltas, key, value| {
                deltas.dci_update.new_entrypoint_params.insert(key, value);
            },
        )?;
        builder.append_delta_entries(
            "dci.trace_results",
            &source.dci_update.trace_results,
            |deltas, key, value| {
                deltas.dci_update.trace_results.insert(key, value);
            },
        )?;
    }

    let mut removed_components = message
        .message
        .removed_components
        .iter()
        .collect::<Vec<_>>();
    removed_components.sort_by(|left, right| left.0.cmp(right.0));
    for (component_id, component) in removed_components {
        builder.append_named("removed_components", component_id, |fragment| {
            fragment
                .message
                .removed_components
                .insert(component_id.clone(), component.clone());
        })?;
    }

    builder.finish()
}

struct ProtocolTailFragmentBuilder<'a> {
    ctx: &'a SnapshotChunkBuildContext<'a>,
    source_message: &'a BroadcasterProtocolMessage,
    sync_statuses: &'a BTreeMap<String, BroadcasterProtocolSyncStatus>,
    fragments: Vec<BroadcasterProtocolMessage>,
    current: BroadcasterProtocolMessage,
    current_has_payload: bool,
    include_sync_for_current: bool,
}

impl<'a> ProtocolTailFragmentBuilder<'a> {
    fn new(
        ctx: &'a SnapshotChunkBuildContext<'a>,
        source_message: &'a BroadcasterProtocolMessage,
        sync_statuses: &'a BTreeMap<String, BroadcasterProtocolSyncStatus>,
        include_sync_statuses: bool,
    ) -> Self {
        Self {
            ctx,
            source_message,
            sync_statuses,
            fragments: Vec::new(),
            current: empty_protocol_fragment(source_message, false),
            current_has_payload: false,
            include_sync_for_current: include_sync_statuses,
        }
    }

    fn append_delta_entries<K, V>(
        &mut self,
        kind: &str,
        entries: &HashMap<K, V>,
        insert: impl Fn(&mut BlockAggregatedChanges, K, V),
    ) -> Result<()>
    where
        K: Clone + Eq + std::hash::Hash + Ord + std::fmt::Debug,
        V: Clone,
    {
        let empty_deltas = self
            .source_message
            .message
            .deltas
            .as_ref()
            .map(empty_block_changes_fragment)
            .ok_or_else(|| anyhow!("delta entry without source deltas"))?;
        let mut entries = entries.iter().collect::<Vec<_>>();
        entries.sort_by(|left, right| left.0.cmp(right.0));
        for (key, value) in entries {
            let key_label = format!("{key:?}");
            self.append_named(kind, &key_label, |fragment| {
                let deltas = fragment
                    .message
                    .deltas
                    .get_or_insert_with(|| empty_deltas.clone());
                insert(deltas, key.clone(), value.clone());
            })?;
        }
        Ok(())
    }

    fn append_named(
        &mut self,
        kind: &str,
        key: &str,
        insert: impl Fn(&mut BroadcasterProtocolMessage),
    ) -> Result<()> {
        let mut candidate = self.current.clone();
        insert(&mut candidate);
        if self.ctx.raw_fragment_fits(
            candidate.clone(),
            self.sync_statuses,
            self.include_sync_for_current,
        )? {
            self.current = candidate;
            self.current_has_payload = true;
            return Ok(());
        }

        if self.current_has_payload {
            self.ensure_current_fits()?;
            self.fragments.push(self.current.clone());
            self.include_sync_for_current = false;
        }

        self.current = empty_protocol_fragment(self.source_message, false);
        if let Some(source) = self.source_message.message.deltas.as_ref() {
            self.current.message.deltas = Some(empty_block_changes_fragment(source));
        }
        insert(&mut self.current);
        self.current_has_payload = true;
        if !self.ctx.raw_fragment_fits(
            self.current.clone(),
            self.sync_statuses,
            self.include_sync_for_current,
        )? {
            let size = self.ctx.raw_fragment_size(
                self.current.clone(),
                self.sync_statuses,
                self.include_sync_for_current,
            )?;
            return Err(anyhow!(
                "broadcaster snapshot leaf for protocol {} kind {kind} key {key} is {size} bytes, above configured max {}",
                self.source_message.protocol,
                self.ctx.max_payload_bytes
            ));
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<BroadcasterProtocolMessage>> {
        if self.current_has_payload {
            self.ensure_current_fits()?;
            self.fragments.push(self.current);
        }
        Ok(self.fragments)
    }

    fn ensure_current_fits(&self) -> Result<()> {
        ensure!(
            self.ctx.raw_fragment_fits(
                self.current.clone(),
                self.sync_statuses,
                self.include_sync_for_current,
            )?,
            "broadcaster snapshot delta/removal fragment for protocol {} exceeds {} bytes",
            self.source_message.protocol,
            self.ctx.max_payload_bytes
        );
        Ok(())
    }
}

fn empty_block_changes_fragment(source: &BlockAggregatedChanges) -> BlockAggregatedChanges {
    BlockAggregatedChanges {
        extractor: source.extractor.clone(),
        chain: source.chain,
        block: source.block.clone(),
        finalized_block_height: source.finalized_block_height,
        db_committed_block_height: source.db_committed_block_height,
        revert: source.revert,
        new_tokens: HashMap::new(),
        account_deltas: HashMap::new(),
        state_deltas: HashMap::new(),
        new_protocol_components: HashMap::new(),
        deleted_protocol_components: HashMap::new(),
        component_balances: HashMap::new(),
        account_balances: HashMap::new(),
        component_tvl: HashMap::new(),
        dci_update: Default::default(),
        partial_block_index: source.partial_block_index,
    }
}

fn log_protocol_snapshot_export(
    ctx: &SnapshotChunkBuildContext<'_>,
    message: &BroadcasterProtocolMessage,
    sync_statuses: &BTreeMap<String, BroadcasterProtocolSyncStatus>,
    fragments: &[BroadcasterProtocolMessage],
) -> Result<()> {
    let mut largest_fragment_bytes = 0usize;
    for (index, fragment) in fragments.iter().enumerate() {
        largest_fragment_bytes = largest_fragment_bytes.max(ctx.raw_fragment_size(
            fragment.clone(),
            sync_statuses,
            index == 0,
        )?);
    }
    info!(
        event = "broadcaster_snapshot_protocol_export",
        protocol = %message.protocol,
        largest_fragment_bytes,
        fragment_count = fragments.len(),
        max_payload_bytes = ctx.max_payload_bytes,
        residual_entry_count = raw_residue_entry_count(&message.message),
        "broadcaster_snapshot_protocol_export"
    );
    Ok(())
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
    account: &Account,
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
    account: &Account,
    slots: &[(Bytes, Bytes)],
) -> BroadcasterProtocolMessage {
    let account = response_account_with_slots(account, slots.iter().cloned().collect());
    vm_storage_account_fragment(message, address, account)
}

fn response_account_with_slots(account: &Account, slots: HashMap<Bytes, Bytes>) -> Account {
    Account::new(
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
    account: Account,
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
    use std::collections::{BTreeMap, HashMap, HashSet};

    use super::{
        BroadcasterReadiness, BroadcasterSnapshotCache, BroadcasterSnapshotExport,
        BroadcasterSnapshotSessionsSnapshot, BroadcasterUpstreamState,
    };
    use anyhow::{anyhow, Result};
    use num_bigint::BigUint;
    use simulator_core::broadcaster::{
        BroadcasterBackend, BroadcasterEnvelope, BroadcasterPayload, BroadcasterProtocolMessage,
        BroadcasterProtocolSyncStatusKind, BroadcasterSnapshotChunk, BroadcasterSubscriptionEvent,
        BroadcasterSubscriptionTracker,
    };
    use tycho_common::{
        dto::{ProtocolStateDelta as SimulationProtocolStateDelta, ResponseToken},
        models::{
            blockchain::{BlockAggregatedChanges, EntryPoint},
            contract::{Account, AccountBalance, AccountDelta},
            protocol::{
                ComponentBalance, ProtocolComponent as DtoProtocolComponent,
                ProtocolComponentState, ProtocolComponentStateDelta,
            },
            Chain as DtoChain, ChangeType,
        },
        Bytes as DtoBytes,
    };
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
            _delta: SimulationProtocolStateDelta,
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
    ) -> Vec<&'a Account> {
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

    fn account_metadata_without_slots(account: &Account) -> Account {
        let mut metadata = account.clone();
        metadata.slots.clear();
        metadata
    }

    fn assert_vm_storage_account_fragments_match(
        export: &BroadcasterSnapshotExport,
        account_address: &DtoBytes,
        expected_account: &Account,
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
    async fn staged_decoded_commit_does_not_revalidate_after_staging() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        cache.apply_update(&native_only_update()).await?;
        let staged = cache
            .stage_update(&native_update_state(11, "native-1", 2))
            .await?;

        cache.commit_staged_update(staged).await?;

        let export = cache.export_snapshot(8_388_608).await?;
        let chunks = snapshot_chunks(&export);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].partitions[0].states[0].component_id, "native-1");
        Ok(())
    }

    #[tokio::test]
    async fn combines_raw_and_rfq_exports_into_one_snapshot_flow() -> Result<()> {
        let raw_cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        raw_cache.apply_update(&native_only_update()).await?;
        let rfq_cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Rfq]);
        rfq_cache
            .apply_update(&rfq_only_update(12, "rfq-1", 7))
            .await?;

        let export = super::combine_snapshot_exports(
            1,
            vec![
                raw_cache.export_snapshot(8_388_608).await?,
                rfq_cache.export_snapshot(8_388_608).await?,
            ],
        )?;

        assert_eq!(export.stream_id, "chain-1-stream-1");
        assert_eq!(export.snapshot_id, "chain-1-snapshot-1");
        assert_eq!(export.payloads.len(), 4);
        let BroadcasterPayload::SnapshotStart(start) = &export.payloads[0] else {
            return Err(anyhow!("expected combined snapshot_start"));
        };
        assert_eq!(
            start.backends,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq]
        );
        assert_eq!(start.total_chunks, 2);
        let chunks = snapshot_chunks(&export);
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[0].partitions[0].backend, BroadcasterBackend::Native);
        assert_eq!(chunks[1].chunk_index, 1);
        assert_eq!(chunks[1].partitions[0].backend, BroadcasterBackend::Rfq);
        assert!(matches!(
            export.payloads.last(),
            Some(BroadcasterPayload::SnapshotEnd(end)) if end.snapshot_id == "chain-1-snapshot-1"
        ));
        assert_payloads_at_most(&export, 8_388_608)?;
        Ok(())
    }

    #[tokio::test]
    async fn combined_snapshot_export_rejects_mismatched_stream_generation() -> Result<()> {
        let raw_cache = BroadcasterSnapshotCache::new_with_initial_generation(
            1,
            vec![BroadcasterBackend::Native],
            1,
        );
        raw_cache.apply_update(&native_only_update()).await?;
        let rfq_cache = BroadcasterSnapshotCache::new_with_initial_generation(
            1,
            vec![BroadcasterBackend::Rfq],
            2,
        );
        rfq_cache
            .apply_update(&rfq_only_update(12, "rfq-1", 7))
            .await?;

        let Err(error) = super::combine_snapshot_exports(
            1,
            vec![
                raw_cache.export_snapshot(8_388_608).await?,
                rfq_cache.export_snapshot(8_388_608).await?,
            ],
        ) else {
            return Err(anyhow!("mismatched stream generations must fail"));
        };

        assert!(
            error
                .to_string()
                .contains("snapshot export stream_id mismatch"),
            "unexpected error: {error}"
        );
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

    #[test]
    fn raw_cache_compacts_protocol_updates_into_snapshot() {
        let component_id = "raw-1";
        let token = DtoBytes::from([17u8; 20]);
        let mut messages = Vec::new();
        super::merge_raw_message(
            &mut messages,
            raw_protocol_message_with_changes(
                "uniswap_v2",
                1,
                Snapshot {
                    states: HashMap::from([(
                        component_id.to_string(),
                        raw_component_with_state(component_id, 1),
                    )]),
                    vm_storage: HashMap::new(),
                },
                None,
                HashMap::new(),
            ),
        );

        for block_number in 2..=20 {
            let mut changes = BlockAggregatedChanges::default();
            changes.state_deltas.insert(
                component_id.to_string(),
                component_state_delta(
                    component_id,
                    [("version", DtoBytes::from([block_number as u8; 32]))],
                    (block_number == 20).then_some("large"),
                ),
            );
            changes.component_balances.insert(
                component_id.to_string(),
                HashMap::from([(
                    token.clone(),
                    component_balance(component_id, token.clone(), block_number as u8),
                )]),
            );
            changes
                .component_tvl
                .insert(component_id.to_string(), block_number as f64);
            let mut compacted = messages[0].message.clone();
            compacted.deltas = Some(changes.clone());
            let stats = super::compact_raw_state_sync_message(&mut compacted);
            assert_eq!(stats.folded_state_updates, 1);
            assert_eq!(stats.folded_component_balances, 1);
            assert_eq!(stats.folded_component_tvl, 1);
            super::merge_raw_message(
                &mut messages,
                raw_protocol_message_with_changes(
                    "uniswap_v2",
                    block_number,
                    Snapshot::default(),
                    Some(changes),
                    HashMap::new(),
                ),
            );
        }

        let message = &messages[0];
        let component = &message.message.snapshots.states[component_id];
        assert_eq!(
            component.state.attributes["version"],
            DtoBytes::from([20u8; 32])
        );
        assert!(!component.state.attributes.contains_key("large"));
        assert_eq!(component.state.balances[&token], DtoBytes::from([20u8; 32]));
        assert_eq!(component.component_tvl, Some(20.0));
        assert!(message.message.deltas.is_none());
        assert!(message.message.removed_components.is_empty());
        assert_eq!(message.message.header.number, 20);
        assert!(matches!(
            message.sync_state,
            SynchronizerState::Ready(ref header) if header.number == 20
        ));
    }

    #[test]
    fn raw_cache_keeps_unfoldable_component_residue() {
        let component_id = "missing";
        let token = DtoBytes::from([18u8; 20]);
        let mut changes = BlockAggregatedChanges::default();
        changes.state_deltas.insert(
            component_id.to_string(),
            component_state_delta(component_id, [("version", DtoBytes::from([2u8; 32]))], []),
        );
        changes.component_balances.insert(
            component_id.to_string(),
            HashMap::from([(token.clone(), component_balance(component_id, token, 2))]),
        );
        changes.component_tvl.insert(component_id.to_string(), 2.0);
        let expected = changes.clone();
        let mut message = raw_protocol_message_with_changes(
            "uniswap_v2",
            2,
            Snapshot::default(),
            Some(changes),
            HashMap::new(),
        );

        let stats = super::compact_raw_state_sync_message(&mut message.message);

        assert_eq!(message.message.deltas, Some(expected));
        assert_eq!(stats.residual_entries, 3);
    }

    #[test]
    fn raw_cache_drops_process_history_not_needed_by_bootstrap() {
        let mut messages = Vec::new();
        for block_number in 1..=2 {
            let token_address = DtoBytes::from([block_number as u8; 20]);
            let mut changes = BlockAggregatedChanges::default();
            changes.new_tokens.insert(
                token_address.clone(),
                ResponseToken {
                    chain: DtoChain::Ethereum.into(),
                    address: token_address,
                    symbol: format!("T{block_number}"),
                    decimals: 18,
                    tax: 0,
                    gas: Vec::new(),
                    quality: 100,
                }
                .into(),
            );
            changes
                .dci_update
                .trace_results
                .insert(format!("trace-{block_number}"), Default::default());
            super::merge_raw_message(
                &mut messages,
                raw_protocol_message_with_changes(
                    "vm:balancer_v2",
                    block_number,
                    Snapshot::default(),
                    Some(changes),
                    HashMap::new(),
                ),
            );
        }

        assert!(messages[0].message.deltas.is_none());
    }

    #[test]
    fn raw_cache_compacts_vm_updates_into_snapshot() {
        let address = DtoBytes::from([31u8; 20]);
        let token = DtoBytes::from([32u8; 20]);
        let mut message = raw_protocol_message_with_changes(
            "vm:balancer_v2",
            1,
            Snapshot {
                states: HashMap::new(),
                vm_storage: HashMap::from([(
                    address.clone(),
                    raw_response_account(address.clone(), 0, 0),
                )]),
            },
            None,
            HashMap::new(),
        );
        let mut changes = BlockAggregatedChanges::default();
        changes.account_deltas.insert(
            address.clone(),
            account_update(
                address.clone(),
                ChangeType::Update,
                7,
                Some(DtoBytes::from([41u8; 32])),
            ),
        );
        changes.account_balances.insert(
            address.clone(),
            HashMap::from([(
                token.clone(),
                account_balance(address.clone(), token.clone(), 42),
            )]),
        );
        message.message.deltas = Some(changes);

        let stats = super::compact_raw_state_sync_message(&mut message.message);
        assert_eq!(stats.folded_account_updates, 1);
        assert_eq!(stats.folded_account_balances, 1);

        let account = &message.message.snapshots.vm_storage[&address];
        assert_eq!(
            account.slots[&DtoBytes::from([7u8; 32])],
            DtoBytes::from([8u8; 32])
        );
        assert_eq!(account.native_balance, DtoBytes::from([41u8; 32]));
        assert_eq!(
            account.token_balances[&token].balance,
            DtoBytes::from([42u8; 32])
        );
        assert!(message.message.deltas.is_none());
    }

    #[test]
    fn raw_cache_keeps_vm_updates_with_distinct_engine_semantics() -> Result<()> {
        let creation = DtoBytes::from([51u8; 20]);
        let deletion = DtoBytes::from([52u8; 20]);
        let unspecified = DtoBytes::from([53u8; 20]);
        let absent = DtoBytes::from([54u8; 20]);
        let mut vm_storage = HashMap::new();
        for address in [&creation, &deletion, &unspecified] {
            vm_storage.insert(address.clone(), raw_response_account(address.clone(), 0, 0));
        }
        let mut changes = BlockAggregatedChanges::default();
        for (address, change, seed) in [
            (creation.clone(), ChangeType::Creation, 1),
            (deletion.clone(), ChangeType::Deletion, 2),
            (
                unspecified.clone(),
                tycho_common::dto::ChangeType::Unspecified.into(),
                3,
            ),
            (absent.clone(), ChangeType::Update, 4),
        ] {
            changes
                .account_deltas
                .insert(address.clone(), account_update(address, change, seed, None));
        }
        let expected = HashMap::from([
            (creation.clone(), changes.account_deltas[&creation].clone()),
            (absent.clone(), changes.account_deltas[&absent].clone()),
        ]);
        let mut message = raw_protocol_message_with_changes(
            "vm:balancer_v2",
            2,
            Snapshot {
                states: HashMap::new(),
                vm_storage,
            },
            Some(changes),
            HashMap::new(),
        );

        super::compact_raw_state_sync_message(&mut message.message);

        let Some(deltas) = message.message.deltas else {
            return Err(anyhow!("expected VM update residue"));
        };
        assert_eq!(deltas.account_deltas, expected);
        Ok(())
    }

    #[test]
    fn raw_cache_account_creation_then_deletion_does_not_resurrect_on_bootstrap() {
        let address = DtoBytes::from([58u8; 20]);
        let mut creation = BlockAggregatedChanges::default();
        creation.account_deltas.insert(
            address.clone(),
            account_update(address.clone(), ChangeType::Creation, 1, None),
        );
        let mut messages = Vec::new();
        super::merge_raw_message(
            &mut messages,
            raw_protocol_message_with_changes(
                "vm:curve",
                10,
                Snapshot::default(),
                Some(creation),
                HashMap::new(),
            ),
        );

        let mut deletion = BlockAggregatedChanges::default();
        deletion.account_deltas.insert(
            address.clone(),
            account_update(address.clone(), ChangeType::Deletion, 2, None),
        );
        super::merge_raw_message(
            &mut messages,
            raw_protocol_message_with_changes(
                "vm:curve",
                11,
                Snapshot::default(),
                Some(deletion),
                HashMap::new(),
            ),
        );

        assert!(!messages[0]
            .message
            .snapshots
            .vm_storage
            .contains_key(&address));
        assert!(messages[0].message.deltas.is_none());
    }

    #[test]
    fn raw_cache_prunes_removed_components_for_fresh_bootstrap() {
        let component_id = "removed";
        let account_address = DtoBytes::from([61u8; 20]);
        let mut changes = BlockAggregatedChanges::default();
        changes.state_deltas.insert(
            component_id.to_string(),
            component_state_delta(component_id, [("version", DtoBytes::from([2u8; 32]))], []),
        );
        changes
            .component_balances
            .insert(component_id.to_string(), HashMap::new());
        changes.component_tvl.insert(component_id.to_string(), 2.0);
        changes.new_protocol_components.insert(
            component_id.to_string(),
            raw_component(component_id, "uniswap_v2", 2),
        );
        changes.deleted_protocol_components.insert(
            component_id.to_string(),
            raw_component(component_id, "uniswap_v2", 2),
        );
        let mut message = raw_protocol_message_with_changes(
            "uniswap_v2",
            2,
            Snapshot {
                states: HashMap::from([(
                    component_id.to_string(),
                    raw_component_with_state(component_id, 1),
                )]),
                vm_storage: HashMap::from([(
                    account_address.clone(),
                    raw_response_account(account_address.clone(), 0, 0),
                )]),
            },
            Some(changes),
            HashMap::from([(
                component_id.to_string(),
                raw_component(component_id, "uniswap_v2", 2),
            )]),
        );

        super::compact_raw_state_sync_message(&mut message.message);

        assert!(!message.message.snapshots.states.contains_key(component_id));
        assert!(message.message.deltas.is_none());
        assert!(message.message.removed_components.is_empty());
        assert!(message
            .message
            .snapshots
            .vm_storage
            .contains_key(&account_address));
    }

    #[test]
    fn raw_cache_prunes_deleted_component_entrypoints() -> Result<()> {
        let surviving_component = "surviving";
        let surviving_entrypoint = "entrypoint-surviving";
        let mut states = HashMap::new();
        let mut changes = BlockAggregatedChanges::default();
        for (component, entrypoint) in [
            ("deleted", "entrypoint-deleted"),
            (surviving_component, surviving_entrypoint),
        ] {
            states.insert(
                component.to_string(),
                raw_component_with_state(component, 1),
            );
            changes.dci_update.new_entrypoints.insert(
                component.to_string(),
                HashSet::from([EntryPoint::new(
                    entrypoint.to_string(),
                    DtoBytes::from([61u8; 20]),
                    "balanceOf(address)".to_string(),
                )]),
            );
            changes
                .dci_update
                .new_entrypoint_params
                .insert(entrypoint.to_string(), HashSet::new());
            changes
                .dci_update
                .trace_results
                .insert(entrypoint.to_string(), Default::default());
        }
        changes.deleted_protocol_components.insert(
            "deleted".to_string(),
            raw_component("deleted", "uniswap_v2", 2),
        );
        let surviving_entrypoints = changes.dci_update.new_entrypoints[surviving_component].clone();
        let mut message = raw_protocol_message_with_changes(
            "uniswap_v2",
            2,
            Snapshot {
                states,
                vm_storage: HashMap::new(),
            },
            Some(changes),
            HashMap::new(),
        );

        super::compact_raw_state_sync_message(&mut message.message);

        assert_eq!(
            message
                .message
                .snapshots
                .states
                .keys()
                .map(String::as_str)
                .collect::<HashSet<_>>(),
            HashSet::from([surviving_component])
        );
        let deltas = message
            .message
            .deltas
            .as_ref()
            .ok_or_else(|| anyhow!("surviving entrypoints must remain"))?;
        assert!(deltas.deleted_protocol_components.is_empty());
        assert_eq!(
            deltas.dci_update.new_entrypoints,
            HashMap::from([(surviving_component.to_string(), surviving_entrypoints)])
        );
        assert_eq!(
            deltas.dci_update.new_entrypoint_params,
            HashMap::from([(surviving_entrypoint.to_string(), HashSet::new())])
        );
        assert_eq!(
            deltas.dci_update.trace_results,
            HashMap::from([(surviving_entrypoint.to_string(), Default::default())])
        );
        Ok(())
    }

    #[test]
    fn raw_cache_residue_count_does_not_grow_for_foldable_updates() {
        let component_id = "raw-1";
        let mut messages = Vec::new();
        super::merge_raw_message(
            &mut messages,
            raw_protocol_message_with_states(HashMap::from([(
                component_id.to_string(),
                raw_component_with_state(component_id, 1),
            )])),
        );

        for block_number in 11..=1_010 {
            let mut changes = BlockAggregatedChanges::default();
            changes.state_deltas.insert(
                component_id.to_string(),
                component_state_delta(
                    component_id,
                    [("version", DtoBytes::from([(block_number % 255) as u8; 32]))],
                    [],
                ),
            );
            super::merge_raw_message(
                &mut messages,
                raw_protocol_message_with_changes(
                    "uniswap_v2",
                    block_number,
                    Snapshot::default(),
                    Some(changes),
                    HashMap::new(),
                ),
            );
            assert_eq!(super::raw_residue_entry_count(&messages[0].message), 0);
        }
    }

    #[tokio::test]
    async fn raw_cache_and_export_stay_bounded_after_100_000_fixed_key_updates() -> Result<()> {
        const KEY_COUNT: usize = 16;
        const UPDATE_COUNT: u64 = 100_000;
        const SIZE_LIMIT: usize = 64 * 1024;

        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let initial_states = (0..KEY_COUNT)
            .map(|index| {
                let component_id = format!("raw-{index:02}");
                (
                    component_id.clone(),
                    raw_component_with_state(&component_id, index as u8),
                )
            })
            .collect();
        cache
            .apply_feed_message(&FeedMessage {
                state_msgs: HashMap::from([(
                    "uniswap_v2".to_string(),
                    raw_protocol_message_with_states(initial_states).message,
                )]),
                sync_states: HashMap::from([(
                    "uniswap_v2".to_string(),
                    SynchronizerState::Ready(block_header(10, 1)),
                )]),
            })
            .await?;

        let materialized_bytes = {
            let mut guard = cache.inner.write().await;
            let partition = guard
                .partitions
                .get_mut(&BroadcasterBackend::Native)
                .ok_or_else(|| anyhow!("native partition missing"))?;
            for update_index in 0..UPDATE_COUNT {
                let component_id = format!("raw-{:02}", update_index as usize % KEY_COUNT);
                let mut changes = BlockAggregatedChanges::default();
                changes.state_deltas.insert(
                    component_id.clone(),
                    component_state_delta(
                        &component_id,
                        [(
                            "version",
                            DtoBytes::from([(update_index % u64::from(u8::MAX)) as u8; 32]),
                        )],
                        [],
                    ),
                );
                super::merge_raw_message(
                    &mut partition.messages,
                    raw_protocol_message_with_changes(
                        "uniswap_v2",
                        update_index + 11,
                        Snapshot::default(),
                        Some(changes),
                        HashMap::new(),
                    ),
                );
            }

            assert_eq!(partition.messages.len(), 1);
            assert_eq!(
                partition.messages[0].message.snapshots.states.len(),
                KEY_COUNT
            );
            assert_eq!(
                super::raw_residue_entry_count(&partition.messages[0].message),
                0
            );
            serde_json::to_vec(&partition.messages)?.len()
        };
        assert!(
            materialized_bytes <= SIZE_LIMIT,
            "materialized state grew to {materialized_bytes} bytes"
        );

        let export = cache.export_snapshot(SIZE_LIMIT).await?;
        assert_payloads_at_most(&export, SIZE_LIMIT)?;
        let export_bytes = export
            .payloads
            .iter()
            .map(|payload| super::payload_size(&export.stream_id, payload))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .sum::<usize>();
        assert!(
            export_bytes <= SIZE_LIMIT,
            "snapshot export grew to {export_bytes} bytes"
        );
        Ok(())
    }

    #[tokio::test]
    async fn raw_cache_long_merge_sequence_exports_under_small_cap() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let component_id = "raw-1";
        cache
            .apply_feed_message(&FeedMessage {
                state_msgs: HashMap::from([(
                    "uniswap_v2".to_string(),
                    raw_protocol_message_with_states(HashMap::from([(
                        component_id.to_string(),
                        raw_component_with_state(component_id, 1),
                    )]))
                    .message,
                )]),
                sync_states: HashMap::from([(
                    "uniswap_v2".to_string(),
                    SynchronizerState::Ready(block_header(10, 1)),
                )]),
            })
            .await?;

        for block_number in 11..=510 {
            let mut changes = BlockAggregatedChanges::default();
            changes.state_deltas.insert(
                component_id.to_string(),
                component_state_delta(
                    component_id,
                    [("version", DtoBytes::from([(block_number % 255) as u8; 32]))],
                    [],
                ),
            );
            cache
                .apply_feed_message(&FeedMessage {
                    state_msgs: HashMap::from([(
                        "uniswap_v2".to_string(),
                        raw_protocol_message_with_changes(
                            "uniswap_v2",
                            block_number,
                            Snapshot::default(),
                            Some(changes),
                            HashMap::new(),
                        )
                        .message,
                    )]),
                    sync_states: HashMap::from([(
                        "uniswap_v2".to_string(),
                        SynchronizerState::Ready(block_header(block_number, block_number as u8)),
                    )]),
                })
                .await?;
        }

        let uncapped = cache.export_snapshot(usize::MAX).await?;
        let max_payload_bytes = first_snapshot_chunk_size(&uncapped)? + 64;
        let export = cache.export_snapshot(max_payload_bytes).await?;
        assert_payloads_at_most(&export, max_payload_bytes)?;
        let component = snapshot_chunks(&export)
            .into_iter()
            .flat_map(|chunk| &chunk.partitions)
            .flat_map(|partition| &partition.messages)
            .find_map(|message| message.message.snapshots.states.get(component_id))
            .ok_or_else(|| anyhow!("expected compacted component"))?;
        assert_eq!(
            component.state.attributes["version"],
            DtoBytes::from([(510 % 255) as u8; 32])
        );
        Ok(())
    }

    #[test]
    fn raw_cache_splits_large_delta_tail() -> Result<()> {
        let mut message = residual_tail_message(24, 384);
        for index in (0..12).rev() {
            let component_id = format!("removed-{index:04}");
            message.message.removed_components.insert(
                component_id.clone(),
                raw_component(&component_id, "uniswap_v2", index as u8),
            );
        }
        let single = residual_tail_message(1, 384);
        let sizing_ctx = snapshot_chunk_build_context(usize::MAX);
        let single_size = sizing_ctx.raw_fragment_size(single, &BTreeMap::new(), false)?;
        let ctx = snapshot_chunk_build_context(single_size + 64);

        let fragments =
            super::split_protocol_message_for_snapshot(&ctx, &message, &BTreeMap::new())?;
        let repeated =
            super::split_protocol_message_for_snapshot(&ctx, &message, &BTreeMap::new())?;

        assert!(fragments.len() > 1);
        for (index, fragment) in fragments.iter().enumerate() {
            assert!(ctx.raw_fragment_fits(fragment.clone(), &BTreeMap::new(), index == 0)?);
        }
        let mut removed_ids = fragments
            .iter()
            .flat_map(|fragment| fragment.message.removed_components.keys().cloned())
            .collect::<Vec<_>>();
        removed_ids.sort();
        assert_eq!(
            removed_ids,
            (0..12)
                .map(|index| format!("removed-{index:04}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(fragments, repeated);
        Ok(())
    }

    #[test]
    fn raw_cache_tail_preserves_unsplittable_delta_fields_and_wire_contract() -> Result<()> {
        let mut message = residual_tail_message(12, 256);
        let deltas = message
            .message
            .deltas
            .as_mut()
            .ok_or_else(|| anyhow!("expected deltas"))?;
        for index in 0..20u8 {
            let token_address = DtoBytes::from([71u8.saturating_add(index); 20]);
            deltas.new_tokens.insert(
                token_address.clone(),
                ResponseToken {
                    chain: DtoChain::Ethereum.into(),
                    address: token_address,
                    symbol: format!("TAIL{index}"),
                    decimals: 18,
                    tax: 0,
                    gas: Vec::new(),
                    quality: 100,
                }
                .into(),
            );
            deltas
                .dci_update
                .trace_results
                .insert(format!("trace-{index}"), Default::default());
        }
        let expected_new_tokens = deltas.new_tokens.clone();
        let expected_dci_update = deltas.dci_update.clone();
        let one = residual_tail_message(1, 256);
        let sizing_ctx = snapshot_chunk_build_context(usize::MAX);
        let cap = sizing_ctx.raw_fragment_size(one, &BTreeMap::new(), false)? + 512;
        let ctx = snapshot_chunk_build_context(cap);

        let fragments =
            super::split_protocol_message_for_snapshot(&ctx, &message, &BTreeMap::new())?;

        assert!(fragments.len() > 1);
        let mut actual_new_tokens = HashMap::new();
        let mut actual_trace_results = HashMap::new();
        for fragment in &fragments {
            let deltas = fragment
                .message
                .deltas
                .as_ref()
                .ok_or_else(|| anyhow!("expected delta fragment"))?;
            actual_new_tokens.extend(deltas.new_tokens.clone());
            actual_trace_results.extend(deltas.dci_update.trace_results.clone());
        }
        assert_eq!(actual_new_tokens, expected_new_tokens);
        assert_eq!(actual_trace_results, expected_dci_update.trace_results);

        for (index, fragment) in fragments.into_iter().enumerate() {
            let json = serde_json::to_vec(&fragment)?;
            let round_trip: BroadcasterProtocolMessage = serde_json::from_slice(&json)?;
            assert_eq!(round_trip, fragment);

            let payload = BroadcasterPayload::SnapshotChunk(ctx.messages_chunk(
                index,
                vec![round_trip],
                BTreeMap::new(),
            )?);
            let envelope = BroadcasterEnvelope::new("chain-1-stream-1", index as u64 + 1, payload);
            let json = serde_json::to_vec(&envelope)?;
            let round_trip: BroadcasterEnvelope = serde_json::from_slice(&json)?;
            assert_eq!(round_trip.stream_id, envelope.stream_id);
            assert_eq!(round_trip.message_seq, envelope.message_seq);
            assert_eq!(
                serde_json::to_value(&round_trip.payload)?,
                serde_json::to_value(&envelope.payload)?
            );
            assert!(matches!(
                round_trip.payload,
                BroadcasterPayload::SnapshotChunk(_)
            ));
        }
        Ok(())
    }

    #[test]
    fn raw_cache_rejects_single_indivisible_tail_item_above_cap() -> Result<()> {
        let empty = raw_protocol_message_with_changes(
            "uniswap_v2",
            10,
            Snapshot::default(),
            Some(BlockAggregatedChanges::default()),
            HashMap::new(),
        );
        let sizing_ctx = snapshot_chunk_build_context(usize::MAX);
        let cap = sizing_ctx.raw_fragment_size(empty, &BTreeMap::new(), false)? + 32;
        let ctx = snapshot_chunk_build_context(cap);
        let message = residual_tail_message(1, 8_192);

        let Err(error) =
            super::split_protocol_message_for_snapshot(&ctx, &message, &BTreeMap::new())
        else {
            return Err(anyhow!("indivisible tail item should exceed cap"));
        };

        let error = error.to_string();
        assert!(error.contains("protocol uniswap_v2"));
        assert!(error.contains("kind state_updates"));
        assert!(error.contains("missing-0000"));
        assert!(error.contains("bytes"));
        assert!(error.contains(&cap.to_string()));
        Ok(())
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
                BroadcasterSnapshotSessionsSnapshot::default(),
                std::time::Duration::from_secs(5),
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
                BroadcasterSnapshotSessionsSnapshot::default(),
                std::time::Duration::from_secs(5),
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
    async fn cache_resets_to_redis_owned_generation() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        cache.apply_update(&native_only_update()).await?;
        let snapshot_before = cache.export_snapshot(8_388_608).await?;
        assert_eq!(snapshot_before.snapshot_id, "chain-1-snapshot-1");

        cache.reset_to_generation(2).await;
        let snapshot_after = cache.export_snapshot(8_388_608).await?;
        assert_eq!(snapshot_after.stream_id, "chain-1-stream-2");
        assert_eq!(snapshot_after.snapshot_id, "chain-1-snapshot-2");
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
                BroadcasterSnapshotSessionsSnapshot::default(),
                std::time::Duration::from_secs(5),
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
                BroadcasterSnapshotSessionsSnapshot::default(),
                std::time::Duration::from_secs(5),
            )
            .await;
        assert_eq!(warming.readiness, BroadcasterReadiness::SnapshotWarmingUp);

        cache.apply_update(&native_only_update()).await?;
        upstream_state.record_update().await;
        let ready = cache
            .status_snapshot(
                500,
                upstream_state.snapshot().await,
                BroadcasterSnapshotSessionsSnapshot::default(),
                std::time::Duration::from_secs(5),
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

    fn native_update_state(block_number: u64, component_id: &str, seed: u8) -> Update {
        let mut states = HashMap::new();
        states.insert(
            component_id.to_string(),
            Box::new(DummySim(seed)) as Box<dyn ProtocolSim>,
        );

        Update::new(block_number, states, HashMap::new()).set_sync_states(HashMap::from([(
            "uniswap_v2".to_string(),
            SynchronizerState::Ready(block_header(block_number, seed)),
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
        account: Account,
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

    fn raw_protocol_message_with_changes(
        protocol: &str,
        block_number: u64,
        snapshots: Snapshot,
        deltas: Option<BlockAggregatedChanges>,
        removed_components: HashMap<String, DtoProtocolComponent>,
    ) -> BroadcasterProtocolMessage {
        let header = block_header(block_number, block_number as u8);
        BroadcasterProtocolMessage::new(
            protocol,
            SynchronizerState::Ready(header.clone()),
            StateSyncMessage {
                header,
                snapshots,
                deltas,
                removed_components,
            },
        )
    }

    fn component_state_delta(
        component_id: &str,
        attributes: impl IntoIterator<Item = (&'static str, DtoBytes)>,
        deleted_attributes: impl IntoIterator<Item = &'static str>,
    ) -> ProtocolComponentStateDelta {
        ProtocolComponentStateDelta::new(
            component_id,
            attributes
                .into_iter()
                .map(|(name, value)| (name.to_string(), value))
                .collect(),
            deleted_attributes.into_iter().map(str::to_string).collect(),
        )
    }

    fn residual_tail_message(entry_count: usize, value_size: usize) -> BroadcasterProtocolMessage {
        let mut changes = BlockAggregatedChanges::default();
        for index in (0..entry_count).rev() {
            let component_id = format!("missing-{index:04}");
            changes.state_deltas.insert(
                component_id.clone(),
                component_state_delta(
                    &component_id,
                    [("value", DtoBytes::from(vec![index as u8; value_size]))],
                    [],
                ),
            );
        }
        raw_protocol_message_with_changes(
            "uniswap_v2",
            10,
            Snapshot::default(),
            Some(changes),
            HashMap::new(),
        )
    }

    fn account_update(
        address: DtoBytes,
        change: ChangeType,
        slot_seed: u8,
        balance: Option<DtoBytes>,
    ) -> AccountDelta {
        AccountDelta::new(
            DtoChain::Ethereum,
            address,
            HashMap::from([(
                DtoBytes::from([slot_seed; 32]),
                Some(DtoBytes::from([slot_seed.saturating_add(1); 32])),
            )]),
            balance,
            None,
            change,
        )
    }

    fn component_balance(component_id: &str, token: DtoBytes, balance: u8) -> ComponentBalance {
        ComponentBalance {
            token,
            balance: DtoBytes::from([balance; 32]),
            balance_float: f64::from(balance),
            modify_tx: DtoBytes::from([balance; 32]),
            component_id: component_id.to_string(),
        }
    }

    fn account_balance(account: DtoBytes, token: DtoBytes, balance: u8) -> AccountBalance {
        AccountBalance {
            account,
            token,
            balance: DtoBytes::from([balance; 32]),
            modify_tx: DtoBytes::from([balance; 32]),
        }
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
            state: ProtocolComponentState {
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
            chain: DtoChain::Ethereum,
            tokens: vec![DtoBytes::from([seed; 20]), DtoBytes::from([seed + 1; 20])],
            contract_addresses: Vec::new(),
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
    ) -> Account {
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

        Account::new(
            DtoChain::Ethereum,
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

    fn linked_header(number: u64, hash_seed: u8, parent_seed: u8) -> BlockHeader {
        BlockHeader {
            hash: Bytes::from([hash_seed; 32]),
            number,
            parent_hash: Bytes::from([parent_seed; 32]),
            revert: false,
            timestamp: number * 10,
            partial_block_index: None,
        }
    }

    fn raw_feed(
        messages: Vec<(&str, StateSyncMessage<BlockHeader>)>,
        statuses: Vec<(&str, BlockHeader)>,
    ) -> FeedMessage<BlockHeader> {
        FeedMessage {
            state_msgs: messages
                .into_iter()
                .map(|(protocol, message)| (protocol.to_string(), message))
                .collect(),
            sync_states: statuses
                .into_iter()
                .map(|(protocol, header)| (protocol.to_string(), SynchronizerState::Ready(header)))
                .collect(),
        }
    }

    fn raw_snapshot_message(
        protocol: &str,
        header: BlockHeader,
        component_seed: u8,
    ) -> StateSyncMessage<BlockHeader> {
        let component_id = format!("{protocol}-pool");
        let mut state = raw_component_with_state(&component_id, component_seed);
        state
            .state
            .attributes
            .insert("version".to_string(), DtoBytes::from([component_seed; 32]));
        StateSyncMessage {
            header,
            snapshots: Snapshot {
                states: HashMap::from([(component_id, state)]),
                vm_storage: HashMap::new(),
            },
            deltas: None,
            removed_components: HashMap::new(),
        }
    }

    fn raw_snapshot_message_with_residue(
        protocol: &str,
        header: BlockHeader,
        component_seed: u8,
    ) -> StateSyncMessage<BlockHeader> {
        let mut message = raw_snapshot_message(protocol, header, component_seed);
        let mut changes = BlockAggregatedChanges::default();
        changes.state_deltas.insert(
            "still-unresolved".to_string(),
            component_state_delta(
                "still-unresolved",
                [("value", DtoBytes::from([99u8; 32]))],
                [],
            ),
        );
        message.deltas = Some(changes);
        message
    }

    #[tokio::test]
    async fn fresh_replacement_waits_for_aligned_protocol_candidates_before_publication(
    ) -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let block_10 = linked_header(10, 10, 9);
        let initial = raw_feed(
            vec![
                (
                    "uniswap_v2",
                    raw_snapshot_message("uniswap_v2", block_10.clone(), 1),
                ),
                (
                    "uniswap_v3",
                    raw_snapshot_message("uniswap_v3", block_10.clone(), 2),
                ),
            ],
            vec![
                ("uniswap_v2", block_10.clone()),
                ("uniswap_v3", block_10.clone()),
            ],
        );
        assert!(cache.apply_feed_message(&initial).await?.is_some());
        let before = serde_json::to_value(cache.export_snapshot(8_388_608).await?.payloads)?;
        cache.begin_same_generation_recovery().await;

        let block_12 = linked_header(12, 12, 11);
        let first_replacement = raw_feed(
            vec![(
                "uniswap_v2",
                raw_snapshot_message("uniswap_v2", block_12.clone(), 9),
            )],
            vec![
                ("uniswap_v2", block_12.clone()),
                ("uniswap_v3", block_10.clone()),
            ],
        );
        assert!(cache
            .apply_feed_message(&first_replacement)
            .await?
            .is_none());
        assert_eq!(
            serde_json::to_value(cache.export_snapshot(8_388_608).await?.payloads)?,
            before
        );

        let second_replacement = raw_feed(
            vec![(
                "uniswap_v3",
                raw_snapshot_message("uniswap_v3", block_12.clone(), 2),
            )],
            vec![
                ("uniswap_v2", block_12.clone()),
                ("uniswap_v3", block_12.clone()),
            ],
        );
        assert!(
            cache
                .apply_feed_message(&second_replacement)
                .await?
                .is_none(),
            "aligned replacement publication is owned by the service recovery worker"
        );
        let after = cache.export_snapshot(8_388_608).await?;
        assert!(after.payloads.iter().any(|payload| match payload {
            BroadcasterPayload::SnapshotChunk(chunk) => chunk
                .partitions
                .iter()
                .flat_map(|partition| &partition.messages)
                .any(|message| message.message.header == block_12),
            _ => false,
        }));
        Ok(())
    }

    #[tokio::test]
    async fn reconnect_mid_recovery_discards_stale_candidates_and_aligns_from_fresh_snapshot(
    ) -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let block_10 = linked_header(10, 10, 9);
        cache
            .apply_feed_message(&raw_feed(
                vec![
                    (
                        "uniswap_v2",
                        raw_snapshot_message("uniswap_v2", block_10.clone(), 1),
                    ),
                    (
                        "uniswap_v3",
                        raw_snapshot_message("uniswap_v3", block_10.clone(), 2),
                    ),
                ],
                vec![
                    ("uniswap_v2", block_10.clone()),
                    ("uniswap_v3", block_10.clone()),
                ],
            ))
            .await?;
        cache.begin_same_generation_recovery().await;

        let block_12 = linked_header(12, 12, 11);
        let first_replacement = raw_feed(
            vec![(
                "uniswap_v2",
                raw_snapshot_message("uniswap_v2", block_12.clone(), 9),
            )],
            vec![("uniswap_v2", block_12), ("uniswap_v3", block_10.clone())],
        );
        assert!(cache
            .apply_feed_message(&first_replacement)
            .await?
            .is_none());
        assert!(cache.replacement_pending().await);

        cache.begin_same_generation_recovery().await;
        let block_15 = linked_header(15, 15, 14);
        let fresh_replacement = raw_feed(
            vec![
                (
                    "uniswap_v2",
                    raw_snapshot_message("uniswap_v2", block_15.clone(), 11),
                ),
                (
                    "uniswap_v3",
                    raw_snapshot_message("uniswap_v3", block_15.clone(), 12),
                ),
            ],
            vec![
                ("uniswap_v2", block_15.clone()),
                ("uniswap_v3", block_15.clone()),
            ],
        );
        let fresh_result = cache.apply_feed_message(&fresh_replacement).await;
        assert!(
            fresh_result.is_ok(),
            "fresh Tycho replacement must align after reconnect: {fresh_result:?}"
        );
        assert!(fresh_result?.is_none());
        assert!(!cache.replacement_pending().await);
        let snapshot = cache.export_snapshot(8_388_608).await?;
        assert!(snapshot.payloads.iter().any(|payload| match payload {
            BroadcasterPayload::SnapshotChunk(chunk) => chunk
                .partitions
                .iter()
                .flat_map(|partition| &partition.messages)
                .any(|message| message.message.header == block_15),
            _ => false,
        }));
        Ok(())
    }

    #[tokio::test]
    async fn rearming_recovery_supersedes_pending_replacement_with_new_id() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let block_10 = linked_header(10, 10, 9);
        cache
            .apply_feed_message(&raw_feed(
                vec![(
                    "uniswap_v2",
                    raw_snapshot_message("uniswap_v2", block_10.clone(), 1),
                )],
                vec![("uniswap_v2", block_10)],
            ))
            .await?;

        cache.begin_same_generation_recovery().await;
        let first_status = cache
            .status_snapshot(
                8_388_608,
                connected_upstream().await,
                BroadcasterSnapshotSessionsSnapshot::default(),
                std::time::Duration::from_secs(5),
            )
            .await;
        assert!(first_status.snapshot.recovery_pending);
        assert_eq!(first_status.snapshot.recovery_id, Some(1));

        cache.begin_same_generation_recovery().await;
        let second_status = cache
            .status_snapshot(
                8_388_608,
                connected_upstream().await,
                BroadcasterSnapshotSessionsSnapshot::default(),
                std::time::Duration::from_secs(5),
            )
            .await;
        assert!(second_status.snapshot.recovery_pending);
        assert_eq!(second_status.snapshot.recovery_id, Some(2));
        Ok(())
    }

    #[tokio::test]
    async fn disconnect_during_from_current_recovery_discards_seeded_candidates() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let block_10 = linked_header(10, 10, 9);
        cache
            .apply_feed_message(&raw_feed(
                vec![
                    (
                        "uniswap_v2",
                        raw_snapshot_message("uniswap_v2", block_10.clone(), 1),
                    ),
                    (
                        "uniswap_v3",
                        raw_snapshot_message("uniswap_v3", block_10.clone(), 2),
                    ),
                ],
                vec![("uniswap_v2", block_10.clone()), ("uniswap_v3", block_10)],
            ))
            .await?;
        cache.begin_same_generation_recovery_from_current().await;
        cache.begin_same_generation_recovery().await;

        let block_15 = linked_header(15, 15, 14);
        let fresh_replacement = raw_feed(
            vec![
                (
                    "uniswap_v2",
                    raw_snapshot_message("uniswap_v2", block_15.clone(), 11),
                ),
                (
                    "uniswap_v3",
                    raw_snapshot_message("uniswap_v3", block_15.clone(), 12),
                ),
            ],
            vec![("uniswap_v2", block_15.clone()), ("uniswap_v3", block_15)],
        );
        let fresh_result = cache.apply_feed_message(&fresh_replacement).await;
        assert!(
            fresh_result.is_ok(),
            "fresh Tycho replacement must discard seeded candidates: {fresh_result:?}"
        );
        assert!(fresh_result?.is_none());
        assert!(!cache.replacement_pending().await);
        Ok(())
    }

    #[tokio::test]
    async fn fresh_replacement_rejects_partial_block_candidate() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let block_10 = linked_header(10, 10, 9);
        cache
            .apply_feed_message(&raw_feed(
                vec![(
                    "uniswap_v2",
                    raw_snapshot_message("uniswap_v2", block_10.clone(), 1),
                )],
                vec![("uniswap_v2", block_10)],
            ))
            .await?;
        let before = serde_json::to_value(cache.export_snapshot(8_388_608).await?.payloads)?;
        cache.begin_same_generation_recovery().await;

        let mut partial_block_12 = linked_header(12, 12, 11);
        partial_block_12.partial_block_index = Some(0);
        let partial_replacement = raw_feed(
            vec![(
                "uniswap_v2",
                raw_snapshot_message("uniswap_v2", partial_block_12.clone(), 2),
            )],
            vec![("uniswap_v2", partial_block_12)],
        );
        assert!(cache
            .apply_feed_message(&partial_replacement)
            .await?
            .is_none());
        assert!(cache.replacement_pending().await);
        assert_eq!(
            serde_json::to_value(cache.export_snapshot(8_388_608).await?.payloads)?,
            before
        );
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the five protocol fixture keeps the reconnect sequence visible"
    )]
    async fn base_shaped_recovery_aligns_five_protocols_with_one_contiguous_subscription(
    ) -> Result<()> {
        const PROTOCOLS: [&str; 5] = [
            "uniswap_v2",
            "uniswap_v3",
            "uniswap_v4",
            "pancakeswap_v3",
            "aerodrome_slipstreams",
        ];
        let cache = BroadcasterSnapshotCache::new(8453, vec![BroadcasterBackend::Native]);
        let clean = BroadcasterSnapshotCache::new(8453, vec![BroadcasterBackend::Native]);
        let block_10 = linked_header(10, 10, 9);
        let block_12 = linked_header(12, 12, 11);
        let initial = raw_feed(
            PROTOCOLS
                .iter()
                .enumerate()
                .map(|(index, protocol)| {
                    (
                        *protocol,
                        if index == 0 {
                            raw_snapshot_message_with_residue(
                                protocol,
                                block_10.clone(),
                                index as u8 + 1,
                            )
                        } else {
                            raw_snapshot_message(protocol, block_10.clone(), index as u8 + 1)
                        },
                    )
                })
                .collect(),
            PROTOCOLS
                .iter()
                .map(|protocol| (*protocol, block_10.clone()))
                .collect(),
        );
        cache.apply_feed_message(&initial).await?;
        cache.begin_same_generation_recovery().await;

        let first = raw_feed(
            vec![(
                PROTOCOLS[0],
                raw_snapshot_message_with_residue(PROTOCOLS[0], block_12.clone(), 9),
            )],
            vec![
                (PROTOCOLS[0], block_12.clone()),
                (PROTOCOLS[1], block_10.clone()),
                (PROTOCOLS[2], block_10.clone()),
                (PROTOCOLS[3], block_10.clone()),
                (PROTOCOLS[4], block_10.clone()),
            ],
        );
        assert!(cache.apply_feed_message(&first).await?.is_none());

        let second = raw_feed(
            vec![
                (
                    PROTOCOLS[1],
                    raw_snapshot_message(PROTOCOLS[1], block_12.clone(), 2),
                ),
                (
                    PROTOCOLS[2],
                    raw_snapshot_message(PROTOCOLS[2], block_12.clone(), 3),
                ),
            ],
            vec![
                (PROTOCOLS[0], block_12.clone()),
                (PROTOCOLS[1], block_12.clone()),
                (PROTOCOLS[2], block_12.clone()),
                (PROTOCOLS[3], block_10.clone()),
                (PROTOCOLS[4], block_10.clone()),
            ],
        );
        assert!(cache.apply_feed_message(&second).await?.is_none());

        let third = raw_feed(
            vec![
                (
                    PROTOCOLS[3],
                    raw_snapshot_message(PROTOCOLS[3], block_12.clone(), 4),
                ),
                (
                    PROTOCOLS[4],
                    raw_snapshot_message(PROTOCOLS[4], block_12.clone(), 5),
                ),
            ],
            PROTOCOLS
                .iter()
                .map(|protocol| (*protocol, block_12.clone()))
                .collect(),
        );
        assert!(
            cache.apply_feed_message(&third).await?.is_none(),
            "aligned replacement is published by the service recovery worker"
        );

        let clean_feed = raw_feed(
            PROTOCOLS
                .iter()
                .enumerate()
                .map(|(index, protocol)| {
                    let seed = if index == 0 { 9 } else { index as u8 + 1 };
                    (
                        *protocol,
                        if index == 0 {
                            raw_snapshot_message_with_residue(protocol, block_12.clone(), seed)
                        } else {
                            raw_snapshot_message(protocol, block_12.clone(), seed)
                        },
                    )
                })
                .collect(),
            PROTOCOLS
                .iter()
                .map(|protocol| (*protocol, block_12.clone()))
                .collect(),
        );
        clean.apply_feed_message(&clean_feed).await?;
        assert_eq!(
            serde_json::to_value(cache.export_snapshot(8_388_608).await?.payloads)?,
            serde_json::to_value(clean.export_snapshot(8_388_608).await?.payloads)?
        );
        Ok(())
    }

    #[tokio::test]
    async fn header_gap_requires_reconnect_without_mutating_cache() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let block_10 = linked_header(10, 10, 9);
        let initial = raw_feed(
            vec![(
                "uniswap_v2",
                raw_snapshot_message("uniswap_v2", block_10.clone(), 1),
            )],
            vec![("uniswap_v2", block_10)],
        );
        cache.apply_feed_message(&initial).await?;
        let before = serde_json::to_value(cache.export_snapshot(8_388_608).await?.payloads)?;
        let block_12 = linked_header(12, 12, 11);
        let incomplete = raw_feed(
            vec![(
                "uniswap_v2",
                StateSyncMessage {
                    header: block_12.clone(),
                    snapshots: Snapshot {
                        states: HashMap::from([(
                            "different-pool".to_string(),
                            raw_component_with_state("different-pool", 7),
                        )]),
                        vm_storage: HashMap::new(),
                    },
                    deltas: None,
                    removed_components: HashMap::new(),
                },
            )],
            vec![("uniswap_v2", block_12)],
        );
        let Err(error) = cache.apply_feed_message(&incomplete).await else {
            return Err(anyhow!("header gap should require a reconnect"));
        };
        assert!(error
            .to_string()
            .contains("requires reconnect and a fresh authoritative replacement"));
        assert_eq!(
            serde_json::to_value(cache.export_snapshot(8_388_608).await?.payloads)?,
            before
        );
        Ok(())
    }

    #[tokio::test]
    async fn normal_raw_staging_keeps_only_the_compact_mutation_after_large_bootstrap() -> Result<()>
    {
        let cache = BroadcasterSnapshotCache::new(8453, vec![BroadcasterBackend::Native]);
        let block_10 = linked_header(10, 10, 9);
        let states = (0..5_900)
            .map(|index| {
                let component_id = format!("pool-{index:04}");
                (
                    component_id.clone(),
                    raw_component_with_state(&component_id, (index % 251) as u8),
                )
            })
            .collect();
        cache
            .apply_feed_message(&raw_feed(
                vec![(
                    "uniswap_v2",
                    StateSyncMessage {
                        header: block_10.clone(),
                        snapshots: Snapshot {
                            states,
                            vm_storage: HashMap::new(),
                        },
                        deltas: None,
                        removed_components: HashMap::new(),
                    },
                )],
                vec![("uniswap_v2", block_10)],
            ))
            .await?;

        let block_11 = linked_header(11, 11, 10);
        let mut changes = BlockAggregatedChanges::default();
        changes.state_deltas.insert(
            "pool-0001".to_string(),
            component_state_delta("pool-0001", [("value", DtoBytes::from([77u8; 32]))], []),
        );
        let staged = cache
            .stage_feed_message(&raw_feed(
                vec![(
                    "uniswap_v2",
                    StateSyncMessage {
                        header: block_11.clone(),
                        snapshots: Snapshot::default(),
                        deltas: Some(changes),
                        removed_components: HashMap::new(),
                    },
                )],
                vec![("uniswap_v2", block_11)],
            ))
            .await?;

        assert!(matches!(
            &staged.apply_mode,
            super::BroadcasterStagedUpdateApplyMode::RawNormal { .. }
        ));
        assert!(
            serde_json::to_vec(staged.message().ok_or_else(|| anyhow!("missing update"))?)?.len()
                < 10_000
        );
        Ok(())
    }

    #[tokio::test]
    async fn raw_cache_unions_sparse_shared_vm_accounts_at_same_block() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Vm]);
        let block = linked_header(10, 10, 9);
        let address = DtoBytes::from([43u8; 20]);
        let first_slot = DtoBytes::from([1u8; 32]);
        let second_slot = DtoBytes::from([2u8; 32]);
        let first_token = DtoBytes::from([3u8; 20]);
        let second_token = DtoBytes::from([4u8; 20]);
        let mut first = raw_response_account(address.clone(), 0, 0);
        first.title = "Curve pool token".to_string();
        first
            .slots
            .insert(first_slot.clone(), DtoBytes::from([11u8; 32]));
        first.token_balances.insert(
            first_token.clone(),
            account_balance(address.clone(), first_token.clone(), 12),
        );
        let mut second = first.clone();
        second.title = "Balancer token".to_string();
        second.slots = HashMap::from([(second_slot.clone(), DtoBytes::from([21u8; 32]))]);
        second.token_balances = HashMap::from([(
            second_token.clone(),
            account_balance(address.clone(), second_token.clone(), 22),
        )]);
        let message = |account| StateSyncMessage {
            header: block.clone(),
            snapshots: Snapshot {
                states: HashMap::new(),
                vm_storage: HashMap::from([(address.clone(), account)]),
            },
            deltas: None,
            removed_components: HashMap::new(),
        };

        cache
            .apply_feed_message(&raw_feed(
                vec![
                    ("vm:curve", message(first)),
                    ("vm:balancer_v2", message(second)),
                ],
                vec![("vm:curve", block.clone()), ("vm:balancer_v2", block)],
            ))
            .await?;

        let export = cache.export_snapshot(8_388_608).await?;
        let accounts = vm_account_fragments(&export, &address);
        assert_eq!(accounts.len(), 2);
        for account in accounts {
            assert_eq!(account.title, "Balancer token");
            assert_eq!(account.slots.len(), 2);
            assert_eq!(
                account.slots.get(&first_slot),
                Some(&DtoBytes::from([11u8; 32]))
            );
            assert_eq!(
                account.slots.get(&second_slot),
                Some(&DtoBytes::from([21u8; 32]))
            );
            assert_eq!(account.token_balances.len(), 2);
            assert_eq!(
                account
                    .token_balances
                    .get(&first_token)
                    .map(|balance| &balance.balance),
                Some(&DtoBytes::from([12u8; 32]))
            );
            assert_eq!(
                account
                    .token_balances
                    .get(&second_token)
                    .map(|balance| &balance.balance),
                Some(&DtoBytes::from([22u8; 32]))
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn raw_cache_uses_protocol_order_for_conflicting_snapshot_slots() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Vm]);
        let block = linked_header(10, 10, 9);
        let address = DtoBytes::from([44u8; 20]);
        let slot = DtoBytes::from([1u8; 32]);
        let mut curve = raw_response_account(address.clone(), 0, 0);
        curve.slots.insert(slot.clone(), DtoBytes::from([31u8; 32]));
        let mut balancer = curve.clone();
        balancer
            .slots
            .insert(slot.clone(), DtoBytes::from([21u8; 32]));
        let message = |account| StateSyncMessage {
            header: block.clone(),
            snapshots: Snapshot {
                states: HashMap::new(),
                vm_storage: HashMap::from([(address.clone(), account)]),
            },
            deltas: None,
            removed_components: HashMap::new(),
        };

        cache
            .apply_feed_message(&raw_feed(
                vec![
                    ("vm:curve", message(curve)),
                    ("vm:balancer_v2", message(balancer)),
                ],
                vec![("vm:curve", block.clone()), ("vm:balancer_v2", block)],
            ))
            .await?;

        let export = cache.export_snapshot(8_388_608).await?;
        for account in vm_account_fragments(&export, &address) {
            assert_eq!(
                account.slots.get(&slot),
                Some(&DtoBytes::from([21u8; 32])),
                "lexicographically first protocol owns overlapping snapshot values"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn raw_cache_rejects_conflicting_shared_vm_account_at_same_block() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Vm]);
        let block = linked_header(10, 10, 9);
        let address = DtoBytes::from([44u8; 20]);
        let mut first = raw_response_account(address.clone(), 1, 32);
        let mut second = first.clone();
        first.native_balance = DtoBytes::from([1u8; 32]);
        second.native_balance = DtoBytes::from([2u8; 32]);
        let message = |account| StateSyncMessage {
            header: block.clone(),
            snapshots: Snapshot {
                states: HashMap::new(),
                vm_storage: HashMap::from([(address.clone(), account)]),
            },
            deltas: None,
            removed_components: HashMap::new(),
        };
        let feed = raw_feed(
            vec![
                ("vm:curve", message(first)),
                ("vm:balancer_v2", message(second)),
            ],
            vec![("vm:curve", block.clone()), ("vm:balancer_v2", block)],
        );
        let Err(error) = cache.apply_feed_message(&feed).await else {
            return Err(anyhow!("same-block account conflict should fail staging"));
        };
        assert!(error.to_string().contains("conflicting VM account"));
        assert!(!cache.is_ready().await);
        Ok(())
    }

    #[tokio::test]
    async fn raw_cache_resolves_staggered_vm_delta_overlap_by_protocol_order() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Vm]);
        let block_10 = linked_header(10, 10, 9);
        let block_11 = linked_header(11, 11, 10);
        let address = DtoBytes::from([45u8; 20]);
        let account = raw_response_account(address.clone(), 1, 32);
        let snapshot_message = || StateSyncMessage {
            header: block_10.clone(),
            snapshots: Snapshot {
                states: HashMap::new(),
                vm_storage: HashMap::from([(address.clone(), account.clone())]),
            },
            deltas: None,
            removed_components: HashMap::new(),
        };
        cache
            .apply_feed_message(&raw_feed(
                vec![
                    ("vm:curve", snapshot_message()),
                    ("vm:balancer_v2", snapshot_message()),
                ],
                vec![
                    ("vm:curve", block_10.clone()),
                    ("vm:balancer_v2", block_10.clone()),
                ],
            ))
            .await?;

        let delta_message = |slot_value| {
            let mut update = account_update(address.clone(), ChangeType::Update, 7, None);
            update.slots = HashMap::from([(
                DtoBytes::from([7u8; 32]),
                Some(DtoBytes::from([slot_value; 32])),
            )]);
            StateSyncMessage {
                header: block_11.clone(),
                snapshots: Snapshot::default(),
                deltas: Some(BlockAggregatedChanges {
                    account_deltas: HashMap::from([(address.clone(), update)]),
                    ..Default::default()
                }),
                removed_components: HashMap::new(),
            }
        };
        assert!(cache
            .apply_feed_message(&raw_feed(
                vec![("vm:curve", delta_message(8))],
                vec![("vm:curve", block_11.clone()), ("vm:balancer_v2", block_10),],
            ))
            .await?
            .is_some());
        let overlapping = raw_feed(
            vec![("vm:balancer_v2", delta_message(9))],
            vec![("vm:curve", block_11.clone()), ("vm:balancer_v2", block_11)],
        );
        assert!(cache.apply_feed_message(&overlapping).await?.is_some());

        let slot = DtoBytes::from([7u8; 32]);
        let export = cache.export_snapshot(8_388_608).await?;
        for account in vm_account_fragments(&export, &address) {
            assert_eq!(
                account.slots.get(&slot),
                Some(&DtoBytes::from([9u8; 32])),
                "balancer sorts before curve and owns the shared delta value"
            );
        }
        Ok(())
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
}
