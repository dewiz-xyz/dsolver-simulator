use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::keccak256;
use anyhow::{Context, Result};
use futures::FutureExt;
use state_history::{CheckpointKind, StreamPosition};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::warn;
use tycho_simulation::{
    protocol::models::Update as TychoUpdate,
    tycho_client::feed::{BlockHeader, FeedMessage},
};

use crate::broadcaster::redis_publisher::{
    format_redis_snapshot_id, format_redis_stream_id, BroadcasterDeploymentPhase,
    BroadcasterRecoveryTransaction, BroadcasterRedisPublisher, BroadcasterRedisPublisherMode,
    OversizedRedisEntry, RecoveryCommitAdoptionUncertain, RecoveryPublicationCancelled,
    STARTUP_HANDOFF_ABORT_AFTER,
};
use crate::broadcaster::state::{
    combine_snapshot_exports, BroadcasterReadiness, BroadcasterRecoverySource,
    BroadcasterRecoveryStatus, BroadcasterSnapshotCache, BroadcasterSnapshotExport,
    BroadcasterSnapshotSessionsSnapshot, BroadcasterStatusSnapshot, BroadcasterUpstreamState,
};
use crate::metrics::{emit_broadcaster_recovery_outcome, emit_broadcaster_snapshot_export_failure};
use simulator_core::broadcaster::{
    BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterPayload,
    BroadcasterRedisReplayBoundary, BroadcasterSnapshotSessionResponse,
    BROADCASTER_SNAPSHOT_ENVELOPE_MAX_BYTES,
};

const SNAPSHOT_BOUNDARY_RETENTION_CACHE_TTL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotSessionError {
    NotFound,
    Expired,
    PayloadOutOfRange,
    Busy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionCloseReason {
    Expired,
    WriterFenceLost,
}

impl SessionCloseReason {
    const fn label(self) -> &'static str {
        match self {
            Self::Expired => "expired",
            Self::WriterFenceLost => "writer_fence_lost",
        }
    }
}

#[derive(Debug, Clone)]
struct BroadcasterSnapshotSessionRegistry {
    next_session_id: Arc<AtomicU64>,
    last_error: Arc<Mutex<Option<String>>>,
    pending_sessions: Arc<Mutex<HashMap<u64, PendingSnapshotSession>>>,
    fetch_permits: Arc<Semaphore>,
}

#[derive(Debug)]
struct PendingSnapshotSession {
    artifact: Arc<BroadcasterSnapshotArtifact>,
    expires_at: Instant,
}

impl PendingSnapshotSession {
    fn is_expired(&self, now: Instant) -> bool {
        now >= self.expires_at
    }
}

impl BroadcasterSnapshotSessionRegistry {
    fn new() -> Self {
        Self {
            next_session_id: Arc::new(AtomicU64::new(1)),
            last_error: Arc::new(Mutex::new(None)),
            pending_sessions: Arc::new(Mutex::new(HashMap::new())),
            fetch_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_SNAPSHOT_FETCHES)),
        }
    }

    async fn create_snapshot_session(
        &self,
        artifact: Arc<BroadcasterSnapshotArtifact>,
        ttl: Duration,
    ) -> Result<BroadcasterSnapshotSessionResponse> {
        let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        let expires_in_ms = ttl.as_millis().try_into().unwrap_or(u64::MAX);
        let expires_at = Instant::now() + ttl;
        let mut sessions = self.pending_sessions.lock().await;
        sessions.retain(|_, session| !session.is_expired(Instant::now()));
        anyhow::ensure!(
            sessions.len() < MAX_SNAPSHOT_SESSIONS,
            "snapshot session capacity reached"
        );
        let mut retained_artifacts = HashSet::new();
        let mut retained_bytes = 0usize;
        for session in sessions.values() {
            let identity = Arc::as_ptr(&session.artifact) as usize;
            if retained_artifacts.insert(identity) {
                retained_bytes = retained_bytes.saturating_add(session.artifact.encoded_bytes);
            }
        }
        let artifact_identity = Arc::as_ptr(&artifact) as usize;
        if retained_artifacts.insert(artifact_identity) {
            retained_bytes = retained_bytes.saturating_add(artifact.encoded_bytes);
        }
        anyhow::ensure!(
            retained_bytes <= MAX_RETAINED_SNAPSHOT_BYTES,
            "snapshot artifact retention capacity reached"
        );
        sessions.insert(
            session_id,
            PendingSnapshotSession {
                artifact: Arc::clone(&artifact),
                expires_at,
            },
        );
        drop(sessions);

        Ok(BroadcasterSnapshotSessionResponse {
            chain_id: artifact.chain_id,
            session_id,
            stream_id: artifact.stream_id.clone(),
            snapshot_id: artifact.snapshot_id.clone(),
            redis_replay_boundary: artifact.redis_replay_boundary.clone(),
            payload_count: artifact.payloads.len() as u32,
            snapshot_chunk_count: artifact.snapshot_chunk_count,
            expires_in_ms,
        })
    }

    async fn snapshot_payload(
        &self,
        session_id: u64,
        index: u32,
    ) -> Result<BroadcasterEnvelope, SnapshotSessionError> {
        let _permit = self
            .fetch_permits
            .try_acquire()
            .map_err(|_| SnapshotSessionError::Busy)?;
        {
            let now = Instant::now();
            let mut guard = self.pending_sessions.lock().await;
            let Some(session) = guard.get(&session_id) else {
                return Err(SnapshotSessionError::NotFound);
            };
            if session.is_expired(now) {
                guard.remove(&session_id);
            } else {
                let Some(envelope) = session.artifact.payloads.get(index as usize).cloned() else {
                    return Err(SnapshotSessionError::PayloadOutOfRange);
                };
                return Ok(envelope);
            }
        }

        self.record_session_closed(session_id, SessionCloseReason::Expired)
            .await;
        Err(SnapshotSessionError::Expired)
    }

    async fn cleanup_expired_snapshot_sessions(&self) {
        let now = Instant::now();
        let expired = {
            let mut guard = self.pending_sessions.lock().await;
            let expired = guard
                .iter()
                .filter_map(|(session_id, session)| session.is_expired(now).then_some(*session_id))
                .collect::<Vec<_>>();
            for session_id in &expired {
                guard.remove(session_id);
            }
            expired
        };

        for session_id in expired {
            self.record_session_closed(session_id, SessionCloseReason::Expired)
                .await;
        }
    }

    async fn disconnect_all(&self, reason: SessionCloseReason) {
        self.pending_sessions.lock().await.clear();

        self.record_last_error(format!("all snapshot sessions closed: {}", reason.label()))
            .await;
    }

    async fn snapshot(&self) -> BroadcasterSnapshotSessionsSnapshot {
        BroadcasterSnapshotSessionsSnapshot {
            active: self.pending_sessions.lock().await.len(),
            last_error: self.last_error.lock().await.clone(),
        }
    }

    async fn record_session_closed(&self, session_id: u64, reason: SessionCloseReason) {
        self.record_last_error(format!(
            "snapshot session {session_id} closed: {}",
            reason.label()
        ))
        .await;
    }

    async fn record_last_error(&self, message: String) {
        *self.last_error.lock().await = Some(message);
    }
}

const MAX_SNAPSHOT_SESSIONS: usize = 128;
const MAX_CONCURRENT_SNAPSHOT_FETCHES: usize = 64;
const MAX_RETAINED_SNAPSHOT_BYTES: usize = 512 * 1024 * 1024;
const RECOVERY_WARN_AFTER: Duration = Duration::from_secs(30);
const RECOVERY_ABORT_AFTER: Duration = Duration::from_secs(60);
const RECOVERY_MAX_LOCAL_FAILURES: u8 = 1;
const DEFAULT_RECOVERY_RETRY_BACKOFF: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryPublicationPhase {
    Idle,
    Building,
    Publishing,
    Draining,
    Fatal,
}

impl RecoveryPublicationPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Building => "building",
            Self::Publishing => "publishing",
            Self::Draining => "draining",
            Self::Fatal => "fatal",
        }
    }

    const fn is_active(self) -> bool {
        matches!(self, Self::Building | Self::Publishing | Self::Draining)
    }
}

#[derive(Debug)]
struct RecoveryPublicationState {
    work_id: u64,
    phase: RecoveryPublicationPhase,
    local_failures: u8,
    started_at: Instant,
    warned: bool,
    complete_native_blocks: HashSet<u64>,
    buffer_a: Vec<simulator_core::broadcaster::BroadcasterUpdateMessage>,
    buffer_a_oldest_at: Option<Instant>,
    cancelled: Arc<AtomicBool>,
    publisher_recovery_id: Option<String>,
    fatal_error: Option<String>,
    max_buffer_a_count: usize,
    max_buffer_b_count: usize,
    last_outcome: Option<&'static str>,
    last_duration_ms: Option<u64>,
    last_encoded_bytes: Option<usize>,
    last_chunk_count: Option<usize>,
}

struct RecoveryPublicationFinish {
    complete_native_block: Option<u64>,
}

type RecoveryAbortMetrics = (u64, usize, usize, usize, usize);

impl Default for RecoveryPublicationState {
    fn default() -> Self {
        Self {
            work_id: 0,
            phase: RecoveryPublicationPhase::Idle,
            local_failures: 0,
            started_at: Instant::now(),
            warned: false,
            complete_native_blocks: HashSet::new(),
            buffer_a: Vec::new(),
            buffer_a_oldest_at: None,
            cancelled: Arc::new(AtomicBool::new(false)),
            publisher_recovery_id: None,
            fatal_error: None,
            max_buffer_a_count: 0,
            max_buffer_b_count: 0,
            last_outcome: None,
            last_duration_ms: None,
            last_encoded_bytes: None,
            last_chunk_count: None,
        }
    }
}

#[derive(Debug)]
struct BroadcasterSnapshotArtifact {
    chain_id: u64,
    stream_id: String,
    snapshot_id: String,
    redis_replay_boundary: BroadcasterRedisReplayBoundary,
    payloads: Arc<Vec<BroadcasterEnvelope>>,
    snapshot_chunk_count: u32,
    encoded_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SnapshotBoundaryRetention {
    Retained,
    NotRetained,
    InspectionFailed(String),
}

impl SnapshotBoundaryRetention {
    pub(crate) fn is_retained(&self) -> bool {
        matches!(self, Self::Retained)
    }
}

#[derive(Debug, Clone)]
struct CachedSnapshotBoundaryRetention {
    boundary: BroadcasterRedisReplayBoundary,
    checked_at: Instant,
    verdict: SnapshotBoundaryRetention,
}

#[derive(Debug)]
struct BroadcasterSnapshotCandidate {
    chain_id: u64,
    export: BroadcasterSnapshotExport,
    base_heads: Vec<BroadcasterBackendHead>,
    captured_recovery_work_ids: Vec<u64>,
    handoff_id: u64,
    snapshot_chunk_count: u32,
    encoded_bytes_upper_bound: usize,
    largest_payload_bytes_upper_bound: usize,
    payload_digests: Vec<String>,
}

#[derive(Debug)]
struct StartupCandidateCapture {
    chain_id: u64,
    handoff_id: u64,
    captured: Vec<(
        BroadcasterSnapshotCache,
        crate::broadcaster::state::BroadcasterSnapshotSource,
        usize,
    )>,
    base_heads: Vec<BroadcasterBackendHead>,
    recovery_work_ids: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct BroadcasterServiceState {
    snapshot_max_payload_bytes: usize,
    cache: BroadcasterSnapshotCache,
    upstream: BroadcasterUpstreamState,
    snapshot_sessions: BroadcasterSnapshotSessionRegistry,
    snapshot_preflight: Arc<RwLock<BroadcasterSnapshotPreflightState>>,
    snapshot_artifact: Arc<RwLock<Option<Arc<BroadcasterSnapshotArtifact>>>>,
    snapshot_boundary_retention: Arc<Mutex<Option<CachedSnapshotBoundaryRetention>>>,
    snapshot_refresh_active: Arc<AtomicBool>,
    recovery_publication: Arc<Mutex<RecoveryPublicationState>>,
    recovery_workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    recovery_monitors: Arc<Mutex<Vec<JoinHandle<()>>>>,
    recovery_retry_backoff: Duration,
    native_progress_lease: Duration,
    next_recovery_source_id: Arc<AtomicU64>,
    redis_publisher: Arc<BroadcasterRedisPublisher>,
    // This gate keeps snapshot export and recovery source capture atomic with
    // respect to accepted updates and replay-boundary capture.
    lifecycle_gate: Arc<Mutex<()>>,
}

#[derive(Debug, Default)]
struct BroadcasterSnapshotPreflightState {
    checked_at: Option<Instant>,
    succeeded_at: Option<Instant>,
    duration_ms: Option<u64>,
    payload_count: Option<usize>,
    largest_payload_bytes: Option<usize>,
    last_error: Option<String>,
}

impl BroadcasterServiceState {
    pub fn with_lifecycle_gate(
        snapshot_max_payload_bytes: usize,
        cache: BroadcasterSnapshotCache,
        upstream: BroadcasterUpstreamState,
        redis_publisher: Arc<BroadcasterRedisPublisher>,
        lifecycle_gate: Arc<Mutex<()>>,
    ) -> Self {
        Self::with_lifecycle_gate_and_recovery_backoff(
            snapshot_max_payload_bytes,
            cache,
            upstream,
            redis_publisher,
            lifecycle_gate,
            DEFAULT_RECOVERY_RETRY_BACKOFF,
        )
    }

    pub fn with_lifecycle_gate_and_recovery_backoff(
        snapshot_max_payload_bytes: usize,
        cache: BroadcasterSnapshotCache,
        upstream: BroadcasterUpstreamState,
        redis_publisher: Arc<BroadcasterRedisPublisher>,
        lifecycle_gate: Arc<Mutex<()>>,
        recovery_retry_backoff: Duration,
    ) -> Self {
        Self::with_lifecycle_gate_and_recovery_backoff_and_lease(
            snapshot_max_payload_bytes,
            cache,
            upstream,
            redis_publisher,
            lifecycle_gate,
            recovery_retry_backoff,
            Duration::from_secs(5),
        )
    }

    pub fn with_lifecycle_gate_and_recovery_backoff_and_lease(
        snapshot_max_payload_bytes: usize,
        cache: BroadcasterSnapshotCache,
        upstream: BroadcasterUpstreamState,
        redis_publisher: Arc<BroadcasterRedisPublisher>,
        lifecycle_gate: Arc<Mutex<()>>,
        recovery_retry_backoff: Duration,
        native_progress_lease: Duration,
    ) -> Self {
        Self {
            snapshot_max_payload_bytes,
            cache,
            upstream,
            snapshot_sessions: BroadcasterSnapshotSessionRegistry::new(),
            snapshot_preflight: Arc::new(RwLock::new(BroadcasterSnapshotPreflightState::default())),
            snapshot_artifact: Arc::new(RwLock::new(None)),
            snapshot_boundary_retention: Arc::new(Mutex::new(None)),
            snapshot_refresh_active: Arc::new(AtomicBool::new(false)),
            recovery_publication: Arc::new(Mutex::new(RecoveryPublicationState::default())),
            recovery_workers: Arc::new(Mutex::new(Vec::new())),
            recovery_monitors: Arc::new(Mutex::new(Vec::new())),
            recovery_retry_backoff,
            native_progress_lease,
            next_recovery_source_id: Arc::new(AtomicU64::new(1)),
            redis_publisher,
            lifecycle_gate,
        }
    }

    pub(crate) fn chain_id(&self) -> u64 {
        self.cache.chain_id()
    }

    pub async fn mark_upstream_connected(&self) {
        let was_connected = self.upstream.snapshot().await.connected;
        self.upstream.mark_connected().await;
        if !was_connected {
            tracing::info!(
                event = "broadcaster_upstream_connected",
                backends = ?self.cache.configured_backends(),
                "Broadcaster connected to its upstream feed"
            );
        }
    }

    pub async fn mark_build_failed(&self, error: impl Into<String>) {
        self.upstream.mark_build_failed(error).await;
    }

    pub async fn mark_stream_disconnected(
        &self,
        reason: impl Into<String>,
        last_error: Option<String>,
    ) {
        let _gate = self.lifecycle_gate.lock().await;
        self.cancel_recovery_publication().await;
        self.upstream.mark_disconnected(reason, last_error).await;
        let backends = self.cache.configured_backends();
        if backends
            .iter()
            .any(|backend| *backend != simulator_core::broadcaster::BroadcasterBackend::Rfq)
        {
            self.cache.begin_same_generation_recovery().await;
        } else {
            self.cache.reset_same_generation().await;
        }
    }

    pub(crate) async fn mark_stream_disconnected_for_exit(
        &self,
        reason: impl Into<String>,
        last_error: Option<String>,
    ) {
        // Fatal feed paths must reach main before the process shutdown deadline can start.
        self.upstream.mark_disconnected(reason, last_error).await;
    }

    async fn cancel_recovery_publication(&self) {
        let mut recovery = self.recovery_publication.lock().await;
        let outcome = Self::cancel_recovery_publication_locked(&mut recovery);
        drop(recovery);
        Self::emit_recovery_abort(outcome);
    }

    pub(crate) async fn stop_recovery_tasks(&self) {
        self.cancel_recovery_publication().await;

        let workers = {
            let mut workers = self.recovery_workers.lock().await;
            std::mem::take(&mut *workers)
        };
        for worker in workers {
            if let Err(error) = worker.await {
                tracing::error!(%error, "Broadcaster recovery task failed while stopping");
            }
        }

        let monitors = {
            let mut monitors = self.recovery_monitors.lock().await;
            std::mem::take(&mut *monitors)
        };
        // These timers never write to Redis. The recovery workers above own that work.
        for monitor in monitors {
            monitor.abort();
            let _ = monitor.await;
        }
    }

    fn cancel_recovery_publication_locked(
        recovery: &mut RecoveryPublicationState,
    ) -> Option<RecoveryAbortMetrics> {
        let outcome = if recovery.phase.is_active() {
            let duration_ms = recovery.started_at.elapsed().as_millis() as u64;
            recovery.last_outcome = Some("aborted");
            recovery.last_duration_ms = Some(duration_ms);
            Some((
                duration_ms,
                recovery.last_encoded_bytes.unwrap_or(0),
                recovery.last_chunk_count.unwrap_or(0),
                recovery.max_buffer_a_count,
                recovery.max_buffer_b_count,
            ))
        } else {
            None
        };
        recovery.cancelled.store(true, Ordering::Release);
        recovery.work_id = recovery.work_id.saturating_add(1);
        recovery.phase = RecoveryPublicationPhase::Idle;
        recovery.buffer_a.clear();
        recovery.buffer_a_oldest_at = None;
        recovery.publisher_recovery_id = None;
        recovery.complete_native_blocks.clear();
        recovery.fatal_error = None;
        outcome
    }

    fn emit_recovery_abort(outcome: Option<RecoveryAbortMetrics>) {
        if let Some((duration_ms, encoded_bytes, chunk_count, buffer_a_count, buffer_b_count)) =
            outcome
        {
            warn!(
                event = "broadcaster_recovery_aborted",
                duration_ms,
                encoded_bytes,
                chunk_count,
                buffer_a_count,
                buffer_b_count,
                "Broadcaster recovery was aborted before completion"
            );
            emit_broadcaster_recovery_outcome(
                "aborted",
                duration_ms,
                encoded_bytes,
                chunk_count,
                buffer_a_count,
                buffer_b_count,
            );
        }
    }

    async fn recovery_is_active(&self) -> bool {
        self.recovery_publication.lock().await.phase.is_active()
    }

    async fn recovery_status(&self) -> BroadcasterRecoveryStatus {
        let (publisher_buffer_count, _, publisher_oldest_age_ms) =
            self.redis_publisher.pending_recovery_payload_stats().await;
        let recovery = self.recovery_publication.lock().await;
        let active = recovery.phase.is_active();
        let age_ms = (active || recovery.phase == RecoveryPublicationPhase::Fatal)
            .then(|| recovery.started_at.elapsed().as_millis() as u64);
        BroadcasterRecoveryStatus {
            active,
            phase: recovery.phase.as_str(),
            age_ms,
            attempt: if active {
                recovery.local_failures.saturating_add(1)
            } else {
                recovery.local_failures
            },
            buffer_a_count: recovery.buffer_a.len(),
            buffer_b_count: publisher_buffer_count,
            oldest_buffered_age_ms: [
                recovery
                    .buffer_a_oldest_at
                    .map(|accepted_at| accepted_at.elapsed().as_millis() as u64),
                publisher_oldest_age_ms,
            ]
            .into_iter()
            .flatten()
            .max(),
            last_outcome: recovery.last_outcome,
            last_duration_ms: recovery.last_duration_ms,
            last_encoded_bytes: recovery.last_encoded_bytes,
            last_chunk_count: recovery.last_chunk_count,
            last_error: recovery.fatal_error.clone(),
        }
    }

    async fn recovery_fatal_error(&self) -> Option<String> {
        let recovery = self.recovery_publication.lock().await;
        (recovery.phase == RecoveryPublicationPhase::Fatal)
            .then(|| recovery.fatal_error.clone())
            .flatten()
    }

    pub(crate) async fn redis_publisher_is_retired(&self) -> bool {
        self.redis_publisher.mode().await == BroadcasterRedisPublisherMode::Retired
    }

    pub(crate) async fn wait_for_shared_publisher_resume(&self) {
        self.redis_publisher.wait_for_feeds_resumed().await;
    }

    pub(crate) fn shared_publisher_pause_epoch(&self) -> u64 {
        self.redis_publisher.feed_pause_epoch()
    }

    pub(crate) async fn wait_for_shared_publisher_pause_after(&self, pause_epoch: u64) {
        self.redis_publisher
            .wait_for_feed_pause_after(pause_epoch)
            .await;
    }

    pub async fn promote_when_ready(
        services: &[Self],
        reason: impl Into<String>,
    ) -> Result<Option<BroadcasterRedisReplayBoundary>> {
        anyhow::ensure!(
            !services.is_empty(),
            "broadcaster promotion requires at least one service"
        );
        ensure_shared_lifecycle(services, "broadcaster promotion")?;

        match services[0].redis_publisher.mode().await {
            BroadcasterRedisPublisherMode::Active => {
                Self::run_snapshot_export_preflight(services).await?;
                return if services[0].snapshot_artifact.read().await.is_some() {
                    services[0]
                        .redis_publisher
                        .replay_boundary()
                        .await
                        .map(Some)
                } else {
                    Ok(None)
                };
            }
            BroadcasterRedisPublisherMode::Passive => {}
            BroadcasterRedisPublisherMode::Retired | BroadcasterRedisPublisherMode::Unhealthy => {
                return Ok(None);
            }
        }

        let Some(candidate) = prepare_startup_snapshot_candidate(services).await? else {
            return Ok(None);
        };
        let handoff_id = candidate.handoff_id;
        match promote_prepared_startup_candidate(services, candidate, reason.into()).await {
            Ok(boundary) => Ok(Some(boundary)),
            Err(error) => {
                services[0]
                    .redis_publisher
                    .abort_startup_handoff(handoff_id, error.to_string())
                    .await;
                Err(error)
            }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "capture and revalidation stay in one flow"
    )]
    pub(crate) async fn capture_state_history_checkpoint(
        services: &[Self],
        kind: CheckpointKind,
        position: Option<StreamPosition>,
        requested_block_number: Option<u64>,
        requested_rfq_observed_at_ms: Option<u64>,
    ) -> Result<Option<u64>> {
        anyhow::ensure!(
            !services.is_empty(),
            "state history checkpoint requires at least one service"
        );
        ensure_shared_lifecycle(services, "state history checkpoint")?;
        let Some(state_history) = services[0].redis_publisher.state_history() else {
            return Ok(None);
        };

        let gate = services[0].lifecycle_gate.lock().await;
        if services[0].redis_publisher.mode().await != BroadcasterRedisPublisherMode::Active {
            return Ok(None);
        }
        for service in services {
            if service.status_snapshot().await.readiness != BroadcasterReadiness::Ready {
                return Ok(None);
            }
        }

        let Some(block_number) = state_history_aligned_backend_head(services).await else {
            return Ok(None);
        };

        let publisher = services[0].redis_publisher.status_snapshot().await;
        let boundary = publisher
            .replay_boundary
            .context("active Redis replay boundary is unavailable")?;
        let capture_position = StreamPosition {
            generation: boundary.generation,
            message_seq: boundary.exclusive_message_seq,
        };
        if position.is_some_and(|requested| requested.generation != capture_position.generation) {
            return Ok(None);
        }
        let chain_id = services[0].cache.chain_id();
        let mut captured = Vec::with_capacity(services.len());
        for service in services {
            anyhow::ensure!(
                service.cache.chain_id() == chain_id,
                "combined state history checkpoint chain_id mismatch"
            );
            let source = service
                .cache
                .pin_snapshot_source()
                .await
                .context("ready state history checkpoint source disappeared")?;
            captured.push((
                service.cache.clone(),
                source,
                service.snapshot_max_payload_bytes,
            ));
        }
        // The catalog clone follows every state pin under the lifecycle gate so it cannot miss
        // tokens already referenced by the pinned state.
        let token_catalog = state_history.clone_token_catalog().await;
        let rfq_observed_at_ms = state_history.rfq_high_water_ms();
        drop(gate);

        let export = tokio::task::spawn_blocking(move || {
            let exports = captured
                .into_iter()
                .map(|(cache, source, max_payload_bytes)| {
                    cache.export_snapshot_source(source, max_payload_bytes)
                })
                .collect::<Result<Vec<_>>>()?;
            combine_snapshot_exports(chain_id, exports)
        })
        .await
        .context("state history checkpoint worker failed")??;

        let gate = services[0].lifecycle_gate.lock().await;
        let current = services[0]
            .redis_publisher
            .status_snapshot()
            .await
            .replay_boundary;
        let services_ready =
            futures::future::join_all(services.iter().map(|service| service.cache.is_ready()))
                .await
                .into_iter()
                .all(|ready| ready);
        let Some((export, token_catalog)) = retain_current_checkpoint_capture(
            &boundary,
            current.as_ref(),
            services_ready,
            (export, token_catalog),
        ) else {
            return Ok(None);
        };
        drop(gate);

        state_history.observe_rfq_export(&export)?;
        // Only the pinned boundary version is guaranteed to describe this exported world.
        let state = crate::broadcaster::state_history::captured_state_from_export(
            kind,
            &export,
            capture_position,
            boundary.state_version,
            block_number,
            rfq_observed_at_ms,
        )?;
        let capture = state_history.checkpoint_capture(state, token_catalog);
        anyhow::ensure!(
            state_history.handle.request_checkpoint(capture),
            "state history checkpoint queue rejected the capture"
        );
        record_boundary_slip_gap_if_any(
            &state_history.handle,
            chain_id,
            position.map(|position| BoundaryCheckpointCursor {
                position,
                block_number: requested_block_number,
                rfq_observed_at_ms: requested_rfq_observed_at_ms,
            }),
            BoundaryCheckpointCursor {
                position: capture_position,
                block_number: Some(block_number),
                rfq_observed_at_ms,
            },
        );
        Ok(Some(block_number))
    }

    pub(crate) async fn state_history_aligned_head(services: &[Self]) -> Option<u64> {
        let first = services.first()?;
        let _gate = first.lifecycle_gate.lock().await;
        if first.redis_publisher.mode().await != BroadcasterRedisPublisherMode::Active {
            return None;
        }
        for service in services {
            if service.status_snapshot().await.readiness != BroadcasterReadiness::Ready {
                return None;
            }
        }
        state_history_aligned_backend_head(services).await
    }

    pub(crate) async fn publisher_mode(&self) -> BroadcasterRedisPublisherMode {
        self.redis_publisher.mode().await
    }

    pub(crate) async fn recover_shared_publisher_when_paused(
        services: &[Self],
        reason: impl Into<String>,
    ) -> Result<Option<BroadcasterRedisReplayBoundary>> {
        anyhow::ensure!(
            !services.is_empty(),
            "broadcaster writer recovery requires at least one service"
        );
        ensure_shared_lifecycle(services, "broadcaster writer recovery")?;
        if services[0].redis_publisher.mode().await != BroadcasterRedisPublisherMode::Unhealthy {
            return Ok(None);
        }
        if !services[0].redis_publisher.feeds_are_paused()
            && !services[0].redis_publisher.writer_fence_is_absent().await
        {
            return Ok(None);
        }

        let gate = services[0].lifecycle_gate.lock().await;
        for service in services {
            if service.upstream.snapshot().await.connected
                || service.recovery_publication.lock().await.phase.is_active()
            {
                return Ok(None);
            }
        }
        if services[0].redis_publisher.mode().await != BroadcasterRedisPublisherMode::Unhealthy {
            return Ok(None);
        }

        let mut base_heads = Vec::new();
        for service in services {
            base_heads.extend(service.cache.backend_heads().await);
        }
        anyhow::ensure!(
            !base_heads.is_empty(),
            "broadcaster writer recovery requires a retained backend head"
        );
        let boundary = services[0]
            .redis_publisher
            .recover_writer(base_heads, reason)
            .await?;
        for service in services {
            service.cache.relabel_generation(boundary.generation).await;
            service
                .snapshot_sessions
                .disconnect_all(SessionCloseReason::WriterFenceLost)
                .await;
        }
        drop(gate);
        Ok(Some(boundary))
    }

    pub(crate) async fn has_current_snapshot_artifact(&self) -> bool {
        let Some(artifact) = self.snapshot_artifact.read().await.clone() else {
            return false;
        };
        let Some(boundary) = self.redis_publisher.status_snapshot().await.replay_boundary else {
            return false;
        };
        artifact_matches_current_boundary(&artifact, &boundary)
    }

    pub(crate) async fn snapshot_boundary_is_retained(&self) -> SnapshotBoundaryRetention {
        let Some(artifact) = self.snapshot_artifact.read().await.clone() else {
            return SnapshotBoundaryRetention::NotRetained;
        };
        let Some(current_boundary) = self.redis_publisher.status_snapshot().await.replay_boundary
        else {
            return SnapshotBoundaryRetention::NotRetained;
        };
        if !artifact_matches_current_boundary(&artifact, &current_boundary) {
            return SnapshotBoundaryRetention::NotRetained;
        }

        {
            let cached = self.snapshot_boundary_retention.lock().await;
            if let Some(cached) = cached.as_ref().filter(|cached| {
                cached.boundary == artifact.redis_replay_boundary
                    && cached.checked_at.elapsed() < SNAPSHOT_BOUNDARY_RETENTION_CACHE_TTL
            }) {
                return cached.verdict.clone();
            }
        }

        let verdict = match self
            .redis_publisher
            .replay_boundary_is_retained(&artifact.redis_replay_boundary)
            .await
        {
            Ok(true) => SnapshotBoundaryRetention::Retained,
            Ok(false) => SnapshotBoundaryRetention::NotRetained,
            Err(error) => SnapshotBoundaryRetention::InspectionFailed(format!("{error:#}")),
        };
        *self.snapshot_boundary_retention.lock().await = Some(CachedSnapshotBoundaryRetention {
            boundary: artifact.redis_replay_boundary.clone(),
            checked_at: Instant::now(),
            verdict: verdict.clone(),
        });
        verdict
    }

    pub async fn apply_update(&self, update: &TychoUpdate) -> Result<()> {
        anyhow::ensure!(
            !self.redis_publisher.feeds_are_paused(),
            "shared Redis publisher paused every feed until writer verification succeeds"
        );
        let _gate = self.lifecycle_gate.lock().await;
        let staged = self.cache.stage_update(update).await?;
        if let Some(message) = staged.message() {
            self.publish_to_redis(BroadcasterPayload::Update(message.clone()))
                .await?;
        }
        self.cache.commit_staged_update(staged).await?;
        self.upstream.record_update().await;
        Ok(())
    }

    pub async fn apply_feed_message(&self, feed: &FeedMessage<BlockHeader>) -> Result<bool> {
        Ok(self.apply_feed_message_with_progress(feed).await?.published)
    }

    #[expect(
        clippy::excessive_nesting,
        reason = "Replacement, buffered and live updates preserve different publication-before-cache commit boundaries"
    )]
    pub(crate) async fn apply_feed_message_with_progress(
        &self,
        feed: &FeedMessage<BlockHeader>,
    ) -> Result<BroadcasterFeedApply> {
        anyhow::ensure!(
            !self.redis_publisher.feeds_are_paused(),
            "shared Redis publisher paused every feed until writer verification succeeds"
        );
        if let Some(error) = self.recovery_fatal_error().await {
            return Err(anyhow::anyhow!(
                "broadcaster recovery worker terminated: {error}"
            ));
        }
        let _gate = self.lifecycle_gate.lock().await;
        let staged = self.cache.stage_feed_message(feed).await?;
        let publishes_update = staged.publishes_update();
        let native_progress = (!staged.has_replacement_ready())
            .then(|| {
                staged.message().and_then(
                    simulator_core::broadcaster::BroadcasterUpdateMessage::complete_native_block,
                )
            })
            .flatten();
        let mut native_progress_was_published = false;
        if staged.has_replacement_ready() {
            let source = self.cache.recovery_source(&staged)?;
            self.cache.commit_staged_update(staged).await?;
            self.start_recovery_publication(source).await;
        } else if let Some(message) = staged.message() {
            if self.buffer_recovery_update(message.clone()).await? {
                self.cache.commit_staged_update(staged).await?;
            } else {
                if let Err(error) = self
                    .publish_to_redis(BroadcasterPayload::Update(message.clone()))
                    .await
                {
                    if error.downcast_ref::<OversizedRedisEntry>().is_some() {
                        self.cache.commit_staged_update(staged).await?;
                        self.cache
                            .begin_same_generation_recovery_from_current()
                            .await;
                        warn!(
                            error = %error,
                            "Oversized live update retained for full-replacement recovery"
                        );
                        return Ok(BroadcasterFeedApply {
                            published: true,
                            native_progress,
                        });
                    }
                    return Err(error);
                }
                self.cache.commit_staged_update(staged).await?;
                native_progress_was_published = true;
            }
        } else {
            self.cache.commit_staged_update(staged).await?;
        }
        if let Some(block_number) = native_progress.filter(|_| native_progress_was_published) {
            self.upstream.record_native_progress(block_number).await;
        }
        Ok(BroadcasterFeedApply {
            published: publishes_update,
            native_progress,
        })
    }

    async fn buffer_recovery_update(
        &self,
        update: simulator_core::broadcaster::BroadcasterUpdateMessage,
    ) -> Result<bool> {
        let mut recovery = self.recovery_publication.lock().await;
        if recovery.phase == RecoveryPublicationPhase::Fatal {
            return Err(anyhow::anyhow!(
                "broadcaster recovery worker terminated: {}",
                recovery
                    .fatal_error
                    .as_deref()
                    .unwrap_or("unknown recovery failure")
            ));
        }
        if recovery.phase == RecoveryPublicationPhase::Idle {
            return Ok(false);
        }

        if let Some(block_number) = update.complete_native_block() {
            recovery.complete_native_blocks.insert(block_number);
        }
        let elapsed = recovery.started_at.elapsed();
        if elapsed >= RECOVERY_WARN_AFTER && !recovery.warned {
            recovery.warned = true;
            warn!(
                event = "broadcaster_recovery_buffer_warning",
                work_id = recovery.work_id,
                elapsed_ms = elapsed.as_millis() as u64,
                distinct_native_blocks = recovery.complete_native_blocks.len(),
                "Broadcaster recovery is retaining live updates"
            );
        }
        if elapsed >= RECOVERY_ABORT_AFTER
            || recovery.complete_native_blocks.len()
                >= self.redis_publisher.recovery_max_buffered_native_blocks()
        {
            recovery.cancelled.store(true, Ordering::Release);
        }

        match recovery.phase {
            RecoveryPublicationPhase::Building => {
                recovery.buffer_a_oldest_at.get_or_insert_with(Instant::now);
                recovery.buffer_a.push(update);
                let buffer_a_count = recovery.buffer_a.len();
                recovery.max_buffer_a_count = recovery.max_buffer_a_count.max(buffer_a_count);
            }
            RecoveryPublicationPhase::Publishing | RecoveryPublicationPhase::Draining => {
                return Ok(false);
            }
            RecoveryPublicationPhase::Idle | RecoveryPublicationPhase::Fatal => {}
        }
        Ok(true)
    }

    async fn start_recovery_publication(&self, source: BroadcasterRecoverySource) {
        let (work_id, cancelled) = {
            let mut recovery = self.recovery_publication.lock().await;
            recovery.cancelled.store(true, Ordering::Release);
            recovery.work_id = recovery.work_id.saturating_add(1);
            recovery.phase = RecoveryPublicationPhase::Building;
            recovery.local_failures = 0;
            recovery.started_at = Instant::now();
            recovery.warned = false;
            recovery.complete_native_blocks.clear();
            recovery.buffer_a.clear();
            recovery.buffer_a_oldest_at = None;
            recovery.cancelled = Arc::new(AtomicBool::new(false));
            recovery.publisher_recovery_id = None;
            recovery.fatal_error = None;
            recovery.max_buffer_a_count = 0;
            recovery.max_buffer_b_count = 0;
            recovery.last_outcome = None;
            recovery.last_duration_ms = None;
            recovery.last_encoded_bytes = None;
            recovery.last_chunk_count = None;
            (recovery.work_id, Arc::clone(&recovery.cancelled))
        };
        let recovery_id = self
            .redis_publisher
            .begin_recovery(&self.cache.configured_backends(), cancelled)
            .await;
        {
            let mut recovery = self.recovery_publication.lock().await;
            if recovery.work_id != work_id || recovery.phase != RecoveryPublicationPhase::Building {
                return;
            }
            recovery.publisher_recovery_id = Some(recovery_id);
        }
        let service = self.clone();
        let worker = tokio::spawn(async move {
            let result =
                AssertUnwindSafe(service.clone().run_recovery_publication(work_id, source))
                    .catch_unwind()
                    .await;
            if result.is_err() {
                service
                    .handle_recovery_worker_termination(
                        work_id,
                        "recovery worker panicked".to_string(),
                    )
                    .await;
            }
        });
        let mut workers = self.recovery_workers.lock().await;
        workers.retain(|worker| !worker.is_finished());
        workers.push(worker);
    }

    async fn handle_recovery_worker_termination(&self, work_id: u64, reason: String) {
        let should_fence = {
            let mut recovery = self.recovery_publication.lock().await;
            if recovery.work_id != work_id || !recovery.phase.is_active() {
                false
            } else {
                recovery.cancelled.store(true, Ordering::Release);
                recovery.phase = RecoveryPublicationPhase::Fatal;
                recovery.fatal_error = Some(reason.clone());
                recovery.last_outcome = Some("failed");
                true
            }
        };
        if !should_fence {
            return;
        }
        self.redis_publisher
            .self_fence(format!("broadcaster recovery worker terminated: {reason}"))
            .await;
        self.upstream
            .mark_disconnected("recovery_worker_terminated", Some(reason))
            .await;
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the bounded build, publish, and drain phases share one retry state machine"
    )]
    #[expect(
        clippy::excessive_nesting,
        clippy::cognitive_complexity,
        reason = "Serialization, cancellation and publication retries share a work ID and distinct local failure allowance"
    )]
    async fn run_recovery_publication(self, work_id: u64, mut source: BroadcasterRecoverySource) {
        loop {
            self.spawn_recovery_bounds_monitor(work_id).await;
            let replacement_complete_native_block = source.complete_native_block();
            let cache = self.cache.clone();
            let replacement = tokio::task::spawn_blocking(move || {
                cache.serialize_recovery_source(source, BROADCASTER_SNAPSHOT_ENVELOPE_MAX_BYTES)
            })
            .await;
            let replacement = replacement
                .map_err(|error| format!("recovery serialization worker failed: {error}"))
                .and_then(|result| {
                    result.map_err(|error| {
                        format!("failed to serialize recovery replacement: {error:#}")
                    })
                });
            let replacement_json = match replacement {
                Ok(replacement_json) => replacement_json,
                Err(reason) => {
                    let Some(retry_source) =
                        self.handle_local_recovery_failure(work_id, reason).await
                    else {
                        return;
                    };
                    source = retry_source;
                    continue;
                }
            };

            let (recovery_id, buffer_a, cancelled) = {
                let mut recovery = self.recovery_publication.lock().await;
                if recovery.work_id != work_id
                    || recovery.phase != RecoveryPublicationPhase::Building
                {
                    return;
                }
                if recovery.cancelled.load(Ordering::Acquire) {
                    drop(recovery);
                    let Some(retry_source) = self
                        .handle_local_recovery_failure(
                            work_id,
                            "recovery exceeded its Buffer A bounds before publication".to_string(),
                        )
                        .await
                    else {
                        return;
                    };
                    source = retry_source;
                    continue;
                }
                recovery.phase = RecoveryPublicationPhase::Publishing;
                let buffer_a = std::mem::take(&mut recovery.buffer_a);
                recovery.buffer_a_oldest_at = None;
                let Some(recovery_id) = recovery.publisher_recovery_id.clone() else {
                    drop(recovery);
                    self.handle_recovery_worker_termination(
                        work_id,
                        "recovery publication has no publisher reservation".to_string(),
                    )
                    .await;
                    return;
                };
                (recovery_id, buffer_a, Arc::clone(&recovery.cancelled))
            };

            let transaction = BroadcasterRecoveryTransaction {
                recovery_id,
                backends: self.cache.configured_backends(),
                replacement_json,
                buffer_a,
            };
            let publication = self
                .redis_publisher
                .publish_recovery(transaction, &cancelled)
                .await;
            let publication = match publication {
                Ok(publication) => publication,
                Err(error)
                    if error
                        .downcast_ref::<RecoveryPublicationCancelled>()
                        .is_some()
                        || error.downcast_ref::<OversizedRedisEntry>().is_some() =>
                {
                    let Some(retry_source) = self
                        .handle_local_recovery_failure(work_id, format!("{error:#}"))
                        .await
                    else {
                        return;
                    };
                    source = retry_source;
                    continue;
                }
                Err(error)
                    if error
                        .downcast_ref::<RecoveryCommitAdoptionUncertain>()
                        .is_some() =>
                {
                    self.handle_recovery_worker_termination(work_id, format!("{error:#}"))
                        .await;
                    return;
                }
                Err(error) => {
                    let Some(retry_source) = self
                        .handle_external_recovery_failure(work_id, format!("{error:#}"))
                        .await
                    else {
                        return;
                    };
                    source = retry_source;
                    continue;
                }
            };

            tracing::info!(
                event = "broadcaster_upstream_recovery_committed",
                recovery_id = publication.recovery_id,
                target_state_version = publication.target_state_version,
                commit_entry_id = publication.commit_entry_id,
                encoded_bytes = publication.encoded_bytes,
                chunk_count = publication.chunk_count,
                buffer_a_count = publication.buffer_a_count,
                replay_boundary = publication.replay_boundary.exclusive_entry_id(),
                "Aligned Tycho replacement state published and committed"
            );
            let commit_position = publication.commit_position;

            {
                let mut recovery = self.recovery_publication.lock().await;
                if recovery.work_id != work_id {
                    return;
                }
                recovery.last_encoded_bytes = Some(publication.encoded_bytes);
                recovery.last_chunk_count = Some(publication.chunk_count);
                recovery.phase = RecoveryPublicationPhase::Draining;
            }

            let Some(finish) = self
                .finish_recovery_publication(work_id, replacement_complete_native_block)
                .await
            else {
                return;
            };
            if let Some(native_block) = finish.complete_native_block {
                self.upstream.record_native_progress(native_block).await;
            }
            let Some(state_history) = self.redis_publisher.state_history() else {
                return;
            };
            let block_number = finish.complete_native_block;
            let rfq_observed_at_ms = state_history.rfq_high_water_ms();
            let checkpoint = state_history
                .request_boundary_checkpoint(commit_position, block_number, rfq_observed_at_ms)
                .await;
            if !matches!(checkpoint, Ok(Some(_))) {
                state_history.handle.record_boundary_failure_gap(
                    self.cache.chain_id(),
                    commit_position,
                    block_number,
                    rfq_observed_at_ms,
                );
                match checkpoint {
                    Ok(None) => warn!(
                        event = "state_history_boundary_checkpoint_failed",
                        chain_id = self.cache.chain_id(),
                        generation = commit_position.generation,
                        message_seq = commit_position.message_seq,
                        "Recovery boundary checkpoint was discarded"
                    ),
                    Err(error) => warn!(
                        event = "state_history_boundary_checkpoint_failed",
                        chain_id = self.cache.chain_id(),
                        generation = commit_position.generation,
                        message_seq = commit_position.message_seq,
                        %error,
                        "Recovery boundary checkpoint failed"
                    ),
                    Ok(Some(_)) => {}
                }
            }
            return;
        }
    }

    async fn spawn_recovery_bounds_monitor(&self, work_id: u64) {
        let (cancelled, started_at) = {
            let recovery = self.recovery_publication.lock().await;
            if recovery.work_id != work_id || !recovery.phase.is_active() {
                return;
            }
            (Arc::clone(&recovery.cancelled), recovery.started_at)
        };
        let recovery_publication = Arc::clone(&self.recovery_publication);
        let monitor = tokio::spawn(async move {
            let warn_after =
                RECOVERY_WARN_AFTER.saturating_sub(Instant::now().duration_since(started_at));
            tokio::time::sleep(warn_after).await;
            {
                let mut recovery = recovery_publication.lock().await;
                if recovery.work_id != work_id
                    || !recovery.phase.is_active()
                    || !Arc::ptr_eq(&recovery.cancelled, &cancelled)
                {
                    return;
                }
                if !recovery.warned {
                    recovery.warned = true;
                    warn!(
                        event = "broadcaster_recovery_buffer_warning",
                        work_id,
                        elapsed_ms = recovery.started_at.elapsed().as_millis() as u64,
                        distinct_native_blocks = recovery.complete_native_blocks.len(),
                        "Broadcaster recovery is retaining live updates"
                    );
                }
            }
            let abort_after =
                RECOVERY_ABORT_AFTER.saturating_sub(Instant::now().duration_since(started_at));
            tokio::time::sleep(abort_after).await;
            let recovery = recovery_publication.lock().await;
            if recovery.work_id == work_id
                && recovery.phase.is_active()
                && Arc::ptr_eq(&recovery.cancelled, &cancelled)
            {
                cancelled.store(true, Ordering::Release);
            }
        });
        let mut monitors = self.recovery_monitors.lock().await;
        monitors.retain(|monitor| !monitor.is_finished());
        monitors.push(monitor);
    }

    async fn finish_recovery_publication(
        &self,
        work_id: u64,
        replacement_complete_native_block: Option<u64>,
    ) -> Option<RecoveryPublicationFinish> {
        let (_, publisher_max_buffer_count, _) =
            self.redis_publisher.pending_recovery_payload_stats().await;
        let mut recovery = self.recovery_publication.lock().await;
        if recovery.work_id != work_id || recovery.phase != RecoveryPublicationPhase::Draining {
            return None;
        }
        let complete_native_block = recovery
            .complete_native_blocks
            .iter()
            .copied()
            .chain(replacement_complete_native_block)
            .max();
        recovery.max_buffer_b_count = recovery.max_buffer_b_count.max(publisher_max_buffer_count);
        let duration_ms = recovery.started_at.elapsed().as_millis() as u64;
        let encoded_bytes = recovery.last_encoded_bytes.unwrap_or(0);
        let chunk_count = recovery.last_chunk_count.unwrap_or(0);
        let buffer_a_count = recovery.max_buffer_a_count;
        let buffer_b_count = recovery.max_buffer_b_count;
        recovery.phase = RecoveryPublicationPhase::Idle;
        recovery.publisher_recovery_id = None;
        recovery.complete_native_blocks.clear();
        recovery.last_outcome = Some("committed");
        recovery.last_duration_ms = Some(duration_ms);
        drop(recovery);
        emit_broadcaster_recovery_outcome(
            "committed",
            duration_ms,
            encoded_bytes,
            chunk_count,
            buffer_a_count,
            buffer_b_count,
        );
        Some(RecoveryPublicationFinish {
            complete_native_block,
        })
    }

    async fn handle_local_recovery_failure(
        &self,
        work_id: u64,
        reason: String,
    ) -> Option<BroadcasterRecoverySource> {
        let retry_id = {
            let mut recovery = self.recovery_publication.lock().await;
            if recovery.work_id != work_id {
                return None;
            }
            if recovery.local_failures >= RECOVERY_MAX_LOCAL_FAILURES {
                let duration_ms = recovery.started_at.elapsed().as_millis() as u64;
                let encoded_bytes = recovery.last_encoded_bytes.unwrap_or(0);
                let chunk_count = recovery.last_chunk_count.unwrap_or(0);
                let buffer_a_count = recovery.max_buffer_a_count;
                let buffer_b_count = recovery.max_buffer_b_count;
                recovery.cancelled.store(true, Ordering::Release);
                recovery.phase = RecoveryPublicationPhase::Fatal;
                recovery.fatal_error = Some(reason.clone());
                recovery.last_outcome = Some("failed");
                recovery.last_duration_ms = Some(duration_ms);
                drop(recovery);
                emit_broadcaster_recovery_outcome(
                    "failed",
                    duration_ms,
                    encoded_bytes,
                    chunk_count,
                    buffer_a_count,
                    buffer_b_count,
                );
                self.redis_publisher
                    .self_fence(format!(
                        "broadcaster recovery self-fenced after its superseding attempt failed: {reason}"
                    ))
                    .await;
                self.upstream
                    .mark_disconnected("recovery_self_fenced", Some(reason.clone()))
                    .await;
                tracing::error!(
                    event = "broadcaster_recovery_self_fenced",
                    work_id,
                    error = %reason,
                    "Broadcaster recovery exhausted its local attempt allowance"
                );
                return None;
            }
            recovery.local_failures = recovery.local_failures.saturating_add(1);
            tracing::warn!(
                event = "broadcaster_recovery_superseding_attempt",
                work_id,
                local_failures = recovery.local_failures,
                error = %reason,
                "Starting the one allowed superseding recovery attempt"
            );
            self.prepare_recovery_retry_locked(&mut recovery)
        };
        tokio::time::sleep(self.recovery_retry_backoff).await;
        self.capture_retry_source(work_id, retry_id).await
    }

    #[expect(
        clippy::excessive_nesting,
        reason = "Cancellation must recheck the work ID under the reacquired lock before fencing or resetting state"
    )]
    async fn handle_external_recovery_failure(
        &self,
        work_id: u64,
        reason: String,
    ) -> Option<BroadcasterRecoverySource> {
        warn!(
            event = "broadcaster_recovery_external_retry",
            work_id,
            error = %reason,
            "Waiting to retry broadcaster recovery without consuming the local attempt allowance"
        );
        loop {
            let recovery_cancelled = {
                let recovery = self.recovery_publication.lock().await;
                if recovery.work_id != work_id {
                    return None;
                }
                recovery.cancelled.load(Ordering::Acquire)
            };
            if recovery_cancelled {
                let outcome = {
                    let mut recovery = self.recovery_publication.lock().await;
                    if recovery.work_id != work_id {
                        return None;
                    }
                    Self::cancel_recovery_publication_locked(&mut recovery)
                };
                Self::emit_recovery_abort(outcome);
                // If pausing observes a retired publisher, worker termination is already a no-op.
                // The promotion task exits on Retired and paused feeds fail closed upstream.
                if let Err(error) = self
                    .redis_publisher
                    .pause_feeds_until_writer_verified()
                    .await
                {
                    self.handle_recovery_worker_termination(work_id, format!("{error:#}"))
                        .await;
                }
                return None;
            }
            match self.redis_publisher.mode().await {
                BroadcasterRedisPublisherMode::Retired => {
                    let mut recovery = self.recovery_publication.lock().await;
                    if recovery.work_id == work_id {
                        recovery.phase = RecoveryPublicationPhase::Fatal;
                        recovery.fatal_error =
                            Some("active Redis writer fence was lost during recovery".to_string());
                    }
                    drop(recovery);
                    self.upstream
                        .mark_disconnected(
                            "writer_fence_lost",
                            Some("active Redis writer fence was lost during recovery".to_string()),
                        )
                        .await;
                    return None;
                }
                BroadcasterRedisPublisherMode::Passive => {}
                BroadcasterRedisPublisherMode::Active
                | BroadcasterRedisPublisherMode::Unhealthy => {
                    if self.redis_publisher.renew_lease().await.is_ok() {
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        let retry_id = {
            let mut recovery = self.recovery_publication.lock().await;
            if recovery.work_id != work_id {
                return None;
            }
            self.prepare_recovery_retry_locked(&mut recovery)
        };
        self.capture_retry_source(work_id, retry_id).await
    }

    fn prepare_recovery_retry_locked(&self, recovery: &mut RecoveryPublicationState) -> u64 {
        recovery.phase = RecoveryPublicationPhase::Building;
        recovery.started_at = Instant::now();
        recovery.warned = false;
        recovery.complete_native_blocks.clear();
        recovery.buffer_a.clear();
        recovery.buffer_a_oldest_at = None;
        recovery.cancelled = Arc::new(AtomicBool::new(false));
        recovery.publisher_recovery_id = None;
        self.next_recovery_source_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn capture_retry_source(
        &self,
        work_id: u64,
        retry_id: u64,
    ) -> Option<BroadcasterRecoverySource> {
        let _gate = self.lifecycle_gate.lock().await;
        let source = self.cache.pin_recovery_source(retry_id).await;
        let cancelled = {
            let mut recovery = self.recovery_publication.lock().await;
            if recovery.work_id != work_id || recovery.phase != RecoveryPublicationPhase::Building {
                return None;
            }
            // Anything buffered before this pin is already represented by the replacement.
            recovery.buffer_a.clear();
            recovery.buffer_a_oldest_at = None;
            Arc::clone(&recovery.cancelled)
        };
        let recovery_id = self
            .redis_publisher
            .begin_recovery(&self.cache.configured_backends(), cancelled)
            .await;
        let mut recovery = self.recovery_publication.lock().await;
        if recovery.work_id != work_id || recovery.phase != RecoveryPublicationPhase::Building {
            return None;
        }
        recovery.publisher_recovery_id = Some(recovery_id);
        Some(source)
    }

    pub async fn broadcast_heartbeat(&self) -> Result<()> {
        if self.recovery_is_active().await || self.redis_publisher.recovery_gate_is_open().await {
            self.redis_publisher.renew_lease().await?;
            self.snapshot_sessions
                .cleanup_expired_snapshot_sessions()
                .await;
            return Ok(());
        }
        let _gate = self.lifecycle_gate.lock().await;
        if self.redis_publisher.mode().await == BroadcasterRedisPublisherMode::Unhealthy {
            self.redis_publisher.renew_lease().await?;
        }
        if let Some(heartbeat) = self.cache.heartbeat().await? {
            self.publish_to_redis(heartbeat).await?;
        } else {
            self.redis_publisher.renew_lease().await?;
        }
        self.snapshot_sessions
            .cleanup_expired_snapshot_sessions()
            .await;
        Ok(())
    }

    pub async fn create_snapshot_session(
        &self,
        ttl: Duration,
    ) -> Result<Option<BroadcasterSnapshotSessionResponse>> {
        Self::create_snapshot_session_for_services(std::slice::from_ref(self), ttl).await
    }

    pub async fn create_snapshot_session_for_services(
        services: &[Self],
        ttl: Duration,
    ) -> Result<Option<BroadcasterSnapshotSessionResponse>> {
        anyhow::ensure!(
            !services.is_empty(),
            "combined broadcaster snapshot session requires at least one service"
        );
        ensure_shared_lifecycle(services, "combined broadcaster snapshot session")?;
        if let Err(error) = services[0].redis_publisher.verify_active_writer().await {
            warn!(
                error = %error,
                "Refusing broadcaster snapshot session without active Redis writer fence"
            );
            return Ok(None);
        }
        let Some(artifact) = services[0].snapshot_artifact.read().await.clone() else {
            return Ok(None);
        };
        let publisher_status = services[0].redis_publisher.status_snapshot().await;
        let Some(current_boundary) = publisher_status.replay_boundary else {
            return Ok(None);
        };
        match services[0].snapshot_boundary_is_retained().await {
            SnapshotBoundaryRetention::Retained => {}
            SnapshotBoundaryRetention::NotRetained => return Ok(None),
            SnapshotBoundaryRetention::InspectionFailed(error) => {
                warn!(
                    error = %error,
                    "Refusing broadcaster snapshot session without retained Redis replay boundary"
                );
                return Ok(None);
            }
        }
        if !artifact_matches_current_boundary(&artifact, &current_boundary) {
            warn!(
                artifact_stream_id = artifact.stream_id,
                artifact_snapshot_id = artifact.snapshot_id,
                "Refusing stale broadcaster snapshot artifact"
            );
            return Ok(None);
        }
        services[0]
            .snapshot_sessions
            .create_snapshot_session(artifact, ttl)
            .await
            .context("failed to create combined broadcaster snapshot session")
            .map(Some)
    }

    pub async fn snapshot_session_payload(
        &self,
        session_id: u64,
        index: u32,
    ) -> Result<BroadcasterEnvelope, SnapshotSessionError> {
        if let Err(error) = self.redis_publisher.verify_active_writer().await {
            warn!(
                error = %error,
                "Refusing broadcaster snapshot payload without active Redis writer fence"
            );
            self.snapshot_sessions
                .disconnect_all(SessionCloseReason::WriterFenceLost)
                .await;
            return Err(SnapshotSessionError::NotFound);
        }
        self.snapshot_sessions
            .snapshot_payload(session_id, index)
            .await
    }

    pub async fn status_snapshot(&self) -> BroadcasterStatusSnapshot {
        let mut snapshot = self
            .cache
            .status_snapshot(
                self.snapshot_max_payload_bytes,
                self.upstream.snapshot().await,
                self.snapshot_sessions.snapshot().await,
                self.native_progress_lease,
            )
            .await;
        let artifact_available = self.has_current_snapshot_artifact().await;
        let preflight = self.snapshot_preflight.read().await;
        let now = Instant::now();
        snapshot.snapshot.exportable = artifact_available;
        snapshot.snapshot.last_export_check_age_ms = preflight
            .checked_at
            .map(|checked_at| now.saturating_duration_since(checked_at).as_millis() as u64);
        snapshot.snapshot.last_export_success_age_ms = preflight
            .succeeded_at
            .map(|succeeded_at| now.saturating_duration_since(succeeded_at).as_millis() as u64);
        snapshot.snapshot.last_export_duration_ms = preflight.duration_ms;
        snapshot.snapshot.last_export_payload_count = preflight.payload_count;
        snapshot.snapshot.largest_payload_bytes = preflight.largest_payload_bytes;
        snapshot.snapshot.payload_limit_utilization_bps =
            preflight.largest_payload_bytes.map(|bytes| {
                let bps = bytes.saturating_mul(10_000) / self.snapshot_max_payload_bytes.max(1);
                u16::try_from(bps.min(10_000)).unwrap_or(10_000)
            });
        snapshot.snapshot.last_export_error = preflight.last_error.clone();
        if snapshot.readiness == BroadcasterReadiness::Ready && !artifact_available {
            snapshot.readiness = BroadcasterReadiness::SnapshotUnexportable;
        }
        let recovery = self.recovery_status().await;
        if recovery.active {
            snapshot.readiness = BroadcasterReadiness::UpstreamRecovering;
        }
        snapshot.recovery = recovery;
        snapshot
    }

    async fn startup_candidate_sources(
        services: &[Self],
    ) -> Result<Option<StartupCandidateCapture>> {
        let _gate = services[0].lifecycle_gate.lock().await;
        if services[0].redis_publisher.mode().await != BroadcasterRedisPublisherMode::Passive
            || !services[0].cache.is_ready().await
        {
            return Ok(None);
        }

        let chain_id = services[0].cache.chain_id();
        let mut captured = Vec::with_capacity(services.len());
        let mut base_heads = Vec::new();
        let mut recovery_work_ids = Vec::with_capacity(services.len());
        for (index, service) in services.iter().enumerate() {
            anyhow::ensure!(
                service.cache.chain_id() == chain_id,
                "combined broadcaster snapshot chain_id mismatch"
            );
            let recovery = service.recovery_publication.lock().await;
            if recovery.phase.is_active() {
                return Ok(None);
            }
            recovery_work_ids.push(recovery.work_id);
            drop(recovery);

            if let Some(source) = service.cache.pin_snapshot_source().await {
                base_heads.extend(source.backend_heads());
                captured.push((
                    service.cache.clone(),
                    source,
                    service.snapshot_max_payload_bytes,
                ));
            } else if index == 0 {
                return Ok(None);
            }
        }
        let handoff_id = services[0].redis_publisher.begin_startup_handoff().await?;
        Ok(Some(StartupCandidateCapture {
            chain_id,
            handoff_id,
            captured,
            base_heads,
            recovery_work_ids,
        }))
    }

    #[expect(
        clippy::too_many_lines,
        reason = "singleflight capture, off-thread export, and artifact fencing form one refresh operation"
    )]
    #[expect(
        clippy::excessive_nesting,
        reason = "Captured sources and recovery IDs stay under the lifecycle lock inside the singleflight export operation"
    )]
    pub async fn run_snapshot_export_preflight(services: &[Self]) -> Result<()> {
        anyhow::ensure!(
            !services.is_empty(),
            "snapshot export preflight requires at least one service"
        );
        ensure_shared_lifecycle(services, "snapshot export preflight")?;
        if services[0].redis_publisher.mode().await != BroadcasterRedisPublisherMode::Active {
            return Ok(());
        }
        if futures::future::join_all(services.iter().map(Self::recovery_is_active))
            .await
            .into_iter()
            .any(|active| active)
        {
            return Ok(());
        }
        if services[0]
            .snapshot_refresh_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        let _refresh_guard = SnapshotRefreshGuard(Arc::clone(&services[0].snapshot_refresh_active));
        let started_at = Instant::now();
        let result = async {
            let (chain_id, boundary, captured, recovery_work_ids) = {
                let _gate = services[0].lifecycle_gate.lock().await;
                let mut recovery_work_ids = Vec::with_capacity(services.len());
                for service in services {
                    let recovery = service.recovery_publication.lock().await;
                    if recovery.phase.is_active() {
                        return Ok(None);
                    }
                    recovery_work_ids.push(recovery.work_id);
                }
                let Some(raw_source) = services[0].cache.pin_snapshot_source().await else {
                    return Ok(None);
                };
                let publisher = services[0].redis_publisher.status_snapshot().await;
                let boundary = publisher.replay_boundary.ok_or_else(|| {
                    anyhow::anyhow!("active Redis replay boundary is unavailable")
                })?;
                let chain_id = services[0].cache.chain_id();
                let mut captured = vec![(
                    services[0].cache.clone(),
                    raw_source,
                    services[0].snapshot_max_payload_bytes,
                )];
                for service in &services[1..] {
                    anyhow::ensure!(
                        service.cache.chain_id() == chain_id,
                        "combined broadcaster snapshot chain_id mismatch"
                    );
                    let Some(source) = service.cache.pin_snapshot_source().await else {
                        continue;
                    };
                    captured.push((
                        service.cache.clone(),
                        source,
                        service.snapshot_max_payload_bytes,
                    ));
                }
                (chain_id, boundary, captured, recovery_work_ids)
            };

            let (artifact, largest_payload_bytes) = tokio::task::spawn_blocking(move || {
                let exports = captured
                    .into_iter()
                    .map(|(cache, source, max_payload_bytes)| {
                        cache.export_snapshot_source(source, max_payload_bytes)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let export = combine_snapshot_exports(chain_id, exports)?;
                let (artifact, largest_payload_bytes) =
                    build_snapshot_artifact(chain_id, export, boundary)?;
                Ok::<_, anyhow::Error>((Arc::new(artifact), largest_payload_bytes))
            })
            .await
            .context("snapshot artifact worker failed")??;
            install_snapshot_artifact_if_current(services, &artifact, &recovery_work_ids).await?;
            Ok(Some((artifact, largest_payload_bytes)))
        }
        .await;
        let duration_ms = started_at.elapsed().as_millis() as u64;
        let checked_at = Instant::now();
        match result {
            Ok(Some((artifact, largest_payload_bytes))) => {
                for service in services {
                    let mut preflight = service.snapshot_preflight.write().await;
                    preflight.checked_at = Some(checked_at);
                    preflight.succeeded_at = Some(checked_at);
                    preflight.duration_ms = Some(duration_ms);
                    preflight.payload_count = Some(artifact.payloads.len());
                    preflight.largest_payload_bytes = Some(largest_payload_bytes);
                    preflight.last_error = None;
                }
                tracing::info!(
                    event = "broadcaster_snapshot_export_succeeded",
                    duration_ms,
                    payload_count = artifact.payloads.len(),
                    largest_payload_bytes,
                    "Broadcaster snapshot artifact refreshed"
                );
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(error) => {
                let error_message = error.to_string();
                for service in services {
                    let mut preflight = service.snapshot_preflight.write().await;
                    preflight.checked_at = Some(checked_at);
                    preflight.duration_ms = Some(duration_ms);
                    preflight.payload_count = None;
                    preflight.largest_payload_bytes = None;
                    preflight.last_error = Some(error_message.clone());
                }
                emit_broadcaster_snapshot_export_failure();
                warn!(
                    event = "broadcaster_snapshot_artifact_refresh_failed",
                    duration_ms,
                    error = %error_message,
                    "Broadcaster snapshot artifact refresh failed"
                );
                Err(error)
            }
        }
    }

    async fn publish_to_redis(&self, payload: BroadcasterPayload) -> Result<()> {
        let result = self
            .redis_publisher
            .publish_accepted_payload(payload)
            .await
            .inspect_err(|error| {
                warn!(
                    event = "redis_publication_failed",
                    error = %error,
                    "Redis broadcaster publication failed"
                );
            });
        if result.is_err()
            && self.redis_publisher.mode().await == BroadcasterRedisPublisherMode::Retired
        {
            self.snapshot_sessions
                .disconnect_all(SessionCloseReason::WriterFenceLost)
                .await;
        }
        result.context("failed to publish accepted broadcaster delta to Redis")
    }

    #[cfg(test)]
    pub(crate) async fn lock_lifecycle_gate_for_test(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.lifecycle_gate.clone().lock_owned().await
    }
}

fn checkpoint_world_matches(
    pinned: &BroadcasterRedisReplayBoundary,
    current: &BroadcasterRedisReplayBoundary,
) -> bool {
    current.generation == pinned.generation
        && current.stream_id == pinned.stream_id
        && current.snapshot_id == pinned.snapshot_id
}

fn retain_current_checkpoint_capture<T>(
    pinned: &BroadcasterRedisReplayBoundary,
    current: Option<&BroadcasterRedisReplayBoundary>,
    services_ready: bool,
    capture: T,
) -> Option<T> {
    (services_ready && current.is_some_and(|current| checkpoint_world_matches(pinned, current)))
        .then_some(capture)
}

struct BoundaryCheckpointCursor {
    position: StreamPosition,
    block_number: Option<u64>,
    rfq_observed_at_ms: Option<u64>,
}

fn record_boundary_slip_gap_if_any(
    handle: &state_history::WriterHandle,
    chain_id: u64,
    requested: Option<BoundaryCheckpointCursor>,
    capture: BoundaryCheckpointCursor,
) {
    let Some(requested) = requested else {
        return;
    };
    if requested.position.generation != capture.position.generation
        || capture.position.message_seq <= requested.position.message_seq
    {
        return;
    }
    let Some(message_seq) = requested.position.message_seq.checked_add(1) else {
        return;
    };
    // A half-bounded gap never matches the reader's overlap checks, and the capture
    // cursor would understate the window, so an unknown request-time cursor becomes
    // zero: unbounded below, matching every earlier range conservatively.
    handle.record_boundary_slip_gap(
        chain_id,
        StreamPosition {
            generation: requested.position.generation,
            message_seq,
        },
        capture.position,
        (
            Some(requested.block_number.unwrap_or(0)),
            capture.block_number,
        ),
        (
            capture
                .rfq_observed_at_ms
                .map(|_| requested.rfq_observed_at_ms.unwrap_or(0)),
            capture.rfq_observed_at_ms,
        ),
    );
}

async fn state_history_aligned_backend_head(services: &[BroadcasterServiceState]) -> Option<u64> {
    let mut aligned_head = None;
    for service in services {
        for head in service.cache.backend_heads().await {
            if matches!(
                head.backend,
                simulator_core::broadcaster::BroadcasterBackend::Native
                    | simulator_core::broadcaster::BroadcasterBackend::Vm
            ) {
                if aligned_head.is_some_and(|current| current != head.block_number) {
                    warn!(
                        event = "state_history_checkpoint_skipped",
                        reason = "backend_skew",
                        "State history checkpoint skipped because backend heads are not aligned"
                    );
                    return None;
                }
                aligned_head = Some(head.block_number);
            }
        }
    }
    aligned_head
}

pub(crate) struct BroadcasterFeedApply {
    pub(crate) published: bool,
    pub(crate) native_progress: Option<u64>,
}

struct SnapshotRefreshGuard(Arc<AtomicBool>);

impl Drop for SnapshotRefreshGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

async fn prepare_startup_snapshot_candidate(
    services: &[BroadcasterServiceState],
) -> Result<Option<BroadcasterSnapshotCandidate>> {
    let Some(capture) = BroadcasterServiceState::startup_candidate_sources(services).await? else {
        return Ok(None);
    };
    let StartupCandidateCapture {
        chain_id,
        handoff_id,
        captured,
        base_heads,
        recovery_work_ids,
    } = capture;
    let started_at = Instant::now();
    let worker = tokio::task::spawn_blocking(move || {
        let exports = captured
            .into_iter()
            .map(|(cache, mut source, max_payload_bytes)| {
                // Reserve generation ID growth before packing; promotion assigns the final IDs.
                source.relabel_generation(chain_id, u64::MAX);
                cache.export_snapshot_source(source, max_payload_bytes)
            })
            .collect::<Result<Vec<_>>>()?;
        let export = combine_snapshot_exports(chain_id, exports)?;
        build_snapshot_candidate(chain_id, export, base_heads, recovery_work_ids, handoff_id)
    });
    let result = match tokio::time::timeout(STARTUP_HANDOFF_ABORT_AFTER, worker).await {
        Ok(joined) => joined
            .context("startup snapshot candidate worker failed")
            .and_then(std::convert::identity),
        Err(_) => Err(anyhow::anyhow!(
            "startup snapshot candidate exceeded the 60-second bound"
        )),
    };
    let duration_ms = started_at.elapsed().as_millis() as u64;
    let checked_at = Instant::now();

    match result {
        Ok(candidate) => {
            for service in services {
                let mut preflight = service.snapshot_preflight.write().await;
                preflight.checked_at = Some(checked_at);
                preflight.succeeded_at = Some(checked_at);
                preflight.duration_ms = Some(duration_ms);
                preflight.payload_count = Some(candidate.export.payloads.len());
                preflight.largest_payload_bytes = Some(candidate.largest_payload_bytes_upper_bound);
                preflight.last_error = None;
            }
            tracing::info!(
                event = "broadcaster_startup_snapshot_candidate_ready",
                duration_ms,
                payload_count = candidate.export.payloads.len(),
                largest_payload_bytes = candidate.largest_payload_bytes_upper_bound,
                "Broadcaster startup snapshot candidate is ready"
            );
            Ok(Some(candidate))
        }
        Err(error) => {
            let error_message = error.to_string();
            services[0]
                .redis_publisher
                .abort_startup_handoff(handoff_id, error_message.clone())
                .await;
            for service in services {
                let mut preflight = service.snapshot_preflight.write().await;
                preflight.checked_at = Some(checked_at);
                preflight.duration_ms = Some(duration_ms);
                preflight.payload_count = None;
                preflight.largest_payload_bytes = None;
                preflight.last_error = Some(error_message.clone());
            }
            emit_broadcaster_snapshot_export_failure();
            Err(error)
        }
    }
}

async fn promote_prepared_startup_candidate(
    services: &[BroadcasterServiceState],
    candidate: BroadcasterSnapshotCandidate,
    reason: String,
) -> Result<BroadcasterRedisReplayBoundary> {
    let _gate = services[0].lifecycle_gate.lock().await;
    anyhow::ensure!(
        services[0].redis_publisher.mode().await == BroadcasterRedisPublisherMode::Passive,
        "startup snapshot candidate can only promote a passive publisher"
    );
    ensure_recovery_work_ids_current(services, &candidate.captured_recovery_work_ids).await?;
    let frozen_handoff = services[0]
        .redis_publisher
        .freeze_startup_handoff(candidate.handoff_id)
        .await?;

    services[0]
        .redis_publisher
        .set_deployment_phase(BroadcasterDeploymentPhase::Promoting, None);
    let boundary = services[0]
        .redis_publisher
        .promote(candidate.base_heads.clone(), reason)
        .await?;
    for service in services {
        service.cache.relabel_generation(boundary.generation).await;
    }

    services[0]
        .redis_publisher
        .set_deployment_phase(BroadcasterDeploymentPhase::ArtifactFinalizing, None);
    let artifact = Arc::new(finalize_snapshot_candidate(candidate, boundary.clone()));
    for service in services {
        *service.snapshot_artifact.write().await = Some(Arc::clone(&artifact));
    }

    services[0]
        .redis_publisher
        .drain_startup_handoff(frozen_handoff)
        .await?;
    services[0].redis_publisher.admit_deployment();
    Ok(boundary)
}

async fn ensure_recovery_work_ids_current(
    services: &[BroadcasterServiceState],
    captured_recovery_work_ids: &[u64],
) -> Result<()> {
    anyhow::ensure!(
        captured_recovery_work_ids.len() == services.len(),
        "startup snapshot candidate recovery fence count changed"
    );
    for (service, captured_work_id) in services.iter().zip(captured_recovery_work_ids) {
        let recovery = service.recovery_publication.lock().await;
        anyhow::ensure!(
            recovery.work_id == *captured_work_id && !recovery.phase.is_active(),
            "startup snapshot candidate crossed a recovery publication boundary"
        );
    }
    Ok(())
}

fn build_snapshot_candidate(
    chain_id: u64,
    mut export: BroadcasterSnapshotExport,
    base_heads: Vec<BroadcasterBackendHead>,
    captured_recovery_work_ids: Vec<u64>,
    handoff_id: u64,
) -> Result<BroadcasterSnapshotCandidate> {
    let placeholder_stream_id = format_redis_stream_id(chain_id, u64::MAX);
    let placeholder_snapshot_id = format_redis_snapshot_id(chain_id, u64::MAX);
    relabel_snapshot_export(&mut export, placeholder_stream_id, placeholder_snapshot_id);

    let mut encoded_bytes_upper_bound = 0usize;
    let mut largest_payload_bytes_upper_bound = 0usize;
    let mut payload_digests = Vec::with_capacity(export.payloads.len());
    for (index, payload) in export.payloads.iter().cloned().enumerate() {
        anyhow::ensure!(
            matches!(
                payload,
                BroadcasterPayload::SnapshotStart(_)
                    | BroadcasterPayload::SnapshotChunk(_)
                    | BroadcasterPayload::SnapshotEnd(_)
            ),
            "startup snapshot candidate contains a non-snapshot payload"
        );
        let message_seq = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
        let envelope = BroadcasterEnvelope::new(export.stream_id.clone(), message_seq, payload);
        let encoded = serde_json::to_vec(&envelope)?;
        anyhow::ensure!(
            encoded.len() <= export.max_payload_bytes,
            "startup snapshot payload {index} is {} bytes, above configured max {}",
            encoded.len(),
            export.max_payload_bytes
        );
        encoded_bytes_upper_bound = encoded_bytes_upper_bound.saturating_add(encoded.len());
        largest_payload_bytes_upper_bound = largest_payload_bytes_upper_bound.max(encoded.len());
        payload_digests.push(format!("{:x}", keccak256(&encoded)));
    }
    let snapshot_chunk_count = export
        .payloads
        .iter()
        .filter(|payload| matches!(payload, BroadcasterPayload::SnapshotChunk(_)))
        .count() as u32;

    Ok(BroadcasterSnapshotCandidate {
        chain_id,
        export,
        base_heads,
        captured_recovery_work_ids,
        handoff_id,
        snapshot_chunk_count,
        encoded_bytes_upper_bound,
        largest_payload_bytes_upper_bound,
        payload_digests,
    })
}

fn finalize_snapshot_candidate(
    mut candidate: BroadcasterSnapshotCandidate,
    redis_replay_boundary: BroadcasterRedisReplayBoundary,
) -> BroadcasterSnapshotArtifact {
    debug_assert_eq!(
        candidate.payload_digests.len(),
        candidate.export.payloads.len()
    );
    relabel_snapshot_export(
        &mut candidate.export,
        redis_replay_boundary.stream_id.clone(),
        redis_replay_boundary.snapshot_id.clone(),
    );
    let payloads = candidate
        .export
        .payloads
        .into_iter()
        .enumerate()
        .map(|(index, payload)| {
            let message_seq = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
            BroadcasterEnvelope::new(
                redis_replay_boundary.stream_id.clone(),
                message_seq,
                payload,
            )
        })
        .collect();
    BroadcasterSnapshotArtifact {
        chain_id: candidate.chain_id,
        stream_id: redis_replay_boundary.stream_id.clone(),
        snapshot_id: redis_replay_boundary.snapshot_id.clone(),
        redis_replay_boundary,
        payloads: Arc::new(payloads),
        snapshot_chunk_count: candidate.snapshot_chunk_count,
        encoded_bytes: candidate.encoded_bytes_upper_bound,
    }
}

fn relabel_snapshot_export(
    export: &mut BroadcasterSnapshotExport,
    stream_id: String,
    snapshot_id: String,
) {
    export.stream_id = stream_id;
    export.snapshot_id = snapshot_id.clone();
    for payload in &mut export.payloads {
        match payload {
            BroadcasterPayload::SnapshotStart(start) => {
                start.snapshot_id.clone_from(&snapshot_id);
            }
            BroadcasterPayload::SnapshotChunk(chunk) => {
                chunk.snapshot_id.clone_from(&snapshot_id);
            }
            BroadcasterPayload::SnapshotEnd(end) => {
                end.snapshot_id.clone_from(&snapshot_id);
            }
            BroadcasterPayload::RecoveryStart(_)
            | BroadcasterPayload::RecoveryChunk(_)
            | BroadcasterPayload::RecoveryCatchUp(_)
            | BroadcasterPayload::RecoveryCommit(_)
            | BroadcasterPayload::Update(_)
            | BroadcasterPayload::Heartbeat(_)
            | BroadcasterPayload::Progress(_) => {}
        }
    }
}

fn build_snapshot_artifact(
    chain_id: u64,
    export: BroadcasterSnapshotExport,
    redis_replay_boundary: BroadcasterRedisReplayBoundary,
) -> Result<(BroadcasterSnapshotArtifact, usize)> {
    anyhow::ensure!(
        export.stream_id == redis_replay_boundary.stream_id,
        "snapshot stream_id mismatch with Redis replay boundary"
    );
    anyhow::ensure!(
        export.snapshot_id == redis_replay_boundary.snapshot_id,
        "snapshot_id mismatch with Redis replay boundary"
    );
    let mut largest_payload_bytes = 0usize;
    let mut encoded_bytes = 0usize;
    let mut message_seq = 1u64;
    let payloads = export
        .payloads
        .into_iter()
        .map(|payload| {
            let envelope = BroadcasterEnvelope::new(export.stream_id.clone(), message_seq, payload);
            message_seq = message_seq.saturating_add(1);
            envelope
        })
        .collect::<Vec<_>>();
    for (index, envelope) in payloads.iter().enumerate() {
        let bytes = serde_json::to_vec(envelope)?.len();
        anyhow::ensure!(
            bytes <= export.max_payload_bytes,
            "snapshot payload {index} is {bytes} bytes, above configured max {}",
            export.max_payload_bytes
        );
        largest_payload_bytes = largest_payload_bytes.max(bytes);
        encoded_bytes = encoded_bytes.saturating_add(bytes);
    }
    let snapshot_chunk_count = payloads
        .iter()
        .filter(|envelope| matches!(envelope.payload, BroadcasterPayload::SnapshotChunk(_)))
        .count() as u32;
    Ok((
        BroadcasterSnapshotArtifact {
            chain_id,
            stream_id: export.stream_id,
            snapshot_id: export.snapshot_id,
            redis_replay_boundary,
            payloads: Arc::new(payloads),
            snapshot_chunk_count,
            encoded_bytes,
        },
        largest_payload_bytes,
    ))
}

fn artifact_matches_current_boundary(
    artifact: &BroadcasterSnapshotArtifact,
    current: &BroadcasterRedisReplayBoundary,
) -> bool {
    artifact.stream_id == current.stream_id
        && artifact.snapshot_id == current.snapshot_id
        && artifact.redis_replay_boundary.generation == current.generation
        && artifact.redis_replay_boundary.exclusive_message_seq <= current.exclusive_message_seq
}

async fn install_snapshot_artifact_if_current(
    services: &[BroadcasterServiceState],
    artifact: &Arc<BroadcasterSnapshotArtifact>,
    captured_recovery_work_ids: &[u64],
) -> Result<()> {
    let _gate = services[0].lifecycle_gate.lock().await;
    anyhow::ensure!(
        captured_recovery_work_ids.len() == services.len(),
        "snapshot artifact recovery fence count changed before publication"
    );
    let mut recovery_is_unchanged = true;
    for (service, captured_work_id) in services.iter().zip(captured_recovery_work_ids) {
        let recovery = service.recovery_publication.lock().await;
        recovery_is_unchanged &=
            recovery.work_id == *captured_work_id && !recovery.phase.is_active();
    }
    anyhow::ensure!(
        recovery_is_unchanged,
        "snapshot artifact crossed a recovery publication boundary"
    );
    let current_boundary = services[0]
        .redis_publisher
        .status_snapshot()
        .await
        .replay_boundary
        .ok_or_else(|| anyhow::anyhow!("active Redis replay boundary disappeared"))?;
    anyhow::ensure!(
        artifact_matches_current_boundary(artifact, &current_boundary),
        "snapshot artifact became stale before publication"
    );
    anyhow::ensure!(
        services[0]
            .redis_publisher
            .replay_boundary_is_retained(&artifact.redis_replay_boundary)
            .await?,
        "snapshot artifact replay boundary was trimmed before publication"
    );
    for service in services {
        *service.snapshot_artifact.write().await = Some(Arc::clone(artifact));
    }
    Ok(())
}

fn ensure_shared_lifecycle(services: &[BroadcasterServiceState], context: &str) -> Result<()> {
    anyhow::ensure!(
        services
            .iter()
            .all(|service| Arc::ptr_eq(&service.lifecycle_gate, &services[0].lifecycle_gate)),
        "{context} requires one lifecycle gate"
    );
    anyhow::ensure!(
        services
            .iter()
            .all(|service| Arc::ptr_eq(&service.redis_publisher, &services[0].redis_publisher)),
        "{context} requires one Redis publisher"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use anyhow::{anyhow, Result};
    use num_bigint::BigUint;
    use tokio::sync::{oneshot, Mutex};
    use tokio::time::{timeout, Duration};
    use tycho_simulation::tycho_common::dto::ProtocolStateDelta;
    use tycho_simulation::tycho_common::simulation::errors::{SimulationError, TransitionError};
    use tycho_simulation::{
        protocol::models::{ProtocolComponent, Update},
        tycho_client::feed::{
            synchronizer::{Snapshot, StateSyncMessage},
            BlockHeader, FeedMessage, SynchronizerState,
        },
        tycho_common::{
            dto::{Chain as DtoChain, ResponseAccount},
            models::{token::Token, Chain},
            simulation::protocol_sim::{Balances, GetAmountOutResult, ProtocolSim},
            Bytes,
        },
    };

    use super::{
        checkpoint_world_matches, retain_current_checkpoint_capture, BroadcasterServiceState,
        BroadcasterSnapshotSessionRegistry, RecoveryPublicationPhase, SnapshotSessionError,
    };
    use crate::broadcaster::redis_publisher::{
        BroadcasterRedisPublisher, BroadcasterRedisPublisherConfig, RedisStreamWriter,
    };
    use crate::broadcaster::state::{
        BroadcasterReadiness, BroadcasterSnapshotCache, BroadcasterSnapshotExport,
        BroadcasterUpstreamState,
    };
    use crate::broadcaster::state_history::{
        test_state_history_runtime, test_state_history_runtime_with_tokens, StateHistoryRuntime,
    };
    use simulator_core::broadcaster::{
        BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterMessageKind,
        BroadcasterPayload, BroadcasterProgress, BroadcasterProtocolSyncStatus,
        BroadcasterRedisStreamEntry, BroadcasterSnapshotEnd, BroadcasterSnapshotStart,
        BroadcasterUpdateMessage, BroadcasterUpdatePartition,
    };

    #[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
    struct DummySim(u8);

    struct CaptureDropProbe(Arc<AtomicBool>);

    impl Drop for CaptureDropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    #[typetag::serde(name = "BroadcasterServiceDummySim")]
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

    #[tokio::test]
    async fn stopping_recovery_tasks_waits_for_worker() {
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(ServiceFakeRedisWriter::default()),
        ));
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            publisher,
            Arc::new(Mutex::new(())),
        );
        let recovery = Arc::clone(&service.recovery_publication);
        let stopped = Arc::new(AtomicBool::new(false));
        let task_stopped = Arc::clone(&stopped);
        let worker = tokio::spawn(async move {
            while !recovery.lock().await.cancelled.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            task_stopped.store(true, Ordering::Release);
        });
        service.recovery_workers.lock().await.push(worker);

        service.stop_recovery_tasks().await;

        assert!(stopped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn recovery_routes_live_updates_through_buffer_a_then_publisher_gate() -> Result<()> {
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(ServiceFakeRedisWriter::default()),
        ));
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            Arc::new(Mutex::new(())),
        );
        {
            let mut recovery = service.recovery_publication.lock().await;
            recovery.work_id = 1;
            recovery.phase = super::RecoveryPublicationPhase::Building;
            recovery.started_at = tokio::time::Instant::now() - Duration::from_secs(30);
        }

        assert!(
            service
                .buffer_recovery_update(native_buffer_update(10)?)
                .await?
        );
        {
            let mut recovery = service.recovery_publication.lock().await;
            assert_eq!(recovery.buffer_a.len(), 1);
            recovery.phase = super::RecoveryPublicationPhase::Publishing;
        }

        let cancelled = Arc::new(AtomicBool::new(false));
        publisher
            .begin_recovery(&[BroadcasterBackend::Native], Arc::clone(&cancelled))
            .await;
        let buffer_b_update = native_buffer_update(11)?;
        assert!(
            !service
                .buffer_recovery_update(buffer_b_update.clone())
                .await?
        );
        publisher
            .publish_accepted_payload(BroadcasterPayload::Update(buffer_b_update))
            .await?;
        {
            let recovery = service.recovery_publication.lock().await;
            assert_eq!(recovery.buffer_a.len(), 1);
        }
        let status = service.recovery_status().await;
        assert!(status.active);
        assert_eq!(status.phase, "publishing");
        assert_eq!(status.buffer_a_count, 1);
        assert_eq!(status.buffer_b_count, 1);
        assert!(status.age_ms.is_some_and(|age_ms| age_ms >= 30_000));
        assert!(status
            .oldest_buffered_age_ms
            .is_some_and(|age_ms| age_ms < 1_000));
        Ok(())
    }

    #[tokio::test]
    async fn configured_native_block_limit_cancels_service_recovery_buffer() -> Result<()> {
        let mut publisher_config = publisher_config();
        publisher_config.recovery_max_buffered_native_blocks = 3;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            Arc::new(BroadcasterRedisPublisher::new(
                publisher_config,
                Arc::new(ServiceFakeRedisWriter::default()),
            )),
            Arc::new(Mutex::new(())),
        );
        {
            let mut recovery = service.recovery_publication.lock().await;
            recovery.work_id = 1;
            recovery.phase = super::RecoveryPublicationPhase::Building;
            recovery.started_at = tokio::time::Instant::now();
        }

        for block in 1..=3 {
            assert!(
                service
                    .buffer_recovery_update(native_buffer_update(block)?)
                    .await?
            );
        }

        let recovery = service.recovery_publication.lock().await;
        assert_eq!(recovery.complete_native_blocks.len(), 3);
        assert!(recovery.cancelled.load(Ordering::Acquire));
        Ok(())
    }

    #[tokio::test]
    async fn recovery_warns_at_thirty_seconds_and_cancels_at_sixty_seconds() -> Result<()> {
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            Arc::new(BroadcasterRedisPublisher::new(
                publisher_config(),
                Arc::new(ServiceFakeRedisWriter::default()),
            )),
            Arc::new(Mutex::new(())),
        );
        {
            let mut recovery = service.recovery_publication.lock().await;
            recovery.work_id = 1;
            recovery.phase = super::RecoveryPublicationPhase::Building;
            recovery.started_at = tokio::time::Instant::now() - super::RECOVERY_WARN_AFTER;
        }
        service
            .buffer_recovery_update(native_buffer_update(10)?)
            .await?;
        assert!(service.recovery_publication.lock().await.warned);

        {
            let mut recovery = service.recovery_publication.lock().await;
            recovery.started_at = tokio::time::Instant::now() - super::RECOVERY_ABORT_AFTER;
            recovery.cancelled.store(false, Ordering::Release);
        }
        service.spawn_recovery_bounds_monitor(1).await;
        timeout(Duration::from_secs(1), async {
            while !service
                .recovery_publication
                .lock()
                .await
                .cancelled
                .load(Ordering::Acquire)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| anyhow!("60-second watchdog did not cancel recovery"))?;
        Ok(())
    }

    #[tokio::test]
    async fn second_local_recovery_failure_self_fences() -> Result<()> {
        let service = ready_service().await?;
        {
            let mut recovery = service.recovery_publication.lock().await;
            recovery.work_id = 1;
            recovery.phase = super::RecoveryPublicationPhase::Building;
            recovery.local_failures = super::RECOVERY_MAX_LOCAL_FAILURES;
        }

        let retry = service
            .handle_local_recovery_failure(1, "second bounded attempt failed".to_string())
            .await;

        assert!(retry.is_none());
        assert_eq!(
            service.redis_publisher.mode().await,
            super::BroadcasterRedisPublisherMode::Retired
        );
        assert_eq!(
            service.recovery_publication.lock().await.phase,
            super::RecoveryPublicationPhase::Fatal
        );
        Ok(())
    }

    #[tokio::test]
    async fn external_recovery_failure_does_not_consume_local_attempt_allowance() -> Result<()> {
        let service = ready_service().await?;
        {
            let mut recovery = service.recovery_publication.lock().await;
            recovery.work_id = 1;
            recovery.phase = super::RecoveryPublicationPhase::Publishing;
            recovery.local_failures = 0;
        }

        let retry = service
            .handle_external_recovery_failure(1, "planned Redis outage".to_string())
            .await;

        assert!(retry.is_some());
        assert_eq!(service.recovery_publication.lock().await.local_failures, 0);
        Ok(())
    }

    #[tokio::test]
    async fn oversized_live_entry_enters_chunked_recovery_without_ordinary_retry() -> Result<()> {
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        publisher
            .promote(base_heads([BroadcasterBackend::Native]), "test_active")
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            publisher,
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        assert!(service.apply_feed_message(&raw_feed(10, 10, 9)).await?);
        let ordinary_updates_before = writer
            .appends()
            .await
            .iter()
            .filter(|entry| entry.kind == BroadcasterMessageKind::Update)
            .count();

        let oversized = raw_feed_with_account_payload(11, 11, 10, "x".repeat(9 * 1024 * 1024));
        assert!(service.apply_feed_message(&oversized).await?);

        assert!(service.cache.replacement_pending().await);
        assert_eq!(
            writer
                .appends()
                .await
                .iter()
                .filter(|entry| entry.kind == BroadcasterMessageKind::Update)
                .count(),
            ordinary_updates_before,
            "the oversized ordinary entry must not be retried"
        );
        Ok(())
    }

    #[tokio::test]
    async fn retained_snapshot_artifact_remains_available_during_recovery() -> Result<()> {
        let service = ready_service().await?;
        {
            let mut recovery = service.recovery_publication.lock().await;
            recovery.work_id = 1;
            recovery.phase = super::RecoveryPublicationPhase::Building;
            recovery.started_at = tokio::time::Instant::now();
        }

        let session = service
            .create_snapshot_session(Duration::from_secs(300))
            .await?
            .ok_or_else(|| anyhow!("retained artifact should remain replayable during recovery"))?;

        assert_eq!(session.redis_replay_boundary.exclusive_message_seq, 2);
        assert!(service.snapshot_artifact.read().await.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_refresh_rejects_artifact_after_recovery_epoch_changes() -> Result<()> {
        let service = ready_service().await?;
        let artifact = service
            .snapshot_artifact
            .write()
            .await
            .take()
            .ok_or_else(|| anyhow!("ready service should have a snapshot artifact"))?;
        let captured_work_id = service.recovery_publication.lock().await.work_id;
        {
            let mut recovery = service.recovery_publication.lock().await;
            recovery.work_id = recovery.work_id.saturating_add(1);
        }

        let Err(error) = super::install_snapshot_artifact_if_current(
            std::slice::from_ref(&service),
            &artifact,
            &[captured_work_id],
        )
        .await
        else {
            return Err(anyhow!(
                "snapshot captured before recovery must not replace the retained artifact"
            ));
        };

        assert!(error
            .to_string()
            .contains("crossed a recovery publication boundary"));
        assert!(service.snapshot_artifact.read().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_refresh_fences_every_shared_publisher_service() -> Result<()> {
        let primary = ready_service().await?;
        let secondary = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Rfq]),
            BroadcasterUpstreamState::default(),
            Arc::clone(&primary.redis_publisher),
            Arc::clone(&primary.lifecycle_gate),
        );
        let artifact = primary
            .snapshot_artifact
            .write()
            .await
            .take()
            .ok_or_else(|| anyhow!("ready service should have a snapshot artifact"))?;
        let captured_work_ids = vec![
            primary.recovery_publication.lock().await.work_id,
            secondary.recovery_publication.lock().await.work_id,
        ];
        {
            let mut recovery = secondary.recovery_publication.lock().await;
            recovery.work_id = recovery.work_id.saturating_add(1);
        }

        let Err(error) = super::install_snapshot_artifact_if_current(
            &[primary.clone(), secondary.clone()],
            &artifact,
            &captured_work_ids,
        )
        .await
        else {
            return Err(anyhow!(
                "snapshot must not cross a sibling service recovery boundary"
            ));
        };

        assert!(error
            .to_string()
            .contains("crossed a recovery publication boundary"));
        assert!(primary.snapshot_artifact.read().await.is_none());
        assert!(secondary.snapshot_artifact.read().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn local_recovery_retry_waits_before_capturing_superseding_source() -> Result<()> {
        let service = ready_service().await?;
        {
            let mut recovery = service.recovery_publication.lock().await;
            recovery.work_id = 1;
            recovery.phase = super::RecoveryPublicationPhase::Building;
            recovery.started_at = tokio::time::Instant::now();
        }

        let mut retry = tokio::spawn({
            let service = service.clone();
            async move {
                service
                    .handle_local_recovery_failure(1, "planned local failure".to_string())
                    .await
            }
        });
        assert!(
            timeout(Duration::from_millis(50), &mut retry)
                .await
                .is_err(),
            "the superseding source must not be captured before bounded backoff"
        );
        let source = timeout(Duration::from_secs(1), retry)
            .await
            .map_err(|_| anyhow!("superseding recovery did not start after bounded backoff"))?
            .map_err(|error| anyhow!("retry task failed: {error}"))?;
        assert!(source.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn recovery_worker_termination_self_fences_publication() -> Result<()> {
        let service = ready_service().await?;
        {
            let mut recovery = service.recovery_publication.lock().await;
            recovery.work_id = 1;
            recovery.phase = super::RecoveryPublicationPhase::Publishing;
        }

        service
            .handle_recovery_worker_termination(1, "planned worker termination".to_string())
            .await;

        assert_eq!(
            service.redis_publisher.mode().await,
            super::BroadcasterRedisPublisherMode::Retired
        );
        let recovery = service.recovery_publication.lock().await;
        assert_eq!(recovery.phase, super::RecoveryPublicationPhase::Fatal);
        assert_eq!(
            recovery.fatal_error.as_deref(),
            Some("planned worker termination")
        );
        Ok(())
    }

    #[tokio::test]
    async fn uncertain_recovery_commit_self_fences_without_starting_another_attempt() -> Result<()>
    {
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        publisher
            .promote(base_heads([BroadcasterBackend::Native]), "test_active")
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        assert!(service.apply_feed_message(&raw_feed(10, 10, 9)).await?);
        BroadcasterServiceState::run_snapshot_export_preflight(std::slice::from_ref(&service))
            .await?;
        let Err(gap_error) = service.apply_feed_message(&raw_feed(12, 12, 11)).await else {
            return Err(anyhow!("header gap must force reconnect"));
        };
        service
            .mark_stream_disconnected("gap", Some(gap_error.to_string()))
            .await;
        service.mark_upstream_connected().await;
        writer.lose_recovery_commit_replies().await;
        assert!(service.apply_feed_message(&raw_feed(12, 12, 11)).await?);

        timeout(Duration::from_secs(1), async {
            while publisher.mode().await != super::BroadcasterRedisPublisherMode::Retired {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| anyhow!("uncertain commit did not self-fence the publisher"))?;

        let appends = writer.appends().await;
        assert_eq!(
            appends
                .iter()
                .filter(|entry| entry.kind == BroadcasterMessageKind::RecoveryStart)
                .count(),
            1,
            "an uncertain authoritative commit must not start another transaction"
        );
        assert_eq!(
            appends
                .iter()
                .filter(|entry| entry.kind == BroadcasterMessageKind::RecoveryCommit)
                .count(),
            1,
            "the fake writer records the authoritative commit before losing its reply"
        );
        Ok(())
    }

    #[tokio::test]
    async fn state_history_recovery_commit_without_slip_records_no_boundary_slip_gap() -> Result<()>
    {
        let writer = ServiceFakeRedisWriter::default();
        let (state_history, probe) = test_state_history_runtime(8);
        let publisher = Arc::new(
            BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer))
                .with_state_history(Arc::clone(&state_history)),
        );
        publisher
            .promote(base_heads([BroadcasterBackend::Native]), "test_active")
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            publisher,
            Arc::new(Mutex::new(())),
        );
        let mut boundary_requests = state_history.take_boundary_request_receiver()?;
        let checkpoint_service = service.clone();
        let (position_sender, position_receiver) = oneshot::channel();
        let checkpoint_task = tokio::spawn(async move {
            let Some(request) = boundary_requests.recv().await else {
                return;
            };
            let _ = position_sender.send(request.position);
            let result = BroadcasterServiceState::capture_state_history_checkpoint(
                std::slice::from_ref(&checkpoint_service),
                state_history::CheckpointKind::Boundary,
                Some(request.position),
                request.complete_native_block,
                request.rfq_high_water_ms,
            )
            .await;
            StateHistoryRuntime::respond_to_boundary_request(request, result);
        });

        service.mark_upstream_connected().await;
        assert!(service.apply_feed_message(&raw_feed(10, 10, 9)).await?);
        BroadcasterServiceState::run_snapshot_export_preflight(std::slice::from_ref(&service))
            .await?;
        let Err(gap_error) = service.apply_feed_message(&raw_feed(12, 12, 11)).await else {
            return Err(anyhow!("header gap must force reconnect"));
        };
        service
            .mark_stream_disconnected("gap", Some(gap_error.to_string()))
            .await;
        service.mark_upstream_connected().await;
        assert!(service.apply_feed_message(&raw_feed(12, 12, 11)).await?);

        timeout(Duration::from_secs(2), async {
            while probe.checkpoints().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| anyhow!("recovery boundary checkpoint was not requested"))?;
        checkpoint_task.await?;
        let checkpoints = probe.checkpoints();
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(
            checkpoints[0].kind(),
            state_history::CheckpointKind::Boundary
        );
        let capture_boundary = service.redis_publisher.replay_boundary().await?;
        assert_eq!(
            checkpoints[0].state_version(),
            Some(capture_boundary.state_version)
        );
        let requested = position_receiver
            .await
            .map_err(|_| anyhow!("recovery boundary checkpoint position was not captured"))?;
        assert_eq!(checkpoints[0].position(), requested);
        assert!(probe
            .gaps()
            .iter()
            .all(|gap| gap.reason != state_history::GapReason::BoundarySlip));
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_capture_enqueues_the_catalog_without_publishing() -> Result<()> {
        let writer = ServiceFakeRedisWriter::default();
        let token = dummy_token(7, "SEVEN");
        let (state_history, probe) = test_state_history_runtime_with_tokens(8, vec![token.clone()]);
        let publisher = Arc::new(
            BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()))
                .with_state_history(state_history),
        );
        publisher
            .promote(base_heads([BroadcasterBackend::Native]), "test_active")
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            publisher,
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        service
            .apply_update(&native_only_update(10, "native-1"))
            .await?;
        BroadcasterServiceState::run_snapshot_export_preflight(std::slice::from_ref(&service))
            .await?;
        let published_before_capture = writer.appends().await.len();

        let completed = BroadcasterServiceState::capture_state_history_checkpoint(
            std::slice::from_ref(&service),
            state_history::CheckpointKind::Interval,
            None,
            None,
            None,
        )
        .await?;

        assert_eq!(completed, Some(10));
        timeout(Duration::from_secs(2), async {
            while probe.token_snapshots().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| anyhow!("checkpoint token catalog was not enqueued"))?;
        let token_snapshots = probe.token_snapshots();
        assert_eq!(token_snapshots.len(), 1);
        assert_eq!(token_snapshots[0].chain_id, 1);
        assert_eq!(token_snapshots[0].tokens.len(), 1);
        assert_eq!(token_snapshots[0].tokens[0].address, token.address);
        assert_eq!(writer.appends().await.len(), published_before_capture);
        assert_eq!(
            service.status_snapshot().await.readiness,
            BroadcasterReadiness::Ready
        );
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_capture_clones_the_live_token_catalog() -> Result<()> {
        // Pins that captures read the shared catalog store at capture time, not a
        // copy taken when the runtime was built. A token folded from the stream
        // after startup must reach the next capture's token snapshot.
        let writer = ServiceFakeRedisWriter::default();
        let store = Arc::new(crate::models::tokens::TokenStore::local_only(
            Default::default(),
            tycho_simulation::tycho_common::models::Chain::Ethereum,
            Duration::from_secs(1),
        ));
        let (state_history, probe) =
            super::super::state_history::test_state_history_runtime_with_store(
                8,
                Arc::clone(&store),
            );
        let publisher = Arc::new(
            BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()))
                .with_state_history(state_history),
        );
        publisher
            .promote(base_heads([BroadcasterBackend::Native]), "test_active")
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            publisher,
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        service
            .apply_update(&native_only_update(10, "native-1"))
            .await?;
        BroadcasterServiceState::run_snapshot_export_preflight(std::slice::from_ref(&service))
            .await?;

        let token = dummy_token(9, "NINE");
        store.insert_batch([token.clone()]).await;

        let completed = BroadcasterServiceState::capture_state_history_checkpoint(
            std::slice::from_ref(&service),
            state_history::CheckpointKind::Interval,
            None,
            None,
            None,
        )
        .await?;

        assert_eq!(completed, Some(10));
        timeout(Duration::from_secs(2), async {
            while probe.token_snapshots().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| anyhow!("checkpoint token catalog was not enqueued"))?;
        let token_snapshots = probe.token_snapshots();
        assert_eq!(token_snapshots.len(), 1);
        assert_eq!(token_snapshots[0].tokens.len(), 1);
        assert_eq!(token_snapshots[0].tokens[0].address, token.address);
        Ok(())
    }

    #[tokio::test]
    async fn state_history_recovery_commit_records_same_generation_boundary_slip_gap() -> Result<()>
    {
        let writer = ServiceFakeRedisWriter::default();
        let (state_history, probe) = test_state_history_runtime(8);
        let publisher = Arc::new(
            BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer))
                .with_state_history(Arc::clone(&state_history)),
        );
        publisher
            .promote(base_heads([BroadcasterBackend::Native]), "test_active")
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            publisher,
            Arc::new(Mutex::new(())),
        );
        let mut boundary_requests = state_history.take_boundary_request_receiver()?;
        let checkpoint_service = service.clone();
        let (request_sender, request_receiver) = oneshot::channel();
        let checkpoint_task = tokio::spawn(async move {
            let Some(request) = boundary_requests.recv().await else {
                return;
            };
            let _ = request_sender.send((request.position, request.complete_native_block));
            let result = async {
                checkpoint_service
                    .apply_update(&native_only_update(13, "native-slip"))
                    .await?;
                BroadcasterServiceState::capture_state_history_checkpoint(
                    std::slice::from_ref(&checkpoint_service),
                    state_history::CheckpointKind::Boundary,
                    Some(request.position),
                    request.complete_native_block,
                    request.rfq_high_water_ms,
                )
                .await
            }
            .await;
            StateHistoryRuntime::respond_to_boundary_request(request, result);
        });

        service.mark_upstream_connected().await;
        assert!(service.apply_feed_message(&raw_feed(10, 10, 9)).await?);
        BroadcasterServiceState::run_snapshot_export_preflight(std::slice::from_ref(&service))
            .await?;
        let Err(gap_error) = service.apply_feed_message(&raw_feed(12, 12, 11)).await else {
            return Err(anyhow!("header gap must force reconnect"));
        };
        service
            .mark_stream_disconnected("gap", Some(gap_error.to_string()))
            .await;
        service.mark_upstream_connected().await;
        assert!(service.apply_feed_message(&raw_feed(12, 12, 11)).await?);

        let (requested, requested_block_number) = timeout(Duration::from_secs(2), request_receiver)
            .await
            .map_err(|_| anyhow!("recovery boundary checkpoint was not requested"))??;
        timeout(Duration::from_secs(2), async {
            while probe.checkpoints().is_empty() || probe.gaps().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| anyhow!("recovery boundary slip was not recorded"))?;
        checkpoint_task.await?;

        let checkpoints = probe.checkpoints();
        assert_eq!(checkpoints.len(), 1);
        let capture_boundary = service.redis_publisher.replay_boundary().await?;
        assert_eq!(
            checkpoints[0].state_version(),
            Some(capture_boundary.state_version)
        );
        let capture_position = checkpoints[0].position();
        let expected_from_message_seq = requested
            .message_seq
            .checked_add(1)
            .ok_or_else(|| anyhow!("test recovery position overflowed"))?;
        assert_eq!(capture_position.generation, requested.generation);
        assert!(capture_position.message_seq > requested.message_seq);

        let gaps = probe.gaps();
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].chain_id, 1);
        assert_eq!(gaps[0].generation, requested.generation);
        assert_eq!(gaps[0].from_message_seq, expected_from_message_seq);
        assert_eq!(gaps[0].to_message_seq, capture_position.message_seq);
        assert_eq!(gaps[0].reason, state_history::GapReason::BoundarySlip);
        assert_eq!(gaps[0].from_block_number, requested_block_number);
        assert_eq!(requested_block_number, Some(12));
        assert_eq!(gaps[0].to_block_number, Some(13));
        assert_eq!(gaps[0].from_observed_at_ms, None);
        assert_eq!(gaps[0].to_observed_at_ms, None);
        Ok(())
    }

    #[tokio::test]
    async fn state_history_slip_gap_with_unknown_request_cursors_is_unbounded_below() {
        let (state_history, probe) = test_state_history_runtime(8);
        super::record_boundary_slip_gap_if_any(
            &state_history.handle,
            1,
            Some(super::BoundaryCheckpointCursor {
                position: state_history::StreamPosition {
                    generation: 3,
                    message_seq: 5,
                },
                block_number: None,
                rfq_observed_at_ms: None,
            }),
            super::BoundaryCheckpointCursor {
                position: state_history::StreamPosition {
                    generation: 3,
                    message_seq: 8,
                },
                block_number: Some(120),
                rfq_observed_at_ms: Some(9_000),
            },
        );

        let gaps = probe.gaps();
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].from_message_seq, 6);
        assert_eq!(gaps[0].to_message_seq, 8);
        assert_eq!(gaps[0].from_block_number, Some(0));
        assert_eq!(gaps[0].to_block_number, Some(120));
        assert_eq!(gaps[0].from_observed_at_ms, Some(0));
        assert_eq!(gaps[0].to_observed_at_ms, Some(9_000));
    }

    #[test]
    fn checkpoint_capture_accepts_message_append_during_export() {
        let mut pinned = replay_boundary();
        pinned.exclusive_message_seq = 7;
        let mut after_append = pinned.clone();
        after_append.exclusive_message_seq = 8;

        assert!(checkpoint_world_matches(&pinned, &after_append));

        after_append.snapshot_id = "moved-snapshot".to_owned();
        assert!(!checkpoint_world_matches(&pinned, &after_append));
    }

    #[test]
    fn moved_world_discards_state_and_token_work_together() {
        let pinned = replay_boundary();
        let mut moved = pinned.clone();
        moved.snapshot_id = "moved-snapshot".to_owned();
        let token_work_dropped = Arc::new(AtomicBool::new(false));

        let retained = retain_current_checkpoint_capture(
            &pinned,
            Some(&moved),
            true,
            ((), CaptureDropProbe(Arc::clone(&token_work_dropped))),
        );

        assert!(retained.is_none());
        assert!(token_work_dropped.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn boundary_checkpoint_uses_pin_time_position_after_slipped_update() -> Result<()> {
        let writer = ServiceFakeRedisWriter::default();
        let (state_history, probe) = test_state_history_runtime(8);
        let publisher = Arc::new(
            BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer))
                .with_state_history(state_history),
        );
        let promotion_boundary = publisher
            .promote(base_heads([BroadcasterBackend::Native]), "test_active")
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        service
            .apply_update(&native_only_update(10, "native-1"))
            .await?;
        BroadcasterServiceState::run_snapshot_export_preflight(std::slice::from_ref(&service))
            .await?;
        let pin_time_boundary = publisher.replay_boundary().await?;

        let completed = BroadcasterServiceState::capture_state_history_checkpoint(
            std::slice::from_ref(&service),
            state_history::CheckpointKind::Boundary,
            Some(state_history::StreamPosition {
                generation: promotion_boundary.generation,
                message_seq: promotion_boundary.exclusive_message_seq,
            }),
            Some(10),
            None,
        )
        .await?;

        assert_eq!(completed, Some(10));
        timeout(Duration::from_secs(2), async {
            while probe.checkpoints().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| anyhow!("pin-time boundary checkpoint was not recorded"))?;
        let checkpoints = probe.checkpoints();
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(
            checkpoints[0].position(),
            state_history::StreamPosition {
                generation: pin_time_boundary.generation,
                message_seq: pin_time_boundary.exclusive_message_seq,
            }
        );
        assert_eq!(
            checkpoints[0].state_version(),
            Some(pin_time_boundary.state_version)
        );
        assert!(pin_time_boundary.exclusive_message_seq > promotion_boundary.exclusive_message_seq);
        Ok(())
    }

    #[tokio::test]
    async fn recovery_commit_discard_records_boundary_failure_gap() -> Result<()> {
        let writer = ServiceFakeRedisWriter::default();
        let (state_history, probe) = test_state_history_runtime(8);
        let publisher = Arc::new(
            BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer))
                .with_state_history(Arc::clone(&state_history)),
        );
        publisher
            .promote(base_heads([BroadcasterBackend::Native]), "test_active")
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            publisher,
            Arc::new(Mutex::new(())),
        );
        let mut boundary_requests = state_history.take_boundary_request_receiver()?;
        let (position_sender, position_receiver) = oneshot::channel();
        let checkpoint_task = tokio::spawn(async move {
            let Some(request) = boundary_requests.recv().await else {
                return;
            };
            let _ = position_sender.send(request.position);
            StateHistoryRuntime::respond_to_boundary_request(request, Ok(None));
        });

        service.mark_upstream_connected().await;
        assert!(service.apply_feed_message(&raw_feed(10, 10, 9)).await?);
        BroadcasterServiceState::run_snapshot_export_preflight(std::slice::from_ref(&service))
            .await?;
        let Err(gap_error) = service.apply_feed_message(&raw_feed(12, 12, 11)).await else {
            return Err(anyhow!("header gap must force reconnect"));
        };
        service
            .mark_stream_disconnected("gap", Some(gap_error.to_string()))
            .await;
        service.mark_upstream_connected().await;
        assert!(service.apply_feed_message(&raw_feed(12, 12, 11)).await?);

        let commit_position = timeout(Duration::from_secs(2), position_receiver)
            .await
            .map_err(|_| anyhow!("recovery boundary checkpoint was not requested"))??;
        timeout(Duration::from_secs(2), async {
            while probe.gaps().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| anyhow!("recovery boundary failure gap was not recorded"))?;
        checkpoint_task.await?;

        let gaps = probe.gaps();
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].chain_id, 1);
        assert_eq!(gaps[0].generation, commit_position.generation);
        assert_eq!(gaps[0].from_message_seq, commit_position.message_seq);
        assert_eq!(gaps[0].to_message_seq, commit_position.message_seq);
        assert_eq!(gaps[0].reason, state_history::GapReason::CheckpointFailed);
        assert_eq!(gaps[0].from_block_number, Some(12));
        assert_eq!(gaps[0].to_block_number, Some(12));
        assert_eq!(gaps[0].from_observed_at_ms, None);
        assert_eq!(gaps[0].to_observed_at_ms, None);
        Ok(())
    }

    async fn wait_for_private_recovery(writer: &ServiceFakeRedisWriter) -> Result<()> {
        timeout(Duration::from_secs(1), async {
            loop {
                let kinds = writer
                    .appends()
                    .await
                    .into_iter()
                    .map(|entry| entry.kind)
                    .collect::<Vec<_>>();
                if kinds.contains(&BroadcasterMessageKind::RecoveryChunk)
                    && !kinds.contains(&BroadcasterMessageKind::RecoveryCommit)
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| anyhow!("recovery did not reach its private pre-commit phase"))
    }

    async fn wait_for_committed_recovery(
        service: &BroadcasterServiceState,
        complete_native_block: u64,
        wait_limit: Duration,
    ) -> Result<()> {
        timeout(wait_limit, async {
            loop {
                let committed = {
                    let recovery = service.recovery_publication.lock().await;
                    recovery.phase == RecoveryPublicationPhase::Idle
                        && recovery.last_outcome == Some("committed")
                };
                // The worker records progress after it releases the recovery lock.
                if committed
                    && service.upstream.last_native_block().await == Some(complete_native_block)
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| {
            anyhow!("recovery did not commit and publish native block {complete_native_block}")
        })
    }

    #[tokio::test]
    async fn recovery_commit_renews_progress_after_a_slow_publication() -> Result<()> {
        let writer = ServiceFakeRedisWriter::default();
        let mut config = publisher_config();
        config.append_retry_window = Duration::from_secs(7);
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            config,
            Arc::new(writer.clone()),
        ));
        publisher
            .promote(base_heads([BroadcasterBackend::Native]), "test_active")
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        assert!(service.apply_feed_message(&raw_feed(10, 10, 9)).await?);
        BroadcasterServiceState::run_snapshot_export_preflight(std::slice::from_ref(&service))
            .await?;
        let Err(gap_error) = service.apply_feed_message(&raw_feed(12, 12, 11)).await else {
            return Err(anyhow!("header gap must force reconnect"));
        };
        service
            .mark_stream_disconnected("gap", Some(gap_error.to_string()))
            .await;
        service.mark_upstream_connected().await;
        writer
            .delay_recovery_commit(Duration::from_millis(5_100))
            .await;
        assert!(service.apply_feed_message(&raw_feed(12, 12, 11)).await?);
        wait_for_private_recovery(&writer).await?;
        assert!(
            service
                .create_snapshot_session(Duration::from_secs(300))
                .await?
                .is_some(),
            "a retained artifact should bootstrap while recovery entries remain private"
        );
        assert!(
            service
                .apply_feed_message(&partial_raw_feed(13, 13, 12))
                .await?
        );

        wait_for_committed_recovery(&service, 12, Duration::from_secs(7)).await?;
        assert!(
            service
                .upstream
                .snapshot()
                .await
                .last_update_age_ms
                .is_some_and(|age_ms| age_ms < 500),
            "the five-second lease must start at successful recovery publication"
        );
        assert_eq!(
            service.upstream.last_native_block().await,
            Some(12),
            "a partial catch-up must not replace the complete recovery progress block"
        );
        Ok(())
    }

    #[tokio::test]
    async fn service_status_snapshot_responds_during_slow_recovery_publication() -> Result<()> {
        let writer = ServiceFakeRedisWriter::default();
        let (service, _publisher) = ready_feed_service_with_writer(writer.clone()).await?;
        writer.delay_recovery_commit(Duration::from_secs(1)).await;
        let Err(gap_error) = service.apply_feed_message(&raw_feed(12, 12, 11)).await else {
            return Err(anyhow!("header gap must force reconnect"));
        };
        service
            .mark_stream_disconnected("gap", Some(gap_error.to_string()))
            .await;
        service.mark_upstream_connected().await;
        assert!(service.apply_feed_message(&raw_feed(12, 12, 11)).await?);
        wait_for_private_recovery(&writer).await?;

        let status = timeout(Duration::from_millis(100), service.status_snapshot())
            .await
            .map_err(|_| anyhow!("service status snapshot blocked on recovery publication"))?;
        assert!(status.recovery.active);

        timeout(Duration::from_secs(2), async {
            while service.recovery_status().await.active {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| anyhow!("slow recovery did not complete"))?;
        Ok(())
    }

    #[tokio::test]
    async fn disconnect_mid_replacement_realigns_after_reconnect() -> Result<()> {
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            Arc::new(BroadcasterRedisPublisher::new(
                publisher_config(),
                Arc::new(ServiceFakeRedisWriter::default()),
            )),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        assert!(
            service
                .apply_feed_message(&raw_feed_for_protocols(
                    10,
                    10,
                    9,
                    &["uniswap_v2", "uniswap_v3"],
                ))
                .await?
        );

        let Err(gap_error) = service.apply_feed_message(&raw_feed(12, 12, 11)).await else {
            return Err(anyhow!("header gap must force reconnect"));
        };
        service
            .mark_stream_disconnected("gap", Some(gap_error.to_string()))
            .await;
        service.mark_upstream_connected().await;
        assert!(!service.apply_feed_message(&raw_feed(13, 13, 12)).await?);

        service.mark_stream_disconnected("stream ended", None).await;
        service.mark_upstream_connected().await;
        let fresh_result = service
            .apply_feed_message(&raw_feed_for_protocols(
                15,
                15,
                14,
                &["uniswap_v2", "uniswap_v3"],
            ))
            .await;
        assert!(
            fresh_result.is_ok(),
            "fresh replacement must realign after reconnect: {fresh_result:?}"
        );
        assert!(fresh_result?);
        Ok(())
    }

    #[tokio::test]
    async fn reconnect_after_gap_publishes_recovery_in_same_generation() -> Result<()> {
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        publisher
            .promote(base_heads([BroadcasterBackend::Native]), "test_active")
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;

        assert!(service.apply_feed_message(&raw_feed(10, 10, 9)).await?);
        BroadcasterServiceState::run_snapshot_export_preflight(std::slice::from_ref(&service))
            .await?;
        assert!(service.snapshot_artifact.read().await.is_some());
        let Err(gap_error) = service.apply_feed_message(&raw_feed(12, 12, 11)).await else {
            return Err(anyhow!(
                "header gap must force reconnect before replacement"
            ));
        };
        service
            .mark_stream_disconnected("gap", Some(gap_error.to_string()))
            .await;
        service.mark_upstream_connected().await;
        assert!(service.apply_feed_message(&raw_feed(12, 12, 11)).await?);
        let recovery_wait = wait_for_committed_recovery(&service, 12, Duration::from_secs(1)).await;
        if recovery_wait.is_err() {
            let (phase, fatal_error) = {
                let recovery = service.recovery_publication.lock().await;
                (recovery.phase, recovery.fatal_error.clone())
            };
            let append_kinds = writer
                .appends()
                .await
                .into_iter()
                .map(|entry| entry.kind)
                .collect::<Vec<_>>();
            return Err(anyhow!(
                "recovery did not commit: phase={:?}, fatal={:?}, appends={:?}",
                phase,
                fatal_error,
                append_kinds
            ));
        }

        let appends = writer.appends().await;
        let kinds = appends.iter().map(|entry| entry.kind).collect::<Vec<_>>();
        assert!(kinds.contains(&BroadcasterMessageKind::RecoveryStart));
        assert!(kinds.contains(&BroadcasterMessageKind::RecoveryChunk));
        assert!(kinds.contains(&BroadcasterMessageKind::RecoveryCommit));
        assert!(appends
            .iter()
            .all(|entry| entry.stream_id == "chain-1-stream-1"));
        assert!(service.snapshot_artifact.read().await.is_some());
        assert!(
            service
                .create_snapshot_session(Duration::from_secs(300))
                .await?
                .is_some(),
            "the retained artifact should admit bootstrap immediately after commit"
        );
        assert!(!service.cache.replacement_pending().await);
        service.mark_stream_disconnected("ended", None).await;
        service.mark_upstream_connected().await;
        assert!(
            service.cache.replacement_pending().await,
            "a later reconnect must prepare a new replacement"
        );
        Ok(())
    }

    #[tokio::test]
    async fn passive_service_warms_cache_without_redis_appends_or_snapshot_sessions() -> Result<()>
    {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            BroadcasterUpstreamState::default(),
            publisher,
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;

        service
            .apply_update(&native_only_update(10, "native-1"))
            .await?;

        let status = service.status_snapshot().await;
        assert!(status.snapshot.ready);
        assert_eq!(status.snapshot.total_states, 1);
        assert!(
            writer.appends().await.is_empty(),
            "passive warmup must update only the local cache"
        );
        assert!(
            service
                .create_snapshot_session(Duration::from_secs(300))
                .await?
                .is_none(),
            "passive publishers must not serve snapshot sessions"
        );
        Ok(())
    }

    #[tokio::test]
    async fn promotion_preserves_warmed_cache_and_enables_snapshot_sessions() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        service
            .apply_update(&native_only_update(10, "native-1"))
            .await?;

        let boundary = BroadcasterServiceState::promote_when_ready(
            std::slice::from_ref(&service),
            "active_writer_promoted",
        )
        .await?
        .ok_or_else(|| anyhow!("ready passive service should promote"))?;

        assert_eq!(boundary.exclusive_message_seq, 1);
        let status = service.status_snapshot().await;
        assert!(status.snapshot.ready);
        assert_eq!(status.snapshot.total_states, 1);
        assert_eq!(status.snapshot.stream_id, boundary.stream_id);
        assert_eq!(status.snapshot.snapshot_id, boundary.snapshot_id);
        assert!(matches!(
            status.snapshot.payload_limit_utilization_bps,
            Some(1..=10_000)
        ));
        assert_eq!(publisher.status_snapshot().await.mode, "active");

        let session = service
            .create_snapshot_session(Duration::from_secs(300))
            .await?
            .ok_or_else(|| anyhow!("active promoted broadcaster should serve a session"))?;
        assert_eq!(session.redis_replay_boundary, boundary);
        assert_eq!(writer.appends().await.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn promotion_preflight_failure_surfaces_degraded_export_status() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        let service = BroadcasterServiceState::with_lifecycle_gate(
            64,
            cache,
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        service
            .apply_update(&native_only_update(10, "native-1"))
            .await?;

        let promotion = BroadcasterServiceState::promote_when_ready(
            std::slice::from_ref(&service),
            "active_writer_promoted",
        )
        .await;

        assert!(promotion.is_err());
        let status = service.status_snapshot().await;
        assert_eq!(status.readiness, BroadcasterReadiness::SnapshotUnexportable);
        assert!(!status.snapshot.exportable);
        assert!(status.snapshot.last_export_error.is_some());
        assert!(writer.appends().await.is_empty());
        assert_eq!(publisher.status_snapshot().await.mode, "passive");
        Ok(())
    }

    #[tokio::test]
    async fn periodic_preflight_skips_warming_cache_without_recording_failure() -> Result<()> {
        let service = BroadcasterServiceState::with_lifecycle_gate(
            64,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            Arc::new(BroadcasterRedisPublisher::new(
                publisher_config(),
                Arc::new(ServiceFakeRedisWriter::default()),
            )),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;

        BroadcasterServiceState::run_snapshot_export_preflight(std::slice::from_ref(&service))
            .await?;

        let status = service.status_snapshot().await;
        assert_eq!(status.readiness, BroadcasterReadiness::SnapshotWarmingUp);
        assert!(status.snapshot.last_export_check_age_ms.is_none());
        assert!(status.snapshot.last_export_error.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn promotion_marker_declares_warmed_cache_backends() -> Result<()> {
        let writer = ServiceFakeRedisWriter::default();
        let old_publisher =
            BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
        old_publisher
            .promote(base_heads([BroadcasterBackend::Native]), "old_active")
            .await?;
        let old_cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let old_update = old_cache
            .apply_update(&native_only_update(10, "old-native"))
            .await?;
        old_publisher
            .publish_accepted_payload(BroadcasterPayload::Update(old_update))
            .await?;

        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        let lifecycle_gate = Arc::new(Mutex::new(()));
        let raw_service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            Arc::clone(&lifecycle_gate),
        );
        let rfq_service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Rfq]),
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            lifecycle_gate,
        );
        raw_service.mark_upstream_connected().await;
        rfq_service.mark_upstream_connected().await;
        raw_service
            .apply_update(&native_only_update(101, "native-1"))
            .await?;
        rfq_service
            .apply_update(&rfq_only_update(202, "rfq-1", 7))
            .await?;

        BroadcasterServiceState::promote_when_ready(
            &[raw_service.clone(), rfq_service.clone()],
            "new_active",
        )
        .await?
        .ok_or_else(|| anyhow!("ready passive services should promote"))?;

        let marker = writer
            .appends()
            .await
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("promotion should append a marker"))?;
        assert_eq!(
            progress_payload(&marker)?.backends,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq]
        );
        Ok(())
    }

    #[tokio::test]
    async fn expired_fence_without_recovery_recovers_generation() -> Result<()> {
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        publisher
            .promote(
                base_heads([BroadcasterBackend::Native]),
                "active_writer_promoted",
            )
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        service
            .apply_update(&native_only_update(10, "native-1"))
            .await?;

        writer.expire_active_writer().await;
        let Err(error) = service.broadcast_heartbeat().await else {
            return Err(anyhow!("expired writer fence should reject the heartbeat"));
        };
        assert!(format!("{error:#}").contains("missing Redis broadcaster writer fence"));
        let status = publisher.status_snapshot().await;
        assert_eq!(status.mode, "unhealthy");
        assert!(status
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("missing"));
        service.mark_stream_disconnected("stream_error", None).await;
        assert!(!publisher.feeds_are_paused());

        let boundary = BroadcasterServiceState::recover_shared_publisher_when_paused(
            std::slice::from_ref(&service),
            "active_writer_recovered",
        )
        .await?
        .ok_or_else(|| anyhow!("missing fence should make writer recovery reachable"))?;

        assert_eq!(boundary.generation, 2);
        assert_eq!(
            publisher.mode().await,
            super::BroadcasterRedisPublisherMode::Active
        );
        assert_eq!(publisher.pending_recovery_payload_stats().await.0, 0);
        Ok(())
    }

    #[tokio::test]
    async fn expired_fence_taken_by_newer_writer_is_not_displaced() -> Result<()> {
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        publisher
            .promote(
                base_heads([BroadcasterBackend::Native]),
                "active_writer_promoted",
            )
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        service
            .apply_update(&native_only_update(10, "native-1"))
            .await?;

        writer.expire_active_writer().await;
        assert!(service.broadcast_heartbeat().await.is_err());
        service.mark_stream_disconnected("stream_error", None).await;
        let newer_publisher =
            BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
        let newer_boundary = newer_publisher
            .promote(
                base_heads([BroadcasterBackend::Native]),
                "newer_writer_promoted",
            )
            .await?;

        assert!(
            BroadcasterServiceState::recover_shared_publisher_when_paused(
                std::slice::from_ref(&service),
                "active_writer_recovered",
            )
            .await?
            .is_none()
        );
        assert_ne!(
            publisher.mode().await,
            super::BroadcasterRedisPublisherMode::Active
        );
        assert_eq!(newer_boundary.generation, 2);
        assert_eq!(writer.active_generation().await, 2);
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_recovery_keeps_lease_renewed_by_heartbeats() -> Result<()> {
        let writer = ServiceFakeRedisWriter::default();
        let (service, publisher) = ready_feed_service_with_writer(writer.clone()).await?;
        writer.delay_recovery_commit(Duration::from_secs(1)).await;
        let Err(gap_error) = service.apply_feed_message(&raw_feed(12, 12, 11)).await else {
            return Err(anyhow!("header gap must force reconnect"));
        };
        service
            .mark_stream_disconnected("gap", Some(gap_error.to_string()))
            .await;
        service.mark_upstream_connected().await;
        assert!(service.apply_feed_message(&raw_feed(12, 12, 11)).await?);
        timeout(Duration::from_secs(1), async {
            while !publisher.recovery_gate_is_open().await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| anyhow!("recovery did not reserve the publisher gate"))?;

        service.mark_stream_disconnected("stream_error", None).await;
        assert!(publisher.recovery_gate_is_open().await);
        assert_eq!(
            service.recovery_publication.lock().await.phase,
            super::RecoveryPublicationPhase::Idle
        );
        let renew_count = writer.renew_count().await;

        service.broadcast_heartbeat().await?;
        service.broadcast_heartbeat().await?;

        assert_eq!(writer.renew_count().await, renew_count + 2);
        assert_eq!(
            publisher.pending_recovery_payload_stats().await,
            (0, 0, None)
        );
        assert!(writer
            .appends()
            .await
            .iter()
            .all(|entry| entry.kind != BroadcasterMessageKind::Heartbeat));
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::excessive_nesting,
        reason = "The bounded async wait captures the publishing work ID and cancellation flag under the same lock"
    )]
    async fn cancelled_recovery_pause_reaches_writer_recovery() -> Result<()> {
        let writer = ServiceFakeRedisWriter::default();
        let (service, publisher) = ready_feed_service_with_writer(writer.clone()).await?;
        writer.delay_recovery_commit(Duration::from_secs(5)).await;
        let Err(gap_error) = service.apply_feed_message(&raw_feed(12, 12, 11)).await else {
            return Err(anyhow!("header gap must force reconnect"));
        };
        service
            .mark_stream_disconnected("gap", Some(gap_error.to_string()))
            .await;
        service.mark_upstream_connected().await;
        assert!(service.apply_feed_message(&raw_feed(12, 12, 11)).await?);
        let (work_id, cancelled) = timeout(Duration::from_secs(1), async {
            loop {
                let recovery = service.recovery_publication.lock().await;
                if recovery.phase == super::RecoveryPublicationPhase::Publishing
                    && recovery.publisher_recovery_id.is_some()
                {
                    return (recovery.work_id, Arc::clone(&recovery.cancelled));
                }
                drop(recovery);
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| anyhow!("recovery did not reach publishing"))?;
        writer.expire_active_writer().await;
        cancelled.store(true, Ordering::Release);
        let pause_epoch = service.shared_publisher_pause_epoch();
        let failure_task = tokio::spawn({
            let service = service.clone();
            async move {
                service
                    .handle_external_recovery_failure(work_id, "planned Redis outage".to_string())
                    .await
            }
        });
        service
            .wait_for_shared_publisher_pause_after(pause_epoch)
            .await;
        {
            let recovery = service.recovery_publication.lock().await;
            assert_eq!(recovery.phase, super::RecoveryPublicationPhase::Idle);
            assert_eq!(recovery.last_outcome, Some("aborted"));
        }

        service.mark_stream_disconnected("stream_error", None).await;
        let boundary = BroadcasterServiceState::recover_shared_publisher_when_paused(
            std::slice::from_ref(&service),
            "active_writer_recovered",
        )
        .await?
        .ok_or_else(|| anyhow!("paused publisher should recover the expired writer fence"))?;
        assert_eq!(boundary.generation, 2);
        timeout(Duration::from_secs(1), failure_task)
            .await
            .map_err(|_| anyhow!("recovery failure task did not observe the new writer"))?
            .map_err(|error| anyhow!("recovery failure task failed: {error}"))?;
        assert!(!publisher.feeds_are_paused());
        Ok(())
    }

    #[tokio::test]
    async fn invalid_decoded_update_fails_before_redis_append() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        publisher
            .promote(
                base_heads([BroadcasterBackend::Native]),
                "active_writer_promoted",
            )
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            BroadcasterUpstreamState::default(),
            publisher,
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;

        let append_count_before = writer.appends().await.len();
        let Err(error) = service
            .apply_update(&native_update_state(11, "missing-native", 2))
            .await
        else {
            return Err(anyhow!(
                "invalid decoded update should fail before Redis append"
            ));
        };

        assert!(
            format!("{error:#}")
                .contains("state missing-native is missing a known broadcaster backend"),
            "unexpected error: {error:#}"
        );
        assert_eq!(writer.appends().await.len(), append_count_before);
        let status = service.status_snapshot().await;
        assert_eq!(status.snapshot.total_states, 0);
        Ok(())
    }

    #[tokio::test]
    async fn retired_service_refuses_updates_without_cache_commit() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let writer = ServiceFakeRedisWriter::default();
        let old = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        let new = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
        old.promote(base_heads([BroadcasterBackend::Native]), "old_active")
            .await?;
        new.promote(base_heads([BroadcasterBackend::Native]), "new_active")
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            BroadcasterUpstreamState::default(),
            Arc::clone(&old),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;

        let Err(error) = service
            .apply_update(&native_only_update(10, "native-1"))
            .await
        else {
            return Err(anyhow!("retired service must refuse accepted updates"));
        };

        assert!(format!("{error:#}").contains("stale Redis broadcaster writer"));
        assert_eq!(old.status_snapshot().await.mode, "retired");
        let status = service.status_snapshot().await;
        assert_eq!(status.snapshot.total_states, 0);
        assert!(!status.snapshot.ready);
        Ok(())
    }

    #[tokio::test]
    async fn retired_service_closes_existing_snapshot_sessions() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let writer = ServiceFakeRedisWriter::default();
        let old = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        let new = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer));
        old.promote(base_heads([BroadcasterBackend::Native]), "old_active")
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            BroadcasterUpstreamState::default(),
            Arc::clone(&old),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        service
            .apply_update(&native_only_update(10, "native-1"))
            .await?;
        BroadcasterServiceState::run_snapshot_export_preflight(std::slice::from_ref(&service))
            .await?;
        let session = service
            .create_snapshot_session(Duration::from_secs(300))
            .await?
            .ok_or_else(|| anyhow!("active service should create a snapshot session"))?;

        new.promote(base_heads([BroadcasterBackend::Native]), "new_active")
            .await?;
        let Err(_error) = service
            .apply_update(&native_only_update(11, "native-2"))
            .await
        else {
            return Err(anyhow!("fenced old service must fail the stale update"));
        };

        let Err(error) = service
            .snapshot_session_payload(session.session_id, 0)
            .await
        else {
            return Err(anyhow!(
                "retired service must not serve old snapshot payloads"
            ));
        };
        assert_eq!(error, SnapshotSessionError::NotFound);
        assert_eq!(service.status_snapshot().await.snapshot_sessions.active, 0);
        Ok(())
    }

    #[tokio::test]
    async fn stale_active_service_refuses_new_snapshot_session_before_append_or_heartbeat(
    ) -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let writer = ServiceFakeRedisWriter::default();
        let old = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        let new = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer));
        old.promote(base_heads([BroadcasterBackend::Native]), "old_active")
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            BroadcasterUpstreamState::default(),
            Arc::clone(&old),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        service
            .apply_update(&native_only_update(10, "native-1"))
            .await?;
        new.promote(base_heads([BroadcasterBackend::Native]), "new_active")
            .await?;

        assert!(
            service
                .create_snapshot_session(Duration::from_secs(300))
                .await?
                .is_none(),
            "stale active services must not create snapshot sessions before append fencing"
        );
        assert_eq!(old.status_snapshot().await.mode, "retired");
        Ok(())
    }

    #[tokio::test]
    async fn stale_active_service_stops_serving_existing_snapshot_session_before_append_or_heartbeat(
    ) -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let writer = ServiceFakeRedisWriter::default();
        let old = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        let new = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer));
        old.promote(base_heads([BroadcasterBackend::Native]), "old_active")
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            BroadcasterUpstreamState::default(),
            Arc::clone(&old),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        service
            .apply_update(&native_only_update(10, "native-1"))
            .await?;
        BroadcasterServiceState::run_snapshot_export_preflight(std::slice::from_ref(&service))
            .await?;
        let session = service
            .create_snapshot_session(Duration::from_secs(300))
            .await?
            .ok_or_else(|| anyhow!("active service should create a snapshot session"))?;
        new.promote(base_heads([BroadcasterBackend::Native]), "new_active")
            .await?;

        let Err(error) = service
            .snapshot_session_payload(session.session_id, 0)
            .await
        else {
            return Err(anyhow!(
                "stale active service must not serve old snapshot payloads"
            ));
        };
        assert_eq!(error, SnapshotSessionError::NotFound);
        assert_eq!(old.status_snapshot().await.mode, "retired");
        assert_eq!(service.status_snapshot().await.snapshot_sessions.active, 0);
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_session_uses_cached_boundary_without_waiting_for_queued_update() -> Result<()>
    {
        let service = ready_service().await?;
        let gate = service.lock_lifecycle_gate_for_test().await;

        let mut subscribe_task = tokio::spawn({
            let service = service.clone();
            async move {
                service
                    .create_snapshot_session(Duration::from_secs(300))
                    .await
            }
        });
        tokio::task::yield_now().await;

        let mut update_task = tokio::spawn({
            let service = service.clone();
            async move {
                service
                    .apply_update(&native_only_update(11, "native-2"))
                    .await
            }
        });

        let session = timeout(Duration::from_millis(25), &mut subscribe_task)
            .await
            .map_err(|_| anyhow!("cached snapshot session should not wait on the lifecycle gate"))?
            .map_err(|error| anyhow!("subscribe task failed: {error}"))??
            .ok_or_else(|| anyhow!("expected ready broadcaster snapshot session"))?;
        assert!(timeout(Duration::from_millis(25), &mut update_task)
            .await
            .is_err());
        drop(gate);

        update_task
            .await
            .map_err(|error| anyhow!("update task failed: {error}"))??;

        let first_payload = service
            .snapshot_session_payload(session.session_id, 0)
            .await
            .map_err(|error| anyhow!("failed to fetch snapshot payload: {error:?}"))?;
        let publisher_status = service.redis_publisher.status_snapshot().await;
        let publisher_boundary = publisher_status
            .replay_boundary
            .ok_or_else(|| anyhow!("expected Redis replay boundary after queued update"))?;

        assert!(matches!(
            first_payload.payload,
            BroadcasterPayload::SnapshotStart(_)
        ));
        assert_eq!(session.redis_replay_boundary.exclusive_message_seq, 2);
        assert_eq!(publisher_boundary.exclusive_message_seq, 3);
        Ok(())
    }

    #[tokio::test]
    async fn shared_snapshot_session_boundary_is_registered_before_queued_rfq_update() -> Result<()>
    {
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer),
        ));
        publisher
            .promote(
                base_heads([BroadcasterBackend::Native, BroadcasterBackend::Rfq]),
                "active_writer_promoted",
            )
            .await?;
        let lifecycle_gate = Arc::new(Mutex::new(()));
        let raw_service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            Arc::clone(&lifecycle_gate),
        );
        let rfq_service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Rfq]),
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            lifecycle_gate,
        );
        raw_service.mark_upstream_connected().await;
        rfq_service.mark_upstream_connected().await;
        raw_service
            .apply_update(&native_only_update(10, "native-1"))
            .await?;
        rfq_service
            .apply_update(&rfq_only_update(12, "rfq-1", 7))
            .await?;
        BroadcasterServiceState::run_snapshot_export_preflight(&[
            raw_service.clone(),
            rfq_service.clone(),
        ])
        .await?;
        let gate = raw_service.lock_lifecycle_gate_for_test().await;

        let services = vec![raw_service.clone(), rfq_service.clone()];
        let mut subscribe_task = tokio::spawn(async move {
            BroadcasterServiceState::create_snapshot_session_for_services(
                &services,
                Duration::from_secs(300),
            )
            .await
        });
        tokio::task::yield_now().await;

        let mut update_task = tokio::spawn({
            let rfq_service = rfq_service.clone();
            async move {
                rfq_service
                    .apply_update(&rfq_only_update(13, "rfq-2", 8))
                    .await
            }
        });

        let session = timeout(Duration::from_millis(25), &mut subscribe_task)
            .await
            .map_err(|_| anyhow!("cached snapshot session should not wait on the lifecycle gate"))?
            .map_err(|error| anyhow!("subscribe task failed: {error}"))??
            .ok_or_else(|| anyhow!("expected combined broadcaster snapshot session"))?;
        assert!(timeout(Duration::from_millis(25), &mut update_task)
            .await
            .is_err());
        drop(gate);
        update_task
            .await
            .map_err(|error| anyhow!("update task failed: {error}"))??;

        let first_payload = raw_service
            .snapshot_session_payload(session.session_id, 0)
            .await
            .map_err(|error| anyhow!("failed to fetch snapshot payload: {error:?}"))?;
        let publisher_status = publisher.status_snapshot().await;
        let publisher_boundary = publisher_status
            .replay_boundary
            .ok_or_else(|| anyhow!("expected Redis replay boundary after queued update"))?;

        assert!(matches!(
            first_payload.payload,
            BroadcasterPayload::SnapshotStart(_)
        ));
        assert_eq!(session.redis_replay_boundary.exclusive_message_seq, 3);
        assert_eq!(publisher_boundary.exclusive_message_seq, 4);
        Ok(())
    }

    #[tokio::test]
    async fn combined_snapshot_session_rejects_mismatched_lifecycle_gate() -> Result<()> {
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer),
        ));
        let raw_service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            Arc::new(Mutex::new(())),
        );
        let rfq_service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Rfq]),
            BroadcasterUpstreamState::default(),
            publisher,
            Arc::new(Mutex::new(())),
        );

        let Err(error) = BroadcasterServiceState::create_snapshot_session_for_services(
            &[raw_service, rfq_service],
            Duration::from_secs(300),
        )
        .await
        else {
            return Err(anyhow!("mismatched lifecycle gates should be rejected"));
        };

        assert!(format!("{error:#}").contains("one lifecycle gate"));
        Ok(())
    }

    #[tokio::test]
    async fn apply_update_fails_when_redis_publication_fails() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let writer = ServiceFakeRedisWriter::default();
        writer.fail_next_appends(100).await;
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        publisher
            .promote(
                base_heads([BroadcasterBackend::Native]),
                "active_writer_promoted",
            )
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;

        let Err(error) = service
            .apply_update(&native_only_update(10, "native-1"))
            .await
        else {
            return Err(anyhow!("accepted update must fail when Redis append fails"));
        };

        assert!(format!("{error:#}").contains("failed to publish accepted broadcaster delta"));
        assert!(publisher.status_snapshot().await.replay_boundary.is_none());
        let status = service.status_snapshot().await;
        assert!(
            !status.snapshot.ready,
            "cache must not accept an update that Redis failed to publish"
        );
        assert_eq!(status.snapshot.total_states, 0);
        Ok(())
    }

    #[tokio::test]
    async fn heartbeat_fails_when_redis_publication_fails() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        publisher
            .promote(
                base_heads([BroadcasterBackend::Native]),
                "active_writer_promoted",
            )
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        service
            .apply_update(&native_only_update(10, "native-1"))
            .await?;
        writer.fail_next_appends(100).await;

        let Err(error) = service.broadcast_heartbeat().await else {
            return Err(anyhow!("heartbeat must fail when Redis append fails"));
        };

        assert!(format!("{error:#}").contains("failed to publish accepted broadcaster delta"));
        assert!(publisher.status_snapshot().await.replay_boundary.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_refresh_matches_direct_export_and_session_boundary() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        publisher
            .promote(
                base_heads([BroadcasterBackend::Native]),
                "active_writer_promoted",
            )
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache.clone(),
            BroadcasterUpstreamState::default(),
            publisher,
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        service
            .apply_update(&native_only_update(10, "native-1"))
            .await?;
        let expected_export = cache.export_snapshot(8_388_608).await?;
        let expected_payloads = expected_export
            .payloads
            .into_iter()
            .enumerate()
            .map(|(index, payload)| {
                let message_seq = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
                BroadcasterEnvelope::new(expected_export.stream_id.clone(), message_seq, payload)
            })
            .collect::<Vec<_>>();
        let expected_encoded_bytes = expected_payloads
            .iter()
            .map(serde_json::to_vec)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|encoded| encoded.len())
            .sum::<usize>();
        BroadcasterServiceState::run_snapshot_export_preflight(std::slice::from_ref(&service))
            .await?;

        let artifact = service
            .snapshot_artifact
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow!("expected refreshed broadcaster snapshot artifact"))?;
        assert_eq!(
            serde_json::to_vec(artifact.payloads.as_ref())?,
            serde_json::to_vec(&expected_payloads)?
        );
        assert_eq!(artifact.encoded_bytes, expected_encoded_bytes);

        let session = service
            .create_snapshot_session(Duration::from_secs(300))
            .await?
            .ok_or_else(|| anyhow!("expected ready broadcaster snapshot session"))?;

        assert_eq!(
            session.redis_replay_boundary.stream_key,
            publisher_config().stream_key
        );
        assert_eq!(session.redis_replay_boundary.stream_id, "chain-1-stream-1");
        assert_eq!(
            session.redis_replay_boundary.snapshot_id,
            "chain-1-snapshot-1"
        );
        assert_eq!(session.redis_replay_boundary.stream_id, session.stream_id);
        assert_eq!(
            session.redis_replay_boundary.snapshot_id,
            session.snapshot_id
        );
        assert_eq!(session.redis_replay_boundary.exclusive_entry_id(), "1-2");
        assert_eq!(session.redis_replay_boundary.exclusive_message_seq, 2);
        assert!(
            writer.appends().await.iter().all(|entry| {
                !matches!(
                    entry.kind,
                    simulator_core::broadcaster::BroadcasterMessageKind::SnapshotStart
                        | simulator_core::broadcaster::BroadcasterMessageKind::SnapshotChunk
                        | simulator_core::broadcaster::BroadcasterMessageKind::SnapshotEnd
                )
            }),
            "creating an HTTP snapshot session must not publish Redis snapshot payloads"
        );
        Ok(())
    }

    #[tokio::test]
    async fn pending_session_serves_payloads_until_expiry() -> Result<()> {
        let registry = BroadcasterSnapshotSessionRegistry::new();
        let session = registry
            .create_snapshot_session(snapshot_artifact(), Duration::from_secs(300))
            .await?;

        let first = registry
            .snapshot_payload(session.session_id, 0)
            .await
            .map_err(|error| anyhow!("payload fetch failed: {error:?}"))?;

        assert_eq!(first.stream_id, "stream-1");
        assert_eq!(first.message_seq, 1);
        assert_eq!(registry.snapshot().await.active, 1);
        Ok(())
    }

    #[tokio::test]
    async fn pending_session_rejects_payload_index_out_of_range() -> Result<()> {
        let registry = BroadcasterSnapshotSessionRegistry::new();
        let session = registry
            .create_snapshot_session(snapshot_artifact(), Duration::from_secs(300))
            .await?;

        let Err(error) = registry.snapshot_payload(session.session_id, 9).await else {
            unreachable!("out-of-range payload should fail");
        };

        assert_eq!(error, SnapshotSessionError::PayloadOutOfRange);
        assert_eq!(registry.snapshot().await.active, 1);
        Ok(())
    }

    #[tokio::test]
    async fn disconnect_all_clears_pending_sessions() -> Result<()> {
        let registry = BroadcasterSnapshotSessionRegistry::new();
        let session = registry
            .create_snapshot_session(snapshot_artifact(), Duration::from_secs(300))
            .await?;

        registry
            .disconnect_all(super::SessionCloseReason::WriterFenceLost)
            .await;

        let Err(error) = registry.snapshot_payload(session.session_id, 0).await else {
            unreachable!("closed snapshot session should not serve payloads");
        };
        assert_eq!(error, SnapshotSessionError::NotFound);
        let snapshot = registry.snapshot().await;
        assert_eq!(snapshot.active, 0);
        assert_eq!(
            snapshot.last_error.as_deref(),
            Some("all snapshot sessions closed: writer_fence_lost")
        );
        Ok(())
    }

    #[tokio::test]
    async fn expired_pending_session_records_expiry() -> Result<()> {
        let registry = BroadcasterSnapshotSessionRegistry::new();
        let session = registry
            .create_snapshot_session(snapshot_artifact(), Duration::from_millis(1))
            .await?;
        tokio::time::sleep(Duration::from_millis(5)).await;

        let Err(error) = registry.snapshot_payload(session.session_id, 0).await else {
            unreachable!("expired session should fail payload fetch");
        };
        assert_eq!(error, SnapshotSessionError::Expired);
        let snapshot = registry.snapshot().await;
        assert_eq!(snapshot.active, 0);
        assert_eq!(
            snapshot.last_error.as_deref(),
            Some("snapshot session 1 closed: expired")
        );
        Ok(())
    }

    async fn ready_service() -> Result<BroadcasterServiceState> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer),
        ));
        publisher
            .promote(
                base_heads([BroadcasterBackend::Native]),
                "active_writer_promoted",
            )
            .await?;
        let upstream = BroadcasterUpstreamState::default();
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            cache,
            upstream,
            publisher,
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        service
            .apply_update(&native_only_update(10, "native-1"))
            .await?;
        BroadcasterServiceState::run_snapshot_export_preflight(std::slice::from_ref(&service))
            .await?;
        Ok(service)
    }

    async fn ready_feed_service_with_writer(
        writer: ServiceFakeRedisWriter,
    ) -> Result<(BroadcasterServiceState, Arc<BroadcasterRedisPublisher>)> {
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer),
        ));
        publisher
            .promote(
                base_heads([BroadcasterBackend::Native]),
                "active_writer_promoted",
            )
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            8_388_608,
            BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        assert!(service.apply_feed_message(&raw_feed(10, 10, 9)).await?);
        BroadcasterServiceState::run_snapshot_export_preflight(std::slice::from_ref(&service))
            .await?;
        Ok((service, publisher))
    }

    #[derive(Debug, Clone, Default)]
    struct ServiceFakeRedisWriter {
        inner: Arc<Mutex<ServiceFakeRedisWriterState>>,
    }

    #[derive(Debug, Default)]
    struct ServiceFakeRedisWriterState {
        appends: Vec<BroadcasterRedisStreamEntry>,
        active_token: Option<String>,
        active_generation: u64,
        renew_count: usize,
        fail_next_appends: usize,
        lose_recovery_commit_replies: bool,
        recovery_commit_delay: Option<Duration>,
    }

    impl ServiceFakeRedisWriter {
        async fn fail_next_appends(&self, count: usize) {
            self.inner.lock().await.fail_next_appends = count;
        }

        async fn appends(&self) -> Vec<BroadcasterRedisStreamEntry> {
            self.inner.lock().await.appends.clone()
        }

        async fn lose_recovery_commit_replies(&self) {
            self.inner.lock().await.lose_recovery_commit_replies = true;
        }

        async fn delay_recovery_commit(&self, delay: Duration) {
            self.inner.lock().await.recovery_commit_delay = Some(delay);
        }

        async fn expire_active_writer(&self) {
            self.inner.lock().await.active_token = None;
        }

        async fn renew_count(&self) -> usize {
            self.inner.lock().await.renew_count
        }

        async fn active_generation(&self) -> u64 {
            self.inner.lock().await.active_generation
        }
    }

    impl RedisStreamWriter for ServiceFakeRedisWriter {
        fn promote<'a>(
            &'a self,
            command: crate::broadcaster::redis_publisher::RedisPromotionCommand<'a>,
        ) -> futures::future::BoxFuture<
            'a,
            Result<crate::broadcaster::redis_publisher::RedisPromotionResult>,
        > {
            Box::pin(async move {
                let mut guard = self.inner.lock().await;
                if command.expected_writer_token.is_some_and(|expected_token| {
                    guard.active_token.is_some()
                        && (guard.active_token.as_deref() != Some(expected_token)
                            || Some(guard.active_generation) != command.expected_generation)
                }) {
                    return Err(anyhow!("stale Redis broadcaster writer token"));
                }
                if command.expected_writer_token.is_some() && guard.active_token.is_none() {
                    guard.active_generation = guard
                        .active_generation
                        .max(command.expected_generation.unwrap_or_default());
                }
                guard.active_generation = guard.active_generation.saturating_add(1);
                guard.active_token = Some(command.writer_token.to_string());
                let generation = guard.active_generation;
                let entry_id = format!("{generation}-1");
                guard
                    .appends
                    .push(entry_from_fields(command.normal_marker_fields, generation)?);
                Ok(crate::broadcaster::redis_publisher::RedisPromotionResult {
                    generation,
                    entry_id,
                })
            })
        }

        fn append_fenced<'a>(
            &'a self,
            command: crate::broadcaster::redis_publisher::RedisAppendCommand<'a>,
        ) -> futures::future::BoxFuture<'a, Result<String>> {
            Box::pin(async move {
                let commit_delay = {
                    let guard = self.inner.lock().await;
                    (command.entry.kind == BroadcasterMessageKind::RecoveryCommit)
                        .then_some(guard.recovery_commit_delay)
                        .flatten()
                };
                if let Some(delay) = commit_delay {
                    tokio::time::sleep(delay).await;
                }
                let mut guard = self.inner.lock().await;
                if guard.active_token.is_none() {
                    return Err(anyhow!("missing Redis broadcaster writer fence"));
                }
                if guard.active_token.as_deref() != Some(command.writer_token)
                    || guard.active_generation != command.generation
                {
                    return Err(anyhow!("stale Redis broadcaster writer token"));
                }
                if guard.fail_next_appends > 0 {
                    guard.fail_next_appends -= 1;
                    return Err(anyhow!("planned append failure"));
                }
                let entry_id = crate::broadcaster::redis_publisher::redis_entry_id(command.entry)?;
                let commit_reply_lost = command.entry.kind
                    == BroadcasterMessageKind::RecoveryCommit
                    && guard.lose_recovery_commit_replies;
                if commit_reply_lost
                    && !guard
                        .appends
                        .iter()
                        .any(|entry| entry.message_seq == command.entry.message_seq)
                {
                    guard.appends.push(command.entry.clone());
                }
                if commit_reply_lost {
                    return Err(anyhow!("planned lost recovery commit reply"));
                }
                guard.appends.push(command.entry.clone());
                Ok(entry_id)
            })
        }

        fn renew_writer<'a>(
            &'a self,
            command: crate::broadcaster::redis_publisher::RedisRenewCommand<'a>,
        ) -> futures::future::BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                let mut guard = self.inner.lock().await;
                guard.renew_count = guard.renew_count.saturating_add(1);
                if guard.active_token.is_none() {
                    return Err(anyhow!(
                        crate::broadcaster::redis_publisher::MISSING_FENCE_MESSAGE
                    ));
                }
                if guard.active_token.as_deref() != Some(command.writer_token)
                    || guard.active_generation != command.generation
                {
                    return Err(anyhow!("stale Redis broadcaster writer token"));
                }
                Ok(())
            })
        }
    }

    fn entry_from_fields(
        fields: &[(String, String)],
        generation: u64,
    ) -> Result<BroadcasterRedisStreamEntry> {
        let mut value = serde_json::Map::new();
        for (field, field_value) in fields {
            let field_value = field_value.replace(
                crate::broadcaster::redis_publisher::GENERATION_PLACEHOLDER,
                &generation.to_string(),
            );
            value.insert(field.clone(), serde_json::Value::String(field_value));
        }
        serde_json::from_value(serde_json::Value::Object(value)).map_err(Into::into)
    }

    fn progress_payload(entry: &BroadcasterRedisStreamEntry) -> Result<BroadcasterProgress> {
        let envelope: BroadcasterEnvelope = serde_json::from_str(&entry.payload_json)?;
        let BroadcasterPayload::Progress(progress) = envelope.payload else {
            return Err(anyhow!("Redis stream entry payload should be progress"));
        };
        Ok(progress)
    }

    #[tokio::test]
    async fn startup_snapshot_packing_reserves_generation_id_growth() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        cache
            .apply_update(&native_only_update(10, "native-1"))
            .await?;
        cache
            .apply_update(&native_only_update(11, "native-2"))
            .await?;
        let export = cache.export_snapshot(usize::MAX).await?;
        let mut full_chunk = export
            .payloads
            .iter()
            .find_map(|payload| match payload {
                BroadcasterPayload::SnapshotChunk(chunk) => Some(chunk.clone()),
                _ => None,
            })
            .ok_or_else(|| anyhow!("fixture should export a chunk"))?;
        assert_eq!(full_chunk.partitions[0].states.len(), 2);
        let expected_states = serde_json::to_value(&full_chunk.partitions[0].states)?;
        full_chunk.chunk_index = u32::MAX;
        let max_payload_bytes = serde_json::to_vec(&BroadcasterEnvelope::new(
            export.stream_id,
            u64::MAX,
            BroadcasterPayload::SnapshotChunk(full_chunk),
        ))?
        .len();
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(ServiceFakeRedisWriter::default()),
        ));
        let service = BroadcasterServiceState::with_lifecycle_gate(
            max_payload_bytes,
            cache,
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        let candidate = super::prepare_startup_snapshot_candidate(std::slice::from_ref(&service))
            .await?
            .ok_or_else(|| anyhow!("ready cache should produce a candidate"))?;
        assert!(candidate.snapshot_chunk_count > 1);
        assert!(candidate.largest_payload_bytes_upper_bound <= max_payload_bytes);
        let boundary = super::promote_prepared_startup_candidate(
            std::slice::from_ref(&service),
            candidate,
            "test_active".to_string(),
        )
        .await?;
        let artifact = service
            .snapshot_artifact
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow!("promotion should install the snapshot artifact"))?;
        assert_eq!(artifact.redis_replay_boundary, boundary);
        assert_eq!(publisher.status_snapshot().await.mode, "active");
        let mut actual_states = Vec::new();
        for envelope in artifact.payloads.iter() {
            assert!(serde_json::to_vec(envelope)?.len() <= max_payload_bytes);
            assert_eq!(envelope.stream_id, boundary.stream_id);
            match &envelope.payload {
                BroadcasterPayload::SnapshotStart(start) => {
                    assert_eq!(start.snapshot_id, boundary.snapshot_id)
                }
                BroadcasterPayload::SnapshotChunk(chunk) => {
                    assert_eq!(chunk.snapshot_id, boundary.snapshot_id);
                    actual_states.extend(
                        chunk
                            .partitions
                            .iter()
                            .flat_map(|partition| partition.states.clone()),
                    );
                }
                BroadcasterPayload::SnapshotEnd(end) => {
                    assert_eq!(end.snapshot_id, boundary.snapshot_id)
                }
                _ => return Err(anyhow!("artifact should contain only snapshot payloads")),
            }
        }
        assert_eq!(serde_json::to_value(actual_states)?, expected_states);
        Ok(())
    }

    #[test]
    fn startup_candidate_is_generation_neutral_until_infallible_finalization() -> Result<()> {
        let candidate = super::build_snapshot_candidate(
            1,
            snapshot_export(),
            base_heads([BroadcasterBackend::Native]),
            vec![3],
            9,
        )?;
        assert_eq!(
            candidate.export.stream_id,
            "chain-1-stream-18446744073709551615"
        );
        assert_eq!(
            candidate.export.snapshot_id,
            "chain-1-snapshot-18446744073709551615"
        );
        assert_eq!(candidate.captured_recovery_work_ids, vec![3]);
        assert_eq!(candidate.handoff_id, 9);
        assert_eq!(candidate.payload_digests.len(), 2);

        let boundary = simulator_core::broadcaster::BroadcasterRedisReplayBoundary::new(
            "dsolver:broadcaster:test:events",
            "chain-1-stream-7",
            "chain-1-snapshot-7",
            7,
            1,
            7 << 48,
        )?;
        let artifact = super::finalize_snapshot_candidate(candidate, boundary);
        assert_eq!(artifact.stream_id, "chain-1-stream-7");
        assert_eq!(artifact.snapshot_id, "chain-1-snapshot-7");
        assert_eq!(artifact.payloads.len(), 2);
        assert!(artifact.payloads.iter().all(|envelope| {
            envelope.stream_id == "chain-1-stream-7"
                && match &envelope.payload {
                    BroadcasterPayload::SnapshotStart(start) => {
                        start.snapshot_id == "chain-1-snapshot-7"
                    }
                    BroadcasterPayload::SnapshotEnd(end) => end.snapshot_id == "chain-1-snapshot-7",
                    _ => false,
                }
        }));
        Ok(())
    }

    fn snapshot_export() -> BroadcasterSnapshotExport {
        BroadcasterSnapshotExport {
            stream_id: "stream-1".to_string(),
            snapshot_id: "snapshot-1".to_string(),
            max_payload_bytes: 8_388_608,
            payloads: vec![
                BroadcasterPayload::SnapshotStart(
                    BroadcasterSnapshotStart::new("snapshot-1", 1, vec![], 0)
                        .unwrap_or_else(|_| unreachable!("snapshot_start")),
                ),
                BroadcasterPayload::SnapshotEnd(BroadcasterSnapshotEnd::new("snapshot-1")),
            ],
        }
    }

    fn snapshot_artifact() -> Arc<super::BroadcasterSnapshotArtifact> {
        Arc::new(
            super::build_snapshot_artifact(1, snapshot_export(), replay_boundary())
                .unwrap_or_else(|_| unreachable!("valid snapshot artifact"))
                .0,
        )
    }

    fn replay_boundary() -> simulator_core::broadcaster::BroadcasterRedisReplayBoundary {
        replay_boundary_with_ids("stream-1", "snapshot-1")
    }

    fn replay_boundary_with_ids(
        stream_id: &str,
        snapshot_id: &str,
    ) -> simulator_core::broadcaster::BroadcasterRedisReplayBoundary {
        simulator_core::broadcaster::BroadcasterRedisReplayBoundary::new(
            "dsolver:broadcaster:test:events",
            stream_id,
            snapshot_id,
            1,
            0,
            0,
        )
        .unwrap_or_else(|_| unreachable!("valid replay boundary"))
    }

    fn publisher_config() -> BroadcasterRedisPublisherConfig {
        BroadcasterRedisPublisherConfig {
            stream_key: "dsolver:broadcaster:test:events".to_string(),
            chain_id: Chain::Ethereum.id(),
            append_retry_window: Duration::from_millis(10),
            maxlen: None,
            writer_lease_ttl: Duration::from_secs(30),
            recovery_max_buffered_native_blocks: 64,
        }
    }

    fn base_heads<const N: usize>(
        backends: [BroadcasterBackend; N],
    ) -> Vec<BroadcasterBackendHead> {
        backends
            .into_iter()
            .enumerate()
            .map(|(index, backend)| BroadcasterBackendHead::new(backend, index as u64))
            .collect()
    }

    fn native_only_update(block_number: u64, component_id: &str) -> Update {
        let protocol = "uniswap_v2";
        let mut new_pairs = HashMap::new();
        new_pairs.insert(
            component_id.to_string(),
            protocol_component(component_id, protocol),
        );

        let mut states = HashMap::new();
        states.insert(
            component_id.to_string(),
            Box::new(DummySim(block_number as u8)) as Box<dyn ProtocolSim>,
        );

        Update::new(block_number, states, new_pairs).set_sync_states(HashMap::from([(
            protocol.to_string(),
            SynchronizerState::Ready(BlockHeader {
                hash: Bytes::from(vec![1u8; 32]),
                number: block_number,
                parent_hash: Bytes::from(vec![2u8; 32]),
                revert: false,
                timestamp: block_number * 10,
                partial_block_index: None,
            }),
        )]))
    }

    fn native_buffer_update(block_number: u64) -> Result<BroadcasterUpdateMessage> {
        let sync_status = BroadcasterProtocolSyncStatus::from_synchronizer_state(
            &SynchronizerState::Ready(BlockHeader {
                hash: Bytes::from(vec![1_u8; 32]),
                number: block_number,
                parent_hash: Bytes::from(vec![2_u8; 32]),
                revert: false,
                timestamp: block_number * 10,
                partial_block_index: None,
            }),
        );
        BroadcasterUpdateMessage::new(vec![BroadcasterUpdatePartition::new(
            BroadcasterBackend::Native,
            block_number,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            std::collections::BTreeMap::from([("uniswap_v2".to_string(), sync_status)]),
        )])
        .map_err(Into::into)
    }

    fn raw_feed(number: u64, hash: u8, parent_hash: u8) -> FeedMessage<BlockHeader> {
        raw_feed_with_account_payload(number, hash, parent_hash, "test".to_string())
    }

    fn raw_feed_for_protocols(
        number: u64,
        hash: u8,
        parent_hash: u8,
        protocols: &[&str],
    ) -> FeedMessage<BlockHeader> {
        let feed = raw_feed(number, hash, parent_hash);
        let message = feed.state_msgs["uniswap_v2"].clone();
        let sync_state = feed.sync_states["uniswap_v2"].clone();
        FeedMessage {
            state_msgs: protocols
                .iter()
                .map(|protocol| ((*protocol).to_string(), message.clone()))
                .collect(),
            sync_states: protocols
                .iter()
                .map(|protocol| ((*protocol).to_string(), sync_state.clone()))
                .collect(),
        }
    }

    fn partial_raw_feed(number: u64, hash: u8, parent_hash: u8) -> FeedMessage<BlockHeader> {
        let mut feed = raw_feed(number, hash, parent_hash);
        for message in feed.state_msgs.values_mut() {
            message.header.partial_block_index = Some(0);
        }
        for sync_state in feed.sync_states.values_mut() {
            if let SynchronizerState::Ready(header) = sync_state {
                header.partial_block_index = Some(0);
            }
        }
        feed
    }

    fn raw_feed_with_account_payload(
        number: u64,
        hash: u8,
        parent_hash: u8,
        account_payload: String,
    ) -> FeedMessage<BlockHeader> {
        let header = BlockHeader {
            hash: Bytes::from([hash; 32]),
            number,
            parent_hash: Bytes::from([parent_hash; 32]),
            revert: false,
            timestamp: number * 10,
            partial_block_index: None,
        };
        let account_address = Bytes::from([1_u8; 20]);
        FeedMessage {
            state_msgs: HashMap::from([(
                "uniswap_v2".to_string(),
                StateSyncMessage {
                    header: header.clone(),
                    snapshots: Snapshot {
                        states: HashMap::new(),
                        vm_storage: HashMap::from([(
                            account_address.clone(),
                            ResponseAccount::new(
                                DtoChain::Ethereum,
                                account_address,
                                account_payload,
                                HashMap::new(),
                                Bytes::from([hash; 32]),
                                HashMap::new(),
                                Bytes::from([1_u8; 32]),
                                Bytes::from([2_u8; 32]),
                                Bytes::from([3_u8; 32]),
                                Bytes::from([4_u8; 32]),
                                None,
                            )
                            .into(),
                        )]),
                    },
                    deltas: None,
                    removed_components: HashMap::new(),
                },
            )]),
            sync_states: HashMap::from([(
                "uniswap_v2".to_string(),
                SynchronizerState::Ready(header),
            )]),
        }
    }

    fn native_update_state(block_number: u64, component_id: &str, seed: u8) -> Update {
        let protocol = "uniswap_v2";
        let mut states = HashMap::new();
        states.insert(
            component_id.to_string(),
            Box::new(DummySim(seed)) as Box<dyn ProtocolSim>,
        );

        Update::new(block_number, states, HashMap::new()).set_sync_states(HashMap::from([(
            protocol.to_string(),
            SynchronizerState::Ready(BlockHeader {
                hash: Bytes::from(vec![seed; 32]),
                number: block_number,
                parent_hash: Bytes::from(vec![seed.saturating_add(1); 32]),
                revert: false,
                timestamp: block_number * 10,
                partial_block_index: None,
            }),
        )]))
    }

    fn rfq_only_update(block_number: u64, component_id: &str, seed: u8) -> Update {
        let protocol = "rfq:hashflow";
        let mut new_pairs = HashMap::new();
        new_pairs.insert(
            component_id.to_string(),
            protocol_component(component_id, protocol),
        );

        let mut states = HashMap::new();
        states.insert(
            component_id.to_string(),
            Box::new(DummySim(seed)) as Box<dyn ProtocolSim>,
        );

        Update::new(block_number, states, new_pairs).set_sync_states(HashMap::from([(
            protocol.to_string(),
            SynchronizerState::Ready(BlockHeader {
                hash: Bytes::from(vec![seed; 32]),
                number: block_number,
                parent_hash: Bytes::from(vec![seed.saturating_add(1); 32]),
                revert: false,
                timestamp: block_number * 10,
                partial_block_index: None,
            }),
        )]))
    }

    fn protocol_component(_component_id: &str, protocol: &str) -> ProtocolComponent {
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
}
