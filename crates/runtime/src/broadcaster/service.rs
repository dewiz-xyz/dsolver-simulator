use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;
use tracing::warn;
use tycho_simulation::{
    protocol::models::Update as TychoUpdate,
    tycho_client::feed::{BlockHeader, FeedMessage},
};

use crate::broadcaster::redis_publisher::{
    BroadcasterRedisPublisher, BroadcasterRedisPublisherMode,
};
use crate::broadcaster::state::{
    combine_snapshot_exports, BroadcasterReadiness, BroadcasterSnapshotCache,
    BroadcasterSnapshotExport, BroadcasterSnapshotSessionsSnapshot, BroadcasterStatusSnapshot,
    BroadcasterUpstreamState,
};
use crate::metrics::emit_broadcaster_snapshot_export_failure;
use simulator_core::broadcaster::{
    BroadcasterEnvelope, BroadcasterPayload, BroadcasterRedisReplayBoundary,
    BroadcasterSnapshotSessionResponse,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotSessionError {
    NotFound,
    Expired,
    PayloadOutOfRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionCloseReason {
    Expired,
    GenerationReset,
}

impl SessionCloseReason {
    const fn label(self) -> &'static str {
        match self {
            Self::Expired => "expired",
            Self::GenerationReset => "generation_reset",
        }
    }
}

#[derive(Debug, Clone)]
struct BroadcasterSnapshotSessionRegistry {
    next_session_id: Arc<AtomicU64>,
    last_error: Arc<Mutex<Option<String>>>,
    pending_sessions: Arc<Mutex<HashMap<u64, PendingSnapshotSession>>>,
}

#[derive(Debug)]
struct PendingSnapshotSession {
    snapshot_payloads: Vec<BroadcasterEnvelope>,
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
        }
    }

    fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.pending_sessions, &other.pending_sessions)
    }

    async fn create_snapshot_session(
        &self,
        snapshot: BroadcasterSnapshotExport,
        chain_id: u64,
        redis_replay_boundary: BroadcasterRedisReplayBoundary,
        ttl: Duration,
    ) -> Result<BroadcasterSnapshotSessionResponse> {
        let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        let stream_id = snapshot.stream_id;
        let snapshot_id = snapshot.snapshot_id;
        let max_payload_bytes = snapshot.max_payload_bytes;
        let snapshot_chunk_count = snapshot
            .payloads
            .iter()
            .filter(|payload| matches!(payload, BroadcasterPayload::SnapshotChunk(_)))
            .count() as u32;
        let mut message_seq = 1u64;
        let snapshot_payloads = snapshot
            .payloads
            .into_iter()
            .map(|payload| {
                let envelope = BroadcasterEnvelope::new(stream_id.clone(), message_seq, payload);
                message_seq = message_seq.saturating_add(1);
                envelope
            })
            .collect::<Vec<_>>();
        for (index, envelope) in snapshot_payloads.iter().enumerate() {
            let bytes = serde_json::to_vec(envelope)
                .with_context(|| format!("snapshot payload {index} is not JSON-serializable"))?;
            anyhow::ensure!(
                bytes.len() <= max_payload_bytes,
                "snapshot payload {index} is {} bytes, above configured max {max_payload_bytes}",
                bytes.len()
            );
        }
        let payload_count = snapshot_payloads.len() as u32;
        let expires_in_ms = ttl.as_millis().try_into().unwrap_or(u64::MAX);
        let expires_at = Instant::now() + ttl;

        anyhow::ensure!(
            stream_id == redis_replay_boundary.stream_id,
            "snapshot stream_id mismatch with Redis replay boundary: snapshot={stream_id}, boundary={}",
            redis_replay_boundary.stream_id
        );
        anyhow::ensure!(
            snapshot_id == redis_replay_boundary.snapshot_id,
            "snapshot_id mismatch with Redis replay boundary: snapshot={snapshot_id}, boundary={}",
            redis_replay_boundary.snapshot_id
        );

        self.pending_sessions.lock().await.insert(
            session_id,
            PendingSnapshotSession {
                snapshot_payloads,
                expires_at,
            },
        );

        Ok(BroadcasterSnapshotSessionResponse {
            chain_id,
            session_id,
            stream_id,
            snapshot_id,
            redis_replay_boundary,
            payload_count,
            snapshot_chunk_count,
            expires_in_ms,
        })
    }

    async fn snapshot_payload(
        &self,
        session_id: u64,
        index: u32,
    ) -> Result<BroadcasterEnvelope, SnapshotSessionError> {
        {
            let now = Instant::now();
            let mut guard = self.pending_sessions.lock().await;
            let Some(session) = guard.get(&session_id) else {
                return Err(SnapshotSessionError::NotFound);
            };
            if session.is_expired(now) {
                guard.remove(&session_id);
            } else {
                let Some(envelope) = session.snapshot_payloads.get(index as usize).cloned() else {
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

#[derive(Debug, Clone)]
pub struct BroadcasterServiceState {
    snapshot_max_payload_bytes: usize,
    cache: BroadcasterSnapshotCache,
    upstream: BroadcasterUpstreamState,
    snapshot_sessions: BroadcasterSnapshotSessionRegistry,
    snapshot_preflight: Arc<RwLock<BroadcasterSnapshotPreflightState>>,
    redis_publisher: Arc<BroadcasterRedisPublisher>,
    // This gate keeps snapshot export plus replay-boundary capture atomic with respect to
    // updates, heartbeats, and generation resets.
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
        Self {
            snapshot_max_payload_bytes,
            cache,
            upstream,
            snapshot_sessions: BroadcasterSnapshotSessionRegistry::new(),
            snapshot_preflight: Arc::new(RwLock::new(BroadcasterSnapshotPreflightState::default())),
            redis_publisher,
            lifecycle_gate,
        }
    }

    pub async fn mark_upstream_connected(&self) {
        self.upstream.mark_connected().await;
    }

    pub async fn mark_build_failed(&self, error: impl Into<String>) {
        self.upstream.mark_build_failed(error).await;
    }

    /// Resets raw/RFQ caches and Redis while holding their shared publication gate.
    ///
    /// All services passed here must have been built with the same gate and Redis publisher.
    pub async fn handle_shared_generation_reset(
        services: &[Self],
        reason: impl Into<String>,
        last_error: Option<String>,
    ) {
        Self::handle_shared_generation_reset_with_cache_scope(
            services, services, reason, last_error,
        )
        .await;
    }

    /// Advances the shared Redis generation while only clearing caches that restarted.
    ///
    /// Snapshot sessions belong to the shared generation, so they are closed for every service.
    /// Preserved caches are relabelled to the new generation instead of being rewarmed.
    pub async fn handle_shared_generation_reset_with_cache_scope(
        generation_services: &[Self],
        cache_reset_services: &[Self],
        reason: impl Into<String>,
        last_error: Option<String>,
    ) {
        assert!(
            !generation_services.is_empty(),
            "broadcaster generation reset requires at least one service"
        );
        assert!(
            !cache_reset_services.is_empty(),
            "broadcaster generation reset requires at least one cache reset service"
        );
        debug_assert!(
            generation_services.iter().all(|service| Arc::ptr_eq(
                &service.lifecycle_gate,
                &generation_services[0].lifecycle_gate
            )),
            "shared broadcaster generation reset requires one lifecycle gate"
        );
        debug_assert!(
            generation_services.iter().all(|service| Arc::ptr_eq(
                &service.redis_publisher,
                &generation_services[0].redis_publisher
            )),
            "shared broadcaster generation reset requires one Redis publisher"
        );
        assert!(
            cache_reset_services.iter().all(|reset_service| {
                generation_services
                    .iter()
                    .any(|service| service.ptr_eq(reset_service))
            }),
            "cache reset services must be part of the shared generation reset"
        );
        let reason = reason.into();
        let _gate = generation_services[0].lifecycle_gate.lock().await;
        let publisher_mode = generation_services[0].redis_publisher.mode().await;
        let mut reset_backends = Vec::new();
        for service in generation_services {
            if cache_reset_services
                .iter()
                .any(|reset_service| service.ptr_eq(reset_service))
            {
                service
                    .upstream
                    .mark_disconnected(reason.clone(), last_error.clone())
                    .await;
            }
            service
                .snapshot_sessions
                .disconnect_all(SessionCloseReason::GenerationReset)
                .await;
            if publisher_mode != BroadcasterRedisPublisherMode::Retired {
                reset_backends.extend(service.cache.configured_backends());
            }
        }
        if publisher_mode == BroadcasterRedisPublisherMode::Retired {
            return;
        }
        reset_backends.sort();
        reset_backends.dedup();
        let boundary = match generation_services[0]
            .redis_publisher
            .reset_generation_boundary(reason, reset_backends)
            .await
        {
            Ok(boundary) => boundary,
            Err(error) => {
                warn!(
                    error = %error,
                    "Skipping broadcaster cache reset because Redis generation reset failed"
                );
                return;
            }
        };
        for service in generation_services {
            if cache_reset_services
                .iter()
                .any(|reset_service| service.ptr_eq(reset_service))
            {
                service.cache.reset_to_generation(boundary.generation).await;
            } else {
                service.cache.relabel_generation(boundary.generation).await;
            }
        }
    }

    fn ptr_eq(&self, other: &Self) -> bool {
        self.snapshot_sessions.ptr_eq(&other.snapshot_sessions)
    }

    pub(crate) async fn redis_publisher_needs_generation_reset(&self) -> bool {
        matches!(
            self.redis_publisher.mode().await,
            BroadcasterRedisPublisherMode::Retired | BroadcasterRedisPublisherMode::Unhealthy
        )
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

        let _gate = services[0].lifecycle_gate.lock().await;
        match services[0].redis_publisher.mode().await {
            BroadcasterRedisPublisherMode::Active => {
                return services[0]
                    .redis_publisher
                    .replay_boundary()
                    .await
                    .map(Some);
            }
            BroadcasterRedisPublisherMode::Passive => {}
            BroadcasterRedisPublisherMode::Retired | BroadcasterRedisPublisherMode::Unhealthy => {
                return Ok(None);
            }
        }

        for service in services {
            let status = service.status_snapshot().await;
            if !matches!(
                status.readiness,
                BroadcasterReadiness::Ready | BroadcasterReadiness::SnapshotUnexportable
            ) {
                return Ok(None);
            }
        }

        if let Err(error) = Self::run_snapshot_export_preflight_locked(services).await {
            warn!(
                event = "broadcaster_snapshot_export_failed",
                error = %error,
                "Snapshot export preflight failed before writer promotion"
            );
            return Ok(None);
        }

        // The process has warmed every configured cache. Promotion is the point
        // where that local state becomes the active snapshot generation.
        let mut base_heads = Vec::new();
        for service in services {
            base_heads.extend(service.cache.backend_heads().await);
        }
        let boundary = services[0]
            .redis_publisher
            .promote(base_heads, reason)
            .await?;
        for service in services {
            service.cache.relabel_generation(boundary.generation).await;
        }
        Ok(Some(boundary))
    }

    pub async fn apply_update(&self, update: &TychoUpdate) -> Result<()> {
        let _gate = self.lifecycle_gate.lock().await;
        let staged = self.cache.stage_update(update).await?;
        if let Some(message) = staged.message() {
            self.publish_to_redis(BroadcasterPayload::Update(message.clone()))
                .await?;
        }
        self.cache.commit_staged_update(staged).await;
        self.upstream.record_update().await;
        Ok(())
    }

    pub async fn apply_feed_message(&self, feed: &FeedMessage<BlockHeader>) -> Result<bool> {
        let _gate = self.lifecycle_gate.lock().await;
        let mut staged = self.cache.stage_feed_message(feed).await?;
        let publishes_update = staged.publishes_update();
        let recovery_stats = staged.recovery_stats();
        if staged.is_recovery_commit() {
            let Some(message) = staged.message() else {
                return Ok(false);
            };
            let serialized_bytes = self
                .cache
                .redis_payload_json_size(BroadcasterPayload::Update(message.clone()))
                .await?;
            if serialized_bytes > self.snapshot_max_payload_bytes {
                warn!(
                    event = "broadcaster_upstream_recovery_failed",
                    serialized_bytes,
                    max_payload_bytes = self.snapshot_max_payload_bytes,
                    "Refusing broadcaster update above the configured payload cap"
                );
                staged.defer_oversized_recovery(format!(
                    "compact recovery is {serialized_bytes} bytes, above configured max {}",
                    self.snapshot_max_payload_bytes
                ));
                self.cache.commit_staged_update(staged).await;
                return Ok(false);
            }
        }
        if let Some(message) = staged.message() {
            self.publish_to_redis(BroadcasterPayload::Update(message.clone()))
                .await?;
        }
        self.cache.commit_staged_update(staged).await;
        if let Some(stats) = recovery_stats {
            tracing::info!(
                event = "broadcaster_upstream_recovery_committed",
                recovery_id = stats.id,
                elapsed_ms = stats.elapsed_ms,
                serialized_bytes = stats.serialized_bytes,
                "Aligned Tycho replacement state published and committed"
            );
        }
        if publishes_update {
            self.upstream.record_update().await;
        }
        Ok(publishes_update)
    }

    pub async fn broadcast_heartbeat(&self) -> Result<()> {
        let _gate = self.lifecycle_gate.lock().await;
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

        let _gate = services[0].lifecycle_gate.lock().await;
        let mut chain_id = None;
        for service in services {
            let status = service.status_snapshot().await;
            if !matches!(
                status.readiness,
                BroadcasterReadiness::Ready
                    | BroadcasterReadiness::SnapshotUnexportable
                    | BroadcasterReadiness::UpstreamRecovering
            ) {
                return Ok(None);
            }
            match chain_id {
                Some(expected) => anyhow::ensure!(
                    status.chain_id == expected,
                    "combined broadcaster snapshot session chain_id mismatch: expected {expected}, found {}",
                    status.chain_id
                ),
                None => chain_id = Some(status.chain_id),
            }
        }
        let snapshot = Self::run_snapshot_export_preflight_locked(services).await?;
        let chain_id = chain_id.ok_or_else(|| {
            anyhow::anyhow!("combined broadcaster snapshot session missing chain_id")
        })?;
        // Snapshot export and replay-boundary capture have to describe the same
        // active writer. Passive or stale writers fail closed here.
        let redis_replay_boundary = match services[0].redis_publisher.replay_boundary().await {
            Ok(boundary) => boundary,
            Err(error) => {
                warn!(
                    error = %error,
                    "Refusing broadcaster snapshot session without Redis replay boundary"
                );
                return Ok(None);
            }
        };
        services[0]
            .snapshot_sessions
            .create_snapshot_session(snapshot, chain_id, redis_replay_boundary, ttl)
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
                .disconnect_all(SessionCloseReason::GenerationReset)
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
            )
            .await;
        let preflight = self.snapshot_preflight.read().await;
        let now = Instant::now();
        snapshot.snapshot.exportable =
            preflight.checked_at.is_none() || preflight.last_error.is_none();
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
        if snapshot.readiness == BroadcasterReadiness::Ready && !snapshot.snapshot.exportable {
            snapshot.readiness = BroadcasterReadiness::SnapshotUnexportable;
        }
        snapshot
    }

    pub async fn run_snapshot_export_preflight(services: &[Self]) -> Result<()> {
        anyhow::ensure!(
            !services.is_empty(),
            "snapshot export preflight requires at least one service"
        );
        ensure_shared_lifecycle(services, "snapshot export preflight")?;
        for service in services {
            if !service.cache.is_ready().await {
                return Ok(());
            }
        }
        let _gate = services[0].lifecycle_gate.lock().await;
        Self::run_snapshot_export_preflight_locked(services)
            .await
            .map(|_| ())
    }

    async fn run_snapshot_export_preflight_locked(
        services: &[Self],
    ) -> Result<BroadcasterSnapshotExport> {
        let started_at = Instant::now();
        let result = async {
            let mut chain_id = None;
            let mut exports = Vec::with_capacity(services.len());
            for service in services {
                let status = service
                    .cache
                    .status_snapshot(
                        service.snapshot_max_payload_bytes,
                        service.upstream.snapshot().await,
                        service.snapshot_sessions.snapshot().await,
                    )
                    .await;
                if !status.snapshot.ready {
                    anyhow::bail!("snapshot cache is still warming up");
                }
                chain_id.get_or_insert(status.chain_id);
                exports.push(
                    service
                        .cache
                        .export_snapshot(service.snapshot_max_payload_bytes)
                        .await?,
                );
            }
            combine_snapshot_exports(
                chain_id
                    .ok_or_else(|| anyhow::anyhow!("snapshot export preflight missing chain_id"))?,
                exports,
            )
        }
        .await;
        let result = result.and_then(|export| {
            validate_snapshot_export_envelopes(&export)
                .map(|largest_payload_bytes| (export, largest_payload_bytes))
        });
        let duration_ms = started_at.elapsed().as_millis() as u64;
        let checked_at = Instant::now();
        match result {
            Ok((export, largest_payload_bytes)) => {
                for service in services {
                    let mut preflight = service.snapshot_preflight.write().await;
                    preflight.checked_at = Some(checked_at);
                    preflight.succeeded_at = Some(checked_at);
                    preflight.duration_ms = Some(duration_ms);
                    preflight.payload_count = Some(export.payloads.len());
                    preflight.largest_payload_bytes = Some(largest_payload_bytes);
                    preflight.last_error = None;
                }
                tracing::info!(
                    event = "broadcaster_snapshot_export_succeeded",
                    duration_ms,
                    payload_count = export.payloads.len(),
                    largest_payload_bytes,
                    max_payload_bytes = export.max_payload_bytes,
                    "Broadcaster snapshot export preflight succeeded"
                );
                Ok(export)
            }
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
                    event = "broadcaster_snapshot_export_failed",
                    duration_ms,
                    error = %error_message,
                    "Broadcaster snapshot export preflight failed"
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
                .disconnect_all(SessionCloseReason::GenerationReset)
                .await;
        }
        result.context("failed to publish accepted broadcaster delta to Redis")
    }

    #[cfg(test)]
    pub(crate) async fn lock_lifecycle_gate_for_test(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.lifecycle_gate.clone().lock_owned().await
    }
}

fn validate_snapshot_export_envelopes(export: &BroadcasterSnapshotExport) -> Result<usize> {
    let mut largest_payload_bytes = 0usize;
    for (index, payload) in export.payloads.iter().enumerate() {
        let message_seq = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
        let envelope =
            BroadcasterEnvelope::new(export.stream_id.clone(), message_seq, payload.clone());
        let bytes = serde_json::to_vec(&envelope)?.len();
        anyhow::ensure!(
            bytes <= export.max_payload_bytes,
            "snapshot payload {index} is {bytes} bytes, above configured max {}",
            export.max_payload_bytes
        );
        largest_payload_bytes = largest_payload_bytes.max(bytes);
    }
    Ok(largest_payload_bytes)
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
    use std::sync::Arc;

    use anyhow::{anyhow, Result};
    use num_bigint::BigUint;
    use tokio::sync::Mutex;
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
            dto::{AccountUpdate, BlockChanges, Chain as DtoChain, ChangeType, ResponseAccount},
            models::{token::Token, Chain},
            simulation::protocol_sim::{Balances, GetAmountOutResult, ProtocolSim},
            Bytes,
        },
    };

    use super::{
        BroadcasterServiceState, BroadcasterSnapshotSessionRegistry, SnapshotSessionError,
    };
    use crate::broadcaster::redis_publisher::{
        BroadcasterRedisPublisher, BroadcasterRedisPublisherConfig, RedisStreamWriter,
    };
    use crate::broadcaster::state::{
        BroadcasterReadiness, BroadcasterSnapshotCache, BroadcasterSnapshotExport,
        BroadcasterUpstreamState,
    };
    use simulator_core::broadcaster::{
        BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterMessageKind,
        BroadcasterPayload, BroadcasterProgress, BroadcasterRedisStreamEntry,
        BroadcasterSnapshotEnd, BroadcasterSnapshotStart,
    };

    #[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
    struct DummySim(u8);

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
        .await?;

        assert!(promotion.is_none());
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
    async fn promotion_marker_handoff_base_heads_match_warmed_caches() -> Result<()> {
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
        let handoff = progress_payload(&marker)?
            .handoff
            .ok_or_else(|| anyhow!("promotion marker should include handoff proof"))?;
        let mut expected_heads = vec![
            BroadcasterBackendHead::new(BroadcasterBackend::Native, 101),
            BroadcasterBackendHead::new(BroadcasterBackend::Rfq, 202),
        ];
        expected_heads.sort_by_key(|head| head.backend);
        assert_eq!(handoff.base_heads, expected_heads);
        Ok(())
    }

    #[tokio::test]
    async fn active_publication_failure_keeps_update_out_of_cache() -> Result<()> {
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
        writer.fail_next_appends(100).await;

        let Err(error) = service
            .apply_update(&native_only_update(10, "native-1"))
            .await
        else {
            return Err(anyhow!("active service must fail when Redis append fails"));
        };

        assert!(format!("{error:#}").contains("failed to publish accepted broadcaster delta"));
        let status = service.status_snapshot().await;
        assert!(!status.snapshot.ready);
        assert_eq!(status.snapshot.total_states, 0);
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
    async fn snapshot_session_boundary_is_registered_before_queued_update() -> Result<()> {
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

        let update_task = tokio::spawn({
            let service = service.clone();
            async move {
                service
                    .apply_update(&native_only_update(11, "native-2"))
                    .await
            }
        });

        assert!(timeout(Duration::from_millis(25), &mut subscribe_task)
            .await
            .is_err());
        drop(gate);

        let session = subscribe_task
            .await
            .map_err(|error| anyhow!("subscribe task failed: {error}"))??
            .ok_or_else(|| anyhow!("expected ready broadcaster snapshot session"))?;

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

        let update_task = tokio::spawn({
            let rfq_service = rfq_service.clone();
            async move {
                rfq_service
                    .apply_update(&rfq_only_update(13, "rfq-2", 8))
                    .await
            }
        });

        assert!(timeout(Duration::from_millis(25), &mut subscribe_task)
            .await
            .is_err());
        drop(gate);

        let session = subscribe_task
            .await
            .map_err(|error| anyhow!("subscribe task failed: {error}"))??
            .ok_or_else(|| anyhow!("expected combined broadcaster snapshot session"))?;
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
    async fn pending_session_is_cleared_by_generation_reset() -> Result<()> {
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

        assert!(timeout(Duration::from_millis(25), &mut subscribe_task)
            .await
            .is_err());
        drop(gate);

        let session = subscribe_task
            .await
            .map_err(|error| anyhow!("subscribe task failed: {error}"))??
            .ok_or_else(|| anyhow!("expected ready broadcaster snapshot session"))?;

        let reset_task = tokio::spawn({
            let service = service.clone();
            async move {
                BroadcasterServiceState::handle_shared_generation_reset(
                    std::slice::from_ref(&service),
                    "stale",
                    Some("boom".to_string()),
                )
                .await
            }
        });
        reset_task
            .await
            .map_err(|error| anyhow!("reset task failed: {error}"))?;

        let Err(error) = service
            .snapshot_session_payload(session.session_id, 0)
            .await
        else {
            return Err(anyhow!("reset snapshot session should not serve payloads"));
        };
        assert_eq!(error, SnapshotSessionError::NotFound);
        let status = service.status_snapshot().await;
        assert_eq!(status.snapshot_sessions.active, 0);
        assert_eq!(
            status.snapshot_sessions.last_error.as_deref(),
            Some("all snapshot sessions closed: generation_reset")
        );
        assert_eq!(status.snapshot.stream_id, "chain-1-stream-2");
        Ok(())
    }

    #[tokio::test]
    async fn generation_reset_requires_new_redis_stream_generation() -> Result<()> {
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
        let initial_stream_id = publisher.status_snapshot().await.stream_id;

        BroadcasterServiceState::handle_shared_generation_reset(
            std::slice::from_ref(&service),
            "stale",
            Some("boom".to_string()),
        )
        .await;

        let reset_status = publisher.status_snapshot().await;
        assert!(reset_status.healthy);
        assert_ne!(reset_status.stream_id, initial_stream_id);
        assert!(reset_status.replay_boundary.is_some());

        service.mark_upstream_connected().await;
        service
            .apply_update(&native_only_update(11, "native-2"))
            .await?;

        let recovered_status = publisher.status_snapshot().await;
        assert!(recovered_status.healthy);
        assert_eq!(recovered_status.stream_id, reset_status.stream_id);
        assert!(recovered_status.replay_boundary.is_some());
        let reset_generation_entries = writer
            .appends()
            .await
            .into_iter()
            .filter(|entry| entry.stream_id == recovered_status.stream_id)
            .map(|entry| (entry.message_seq, entry.kind))
            .collect::<Vec<_>>();
        assert_eq!(
            reset_generation_entries,
            vec![
                (1, BroadcasterMessageKind::Progress),
                (2, BroadcasterMessageKind::Update)
            ]
        );
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
    #[expect(
        clippy::too_many_lines,
        reason = "the regression keeps session, generation, oversize, and recovery assertions together"
    )]
    async fn oversized_initial_raw_snapshot_warms_but_recovery_diff_stays_private() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Vm]);
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
        ));
        publisher
            .promote(base_heads([BroadcasterBackend::Vm]), "test_active")
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            2_500,
            cache,
            BroadcasterUpstreamState::default(),
            Arc::clone(&publisher),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        let header = |number, hash, parent| BlockHeader {
            hash: Bytes::from([hash; 32]),
            number,
            parent_hash: Bytes::from([parent; 32]),
            revert: false,
            timestamp: number * 10,
            partial_block_index: None,
        };
        let address = Bytes::from([42u8; 20]);
        let feed = |block: BlockHeader, value: u8| {
            let account = ResponseAccount::new(
                DtoChain::Ethereum,
                address.clone(),
                "large-account".to_string(),
                (0..20)
                    .map(|index| (Bytes::from([index; 32]), Bytes::from(vec![value; 256])))
                    .collect(),
                Bytes::from([value; 32]),
                HashMap::new(),
                Bytes::from([7u8; 128]),
                Bytes::from([8u8; 32]),
                Bytes::from([9u8; 32]),
                Bytes::from([10u8; 32]),
                None,
            );
            FeedMessage {
                state_msgs: HashMap::from([(
                    "vm:curve".to_string(),
                    StateSyncMessage {
                        header: block.clone(),
                        snapshots: Snapshot {
                            states: HashMap::new(),
                            vm_storage: HashMap::from([(address.clone(), account)]),
                        },
                        deltas: None,
                        removed_components: HashMap::new(),
                    },
                )]),
                sync_states: HashMap::from([(
                    "vm:curve".to_string(),
                    SynchronizerState::Ready(block),
                )]),
            }
        };

        assert!(
            service
                .apply_feed_message(&feed(header(10, 10, 9), 1))
                .await?
        );
        let session = service
            .create_snapshot_session(Duration::from_secs(300))
            .await?
            .ok_or_else(|| anyhow!("active service should create a snapshot session"))?;
        let boundary_before = publisher
            .status_snapshot()
            .await
            .replay_boundary
            .ok_or_else(|| anyhow!("active publisher should expose a boundary"))?;
        let before =
            serde_json::to_value(service.cache.export_snapshot(8_388_608).await?.payloads)?;
        let append_count = writer.appends().await.len();

        assert!(
            !service
                .apply_feed_message(&feed(header(12, 12, 11), 2))
                .await?
        );
        assert_eq!(writer.appends().await.len(), append_count);
        let degraded = service.status_snapshot().await;
        assert_eq!(degraded.readiness, BroadcasterReadiness::UpstreamRecovering);
        assert!(degraded.snapshot.recovery_pending);
        assert!(degraded.snapshot.recovery_error.is_some());
        let recovery_id = degraded.snapshot.recovery_id;
        assert_eq!(
            publisher.status_snapshot().await.replay_boundary,
            Some(boundary_before.clone())
        );
        assert!(service
            .snapshot_session_payload(session.session_id, 0)
            .await
            .is_ok());
        let recovery_session = service
            .create_snapshot_session(Duration::from_secs(300))
            .await?
            .ok_or_else(|| anyhow!("committed snapshot should remain available during recovery"))?;
        assert_eq!(recovery_session.stream_id, boundary_before.stream_id);
        assert_eq!(recovery_session.snapshot_id, boundary_before.snapshot_id);
        assert_eq!(recovery_session.redis_replay_boundary, boundary_before);
        assert!(service
            .snapshot_session_payload(recovery_session.session_id, 0)
            .await
            .is_ok());
        assert_eq!(writer.appends().await.len(), append_count);
        assert_eq!(
            serde_json::to_value(service.cache.export_snapshot(8_388_608).await?.payloads,)?,
            before
        );
        assert!(
            !service
                .apply_feed_message(&feed(header(12, 12, 11), 2))
                .await?
        );
        assert_eq!(writer.appends().await.len(), append_count);
        assert_eq!(
            service.status_snapshot().await.snapshot.recovery_id,
            recovery_id
        );

        let block_13 = header(13, 13, 12);
        let deletion = FeedMessage {
            state_msgs: HashMap::from([(
                "vm:curve".to_string(),
                StateSyncMessage {
                    header: block_13.clone(),
                    snapshots: Snapshot::default(),
                    deltas: Some(BlockChanges {
                        account_updates: HashMap::from([(
                            address.clone(),
                            AccountUpdate::new(
                                address,
                                DtoChain::Ethereum,
                                HashMap::new(),
                                None,
                                None,
                                ChangeType::Deletion,
                            ),
                        )]),
                        ..Default::default()
                    }),
                    removed_components: HashMap::new(),
                },
            )]),
            sync_states: HashMap::from([(
                "vm:curve".to_string(),
                SynchronizerState::Ready(block_13),
            )]),
        };
        assert!(service.apply_feed_message(&deletion).await?);
        assert_eq!(writer.appends().await.len(), append_count + 1);
        let recovered = service.status_snapshot().await;
        assert_eq!(recovered.readiness, BroadcasterReadiness::Ready);
        assert!(!recovered.snapshot.recovery_pending);
        let boundary_after = publisher
            .status_snapshot()
            .await
            .replay_boundary
            .ok_or_else(|| anyhow!("recovered publisher should keep its boundary"))?;
        assert_eq!(boundary_after.generation, boundary_before.generation);
        assert_eq!(boundary_after.stream_id, boundary_before.stream_id);
        assert_eq!(boundary_after.snapshot_id, boundary_before.snapshot_id);
        assert_eq!(
            boundary_after.exclusive_message_seq,
            boundary_before.exclusive_message_seq + 1
        );
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the exact wire boundary compares accepted and deferred recovery services"
    )]
    async fn compact_recovery_cap_uses_full_redis_envelope_size() -> Result<()> {
        let header = |number, hash, parent| BlockHeader {
            hash: Bytes::from([hash; 32]),
            number,
            parent_hash: Bytes::from([parent; 32]),
            revert: false,
            timestamp: number * 10,
            partial_block_index: None,
        };
        let address = Bytes::from([43u8; 20]);
        let feed = |block: BlockHeader, value: u8| FeedMessage {
            state_msgs: HashMap::from([(
                "vm:curve".to_string(),
                StateSyncMessage {
                    header: block.clone(),
                    snapshots: Snapshot {
                        states: HashMap::new(),
                        vm_storage: HashMap::from([(
                            address.clone(),
                            ResponseAccount::new(
                                DtoChain::Ethereum,
                                address.clone(),
                                "account".to_string(),
                                HashMap::from([(Bytes::from([1u8; 32]), Bytes::from([value; 64]))]),
                                Bytes::from([value; 32]),
                                HashMap::new(),
                                Bytes::from([value; 32]),
                                Bytes::from([2u8; 32]),
                                Bytes::from([3u8; 32]),
                                Bytes::from([4u8; 32]),
                                None,
                            ),
                        )]),
                    },
                    deltas: None,
                    removed_components: HashMap::new(),
                },
            )]),
            sync_states: HashMap::from([("vm:curve".to_string(), SynchronizerState::Ready(block))]),
        };
        let initial = feed(header(10, 10, 9), 1);
        let replacement = feed(header(12, 12, 11), 2);

        let sizing_cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Vm]);
        sizing_cache.apply_feed_message(&initial).await?;
        let staged = sizing_cache.stage_feed_message(&replacement).await?;
        let payload = BroadcasterPayload::Update(
            staged
                .message()
                .ok_or_else(|| anyhow!("aligned recovery should produce a compact update"))?
                .clone(),
        );
        let wire_bytes = sizing_cache
            .redis_payload_json_size(payload.clone())
            .await?;
        let payload_only_bytes = serde_json::to_vec(&payload)?.len();
        assert!(wire_bytes > payload_only_bytes);

        let accepted_cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Vm]);
        let accepted_writer = ServiceFakeRedisWriter::default();
        let accepted_publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(accepted_writer.clone()),
        ));
        accepted_publisher
            .promote(base_heads([BroadcasterBackend::Vm]), "test_active")
            .await?;
        let accepted = BroadcasterServiceState::with_lifecycle_gate(
            wire_bytes,
            accepted_cache,
            BroadcasterUpstreamState::default(),
            accepted_publisher,
            Arc::new(Mutex::new(())),
        );
        accepted.mark_upstream_connected().await;
        assert!(accepted.apply_feed_message(&initial).await?);
        let accepted_before = accepted_writer.appends().await.len();
        assert!(accepted.apply_feed_message(&replacement).await?);
        assert_eq!(accepted_writer.appends().await.len(), accepted_before + 1);

        let deferred_cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Vm]);
        let deferred_writer = ServiceFakeRedisWriter::default();
        let deferred_publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(deferred_writer.clone()),
        ));
        deferred_publisher
            .promote(base_heads([BroadcasterBackend::Vm]), "test_active")
            .await?;
        let deferred = BroadcasterServiceState::with_lifecycle_gate(
            wire_bytes - 1,
            deferred_cache,
            BroadcasterUpstreamState::default(),
            deferred_publisher,
            Arc::new(Mutex::new(())),
        );
        deferred.mark_upstream_connected().await;
        assert!(deferred.apply_feed_message(&initial).await?);
        let deferred_before = deferred_writer.appends().await.len();
        assert!(!deferred.apply_feed_message(&replacement).await?);
        assert_eq!(deferred_writer.appends().await.len(), deferred_before);
        assert_eq!(
            deferred.status_snapshot().await.readiness,
            BroadcasterReadiness::UpstreamRecovering
        );
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
    async fn shared_generation_reset_keeps_backend_caches_and_redis_publisher_aligned() -> Result<()>
    {
        let (raw_service, rfq_service, _writer, publisher) = ready_raw_and_rfq_services().await?;

        BroadcasterServiceState::handle_shared_generation_reset(
            &[raw_service.clone(), rfq_service.clone()],
            "stale",
            Some("boom".to_string()),
        )
        .await;

        let raw_status = raw_service.status_snapshot().await;
        assert_eq!(
            raw_status.readiness,
            BroadcasterReadiness::UpstreamDisconnected
        );
        assert_eq!(raw_status.snapshot.stream_id, "chain-1-stream-2");
        assert_eq!(raw_status.snapshot.total_states, 0);

        let rfq_status = rfq_service.status_snapshot().await;
        assert_eq!(
            rfq_status.readiness,
            BroadcasterReadiness::UpstreamDisconnected
        );
        assert_eq!(rfq_status.snapshot.stream_id, "chain-1-stream-2");
        assert_eq!(rfq_status.snapshot.total_states, 0);

        assert_eq!(
            publisher.status_snapshot().await.stream_id,
            "chain-1-stream-2"
        );
        Ok(())
    }

    #[tokio::test]
    async fn rfq_generation_reset_relabels_raw_cache_without_clearing_it() -> Result<()> {
        let (raw_service, rfq_service, _writer, _publisher) = ready_raw_and_rfq_services().await?;
        let raw_before = raw_service.status_snapshot().await;
        assert_eq!(raw_before.readiness, BroadcasterReadiness::Ready);
        assert_eq!(raw_before.snapshot.total_states, 1);

        BroadcasterServiceState::handle_shared_generation_reset_with_cache_scope(
            &[raw_service.clone(), rfq_service.clone()],
            std::slice::from_ref(&rfq_service),
            "rfq_restart",
            Some("rfq stale".to_string()),
        )
        .await;

        let raw_after = raw_service.status_snapshot().await;
        assert_eq!(raw_after.readiness, BroadcasterReadiness::Ready);
        assert_eq!(raw_after.snapshot.stream_id, "chain-1-stream-2");
        assert_eq!(raw_after.snapshot.snapshot_id, "chain-1-snapshot-2");
        assert_eq!(
            raw_after.snapshot.total_states,
            raw_before.snapshot.total_states
        );

        let rfq_after = rfq_service.status_snapshot().await;
        assert_eq!(
            rfq_after.readiness,
            BroadcasterReadiness::UpstreamDisconnected
        );
        assert_eq!(rfq_after.snapshot.stream_id, "chain-1-stream-2");
        assert_eq!(rfq_after.snapshot.snapshot_id, "chain-1-snapshot-2");
        assert_eq!(rfq_after.snapshot.total_states, 0);
        Ok(())
    }

    #[tokio::test]
    async fn raw_generation_reset_relabels_rfq_cache_without_clearing_it() -> Result<()> {
        let (raw_service, rfq_service, _writer, _publisher) = ready_raw_and_rfq_services().await?;
        let rfq_before = rfq_service.status_snapshot().await;
        assert_eq!(rfq_before.readiness, BroadcasterReadiness::Ready);
        assert_eq!(rfq_before.snapshot.total_states, 1);

        BroadcasterServiceState::handle_shared_generation_reset_with_cache_scope(
            &[raw_service.clone(), rfq_service.clone()],
            std::slice::from_ref(&raw_service),
            "raw_restart",
            Some("raw stale".to_string()),
        )
        .await;

        let raw_after = raw_service.status_snapshot().await;
        assert_eq!(
            raw_after.readiness,
            BroadcasterReadiness::UpstreamDisconnected
        );
        assert_eq!(raw_after.snapshot.stream_id, "chain-1-stream-2");
        assert_eq!(raw_after.snapshot.snapshot_id, "chain-1-snapshot-2");
        assert_eq!(raw_after.snapshot.total_states, 0);

        let rfq_after = rfq_service.status_snapshot().await;
        assert_eq!(rfq_after.readiness, BroadcasterReadiness::Ready);
        assert_eq!(rfq_after.snapshot.stream_id, "chain-1-stream-2");
        assert_eq!(rfq_after.snapshot.snapshot_id, "chain-1-snapshot-2");
        assert_eq!(
            rfq_after.snapshot.total_states,
            rfq_before.snapshot.total_states
        );
        Ok(())
    }

    #[tokio::test]
    async fn partial_generation_reset_uses_all_configured_backends_for_marker() -> Result<()> {
        let (raw_service, rfq_service, writer, _publisher) = raw_and_rfq_services().await?;

        BroadcasterServiceState::handle_shared_generation_reset_with_cache_scope(
            &[raw_service.clone(), rfq_service.clone()],
            std::slice::from_ref(&rfq_service),
            "rfq_restart",
            Some("rfq stale".to_string()),
        )
        .await;

        let reset_marker = writer
            .appends()
            .await
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("generation reset should publish a marker"))?;
        assert_eq!(reset_marker.stream_id, "chain-1-stream-2");
        assert_eq!(reset_marker.message_seq, 1);
        assert_eq!(reset_marker.backend_scope, "native,rfq");
        assert_eq!(
            progress_payload(&reset_marker)?.backends,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq]
        );
        Ok(())
    }

    #[tokio::test]
    async fn combined_snapshot_session_works_after_preserved_cache_is_relabelled() -> Result<()> {
        let (raw_service, rfq_service, _writer, _publisher) = ready_raw_and_rfq_services().await?;
        let old_session = BroadcasterServiceState::create_snapshot_session_for_services(
            &[raw_service.clone(), rfq_service.clone()],
            Duration::from_secs(300),
        )
        .await?
        .ok_or_else(|| anyhow!("expected initial combined snapshot session"))?;

        BroadcasterServiceState::handle_shared_generation_reset_with_cache_scope(
            &[raw_service.clone(), rfq_service.clone()],
            std::slice::from_ref(&rfq_service),
            "rfq_restart",
            Some("rfq stale".to_string()),
        )
        .await;

        let Err(error) = raw_service
            .snapshot_session_payload(old_session.session_id, 0)
            .await
        else {
            return Err(anyhow!("old generation snapshot session should be closed"));
        };
        assert_eq!(error, SnapshotSessionError::NotFound);

        rfq_service.mark_upstream_connected().await;
        rfq_service
            .apply_update(&rfq_only_update(203, "rfq-2", 8))
            .await?;

        let session = BroadcasterServiceState::create_snapshot_session_for_services(
            &[raw_service.clone(), rfq_service.clone()],
            Duration::from_secs(300),
        )
        .await?
        .ok_or_else(|| anyhow!("expected combined snapshot session after RFQ rewarm"))?;

        assert_eq!(session.stream_id, "chain-1-stream-2");
        assert_eq!(session.snapshot_id, "chain-1-snapshot-2");
        assert_eq!(session.redis_replay_boundary.stream_id, "chain-1-stream-2");
        assert_eq!(
            session.redis_replay_boundary.snapshot_id,
            "chain-1-snapshot-2"
        );
        let start = raw_service
            .snapshot_session_payload(session.session_id, 0)
            .await
            .map_err(|error| anyhow!("snapshot payload failed: {error:?}"))?;
        assert_eq!(start.stream_id, "chain-1-stream-2");
        assert!(matches!(
            start.payload,
            BroadcasterPayload::SnapshotStart(ref start)
                if start.snapshot_id == "chain-1-snapshot-2"
                    && start.backends == vec![BroadcasterBackend::Native, BroadcasterBackend::Rfq]
        ));
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_session_response_includes_redis_replay_boundary() -> Result<()> {
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
        service
            .apply_update(&native_only_update(10, "native-1"))
            .await?;

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
    async fn pending_session_rejects_snapshot_stream_id_boundary_mismatch() -> Result<()> {
        let registry = BroadcasterSnapshotSessionRegistry::new();

        let Err(error) = registry
            .create_snapshot_session(
                snapshot_export(),
                1,
                replay_boundary_with_ids("stream-2", "snapshot-1"),
                Duration::from_secs(300),
            )
            .await
        else {
            return Err(anyhow!(
                "snapshot stream_id mismatch should reject the session"
            ));
        };

        assert!(
            format!("{error:#}").contains("snapshot stream_id mismatch"),
            "unexpected error: {error:#}"
        );
        assert_eq!(registry.snapshot().await.active, 0);
        Ok(())
    }

    #[tokio::test]
    async fn pending_session_rejects_snapshot_id_boundary_mismatch() -> Result<()> {
        let registry = BroadcasterSnapshotSessionRegistry::new();

        let Err(error) = registry
            .create_snapshot_session(
                snapshot_export(),
                1,
                replay_boundary_with_ids("stream-1", "snapshot-2"),
                Duration::from_secs(300),
            )
            .await
        else {
            return Err(anyhow!("snapshot_id mismatch should reject the session"));
        };

        assert!(
            format!("{error:#}").contains("snapshot_id mismatch"),
            "unexpected error: {error:#}"
        );
        assert_eq!(registry.snapshot().await.active, 0);
        Ok(())
    }

    #[tokio::test]
    async fn pending_session_serves_payloads_until_expiry() -> Result<()> {
        let registry = BroadcasterSnapshotSessionRegistry::new();
        let session = registry
            .create_snapshot_session(
                snapshot_export(),
                1,
                replay_boundary(),
                Duration::from_secs(300),
            )
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
            .create_snapshot_session(
                snapshot_export(),
                1,
                replay_boundary(),
                Duration::from_secs(300),
            )
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
            .create_snapshot_session(
                snapshot_export(),
                1,
                replay_boundary(),
                Duration::from_secs(300),
            )
            .await?;

        registry
            .disconnect_all(super::SessionCloseReason::GenerationReset)
            .await;

        let Err(error) = registry.snapshot_payload(session.session_id, 0).await else {
            unreachable!("closed snapshot session should not serve payloads");
        };
        assert_eq!(error, SnapshotSessionError::NotFound);
        let snapshot = registry.snapshot().await;
        assert_eq!(snapshot.active, 0);
        assert_eq!(
            snapshot.last_error.as_deref(),
            Some("all snapshot sessions closed: generation_reset")
        );
        Ok(())
    }

    #[tokio::test]
    async fn expired_pending_session_records_expiry() -> Result<()> {
        let registry = BroadcasterSnapshotSessionRegistry::new();
        let session = registry
            .create_snapshot_session(
                snapshot_export(),
                1,
                replay_boundary(),
                Duration::from_millis(1),
            )
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
        Ok(service)
    }

    async fn raw_and_rfq_services() -> Result<(
        BroadcasterServiceState,
        BroadcasterServiceState,
        ServiceFakeRedisWriter,
        Arc<BroadcasterRedisPublisher>,
    )> {
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            publisher_config(),
            Arc::new(writer.clone()),
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

        Ok((raw_service, rfq_service, writer, publisher))
    }

    async fn ready_raw_and_rfq_services() -> Result<(
        BroadcasterServiceState,
        BroadcasterServiceState,
        ServiceFakeRedisWriter,
        Arc<BroadcasterRedisPublisher>,
    )> {
        let (raw_service, rfq_service, writer, publisher) = raw_and_rfq_services().await?;
        raw_service.mark_upstream_connected().await;
        rfq_service.mark_upstream_connected().await;
        raw_service
            .apply_update(&native_only_update(101, "native-1"))
            .await?;
        rfq_service
            .apply_update(&rfq_only_update(202, "rfq-1", 7))
            .await?;
        Ok((raw_service, rfq_service, writer, publisher))
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
        fail_next_appends: usize,
    }

    impl ServiceFakeRedisWriter {
        async fn fail_next_appends(&self, count: usize) {
            self.inner.lock().await.fail_next_appends = count;
        }

        async fn appends(&self) -> Vec<BroadcasterRedisStreamEntry> {
            self.inner.lock().await.appends.clone()
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
                if let Some(expected_token) = command.expected_writer_token {
                    if guard.active_token.is_some()
                        && (guard.active_token.as_deref() != Some(expected_token)
                            || Some(guard.active_generation) != command.expected_generation)
                    {
                        return Err(anyhow!("stale Redis broadcaster writer token"));
                    }
                    if guard.active_token.is_none() {
                        guard.active_generation = guard
                            .active_generation
                            .max(command.expected_generation.unwrap_or_default());
                    }
                }
                guard.active_generation = guard.active_generation.saturating_add(1);
                guard.active_token = Some(command.writer_token.to_string());
                let generation = guard.active_generation;
                let entry_id = format!("{generation}-1");
                let previous_tail = guard.appends.last().cloned();
                let marker_fields =
                    if previous_tail.is_some() && !command.handoff_marker_fields.is_empty() {
                        command.handoff_marker_fields
                    } else {
                        command.normal_marker_fields
                    };
                let previous_stream_id = previous_tail
                    .as_ref()
                    .map(|entry| entry.stream_id.as_str())
                    .unwrap_or_default();
                let previous_entry_id = previous_tail
                    .as_ref()
                    .map(crate::broadcaster::redis_publisher::redis_entry_id)
                    .transpose()?
                    .unwrap_or_default();
                guard.appends.push(entry_from_fields(
                    marker_fields,
                    generation,
                    previous_stream_id,
                    &previous_entry_id,
                )?);
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
                let mut guard = self.inner.lock().await;
                if guard.active_token.is_some()
                    && (guard.active_token.as_deref() != Some(command.writer_token)
                        || guard.active_generation != command.generation)
                {
                    return Err(anyhow!("stale Redis broadcaster writer token"));
                }
                if guard.active_token.is_none() {
                    guard.active_generation = guard.active_generation.max(command.generation);
                }
                if guard.fail_next_appends > 0 {
                    guard.fail_next_appends -= 1;
                    return Err(anyhow!("planned append failure"));
                }
                let entry_id = crate::broadcaster::redis_publisher::redis_entry_id(command.entry)?;
                guard.appends.push(command.entry.clone());
                Ok(entry_id)
            })
        }

        fn renew_writer<'a>(
            &'a self,
            command: crate::broadcaster::redis_publisher::RedisRenewCommand<'a>,
        ) -> futures::future::BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                let guard = self.inner.lock().await;
                if guard.active_token.is_some()
                    && (guard.active_token.as_deref() != Some(command.writer_token)
                        || guard.active_generation != command.generation)
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
        previous_stream_id: &str,
        previous_entry_id: &str,
    ) -> Result<BroadcasterRedisStreamEntry> {
        let mut value = serde_json::Map::new();
        for (field, field_value) in fields {
            let field_value = field_value
                .replace(
                    crate::broadcaster::redis_publisher::GENERATION_PLACEHOLDER,
                    &generation.to_string(),
                )
                .replace(
                    crate::broadcaster::redis_publisher::PREVIOUS_STREAM_ID_PLACEHOLDER,
                    previous_stream_id,
                )
                .replace(
                    crate::broadcaster::redis_publisher::PREVIOUS_ENTRY_ID_PLACEHOLDER,
                    previous_entry_id,
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
