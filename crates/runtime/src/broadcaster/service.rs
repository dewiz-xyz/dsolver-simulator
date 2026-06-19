use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::warn;
use tycho_simulation::{
    protocol::models::Update as TychoUpdate,
    tycho_client::feed::{BlockHeader, FeedMessage},
};

use crate::broadcaster::redis_publisher::BroadcasterRedisPublisher;
use crate::broadcaster::state::{
    combine_snapshot_exports, BroadcasterReadiness, BroadcasterSnapshotCache,
    BroadcasterSnapshotExport, BroadcasterSnapshotSessionsSnapshot, BroadcasterStatusSnapshot,
    BroadcasterUpstreamState,
};
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
    redis_publisher: Arc<BroadcasterRedisPublisher>,
    // This gate keeps snapshot export plus replay-boundary capture atomic with respect to
    // updates, heartbeats, and generation resets.
    lifecycle_gate: Arc<Mutex<()>>,
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
        assert!(
            !services.is_empty(),
            "broadcaster generation reset requires at least one service"
        );
        debug_assert!(
            services
                .iter()
                .all(|service| Arc::ptr_eq(&service.lifecycle_gate, &services[0].lifecycle_gate)),
            "shared broadcaster generation reset requires one lifecycle gate"
        );
        debug_assert!(
            services
                .iter()
                .all(|service| Arc::ptr_eq(&service.redis_publisher, &services[0].redis_publisher)),
            "shared broadcaster generation reset requires one Redis publisher"
        );
        let reason = reason.into();
        let _gate = services[0].lifecycle_gate.lock().await;
        let mut reset_backends = Vec::new();
        for service in services {
            service
                .upstream
                .mark_disconnected(reason.clone(), last_error.clone())
                .await;
            service
                .snapshot_sessions
                .disconnect_all(SessionCloseReason::GenerationReset)
                .await;
            reset_backends.extend(service.cache.configured_backends());
            service.cache.reset_generation().await;
        }
        reset_backends.sort();
        reset_backends.dedup();
        services[0]
            .redis_publisher
            .reset_generation(reason, reset_backends)
            .await;
    }

    pub async fn apply_update(&self, update: &TychoUpdate) -> Result<()> {
        let _gate = self.lifecycle_gate.lock().await;
        let staged = self.cache.stage_update(update).await?;
        self.publish_to_redis(BroadcasterPayload::Update(staged.message().clone()))
            .await?;
        self.cache.commit_staged_update(staged).await?;
        self.upstream.record_update().await;
        Ok(())
    }

    pub async fn apply_feed_message(&self, feed: &FeedMessage<BlockHeader>) -> Result<()> {
        let _gate = self.lifecycle_gate.lock().await;
        let staged = self.cache.stage_feed_message(feed)?;
        self.publish_to_redis(BroadcasterPayload::Update(staged.message().clone()))
            .await?;
        self.cache.commit_staged_update(staged).await?;
        self.upstream.record_update().await;
        Ok(())
    }

    pub async fn broadcast_heartbeat(&self) -> Result<()> {
        let _gate = self.lifecycle_gate.lock().await;
        if let Some(heartbeat) = self.cache.heartbeat().await? {
            self.publish_to_redis(heartbeat).await?;
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
        let mut exports = Vec::with_capacity(services.len());

        for service in services {
            let status = service.status_snapshot().await;
            if status.readiness != BroadcasterReadiness::Ready {
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
            exports.push(
                service
                    .cache
                    .export_snapshot(service.snapshot_max_payload_bytes)
                    .await?,
            );
        }

        let chain_id = chain_id.ok_or_else(|| {
            anyhow::anyhow!("combined broadcaster snapshot session missing chain_id")
        })?;
        let snapshot = combine_snapshot_exports(chain_id, exports)?;
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
        self.snapshot_sessions
            .snapshot_payload(session_id, index)
            .await
    }

    pub async fn status_snapshot(&self) -> BroadcasterStatusSnapshot {
        self.cache
            .status_snapshot(
                self.snapshot_max_payload_bytes,
                self.upstream.snapshot().await,
                self.snapshot_sessions.snapshot().await,
            )
            .await
    }

    async fn publish_to_redis(&self, payload: BroadcasterPayload) -> Result<()> {
        self.redis_publisher
            .publish_accepted_payload(payload)
            .await
            .inspect_err(|error| {
                warn!(
                    event = "redis_publication_failed",
                    error = %error,
                    "Redis broadcaster publication failed"
                );
            })
            .context("failed to publish accepted broadcaster delta to Redis")
    }

    #[cfg(test)]
    pub(crate) async fn lock_lifecycle_gate_for_test(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.lifecycle_gate.clone().lock_owned().await
    }
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
        tycho_client::feed::{BlockHeader, SynchronizerState},
        tycho_common::{
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
        BroadcasterSnapshotCache, BroadcasterSnapshotExport, BroadcasterUpstreamState,
    };
    use simulator_core::broadcaster::{
        BroadcasterBackend, BroadcasterMessageKind, BroadcasterPayload,
        BroadcasterRedisStreamEntry, BroadcasterSnapshotEnd, BroadcasterSnapshotStart,
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
        assert_eq!(session.redis_replay_boundary.exclusive_message_seq, 1);
        assert_eq!(publisher_boundary.exclusive_message_seq, 2);
        Ok(())
    }

    #[tokio::test]
    async fn shared_snapshot_session_boundary_is_registered_before_queued_rfq_update() -> Result<()>
    {
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new_with_initial_generation(
            publisher_config(),
            Arc::new(writer),
            1,
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
        assert_eq!(session.redis_replay_boundary.exclusive_message_seq, 2);
        assert_eq!(publisher_boundary.exclusive_message_seq, 3);
        Ok(())
    }

    #[tokio::test]
    async fn combined_snapshot_session_rejects_mismatched_lifecycle_gate() -> Result<()> {
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new_with_initial_generation(
            publisher_config(),
            Arc::new(writer),
            1,
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
        let publisher = Arc::new(BroadcasterRedisPublisher::new_with_initial_generation(
            publisher_config(),
            Arc::new(writer.clone()),
            1,
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
        let publisher = Arc::new(BroadcasterRedisPublisher::new_with_initial_generation(
            publisher_config(),
            Arc::new(writer.clone()),
            1,
        ));
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
        let publisher = Arc::new(BroadcasterRedisPublisher::new_with_initial_generation(
            publisher_config(),
            Arc::new(writer.clone()),
            1,
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
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new_with_initial_generation(
            publisher_config(),
            Arc::new(writer),
            1,
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

        BroadcasterServiceState::handle_shared_generation_reset(
            &[raw_service.clone(), rfq_service.clone()],
            "stale",
            Some("boom".to_string()),
        )
        .await;

        assert_eq!(
            raw_service.status_snapshot().await.snapshot.stream_id,
            "chain-1-stream-2"
        );
        assert_eq!(
            rfq_service.status_snapshot().await.snapshot.stream_id,
            "chain-1-stream-2"
        );
        assert_eq!(
            publisher.status_snapshot().await.stream_id,
            "chain-1-stream-2"
        );
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_session_response_includes_redis_replay_boundary() -> Result<()> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let writer = ServiceFakeRedisWriter::default();
        let publisher = Arc::new(BroadcasterRedisPublisher::new_with_initial_generation(
            publisher_config(),
            Arc::new(writer.clone()),
            1,
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
        assert_eq!(session.redis_replay_boundary.exclusive_entry_id(), "1-1");
        assert_eq!(session.redis_replay_boundary.exclusive_message_seq, 1);
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
        let publisher = Arc::new(BroadcasterRedisPublisher::new_with_initial_generation(
            publisher_config(),
            Arc::new(writer),
            1,
        ));
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

    #[derive(Debug, Clone, Default)]
    struct ServiceFakeRedisWriter {
        inner: Arc<Mutex<ServiceFakeRedisWriterState>>,
    }

    #[derive(Debug, Default)]
    struct ServiceFakeRedisWriterState {
        appends: Vec<BroadcasterRedisStreamEntry>,
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
        fn append<'a>(
            &'a self,
            _stream_key: &'a str,
            _maxlen: Option<u64>,
            entry: &'a BroadcasterRedisStreamEntry,
        ) -> futures::future::BoxFuture<'a, Result<String>> {
            Box::pin(async move {
                let mut guard = self.inner.lock().await;
                if guard.fail_next_appends > 0 {
                    guard.fail_next_appends -= 1;
                    return Err(anyhow!("planned append failure"));
                }
                let entry_id = format!("1000-{}", guard.appends.len());
                guard.appends.push(entry.clone());
                Ok(entry_id)
            })
        }
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
        simulator_core::broadcaster::BroadcasterRedisReplayBoundary::new(
            "dsolver:broadcaster:test:events",
            "stream-1",
            "snapshot-1",
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
        }
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
