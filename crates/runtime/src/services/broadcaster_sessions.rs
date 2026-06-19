use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::broadcaster::state::{BroadcasterSnapshotExport, BroadcasterSnapshotSessionsSnapshot};
use simulator_core::broadcaster::{
    BroadcasterEnvelope, BroadcasterPayload, BroadcasterRedisReplayBoundary,
    BroadcasterSnapshotSessionResponse,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionCloseReason {
    Expired,
    GenerationReset,
    Shutdown,
}

impl SessionCloseReason {
    const fn label(self) -> &'static str {
        match self {
            Self::Expired => "expired",
            Self::GenerationReset => "generation_reset",
            Self::Shutdown => "shutdown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotSessionError {
    NotFound,
    Expired,
    PayloadOutOfRange,
}

#[derive(Debug, Clone)]
pub struct BroadcasterSnapshotSessionRegistry {
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
    pub fn new() -> Self {
        Self {
            next_session_id: Arc::new(AtomicU64::new(1)),
            last_error: Arc::new(Mutex::new(None)),
            pending_sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn create_snapshot_session(
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

    pub async fn snapshot_payload(
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

    pub async fn cleanup_expired_snapshot_sessions(&self) {
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

    pub async fn disconnect_all(&self, reason: SessionCloseReason) {
        self.pending_sessions.lock().await.clear();

        self.record_last_error(format!("all snapshot sessions closed: {}", reason.label()))
            .await;
    }

    pub async fn snapshot(&self) -> BroadcasterSnapshotSessionsSnapshot {
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

impl Default for BroadcasterSnapshotSessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::{anyhow, Result};

    use super::{BroadcasterSnapshotSessionRegistry, SessionCloseReason, SnapshotSessionError};
    use crate::broadcaster::state::BroadcasterSnapshotExport;
    use simulator_core::broadcaster::{
        BroadcasterPayload, BroadcasterRedisReplayBoundary, BroadcasterSnapshotEnd,
        BroadcasterSnapshotStart,
    };

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

    fn replay_boundary() -> BroadcasterRedisReplayBoundary {
        BroadcasterRedisReplayBoundary::new(
            "dsolver:broadcaster:test:events",
            "stream-1",
            "snapshot-1",
            1,
            0,
        )
        .unwrap_or_else(|_| unreachable!("valid replay boundary"))
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
            .disconnect_all(SessionCloseReason::GenerationReset)
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
}
