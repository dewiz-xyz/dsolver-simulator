use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::Instant;

use crate::broadcaster::state::{BroadcasterSnapshotExport, BroadcasterSubscriberSnapshot};
use simulator_core::broadcaster::{
    BroadcasterEnvelope, BroadcasterPayload, BroadcasterSnapshotSessionResponse,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionCloseReason {
    Lagged,
    Expired,
    GenerationReset,
    Shutdown,
}

impl SessionCloseReason {
    const fn label(self) -> &'static str {
        match self {
            Self::Lagged => "lagged",
            Self::Expired => "expired",
            Self::GenerationReset => "generation_reset",
            Self::Shutdown => "shutdown",
        }
    }
}

pub struct BroadcasterAttachedSession {
    pub session_id: u64,
    pub stream_id: String,
    pub next_message_seq: u64,
    pub receiver: mpsc::Receiver<BroadcasterPayload>,
    pub close_receiver: oneshot::Receiver<SessionCloseReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotSessionError {
    NotFound,
    Expired,
    AlreadyAttached,
    PayloadOutOfRange,
}

#[derive(Debug, Clone)]
pub struct BroadcasterSubscriberRegistry {
    buffer_capacity: usize,
    next_session_id: Arc<AtomicU64>,
    lag_disconnects: Arc<AtomicU64>,
    last_error: Arc<Mutex<Option<String>>>,
    inner: Arc<Mutex<HashMap<u64, SubscriberHandle>>>,
    pending_sessions: Arc<Mutex<HashMap<u64, PendingSnapshotSession>>>,
}

#[derive(Debug)]
struct SubscriberHandle {
    sender: mpsc::Sender<BroadcasterPayload>,
    close_tx: Option<oneshot::Sender<SessionCloseReason>>,
}

#[derive(Debug)]
struct PendingSnapshotSession {
    stream_id: String,
    snapshot_payloads: Vec<BroadcasterEnvelope>,
    receiver: Option<mpsc::Receiver<BroadcasterPayload>>,
    close_receiver: Option<oneshot::Receiver<SessionCloseReason>>,
    expires_at: Instant,
    attached: bool,
}

impl PendingSnapshotSession {
    fn is_expired(&self, now: Instant) -> bool {
        !self.attached && now >= self.expires_at
    }

    fn next_message_seq(&self) -> u64 {
        self.snapshot_payloads
            .len()
            .saturating_add(1)
            .try_into()
            .unwrap_or(u64::MAX)
    }
}

impl BroadcasterSubscriberRegistry {
    pub fn new(buffer_capacity: usize) -> Self {
        Self {
            buffer_capacity,
            next_session_id: Arc::new(AtomicU64::new(1)),
            lag_disconnects: Arc::new(AtomicU64::new(0)),
            last_error: Arc::new(Mutex::new(None)),
            inner: Arc::new(Mutex::new(HashMap::new())),
            pending_sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn create_snapshot_session(
        &self,
        snapshot: BroadcasterSnapshotExport,
        chain_id: u64,
        ttl: Duration,
    ) -> Result<BroadcasterSnapshotSessionResponse> {
        let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(self.buffer_capacity);
        let (close_tx, close_receiver) = oneshot::channel();
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

        self.inner.lock().await.insert(
            session_id,
            SubscriberHandle {
                sender,
                close_tx: Some(close_tx),
            },
        );
        self.pending_sessions.lock().await.insert(
            session_id,
            PendingSnapshotSession {
                stream_id: stream_id.clone(),
                snapshot_payloads,
                receiver: Some(receiver),
                close_receiver: Some(close_receiver),
                expires_at,
                attached: false,
            },
        );

        Ok(BroadcasterSnapshotSessionResponse {
            chain_id,
            session_id,
            stream_id,
            snapshot_id,
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
        let expired = {
            let now = Instant::now();
            let mut guard = self.pending_sessions.lock().await;
            let Some(session) = guard.get(&session_id) else {
                return Err(SnapshotSessionError::NotFound);
            };
            if session.is_expired(now) {
                guard.remove(&session_id);
                true
            } else if session.attached {
                return Err(SnapshotSessionError::AlreadyAttached);
            } else {
                let Some(envelope) = session.snapshot_payloads.get(index as usize).cloned() else {
                    return Err(SnapshotSessionError::PayloadOutOfRange);
                };
                return Ok(envelope);
            }
        };

        if expired {
            self.disconnect_subscriber(session_id, SessionCloseReason::Expired)
                .await;
            return Err(SnapshotSessionError::Expired);
        }

        Err(SnapshotSessionError::NotFound)
    }

    pub async fn attach_snapshot_session(
        &self,
        session_id: u64,
    ) -> Result<BroadcasterAttachedSession, SnapshotSessionError> {
        let expired = {
            let now = Instant::now();
            let mut guard = self.pending_sessions.lock().await;
            let Some(session) = guard.get_mut(&session_id) else {
                return Err(SnapshotSessionError::NotFound);
            };
            if session.is_expired(now) {
                guard.remove(&session_id);
                true
            } else if session.attached {
                return Err(SnapshotSessionError::AlreadyAttached);
            } else {
                let Some(receiver) = session.receiver.take() else {
                    return Err(SnapshotSessionError::AlreadyAttached);
                };
                let Some(close_receiver) = session.close_receiver.take() else {
                    return Err(SnapshotSessionError::AlreadyAttached);
                };
                let next_message_seq = session.next_message_seq();
                drop(std::mem::take(&mut session.snapshot_payloads));
                session.attached = true;
                return Ok(BroadcasterAttachedSession {
                    session_id,
                    stream_id: session.stream_id.clone(),
                    next_message_seq,
                    receiver,
                    close_receiver,
                });
            }
        };

        if expired {
            self.disconnect_subscriber(session_id, SessionCloseReason::Expired)
                .await;
            return Err(SnapshotSessionError::Expired);
        }

        Err(SnapshotSessionError::NotFound)
    }

    pub async fn broadcast(&self, payload: BroadcasterPayload) {
        let mut lagged = Vec::new();
        let mut closed = Vec::new();
        let mut last_error = None;
        let mut guard = self.inner.lock().await;

        for (session_id, handle) in guard.iter_mut() {
            match handle.sender.try_send(payload.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.lag_disconnects.fetch_add(1, Ordering::Relaxed);
                    if let Some(close_tx) = handle.close_tx.take() {
                        let _ = close_tx.send(SessionCloseReason::Lagged);
                    }
                    last_error = Some(format!(
                        "subscriber {session_id} disconnected: {}",
                        SessionCloseReason::Lagged.label()
                    ));
                    lagged.push(*session_id);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    last_error = Some(format!(
                        "subscriber {session_id} disconnected: channel_closed"
                    ));
                    closed.push(*session_id);
                }
            }
        }

        let removed_sessions = lagged
            .iter()
            .chain(closed.iter())
            .copied()
            .collect::<Vec<_>>();
        for session_id in &removed_sessions {
            guard.remove(session_id);
        }
        drop(guard);

        if !removed_sessions.is_empty() {
            self.remove_pending_sessions(&removed_sessions).await;
        }

        if let Some(last_error) = last_error {
            self.record_last_error(last_error).await;
        }
    }

    pub async fn remove(&self, session_id: u64) {
        self.inner.lock().await.remove(&session_id);
        self.pending_sessions.lock().await.remove(&session_id);
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
            self.disconnect_subscriber(session_id, SessionCloseReason::Expired)
                .await;
        }
    }

    pub async fn disconnect_all(&self, reason: SessionCloseReason) {
        let mut guard = self.inner.lock().await;
        for handle in guard.values_mut() {
            if let Some(close_tx) = handle.close_tx.take() {
                let _ = close_tx.send(reason);
            }
        }
        guard.clear();
        drop(guard);
        self.pending_sessions.lock().await.clear();

        self.record_last_error(format!("all subscribers disconnected: {}", reason.label()))
            .await;
    }

    pub async fn snapshot(&self) -> BroadcasterSubscriberSnapshot {
        BroadcasterSubscriberSnapshot {
            active: self.inner.lock().await.len(),
            lag_disconnects: self.lag_disconnects.load(Ordering::Relaxed),
            last_error: self.last_error.lock().await.clone(),
        }
    }

    async fn record_last_error(&self, message: String) {
        *self.last_error.lock().await = Some(message);
    }

    async fn disconnect_subscriber(&self, session_id: u64, reason: SessionCloseReason) {
        let mut guard = self.inner.lock().await;
        if let Some(mut handle) = guard.remove(&session_id) {
            if let Some(close_tx) = handle.close_tx.take() {
                let _ = close_tx.send(reason);
            }
        }
        drop(guard);
        self.record_last_error(format!(
            "subscriber {session_id} disconnected: {}",
            reason.label()
        ))
        .await;
    }

    async fn remove_pending_sessions(&self, session_ids: &[u64]) {
        let mut guard = self.pending_sessions.lock().await;
        for session_id in session_ids {
            guard.remove(session_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::{anyhow, Result};

    use super::{BroadcasterSubscriberRegistry, SessionCloseReason, SnapshotSessionError};
    use crate::broadcaster::state::BroadcasterSnapshotExport;
    use simulator_core::broadcaster::{
        BroadcasterPayload, BroadcasterSnapshotEnd, BroadcasterSnapshotStart,
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

    #[tokio::test]
    async fn lagged_subscriber_is_disconnected_without_affecting_fast_peer() -> Result<()> {
        let registry = BroadcasterSubscriberRegistry::new(1);
        let lagged_session = registry
            .create_snapshot_session(snapshot_export(), 1, Duration::from_secs(300))
            .await?;
        let fast_session = registry
            .create_snapshot_session(snapshot_export(), 1, Duration::from_secs(300))
            .await?;
        let lagged = registry
            .attach_snapshot_session(lagged_session.session_id)
            .await
            .map_err(|error| anyhow!("lagged attach failed: {error:?}"))?;
        let mut fast = registry
            .attach_snapshot_session(fast_session.session_id)
            .await
            .map_err(|error| anyhow!("fast attach failed: {error:?}"))?;

        registry
            .broadcast(BroadcasterPayload::SnapshotEnd(
                BroadcasterSnapshotEnd::new("snapshot-1"),
            ))
            .await;
        assert!(fast.receiver.try_recv().is_ok());
        registry
            .broadcast(BroadcasterPayload::SnapshotEnd(
                BroadcasterSnapshotEnd::new("snapshot-1"),
            ))
            .await;

        let reason = lagged.close_receiver.await?;
        assert_eq!(reason, SessionCloseReason::Lagged);
        assert!(fast.receiver.try_recv().is_ok());
        let snapshot = registry.snapshot().await;
        assert_eq!(snapshot.active, 1);
        assert_eq!(snapshot.lag_disconnects, 1);
        assert_eq!(
            snapshot.last_error.as_deref(),
            Some("subscriber 1 disconnected: lagged")
        );
        Ok(())
    }

    #[tokio::test]
    async fn disconnect_all_clears_registry() -> Result<()> {
        let registry = BroadcasterSubscriberRegistry::new(2);
        let session = registry
            .create_snapshot_session(snapshot_export(), 1, Duration::from_secs(300))
            .await?;
        let session = registry
            .attach_snapshot_session(session.session_id)
            .await
            .map_err(|error| anyhow!("attach failed: {error:?}"))?;

        registry
            .disconnect_all(SessionCloseReason::GenerationReset)
            .await;

        let reason = session.close_receiver.await?;
        assert_eq!(reason, SessionCloseReason::GenerationReset);
        let snapshot = registry.snapshot().await;
        assert_eq!(snapshot.active, 0);
        assert_eq!(
            snapshot.last_error.as_deref(),
            Some("all subscribers disconnected: generation_reset")
        );
        Ok(())
    }

    #[tokio::test]
    async fn pending_session_serves_payloads_then_attaches_once() -> Result<()> {
        let registry = BroadcasterSubscriberRegistry::new(2);
        let session = registry
            .create_snapshot_session(snapshot_export(), 1, Duration::from_secs(300))
            .await?;

        let first = registry
            .snapshot_payload(session.session_id, 0)
            .await
            .map_err(|error| anyhow!("payload fetch failed: {error:?}"))?;
        assert_eq!(first.stream_id, "stream-1");
        assert_eq!(first.message_seq, 1);
        let attached = registry
            .attach_snapshot_session(session.session_id)
            .await
            .map_err(|error| anyhow!("attach failed: {error:?}"))?;
        assert_eq!(attached.next_message_seq, 3);

        let Err(error) = registry.snapshot_payload(session.session_id, 0).await else {
            unreachable!("attached session should not serve HTTP snapshot payloads");
        };
        assert_eq!(error, SnapshotSessionError::AlreadyAttached);
        let Err(error) = registry.attach_snapshot_session(session.session_id).await else {
            unreachable!("attached session should not attach twice");
        };
        assert_eq!(error, SnapshotSessionError::AlreadyAttached);
        Ok(())
    }

    #[tokio::test]
    async fn attached_session_drops_http_snapshot_payloads() -> Result<()> {
        let registry = BroadcasterSubscriberRegistry::new(2);
        let session = registry
            .create_snapshot_session(snapshot_export(), 1, Duration::from_secs(300))
            .await?;

        let attached = registry
            .attach_snapshot_session(session.session_id)
            .await
            .map_err(|error| anyhow!("attach failed: {error:?}"))?;
        assert_eq!(attached.next_message_seq, 3);

        let guard = registry.pending_sessions.lock().await;
        let pending = guard
            .get(&session.session_id)
            .ok_or_else(|| anyhow!("attached session should remain registered"))?;
        assert!(pending.attached);
        assert_eq!(pending.snapshot_payloads.len(), 0);
        assert_eq!(pending.snapshot_payloads.capacity(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn expired_pending_session_disconnects_subscriber() -> Result<()> {
        let registry = BroadcasterSubscriberRegistry::new(2);
        let session = registry
            .create_snapshot_session(snapshot_export(), 1, Duration::from_millis(1))
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
            Some("subscriber 1 disconnected: expired")
        );
        Ok(())
    }
}
