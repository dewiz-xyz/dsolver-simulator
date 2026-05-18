use std::time::Duration;

use anyhow::Result;
use tokio::sync::Mutex;
use tycho_simulation::{
    protocol::models::Update as TychoUpdate,
    tycho_client::feed::{BlockHeader, FeedMessage},
};

use crate::broadcaster::state::{
    BroadcasterLiveState, BroadcasterReadiness, BroadcasterSnapshotCache,
    BroadcasterStatusSnapshot, BroadcasterUpstreamState,
};
use crate::services::broadcaster_sessions::{
    BroadcasterAttachedSession, BroadcasterSubscriberRegistry, SessionCloseReason,
    SnapshotSessionError,
};
use simulator_core::broadcaster::{
    BroadcasterEnvelope, BroadcasterPayload, BroadcasterSnapshotSessionResponse,
};

#[derive(Debug, Clone)]
pub struct BroadcasterServiceState {
    snapshot_max_payload_bytes: usize,
    cache: BroadcasterSnapshotCache,
    upstream: BroadcasterUpstreamState,
    subscribers: BroadcasterSubscriberRegistry,
    // This gate keeps snapshot export plus subscriber registration atomic with respect to
    // updates, heartbeats, and generation resets.
    lifecycle_gate: std::sync::Arc<Mutex<()>>,
}

impl BroadcasterServiceState {
    pub fn new(
        snapshot_max_payload_bytes: usize,
        subscriber_buffer_capacity: usize,
        cache: BroadcasterSnapshotCache,
        upstream: BroadcasterUpstreamState,
    ) -> Self {
        Self {
            snapshot_max_payload_bytes,
            cache,
            upstream,
            subscribers: BroadcasterSubscriberRegistry::new(subscriber_buffer_capacity),
            lifecycle_gate: std::sync::Arc::new(Mutex::new(())),
        }
    }

    pub async fn mark_upstream_connected(&self) {
        self.upstream.mark_connected().await;
    }

    pub async fn mark_build_failed(&self, error: impl Into<String>) {
        self.upstream.mark_build_failed(error).await;
    }

    pub async fn handle_generation_reset(
        &self,
        reason: impl Into<String>,
        last_error: Option<String>,
    ) -> BroadcasterLiveState {
        let _gate = self.lifecycle_gate.lock().await;
        self.upstream.mark_disconnected(reason, last_error).await;
        self.subscribers
            .disconnect_all(SessionCloseReason::GenerationReset)
            .await;
        self.cache.reset_generation().await
    }

    pub async fn apply_update(&self, update: &TychoUpdate) -> Result<()> {
        let _gate = self.lifecycle_gate.lock().await;
        let message = self.cache.apply_update(update).await?;
        self.upstream.record_update().await;
        self.subscribers
            .broadcast(BroadcasterPayload::Update(message))
            .await;
        Ok(())
    }

    pub async fn apply_feed_message(&self, feed: &FeedMessage<BlockHeader>) -> Result<()> {
        let _gate = self.lifecycle_gate.lock().await;
        let message = self.cache.apply_feed_message(feed).await?;
        self.upstream.record_update().await;
        self.subscribers
            .broadcast(BroadcasterPayload::Update(message))
            .await;
        Ok(())
    }

    pub async fn broadcast_heartbeat(&self) -> Result<()> {
        let _gate = self.lifecycle_gate.lock().await;
        if let Some(heartbeat) = self.cache.heartbeat().await? {
            self.subscribers.broadcast(heartbeat).await;
        }
        self.subscribers.cleanup_expired_snapshot_sessions().await;
        Ok(())
    }

    pub async fn create_snapshot_session(
        &self,
        ttl: Duration,
    ) -> Result<Option<BroadcasterSnapshotSessionResponse>> {
        let _gate = self.lifecycle_gate.lock().await;
        let status = self.status_snapshot().await;
        if status.readiness != BroadcasterReadiness::Ready {
            return Ok(None);
        }

        let snapshot = self
            .cache
            .export_snapshot(self.snapshot_max_payload_bytes)
            .await?;
        self.subscribers
            .create_snapshot_session(snapshot, status.chain_id, ttl)
            .await
            .map(Some)
    }

    pub async fn snapshot_session_payload(
        &self,
        session_id: u64,
        index: u32,
    ) -> Result<BroadcasterEnvelope, SnapshotSessionError> {
        self.subscribers.snapshot_payload(session_id, index).await
    }

    pub async fn attach_snapshot_session(
        &self,
        session_id: u64,
    ) -> Result<BroadcasterAttachedSession, SnapshotSessionError> {
        self.subscribers.attach_snapshot_session(session_id).await
    }

    pub async fn remove_subscriber(&self, session_id: u64) {
        self.subscribers.remove(session_id).await;
    }

    pub async fn shutdown(&self) {
        self.subscribers
            .disconnect_all(SessionCloseReason::Shutdown)
            .await;
    }

    pub async fn status_snapshot(&self) -> BroadcasterStatusSnapshot {
        self.cache
            .status_snapshot(
                self.snapshot_max_payload_bytes,
                self.upstream.snapshot().await,
                self.subscribers.snapshot().await,
            )
            .await
    }

    #[cfg(test)]
    pub(crate) async fn lock_lifecycle_gate_for_test(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.lifecycle_gate.clone().lock_owned().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::{anyhow, Result};
    use num_bigint::BigUint;
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

    use super::BroadcasterServiceState;
    use crate::broadcaster::state::{BroadcasterSnapshotCache, BroadcasterUpstreamState};
    use simulator_core::broadcaster::{BroadcasterBackend, BroadcasterPayload};

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

    #[tokio::test]
    async fn subscribe_registers_before_queued_live_update_is_broadcast() -> Result<()> {
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
            .ok_or_else(|| anyhow!("expected ready broadcaster subscriber"))?;

        update_task
            .await
            .map_err(|error| anyhow!("update task failed: {error}"))??;

        let mut registration = service
            .attach_snapshot_session(session.session_id)
            .await
            .map_err(|error| anyhow!("failed to attach snapshot session: {error:?}"))?;

        let live_payload = registration
            .receiver
            .recv()
            .await
            .ok_or_else(|| anyhow!("expected queued live update"))?;
        assert!(matches!(live_payload, BroadcasterPayload::Update(_)));
        Ok(())
    }

    #[tokio::test]
    async fn attached_session_is_disconnected_by_generation_reset() -> Result<()> {
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
            .ok_or_else(|| anyhow!("expected ready broadcaster subscriber"))?;

        let registration = service
            .attach_snapshot_session(session.session_id)
            .await
            .map_err(|error| anyhow!("failed to attach snapshot session: {error:?}"))?;
        let reset_task = tokio::spawn({
            let service = service.clone();
            async move {
                service
                    .handle_generation_reset("stale", Some("boom".to_string()))
                    .await
            }
        });
        let reason = registration
            .close_receiver
            .await
            .map_err(|_| anyhow!("expected reset close signal"))?;

        let live_state = reset_task
            .await
            .map_err(|error| anyhow!("reset task failed: {error}"))?;
        assert_eq!(
            reason,
            crate::services::broadcaster_sessions::SessionCloseReason::GenerationReset
        );
        assert_eq!(live_state.stream_id, "chain-1-stream-2");
        Ok(())
    }

    async fn ready_service() -> Result<BroadcasterServiceState> {
        let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Native]);
        let upstream = BroadcasterUpstreamState::default();
        let service = BroadcasterServiceState::new(8_388_608, 8, cache, upstream);
        service.mark_upstream_connected().await;
        service
            .apply_update(&native_only_update(10, "native-1"))
            .await?;
        Ok(service)
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
