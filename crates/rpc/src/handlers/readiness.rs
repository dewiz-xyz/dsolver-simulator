use std::collections::BTreeMap;

use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;
use simulator_core::broadcaster::BroadcasterRedisReplayBoundary;

use crate::models::state::{
    AppState, SimulatorBackendKind, SimulatorBackendStatusSnapshot,
    SimulatorBackendSubscriptionSnapshot, SimulatorReadinessReason, SimulatorServiceStatus,
    SimulatorStatusSnapshot,
};

#[derive(Serialize)]
pub struct StatusPayload {
    status: &'static str,
    block: u64,
    pools: usize,
    chain_id: u64,
    backends: BTreeMap<&'static str, BackendStatusPayload>,
}

impl From<SimulatorStatusSnapshot> for StatusPayload {
    fn from(snapshot: SimulatorStatusSnapshot) -> Self {
        let native_backend = snapshot
            .backends
            .iter()
            .find(|backend| backend.kind == SimulatorBackendKind::Native);

        Self {
            status: snapshot.status.label(),
            block: native_backend
                .and_then(|backend| backend.block_number)
                .unwrap_or(0),
            pools: native_backend.map_or(0, |backend| backend.pool_count),
            chain_id: snapshot.chain_id,
            backends: snapshot
                .backends
                .into_iter()
                .map(|backend| (backend.kind.label(), backend.into()))
                .collect(),
        }
    }
}

#[derive(Serialize)]
pub struct BackendStatusPayload {
    enabled: bool,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    block_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    update_timestamp: Option<u64>,
    pool_count: usize,
    restart_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rebuild_duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_update_age_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subscription: Option<BackendSubscriptionPayload>,
}

impl From<SimulatorBackendStatusSnapshot> for BackendStatusPayload {
    fn from(snapshot: SimulatorBackendStatusSnapshot) -> Self {
        Self {
            enabled: snapshot.enabled,
            status: snapshot.readiness.label(),
            reason: snapshot.reason.map(SimulatorReadinessReason::label),
            block_number: snapshot.block_number,
            update_timestamp: snapshot.update_timestamp,
            pool_count: snapshot.pool_count,
            restart_count: snapshot.restart_count,
            last_error: snapshot.last_error,
            rebuild_duration_ms: snapshot.rebuild_duration_ms,
            last_update_age_ms: snapshot.last_update_age_ms,
            subscription: snapshot.subscription.map(Into::into),
        }
    }
}

#[derive(Serialize)]
pub struct BackendSubscriptionPayload {
    connected: bool,
    bootstrap_complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    redis_replay_boundary: Option<BroadcasterRedisReplayBoundary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    redis_replay_checkpoint: Option<String>,
    redis_replay_caught_up: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    redis_gap_reason: Option<String>,
    redis_transport_status: &'static str,
    redis_transport_retry_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    redis_transport_last_error: Option<String>,
    restart_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
}

impl From<SimulatorBackendSubscriptionSnapshot> for BackendSubscriptionPayload {
    fn from(snapshot: SimulatorBackendSubscriptionSnapshot) -> Self {
        Self {
            connected: snapshot.connected,
            bootstrap_complete: snapshot.bootstrap_complete,
            stream_id: snapshot.stream_id,
            snapshot_id: snapshot.snapshot_id,
            redis_replay_boundary: snapshot.redis_replay_boundary,
            redis_replay_checkpoint: snapshot.redis_replay_checkpoint,
            redis_replay_caught_up: snapshot.redis_replay_caught_up,
            redis_gap_reason: snapshot.redis_gap_reason,
            redis_transport_status: snapshot.redis_transport_status.label(),
            redis_transport_retry_count: snapshot.redis_transport_retry_count,
            redis_transport_last_error: snapshot.redis_transport_last_error,
            restart_count: snapshot.restart_count,
            last_error: snapshot.last_error,
        }
    }
}

pub async fn status(State(state): State<AppState>) -> (StatusCode, Json<StatusPayload>) {
    let snapshot = state.status_snapshot().await;
    let status_code = if snapshot.status == SimulatorServiceStatus::Ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status_code, Json(snapshot.into()))
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use super::{status, StatusPayload};
    use crate::config::SlippageConfig;
    use crate::models::state::{
        AppState, BroadcasterSubscriptionStatus, ConfiguredBackends, RfqClientConfig, StateStore,
        VmStreamStatus,
    };
    use crate::models::stream_health::StreamHealth;
    use crate::models::tokens::TokenStore;
    use axum::{extract::State, http::StatusCode, Json};
    use chrono::NaiveDateTime;
    use num_bigint::BigUint;
    use num_traits::Zero;
    use simulator_core::broadcaster::BroadcasterRedisReplayBoundary;
    use tycho_simulation::protocol::models::{ProtocolComponent, Update};
    use tycho_simulation::tycho_common::dto::ProtocolStateDelta;
    use tycho_simulation::tycho_common::models::{token::Token, Chain};
    use tycho_simulation::tycho_common::simulation::errors::{SimulationError, TransitionError};
    use tycho_simulation::tycho_common::simulation::protocol_sim::{
        Balances, GetAmountOutResult, ProtocolSim,
    };
    use tycho_simulation::tycho_common::Bytes;

    #[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
    struct ReadyStateSim;

    #[typetag::serde]
    impl ProtocolSim for ReadyStateSim {
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
                BigUint::zero(),
                self.clone_box(),
            ))
        }

        fn get_limits(
            &self,
            _sell_token: Bytes,
            _buy_token: Bytes,
        ) -> Result<(BigUint, BigUint), SimulationError> {
            Ok((BigUint::zero(), BigUint::zero()))
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

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn eq(&self, other: &dyn ProtocolSim) -> bool {
            other.as_any().is::<ReadyStateSim>()
        }
    }

    fn address(seed: u8) -> Bytes {
        Bytes::from([seed; 20])
    }

    fn token(seed: u8, symbol: &str) -> Token {
        Token::new(&address(seed), symbol, 18, 0, &[], Chain::Ethereum, 100)
    }

    async fn seed_native_ready_store(state: &AppState) {
        let component = ProtocolComponent::new(
            address(3),
            "uniswap_v2".to_string(),
            "uniswap_v2".to_string(),
            Chain::Ethereum,
            vec![token(1, "TKNA"), token(2, "TKNB")],
            Vec::new(),
            HashMap::new(),
            Bytes::default(),
            NaiveDateTime::default(),
        );
        let states = HashMap::from([(
            "pool-native".to_string(),
            Box::new(ReadyStateSim) as Box<dyn ProtocolSim>,
        )]);
        let new_pairs = HashMap::from([("pool-native".to_string(), component)]);
        state
            .native_state_store
            .apply_update(Update::new(1, states, new_pairs))
            .await;
    }

    fn test_state(enable_vm_pools: bool, enable_rfq_pools: bool) -> AppState {
        let token_store = Arc::new(TokenStore::new(
            HashMap::new(),
            "http://localhost".to_string(),
            "test".to_string(),
            Chain::Ethereum,
            Duration::from_millis(10),
        ));
        AppState {
            chain: Chain::Ethereum,
            rfq_client_config: Arc::new(RfqClientConfig::default()),
            native_token_protocol_allowlist: Arc::new(vec!["rocketpool".to_string()]),
            tokens: Arc::clone(&token_store),
            native_broadcaster_subscription: BroadcasterSubscriptionStatus::ready_for_test(),
            vm_broadcaster_subscription: BroadcasterSubscriptionStatus::ready_for_test(),
            rfq_broadcaster_subscription: BroadcasterSubscriptionStatus::ready_for_test(),
            native_state_store: Arc::new(StateStore::new(Arc::clone(&token_store))),
            vm_state_store: Arc::new(StateStore::new(token_store.clone())),
            rfq_state_store: Arc::new(StateStore::new(token_store)),
            native_stream_health: Arc::new(StreamHealth::new()),
            vm_stream_health: Arc::new(StreamHealth::new()),
            rfq_stream_health: Arc::new(StreamHealth::new()),
            vm_stream: Arc::new(tokio::sync::RwLock::new(VmStreamStatus::default())),
            configured_backends: ConfiguredBackends {
                vm: enable_vm_pools,
                rfq: enable_rfq_pools,
            },
            enable_vm_pools,
            enable_rfq_pools,
            readiness_stale: Duration::from_secs(120),
            request_timeout: Duration::from_millis(1000),
            vm_simulation_rebuild_gate: Arc::new(tokio::sync::RwLock::new(())),
            rfq_simulation_rebuild_gate: Arc::new(tokio::sync::RwLock::new(())),
            slippage: SlippageConfig::default(),
            erc4626_deposits_enabled: false,
            erc4626_pair_policies: Arc::new(Vec::new()),
            reset_allowance_tokens: Arc::new(HashMap::new()),
        }
    }

    #[tokio::test]
    async fn status_exposes_legacy_native_block_and_pool_fields() {
        let state = test_state(false, false);
        seed_native_ready_store(&state).await;
        state.native_stream_health.record_update(1).await;

        let (status_code, Json(payload)): (_, Json<StatusPayload>) = status(State(state)).await;

        assert_eq!(status_code, StatusCode::OK);
        assert_eq!(payload.status, "ready");
        assert_eq!(payload.block, 1);
        assert_eq!(payload.pools, 1);
        assert_eq!(Some(payload.block), payload.backends["native"].block_number);
        assert_eq!(payload.pools, payload.backends["native"].pool_count);
    }

    #[tokio::test]
    async fn status_returns_service_unavailable_for_stale_native_state() {
        let mut state = test_state(false, false);
        seed_native_ready_store(&state).await;
        assert!(state.native_state_store.is_ready());
        state.native_stream_health.record_update(1).await;
        state.readiness_stale = Duration::ZERO;

        let (status_code, Json(payload)): (_, Json<StatusPayload>) = status(State(state)).await;

        assert_eq!(status_code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(payload.status, "stale");
        assert_eq!(Some(payload.block), payload.backends["native"].block_number);
        assert_eq!(payload.pools, payload.backends["native"].pool_count);
        assert_eq!(payload.backends["native"].status, "stale");
        assert_eq!(payload.backends["native"].reason, Some("stale"));
    }

    #[tokio::test]
    async fn status_stays_unavailable_when_vm_is_ready_but_native_is_not() {
        let state = test_state(true, true);
        let vm_component = ProtocolComponent::new(
            address(4),
            "vm:curve".to_string(),
            "curve_pool".to_string(),
            Chain::Ethereum,
            vec![token(5, "TKNA"), token(6, "TKNB")],
            Vec::new(),
            HashMap::new(),
            Bytes::default(),
            NaiveDateTime::default(),
        );
        state
            .vm_state_store
            .apply_update(Update::new(
                1,
                HashMap::from([(
                    "pool-vm".to_string(),
                    Box::new(ReadyStateSim) as Box<dyn ProtocolSim>,
                )]),
                HashMap::from([("pool-vm".to_string(), vm_component)]),
            ))
            .await;
        state.vm_stream_health.record_update(1).await;
        {
            let mut vm_status = state.vm_stream.write().await;
            vm_status.rebuilding = false;
        }

        let (status_code, Json(payload)): (_, Json<StatusPayload>) = status(State(state)).await;

        assert_eq!(status_code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(payload.status, "warming_up");
        assert_eq!(payload.backends["native"].status, "warming_up");
        assert_eq!(payload.backends["native"].reason, Some("state_warming_up"));
        assert_eq!(payload.backends["vm"].status, "ready");
        assert!(payload.backends["rfq"].enabled);
        assert_eq!(payload.backends["rfq"].status, "warming_up");
        assert_eq!(payload.backends["rfq"].reason, Some("state_warming_up"));
        assert!(payload.backends["rfq"].subscription.is_some());
    }

    #[tokio::test]
    async fn status_reports_rfq_broadcaster_subscription_restart_and_error() {
        let state = test_state(false, true);
        state
            .rfq_broadcaster_subscription
            .mark_disconnected(Some("rfq broadcaster dropped".to_string()))
            .await;

        let (_status_code, Json(payload)): (_, Json<StatusPayload>) = status(State(state)).await;

        let rfq = &payload.backends["rfq"];
        let subscription = rfq
            .subscription
            .as_ref()
            .unwrap_or_else(|| unreachable!("RFQ status must include subscription"));

        assert!(rfq.enabled);
        assert_eq!(rfq.status, "warming_up");
        assert_eq!(rfq.reason, Some("broadcaster_disconnected"));
        assert_eq!(rfq.restart_count, 1);
        assert_eq!(rfq.last_error.as_deref(), Some("rfq broadcaster dropped"));
        assert!(!subscription.connected);
        assert!(!subscription.bootstrap_complete);
        assert_eq!(subscription.restart_count, 1);
        assert_eq!(
            subscription.last_error.as_deref(),
            Some("rfq broadcaster dropped")
        );
    }

    #[tokio::test]
    async fn status_returns_service_unavailable_for_enabled_backend_redis_gap() {
        let state = test_state(false, true);
        seed_native_ready_store(&state).await;
        state.native_stream_health.record_update(1).await;

        let rfq_component = ProtocolComponent::new(
            address(31),
            "rfq:hashflow".to_string(),
            "hashflow".to_string(),
            Chain::Ethereum,
            vec![token(32, "RFQA"), token(33, "RFQB")],
            Vec::new(),
            HashMap::new(),
            Bytes::default(),
            NaiveDateTime::default(),
        );
        state
            .rfq_state_store
            .apply_update(Update::new(
                1,
                HashMap::from([(
                    "pool-rfq".to_string(),
                    Box::new(ReadyStateSim) as Box<dyn ProtocolSim>,
                )]),
                HashMap::from([("pool-rfq".to_string(), rfq_component)]),
            ))
            .await;
        state.rfq_stream_health.record_update(1).await;
        state
            .rfq_broadcaster_subscription
            .mark_redis_gap("RFQ Redis replay gap")
            .await;

        let (status_code, Json(payload)): (_, Json<StatusPayload>) = status(State(state)).await;

        assert_eq!(status_code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(payload.status, "warming_up");
        assert_eq!(payload.backends["native"].status, "ready");
        assert_eq!(payload.backends["rfq"].status, "warming_up");
        assert_eq!(payload.backends["rfq"].reason, Some("redis_replay_gap"));
        assert_eq!(
            payload.backends["rfq"]
                .subscription
                .as_ref()
                .and_then(|subscription| subscription.redis_gap_reason.as_deref()),
            Some("RFQ Redis replay gap")
        );
    }

    #[tokio::test]
    async fn status_reports_redis_replay_boundary_and_checkpoint() {
        let state = test_state(false, false);
        let boundary = BroadcasterRedisReplayBoundary::new(
            "dsolver:broadcaster:test:1:events",
            "stream-test",
            "snapshot-test",
            2,
            12,
        )
        .unwrap_or_else(|err| unreachable!("valid replay boundary: {err}"));
        state
            .native_broadcaster_subscription
            .mark_bootstrap_complete_with_redis_boundary(boundary.clone())
            .await;
        state
            .native_broadcaster_subscription
            .mark_redis_catch_up_checkpoint("2-14")
            .await;

        let (_status_code, Json(payload)): (_, Json<StatusPayload>) = status(State(state)).await;
        let subscription = payload.backends["native"]
            .subscription
            .as_ref()
            .unwrap_or_else(|| unreachable!("native status must include subscription"));

        assert_eq!(subscription.redis_replay_boundary, Some(boundary));
        assert_eq!(
            subscription.redis_replay_checkpoint.as_deref(),
            Some("2-14")
        );
        assert!(subscription.redis_replay_caught_up);
        assert!(subscription.redis_gap_reason.is_none());
        assert_eq!(subscription.redis_transport_status, "connected");
        assert_eq!(subscription.redis_transport_retry_count, 0);
        assert!(subscription.redis_transport_last_error.is_none());
    }

    #[tokio::test]
    async fn status_reports_rfq_update_timestamp_without_block_number() -> serde_json::Result<()> {
        let state = test_state(false, true);
        seed_native_ready_store(&state).await;
        state.native_stream_health.record_update(1).await;

        let rfq_component = ProtocolComponent::new(
            address(31),
            "rfq:hashflow".to_string(),
            "hashflow".to_string(),
            Chain::Ethereum,
            vec![token(32, "RFQA"), token(33, "RFQB")],
            Vec::new(),
            HashMap::new(),
            Bytes::default(),
            NaiveDateTime::default(),
        );
        state
            .rfq_state_store
            .apply_update(Update::new(
                1_710_000_000,
                HashMap::from([(
                    "pool-rfq".to_string(),
                    Box::new(ReadyStateSim) as Box<dyn ProtocolSim>,
                )]),
                HashMap::from([("pool-rfq".to_string(), rfq_component)]),
            ))
            .await;
        state.rfq_stream_health.record_update(1_710_000_000).await;

        let (_status_code, Json(payload)): (_, Json<StatusPayload>) = status(State(state)).await;
        let value = serde_json::to_value(&payload.backends["rfq"])?;

        assert_eq!(value["update_timestamp"], 1_710_000_000);
        assert!(value.get("block_number").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn status_reports_configured_disabled_backends_and_omits_unconfigured() {
        let mut state = test_state(false, false);
        state.configured_backends.vm = true;

        let (_status_code, Json(payload)): (_, Json<StatusPayload>) = status(State(state)).await;

        assert!(payload.backends.contains_key("native"));
        assert!(!payload.backends["vm"].enabled);
        assert_eq!(payload.backends["vm"].status, "disabled");
        assert_eq!(payload.backends["vm"].reason, Some("disabled_by_config"));
        assert!(!payload.backends.contains_key("rfq"));
    }
}
