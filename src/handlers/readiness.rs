use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;

use crate::models::state::{AppState, NativeReadiness, VmReadiness};

#[derive(Serialize)]
pub struct StatusPayload {
    status: &'static str,
    chain_id: u64,
    block: u64,
    pools: usize,
    vm_enabled: bool,
    vm_status: &'static str,
    vm_block: u64,
    vm_pools: usize,
    vm_restarts: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    vm_last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    vm_rebuild_duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    vm_last_update_age_ms: Option<u64>,
}

pub async fn status(State(state): State<AppState>) -> (StatusCode, Json<StatusPayload>) {
    let block = state.current_block().await;
    let pools = state.total_pools().await;
    let native_readiness = state.native_readiness().await;

    let vm_enabled = state.enable_vm_pools;
    let vm_status_snapshot = state.vm_stream.read().await.clone();
    let vm_readiness = state.vm_readiness().await;
    let vm_status = vm_readiness.label();

    let vm_block = state.vm_block().await;
    let vm_pools = state.vm_pools().await;
    let vm_restarts = vm_status_snapshot.restart_count;
    let vm_last_error = vm_status_snapshot.last_error.clone();
    let vm_rebuild_duration_ms = if matches!(vm_readiness, VmReadiness::Rebuilding) {
        vm_status_snapshot
            .rebuild_started_at
            .map(|instant| instant.elapsed().as_millis() as u64)
    } else {
        None
    };
    let vm_last_update_age_ms = state.vm_update_age_ms().await;
    let status = native_readiness.label();

    let status_code = if matches!(native_readiness, NativeReadiness::Ready) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status_code,
        Json(StatusPayload {
            status,
            chain_id: state.chain.id(),
            block,
            pools,
            vm_enabled,
            vm_status,
            vm_block,
            vm_pools,
            vm_restarts,
            vm_last_error,
            vm_rebuild_duration_ms,
            vm_last_update_age_ms,
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use super::{status, StatusPayload};
    use crate::models::state::{AppState, RfqStreamStatus, StateStore, VmStreamStatus};
    use crate::models::stream_health::StreamHealth;
    use crate::models::tokens::TokenStore;
    use axum::{extract::State, http::StatusCode, Json};
    use chrono::NaiveDateTime;
    use num_bigint::BigUint;
    use num_traits::Zero;
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
            native_token_protocol_allowlist: Arc::new(vec!["rocketpool".to_string()]),
            tokens: Arc::clone(&token_store),
            native_state_store: Arc::new(StateStore::new(Arc::clone(&token_store))),
            vm_state_store: Arc::new(StateStore::new(token_store.clone())),
            rfq_state_store: Arc::new(StateStore::new(token_store)),
            native_stream_health: Arc::new(StreamHealth::new()),
            vm_stream_health: Arc::new(StreamHealth::new()),
            rfq_stream_health: Arc::new(StreamHealth::new()),
            vm_stream: Arc::new(tokio::sync::RwLock::new(VmStreamStatus::default())),
            rfq_stream: Arc::new(tokio::sync::RwLock::new(RfqStreamStatus::default())),
            enable_vm_pools,
            enable_rfq_pools,
            readiness_stale: Duration::from_secs(120),
            quote_timeout: Duration::from_millis(100),
            pool_timeout_native: Duration::from_millis(50),
            pool_timeout_vm: Duration::from_millis(50),
            pool_timeout_rfq: Duration::from_millis(50),
            request_timeout: Duration::from_millis(1000),
            native_sim_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
            vm_sim_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
            rfq_sim_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
            erc4626_deposits_enabled: false,
            reset_allowance_tokens: Arc::new(HashMap::new()),
            native_sim_concurrency: 1,
            vm_sim_concurrency: 1,
            rfq_sim_concurrency: 1,
        }
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
        assert_eq!(payload.status, "warming_up");
    }

    #[tokio::test]
    async fn status_reports_vm_rebuilding() {
        let state = test_state(true, true);
        {
            let mut vm_status = state.vm_stream.write().await;
            vm_status.rebuilding = true;
        }

        let (_, Json(payload)): (_, Json<StatusPayload>) = status(State(state)).await;

        assert_eq!(payload.vm_status, "rebuilding");
    }
}
