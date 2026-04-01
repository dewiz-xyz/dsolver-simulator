use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::NaiveDateTime;
use tokio::sync::{RwLock, Semaphore};
use tycho_simulation::protocol::models::ProtocolComponent;
use tycho_simulation::tycho_common::models::token::Token;
use tycho_simulation::tycho_common::models::Chain;
use tycho_simulation::tycho_common::Bytes;

use crate::models::messages::PoolRef;
use crate::models::state::{AppState, RfqStreamStatus, StateStore, VmStreamStatus};
use crate::models::stream_health::StreamHealth;
use crate::models::tokens::TokenStore;

#[expect(
    clippy::unwrap_used,
    reason = "Test fixtures use hard-coded valid addresses."
)]
pub(super) fn fixture_bytes(address: &str) -> Bytes {
    Bytes::from_str(address).unwrap()
}

pub(super) fn pool_ref(id: &str) -> PoolRef {
    PoolRef {
        protocol: "uniswap_v2".to_string(),
        component_id: id.to_string(),
        pool_address: None,
    }
}

pub(super) fn dummy_component() -> ProtocolComponent {
    component_with_tokens(
        "0x0000000000000000000000000000000000000009",
        vec![
            dummy_token("0x0000000000000000000000000000000000000001"),
            dummy_token("0x0000000000000000000000000000000000000002"),
        ],
    )
}

pub(super) fn component_with_protocol(
    id: &str,
    protocol_system: &str,
    protocol_type_name: &str,
    tokens: Vec<Token>,
) -> ProtocolComponent {
    ProtocolComponent::new(
        fixture_bytes(id),
        protocol_system.to_string(),
        protocol_type_name.to_string(),
        Chain::Ethereum,
        tokens,
        Vec::new(),
        HashMap::new(),
        Bytes::default(),
        NaiveDateTime::default(),
    )
}

pub(super) fn component_with_tokens(id: &str, tokens: Vec<Token>) -> ProtocolComponent {
    component_with_protocol(id, "uniswap_v2", "uniswap_v2", tokens)
}

pub(super) fn dummy_token(address: &str) -> Token {
    let bytes = fixture_bytes(address);
    Token::new(&bytes, "TKN", 18, 0, &[], Chain::Ethereum, 100)
}

pub(super) fn token_store_with_tokens(tokens: impl IntoIterator<Item = Token>) -> Arc<TokenStore> {
    let initial_tokens = tokens
        .into_iter()
        .map(|token| (token.address.clone(), token))
        .collect();
    Arc::new(TokenStore::new(
        initial_tokens,
        "http://localhost".to_string(),
        "test".to_string(),
        Chain::Ethereum,
        Duration::from_millis(10),
    ))
}

pub(super) fn test_state_stores(
    token_store: &Arc<TokenStore>,
) -> (Arc<StateStore>, Arc<StateStore>, Arc<StateStore>) {
    (
        Arc::new(StateStore::new(Arc::clone(token_store))),
        Arc::new(StateStore::new(Arc::clone(token_store))),
        Arc::new(StateStore::new(Arc::clone(token_store))),
    )
}

pub(super) struct TestAppStateConfig {
    pub(super) enable_vm_pools: bool,
    pub(super) enable_rfq_pools: bool,
    pub(super) erc4626_deposits_enabled: bool,
    pub(super) quote_timeout: Duration,
    pub(super) pool_timeout_native: Duration,
    pub(super) pool_timeout_vm: Duration,
    pub(super) pool_timeout_rfq: Duration,
    pub(super) request_timeout: Duration,
}

impl Default for TestAppStateConfig {
    fn default() -> Self {
        Self {
            enable_vm_pools: false,
            enable_rfq_pools: false,
            erc4626_deposits_enabled: false,
            quote_timeout: Duration::from_millis(10),
            pool_timeout_native: Duration::from_millis(10),
            pool_timeout_vm: Duration::from_millis(10),
            pool_timeout_rfq: Duration::from_millis(10),
            request_timeout: Duration::from_millis(10),
        }
    }
}

pub(super) fn test_app_state(
    token_store: Arc<TokenStore>,
    native_state_store: Arc<StateStore>,
    vm_state_store: Arc<StateStore>,
    rfq_state_store: Arc<StateStore>,
    config: TestAppStateConfig,
) -> AppState {
    AppState {
        chain: Chain::Ethereum,
        native_token_protocol_allowlist: Arc::new(vec!["rocketpool".to_string()]),
        tokens: token_store,
        native_state_store,
        vm_state_store,
        rfq_state_store,
        native_stream_health: Arc::new(StreamHealth::new()),
        vm_stream_health: Arc::new(StreamHealth::new()),
        rfq_stream_health: Arc::new(StreamHealth::new()),
        vm_stream: Arc::new(RwLock::new(VmStreamStatus::default())),
        rfq_stream: Arc::new(RwLock::new(RfqStreamStatus::default())),
        enable_vm_pools: config.enable_vm_pools,
        enable_rfq_pools: config.enable_rfq_pools,
        readiness_stale: Duration::from_secs(120),
        quote_timeout: config.quote_timeout,
        pool_timeout_native: config.pool_timeout_native,
        pool_timeout_vm: config.pool_timeout_vm,
        pool_timeout_rfq: config.pool_timeout_rfq,
        request_timeout: config.request_timeout,
        native_sim_semaphore: Arc::new(Semaphore::new(1)),
        vm_sim_semaphore: Arc::new(Semaphore::new(1)),
        rfq_sim_semaphore: Arc::new(Semaphore::new(1)),
        erc4626_deposits_enabled: config.erc4626_deposits_enabled,
        reset_allowance_tokens: Arc::new(HashMap::new()),
        native_sim_concurrency: 1,
        vm_sim_concurrency: 1,
        rfq_sim_concurrency: 1,
    }
}
