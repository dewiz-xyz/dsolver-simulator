use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::NaiveDateTime;
use tokio::sync::RwLock;
use tycho_simulation::protocol::models::ProtocolComponent;
use tycho_simulation::tycho_common::models::token::Token;
use tycho_simulation::tycho_common::models::Chain;
use tycho_simulation::tycho_common::Bytes;

use crate::config::SlippageConfig;
use crate::models::erc4626::Erc4626PairPolicy;
use crate::models::messages::PoolRef;
use crate::models::state::{
    AppState, BroadcasterSubscriptionStatus, ConfiguredBackends, RfqStreamStatus, StateStore,
    VmStreamStatus,
};
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
    token_store: Arc<TokenStore>,
) -> (Arc<StateStore>, Arc<StateStore>, Arc<StateStore>) {
    (
        Arc::new(StateStore::new(Arc::clone(&token_store))),
        Arc::new(StateStore::new(Arc::clone(&token_store))),
        Arc::new(StateStore::new(token_store)),
    )
}

fn erc4626_pair_policies() -> Vec<Erc4626PairPolicy> {
    vec![
        Erc4626PairPolicy {
            asset_symbol: "USDS".to_string(),
            share_symbol: "sUSDS".to_string(),
            asset: fixture_bytes("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
            share: fixture_bytes("0xa3931d71877c0e7a3148cb7eb4463524fec27fbd"),
            allow_asset_to_share: true,
            allow_share_to_asset: true,
        },
        Erc4626PairPolicy {
            asset_symbol: "USDC".to_string(),
            share_symbol: "sUSDC".to_string(),
            asset: fixture_bytes("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
            share: fixture_bytes("0xBc65ad17c5C0a2A4D159fa5a503f4992c7B545FE"),
            allow_asset_to_share: true,
            allow_share_to_asset: true,
        },
        Erc4626PairPolicy {
            asset_symbol: "PYUSD".to_string(),
            share_symbol: "spPYUSD".to_string(),
            asset: fixture_bytes("0x6c3ea9036406852006290770BEdFcAbA0e23A0e8"),
            share: fixture_bytes("0x80128DbB9f07b93DDE62A6daeadb69ED14a7D354"),
            allow_asset_to_share: true,
            allow_share_to_asset: true,
        },
    ]
}

pub(super) struct TestAppStateConfig {
    pub(super) enable_vm_pools: bool,
    pub(super) enable_rfq_pools: bool,
    pub(super) erc4626_deposits_enabled: bool,
    pub(super) request_timeout: Duration,
}

impl Default for TestAppStateConfig {
    fn default() -> Self {
        Self {
            enable_vm_pools: false,
            enable_rfq_pools: false,
            erc4626_deposits_enabled: false,
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
        native_broadcaster_subscription: BroadcasterSubscriptionStatus::ready_for_test(),
        vm_broadcaster_subscription: BroadcasterSubscriptionStatus::ready_for_test(),
        native_state_store,
        vm_state_store,
        rfq_state_store,
        native_stream_health: Arc::new(StreamHealth::new()),
        vm_stream_health: Arc::new(StreamHealth::new()),
        rfq_stream_health: Arc::new(StreamHealth::new()),
        vm_stream: Arc::new(RwLock::new(VmStreamStatus::default())),
        rfq_stream: Arc::new(RwLock::new(RfqStreamStatus::default())),
        configured_backends: ConfiguredBackends {
            vm: config.enable_vm_pools,
            rfq: config.enable_rfq_pools,
        },
        enable_vm_pools: config.enable_vm_pools,
        enable_rfq_pools: config.enable_rfq_pools,
        readiness_stale: Duration::from_secs(120),
        request_timeout: config.request_timeout,
        simulation_rebuild_gate: Arc::new(RwLock::new(())),
        slippage: SlippageConfig::default(),
        erc4626_deposits_enabled: config.erc4626_deposits_enabled,
        erc4626_pair_policies: Arc::new(erc4626_pair_policies()),
        reset_allowance_tokens: Arc::new(HashMap::new()),
    }
}
