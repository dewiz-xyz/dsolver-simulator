use std::any::Any;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, U256};
use dsolver_simulator::config::MemoryConfig;
use dsolver_simulator::handlers::stream::{
    process_stream, StreamKind, StreamRestartReason, StreamSupervisorConfig,
};
use dsolver_simulator::models::messages::AmountOutRequest;
use dsolver_simulator::models::state::{AppState, RfqStreamStatus, StateStore, VmStreamStatus};
use dsolver_simulator::models::stream_health::StreamHealth;
use dsolver_simulator::models::tokens::TokenStore;
use dsolver_simulator::services::quotes::get_amounts_out;
use futures::stream;
use jemalloc_ctl as jemalloc;
use jemallocator::Jemalloc;
use num_bigint::BigUint;
use tokio::sync::Semaphore;
use tokio::task::yield_now;
use tokio::time::advance;
use tycho_simulation::evm::engine_db::SHARED_TYCHO_DB;
use tycho_simulation::evm::tycho_models::{AccountUpdate, ChangeType};
use tycho_simulation::protocol::models::{ProtocolComponent, Update};
use tycho_simulation::tycho_client::feed::{BlockHeader, SynchronizerState};
use tycho_simulation::tycho_common::{
    dto::ProtocolStateDelta,
    models::{token::Token, Chain},
    simulation::{
        errors::{SimulationError, TransitionError},
        protocol_sim::{Balances, GetAmountOutResult, ProtocolSim},
    },
    Bytes,
};

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct DummySim;

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
        _amount_in: BigUint,
        _token_in: &Token,
        _token_out: &Token,
    ) -> Result<GetAmountOutResult, SimulationError> {
        Ok(GetAmountOutResult::new(
            BigUint::from(0u8),
            BigUint::from(0u8),
            Box::new(self.clone()),
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn eq(&self, other: &dyn ProtocolSim) -> bool {
        other.as_any().is::<DummySim>()
    }
}

struct QuoteBlockingFixture {
    app_state: AppState,
    vm_state_store: Arc<StateStore>,
    rfq_state_store: Arc<StateStore>,
}

fn address_hex(seed: u8) -> String {
    format!("{:02x}", seed).repeat(20)
}

#[expect(
    clippy::expect_used,
    reason = "Deterministic fixture ids always decode as addresses."
)]
fn address(seed: u8) -> Bytes {
    Bytes::from_str(&address_hex(seed)).expect("valid address")
}

fn make_token(seed: u8, symbol: &str) -> Token {
    Token::new(&address(seed), symbol, 18, 0, &[], Chain::Ethereum, 100)
}

async fn build_quote_blocking_fixture() -> QuoteBlockingFixture {
    let token_a = make_token(1, "TKNA");
    let token_b = make_token(2, "TKNB");

    let mut token_map = HashMap::new();
    token_map.insert(token_a.address.clone(), token_a.clone());
    token_map.insert(token_b.address.clone(), token_b.clone());

    let token_store = Arc::new(TokenStore::new(
        token_map,
        "http://localhost".to_string(),
        "test".to_string(),
        Chain::Ethereum,
        Duration::from_secs(1),
    ));

    let native_state_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
    let vm_state_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
    let rfq_state_store = Arc::new(StateStore::new(Arc::clone(&token_store)));

    let native_component = make_component(
        10,
        "uniswap_v2",
        "uniswap_v2_pool",
        vec![token_a.clone(), token_b.clone()],
    );
    let vm_component = make_component(
        11,
        "vm:curve",
        "curve_pool",
        vec![token_a.clone(), token_b.clone()],
    );
    let rfq_component = make_component(
        11,
        "rfq:hashflow",
        "curve_pool",
        vec![token_a.clone(), token_b.clone()],
    );

    let native_update = make_update(
        1,
        vec![(
            "pool-native".to_string(),
            native_component,
            Box::new(DummySim),
        )],
        HashMap::from([ready_state("uniswap_v2", 1)]),
    );
    let vm_update = make_update(
        1,
        vec![("pool-vm".to_string(), vm_component, Box::new(DummySim))],
        HashMap::from([ready_state("vm:curve", 1)]),
    );
    let rfq_update = make_update(
        1,
        vec![("pool-rfq".to_string(), rfq_component, Box::new(DummySim))],
        HashMap::from([ready_state("rfq:hashflow", 1)]),
    );

    native_state_store.apply_update(native_update).await;
    vm_state_store.apply_update(vm_update).await;
    rfq_state_store.apply_update(rfq_update).await;

    let app_state = AppState {
        chain: Chain::Ethereum,
        native_token_protocol_allowlist: Arc::new(vec!["rocketpool".to_string()]),
        tokens: Arc::clone(&token_store),
        native_state_store,
        vm_state_store: Arc::clone(&vm_state_store),
        rfq_state_store: Arc::clone(&rfq_state_store),
        native_stream_health: Arc::new(StreamHealth::new()),
        vm_stream_health: Arc::new(StreamHealth::new()),
        rfq_stream_health: Arc::new(StreamHealth::new()),
        vm_stream: Arc::new(tokio::sync::RwLock::new(VmStreamStatus::default())),
        rfq_stream: Arc::new(tokio::sync::RwLock::new(RfqStreamStatus::default())),
        enable_vm_pools: true,
        enable_rfq_pools: true,
        readiness_stale: Duration::from_secs(120),
        quote_timeout: Duration::from_millis(100),
        pool_timeout_native: Duration::from_millis(50),
        pool_timeout_vm: Duration::from_millis(50),
        pool_timeout_rfq: Duration::from_millis(50),
        request_timeout: Duration::from_millis(1000),
        native_sim_semaphore: Arc::new(Semaphore::new(4)),
        vm_sim_semaphore: Arc::new(Semaphore::new(4)),
        rfq_sim_semaphore: Arc::new(Semaphore::new(4)),
        erc4626_deposits_enabled: false,
        reset_allowance_tokens: Arc::new(HashMap::new()),
        native_sim_concurrency: 4,
        vm_sim_concurrency: 4,
        rfq_sim_concurrency: 4,
    };

    QuoteBlockingFixture {
        app_state,
        vm_state_store,
        rfq_state_store,
    }
}

async fn assert_quotes_block_vm_and_rfq(app_state: AppState) {
    let request = AmountOutRequest {
        request_id: "req-1".to_string(),
        auction_id: None,
        token_in: format!("0x{}", address_hex(1)),
        token_out: format!("0x{}", address_hex(2)),
        amounts: vec!["1000".to_string()],
    };

    let computation = get_amounts_out(app_state, request, None).await;
    assert!(computation.metrics.skipped_vm_unavailable);
    assert!(computation.metrics.skipped_rfq_unavailable);
    assert_eq!(computation.metrics.scheduled_vm_pools, 0);
    assert_eq!(computation.metrics.scheduled_rfq_pools, 0);
    assert!(computation.metrics.scheduled_native_pools > 0);
}

fn address_hex_from_seed(seed: u64) -> String {
    let seed_bytes = seed.to_be_bytes();
    let mut hex = String::with_capacity(40);
    for idx in 0..20 {
        let byte = seed_bytes[idx % seed_bytes.len()].wrapping_add(idx as u8);
        hex.push_str(&format!("{:02x}", byte));
    }
    hex
}

#[expect(
    clippy::expect_used,
    reason = "Deterministic fixture ids always decode as addresses."
)]
fn address_from_seed(seed: u64) -> Bytes {
    Bytes::from_str(&address_hex_from_seed(seed)).expect("valid address")
}

fn make_token_seed(seed: u64, symbol: &str) -> Token {
    Token::new(
        &address_from_seed(seed),
        symbol,
        18,
        0,
        &[],
        Chain::Ethereum,
        100,
    )
}

#[expect(
    clippy::expect_used,
    reason = "The Unix epoch timestamp is always valid."
)]
fn make_component(
    id_seed: u8,
    protocol_system: &str,
    protocol_type_name: &str,
    tokens: Vec<Token>,
) -> ProtocolComponent {
    ProtocolComponent::new(
        address(id_seed),
        protocol_system.to_string(),
        protocol_type_name.to_string(),
        Chain::Ethereum,
        tokens,
        Vec::new(),
        HashMap::new(),
        Bytes::new(),
        chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
            .expect("valid timestamp")
            .naive_utc(),
    )
}

#[expect(
    clippy::expect_used,
    reason = "The Unix epoch timestamp is always valid."
)]
fn make_component_seed(
    id_seed: u64,
    protocol_system: &str,
    protocol_type_name: &str,
    tokens: Vec<Token>,
) -> ProtocolComponent {
    ProtocolComponent::new(
        address_from_seed(id_seed),
        protocol_system.to_string(),
        protocol_type_name.to_string(),
        Chain::Ethereum,
        tokens,
        Vec::new(),
        HashMap::new(),
        Bytes::new(),
        chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
            .expect("valid timestamp")
            .naive_utc(),
    )
}

fn make_update(
    block: u64,
    pairs: Vec<(String, ProtocolComponent, Box<dyn ProtocolSim>)>,
    sync_states: HashMap<String, SynchronizerState>,
) -> Update {
    let mut states = HashMap::new();
    let mut new_pairs = HashMap::new();
    for (id, component, state) in pairs {
        states.insert(id.clone(), state);
        new_pairs.insert(id, component);
    }
    Update::new(block, states, new_pairs).set_sync_states(sync_states)
}

fn block_header(number: u64) -> BlockHeader {
    BlockHeader {
        number,
        timestamp: number,
        ..Default::default()
    }
}

fn ready_state(protocol: &str, number: u64) -> (String, SynchronizerState) {
    (
        protocol.to_string(),
        SynchronizerState::Ready(block_header(number)),
    )
}

#[derive(Debug)]
struct TestError(&'static str);

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for TestError {}

fn missing_block_error() -> Result<Update, Box<dyn std::error::Error + Send + Sync + 'static>> {
    Err(Box::new(TestError("Missing block!")))
}

#[expect(
    clippy::expect_used,
    reason = "The seeded test range is bounded to avoid overflow."
)]
fn build_pairs(
    protocol_system: &str,
    protocol_type_name: &str,
    seed_offset: u8,
    count: usize,
) -> Vec<(String, ProtocolComponent, Box<dyn ProtocolSim>)> {
    let mut pairs: Vec<(String, ProtocolComponent, Box<dyn ProtocolSim>)> =
        Vec::with_capacity(count);
    for i in 0..count {
        let seed = seed_offset as u16 + (i as u16) * 2;
        assert!(
            seed <= u8::MAX as u16,
            "seed offset too large for deterministic ids"
        );
        let seed = seed as u8;
        let next_seed = seed.checked_add(1).expect("seed overflow");
        let symbol_a = format!("T{}", seed);
        let symbol_b = format!("T{}", next_seed);
        let token_a = make_token(seed, &symbol_a);
        let token_b = make_token(next_seed, &symbol_b);
        let id = format!("{}-{}", protocol_system, seed);
        let component = make_component(
            seed,
            protocol_system,
            protocol_type_name,
            vec![token_a, token_b],
        );
        pairs.push((id, component, Box::new(DummySim) as Box<dyn ProtocolSim>));
    }
    pairs
}

fn build_pairs_seeded(
    protocol_system: &str,
    protocol_type_name: &str,
    seed_offset: u64,
    count: usize,
) -> Vec<(String, ProtocolComponent, Box<dyn ProtocolSim>)> {
    let mut pairs: Vec<(String, ProtocolComponent, Box<dyn ProtocolSim>)> =
        Vec::with_capacity(count);
    for i in 0..count {
        let seed = seed_offset + (i as u64) * 2;
        let next_seed = seed + 1;
        let symbol_a = format!("T{}", seed);
        let symbol_b = format!("T{}", next_seed);
        let token_a = make_token_seed(seed, &symbol_a);
        let token_b = make_token_seed(next_seed, &symbol_b);
        let id = format!("{}-{}", protocol_system, seed);
        let component = make_component_seed(
            seed,
            protocol_system,
            protocol_type_name,
            vec![token_a, token_b],
        );
        pairs.push((id, component, Box::new(DummySim) as Box<dyn ProtocolSim>));
    }
    pairs
}

fn account_address(seed: u64) -> Address {
    let seed_bytes = seed.to_be_bytes();
    let mut bytes = [0u8; 20];
    for (idx, byte) in bytes.iter_mut().enumerate() {
        let seed_byte = seed_bytes[idx % seed_bytes.len()];
        *byte = seed_byte.wrapping_add(idx as u8);
    }
    Address::from_slice(&bytes)
}

fn make_account_update(seed: u64, slots_per_account: usize, code_bytes: usize) -> AccountUpdate {
    let mut slots = HashMap::with_capacity(slots_per_account);
    for slot in 0..slots_per_account {
        slots.insert(U256::from(slot as u64), U256::from(seed + slot as u64));
    }

    AccountUpdate::new(
        account_address(seed),
        Chain::Ethereum,
        slots,
        Some(U256::from(seed)),
        Some(vec![seed as u8; code_bytes]),
        ChangeType::Creation,
    )
}

fn build_account_updates(
    account_count: usize,
    slots_per_account: usize,
    code_bytes: usize,
    seed_offset: u64,
) -> Vec<AccountUpdate> {
    let mut updates = Vec::with_capacity(account_count);
    for idx in 0..account_count {
        let seed = seed_offset + idx as u64;
        updates.push(make_account_update(seed, slots_per_account, code_bytes));
    }
    updates
}

#[expect(
    clippy::expect_used,
    reason = "The jemalloc control API is required for these allocator harnesses."
)]
fn jemalloc_allocated_bytes() -> usize {
    jemalloc::epoch::advance().expect("jemalloc epoch advance");
    jemalloc::stats::allocated::read().expect("jemalloc allocated bytes")
}

fn default_stream_config() -> StreamSupervisorConfig {
    StreamSupervisorConfig {
        stream_stale: Duration::from_secs(3600),
        missing_block_burst: 3,
        missing_block_window: Duration::from_secs(60),
        error_burst: 3,
        error_window: Duration::from_secs(60),
        resync_grace: Duration::from_secs(60),
        restart_backoff_min: Duration::from_millis(10),
        restart_backoff_max: Duration::from_millis(20),
        restart_backoff_jitter_pct: 0.0,
        memory: MemoryConfig {
            purge_enabled: false,
            snapshots_enabled: false,
            snapshots_min_interval_secs: 60,
            snapshots_min_new_pairs: 1000,
            snapshots_emit_emf: false,
        },
    }
}

#[tokio::test]
async fn missing_block_burst_does_not_restart_below_threshold() {
    let token_store = Arc::new(TokenStore::new(
        HashMap::new(),
        "http://localhost".to_string(),
        "test".to_string(),
        Chain::Ethereum,
        Duration::from_secs(1),
    ));
    let state_store = Arc::new(StateStore::new(token_store));
    let health = Arc::new(StreamHealth::new());

    let stream = stream::iter(vec![missing_block_error(), missing_block_error()]);

    let exit = process_stream(
        StreamKind::Native,
        stream,
        state_store,
        health,
        default_stream_config(),
    )
    .await;

    assert_eq!(exit.reason, StreamRestartReason::Ended);
}

#[tokio::test]
async fn missing_block_burst_restarts_at_threshold() {
    let token_store = Arc::new(TokenStore::new(
        HashMap::new(),
        "http://localhost".to_string(),
        "test".to_string(),
        Chain::Ethereum,
        Duration::from_secs(1),
    ));
    let state_store = Arc::new(StateStore::new(token_store));
    let health = Arc::new(StreamHealth::new());

    let stream = stream::iter(vec![
        missing_block_error(),
        missing_block_error(),
        missing_block_error(),
    ]);

    let exit = process_stream(
        StreamKind::Native,
        stream,
        state_store,
        health,
        default_stream_config(),
    )
    .await;

    assert_eq!(exit.reason, StreamRestartReason::MissingBlock);
}

#[tokio::test(start_paused = true)]
#[expect(
    clippy::expect_used,
    reason = "The spawned stream task should always join in this test."
)]
async fn native_stream_restarts_on_stale() {
    let token_store = Arc::new(TokenStore::new(
        HashMap::new(),
        "http://localhost".to_string(),
        "test".to_string(),
        Chain::Ethereum,
        Duration::from_secs(1),
    ));
    let state_store = Arc::new(StateStore::new(token_store));
    let health = Arc::new(StreamHealth::new());

    let mut cfg = default_stream_config();
    cfg.stream_stale = Duration::from_secs(5);

    let stream = futures::stream::pending();
    let handle = tokio::spawn(process_stream(
        StreamKind::Native,
        stream,
        state_store,
        health,
        cfg,
    ));

    advance(Duration::from_secs(6)).await;
    yield_now().await;

    let exit = handle.await.expect("stream task join");
    assert_eq!(exit.reason, StreamRestartReason::Stale);
}

#[tokio::test]
async fn vm_rebuild_resets_store_and_blocks_quotes() {
    let fixture = build_quote_blocking_fixture().await;
    let app_state = fixture.app_state.clone();
    let vm_state_store = fixture.vm_state_store;
    let rfq_state_store = fixture.rfq_state_store;

    assert!(app_state.vm_ready().await);
    assert!(app_state.rfq_ready().await);

    vm_state_store.reset().await;
    {
        let mut status = app_state.vm_stream.write().await;
        status.rebuilding = true;
        status.rebuild_started_at = Some(tokio::time::Instant::now());
    }

    rfq_state_store.reset().await;
    {
        let mut status = app_state.rfq_stream.write().await;
        status.rebuilding = true;
        status.rebuild_started_at = Some(tokio::time::Instant::now());
    }

    assert!(!app_state.vm_ready().await);
    assert_eq!(vm_state_store.total_states().await, 0);

    assert!(!app_state.rfq_ready().await);
    assert_eq!(rfq_state_store.total_states().await, 0);

    assert_quotes_block_vm_and_rfq(app_state).await;
}

#[tokio::test]
async fn jemalloc_memory_plateau_after_reset() {
    const POOL_COUNT: usize = 64;
    const DRIFT_FLOOR_BYTES: usize = 8 * 1024 * 1024;

    let token_store = Arc::new(TokenStore::new(
        HashMap::new(),
        "http://localhost".to_string(),
        "test".to_string(),
        Chain::Ethereum,
        Duration::from_secs(1),
    ));
    let state_store = Arc::new(StateStore::new(token_store));

    let mut initial_pairs = build_pairs("uniswap_v2", "uniswap_v2_pool", 1, POOL_COUNT);
    initial_pairs.extend(build_pairs(
        "uniswap_v3",
        "uniswap_v3_pool",
        120,
        POOL_COUNT,
    ));

    let initial_update = make_update(10, initial_pairs, HashMap::new());
    state_store.apply_update(initial_update).await;

    assert_eq!(state_store.total_states().await, POOL_COUNT * 2);
    let baseline = jemalloc_allocated_bytes();

    state_store.reset().await;

    let mut resync_pairs = build_pairs("uniswap_v2", "uniswap_v2_pool", 1, POOL_COUNT);
    resync_pairs.extend(build_pairs(
        "uniswap_v3",
        "uniswap_v3_pool",
        120,
        POOL_COUNT,
    ));
    let resync_update = make_update(11, resync_pairs, HashMap::new());
    state_store.apply_update(resync_update).await;

    assert_eq!(state_store.total_states().await, POOL_COUNT * 2);
    let after = jemalloc_allocated_bytes();

    let allowed_drift = (baseline / 20).max(DRIFT_FLOOR_BYTES);
    let delta = baseline.abs_diff(after);
    assert!(
        delta <= allowed_drift,
        "jemalloc allocated drifted too far: baseline={} after={} delta={} allowed={}",
        baseline,
        after,
        delta,
        allowed_drift
    );
}

#[tokio::test]
#[expect(
    clippy::expect_used,
    reason = "This harness intentionally treats TychoDB operations as hard invariants."
)]
async fn memory_spike_breakdown_harness() {
    const POOL_COUNT: usize = 512;
    const DB_ACCOUNT_COUNT: usize = 4000;
    const DB_SLOTS_PER_ACCOUNT: usize = 64;
    const DB_CODE_BYTES: usize = 2048;

    SHARED_TYCHO_DB
        .clear()
        .expect("clear TychoDB before harness");

    let token_store = Arc::new(TokenStore::new(
        HashMap::new(),
        "http://localhost".to_string(),
        "test".to_string(),
        Chain::Ethereum,
        Duration::from_secs(1),
    ));
    let state_store = Arc::new(StateStore::new(token_store));

    let baseline = jemalloc_allocated_bytes();

    let mut pairs = build_pairs_seeded("uniswap_v2", "uniswap_v2_pool", 1, POOL_COUNT);
    pairs.extend(build_pairs_seeded(
        "uniswap_v3",
        "uniswap_v3_pool",
        10_000,
        POOL_COUNT,
    ));
    let update = make_update(1, pairs, HashMap::new());
    state_store.apply_update(update).await;
    let after_state_update = jemalloc_allocated_bytes();

    state_store.reset().await;
    let after_state_reset = jemalloc_allocated_bytes();

    drop(state_store);
    yield_now().await;
    let after_state_drop = jemalloc_allocated_bytes();

    SHARED_TYCHO_DB
        .clear()
        .expect("clear TychoDB before db update");
    let after_db_clear = jemalloc_allocated_bytes();

    let db_updates =
        build_account_updates(DB_ACCOUNT_COUNT, DB_SLOTS_PER_ACCOUNT, DB_CODE_BYTES, 1);
    SHARED_TYCHO_DB
        .update(db_updates, Some(block_header(100)))
        .expect("update TychoDB");
    let after_db_update = jemalloc_allocated_bytes();

    SHARED_TYCHO_DB
        .clear()
        .expect("clear TychoDB after db update");
    let after_db_reset = jemalloc_allocated_bytes();

    println!(
        "memory_breakdown baseline={} after_state_update={} after_state_reset={} after_state_drop={} after_db_clear={} after_db_update={} after_db_reset={}",
        baseline,
        after_state_update,
        after_state_reset,
        after_state_drop,
        after_db_clear,
        after_db_update,
        after_db_reset
    );
}

#[tokio::test]
#[expect(
    clippy::expect_used,
    reason = "This harness intentionally treats TychoDB operations as hard invariants."
)]
async fn shared_db_rebuild_stress_harness() {
    const DB_ACCOUNT_COUNT: usize = 4000;
    const DB_SLOTS_PER_ACCOUNT: usize = 64;
    const DB_CODE_BYTES: usize = 2048;

    SHARED_TYCHO_DB
        .clear()
        .expect("clear TychoDB before harness");
    let baseline = jemalloc_allocated_bytes();

    let first_updates = build_account_updates(
        DB_ACCOUNT_COUNT,
        DB_SLOTS_PER_ACCOUNT,
        DB_CODE_BYTES,
        50_000,
    );
    SHARED_TYCHO_DB
        .update(first_updates, Some(block_header(200)))
        .expect("update TychoDB first");
    let after_first = jemalloc_allocated_bytes();

    SHARED_TYCHO_DB
        .clear()
        .expect("clear TychoDB between rebuilds");
    let after_clear = jemalloc_allocated_bytes();

    let second_updates = build_account_updates(
        DB_ACCOUNT_COUNT,
        DB_SLOTS_PER_ACCOUNT,
        DB_CODE_BYTES,
        150_000,
    );
    SHARED_TYCHO_DB
        .update(second_updates, Some(block_header(201)))
        .expect("update TychoDB second");
    let after_second = jemalloc_allocated_bytes();

    println!(
        "shared_db_stress baseline={} after_first={} after_clear={} after_second={}",
        baseline, after_first, after_clear, after_second
    );
}
