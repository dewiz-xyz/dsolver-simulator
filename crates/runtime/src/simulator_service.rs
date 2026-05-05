use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info};
use tycho_simulation::tycho_common::{
    models::{token::Token, Chain},
    Bytes,
};
use tycho_simulation::utils::load_all_tokens;

use crate::config::{init_logging, load_config, AppConfig, MemoryConfig};
use crate::memory::maybe_log_memory_snapshot;
use crate::models::rfq::bebop::BebopResponse;
use crate::models::rfq::hashflow::read_hashflow_csv;
use crate::models::rfq::liquorice::TokenLiquorice;
use crate::models::state::{
    AppState, BroadcasterSubscriptionStatus, RfqStreamStatus, StateStore, VmStreamStatus,
};
use crate::models::stream_health::StreamHealth;
use crate::models::tokens::TokenStore;
use crate::services::broadcaster_subscription::{
    supervise_broadcaster_subscription, BroadcasterSubscriptionControls,
    NativeBroadcasterSubscriptionControls, VmBroadcasterSubscriptionControls,
};
use crate::services::stream_builder::{build_rfq_stream, RFQConfig, RFQTokenStores};
use crate::services::QuoteService;
use crate::stream::{supervise_rfq_stream, RfqStreamControls, StreamSupervisorConfig};

struct RfqStreamDeps<'a> {
    resources: &'a StreamResources,
    app_state: &'a AppState,
    bebop_tokens: Arc<TokenStore>,
    hashflow_tokens: Arc<TokenStore>,
    liquorice_tokens: Arc<TokenStore>,
}

pub struct SimulatorServiceParts {
    pub config: AppConfig,
    pub runtime: SimulatorRuntime,
}

/// Runtime-owned simulator services exposed to the RPC shell.
#[derive(Clone)]
pub struct SimulatorRuntime {
    app_state: AppState,
    quote_service: QuoteService,
}

impl SimulatorRuntime {
    pub fn new(app_state: AppState) -> Self {
        Self {
            quote_service: QuoteService::new(app_state.clone()),
            app_state,
        }
    }

    pub fn app_state(&self) -> AppState {
        self.app_state.clone()
    }

    pub fn quote_service(&self) -> QuoteService {
        self.quote_service.clone()
    }

    pub fn request_timeout(&self) -> Duration {
        self.app_state.request_timeout()
    }
}

pub async fn build_simulator_service() -> anyhow::Result<SimulatorServiceParts> {
    init_logging();
    let config = load_config();
    let chain = config.chain_profile.chain;
    let tycho_url = config.tycho_url.clone();
    let effective_rfq_enabled =
        config.enable_rfq_pools && !config.chain_profile.rfq_protocols.is_empty();
    info!(chain_id = chain.id(), chain = %chain, "Initializing price service...");
    log_memory_config(config.memory);
    log_erc4626_capability(&config);
    spawn_memory_snapshot_task(config.memory);

    let tokens = load_token_store(&config, &tycho_url).await?;
    let bebop_tokens: Arc<TokenStore>;
    let hashflow_tokens: Arc<TokenStore>;
    let liquorice_tokens: Arc<TokenStore>;

    if effective_rfq_enabled {
        // RFQ enabled means RFQ bootstrap must succeed.
        bebop_tokens = if rfq_protocol_enabled(&config.chain_profile.rfq_protocols, "rfq:bebop") {
            load_bebop_token_store(&config, &tycho_url, &config.bebop_url, chain).await?
        } else {
            new_token_store(HashMap::new(), &tycho_url, &config)
        };
        hashflow_tokens =
            if rfq_protocol_enabled(&config.chain_profile.rfq_protocols, "rfq:hashflow") {
                load_hashflow_token_store(&config, &tycho_url, &config.hashflow_filename, chain)?
            } else {
                new_token_store(HashMap::new(), &tycho_url, &config)
            };
        liquorice_tokens =
            if rfq_protocol_enabled(&config.chain_profile.rfq_protocols, "rfq:liquorice") {
                let liquorice_url = config.liquorice_url.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("Liquorice RFQ enabled for {chain} without liquorice_url")
                })?;
                load_liquorice_token_store(&config, &tycho_url, liquorice_url, chain).await?
            } else {
                new_token_store(HashMap::new(), &tycho_url, &config)
            };
    } else {
        bebop_tokens = new_token_store(HashMap::new(), &tycho_url, &config);
        hashflow_tokens = new_token_store(HashMap::new(), &tycho_url, &config);
        liquorice_tokens = new_token_store(HashMap::new(), &tycho_url, &config);
    }

    let stream_resources = create_stream_resources(Arc::clone(&tokens));
    let app_state = build_app_state(&config, Arc::clone(&tokens), &stream_resources);
    let supervisor_cfg = build_supervisor_config(&config);

    log_rebuild_config(&config);
    spawn_broadcaster_subscription_task(&config, &supervisor_cfg, &stream_resources, &app_state);
    let rfq_config = RFQConfig {
        bebop_user: config.bebop_user.clone(),
        bebop_key: config.bebop_key.clone(),
        hashflow_user: config.hashflow_user.clone(),
        hashflow_key: config.hashflow_key.clone(),
        liquorice_user: config.liquorice_user.clone(),
        liquorice_key: config.liquorice_key.clone(),
    };
    spawn_rfq_stream_task(
        &config,
        &supervisor_cfg,
        Arc::clone(&tokens),
        RfqStreamDeps {
            resources: &stream_resources,
            app_state: &app_state,
            bebop_tokens: Arc::clone(&bebop_tokens),
            hashflow_tokens: Arc::clone(&hashflow_tokens),
            liquorice_tokens: Arc::clone(&liquorice_tokens),
        },
        rfq_config,
    );

    Ok(SimulatorServiceParts {
        config,
        runtime: SimulatorRuntime::new(app_state),
    })
}

struct StreamResources {
    native_state_store: Arc<StateStore>,
    vm_state_store: Arc<StateStore>,
    rfq_state_store: Arc<StateStore>,
    native_stream_health: Arc<StreamHealth>,
    vm_stream_health: Arc<StreamHealth>,
    rfq_stream_health: Arc<StreamHealth>,
    vm_stream: Arc<tokio::sync::RwLock<VmStreamStatus>>,
    rfq_stream: Arc<tokio::sync::RwLock<RfqStreamStatus>>,
}

fn log_memory_config(memory: MemoryConfig) {
    info!(
        event = "memory_config",
        purge_enabled = memory.purge_enabled,
        snapshots_enabled = memory.snapshots_enabled,
        min_interval_secs = memory.snapshots_min_interval_secs,
        min_new_pairs = memory.snapshots_min_new_pairs,
        emf_enabled = memory.snapshots_emit_emf,
        "Memory config loaded"
    );
    maybe_log_memory_snapshot("service", "startup", None, memory, true);
}

fn spawn_memory_snapshot_task(memory_cfg: MemoryConfig) {
    if !memory_cfg.snapshots_enabled {
        return;
    }

    tokio::spawn(async move {
        let mut ticker =
            tokio::time::interval(Duration::from_secs(memory_cfg.snapshots_min_interval_secs));
        // `interval` ticks immediately on first await; skip it so "startup" remains first.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            maybe_log_memory_snapshot("service", "periodic", None, memory_cfg, false);
        }
    });
}

async fn load_token_store(config: &AppConfig, tycho_url: &str) -> anyhow::Result<Arc<TokenStore>> {
    let chain = config.chain_profile.chain;
    let all_tokens = load_all_tokens(
        tycho_url,
        false,
        Some(&config.api_key),
        true,
        chain,
        Some(10),
        None,
    )
    .await?;
    info!("Loaded {} tokens", all_tokens.len());

    Ok(Arc::new(TokenStore::new(
        all_tokens,
        tycho_url.to_string(),
        config.api_key.clone(),
        chain,
        Duration::from_millis(config.token_refresh_timeout_ms),
    )))
}

async fn load_bebop_token_store(
    config: &AppConfig,
    tycho_url: &str,
    bebop_url: &str,
    chain: Chain,
) -> anyhow::Result<Arc<TokenStore>> {
    let client = Client::new();
    let bebop_tokens = load_bebop_tokens(&client, bebop_url, chain).await?;
    info!("all bebop tokens: {:?}", bebop_tokens);
    Ok(new_token_store(bebop_tokens, tycho_url, config))
}

async fn load_bebop_tokens(
    client: &Client,
    bebop_url: &str,
    chain: Chain,
) -> anyhow::Result<HashMap<Bytes, Token>> {
    let response: BebopResponse = client
        .get(bebop_url)
        .query(&[
            ("active_only", "true"),
            ("gasless", "false"),
            ("expiry_type", "standard"),
        ])
        .header("accept", "application/json")
        .send()
        .await?
        .json()
        .await?;

    response
        .tokens
        .into_iter()
        .filter_map(|(_ticker, token)| match token.to_tycho_token(chain) {
            Ok(Some(new)) => Some(Ok((new.address.clone(), new))),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        })
        .collect::<Result<_, _>>()
        .map_err(|error| anyhow::anyhow!("Failed to parse Bebop token: {}", error))
}

fn load_hashflow_token_store(
    config: &AppConfig,
    tycho_url: &str,
    hashflow_filename: &str,
    chain: Chain,
) -> anyhow::Result<Arc<TokenStore>> {
    let hashflow_tokens = read_hashflow_csv(hashflow_filename, chain)
        .map_err(|error| anyhow::anyhow!("Failed to read hashflow CSV: {}", error))?;
    info!("all_hashflow_tokens: {:?}", hashflow_tokens);
    Ok(new_token_store(hashflow_tokens, tycho_url, config))
}

async fn load_liquorice_token_store(
    config: &AppConfig,
    tycho_url: &str,
    liquorice_url: &str,
    chain: Chain,
) -> anyhow::Result<Arc<TokenStore>> {
    let client = Client::new();
    let liquorice_tokens = load_liquorice_tokens(
        &client,
        liquorice_url,
        chain,
        &config.liquorice_user,
        &config.liquorice_key,
    )
    .await?;
    info!("all_liquorice_tokens: {:?}", liquorice_tokens);
    Ok(new_token_store(liquorice_tokens, tycho_url, config))
}

async fn load_liquorice_tokens(
    client: &Client,
    liquorice_url: &str,
    chain: Chain,
    solver: &str,
    authorization: &str,
) -> anyhow::Result<HashMap<Bytes, Token>> {
    let chain_id = chain.id().to_string();
    let response: Vec<TokenLiquorice> = client
        .get(liquorice_url)
        .query(&[("chainId", chain_id)])
        .header("accept", "application/json")
        .header("solver", solver)
        .header("authorization", authorization)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    response
        .into_iter()
        .map(|token| {
            token
                .to_tycho_token(chain)
                .map(|new| (new.address.clone(), new))
        })
        .collect::<Result<_, _>>()
        .map_err(|error| anyhow::anyhow!("Failed to parse Liquorice token: {}", error))
}

fn rfq_protocol_enabled(protocols: &[String], protocol: &str) -> bool {
    protocols.iter().any(|configured| configured == protocol)
}

fn new_token_store(
    tokens: HashMap<Bytes, Token>,
    tycho_url: &str,
    config: &AppConfig,
) -> Arc<TokenStore> {
    Arc::new(TokenStore::new(
        tokens,
        tycho_url.to_string(),
        config.api_key.clone(),
        config.chain_profile.chain,
        Duration::from_millis(config.token_refresh_timeout_ms),
    ))
}

fn create_stream_resources(tokens: Arc<TokenStore>) -> StreamResources {
    let native_state_store = Arc::new(StateStore::new(Arc::clone(&tokens)));
    let vm_state_store = Arc::new(StateStore::new(Arc::clone(&tokens)));
    let rfq_state_store = Arc::new(StateStore::new(tokens));
    let native_stream_health = Arc::new(StreamHealth::new());
    let vm_stream_health = Arc::new(StreamHealth::new());
    let rfq_stream_health = Arc::new(StreamHealth::new());
    let vm_stream = Arc::new(tokio::sync::RwLock::new(VmStreamStatus::default()));
    let rfq_stream = Arc::new(tokio::sync::RwLock::new(RfqStreamStatus::default()));
    debug!("Created shared state");

    StreamResources {
        native_state_store,
        vm_state_store,
        rfq_state_store,
        native_stream_health,
        vm_stream_health,
        rfq_stream_health,
        vm_stream,
        rfq_stream,
    }
}

fn build_app_state(
    config: &AppConfig,
    tokens: Arc<TokenStore>,
    resources: &StreamResources,
) -> AppState {
    let chain = config.chain_profile.chain;
    let readiness_stale = Duration::from_secs(config.readiness_stale_secs);
    let request_timeout = Duration::from_millis(config.request_timeout_ms);
    let configured_vm_pools = !config.chain_profile.vm_protocols.is_empty();
    let configured_rfq_pools = !config.chain_profile.rfq_protocols.is_empty();
    // VM is only effective when enabled and the selected chain exposes VM protocols.
    let effective_vm_enabled = config.enable_vm_pools && configured_vm_pools;
    // RFQ is only effective when enabled and the selected chain exposes RFQ protocols.
    let effective_rfq_enabled = config.enable_rfq_pools && configured_rfq_pools;

    AppState {
        chain,
        native_token_protocol_allowlist: Arc::new(
            config.chain_profile.native_token_protocol_allowlist.clone(),
        ),
        tokens,
        native_broadcaster_subscription: BroadcasterSubscriptionStatus::default(),
        vm_broadcaster_subscription: BroadcasterSubscriptionStatus::default(),
        native_state_store: Arc::clone(&resources.native_state_store),
        vm_state_store: Arc::clone(&resources.vm_state_store),
        rfq_state_store: Arc::clone(&resources.rfq_state_store),
        native_stream_health: Arc::clone(&resources.native_stream_health),
        vm_stream_health: Arc::clone(&resources.vm_stream_health),
        rfq_stream_health: Arc::clone(&resources.rfq_stream_health),
        vm_stream: Arc::clone(&resources.vm_stream),
        rfq_stream: Arc::clone(&resources.rfq_stream),
        configured_backends: crate::models::state::ConfiguredBackends {
            vm: configured_vm_pools,
            rfq: configured_rfq_pools,
        },
        enable_vm_pools: effective_vm_enabled,
        enable_rfq_pools: effective_rfq_enabled,
        readiness_stale,
        request_timeout,
        simulation_rebuild_gate: Arc::new(tokio::sync::RwLock::new(())),
        slippage: config.slippage,
        erc4626_deposits_enabled: config.rpc_url.is_some(),
        erc4626_pair_policies: Arc::clone(&config.erc4626_pair_policies),
        reset_allowance_tokens: Arc::clone(&config.reset_allowance_tokens),
    }
}

fn log_erc4626_capability(config: &AppConfig) {
    if config.rpc_url.is_some() {
        info!("ERC4626 deposits enabled: RPC_URL is configured");
    } else {
        info!("ERC4626 deposits disabled: RPC_URL is not configured; redeems remain enabled");
    }
}

fn build_supervisor_config(config: &AppConfig) -> StreamSupervisorConfig {
    StreamSupervisorConfig {
        readiness_stale: Duration::from_secs(config.readiness_stale_secs),
        stream_stale: Duration::from_secs(config.stream_stale_secs),
        missing_block_burst: config.stream_missing_block_burst,
        missing_block_window: Duration::from_secs(config.stream_missing_block_window_secs),
        error_burst: config.stream_error_burst,
        error_window: Duration::from_secs(config.stream_error_window_secs),
        resync_grace: Duration::from_secs(config.resync_grace_secs),
        restart_backoff_min: Duration::from_millis(config.stream_restart_backoff_min_ms),
        restart_backoff_max: Duration::from_millis(config.stream_restart_backoff_max_ms),
        restart_backoff_jitter_pct: config.stream_restart_backoff_jitter_pct,
        memory: config.memory,
    }
}
fn log_rebuild_config(config: &AppConfig) {
    let effective_vm_enabled =
        config.enable_vm_pools && !config.chain_profile.vm_protocols.is_empty();
    let effective_rfq_enabled =
        config.enable_rfq_pools && !config.chain_profile.rfq_protocols.is_empty();
    info!(
        enable_vm_pools = effective_vm_enabled,
        requested_vm_pools = config.enable_vm_pools,
        enable_rfq_pools = effective_rfq_enabled,
        requested_rfq_pools = config.enable_rfq_pools,
        "Initialized backend rebuild gate"
    );
}

fn spawn_broadcaster_subscription_task(
    config: &AppConfig,
    supervisor_cfg: &StreamSupervisorConfig,
    resources: &StreamResources,
    app_state: &AppState,
) {
    spawn_native_broadcaster_subscription_task(config, supervisor_cfg, resources, app_state);
    if app_state.enable_vm_pools {
        spawn_vm_broadcaster_subscription_task(config, supervisor_cfg, resources, app_state);
    }
}

fn spawn_native_broadcaster_subscription_task(
    config: &AppConfig,
    supervisor_cfg: &StreamSupervisorConfig,
    resources: &StreamResources,
    app_state: &AppState,
) {
    let controls = BroadcasterSubscriptionControls::Native(NativeBroadcasterSubscriptionControls {
        broadcaster_subscription: app_state.native_broadcaster_subscription.clone(),
        state_store: Arc::clone(&resources.native_state_store),
        stream_health: Arc::clone(&resources.native_stream_health),
    });
    spawn_backend_broadcaster_subscription_task(
        "native",
        config.tycho_broadcaster_ws_url.clone(),
        config.chain_profile.chain.id(),
        supervisor_cfg.clone(),
        controls,
    );
}

fn spawn_vm_broadcaster_subscription_task(
    config: &AppConfig,
    supervisor_cfg: &StreamSupervisorConfig,
    resources: &StreamResources,
    app_state: &AppState,
) {
    let controls = BroadcasterSubscriptionControls::Vm(VmBroadcasterSubscriptionControls {
        broadcaster_subscription: app_state.vm_broadcaster_subscription.clone(),
        state_store: Arc::clone(&resources.vm_state_store),
        stream_health: Arc::clone(&resources.vm_stream_health),
        vm_stream: Arc::clone(&resources.vm_stream),
        simulation_rebuild_gate: app_state.simulation_rebuild_gate(),
    });
    spawn_backend_broadcaster_subscription_task(
        "vm",
        config.tycho_broadcaster_ws_url.clone(),
        config.chain_profile.chain.id(),
        supervisor_cfg.clone(),
        controls,
    );
}

fn spawn_backend_broadcaster_subscription_task(
    backend: &'static str,
    ws_url: String,
    expected_chain_id: u64,
    supervisor_cfg: StreamSupervisorConfig,
    controls: BroadcasterSubscriptionControls,
) {
    tokio::spawn(async move {
        info!(backend, "Starting broadcaster subscription supervisor...");
        supervise_broadcaster_subscription(ws_url, expected_chain_id, supervisor_cfg, controls)
            .await;
    });
    debug!(backend, "Broadcaster subscription supervisor task spawned");
}

fn spawn_rfq_stream_task(
    config: &AppConfig,
    supervisor_cfg: &StreamSupervisorConfig,
    tokens: Arc<TokenStore>,
    deps: RfqStreamDeps<'_>,
    rfq_config: RFQConfig,
) {
    let chain = config.chain_profile.chain;
    let effective_rfq_enabled =
        config.enable_rfq_pools && !config.chain_profile.rfq_protocols.is_empty();
    if !effective_rfq_enabled {
        if !config.enable_rfq_pools {
            info!("RFQ pool feeds disabled");
        } else {
            info!(
                chain = %chain,
                "RFQ pool feeds enabled but no RFQ protocols configured for this chain; skipping RFQ stream"
            );
        }
        return;
    }

    let rfq_supervisor_cfg = supervisor_cfg.clone();
    let tokens_bg = tokens;
    let bebop_tokens_bg = deps.bebop_tokens;
    let hashflow_tokens_bg = deps.hashflow_tokens;
    let liquorice_tokens_bg = deps.liquorice_tokens;
    let state_store_bg = Arc::clone(&deps.resources.rfq_state_store);
    let health_bg = Arc::clone(&deps.resources.rfq_stream_health);
    let rfq_stream_bg = Arc::clone(&deps.resources.rfq_stream);
    let simulation_rebuild_gate = deps.app_state.simulation_rebuild_gate();
    let tvl_threshold = config.tvl_threshold;
    let rfq_protocols = config.chain_profile.rfq_protocols.clone();
    tokio::spawn(async move {
        info!("Starting RFQ protocol stream supervisor...");
        supervise_rfq_stream(
            move || {
                let tokens = Arc::clone(&tokens_bg);
                let bebop_tokens = Arc::clone(&bebop_tokens_bg);
                let hashflow_tokens = Arc::clone(&hashflow_tokens_bg);
                let liquorice_tokens = Arc::clone(&liquorice_tokens_bg);
                let protocols = rfq_protocols.clone();
                let rfq_config = rfq_config.clone();
                let token_stores = RFQTokenStores {
                    tokens,
                    bebop: bebop_tokens,
                    hashflow: hashflow_tokens,
                    liquorice: liquorice_tokens,
                };
                async move {
                    build_rfq_stream(tvl_threshold, token_stores, chain, &protocols, rfq_config)
                        .await
                }
            },
            state_store_bg,
            health_bg,
            rfq_supervisor_cfg,
            RfqStreamControls {
                rfq_stream: rfq_stream_bg,
                simulation_rebuild_gate,
            },
        )
        .await;
    });
    debug!("RFQ stream supervisor task spawned");
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;
    use std::time::Duration;

    use crate::config::{AppConfig, ChainProfile, MemoryConfig, SlippageConfig};
    use crate::models::tokens::TokenStore;
    use tycho_simulation::tycho_common::{models::Chain, Bytes};

    use super::{build_app_state, create_stream_resources};

    fn build_test_config(
        chain_profile: ChainProfile,
        enable_vm_pools: bool,
        enable_rfq_pools: bool,
        rpc_url: Option<&str>,
    ) -> AppConfig {
        let reset_allowance_tokens = Arc::new(chain_profile.reset_allowance_tokens.clone());
        let erc4626_pair_policies = Arc::new(chain_profile.erc4626_pair_policies.clone());

        AppConfig {
            chain_profile,
            tycho_url: "http://localhost:4242".to_string(),
            tycho_broadcaster_ws_url: "ws://127.0.0.1:3001/ws".to_string(),
            bebop_url: "https://example.com/bebop".to_string(),
            hashflow_filename: "./hashflow.csv".to_string(),
            liquorice_url: Some("https://example.com/liquorice".to_string()),
            api_key: "test-api-key".to_string(),
            rpc_url: rpc_url.map(str::to_string),
            tvl_threshold: 100.0,
            tvl_keep_threshold: 20.0,
            port: 3000,
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            request_timeout_ms: 4_500,
            token_refresh_timeout_ms: 1_000,
            enable_vm_pools,
            enable_rfq_pools,
            reset_allowance_tokens,
            erc4626_pair_policies,
            stream_stale_secs: 120,
            stream_missing_block_burst: 3,
            stream_missing_block_window_secs: 60,
            stream_error_burst: 3,
            stream_error_window_secs: 60,
            resync_grace_secs: 60,
            stream_restart_backoff_min_ms: 500,
            stream_restart_backoff_max_ms: 30_000,
            stream_restart_backoff_jitter_pct: 0.2,
            readiness_stale_secs: 300,
            slippage: SlippageConfig::default(),
            memory: MemoryConfig {
                purge_enabled: true,
                snapshots_enabled: false,
                snapshots_min_interval_secs: 60,
                snapshots_min_new_pairs: 1_000,
                snapshots_emit_emf: false,
            },
            bebop_user: "bebop-user".to_string(),
            bebop_key: "bebop-key".to_string(),
            hashflow_user: "hashflow-user".to_string(),
            hashflow_key: "hashflow-key".to_string(),
            liquorice_user: "liquorice-user".to_string(),
            liquorice_key: "liquorice-key".to_string(),
        }
    }

    fn build_test_token_store(chain: Chain) -> Arc<TokenStore> {
        Arc::new(TokenStore::new(
            HashMap::new(),
            "http://localhost".to_string(),
            "test-api-key".to_string(),
            chain,
            Duration::from_millis(10),
        ))
    }

    fn base_chain_profile() -> ChainProfile {
        ChainProfile {
            chain: Chain::Base,
            native_protocols: vec![
                "uniswap_v2".to_string(),
                "uniswap_v3".to_string(),
                "uniswap_v4".to_string(),
                "pancakeswap_v3".to_string(),
            ],
            vm_protocols: Vec::new(),
            rfq_protocols: vec!["rfq:bebop".to_string(), "rfq:hashflow".to_string()],
            native_token_protocol_allowlist: Vec::new(),
            reset_allowance_tokens: HashMap::new(),
            erc4626_pair_policies: Vec::new(),
        }
    }

    fn ethereum_chain_profile() -> ChainProfile {
        let mut reset_allowance_tokens = HashMap::new();
        reset_allowance_tokens.insert(1, HashSet::from([Bytes::from([7_u8; 20])]));

        ChainProfile {
            chain: Chain::Ethereum,
            native_protocols: vec![
                "uniswap_v2".to_string(),
                "uniswap_v3".to_string(),
                "rocketpool".to_string(),
            ],
            vm_protocols: vec!["vm:curve".to_string()],
            rfq_protocols: vec!["rfq:hashflow".to_string(), "rfq:liquorice".to_string()],
            native_token_protocol_allowlist: vec!["rocketpool".to_string()],
            reset_allowance_tokens,
            erc4626_pair_policies: Vec::new(),
        }
    }

    #[test]
    fn build_app_state_disables_effective_vm_but_keeps_rfq_for_base_profile() {
        let config = build_test_config(base_chain_profile(), true, true, None);
        let tokens = build_test_token_store(Chain::Base);
        let resources = create_stream_resources(Arc::clone(&tokens));

        let app_state = build_app_state(&config, Arc::clone(&tokens), &resources);

        assert_eq!(app_state.chain, Chain::Base);
        assert!(!app_state.enable_vm_pools);
        assert!(app_state.enable_rfq_pools);
        assert!(app_state.native_token_protocol_allowlist.is_empty());
        assert!(app_state.reset_allowance_tokens.is_empty());
        assert!(!app_state.erc4626_deposits_enabled);
    }

    #[test]
    fn build_app_state_keeps_effective_vm_for_ethereum_profile() {
        let config = build_test_config(
            ethereum_chain_profile(),
            true,
            true,
            Some("http://localhost:8545"),
        );
        let tokens = build_test_token_store(Chain::Ethereum);
        let resources = create_stream_resources(Arc::clone(&tokens));

        let app_state = build_app_state(&config, Arc::clone(&tokens), &resources);

        assert_eq!(app_state.chain, Chain::Ethereum);
        assert!(app_state.enable_vm_pools);
        assert!(app_state.enable_rfq_pools);
        assert_eq!(
            app_state.native_token_protocol_allowlist.as_ref(),
            &vec!["rocketpool".to_string()]
        );
        assert!(app_state.reset_allowance_tokens.contains_key(&1));
        assert!(app_state.erc4626_deposits_enabled);
    }
}
