use reqwest::{Client, StatusCode};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use simulator_core::broadcaster::BroadcasterTokenSnapshotResponse;
use tracing::{debug, info, warn};
use tycho_simulation::tycho_common::{
    models::{token::Token, Chain},
    Bytes,
};

use crate::config::{init_logging, load_config, AppConfig, MemoryConfig};
use crate::memory::maybe_log_memory_snapshot;
use crate::models::state::{AppState, BroadcasterSubscriptionStatus, StateStore, VmStreamStatus};
use crate::models::stream_health::StreamHealth;
use crate::models::tokens::{
    derive_broadcaster_token_lookup_url, derive_broadcaster_token_snapshot_url, TokenStore,
};
use crate::services::broadcaster_subscription::{
    supervise_broadcaster_subscription, BroadcasterSubscriptionControls,
    NativeBroadcasterSubscriptionControls, RfqBroadcasterSubscriptionControls,
    VmBroadcasterSubscriptionControls,
};
use crate::services::{EncodeService, QuoteService};
use crate::stream::StreamSupervisorConfig;

const TOKEN_SNAPSHOT_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(250);
const TOKEN_SNAPSHOT_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

pub struct SimulatorServiceParts {
    pub config: AppConfig,
    pub runtime: SimulatorRuntime,
}

/// Runtime-owned simulator services exposed to the RPC shell.
#[derive(Clone)]
pub struct SimulatorRuntime {
    app_state: AppState,
    quote_service: QuoteService,
    encode_service: EncodeService,
}

impl SimulatorRuntime {
    pub fn new(app_state: AppState) -> Self {
        Self {
            quote_service: QuoteService::new(app_state.clone()),
            encode_service: EncodeService::new(app_state.clone()),
            app_state,
        }
    }

    pub fn app_state(&self) -> AppState {
        self.app_state.clone()
    }

    pub fn quote_service(&self) -> QuoteService {
        self.quote_service.clone()
    }

    pub fn encode_service(&self) -> EncodeService {
        self.encode_service.clone()
    }

    pub fn request_timeout(&self) -> Duration {
        self.app_state.request_timeout()
    }
}

pub async fn build_simulator_service() -> anyhow::Result<SimulatorServiceParts> {
    init_logging();
    let config = load_config();
    let chain = config.chain_profile.chain;
    info!(chain_id = chain.id(), chain = %chain, "Initializing price service...");
    log_memory_config(config.memory);
    log_erc4626_capability(&config);
    spawn_memory_snapshot_task(config.memory);

    let tokens = load_token_store(&config).await?;
    let stream_resources = create_stream_resources(Arc::clone(&tokens));
    let app_state = build_app_state(&config, Arc::clone(&tokens), &stream_resources);
    let supervisor_cfg = build_supervisor_config(&config);

    log_rebuild_config(&config);
    spawn_broadcaster_subscription_task(&config, &supervisor_cfg, &stream_resources, &app_state);

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

async fn load_token_store(config: &AppConfig) -> anyhow::Result<Arc<TokenStore>> {
    let chain = config.chain_profile.chain;
    let lookup_url = derive_broadcaster_token_lookup_url(&config.tycho_broadcaster_ws_url)
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let snapshot_url = derive_broadcaster_token_snapshot_url(&config.tycho_broadcaster_ws_url)
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let fetch_timeout = Duration::from_millis(config.token_refresh_timeout_ms);
    let snapshot_timeout = Duration::from_millis(config.token_snapshot_timeout_ms);
    let initial_tokens = load_broadcaster_token_snapshot_with_retry(
        &Client::new(),
        &snapshot_url,
        chain,
        snapshot_timeout,
    )
    .await?;
    info!(
        %lookup_url,
        token_count = initial_tokens.len(),
        "Initialized broadcaster-backed token metadata mirror"
    );

    Ok(Arc::new(TokenStore::broadcaster_backed(
        initial_tokens,
        lookup_url,
        chain,
        fetch_timeout,
    )))
}

async fn load_broadcaster_token_snapshot_with_retry(
    client: &Client,
    snapshot_url: &str,
    chain: Chain,
    deadline: Duration,
) -> anyhow::Result<HashMap<Bytes, Token>> {
    let started_at = Instant::now();
    let mut attempt = 1;
    let mut next_delay = TOKEN_SNAPSHOT_RETRY_INITIAL_DELAY;

    loop {
        let elapsed = started_at.elapsed();
        let remaining = deadline
            .checked_sub(elapsed)
            .ok_or_else(|| anyhow::anyhow!("Timed out loading broadcaster token snapshot"))?;

        match load_broadcaster_token_snapshot(client, snapshot_url, chain, remaining).await {
            Ok(tokens) => return Ok(tokens),
            Err(error) if error.is_retryable() => {
                let elapsed = started_at.elapsed();
                let Some(remaining) = deadline.checked_sub(elapsed) else {
                    return Err(error.into_anyhow(snapshot_url));
                };
                let delay = next_delay.min(remaining);
                warn!(
                    %snapshot_url,
                    attempt,
                    elapsed_ms = elapsed.as_millis() as u64,
                    next_delay_ms = delay.as_millis() as u64,
                    error = %error,
                    "Retrying broadcaster token snapshot load"
                );
                tokio::time::sleep(delay).await;
                next_delay = (next_delay * 2).min(TOKEN_SNAPSHOT_RETRY_MAX_DELAY);
                attempt += 1;
            }
            Err(error) => return Err(error.into_anyhow(snapshot_url)),
        }
    }
}

#[derive(Debug)]
enum TokenSnapshotLoadError {
    Request(reqwest::Error),
    Body(reqwest::Error),
    Json(serde_json::Error),
    Status(StatusCode),
    InvalidChain { actual: u64, expected: u64 },
    InvalidToken(anyhow::Error),
}

impl TokenSnapshotLoadError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::Request(_) | Self::Body(_) => true,
            Self::Status(status) => {
                status.is_server_error() || *status == StatusCode::TOO_MANY_REQUESTS
            }
            Self::Json(_) | Self::InvalidChain { .. } | Self::InvalidToken(_) => false,
        }
    }

    fn into_anyhow(self, snapshot_url: &str) -> anyhow::Error {
        match self {
            Self::Request(error) => anyhow::anyhow!(
                "Failed to fetch broadcaster token snapshot from {snapshot_url}: {error}"
            ),
            Self::Body(error) => anyhow::anyhow!(
                "Failed to read broadcaster token snapshot from {snapshot_url}: {error}"
            ),
            Self::Json(error) => anyhow::anyhow!(
                "Failed to decode broadcaster token snapshot from {snapshot_url}: {error}"
            ),
            Self::Status(status) => anyhow::anyhow!(
                "Failed to fetch broadcaster token snapshot from {snapshot_url}: HTTP {status}"
            ),
            Self::InvalidChain { actual, expected } => anyhow::anyhow!(
                "broadcaster token snapshot chain_id {actual} does not match simulator chain_id {expected}"
            ),
            Self::InvalidToken(error) => {
                anyhow::anyhow!("Failed to parse broadcaster token snapshot: {error}")
            }
        }
    }
}

impl std::fmt::Display for TokenSnapshotLoadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request(error) | Self::Body(error) => write!(formatter, "{error}"),
            Self::Json(error) => write!(formatter, "{error}"),
            Self::Status(status) => write!(formatter, "HTTP {status}"),
            Self::InvalidChain { actual, expected } => write!(
                formatter,
                "chain_id {actual} does not match simulator chain_id {expected}"
            ),
            Self::InvalidToken(error) => write!(formatter, "{error}"),
        }
    }
}

async fn load_broadcaster_token_snapshot(
    client: &Client,
    snapshot_url: &str,
    chain: Chain,
    fetch_timeout: Duration,
) -> Result<HashMap<Bytes, Token>, TokenSnapshotLoadError> {
    let response = client
        .get(snapshot_url)
        .timeout(fetch_timeout)
        .send()
        .await
        .map_err(TokenSnapshotLoadError::Request)?;
    let status = response.status();
    if !status.is_success() {
        return Err(TokenSnapshotLoadError::Status(status));
    }
    let body = response
        .bytes()
        .await
        .map_err(TokenSnapshotLoadError::Body)?;
    let response = serde_json::from_slice::<BroadcasterTokenSnapshotResponse>(&body)
        .map_err(TokenSnapshotLoadError::Json)?;

    if response.chain_id != chain.id() {
        return Err(TokenSnapshotLoadError::InvalidChain {
            actual: response.chain_id,
            expected: chain.id(),
        });
    }

    response
        .tokens
        .into_iter()
        .map(|token| {
            token
                .into_token(chain)
                .map(|token| (token.address.clone(), token))
        })
        .collect::<Result<_, _>>()
        .map_err(|error| TokenSnapshotLoadError::InvalidToken(anyhow::anyhow!("{error}")))
}

fn create_stream_resources(tokens: Arc<TokenStore>) -> StreamResources {
    let native_state_store = Arc::new(StateStore::new(Arc::clone(&tokens)));
    let vm_state_store = Arc::new(StateStore::new(Arc::clone(&tokens)));
    let rfq_state_store = Arc::new(StateStore::new(tokens));
    let native_stream_health = Arc::new(StreamHealth::new());
    let vm_stream_health = Arc::new(StreamHealth::new());
    let rfq_stream_health = Arc::new(StreamHealth::new());
    let vm_stream = Arc::new(tokio::sync::RwLock::new(VmStreamStatus::default()));
    debug!("Created shared state");

    StreamResources {
        native_state_store,
        vm_state_store,
        rfq_state_store,
        native_stream_health,
        vm_stream_health,
        rfq_stream_health,
        vm_stream,
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
        rfq_broadcaster_subscription: BroadcasterSubscriptionStatus::default(),
        native_state_store: Arc::clone(&resources.native_state_store),
        vm_state_store: Arc::clone(&resources.vm_state_store),
        rfq_state_store: Arc::clone(&resources.rfq_state_store),
        native_stream_health: Arc::clone(&resources.native_stream_health),
        vm_stream_health: Arc::clone(&resources.vm_stream_health),
        rfq_stream_health: Arc::clone(&resources.rfq_stream_health),
        vm_stream: Arc::clone(&resources.vm_stream),
        configured_backends: crate::models::state::ConfiguredBackends {
            vm: configured_vm_pools,
            rfq: configured_rfq_pools,
        },
        enable_vm_pools: effective_vm_enabled,
        enable_rfq_pools: effective_rfq_enabled,
        readiness_stale,
        request_timeout,
        vm_simulation_rebuild_gate: Arc::new(tokio::sync::RwLock::new(())),
        rfq_simulation_rebuild_gate: Arc::new(tokio::sync::RwLock::new(())),
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
    for backend in broadcaster_subscription_plan(app_state) {
        spawn_broadcaster_subscription_backend(
            backend,
            config,
            supervisor_cfg,
            resources,
            app_state,
        );
    }
}

#[cfg(test)]
fn enabled_broadcaster_subscription_backends(app_state: &AppState) -> Vec<&'static str> {
    broadcaster_subscription_plan(app_state)
        .into_iter()
        .map(BroadcasterSubscriptionBackend::label)
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BroadcasterSubscriptionBackend {
    Native,
    Vm,
    Rfq,
}

impl BroadcasterSubscriptionBackend {
    #[cfg(test)]
    const fn label(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Vm => "vm",
            Self::Rfq => "rfq",
        }
    }
}

fn broadcaster_subscription_plan(app_state: &AppState) -> Vec<BroadcasterSubscriptionBackend> {
    let mut backends = vec![BroadcasterSubscriptionBackend::Native];
    if app_state.enable_vm_pools {
        backends.push(BroadcasterSubscriptionBackend::Vm);
    }
    if app_state.enable_rfq_pools {
        backends.push(BroadcasterSubscriptionBackend::Rfq);
    }
    backends
}

fn spawn_broadcaster_subscription_backend(
    backend: BroadcasterSubscriptionBackend,
    config: &AppConfig,
    supervisor_cfg: &StreamSupervisorConfig,
    resources: &StreamResources,
    app_state: &AppState,
) {
    match backend {
        BroadcasterSubscriptionBackend::Native => {
            spawn_native_broadcaster_subscription_task(
                config,
                supervisor_cfg,
                resources,
                app_state,
            );
        }
        BroadcasterSubscriptionBackend::Vm => {
            spawn_vm_broadcaster_subscription_task(config, supervisor_cfg, resources, app_state);
        }
        BroadcasterSubscriptionBackend::Rfq => {
            spawn_rfq_broadcaster_subscription_task(config, supervisor_cfg, resources, app_state);
        }
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
        tokens: Arc::clone(&app_state.tokens),
        protocols: config.chain_profile.native_protocols.clone(),
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
        tokens: Arc::clone(&app_state.tokens),
        protocols: config.chain_profile.vm_protocols.clone(),
        vm_stream: Arc::clone(&resources.vm_stream),
        simulation_rebuild_gate: app_state.vm_simulation_rebuild_gate(),
    });
    spawn_backend_broadcaster_subscription_task(
        "vm",
        config.tycho_broadcaster_ws_url.clone(),
        config.chain_profile.chain.id(),
        supervisor_cfg.clone(),
        controls,
    );
}

fn spawn_rfq_broadcaster_subscription_task(
    config: &AppConfig,
    supervisor_cfg: &StreamSupervisorConfig,
    resources: &StreamResources,
    app_state: &AppState,
) {
    let controls = BroadcasterSubscriptionControls::Rfq(RfqBroadcasterSubscriptionControls {
        broadcaster_subscription: app_state.rfq_broadcaster_subscription.clone(),
        state_store: Arc::clone(&resources.rfq_state_store),
        stream_health: Arc::clone(&resources.rfq_stream_health),
        tokens: Arc::clone(&app_state.tokens),
        protocols: config.chain_profile.rfq_protocols.clone(),
        simulation_rebuild_gate: app_state.rfq_simulation_rebuild_gate(),
    });
    spawn_backend_broadcaster_subscription_task(
        "rfq",
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

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use crate::config::{AppConfig, ChainProfile, MemoryConfig, SlippageConfig};
    use crate::models::tokens::TokenStore;
    use anyhow::{anyhow, Result};
    use simulator_core::broadcaster::{
        BroadcasterTokenDto, BroadcasterTokenLookupResponse, BroadcasterTokenSnapshotResponse,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;
    use tycho_simulation::tycho_common::{models::token::Token, models::Chain, Bytes};

    use super::{
        broadcaster_subscription_plan, build_app_state, create_stream_resources,
        enabled_broadcaster_subscription_backends, load_token_store,
        BroadcasterSubscriptionBackend,
    };

    struct TokenAuthority {
        ws_url: String,
        lookup_hits: Arc<AtomicUsize>,
        snapshot_hits: Arc<AtomicUsize>,
        tycho_hits: Arc<AtomicUsize>,
        task: JoinHandle<Result<()>>,
    }

    #[derive(Default)]
    struct TokenAuthorityOptions {
        snapshot_delay: Duration,
        snapshot_failures: usize,
        truncated_snapshot_responses: usize,
        snapshot_chain: Option<Chain>,
    }

    struct TokenAuthorityState {
        tokens: Arc<Vec<Token>>,
        lookup_hits: Arc<AtomicUsize>,
        snapshot_hits: Arc<AtomicUsize>,
        tycho_hits: Arc<AtomicUsize>,
        snapshot_failures: Arc<AtomicUsize>,
        truncated_snapshot_responses: Arc<AtomicUsize>,
        snapshot_delay: Duration,
        snapshot_chain: Option<Chain>,
    }

    impl Drop for TokenAuthority {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl TokenAuthority {
        async fn spawn(tokens: Vec<Token>) -> Result<Self> {
            Self::spawn_with_options(tokens, TokenAuthorityOptions::default()).await
        }

        async fn spawn_with_options(
            tokens: Vec<Token>,
            options: TokenAuthorityOptions,
        ) -> Result<Self> {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let ws_url = format!("ws://{}/ws", listener.local_addr()?);
            let lookup_hits = Arc::new(AtomicUsize::new(0));
            let snapshot_hits = Arc::new(AtomicUsize::new(0));
            let tycho_hits = Arc::new(AtomicUsize::new(0));
            let state = Arc::new(TokenAuthorityState {
                tokens: Arc::new(tokens),
                lookup_hits: Arc::clone(&lookup_hits),
                snapshot_hits: Arc::clone(&snapshot_hits),
                tycho_hits: Arc::clone(&tycho_hits),
                snapshot_failures: Arc::new(AtomicUsize::new(options.snapshot_failures)),
                truncated_snapshot_responses: Arc::new(AtomicUsize::new(
                    options.truncated_snapshot_responses,
                )),
                snapshot_delay: options.snapshot_delay,
                snapshot_chain: options.snapshot_chain,
            });
            let task = tokio::spawn(async move {
                loop {
                    let (stream, _) = listener.accept().await?;
                    let state = Arc::clone(&state);
                    tokio::spawn(async move {
                        let _ = handle_token_authority_request(stream, state).await;
                    });
                }
            });

            Ok(Self {
                ws_url,
                lookup_hits,
                snapshot_hits,
                tycho_hits,
                task,
            })
        }
    }

    async fn write_http_response(stream: &mut TcpStream, status: &str, body: &[u8]) -> Result<()> {
        write_http_response_with_content_length(stream, status, body, body.len()).await
    }

    async fn write_http_response_with_content_length(
        stream: &mut TcpStream,
        status: &str,
        body: &[u8],
        content_length: usize,
    ) -> Result<()> {
        let headers = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {content_length}\r\nconnection: close\r\n\r\n"
        );
        stream.write_all(headers.as_bytes()).await?;
        stream.write_all(body).await?;
        Ok(())
    }

    async fn handle_token_authority_request(
        mut stream: TcpStream,
        state: Arc<TokenAuthorityState>,
    ) -> Result<()> {
        let mut buffer = vec![0_u8; 4096];
        let read = stream.read(&mut buffer).await?;
        let request = String::from_utf8_lossy(&buffer[..read]);
        let path = request
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .ok_or_else(|| anyhow!("token authority request line missing path"))?;

        let (status, body) = match path {
            "/tokens/lookup" => {
                state.lookup_hits.fetch_add(1, Ordering::SeqCst);
                let response = BroadcasterTokenLookupResponse {
                    tokens: state
                        .tokens
                        .iter()
                        .cloned()
                        .map(BroadcasterTokenDto::from)
                        .collect(),
                    missing: Vec::new(),
                };
                ("200 OK", serde_json::to_vec(&response)?)
            }
            "/tokens/snapshot" => {
                state.snapshot_hits.fetch_add(1, Ordering::SeqCst);
                if !state.snapshot_delay.is_zero() {
                    tokio::time::sleep(state.snapshot_delay).await;
                }
                if state
                    .snapshot_failures
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
                {
                    return write_http_response(
                        &mut stream,
                        "503 Service Unavailable",
                        b"snapshot unavailable",
                    )
                    .await;
                }
                if state
                    .truncated_snapshot_responses
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
                {
                    return write_http_response_with_content_length(
                        &mut stream,
                        "200 OK",
                        b"{\"chainId\":1,\"tokens\":[",
                        1024,
                    )
                    .await;
                }
                let response = BroadcasterTokenSnapshotResponse {
                    chain_id: state
                        .snapshot_chain
                        .or_else(|| state.tokens.first().map(|token| token.chain))
                        .unwrap_or(Chain::Ethereum)
                        .id(),
                    tokens: state
                        .tokens
                        .iter()
                        .cloned()
                        .map(BroadcasterTokenDto::from)
                        .collect(),
                };
                ("200 OK", serde_json::to_vec(&response)?)
            }
            "/v1/tokens" => {
                state.tycho_hits.fetch_add(1, Ordering::SeqCst);
                (
                    "500 Internal Server Error",
                    b"tycho fetch forbidden".to_vec(),
                )
            }
            _ => ("404 Not Found", b"not found".to_vec()),
        };

        write_http_response(&mut stream, status, &body).await
    }

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
            token_snapshot_timeout_ms: 125_000,
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

    fn test_token(address: &Bytes, chain: Chain) -> Token {
        Token::new(address, "LOOKUP", 18, 0, &[], chain, 100)
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

    #[tokio::test]
    async fn load_token_store_preloads_broadcaster_token_snapshot() -> Result<()> {
        let token_address = Bytes::from([9_u8; 20]);
        let authority =
            TokenAuthority::spawn(vec![test_token(&token_address, Chain::Ethereum)]).await?;
        let mut config = build_test_config(ethereum_chain_profile(), false, false, None);
        config.tycho_url = authority
            .ws_url
            .replace("ws://", "http://")
            .trim_end_matches("/ws")
            .to_string();
        config.tycho_broadcaster_ws_url = authority.ws_url.clone();

        let store = load_token_store(&config).await?;
        let resolved = store.ensure(&token_address).await?;

        assert!(resolved.is_some());
        assert_eq!(
            authority.snapshot_hits.load(Ordering::SeqCst),
            1,
            "production simulator token store should preload the broadcaster snapshot"
        );
        assert_eq!(
            authority.lookup_hits.load(Ordering::SeqCst),
            0,
            "snapshot-seeded token lookups should hit the local mirror"
        );
        assert_eq!(
            authority.tycho_hits.load(Ordering::SeqCst),
            0,
            "production simulator token store must not fetch token metadata from Tycho"
        );
        Ok(())
    }

    #[tokio::test]
    async fn load_token_store_uses_broadcaster_lookup_for_snapshot_misses() -> Result<()> {
        let cached_address = Bytes::from([9_u8; 20]);
        let missed_address = Bytes::from([10_u8; 20]);
        let authority =
            TokenAuthority::spawn(vec![test_token(&cached_address, Chain::Ethereum)]).await?;
        let mut config = build_test_config(ethereum_chain_profile(), false, false, None);
        config.tycho_url = authority
            .ws_url
            .replace("ws://", "http://")
            .trim_end_matches("/ws")
            .to_string();
        config.tycho_broadcaster_ws_url = authority.ws_url.clone();

        let store = load_token_store(&config).await?;
        let resolved = store.ensure(&missed_address).await?;

        assert!(resolved.is_none());
        assert_eq!(authority.snapshot_hits.load(Ordering::SeqCst), 1);
        assert_eq!(
            authority.lookup_hits.load(Ordering::SeqCst),
            1,
            "tokens absent from the startup snapshot should use broadcaster lookup"
        );
        assert_eq!(
            authority.tycho_hits.load(Ordering::SeqCst),
            0,
            "production simulator token store must not fetch token metadata from Tycho"
        );
        Ok(())
    }

    #[tokio::test]
    async fn load_token_store_uses_snapshot_timeout_for_startup_snapshot() -> Result<()> {
        let token_address = Bytes::from([9_u8; 20]);
        let authority = TokenAuthority::spawn_with_options(
            vec![test_token(&token_address, Chain::Ethereum)],
            TokenAuthorityOptions {
                snapshot_delay: Duration::from_millis(20),
                ..TokenAuthorityOptions::default()
            },
        )
        .await?;
        let mut config = build_test_config(ethereum_chain_profile(), false, false, None);
        config.tycho_broadcaster_ws_url = authority.ws_url.clone();
        config.token_refresh_timeout_ms = 1;
        config.token_snapshot_timeout_ms = 500;

        let store = load_token_store(&config).await?;
        let resolved = store.ensure(&token_address).await?;

        assert!(resolved.is_some());
        assert_eq!(authority.snapshot_hits.load(Ordering::SeqCst), 1);
        assert_eq!(
            authority.lookup_hits.load(Ordering::SeqCst),
            0,
            "startup snapshot must not inherit the single-token refresh timeout"
        );
        Ok(())
    }

    #[tokio::test]
    async fn load_token_store_retries_retryable_snapshot_failures() -> Result<()> {
        let token_address = Bytes::from([9_u8; 20]);
        let authority = TokenAuthority::spawn_with_options(
            vec![test_token(&token_address, Chain::Ethereum)],
            TokenAuthorityOptions {
                snapshot_failures: 1,
                ..TokenAuthorityOptions::default()
            },
        )
        .await?;
        let mut config = build_test_config(ethereum_chain_profile(), false, false, None);
        config.tycho_broadcaster_ws_url = authority.ws_url.clone();
        config.token_snapshot_timeout_ms = 1_000;

        let store = load_token_store(&config).await?;
        let resolved = store.ensure(&token_address).await?;

        assert!(resolved.is_some());
        assert_eq!(
            authority.snapshot_hits.load(Ordering::SeqCst),
            2,
            "a transient broadcaster snapshot failure should be retried"
        );
        Ok(())
    }

    #[tokio::test]
    async fn load_token_store_retries_truncated_snapshot_bodies() -> Result<()> {
        let token_address = Bytes::from([9_u8; 20]);
        let authority = TokenAuthority::spawn_with_options(
            vec![test_token(&token_address, Chain::Ethereum)],
            TokenAuthorityOptions {
                truncated_snapshot_responses: 1,
                ..TokenAuthorityOptions::default()
            },
        )
        .await?;
        let mut config = build_test_config(ethereum_chain_profile(), false, false, None);
        config.tycho_broadcaster_ws_url = authority.ws_url.clone();
        config.token_snapshot_timeout_ms = 1_000;

        let store = load_token_store(&config).await?;
        let resolved = store.ensure(&token_address).await?;

        assert!(resolved.is_some());
        assert_eq!(
            authority.snapshot_hits.load(Ordering::SeqCst),
            2,
            "a truncated snapshot body should be retried before startup fails"
        );
        Ok(())
    }

    #[tokio::test]
    async fn load_token_store_does_not_retry_snapshot_chain_mismatch() -> Result<()> {
        let token_address = Bytes::from([9_u8; 20]);
        let authority = TokenAuthority::spawn_with_options(
            vec![test_token(&token_address, Chain::Ethereum)],
            TokenAuthorityOptions {
                snapshot_chain: Some(Chain::Base),
                ..TokenAuthorityOptions::default()
            },
        )
        .await?;
        let mut config = build_test_config(ethereum_chain_profile(), false, false, None);
        config.tycho_broadcaster_ws_url = authority.ws_url.clone();

        let Err(error) = load_token_store(&config).await else {
            anyhow::bail!("chain mismatch should fail startup");
        };

        assert!(error.to_string().contains("chain_id"));
        assert_eq!(authority.snapshot_hits.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn load_token_store_enforces_snapshot_startup_deadline() -> Result<()> {
        let token_address = Bytes::from([9_u8; 20]);
        let authority = TokenAuthority::spawn_with_options(
            vec![test_token(&token_address, Chain::Ethereum)],
            TokenAuthorityOptions {
                snapshot_failures: usize::MAX,
                ..TokenAuthorityOptions::default()
            },
        )
        .await?;
        let mut config = build_test_config(ethereum_chain_profile(), false, false, None);
        config.tycho_broadcaster_ws_url = authority.ws_url.clone();
        config.token_snapshot_timeout_ms = 20;

        let Err(error) = load_token_store(&config).await else {
            anyhow::bail!("snapshot startup deadline should fail when retries never succeed");
        };

        assert!(error.to_string().contains("Timed out"));
        assert!(authority.snapshot_hits.load(Ordering::SeqCst) >= 1);
        Ok(())
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
    fn enabled_broadcaster_subscription_backends_include_rfq_when_effective() {
        let config = build_test_config(base_chain_profile(), true, true, None);
        let tokens = build_test_token_store(Chain::Base);
        let resources = create_stream_resources(Arc::clone(&tokens));
        let app_state = build_app_state(&config, Arc::clone(&tokens), &resources);

        assert_eq!(
            broadcaster_subscription_plan(&app_state),
            vec![
                BroadcasterSubscriptionBackend::Native,
                BroadcasterSubscriptionBackend::Rfq
            ]
        );
        assert_eq!(
            enabled_broadcaster_subscription_backends(&app_state),
            vec!["native", "rfq"]
        );
    }

    #[test]
    fn simulator_startup_source_has_no_direct_rfq_provider_stream_path() {
        let source = include_str!("simulator_service.rs");
        let forbidden_needles = [
            ["spawn", "rfq", "stream", "task"].join("_"),
            ["supervise", "rfq", "stream"].join("_"),
            ["build", "rfq", "stream"].join("_"),
            ["RFQ", "Config"].join(""),
            ["RFQ", "TokenStores"].join(""),
        ];

        for needle in forbidden_needles {
            assert!(
                !source.contains(&needle),
                "simulator startup must not reference direct RFQ provider stream API `{needle}`"
            );
        }
    }

    #[tokio::test]
    async fn build_app_state_initializes_rfq_broadcaster_subscription_status() {
        let config = build_test_config(base_chain_profile(), false, true, None);
        let tokens = build_test_token_store(Chain::Base);
        let resources = create_stream_resources(Arc::clone(&tokens));
        let app_state = build_app_state(&config, Arc::clone(&tokens), &resources);

        let snapshot = app_state.rfq_broadcaster_subscription.snapshot().await;
        assert!(!snapshot.connected);
        assert!(!snapshot.bootstrap_complete);
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
