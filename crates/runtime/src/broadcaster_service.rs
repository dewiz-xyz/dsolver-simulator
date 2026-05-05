use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tracing::info;
use tycho_simulation::utils::load_all_tokens;

use crate::config::{init_logging, load_broadcaster_config, BroadcasterConfig, MemoryConfig};
use crate::memory::maybe_log_memory_snapshot;
use crate::models::broadcaster::{BroadcasterSnapshotCache, BroadcasterUpstreamState};
use crate::models::stream_health::StreamHealth;
use crate::models::tokens::{TokenStore, TokenStoreError};
use crate::services::broadcaster::BroadcasterServiceState;
use crate::services::stream_builder::{build_broadcaster_stream, BroadcasterProtocols};
use crate::stream::{
    supervise_broadcaster_stream, BroadcasterStreamControls, StreamSupervisorConfig,
};
use simulator_core::broadcaster::{
    BroadcasterTokenDto, BroadcasterTokenLookupResponse, BroadcasterTokenSnapshotResponse,
};
use tycho_simulation::tycho_common::Bytes;

#[derive(Clone)]
pub struct BroadcasterAppState {
    service: BroadcasterServiceState,
    tokens: Arc<TokenStore>,
    chain_id: u64,
}

impl BroadcasterAppState {
    pub fn new(service: BroadcasterServiceState, tokens: Arc<TokenStore>, chain_id: u64) -> Self {
        Self {
            service,
            tokens,
            chain_id,
        }
    }

    pub async fn subscribe(
        &self,
    ) -> Result<Option<crate::services::broadcaster_sessions::BroadcasterSessionRegistration>> {
        self.service.subscribe().await
    }

    pub async fn status_snapshot(&self) -> crate::models::broadcaster::BroadcasterStatusSnapshot {
        self.service.status_snapshot().await
    }

    pub async fn remove_subscriber(&self, session_id: u64) {
        self.service.remove_subscriber(session_id).await;
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub async fn lookup_tokens(
        &self,
        addresses: Vec<Bytes>,
    ) -> Result<BroadcasterTokenLookupResponse, TokenStoreError> {
        let mut tokens = Vec::new();
        let mut missing = Vec::new();

        for address in addresses {
            match self.tokens.ensure(&address).await? {
                Some(token) => tokens.push(BroadcasterTokenDto::from(token)),
                None => missing.push(address),
            }
        }

        Ok(BroadcasterTokenLookupResponse { tokens, missing })
    }

    pub async fn token_snapshot(&self) -> BroadcasterTokenSnapshotResponse {
        let tokens = self
            .tokens
            .snapshot()
            .await
            .into_values()
            .map(BroadcasterTokenDto::from)
            .collect();

        BroadcasterTokenSnapshotResponse {
            chain_id: self.chain_id,
            tokens,
        }
    }
}

pub struct BroadcasterServiceParts {
    pub config: BroadcasterConfig,
    pub app_state: BroadcasterAppState,
}

pub async fn build_broadcaster_service() -> Result<BroadcasterServiceParts> {
    init_logging();

    let config = load_broadcaster_config();
    let chain = config.chain_profile.chain;
    info!(chain_id = chain.id(), chain = %chain, "Initializing Tycho broadcaster...");
    log_memory_config(config.memory);

    let tokens = load_token_store(&config).await?;
    let configured_backends = configured_backends(&config);
    let cache = BroadcasterSnapshotCache::new(chain.id(), configured_backends);
    let upstream_state = BroadcasterUpstreamState::default();
    let service = BroadcasterServiceState::new(
        config.tuning.snapshot_chunk_size,
        config.tuning.subscriber_buffer_capacity,
        cache,
        upstream_state,
    );
    let health = Arc::new(StreamHealth::new());
    let supervisor_cfg = build_supervisor_config(&config);

    spawn_broadcaster_stream_task(
        &config,
        supervisor_cfg,
        Arc::clone(&tokens),
        Arc::clone(&health),
        service.clone(),
    );
    spawn_heartbeat_task(
        service.clone(),
        Duration::from_secs(config.tuning.heartbeat_interval_secs),
    );

    Ok(BroadcasterServiceParts {
        config,
        app_state: BroadcasterAppState::new(service, tokens, chain.id()),
    })
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
    maybe_log_memory_snapshot("broadcaster", "startup", None, memory, true);
}

async fn load_token_store(config: &BroadcasterConfig) -> Result<Arc<TokenStore>> {
    let chain = config.chain_profile.chain;
    let all_tokens = load_all_tokens(
        &config.tycho_url,
        false,
        Some(&config.api_key),
        true,
        chain,
        Some(config.tuning.token_min_quality),
        None,
    )
    .await?;
    info!("Loaded {} broadcaster tokens", all_tokens.len());

    Ok(Arc::new(TokenStore::new(
        all_tokens,
        config.tycho_url.clone(),
        config.api_key.clone(),
        chain,
        Duration::from_millis(config.token_refresh_timeout_ms),
    )))
}

fn configured_backends(
    config: &BroadcasterConfig,
) -> Vec<simulator_core::broadcaster::BroadcasterBackend> {
    let mut backends = Vec::new();
    if !config.chain_profile.native_protocols.is_empty() {
        backends.push(simulator_core::broadcaster::BroadcasterBackend::Native);
    }
    if !config.chain_profile.vm_protocols.is_empty() {
        backends.push(simulator_core::broadcaster::BroadcasterBackend::Vm);
    }
    backends
}

fn build_supervisor_config(config: &BroadcasterConfig) -> StreamSupervisorConfig {
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

fn spawn_broadcaster_stream_task(
    config: &BroadcasterConfig,
    supervisor_cfg: StreamSupervisorConfig,
    tokens: Arc<TokenStore>,
    health: Arc<StreamHealth>,
    service: BroadcasterServiceState,
) {
    let chain = config.chain_profile.chain;
    let tycho_url = config.tycho_url.clone();
    let api_key = config.api_key.clone();
    let tvl_threshold = config.tvl_threshold;
    let tvl_keep_threshold = config.tvl_keep_threshold;
    let protocols = BroadcasterProtocols {
        native: config.chain_profile.native_protocols.clone(),
        vm: config.chain_profile.vm_protocols.clone(),
    };

    tokio::spawn(async move {
        info!("Starting broadcaster upstream supervisor...");
        supervise_broadcaster_stream(
            move || {
                let tokens = Arc::clone(&tokens);
                let tycho_url = tycho_url.clone();
                let api_key = api_key.clone();
                let protocols = protocols.clone();
                async move {
                    build_broadcaster_stream(
                        &tycho_url,
                        &api_key,
                        tvl_threshold,
                        tvl_keep_threshold,
                        tokens,
                        chain,
                        &protocols,
                    )
                    .await
                }
            },
            health,
            supervisor_cfg,
            BroadcasterStreamControls { service },
        )
        .await;
    });
}

fn spawn_heartbeat_task(service: BroadcasterServiceState, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if let Err(error) = service.broadcast_heartbeat().await {
                info!(error = %error, "Skipping heartbeat broadcast after runtime error");
            }
        }
    });
}
