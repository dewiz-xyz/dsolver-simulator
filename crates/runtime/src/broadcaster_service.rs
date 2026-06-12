use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tracing::info;
use tycho_simulation::utils::load_all_tokens;

use crate::broadcaster::state::{BroadcasterSnapshotCache, BroadcasterUpstreamState};
use crate::config::{init_logging, load_broadcaster_config, BroadcasterConfig, MemoryConfig};
use crate::memory::maybe_log_memory_snapshot;
use crate::models::stream_health::StreamHealth;
use crate::models::tokens::{TokenStore, TokenStoreError};
use crate::services::broadcaster::BroadcasterServiceState;
use crate::services::rfq_tokens::{load_rfq_token_stores, RfqTokenStoreConfig};
use crate::services::stream_builder::{
    build_broadcaster_raw_stream, build_rfq_stream, BroadcasterProtocols, RFQConfig, RFQTokenStores,
};
use crate::stream::{
    supervise_broadcaster_raw_stream, supervise_broadcaster_stream, BroadcasterStreamControls,
    StreamSupervisorConfig,
};
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterEnvelope, BroadcasterSnapshotSessionResponse,
    BroadcasterTokenDto, BroadcasterTokenLookupResponse, BroadcasterTokenSnapshotResponse,
};
use tycho_simulation::tycho_common::Bytes;

#[derive(Clone)]
pub struct BroadcasterAppState {
    raw_service: BroadcasterServiceState,
    rfq_service: Option<BroadcasterServiceState>,
    tokens: Arc<TokenStore>,
    chain_id: u64,
    snapshot_session_ttl: Duration,
}

impl BroadcasterAppState {
    pub fn new(service: BroadcasterServiceState, tokens: Arc<TokenStore>, chain_id: u64) -> Self {
        Self::with_snapshot_session_ttl(service, tokens, chain_id, Duration::from_secs(300))
    }

    pub fn with_snapshot_session_ttl(
        service: BroadcasterServiceState,
        tokens: Arc<TokenStore>,
        chain_id: u64,
        snapshot_session_ttl: Duration,
    ) -> Self {
        Self {
            raw_service: service,
            rfq_service: None,
            tokens,
            chain_id,
            snapshot_session_ttl,
        }
    }

    pub fn with_rfq_snapshot_session_ttl(
        raw_service: BroadcasterServiceState,
        rfq_service: BroadcasterServiceState,
        tokens: Arc<TokenStore>,
        chain_id: u64,
        snapshot_session_ttl: Duration,
    ) -> Self {
        Self {
            raw_service,
            rfq_service: Some(rfq_service),
            tokens,
            chain_id,
            snapshot_session_ttl,
        }
    }

    pub async fn create_snapshot_session(
        &self,
    ) -> Result<Option<BroadcasterSnapshotSessionResponse>> {
        self.raw_service
            .create_snapshot_session(self.snapshot_session_ttl)
            .await
    }

    pub async fn create_rfq_snapshot_session(
        &self,
    ) -> Result<Option<BroadcasterSnapshotSessionResponse>> {
        let Some(service) = &self.rfq_service else {
            return Ok(None);
        };
        service
            .create_snapshot_session(self.snapshot_session_ttl)
            .await
    }

    pub async fn snapshot_session_payload(
        &self,
        session_id: u64,
        index: u32,
    ) -> Result<BroadcasterEnvelope, crate::services::broadcaster_sessions::SnapshotSessionError>
    {
        self.raw_service
            .snapshot_session_payload(session_id, index)
            .await
    }

    pub async fn rfq_snapshot_session_payload(
        &self,
        session_id: u64,
        index: u32,
    ) -> Result<BroadcasterEnvelope, crate::services::broadcaster_sessions::SnapshotSessionError>
    {
        let Some(service) = &self.rfq_service else {
            return Err(crate::services::broadcaster_sessions::SnapshotSessionError::NotFound);
        };
        service.snapshot_session_payload(session_id, index).await
    }

    pub async fn attach_snapshot_session(
        &self,
        session_id: u64,
    ) -> Result<
        crate::services::broadcaster_sessions::BroadcasterAttachedSession,
        crate::services::broadcaster_sessions::SnapshotSessionError,
    > {
        self.raw_service.attach_snapshot_session(session_id).await
    }

    pub async fn attach_rfq_snapshot_session(
        &self,
        session_id: u64,
    ) -> Result<
        crate::services::broadcaster_sessions::BroadcasterAttachedSession,
        crate::services::broadcaster_sessions::SnapshotSessionError,
    > {
        let Some(service) = &self.rfq_service else {
            return Err(crate::services::broadcaster_sessions::SnapshotSessionError::NotFound);
        };
        service.attach_snapshot_session(session_id).await
    }

    pub async fn status_snapshot(&self) -> crate::broadcaster::state::BroadcasterStatusSnapshot {
        self.raw_service.status_snapshot().await
    }

    pub async fn rfq_status_snapshot(
        &self,
    ) -> Option<crate::broadcaster::state::BroadcasterStatusSnapshot> {
        let service = self.rfq_service.as_ref()?;
        Some(service.status_snapshot().await)
    }

    pub async fn remove_subscriber(&self, session_id: u64) {
        self.raw_service.remove_subscriber(session_id).await;
    }

    pub async fn remove_rfq_subscriber(&self, session_id: u64) {
        if let Some(service) = &self.rfq_service {
            service.remove_subscriber(session_id).await;
        }
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
    let raw_cache = BroadcasterSnapshotCache::new(chain.id(), raw_configured_backends(&config));
    let raw_upstream_state = BroadcasterUpstreamState::default();
    let raw_service = BroadcasterServiceState::new(
        config.tuning.snapshot_max_payload_bytes,
        config.tuning.subscriber_buffer_capacity,
        raw_cache,
        raw_upstream_state,
    );
    let raw_health = Arc::new(StreamHealth::new());
    let supervisor_cfg = build_supervisor_config(&config);

    spawn_broadcaster_stream_task(
        &config,
        supervisor_cfg.clone(),
        Arc::clone(&raw_health),
        raw_service.clone(),
    );
    let rfq_service = if let Some(rfq_backends) = rfq_configured_backends(&config) {
        let rfq_cache = BroadcasterSnapshotCache::new(chain.id(), rfq_backends);
        let rfq_upstream_state = BroadcasterUpstreamState::default();
        let service = BroadcasterServiceState::new(
            config.tuning.snapshot_max_payload_bytes,
            config.tuning.subscriber_buffer_capacity,
            rfq_cache,
            rfq_upstream_state,
        );
        let rfq_token_stores = load_rfq_token_stores(RfqTokenStoreConfig {
            tokens: Arc::clone(&tokens),
            chain,
            token_refresh_timeout: Duration::from_millis(config.token_refresh_timeout_ms),
            protocols: &config.chain_profile.rfq_protocols,
            bebop_url: &config.bebop_url,
            hashflow_filename: &config.hashflow_filename,
            liquorice_url: config.liquorice_url.as_deref(),
            liquorice_user: &config.liquorice_user,
            liquorice_key: &config.liquorice_key,
        })
        .await?;
        spawn_broadcaster_rfq_stream_task(
            &config,
            supervisor_cfg.clone(),
            service.clone(),
            rfq_token_stores,
        );
        spawn_heartbeat_task(
            service.clone(),
            Duration::from_secs(config.tuning.heartbeat_interval_secs),
        );
        Some(service)
    } else {
        None
    };
    spawn_heartbeat_task(
        raw_service.clone(),
        Duration::from_secs(config.tuning.heartbeat_interval_secs),
    );
    let snapshot_session_ttl = Duration::from_secs(config.tuning.snapshot_session_ttl_secs);
    let app_state = match rfq_service {
        Some(rfq_service) => BroadcasterAppState::with_rfq_snapshot_session_ttl(
            raw_service,
            rfq_service,
            tokens,
            chain.id(),
            snapshot_session_ttl,
        ),
        None => BroadcasterAppState::with_snapshot_session_ttl(
            raw_service,
            tokens,
            chain.id(),
            snapshot_session_ttl,
        ),
    };

    Ok(BroadcasterServiceParts { config, app_state })
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

fn raw_configured_backends(config: &BroadcasterConfig) -> Vec<BroadcasterBackend> {
    let mut backends = Vec::new();
    if !config.chain_profile.native_protocols.is_empty() {
        backends.push(BroadcasterBackend::Native);
    }
    if !config.chain_profile.vm_protocols.is_empty() {
        backends.push(BroadcasterBackend::Vm);
    }
    backends
}

fn rfq_configured_backends(config: &BroadcasterConfig) -> Option<Vec<BroadcasterBackend>> {
    effective_rfq_enabled(config).then_some(vec![BroadcasterBackend::Rfq])
}

fn effective_rfq_enabled(config: &BroadcasterConfig) -> bool {
    config.enable_rfq_pools && !config.chain_profile.rfq_protocols.is_empty()
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
        supervise_broadcaster_raw_stream(
            move || {
                let tycho_url = tycho_url.clone();
                let api_key = api_key.clone();
                let protocols = protocols.clone();
                async move {
                    build_broadcaster_raw_stream(
                        &tycho_url,
                        &api_key,
                        tvl_threshold,
                        tvl_keep_threshold,
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

fn spawn_broadcaster_rfq_stream_task(
    config: &BroadcasterConfig,
    supervisor_cfg: StreamSupervisorConfig,
    service: BroadcasterServiceState,
    token_stores: RFQTokenStores,
) {
    let chain = config.chain_profile.chain;
    let tvl_threshold = config.tvl_threshold;
    let protocols = config.chain_profile.rfq_protocols.clone();
    let rfq_config = RFQConfig {
        bebop_user: config.bebop_user.clone(),
        bebop_key: config.bebop_key.clone(),
        hashflow_user: config.hashflow_user.clone(),
        hashflow_key: config.hashflow_key.clone(),
        liquorice_user: config.liquorice_user.clone(),
        liquorice_key: config.liquorice_key.clone(),
    };
    let health = Arc::new(StreamHealth::new());

    tokio::spawn(async move {
        info!("Starting broadcaster RFQ stream supervisor...");
        supervise_broadcaster_stream(
            move || {
                let token_stores = token_stores.clone();
                let protocols = protocols.clone();
                let rfq_config = rfq_config.clone();
                async move {
                    build_rfq_stream(tvl_threshold, token_stores, chain, &protocols, rfq_config)
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

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::net::{IpAddr, Ipv4Addr};

    use simulator_core::broadcaster::BroadcasterBackend;
    use tycho_simulation::tycho_common::{models::Chain, Bytes};

    use super::{raw_configured_backends, rfq_configured_backends};
    use crate::config::{BroadcasterConfig, BroadcasterTuning, ChainProfile, MemoryConfig};

    fn test_config() -> BroadcasterConfig {
        BroadcasterConfig {
            chain_profile: ChainProfile {
                chain: Chain::Ethereum,
                native_protocols: vec!["uniswap_v2".to_string()],
                vm_protocols: Vec::new(),
                rfq_protocols: vec!["rfq:bebop".to_string()],
                native_token_protocol_allowlist: Vec::new(),
                reset_allowance_tokens: HashMap::<u64, HashSet<Bytes>>::new(),
                erc4626_pair_policies: Vec::new(),
            },
            tycho_url: "http://localhost:4242".to_string(),
            bebop_url: "https://example.com/bebop".to_string(),
            hashflow_filename: "./hashflow.csv".to_string(),
            liquorice_url: None,
            api_key: "test-api-key".to_string(),
            tvl_threshold: 100.0,
            tvl_keep_threshold: 20.0,
            port: 3001,
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            enable_rfq_pools: true,
            token_refresh_timeout_ms: 1_000,
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
            memory: MemoryConfig {
                purge_enabled: true,
                snapshots_enabled: false,
                snapshots_min_interval_secs: 60,
                snapshots_min_new_pairs: 1_000,
                snapshots_emit_emf: false,
            },
            tuning: BroadcasterTuning {
                snapshot_max_payload_bytes: 8_388_608,
                subscriber_buffer_capacity: 128,
                heartbeat_interval_secs: 5,
                token_min_quality: 0,
                snapshot_session_ttl_secs: 300,
            },
            bebop_user: "bebop-user".to_string(),
            bebop_key: "bebop-key".to_string(),
            hashflow_user: String::new(),
            hashflow_key: String::new(),
            liquorice_user: String::new(),
            liquorice_key: String::new(),
        }
    }

    #[test]
    fn raw_configured_backends_omit_rfq_when_effective() {
        let config = test_config();

        assert_eq!(
            raw_configured_backends(&config),
            vec![BroadcasterBackend::Native]
        );
    }

    #[test]
    fn rfq_configured_backends_include_only_rfq_when_effective() {
        let config = test_config();

        assert_eq!(
            rfq_configured_backends(&config),
            Some(vec![BroadcasterBackend::Rfq])
        );
    }

    #[test]
    fn rfq_configured_backends_are_absent_when_disabled() {
        let mut config = test_config();
        config.enable_rfq_pools = false;

        assert_eq!(rfq_configured_backends(&config), None);
    }
}
