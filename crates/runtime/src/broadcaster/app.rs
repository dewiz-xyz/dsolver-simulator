use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tycho_simulation::utils::load_all_tokens;

use state_history::{
    S3CheckpointStore, StateHistoryCheckpointWriter, StateHistoryPgStore, StateHistoryWriter,
};

use crate::broadcaster::redis_publisher::{
    BroadcasterRedisPublisher, BroadcasterRedisPublisherConfig, TokioRedisStreamWriter,
};
use crate::broadcaster::service::{BroadcasterServiceState, SnapshotSessionError};
use crate::broadcaster::state::{
    BroadcasterReadiness, BroadcasterSnapshotCache, BroadcasterStatusSnapshot,
    BroadcasterUpstreamState,
};
use crate::config::{
    init_logging, load_broadcaster_config, load_broadcaster_redis_config, BroadcasterConfig,
    MemoryConfig,
};
use crate::memory::maybe_log_memory_snapshot;
use crate::models::stream_health::StreamHealth;
use crate::models::tokens::{TokenStore, TokenStoreError};
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
    redis_publisher: Arc<BroadcasterRedisPublisher>,
    state_history: Option<StateHistoryWriter>,
    state_history_checkpoints: Option<StateHistoryCheckpointWriter>,
    tokens: Arc<TokenStore>,
    chain_id: u64,
    snapshot_session_ttl: Duration,
    producer_lifecycle: ProducerLifecycle,
}

#[derive(Clone)]
struct ProducerLifecycle {
    cancellation: CancellationToken,
    handles: Arc<tokio::sync::Mutex<Option<Vec<JoinHandle<()>>>>>,
}

impl ProducerLifecycle {
    fn idle() -> Self {
        Self::new(CancellationToken::new(), Vec::new())
    }

    fn new(cancellation: CancellationToken, handles: Vec<JoinHandle<()>>) -> Self {
        Self {
            cancellation,
            handles: Arc::new(tokio::sync::Mutex::new(Some(handles))),
        }
    }

    fn begin_shutdown(&self) {
        self.cancellation.cancel();
    }

    async fn join(&self, grace_period: Duration) {
        self.begin_shutdown();
        let Some(handles) = self.handles.lock().await.take() else {
            return;
        };
        let mut join_all = Box::pin(async move {
            for handle in handles {
                match handle.await {
                    Ok(()) => {}
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => warn!(error = %error, "Broadcaster producer task failed"),
                }
            }
        });
        if tokio::time::timeout(grace_period, &mut join_all)
            .await
            .is_err()
        {
            warn!("Broadcaster producers are still draining after the shutdown grace period");
            join_all.await;
        }
    }
}

impl BroadcasterAppState {
    #[expect(
        clippy::too_many_arguments,
        reason = "app state is assembled from independent runtime handles at startup"
    )]
    pub fn with_snapshot_session_ttl(
        raw_service: BroadcasterServiceState,
        rfq_service: Option<BroadcasterServiceState>,
        tokens: Arc<TokenStore>,
        chain_id: u64,
        snapshot_session_ttl: Duration,
        redis_publisher: Arc<BroadcasterRedisPublisher>,
        state_history: Option<StateHistoryWriter>,
        state_history_checkpoints: Option<StateHistoryCheckpointWriter>,
    ) -> Self {
        Self::with_producer_lifecycle(
            raw_service,
            rfq_service,
            tokens,
            chain_id,
            snapshot_session_ttl,
            redis_publisher,
            state_history,
            state_history_checkpoints,
            ProducerLifecycle::idle(),
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "app state is assembled from independent runtime handles at startup"
    )]
    fn with_producer_lifecycle(
        raw_service: BroadcasterServiceState,
        rfq_service: Option<BroadcasterServiceState>,
        tokens: Arc<TokenStore>,
        chain_id: u64,
        snapshot_session_ttl: Duration,
        redis_publisher: Arc<BroadcasterRedisPublisher>,
        state_history: Option<StateHistoryWriter>,
        state_history_checkpoints: Option<StateHistoryCheckpointWriter>,
        producer_lifecycle: ProducerLifecycle,
    ) -> Self {
        Self {
            raw_service,
            rfq_service,
            redis_publisher,
            state_history,
            state_history_checkpoints,
            tokens,
            chain_id,
            snapshot_session_ttl,
            producer_lifecycle,
        }
    }

    pub async fn create_snapshot_session(
        &self,
    ) -> Result<Option<BroadcasterSnapshotSessionResponse>> {
        let mut services = vec![self.raw_service.clone()];
        if let Some(rfq_service) = &self.rfq_service {
            services.push(rfq_service.clone());
        }
        BroadcasterServiceState::create_snapshot_session_for_services(
            &services,
            self.snapshot_session_ttl,
        )
        .await
    }

    pub async fn snapshot_session_payload(
        &self,
        session_id: u64,
        index: u32,
    ) -> Result<BroadcasterEnvelope, SnapshotSessionError> {
        self.raw_service
            .snapshot_session_payload(session_id, index)
            .await
    }

    pub async fn status_snapshot(&self) -> BroadcasterStatusSnapshot {
        let mut snapshot = self.raw_service.status_snapshot().await;
        if let Some(rfq_service) = &self.rfq_service {
            snapshot = combine_status_snapshots(snapshot, rfq_service.status_snapshot().await);
        }
        let redis_status = self.redis_publisher.verified_status_snapshot().await;
        if snapshot.readiness == BroadcasterReadiness::Ready {
            snapshot.readiness = redis_readiness(redis_status.mode);
        }
        snapshot.redis_publisher = Some(redis_status);
        if let Some(state_history) = &self.state_history {
            let mut state_history_status: crate::broadcaster::state::BroadcasterStateHistoryStatus =
                state_history.snapshot().await.into();
            if let Some(checkpoints) = &self.state_history_checkpoints {
                state_history_status.checkpoints = Some(checkpoints.snapshot().await.into());
            }
            snapshot.state_history = Some(state_history_status);
        }
        snapshot
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

    pub fn begin_shutdown(&self) {
        self.producer_lifecycle.begin_shutdown();
    }

    pub async fn finish_shutdown(&self) -> Result<()> {
        self.producer_lifecycle.join(Duration::from_secs(10)).await;
        if let Some(state_history) = &self.state_history {
            state_history.shutdown(Duration::from_secs(10)).await?;
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.begin_shutdown();
        self.finish_shutdown().await
    }
}

pub struct BroadcasterServiceParts {
    pub config: BroadcasterConfig,
    pub app_state: BroadcasterAppState,
}

#[expect(
    clippy::too_many_lines,
    reason = "startup keeps service construction and producer ownership in one place"
)]
pub async fn build_broadcaster_service() -> Result<BroadcasterServiceParts> {
    init_logging();

    let config = load_broadcaster_config();
    let chain = config.chain_profile.chain;
    info!(chain_id = chain.id(), chain = %chain, "Initializing Tycho broadcaster...");
    log_memory_config(config.memory);

    let tokens = load_token_store(&config).await?;
    let raw_backends = raw_configured_backends(&config);
    let heartbeat_interval = Duration::from_secs(config.tuning.heartbeat_interval_secs);
    let (state_history, state_history_checkpoints) = build_state_history(&config).await?;
    let redis_publisher =
        build_redis_publisher(chain.id(), heartbeat_interval, state_history.clone()).await?;
    let raw_cache = BroadcasterSnapshotCache::new(chain.id(), raw_backends.clone());
    let raw_upstream_state = BroadcasterUpstreamState::default();
    let rfq_backends = rfq_configured_backends(&config);
    let rfq_cache = rfq_backends
        .as_ref()
        .map(|backends| BroadcasterSnapshotCache::new(chain.id(), backends.clone()));
    let rfq_upstream_state = rfq_cache
        .as_ref()
        .map(|_| BroadcasterUpstreamState::default());
    let publication_gate = Arc::new(tokio::sync::Mutex::new(()));
    let raw_service = BroadcasterServiceState::with_lifecycle_gate(
        config.tuning.snapshot_max_payload_bytes,
        raw_cache,
        raw_upstream_state,
        Arc::clone(&redis_publisher),
        Arc::clone(&publication_gate),
    );
    let raw_health = Arc::new(StreamHealth::new());
    let supervisor_cfg = build_supervisor_config(&config);
    let cancellation = CancellationToken::new();
    let mut producer_handles = Vec::new();

    let rfq_service =
        if let (Some(rfq_cache), Some(rfq_upstream_state)) = (rfq_cache, rfq_upstream_state) {
            let service = BroadcasterServiceState::with_lifecycle_gate(
                config.tuning.snapshot_max_payload_bytes,
                rfq_cache,
                rfq_upstream_state,
                Arc::clone(&redis_publisher),
                Arc::clone(&publication_gate),
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
            producer_handles.push(spawn_broadcaster_rfq_stream_task(
                &config,
                supervisor_cfg.clone(),
                service.clone(),
                vec![raw_service.clone(), service.clone()],
                vec![service.clone()],
                rfq_token_stores,
                cancellation.clone(),
            ));
            Some(service)
        } else {
            None
        };
    let generation_services = match &rfq_service {
        Some(rfq_service) => vec![raw_service.clone(), rfq_service.clone()],
        None => vec![raw_service.clone()],
    };
    // New broadcasters start passive and warm their caches first. This task is
    // the local promotion loop; /status stays non-ready until it wins the fence.
    producer_handles.push(spawn_promotion_task(
        generation_services.clone(),
        Duration::from_secs(1),
        cancellation.clone(),
    ));
    producer_handles.push(spawn_broadcaster_stream_task(
        &config,
        supervisor_cfg.clone(),
        Arc::clone(&raw_health),
        raw_service.clone(),
        generation_services.clone(),
        vec![raw_service.clone()],
        cancellation.clone(),
    ));
    producer_handles.push(spawn_heartbeat_task(
        generation_services,
        heartbeat_interval,
        cancellation.clone(),
    ));
    if let Some(handle) = maybe_spawn_state_history_checkpoint_task(
        &config,
        &raw_service,
        rfq_service.as_ref(),
        state_history_checkpoints.clone(),
        cancellation.clone(),
    ) {
        producer_handles.push(handle);
    }
    let producer_lifecycle = ProducerLifecycle::new(cancellation, producer_handles);
    let snapshot_session_ttl = Duration::from_secs(config.tuning.snapshot_session_ttl_secs);
    let app_state = BroadcasterAppState::with_producer_lifecycle(
        raw_service,
        rfq_service,
        tokens,
        chain.id(),
        snapshot_session_ttl,
        redis_publisher,
        state_history,
        state_history_checkpoints,
        producer_lifecycle,
    );

    Ok(BroadcasterServiceParts { config, app_state })
}

async fn build_state_history(
    config: &BroadcasterConfig,
) -> Result<(
    Option<StateHistoryWriter>,
    Option<StateHistoryCheckpointWriter>,
)> {
    let Some(state_history) = &config.state_history else {
        return Ok((None, None));
    };
    let store = StateHistoryPgStore::connect(&state_history.database_url).await?;
    store.validate_schema().await?;
    let checkpoint_store = S3CheckpointStore::from_env_config(
        &state_history.s3_region,
        state_history.s3_bucket.clone(),
        state_history.s3_endpoint_url.as_deref(),
        state_history.s3_force_path_style,
    )
    .await?;
    let writer = StateHistoryWriter::spawn(store.clone(), state_history.writer.clone())?;
    let checkpoints =
        StateHistoryCheckpointWriter::new(store, checkpoint_store, state_history.s3_prefix.clone());
    Ok((Some(writer), Some(checkpoints)))
}

fn spawn_state_history_checkpoint_task(
    services: Vec<BroadcasterServiceState>,
    checkpoints: StateHistoryCheckpointWriter,
    checkpoint_block_interval: u64,
    poll_interval: Duration,
    cancellation: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_checkpoint_block: Option<u64> = None;
        let mut failure_backoff = CheckpointFailureBackoff::default();
        let mut next_delay = poll_interval;
        loop {
            if sleep_or_cancel(next_delay, &cancellation).await {
                return;
            }
            next_delay = poll_interval;
            let Some(current_block) = checkpoint_block_candidate(&services).await else {
                continue;
            };
            if !should_write_state_history_checkpoint(
                last_checkpoint_block,
                current_block,
                checkpoint_block_interval,
            ) {
                continue;
            }

            match BroadcasterServiceState::create_state_history_checkpoint_for_services(&services)
                .await
            {
                Ok(Some(archive)) => {
                    let block_number = archive.metadata.block_number;
                    match checkpoints.write_checkpoint(archive).await {
                        Ok(outcome) => {
                            last_checkpoint_block = Some(block_number);
                            failure_backoff.reset();
                            info!(
                                event = "state_history_checkpoint_complete",
                                block_number,
                                s3_key = outcome.s3_key.as_str(),
                                "State history checkpoint completed"
                            );
                        }
                        Err(error) => {
                            next_delay = failure_backoff.record_failure();
                            warn!(
                                event = "state_history_checkpoint_failed",
                                block_number,
                                error = %error,
                                "State history checkpoint failed"
                            );
                        }
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    next_delay = failure_backoff.record_failure();
                    warn!(
                        event = "state_history_checkpoint_capture_failed",
                        error = %error,
                        "State history checkpoint capture failed"
                    );
                }
            }
        }
    })
}

fn maybe_spawn_state_history_checkpoint_task(
    config: &BroadcasterConfig,
    raw_service: &BroadcasterServiceState,
    rfq_service: Option<&BroadcasterServiceState>,
    checkpoints: Option<StateHistoryCheckpointWriter>,
    cancellation: CancellationToken,
) -> Option<JoinHandle<()>> {
    let (Some(checkpoints), Some(history_config)) = (checkpoints, config.state_history.as_ref())
    else {
        return None;
    };
    let services = match rfq_service {
        Some(rfq_service) => vec![raw_service.clone(), rfq_service.clone()],
        None => vec![raw_service.clone()],
    };
    Some(spawn_state_history_checkpoint_task(
        services,
        checkpoints,
        history_config.checkpoint_block_interval,
        Duration::from_secs(history_config.checkpoint_poll_interval_secs),
        cancellation,
    ))
}

const CHECKPOINT_FAILURE_BACKOFF_SECS: [u64; 6] = [30, 60, 120, 240, 480, 600];

#[derive(Default)]
struct CheckpointFailureBackoff {
    failure_count: usize,
}

impl CheckpointFailureBackoff {
    fn record_failure(&mut self) -> Duration {
        let index = self
            .failure_count
            .min(CHECKPOINT_FAILURE_BACKOFF_SECS.len() - 1);
        self.failure_count = self.failure_count.saturating_add(1);
        Duration::from_secs(CHECKPOINT_FAILURE_BACKOFF_SECS[index])
    }

    fn reset(&mut self) {
        self.failure_count = 0;
    }
}

async fn checkpoint_block_candidate(services: &[BroadcasterServiceState]) -> Option<u64> {
    let mut block_numbers = Vec::new();
    for service in services {
        let status = service.status_snapshot().await;
        if status.readiness != BroadcasterReadiness::Ready {
            return None;
        }
        block_numbers.extend(status.backends.iter().filter_map(|(backend, status)| {
            matches!(backend, BroadcasterBackend::Native | BroadcasterBackend::Vm)
                .then_some(status.block_number)
                .flatten()
        }));
    }
    block_numbers.into_iter().min()
}

fn should_write_state_history_checkpoint(
    last_checkpoint_block: Option<u64>,
    current_block: u64,
    checkpoint_block_interval: u64,
) -> bool {
    last_checkpoint_block
        .map(|last| current_block.saturating_sub(last) >= checkpoint_block_interval)
        .unwrap_or(true)
}

fn combine_status_snapshots(
    mut raw: BroadcasterStatusSnapshot,
    rfq: BroadcasterStatusSnapshot,
) -> BroadcasterStatusSnapshot {
    raw.readiness = combine_readiness(raw.readiness, rfq.readiness);
    raw.upstream.connected = raw.upstream.connected && rfq.upstream.connected;
    raw.upstream.restart_count = raw
        .upstream
        .restart_count
        .saturating_add(rfq.upstream.restart_count);
    raw.upstream.last_error = raw.upstream.last_error.or(rfq.upstream.last_error);
    raw.upstream.last_disconnect_reason = raw
        .upstream
        .last_disconnect_reason
        .or(rfq.upstream.last_disconnect_reason);
    raw.upstream.last_update_age_ms = raw
        .upstream
        .last_update_age_ms
        .max(rfq.upstream.last_update_age_ms);
    raw.snapshot.ready = raw.snapshot.ready && rfq.snapshot.ready;
    raw.snapshot
        .configured_backends
        .extend(rfq.snapshot.configured_backends);
    raw.snapshot.configured_backends.sort();
    raw.snapshot.configured_backends.dedup();
    raw.snapshot.total_states = raw
        .snapshot
        .total_states
        .saturating_add(rfq.snapshot.total_states);
    raw.snapshot_sessions.active = raw
        .snapshot_sessions
        .active
        .saturating_add(rfq.snapshot_sessions.active);
    raw.snapshot_sessions.last_error = raw
        .snapshot_sessions
        .last_error
        .or(rfq.snapshot_sessions.last_error);
    raw.backends.extend(rfq.backends);
    raw
}

fn combine_readiness(
    left: BroadcasterReadiness,
    right: BroadcasterReadiness,
) -> BroadcasterReadiness {
    left.max(right)
}

fn redis_readiness(mode: &str) -> BroadcasterReadiness {
    match mode {
        "active" => BroadcasterReadiness::Ready,
        "passive" => BroadcasterReadiness::RedisPublisherPassive,
        "retired" => BroadcasterReadiness::RedisPublisherRetired,
        _ => BroadcasterReadiness::RedisPublisherUnhealthy,
    }
}

async fn build_redis_publisher(
    chain_id: u64,
    heartbeat_interval: Duration,
    state_history: Option<StateHistoryWriter>,
) -> Result<Arc<BroadcasterRedisPublisher>> {
    let redis_config = load_broadcaster_redis_config();
    let writer = Arc::new(TokioRedisStreamWriter::connect(&redis_config.redis_url).await?);
    let publisher = Arc::new(
        BroadcasterRedisPublisher::new(
            BroadcasterRedisPublisherConfig::from_redis_config(
                &redis_config,
                chain_id,
                heartbeat_interval,
            ),
            writer,
        )
        .with_state_history_writer(state_history),
    );
    Ok(publisher)
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
    generation_services: Vec<BroadcasterServiceState>,
    cache_reset_services: Vec<BroadcasterServiceState>,
    cancellation: CancellationToken,
) -> JoinHandle<()> {
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
            BroadcasterStreamControls {
                service,
                generation_services,
                cache_reset_services,
            },
            cancellation,
        )
        .await;
    })
}

fn spawn_broadcaster_rfq_stream_task(
    config: &BroadcasterConfig,
    supervisor_cfg: StreamSupervisorConfig,
    service: BroadcasterServiceState,
    generation_services: Vec<BroadcasterServiceState>,
    cache_reset_services: Vec<BroadcasterServiceState>,
    token_stores: RFQTokenStores,
    cancellation: CancellationToken,
) -> JoinHandle<()> {
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
            BroadcasterStreamControls {
                service,
                generation_services,
                cache_reset_services,
            },
            cancellation,
        )
        .await;
    })
}

fn spawn_heartbeat_task(
    services: Vec<BroadcasterServiceState>,
    interval: Duration,
    cancellation: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return,
                _ = ticker.tick() => {}
            }
            for service in &services {
                if let Err(error) = service.broadcast_heartbeat().await {
                    let error = error.to_string();
                    info!(error = %error, "Resetting broadcaster generation after heartbeat error");
                    BroadcasterServiceState::handle_shared_generation_reset(
                        &services,
                        "heartbeat_error",
                        Some(error),
                    )
                    .await;
                    break;
                }
            }
        }
    })
}

fn spawn_promotion_task(
    services: Vec<BroadcasterServiceState>,
    interval: Duration,
    cancellation: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return,
                _ = ticker.tick() => {}
            }
            match BroadcasterServiceState::promote_when_ready(&services, "active_writer_promoted")
                .await
            {
                Ok(Some(boundary)) => {
                    info!(
                        stream_id = boundary.stream_id.as_str(),
                        snapshot_id = boundary.snapshot_id.as_str(),
                        generation = boundary.generation,
                        "Broadcaster active writer promoted"
                    );
                    return;
                }
                Ok(None) => {}
                Err(error) => {
                    info!(error = %error, "Broadcaster active writer promotion skipped");
                }
            }
        }
    })
}

async fn sleep_or_cancel(duration: Duration, cancellation: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => true,
        _ = sleep(duration) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    use simulator_core::broadcaster::BroadcasterBackend;
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;
    use tycho_simulation::tycho_common::{models::Chain, Bytes};

    use super::{
        raw_configured_backends, rfq_configured_backends, should_write_state_history_checkpoint,
        CheckpointFailureBackoff, ProducerLifecycle,
    };
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
                heartbeat_interval_secs: 5,
                token_min_quality: 0,
                snapshot_session_ttl_secs: 300,
            },
            state_history: None,
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

    #[test]
    fn state_history_checkpoint_interval_is_measured_from_last_completed_block() {
        assert!(should_write_state_history_checkpoint(None, 100, 500));
        assert!(!should_write_state_history_checkpoint(Some(100), 599, 500));
        assert!(should_write_state_history_checkpoint(Some(100), 600, 500));
        assert!(!should_write_state_history_checkpoint(
            Some(u64::MAX - 10),
            u64::MAX,
            500
        ));
    }

    #[test]
    fn checkpoint_failure_backoff_caps_and_resets() {
        let mut backoff = CheckpointFailureBackoff::default();
        let observed = (0..7)
            .map(|_| backoff.record_failure().as_secs())
            .collect::<Vec<_>>();
        assert_eq!(observed, vec![30, 60, 120, 240, 480, 600, 600]);

        backoff.reset();
        assert_eq!(backoff.record_failure().as_secs(), 30);
    }

    #[tokio::test]
    async fn producer_join_finishes_history_enqueue_before_drain() {
        let (release, wait_for_release) = oneshot::channel();
        let (events, mut observed_events) = tokio::sync::mpsc::unbounded_channel();
        let producer_events = events.clone();
        let cancellation = CancellationToken::new();
        let producer_cancellation = cancellation.clone();
        let handle = tokio::spawn(async move {
            let _ = wait_for_release.await;
            let _ = producer_events.send("same_block_update_published");
            let _ = producer_events.send("history_enqueued");
            if !producer_cancellation.is_cancelled() {
                let _ = producer_events.send("published_after_shutdown");
            }
        });
        let lifecycle = ProducerLifecycle::new(cancellation, vec![handle]);
        let mut join = tokio::spawn(async move {
            lifecycle.join(Duration::ZERO).await;
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut join)
                .await
                .is_err(),
            "producer join returned before the in-flight task completed"
        );

        assert!(release.send(()).is_ok());
        assert!(join.await.is_ok());
        let _ = events.send("history_drained");
        let mut sequence = Vec::new();
        while let Ok(event) = observed_events.try_recv() {
            sequence.push(event);
        }
        assert_eq!(
            sequence,
            vec![
                "same_block_update_published",
                "history_enqueued",
                "history_drained"
            ]
        );
    }
}
