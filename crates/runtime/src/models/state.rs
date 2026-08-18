use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use simulator_core::broadcaster::BroadcasterRedisReplayBoundary;
use simulator_replay::{ApplyReport, PoolEntry, ReplayWorld, StatePoint};
use tokio::sync::{watch, Mutex, OwnedRwLockReadGuard, RwLock};
use tokio::time::Instant;
use tycho_simulation::{
    protocol::models::Update,
    tycho_common::{
        models::{token::Token, Chain},
        Bytes,
    },
};

use crate::config::SlippageConfig;

use super::{
    erc4626::Erc4626PairPolicy, protocol::ProtocolKind, stream_health::StreamHealth,
    tokens::TokenStore,
};

#[cfg(test)]
const NATIVE_TOKEN_ADDRESS_BYTES: [u8; 20] = [0u8; 20];
const READINESS_POLL_INTERVAL_MS: u64 = 50;

#[cfg(test)]
fn native_token_address() -> Bytes {
    Bytes::from(NATIVE_TOKEN_ADDRESS_BYTES)
}

#[derive(Clone)]
pub struct AppState {
    pub chain: Chain,
    pub rfq_client_config: Arc<RfqClientConfig>,
    pub native_token_protocol_allowlist: Arc<Vec<String>>,
    pub tokens: Arc<TokenStore>,
    pub native_broadcaster_subscription: BroadcasterSubscriptionStatus,
    pub vm_broadcaster_subscription: BroadcasterSubscriptionStatus,
    pub rfq_broadcaster_subscription: BroadcasterSubscriptionStatus,
    pub native_state_store: Arc<StateStore>,
    pub vm_state_store: Arc<StateStore>,
    pub rfq_state_store: Arc<StateStore>,
    pub native_stream_health: Arc<StreamHealth>,
    pub vm_stream_health: Arc<StreamHealth>,
    pub rfq_stream_health: Arc<StreamHealth>,
    pub vm_stream: Arc<RwLock<VmStreamStatus>>,
    pub configured_backends: ConfiguredBackends,
    pub enable_vm_pools: bool,
    pub enable_rfq_pools: bool,
    pub native_progress_lease: Duration,
    pub optional_backend_stale: Duration,
    pub request_timeout: Duration,
    pub vm_simulation_rebuild_gate: Arc<RwLock<()>>,
    pub rfq_simulation_rebuild_gate: Arc<RwLock<()>>,
    pub slippage: SlippageConfig,
    pub erc4626_deposits_enabled: bool,
    pub erc4626_pair_policies: Arc<Vec<Erc4626PairPolicy>>,
    pub reset_allowance_tokens: Arc<HashMap<u64, HashSet<Bytes>>>,
}

#[derive(Clone, Default)]
pub struct RfqClientConfig {
    pub tvl_threshold: f64,
    pub bebop_key: String,
    pub hashflow_user: String,
    pub hashflow_key: String,
    pub liquorice_user: String,
    pub liquorice_key: String,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ConfiguredBackends {
    // Native is always configured; only optional manifest-backed backends live here.
    pub vm: bool,
    pub rfq: bool,
}

#[derive(Debug)]
pub(crate) struct SimulationRebuildGuard {
    _vm_read_guard: Option<OwnedRwLockReadGuard<()>>,
    _rfq_read_guard: Option<OwnedRwLockReadGuard<()>>,
}

impl SimulationRebuildGuard {
    pub(crate) const fn blocks_required_rebuilds(&self, uses_vm: bool, uses_rfq: bool) -> bool {
        (!uses_vm || self._vm_read_guard.is_some()) && (!uses_rfq || self._rfq_read_guard.is_some())
    }
}

#[derive(Debug, Clone, Default)]
pub struct BroadcasterSubscriptionStatus {
    inner: Arc<RwLock<BroadcasterSubscriptionStatusData>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RedisTransportStatus {
    #[default]
    Connecting,
    Connected,
    Retrying,
    Failed,
}

impl RedisTransportStatus {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Connecting => "connecting",
            Self::Connected => "connected",
            Self::Retrying => "retrying",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Default)]
struct BroadcasterSubscriptionStatusData {
    connected: bool,
    bootstrap_complete: bool,
    stream_id: Option<String>,
    snapshot_id: Option<String>,
    redis_replay_boundary: Option<BroadcasterRedisReplayBoundary>,
    redis_replay_checkpoint: Option<String>,
    redis_replay_caught_up: bool,
    redis_gap_reason: Option<String>,
    redis_transport_status: RedisTransportStatus,
    redis_transport_retry_count: u64,
    redis_transport_last_error: Option<String>,
    publisher_to_consumer_delay_ms: Option<u64>,
    restart_count: u64,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct BroadcasterSubscriptionSnapshot {
    pub connected: bool,
    pub bootstrap_complete: bool,
    pub stream_id: Option<String>,
    pub snapshot_id: Option<String>,
    pub redis_replay_boundary: Option<BroadcasterRedisReplayBoundary>,
    pub redis_replay_checkpoint: Option<String>,
    pub redis_replay_caught_up: bool,
    pub redis_gap_reason: Option<String>,
    pub redis_transport_status: RedisTransportStatus,
    pub redis_transport_retry_count: u64,
    pub redis_transport_last_error: Option<String>,
    pub publisher_to_consumer_delay_ms: Option<u64>,
    pub restart_count: u64,
    pub last_error: Option<String>,
}

impl BroadcasterSubscriptionStatus {
    pub async fn mark_connected(&self) {
        let mut guard = self.inner.write().await;
        guard.connected = true;
        guard.bootstrap_complete = false;
        guard.stream_id = None;
        guard.snapshot_id = None;
        guard.redis_replay_boundary = None;
        guard.redis_replay_checkpoint = None;
        guard.redis_replay_caught_up = false;
        guard.redis_gap_reason = None;
        guard.redis_transport_status = RedisTransportStatus::Connecting;
        guard.redis_transport_retry_count = 0;
        guard.redis_transport_last_error = None;
        guard.publisher_to_consumer_delay_ms = None;
        guard.last_error = None;
    }

    pub async fn mark_snapshot_started(
        &self,
        stream_id: impl Into<String>,
        snapshot_id: impl Into<String>,
    ) {
        let mut guard = self.inner.write().await;
        guard.connected = true;
        guard.bootstrap_complete = false;
        guard.stream_id = Some(stream_id.into());
        guard.snapshot_id = Some(snapshot_id.into());
        guard.redis_replay_boundary = None;
        guard.redis_replay_checkpoint = None;
        guard.redis_replay_caught_up = false;
        guard.redis_gap_reason = None;
        guard.redis_transport_status = RedisTransportStatus::Connecting;
        guard.redis_transport_retry_count = 0;
        guard.redis_transport_last_error = None;
        guard.publisher_to_consumer_delay_ms = None;
        guard.last_error = None;
    }

    pub async fn mark_bootstrap_complete_with_redis_boundary(
        &self,
        boundary: BroadcasterRedisReplayBoundary,
    ) {
        let mut guard = self.inner.write().await;
        guard.connected = true;
        guard.bootstrap_complete = true;
        guard.redis_replay_checkpoint = Some(boundary.exclusive_entry_id());
        guard.redis_replay_boundary = Some(boundary);
        guard.redis_replay_caught_up = false;
        guard.redis_gap_reason = None;
        guard.redis_transport_status = RedisTransportStatus::Connected;
        guard.redis_transport_retry_count = 0;
        guard.redis_transport_last_error = None;
        guard.publisher_to_consumer_delay_ms = None;
        guard.last_error = None;
    }

    pub async fn mark_recovery_started(&self) {
        let mut guard = self.inner.write().await;
        guard.connected = true;
        guard.bootstrap_complete = false;
        guard.redis_replay_caught_up = false;
        guard.redis_gap_reason = None;
        guard.redis_transport_status = RedisTransportStatus::Connected;
        guard.redis_transport_last_error = None;
        guard.publisher_to_consumer_delay_ms = None;
        guard.last_error = None;
    }

    pub async fn mark_redis_replay_checkpoint(&self, checkpoint: impl Into<String>) {
        let mut guard = self.inner.write().await;
        guard.redis_replay_checkpoint = Some(checkpoint.into());
        guard.redis_gap_reason = None;
    }

    pub async fn mark_redis_catch_up_checkpoint(&self, checkpoint: impl Into<String>) {
        let mut guard = self.inner.write().await;
        guard.redis_replay_checkpoint = Some(checkpoint.into());
        guard.redis_replay_caught_up = true;
        guard.redis_gap_reason = None;
    }

    pub async fn mark_redis_gap(&self, reason: impl Into<String>) {
        let mut guard = self.inner.write().await;
        guard.redis_replay_caught_up = false;
        guard.redis_gap_reason = Some(reason.into());
    }

    pub async fn mark_redis_transport_error(&self, error: impl Into<String>) {
        let mut guard = self.inner.write().await;
        guard.redis_transport_status = RedisTransportStatus::Retrying;
        guard.redis_transport_retry_count = guard.redis_transport_retry_count.saturating_add(1);
        guard.redis_transport_last_error = Some(error.into());
    }

    pub async fn mark_redis_transport_recovered(&self) {
        let mut guard = self.inner.write().await;
        guard.redis_transport_status = RedisTransportStatus::Connected;
        guard.redis_transport_last_error = None;
    }

    pub async fn mark_redis_transport_failed(&self, error: impl Into<String>) {
        let mut guard = self.inner.write().await;
        guard.redis_transport_status = RedisTransportStatus::Failed;
        guard.redis_transport_last_error = Some(error.into());
    }

    pub async fn record_publisher_to_consumer_delay(&self, published_at_ms: u64) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_millis() as u64);
        self.inner.write().await.publisher_to_consumer_delay_ms =
            Some(now_ms.saturating_sub(published_at_ms));
    }

    pub async fn mark_disconnected(&self, last_error: Option<String>) {
        let mut guard = self.inner.write().await;
        guard.connected = false;
        guard.bootstrap_complete = false;
        guard.stream_id = None;
        guard.snapshot_id = None;
        guard.redis_replay_boundary = None;
        guard.redis_replay_checkpoint = None;
        guard.redis_replay_caught_up = false;
        guard.redis_gap_reason = None;
        guard.redis_transport_status = RedisTransportStatus::Connecting;
        guard.redis_transport_retry_count = 0;
        guard.redis_transport_last_error = None;
        guard.publisher_to_consumer_delay_ms = None;
        guard.restart_count = guard.restart_count.saturating_add(1);
        guard.last_error = last_error;
    }

    pub async fn snapshot(&self) -> BroadcasterSubscriptionSnapshot {
        let guard = self.inner.read().await;
        BroadcasterSubscriptionSnapshot {
            connected: guard.connected,
            bootstrap_complete: guard.bootstrap_complete,
            stream_id: guard.stream_id.clone(),
            snapshot_id: guard.snapshot_id.clone(),
            redis_replay_boundary: guard.redis_replay_boundary.clone(),
            redis_replay_checkpoint: guard.redis_replay_checkpoint.clone(),
            redis_replay_caught_up: guard.redis_replay_caught_up,
            redis_gap_reason: guard.redis_gap_reason.clone(),
            redis_transport_status: guard.redis_transport_status,
            redis_transport_retry_count: guard.redis_transport_retry_count,
            redis_transport_last_error: guard.redis_transport_last_error.clone(),
            publisher_to_consumer_delay_ms: guard.publisher_to_consumer_delay_ms,
            restart_count: guard.restart_count,
            last_error: guard.last_error.clone(),
        }
    }

    pub fn ready_for_test() -> Self {
        Self {
            inner: Arc::new(RwLock::new(BroadcasterSubscriptionStatusData {
                connected: true,
                bootstrap_complete: true,
                stream_id: Some("stream-test".to_string()),
                snapshot_id: Some("snapshot-test".to_string()),
                redis_replay_boundary: None,
                redis_replay_checkpoint: None,
                redis_replay_caught_up: true,
                redis_gap_reason: None,
                redis_transport_status: RedisTransportStatus::Connected,
                redis_transport_retry_count: 0,
                redis_transport_last_error: None,
                publisher_to_consumer_delay_ms: Some(0),
                restart_count: 0,
                last_error: None,
            })),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeReadiness {
    Ready,
    WarmingUp,
    Stale,
}

impl NativeReadiness {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::WarmingUp => "warming_up",
            Self::Stale => "stale",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VmReadiness {
    Disabled,
    WarmingUp,
    Rebuilding,
    Ready,
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RfqReadiness {
    Disabled,
    WarmingUp,
    Rebuilding,
    Ready,
    Stale,
}

impl VmReadiness {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::WarmingUp => "warming_up",
            Self::Rebuilding => "rebuilding",
            Self::Ready => "ready",
            Self::Stale => "stale",
        }
    }
}

impl RfqReadiness {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::WarmingUp => "warming_up",
            Self::Rebuilding => "rebuilding",
            Self::Ready => "ready",
            Self::Stale => "stale",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimulatorServiceStatus {
    Ready,
    WarmingUp,
    Stale,
}

impl SimulatorServiceStatus {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::WarmingUp => "warming_up",
            Self::Stale => "stale",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SimulatorBackendKind {
    Native,
    Vm,
    Rfq,
}

impl SimulatorBackendKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Vm => "vm",
            Self::Rfq => "rfq",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimulatorBackendReadiness {
    Disabled,
    WarmingUp,
    Rebuilding,
    Ready,
    Stale,
}

impl SimulatorBackendReadiness {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::WarmingUp => "warming_up",
            Self::Rebuilding => "rebuilding",
            Self::Ready => "ready",
            Self::Stale => "stale",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimulatorReadinessReason {
    DisabledByConfig,
    BroadcasterDisconnected,
    SnapshotBootstrapping,
    RedisReplayCatchingUp,
    RedisReplayGap,
    StateWarmingUp,
    Rebuilding,
    Stale,
}

impl SimulatorReadinessReason {
    pub const fn label(self) -> &'static str {
        match self {
            Self::DisabledByConfig => "disabled_by_config",
            Self::BroadcasterDisconnected => "broadcaster_disconnected",
            Self::SnapshotBootstrapping => "snapshot_bootstrapping",
            Self::RedisReplayCatchingUp => "redis_replay_catching_up",
            Self::RedisReplayGap => "redis_replay_gap",
            Self::StateWarmingUp => "state_warming_up",
            Self::Rebuilding => "rebuilding",
            Self::Stale => "stale",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SimulatorStatusSnapshot {
    pub status: SimulatorServiceStatus,
    pub chain_id: u64,
    pub backends: Vec<SimulatorBackendStatusSnapshot>,
}

#[derive(Debug, Clone)]
pub struct SimulatorBackendStatusSnapshot {
    pub kind: SimulatorBackendKind,
    pub enabled: bool,
    pub readiness: SimulatorBackendReadiness,
    pub reason: Option<SimulatorReadinessReason>,
    pub block_number: Option<u64>,
    pub update_timestamp: Option<u64>,
    pub pool_count: usize,
    pub restart_count: u64,
    pub last_error: Option<String>,
    pub rebuild_duration_ms: Option<u64>,
    pub last_update_age_ms: Option<u64>,
    pub publisher_to_consumer_delay_ms: Option<u64>,
    pub subscription: Option<SimulatorBackendSubscriptionSnapshot>,
}

#[derive(Debug, Clone)]
pub struct SimulatorBackendSubscriptionSnapshot {
    pub connected: bool,
    pub bootstrap_complete: bool,
    pub stream_id: Option<String>,
    pub snapshot_id: Option<String>,
    pub redis_replay_boundary: Option<BroadcasterRedisReplayBoundary>,
    pub redis_replay_checkpoint: Option<String>,
    pub redis_replay_caught_up: bool,
    pub redis_gap_reason: Option<String>,
    pub redis_transport_status: RedisTransportStatus,
    pub redis_transport_retry_count: u64,
    pub redis_transport_last_error: Option<String>,
    pub restart_count: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodeAvailability {
    Ready,
    NativeWarmingUp,
    NativeStale,
    VmDisabled,
    VmWarmingUp,
    VmRebuilding,
    VmStale,
    RfqDisabled,
    RfqWarmingUp,
    RfqRebuilding,
    RfqStale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeFenceStatus {
    Current,
    Changed,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NativeRouteFenceCheck {
    pub(crate) status: NativeFenceStatus,
    pub(crate) current_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NativePoolFenceStatus {
    Available(HashSet<String>),
    Unavailable,
}

impl EncodeAvailability {
    pub const fn availability_message(self) -> Option<&'static str> {
        match self {
            Self::Ready => None,
            Self::NativeWarmingUp => Some("Encode unavailable: native state warming up"),
            Self::NativeStale => Some("Encode unavailable: native state stale"),
            Self::VmDisabled => Some("Encode unavailable: VM pools disabled for requested route"),
            Self::RfqDisabled => Some("Encode unavailable: RFQ pools disabled for requested route"),
            Self::VmWarmingUp => {
                Some("Encode unavailable: VM state warming up for requested route")
            }
            Self::RfqWarmingUp => {
                Some("Encode unavailable: RFQ state warming up for requested route")
            }
            Self::VmRebuilding => {
                Some("Encode unavailable: VM state rebuilding for requested route")
            }
            Self::RfqRebuilding => {
                Some("Encode unavailable: RFQ state rebuilding for requested route")
            }
            Self::VmStale => Some("Encode unavailable: VM state stale for requested route"),
            Self::RfqStale => Some("Encode unavailable: RFQ state stale for requested route"),
        }
    }
}

impl AppState {
    async fn native_broadcaster_bootstrap_ready(&self) -> bool {
        let subscription = self.native_broadcaster_subscription.snapshot().await;
        subscription_readiness_reason(&subscription).is_none()
    }

    async fn vm_broadcaster_bootstrap_ready(&self) -> bool {
        let subscription = self.vm_broadcaster_subscription.snapshot().await;
        subscription_readiness_reason(&subscription).is_none()
    }

    async fn rfq_broadcaster_bootstrap_ready(&self) -> bool {
        let subscription = self.rfq_broadcaster_subscription.snapshot().await;
        subscription_readiness_reason(&subscription).is_none()
    }

    pub async fn status_snapshot(&self) -> SimulatorStatusSnapshot {
        let native = self.native_status_snapshot().await;
        let mut backends = vec![native];

        if self.configured_backends.vm {
            backends.push(self.vm_status_snapshot().await);
        }
        if self.configured_backends.rfq {
            backends.push(self.rfq_status_snapshot().await);
        }
        let status = service_status_from_backends(&backends);

        SimulatorStatusSnapshot {
            status,
            chain_id: self.chain.id(),
            backends,
        }
    }

    async fn native_status_snapshot(&self) -> SimulatorBackendStatusSnapshot {
        let subscription = self.native_broadcaster_subscription.snapshot().await;
        let subscription_reason = subscription_readiness_reason(&subscription);
        let last_update_age_ms = self.native_update_age_ms().await;
        let stream_restart_count = self.native_stream_health.restart_count().await;
        let stream_last_error = self.native_stream_health.last_error().await;
        let (state_ready, requests_allowed, _) = self.native_state_store.request_snapshot().await;
        let readiness = if subscription_reason.is_some() || !state_ready || !requests_allowed {
            SimulatorBackendReadiness::WarmingUp
        } else if is_update_stale(last_update_age_ms, self.native_progress_lease_ms()) {
            SimulatorBackendReadiness::Stale
        } else {
            SimulatorBackendReadiness::Ready
        };
        let reason = match readiness {
            SimulatorBackendReadiness::WarmingUp => {
                subscription_reason.or(Some(SimulatorReadinessReason::StateWarmingUp))
            }
            SimulatorBackendReadiness::Stale => Some(SimulatorReadinessReason::Stale),
            SimulatorBackendReadiness::Ready
            | SimulatorBackendReadiness::Disabled
            | SimulatorBackendReadiness::Rebuilding => None,
        };

        SimulatorBackendStatusSnapshot {
            kind: SimulatorBackendKind::Native,
            enabled: true,
            readiness,
            reason,
            block_number: Some(self.current_block().await),
            update_timestamp: None,
            pool_count: self.native_state_store.total_states().await,
            restart_count: stream_restart_count,
            last_error: subscription.last_error.clone().or(stream_last_error),
            rebuild_duration_ms: None,
            last_update_age_ms,
            publisher_to_consumer_delay_ms: subscription.publisher_to_consumer_delay_ms,
            subscription: Some(subscription.into()),
        }
    }

    async fn vm_status_snapshot(&self) -> SimulatorBackendStatusSnapshot {
        let subscription = self.vm_broadcaster_subscription.snapshot().await;
        let subscription_reason = subscription_readiness_reason(&subscription);
        let stream_status = self.vm_stream.read().await.clone();
        let last_update_age_ms = self.vm_update_age_ms().await;
        let readiness = if !self.enable_vm_pools {
            SimulatorBackendReadiness::Disabled
        } else if stream_status.rebuilding {
            SimulatorBackendReadiness::Rebuilding
        } else if subscription_reason.is_some() || !self.vm_state_store.is_ready() {
            SimulatorBackendReadiness::WarmingUp
        } else if is_update_stale(last_update_age_ms, self.optional_backend_stale_ms()) {
            SimulatorBackendReadiness::Stale
        } else {
            SimulatorBackendReadiness::Ready
        };
        let reason = match readiness {
            SimulatorBackendReadiness::Disabled => Some(SimulatorReadinessReason::DisabledByConfig),
            SimulatorBackendReadiness::Rebuilding => Some(SimulatorReadinessReason::Rebuilding),
            SimulatorBackendReadiness::WarmingUp => {
                subscription_reason.or(Some(SimulatorReadinessReason::StateWarmingUp))
            }
            SimulatorBackendReadiness::Stale => Some(SimulatorReadinessReason::Stale),
            SimulatorBackendReadiness::Ready => None,
        };

        SimulatorBackendStatusSnapshot {
            kind: SimulatorBackendKind::Vm,
            enabled: self.enable_vm_pools,
            readiness,
            reason,
            block_number: self.enable_vm_pools.then_some(self.vm_block().await),
            update_timestamp: None,
            pool_count: self.vm_pools().await,
            restart_count: stream_status.restart_count,
            last_error: subscription
                .last_error
                .clone()
                .or_else(|| stream_status.last_error.clone()),
            rebuild_duration_ms: if matches!(readiness, SimulatorBackendReadiness::Rebuilding) {
                stream_status
                    .rebuild_started_at
                    .map(|instant| instant.elapsed().as_millis() as u64)
            } else {
                None
            },
            last_update_age_ms,
            publisher_to_consumer_delay_ms: subscription.publisher_to_consumer_delay_ms,
            subscription: self.enable_vm_pools.then_some(subscription.into()),
        }
    }

    async fn rfq_status_snapshot(&self) -> SimulatorBackendStatusSnapshot {
        let subscription = self.rfq_broadcaster_subscription.snapshot().await;
        let subscription_reason = subscription_readiness_reason(&subscription);
        let last_update_age_ms = self.rfq_update_age_ms().await;
        let readiness = if !self.enable_rfq_pools {
            SimulatorBackendReadiness::Disabled
        } else if subscription_reason.is_some() || !self.rfq_state_store.is_ready() {
            SimulatorBackendReadiness::WarmingUp
        } else if is_update_stale(last_update_age_ms, self.optional_backend_stale_ms()) {
            SimulatorBackendReadiness::Stale
        } else {
            SimulatorBackendReadiness::Ready
        };
        let reason = match readiness {
            SimulatorBackendReadiness::Disabled => Some(SimulatorReadinessReason::DisabledByConfig),
            SimulatorBackendReadiness::WarmingUp => {
                subscription_reason.or(Some(SimulatorReadinessReason::StateWarmingUp))
            }
            SimulatorBackendReadiness::Stale => Some(SimulatorReadinessReason::Stale),
            SimulatorBackendReadiness::Ready | SimulatorBackendReadiness::Rebuilding => None,
        };
        let update_timestamp = if self.rfq_state_store.is_ready() {
            Some(self.rfq_update_timestamp().await)
        } else {
            None
        };

        SimulatorBackendStatusSnapshot {
            kind: SimulatorBackendKind::Rfq,
            enabled: self.enable_rfq_pools,
            readiness,
            reason,
            block_number: None,
            update_timestamp,
            pool_count: self.rfq_pools().await,
            restart_count: subscription.restart_count,
            last_error: subscription.last_error.clone(),
            rebuild_duration_ms: None,
            last_update_age_ms,
            publisher_to_consumer_delay_ms: subscription.publisher_to_consumer_delay_ms,
            subscription: self.enable_rfq_pools.then_some(subscription.into()),
        }
    }

    pub async fn current_block(&self) -> u64 {
        self.native_state_store.current_block().await
    }

    pub async fn current_vm_block(&self) -> Option<u64> {
        if !self.vm_ready().await {
            return None;
        }
        Some(self.vm_state_store.current_block().await)
    }

    pub async fn current_rfq_update_timestamp(&self) -> Option<u64> {
        if !self.rfq_ready().await {
            return None;
        }
        Some(self.rfq_state_store.current_block().await)
    }

    pub async fn total_pools(&self) -> usize {
        let native = self.native_state_store.total_states().await;
        let vm = self.vm_state_store.total_states().await;
        let rfq = self.rfq_state_store.total_states().await;
        native + vm + rfq
    }

    pub async fn is_ready(&self) -> bool {
        matches!(self.native_readiness().await, NativeReadiness::Ready)
    }

    pub fn optional_backend_stale_ms(&self) -> u64 {
        self.optional_backend_stale.as_millis() as u64
    }

    pub fn native_progress_lease_ms(&self) -> u64 {
        self.native_progress_lease.as_millis() as u64
    }

    pub async fn native_update_age_ms(&self) -> Option<u64> {
        self.native_stream_health.last_update_age_ms().await
    }

    pub async fn vm_update_age_ms(&self) -> Option<u64> {
        self.vm_stream_health.last_update_age_ms().await
    }

    pub async fn rfq_update_age_ms(&self) -> Option<u64> {
        self.rfq_stream_health.last_update_age_ms().await
    }

    pub async fn native_readiness(&self) -> NativeReadiness {
        if !self.native_broadcaster_bootstrap_ready().await {
            return NativeReadiness::WarmingUp;
        }

        let (state_ready, requests_allowed, _) = self.native_state_store.request_snapshot().await;
        if !state_ready || !requests_allowed {
            return NativeReadiness::WarmingUp;
        }

        if is_update_stale(
            self.native_update_age_ms().await,
            self.native_progress_lease_ms(),
        ) {
            NativeReadiness::Stale
        } else {
            NativeReadiness::Ready
        }
    }

    pub(crate) async fn native_route_fence_status(
        &self,
        pinned: &PublishedStatePin,
        pool_ids: &HashSet<String>,
    ) -> NativeRouteFenceCheck {
        let current = self.native_state_store.pin().await;
        let status = match self
            .native_pool_fence_status(pinned, &current, pool_ids)
            .await
        {
            NativePoolFenceStatus::Available(changed_pool_ids) if changed_pool_ids.is_empty() => {
                NativeFenceStatus::Current
            }
            NativePoolFenceStatus::Available(_) => NativeFenceStatus::Changed,
            NativePoolFenceStatus::Unavailable => NativeFenceStatus::Unavailable,
        };
        NativeRouteFenceCheck {
            status,
            current_generation: current.request_generation(),
        }
    }

    pub(crate) async fn native_request_availability(&self) -> (bool, u64) {
        let bootstrap_ready = self.native_broadcaster_bootstrap_ready().await;
        let update_is_stale = is_update_stale(
            self.native_update_age_ms().await,
            self.native_progress_lease_ms(),
        );
        let (state_ready, requests_allowed, current_generation) =
            self.native_state_store.request_snapshot().await;
        (
            bootstrap_ready && !update_is_stale && state_ready && requests_allowed,
            current_generation,
        )
    }

    pub(crate) async fn native_pool_fence_status(
        &self,
        pinned: &PublishedStatePin,
        current: &PublishedStatePin,
        pool_ids: &HashSet<String>,
    ) -> NativePoolFenceStatus {
        if !self.native_broadcaster_bootstrap_ready().await {
            return NativePoolFenceStatus::Unavailable;
        }
        if is_update_stale(
            self.native_update_age_ms().await,
            self.native_progress_lease_ms(),
        ) {
            return NativePoolFenceStatus::Unavailable;
        }
        // The request flags must come from the same publication as the identity compare.
        // fence_requests only flips requests_allowed and keeps every pool Arc, so a fence
        // landing between a separate availability read and the pin would pass as Current.
        if !current.state.ready || !current.state.requests_allowed {
            return NativePoolFenceStatus::Unavailable;
        }
        NativePoolFenceStatus::Available(pinned.changed_pool_ids(current, pool_ids))
    }

    pub async fn vm_readiness(&self) -> VmReadiness {
        if !self.enable_vm_pools {
            return VmReadiness::Disabled;
        }

        if self.vm_rebuilding().await {
            return VmReadiness::Rebuilding;
        }

        if !self.vm_broadcaster_bootstrap_ready().await {
            return VmReadiness::WarmingUp;
        }

        if !self.vm_state_store.is_ready() {
            return VmReadiness::WarmingUp;
        }

        if is_update_stale(
            self.vm_update_age_ms().await,
            self.optional_backend_stale_ms(),
        ) {
            VmReadiness::Stale
        } else {
            VmReadiness::Ready
        }
    }

    pub async fn rfq_readiness(&self) -> RfqReadiness {
        if !self.enable_rfq_pools {
            return RfqReadiness::Disabled;
        }

        if !self.rfq_broadcaster_bootstrap_ready().await {
            return RfqReadiness::WarmingUp;
        }

        if !self.rfq_state_store.is_ready() {
            return RfqReadiness::WarmingUp;
        }

        if is_update_stale(
            self.rfq_update_age_ms().await,
            self.optional_backend_stale_ms(),
        ) {
            RfqReadiness::Stale
        } else {
            RfqReadiness::Ready
        }
    }

    pub(crate) async fn encode_availability(
        &self,
        uses_native: bool,
        uses_vm: bool,
        uses_rfq: bool,
    ) -> EncodeAvailability {
        if uses_native {
            match self.native_readiness().await {
                NativeReadiness::Ready => {}
                NativeReadiness::WarmingUp => return EncodeAvailability::NativeWarmingUp,
                NativeReadiness::Stale => return EncodeAvailability::NativeStale,
            }
        }

        if uses_vm {
            match self.vm_readiness().await {
                VmReadiness::Disabled => return EncodeAvailability::VmDisabled,
                VmReadiness::WarmingUp => return EncodeAvailability::VmWarmingUp,
                VmReadiness::Rebuilding => return EncodeAvailability::VmRebuilding,
                VmReadiness::Ready => {}
                VmReadiness::Stale => return EncodeAvailability::VmStale,
            }
        }

        if uses_rfq {
            match self.rfq_readiness().await {
                RfqReadiness::Disabled => return EncodeAvailability::RfqDisabled,
                RfqReadiness::WarmingUp => return EncodeAvailability::RfqWarmingUp,
                RfqReadiness::Rebuilding => return EncodeAvailability::RfqRebuilding,
                RfqReadiness::Ready => {}
                RfqReadiness::Stale => return EncodeAvailability::RfqStale,
            }
        }

        EncodeAvailability::Ready
    }

    pub async fn wait_for_readiness(&self, wait: Duration) -> bool {
        if self.is_ready().await {
            return true;
        }

        let deadline = Instant::now() + wait;
        loop {
            let now = Instant::now();
            if now >= deadline {
                return self.is_ready().await;
            }

            let remaining = deadline.saturating_duration_since(now);
            let sleep_for = remaining.min(Duration::from_millis(READINESS_POLL_INTERVAL_MS));
            tokio::time::sleep(sleep_for).await;

            if self.is_ready().await {
                return true;
            }
        }
    }

    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    pub fn vm_simulation_rebuild_gate(&self) -> Arc<RwLock<()>> {
        Arc::clone(&self.vm_simulation_rebuild_gate)
    }

    pub fn rfq_simulation_rebuild_gate(&self) -> Arc<RwLock<()>> {
        Arc::clone(&self.rfq_simulation_rebuild_gate)
    }

    pub(crate) async fn acquire_simulation_rebuild_guard(
        &self,
        uses_vm: bool,
        uses_rfq: bool,
    ) -> Arc<SimulationRebuildGuard> {
        let vm_read_guard = if uses_vm {
            Some(self.vm_simulation_rebuild_gate.clone().read_owned().await)
        } else {
            None
        };
        let rfq_read_guard = if uses_rfq {
            Some(self.rfq_simulation_rebuild_gate.clone().read_owned().await)
        } else {
            None
        };
        Arc::new(SimulationRebuildGuard {
            _vm_read_guard: vm_read_guard,
            _rfq_read_guard: rfq_read_guard,
        })
    }

    pub(crate) async fn pool_rebuild_guard_requirements(
        &self,
        id: &str,
    ) -> Result<Option<(bool, bool)>, EncodeAvailability> {
        if self.native_state_store.has_pool(id).await {
            return match self.native_readiness().await {
                NativeReadiness::Ready => Ok(Some((false, false))),
                NativeReadiness::WarmingUp => Err(EncodeAvailability::NativeWarmingUp),
                NativeReadiness::Stale => Err(EncodeAvailability::NativeStale),
            };
        }

        if self.vm_state_store.has_pool(id).await {
            return match self.vm_readiness().await {
                VmReadiness::Disabled => Err(EncodeAvailability::VmDisabled),
                VmReadiness::WarmingUp => Err(EncodeAvailability::VmWarmingUp),
                VmReadiness::Rebuilding => Err(EncodeAvailability::VmRebuilding),
                VmReadiness::Ready => Ok(Some((true, false))),
                VmReadiness::Stale => Err(EncodeAvailability::VmStale),
            };
        }

        if self.rfq_state_store.has_pool(id).await {
            return match self.rfq_readiness().await {
                RfqReadiness::Disabled => Err(EncodeAvailability::RfqDisabled),
                RfqReadiness::WarmingUp => Err(EncodeAvailability::RfqWarmingUp),
                RfqReadiness::Rebuilding => Err(EncodeAvailability::RfqRebuilding),
                RfqReadiness::Ready => Ok(Some((false, true))),
                RfqReadiness::Stale => Err(EncodeAvailability::RfqStale),
            };
        }

        Ok(None)
    }

    pub async fn pool_by_id(&self, id: &str) -> Result<Option<PoolEntry>, EncodeAvailability> {
        if self.native_state_store.has_pool(id).await {
            return match self.native_readiness().await {
                NativeReadiness::Ready => Ok(self.native_state_store.pool_by_id(id).await),
                NativeReadiness::WarmingUp => Err(EncodeAvailability::NativeWarmingUp),
                NativeReadiness::Stale => Err(EncodeAvailability::NativeStale),
            };
        }

        // Route hints can miss VM/RFQ backends, so encode lookup needs the actual backend
        // readiness here to distinguish "unavailable" from "pool not found".
        if self.vm_state_store.has_pool(id).await {
            return match self.vm_readiness().await {
                VmReadiness::Disabled => Err(EncodeAvailability::VmDisabled),
                VmReadiness::WarmingUp => Err(EncodeAvailability::VmWarmingUp),
                VmReadiness::Rebuilding => Err(EncodeAvailability::VmRebuilding),
                VmReadiness::Ready => Ok(self.vm_state_store.pool_by_id(id).await),
                VmReadiness::Stale => Err(EncodeAvailability::VmStale),
            };
        }

        if self.rfq_state_store.has_pool(id).await {
            return match self.rfq_readiness().await {
                RfqReadiness::Disabled => Err(EncodeAvailability::RfqDisabled),
                RfqReadiness::WarmingUp => Err(EncodeAvailability::RfqWarmingUp),
                RfqReadiness::Rebuilding => Err(EncodeAvailability::RfqRebuilding),
                RfqReadiness::Ready => Ok(self.rfq_state_store.pool_by_id(id).await),
                RfqReadiness::Stale => Err(EncodeAvailability::RfqStale),
            };
        }

        Ok(None)
    }

    pub async fn vm_ready(&self) -> bool {
        matches!(self.vm_readiness().await, VmReadiness::Ready)
    }

    pub async fn rfq_ready(&self) -> bool {
        matches!(self.rfq_readiness().await, RfqReadiness::Ready)
    }

    pub async fn vm_rebuilding(&self) -> bool {
        self.vm_stream.read().await.rebuilding
    }

    pub async fn vm_block(&self) -> u64 {
        self.vm_state_store.current_block().await
    }

    pub async fn rfq_update_timestamp(&self) -> u64 {
        self.rfq_state_store.current_block().await
    }

    pub async fn vm_pools(&self) -> usize {
        self.vm_state_store.total_states().await
    }

    pub async fn rfq_pools(&self) -> usize {
        self.rfq_state_store.total_states().await
    }
}

fn is_update_stale(last_update_age_ms: Option<u64>, readiness_stale_ms: u64) -> bool {
    last_update_age_ms
        .map(|age| age >= readiness_stale_ms)
        .unwrap_or(true)
}

fn subscription_readiness_reason(
    subscription: &BroadcasterSubscriptionSnapshot,
) -> Option<SimulatorReadinessReason> {
    if subscription.redis_gap_reason.is_some() {
        Some(SimulatorReadinessReason::RedisReplayGap)
    } else if !subscription.connected {
        Some(SimulatorReadinessReason::BroadcasterDisconnected)
    } else if !subscription.bootstrap_complete {
        Some(SimulatorReadinessReason::SnapshotBootstrapping)
    } else if !subscription.redis_replay_caught_up {
        Some(SimulatorReadinessReason::RedisReplayCatchingUp)
    } else {
        None
    }
}

fn service_status_from_backends(
    backends: &[SimulatorBackendStatusSnapshot],
) -> SimulatorServiceStatus {
    match backends
        .first()
        .map(|backend| backend.readiness)
        .unwrap_or(SimulatorBackendReadiness::WarmingUp)
    {
        SimulatorBackendReadiness::Ready => SimulatorServiceStatus::Ready,
        SimulatorBackendReadiness::Stale => SimulatorServiceStatus::Stale,
        _ => SimulatorServiceStatus::WarmingUp,
    }
}

impl From<BroadcasterSubscriptionSnapshot> for SimulatorBackendSubscriptionSnapshot {
    fn from(snapshot: BroadcasterSubscriptionSnapshot) -> Self {
        Self {
            connected: snapshot.connected,
            bootstrap_complete: snapshot.bootstrap_complete,
            stream_id: snapshot.stream_id,
            snapshot_id: snapshot.snapshot_id,
            redis_replay_boundary: snapshot.redis_replay_boundary,
            redis_replay_checkpoint: snapshot.redis_replay_checkpoint,
            redis_replay_caught_up: snapshot.redis_replay_caught_up,
            redis_gap_reason: snapshot.redis_gap_reason,
            redis_transport_status: snapshot.redis_transport_status,
            redis_transport_retry_count: snapshot.redis_transport_retry_count,
            redis_transport_last_error: snapshot.redis_transport_last_error,
            restart_count: snapshot.restart_count,
            last_error: snapshot.last_error,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct VmStreamStatus {
    pub rebuilding: bool,
    pub restart_count: u64,
    pub last_error: Option<String>,
    pub rebuild_started_at: Option<Instant>,
}

#[derive(Clone)]
struct PublishedStateStore {
    version: u64,
    request_generation: u64,
    requests_allowed: bool,
    point: StatePoint,
    ready: bool,
}

impl PublishedStateStore {
    fn empty(tokens: HashMap<Bytes, Token>, wrapped_native_token: Option<Bytes>) -> Self {
        Self {
            version: 0,
            request_generation: 0,
            requests_allowed: false,
            point: ReplayWorld::new(tokens, wrapped_native_token).pin(),
            ready: false,
        }
    }
}

#[derive(Clone)]
pub(crate) struct PublishedStatePin {
    state: Arc<PublishedStateStore>,
}

impl PublishedStatePin {
    #[cfg(test)]
    pub(crate) fn version(&self) -> u64 {
        self.state.version
    }

    pub(crate) fn request_generation(&self) -> u64 {
        self.state.request_generation
    }

    pub(crate) fn current_block(&self) -> u64 {
        self.state.point.current_block()
    }

    pub(crate) fn total_states(&self) -> usize {
        self.state.point.total_states()
    }

    pub(crate) fn token(&self, address: &Bytes) -> Option<Token> {
        self.state.point.token(address)
    }

    pub(crate) fn matching_pools_by_addresses(
        &self,
        token_in: &Bytes,
        token_out: &Bytes,
    ) -> Vec<(String, PoolEntry)> {
        self.state
            .point
            .matching_pools_by_addresses(token_in, token_out)
    }

    pub(crate) fn pool_by_id(&self, id: &str) -> Option<PoolEntry> {
        self.state.point.pool_by_id(id)
    }

    /// Pointer identity is a valid fence because updates replace state and component Arcs instead
    /// of mutating them. `get_amount_out` borrows state and returns a separate state after the
    /// swap, while delta application requires `&mut self` upstream. Native protocol states do not
    /// use interior mutability, which the invariant test below enforces with a real implementation.
    pub(crate) fn changed_pool_ids(
        &self,
        current: &Self,
        pool_ids: &HashSet<String>,
    ) -> HashSet<String> {
        pool_ids
            .iter()
            .filter(|pool_id| {
                let pinned_entry = self.pool_by_id(pool_id);
                let current_entry = current.pool_by_id(pool_id);
                match (pinned_entry, current_entry) {
                    (None, None) => {
                        // Neither pin has an identity that could have moved during encode.
                        false
                    }
                    (None, Some(_)) | (Some(_), None) => true,
                    (
                        Some((pinned_state, pinned_component)),
                        Some((current_state, current_component)),
                    ) => {
                        // Component identity catches pools removed and re-added between pins.
                        !Arc::ptr_eq(&pinned_state, &current_state)
                            || !Arc::ptr_eq(&pinned_component, &current_component)
                    }
                }
            })
            .cloned()
            .collect()
    }
}

pub type UpdateMetrics = ApplyReport;

pub struct StateStore {
    tokens: Arc<TokenStore>,
    publish_tokens_to_store: bool,
    published: RwLock<Arc<PublishedStateStore>>,
    update_guard: Mutex<()>,
    ready_tx: watch::Sender<bool>,
}

impl StateStore {
    pub fn new(tokens: Arc<TokenStore>) -> Self {
        let initial_tokens = tokens.initial_snapshot();
        Self::new_with_token_publication(tokens, initial_tokens, true)
    }

    #[cfg(test)]
    pub(crate) fn new_private(tokens: Arc<TokenStore>) -> Self {
        let initial_tokens = tokens.initial_snapshot();
        Self::new_with_token_publication(tokens, initial_tokens, false)
    }

    pub(crate) fn new_private_with_snapshot(
        tokens: Arc<TokenStore>,
        initial_tokens: HashMap<Bytes, Token>,
    ) -> Self {
        Self::new_with_token_publication(tokens, initial_tokens, false)
    }

    fn new_with_token_publication(
        tokens: Arc<TokenStore>,
        initial_tokens: HashMap<Bytes, Token>,
        publish_tokens_to_store: bool,
    ) -> Self {
        let wrapped_native_token = tokens.wrapped_native_token();
        let (ready_tx, _) = watch::channel(false);
        StateStore {
            tokens,
            publish_tokens_to_store,
            published: RwLock::new(Arc::new(PublishedStateStore::empty(
                initial_tokens,
                wrapped_native_token,
            ))),
            update_guard: Mutex::new(()),
            ready_tx,
        }
    }

    pub(crate) async fn pin(&self) -> PublishedStatePin {
        PublishedStatePin {
            state: Arc::clone(&*self.published.read().await),
        }
    }

    pub(crate) async fn state_version(&self) -> u64 {
        self.published.read().await.version
    }

    #[cfg(test)]
    pub(crate) async fn request_generation(&self) -> u64 {
        self.published.read().await.request_generation
    }

    pub(crate) async fn fence_requests(&self) {
        let _guard = self.update_guard.lock().await;
        let current = Arc::clone(&*self.published.read().await);
        let mut replacement = (*current).clone();
        replacement.requests_allowed = false;
        self.publish(replacement).await;
    }

    async fn request_snapshot(&self) -> (bool, bool, u64) {
        let current = self.published.read().await;
        (
            current.ready,
            current.requests_allowed,
            current.request_generation,
        )
    }

    pub(crate) async fn align_bootstrap_state_version(&self, state_version: u64) {
        let _guard = self.update_guard.lock().await;
        let current = Arc::clone(&*self.published.read().await);
        let mut replacement = (*current).clone();
        replacement.version = state_version;
        replacement.requests_allowed = replacement.ready;
        self.publish(replacement).await;
    }

    pub async fn reset(&self) {
        let _guard = self.update_guard.lock().await;
        let current = Arc::clone(&*self.published.read().await);
        let mut world = ReplayWorld::from_point(&current.point);
        world.clear();
        let mut replacement = (*current).clone();
        replacement.point = world.pin();
        replacement.version = current.version.saturating_add(1);
        replacement.ready = false;
        replacement.requests_allowed = false;
        self.publish(replacement).await;
    }

    pub async fn reset_protocols(&self, protocols: &[ProtocolKind]) {
        if protocols.is_empty() {
            return;
        }

        if protocols.len() >= ProtocolKind::ALL.len() {
            self.reset().await;
            return;
        }

        let _guard = self.update_guard.lock().await;
        let current = Arc::clone(&*self.published.read().await);
        let mut world = ReplayWorld::from_point(&current.point);
        if !world.reset_protocols(protocols) {
            return;
        }
        let mut replacement = (*current).clone();
        replacement.point = world.pin();
        replacement.version = current.version.saturating_add(1);
        replacement.ready = replacement.point.total_states() > 0;
        replacement.requests_allowed = replacement.ready;
        self.publish(replacement).await;
    }

    pub async fn apply_update(&self, update: Update) -> UpdateMetrics {
        let _guard = self.update_guard.lock().await;
        let current = Arc::clone(&*self.published.read().await);
        let state_version = current.version.saturating_add(1);
        self.apply_update_locked(update, current, state_version)
            .await
    }

    pub(crate) async fn apply_update_at_version(
        &self,
        update: Update,
        state_version: u64,
    ) -> anyhow::Result<UpdateMetrics> {
        let _guard = self.update_guard.lock().await;
        let current = Arc::clone(&*self.published.read().await);
        anyhow::ensure!(
            state_version > current.version,
            "state version {state_version} must be newer than published version {}",
            current.version
        );
        Ok(self
            .apply_update_locked(update, current, state_version)
            .await)
    }

    async fn apply_update_locked(
        &self,
        update: Update,
        current: Arc<PublishedStateStore>,
        state_version: u64,
    ) -> UpdateMetrics {
        let mut world = ReplayWorld::from_point(&current.point);
        let report = world.apply(update);
        if self.publish_tokens_to_store && !report.tokens_to_cache().is_empty() {
            self.tokens
                .insert_batch(report.tokens_to_cache().iter().cloned())
                .await;
        }

        let mut replacement = (*current).clone();
        replacement.version = state_version;
        replacement.point = world.pin();
        replacement.ready = report.total_pairs > 0;
        replacement.requests_allowed = replacement.ready;
        self.publish(replacement).await;
        report
    }

    async fn publish(&self, mut replacement: PublishedStateStore) {
        let current = Arc::clone(&*self.published.read().await);
        replacement.request_generation = current.request_generation.saturating_add(1);
        let ready = replacement.ready;
        let previous_ready = current.ready;
        *self.published.write().await = Arc::new(replacement);
        if previous_ready != ready {
            self.ready_tx.send_replace(ready);
        }
    }

    pub(crate) async fn publish_candidate(
        &self,
        candidate: PublishedStatePin,
        target_version: u64,
    ) -> anyhow::Result<()> {
        let _guard = self.update_guard.lock().await;
        let current_version = self.published.read().await.version;
        anyhow::ensure!(
            target_version > current_version,
            "candidate state version {target_version} must be newer than published version {current_version}"
        );
        let mut replacement = (*candidate.state).clone();
        replacement.version = target_version;
        replacement.requests_allowed = replacement.ready;
        let tokens = replacement.point.tokens().into_values().collect::<Vec<_>>();
        self.publish(replacement).await;
        self.tokens.insert_batch(tokens).await;
        Ok(())
    }

    pub async fn current_block(&self) -> u64 {
        self.published.read().await.point.current_block()
    }

    pub async fn total_states(&self) -> usize {
        self.published.read().await.point.total_states()
    }

    pub async fn has_pool(&self, id: &str) -> bool {
        self.published.read().await.point.pool_by_id(id).is_some()
    }

    pub fn is_ready(&self) -> bool {
        self.ready_tx.borrow().to_owned()
    }

    pub async fn wait_until_ready(&self, wait: Duration) -> bool {
        if self.is_ready() {
            return true;
        }
        let mut rx = self.ready_tx.subscribe();
        let sleep = tokio::time::sleep(wait);
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                changed = rx.changed() => {
                    if changed.is_err() {
                        return self.is_ready();
                    }
                    if *rx.borrow() {
                        return true;
                    }
                }
                _ = sleep.as_mut() => {
                    return self.is_ready();
                }
            }
        }
    }

    pub(crate) async fn matching_pools_by_addresses(
        &self,
        token_in: &Bytes,
        token_out: &Bytes,
    ) -> Vec<(String, PoolEntry)> {
        self.pin()
            .await
            .matching_pools_by_addresses(token_in, token_out)
    }

    pub(crate) async fn pool_by_id(&self, id: &str) -> Option<PoolEntry> {
        self.pin().await.pool_by_id(id)
    }

    #[cfg(test)]
    pub(crate) async fn pool_ids_by_protocol_system(&self, protocol_system: &str) -> Vec<String> {
        self.published
            .read()
            .await
            .point
            .pool_ids_by_protocol_system(protocol_system)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Duration;

    use num_bigint::BigUint;
    use tycho_simulation::{
        evm::protocol::uniswap_v2::state::UniswapV2State,
        protocol::models::{DecoderContext, ProtocolComponent, TryFromWithBlock},
        tycho_client::feed::{
            dto::ComponentWithState as DtoComponentWithState, synchronizer::ComponentWithState,
            BlockHeader,
        },
        tycho_common::{
            dto::ProtocolStateDelta,
            models::{token::Token, Chain},
            simulation::{
                errors::{SimulationError, TransitionError},
                protocol_sim::{Balances, GetAmountOutResult, ProtocolSim},
            },
            Bytes,
        },
    };

    #[tokio::test]
    async fn redis_transport_failure_preserves_replay_state() {
        let status = BroadcasterSubscriptionStatus::default();
        let boundary = replay_boundary(7);
        status
            .mark_bootstrap_complete_with_redis_boundary(boundary.clone())
            .await;
        status.mark_redis_catch_up_checkpoint("1-7").await;

        status
            .mark_redis_transport_error("Redis XINFO STREAM failed: timeout")
            .await;

        let failed = status.snapshot().await;
        assert!(failed.connected);
        assert!(failed.bootstrap_complete);
        assert_eq!(failed.redis_replay_boundary, Some(boundary));
        assert_eq!(failed.redis_replay_checkpoint.as_deref(), Some("1-7"));
        assert!(failed.redis_replay_caught_up);
        assert_eq!(
            failed.redis_transport_status,
            RedisTransportStatus::Retrying
        );
        assert_eq!(failed.redis_transport_retry_count, 1);
        assert!(failed.redis_transport_last_error.is_some());

        status.mark_redis_transport_recovered().await;

        let recovered = status.snapshot().await;
        assert_eq!(
            recovered.redis_transport_status,
            RedisTransportStatus::Connected
        );
        assert_eq!(recovered.redis_transport_retry_count, 1);
        assert!(recovered.redis_transport_last_error.is_none());
    }

    #[tokio::test]
    async fn subscription_status_records_and_clears_publisher_delay() {
        let status = BroadcasterSubscriptionStatus::default();

        status.record_publisher_to_consumer_delay(u64::MAX).await;
        assert_eq!(
            status.snapshot().await.publisher_to_consumer_delay_ms,
            Some(0)
        );

        status.mark_disconnected(None).await;
        assert_eq!(status.snapshot().await.publisher_to_consumer_delay_ms, None);
    }

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

    fn address(seed: u8) -> Bytes {
        Bytes::from([seed; 20])
    }

    fn mk_token(seed: u8, symbol: &str) -> Token {
        Token::new(&address(seed), symbol, 18, 0, &[], Chain::Ethereum, 100)
    }

    fn mk_component(
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
                .unwrap_or_else(|| unreachable!("valid timestamp"))
                .naive_utc(),
        )
    }

    fn mk_update(pairs: Vec<(String, ProtocolComponent, Box<dyn ProtocolSim>)>) -> Update {
        let mut states = HashMap::new();
        let mut new_pairs = HashMap::new();
        for (id, component, state) in pairs {
            states.insert(id.clone(), state);
            new_pairs.insert(id, component);
        }
        Update::new(1, states, new_pairs)
    }

    fn replay_boundary(exclusive_message_seq: u64) -> BroadcasterRedisReplayBoundary {
        BroadcasterRedisReplayBoundary::new(
            "redis-stream",
            "stream-2",
            "snapshot-2",
            1,
            exclusive_message_seq,
            exclusive_message_seq,
        )
        .unwrap_or_else(|error| unreachable!("valid replay boundary: {error}"))
    }

    struct TestAppStateStores {
        token_store: Arc<TokenStore>,
        native_state_store: Arc<StateStore>,
        vm_state_store: Arc<StateStore>,
        rfq_state_store: Arc<StateStore>,
    }

    struct PoolFlags {
        enable_vm_pools: bool,
        enable_rfq_pools: bool,
    }

    fn build_test_app_state(stores: TestAppStateStores, flags: PoolFlags) -> AppState {
        AppState {
            chain: Chain::Ethereum,
            rfq_client_config: Arc::new(RfqClientConfig::default()),
            native_token_protocol_allowlist: Arc::new(vec!["rocketpool".to_string()]),
            tokens: stores.token_store,
            native_broadcaster_subscription: BroadcasterSubscriptionStatus::ready_for_test(),
            vm_broadcaster_subscription: BroadcasterSubscriptionStatus::ready_for_test(),
            rfq_broadcaster_subscription: BroadcasterSubscriptionStatus::ready_for_test(),
            native_state_store: stores.native_state_store,
            vm_state_store: stores.vm_state_store,
            rfq_state_store: stores.rfq_state_store,
            native_stream_health: Arc::new(StreamHealth::ready_for_test(1)),
            vm_stream_health: Arc::new(StreamHealth::ready_for_test(1)),
            rfq_stream_health: Arc::new(StreamHealth::ready_for_test(1)),
            vm_stream: Arc::new(RwLock::new(VmStreamStatus::default())),
            configured_backends: ConfiguredBackends {
                vm: flags.enable_vm_pools,
                rfq: flags.enable_rfq_pools,
            },
            enable_vm_pools: flags.enable_vm_pools,
            enable_rfq_pools: flags.enable_rfq_pools,
            native_progress_lease: Duration::from_secs(120),
            optional_backend_stale: Duration::from_secs(120),
            request_timeout: Duration::from_millis(1000),
            vm_simulation_rebuild_gate: Arc::new(RwLock::new(())),
            rfq_simulation_rebuild_gate: Arc::new(RwLock::new(())),
            slippage: SlippageConfig::default(),
            erc4626_deposits_enabled: false,
            erc4626_pair_policies: Arc::new(Vec::new()),
            reset_allowance_tokens: Arc::new(HashMap::new()),
        }
    }

    async fn build_readiness_test_state(enable_vm_pools: bool, enable_rfq_pools: bool) -> AppState {
        let token_store = Arc::new(TokenStore::new(
            HashMap::new(),
            "http://localhost".to_string(),
            "test".to_string(),
            Chain::Ethereum,
            Duration::from_millis(10),
        ));
        let native_state_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
        let vm_state_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
        let rfq_state_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
        let native_component = mk_component(
            28,
            "uniswap_v2",
            "uniswap_v2_pool",
            vec![mk_token(29, "TKNA"), mk_token(30, "TKNB")],
        );
        native_state_store
            .apply_update(mk_update(vec![(
                "pool-native".to_string(),
                native_component,
                Box::new(DummySim),
            )]))
            .await;

        build_test_app_state(
            TestAppStateStores {
                token_store,
                native_state_store,
                vm_state_store,
                rfq_state_store,
            },
            PoolFlags {
                enable_vm_pools,
                enable_rfq_pools,
            },
        )
    }

    async fn build_ready_state_with_vm_and_rfq() -> AppState {
        let state = build_readiness_test_state(true, true).await;
        state.native_stream_health.record_update(1).await;
        state
            .vm_state_store
            .apply_update(mk_update(vec![(
                "pool-vm".to_string(),
                mk_component(
                    38,
                    "vm:curve",
                    "curve_pool",
                    vec![mk_token(39, "TKNA"), mk_token(40, "TKNB")],
                ),
                Box::new(DummySim),
            )]))
            .await;
        state
            .rfq_state_store
            .apply_update(mk_update(vec![(
                "pool-rfq".to_string(),
                mk_component(
                    41,
                    "rfq:hashflow",
                    "hashflow_pool",
                    vec![mk_token(42, "TKNA"), mk_token(43, "TKNB")],
                ),
                Box::new(DummySim),
            )]))
            .await;
        state.vm_stream_health.record_update(1).await;
        state.rfq_stream_health.record_update(1).await;
        state
    }

    async fn assert_optional_backend_gap_does_not_close_native_readiness(
        backend: SimulatorBackendKind,
        gap_reason: &'static str,
    ) {
        let state = build_ready_state_with_vm_and_rfq().await;
        assert!(state.is_ready().await);
        assert_eq!(
            state.status_snapshot().await.status,
            SimulatorServiceStatus::Ready
        );

        match backend {
            SimulatorBackendKind::Vm => {
                state
                    .vm_broadcaster_subscription
                    .mark_redis_gap(gap_reason)
                    .await;
            }
            SimulatorBackendKind::Rfq => {
                state
                    .rfq_broadcaster_subscription
                    .mark_redis_gap(gap_reason)
                    .await;
            }
            SimulatorBackendKind::Native => {
                state
                    .native_broadcaster_subscription
                    .mark_redis_gap(gap_reason)
                    .await;
            }
        }

        assert!(state.is_ready().await);
        let snapshot = state.status_snapshot().await;
        let backend_snapshot = backend_snapshot(&snapshot, backend);
        assert_eq!(snapshot.status, SimulatorServiceStatus::Ready);
        assert_eq!(
            backend_snapshot.reason,
            Some(SimulatorReadinessReason::RedisReplayGap)
        );
    }

    #[tokio::test]
    async fn simulation_rebuild_guard_holds_and_releases_vm_read_guard() {
        let state = build_readiness_test_state(true, true).await;

        let guard = state.acquire_simulation_rebuild_guard(true, false).await;
        let vm_gate = state.vm_simulation_rebuild_gate();
        let rfq_gate = state.rfq_simulation_rebuild_gate();

        assert!(
            vm_gate.try_write().is_err(),
            "VM guard should block rebuild writers"
        );
        assert!(
            rfq_gate.try_write().is_ok(),
            "VM-only guard should not block RFQ rebuild writers"
        );

        drop(guard);

        assert!(
            vm_gate.try_write().is_ok(),
            "VM rebuild writer should acquire after guard drop"
        );
    }

    #[tokio::test]
    async fn simulation_rebuild_guard_holds_and_releases_rfq_read_guard() {
        let state = build_readiness_test_state(true, true).await;

        let guard = state.acquire_simulation_rebuild_guard(false, true).await;
        let vm_gate = state.vm_simulation_rebuild_gate();
        let rfq_gate = state.rfq_simulation_rebuild_gate();

        assert!(
            vm_gate.try_write().is_ok(),
            "RFQ-only guard should not block VM rebuild writers"
        );
        assert!(
            rfq_gate.try_write().is_err(),
            "RFQ-only guard should block RFQ rebuild writers"
        );

        drop(guard);

        assert!(
            rfq_gate.try_write().is_ok(),
            "RFQ rebuild writer should acquire after guard drop"
        );
    }

    #[tokio::test]
    async fn simulation_rebuild_guard_holds_both_guards_for_mixed_routes() {
        let state = build_readiness_test_state(true, true).await;

        let guard = state.acquire_simulation_rebuild_guard(true, true).await;
        let vm_gate = state.vm_simulation_rebuild_gate();
        let rfq_gate = state.rfq_simulation_rebuild_gate();

        assert!(
            vm_gate.try_write().is_err(),
            "mixed guard should block VM rebuild writers"
        );
        assert!(
            rfq_gate.try_write().is_err(),
            "mixed guard should block RFQ rebuild writers"
        );

        drop(guard);

        assert!(
            vm_gate.try_write().is_ok(),
            "VM rebuild writer should acquire after mixed guard drop"
        );
        assert!(
            rfq_gate.try_write().is_ok(),
            "RFQ rebuild writer should acquire after mixed guard drop"
        );
    }

    #[tokio::test]
    async fn simulation_rebuild_guard_is_noop_for_native_only_reads() {
        let state = build_readiness_test_state(true, true).await;

        let guard = state.acquire_simulation_rebuild_guard(false, false).await;
        let vm_gate = state.vm_simulation_rebuild_gate();
        let rfq_gate = state.rfq_simulation_rebuild_gate();

        assert!(
            vm_gate.try_write().is_ok(),
            "native-only guard should not block VM rebuild writers"
        );
        assert!(
            rfq_gate.try_write().is_ok(),
            "native-only guard should not block RFQ rebuild writers"
        );

        drop(guard);
    }

    fn backend_snapshot(
        snapshot: &SimulatorStatusSnapshot,
        kind: SimulatorBackendKind,
    ) -> &SimulatorBackendStatusSnapshot {
        snapshot
            .backends
            .iter()
            .find(|backend| backend.kind == kind)
            .unwrap_or_else(|| unreachable!("status snapshot must include {kind:?} backend"))
    }

    #[tokio::test]
    async fn native_readiness_distinguishes_ready_stale_and_warming_up() {
        let warming_up_state = {
            let token_store = Arc::new(TokenStore::new(
                HashMap::new(),
                "http://localhost".to_string(),
                "test".to_string(),
                Chain::Ethereum,
                Duration::from_millis(10),
            ));
            let native_state_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
            let vm_state_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
            let rfq_state_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
            build_test_app_state(
                TestAppStateStores {
                    token_store,
                    native_state_store,
                    vm_state_store,
                    rfq_state_store,
                },
                PoolFlags {
                    enable_vm_pools: false,
                    enable_rfq_pools: false,
                },
            )
        };
        assert_eq!(
            warming_up_state.native_readiness().await,
            NativeReadiness::WarmingUp
        );

        let mut ready_state = build_readiness_test_state(false, false).await;
        ready_state.native_stream_health.record_update(1).await;
        assert_eq!(ready_state.native_readiness().await, NativeReadiness::Ready);

        ready_state.native_progress_lease = Duration::ZERO;
        assert_eq!(ready_state.native_readiness().await, NativeReadiness::Stale);
    }

    #[tokio::test]
    async fn native_readiness_requires_connected_bootstrapped_broadcaster_subscription() {
        let state = build_readiness_test_state(false, false).await;
        state.native_stream_health.record_update(1).await;
        assert!(state.is_ready().await);

        state
            .native_broadcaster_subscription
            .mark_disconnected(None)
            .await;
        assert!(!state.is_ready().await);
        assert_eq!(state.native_readiness().await, NativeReadiness::WarmingUp);

        state.native_broadcaster_subscription.mark_connected().await;
        assert!(!state.is_ready().await);
        assert_eq!(state.native_readiness().await, NativeReadiness::WarmingUp);

        state
            .native_broadcaster_subscription
            .mark_snapshot_started("stream-2", "snapshot-2")
            .await;
        assert!(!state.is_ready().await);
        assert_eq!(state.native_readiness().await, NativeReadiness::WarmingUp);

        state
            .native_broadcaster_subscription
            .mark_bootstrap_complete_with_redis_boundary(replay_boundary(1))
            .await;
        state
            .native_broadcaster_subscription
            .mark_redis_catch_up_checkpoint("1-1")
            .await;
        assert!(state.is_ready().await);
        assert_eq!(state.native_readiness().await, NativeReadiness::Ready);
    }

    #[tokio::test]
    async fn native_readiness_waits_for_redis_catch_up_after_replay_boundary() {
        let state = build_readiness_test_state(false, false).await;
        state.native_stream_health.record_update(1).await;
        state
            .native_broadcaster_subscription
            .mark_snapshot_started("stream-2", "snapshot-2")
            .await;
        state
            .native_broadcaster_subscription
            .mark_bootstrap_complete_with_redis_boundary(replay_boundary(7))
            .await;

        assert!(!state.is_ready().await);
        let snapshot = state.status_snapshot().await;
        let native = backend_snapshot(&snapshot, SimulatorBackendKind::Native);
        assert_eq!(native.readiness, SimulatorBackendReadiness::WarmingUp);
        assert_eq!(
            native.reason,
            Some(SimulatorReadinessReason::RedisReplayCatchingUp)
        );

        state
            .native_broadcaster_subscription
            .mark_redis_catch_up_checkpoint("1-7")
            .await;

        assert!(state.is_ready().await);
        assert_eq!(state.native_readiness().await, NativeReadiness::Ready);
    }

    #[tokio::test]
    async fn native_readiness_surfaces_redis_replay_gap() {
        let state = build_readiness_test_state(false, false).await;
        state.native_stream_health.record_update(1).await;
        state
            .native_broadcaster_subscription
            .mark_snapshot_started("stream-2", "snapshot-2")
            .await;
        state
            .native_broadcaster_subscription
            .mark_bootstrap_complete_with_redis_boundary(replay_boundary(7))
            .await;
        state
            .native_broadcaster_subscription
            .mark_disconnected(Some("Redis replay gap: expected message_seq 8".to_string()))
            .await;
        state
            .native_broadcaster_subscription
            .mark_redis_gap("Redis replay gap: expected message_seq 8")
            .await;

        assert!(!state.is_ready().await);
        let snapshot = state.status_snapshot().await;
        let native = backend_snapshot(&snapshot, SimulatorBackendKind::Native);
        assert_eq!(native.readiness, SimulatorBackendReadiness::WarmingUp);
        assert_eq!(
            native.reason,
            Some(SimulatorReadinessReason::RedisReplayGap)
        );
        assert_eq!(
            native
                .subscription
                .as_ref()
                .and_then(|subscription| subscription.redis_gap_reason.as_deref()),
            Some("Redis replay gap: expected message_seq 8")
        );
    }

    #[tokio::test]
    async fn optional_backend_redis_gap_does_not_close_native_readiness() {
        assert_optional_backend_gap_does_not_close_native_readiness(
            SimulatorBackendKind::Vm,
            "VM Redis replay gap",
        )
        .await;
        assert_optional_backend_gap_does_not_close_native_readiness(
            SimulatorBackendKind::Rfq,
            "RFQ Redis replay gap",
        )
        .await;
    }

    #[tokio::test]
    async fn native_readiness_clears_stale_redis_gap_on_plain_disconnect() {
        let state = build_readiness_test_state(false, false).await;
        state.native_stream_health.record_update(1).await;
        state
            .native_broadcaster_subscription
            .mark_snapshot_started("stream-2", "snapshot-2")
            .await;
        state
            .native_broadcaster_subscription
            .mark_bootstrap_complete_with_redis_boundary(replay_boundary(7))
            .await;
        state
            .native_broadcaster_subscription
            .mark_redis_gap("Redis replay gap: expected message_seq 8")
            .await;
        state
            .native_broadcaster_subscription
            .mark_disconnected(Some("Redis connection failed".to_string()))
            .await;

        let snapshot = state.status_snapshot().await;
        let native = backend_snapshot(&snapshot, SimulatorBackendKind::Native);
        assert_eq!(
            native.reason,
            Some(SimulatorReadinessReason::BroadcasterDisconnected)
        );
        assert!(native
            .subscription
            .as_ref()
            .and_then(|subscription| subscription.redis_gap_reason.as_deref())
            .is_none());
        assert!(native.subscription.is_some());
        if let Some(subscription) = native.subscription.as_ref() {
            assert!(subscription.stream_id.is_none());
            assert!(subscription.redis_replay_boundary.is_none());
        }
    }

    #[tokio::test]
    async fn native_status_keeps_backend_and_subscription_restart_counts_separate() {
        let state = build_readiness_test_state(false, false).await;
        state
            .native_broadcaster_subscription
            .mark_disconnected(Some("native broadcaster dropped".to_string()))
            .await;
        state.native_stream_health.increment_restart().await;

        let snapshot = state.status_snapshot().await;
        let native = backend_snapshot(&snapshot, SimulatorBackendKind::Native);

        assert_eq!(native.restart_count, 1);
        assert_eq!(
            native
                .subscription
                .as_ref()
                .unwrap_or_else(|| unreachable!("native status must include subscription"))
                .restart_count,
            1
        );
    }

    #[tokio::test]
    async fn vm_status_keeps_backend_and_subscription_restart_counts_separate() {
        let state = build_readiness_test_state(true, false).await;
        state
            .vm_broadcaster_subscription
            .mark_disconnected(Some("vm broadcaster dropped".to_string()))
            .await;
        {
            let mut vm_status = state.vm_stream.write().await;
            vm_status.restart_count = 1;
        }

        let snapshot = state.status_snapshot().await;
        let vm = backend_snapshot(&snapshot, SimulatorBackendKind::Vm);

        assert_eq!(vm.restart_count, 1);
        assert_eq!(
            vm.subscription
                .as_ref()
                .unwrap_or_else(|| unreachable!("VM status must include subscription"))
                .restart_count,
            1
        );
    }

    #[tokio::test]
    async fn rfq_and_vm_readiness_distinguishes_disabled_warming_up_ready_and_stale() {
        let disabled_state = build_readiness_test_state(false, false).await;
        assert_eq!(disabled_state.vm_readiness().await, VmReadiness::Disabled);
        assert_eq!(disabled_state.rfq_readiness().await, RfqReadiness::Disabled);

        let mut state = build_readiness_test_state(true, true).await;
        assert_eq!(state.vm_readiness().await, VmReadiness::WarmingUp);
        assert_eq!(state.rfq_readiness().await, RfqReadiness::WarmingUp);

        let vm_component = mk_component(
            31,
            "vm:curve",
            "curve_pool",
            vec![mk_token(32, "TKNA"), mk_token(33, "TKNB")],
        );
        let rfq_component = mk_component(
            31,
            "rfq:hashflow",
            "hashflow",
            vec![mk_token(32, "TKNA"), mk_token(33, "TKNB")],
        );
        state
            .vm_state_store
            .apply_update(mk_update(vec![(
                "pool-vm".to_string(),
                vm_component,
                Box::new(DummySim),
            )]))
            .await;
        state
            .rfq_state_store
            .apply_update(mk_update(vec![(
                "pool-rfq".to_string(),
                rfq_component,
                Box::new(DummySim),
            )]))
            .await;
        state.vm_stream_health.record_update(1).await;
        state.rfq_stream_health.record_update(1).await;
        assert_eq!(state.vm_readiness().await, VmReadiness::Ready);
        assert_eq!(state.rfq_readiness().await, RfqReadiness::Ready);

        {
            let mut vm_status = state.vm_stream.write().await;
            vm_status.rebuilding = true;
        }
        assert_eq!(state.vm_readiness().await, VmReadiness::Rebuilding);
        assert_eq!(state.rfq_readiness().await, RfqReadiness::Ready);

        {
            let mut vm_status = state.vm_stream.write().await;
            vm_status.rebuilding = false;
        }
        state.optional_backend_stale = Duration::ZERO;
        assert_eq!(state.vm_readiness().await, VmReadiness::Stale);
        assert_eq!(state.rfq_readiness().await, RfqReadiness::Stale);
    }

    #[tokio::test]
    async fn vm_readiness_uses_vm_local_broadcaster_bootstrap() {
        let state = build_readiness_test_state(true, false).await;
        state
            .native_broadcaster_subscription
            .mark_disconnected(None)
            .await;
        state
            .vm_broadcaster_subscription
            .mark_disconnected(None)
            .await;

        state
            .vm_state_store
            .apply_update(mk_update(vec![(
                "pool-vm".to_string(),
                mk_component(
                    41,
                    "vm:curve",
                    "curve_pool",
                    vec![mk_token(42, "TKNA"), mk_token(43, "TKNB")],
                ),
                Box::new(DummySim),
            )]))
            .await;

        assert_eq!(state.vm_readiness().await, VmReadiness::WarmingUp);
        assert_eq!(
            state.encode_availability(false, true, false).await,
            EncodeAvailability::VmWarmingUp
        );

        state.vm_broadcaster_subscription.mark_connected().await;
        assert_eq!(state.vm_readiness().await, VmReadiness::WarmingUp);

        state
            .vm_broadcaster_subscription
            .mark_snapshot_started("stream-2", "snapshot-2")
            .await;
        assert_eq!(state.vm_readiness().await, VmReadiness::WarmingUp);

        state
            .vm_broadcaster_subscription
            .mark_bootstrap_complete_with_redis_boundary(replay_boundary(1))
            .await;
        state.vm_stream_health.record_update(1).await;

        assert_eq!(state.native_readiness().await, NativeReadiness::WarmingUp);
        assert_eq!(state.vm_readiness().await, VmReadiness::WarmingUp);

        state
            .vm_broadcaster_subscription
            .mark_redis_catch_up_checkpoint("1-1")
            .await;

        assert_eq!(state.vm_readiness().await, VmReadiness::Ready);
        assert_eq!(
            state.encode_availability(false, true, false).await,
            EncodeAvailability::Ready
        );
    }

    #[tokio::test]
    async fn rfq_readiness_requires_connected_bootstrapped_broadcaster_subscription() {
        let state = build_readiness_test_state(false, true).await;
        state
            .rfq_state_store
            .apply_update(mk_update(vec![(
                "pool-rfq".to_string(),
                mk_component(
                    45,
                    "rfq:hashflow",
                    "hashflow_pool",
                    vec![mk_token(46, "TKNA"), mk_token(47, "TKNB")],
                ),
                Box::new(DummySim),
            )]))
            .await;
        state.rfq_stream_health.record_update(1).await;

        assert_eq!(state.rfq_readiness().await, RfqReadiness::Ready);

        state
            .rfq_broadcaster_subscription
            .mark_disconnected(None)
            .await;
        assert_eq!(state.rfq_readiness().await, RfqReadiness::WarmingUp);

        state.rfq_broadcaster_subscription.mark_connected().await;
        assert_eq!(state.rfq_readiness().await, RfqReadiness::WarmingUp);

        state
            .rfq_broadcaster_subscription
            .mark_snapshot_started("stream-rfq", "snapshot-rfq")
            .await;
        assert_eq!(state.rfq_readiness().await, RfqReadiness::WarmingUp);

        state
            .rfq_broadcaster_subscription
            .mark_bootstrap_complete_with_redis_boundary(replay_boundary(1))
            .await;
        state
            .rfq_broadcaster_subscription
            .mark_redis_catch_up_checkpoint("1-1")
            .await;

        assert_eq!(state.rfq_readiness().await, RfqReadiness::Ready);
    }

    #[tokio::test]
    async fn rfq_status_reports_broadcaster_subscription_restart_and_error() {
        let state = build_readiness_test_state(false, true).await;
        state
            .rfq_broadcaster_subscription
            .mark_disconnected(Some("rfq broadcaster dropped".to_string()))
            .await;

        let snapshot = state.status_snapshot().await;
        let rfq = backend_snapshot(&snapshot, SimulatorBackendKind::Rfq);
        let subscription = rfq
            .subscription
            .as_ref()
            .unwrap_or_else(|| unreachable!("RFQ status must include subscription"));

        assert_eq!(rfq.readiness, SimulatorBackendReadiness::WarmingUp);
        assert_eq!(
            rfq.reason,
            Some(SimulatorReadinessReason::BroadcasterDisconnected)
        );
        assert_eq!(rfq.restart_count, 1);
        assert_eq!(rfq.last_error.as_deref(), Some("rfq broadcaster dropped"));
        assert_eq!(subscription.restart_count, 1);
        assert_eq!(
            subscription.last_error.as_deref(),
            Some("rfq broadcaster dropped")
        );
    }

    #[tokio::test]
    async fn encode_availability_gates_backends_by_route_shape() {
        let native_ready_vm_and_rfq_disabled = build_readiness_test_state(false, false).await;
        native_ready_vm_and_rfq_disabled
            .native_stream_health
            .record_update(1)
            .await;

        assert_eq!(
            native_ready_vm_and_rfq_disabled
                .encode_availability(true, false, false)
                .await,
            EncodeAvailability::Ready
        );
        assert_eq!(
            native_ready_vm_and_rfq_disabled
                .encode_availability(false, true, false)
                .await,
            EncodeAvailability::VmDisabled
        );
        assert_eq!(
            native_ready_vm_and_rfq_disabled
                .encode_availability(true, true, false)
                .await,
            EncodeAvailability::VmDisabled
        );

        let token_store = Arc::new(TokenStore::new(
            HashMap::new(),
            "http://localhost".to_string(),
            "test".to_string(),
            Chain::Ethereum,
            Duration::from_millis(10),
        ));
        let native_state_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
        let vm_state_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
        let rfq_state_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
        let native_warming_vm_ready = build_test_app_state(
            TestAppStateStores {
                token_store,
                native_state_store,
                vm_state_store: Arc::clone(&vm_state_store),
                rfq_state_store: Arc::clone(&rfq_state_store),
            },
            PoolFlags {
                enable_vm_pools: true,
                enable_rfq_pools: true,
            },
        );
        vm_state_store
            .apply_update(mk_update(vec![(
                "pool-vm".to_string(),
                mk_component(
                    31,
                    "vm:curve",
                    "curve_pool",
                    vec![mk_token(32, "TKNA"), mk_token(33, "TKNB")],
                ),
                Box::new(DummySim),
            )]))
            .await;
        native_warming_vm_ready
            .vm_stream_health
            .record_update(1)
            .await;

        assert_eq!(
            native_warming_vm_ready
                .encode_availability(true, false, false)
                .await,
            EncodeAvailability::NativeWarmingUp
        );
        assert_eq!(
            native_warming_vm_ready
                .encode_availability(false, true, false)
                .await,
            EncodeAvailability::Ready
        );
        assert_eq!(
            native_warming_vm_ready
                .encode_availability(true, true, false)
                .await,
            EncodeAvailability::NativeWarmingUp
        );
    }

    #[tokio::test]
    async fn pool_by_id_only_exposes_encode_ready_vm_and_rfq_entries() {
        let mut state = build_readiness_test_state(true, true).await;

        state
            .vm_state_store
            .apply_update(mk_update(vec![(
                "pool-vm".to_string(),
                mk_component(
                    36,
                    "vm:curve",
                    "curve_pool",
                    vec![mk_token(34, "TKNA"), mk_token(35, "TKNB")],
                ),
                Box::new(DummySim),
            )]))
            .await;
        state
            .rfq_state_store
            .apply_update(mk_update(vec![(
                "pool-rfq".to_string(),
                mk_component(
                    37,
                    "rfq:hashflow",
                    "hashflow_pool",
                    vec![mk_token(36, "TKNA"), mk_token(37, "TKNB")],
                ),
                Box::new(DummySim),
            )]))
            .await;

        state.vm_stream_health.record_update(1).await;
        state.rfq_stream_health.record_update(1).await;

        assert_eq!(state.vm_readiness().await, VmReadiness::Ready);
        assert_eq!(state.rfq_readiness().await, RfqReadiness::Ready);
        assert!(matches!(state.pool_by_id("pool-vm").await, Ok(Some(_))));
        assert!(matches!(state.pool_by_id("pool-rfq").await, Ok(Some(_))));

        {
            let mut vm_status = state.vm_stream.write().await;
            vm_status.rebuilding = true;
        }

        assert_eq!(state.vm_readiness().await, VmReadiness::Rebuilding);
        assert_eq!(state.rfq_readiness().await, RfqReadiness::Ready);
        assert!(matches!(
            state.pool_by_id("pool-vm").await,
            Err(EncodeAvailability::VmRebuilding)
        ));
        assert!(matches!(state.pool_by_id("pool-rfq").await, Ok(Some(_))));

        {
            let mut vm_status = state.vm_stream.write().await;
            vm_status.rebuilding = false;
        }

        assert!(matches!(state.pool_by_id("pool-vm").await, Ok(Some(_))));
        assert!(matches!(state.pool_by_id("pool-rfq").await, Ok(Some(_))));

        state.optional_backend_stale = Duration::ZERO;

        assert_eq!(state.vm_readiness().await, VmReadiness::Stale);
        assert_eq!(state.rfq_readiness().await, RfqReadiness::Stale);
        assert!(matches!(
            state.pool_by_id("pool-vm").await,
            Err(EncodeAvailability::VmStale)
        ));
        assert!(matches!(
            state.pool_by_id("pool-rfq").await,
            Err(EncodeAvailability::RfqStale)
        ));
    }

    #[tokio::test]
    async fn reset_protocols_preserves_unaffected_pools() {
        let token_store = Arc::new(TokenStore::new(
            HashMap::new(),
            "http://localhost".to_string(),
            "test".to_string(),
            Chain::Ethereum,
            Duration::from_secs(1),
        ));
        let store = StateStore::new(token_store);

        let token_a = mk_token(1, "TKNA");
        let token_b = mk_token(2, "TKNB");
        let token_c = mk_token(3, "TKNC");
        let token_d = mk_token(4, "TKND");

        let component_v2 = mk_component(
            10,
            "uniswap_v2",
            "uniswap_v2_pool",
            vec![token_a.clone(), token_b.clone()],
        );
        let component_v3 = mk_component(
            11,
            "uniswap_v3",
            "uniswap_v3_pool",
            vec![token_c.clone(), token_d.clone()],
        );

        let update = mk_update(vec![
            ("pool-v2".to_string(), component_v2, Box::new(DummySim)),
            ("pool-v3".to_string(), component_v3, Box::new(DummySim)),
        ]);
        store.apply_update(update).await;

        assert_eq!(store.total_states().await, 2);
        assert!(store.is_ready());

        store.reset_protocols(&[ProtocolKind::UniswapV2]).await;

        assert_eq!(store.total_states().await, 1);
        assert!(store.is_ready());

        let remaining = store
            .matching_pools_by_addresses(&token_c.address, &token_d.address)
            .await;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0, "pool-v3");

        let cleared = store
            .matching_pools_by_addresses(&token_a.address, &token_b.address)
            .await;
        assert!(cleared.is_empty());
    }

    #[tokio::test]
    async fn reset_protocols_updates_ready_when_empty() {
        let token_store = Arc::new(TokenStore::new(
            HashMap::new(),
            "http://localhost".to_string(),
            "test".to_string(),
            Chain::Ethereum,
            Duration::from_secs(1),
        ));
        let store = StateStore::new(token_store);

        let token_a = mk_token(5, "TKN1");
        let token_b = mk_token(6, "TKN2");
        let component = mk_component(12, "uniswap_v2", "uniswap_v2_pool", vec![token_a, token_b]);

        let update = mk_update(vec![(
            "pool-only".to_string(),
            component,
            Box::new(DummySim),
        )]);
        store.apply_update(update).await;

        assert!(store.is_ready());

        store.reset_protocols(&[ProtocolKind::UniswapV2]).await;

        assert_eq!(store.total_states().await, 0);
        assert!(!store.is_ready());
    }

    #[tokio::test]
    async fn total_pools_includes_vm_store() {
        let token_store = Arc::new(TokenStore::new(
            HashMap::new(),
            "http://localhost".to_string(),
            "test".to_string(),
            Chain::Ethereum,
            Duration::from_secs(1),
        ));
        let native_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
        let vm_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
        let rfq_store = Arc::new(StateStore::new(Arc::clone(&token_store)));

        let token_a = mk_token(7, "TKNA");
        let token_b = mk_token(8, "TKNB");

        let native_component = mk_component(
            20,
            "uniswap_v2",
            "uniswap_v2_pool",
            vec![token_a.clone(), token_b.clone()],
        );
        let vm_component = mk_component(21, "vm:curve", "curve_pool", vec![token_a, token_b]);

        native_store
            .apply_update(mk_update(vec![(
                "pool-native".to_string(),
                native_component,
                Box::new(DummySim),
            )]))
            .await;
        vm_store
            .apply_update(mk_update(vec![(
                "pool-vm".to_string(),
                vm_component,
                Box::new(DummySim),
            )]))
            .await;

        let app_state = build_test_app_state(
            TestAppStateStores {
                token_store: Arc::clone(&token_store),
                native_state_store: Arc::clone(&native_store),
                vm_state_store: Arc::clone(&vm_store),
                rfq_state_store: Arc::clone(&rfq_store),
            },
            PoolFlags {
                enable_vm_pools: true,
                enable_rfq_pools: true,
            },
        );

        assert_eq!(app_state.total_pools().await, 2);
        assert_eq!(app_state.current_vm_block().await, Some(1));
    }

    #[tokio::test]
    async fn vm_readiness_reports_rebuilding_before_staleness() {
        let token_store = Arc::new(TokenStore::new(
            HashMap::new(),
            "http://localhost".to_string(),
            "test".to_string(),
            Chain::Ethereum,
            Duration::from_secs(1),
        ));
        let native_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
        let vm_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
        let rfq_store = Arc::new(StateStore::new(Arc::clone(&token_store)));

        native_store
            .apply_update(mk_update(vec![(
                "pool-native".to_string(),
                mk_component(
                    34,
                    "uniswap_v2",
                    "uniswap_v2_pool",
                    vec![mk_token(19, "TKNA"), mk_token(20, "TKNB")],
                ),
                Box::new(DummySim),
            )]))
            .await;
        vm_store
            .apply_update(mk_update(vec![(
                "pool-vm".to_string(),
                mk_component(
                    35,
                    "vm:curve",
                    "curve_pool",
                    vec![mk_token(21, "TKNA"), mk_token(22, "TKNB")],
                ),
                Box::new(DummySim),
            )]))
            .await;

        let app_state = build_test_app_state(
            TestAppStateStores {
                token_store: Arc::clone(&token_store),
                native_state_store: Arc::clone(&native_store),
                vm_state_store: Arc::clone(&vm_store),
                rfq_state_store: Arc::clone(&rfq_store),
            },
            PoolFlags {
                enable_vm_pools: true,
                enable_rfq_pools: true,
            },
        );

        app_state.native_stream_health.record_update(1).await;
        app_state.vm_stream_health.record_update(1).await;
        {
            let mut vm_status = app_state.vm_stream.write().await;
            vm_status.rebuilding = true;
        }

        assert_eq!(app_state.vm_readiness().await, VmReadiness::Rebuilding);
        assert_eq!(app_state.native_readiness().await, NativeReadiness::Ready);
        assert_eq!(
            app_state.encode_availability(false, true, false).await,
            EncodeAvailability::VmRebuilding
        );
    }

    enum RequestTokenIn {
        Wrapped,
        Native,
        Other,
    }

    struct MatchingPoolsCase {
        name: &'static str,
        request_token_in: RequestTokenIn,
        include_wrapped_pool: bool,
        include_native_pool: bool,
        expected_pool_ids: &'static [&'static str],
    }

    fn known_address(value: &str) -> Bytes {
        Bytes::from_str(value).unwrap_or_else(|err| unreachable!("valid address: {err}"))
    }

    async fn run_matching_pools_case(case: MatchingPoolsCase, wrapped_address: &Bytes) {
        let native_address = native_token_address();
        let token_x = mk_token(9, "TKNX");
        let token_y = mk_token(10, "TKNY");
        let wrapped_token = Token::new(wrapped_address, "WETH", 18, 0, &[], Chain::Ethereum, 100);
        let native_token = Token::new(&native_address, "ETH", 18, 0, &[], Chain::Ethereum, 100);

        let token_store = Arc::new(TokenStore::new(
            HashMap::new(),
            "http://localhost".to_string(),
            "test".to_string(),
            Chain::Ethereum,
            Duration::from_secs(1),
        ));
        let store = StateStore::new(token_store);
        let mut update_pairs = Vec::new();

        if case.include_wrapped_pool {
            update_pairs.push((
                "pool-weth".to_string(),
                mk_component(
                    20,
                    "uniswap_v2",
                    "uniswap_v2_pool",
                    vec![wrapped_token.clone(), token_x.clone()],
                ),
                Box::new(DummySim) as Box<dyn ProtocolSim>,
            ));
        }

        if case.include_native_pool {
            update_pairs.push((
                "pool-native".to_string(),
                mk_component(
                    21,
                    "uniswap_v2",
                    "uniswap_v2_pool",
                    vec![native_token.clone(), token_x.clone()],
                ),
                Box::new(DummySim) as Box<dyn ProtocolSim>,
            ));
        }

        store.apply_update(mk_update(update_pairs)).await;

        let request_token_in = match case.request_token_in {
            RequestTokenIn::Wrapped => wrapped_address,
            RequestTokenIn::Native => &native_address,
            RequestTokenIn::Other => &token_y.address,
        };
        let matches = store
            .matching_pools_by_addresses(request_token_in, &token_x.address)
            .await;
        let ids: HashSet<String> = matches.into_iter().map(|(id, _)| id).collect();
        let expected: HashSet<String> = case
            .expected_pool_ids
            .iter()
            .map(ToString::to_string)
            .collect();

        assert_eq!(ids, expected, "case {}", case.name);
    }

    #[tokio::test]
    async fn matching_pools_native_wrapped_aliasing_cases() {
        let wrapped_address = known_address("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2");

        for case in [
            MatchingPoolsCase {
                name: "wrapped request prefers exact wrapped pools",
                request_token_in: RequestTokenIn::Wrapped,
                include_wrapped_pool: true,
                include_native_pool: true,
                expected_pool_ids: &["pool-weth"],
            },
            MatchingPoolsCase {
                name: "wrapped request falls back to native-only pool",
                request_token_in: RequestTokenIn::Wrapped,
                include_wrapped_pool: false,
                include_native_pool: true,
                expected_pool_ids: &["pool-native"],
            },
            MatchingPoolsCase {
                name: "native request prefers exact native pools",
                request_token_in: RequestTokenIn::Native,
                include_wrapped_pool: true,
                include_native_pool: true,
                expected_pool_ids: &["pool-native"],
            },
            MatchingPoolsCase {
                name: "native request falls back to wrapped-only pool",
                request_token_in: RequestTokenIn::Native,
                include_wrapped_pool: true,
                include_native_pool: false,
                expected_pool_ids: &["pool-weth"],
            },
            MatchingPoolsCase {
                name: "non wrapped request does not include native pool",
                request_token_in: RequestTokenIn::Other,
                include_wrapped_pool: false,
                include_native_pool: true,
                expected_pool_ids: &[],
            },
        ] {
            run_matching_pools_case(case, &wrapped_address).await;
        }
    }

    async fn build_native_identity_fence_state() -> AppState {
        let state = build_readiness_test_state(false, false).await;
        state
            .native_state_store
            .apply_update(mk_update(vec![(
                "pool-b".to_string(),
                mk_component(
                    31,
                    "uniswap_v2",
                    "uniswap_v2_pool",
                    vec![mk_token(32, "TKNC"), mk_token(33, "TKND")],
                ),
                Box::new(DummySim),
            )]))
            .await;
        state
    }

    #[tokio::test]
    async fn native_route_fence_tracks_touched_pool_identity_only() {
        let state = build_native_identity_fence_state().await;
        let pinned = state.native_state_store.pin().await;
        let (pinned_a_state, pinned_a_component) = pinned
            .pool_by_id("pool-native")
            .unwrap_or_else(|| unreachable!("fixture pool exists"));
        let (pinned_b_state, pinned_b_component) = pinned
            .pool_by_id("pool-b")
            .unwrap_or_else(|| unreachable!("fixture pool exists"));

        state
            .native_state_store
            .apply_update(Update::new(
                2,
                HashMap::from([(
                    "pool-native".to_string(),
                    Box::new(DummySim) as Box<dyn ProtocolSim>,
                )]),
                HashMap::new(),
            ))
            .await;

        let current = state.native_state_store.pin().await;
        let (current_a_state, current_a_component) = current
            .pool_by_id("pool-native")
            .unwrap_or_else(|| unreachable!("fixture pool exists"));
        let (current_b_state, current_b_component) = current
            .pool_by_id("pool-b")
            .unwrap_or_else(|| unreachable!("fixture pool exists"));
        assert!(!Arc::ptr_eq(&pinned_a_state, &current_a_state));
        assert!(Arc::ptr_eq(&pinned_a_component, &current_a_component));
        assert!(Arc::ptr_eq(&pinned_b_state, &current_b_state));
        assert!(Arc::ptr_eq(&pinned_b_component, &current_b_component));

        let pool_a = HashSet::from(["pool-native".to_string()]);
        let pool_b = HashSet::from(["pool-b".to_string()]);
        assert_eq!(
            state
                .native_route_fence_status(&pinned, &pool_a)
                .await
                .status,
            NativeFenceStatus::Changed
        );
        assert_eq!(
            state
                .native_route_fence_status(&pinned, &pool_b)
                .await
                .status,
            NativeFenceStatus::Current
        );
        assert_eq!(
            state
                .native_pool_fence_status(
                    &pinned,
                    &current,
                    &HashSet::from(["pool-native".to_string(), "pool-b".to_string()])
                )
                .await,
            NativePoolFenceStatus::Available(HashSet::from(["pool-native".to_string()]))
        );
    }

    #[tokio::test]
    async fn get_amount_out_preserves_published_pool_identity_and_result() {
        const SNAPSHOTS_JSON: &str =
            include_str!("../../tests/fixtures/simulation_baseline_snapshots.json");
        const POOL_ID: &str = "uniswap-v2-baseline";

        let mut snapshots: HashMap<String, DtoComponentWithState> =
            serde_json::from_str(SNAPSHOTS_JSON)
                .unwrap_or_else(|error| unreachable!("valid simulation snapshots: {error}"));
        let snapshot: ComponentWithState = snapshots
            .remove("uniswap_v2")
            .unwrap_or_else(|| unreachable!("Uniswap V2 snapshot exists"))
            .into();
        let tokens = snapshot
            .component
            .tokens
            .iter()
            .enumerate()
            .map(|(index, address)| {
                Token::new(
                    address,
                    &format!("TOKEN{index}"),
                    18,
                    0,
                    &[Some(10_000)],
                    Chain::Base,
                    100,
                )
            })
            .collect::<Vec<_>>();
        let token_in = tokens[0].clone();
        let token_out = tokens[1].clone();
        let all_tokens = tokens
            .iter()
            .cloned()
            .map(|token| (token.address.clone(), token))
            .collect::<HashMap<_, _>>();
        let component = ProtocolComponent::new(
            address(42),
            snapshot.component.protocol_system.clone(),
            snapshot.component.protocol_type_name.clone(),
            snapshot.component.chain,
            tokens.clone(),
            snapshot.component.contract_addresses.clone(),
            snapshot.component.static_attributes.clone(),
            snapshot.component.creation_tx.clone(),
            snapshot.component.created_at,
        );
        let pool_state = UniswapV2State::try_from_with_header(
            snapshot,
            BlockHeader {
                number: 20_000_000,
                timestamp: 1_700_000_000,
                ..Default::default()
            },
            &HashMap::new(),
            &all_tokens,
            &DecoderContext::new(),
        )
        .await
        .unwrap_or_else(|error| unreachable!("valid Uniswap V2 snapshot: {error}"));

        let token_store = Arc::new(TokenStore::new(
            HashMap::new(),
            "http://localhost".to_string(),
            "test".to_string(),
            Chain::Base,
            Duration::from_secs(1),
        ));
        let store = StateStore::new(token_store);
        store
            .apply_update(Update::new(
                20_000_000,
                HashMap::from([(
                    POOL_ID.to_string(),
                    Box::new(pool_state) as Box<dyn ProtocolSim>,
                )]),
                HashMap::from([(POOL_ID.to_string(), component)]),
            ))
            .await;

        let pinned = store.pin().await;
        let (pinned_state, _) = pinned
            .pool_by_id(POOL_ID)
            .unwrap_or_else(|| unreachable!("fixture pool exists"));
        let amount_in = BigUint::from(1_000_000_000_000u64);
        let first = pinned_state
            .get_amount_out(amount_in.clone(), &token_in, &token_out)
            .unwrap_or_else(|error| unreachable!("fixture swap succeeds: {error}"));

        let published = store.pin().await;
        let (published_state, _) = published
            .pool_by_id(POOL_ID)
            .unwrap_or_else(|| unreachable!("fixture pool exists"));
        assert!(Arc::ptr_eq(&pinned_state, &published_state));

        let repeated = pinned_state
            .get_amount_out(amount_in, &token_in, &token_out)
            .unwrap_or_else(|error| unreachable!("repeated fixture swap succeeds: {error}"));
        assert_eq!(first.amount, repeated.amount);
        assert_eq!(first.gas, repeated.gas);
    }

    #[tokio::test]
    async fn native_route_fence_marks_removed_pool_changed() {
        let state = build_native_identity_fence_state().await;
        let pinned = state.native_state_store.pin().await;
        let component = pinned
            .pool_by_id("pool-native")
            .map(|(_, component)| component.as_ref().clone())
            .unwrap_or_else(|| unreachable!("fixture pool exists"));

        state
            .native_state_store
            .apply_update(
                Update::new(2, HashMap::new(), HashMap::new())
                    .set_removed_pairs(HashMap::from([("pool-native".to_string(), component)])),
            )
            .await;

        assert_eq!(
            state
                .native_route_fence_status(&pinned, &HashSet::from(["pool-native".to_string()]))
                .await
                .status,
            NativeFenceStatus::Changed
        );
    }

    #[tokio::test]
    async fn native_route_fence_distinguishes_missing_removed_and_added_pools() {
        let state = build_native_identity_fence_state().await;
        let pinned = state.native_state_store.pin().await;
        let removed_component = pinned
            .pool_by_id("pool-native")
            .map(|(_, component)| component.as_ref().clone())
            .unwrap_or_else(|| unreachable!("fixture pool exists"));
        let added_component = mk_component(
            34,
            "uniswap_v2",
            "uniswap_v2_pool",
            vec![mk_token(35, "TKNE"), mk_token(36, "TKNF")],
        );

        state
            .native_state_store
            .apply_update(
                mk_update(vec![(
                    "pool-added".to_string(),
                    added_component,
                    Box::new(DummySim),
                )])
                .set_removed_pairs(HashMap::from([(
                    "pool-native".to_string(),
                    removed_component,
                )])),
            )
            .await;

        let missing_pool = HashSet::from(["pool-missing".to_string()]);
        assert_eq!(
            state
                .native_route_fence_status(&pinned, &missing_pool)
                .await
                .status,
            NativeFenceStatus::Current
        );
        let current = state.native_state_store.pin().await;
        assert_eq!(
            state
                .native_pool_fence_status(&pinned, &current, &missing_pool)
                .await,
            NativePoolFenceStatus::Available(HashSet::new())
        );
        assert_eq!(
            state
                .native_route_fence_status(&pinned, &HashSet::from(["pool-native".to_string()]))
                .await
                .status,
            NativeFenceStatus::Changed
        );
        assert_eq!(
            state
                .native_route_fence_status(&pinned, &HashSet::from(["pool-added".to_string()]))
                .await
                .status,
            NativeFenceStatus::Changed
        );
    }

    #[tokio::test]
    async fn native_pool_identity_marks_full_reset_changed() {
        let state = build_native_identity_fence_state().await;
        let pinned = state.native_state_store.pin().await;

        state.native_state_store.reset().await;
        let current = state.native_state_store.pin().await;

        assert_eq!(
            pinned.changed_pool_ids(
                &current,
                &HashSet::from(["pool-native".to_string(), "pool-b".to_string()])
            ),
            HashSet::from(["pool-native".to_string(), "pool-b".to_string()])
        );
    }

    #[tokio::test]
    async fn native_route_fence_ignores_bootstrap_version_alignment() {
        let state = build_native_identity_fence_state().await;
        let pinned = state.native_state_store.pin().await;

        state
            .native_state_store
            .align_bootstrap_state_version(pinned.version())
            .await;

        let current = state.native_state_store.pin().await;
        assert_ne!(
            pinned.request_generation(),
            current.request_generation(),
            "alignment should still advance the request generation"
        );
        assert_eq!(
            state
                .native_route_fence_status(&pinned, &HashSet::from(["pool-native".to_string()]))
                .await
                .status,
            NativeFenceStatus::Current
        );
    }

    #[tokio::test]
    async fn native_route_fence_marks_recovery_candidate_changed() {
        let state = build_native_identity_fence_state().await;
        let pinned = state.native_state_store.pin().await;
        let candidate_store = StateStore::new_private(Arc::clone(&state.tokens));
        candidate_store
            .apply_update(mk_update(vec![(
                "pool-native".to_string(),
                mk_component(
                    28,
                    "uniswap_v2",
                    "uniswap_v2_pool",
                    vec![mk_token(29, "TKNA"), mk_token(30, "TKNB")],
                ),
                Box::new(DummySim),
            )]))
            .await;
        let candidate = candidate_store.pin().await;

        state
            .native_state_store
            .publish_candidate(candidate, pinned.version().saturating_add(1))
            .await
            .unwrap_or_else(|error| unreachable!("newer candidate publishes: {error}"));

        assert_eq!(
            state
                .native_route_fence_status(&pinned, &HashSet::from(["pool-native".to_string()]))
                .await
                .status,
            NativeFenceStatus::Changed
        );
    }

    #[tokio::test]
    async fn native_route_fence_reports_fenced_requests_unavailable() {
        let state = build_native_identity_fence_state().await;
        let pinned = state.native_state_store.pin().await;

        state.native_state_store.fence_requests().await;

        assert_eq!(
            state
                .native_route_fence_status(&pinned, &HashSet::from(["pool-native".to_string()]))
                .await
                .status,
            NativeFenceStatus::Unavailable
        );
    }

    #[tokio::test]
    async fn pinned_readers_keep_complete_old_version_after_publication() {
        let token_store = Arc::new(TokenStore::new(
            HashMap::new(),
            "http://localhost".to_string(),
            "test".to_string(),
            Chain::Ethereum,
            Duration::from_secs(1),
        ));
        let store = Arc::new(StateStore::new(token_store));

        let token_a = mk_token(70, "TKNA");
        let token_b = mk_token(71, "TKNB");

        let component_v1 = mk_component(
            40,
            "uniswap_v2",
            "uniswap_v2_pool",
            vec![token_a.clone(), token_b.clone()],
        );
        let component_v2 = mk_component(
            41,
            "uniswap_v2",
            "uniswap_v2_pool",
            vec![token_a.clone(), token_b.clone()],
        );

        store
            .apply_update(mk_update(vec![(
                "pool-v1".to_string(),
                component_v1.clone(),
                Box::new(DummySim),
            )]))
            .await;
        let old_pin = store.pin().await;

        let update = mk_update(vec![(
            "pool-v2".to_string(),
            component_v2,
            Box::new(DummySim),
        )])
        .set_removed_pairs(HashMap::from([("pool-v1".to_string(), component_v1)]));
        store.apply_update(update).await;
        let new_pin = store.pin().await;

        let old_ids: HashSet<String> = old_pin
            .matching_pools_by_addresses(&token_a.address, &token_b.address)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let new_ids: HashSet<String> = new_pin
            .matching_pools_by_addresses(&token_a.address, &token_b.address)
            .into_iter()
            .map(|(id, _)| id)
            .collect();

        assert_eq!(old_ids, HashSet::from(["pool-v1".to_string()]));
        assert_eq!(new_ids, HashSet::from(["pool-v2".to_string()]));
        assert_ne!(old_pin.version(), new_pin.version());
    }
}
