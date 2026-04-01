use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{watch, RwLock, Semaphore};
use tokio::time::Instant;
use tycho_simulation::{
    protocol::models::{ProtocolComponent, Update},
    tycho_common::{
        models::{token::Token, Chain},
        simulation::protocol_sim::ProtocolSim,
        Bytes,
    },
};

use super::{protocol::ProtocolKind, stream_health::StreamHealth, tokens::TokenStore};

const UPDATE_ANOMALY_SAMPLE_CAP: usize = 6;
const NATIVE_TOKEN_ADDRESS_BYTES: [u8; 20] = [0u8; 20];

fn native_token_address() -> Bytes {
    Bytes::from(NATIVE_TOKEN_ADDRESS_BYTES)
}

#[derive(Clone)]
pub struct AppState {
    pub chain: Chain,
    pub native_token_protocol_allowlist: Arc<Vec<String>>,
    pub tokens: Arc<TokenStore>,
    pub native_state_store: Arc<StateStore>,
    pub vm_state_store: Arc<StateStore>,
    pub rfq_state_store: Arc<StateStore>,
    pub native_stream_health: Arc<StreamHealth>,
    pub vm_stream_health: Arc<StreamHealth>,
    pub rfq_stream_health: Arc<StreamHealth>,
    pub vm_stream: Arc<RwLock<VmStreamStatus>>,
    pub rfq_stream: Arc<RwLock<RfqStreamStatus>>,
    pub enable_vm_pools: bool,
    pub enable_rfq_pools: bool,
    pub readiness_stale: Duration,
    pub quote_timeout: Duration,
    pub pool_timeout_native: Duration,
    pub pool_timeout_vm: Duration,
    pub pool_timeout_rfq: Duration,
    pub request_timeout: Duration,
    pub native_sim_semaphore: Arc<Semaphore>,
    pub vm_sim_semaphore: Arc<Semaphore>,
    pub rfq_sim_semaphore: Arc<Semaphore>,
    pub erc4626_deposits_enabled: bool,
    pub reset_allowance_tokens: Arc<HashMap<u64, HashSet<Bytes>>>,
    pub native_sim_concurrency: usize,
    pub vm_sim_concurrency: usize,
    pub rfq_sim_concurrency: usize,
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
            Self::WarmingUp | Self::Stale => "warming_up",
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
            Self::WarmingUp | Self::Stale => "warming_up",
            Self::Rebuilding => "rebuilding",
            Self::Ready => "ready",
        }
    }
}

impl RfqReadiness {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::WarmingUp | Self::Stale => "warming_up",
            Self::Rebuilding => "rebuilding",
            Self::Ready => "ready",
        }
    }
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
    pub async fn current_block(&self) -> u64 {
        self.native_state_store.current_block().await
    }

    pub async fn current_vm_block(&self) -> Option<u64> {
        if !self.vm_ready().await {
            return None;
        }
        Some(self.vm_state_store.current_block().await)
    }

    pub async fn current_rfq_block(&self) -> Option<u64> {
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

    pub fn is_ready(&self) -> bool {
        self.native_state_store.is_ready()
    }

    pub fn readiness_stale_ms(&self) -> u64 {
        self.readiness_stale.as_millis() as u64
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
        if !self.is_ready() {
            return NativeReadiness::WarmingUp;
        }

        if is_update_stale(self.native_update_age_ms().await, self.readiness_stale_ms()) {
            NativeReadiness::Stale
        } else {
            NativeReadiness::Ready
        }
    }

    pub async fn vm_readiness(&self) -> VmReadiness {
        if !self.enable_vm_pools {
            return VmReadiness::Disabled;
        }

        if self.vm_rebuilding().await {
            return VmReadiness::Rebuilding;
        }

        if !self.vm_state_store.is_ready() {
            return VmReadiness::WarmingUp;
        }

        if is_update_stale(self.vm_update_age_ms().await, self.readiness_stale_ms()) {
            VmReadiness::Stale
        } else {
            VmReadiness::Ready
        }
    }

    pub async fn rfq_readiness(&self) -> RfqReadiness {
        if !self.enable_rfq_pools {
            return RfqReadiness::Disabled;
        }

        if self.rfq_rebuilding().await {
            return RfqReadiness::Rebuilding;
        }

        if !self.rfq_state_store.is_ready() {
            return RfqReadiness::WarmingUp;
        }

        if is_update_stale(self.rfq_update_age_ms().await, self.readiness_stale_ms()) {
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
        self.native_state_store.wait_until_ready(wait).await
    }

    pub fn quote_timeout(&self) -> Duration {
        self.quote_timeout
    }

    pub fn pool_timeout_native(&self) -> Duration {
        self.pool_timeout_native
    }

    pub fn pool_timeout_vm(&self) -> Duration {
        self.pool_timeout_vm
    }

    pub fn pool_timeout_rfq(&self) -> Duration {
        self.pool_timeout_rfq
    }

    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }
    pub fn native_sim_semaphore(&self) -> Arc<Semaphore> {
        Arc::clone(&self.native_sim_semaphore)
    }

    pub fn vm_sim_semaphore(&self) -> Arc<Semaphore> {
        Arc::clone(&self.vm_sim_semaphore)
    }

    pub fn rfq_sim_semaphore(&self) -> Arc<Semaphore> {
        Arc::clone(&self.rfq_sim_semaphore)
    }

    pub async fn pool_by_id(&self, id: &str) -> Option<PoolEntry> {
        if let Some(entry) = self.native_state_store.pool_by_id(id).await {
            return Some(entry);
        }

        if let Some(entry) = self.vm_state_store.pool_by_id(id).await {
            return Some(entry);
        }

        if !self.vm_ready().await && !self.rfq_ready().await {
            return None;
        }

        self.rfq_state_store.pool_by_id(id).await
    }

    pub async fn vm_ready(&self) -> bool {
        if !self.enable_vm_pools {
            return false;
        }
        if self.vm_rebuilding().await {
            return false;
        }
        self.vm_state_store.is_ready()
    }

    pub async fn rfq_ready(&self) -> bool {
        if !self.enable_rfq_pools {
            return false;
        }
        if self.rfq_rebuilding().await {
            return false;
        }
        self.rfq_state_store.is_ready()
    }

    pub async fn vm_rebuilding(&self) -> bool {
        self.vm_stream.read().await.rebuilding
    }

    pub async fn rfq_rebuilding(&self) -> bool {
        self.rfq_stream.read().await.rebuilding
    }

    pub async fn vm_block(&self) -> u64 {
        self.vm_state_store.current_block().await
    }

    pub async fn rfq_block(&self) -> u64 {
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
        .unwrap_or(false)
}

#[derive(Debug, Clone, Default)]
pub struct VmStreamStatus {
    pub rebuilding: bool,
    pub restart_count: u64,
    pub last_error: Option<String>,
    pub rebuild_started_at: Option<Instant>,
}

#[derive(Debug, Clone, Default)]
pub struct RfqStreamStatus {
    pub rebuilding: bool,
    pub restart_count: u64,
    pub last_error: Option<String>,
    pub rebuild_started_at: Option<Instant>,
}

pub(crate) type PoolEntry = (Arc<dyn ProtocolSim>, Arc<ProtocolComponent>);
type SharedPoolStates = Arc<RwLock<HashMap<String, PoolEntry>>>;

#[derive(Clone, Default)]
struct ProtocolShard {
    states: SharedPoolStates,
}

impl ProtocolShard {
    async fn insert_new(
        &self,
        id: String,
        state: Arc<dyn ProtocolSim>,
        component: Arc<ProtocolComponent>,
    ) {
        let mut guard = self.states.write().await;
        guard.insert(id, (state, component));
    }

    async fn update_state(&self, id: &str, state: Arc<dyn ProtocolSim>) -> bool {
        let mut guard = self.states.write().await;
        if let Some((existing_state, _)) = guard.get_mut(id) {
            *existing_state = state;
            true
        } else {
            false
        }
    }

    async fn remove(&self, id: &str) -> Option<PoolEntry> {
        let mut guard = self.states.write().await;
        guard.remove(id)
    }

    async fn len(&self) -> usize {
        let guard = self.states.read().await;
        guard.len()
    }

    async fn clear(&self) {
        let mut guard = self.states.write().await;
        *guard = HashMap::new();
    }
}

pub struct UpdateMetrics {
    pub block_number: u64,
    pub updated_states: usize,
    pub new_pairs: usize,
    pub removed_pairs: usize,
    pub total_pairs: usize,
    pub new_pairs_missing_state: usize,
    pub unknown_protocol_new_pairs: usize,
    pub updates_for_unknown_pair: usize,
    pub states_missing_in_shard: usize,
    pub removed_unknown_pair: usize,
    pub anomaly_samples: Vec<String>,
}

impl UpdateMetrics {
    pub fn has_anomalies(&self) -> bool {
        self.new_pairs_missing_state > 0
            || self.unknown_protocol_new_pairs > 0
            || self.updates_for_unknown_pair > 0
            || self.states_missing_in_shard > 0
            || self.removed_unknown_pair > 0
    }
}

#[derive(Default)]
struct TokenIndexChanges {
    additions: Vec<(Bytes, String)>,
    removals: Vec<(Bytes, String)>,
}

#[derive(Default)]
struct UpdateAccumulator {
    new_pairs_count: usize,
    new_pairs_missing_state: usize,
    unknown_protocol_new_pairs: usize,
    updated_states: usize,
    updates_for_unknown_pair: usize,
    states_missing_in_shard: usize,
    removed_pairs_count: usize,
    removed_unknown_pair: usize,
    anomaly_samples: Vec<String>,
    tokens_to_cache: Vec<Token>,
}

impl UpdateAccumulator {
    fn record_anomaly(&mut self, kind: &str, value: &str) {
        add_anomaly_sample(&mut self.anomaly_samples, kind, value);
    }
}

pub struct StateStore {
    tokens: Arc<TokenStore>,
    shards: HashMap<ProtocolKind, ProtocolShard>,
    id_to_kind: RwLock<HashMap<String, ProtocolKind>>,
    // Token -> pool id index to avoid scanning all pools on each quote.
    token_index: RwLock<HashMap<Bytes, HashSet<String>>>,
    block_number: RwLock<u64>,
    ready: AtomicBool,
    ready_tx: watch::Sender<bool>,
}

impl StateStore {
    pub fn new(tokens: Arc<TokenStore>) -> Self {
        let mut shards = HashMap::new();
        for kind in ProtocolKind::ALL {
            shards.insert(kind, ProtocolShard::default());
        }
        let (ready_tx, _) = watch::channel(false);
        StateStore {
            tokens,
            shards,
            id_to_kind: RwLock::new(HashMap::new()),
            token_index: RwLock::new(HashMap::new()),
            block_number: RwLock::new(0),
            ready: AtomicBool::new(false),
            ready_tx,
        }
    }

    async fn rebuild_indexes(&self) -> usize {
        let mut id_to_kind = HashMap::new();
        let mut token_index: HashMap<Bytes, HashSet<String>> = HashMap::new();
        let mut total_pairs = 0usize;

        for (kind, shard) in self.shards.iter() {
            let guard = shard.states.read().await;
            for (id, (_, component)) in guard.iter() {
                id_to_kind.insert(id.clone(), *kind);
                total_pairs += 1;
                for token in component.tokens.iter() {
                    token_index
                        .entry(token.address.clone())
                        .or_default()
                        .insert(id.clone());
                }
            }
        }

        *self.id_to_kind.write().await = id_to_kind;
        *self.token_index.write().await = token_index;
        total_pairs
    }

    pub async fn reset(&self) {
        for shard in self.shards.values() {
            shard.clear().await;
        }
        *self.id_to_kind.write().await = HashMap::new();
        *self.token_index.write().await = HashMap::new();
        *self.block_number.write().await = 0;
        self.ready.store(false, Ordering::Relaxed);
        let _ = self.ready_tx.send(false);
    }

    pub async fn reset_protocols(&self, protocols: &[ProtocolKind]) {
        if protocols.is_empty() {
            return;
        }

        if protocols.len() >= self.shards.len() {
            self.reset().await;
            return;
        }

        let mut any_cleared = false;
        for kind in protocols {
            if let Some(shard) = self.shards.get(kind) {
                shard.clear().await;
                any_cleared = true;
            }
        }

        if !any_cleared {
            return;
        }

        let total_pairs = self.rebuild_indexes().await;
        let was_ready = self.ready.load(Ordering::Acquire);
        let now_ready = total_pairs > 0;
        if was_ready != now_ready {
            self.ready.store(now_ready, Ordering::Release);
            let _ = self.ready_tx.send(now_ready);
        }
    }

    pub async fn apply_update(&self, mut update: Update) -> UpdateMetrics {
        // EVM feeds always return a block number
        let block_number = update.block_number_or_timestamp;
        {
            let mut guard = self.block_number.write().await;
            *guard = block_number;
        }

        let mut index_changes = TokenIndexChanges::default();
        let mut stats = UpdateAccumulator::default();

        self.apply_new_pairs(
            update.new_pairs,
            &mut update.states,
            &mut index_changes,
            &mut stats,
        )
        .await;
        self.cache_new_tokens(&mut stats).await;
        self.apply_state_updates(update.states, &mut stats).await;
        self.apply_removed_pairs(update.removed_pairs, &mut index_changes, &mut stats)
            .await;

        if !index_changes.additions.is_empty() || !index_changes.removals.is_empty() {
            let mut index = self.token_index.write().await;
            apply_token_index_changes(&mut index, index_changes.additions, index_changes.removals);
        }

        let total_pairs = self.total_states().await;
        self.update_ready_state(total_pairs);

        UpdateMetrics {
            block_number,
            updated_states: stats.updated_states,
            new_pairs: stats.new_pairs_count,
            removed_pairs: stats.removed_pairs_count,
            total_pairs,
            new_pairs_missing_state: stats.new_pairs_missing_state,
            unknown_protocol_new_pairs: stats.unknown_protocol_new_pairs,
            updates_for_unknown_pair: stats.updates_for_unknown_pair,
            states_missing_in_shard: stats.states_missing_in_shard,
            removed_unknown_pair: stats.removed_unknown_pair,
            anomaly_samples: stats.anomaly_samples,
        }
    }

    async fn apply_new_pairs(
        &self,
        new_pairs: HashMap<String, ProtocolComponent>,
        states: &mut HashMap<String, Box<dyn ProtocolSim>>,
        index_changes: &mut TokenIndexChanges,
        stats: &mut UpdateAccumulator,
    ) {
        for (id, component) in new_pairs {
            match ProtocolKind::from_component(&component) {
                Some(kind) => {
                    self.insert_new_pair(id, kind, component, states, index_changes, stats)
                        .await;
                }
                None => {
                    stats.unknown_protocol_new_pairs += 1;
                    stats.record_anomaly("unknown_protocol_new_pairs", id.as_str());
                }
            }
        }
    }

    async fn insert_new_pair(
        &self,
        id: String,
        kind: ProtocolKind,
        component: ProtocolComponent,
        states: &mut HashMap<String, Box<dyn ProtocolSim>>,
        index_changes: &mut TokenIndexChanges,
        stats: &mut UpdateAccumulator,
    ) {
        let Some(state) = states.remove(&id) else {
            stats.new_pairs_missing_state += 1;
            stats.record_anomaly("new_pairs_missing_state", id.as_str());
            return;
        };
        let Some(shard) = self.shards.get(&kind) else {
            stats.unknown_protocol_new_pairs += 1;
            stats.record_anomaly("unknown_protocol_new_pairs", id.as_str());
            return;
        };

        let component = Arc::new(component);
        self.id_to_kind.write().await.insert(id.clone(), kind);
        stats
            .tokens_to_cache
            .extend(component.tokens.iter().cloned());
        for token in &component.tokens {
            index_changes
                .additions
                .push((token.address.clone(), id.clone()));
        }
        shard
            .insert_new(id, Arc::from(state), Arc::clone(&component))
            .await;
        stats.new_pairs_count += 1;
    }

    async fn cache_new_tokens(&self, stats: &mut UpdateAccumulator) {
        if stats.tokens_to_cache.is_empty() {
            return;
        }

        self.tokens
            .insert_batch(stats.tokens_to_cache.drain(..))
            .await;
    }

    async fn apply_state_updates(
        &self,
        states: HashMap<String, Box<dyn ProtocolSim>>,
        stats: &mut UpdateAccumulator,
    ) {
        for (id, state) in states {
            let kind_opt = { self.id_to_kind.read().await.get(&id).copied() };
            if let Some(kind) = kind_opt {
                if let Some(shard) = self.shards.get(&kind) {
                    if shard.update_state(&id, Arc::from(state)).await {
                        stats.updated_states += 1;
                    } else {
                        stats.states_missing_in_shard += 1;
                        stats.record_anomaly("states_missing_in_shard", id.as_str());
                    }
                }
            } else {
                stats.updates_for_unknown_pair += 1;
                stats.record_anomaly("updates_for_unknown_pair", id.as_str());
            }
        }
    }

    async fn apply_removed_pairs(
        &self,
        removed_pairs: HashMap<String, ProtocolComponent>,
        index_changes: &mut TokenIndexChanges,
        stats: &mut UpdateAccumulator,
    ) {
        for (id, component) in removed_pairs {
            for token in &component.tokens {
                index_changes
                    .removals
                    .push((token.address.clone(), id.clone()));
            }

            let removed_kind = {
                let mut guard = self.id_to_kind.write().await;
                guard.remove(&id)
            };

            if let Some(kind) = removed_kind {
                if let Some(shard) = self.shards.get(&kind) {
                    shard.remove(&id).await;
                    stats.removed_pairs_count += 1;
                }
            } else {
                stats.removed_unknown_pair += 1;
                stats.record_anomaly("removed_unknown_pair", id.as_str());
            }
        }
    }

    fn update_ready_state(&self, total_pairs: usize) {
        let mut broadcast_value = None;
        if total_pairs > 0 && !self.ready.load(Ordering::Acquire) {
            self.ready.store(true, Ordering::Release);
            broadcast_value = Some(true);
        } else if total_pairs == 0 && self.ready.load(Ordering::Acquire) {
            self.ready.store(false, Ordering::Release);
            broadcast_value = Some(false);
        }

        if let Some(value) = broadcast_value {
            let _ = self.ready_tx.send(value);
        }
    }

    pub async fn current_block(&self) -> u64 {
        let guard = self.block_number.read().await;
        *guard
    }

    pub async fn total_states(&self) -> usize {
        let mut total = 0;
        for shard in self.shards.values() {
            total += shard.len().await;
        }
        total
    }

    pub async fn has_pool(&self, id: &str) -> bool {
        self.id_to_kind.read().await.contains_key(id)
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
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

    fn ids_for_token_from_index(
        &self,
        index: &HashMap<Bytes, HashSet<String>>,
        token: &Bytes,
    ) -> Option<HashSet<String>> {
        let wrapped_native_token = self.tokens.wrapped_native_token();
        let native_address = native_token_address();
        let needs_native = wrapped_native_token.as_ref() == Some(token);
        let needs_wrapped = *token == native_address;

        let mut ids = index.get(token).cloned().unwrap_or_default();

        if needs_native {
            if let Some(native_ids) = index.get(&native_address) {
                ids.extend(native_ids.iter().cloned());
            }
        }

        if needs_wrapped {
            if let Some(wrapped_address) = wrapped_native_token.as_ref() {
                if let Some(wrapped_ids) = index.get(wrapped_address) {
                    ids.extend(wrapped_ids.iter().cloned());
                }
            }
        }

        (!ids.is_empty()).then_some(ids)
    }

    fn intersect_pool_ids(left: HashSet<String>, right: HashSet<String>) -> Vec<String> {
        let (smaller, larger) = if left.len() <= right.len() {
            (left, right)
        } else {
            (right, left)
        };

        let mut candidate_ids = Vec::with_capacity(smaller.len());
        for id in smaller {
            if larger.contains(&id) {
                candidate_ids.push(id);
            }
        }

        candidate_ids
    }

    async fn pool_entries_for_candidate_ids(
        &self,
        candidate_ids: &[String],
    ) -> Vec<(String, PoolEntry)> {
        if candidate_ids.is_empty() {
            return Vec::new();
        }

        let id_kinds: Vec<(String, ProtocolKind)> = {
            let guard = self.id_to_kind.read().await;
            candidate_ids
                .iter()
                .filter_map(|id| guard.get(id).copied().map(|kind| (id.clone(), kind)))
                .collect()
        };

        if id_kinds.is_empty() {
            return Vec::new();
        }

        let mut by_kind: HashMap<ProtocolKind, Vec<String>> = HashMap::new();
        for (id, kind) in id_kinds {
            by_kind.entry(kind).or_default().push(id);
        }

        let mut result = Vec::new();
        for (kind, ids) in by_kind {
            if let Some(shard) = self.shards.get(&kind) {
                let guard = shard.states.read().await;
                for id in ids {
                    if let Some((state, component)) = guard.get(&id) {
                        result.push((id, (Arc::clone(state), Arc::clone(component))));
                    }
                }
            }
        }

        result
    }

    pub(crate) async fn matching_pools_by_addresses(
        &self,
        token_in: &Bytes,
        token_out: &Bytes,
    ) -> Vec<(String, PoolEntry)> {
        // Keep both token ID lookups on the same index snapshot under concurrent updates.
        let (token_in_exact_ids, token_out_exact_ids, token_in_ids, token_out_ids) = {
            let index = self.token_index.read().await;
            (
                index.get(token_in).cloned(),
                index.get(token_out).cloned(),
                self.ids_for_token_from_index(&index, token_in),
                self.ids_for_token_from_index(&index, token_out),
            )
        };

        let exact_candidate_ids = match (token_in_exact_ids, token_out_exact_ids) {
            (Some(token_in_exact_ids), Some(token_out_exact_ids)) => {
                Self::intersect_pool_ids(token_in_exact_ids, token_out_exact_ids)
            }
            _ => Vec::new(),
        };

        // Prefer exact token matches first, but if those IDs are stale by the time we
        // resolve active pools, fall back to alias-expanded candidates.
        if !exact_candidate_ids.is_empty() {
            let exact_entries = self
                .pool_entries_for_candidate_ids(exact_candidate_ids.as_slice())
                .await;
            if !exact_entries.is_empty() {
                return exact_entries;
            }
        }

        let alias_candidate_ids = match (token_in_ids, token_out_ids) {
            (Some(token_in_ids), Some(token_out_ids)) => {
                Self::intersect_pool_ids(token_in_ids, token_out_ids)
            }
            _ => Vec::new(),
        };

        self.pool_entries_for_candidate_ids(alias_candidate_ids.as_slice())
            .await
    }

    pub(crate) async fn pool_by_id(&self, id: &str) -> Option<PoolEntry> {
        let kind_opt = { self.id_to_kind.read().await.get(id).copied() };
        let kind = kind_opt?;
        let shard = self.shards.get(&kind)?;
        let guard = shard.states.read().await;
        guard
            .get(id)
            .map(|(state, component)| (Arc::clone(state), Arc::clone(component)))
    }
}

fn apply_token_index_changes(
    index: &mut HashMap<Bytes, HashSet<String>>,
    index_additions: Vec<(Bytes, String)>,
    index_removals: Vec<(Bytes, String)>,
) {
    for (token, id) in index_additions {
        index.entry(token).or_default().insert(id);
    }

    for (token, id) in index_removals {
        if let Some(set) = index.get_mut(&token) {
            set.remove(&id);
            if set.is_empty() {
                index.remove(&token);
            }
        }
    }
}

fn add_anomaly_sample(samples: &mut Vec<String>, kind: &str, value: &str) {
    if samples.len() >= UPDATE_ANOMALY_SAMPLE_CAP {
        return;
    }
    samples.push(format!("{kind}:{value}"));
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
        protocol::models::ProtocolComponent,
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

    fn token(address: &str) -> Bytes {
        Bytes::from_str(address.trim_start_matches("0x"))
            .unwrap_or_else(|err| unreachable!("valid token address: {err}"))
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

    struct SimConcurrency {
        native: usize,
        vm: usize,
        rfq: usize,
    }

    fn build_test_app_state(
        stores: TestAppStateStores,
        flags: PoolFlags,
        concurrency: SimConcurrency,
    ) -> AppState {
        AppState {
            chain: Chain::Ethereum,
            native_token_protocol_allowlist: Arc::new(vec!["rocketpool".to_string()]),
            tokens: stores.token_store,
            native_state_store: stores.native_state_store,
            vm_state_store: stores.vm_state_store,
            rfq_state_store: stores.rfq_state_store,
            native_stream_health: Arc::new(StreamHealth::new()),
            vm_stream_health: Arc::new(StreamHealth::new()),
            rfq_stream_health: Arc::new(StreamHealth::new()),
            vm_stream: Arc::new(RwLock::new(VmStreamStatus::default())),
            rfq_stream: Arc::new(RwLock::new(RfqStreamStatus::default())),
            enable_vm_pools: flags.enable_vm_pools,
            enable_rfq_pools: flags.enable_rfq_pools,
            readiness_stale: Duration::from_secs(120),
            quote_timeout: Duration::from_millis(100),
            pool_timeout_native: Duration::from_millis(50),
            pool_timeout_vm: Duration::from_millis(50),
            pool_timeout_rfq: Duration::from_millis(50),
            request_timeout: Duration::from_millis(1000),
            native_sim_semaphore: Arc::new(Semaphore::new(concurrency.native)),
            vm_sim_semaphore: Arc::new(Semaphore::new(concurrency.vm)),
            rfq_sim_semaphore: Arc::new(Semaphore::new(concurrency.rfq)),
            erc4626_deposits_enabled: false,
            reset_allowance_tokens: Arc::new(HashMap::new()),
            native_sim_concurrency: concurrency.native,
            vm_sim_concurrency: concurrency.vm,
            rfq_sim_concurrency: concurrency.rfq,
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
            SimConcurrency {
                native: 1,
                vm: 1,
                rfq: 1,
            },
        )
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
                SimConcurrency {
                    native: 1,
                    vm: 1,
                    rfq: 1,
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

        ready_state.readiness_stale = Duration::ZERO;
        assert_eq!(ready_state.native_readiness().await, NativeReadiness::Stale);
    }

    #[tokio::test]
    async fn rfq_and_vm_readiness_distinguishes_disabled_warming_up_rebuilding_ready_and_stale() {
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

        {
            let mut vm_status = state.vm_stream.write().await;
            vm_status.rebuilding = true;
        }
        {
            let mut rfq_status = state.rfq_stream.write().await;
            rfq_status.rebuilding = true;
        }
        assert_eq!(state.vm_readiness().await, VmReadiness::Rebuilding);
        assert_eq!(state.rfq_readiness().await, RfqReadiness::Rebuilding);

        {
            let mut vm_status = state.vm_stream.write().await;
            vm_status.rebuilding = false;
        }
        {
            let mut rfq_status = state.rfq_stream.write().await;
            rfq_status.rebuilding = false;
        }
        state.readiness_stale = Duration::ZERO;
        assert_eq!(state.vm_readiness().await, VmReadiness::Stale);
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
            SimConcurrency {
                native: 1,
                vm: 1,
                rfq: 1,
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

    #[test]
    fn apply_token_index_changes_additions_and_removals() {
        let mut index: HashMap<Bytes, HashSet<String>> = HashMap::new();

        let token_a = token("0x0000000000000000000000000000000000000001");
        let token_b = token("0x0000000000000000000000000000000000000002");

        apply_token_index_changes(
            &mut index,
            vec![
                (token_a.clone(), "pool1".to_string()),
                (token_a.clone(), "pool2".to_string()),
                (token_b.clone(), "pool1".to_string()),
            ],
            vec![],
        );

        assert_eq!(
            index
                .get(&token_a)
                .unwrap_or_else(|| unreachable!("token_a must be indexed")),
            &HashSet::from(["pool1".to_string(), "pool2".to_string()])
        );
        assert_eq!(
            index
                .get(&token_b)
                .unwrap_or_else(|| unreachable!("token_b must be indexed")),
            &HashSet::from(["pool1".to_string()])
        );

        apply_token_index_changes(
            &mut index,
            vec![],
            vec![
                (token_a.clone(), "pool1".to_string()),
                (token_b.clone(), "pool1".to_string()),
            ],
        );

        assert_eq!(
            index
                .get(&token_a)
                .unwrap_or_else(|| unreachable!("token_a must remain indexed")),
            &HashSet::from(["pool2".to_string()])
        );
        assert!(!index.contains_key(&token_b));
    }

    #[test]
    fn apply_token_index_changes_matches_sequential_application() {
        let token_a = token("0x0000000000000000000000000000000000000003");
        let token_b = token("0x0000000000000000000000000000000000000004");

        let additions = vec![
            (token_a.clone(), "pool1".to_string()),
            (token_a.clone(), "pool2".to_string()),
            (token_b.clone(), "pool2".to_string()),
        ];
        let removals = vec![
            (token_a.clone(), "pool1".to_string()),
            (token_b.clone(), "pool2".to_string()),
        ];

        let mut batched: HashMap<Bytes, HashSet<String>> = HashMap::new();
        apply_token_index_changes(&mut batched, additions.clone(), removals.clone());

        let mut sequential: HashMap<Bytes, HashSet<String>> = HashMap::new();
        for (token, id) in additions {
            sequential.entry(token).or_default().insert(id);
        }
        for (token, id) in removals {
            if let Some(set) = sequential.get_mut(&token) {
                set.remove(&id);
                if set.is_empty() {
                    sequential.remove(&token);
                }
            }
        }

        assert_eq!(batched, sequential);
    }

    #[tokio::test]
    async fn apply_update_tracks_anomaly_counters_and_samples() {
        let token_store = Arc::new(TokenStore::new(
            HashMap::new(),
            "http://localhost".to_string(),
            "test".to_string(),
            Chain::Ethereum,
            Duration::from_secs(1),
        ));
        let store = StateStore::new(token_store);

        let token_a = mk_token(20, "TKNA");
        let token_b = mk_token(21, "TKNB");

        let missing_state_component = mk_component(
            30,
            "uniswap_v2",
            "uniswap_v2_pool",
            vec![token_a.clone(), token_b.clone()],
        );
        let unknown_protocol_component = mk_component(
            31,
            "mystery_exchange",
            "mystery_pool",
            vec![token_a.clone(), token_b.clone()],
        );
        let removed_unknown_component =
            mk_component(32, "uniswap_v2", "uniswap_v2_pool", vec![token_a, token_b]);

        store
            .id_to_kind
            .write()
            .await
            .insert("stale-shard".to_string(), ProtocolKind::UniswapV2);

        let mut states: HashMap<String, Box<dyn ProtocolSim>> = HashMap::new();
        states.insert("unknown-update".to_string(), Box::new(DummySim));
        states.insert("stale-shard".to_string(), Box::new(DummySim));

        let mut new_pairs = HashMap::new();
        new_pairs.insert("missing-state".to_string(), missing_state_component);
        new_pairs.insert("unknown-proto".to_string(), unknown_protocol_component);

        let mut removed_pairs = HashMap::new();
        removed_pairs.insert("missing-removal".to_string(), removed_unknown_component);

        let metrics = store
            .apply_update(Update::new(2, states, new_pairs).set_removed_pairs(removed_pairs))
            .await;

        assert_eq!(metrics.new_pairs_missing_state, 1);
        assert_eq!(metrics.unknown_protocol_new_pairs, 1);
        assert_eq!(metrics.updates_for_unknown_pair, 1);
        assert_eq!(metrics.states_missing_in_shard, 1);
        assert_eq!(metrics.removed_unknown_pair, 1);
        assert!(metrics.has_anomalies());

        assert_eq!(metrics.anomaly_samples.len(), 5);
        assert!(metrics
            .anomaly_samples
            .iter()
            .any(|sample| sample == "new_pairs_missing_state:missing-state"));
        assert!(metrics
            .anomaly_samples
            .iter()
            .any(|sample| sample.starts_with("unknown_protocol_new_pairs:")));
        assert!(metrics
            .anomaly_samples
            .iter()
            .any(|sample| sample == "updates_for_unknown_pair:unknown-update"));
        assert!(metrics
            .anomaly_samples
            .iter()
            .any(|sample| sample == "states_missing_in_shard:stale-shard"));
        assert!(metrics
            .anomaly_samples
            .iter()
            .any(|sample| sample == "removed_unknown_pair:missing-removal"));
    }

    #[tokio::test]
    async fn apply_update_caps_anomaly_samples() {
        let token_store = Arc::new(TokenStore::new(
            HashMap::new(),
            "http://localhost".to_string(),
            "test".to_string(),
            Chain::Ethereum,
            Duration::from_secs(1),
        ));
        let store = StateStore::new(token_store);

        let mut states: HashMap<String, Box<dyn ProtocolSim>> = HashMap::new();
        for idx in 0..10 {
            states.insert(format!("unknown-{idx}"), Box::new(DummySim));
        }

        let metrics = store
            .apply_update(Update::new(7, states, HashMap::new()))
            .await;

        assert_eq!(metrics.updates_for_unknown_pair, 10);
        assert_eq!(metrics.anomaly_samples.len(), UPDATE_ANOMALY_SAMPLE_CAP);
        assert!(metrics
            .anomaly_samples
            .iter()
            .all(|sample| sample.starts_with("updates_for_unknown_pair:")));
    }

    #[tokio::test]
    async fn apply_update_without_anomalies_has_zero_counters() {
        let token_store = Arc::new(TokenStore::new(
            HashMap::new(),
            "http://localhost".to_string(),
            "test".to_string(),
            Chain::Ethereum,
            Duration::from_secs(1),
        ));
        let store = StateStore::new(token_store);

        let token_a = mk_token(40, "TKNA");
        let token_b = mk_token(41, "TKNB");
        let component = mk_component(42, "uniswap_v2", "uniswap_v2_pool", vec![token_a, token_b]);

        let metrics = store
            .apply_update(mk_update(vec![(
                "pool-1".to_string(),
                component,
                Box::new(DummySim),
            )]))
            .await;

        assert_eq!(metrics.new_pairs_missing_state, 0);
        assert_eq!(metrics.unknown_protocol_new_pairs, 0);
        assert_eq!(metrics.updates_for_unknown_pair, 0);
        assert_eq!(metrics.states_missing_in_shard, 0);
        assert_eq!(metrics.removed_unknown_pair, 0);
        assert!(metrics.anomaly_samples.is_empty());
        assert!(!metrics.has_anomalies());
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
            SimConcurrency {
                native: 1,
                vm: 1,
                rfq: 1,
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
            SimConcurrency {
                native: 1,
                vm: 1,
                rfq: 1,
            },
        );

        app_state.native_stream_health.record_update(1).await;
        app_state.vm_stream_health.record_update(1).await;
        {
            let mut vm_status = app_state.vm_stream.write().await;
            vm_status.rebuilding = true;
        }

        assert_eq!(app_state.vm_readiness().await, VmReadiness::Rebuilding);
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

    #[tokio::test]
    async fn matching_pools_falls_back_to_alias_when_exact_ids_are_stale() {
        let token_store = Arc::new(TokenStore::new(
            HashMap::new(),
            "http://localhost".to_string(),
            "test".to_string(),
            Chain::Ethereum,
            Duration::from_secs(1),
        ));
        let store = StateStore::new(token_store);

        let wrapped_address = known_address("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2");
        let native_address = native_token_address();
        let token_x = mk_token(12, "TKNX");
        let native_token = Token::new(&native_address, "ETH", 18, 0, &[], Chain::Ethereum, 100);

        // Keep one live native-only pool so alias fallback has a valid target.
        store
            .apply_update(mk_update(vec![(
                "pool-native".to_string(),
                mk_component(
                    22,
                    "rocketpool",
                    "rocketpool",
                    vec![native_token, token_x.clone()],
                ),
                Box::new(DummySim),
            )]))
            .await;

        // Inject stale exact IDs that no longer exist in id_to_kind.
        let mut index_guard = store.token_index.write().await;
        *index_guard = HashMap::from([
            (
                wrapped_address.clone(),
                HashSet::from(["pool-stale".to_string()]),
            ),
            (
                native_address.clone(),
                HashSet::from(["pool-native".to_string()]),
            ),
            (
                token_x.address.clone(),
                HashSet::from(["pool-stale".to_string(), "pool-native".to_string()]),
            ),
        ]);
        drop(index_guard);

        let matches = store
            .matching_pools_by_addresses(&wrapped_address, &token_x.address)
            .await;
        let ids: HashSet<String> = matches.into_iter().map(|(id, _)| id).collect();

        assert_eq!(ids, HashSet::from(["pool-native".to_string()]));
    }

    #[tokio::test]
    async fn matching_pools_reads_token_ids_from_single_snapshot() {
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
            .apply_update(mk_update(vec![
                ("pool-v1".to_string(), component_v1, Box::new(DummySim)),
                ("pool-v2".to_string(), component_v2, Box::new(DummySim)),
            ]))
            .await;

        let token_a_address = token_a.address.clone();
        let token_b_address = token_b.address.clone();
        let mut index_guard = store.token_index.write().await;
        *index_guard = HashMap::from([
            (
                token_a_address.clone(),
                HashSet::from(["pool-v1".to_string()]),
            ),
            (
                token_b_address.clone(),
                HashSet::from(["pool-v1".to_string()]),
            ),
        ]);

        let query_store = Arc::clone(&store);
        let query_token_a = token_a_address.clone();
        let query_token_b = token_b_address.clone();
        let query_task = tokio::spawn(async move {
            query_store
                .matching_pools_by_addresses(&query_token_a, &query_token_b)
                .await
        });

        // Queue the reader before the writer while an external write lock is held.
        // The previous two-read implementation could then observe v1 for token_a and v2
        // for token_b, causing a transient empty intersection.
        tokio::task::yield_now().await;

        let writer_store = Arc::clone(&store);
        let writer_token_a = token_a_address;
        let writer_token_b = token_b_address;
        let writer_task = tokio::spawn(async move {
            let mut index = writer_store.token_index.write().await;
            *index = HashMap::from([
                (writer_token_a, HashSet::from(["pool-v2".to_string()])),
                (writer_token_b, HashSet::from(["pool-v2".to_string()])),
            ]);
        });

        drop(index_guard);

        writer_task
            .await
            .unwrap_or_else(|err| unreachable!("writer task should succeed: {err}"));
        let matches = query_task
            .await
            .unwrap_or_else(|err| unreachable!("query task should succeed: {err}"));
        let ids: HashSet<String> = matches.into_iter().map(|(id, _)| id).collect();

        assert_eq!(ids, HashSet::from(["pool-v1".to_string()]));
    }
}
