use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use serde::Serialize;
use thiserror::Error;
use tycho_simulation::{
    protocol::models::{ProtocolComponent, Update},
    tycho_common::{models::token::Token, simulation::protocol_sim::ProtocolSim, Bytes},
};

use simulator_core::models::protocol::ProtocolKind;

use crate::ReplayBackend;

const UPDATE_ANOMALY_SAMPLE_CAP: usize = 6;
const NATIVE_TOKEN_ADDRESS_BYTES: [u8; 20] = [0u8; 20];

pub type PoolEntry = (Arc<dyn ProtocolSim>, Arc<ProtocolComponent>);

#[derive(Clone)]
struct ReplayState {
    shards: HashMap<ProtocolKind, HashMap<String, PoolEntry>>,
    id_to_kind: HashMap<String, ProtocolKind>,
    token_index: HashMap<Bytes, HashSet<String>>,
    tokens: HashMap<Bytes, Token>,
    wrapped_native_token: Option<Bytes>,
    block_number: u64,
}

impl ReplayState {
    fn empty(tokens: HashMap<Bytes, Token>, wrapped_native_token: Option<Bytes>) -> Self {
        let mut shards = HashMap::new();
        for kind in ProtocolKind::ALL {
            shards.insert(kind, HashMap::new());
        }

        Self {
            shards,
            id_to_kind: HashMap::new(),
            token_index: HashMap::new(),
            tokens,
            wrapped_native_token,
            block_number: 0,
        }
    }
}

#[derive(Clone)]
pub struct StatePoint {
    state: Arc<ReplayState>,
}

impl StatePoint {
    pub fn current_block(&self) -> u64 {
        self.state.block_number
    }

    pub fn total_states(&self) -> usize {
        self.state.id_to_kind.len()
    }

    pub fn token(&self, address: &Bytes) -> Option<Token> {
        self.state.tokens.get(address).cloned()
    }

    pub fn tokens(&self) -> HashMap<Bytes, Token> {
        self.state.tokens.clone()
    }

    pub fn wrapped_native_token(&self) -> Option<Bytes> {
        self.state.wrapped_native_token.clone()
    }

    pub fn matching_pools_by_addresses(
        &self,
        token_in: &Bytes,
        token_out: &Bytes,
    ) -> Vec<(String, PoolEntry)> {
        matching_pools_from_state(&self.state, token_in, token_out)
    }

    pub fn pool_by_id(&self, id: &str) -> Option<PoolEntry> {
        pool_by_id_from_state(&self.state, id)
    }

    pub fn pool_ids_by_protocol_system(&self, protocol_system: &str) -> Vec<String> {
        let mut ids = self
            .state
            .shards
            .values()
            .flat_map(|shard| shard.iter())
            .filter(|(_, (_, component))| component.protocol_system == protocol_system)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    pub fn changed_pool_ids(&self, current: &Self, pool_ids: &HashSet<String>) -> HashSet<String> {
        pool_ids
            .iter()
            .filter(|pool_id| {
                let pinned_entry = self.pool_by_id(pool_id);
                let current_entry = current.pool_by_id(pool_id);
                match (pinned_entry, current_entry) {
                    (None, None) => false,
                    (None, Some(_)) | (Some(_), None) => true,
                    (
                        Some((pinned_state, pinned_component)),
                        Some((current_state, current_component)),
                    ) => {
                        !Arc::ptr_eq(&pinned_state, &current_state)
                            || !Arc::ptr_eq(&pinned_component, &current_component)
                    }
                }
            })
            .cloned()
            .collect()
    }

    pub fn canonical_state(
        &self,
        backends: &[ReplayBackend],
    ) -> Result<CanonicalState, CanonicalStateError> {
        let selected = backends.iter().copied().collect::<BTreeSet<_>>();
        let mut pools = Vec::new();
        let mut token_addresses = BTreeSet::new();
        let mut backend_counts = BTreeMap::new();

        for (kind, shard) in &self.state.shards {
            let backend = ReplayBackend::from_protocol_kind(*kind);
            if !selected.contains(&backend) {
                continue;
            }
            for (component_id, (state, component)) in shard {
                for token in &component.tokens {
                    token_addresses.insert(token.address.clone());
                }
                pools.push(CanonicalPool {
                    backend,
                    component_id: component_id.clone(),
                    component: serde_json::to_value(component.as_ref())?,
                    state: serde_json::to_value(state.as_ref())?,
                });
                *backend_counts.entry(backend).or_insert(0) += 1;
            }
        }
        pools.sort_by(|left, right| {
            (left.backend, left.component_id.as_str())
                .cmp(&(right.backend, right.component_id.as_str()))
        });

        let tokens = token_addresses
            .into_iter()
            .filter_map(|address| self.state.tokens.get(&address))
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?;
        let bytes = serde_json::to_vec(&CanonicalDocument {
            block_number: self.state.block_number,
            pools,
            tokens,
        })?;
        Ok(CanonicalState {
            bytes,
            component_count: backend_counts.values().sum(),
            backend_counts,
        })
    }

    pub(crate) fn pool_by_id_with_kind(&self, id: &str) -> Option<(ProtocolKind, PoolEntry)> {
        let kind = self.state.id_to_kind.get(id).copied()?;
        self.pool_by_id(id).map(|entry| (kind, entry))
    }
}

pub struct ReplayWorld {
    state: Arc<ReplayState>,
}

impl ReplayWorld {
    pub fn new(tokens: HashMap<Bytes, Token>, wrapped_native_token: Option<Bytes>) -> Self {
        Self {
            state: Arc::new(ReplayState::empty(tokens, wrapped_native_token)),
        }
    }

    pub fn from_point(point: &StatePoint) -> Self {
        Self {
            state: Arc::clone(&point.state),
        }
    }

    pub fn restore(&mut self, update: Update) -> ApplyReport {
        let tokens = self.state.tokens.clone();
        let wrapped_native_token = self.state.wrapped_native_token.clone();
        self.state = Arc::new(ReplayState::empty(tokens, wrapped_native_token));
        self.apply(update)
    }

    pub fn apply(&mut self, mut update: Update) -> ApplyReport {
        let state = Arc::make_mut(&mut self.state);
        let block_number = update.block_number_or_timestamp;
        state.block_number = block_number;
        let mut stats = UpdateAccumulator::default();

        apply_new_pairs(
            state,
            std::mem::take(&mut update.new_pairs),
            &mut update.states,
            &mut stats,
        );
        apply_state_updates(state, std::mem::take(&mut update.states), &mut stats);
        apply_removed_pairs(state, std::mem::take(&mut update.removed_pairs), &mut stats);
        for token in &stats.tokens_to_cache {
            state
                .tokens
                .entry(token.address.clone())
                .or_insert_with(|| token.clone());
        }

        ApplyReport {
            block_number,
            updated_states: stats.updated_states,
            new_pairs: stats.new_pairs_count,
            removed_pairs: stats.removed_pairs_count,
            total_pairs: state.id_to_kind.len(),
            new_pairs_missing_state: stats.new_pairs_missing_state,
            unknown_protocol_new_pairs: stats.unknown_protocol_new_pairs,
            updates_for_unknown_pair: stats.updates_for_unknown_pair,
            states_missing_in_shard: stats.states_missing_in_shard,
            removed_unknown_pair: stats.removed_unknown_pair,
            anomaly_samples: stats.anomaly_samples,
            tokens_to_cache: stats.tokens_to_cache,
        }
    }

    pub fn clear(&mut self) {
        let tokens = self.state.tokens.clone();
        let wrapped_native_token = self.state.wrapped_native_token.clone();
        self.state = Arc::new(ReplayState::empty(tokens, wrapped_native_token));
    }

    pub fn reset_protocols(&mut self, protocols: &[ProtocolKind]) -> bool {
        if protocols.is_empty() {
            return false;
        }
        if protocols.len() >= ProtocolKind::ALL.len() {
            self.clear();
            return true;
        }

        let state = Arc::make_mut(&mut self.state);
        let mut any_cleared = false;
        for kind in protocols {
            if let Some(shard) = state.shards.get_mut(kind) {
                shard.clear();
                any_cleared = true;
            }
        }
        if any_cleared {
            rebuild_indexes(state);
        }
        any_cleared
    }

    pub fn pin(&self) -> StatePoint {
        StatePoint {
            state: Arc::clone(&self.state),
        }
    }
}

pub struct ApplyReport {
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
    tokens_to_cache: Vec<Token>,
}

impl ApplyReport {
    pub fn has_anomalies(&self) -> bool {
        self.new_pairs_missing_state > 0
            || self.unknown_protocol_new_pairs > 0
            || self.updates_for_unknown_pair > 0
            || self.states_missing_in_shard > 0
            || self.removed_unknown_pair > 0
    }

    pub fn tokens_to_cache(&self) -> &[Token] {
        &self.tokens_to_cache
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalState {
    bytes: Vec<u8>,
    pub component_count: usize,
    pub backend_counts: BTreeMap<ReplayBackend, usize>,
}

impl CanonicalState {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Debug, Error)]
pub enum CanonicalStateError {
    #[error("failed to serialize canonical simulator state: {0}")]
    Serialize(#[from] serde_json::Error),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalDocument {
    block_number: u64,
    pools: Vec<CanonicalPool>,
    tokens: Vec<serde_json::Value>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalPool {
    backend: ReplayBackend,
    component_id: String,
    component: serde_json::Value,
    state: serde_json::Value,
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
        if self.anomaly_samples.len() >= UPDATE_ANOMALY_SAMPLE_CAP {
            return;
        }
        self.anomaly_samples.push(format!("{kind}:{value}"));
    }
}

fn apply_new_pairs(
    state: &mut ReplayState,
    new_pairs: HashMap<String, ProtocolComponent>,
    states: &mut HashMap<String, Box<dyn ProtocolSim>>,
    stats: &mut UpdateAccumulator,
) {
    for (id, component) in new_pairs {
        let Some(kind) = ProtocolKind::from_component(&component) else {
            stats.unknown_protocol_new_pairs += 1;
            stats.record_anomaly("unknown_protocol_new_pairs", id.as_str());
            continue;
        };
        let Some(protocol_state) = states.remove(&id) else {
            stats.new_pairs_missing_state += 1;
            stats.record_anomaly("new_pairs_missing_state", id.as_str());
            continue;
        };
        let Some(shard) = state.shards.get_mut(&kind) else {
            stats.unknown_protocol_new_pairs += 1;
            stats.record_anomaly("unknown_protocol_new_pairs", id.as_str());
            continue;
        };

        let component = Arc::new(component);
        state.id_to_kind.insert(id.clone(), kind);
        stats
            .tokens_to_cache
            .extend(component.tokens.iter().cloned());
        for token in &component.tokens {
            state
                .token_index
                .entry(token.address.clone())
                .or_default()
                .insert(id.clone());
        }
        shard.insert(id, (Arc::from(protocol_state), component));
        stats.new_pairs_count += 1;
    }
}

fn apply_state_updates(
    state: &mut ReplayState,
    states: HashMap<String, Box<dyn ProtocolSim>>,
    stats: &mut UpdateAccumulator,
) {
    for (id, protocol_state) in states {
        let Some(kind) = state.id_to_kind.get(&id).copied() else {
            stats.updates_for_unknown_pair += 1;
            stats.record_anomaly("updates_for_unknown_pair", id.as_str());
            continue;
        };
        let Some(shard) = state.shards.get_mut(&kind) else {
            stats.states_missing_in_shard += 1;
            stats.record_anomaly("states_missing_in_shard", id.as_str());
            continue;
        };
        if let Some((existing_state, _)) = shard.get_mut(&id) {
            *existing_state = Arc::from(protocol_state);
            stats.updated_states += 1;
        } else {
            stats.states_missing_in_shard += 1;
            stats.record_anomaly("states_missing_in_shard", id.as_str());
        }
    }
}

fn apply_removed_pairs(
    state: &mut ReplayState,
    removed_pairs: HashMap<String, ProtocolComponent>,
    stats: &mut UpdateAccumulator,
) {
    for (id, component) in removed_pairs {
        for token in &component.tokens {
            if let Some(pool_ids) = state.token_index.get_mut(&token.address) {
                pool_ids.remove(&id);
                if pool_ids.is_empty() {
                    state.token_index.remove(&token.address);
                }
            }
        }

        if let Some(kind) = state.id_to_kind.remove(&id) {
            if let Some(shard) = state.shards.get_mut(&kind) {
                shard.remove(&id);
                stats.removed_pairs_count += 1;
            }
        } else {
            stats.removed_unknown_pair += 1;
            stats.record_anomaly("removed_unknown_pair", id.as_str());
        }
    }
}

fn rebuild_indexes(state: &mut ReplayState) {
    state.id_to_kind.clear();
    state.token_index.clear();
    for (kind, shard) in &state.shards {
        for (id, (_, component)) in shard {
            state.id_to_kind.insert(id.clone(), *kind);
            for token in &component.tokens {
                state
                    .token_index
                    .entry(token.address.clone())
                    .or_default()
                    .insert(id.clone());
            }
        }
    }
}

fn native_token_address() -> Bytes {
    Bytes::from(NATIVE_TOKEN_ADDRESS_BYTES)
}

fn ids_for_token(state: &ReplayState, token: &Bytes) -> Option<HashSet<String>> {
    let native_address = native_token_address();
    let needs_native = state.wrapped_native_token.as_ref() == Some(token);
    let needs_wrapped = *token == native_address;
    let mut ids = state.token_index.get(token).cloned().unwrap_or_default();
    if needs_native {
        if let Some(native_ids) = state.token_index.get(&native_address) {
            ids.extend(native_ids.iter().cloned());
        }
    }
    if needs_wrapped {
        if let Some(wrapped_address) = state.wrapped_native_token.as_ref() {
            if let Some(wrapped_ids) = state.token_index.get(wrapped_address) {
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
    smaller
        .into_iter()
        .filter(|id| larger.contains(id))
        .collect()
}

fn pool_entries_for_ids(state: &ReplayState, ids: Vec<String>) -> Vec<(String, PoolEntry)> {
    ids.into_iter()
        .filter_map(|id| pool_by_id_from_state(state, &id).map(|entry| (id, entry)))
        .collect()
}

fn matching_pools_from_state(
    state: &ReplayState,
    token_in: &Bytes,
    token_out: &Bytes,
) -> Vec<(String, PoolEntry)> {
    if let (Some(token_in_ids), Some(token_out_ids)) = (
        state.token_index.get(token_in).cloned(),
        state.token_index.get(token_out).cloned(),
    ) {
        let exact = pool_entries_for_ids(state, intersect_pool_ids(token_in_ids, token_out_ids));
        if !exact.is_empty() {
            return exact;
        }
    }

    match (
        ids_for_token(state, token_in),
        ids_for_token(state, token_out),
    ) {
        (Some(token_in_ids), Some(token_out_ids)) => {
            pool_entries_for_ids(state, intersect_pool_ids(token_in_ids, token_out_ids))
        }
        _ => Vec::new(),
    }
}

fn pool_by_id_from_state(state: &ReplayState, id: &str) -> Option<PoolEntry> {
    let kind = state.id_to_kind.get(id)?;
    let (protocol_state, component) = state.shards.get(kind)?.get(id)?;
    Some((Arc::clone(protocol_state), Arc::clone(component)))
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use num_bigint::BigUint;
    use tycho_simulation::tycho_common::{
        dto::ProtocolStateDelta,
        models::{token::Token, Chain},
        simulation::{
            errors::{SimulationError, TransitionError},
            protocol_sim::{Balances, GetAmountOutResult, ProtocolSim},
        },
        Bytes,
    };

    use super::*;

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
            amount_in: BigUint,
            _token_in: &Token,
            _token_out: &Token,
        ) -> Result<GetAmountOutResult, SimulationError> {
            Ok(GetAmountOutResult::new(
                amount_in,
                BigUint::from(0_u8),
                self.clone_box(),
            ))
        }

        fn get_limits(
            &self,
            _sell_token: Bytes,
            _buy_token: Bytes,
        ) -> Result<(BigUint, BigUint), SimulationError> {
            Ok((BigUint::from(0_u8), BigUint::from(0_u8)))
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
            other.as_any().is::<Self>()
        }
    }

    fn address(seed: u8) -> Bytes {
        Bytes::from([seed; 20])
    }

    fn token(seed: u8, symbol: &str) -> Token {
        Token::new(&address(seed), symbol, 18, 0, &[], Chain::Ethereum, 100)
    }

    fn component(
        seed: u8,
        protocol_system: &str,
        protocol_type_name: &str,
        tokens: Vec<Token>,
    ) -> ProtocolComponent {
        ProtocolComponent::new(
            address(seed),
            protocol_system.to_string(),
            protocol_type_name.to_string(),
            Chain::Ethereum,
            tokens,
            Vec::new(),
            HashMap::new(),
            Bytes::new(),
            chrono::NaiveDateTime::default(),
        )
    }

    fn pair_update(id: &str, component: ProtocolComponent) -> Update {
        Update::new(
            1,
            HashMap::from([(id.to_string(), Box::new(DummySim) as Box<dyn ProtocolSim>)]),
            HashMap::from([(id.to_string(), component)]),
        )
    }

    #[test]
    fn apply_tracks_all_anomaly_counters_and_samples() {
        let token_a = token(20, "TKNA");
        let token_b = token(21, "TKNB");
        let missing_state = component(
            30,
            "uniswap_v2",
            "uniswap_v2_pool",
            vec![token_a.clone(), token_b.clone()],
        );
        let unknown_protocol = component(
            31,
            "mystery_exchange",
            "mystery_pool",
            vec![token_a.clone(), token_b.clone()],
        );
        let removed_unknown =
            component(32, "uniswap_v2", "uniswap_v2_pool", vec![token_a, token_b]);
        let mut world = ReplayWorld::new(HashMap::new(), None);
        Arc::make_mut(&mut world.state)
            .id_to_kind
            .insert("stale-shard".to_string(), ProtocolKind::UniswapV2);

        let states = HashMap::from([
            (
                "unknown-update".to_string(),
                Box::new(DummySim) as Box<dyn ProtocolSim>,
            ),
            (
                "stale-shard".to_string(),
                Box::new(DummySim) as Box<dyn ProtocolSim>,
            ),
        ]);
        let new_pairs = HashMap::from([
            ("missing-state".to_string(), missing_state),
            ("unknown-proto".to_string(), unknown_protocol),
        ]);
        let removed_pairs = HashMap::from([("missing-removal".to_string(), removed_unknown)]);

        let report =
            world.apply(Update::new(2, states, new_pairs).set_removed_pairs(removed_pairs));

        assert_eq!(report.new_pairs_missing_state, 1);
        assert_eq!(report.unknown_protocol_new_pairs, 1);
        assert_eq!(report.updates_for_unknown_pair, 1);
        assert_eq!(report.states_missing_in_shard, 1);
        assert_eq!(report.removed_unknown_pair, 1);
        assert!(report.has_anomalies());
        assert_eq!(report.anomaly_samples.len(), 5);
        assert!(report
            .anomaly_samples
            .iter()
            .any(|sample| sample == "new_pairs_missing_state:missing-state"));
        assert!(report
            .anomaly_samples
            .iter()
            .any(|sample| sample.starts_with("unknown_protocol_new_pairs:")));
        assert!(report
            .anomaly_samples
            .iter()
            .any(|sample| sample == "updates_for_unknown_pair:unknown-update"));
        assert!(report
            .anomaly_samples
            .iter()
            .any(|sample| sample == "states_missing_in_shard:stale-shard"));
        assert!(report
            .anomaly_samples
            .iter()
            .any(|sample| sample == "removed_unknown_pair:missing-removal"));
    }

    #[test]
    fn apply_caps_anomaly_samples() {
        let states = (0..10)
            .map(|index| {
                (
                    format!("unknown-{index}"),
                    Box::new(DummySim) as Box<dyn ProtocolSim>,
                )
            })
            .collect();
        let mut world = ReplayWorld::new(HashMap::new(), None);

        let report = world.apply(Update::new(7, states, HashMap::new()));

        assert_eq!(report.updates_for_unknown_pair, 10);
        assert_eq!(report.anomaly_samples.len(), UPDATE_ANOMALY_SAMPLE_CAP);
        assert!(report
            .anomaly_samples
            .iter()
            .all(|sample| sample.starts_with("updates_for_unknown_pair:")));
    }

    #[test]
    fn clean_apply_has_zero_anomaly_counters() {
        let token_a = token(40, "TKNA");
        let token_b = token(41, "TKNB");
        let pool = component(42, "uniswap_v2", "uniswap_v2_pool", vec![token_a, token_b]);
        let mut world = ReplayWorld::new(HashMap::new(), None);

        let report = world.apply(pair_update("pool-1", pool));

        assert_eq!(report.new_pairs_missing_state, 0);
        assert_eq!(report.unknown_protocol_new_pairs, 0);
        assert_eq!(report.updates_for_unknown_pair, 0);
        assert_eq!(report.states_missing_in_shard, 0);
        assert_eq!(report.removed_unknown_pair, 0);
        assert!(report.anomaly_samples.is_empty());
        assert!(!report.has_anomalies());
    }

    #[test]
    fn matching_pools_falls_back_to_alias_when_exact_ids_are_stale() {
        let wrapped_address = address(50);
        let native_address = native_token_address();
        let token_out = token(51, "TKNX");
        let native_token = Token::new(&native_address, "ETH", 18, 0, &[], Chain::Ethereum, 100);
        let pool = component(
            52,
            "rocketpool",
            "rocketpool",
            vec![native_token, token_out.clone()],
        );
        let mut world = ReplayWorld::new(HashMap::new(), Some(wrapped_address.clone()));
        world.apply(pair_update("pool-native", pool));
        Arc::make_mut(&mut world.state).token_index = HashMap::from([
            (
                wrapped_address.clone(),
                HashSet::from(["pool-stale".to_string()]),
            ),
            (native_address, HashSet::from(["pool-native".to_string()])),
            (
                token_out.address.clone(),
                HashSet::from(["pool-stale".to_string(), "pool-native".to_string()]),
            ),
        ]);

        let ids = world
            .pin()
            .matching_pools_by_addresses(&wrapped_address, &token_out.address)
            .into_iter()
            .map(|(id, _)| id)
            .collect::<HashSet<_>>();

        assert_eq!(ids, HashSet::from(["pool-native".to_string()]));
    }
}
