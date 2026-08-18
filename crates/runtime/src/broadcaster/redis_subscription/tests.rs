use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::keccak256;
use anyhow::{anyhow, Result};
use num_bigint::BigUint;
use sha2::{Digest, Sha256};
use simulator_replay::{DecoderConfig, ReplayBackend, ReplayDecoder};
use tokio::sync::RwLock;
use tycho_simulation::tycho_common::dto::{BlockAggregatedChanges, ProtocolStateDelta};
use tycho_simulation::tycho_common::simulation::errors::{SimulationError, TransitionError};
use tycho_simulation::{
    evm::decoder::TychoStreamDecoder,
    protocol::{
        errors::InvalidSnapshotError,
        models::{DecoderContext, ProtocolComponent, TryFromWithBlock, Update},
    },
    tycho_client::feed::{
        synchronizer::{ComponentWithState, Snapshot, StateSyncMessage},
        BlockHeader, SynchronizerState,
    },
    tycho_common::{
        dto::{
            Chain as DtoChain, ComponentBalance, EntryPoint,
            ProtocolComponent as DtoProtocolComponent, RPCTracerParams, ResponseAccount,
            ResponseProtocolState, ResponseToken, TokenBalances, TracingParams,
        },
        models::{token::Token, Chain},
        simulation::protocol_sim::{Balances, GetAmountOutResult, ProtocolSim},
        Bytes,
    },
};

use super::processor::{
    handle_subscription_reset, BroadcasterSubscriptionProcessor, PreparedRedisProcessor,
};
use super::snapshot::RawSnapshotReassembly;
use super::{
    apply_recovery_message, apply_replay_batch, committed_recovery_complete_native_block,
    mark_redis_replay_checkpoints, mark_redis_transport_failed,
    process_broadcaster_redis_subscription, redis_transport_error, replay_error_exit,
    reset_outer_backoff_after_catch_up, subscription_exit_requires_rebuild,
    BroadcasterSubscriptionControls, NativeBroadcasterSubscriptionControls,
    PreparedBroadcasterRedisSubscription, RedisRetrySleeper, ReplayPollSource,
    SubscriptionExitReason, VmBroadcasterSubscriptionControls,
};
use crate::broadcaster::state::BroadcasterSnapshotCache;
use crate::config::MemoryConfig;
use crate::models::state::{BroadcasterSubscriptionStatus, StateStore, VmStreamStatus};
use crate::models::stream_health::StreamHealth;
use crate::models::tokens::TokenStore;
use crate::services::simulation_executor::{SimulationExecutionError, SimulationExecutor};
use crate::stream::StreamSupervisorConfig;
use broadcaster_replay_client::{
    BroadcasterReplayClientError, ReplayBatch, ReplayCheckpoint, ReplayMessage, ReplayPoll,
};
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterHeartbeat,
    BroadcasterPayload, BroadcasterProtocolMessage, BroadcasterProtocolSyncStatus,
    BroadcasterRecoveryCatchUp, BroadcasterRecoveryChunk, BroadcasterRecoveryCommit,
    BroadcasterRecoveryManifest, BroadcasterRecoveryStart, BroadcasterRedisReplayBoundary,
    BroadcasterRedisStreamEntry, BroadcasterSnapshotChunk, BroadcasterSnapshotEnd,
    BroadcasterSnapshotPartition, BroadcasterSnapshotStart, BroadcasterStateDelta,
    BroadcasterStateEntry, BroadcasterTokenDto, BroadcasterUpdateMessage,
    BroadcasterUpdatePartition,
};

const NATIVE_CHECKPOINT_V1: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../simulator-replay/tests/fixtures/native_checkpoint_v1.json"
));
const NATIVE_DELTA_V1: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../simulator-replay/tests/fixtures/native_delta_v1.json"
));
const RFQ_CHECKPOINT_V1: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../simulator-replay/tests/fixtures/rfq_checkpoint_v1.json"
));
const RFQ_DELTA_V1: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../simulator-replay/tests/fixtures/rfq_delta_v1.json"
));

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct DummySim(u8);

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
            BigUint::from(0u8),
            self.clone_box(),
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
        other
            .as_any()
            .downcast_ref::<DummySim>()
            .map(|value| value.0 == self.0)
            .unwrap_or(false)
    }
}

impl TryFromWithBlock<ComponentWithState, BlockHeader> for DummySim {
    type Error = InvalidSnapshotError;

    async fn try_from_with_header(
        _snapshot: ComponentWithState,
        _block: BlockHeader,
        _account_balances: &HashMap<Bytes, HashMap<Bytes, Bytes>>,
        _all_tokens: &HashMap<Bytes, Token>,
        _decoder_context: &DecoderContext,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self(0))
    }
}

#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
struct StatefulSim {
    attributes: HashMap<String, Bytes>,
    balances: HashMap<Bytes, Bytes>,
}

#[typetag::serde]
impl ProtocolSim for StatefulSim {
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
            BigUint::from(0u8),
            self.clone_box(),
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
        delta: ProtocolStateDelta,
        _tokens: &HashMap<Bytes, Token>,
        balances: &Balances,
    ) -> Result<(), TransitionError> {
        for attribute in delta.deleted_attributes {
            self.attributes.remove(&attribute);
        }
        self.attributes.extend(delta.updated_attributes);
        if let Some(component_balances) = balances.component_balances.get(&delta.component_id) {
            self.balances.clone_from(component_balances);
        }
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
        other.as_any().downcast_ref::<Self>() == Some(self)
    }
}

impl TryFromWithBlock<ComponentWithState, BlockHeader> for StatefulSim {
    type Error = InvalidSnapshotError;

    async fn try_from_with_header(
        snapshot: ComponentWithState,
        block: BlockHeader,
        _account_balances: &HashMap<Bytes, HashMap<Bytes, Bytes>>,
        _all_tokens: &HashMap<Bytes, Token>,
        _decoder_context: &DecoderContext,
    ) -> std::result::Result<Self, Self::Error> {
        let mut attributes = snapshot.state.attributes;
        attributes.insert(
            "block_number".to_string(),
            Bytes::from(block.number.to_be_bytes().to_vec()),
        );
        attributes.insert(
            "block_timestamp".to_string(),
            Bytes::from(block.timestamp.to_be_bytes().to_vec()),
        );
        Ok(Self {
            attributes,
            balances: snapshot.state.balances,
        })
    }
}

struct TestControls {
    token_store: Arc<TokenStore>,
    native_subscription: BroadcasterSubscriptionStatus,
    vm_subscription: BroadcasterSubscriptionStatus,
    rfq_subscription: BroadcasterSubscriptionStatus,
    native_state_store: Arc<StateStore>,
    vm_state_store: Arc<StateStore>,
    rfq_state_store: Arc<StateStore>,
    native_stream_health: Arc<StreamHealth>,
    vm_stream_health: Arc<StreamHealth>,
    rfq_stream_health: Arc<StreamHealth>,
    vm_stream: Arc<RwLock<VmStreamStatus>>,
    vm_simulation_rebuild_gate: Arc<RwLock<()>>,
    rfq_simulation_rebuild_gate: Arc<RwLock<()>>,
}

impl TestControls {
    fn new() -> Self {
        let token_store = Arc::new(TokenStore::new(
            HashMap::new(),
            "http://localhost".to_string(),
            "test".to_string(),
            Chain::Ethereum,
            Duration::from_millis(10),
        ));

        Self {
            token_store: Arc::clone(&token_store),
            native_subscription: BroadcasterSubscriptionStatus::default(),
            vm_subscription: BroadcasterSubscriptionStatus::default(),
            rfq_subscription: BroadcasterSubscriptionStatus::default(),
            native_state_store: Arc::new(StateStore::new(Arc::clone(&token_store))),
            vm_state_store: Arc::new(StateStore::new(Arc::clone(&token_store))),
            rfq_state_store: Arc::new(StateStore::new(Arc::clone(&token_store))),
            native_stream_health: Arc::new(StreamHealth::new()),
            vm_stream_health: Arc::new(StreamHealth::new()),
            rfq_stream_health: Arc::new(StreamHealth::new()),
            vm_stream: Arc::new(RwLock::new(VmStreamStatus::default())),
            vm_simulation_rebuild_gate: Arc::new(RwLock::new(())),
            rfq_simulation_rebuild_gate: Arc::new(RwLock::new(())),
        }
    }

    fn native(&self) -> BroadcasterSubscriptionControls {
        BroadcasterSubscriptionControls::Native(NativeBroadcasterSubscriptionControls {
            broadcaster_subscription: self.native_subscription.clone(),
            state_store: Arc::clone(&self.native_state_store),
            stream_health: Arc::clone(&self.native_stream_health),
            tokens: Arc::clone(&self.token_store),
            protocols: vec!["uniswap_v2".to_string()],
        })
    }

    fn vm(&self) -> BroadcasterSubscriptionControls {
        BroadcasterSubscriptionControls::Vm(VmBroadcasterSubscriptionControls {
            broadcaster_subscription: self.vm_subscription.clone(),
            state_store: Arc::clone(&self.vm_state_store),
            stream_health: Arc::clone(&self.vm_stream_health),
            tokens: Arc::clone(&self.token_store),
            protocols: vec!["vm:curve".to_string()],
            vm_stream: Arc::clone(&self.vm_stream),
            simulation_rebuild_gate: Arc::clone(&self.vm_simulation_rebuild_gate),
            wire_backend: BroadcasterBackend::Vm,
            recovery_guard_held: false,
        })
    }

    fn rfq(&self) -> BroadcasterSubscriptionControls {
        BroadcasterSubscriptionControls::Rfq(super::RfqBroadcasterSubscriptionControls {
            broadcaster_subscription: self.rfq_subscription.clone(),
            state_store: Arc::clone(&self.rfq_state_store),
            stream_health: Arc::clone(&self.rfq_stream_health),
            tokens: Arc::clone(&self.token_store),
            protocols: vec!["rfq:hashflow".to_string()],
            simulation_rebuild_gate: Arc::clone(&self.rfq_simulation_rebuild_gate),
        })
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LiveParityCheckpoint {
    chain_id: u64,
    tokens: Vec<BroadcasterTokenDto>,
    partition: BroadcasterSnapshotPartition,
    quote: LiveParityQuote,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LiveParityDelta {
    applicable_backends: Vec<BroadcasterBackend>,
    update: BroadcasterUpdateMessage,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LiveParityQuote {
    backend: BroadcasterBackend,
    protocol: String,
    component_id: String,
    token_in: Bytes,
    token_out: Bytes,
    amount_in: String,
}

struct LiveParityObservation {
    state_digest: String,
    amount_out: String,
    block_number: u64,
    state_count: usize,
}

#[tokio::test]
async fn native_and_bebop_fixtures_characterize_current_live_path() -> Result<()> {
    let native = run_current_live_fixture(NATIVE_CHECKPOINT_V1, NATIVE_DELTA_V1).await?;
    assert_eq!(native.block_number, 101);
    assert_eq!(native.state_count, 1);
    assert_eq!(native.amount_out, "1660562");
    assert_eq!(
        native.state_digest,
        "2ced602b273a04c50000edb041a94f9df77862c2889cbce5ca76438adf4596e3"
    );

    let rfq = run_current_live_fixture(RFQ_CHECKPOINT_V1, RFQ_DELTA_V1).await?;
    assert_eq!(rfq.block_number, 1_710_000_005);
    assert_eq!(rfq.state_count, 1);
    assert_eq!(rfq.amount_out, "1995000000");
    assert_eq!(
        rfq.state_digest,
        "e3f5c9f476d43de6e4eb7840644c5beccd90e00c9076334e02a958bee6d3f201"
    );
    Ok(())
}

#[tokio::test]
async fn live_apply_anomaly_report_is_characterized() {
    let controls = TestControls::new();
    let update = Update::new(
        102,
        HashMap::new(),
        HashMap::from([("missing-state".to_string(), native_component())]),
    );

    let report = controls.native_state_store.apply_update(update).await;

    assert_eq!(report.block_number, 102);
    assert_eq!(report.new_pairs_missing_state, 1);
    assert_eq!(report.total_pairs, 0);
    assert_eq!(
        report.anomaly_samples,
        vec!["new_pairs_missing_state:missing-state"]
    );
    assert!(report.has_anomalies());
}

#[tokio::test]
#[expect(
    clippy::panic,
    reason = "the characterization test verifies current panic containment"
)]
async fn live_quote_worker_panic_is_contained() {
    let result = SimulationExecutor::new()
        .run(|| panic!("fixture quote panic"))
        .await;

    assert!(matches!(
        result,
        Err(SimulationExecutionError::WorkerPanicked(_))
    ));
}

#[expect(
    clippy::too_many_lines,
    reason = "the characterization keeps one complete runtime replay path visible"
)]
async fn run_current_live_fixture(
    checkpoint_json: &str,
    delta_json: &str,
) -> Result<LiveParityObservation> {
    let checkpoint: LiveParityCheckpoint = serde_json::from_str(checkpoint_json)?;
    let delta: LiveParityDelta = serde_json::from_str(delta_json)?;
    let chain = Chain::Base;
    if checkpoint.chain_id != chain.id() {
        return Err(anyhow!(
            "fixture chain mismatch: expected {}, got {}",
            chain.id(),
            checkpoint.chain_id
        ));
    }
    if checkpoint.partition.backend != checkpoint.quote.backend {
        return Err(anyhow!("fixture checkpoint backend does not match quote"));
    }
    if !delta
        .applicable_backends
        .contains(&checkpoint.quote.backend)
    {
        return Err(anyhow!("fixture delta does not apply to the quote backend"));
    }

    let tokens = checkpoint
        .tokens
        .into_iter()
        .map(|token| token.into_token(chain))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let initial_tokens = tokens
        .into_iter()
        .map(|token| (token.address.clone(), token))
        .collect();
    let token_store = Arc::new(TokenStore::new(
        initial_tokens,
        "http://localhost".to_string(),
        "fixture".to_string(),
        chain,
        Duration::from_millis(10),
    ));
    let state_store = Arc::new(StateStore::new(Arc::clone(&token_store)));
    let controls = fixture_subscription_controls(
        checkpoint.quote.backend,
        checkpoint.quote.protocol.clone(),
        Arc::clone(&token_store),
        Arc::clone(&state_store),
    )?;
    let mut processor = BroadcasterSubscriptionProcessor::new(chain.id(), controls, None);

    processor
        .observe(BroadcasterEnvelope::new(
            "fixture-stream",
            1,
            BroadcasterPayload::SnapshotStart(BroadcasterSnapshotStart::new(
                "fixture-snapshot",
                chain.id(),
                vec![checkpoint.quote.backend],
                1,
            )?),
        ))
        .await?;
    processor
        .observe(BroadcasterEnvelope::new(
            "fixture-stream",
            2,
            BroadcasterPayload::SnapshotChunk(BroadcasterSnapshotChunk::new(
                "fixture-snapshot",
                0,
                vec![checkpoint.partition],
            )?),
        ))
        .await?;
    processor
        .observe(BroadcasterEnvelope::new(
            "fixture-stream",
            3,
            BroadcasterPayload::SnapshotEnd(BroadcasterSnapshotEnd::new("fixture-snapshot")),
        ))
        .await?;
    processor
        .observe(BroadcasterEnvelope::new(
            "fixture-stream",
            4,
            BroadcasterPayload::Update(delta.update),
        ))
        .await?;

    let pin = state_store.pin().await;
    let (pool_state, component) = pin
        .pool_by_id(&checkpoint.quote.component_id)
        .ok_or_else(|| anyhow!("fixture pool was not published"))?;
    if component.protocol_system != checkpoint.quote.protocol {
        return Err(anyhow!("fixture protocol was not preserved"));
    }
    let token_in = pin
        .token(&checkpoint.quote.token_in)
        .ok_or_else(|| anyhow!("fixture input token was not published"))?;
    let token_out = pin
        .token(&checkpoint.quote.token_out)
        .ok_or_else(|| anyhow!("fixture output token was not published"))?;
    let amount_out = pool_state
        .get_amount_out(checkpoint.quote.amount_in.parse()?, &token_in, &token_out)?
        .amount
        .to_string();
    let canonical = serde_json::json!({
        "blockNumber": pin.current_block(),
        "component": serde_json::to_value(component.as_ref())?,
        "state": serde_json::to_value(pool_state.as_ref())?,
        "tokenIn": token_in,
        "tokenOut": token_out,
        "totalStates": pin.total_states(),
    });
    let state_digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&canonical)?));

    Ok(LiveParityObservation {
        state_digest,
        amount_out,
        block_number: pin.current_block(),
        state_count: pin.total_states(),
    })
}

fn fixture_subscription_controls(
    backend: BroadcasterBackend,
    protocol: String,
    tokens: Arc<TokenStore>,
    state_store: Arc<StateStore>,
) -> Result<BroadcasterSubscriptionControls> {
    let broadcaster_subscription = BroadcasterSubscriptionStatus::default();
    let stream_health = Arc::new(StreamHealth::new());
    match backend {
        BroadcasterBackend::Native => Ok(BroadcasterSubscriptionControls::Native(
            NativeBroadcasterSubscriptionControls {
                broadcaster_subscription,
                state_store,
                stream_health,
                tokens,
                protocols: vec![protocol],
            },
        )),
        BroadcasterBackend::Rfq => Ok(BroadcasterSubscriptionControls::Rfq(
            super::RfqBroadcasterSubscriptionControls {
                broadcaster_subscription,
                state_store,
                stream_health,
                tokens,
                protocols: vec![protocol],
                simulation_rebuild_gate: Arc::new(RwLock::new(())),
            },
        )),
        BroadcasterBackend::Vm => Err(anyhow!("VM is outside the Phase 2 parity fixture set")),
    }
}

#[test]
fn redis_transport_errors_retry_without_rebootstrap() {
    let read = BroadcasterReplayClientError::RedisReadTransport {
        message: "timeout".to_string(),
    };
    let inspect = BroadcasterReplayClientError::RedisInspectTransport {
        message: "connection reset".to_string(),
    };

    assert!(redis_transport_error(&read));
    assert!(redis_transport_error(&inspect));
}

#[test]
fn replay_data_errors_keep_state_invalidating_reason() {
    let decode = replay_error_exit(BroadcasterReplayClientError::RedisDecode {
        message: "bad payload".to_string(),
    });
    let gap = replay_error_exit(BroadcasterReplayClientError::RedisGap {
        message: "missing entry".to_string(),
    });

    assert_eq!(decode.reason, SubscriptionExitReason::RedisDecode);
    assert_eq!(gap.reason, SubscriptionExitReason::RedisGap);
}

#[tokio::test]
async fn transient_redis_failures_preserve_checkpoint_and_reset_backoffs() -> Result<()> {
    let (controls, prepared) = prepared_native_replay_subscription().await?;
    let source = FakeReplayPollSource::new([
        FakeReplayPoll::RetryableServerError,
        FakeReplayPoll::Batch,
        FakeReplayPoll::CaughtUp,
        FakeReplayPoll::TransportError,
        FakeReplayPoll::CaughtUp,
        FakeReplayPoll::DecodeError,
    ]);
    let sleeper = RecordingRetrySleeper::default();
    let cfg = redis_test_supervisor_config();

    let (exit, _rebuilds, caught_up_once) =
        process_broadcaster_redis_subscription(&source, prepared, &cfg, &sleeper).await;

    assert_eq!(exit.reason, SubscriptionExitReason::RedisDecode);
    assert!(caught_up_once);
    assert_eq!(
        source.observed_checkpoints(),
        vec!["7-103", "7-103", "7-104", "7-104", "7-104", "7-104"],
        "transport retries must resume from the last accepted checkpoint"
    );
    assert_eq!(
        sleeper.durations(),
        vec![cfg.restart_backoff_min, cfg.restart_backoff_min],
        "a successful Redis command must reset transport backoff"
    );
    assert_eq!(
        reset_outer_backoff_after_catch_up(cfg.restart_backoff_max, &cfg, caught_up_once),
        cfg.restart_backoff_min
    );

    let snapshot = controls.native_subscription.snapshot().await;
    assert!(snapshot.connected);
    assert!(snapshot.bootstrap_complete);
    assert_eq!(snapshot.redis_replay_checkpoint.as_deref(), Some("7-104"));
    assert_eq!(snapshot.restart_count, 0);
    assert_eq!(snapshot.redis_transport_retry_count, 2);
    assert_eq!(controls.native_state_store.current_block().await, 71);
    assert_eq!(controls.native_state_store.total_states().await, 1);
    let (state, _) = controls
        .native_state_store
        .pool_by_id("pool-native")
        .await
        .ok_or_else(|| anyhow!("native pool should remain available"))?;
    assert_eq!(
        state
            .as_any()
            .downcast_ref::<DummySim>()
            .map(|state| state.0),
        Some(3),
        "the replay batch must apply its state transition exactly once"
    );
    Ok(())
}

#[tokio::test]
async fn permanent_redis_command_failure_stops_without_rebuilding_state() -> Result<()> {
    let (controls, prepared) = prepared_native_replay_subscription().await?;
    let source = FakeReplayPollSource::new([FakeReplayPoll::PermanentCommandError]);
    let sleeper = RecordingRetrySleeper::default();
    let cfg = redis_test_supervisor_config();
    let subscription_controls = vec![controls.native()];

    let (exit, _rebuilds, caught_up_once) =
        process_broadcaster_redis_subscription(&source, prepared, &cfg, &sleeper).await;

    assert_eq!(exit.reason, SubscriptionExitReason::RedisCommand);
    assert!(!subscription_exit_requires_rebuild(exit.reason));
    assert!(!caught_up_once);
    assert_eq!(source.observed_checkpoints(), vec!["7-103"]);
    assert!(sleeper.durations().is_empty());

    mark_redis_transport_failed(&subscription_controls, exit.message).await;

    let snapshot = controls.native_subscription.snapshot().await;
    assert!(snapshot.connected);
    assert!(snapshot.bootstrap_complete);
    assert_eq!(snapshot.redis_replay_checkpoint.as_deref(), Some("7-103"));
    assert_eq!(snapshot.restart_count, 0);
    assert_eq!(
        snapshot.redis_transport_status,
        crate::models::state::RedisTransportStatus::Failed
    );
    assert_eq!(controls.native_state_store.current_block().await, 70);
    assert_eq!(controls.native_state_store.total_states().await, 1);
    Ok(())
}

#[derive(Clone, Copy)]
enum FakeReplayPoll {
    RetryableServerError,
    TransportError,
    PermanentCommandError,
    Batch,
    CaughtUp,
    DecodeError,
}

struct FakeReplayPollSource {
    polls: std::sync::Mutex<std::collections::VecDeque<FakeReplayPoll>>,
    observed_checkpoints: std::sync::Mutex<Vec<String>>,
}

impl FakeReplayPollSource {
    fn new(polls: impl IntoIterator<Item = FakeReplayPoll>) -> Self {
        Self {
            polls: std::sync::Mutex::new(polls.into_iter().collect()),
            observed_checkpoints: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn observed_checkpoints(&self) -> Vec<String> {
        self.observed_checkpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl ReplayPollSource for FakeReplayPollSource {
    async fn read_next<'a>(
        &'a self,
        checkpoint: &'a ReplayCheckpoint,
    ) -> std::result::Result<ReplayPoll, BroadcasterReplayClientError> {
        self.observed_checkpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(checkpoint.entry_id().to_string());
        let poll = self
            .polls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .unwrap_or(FakeReplayPoll::DecodeError);
        match poll {
            FakeReplayPoll::RetryableServerError => {
                Err(BroadcasterReplayClientError::RedisReadTransport {
                    message: "TRYAGAIN retry the command".to_string(),
                })
            }
            FakeReplayPoll::TransportError => {
                Err(BroadcasterReplayClientError::RedisReadTransport {
                    message: "timeout".to_string(),
                })
            }
            FakeReplayPoll::PermanentCommandError => {
                Err(BroadcasterReplayClientError::RedisInspect {
                    message: "NOPERM this user has no permissions".to_string(),
                })
            }
            FakeReplayPoll::Batch => {
                let envelope =
                    update_envelope_for_stream("stream-7", 104, 71).map_err(|error| {
                        BroadcasterReplayClientError::RedisDecode {
                            message: error.to_string(),
                        }
                    })?;
                let entry = redis_entry_for_scope(&envelope, "native");
                let checkpoint_after = ReplayCheckpoint::new(
                    redis_boundary("stream-7", "snapshot-7", 7, 104).map_err(|error| {
                        BroadcasterReplayClientError::RedisDecode {
                            message: error.to_string(),
                        }
                    })?,
                    Chain::Ethereum.id(),
                );
                Ok(ReplayPoll::Batch(ReplayBatch {
                    items: vec![ReplayMessage {
                        entry_id: "7-104".to_string(),
                        entry,
                        envelope,
                        checkpoint_after,
                    }],
                    caught_up_after_batch: false,
                }))
            }
            FakeReplayPoll::CaughtUp => Ok(ReplayPoll::CaughtUp {
                checkpoint: checkpoint.clone(),
            }),
            FakeReplayPoll::DecodeError => Err(BroadcasterReplayClientError::RedisDecode {
                message: "bad payload".to_string(),
            }),
        }
    }
}

struct RecoveryThenGapPollSource {
    start: std::sync::Mutex<Option<ReplayMessage>>,
}

impl RecoveryThenGapPollSource {
    fn new(start: ReplayMessage) -> Self {
        Self {
            start: std::sync::Mutex::new(Some(start)),
        }
    }
}

impl ReplayPollSource for RecoveryThenGapPollSource {
    async fn read_next<'a>(
        &'a self,
        _checkpoint: &'a ReplayCheckpoint,
    ) -> std::result::Result<ReplayPoll, BroadcasterReplayClientError> {
        let start = self
            .start
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(start) = start {
            return Ok(ReplayPoll::Batch(ReplayBatch {
                items: vec![start],
                caught_up_after_batch: false,
            }));
        }
        Err(BroadcasterReplayClientError::RedisGap {
            message: "recovery transaction was trimmed before commit".to_string(),
        })
    }
}

#[derive(Default)]
struct RecordingRetrySleeper {
    durations: std::sync::Mutex<Vec<Duration>>,
}

impl RecordingRetrySleeper {
    fn durations(&self) -> Vec<Duration> {
        self.durations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl RedisRetrySleeper for RecordingRetrySleeper {
    async fn sleep(&self, duration: Duration) {
        self.durations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(duration);
    }
}

fn redis_test_supervisor_config() -> StreamSupervisorConfig {
    StreamSupervisorConfig {
        readiness_stale: Duration::from_secs(120),
        stream_stale: Duration::from_secs(120),
        missing_block_burst: 3,
        missing_block_window: Duration::from_secs(60),
        error_burst: 3,
        error_window: Duration::from_secs(60),
        resync_grace: Duration::from_secs(60),
        restart_backoff_min: Duration::from_millis(10),
        restart_backoff_max: Duration::from_millis(80),
        restart_backoff_jitter_pct: 0.0,
        memory: MemoryConfig {
            purge_enabled: false,
            snapshots_enabled: false,
            snapshots_min_interval_secs: 60,
            snapshots_min_new_pairs: 1_000,
            snapshots_emit_emf: false,
        },
    }
}

async fn bootstrap(processor: &mut BroadcasterSubscriptionProcessor) -> Result<()> {
    processor.observe(snapshot_start_envelope()?).await?;
    processor.observe(snapshot_chunk_envelope()?).await?;
    processor.observe(snapshot_end_envelope()).await?;
    Ok(())
}

#[tokio::test]
async fn redis_replay_boundary_rebases_processor_after_http_snapshot() -> Result<()> {
    let controls = TestControls::new();
    let mut processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.native(), None);
    bootstrap(&mut processor).await?;

    processor.align_redis_replay_boundary(&replay_boundary(18)?)?;
    processor.observe(heartbeat_envelope_at(19)?).await?;

    let snapshot = controls.native_subscription.snapshot().await;
    assert!(snapshot.redis_gap_reason.is_none());
    assert_eq!(controls.native_state_store.current_block().await, 10);
    Ok(())
}

#[tokio::test]
async fn redis_entry_applies_one_exact_publisher_version() -> Result<()> {
    let controls = TestControls::new();
    let mut processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.native(), None);
    bootstrap(&mut processor).await?;
    let envelope = update_envelope_at(4)?;
    let mut entry = redis_entry_for_scope(&envelope, "native");
    entry.state_version = 41;

    processor.observe_redis_delta(&entry, &envelope).await?;

    assert_eq!(controls.native_state_store.state_version().await, 41);
    assert_eq!(controls.native_state_store.current_block().await, 12);
    Ok(())
}

#[tokio::test]
async fn recovery_copy_awaits_the_current_token_snapshot() {
    let controls = TestControls::new();
    let token = dummy_token(44, "LATE");
    controls.token_store.insert_batch([token.clone()]).await;

    let recovery = controls.native().recovery_copy().await;
    let pin = recovery.state_store().pin().await;

    assert_eq!(
        pin.token(&token.address).map(|token| token.symbol),
        Some("LATE".to_string())
    );
}

#[tokio::test]
async fn redis_replay_skips_foreign_backend_scope_without_sequence_gap() -> Result<()> {
    let controls = TestControls::new();
    let mut processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.native(), None);
    bootstrap(&mut processor).await?;

    let rfq_envelope = rfq_heartbeat_envelope_at(4)?;
    let rfq_entry = redis_entry_for_scope(&rfq_envelope, "rfq");
    processor
        .observe_redis_delta(&rfq_entry, &rfq_envelope)
        .await?;

    processor.observe(heartbeat_envelope_at(5)?).await?;

    assert_eq!(controls.native_state_store.current_block().await, 10);
    assert_eq!(controls.native_state_store.total_states().await, 1);
    assert_eq!(controls.rfq_state_store.total_states().await, 0);
    Ok(())
}

#[tokio::test]
async fn redis_replay_status_checkpoint_does_not_move_behind_backend_boundary() -> Result<()> {
    let controls = TestControls::new();
    let native_boundary = replay_boundary(3)?;
    let vm_boundary = replay_boundary(7)?;
    controls
        .native_subscription
        .mark_bootstrap_complete_with_redis_boundary(native_boundary.clone())
        .await;
    controls
        .vm_subscription
        .mark_bootstrap_complete_with_redis_boundary(vm_boundary.clone())
        .await;
    let processors = vec![
        PreparedRedisProcessor {
            index: 0,
            processor: BroadcasterSubscriptionProcessor::new(
                Chain::Ethereum.id(),
                controls.native(),
                None,
            ),
            replay_boundary: native_boundary,
        },
        PreparedRedisProcessor {
            index: 1,
            processor: BroadcasterSubscriptionProcessor::new(
                Chain::Ethereum.id(),
                controls.vm(),
                None,
            ),
            replay_boundary: vm_boundary,
        },
    ];

    mark_redis_replay_checkpoints(&processors, "1-5", 5).await;

    assert_eq!(
        controls
            .native_subscription
            .snapshot()
            .await
            .redis_replay_checkpoint
            .as_deref(),
        Some("1-5")
    );
    assert_eq!(
        controls
            .vm_subscription
            .snapshot()
            .await
            .redis_replay_checkpoint
            .as_deref(),
        Some("1-7")
    );
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the test keeps the full private assembly and atomic commit proof in one scenario"
)]
async fn recovery_stays_private_until_commit_then_swaps_native_version() -> Result<()> {
    let (controls, mut prepared) = prepared_native_replay_subscription().await?;
    let mut checkpoint = ReplayCheckpoint::new(
        redis_boundary("stream-7", "snapshot-7", 7, 103)?,
        Chain::Ethereum.id(),
    );
    let replacement = recovery_snapshot_json(80)?;
    let digest = format!("{:x}", keccak256(replacement.as_bytes()));
    let target_state_version = 104;
    let mut chunk_encoded_bytes = 1_u64;
    loop {
        let candidate = recovery_replay_message(
            105,
            BroadcasterPayload::RecoveryChunk(BroadcasterRecoveryChunk::new(
                "recovery-1",
                target_state_version,
                vec![BroadcasterBackend::Native],
                0,
                chunk_encoded_bytes,
                digest.clone(),
                replacement.clone(),
            )?),
        )?;
        let actual = u64::try_from(super::replay_entry_encoded_size(
            &prepared.replay_boundary.stream_key,
            prepared.redis_maxlen,
            &candidate.entry,
        )?)?;
        if actual == chunk_encoded_bytes {
            break;
        }
        chunk_encoded_bytes = actual;
    }
    let manifest = BroadcasterRecoveryManifest::new(
        "recovery-1",
        target_state_version,
        vec![BroadcasterBackend::Native],
        u64::try_from(replacement.len())?,
        vec![chunk_encoded_bytes],
        vec![digest.clone()],
        digest.clone(),
        0,
        106,
    )?;
    let start = recovery_replay_message(
        104,
        BroadcasterPayload::RecoveryStart(BroadcasterRecoveryStart::new(manifest)?),
    )?;
    let request_generation_before_recovery = controls.native_state_store.request_generation().await;
    apply_replay_batch(&mut prepared, &mut checkpoint, vec![start])
        .await
        .map_err(|exit| anyhow!(exit.message))?;

    assert_eq!(controls.native_state_store.current_block().await, 70);
    assert!(
        controls.native_state_store.request_generation().await > request_generation_before_recovery,
        "RecoveryStart must fence requests before assembling private replacement state"
    );
    assert!(
        !controls
            .native_subscription
            .snapshot()
            .await
            .bootstrap_complete
    );

    let chunk = recovery_replay_message(
        105,
        BroadcasterPayload::RecoveryChunk(BroadcasterRecoveryChunk::new(
            "recovery-1",
            target_state_version,
            vec![BroadcasterBackend::Native],
            0,
            chunk_encoded_bytes,
            digest.clone(),
            replacement,
        )?),
    )?;
    apply_replay_batch(&mut prepared, &mut checkpoint, vec![chunk])
        .await
        .map_err(|exit| anyhow!(exit.message))?;
    assert_eq!(controls.native_state_store.current_block().await, 70);

    let commit = recovery_replay_message(
        106,
        BroadcasterPayload::RecoveryCommit(BroadcasterRecoveryCommit::new(
            "recovery-1",
            target_state_version,
            vec![BroadcasterBackend::Native],
            digest,
            0,
            106,
        )?),
    )?;
    apply_replay_batch(&mut prepared, &mut checkpoint, vec![commit])
        .await
        .map_err(|exit| anyhow!(exit.message))?;

    assert_eq!(controls.native_state_store.current_block().await, 80);
    assert_eq!(
        controls.native_state_store.state_version().await,
        target_state_version
    );
    assert!(
        controls
            .native_subscription
            .snapshot()
            .await
            .bootstrap_complete
    );
    Ok(())
}

#[tokio::test]
async fn malformed_or_incomplete_recovery_never_exposes_candidate_state() -> Result<()> {
    let (controls, mut prepared) = prepared_native_replay_subscription().await?;
    let (start, _, commit) = recovery_messages(&prepared, "recovery-malformed", 104, 80, 104)?;
    apply_recovery_message(&mut prepared, &start).await?;

    let Err(incomplete_error) = apply_recovery_message(&mut prepared, &commit).await else {
        return Err(anyhow!("commit without its declared chunk must fail"));
    };
    assert!(incomplete_error
        .to_string()
        .contains("before every declared fragment"));
    assert_eq!(controls.native_state_store.current_block().await, 70);

    let (start, mut corrupt, _) = recovery_messages(&prepared, "recovery-corrupt", 107, 81, 107)?;
    apply_recovery_message(&mut prepared, &start).await?;
    let BroadcasterPayload::RecoveryChunk(corrupt_chunk) = &mut corrupt.envelope.payload else {
        return Err(anyhow!("test fixture must contain a recovery chunk"));
    };
    corrupt_chunk.payload_fragment.push('x');
    let Err(corrupt_error) = apply_recovery_message(&mut prepared, &corrupt).await else {
        return Err(anyhow!("corrupt recovery fragment must fail"));
    };
    assert!(corrupt_error.to_string().contains("digest"));
    assert_eq!(controls.native_state_store.current_block().await, 70);
    Ok(())
}

#[tokio::test]
async fn reordered_recovery_chunk_is_rejected_before_private_assembly_changes() -> Result<()> {
    let (controls, mut prepared) = prepared_native_replay_subscription().await?;
    let (mut start, mut chunk, _) =
        recovery_messages(&prepared, "recovery-reordered", 104, 80, 104)?;
    let BroadcasterPayload::RecoveryStart(start_payload) = &mut start.envelope.payload else {
        return Err(anyhow!("test fixture must contain RecoveryStart"));
    };
    let first_chunk_bytes = start_payload.manifest.chunk_encoded_bytes[0];
    let first_chunk_digest = start_payload.manifest.chunk_digests[0].clone();
    start_payload
        .manifest
        .chunk_encoded_bytes
        .push(first_chunk_bytes);
    start_payload
        .manifest
        .chunk_digests
        .push(first_chunk_digest);
    start_payload.manifest.chunk_count = 2;
    start_payload.manifest.commit_watermark =
        start_payload.manifest.commit_watermark.saturating_add(1);
    let BroadcasterPayload::RecoveryChunk(chunk_payload) = &mut chunk.envelope.payload else {
        return Err(anyhow!("test fixture must contain RecoveryChunk"));
    };
    chunk_payload.chunk_index = 1;

    apply_recovery_message(&mut prepared, &start).await?;
    let Err(error) = apply_recovery_message(&mut prepared, &chunk).await else {
        return Err(anyhow!(
            "second recovery chunk must not arrive before the first"
        ));
    };

    assert!(error.to_string().contains("arrived out of order"));
    assert_eq!(controls.native_state_store.current_block().await, 70);
    assert!(prepared
        .recovery
        .as_ref()
        .is_some_and(|recovery| recovery.chunks.is_empty()));
    Ok(())
}

#[tokio::test]
async fn newer_recovery_supersedes_private_orphan_and_rejects_old_tail() -> Result<()> {
    let (controls, mut prepared) = prepared_native_replay_subscription().await?;
    let (old_start, old_chunk, _) = recovery_messages(&prepared, "recovery-old", 104, 80, 104)?;
    let (new_start, _, _) = recovery_messages(&prepared, "recovery-new", 107, 81, 107)?;

    apply_recovery_message(&mut prepared, &old_start).await?;
    apply_recovery_message(&mut prepared, &new_start).await?;

    assert_eq!(
        prepared
            .recovery
            .as_ref()
            .map(|recovery| recovery.manifest.recovery_id.as_str()),
        Some("recovery-new")
    );
    let Err(error) = apply_recovery_message(&mut prepared, &old_chunk).await else {
        return Err(anyhow!("superseded recovery tail must be rejected"));
    };
    assert!(error.to_string().contains("recovery identity"));
    assert_eq!(controls.native_state_store.current_block().await, 70);
    Ok(())
}

#[tokio::test]
async fn producer_crash_before_or_after_commit_is_replayed_on_consumer_restart() -> Result<()> {
    let (_, prepared_fixture) = prepared_native_replay_subscription().await?;
    let retained = recovery_messages(&prepared_fixture, "recovery-restart", 104, 80, 104)?;
    drop(prepared_fixture);

    let (controls, mut restarted) = prepared_native_replay_subscription().await?;
    apply_recovery_message(&mut restarted, &retained.0).await?;
    apply_recovery_message(&mut restarted, &retained.1).await?;
    assert_eq!(controls.native_state_store.current_block().await, 70);
    drop(restarted);

    let (controls, mut restarted_again) = prepared_native_replay_subscription().await?;
    apply_recovery_message(&mut restarted_again, &retained.0).await?;
    apply_recovery_message(&mut restarted_again, &retained.1).await?;
    apply_recovery_message(&mut restarted_again, &retained.2).await?;

    assert_eq!(controls.native_state_store.current_block().await, 80);
    assert_eq!(controls.native_state_store.state_version().await, 104);
    Ok(())
}

#[tokio::test]
async fn trimmed_recovery_transaction_forces_http_fallback_without_exposure() -> Result<()> {
    let (controls, prepared) = prepared_native_replay_subscription().await?;
    let (start, _, _) = recovery_messages(&prepared, "recovery-trimmed", 104, 80, 104)?;
    let source = RecoveryThenGapPollSource::new(start);
    let sleeper = RecordingRetrySleeper::default();

    let (exit, mut rebuilds, caught_up_once) = process_broadcaster_redis_subscription(
        &source,
        prepared,
        &redis_test_supervisor_config(),
        &sleeper,
    )
    .await;

    assert_eq!(exit.reason, SubscriptionExitReason::RedisGap);
    assert!(subscription_exit_requires_rebuild(exit.reason));
    assert!(!caught_up_once);
    assert_eq!(controls.native_state_store.current_block().await, 70);
    assert!(
        !controls
            .native_subscription
            .snapshot()
            .await
            .bootstrap_complete
    );

    let native_controls = controls.native();
    let rebuild = rebuilds.get_mut(0).and_then(Option::take);
    let continued = handle_subscription_reset(&native_controls, Some(exit.message), rebuild).await;
    assert!(continued.is_none());
    assert!(!controls.native_state_store.is_ready());
    assert_eq!(controls.native_state_store.current_block().await, 0);
    Ok(())
}

fn redis_entry_for_scope(
    envelope: &BroadcasterEnvelope,
    backend_scope: &str,
) -> BroadcasterRedisStreamEntry {
    BroadcasterRedisStreamEntry {
        schema_version: "1".to_string(),
        chain_id: Chain::Ethereum.id(),
        stream_id: envelope.stream_id.clone(),
        message_seq: envelope.message_seq,
        state_version: envelope.message_seq,
        kind: envelope.kind(),
        snapshot_id: Some("snapshot-1".to_string()),
        backend_scope: backend_scope.to_string(),
        block_number: None,
        observed_timestamp_ms: None,
        published_at_ms: 1,
        payload_json: String::new(),
    }
}

fn recovery_snapshot_json(block_number: u64) -> Result<String> {
    let snapshot_id = "recovery-snapshot-1";
    let stream_id = "recovery-stream-1";
    let envelopes = vec![
        BroadcasterEnvelope::new(
            stream_id,
            1,
            BroadcasterPayload::SnapshotStart(BroadcasterSnapshotStart::new(
                snapshot_id,
                Chain::Ethereum.id(),
                vec![BroadcasterBackend::Native],
                1,
            )?),
        ),
        BroadcasterEnvelope::new(
            stream_id,
            2,
            BroadcasterPayload::SnapshotChunk(BroadcasterSnapshotChunk::new(
                snapshot_id,
                0,
                vec![BroadcasterSnapshotPartition::new(
                    BroadcasterBackend::Native,
                    block_number,
                    Vec::new(),
                    BTreeMap::new(),
                )],
            )?),
        ),
        BroadcasterEnvelope::new(
            stream_id,
            3,
            BroadcasterPayload::SnapshotEnd(BroadcasterSnapshotEnd::new(snapshot_id)),
        ),
    ];
    serde_json::to_string(&envelopes).map_err(Into::into)
}

fn recovery_messages(
    prepared: &PreparedBroadcasterRedisSubscription,
    recovery_id: &str,
    target_state_version: u64,
    block_number: u64,
    start_seq: u64,
) -> Result<(ReplayMessage, ReplayMessage, ReplayMessage)> {
    let replacement = recovery_snapshot_json(block_number)?;
    let digest = format!("{:x}", keccak256(replacement.as_bytes()));
    let chunk_seq = start_seq.saturating_add(1);
    let commit_seq = start_seq.saturating_add(2);
    let mut chunk_encoded_bytes = 1_u64;
    loop {
        let candidate = recovery_replay_message(
            chunk_seq,
            BroadcasterPayload::RecoveryChunk(BroadcasterRecoveryChunk::new(
                recovery_id,
                target_state_version,
                vec![BroadcasterBackend::Native],
                0,
                chunk_encoded_bytes,
                digest.clone(),
                replacement.clone(),
            )?),
        )?;
        let actual = u64::try_from(super::replay_entry_encoded_size(
            &prepared.replay_boundary.stream_key,
            prepared.redis_maxlen,
            &candidate.entry,
        )?)?;
        if actual == chunk_encoded_bytes {
            break;
        }
        chunk_encoded_bytes = actual;
    }
    let manifest = BroadcasterRecoveryManifest::new(
        recovery_id,
        target_state_version,
        vec![BroadcasterBackend::Native],
        u64::try_from(replacement.len())?,
        vec![chunk_encoded_bytes],
        vec![digest.clone()],
        digest.clone(),
        0,
        commit_seq,
    )?;
    let start = recovery_replay_message(
        start_seq,
        BroadcasterPayload::RecoveryStart(BroadcasterRecoveryStart::new(manifest)?),
    )?;
    let chunk = recovery_replay_message(
        chunk_seq,
        BroadcasterPayload::RecoveryChunk(BroadcasterRecoveryChunk::new(
            recovery_id,
            target_state_version,
            vec![BroadcasterBackend::Native],
            0,
            chunk_encoded_bytes,
            digest.clone(),
            replacement,
        )?),
    )?;
    let commit = recovery_replay_message(
        commit_seq,
        BroadcasterPayload::RecoveryCommit(BroadcasterRecoveryCommit::new(
            recovery_id,
            target_state_version,
            vec![BroadcasterBackend::Native],
            digest,
            0,
            commit_seq,
        )?),
    )?;
    Ok((start, chunk, commit))
}

fn recovery_replay_message(message_seq: u64, payload: BroadcasterPayload) -> Result<ReplayMessage> {
    let state_version = match &payload {
        BroadcasterPayload::RecoveryStart(start) => start.manifest.target_state_version,
        BroadcasterPayload::RecoveryChunk(chunk) => chunk.target_state_version,
        BroadcasterPayload::RecoveryCatchUp(catch_up) => catch_up.target_state_version,
        BroadcasterPayload::RecoveryCommit(commit) => commit.target_state_version,
        _ => message_seq,
    };
    let envelope = BroadcasterEnvelope::new("stream-7", message_seq, payload);
    let boundary = redis_boundary("stream-7", "snapshot-7", 7, message_seq)?;
    Ok(ReplayMessage {
        entry_id: boundary.exclusive_entry_id(),
        entry: BroadcasterRedisStreamEntry::from_envelope(
            Chain::Ethereum.id(),
            &envelope,
            state_version,
        )?,
        envelope,
        checkpoint_after: ReplayCheckpoint::new(boundary, Chain::Ethereum.id()),
    })
}

fn native_component() -> ProtocolComponent {
    ProtocolComponent::new(
        Bytes::from([1u8; 20]),
        "uniswap_v2".to_string(),
        "uniswap_v2".to_string(),
        Chain::Ethereum,
        vec![dummy_token(2, "TKNA"), dummy_token(3, "TKNB")],
        Vec::new(),
        HashMap::new(),
        Bytes::from([9u8; 32]),
        chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
            .unwrap_or_else(|| unreachable!("valid timestamp"))
            .naive_utc(),
    )
}

fn replay_boundary(exclusive_message_seq: u64) -> Result<BroadcasterRedisReplayBoundary> {
    BroadcasterRedisReplayBoundary::new(
        "dsolver:broadcaster:test:events",
        "stream-1",
        "snapshot-1",
        1,
        exclusive_message_seq,
        exclusive_message_seq,
    )
    .map_err(Into::into)
}

fn redis_boundary(
    stream_id: &str,
    snapshot_id: &str,
    generation: u64,
    exclusive_message_seq: u64,
) -> Result<BroadcasterRedisReplayBoundary> {
    BroadcasterRedisReplayBoundary::new(
        "dsolver:broadcaster:test:events",
        stream_id,
        snapshot_id,
        generation,
        exclusive_message_seq,
        exclusive_message_seq,
    )
    .map_err(Into::into)
}

async fn prepared_native_replay_subscription(
) -> Result<(TestControls, PreparedBroadcasterRedisSubscription)> {
    let controls = TestControls::new();
    let old_boundary = redis_boundary("stream-7", "snapshot-7", 7, 103)?;
    let mut processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.native(), None);
    processor.set_bootstrap_redis_replay_boundary(old_boundary.clone());
    bootstrap_native_stream(&mut processor, "stream-7", "snapshot-7", 70).await?;
    processor.align_redis_replay_boundary(&old_boundary)?;

    let prepared = PreparedBroadcasterRedisSubscription {
        processors: vec![PreparedRedisProcessor {
            index: 0,
            processor,
            replay_boundary: old_boundary.clone(),
        }],
        replay_boundary: old_boundary.clone(),
        expected_chain_id: Chain::Ethereum.id(),
        recovery: None,
        redis_maxlen: None,
    };
    Ok((controls, prepared))
}

async fn bootstrap_native_stream(
    processor: &mut BroadcasterSubscriptionProcessor,
    stream_id: &str,
    snapshot_id: &str,
    block_number: u64,
) -> Result<()> {
    processor
        .observe(BroadcasterEnvelope::new(
            stream_id,
            1,
            BroadcasterPayload::SnapshotStart(BroadcasterSnapshotStart::new(
                snapshot_id,
                Chain::Ethereum.id(),
                vec![BroadcasterBackend::Native],
                1,
            )?),
        ))
        .await?;
    processor
        .observe(BroadcasterEnvelope::new(
            stream_id,
            2,
            BroadcasterPayload::SnapshotChunk(BroadcasterSnapshotChunk::new(
                snapshot_id,
                0,
                vec![BroadcasterSnapshotPartition::new(
                    BroadcasterBackend::Native,
                    block_number,
                    vec![BroadcasterStateEntry::new(
                        "pool-native",
                        native_component(),
                        Box::new(DummySim(1)),
                    )],
                    BTreeMap::new(),
                )],
            )?),
        ))
        .await?;
    processor
        .observe(BroadcasterEnvelope::new(
            stream_id,
            3,
            BroadcasterPayload::SnapshotEnd(BroadcasterSnapshotEnd::new(snapshot_id)),
        ))
        .await
}

fn vm_component() -> ProtocolComponent {
    ProtocolComponent::new(
        Bytes::from([4u8; 20]),
        "vm:curve".to_string(),
        "curve_pool".to_string(),
        Chain::Ethereum,
        vec![dummy_token(5, "TKNC"), dummy_token(6, "TKND")],
        Vec::new(),
        HashMap::new(),
        Bytes::from([8u8; 32]),
        chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
            .unwrap_or_else(|| unreachable!("valid timestamp"))
            .naive_utc(),
    )
}

fn rfq_component() -> ProtocolComponent {
    ProtocolComponent::new(
        Bytes::from([7u8; 20]),
        "rfq:hashflow".to_string(),
        "hashflow_pool".to_string(),
        Chain::Ethereum,
        vec![dummy_token(8, "RFQA"), dummy_token(9, "RFQB")],
        Vec::new(),
        HashMap::new(),
        Bytes::from([6u8; 32]),
        chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
            .unwrap_or_else(|| unreachable!("valid timestamp"))
            .naive_utc(),
    )
}

fn dummy_token(seed: u8, symbol: &str) -> Token {
    let address = Bytes::from([seed; 20]);
    Token::new(&address, symbol, 18, 0, &[], Chain::Ethereum, 100)
}

fn snapshot_start_envelope() -> Result<BroadcasterEnvelope> {
    snapshot_start_envelope_for_chain(Chain::Ethereum.id())
}

fn snapshot_start_envelope_for_chain(chain_id: u64) -> Result<BroadcasterEnvelope> {
    Ok(BroadcasterEnvelope::new(
        "stream-1",
        1,
        BroadcasterPayload::SnapshotStart(BroadcasterSnapshotStart::new(
            "snapshot-1",
            chain_id,
            vec![BroadcasterBackend::Native, BroadcasterBackend::Vm],
            1,
        )?),
    ))
}

fn snapshot_chunk_envelope() -> Result<BroadcasterEnvelope> {
    Ok(BroadcasterEnvelope::new(
        "stream-1",
        2,
        BroadcasterPayload::SnapshotChunk(BroadcasterSnapshotChunk::new(
            "snapshot-1",
            0,
            vec![
                BroadcasterSnapshotPartition::new(
                    BroadcasterBackend::Native,
                    10,
                    vec![BroadcasterStateEntry::new(
                        "pool-native",
                        native_component(),
                        Box::new(DummySim(1)),
                    )],
                    BTreeMap::new(),
                ),
                BroadcasterSnapshotPartition::new(
                    BroadcasterBackend::Vm,
                    11,
                    vec![BroadcasterStateEntry::new(
                        "pool-vm",
                        vm_component(),
                        Box::new(DummySim(2)),
                    )],
                    BTreeMap::new(),
                ),
            ],
        )?),
    ))
}

fn snapshot_end_envelope() -> BroadcasterEnvelope {
    BroadcasterEnvelope::new(
        "stream-1",
        3,
        BroadcasterPayload::SnapshotEnd(BroadcasterSnapshotEnd::new("snapshot-1")),
    )
}

fn rfq_snapshot_start_envelope(total_chunks: u32) -> Result<BroadcasterEnvelope> {
    Ok(BroadcasterEnvelope::new(
        "stream-1",
        1,
        BroadcasterPayload::SnapshotStart(BroadcasterSnapshotStart::new(
            "snapshot-1",
            Chain::Ethereum.id(),
            vec![BroadcasterBackend::Rfq],
            total_chunks,
        )?),
    ))
}

fn rfq_snapshot_chunk_envelope(block_number: u64) -> Result<BroadcasterEnvelope> {
    Ok(BroadcasterEnvelope::new(
        "stream-1",
        2,
        BroadcasterPayload::SnapshotChunk(BroadcasterSnapshotChunk::new(
            "snapshot-1",
            0,
            vec![BroadcasterSnapshotPartition::new(
                BroadcasterBackend::Rfq,
                block_number,
                vec![BroadcasterStateEntry::new(
                    "pool-rfq",
                    rfq_component(),
                    Box::new(DummySim(7)),
                )],
                BTreeMap::new(),
            )],
        )?),
    ))
}

fn empty_rfq_snapshot_chunk_envelope(block_number: u64) -> Result<BroadcasterEnvelope> {
    Ok(BroadcasterEnvelope::new(
        "stream-1",
        2,
        BroadcasterPayload::SnapshotChunk(BroadcasterSnapshotChunk::new(
            "snapshot-1",
            0,
            vec![BroadcasterSnapshotPartition::new(
                BroadcasterBackend::Rfq,
                block_number,
                Vec::new(),
                BTreeMap::new(),
            )],
        )?),
    ))
}

fn update_envelope() -> Result<BroadcasterEnvelope> {
    update_envelope_at(4)
}

fn update_envelope_at(message_seq: u64) -> Result<BroadcasterEnvelope> {
    update_envelope_for_stream("stream-1", message_seq, 12)
}

fn update_envelope_for_stream(
    stream_id: &str,
    message_seq: u64,
    block_number: u64,
) -> Result<BroadcasterEnvelope> {
    let header = raw_block_header(block_number, block_number as u8);
    Ok(BroadcasterEnvelope::new(
        stream_id,
        message_seq,
        BroadcasterPayload::Update(BroadcasterUpdateMessage::new(vec![
            BroadcasterUpdatePartition::new(
                BroadcasterBackend::Native,
                block_number,
                Vec::new(),
                vec![BroadcasterStateDelta::new(
                    "pool-native",
                    BroadcasterBackend::Native,
                    Box::new(DummySim(3)),
                )],
                Vec::new(),
                BTreeMap::from([(
                    "uniswap_v2".to_string(),
                    BroadcasterProtocolSyncStatus::from_synchronizer_state(
                        &SynchronizerState::Ready(header),
                    ),
                )]),
            ),
        ])?),
    ))
}

fn partial_native_update_envelope(
    message_seq: u64,
    block_number: u64,
) -> Result<BroadcasterEnvelope> {
    let mut envelope = update_envelope_for_stream("stream-1", message_seq, block_number)?;
    let BroadcasterPayload::Update(update) = &mut envelope.payload else {
        return Err(anyhow!("expected native update payload"));
    };
    let status = update.partitions[0]
        .sync_statuses
        .get_mut("uniswap_v2")
        .ok_or_else(|| anyhow!("native sync status missing"))?;
    status
        .block
        .as_mut()
        .ok_or_else(|| anyhow!("native sync block missing"))?
        .partial_block_index = Some(0);
    Ok(envelope)
}

fn rfq_update_envelope(block_number: u64) -> Result<BroadcasterEnvelope> {
    Ok(BroadcasterEnvelope::new(
        "stream-1",
        4,
        BroadcasterPayload::Update(BroadcasterUpdateMessage::new(vec![
            BroadcasterUpdatePartition::new(
                BroadcasterBackend::Rfq,
                block_number,
                vec![BroadcasterStateEntry::new(
                    "pool-rfq",
                    rfq_component(),
                    Box::new(DummySim(8)),
                )],
                Vec::new(),
                Vec::new(),
                BTreeMap::new(),
            ),
        ])?),
    ))
}

fn raw_rfq_snapshot_chunk_envelope() -> Result<BroadcasterEnvelope> {
    Ok(BroadcasterEnvelope::new(
        "stream-1",
        2,
        BroadcasterPayload::SnapshotChunk(BroadcasterSnapshotChunk::new(
            "snapshot-1",
            0,
            vec![BroadcasterSnapshotPartition::with_messages(
                BroadcasterBackend::Rfq,
                21,
                vec![BroadcasterProtocolMessage::new(
                    "rfq:hashflow",
                    SynchronizerState::Ready(raw_block_header(21, 7)),
                    StateSyncMessage {
                        header: raw_block_header(21, 7),
                        snapshots: Snapshot::default(),
                        deltas: None,
                        removed_components: HashMap::new(),
                    },
                )],
                BTreeMap::new(),
            )],
        )?),
    ))
}

fn heartbeat_envelope() -> Result<BroadcasterEnvelope> {
    heartbeat_envelope_at(4)
}

fn heartbeat_envelope_at(message_seq: u64) -> Result<BroadcasterEnvelope> {
    Ok(BroadcasterEnvelope::new(
        "stream-1",
        message_seq,
        BroadcasterPayload::Heartbeat(BroadcasterHeartbeat::new(
            1,
            "snapshot-1",
            vec![
                BroadcasterBackendHead::new(BroadcasterBackend::Native, 14),
                BroadcasterBackendHead::new(BroadcasterBackend::Vm, 15),
            ],
        )?),
    ))
}

fn rfq_heartbeat_envelope_at(message_seq: u64) -> Result<BroadcasterEnvelope> {
    Ok(BroadcasterEnvelope::new(
        "stream-1",
        message_seq,
        BroadcasterPayload::Heartbeat(BroadcasterHeartbeat::new(
            1,
            "snapshot-1",
            vec![BroadcasterBackendHead::new(BroadcasterBackend::Rfq, 21)],
        )?),
    ))
}

fn vm_only_snapshot_start_envelope(total_chunks: u32) -> Result<BroadcasterEnvelope> {
    Ok(BroadcasterEnvelope::new(
        "stream-1",
        1,
        BroadcasterPayload::SnapshotStart(BroadcasterSnapshotStart::new(
            "snapshot-1",
            Chain::Ethereum.id(),
            vec![BroadcasterBackend::Vm],
            total_chunks,
        )?),
    ))
}

fn raw_snapshot_chunk_envelope(
    message_seq: u64,
    chunk_index: u32,
    block_number: u64,
    messages: Vec<BroadcasterProtocolMessage>,
) -> Result<BroadcasterEnvelope> {
    Ok(BroadcasterEnvelope::new(
        "stream-1",
        message_seq,
        BroadcasterPayload::SnapshotChunk(BroadcasterSnapshotChunk::new(
            "snapshot-1",
            chunk_index,
            vec![BroadcasterSnapshotPartition::with_messages(
                BroadcasterBackend::Vm,
                block_number,
                messages,
                BTreeMap::new(),
            )],
        )?),
    ))
}

fn snapshot_end_envelope_at(message_seq: u64) -> BroadcasterEnvelope {
    BroadcasterEnvelope::new(
        "stream-1",
        message_seq,
        BroadcasterPayload::SnapshotEnd(BroadcasterSnapshotEnd::new("snapshot-1")),
    )
}

fn raw_decoder() -> Arc<ReplayDecoder> {
    let mut decoder = TychoStreamDecoder::new();
    decoder.register_decoder::<DummySim>("vm:curve");
    Arc::new(ReplayDecoder::with_decoder(
        DecoderConfig::for_backend(ReplayBackend::Vm, vec!["vm:curve".to_string()], 0),
        Arc::new(decoder),
    ))
}

fn stateful_decoder() -> Arc<ReplayDecoder> {
    let mut decoder = TychoStreamDecoder::new();
    decoder.register_decoder::<StatefulSim>("vm:curve");
    Arc::new(ReplayDecoder::with_decoder(
        DecoderConfig::for_backend(ReplayBackend::Vm, vec!["vm:curve".to_string()], 0),
        Arc::new(decoder),
    ))
}

#[test]
fn raw_snapshot_reassembly_merges_split_vm_storage_account() -> Result<()> {
    let account_address = Bytes::from([42u8; 20]);
    let mut reassembly = RawSnapshotReassembly::default();
    reassembly.push(raw_protocol_message(
        account_address.clone(),
        raw_response_account(account_address.clone(), "vm-account", &[(1, 11)]),
    ))?;
    reassembly.push(raw_protocol_message(
        account_address.clone(),
        raw_response_account(account_address.clone(), "vm-account", &[(2, 22), (3, 33)]),
    ))?;

    let messages = reassembly.take_messages();
    assert_eq!(messages.len(), 1);
    let account = messages[0]
        .message
        .snapshots
        .vm_storage
        .get(&account_address)
        .ok_or_else(|| anyhow!("expected reassembled VM account"))?;
    assert_eq!(account.slots.len(), 3);
    assert_eq!(
        account.slots[&Bytes::from([1u8; 32])],
        Bytes::from([11u8; 32])
    );
    assert_eq!(
        account.slots[&Bytes::from([2u8; 32])],
        Bytes::from([22u8; 32])
    );
    assert_eq!(
        account.slots[&Bytes::from([3u8; 32])],
        Bytes::from([33u8; 32])
    );
    Ok(())
}

#[test]
fn raw_snapshot_reassembly_rejects_metadata_mismatch() -> Result<()> {
    let account_address = Bytes::from([42u8; 20]);
    let mut reassembly = RawSnapshotReassembly::default();
    reassembly.push(raw_protocol_message(
        account_address.clone(),
        raw_response_account(account_address.clone(), "vm-account", &[(1, 11)]),
    ))?;

    let Err(error) = reassembly.push(raw_protocol_message(
        account_address.clone(),
        raw_response_account(account_address, "changed-title", &[(2, 22)]),
    )) else {
        return Err(anyhow!("metadata mismatch should fail"));
    };

    assert!(error
        .to_string()
        .contains("broadcaster snapshot VM storage metadata mismatch"));
    Ok(())
}

#[test]
fn raw_snapshot_reassembly_rejects_header_mismatch() -> Result<()> {
    let mut reassembly = RawSnapshotReassembly::default();
    let sync_header = raw_block_header(10, 1);
    reassembly.push(raw_protocol_message_with_header(
        sync_header.clone(),
        SynchronizerState::Ready(sync_header.clone()),
    ))?;

    let Err(error) = reassembly.push(raw_protocol_message_with_header(
        raw_block_header(11, 2),
        SynchronizerState::Ready(sync_header),
    )) else {
        return Err(anyhow!("header mismatch should fail"));
    };

    assert!(error.to_string().contains("header mismatch"));
    Ok(())
}

#[test]
fn raw_snapshot_reassembly_rejects_sync_state_mismatch() -> Result<()> {
    let mut reassembly = RawSnapshotReassembly::default();
    let header = raw_block_header(10, 1);
    reassembly.push(raw_protocol_message_with_header(
        header.clone(),
        SynchronizerState::Ready(header.clone()),
    ))?;

    let Err(error) = reassembly.push(raw_protocol_message_with_header(
        header.clone(),
        SynchronizerState::Delayed(header),
    )) else {
        return Err(anyhow!("sync_state mismatch should fail"));
    };

    assert!(error.to_string().contains("sync_state mismatch"));
    Ok(())
}

#[test]
fn raw_snapshot_reassembly_rejects_duplicate_snapshot_state_conflict() -> Result<()> {
    let mut reassembly = RawSnapshotReassembly::default();
    reassembly.push(raw_protocol_message_with_ids(&["pool-a"], &[]))?;

    let Err(error) = reassembly.push(raw_protocol_message_with_ids(&["pool-a"], &[])) else {
        return Err(anyhow!("duplicate snapshot state should fail"));
    };

    assert!(error.to_string().contains("duplicate snapshot state"));
    Ok(())
}

#[test]
fn raw_snapshot_reassembly_rejects_duplicate_removal_conflict() -> Result<()> {
    let mut reassembly = RawSnapshotReassembly::default();
    reassembly.push(raw_protocol_message_with_ids(&[], &["pool-a"]))?;

    let Err(error) = reassembly.push(raw_protocol_message_with_ids(&[], &["pool-a"])) else {
        return Err(anyhow!("duplicate removal should fail"));
    };

    assert!(error.to_string().contains("duplicate removed component"));
    Ok(())
}

#[test]
fn raw_snapshot_reassembly_rejects_snapshot_removal_overlap() -> Result<()> {
    let mut reassembly = RawSnapshotReassembly::default();
    reassembly.push(raw_protocol_message_with_ids(&["pool-a"], &[]))?;

    let Err(error) = reassembly.push(raw_protocol_message_with_ids(&[], &["pool-a"])) else {
        return Err(anyhow!("snapshot/removal overlap should fail"));
    };

    assert!(error.to_string().contains("snapshot/removal overlap"));
    Ok(())
}

#[test]
fn raw_snapshot_reassembly_happy_path_preserves_header_and_sync_state() -> Result<()> {
    let header = raw_block_header(10, 1);
    let sync_state = SynchronizerState::Ready(header.clone());
    let mut reassembly = RawSnapshotReassembly::default();
    reassembly.push(raw_protocol_message_with_parts(
        header.clone(),
        sync_state.clone(),
        &["pool-a"],
        &[],
        HashMap::new(),
    ))?;
    reassembly.push(raw_protocol_message_with_parts(
        header.clone(),
        sync_state.clone(),
        &[],
        &["pool-b"],
        HashMap::new(),
    ))?;

    let messages = reassembly.take_messages();
    assert_eq!(messages.len(), 1);
    let message = &messages[0];
    assert_eq!(message.message.header, header);
    assert_eq!(message.sync_state, sync_state);
    assert!(message.message.snapshots.states.contains_key("pool-a"));
    assert!(message.message.removed_components.contains_key("pool-b"));
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the fragmented export and consumer reassembly contract stays in one regression"
)]
async fn raw_cache_tail_reassembly_matches_unsplit_message() -> Result<()> {
    let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Vm]);
    let header = raw_block_header(10, 1);
    let mut changes = BlockAggregatedChanges::default();
    for index in (0..24).rev() {
        let component_id = format!("missing-{index:04}");
        changes.state_updates.insert(
            component_id.clone(),
            ProtocolStateDelta {
                component_id,
                updated_attributes: HashMap::from([(
                    "value".to_string(),
                    Bytes::from(vec![index as u8; 384]),
                )]),
                deleted_attributes: Default::default(),
            },
        );
    }
    for index in 0..20u8 {
        let token_address = Bytes::from([91u8.saturating_add(index); 20]);
        changes.new_tokens.insert(
            token_address.clone(),
            ResponseToken {
                chain: DtoChain::Ethereum,
                address: token_address,
                symbol: format!("TAIL{index}"),
                decimals: 18,
                tax: 0,
                gas: Vec::new(),
                quality: 100,
            },
        );
        let entrypoint_id = format!("trace-{index}");
        changes
            .dci_update
            .new_entrypoints
            .entry("missing-dci".to_string())
            .or_default()
            .insert(EntryPoint {
                external_id: entrypoint_id.clone(),
                target: Bytes::from([index; 20]),
                signature: format!("value{index}()"),
            });
        changes
            .dci_update
            .new_entrypoint_params
            .entry(entrypoint_id.clone())
            .or_default()
            .insert((
                TracingParams::RPCTracer(RPCTracerParams {
                    caller: None,
                    calldata: Bytes::from([index; 4]),
                    state_overrides: None,
                    prune_addresses: None,
                }),
                "missing-dci".to_string(),
            ));
        changes
            .dci_update
            .trace_results
            .insert(entrypoint_id, Default::default());
    }
    cache
        .apply_feed_message(&tycho_simulation::tycho_client::feed::FeedMessage {
            state_msgs: HashMap::from([(
                "vm:curve".to_string(),
                StateSyncMessage {
                    header: header.clone(),
                    snapshots: Snapshot {
                        states: HashMap::from([(
                            "pool-a".to_string(),
                            raw_component_with_state("pool-a"),
                        )]),
                        vm_storage: HashMap::new(),
                    },
                    deltas: Some(changes.into()),
                    removed_components: HashMap::new(),
                },
            )]),
            sync_states: HashMap::from([(
                "vm:curve".to_string(),
                SynchronizerState::Ready(header),
            )]),
        })
        .await?;

    let unsplit_export = cache.export_snapshot(usize::MAX).await?;
    let unsplit_message = unsplit_export
        .payloads
        .iter()
        .filter_map(|payload| match payload {
            BroadcasterPayload::SnapshotChunk(chunk) => Some(chunk),
            _ => None,
        })
        .flat_map(|chunk| &chunk.partitions)
        .flat_map(|partition| &partition.messages)
        .next()
        .cloned()
        .ok_or_else(|| anyhow!("expected unsplit raw message"))?;
    let unsplit_size = serde_json::to_vec(&unsplit_export.payloads[1])?.len();
    let split_export = cache.export_snapshot(unsplit_size / 2).await?;
    let mut reassembly = RawSnapshotReassembly::default();
    let mut fragment_count = 0usize;
    for message in split_export
        .payloads
        .iter()
        .filter_map(|payload| match payload {
            BroadcasterPayload::SnapshotChunk(chunk) => Some(chunk),
            _ => None,
        })
        .flat_map(|chunk| &chunk.partitions)
        .flat_map(|partition| &partition.messages)
    {
        let json = serde_json::to_vec(message)?;
        let decoded: BroadcasterProtocolMessage = serde_json::from_slice(&json)?;
        reassembly.push(decoded)?;
        fragment_count += 1;
    }

    assert!(fragment_count > 1);
    assert_eq!(reassembly.take_messages(), vec![unsplit_message]);
    Ok(())
}

#[tokio::test]
async fn raw_component_removal_inverse_removes_consumer_state() -> Result<()> {
    let controls = TestControls::new();
    let mut processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.vm(), None);
    bootstrap(&mut processor).await?;
    let component_id = "0x1111111111111111111111111111111111111111";
    controls
        .vm_state_store
        .apply_update(tycho_simulation::protocol::models::Update::new(
            11,
            HashMap::from([(
                component_id.to_string(),
                Box::new(DummySim(7)) as Box<dyn ProtocolSim>,
            )]),
            HashMap::from([(component_id.to_string(), vm_component())]),
        ))
        .await;
    assert!(controls.vm_state_store.has_pool(component_id).await);

    let mut removal = raw_protocol_message_with_ids(&[], &[component_id]);
    let removal_header = raw_block_header(12, 2);
    removal.message.header = removal_header.clone();
    removal.sync_state = SynchronizerState::Ready(removal_header);
    let update = BroadcasterUpdateMessage::new(vec![BroadcasterUpdatePartition::with_messages(
        BroadcasterBackend::Vm,
        12,
        vec![removal],
        BTreeMap::new(),
    )])?;
    processor
        .observe(BroadcasterEnvelope::new(
            "stream-1",
            4,
            BroadcasterPayload::Update(update),
        ))
        .await?;

    assert!(!controls.vm_state_store.has_pool(component_id).await);
    assert_eq!(controls.vm_state_store.current_block().await, 12);
    Ok(())
}

struct StatefulCompactionFixture {
    component_id: &'static str,
    token: Bytes,
    snapshot_state: ComponentWithState,
    changes: BlockAggregatedChanges,
    initial_header: BlockHeader,
    final_header: BlockHeader,
}

fn stateful_compaction_fixture() -> StatefulCompactionFixture {
    let component_id = "0x1111111111111111111111111111111111111111";
    let token = Bytes::from([81u8; 20]);
    let snapshot_state = ComponentWithState {
        state: ResponseProtocolState {
            component_id: component_id.to_string(),
            attributes: HashMap::from([
                ("value".to_string(), Bytes::from([1u8; 32])),
                ("deleted".to_string(), Bytes::from([9u8; 32])),
            ]),
            balances: HashMap::from([(token.clone(), Bytes::from([10u8; 32]))]),
        }
        .into(),
        component: raw_dto_protocol_component(component_id).into(),
        component_tvl: Some(1.0),
        entrypoints: Vec::new(),
    };
    let mut changes = BlockAggregatedChanges::default();
    changes.state_updates.insert(
        component_id.to_string(),
        ProtocolStateDelta {
            component_id: component_id.to_string(),
            updated_attributes: HashMap::from([("value".to_string(), Bytes::from([2u8; 32]))]),
            deleted_attributes: ["deleted".to_string()].into_iter().collect(),
        },
    );
    changes.component_balances.insert(
        component_id.to_string(),
        TokenBalances(HashMap::from([(
            token.clone(),
            ComponentBalance {
                token: token.clone(),
                balance: Bytes::from([20u8; 32]),
                balance_float: 20.0,
                modify_tx: Bytes::from([2u8; 32]),
                component_id: component_id.to_string(),
            },
        )])),
    );
    changes.component_tvl.insert(component_id.to_string(), 2.0);
    StatefulCompactionFixture {
        component_id,
        token,
        snapshot_state,
        changes,
        initial_header: raw_block_header(10, 1),
        final_header: raw_block_header(11, 2),
    }
}

async fn bootstrap_uncompacted_stateful(
    fixture: &StatefulCompactionFixture,
) -> Result<TestControls> {
    let uncompacted_message = BroadcasterProtocolMessage::new(
        "vm:curve",
        SynchronizerState::Ready(fixture.final_header.clone()),
        StateSyncMessage {
            header: fixture.final_header.clone(),
            snapshots: Snapshot {
                states: HashMap::from([(
                    fixture.component_id.to_string(),
                    fixture.snapshot_state.clone(),
                )]),
                vm_storage: HashMap::new(),
            },
            deltas: Some(fixture.changes.clone().into()),
            removed_components: HashMap::new(),
        },
    );

    let uncompacted_controls = TestControls::new();
    let mut uncompacted_processor = BroadcasterSubscriptionProcessor::with_decoder(
        Chain::Ethereum.id(),
        uncompacted_controls.vm(),
        stateful_decoder(),
        None,
    );
    uncompacted_processor.set_bootstrap_redis_replay_boundary(
        super::processor::default_test_redis_replay_boundary(),
    );
    uncompacted_processor
        .observe(vm_only_snapshot_start_envelope(1)?)
        .await?;
    uncompacted_processor
        .observe(raw_snapshot_chunk_envelope(
            2,
            0,
            11,
            vec![uncompacted_message],
        )?)
        .await?;
    uncompacted_processor
        .observe(snapshot_end_envelope_at(3))
        .await?;
    Ok(uncompacted_controls)
}

async fn bootstrap_compacted_stateful(fixture: &StatefulCompactionFixture) -> Result<TestControls> {
    let cache = BroadcasterSnapshotCache::new(1, vec![BroadcasterBackend::Vm]);
    cache
        .apply_feed_message(&tycho_simulation::tycho_client::feed::FeedMessage {
            state_msgs: HashMap::from([(
                "vm:curve".to_string(),
                StateSyncMessage {
                    header: fixture.initial_header.clone(),
                    snapshots: Snapshot {
                        states: HashMap::from([(
                            fixture.component_id.to_string(),
                            fixture.snapshot_state.clone(),
                        )]),
                        vm_storage: HashMap::new(),
                    },
                    deltas: None,
                    removed_components: HashMap::new(),
                },
            )]),
            sync_states: HashMap::from([(
                "vm:curve".to_string(),
                SynchronizerState::Ready(fixture.initial_header.clone()),
            )]),
        })
        .await?;
    cache
        .apply_feed_message(&tycho_simulation::tycho_client::feed::FeedMessage {
            state_msgs: HashMap::from([(
                "vm:curve".to_string(),
                StateSyncMessage {
                    header: fixture.final_header.clone(),
                    snapshots: Snapshot::default(),
                    deltas: Some(fixture.changes.clone().into()),
                    removed_components: HashMap::new(),
                },
            )]),
            sync_states: HashMap::from([(
                "vm:curve".to_string(),
                SynchronizerState::Ready(fixture.final_header.clone()),
            )]),
        })
        .await?;
    let compacted_export = cache.export_snapshot(8_388_608).await?;
    let compacted_controls = TestControls::new();
    let mut compacted_processor = BroadcasterSubscriptionProcessor::with_decoder(
        Chain::Ethereum.id(),
        compacted_controls.vm(),
        stateful_decoder(),
        None,
    );
    compacted_processor.set_bootstrap_redis_replay_boundary(BroadcasterRedisReplayBoundary::new(
        "stream:test".to_string(),
        compacted_export.stream_id.clone(),
        compacted_export.snapshot_id.clone(),
        1,
        0,
        0,
    )?);
    for (index, payload) in compacted_export.payloads.into_iter().enumerate() {
        compacted_processor
            .observe(BroadcasterEnvelope::new(
                compacted_export.stream_id.clone(),
                index as u64 + 1,
                payload,
            ))
            .await?;
    }
    Ok(compacted_controls)
}

#[tokio::test]
async fn raw_cache_compaction_matches_consumer_state_transition() -> Result<()> {
    let fixture = stateful_compaction_fixture();
    let uncompacted_controls = bootstrap_uncompacted_stateful(&fixture).await?;
    let compacted_controls = bootstrap_compacted_stateful(&fixture).await?;

    let uncompacted_pool = uncompacted_controls
        .vm_state_store
        .pool_by_id(fixture.component_id)
        .await
        .ok_or_else(|| anyhow!("expected uncompacted pool"))?;
    let compacted_pool = compacted_controls
        .vm_state_store
        .pool_by_id(fixture.component_id)
        .await
        .ok_or_else(|| anyhow!("expected compacted pool"))?;
    assert!(uncompacted_pool.0.eq(compacted_pool.0.as_ref()));
    assert_eq!(uncompacted_pool.1.as_ref(), compacted_pool.1.as_ref());
    assert_eq!(
        uncompacted_controls.vm_state_store.current_block().await,
        11
    );
    assert_eq!(compacted_controls.vm_state_store.current_block().await, 11);
    let state = compacted_pool
        .0
        .as_any()
        .downcast_ref::<StatefulSim>()
        .ok_or_else(|| anyhow!("expected stateful simulation"))?;
    assert_eq!(state.attributes["value"], Bytes::from([2u8; 32]));
    assert!(!state.attributes.contains_key("deleted"));
    assert_eq!(state.balances[&fixture.token], Bytes::from([20u8; 32]));
    Ok(())
}

#[tokio::test]
async fn snapshot_bootstrap_populates_native_and_vm_separately() -> Result<()> {
    let controls = TestControls::new();
    let mut native_processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.native(), None);
    let mut vm_processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.vm(), None);

    native_processor.observe(snapshot_start_envelope()?).await?;
    native_processor.observe(snapshot_chunk_envelope()?).await?;
    vm_processor.observe(snapshot_start_envelope()?).await?;
    vm_processor.observe(snapshot_chunk_envelope()?).await?;

    assert!(controls.native_state_store.has_pool("pool-native").await);
    assert!(!controls.native_state_store.has_pool("pool-vm").await);
    assert!(!controls.vm_state_store.has_pool("pool-native").await);
    assert!(controls.vm_state_store.has_pool("pool-vm").await);
    assert!(
        !controls
            .native_subscription
            .snapshot()
            .await
            .bootstrap_complete
    );
    assert!(!controls.vm_subscription.snapshot().await.bootstrap_complete);

    native_processor.observe(snapshot_end_envelope()).await?;
    vm_processor.observe(snapshot_end_envelope()).await?;

    let native_snapshot = controls.native_subscription.snapshot().await;
    assert!(native_snapshot.connected);
    assert!(native_snapshot.bootstrap_complete);
    let vm_snapshot = controls.vm_subscription.snapshot().await;
    assert!(vm_snapshot.connected);
    assert!(vm_snapshot.bootstrap_complete);
    assert_eq!(controls.native_state_store.current_block().await, 10);
    assert_eq!(controls.vm_state_store.current_block().await, 11);
    assert!(controls
        .native_stream_health
        .last_update_age_ms()
        .await
        .is_none());
    assert!(controls
        .vm_stream_health
        .last_update_age_ms()
        .await
        .is_none());
    Ok(())
}

#[tokio::test]
async fn rfq_snapshot_partition_hydrates_rfq_state_store() -> Result<()> {
    let controls = TestControls::new();
    let mut rfq_processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.rfq(), None);

    rfq_processor
        .observe(rfq_snapshot_start_envelope(1)?)
        .await?;
    rfq_processor
        .observe(rfq_snapshot_chunk_envelope(21)?)
        .await?;
    rfq_processor.observe(snapshot_end_envelope()).await?;

    assert_eq!(controls.rfq_state_store.current_block().await, 21);
    assert_eq!(controls.rfq_state_store.total_states().await, 1);
    assert!(controls.rfq_state_store.has_pool("pool-rfq").await);
    assert!(
        controls
            .rfq_subscription
            .snapshot()
            .await
            .bootstrap_complete
    );
    Ok(())
}

#[tokio::test]
async fn rfq_live_update_partition_advances_rfq_state_store() -> Result<()> {
    let controls = TestControls::new();
    let mut rfq_processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.rfq(), None);

    rfq_processor
        .observe(rfq_snapshot_start_envelope(1)?)
        .await?;
    rfq_processor
        .observe(empty_rfq_snapshot_chunk_envelope(20)?)
        .await?;
    rfq_processor.observe(snapshot_end_envelope()).await?;
    rfq_processor.observe(rfq_update_envelope(22)?).await?;

    assert_eq!(controls.rfq_state_store.current_block().await, 22);
    assert_eq!(controls.rfq_state_store.total_states().await, 1);
    assert!(controls.rfq_state_store.has_pool("pool-rfq").await);
    assert_eq!(controls.rfq_stream_health.last_block().await, 22);
    Ok(())
}

#[tokio::test]
async fn rfq_raw_message_partition_fails_explicitly() -> Result<()> {
    let controls = TestControls::new();
    let mut rfq_processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.rfq(), None);

    rfq_processor
        .observe(rfq_snapshot_start_envelope(1)?)
        .await?;
    let Err(error) = rfq_processor
        .observe(raw_rfq_snapshot_chunk_envelope()?)
        .await
    else {
        return Err(anyhow!("raw rfq partition should fail"));
    };

    assert!(
        error
            .to_string()
            .contains("raw RFQ broadcaster messages are unsupported"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn raw_snapshot_bootstrap_buffers_split_messages_until_snapshot_end() -> Result<()> {
    let controls = TestControls::new();
    let vm_controls = controls.vm();
    let mut processor = BroadcasterSubscriptionProcessor::with_decoder(
        Chain::Ethereum.id(),
        vm_controls,
        raw_decoder(),
        None,
    );
    processor.set_bootstrap_redis_replay_boundary(super::processor::default_test_redis_replay_boundary());
    let header = raw_block_header(21, 9);
    let sync_state = SynchronizerState::Ready(header.clone());

    processor
        .observe(vm_only_snapshot_start_envelope(2)?)
        .await?;
    processor
        .observe(raw_snapshot_chunk_envelope(
            2,
            0,
            21,
            vec![raw_protocol_message_with_parts(
                header.clone(),
                sync_state.clone(),
                &["0x1111111111111111111111111111111111111111"],
                &[],
                HashMap::new(),
            )],
        )?)
        .await?;

    assert!(!processor.bootstrap_complete());
    assert!(
        !controls
            .vm_state_store
            .has_pool("0x1111111111111111111111111111111111111111")
            .await
    );
    assert_eq!(controls.vm_state_store.current_block().await, 0);
    assert_eq!(controls.vm_stream_health.last_block().await, 0);

    processor
        .observe(raw_snapshot_chunk_envelope(
            3,
            1,
            21,
            vec![raw_protocol_message_with_parts(
                header,
                sync_state,
                &["0x2222222222222222222222222222222222222222"],
                &[],
                HashMap::new(),
            )],
        )?)
        .await?;

    assert!(!processor.bootstrap_complete());
    assert!(
        !controls
            .vm_state_store
            .has_pool("0x1111111111111111111111111111111111111111")
            .await
    );
    assert!(
        !controls
            .vm_state_store
            .has_pool("0x2222222222222222222222222222222222222222")
            .await
    );
    assert_eq!(controls.vm_state_store.current_block().await, 0);
    assert_eq!(controls.vm_stream_health.last_block().await, 0);

    processor.observe(snapshot_end_envelope_at(4)).await?;

    let snapshot = controls.vm_subscription.snapshot().await;
    assert!(snapshot.connected);
    assert!(snapshot.bootstrap_complete);
    assert!(processor.bootstrap_complete());
    assert!(
        controls
            .vm_state_store
            .has_pool("0x1111111111111111111111111111111111111111")
            .await
    );
    assert!(
        controls
            .vm_state_store
            .has_pool("0x2222222222222222222222222222222222222222")
            .await
    );
    assert_eq!(controls.vm_state_store.current_block().await, 21);
    assert_eq!(controls.vm_stream_health.last_block().await, 0);
    assert!(controls
        .vm_stream_health
        .last_update_age_ms()
        .await
        .is_none());
    Ok(())
}

#[tokio::test]
async fn http_snapshot_bootstrap_decodes_unsplit_raw_message() -> Result<()> {
    let controls = TestControls::new();
    let vm_controls = controls.vm();
    let mut processor = BroadcasterSubscriptionProcessor::with_decoder(
        Chain::Ethereum.id(),
        vm_controls.clone(),
        raw_decoder(),
        None,
    );
    let header = raw_block_header(30, 10);
    let payloads = vec![
        vm_only_snapshot_start_envelope(1)?,
        raw_snapshot_chunk_envelope(
            2,
            0,
            30,
            vec![raw_protocol_message_with_parts(
                header.clone(),
                SynchronizerState::Ready(header),
                &["0x3333333333333333333333333333333333333333"],
                &[],
                HashMap::new(),
            )],
        )?,
        snapshot_end_envelope_at(3),
    ];
    processor.set_bootstrap_redis_replay_boundary(replay_boundary(3)?);
    vm_controls.stream_health().mark_started().await;
    for payload in payloads {
        processor.observe(payload).await?;
    }

    assert!(processor.bootstrap_complete());
    assert!(
        controls
            .vm_state_store
            .has_pool("0x3333333333333333333333333333333333333333")
            .await
    );
    assert_eq!(controls.vm_state_store.current_block().await, 30);
    assert_eq!(controls.vm_stream_health.last_block().await, 0);

    let snapshot = controls.vm_subscription.snapshot().await;
    assert!(snapshot.connected);
    assert!(snapshot.bootstrap_complete);
    assert_eq!(snapshot.stream_id.as_deref(), Some("stream-1"));
    assert_eq!(snapshot.snapshot_id.as_deref(), Some("snapshot-1"));
    Ok(())
}

#[tokio::test]
async fn snapshot_start_rejects_unexpected_chain_id_before_applying_state() -> Result<()> {
    let controls = TestControls::new();
    let mut processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.native(), None);

    let result = processor
        .observe(snapshot_start_envelope_for_chain(Chain::Base.id())?)
        .await;
    let Err(error) = result else {
        unreachable!("mismatched broadcaster chain id should be rejected");
    };

    assert!(error
        .to_string()
        .contains("broadcaster chain id mismatch for native subscription"));
    let snapshot = controls.native_subscription.snapshot().await;
    assert!(!snapshot.connected);
    assert!(!snapshot.bootstrap_complete);
    assert_eq!(controls.native_state_store.current_block().await, 0);
    assert!(!controls.native_state_store.is_ready());
    Ok(())
}

#[tokio::test]
async fn heartbeat_does_not_refresh_backend_progress_without_state() -> Result<()> {
    let controls = TestControls::new();
    let mut native_processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.native(), None);
    let mut vm_processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.vm(), None);

    bootstrap(&mut native_processor).await?;
    bootstrap(&mut vm_processor).await?;
    native_processor.observe(heartbeat_envelope()?).await?;
    vm_processor.observe(heartbeat_envelope()?).await?;

    assert_eq!(controls.native_state_store.current_block().await, 10);
    assert_eq!(controls.vm_state_store.current_block().await, 11);
    assert!(controls.native_state_store.has_pool("pool-native").await);
    assert!(controls.vm_state_store.has_pool("pool-vm").await);
    Ok(())
}

#[tokio::test]
async fn live_update_keeps_native_and_vm_partitioned() -> Result<()> {
    let controls = TestControls::new();
    let mut native_processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.native(), None);
    let mut vm_processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.vm(), None);

    bootstrap(&mut native_processor).await?;
    bootstrap(&mut vm_processor).await?;
    native_processor.observe(update_envelope()?).await?;
    vm_processor.observe(update_envelope()?).await?;

    assert_eq!(controls.native_state_store.current_block().await, 12);
    assert_eq!(controls.vm_state_store.current_block().await, 11);
    assert!(controls.native_state_store.has_pool("pool-native").await);
    assert!(controls.vm_state_store.has_pool("pool-vm").await);
    Ok(())
}

#[tokio::test]
async fn partial_and_unknown_native_updates_do_not_renew_consumer_progress() -> Result<()> {
    let controls = TestControls::new();
    let mut native_processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.native(), None);
    controls.native_stream_health.mark_started().await;
    bootstrap(&mut native_processor).await?;

    native_processor.observe(update_envelope_at(4)?).await?;
    assert_eq!(controls.native_stream_health.last_block().await, 12);
    native_processor
        .observe(partial_native_update_envelope(5, 13)?)
        .await?;

    assert_eq!(controls.native_state_store.current_block().await, 13);
    assert_eq!(controls.native_stream_health.last_block().await, 12);
    let unknown = BroadcasterUpdatePartition::new(
        BroadcasterBackend::Native,
        14,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        BTreeMap::new(),
    );
    assert_eq!(unknown.complete_native_block(), None);
    Ok(())
}

#[test]
fn recovery_progress_uses_complete_replacement_not_partial_catch_up() -> Result<()> {
    let BroadcasterPayload::Update(mut replacement_update) = update_envelope_at(4)?.payload else {
        return Err(anyhow!("expected complete native replacement update"));
    };
    let replacement_partition = replacement_update
        .partitions
        .pop()
        .ok_or_else(|| anyhow!("complete native replacement partition missing"))?;
    let replacement_partition = BroadcasterSnapshotPartition::new(
        replacement_partition.backend,
        replacement_partition.block_number,
        Vec::new(),
        replacement_partition.sync_statuses,
    );
    let snapshot = vec![BroadcasterEnvelope::new(
        "recovery-stream",
        2,
        BroadcasterPayload::SnapshotChunk(BroadcasterSnapshotChunk::new(
            "recovery-snapshot",
            0,
            vec![replacement_partition],
        )?),
    )];
    let BroadcasterPayload::Update(partial_update) = partial_native_update_envelope(5, 13)?.payload
    else {
        return Err(anyhow!("expected partial native catch-up update"));
    };
    let buffer_a = BTreeMap::from([(
        0,
        BroadcasterRecoveryCatchUp::new("recovery-partial", 104, 0, partial_update)?,
    )]);

    assert_eq!(
        committed_recovery_complete_native_block(&snapshot, &buffer_a),
        Some(12)
    );
    Ok(())
}

#[tokio::test]
async fn native_subscription_reset_does_not_wait_on_vm_permits() -> Result<()> {
    let controls = TestControls::new();
    let mut native_processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.native(), None);
    let mut vm_processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.vm(), None);

    controls.native_stream_health.mark_started().await;
    controls.vm_stream_health.mark_started().await;

    bootstrap(&mut native_processor).await?;
    bootstrap(&mut vm_processor).await?;
    let vm_rebuild_read_guard = Arc::clone(&controls.vm_simulation_rebuild_gate)
        .read_owned()
        .await;

    let native_controls = controls.native();
    let reset = tokio::time::timeout(
        Duration::from_millis(50),
        handle_subscription_reset(
            &native_controls,
            Some("native broadcaster dropped".to_string()),
            None,
        ),
    )
    .await?;
    drop(vm_rebuild_read_guard);

    assert!(reset.is_none());
    let broadcaster_snapshot = controls.native_subscription.snapshot().await;
    assert!(!broadcaster_snapshot.connected);
    assert!(!broadcaster_snapshot.bootstrap_complete);
    assert_eq!(broadcaster_snapshot.snapshot_id, None);
    assert_eq!(broadcaster_snapshot.restart_count, 1);
    assert_eq!(
        broadcaster_snapshot.last_error.as_deref(),
        Some("native broadcaster dropped")
    );

    assert_eq!(controls.native_state_store.current_block().await, 0);
    assert!(!controls.native_state_store.has_pool("pool-native").await);
    assert!(!controls.native_state_store.is_ready());
    assert_eq!(controls.vm_state_store.current_block().await, 11);
    assert!(controls.vm_state_store.has_pool("pool-vm").await);
    assert!(controls.vm_state_store.is_ready());

    assert_eq!(controls.native_stream_health.restart_count().await, 1);
    assert_eq!(controls.vm_stream_health.restart_count().await, 0);
    assert_eq!(
        controls.native_stream_health.last_error().await.as_deref(),
        Some("native broadcaster dropped")
    );

    let vm_stream = controls.vm_stream.read().await;
    assert!(!vm_stream.rebuilding);
    assert_eq!(vm_stream.restart_count, 0);
    assert!(vm_stream.last_error.is_none());
    assert!(vm_stream.rebuild_started_at.is_none());
    Ok(())
}

#[tokio::test]
async fn vm_subscription_reset_finishes_rebuild_after_bootstrap() -> Result<()> {
    let controls = TestControls::new();
    let mut processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.vm(), None);

    controls.vm_stream_health.mark_started().await;
    bootstrap(&mut processor).await?;

    let vm_controls = controls.vm();
    let vm_rebuild = handle_subscription_reset(
        &vm_controls,
        Some("vm broadcaster dropped".to_string()),
        None,
    )
    .await;
    assert!(vm_rebuild.is_some());

    let broadcaster_snapshot = controls.vm_subscription.snapshot().await;
    assert!(!broadcaster_snapshot.connected);
    assert!(!broadcaster_snapshot.bootstrap_complete);
    assert_eq!(broadcaster_snapshot.snapshot_id, None);
    assert_eq!(broadcaster_snapshot.restart_count, 1);
    assert_eq!(
        broadcaster_snapshot.last_error.as_deref(),
        Some("vm broadcaster dropped")
    );

    assert_eq!(controls.vm_state_store.current_block().await, 0);
    assert!(!controls.vm_state_store.has_pool("pool-vm").await);
    assert!(!controls.vm_state_store.is_ready());

    assert_eq!(controls.vm_stream_health.restart_count().await, 1);
    assert_eq!(
        controls.vm_stream_health.last_error().await.as_deref(),
        Some("vm broadcaster dropped")
    );

    let vm_stream = controls.vm_stream.read().await;
    assert!(vm_stream.rebuilding);
    assert_eq!(vm_stream.restart_count, 1);
    assert_eq!(
        vm_stream.last_error.as_deref(),
        Some("vm broadcaster dropped")
    );
    assert!(vm_stream.rebuild_started_at.is_some());
    drop(vm_stream);

    let mut processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.vm(), vm_rebuild);
    bootstrap(&mut processor).await?;

    let vm_stream = controls.vm_stream.read().await;
    assert!(!vm_stream.rebuilding);
    assert!(vm_stream.rebuild_started_at.is_none());
    Ok(())
}

#[tokio::test]
async fn vm_subscription_reset_does_not_wait_on_rfq_route_guards() -> Result<()> {
    let controls = TestControls::new();
    let mut processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.vm(), None);

    controls.vm_stream_health.mark_started().await;
    bootstrap(&mut processor).await?;

    let rfq_route_guard = Arc::clone(&controls.rfq_simulation_rebuild_gate)
        .read_owned()
        .await;
    let vm_controls = controls.vm();
    let vm_rebuild = tokio::time::timeout(
        Duration::from_secs(5),
        handle_subscription_reset(
            &vm_controls,
            Some("vm broadcaster dropped".to_string()),
            None,
        ),
    )
    .await?;
    drop(rfq_route_guard);

    assert!(vm_rebuild.is_some());
    assert_eq!(controls.vm_state_store.current_block().await, 0);
    assert!(!controls.vm_state_store.has_pool("pool-vm").await);
    assert!(controls.rfq_simulation_rebuild_gate.try_write().is_ok());
    Ok(())
}

#[tokio::test]
async fn rfq_subscription_reset_waits_on_rfq_rebuild_gate_until_bootstrap() -> Result<()> {
    let controls = TestControls::new();
    let mut native_processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.native(), None);
    let mut vm_processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.vm(), None);
    let mut processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.rfq(), None);

    bootstrap(&mut native_processor).await?;
    bootstrap(&mut vm_processor).await?;
    controls.rfq_stream_health.mark_started().await;
    processor.observe(rfq_snapshot_start_envelope(1)?).await?;
    processor.observe(rfq_snapshot_chunk_envelope(21)?).await?;
    processor.observe(snapshot_end_envelope()).await?;

    let route_read_guard = Arc::clone(&controls.rfq_simulation_rebuild_gate)
        .read_owned()
        .await;
    let rfq_controls = controls.rfq();
    let mut reset_task = tokio::spawn(async move {
        handle_subscription_reset(
            &rfq_controls,
            Some("rfq broadcaster dropped".to_string()),
            None,
        )
        .await
    });

    wait_for_subscription_restart(&controls.rfq_subscription, 1).await?;
    assert!(
        !reset_task.is_finished(),
        "RFQ reset should wait for in-flight RFQ route guards"
    );
    assert!(controls.rfq_state_store.has_pool("pool-rfq").await);

    drop(route_read_guard);

    let rfq_rebuild = tokio::time::timeout(Duration::from_secs(5), &mut reset_task).await??;
    assert!(rfq_rebuild.is_some());
    assert!(controls.rfq_simulation_rebuild_gate.try_write().is_err());
    assert!(controls.vm_simulation_rebuild_gate.try_write().is_ok());
    assert_eq!(controls.rfq_state_store.current_block().await, 0);
    assert!(!controls.rfq_state_store.has_pool("pool-rfq").await);
    assert!(!controls.rfq_state_store.is_ready());
    assert_ne!(controls.native_state_store.current_block().await, 0);
    assert!(controls.native_state_store.has_pool("pool-native").await);
    assert_ne!(controls.vm_state_store.current_block().await, 0);
    assert!(controls.vm_state_store.has_pool("pool-vm").await);

    let mut processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.rfq(), rfq_rebuild);
    processor.observe(rfq_snapshot_start_envelope(1)?).await?;
    processor.observe(rfq_snapshot_chunk_envelope(23)?).await?;
    processor.observe(snapshot_end_envelope()).await?;

    assert!(controls.rfq_simulation_rebuild_gate.try_write().is_ok());
    assert_eq!(controls.rfq_state_store.current_block().await, 23);
    assert!(controls.rfq_state_store.has_pool("pool-rfq").await);
    Ok(())
}

async fn wait_for_subscription_restart(
    status: &BroadcasterSubscriptionStatus,
    expected_restart_count: u64,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if status.snapshot().await.restart_count >= expected_restart_count {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| anyhow!("timed out waiting for broadcaster subscription restart"))?;
    Ok(())
}

#[tokio::test]
async fn native_processor_ignores_vm_partitions() -> Result<()> {
    let controls = TestControls::new();
    let mut processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.native(), None);

    controls.native_stream_health.mark_started().await;

    processor.observe(snapshot_start_envelope()?).await?;
    processor.observe(snapshot_chunk_envelope()?).await?;

    assert!(controls.native_state_store.has_pool("pool-native").await);
    assert!(!controls.vm_state_store.has_pool("pool-vm").await);
    assert_eq!(controls.native_state_store.current_block().await, 10);
    assert_eq!(controls.vm_state_store.current_block().await, 0);

    processor.observe(snapshot_end_envelope()).await?;

    let broadcaster_snapshot = controls.native_subscription.snapshot().await;
    assert!(processor.bootstrap_complete());
    assert!(broadcaster_snapshot.bootstrap_complete);
    assert!(broadcaster_snapshot.connected);
    assert_eq!(controls.native_stream_health.last_block().await, 0);
    assert!(controls
        .native_stream_health
        .last_update_age_ms()
        .await
        .is_none());
    Ok(())
}

fn raw_protocol_message(
    account_address: Bytes,
    account: ResponseAccount,
) -> BroadcasterProtocolMessage {
    let header = raw_block_header(10, 1);
    raw_protocol_message_with_parts(
        header.clone(),
        SynchronizerState::Ready(header),
        &[],
        &[],
        HashMap::from([(account_address, account)]),
    )
}

fn raw_protocol_message_with_ids(
    state_ids: &[&str],
    removal_ids: &[&str],
) -> BroadcasterProtocolMessage {
    let header = raw_block_header(10, 1);
    raw_protocol_message_with_parts(
        header.clone(),
        SynchronizerState::Ready(header),
        state_ids,
        removal_ids,
        HashMap::new(),
    )
}

fn raw_protocol_message_with_header(
    header: BlockHeader,
    sync_state: SynchronizerState,
) -> BroadcasterProtocolMessage {
    raw_protocol_message_with_parts(header, sync_state, &[], &[], HashMap::new())
}

fn raw_protocol_message_with_parts(
    header: BlockHeader,
    sync_state: SynchronizerState,
    state_ids: &[&str],
    removal_ids: &[&str],
    vm_storage: HashMap<Bytes, ResponseAccount>,
) -> BroadcasterProtocolMessage {
    BroadcasterProtocolMessage::new(
        "vm:curve",
        sync_state,
        StateSyncMessage {
            header,
            snapshots: Snapshot {
                states: state_ids
                    .iter()
                    .map(|component_id| {
                        (
                            (*component_id).to_string(),
                            raw_component_with_state(component_id),
                        )
                    })
                    .collect(),
                vm_storage: vm_storage
                    .into_iter()
                    .map(|(address, account)| (address, account.into()))
                    .collect(),
            },
            deltas: None,
            removed_components: removal_ids
                .iter()
                .map(|component_id| {
                    (
                        (*component_id).to_string(),
                        raw_dto_protocol_component(component_id).into(),
                    )
                })
                .collect(),
        },
    )
}

fn raw_component_with_state(component_id: &str) -> ComponentWithState {
    ComponentWithState {
        state: ResponseProtocolState {
            component_id: component_id.to_string(),
            attributes: HashMap::new(),
            balances: HashMap::new(),
        }
        .into(),
        component: raw_dto_protocol_component(component_id).into(),
        component_tvl: None,
        entrypoints: Vec::new(),
    }
}

fn raw_dto_protocol_component(component_id: &str) -> DtoProtocolComponent {
    DtoProtocolComponent {
        id: component_id.to_string(),
        protocol_system: "vm:curve".to_string(),
        protocol_type_name: "curve_pool".to_string(),
        chain: DtoChain::Ethereum,
        tokens: Vec::new(),
        contract_ids: Vec::new(),
        static_attributes: HashMap::new(),
        change: Default::default(),
        creation_tx: Bytes::from([0u8; 32]),
        created_at: chrono::NaiveDateTime::default(),
    }
}

fn raw_block_header(number: u64, seed: u8) -> BlockHeader {
    BlockHeader {
        hash: Bytes::from(vec![seed; 32]),
        number,
        parent_hash: Bytes::from(vec![seed.saturating_add(1); 32]),
        revert: false,
        timestamp: number * 10,
        partial_block_index: None,
    }
}

fn raw_response_account(address: Bytes, title: &str, slot_values: &[(u8, u8)]) -> ResponseAccount {
    let slots = slot_values
        .iter()
        .map(|(slot_seed, value_seed)| {
            (
                Bytes::from([*slot_seed; 32]),
                Bytes::from([*value_seed; 32]),
            )
        })
        .collect();
    ResponseAccount::new(
        DtoChain::Ethereum,
        address,
        title.to_string(),
        slots,
        Bytes::from([0u8; 32]),
        HashMap::new(),
        Bytes::from([7u8; 32]),
        Bytes::from([8u8; 32]),
        Bytes::from([9u8; 32]),
        Bytes::from([10u8; 32]),
        None,
    )
}
