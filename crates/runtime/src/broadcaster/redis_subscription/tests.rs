use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use num_bigint::BigUint;
use tokio::sync::RwLock;
use tycho_simulation::tycho_common::dto::ProtocolStateDelta;
use tycho_simulation::tycho_common::simulation::errors::{SimulationError, TransitionError};
use tycho_simulation::{
    evm::decoder::TychoStreamDecoder,
    protocol::{
        errors::InvalidSnapshotError,
        models::{DecoderContext, ProtocolComponent, TryFromWithBlock},
    },
    tycho_client::feed::{
        synchronizer::{ComponentWithState, Snapshot, StateSyncMessage},
        BlockHeader, SynchronizerState,
    },
    tycho_common::{
        dto::{
            Chain as DtoChain, ProtocolComponent as DtoProtocolComponent, ResponseAccount,
            ResponseProtocolState,
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
    apply_replay_batch, continue_redis_generation_handoff, mark_redis_replay_checkpoints,
    BroadcasterSubscriptionControls, NativeBroadcasterSubscriptionControls,
    PreparedBroadcasterRedisSubscription, VmBroadcasterSubscriptionControls,
};
use crate::models::state::{BroadcasterSubscriptionStatus, StateStore, VmStreamStatus};
use crate::models::stream_health::StreamHealth;
use crate::models::tokens::TokenStore;
use broadcaster_replay_client::{
    GenerationHandoffCandidate, ReplayBatchItem, ReplayCheckpoint, ReplayMessage,
};
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterGenerationHandoff,
    BroadcasterHeartbeat, BroadcasterPayload, BroadcasterProgress, BroadcasterProtocolMessage,
    BroadcasterRedisReplayBoundary, BroadcasterRedisStreamEntry, BroadcasterSnapshotChunk,
    BroadcasterSnapshotEnd, BroadcasterSnapshotPartition, BroadcasterSnapshotStart,
    BroadcasterStateDelta, BroadcasterStateEntry, BroadcasterUpdateMessage,
    BroadcasterUpdatePartition,
};

const REDIS_EVENT_TIME_MS: u64 = 1_710_000_000_123;

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
    assert_eq!(controls.native_state_store.current_block().await, 14);
    Ok(())
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

    assert_eq!(controls.native_state_store.current_block().await, 14);
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
async fn redis_generation_handoff_accepts_marker() -> Result<()> {
    let (controls, mut prepared) = prepared_native_handoff_subscription().await?;

    continue_redis_generation_handoff(
        &mut prepared,
        &handoff_candidate(
            "stream-7",
            "7-103",
            vec![BroadcasterBackendHead::new(BroadcasterBackend::Native, 70)],
        )?,
    )
    .await
    .map_err(|error| anyhow!(error))?;

    let snapshot = controls.native_subscription.snapshot().await;
    assert_eq!(snapshot.restart_count, 0);
    assert_eq!(snapshot.stream_id.as_deref(), Some("stream-8"));
    assert_eq!(snapshot.snapshot_id.as_deref(), Some("snapshot-8"));
    assert_eq!(snapshot.redis_replay_checkpoint.as_deref(), Some("8-1"));
    assert!(!snapshot.redis_replay_caught_up);
    let status_boundary = snapshot
        .redis_replay_boundary
        .as_ref()
        .ok_or_else(|| anyhow!("handoff should publish the new Redis boundary"))?;
    assert_eq!(status_boundary.stream_id, "stream-8");
    assert_eq!(status_boundary.snapshot_id, "snapshot-8");
    assert_eq!(status_boundary.generation, 8);
    assert_eq!(status_boundary.exclusive_message_seq, 1);
    Ok(())
}

#[tokio::test]
async fn redis_replay_batch_applies_update_after_generation_handoff() -> Result<()> {
    let (controls, mut prepared) = prepared_native_handoff_subscription().await?;
    let old_boundary = redis_boundary("stream-7", "snapshot-7", 7, 103)?;
    let new_boundary = redis_boundary("stream-8", "snapshot-8", 8, 1)?;
    let update_boundary = redis_boundary("stream-8", "snapshot-8", 8, 2)?;
    let update = update_envelope_for_stream("stream-8", 2, 71)?;
    let mut update_entry = redis_entry_for_scope(&update, "native");
    update_entry.snapshot_id = Some("snapshot-8".to_string());
    let mut checkpoint = ReplayCheckpoint::new(old_boundary, Chain::Ethereum.id());

    apply_replay_batch(
        &mut prepared,
        &mut checkpoint,
        vec![
            ReplayBatchItem::GenerationHandoff(handoff_candidate(
                "stream-7",
                "7-103",
                vec![BroadcasterBackendHead::new(BroadcasterBackend::Native, 70)],
            )?),
            ReplayBatchItem::Message(ReplayMessage {
                entry_id: "8-2".to_string(),
                entry: update_entry,
                envelope: update,
                checkpoint_after: ReplayCheckpoint::new(update_boundary, Chain::Ethereum.id()),
            }),
        ],
    )
    .await
    .map_err(|exit| anyhow!(exit.message))?;

    assert_eq!(controls.native_state_store.current_block().await, 71);
    assert_eq!(checkpoint.entry_id(), "8-2");
    assert_eq!(prepared.replay_boundary, new_boundary);
    assert_eq!(
        controls
            .native_subscription
            .snapshot()
            .await
            .redis_replay_checkpoint
            .as_deref(),
        Some("8-2")
    );
    Ok(())
}

#[tokio::test]
async fn redis_generation_handoff_rejects_invalid_proofs_before_applying_state() -> Result<()> {
    let invalid_messages = [
        ("missing handoff", handoff_candidate_without_proof()?),
        (
            "missing backend head",
            handoff_candidate("stream-7", "7-103", Vec::new())?,
        ),
        (
            "extra backend head",
            handoff_candidate(
                "stream-7",
                "7-103",
                vec![
                    BroadcasterBackendHead::new(BroadcasterBackend::Native, 70),
                    BroadcasterBackendHead::new(BroadcasterBackend::Vm, 11),
                ],
            )?,
        ),
        (
            "base head mismatch",
            handoff_candidate(
                "stream-7",
                "7-103",
                vec![BroadcasterBackendHead::new(BroadcasterBackend::Native, 69)],
            )?,
        ),
    ];

    for (invalid, candidate) in invalid_messages {
        let (controls, mut prepared) = prepared_native_handoff_subscription().await?;
        let error = continue_redis_generation_handoff(&mut prepared, &candidate)
            .await
            .err()
            .ok_or_else(|| anyhow!("{invalid} should fail closed"))?;

        assert!(
            error.to_string().contains("Redis replay gap"),
            "{invalid} should return a Redis replay gap: {error}"
        );
        assert_eq!(controls.native_state_store.current_block().await, 70);
        let snapshot = controls.native_subscription.snapshot().await;
        assert_eq!(snapshot.restart_count, 0);
        assert_eq!(snapshot.stream_id.as_deref(), Some("stream-7"));
        assert_eq!(snapshot.snapshot_id.as_deref(), Some("snapshot-7"));
        assert_eq!(snapshot.redis_replay_checkpoint.as_deref(), Some("7-103"));
        let boundary = snapshot
            .redis_replay_boundary
            .as_ref()
            .ok_or_else(|| anyhow!("old Redis boundary should remain visible"))?;
        assert_eq!(boundary.generation, 7);
    }
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
        kind: envelope.kind(),
        snapshot_id: Some("snapshot-1".to_string()),
        backend_scope: backend_scope.to_string(),
        block_number: None,
        observed_timestamp_ms: None,
        event_time_ms: REDIS_EVENT_TIME_MS,
        payload_json: String::new(),
    }
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
    )
    .map_err(Into::into)
}

async fn prepared_native_handoff_subscription(
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

fn handoff_candidate(
    previous_stream_id: &str,
    previous_entry_id: &str,
    base_heads: Vec<BroadcasterBackendHead>,
) -> Result<GenerationHandoffCandidate> {
    generation_handoff_candidate(Some(BroadcasterGenerationHandoff::new(
        previous_stream_id,
        previous_entry_id,
        base_heads,
    )?))
}

fn handoff_candidate_without_proof() -> Result<GenerationHandoffCandidate> {
    generation_handoff_candidate(None)
}

fn generation_handoff_candidate(
    handoff: Option<BroadcasterGenerationHandoff>,
) -> Result<GenerationHandoffCandidate> {
    let progress = match handoff {
        Some(handoff) => BroadcasterProgress::new_with_handoff(
            Chain::Ethereum.id(),
            "snapshot-8",
            vec![BroadcasterBackend::Native],
            "active_writer_promoted",
            handoff,
        )?,
        None => BroadcasterProgress::new(
            Chain::Ethereum.id(),
            "snapshot-8",
            vec![BroadcasterBackend::Native],
            "active_writer_promoted",
        )?,
    };
    let envelope = BroadcasterEnvelope::new("stream-8", 1, BroadcasterPayload::Progress(progress));
    let boundary = redis_boundary("stream-8", "snapshot-8", 8, 1)?;
    Ok(GenerationHandoffCandidate {
        entry: BroadcasterRedisStreamEntry::from_envelope(
            Chain::Ethereum.id(),
            1_710_000_000_123,
            &envelope,
        )?,
        envelope,
        boundary: boundary.clone(),
        checkpoint_after: ReplayCheckpoint::new(boundary, Chain::Ethereum.id()),
    })
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
                BTreeMap::new(),
            ),
        ])?),
    ))
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

fn raw_decoder() -> Arc<TychoStreamDecoder<BlockHeader>> {
    let mut decoder = TychoStreamDecoder::new();
    decoder.register_decoder::<DummySim>("vm:curve");
    Arc::new(decoder)
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
        .is_some());
    assert!(controls
        .vm_stream_health
        .last_update_age_ms()
        .await
        .is_some());
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
    assert_eq!(controls.vm_stream_health.last_block().await, 21);
    assert!(controls
        .vm_stream_health
        .last_update_age_ms()
        .await
        .is_some());
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
    assert_eq!(controls.vm_stream_health.last_block().await, 30);

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
async fn heartbeat_refreshes_backend_blocks_without_new_state() -> Result<()> {
    let controls = TestControls::new();
    let mut native_processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.native(), None);
    let mut vm_processor =
        BroadcasterSubscriptionProcessor::new(Chain::Ethereum.id(), controls.vm(), None);

    bootstrap(&mut native_processor).await?;
    bootstrap(&mut vm_processor).await?;
    native_processor.observe(heartbeat_envelope()?).await?;
    vm_processor.observe(heartbeat_envelope()?).await?;

    assert_eq!(controls.native_state_store.current_block().await, 14);
    assert_eq!(controls.vm_state_store.current_block().await, 15);
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
    assert_eq!(controls.native_stream_health.last_block().await, 10);
    assert!(controls
        .native_stream_health
        .last_update_age_ms()
        .await
        .is_some());
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
                vm_storage,
            },
            deltas: None,
            removed_components: removal_ids
                .iter()
                .map(|component_id| {
                    (
                        (*component_id).to_string(),
                        raw_dto_protocol_component(component_id),
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
        },
        component: raw_dto_protocol_component(component_id),
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
