use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::PathBuf,
    time::Duration,
};

use anyhow::Result;
use chrono::NaiveDateTime;
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterHeartbeat,
    BroadcasterPayload, BroadcasterProgress, BroadcasterProtocolMessage,
    BroadcasterProtocolSyncStatus, BroadcasterRecoveryCatchUp, BroadcasterRecoveryChunk,
    BroadcasterRecoveryCommit, BroadcasterRecoveryManifest, BroadcasterRecoveryStart,
    BroadcasterSnapshotChunk, BroadcasterSnapshotEnd, BroadcasterSnapshotPartition,
    BroadcasterSnapshotStart, BroadcasterStateEntry, BroadcasterUpdateMessage,
    BroadcasterUpdatePartition,
};
use tycho_simulation::{
    protocol::models::ProtocolComponent as SimulationProtocolComponent,
    rfq::protocols::bebop::{client::BebopClient, state::BebopState},
    tycho_client::feed::{
        synchronizer::{ComponentWithState, Snapshot, StateSyncMessage},
        BlockHeader, SynchronizerState,
    },
    tycho_common::{
        dto::{
            AccountBalance, AccountUpdate, Block, BlockAggregatedChanges, Chain as DtoChain,
            ChangeType, ComponentBalance, DCIUpdate, EntryPoint, EntryPointWithTracingParams,
            ProtocolComponent, ProtocolStateDelta, RPCTracerParams, ResponseAccount,
            ResponseProtocolState, TokenBalances, TracingParams, TracingResult,
        },
        models::{token::Token, Chain},
        Bytes,
    },
};

const STREAM_ID: &str = "wire-fixture-stream";
const SNAPSHOT_ID: &str = "wire-fixture-snapshot";
const RECOVERY_ID: &str = "wire-fixture-recovery";

#[test]
fn native_update_matches_wire_fixture() -> Result<()> {
    assert_wire_fixture("native_update.json", native_update_envelope(1)?)
}

#[test]
fn rfq_update_matches_wire_fixture() -> Result<()> {
    assert_wire_fixture("rfq_update.json", rfq_update_envelope(2)?)
}

/// An RFQ update written before the Bebop client gained its optional origin fields still
/// reads, as the current wire form with those fields empty. Only this direction is tested.
#[test]
fn rfq_update_written_before_the_bebop_origin_fields_still_reads() -> Result<()> {
    let legacy = fs::read(fixture_path("rfq_update_before_bebop_origin.json"))?;
    let decoded: BroadcasterEnvelope = serde_json::from_slice(&legacy)?;
    let current = fs::read(fixture_path("rfq_update.json"))?;
    assert_eq!(serde_json::to_string(&decoded)?.as_bytes(), current);
    Ok(())
}

#[test]
fn heartbeat_matches_wire_fixture() -> Result<()> {
    let heartbeat = BroadcasterHeartbeat::new(
        8453,
        SNAPSHOT_ID,
        vec![
            BroadcasterBackendHead::new(BroadcasterBackend::Rfq, 1_710_000_000),
            BroadcasterBackendHead::new(BroadcasterBackend::Vm, 19_000_001),
            BroadcasterBackendHead::new(BroadcasterBackend::Native, 19_000_000),
        ],
    )?;
    assert_wire_fixture(
        "heartbeat.json",
        envelope(3, BroadcasterPayload::Heartbeat(heartbeat)),
    )
}

#[test]
fn progress_matches_wire_fixture() -> Result<()> {
    let progress = BroadcasterProgress::new(
        8453,
        SNAPSHOT_ID,
        vec![BroadcasterBackend::Vm, BroadcasterBackend::Native],
        "snapshot assembly in progress",
    )?;
    assert_wire_fixture(
        "progress.json",
        envelope(4, BroadcasterPayload::Progress(progress)),
    )
}

#[test]
fn snapshot_start_matches_wire_fixture() -> Result<()> {
    let start = BroadcasterSnapshotStart::new(
        SNAPSHOT_ID,
        8453,
        vec![
            BroadcasterBackend::Rfq,
            BroadcasterBackend::Native,
            BroadcasterBackend::Vm,
        ],
        1,
    )?;
    assert_wire_fixture(
        "snapshot_start.json",
        envelope(5, BroadcasterPayload::SnapshotStart(start)),
    )
}

#[test]
fn snapshot_chunk_matches_wire_fixture() -> Result<()> {
    let header = block_header(19_000_000, 0x11);
    let partition = BroadcasterSnapshotPartition::with_messages(
        BroadcasterBackend::Native,
        header.number,
        vec![protocol_message(
            "uniswap_v2",
            SynchronizerState::Ready(header.clone()),
            empty_state_sync_message(header),
        )],
        BTreeMap::from([(
            "uniswap_v2".to_string(),
            BroadcasterProtocolSyncStatus::from_synchronizer_state(&SynchronizerState::Ready(
                block_header(19_000_000, 0x11),
            )),
        )]),
    );
    let chunk = BroadcasterSnapshotChunk::new(SNAPSHOT_ID, 0, vec![partition])?;
    assert_wire_fixture(
        "snapshot_chunk.json",
        envelope(6, BroadcasterPayload::SnapshotChunk(chunk)),
    )
}

#[test]
fn snapshot_end_matches_wire_fixture() -> Result<()> {
    assert_wire_fixture(
        "snapshot_end.json",
        envelope(
            7,
            BroadcasterPayload::SnapshotEnd(BroadcasterSnapshotEnd::new(SNAPSHOT_ID)),
        ),
    )
}

#[test]
fn recovery_start_matches_wire_fixture() -> Result<()> {
    let start = BroadcasterRecoveryStart::new(recovery_manifest()?)?;
    assert_wire_fixture(
        "recovery_start.json",
        envelope(8, BroadcasterPayload::RecoveryStart(start)),
    )
}

#[test]
fn recovery_chunk_matches_wire_fixture() -> Result<()> {
    let chunk = BroadcasterRecoveryChunk::new(
        RECOVERY_ID,
        42,
        vec![BroadcasterBackend::Vm, BroadcasterBackend::Native],
        0,
        128,
        "sha256:chunk-0",
        "eyJwYXJ0aXRpb25zIjpbXX0=",
    )?;
    assert_wire_fixture(
        "recovery_chunk.json",
        envelope(9, BroadcasterPayload::RecoveryChunk(chunk)),
    )
}

#[test]
fn recovery_catch_up_matches_wire_fixture() -> Result<()> {
    let update = BroadcasterUpdateMessage::new(vec![BroadcasterUpdatePartition::with_messages(
        BroadcasterBackend::Native,
        19_000_001,
        Vec::new(),
        BTreeMap::from([(
            "uniswap_v2".to_string(),
            BroadcasterProtocolSyncStatus::from_synchronizer_state(&SynchronizerState::Ready(
                block_header(19_000_001, 0x12),
            )),
        )]),
    )])?;
    let catch_up = BroadcasterRecoveryCatchUp::new(RECOVERY_ID, 42, 0, update)?;
    assert_wire_fixture(
        "recovery_catch_up.json",
        envelope(10, BroadcasterPayload::RecoveryCatchUp(catch_up)),
    )
}

#[test]
fn recovery_commit_matches_wire_fixture() -> Result<()> {
    let commit = BroadcasterRecoveryCommit::new(
        RECOVERY_ID,
        42,
        vec![BroadcasterBackend::Vm, BroadcasterBackend::Native],
        "sha256:replacement",
        1,
        19_000_001,
    )?;
    assert_wire_fixture(
        "recovery_commit.json",
        envelope(11, BroadcasterPayload::RecoveryCommit(commit)),
    )
}

fn assert_wire_fixture(name: &str, envelope: BroadcasterEnvelope) -> Result<()> {
    let serialized = serde_json::to_string(&envelope)?;
    let fixture_path = fixture_path(name);

    if std::env::var_os("UPDATE_WIRE_FIXTURES").is_some() {
        fs::write(&fixture_path, serialized.as_bytes())?;
    }

    let fixture = fs::read(&fixture_path)?;
    assert_eq!(serialized.as_bytes(), fixture);

    let decoded: BroadcasterEnvelope = serde_json::from_slice(&fixture)?;
    assert_eq!(serde_json::to_string(&decoded)?.as_bytes(), fixture);
    Ok(())
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("wire")
        .join(name)
}

fn envelope(message_seq: u64, payload: BroadcasterPayload) -> BroadcasterEnvelope {
    BroadcasterEnvelope::new(STREAM_ID, message_seq, payload)
}

fn native_update_envelope(message_seq: u64) -> Result<BroadcasterEnvelope> {
    let sync_states = [
        SynchronizerState::Started,
        SynchronizerState::Ready(block_header(19_000_000, 0x21)),
        SynchronizerState::Delayed(block_header(18_999_999, 0x22)),
        SynchronizerState::Stale(block_header(18_999_998, 0x23)),
        SynchronizerState::Advanced(block_header(19_000_001, 0x24)),
        SynchronizerState::Ended("stream closed by fixture".to_string()),
    ];
    let protocols = [
        "uniswap_v2",
        "uniswap_v3",
        "uniswap_v4",
        "aerodrome_slipstreams",
        "pancakeswap_v2",
        "pancakeswap_v3",
    ];

    // Tycho's tagged Ended newtype cannot serialize, so its wire shape is covered by sync_statuses.
    let messages = protocols
        .iter()
        .zip(sync_states.iter().take(5))
        .enumerate()
        .map(|(index, (protocol, sync_state))| {
            let header = block_header(19_000_000 + index as u64, 0x30 + index as u8);
            let message = if index == 0 {
                populated_state_sync_message(header)
            } else {
                empty_state_sync_message(header)
            };
            protocol_message(protocol, sync_state.clone(), message)
        })
        .collect();
    let sync_statuses = protocols
        .iter()
        .zip(sync_states.iter())
        .map(|(protocol, state)| {
            (
                (*protocol).to_string(),
                BroadcasterProtocolSyncStatus::from_synchronizer_state(state),
            )
        })
        .collect();
    let partition = BroadcasterUpdatePartition::with_messages(
        BroadcasterBackend::Native,
        19_000_005,
        messages,
        sync_statuses,
    );
    let update = BroadcasterUpdateMessage::new(vec![partition])?;
    Ok(envelope(message_seq, BroadcasterPayload::Update(update)))
}

fn rfq_update_envelope(message_seq: u64) -> Result<BroadcasterEnvelope> {
    let component_id = "rfq:bebop:WETH-USDC";
    let state = bebop_state()?;
    let component = SimulationProtocolComponent::new(
        bytes(0xd1, 32),
        "rfq:bebop".to_string(),
        "bebop_pool".to_string(),
        Chain::Ethereum,
        vec![state.base_token.clone(), state.quote_token.clone()],
        Vec::new(),
        HashMap::from([("market".to_string(), bytes(0xd2, 9))]),
        bytes(0xd0, 32),
        NaiveDateTime::default(),
    );
    let entry = BroadcasterStateEntry::new(component_id, component, Box::new(state));
    let partition = BroadcasterUpdatePartition::new(
        BroadcasterBackend::Rfq,
        1_710_000_000,
        vec![entry],
        Vec::new(),
        Vec::new(),
        BTreeMap::new(),
    );
    let update = BroadcasterUpdateMessage::new(vec![partition])?;
    Ok(envelope(message_seq, BroadcasterPayload::Update(update)))
}

fn recovery_manifest() -> Result<BroadcasterRecoveryManifest> {
    BroadcasterRecoveryManifest::new(
        RECOVERY_ID,
        42,
        vec![BroadcasterBackend::Vm, BroadcasterBackend::Native],
        128,
        vec![128],
        vec!["sha256:chunk-0".to_string()],
        "sha256:replacement",
        1,
        19_000_001,
    )
    .map_err(Into::into)
}

fn protocol_message(
    protocol: &str,
    sync_state: SynchronizerState,
    message: StateSyncMessage<BlockHeader>,
) -> BroadcasterProtocolMessage {
    BroadcasterProtocolMessage::new(protocol, sync_state, message)
}

fn populated_state_sync_message(header: BlockHeader) -> StateSyncMessage<BlockHeader> {
    let component_id = "native-pool-1";
    let account_address = bytes(0xa1, 20);
    let token_address = bytes(0xb1, 20);
    let (entrypoint, tracing_result) = fixture_entrypoint();
    StateSyncMessage {
        header: header.clone(),
        snapshots: Snapshot {
            states: HashMap::from([(
                component_id.to_string(),
                ComponentWithState {
                    state: ResponseProtocolState {
                        component_id: component_id.to_string(),
                        attributes: HashMap::from([("reserve0".to_string(), bytes(0x01, 32))]),
                        balances: HashMap::from([(token_address.clone(), bytes(0x02, 32))]),
                    }
                    .into(),
                    component: protocol_component(component_id, "uniswap_v2", "uniswap_v2_pool")
                        .into(),
                    component_tvl: Some(1_234_567.5),
                    entrypoints: vec![(entrypoint.into(), tracing_result.into())],
                },
            )]),
            vm_storage: HashMap::from([(
                account_address.clone(),
                response_account(account_address.clone(), token_address.clone()).into(),
            )]),
        },
        deltas: Some(block_changes(header, component_id, account_address, token_address).into()),
        removed_components: HashMap::from([(
            "native-pool-removed".to_string(),
            protocol_component("native-pool-removed", "uniswap_v2", "uniswap_v2_pool").into(),
        )]),
    }
}

fn empty_state_sync_message(header: BlockHeader) -> StateSyncMessage<BlockHeader> {
    StateSyncMessage {
        header,
        snapshots: Snapshot::default(),
        deltas: None,
        removed_components: HashMap::new(),
    }
}

fn block_changes(
    header: BlockHeader,
    component_id: &str,
    account_address: Bytes,
    token_address: Bytes,
) -> BlockAggregatedChanges {
    let entrypoint = fixture_entrypoint().0;
    let entrypoint_id = entrypoint.entry_point.external_id.clone();
    let tracing_params = entrypoint.params;
    BlockAggregatedChanges {
        extractor: "uniswap_v2".to_string(),
        chain: DtoChain::Ethereum,
        block: Block {
            number: header.number,
            hash: header.hash,
            parent_hash: header.parent_hash,
            chain: DtoChain::Ethereum,
            ts: NaiveDateTime::default(),
        },
        finalized_block_height: 18_999_900,
        revert: false,
        new_tokens: HashMap::new(),
        account_updates: HashMap::from([(
            account_address.clone(),
            AccountUpdate::new(
                account_address.clone(),
                DtoChain::Ethereum,
                HashMap::from([(bytes(0x01, 32), bytes(0x03, 32))]),
                Some(bytes(0x04, 32)),
                Some(bytes(0x60, 8)),
                ChangeType::Update,
            ),
        )]),
        state_updates: HashMap::from([(
            component_id.to_string(),
            ProtocolStateDelta {
                component_id: component_id.to_string(),
                updated_attributes: HashMap::from([("reserve1".to_string(), bytes(0x05, 32))]),
                deleted_attributes: HashSet::from(["obsolete".to_string()]),
            },
        )]),
        new_protocol_components: HashMap::from([(
            "native-pool-new".to_string(),
            protocol_component("native-pool-new", "uniswap_v2", "uniswap_v2_pool"),
        )]),
        deleted_protocol_components: HashMap::from([(
            "native-pool-deleted".to_string(),
            protocol_component("native-pool-deleted", "uniswap_v2", "uniswap_v2_pool"),
        )]),
        component_balances: HashMap::from([(
            component_id.to_string(),
            TokenBalances(HashMap::from([(
                token_address.clone(),
                ComponentBalance {
                    token: token_address.clone(),
                    balance: bytes(0x06, 32),
                    balance_float: 42.5,
                    modify_tx: bytes(0xc1, 32),
                    component_id: component_id.to_string(),
                },
            )])),
        )]),
        account_balances: HashMap::from([(
            account_address.clone(),
            HashMap::from([(
                token_address.clone(),
                AccountBalance {
                    account: account_address,
                    token: token_address,
                    balance: bytes(0x07, 32),
                    modify_tx: bytes(0xc2, 32),
                },
            )]),
        )]),
        component_tvl: HashMap::from([(component_id.to_string(), 1_234_600.0)]),
        dci_update: DCIUpdate {
            new_entrypoints: HashMap::from([(
                component_id.to_string(),
                HashSet::from([entrypoint.entry_point]),
            )]),
            new_entrypoint_params: HashMap::from([(
                entrypoint_id.clone(),
                HashSet::from([(tracing_params, component_id.to_string())]),
            )]),
            trace_results: HashMap::from([(entrypoint_id, TracingResult::default())]),
        },
        partial_block_index: Some(0),
    }
}

fn fixture_entrypoint() -> (EntryPointWithTracingParams, TracingResult) {
    (
        EntryPointWithTracingParams {
            entry_point: EntryPoint {
                external_id: "0xe1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1:sync()".to_string(),
                target: bytes(0xe1, 20),
                signature: "sync()".to_string(),
            },
            params: TracingParams::RPCTracer(RPCTracerParams {
                caller: Some(bytes(0xe2, 20)),
                calldata: bytes(0xaa, 4),
                state_overrides: None,
                prune_addresses: Some(vec![bytes(0xe3, 20)]),
            }),
        },
        TracingResult::default(),
    )
}

fn response_account(address: Bytes, token_address: Bytes) -> ResponseAccount {
    ResponseAccount::new(
        DtoChain::Ethereum,
        address,
        "fixture vault".to_string(),
        HashMap::from([(bytes(0x10, 32), bytes(0x11, 32))]),
        bytes(0x12, 32),
        HashMap::from([(token_address, bytes(0x13, 32))]),
        bytes(0x60, 12),
        bytes(0x14, 32),
        bytes(0x15, 32),
        bytes(0x16, 32),
        Some(bytes(0x17, 32)),
    )
}

fn protocol_component(
    id: &str,
    protocol_system: &str,
    protocol_type_name: &str,
) -> ProtocolComponent {
    ProtocolComponent {
        id: id.to_string(),
        protocol_system: protocol_system.to_string(),
        protocol_type_name: protocol_type_name.to_string(),
        chain: DtoChain::Ethereum,
        tokens: vec![bytes(0xb1, 20), bytes(0xb2, 20)],
        contract_ids: vec![bytes(0xa1, 20)],
        static_attributes: HashMap::from([("fee".to_string(), bytes(0x1e, 3))]),
        change: ChangeType::Creation,
        creation_tx: bytes(0xc0, 32),
        created_at: NaiveDateTime::default(),
    }
}

fn bebop_state() -> Result<BebopState> {
    let base_token = token(0xb1, "WETH", 18);
    let quote_token = token(0xb2, "USDC", 6);
    let base_address = base_token.address.to_vec();
    let quote_address = quote_token.address.to_vec();
    let client = BebopClient::new(
        Chain::Ethereum,
        HashSet::from([base_token.address.clone()]),
        100.0,
        "fixture-key".to_string(),
        HashSet::from([quote_token.address.clone()]),
        Duration::from_secs(5),
        None,
        None,
        None,
    )?;
    Ok(serde_json::from_value(serde_json::json!({
        "base_token": base_token,
        "quote_token": quote_token,
        "price_data": {
            "base": base_address,
            "quote": quote_address,
            "last_update_ts": 1_710_000_000_u64,
            "bids": [2_000.0, 10.0],
            "asks": [2_001.0, 11.0],
        },
        "client": client,
    }))?)
}

fn token(seed: u8, symbol: &str, decimals: u32) -> Token {
    Token::new(
        &bytes(seed, 20),
        symbol,
        decimals,
        0,
        &[Some(50_000)],
        Chain::Ethereum,
        100,
    )
}

fn block_header(number: u64, seed: u8) -> BlockHeader {
    BlockHeader {
        hash: bytes(seed, 32),
        number,
        parent_hash: bytes(seed.saturating_add(1), 32),
        revert: false,
        timestamp: 1_710_000_000 + number,
        partial_block_index: None,
    }
}

fn bytes(seed: u8, length: usize) -> Bytes {
    Bytes::from(vec![seed; length])
}
