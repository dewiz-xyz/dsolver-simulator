use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    env,
    process::ExitCode,
    sync::Arc,
    time::Duration,
};

use anyhow::{ensure, Context};
use aws_sdk_s3::config::Region;
use runtime::{
    broadcaster::{state::BroadcasterSnapshotCache, state_history::captured_state_from_export},
    models::tokens::TokenStore,
};
use simulator_core::broadcaster::{BroadcasterBackend, BroadcasterTokenDto};
use state_history::{
    encode_archive, ArchiveMetadata, Backend, BacktestRequest, BlockTimeObservation, CapturedState,
    CheckpointArchive, CheckpointCapture, CheckpointKind, CheckpointManifest,
    CheckpointObjectStore, DeltaBackendCursor, DeltaEntry, GapReason, GapRecord, NoBaseFee,
    RangeGap, RangeGapKind, RangePlan, RangeRequest, StateHistoryReader, StateHistoryStore,
    StateHistoryWriter, StreamPosition, TokenSnapshotRef, WriterConfig, WriterHandle,
};
use tycho_simulation::{
    tycho_client::feed::{
        synchronizer::{ComponentWithState, Snapshot, StateSyncMessage},
        BlockHeader, FeedMessage, SynchronizerState,
    },
    tycho_common::{
        models::{
            blockchain::BlockAggregatedChanges,
            protocol::{ProtocolComponent, ProtocolComponentState},
            token::Token,
            Chain,
        },
        Bytes,
    },
};

const CHAIN_ID: u64 = 8_453;
const DEFAULT_DATABASE_URL: &str = "postgres://postgres:postgres@127.0.0.1:55432/state_history";
const DEFAULT_S3_BUCKET: &str = "state-history-analysis";
const DEFAULT_S3_PREFIX: &str = "smoke";
const DEFAULT_S3_REGION: &str = "eu-central-1";
const DEFAULT_S3_ENDPOINT_URL: &str = "http://127.0.0.1:59000";

#[derive(Clone, Copy)]
struct TokenObjectStorage<'a> {
    bucket: &'a str,
    prefix: &'a str,
    region: &'a str,
    endpoint: &'a str,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => {
            println!("PASS state-history-analysis complete");
            ExitCode::SUCCESS
        }
        Err(error) => {
            println!("FAIL state-history-analysis: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> anyhow::Result<()> {
    let database_url = env_or_default("DATABASE_URL", DEFAULT_DATABASE_URL);
    let bucket = env_or_default("STATE_HISTORY_S3_BUCKET", DEFAULT_S3_BUCKET);
    let prefix = env_or_default("STATE_HISTORY_S3_PREFIX", DEFAULT_S3_PREFIX);
    let region = env_or_default("STATE_HISTORY_S3_REGION", DEFAULT_S3_REGION);
    let endpoint = env_or_default("STATE_HISTORY_S3_ENDPOINT_URL", DEFAULT_S3_ENDPOINT_URL);

    let store = StateHistoryStore::connect(&database_url).await?;
    store.run_migrations().await?;
    store.validate_schema().await?;
    println!("PASS migrations and schema validation");

    let seed_objects = object_store(&bucket, &prefix, &region, &endpoint).await;
    let first_checkpoint = persist_checkpoint(
        &store,
        &seed_objects,
        capture(1, 10, 100, CheckpointKind::Interval, "generation-1")?,
    )
    .await?;
    seed_deltas(&store, 1, 11, 40, 100, 110).await?;

    let second_checkpoint = persist_checkpoint(
        &store,
        &seed_objects,
        capture(2, 1, 110, CheckpointKind::Boundary, "generation-2")?,
    )
    .await?;
    seed_deltas(&store, 2, 2, 20, 110, 118).await?;
    record_gap(&store, 2, 8, 10).await?;
    println!("PASS seeded two generations and queue_overflow gap 2:8..=10");

    let pool = store.pool().clone();
    let reader = StateHistoryReader::new(
        pool.clone(),
        object_store(&bucket, &prefix, &region, &endpoint).await,
    );

    let short_plan = reader.resolve_range(&range_request(100, 105)?).await?;
    assert_plan(&short_plan, &[(position(1, 10), positions(1, 11, 28))], &[])?;
    short_plan.ensure_gap_free()?;
    println!("PASS resolve_range 100..=105 returned one gap-free leg");
    ensure!(
        short_plan.legs[0].checkpoint.token_reference.is_none() && short_plan.gaps.is_empty(),
        "token-less anchor must resolve with None and no gap"
    );
    println!("PASS token-less anchor resolved with None and no gap");

    let explicit_request = range_request(100, 115)?;
    let explicit_plan = reader.resolve_range(&explicit_request).await?;
    let recorded_gap = expected_recorded_gap();
    let replay_legs = [
        (position(1, 10), positions(1, 11, 40)),
        (position(2, 1), positions(2, 2, 15)),
    ];
    assert_plan(
        &explicit_plan,
        &replay_legs,
        std::slice::from_ref(&recorded_gap),
    )?;
    println!("PASS resolve_range 100..=115 returned two ordered legs and the recorded gap");

    let backtest = reader
        .resolve_backtest_range(&BacktestRequest::new(CHAIN_ID, 100, 115, all_backends())?)
        .await?;
    ensure!(
        backtest.start_block_timestamp_ms == block_timestamp_ms(100)
            && backtest.end_block_timestamp_ms == block_timestamp_ms(115)
            && backtest.rfq_bounds == Some(rfq_bounds(100, 115)),
        "backtest timestamp bounds did not match the seeded block_times rows"
    );
    assert_plan(
        &backtest.range,
        &replay_legs,
        std::slice::from_ref(&recorded_gap),
    )?;
    println!("PASS resolve_backtest_range matched the explicit RFQ-bounds plan");

    assert_checkpoint_round_trip(
        &reader,
        &explicit_plan.legs[0].checkpoint,
        &first_checkpoint,
    )
    .await?;
    assert_checkpoint_round_trip(
        &reader,
        &explicit_plan.legs[1].checkpoint,
        &second_checkpoint,
    )
    .await?;
    println!("PASS fetch_checkpoint round-tripped both hash-verified archives");

    StateHistoryStore::fail_checkpoint(&pool, second_checkpoint.id, "injected boundary failure")
        .await?;
    let failed_plan = reader.resolve_range(&explicit_request).await?;
    assert_failed_boundary(&failed_plan, recorded_gap)?;
    println!("PASS failed gen-2 boundary replaced replay leg 2 with MissingCheckpoint");

    run_token_snapshot_flow(&database_url, &bucket, &prefix, &region, &endpoint, &reader).await?;

    Ok(())
}

async fn run_token_snapshot_flow(
    database_url: &str,
    bucket: &str,
    prefix: &str,
    region: &str,
    endpoint: &str,
    reader: &StateHistoryReader,
) -> anyhow::Result<()> {
    let token_objects = TokenObjectStorage {
        bucket,
        prefix,
        region,
        endpoint,
    };
    let initial_tokens = vec![test_token(0x11, "INITIAL-A"), test_token(0x22, "INITIAL-B")];
    let stream_token = test_token(0x33, "STREAM");
    let token_store = Arc::new(TokenStore::local_only(
        initial_tokens
            .iter()
            .cloned()
            .map(|token| (token.address.clone(), token))
            .collect(),
        Chain::Base,
        Duration::from_secs(1),
    ));
    let cache = BroadcasterSnapshotCache::with_token_catalog(
        CHAIN_ID,
        vec![BroadcasterBackend::Native],
        Arc::clone(&token_store),
        0,
    );
    ensure!(
        cache
            .apply_feed_message(&initial_component_feed())
            .await?
            .is_some(),
        "initial broadcaster feed was not applied"
    );

    let writer = StateHistoryWriter::spawn(
        WriterConfig::default(),
        StateHistoryStore::connect(database_url).await?,
        object_store(bucket, prefix, region, endpoint).await,
        Arc::new(NoBaseFee),
    );
    let expected_initial_catalog = canonical_token_json(&initial_tokens)?;

    let first_manifest = write_checkpoint_and_resolve(
        &writer,
        checkpoint_capture(&cache, &token_store, position(3, 1), 119, prefix).await?,
        reader,
        1,
        119,
    )
    .await?;
    ensure!(
        cache
            .apply_feed_message(&unchanged_catalog_feed())
            .await?
            .is_some(),
        "unchanged-catalog broadcaster feed was not applied"
    );
    let second_manifest = write_checkpoint_and_resolve(
        &writer,
        checkpoint_capture(&cache, &token_store, position(3, 2), 120, prefix).await?,
        reader,
        2,
        120,
    )
    .await?;
    assert_unchanged_catalog(
        reader,
        &first_manifest,
        &second_manifest,
        &expected_initial_catalog,
        token_objects,
    )
    .await?;
    let first_reference = required_token_reference(&first_manifest)?;

    ensure!(
        cache
            .apply_feed_message(&catalog_change_feed(stream_token.clone()))
            .await?
            .is_some(),
        "mid-stream broadcaster feed was not applied"
    );
    let third_manifest = write_checkpoint_and_resolve(
        &writer,
        checkpoint_capture(&cache, &token_store, position(3, 3), 121, prefix).await?,
        reader,
        3,
        121,
    )
    .await?;
    let third_reference = required_token_reference(&third_manifest)?;
    assert_catalog_change(reader, first_reference, third_reference, token_objects).await?;

    assert_token_snapshot_superset(reader, &third_manifest, &stream_token).await?;

    writer.shutdown(Duration::from_secs(5)).await;
    Ok(())
}

async fn assert_unchanged_catalog(
    reader: &StateHistoryReader,
    first_manifest: &CheckpointManifest,
    second_manifest: &CheckpointManifest,
    expected_catalog: &serde_json::Value,
    token_objects: TokenObjectStorage<'_>,
) -> anyhow::Result<()> {
    let first_reference = required_token_reference(first_manifest)?;
    let second_reference = required_token_reference(second_manifest)?;
    ensure!(
        first_reference == second_reference,
        "unchanged token catalog produced different token references"
    );
    let dedup_keys = list_token_object_keys(token_objects).await?;
    ensure!(
        dedup_keys == vec![first_reference.s3_key.clone()],
        "unchanged token catalog should leave exactly one MinIO token object, got {dedup_keys:?}"
    );
    println!("PASS unchanged token catalog reused one token reference and one MinIO object");

    let fetched_catalog = fetch_token_catalog(reader, first_manifest).await?;
    ensure!(
        fetched_catalog == *expected_catalog,
        "reader token catalog differs from the exact seeded catalog"
    );
    println!("PASS token snapshot reader returned the exact seeded catalog");
    Ok(())
}

async fn assert_catalog_change(
    reader: &StateHistoryReader,
    first_reference: &TokenSnapshotRef,
    third_reference: &TokenSnapshotRef,
    token_objects: TokenObjectStorage<'_>,
) -> anyhow::Result<()> {
    let refreshed_first = resolve_single_native_anchor(reader, 119).await?;
    ensure!(
        required_token_reference(&refreshed_first)? == first_reference
            && first_reference.sha256 != third_reference.sha256,
        "catalog change did not preserve the earlier reference and create a new hash"
    );
    let changed_keys = list_token_object_keys(token_objects).await?;
    let expected_keys = BTreeSet::from([
        first_reference.s3_key.clone(),
        third_reference.s3_key.clone(),
    ]);
    ensure!(
        changed_keys.into_iter().collect::<BTreeSet<_>>() == expected_keys,
        "catalog change should leave both MinIO token objects"
    );
    println!("PASS catalog change preserved the earlier token reference and both MinIO objects");
    Ok(())
}

async fn assert_token_snapshot_superset(
    reader: &StateHistoryReader,
    manifest: &CheckpointManifest,
    stream_token: &Token,
) -> anyhow::Result<()> {
    let checkpoint = reader.fetch_checkpoint(manifest).await?;
    let component_addresses = component_token_addresses(&checkpoint)?;
    let fetched_catalog = fetch_token_catalog(reader, manifest).await?;
    let catalog_addresses = token_catalog_addresses(&fetched_catalog)?;
    ensure!(
        component_addresses.contains(&stream_token.address.to_string()),
        "later checkpoint does not reference the mid-stream token"
    );
    ensure!(
        component_addresses.is_subset(&catalog_addresses),
        "checkpoint component token references {component_addresses:?} are not a subset of the token snapshot {catalog_addresses:?}"
    );
    println!(
        "PASS real broadcaster apply -> writer -> PostgreSQL/MinIO -> reader preserved the token snapshot superset"
    );
    Ok(())
}

async fn checkpoint_capture(
    cache: &BroadcasterSnapshotCache,
    token_store: &TokenStore,
    capture_position: StreamPosition,
    block_number: u64,
    prefix: &str,
) -> anyhow::Result<CheckpointCapture> {
    let export = cache.export_snapshot(8_388_608).await?;
    let state = captured_state_from_export(
        CheckpointKind::Interval,
        &export,
        capture_position,
        3,
        block_number,
        None,
    )?;
    let tokens = token_store
        .snapshot()
        .await
        .into_values()
        .map(BroadcasterTokenDto::from)
        .collect();
    Ok(CheckpointCapture::new(state, tokens, prefix.to_owned()))
}

async fn write_checkpoint_and_resolve(
    writer: &WriterHandle,
    capture: CheckpointCapture,
    reader: &StateHistoryReader,
    expected_completed: u64,
    block_number: u64,
) -> anyhow::Result<CheckpointManifest> {
    ensure!(
        writer.request_checkpoint(capture),
        "writer rejected checkpoint {expected_completed}"
    );
    wait_for_checkpoints(writer, expected_completed).await?;
    resolve_single_native_anchor(reader, block_number).await
}

async fn wait_for_checkpoints(
    writer: &WriterHandle,
    expected_completed: u64,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let status = writer.status();
        if status.checkpoints_completed == expected_completed {
            return Ok(());
        }
        ensure!(
            status.checkpoints_failed == 0,
            "writer failed while waiting for checkpoint {expected_completed}: {status:?}"
        );
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("writer did not complete checkpoint {expected_completed}: {status:?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn resolve_single_native_anchor(
    reader: &StateHistoryReader,
    block_number: u64,
) -> anyhow::Result<CheckpointManifest> {
    let plan = reader
        .resolve_range(&RangeRequest::new(
            CHAIN_ID,
            block_number,
            block_number,
            vec![Backend::Native],
        )?)
        .await?;
    ensure!(
        plan.legs.len() == 1 && plan.gaps.is_empty(),
        "expected one gap-free native checkpoint at block {block_number}, got {plan:?}"
    );
    Ok(plan.legs[0].checkpoint.clone())
}

fn required_token_reference(manifest: &CheckpointManifest) -> anyhow::Result<&TokenSnapshotRef> {
    manifest
        .token_reference
        .as_ref()
        .context("completed checkpoint is missing its token reference")
}

async fn fetch_token_catalog(
    reader: &StateHistoryReader,
    manifest: &CheckpointManifest,
) -> anyhow::Result<serde_json::Value> {
    let reference = required_token_reference(manifest)?;
    let snapshot = reader
        .fetch_checkpoint_token_snapshot(manifest)
        .await?
        .context("completed checkpoint is missing its token snapshot")?;
    ensure!(
        snapshot.chain_id == CHAIN_ID
            && snapshot.token_count == reference.token_count
            && snapshot.token_count
                == u64::try_from(
                    serde_json::from_str::<Vec<serde_json::Value>>(snapshot.tokens.get())?.len()
                )?,
        "fetched token snapshot metadata differs from its token reference"
    );
    serde_json::from_str(snapshot.tokens.get()).context("failed to parse token snapshot catalog")
}

// `CheckpointObjectStore` never lists (production only puts and fetches), so the dedup
// assertion enumerates token keys with its own S3 client instead of widening that API.
async fn list_token_object_keys(
    token_objects: TokenObjectStorage<'_>,
) -> anyhow::Result<Vec<String>> {
    let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(token_objects.region.to_owned()))
        .load()
        .await;
    let config = aws_sdk_s3::config::Builder::from(&shared_config)
        .endpoint_url(token_objects.endpoint)
        .force_path_style(true)
        .build();
    let output = aws_sdk_s3::Client::from_conf(config)
        .list_objects_v2()
        .bucket(token_objects.bucket)
        .prefix(token_object_prefix(token_objects.prefix))
        .send()
        .await
        .context("failed to list MinIO token objects")?;
    let mut keys = output
        .contents()
        .iter()
        .filter_map(|object| object.key().map(str::to_owned))
        .collect::<Vec<_>>();
    keys.sort();
    Ok(keys)
}

fn token_object_prefix(prefix: &str) -> String {
    let suffix = format!("chain={CHAIN_ID}/tokens/");
    if prefix.is_empty() {
        suffix
    } else {
        format!("{prefix}/{suffix}")
    }
}

fn canonical_token_json(tokens: &[Token]) -> anyhow::Result<serde_json::Value> {
    let mut tokens = tokens
        .iter()
        .cloned()
        .map(BroadcasterTokenDto::from)
        .collect::<Vec<_>>();
    tokens.sort_by(|left, right| left.address.cmp(&right.address));
    serde_json::to_value(tokens).context("failed to serialize expected token catalog")
}

fn component_token_addresses(checkpoint: &CheckpointArchive) -> anyhow::Result<BTreeSet<String>> {
    let mut addresses = BTreeSet::new();
    for payload in &checkpoint.payloads_json {
        let value: serde_json::Value =
            serde_json::from_str(payload).context("failed to parse checkpoint payload")?;
        collect_component_token_addresses(&value, &mut addresses)?;
    }
    ensure!(
        !addresses.is_empty(),
        "checkpoint contains no component token references"
    );
    Ok(addresses)
}

fn collect_component_token_addresses(
    value: &serde_json::Value,
    addresses: &mut BTreeSet<String>,
) -> anyhow::Result<()> {
    match value {
        serde_json::Value::Object(object) => {
            // The spec pins the superset check to JSON level: walk the raw payloads for
            // component objects instead of decoding them with Tycho types. Payloads are our
            // own snake_case serialization, so `protocol_system` marks a component.
            if object.contains_key("protocol_system") {
                if let Some(tokens) = object.get("tokens").and_then(serde_json::Value::as_array) {
                    for token in tokens {
                        let address = token
                            .as_str()
                            .context("component token reference must be a JSON string")?;
                        addresses.insert(address.to_owned());
                    }
                }
            }
            for child in object.values() {
                collect_component_token_addresses(child, addresses)?;
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                collect_component_token_addresses(child, addresses)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn token_catalog_addresses(tokens: &serde_json::Value) -> anyhow::Result<BTreeSet<String>> {
    tokens
        .as_array()
        .context("token catalog must be a JSON array")?
        .iter()
        .map(|token| {
            token
                .get("address")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .context("token catalog entry is missing its address")
        })
        .collect()
}

fn initial_component_feed() -> FeedMessage<BlockHeader> {
    let header = block_header(119, 0x6f, 0x6e);
    let component = protocol_component(
        "initial-pool",
        vec![token_address(0x11), token_address(0x22)],
    );
    FeedMessage {
        state_msgs: HashMap::from([(
            "uniswap_v2".to_owned(),
            StateSyncMessage {
                header: header.clone(),
                snapshots: Snapshot {
                    states: HashMap::from([(
                        component.id.clone(),
                        ComponentWithState {
                            state: ProtocolComponentState {
                                component_id: component.id.clone(),
                                attributes: HashMap::new(),
                                balances: HashMap::new(),
                            },
                            component,
                            component_tvl: Some(1.0),
                            entrypoints: Vec::new(),
                        },
                    )]),
                    vm_storage: HashMap::new(),
                },
                deltas: None,
                removed_components: HashMap::new(),
            },
        )]),
        sync_states: HashMap::from([("uniswap_v2".to_owned(), SynchronizerState::Ready(header))]),
    }
}

fn unchanged_catalog_feed() -> FeedMessage<BlockHeader> {
    let header = block_header(120, 0x70, 0x6f);
    FeedMessage {
        state_msgs: HashMap::from([(
            "uniswap_v2".to_owned(),
            StateSyncMessage {
                header: header.clone(),
                snapshots: Snapshot::default(),
                deltas: None,
                removed_components: HashMap::new(),
            },
        )]),
        sync_states: HashMap::from([("uniswap_v2".to_owned(), SynchronizerState::Ready(header))]),
    }
}

fn catalog_change_feed(stream_token: Token) -> FeedMessage<BlockHeader> {
    let header = block_header(121, 0x71, 0x70);
    let mut changes = BlockAggregatedChanges::default();
    changes
        .new_tokens
        .insert(stream_token.address.clone(), stream_token);
    let component = protocol_component(
        "stream-pool",
        vec![token_address(0x22), token_address(0x33)],
    );
    changes
        .new_protocol_components
        .insert(component.id.clone(), component);
    FeedMessage {
        state_msgs: HashMap::from([(
            "uniswap_v2".to_owned(),
            StateSyncMessage {
                header: header.clone(),
                snapshots: Snapshot::default(),
                deltas: Some(changes),
                removed_components: HashMap::new(),
            },
        )]),
        sync_states: HashMap::from([("uniswap_v2".to_owned(), SynchronizerState::Ready(header))]),
    }
}

fn protocol_component(component_id: &str, tokens: Vec<Bytes>) -> ProtocolComponent {
    ProtocolComponent {
        id: component_id.to_owned(),
        protocol_system: "uniswap_v2".to_owned(),
        protocol_type_name: "uniswap_v2".to_owned(),
        chain: Chain::Base,
        tokens,
        contract_addresses: Vec::new(),
        static_attributes: HashMap::new(),
        change: Default::default(),
        creation_tx: Bytes::from([0x44; 32]),
        created_at: Default::default(),
    }
}

fn test_token(seed: u8, symbol: &str) -> Token {
    Token::new(
        &token_address(seed),
        symbol,
        18,
        0,
        &[Some(21_000)],
        Chain::Base,
        100,
    )
}

fn token_address(seed: u8) -> Bytes {
    Bytes::from([seed; 20])
}

fn block_header(number: u64, hash_seed: u8, parent_seed: u8) -> BlockHeader {
    BlockHeader {
        hash: Bytes::from([hash_seed; 32]),
        number,
        parent_hash: Bytes::from([parent_seed; 32]),
        revert: false,
        timestamp: number * 10,
        partial_block_index: None,
    }
}

fn assert_failed_boundary(failed_plan: &RangePlan, recorded_gap: RangeGap) -> anyhow::Result<()> {
    let missing_checkpoint = RangeGap {
        kind: RangeGapKind::MissingCheckpoint,
        generation: Some(2),
        from_message_seq: Some(1),
        to_message_seq: Some(15),
        from_position: Some(position(2, 1)),
        to_position: Some(position(2, 15)),
        from_block: None,
        to_block_inclusive: None,
        from_observed_at_ms: None,
        to_observed_at_ms: None,
        reason: "no complete boundary checkpoint at generation 2 message sequence 1".to_owned(),
    };
    assert_plan(
        failed_plan,
        &[(position(1, 10), positions(1, 11, 40))],
        &[missing_checkpoint, recorded_gap],
    )
}

fn expected_recorded_gap() -> RangeGap {
    RangeGap {
        kind: RangeGapKind::Recorded,
        generation: Some(2),
        from_message_seq: Some(8),
        to_message_seq: Some(10),
        from_position: Some(position(2, 8)),
        to_position: Some(position(2, 10)),
        from_block: Some(112),
        to_block_inclusive: Some(113),
        from_observed_at_ms: Some(block_timestamp_ms(113) - 1),
        to_observed_at_ms: Some(block_timestamp_ms(114) - 1),
        reason: "queue_overflow".to_owned(),
    }
}

struct SeededCheckpoint {
    id: i64,
    archive: CheckpointArchive,
    sha256: String,
}

async fn persist_checkpoint(
    store: &StateHistoryStore,
    objects: &CheckpointObjectStore,
    capture: CapturedState,
) -> anyhow::Result<SeededCheckpoint> {
    let mut connection = store.pool().acquire().await?;
    let id = StateHistoryStore::create_checkpoint(&mut connection, &capture).await?;
    let archive = CheckpointArchive {
        metadata: ArchiveMetadata {
            schema_version: 1,
            chain_id: capture.chain_id(),
            position: capture.position(),
            state_version: capture.state_version(),
            kind: capture.kind(),
            block_number: capture.block_number(),
            rfq_observed_at_ms: capture.rfq_observed_at_ms(),
            backends: capture.backends().to_vec(),
        },
        payloads_json: capture.payloads_json().to_vec(),
    };
    let encoded = encode_archive(archive.clone())?;
    let key = objects.key_for(capture.chain_id(), capture.position(), capture.kind());
    objects.put(&key, encoded.bytes).await?;
    StateHistoryStore::complete_checkpoint(
        &mut connection,
        id,
        &key,
        &encoded.info,
        None,
        capture.block_times(),
        capture.position(),
        capture.chain_id(),
    )
    .await?;

    Ok(SeededCheckpoint {
        id,
        archive,
        sha256: encoded.info.sha256,
    })
}

async fn seed_deltas(
    store: &StateHistoryStore,
    generation: u64,
    first_seq: u64,
    last_seq: u64,
    first_block: u64,
    last_block: u64,
) -> anyhow::Result<()> {
    let mut connection = store.pool().acquire().await?;
    for message_seq in first_seq..=last_seq {
        let block_number = first_block
            + (message_seq - first_seq) * (last_block - first_block) / (last_seq - first_seq);
        let rfq_observed_at_ms = block_timestamp_ms(block_number + 1) - 1;
        let delta = DeltaEntry::new(
            CHAIN_ID,
            position(generation, message_seq),
            rfq_observed_at_ms,
            format!(
                r#"{{"generation":{generation},"message_seq":{message_seq},"block":{block_number}}}"#
            ),
            vec![
                DeltaBackendCursor {
                    backend: Backend::Native,
                    block_number: Some(block_number),
                    observed_at_ms: None,
                },
                DeltaBackendCursor {
                    backend: Backend::Vm,
                    block_number: Some(block_number),
                    observed_at_ms: None,
                },
                DeltaBackendCursor {
                    backend: Backend::Rfq,
                    block_number: None,
                    observed_at_ms: Some(rfq_observed_at_ms),
                },
            ],
            vec![block_time(block_number)],
        )?;
        StateHistoryStore::insert_delta(&mut connection, &delta, &BTreeMap::new())
            .await
            .with_context(|| {
                format!("failed to seed delta at generation {generation} sequence {message_seq}")
            })?;
    }
    Ok(())
}

async fn record_gap(
    store: &StateHistoryStore,
    generation: u64,
    from_message_seq: u64,
    to_message_seq: u64,
) -> anyhow::Result<()> {
    let mut connection = store.pool().acquire().await?;
    StateHistoryStore::record_gap(
        &mut connection,
        &GapRecord {
            chain_id: CHAIN_ID,
            generation,
            from_message_seq,
            to_message_seq,
            reason: GapReason::QueueOverflow,
            from_block_number: Some(112),
            to_block_number: Some(113),
            from_observed_at_ms: Some(block_timestamp_ms(113) - 1),
            to_observed_at_ms: Some(block_timestamp_ms(114) - 1),
        },
    )
    .await
}

fn assert_plan(
    plan: &RangePlan,
    expected_legs: &[(StreamPosition, Vec<StreamPosition>)],
    expected_gaps: &[RangeGap],
) -> anyhow::Result<()> {
    ensure!(
        plan.legs.len() == expected_legs.len(),
        "expected {} legs, got {}",
        expected_legs.len(),
        plan.legs.len()
    );
    for (leg, (checkpoint, deltas)) in plan.legs.iter().zip(expected_legs) {
        ensure!(
            leg.checkpoint.position == *checkpoint,
            "expected checkpoint {checkpoint:?}, got {:?}",
            leg.checkpoint.position
        );
        let actual = leg
            .deltas
            .iter()
            .map(|delta| delta.position)
            .collect::<Vec<_>>();
        ensure!(
            actual == *deltas,
            "unexpected replay positions after checkpoint {checkpoint:?}: {actual:?}"
        );
    }
    ensure!(
        plan.gaps == expected_gaps,
        "expected gaps {expected_gaps:?}, got {:?}",
        plan.gaps
    );
    Ok(())
}

async fn assert_checkpoint_round_trip(
    reader: &StateHistoryReader,
    manifest: &CheckpointManifest,
    seeded: &SeededCheckpoint,
) -> anyhow::Result<()> {
    ensure!(
        manifest.id == seeded.id
            && manifest.archive_sha256.as_deref() == Some(seeded.sha256.as_str()),
        "checkpoint manifest hash differs from encoded archive hash"
    );
    let fetched = reader.fetch_checkpoint(manifest).await?;
    ensure!(
        fetched == seeded.archive,
        "fetched checkpoint archive differs from its seeded value"
    );
    Ok(())
}

fn capture(
    generation: u64,
    message_seq: u64,
    block_number: u64,
    kind: CheckpointKind,
    label: &str,
) -> anyhow::Result<CapturedState> {
    CapturedState::new(
        CHAIN_ID,
        position(generation, message_seq),
        Some(generation),
        kind,
        block_number,
        Some(block_timestamp_ms(block_number)),
        all_backends(),
        vec![format!(r#"{{"state":"{label}"}}"#)],
        vec![],
    )
    .map_err(Into::into)
}

fn range_request(start_block: u64, end_block: u64) -> anyhow::Result<RangeRequest> {
    let (start_ms, end_ms) = rfq_bounds(start_block, end_block);
    Ok(
        RangeRequest::new(CHAIN_ID, start_block, end_block, all_backends())?
            .with_rfq_bounds(start_ms, end_ms)?,
    )
}

fn all_backends() -> Vec<Backend> {
    vec![Backend::Native, Backend::Vm, Backend::Rfq]
}

fn positions(generation: u64, first_seq: u64, last_seq: u64) -> Vec<StreamPosition> {
    (first_seq..=last_seq)
        .map(|message_seq| position(generation, message_seq))
        .collect()
}

const fn position(generation: u64, message_seq: u64) -> StreamPosition {
    StreamPosition {
        generation,
        message_seq,
    }
}

fn block_time(block_number: u64) -> BlockTimeObservation {
    BlockTimeObservation {
        block_number,
        timestamp_ms: block_timestamp_ms(block_number),
        block_hash: Some(format!("block-{block_number}")),
        parent_hash: Some(format!("block-{}", block_number - 1)),
    }
}

const fn rfq_bounds(start_block: u64, end_block: u64) -> (u64, u64) {
    (
        block_timestamp_ms(start_block),
        block_timestamp_ms(end_block + 1) - 1,
    )
}

const fn block_timestamp_ms(block_number: u64) -> u64 {
    block_number * 1_000
}

fn env_or_default(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

async fn object_store(
    bucket: &str,
    prefix: &str,
    region: &str,
    endpoint: &str,
) -> CheckpointObjectStore {
    CheckpointObjectStore::from_env_config(
        bucket.to_owned(),
        prefix.to_owned(),
        region.to_owned(),
        Some(endpoint.to_owned()),
        true,
    )
    .await
}
