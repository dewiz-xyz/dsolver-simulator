use std::{
    collections::{BTreeSet, HashMap},
    env,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use broadcaster_replay_client::{
    BroadcasterReplayClient, BroadcasterReplayConfig, ReplayBatchItem, ReplayCheckpoint, ReplayPoll,
};
use futures::StreamExt;
use num_bigint::BigUint;
use num_traits::Zero;
use serde_json::Value;
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterEnvelope, BroadcasterPayload, BroadcasterProtocolMessage,
    BroadcasterSnapshotPartition, BroadcasterTokenSnapshotResponse, BroadcasterUpdatePartition,
};
use tokio::{sync::RwLock, time::Instant};
use tycho_simulation::{
    evm::decoder::TychoStreamDecoder,
    protocol::models::{ProtocolComponent, Update},
    tycho_client::feed::{BlockHeader, FeedMessage},
    tycho_common::{
        models::{token::Token, Chain},
        simulation::protocol_sim::ProtocolSim,
        Bytes,
    },
};

use super::processor::{BroadcasterSubscriptionProcessor, PreparedRedisProcessor};
use super::snapshot::RawSnapshotReassembly;
use super::{
    apply_replay_batch, mark_redis_catch_up_checkpoints, BroadcasterSubscriptionControls,
    PreparedBroadcasterRedisSubscription, VmBroadcasterSubscriptionControls,
};
use crate::{
    models::{
        state::{BroadcasterSubscriptionStatus, StateStore, VmStreamStatus},
        stream_health::StreamHealth,
        tokens::{
            derive_broadcaster_token_lookup_url, derive_broadcaster_token_snapshot_url, TokenStore,
        },
    },
    services::stream_builder::build_broadcaster_subscription_decoder,
};

const LIVE_E2E_GATE: &str = "DSOLVER_LIVE_MAINNET_VM_E2E";
const BROADCASTER_URL_ENV: &str = "TYCHO_BROADCASTER_URL";
const REDIS_URL_ENV: &str = "BROADCASTER_REDIS_URL";
const EXPECTED_CHAIN_ID: u64 = 1;
const CURVE_PROTOCOL: &str = "vm:curve";
const BALANCER_PROTOCOL: &str = "vm:balancer_v2";
const VM_PROTOCOLS: [&str; 3] = [CURVE_PROTOCOL, BALANCER_PROTOCOL, "vm:maverick_v2"];
const REDIS_BLOCK_MS: u64 = 5_000;
const REDIS_READ_COUNT: u64 = 256;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const LIVE_DELTA_TIMEOUT: Duration = Duration::from_secs(20 * 60);
const LIVE_PROGRESS_INTERVAL: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live mainnet broadcaster and Redis e2e"]
async fn mainnet_vm_liquidity_survives_snapshot_and_redis_delta_wire() -> Result<()> {
    if !live_e2e_enabled() {
        println!("skipping live VM wire e2e; set {LIVE_E2E_GATE}=1 to run it");
        return Ok(());
    }

    let config = LiveConfig::from_env()?;
    require_broadcaster_ready(&config.broadcaster_url).await?;

    let replay_client = BroadcasterReplayClient::connect(BroadcasterReplayConfig {
        broadcaster_url: config.broadcaster_url.clone(),
        redis_url: config.redis_url,
        block_ms: REDIS_BLOCK_MS,
        read_count: REDIS_READ_COUNT,
        request_timeout: REQUEST_TIMEOUT,
    })
    .await
    .context("failed to connect broadcaster replay client")?;

    let (controls, state_store, protocols) = build_vm_controls(&config.broadcaster_url).await?;
    let decoder = build_broadcaster_subscription_decoder(
        controls.tokens(),
        BroadcasterBackend::Vm,
        &protocols,
    )
    .await
    .context("failed to build real VM broadcaster decoder")?;
    let evidence_decoder = build_broadcaster_subscription_decoder(
        controls.tokens(),
        BroadcasterBackend::Vm,
        &protocols,
    )
    .await
    .context("failed to build VM evidence decoder")?;
    let mut processor =
        BroadcasterSubscriptionProcessor::with_decoder(EXPECTED_CHAIN_ID, controls, decoder, None);

    let (snapshot_evidence, replay_boundary) = bootstrap_snapshot(
        &replay_client,
        &mut processor,
        &evidence_decoder,
        &state_store,
    )
    .await?;
    snapshot_evidence.require_snapshot_material()?;
    require_non_zero_quote(
        "snapshot",
        CURVE_PROTOCOL,
        snapshot_evidence.pool_ids(CURVE_PROTOCOL),
        &state_store,
    )
    .await?;
    require_non_zero_quote(
        "snapshot",
        BALANCER_PROTOCOL,
        snapshot_evidence.pool_ids(BALANCER_PROTOCOL),
        &state_store,
    )
    .await?;

    let mut prepared = PreparedBroadcasterRedisSubscription {
        processors: vec![PreparedRedisProcessor {
            index: 0,
            processor,
            replay_boundary: replay_boundary.clone(),
        }],
        replay_boundary,
        expected_chain_id: EXPECTED_CHAIN_ID,
    };

    poll_live_redis_until_quotes(
        &replay_client,
        &mut prepared,
        &evidence_decoder,
        &state_store,
    )
    .await
}

struct LiveConfig {
    broadcaster_url: String,
    redis_url: String,
}

impl LiveConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            broadcaster_url: required_env(BROADCASTER_URL_ENV)?,
            redis_url: required_env(REDIS_URL_ENV)?,
        })
    }
}

fn live_e2e_enabled() -> bool {
    env::var(LIVE_E2E_GATE).as_deref() == Ok("1")
}

fn required_env(key: &str) -> Result<String> {
    let value = env::var(key).with_context(|| format!("{key} must be set for live VM e2e"))?;
    if value.trim().is_empty() {
        bail!("{key} must not be empty for live VM e2e");
    }
    Ok(value)
}

async fn build_vm_controls(
    broadcaster_url: &str,
) -> Result<(
    BroadcasterSubscriptionControls,
    Arc<StateStore>,
    Vec<String>,
)> {
    let lookup_url = derive_broadcaster_token_lookup_url(broadcaster_url)
        .context("failed to derive broadcaster token lookup URL")?;
    let initial_tokens = load_broadcaster_token_snapshot(broadcaster_url).await?;
    let tokens = Arc::new(TokenStore::broadcaster_backed(
        initial_tokens,
        lookup_url,
        Chain::Ethereum,
        REQUEST_TIMEOUT,
    ));
    let state_store = Arc::new(StateStore::new(Arc::clone(&tokens)));
    let protocols = VM_PROTOCOLS
        .iter()
        .map(|protocol| (*protocol).to_string())
        .collect::<Vec<_>>();
    let controls = BroadcasterSubscriptionControls::Vm(VmBroadcasterSubscriptionControls {
        broadcaster_subscription: BroadcasterSubscriptionStatus::default(),
        state_store: Arc::clone(&state_store),
        stream_health: Arc::new(StreamHealth::new()),
        tokens,
        protocols: protocols.clone(),
        vm_stream: Arc::new(RwLock::new(VmStreamStatus::default())),
        simulation_rebuild_gate: Arc::new(RwLock::new(())),
    });

    Ok((controls, state_store, protocols))
}

async fn load_broadcaster_token_snapshot(broadcaster_url: &str) -> Result<HashMap<Bytes, Token>> {
    let snapshot_url = derive_broadcaster_token_snapshot_url(broadcaster_url)
        .context("failed to derive broadcaster token snapshot URL")?;
    let response = reqwest::Client::new()
        .get(snapshot_url.as_str())
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .with_context(|| {
            format!("failed to fetch broadcaster token snapshot from {snapshot_url}")
        })?;
    let http_status = response.status();
    if !http_status.is_success() {
        bail!("broadcaster token snapshot returned {http_status}");
    }

    let response = response
        .json::<BroadcasterTokenSnapshotResponse>()
        .await
        .context("failed to decode broadcaster token snapshot JSON")?;
    if response.chain_id != EXPECTED_CHAIN_ID {
        bail!(
            "broadcaster token snapshot chain_id mismatch: expected {}, got {}",
            EXPECTED_CHAIN_ID,
            response.chain_id
        );
    }

    response
        .tokens
        .into_iter()
        .map(|token| {
            token
                .into_token(Chain::Ethereum)
                .map(|token| (token.address.clone(), token))
        })
        .collect::<Result<_, _>>()
        .context("failed to parse broadcaster token snapshot")
}

async fn require_broadcaster_ready(broadcaster_url: &str) -> Result<()> {
    let status_url =
        crate::models::broadcaster_urls::derive_broadcaster_http_url(broadcaster_url, "status")
            .context("failed to derive broadcaster status URL")?;
    let response = reqwest::Client::new()
        .get(status_url.as_str())
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .with_context(|| format!("failed to fetch broadcaster status from {status_url}"))?;
    let http_status = response.status();
    let body = response
        .json::<Value>()
        .await
        .context("failed to decode broadcaster status JSON")?;

    if !http_status.is_success() {
        bail!("broadcaster status returned {http_status}: {body}");
    }
    require_json_u64(&body, &["chain_id"], EXPECTED_CHAIN_ID)?;
    require_json_str(&body, &["status"], "ready")?;
    require_json_bool(&body, &["snapshot", "ready"], true)?;
    require_json_array_contains(&body, &["snapshot", "configured_backends"], "vm")?;
    require_json_str(&body, &["redis_publisher", "mode"], "active")?;
    require_json_bool(&body, &["redis_publisher", "healthy"], true)?;

    let vm_backend = json_path(&body, &["backends", "vm"])?;
    let pool_count = json_path(vm_backend, &["pool_count"])?
        .as_u64()
        .ok_or_else(|| anyhow!("broadcaster status backends.vm.pool_count must be a number"))?;
    if pool_count == 0 {
        bail!("broadcaster status reports zero VM pools");
    }
    if json_path(vm_backend, &["block_number"])?.as_u64().is_none() {
        bail!("broadcaster status backends.vm.block_number must be present");
    }

    Ok(())
}

async fn bootstrap_snapshot(
    replay_client: &BroadcasterReplayClient,
    processor: &mut BroadcasterSubscriptionProcessor,
    evidence_decoder: &TychoStreamDecoder<BlockHeader>,
    state_store: &StateStore,
) -> Result<(
    RequiredProtocolEvidence,
    simulator_core::broadcaster::BroadcasterRedisReplayBoundary,
)> {
    let session = replay_client
        .create_snapshot_session()
        .await
        .context("failed to create live broadcaster snapshot session")?;
    if session.chain_id != EXPECTED_CHAIN_ID {
        bail!(
            "snapshot session chain_id mismatch: expected {}, got {}",
            EXPECTED_CHAIN_ID,
            session.chain_id
        );
    }

    processor.set_bootstrap_redis_replay_boundary(session.redis_replay_boundary.clone());
    processor.controls.stream_health().mark_started().await;

    let mut evidence = RequiredProtocolEvidence::default();
    let mut payloads = replay_client.snapshot_payloads(&session);
    while let Some(envelope_result) = payloads.next().await {
        let envelope = envelope_result.context("failed to fetch broadcaster snapshot payload")?;
        evidence.observe_snapshot_envelope(&envelope)?;
        processor
            .observe(envelope)
            .await
            .context("failed to apply broadcaster snapshot payload")?;
    }

    if !processor.bootstrap_complete() {
        bail!(
            "broadcaster HTTP snapshot session {} ended before VM bootstrap completed",
            session.session_id
        );
    }
    processor
        .align_redis_replay_boundary(&session.redis_replay_boundary)
        .context("failed to align VM snapshot with Redis replay boundary")?;
    evidence
        .decode_snapshot_messages(evidence_decoder)
        .await
        .context("failed to decode VM snapshot evidence")?;
    evidence.collect_stored_pools(state_store).await;
    require_captured_pools_are_present(&evidence, state_store).await?;

    Ok((evidence, session.redis_replay_boundary.clone()))
}

async fn require_captured_pools_are_present(
    evidence: &RequiredProtocolEvidence,
    state_store: &StateStore,
) -> Result<()> {
    for protocol in [CURVE_PROTOCOL, BALANCER_PROTOCOL] {
        if evidence.pool_ids(protocol).is_empty() {
            bail!(
                "decoded {protocol} snapshot material produced no quote candidates; raw_count={}",
                evidence.raw_pool_count(protocol)
            );
        }
        let mut found_in_store = false;
        for pool_id in evidence.pool_ids(protocol) {
            if state_store.pool_by_id(pool_id).await.is_some() {
                found_in_store = true;
                break;
            }
        }
        if found_in_store {
            continue;
        }
        bail!(
            "decoded {protocol} snapshot pool IDs were not found in StateStore; raw_count={} decoded_count={}",
            evidence.raw_pool_count(protocol),
            evidence.pool_ids(protocol).len()
        );
    }
    Ok(())
}

async fn poll_live_redis_until_quotes(
    replay_client: &BroadcasterReplayClient,
    prepared: &mut PreparedBroadcasterRedisSubscription,
    evidence_decoder: &TychoStreamDecoder<BlockHeader>,
    state_store: &Arc<StateStore>,
) -> Result<()> {
    let mut checkpoint =
        ReplayCheckpoint::new(prepared.replay_boundary.clone(), prepared.expected_chain_id);
    let started_at = Instant::now();
    let mut last_progress = started_at;
    let mut live_evidence = RequiredProtocolEvidence::default();
    let mut verified = RequiredProtocolFlags::default();

    loop {
        if started_at.elapsed() >= LIVE_DELTA_TIMEOUT {
            bail!(
                "timed out after {}s waiting for Curve and Balancer VM Redis deltas; \
                 curve_updates={} curve_candidates={} balancer_updates={} balancer_candidates={} checkpoint={}",
                LIVE_DELTA_TIMEOUT.as_secs(),
                live_evidence.event_count(CURVE_PROTOCOL),
                live_evidence.pool_count(CURVE_PROTOCOL),
                live_evidence.event_count(BALANCER_PROTOCOL),
                live_evidence.pool_count(BALANCER_PROTOCOL),
                checkpoint.entry_id()
            );
        }

        let poll = replay_client
            .read_next(&checkpoint)
            .await
            .context("failed to read next broadcaster Redis replay batch")?;
        match poll {
            ReplayPoll::Pending => {
                maybe_print_live_progress(
                    &mut last_progress,
                    started_at,
                    &checkpoint,
                    &live_evidence,
                );
            }
            ReplayPoll::CaughtUp {
                checkpoint: caught_up_checkpoint,
            } => {
                checkpoint = caught_up_checkpoint;
                mark_redis_catch_up_checkpoints(&prepared.processors, checkpoint.entry_id()).await;
                maybe_print_live_progress(
                    &mut last_progress,
                    started_at,
                    &checkpoint,
                    &live_evidence,
                );
            }
            ReplayPoll::Batch(batch) => {
                live_evidence
                    .observe_replay_items(state_store.as_ref(), evidence_decoder, &batch.items)
                    .await;
                let caught_up_after_batch = batch.caught_up_after_batch;
                apply_replay_batch(prepared, &mut checkpoint, batch.items)
                    .await
                    .map_err(|exit| {
                        anyhow!("failed to apply broadcaster Redis batch: {}", exit.message)
                    })?;
                if caught_up_after_batch {
                    mark_redis_catch_up_checkpoints(&prepared.processors, checkpoint.entry_id())
                        .await;
                }
                verify_live_quotes_if_ready(&mut verified, &live_evidence, state_store).await?;
                maybe_print_live_progress(
                    &mut last_progress,
                    started_at,
                    &checkpoint,
                    &live_evidence,
                );
                if verified.all() {
                    println!(
                        "live VM Redis deltas verified after {}s at checkpoint {}",
                        started_at.elapsed().as_secs(),
                        checkpoint.entry_id()
                    );
                    return Ok(());
                }
            }
        }
    }
}

async fn verify_live_quotes_if_ready(
    verified: &mut RequiredProtocolFlags,
    live_evidence: &RequiredProtocolEvidence,
    state_store: &StateStore,
) -> Result<()> {
    if !verified.curve && live_evidence.has_quote_candidates(CURVE_PROTOCOL) {
        require_non_zero_quote(
            "live Redis delta",
            CURVE_PROTOCOL,
            live_evidence.pool_ids(CURVE_PROTOCOL),
            state_store,
        )
        .await?;
        verified.curve = true;
    }
    if !verified.balancer && live_evidence.has_quote_candidates(BALANCER_PROTOCOL) {
        require_non_zero_quote(
            "live Redis delta",
            BALANCER_PROTOCOL,
            live_evidence.pool_ids(BALANCER_PROTOCOL),
            state_store,
        )
        .await?;
        verified.balancer = true;
    }
    Ok(())
}

async fn require_non_zero_quote(
    phase: &str,
    protocol: &str,
    pool_ids: impl IntoIterator<Item = &String>,
    state_store: &StateStore,
) -> Result<()> {
    let mut checked_pools = 0usize;
    let mut checked_pairs = 0usize;
    let mut errors = Vec::new();

    for pool_id in pool_ids {
        let Some((state, component)) = state_store.pool_by_id(pool_id).await else {
            continue;
        };
        if component.protocol_system != protocol {
            continue;
        }
        checked_pools = checked_pools.saturating_add(1);
        if let Some(result) =
            first_non_zero_quote(&*state, &component, &mut checked_pairs, &mut errors)
        {
            println!(
                "{phase} {protocol} quote ok pool={} {} -> {} amount_out={}",
                pool_id, result.token_in, result.token_out, result.amount_out
            );
            return Ok(());
        }
    }

    bail!(
        "{phase} {protocol} produced no non-zero quote from {checked_pools} pools \
         and {checked_pairs} ordered token pairs; sample errors: {}",
        errors.join("; ")
    );
}

fn first_non_zero_quote(
    state: &dyn ProtocolSim,
    component: &ProtocolComponent,
    checked_pairs: &mut usize,
    errors: &mut Vec<String>,
) -> Option<QuoteResult> {
    for token_in in &component.tokens {
        for token_out in &component.tokens {
            if token_in.address == token_out.address {
                continue;
            }
            *checked_pairs = checked_pairs.saturating_add(1);
            let amount_in = token_unit_amount(token_in);
            match state.get_amount_out(amount_in, token_in, token_out) {
                Ok(result) if !result.amount.is_zero() => {
                    return Some(QuoteResult {
                        token_in: token_in.symbol.clone(),
                        token_out: token_out.symbol.clone(),
                        amount_out: result.amount.to_string(),
                    });
                }
                Err(error) if errors.len() < 6 => errors.push(error.to_string()),
                Ok(_) | Err(_) => {}
            }
        }
    }
    None
}

fn token_unit_amount(token: &Token) -> BigUint {
    BigUint::from(10u32).pow(token.decimals.min(18))
}

fn maybe_print_live_progress(
    last_progress: &mut Instant,
    started_at: Instant,
    checkpoint: &ReplayCheckpoint,
    evidence: &RequiredProtocolEvidence,
) {
    if last_progress.elapsed() < LIVE_PROGRESS_INTERVAL {
        return;
    }
    println!(
        "waiting for live VM Redis deltas: elapsed={}s checkpoint={} curve_updates={} curve_candidates={} balancer_updates={} balancer_candidates={}",
        started_at.elapsed().as_secs(),
        checkpoint.entry_id(),
        evidence.event_count(CURVE_PROTOCOL),
        evidence.pool_count(CURVE_PROTOCOL),
        evidence.event_count(BALANCER_PROTOCOL),
        evidence.pool_count(BALANCER_PROTOCOL)
    );
    *last_progress = Instant::now();
}

#[derive(Default)]
struct RequiredProtocolFlags {
    curve: bool,
    balancer: bool,
}

impl RequiredProtocolFlags {
    const fn all(&self) -> bool {
        self.curve && self.balancer
    }
}

#[derive(Default)]
struct RequiredProtocolEvidence {
    curve: ProtocolEvidence,
    balancer: ProtocolEvidence,
    raw_snapshot: RawSnapshotReassembly,
}

impl RequiredProtocolEvidence {
    fn observe_snapshot_envelope(&mut self, envelope: &BroadcasterEnvelope) -> Result<()> {
        let BroadcasterPayload::SnapshotChunk(chunk) = &envelope.payload else {
            return Ok(());
        };
        for partition in &chunk.partitions {
            self.observe_snapshot_partition(partition)?;
        }
        Ok(())
    }

    async fn observe_replay_items(
        &mut self,
        state_store: &StateStore,
        evidence_decoder: &TychoStreamDecoder<BlockHeader>,
        items: &[ReplayBatchItem],
    ) {
        for item in items {
            if let ReplayBatchItem::Message(message) = item {
                if let Err(error) = self
                    .observe_live_envelope(state_store, evidence_decoder, &message.envelope)
                    .await
                {
                    println!("failed to decode live VM evidence: {error:#}");
                }
            }
        }
    }

    async fn observe_live_envelope(
        &mut self,
        state_store: &StateStore,
        evidence_decoder: &TychoStreamDecoder<BlockHeader>,
        envelope: &BroadcasterEnvelope,
    ) -> Result<()> {
        let BroadcasterPayload::Update(update) = &envelope.payload else {
            return Ok(());
        };
        for partition in &update.partitions {
            self.observe_live_partition(state_store, evidence_decoder, partition)
                .await?;
        }
        Ok(())
    }

    fn observe_snapshot_partition(
        &mut self,
        partition: &BroadcasterSnapshotPartition,
    ) -> Result<()> {
        if partition.backend != BroadcasterBackend::Vm {
            return Ok(());
        }
        for entry in &partition.states {
            self.record_snapshot_material(
                entry.component.protocol_system.as_str(),
                entry.component_id.as_str(),
            );
            self.record_pool(
                entry.component.protocol_system.as_str(),
                entry.component_id.as_str(),
            );
        }
        for message in &partition.messages {
            self.observe_raw_snapshot_message(message)?;
        }
        Ok(())
    }

    async fn observe_live_partition(
        &mut self,
        state_store: &StateStore,
        evidence_decoder: &TychoStreamDecoder<BlockHeader>,
        partition: &BroadcasterUpdatePartition,
    ) -> Result<()> {
        if partition.backend != BroadcasterBackend::Vm {
            return Ok(());
        }
        for entry in &partition.new_pairs {
            self.record_live_pool(
                entry.component.protocol_system.as_str(),
                entry.component_id.as_str(),
            );
        }
        for delta in &partition.updated_states {
            self.record_live_state_delta(state_store, delta.component_id.as_str())
                .await;
        }
        for message in &partition.messages {
            self.observe_raw_live_message(state_store, evidence_decoder, message)
                .await?;
        }
        Ok(())
    }

    async fn record_live_state_delta(&mut self, state_store: &StateStore, component_id: &str) {
        let Some((_state, component)) = state_store.pool_by_id(component_id).await else {
            return;
        };
        self.record_live_pool(component.protocol_system.as_str(), component_id);
    }

    fn observe_raw_snapshot_message(&mut self, message: &BroadcasterProtocolMessage) -> Result<()> {
        for component_id in message.message.snapshots.states.keys() {
            self.record_snapshot_material(message.protocol.as_str(), component_id.as_str());
        }
        self.raw_snapshot.push(message.clone())
    }

    async fn observe_raw_live_message(
        &mut self,
        state_store: &StateStore,
        evidence_decoder: &TychoStreamDecoder<BlockHeader>,
        message: &BroadcasterProtocolMessage,
    ) -> Result<()> {
        if raw_message_has_live_material(message) {
            self.record_event(message.protocol.as_str());
        }
        for component_id in message.message.snapshots.states.keys() {
            self.record_live_pool(message.protocol.as_str(), component_id.as_str());
        }
        if let Some(deltas) = &message.message.deltas {
            for component_id in deltas.new_protocol_components.keys() {
                self.record_live_pool(message.protocol.as_str(), component_id.as_str());
            }
            for component_id in deltas.state_updates.keys() {
                self.record_live_pool(message.protocol.as_str(), component_id.as_str());
            }
        }
        let update = decode_protocol_message(evidence_decoder, message)
            .await
            .with_context(|| format!("failed to decode live evidence for {}", message.protocol))?;
        self.record_decoded_update(Some(state_store), &update, true)
            .await;
        Ok(())
    }

    async fn decode_snapshot_messages(
        &mut self,
        evidence_decoder: &TychoStreamDecoder<BlockHeader>,
    ) -> Result<()> {
        for message in self.raw_snapshot.take_messages() {
            let update = decode_protocol_message(evidence_decoder, &message)
                .await
                .with_context(|| {
                    format!(
                        "failed to decode snapshot evidence for {}",
                        message.protocol
                    )
                })?;
            self.record_decoded_update(None, &update, false).await;
        }
        Ok(())
    }

    async fn record_decoded_update(
        &mut self,
        state_store: Option<&StateStore>,
        update: &Update,
        live: bool,
    ) {
        for (component_id, component) in &update.new_pairs {
            if live {
                self.record_live_pool(component.protocol_system.as_str(), component_id);
            } else {
                self.record_pool(component.protocol_system.as_str(), component_id);
            }
        }

        for component_id in update.states.keys() {
            if update.new_pairs.contains_key(component_id) {
                continue;
            }
            let Some(state_store) = state_store else {
                continue;
            };
            self.record_live_state_delta(state_store, component_id)
                .await;
        }
    }

    async fn collect_stored_pools(&mut self, state_store: &StateStore) {
        for protocol in [CURVE_PROTOCOL, BALANCER_PROTOCOL] {
            for pool_id in state_store.pool_ids_by_protocol_system(protocol).await {
                self.record_pool(protocol, pool_id.as_str());
            }
        }
    }

    fn record_live_pool(&mut self, protocol: &str, component_id: &str) {
        self.record_pool(protocol, component_id);
        self.record_event(protocol);
    }

    fn record_pool(&mut self, protocol: &str, component_id: &str) {
        if let Some(evidence) = self.protocol_mut(protocol) {
            evidence.pool_ids.insert(component_id.to_string());
        }
    }

    fn record_snapshot_material(&mut self, protocol: &str, component_id: &str) {
        if let Some(evidence) = self.protocol_mut(protocol) {
            evidence.raw_pool_ids.insert(component_id.to_string());
        }
    }

    fn record_event(&mut self, protocol: &str) {
        if let Some(evidence) = self.protocol_mut(protocol) {
            evidence.events = evidence.events.saturating_add(1);
        }
    }

    fn require_snapshot_material(&self) -> Result<()> {
        if self.curve.raw_pool_ids.is_empty() || self.balancer.raw_pool_ids.is_empty() {
            bail!(
                "snapshot VM material must include Curve and Balancer pools; curve_count={} balancer_count={}",
                self.curve.raw_pool_ids.len(),
                self.balancer.raw_pool_ids.len()
            );
        }
        Ok(())
    }

    fn pool_ids(&self, protocol: &str) -> &BTreeSet<String> {
        if protocol == BALANCER_PROTOCOL {
            &self.balancer.pool_ids
        } else {
            &self.curve.pool_ids
        }
    }

    fn event_count(&self, protocol: &str) -> usize {
        match protocol {
            CURVE_PROTOCOL => self.curve.events,
            BALANCER_PROTOCOL => self.balancer.events,
            _ => 0,
        }
    }

    fn raw_pool_count(&self, protocol: &str) -> usize {
        match protocol {
            CURVE_PROTOCOL => self.curve.raw_pool_ids.len(),
            BALANCER_PROTOCOL => self.balancer.raw_pool_ids.len(),
            _ => 0,
        }
    }

    fn pool_count(&self, protocol: &str) -> usize {
        self.pool_ids(protocol).len()
    }

    fn has_quote_candidates(&self, protocol: &str) -> bool {
        self.event_count(protocol) > 0 && self.pool_count(protocol) > 0
    }

    fn protocol_mut(&mut self, protocol: &str) -> Option<&mut ProtocolEvidence> {
        match protocol {
            CURVE_PROTOCOL => Some(&mut self.curve),
            BALANCER_PROTOCOL => Some(&mut self.balancer),
            _ => None,
        }
    }
}

#[derive(Default)]
struct ProtocolEvidence {
    raw_pool_ids: BTreeSet<String>,
    pool_ids: BTreeSet<String>,
    events: usize,
}

struct QuoteResult {
    token_in: String,
    token_out: String,
    amount_out: String,
}

async fn decode_protocol_message(
    decoder: &TychoStreamDecoder<BlockHeader>,
    raw: &BroadcasterProtocolMessage,
) -> Result<Update> {
    let mut state_msgs = HashMap::new();
    state_msgs.insert(raw.protocol.clone(), raw.message.clone());
    let mut sync_states = HashMap::new();
    sync_states.insert(raw.protocol.clone(), raw.sync_state.clone());
    let feed = FeedMessage {
        state_msgs,
        sync_states,
    };
    decoder
        .decode(&feed)
        .await
        .map_err(|error| anyhow!("failed to decode broadcaster raw payload: {error}"))
}

fn raw_message_has_live_material(message: &BroadcasterProtocolMessage) -> bool {
    let snapshots = &message.message.snapshots;
    let has_snapshot = !snapshots.states.is_empty() || !snapshots.vm_storage.is_empty();
    let has_delta = message.message.deltas.as_ref().is_some_and(|deltas| {
        !deltas.account_updates.is_empty()
            || !deltas.state_updates.is_empty()
            || !deltas.new_protocol_components.is_empty()
            || !deltas.deleted_protocol_components.is_empty()
    });

    has_snapshot || has_delta || !message.message.removed_components.is_empty()
}

fn json_path<'a>(body: &'a Value, path: &[&str]) -> Result<&'a Value> {
    let mut current = body;
    for segment in path {
        current = current
            .get(*segment)
            .ok_or_else(|| anyhow!("broadcaster status missing {}", path.join(".")))?;
    }
    Ok(current)
}

fn require_json_str(body: &Value, path: &[&str], expected: &str) -> Result<()> {
    let actual = json_path(body, path)?
        .as_str()
        .ok_or_else(|| anyhow!("broadcaster status {} must be a string", path.join(".")))?;
    if actual != expected {
        bail!(
            "broadcaster status {} mismatch: expected {expected}, got {actual}",
            path.join(".")
        );
    }
    Ok(())
}

fn require_json_bool(body: &Value, path: &[&str], expected: bool) -> Result<()> {
    let actual = json_path(body, path)?
        .as_bool()
        .ok_or_else(|| anyhow!("broadcaster status {} must be a bool", path.join(".")))?;
    if actual != expected {
        bail!(
            "broadcaster status {} mismatch: expected {expected}, got {actual}",
            path.join(".")
        );
    }
    Ok(())
}

fn require_json_u64(body: &Value, path: &[&str], expected: u64) -> Result<()> {
    let actual = json_path(body, path)?
        .as_u64()
        .ok_or_else(|| anyhow!("broadcaster status {} must be a number", path.join(".")))?;
    if actual != expected {
        bail!(
            "broadcaster status {} mismatch: expected {expected}, got {actual}",
            path.join(".")
        );
    }
    Ok(())
}

fn require_json_array_contains(body: &Value, path: &[&str], expected: &str) -> Result<()> {
    let values = json_path(body, path)?
        .as_array()
        .ok_or_else(|| anyhow!("broadcaster status {} must be an array", path.join(".")))?;
    if values.iter().any(|value| value.as_str() == Some(expected)) {
        return Ok(());
    }
    bail!(
        "broadcaster status {} must include {expected}",
        path.join(".")
    );
}
