use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use futures::StreamExt;
use rand::Rng;
use reqwest::{Client, StatusCode};
use tokio::sync::{OwnedRwLockWriteGuard, RwLock};
use tokio::time::{timeout, Instant};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error as WsError, Message},
};
use tracing::{info, warn};
use tycho_simulation::{
    evm::decoder::TychoStreamDecoder,
    evm::engine_db::SHARED_TYCHO_DB,
    protocol::models::{ProtocolComponent, Update},
    tycho_client::feed::{BlockHeader, FeedMessage},
    tycho_common::simulation::protocol_sim::ProtocolSim,
};

use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterPayload,
    BroadcasterProtocolMessage, BroadcasterSnapshotPartition, BroadcasterSnapshotSessionResponse,
    BroadcasterSubscriptionTracker, BroadcasterUpdatePartition,
};

use crate::memory::maybe_purge_allocator;
use crate::models::broadcaster_urls::{
    derive_broadcaster_http_url, derive_broadcaster_session_ws_url,
};
use crate::models::state::{BroadcasterSubscriptionStatus, StateStore, VmStreamStatus};
use crate::models::stream_health::StreamHealth;
use crate::models::tokens::TokenStore;
use crate::services::stream_builder::build_broadcaster_subscription_decoder;
use crate::stream::StreamSupervisorConfig;

const SNAPSHOT_DOWNLOAD_CONCURRENCY: usize = 4;

#[derive(Clone)]
pub enum BroadcasterSubscriptionControls {
    Native(NativeBroadcasterSubscriptionControls),
    Vm(VmBroadcasterSubscriptionControls),
}

#[derive(Clone)]
pub struct NativeBroadcasterSubscriptionControls {
    pub broadcaster_subscription: BroadcasterSubscriptionStatus,
    pub state_store: Arc<StateStore>,
    pub stream_health: Arc<StreamHealth>,
    pub tokens: Arc<TokenStore>,
    pub protocols: Vec<String>,
}

#[derive(Clone)]
pub struct VmBroadcasterSubscriptionControls {
    pub broadcaster_subscription: BroadcasterSubscriptionStatus,
    pub state_store: Arc<StateStore>,
    pub stream_health: Arc<StreamHealth>,
    pub tokens: Arc<TokenStore>,
    pub protocols: Vec<String>,
    pub vm_stream: Arc<RwLock<VmStreamStatus>>,
    pub simulation_rebuild_gate: Arc<RwLock<()>>,
}

impl BroadcasterSubscriptionControls {
    fn backend(&self) -> BroadcasterBackend {
        match self {
            Self::Native(_) => BroadcasterBackend::Native,
            Self::Vm(_) => BroadcasterBackend::Vm,
        }
    }

    fn backend_label(&self) -> &'static str {
        self.backend().as_str()
    }

    fn broadcaster_subscription(&self) -> &BroadcasterSubscriptionStatus {
        match self {
            Self::Native(controls) => &controls.broadcaster_subscription,
            Self::Vm(controls) => &controls.broadcaster_subscription,
        }
    }

    fn state_store(&self) -> &Arc<StateStore> {
        match self {
            Self::Native(controls) => &controls.state_store,
            Self::Vm(controls) => &controls.state_store,
        }
    }

    fn stream_health(&self) -> &Arc<StreamHealth> {
        match self {
            Self::Native(controls) => &controls.stream_health,
            Self::Vm(controls) => &controls.stream_health,
        }
    }

    fn tokens(&self) -> Arc<TokenStore> {
        match self {
            Self::Native(controls) => Arc::clone(&controls.tokens),
            Self::Vm(controls) => Arc::clone(&controls.tokens),
        }
    }

    fn protocols(&self) -> &[String] {
        match self {
            Self::Native(controls) => &controls.protocols,
            Self::Vm(controls) => &controls.protocols,
        }
    }
}

pub async fn supervise_broadcaster_subscription(
    ws_url: String,
    expected_chain_id: u64,
    cfg: StreamSupervisorConfig,
    controls: BroadcasterSubscriptionControls,
) {
    let mut backoff = cfg.restart_backoff_min;
    let mut vm_rebuild = None;
    let client = Client::new();

    loop {
        let bootstrap = match prepare_broadcaster_subscription(
            &client,
            &ws_url,
            expected_chain_id,
            &cfg,
            &controls,
            vm_rebuild,
        )
        .await
        {
            Ok(bootstrap) => bootstrap,
            Err(error) => {
                (vm_rebuild, backoff) = reset_subscription_after_error(
                    &controls,
                    &cfg,
                    backoff,
                    error.vm_rebuild,
                    error.message,
                    error.event,
                    error.detail,
                )
                .await;
                continue;
            }
        };

        let stream = match connect_async(bootstrap.session_ws_url.as_str()).await {
            Ok((stream, _response)) => stream,
            Err(error) => {
                let message = format!("Failed to connect to broadcaster websocket: {error}");
                (vm_rebuild, backoff) = reset_subscription_after_error(
                    &controls,
                    &cfg,
                    backoff,
                    bootstrap.processor.vm_rebuild,
                    message,
                    "broadcaster_subscription_connect_failed",
                    "Failed to connect to broadcaster subscription",
                )
                .await;
                continue;
            }
        };

        info!(
            ws_url = bootstrap.session_ws_url,
            session_id = bootstrap.session.session_id,
            backend = controls.backend_label(),
            "Connected to broadcaster subscription"
        );
        let (exit, next_vm_rebuild) =
            process_broadcaster_subscription(stream, bootstrap.processor, &cfg).await;
        (vm_rebuild, backoff) =
            restart_after_subscription_exit(&controls, &cfg, backoff, exit, next_vm_rebuild).await;
    }
}

struct PreparedBroadcasterSubscription {
    processor: BroadcasterSubscriptionProcessor,
    session: BroadcasterSnapshotSessionResponse,
    session_ws_url: String,
}

struct PrepareBroadcasterSubscriptionError {
    message: String,
    event: &'static str,
    detail: &'static str,
    vm_rebuild: Option<VmSubscriptionRebuildState>,
}

impl PrepareBroadcasterSubscriptionError {
    fn new(
        message: String,
        event: &'static str,
        detail: &'static str,
        vm_rebuild: Option<VmSubscriptionRebuildState>,
    ) -> Self {
        Self {
            message,
            event,
            detail,
            vm_rebuild,
        }
    }
}

async fn prepare_broadcaster_subscription(
    client: &Client,
    ws_url: &str,
    expected_chain_id: u64,
    cfg: &StreamSupervisorConfig,
    controls: &BroadcasterSubscriptionControls,
    vm_rebuild: Option<VmSubscriptionRebuildState>,
) -> std::result::Result<PreparedBroadcasterSubscription, PrepareBroadcasterSubscriptionError> {
    let snapshot_sessions_url = match derive_broadcaster_http_url(ws_url, "snapshot-sessions") {
        Ok(url) => url,
        Err(error) => {
            return Err(PrepareBroadcasterSubscriptionError::new(
                format!("Invalid broadcaster websocket URL: {error}"),
                "broadcaster_subscription_url_invalid",
                "Failed to derive broadcaster snapshot session URL",
                vm_rebuild,
            ));
        }
    };
    let decoder = match build_broadcaster_subscription_decoder(
        controls.tokens(),
        controls.backend(),
        controls.protocols(),
    )
    .await
    {
        Ok(decoder) => decoder,
        Err(error) => {
            return Err(PrepareBroadcasterSubscriptionError::new(
                error.to_string(),
                "broadcaster_subscription_decoder_failed",
                "Failed to build broadcaster subscription decoder",
                vm_rebuild,
            ));
        }
    };
    let mut processor = BroadcasterSubscriptionProcessor::with_decoder(
        expected_chain_id,
        controls.clone(),
        decoder,
        vm_rebuild,
    );
    let session = match bootstrap_broadcaster_snapshot(
        client,
        ws_url,
        &snapshot_sessions_url,
        &mut processor,
        controls,
        cfg,
    )
    .await
    {
        Ok(session) => session,
        Err(error) => {
            return Err(PrepareBroadcasterSubscriptionError::new(
                error.to_string(),
                "broadcaster_subscription_bootstrap_failed",
                "Failed to bootstrap broadcaster subscription from HTTP snapshot",
                processor.vm_rebuild,
            ));
        }
    };
    let session_ws_url = match derive_broadcaster_session_ws_url(ws_url, session.session_id) {
        Ok(url) => url,
        Err(error) => {
            return Err(PrepareBroadcasterSubscriptionError::new(
                format!("Invalid broadcaster websocket URL: {error}"),
                "broadcaster_subscription_url_invalid",
                "Failed to derive broadcaster session websocket URL",
                processor.vm_rebuild,
            ));
        }
    };

    Ok(PreparedBroadcasterSubscription {
        processor,
        session,
        session_ws_url,
    })
}

async fn reset_subscription_after_error(
    controls: &BroadcasterSubscriptionControls,
    cfg: &StreamSupervisorConfig,
    backoff: Duration,
    vm_rebuild: Option<VmSubscriptionRebuildState>,
    message: String,
    event: &'static str,
    detail: &'static str,
) -> (Option<VmSubscriptionRebuildState>, Duration) {
    let vm_rebuild = handle_subscription_reset(controls, Some(message.clone()), vm_rebuild).await;
    let backoff_ms = jittered_backoff_ms(backoff, cfg.restart_backoff_jitter_pct);
    warn!(
        event = event,
        backend = controls.backend_label(),
        backoff_ms,
        error = %message,
        "{detail}"
    );
    sleep_backoff(backoff_ms, cfg.memory).await;
    (vm_rebuild, next_backoff(backoff, cfg.restart_backoff_max))
}

async fn restart_after_subscription_exit(
    controls: &BroadcasterSubscriptionControls,
    cfg: &StreamSupervisorConfig,
    backoff: Duration,
    exit: SubscriptionExit,
    vm_rebuild: Option<VmSubscriptionRebuildState>,
) -> (Option<VmSubscriptionRebuildState>, Duration) {
    let vm_rebuild = handle_subscription_reset(controls, exit.last_error.clone(), vm_rebuild).await;
    let backoff_ms = jittered_backoff_ms(backoff, cfg.restart_backoff_jitter_pct);
    warn!(
        event = "broadcaster_subscription_restart",
        backend = controls.backend_label(),
        reason = exit.reason,
        backoff_ms,
        last_error = exit.last_error.as_deref(),
        "Restarting broadcaster subscription"
    );
    sleep_backoff(backoff_ms, cfg.memory).await;
    (vm_rebuild, next_backoff(backoff, cfg.restart_backoff_max))
}

struct BroadcasterSubscriptionProcessor {
    expected_chain_id: u64,
    controls: BroadcasterSubscriptionControls,
    decoder: Arc<TychoStreamDecoder<BlockHeader>>,
    tracker: BroadcasterSubscriptionTracker,
    bootstrap_block: Option<u64>,
    vm_rebuild: Option<VmSubscriptionRebuildState>,
}

impl BroadcasterSubscriptionProcessor {
    #[cfg(test)]
    fn new(
        expected_chain_id: u64,
        controls: BroadcasterSubscriptionControls,
        vm_rebuild: Option<VmSubscriptionRebuildState>,
    ) -> Self {
        Self::with_decoder(
            expected_chain_id,
            controls,
            Arc::new(TychoStreamDecoder::new()),
            vm_rebuild,
        )
    }

    fn with_decoder(
        expected_chain_id: u64,
        controls: BroadcasterSubscriptionControls,
        decoder: Arc<TychoStreamDecoder<BlockHeader>>,
        vm_rebuild: Option<VmSubscriptionRebuildState>,
    ) -> Self {
        Self {
            expected_chain_id,
            controls,
            decoder,
            tracker: BroadcasterSubscriptionTracker::new(),
            bootstrap_block: None,
            vm_rebuild,
        }
    }

    fn bootstrap_complete(&self) -> bool {
        matches!(
            self.tracker.state(),
            simulator_core::broadcaster::BroadcasterSubscriptionState::Live { .. }
        )
    }

    async fn observe(&mut self, envelope: BroadcasterEnvelope) -> Result<()> {
        if let BroadcasterPayload::SnapshotStart(start) = &envelope.payload {
            self.ensure_snapshot_chain_id(start.chain_id)?;
        }

        self.tracker
            .observe(&envelope)
            .map_err(|error| anyhow!("invalid broadcaster envelope: {error}"))?;

        match envelope.payload {
            BroadcasterPayload::SnapshotStart(start) => {
                self.bootstrap_block = None;
                self.controls
                    .broadcaster_subscription()
                    .mark_snapshot_started(envelope.stream_id, start.snapshot_id)
                    .await;
            }
            BroadcasterPayload::SnapshotChunk(chunk) => {
                for partition in chunk.partitions {
                    if partition.backend == self.controls.backend() {
                        self.bootstrap_block = Some(partition.block_number);
                        self.apply_snapshot_partition(partition).await?;
                    }
                }
            }
            BroadcasterPayload::SnapshotEnd(_end) => {
                self.refresh_bootstrap_health().await;
                self.controls
                    .broadcaster_subscription()
                    .mark_bootstrap_complete()
                    .await;
                self.finish_vm_rebuild().await;
            }
            BroadcasterPayload::Update(update) => {
                for partition in update.partitions {
                    if partition.backend == self.controls.backend() {
                        self.apply_live_update_partition(partition).await?;
                    }
                }
            }
            BroadcasterPayload::Heartbeat(heartbeat) => {
                for head in heartbeat.backend_heads {
                    if head.backend == self.controls.backend() {
                        self.apply_heartbeat(head).await;
                    }
                }
            }
        }

        Ok(())
    }

    fn ensure_snapshot_chain_id(&self, chain_id: u64) -> Result<()> {
        if chain_id != self.expected_chain_id {
            return Err(anyhow!(
                "broadcaster chain id mismatch for {} subscription: expected {}, got {}",
                self.controls.backend_label(),
                self.expected_chain_id,
                chain_id
            ));
        }
        Ok(())
    }

    async fn finish_vm_rebuild(&mut self) {
        let Some(rebuild) = self.vm_rebuild.take() else {
            return;
        };

        drop(rebuild.guard);

        if let BroadcasterSubscriptionControls::Vm(controls) = &self.controls {
            let mut vm_stream = controls.vm_stream.write().await;
            vm_stream.rebuilding = false;
            vm_stream.rebuild_started_at = None;
        }
    }

    async fn apply_snapshot_partition(
        &self,
        partition: BroadcasterSnapshotPartition,
    ) -> Result<()> {
        if !partition.messages.is_empty() {
            return self.apply_protocol_messages(partition.messages).await;
        }

        let update = snapshot_partition_update(partition);
        self.controls.state_store().apply_update(update).await;
        Ok(())
    }

    async fn apply_live_update_partition(
        &self,
        partition: BroadcasterUpdatePartition,
    ) -> Result<()> {
        let block_number = partition.block_number;
        if !partition.messages.is_empty() {
            self.apply_protocol_messages(partition.messages).await?;
            self.controls
                .stream_health()
                .record_update(block_number)
                .await;
            return Ok(());
        }

        let update = live_partition_update(partition);
        self.controls.state_store().apply_update(update).await;
        self.controls
            .stream_health()
            .record_update(block_number)
            .await;
        Ok(())
    }

    async fn apply_protocol_messages(
        &self,
        messages: Vec<BroadcasterProtocolMessage>,
    ) -> Result<()> {
        for message in messages {
            self.apply_protocol_message(message).await?;
        }
        Ok(())
    }

    async fn apply_protocol_message(&self, raw: BroadcasterProtocolMessage) -> Result<()> {
        let mut state_msgs = HashMap::new();
        state_msgs.insert(raw.protocol.clone(), raw.message);
        let mut sync_states = HashMap::new();
        sync_states.insert(raw.protocol, raw.sync_state);
        let feed = FeedMessage {
            state_msgs,
            sync_states,
        };
        let update = self
            .decoder
            .decode(&feed)
            .await
            .map_err(|error| anyhow!("failed to decode broadcaster raw payload: {error}"))?;
        self.controls.state_store().apply_update(update).await;
        Ok(())
    }

    async fn apply_heartbeat(&self, head: BroadcasterBackendHead) {
        self.controls
            .state_store()
            .apply_update(Update::new(
                head.block_number,
                HashMap::new(),
                HashMap::new(),
            ))
            .await;
        self.controls
            .stream_health()
            .record_update(head.block_number)
            .await;
    }

    async fn refresh_bootstrap_health(&self) {
        if let Some(block_number) = self.bootstrap_block {
            self.controls
                .stream_health()
                .record_update(block_number)
                .await;
        }
    }
}

async fn bootstrap_broadcaster_snapshot(
    client: &Client,
    ws_url: &str,
    snapshot_sessions_url: &str,
    processor: &mut BroadcasterSubscriptionProcessor,
    controls: &BroadcasterSubscriptionControls,
    cfg: &StreamSupervisorConfig,
) -> Result<BroadcasterSnapshotSessionResponse> {
    let session =
        create_broadcaster_snapshot_session(client, snapshot_sessions_url, cfg.readiness_stale)
            .await?;
    controls.stream_health().mark_started().await;

    {
        let mut payloads = futures::stream::iter(0..session.payload_count)
            .map(|index| {
                let session = session.clone();
                async move {
                    fetch_broadcaster_snapshot_payload(
                        client,
                        ws_url,
                        &session,
                        index,
                        cfg.readiness_stale,
                    )
                    .await
                }
            })
            .buffered(SNAPSHOT_DOWNLOAD_CONCURRENCY);

        while let Some(envelope) = payloads.next().await {
            processor.observe(envelope?).await?;
        }
    }

    if !processor.bootstrap_complete() {
        return Err(anyhow!(
            "broadcaster HTTP snapshot session {} ended before bootstrap completed",
            session.session_id
        ));
    }

    Ok(session)
}

async fn create_broadcaster_snapshot_session(
    client: &Client,
    snapshot_sessions_url: &str,
    request_timeout: Duration,
) -> Result<BroadcasterSnapshotSessionResponse> {
    let response = client
        .post(snapshot_sessions_url)
        .timeout(request_timeout)
        .send()
        .await
        .map_err(|error| {
            anyhow!(
                "failed to create broadcaster snapshot session at {snapshot_sessions_url}: {error}"
            )
        })?;
    decode_success_json(
        response,
        snapshot_sessions_url,
        "create broadcaster snapshot session",
    )
    .await
}

async fn fetch_broadcaster_snapshot_payload(
    client: &Client,
    ws_url: &str,
    session: &BroadcasterSnapshotSessionResponse,
    index: u32,
    request_timeout: Duration,
) -> Result<BroadcasterEnvelope> {
    let payload_url = derive_broadcaster_http_url(
        ws_url,
        &format!("snapshot-sessions/{}/payloads/{index}", session.session_id),
    )
    .map_err(|error| anyhow!("failed to derive broadcaster snapshot payload URL: {error}"))?;
    let response = client
        .get(&payload_url)
        .timeout(request_timeout)
        .send()
        .await
        .map_err(|error| {
            anyhow!(
                "failed to fetch broadcaster snapshot payload {index} from {payload_url}: {error}"
            )
        })?;
    decode_success_json(response, &payload_url, "fetch broadcaster snapshot payload").await
}

async fn decode_success_json<T>(
    response: reqwest::Response,
    url: &str,
    operation: &str,
) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let status = response.status();
    if !status.is_success() {
        return Err(http_status_error(operation, url, status));
    }
    let body = response
        .bytes()
        .await
        .map_err(|error| anyhow!("failed to read {operation} response from {url}: {error}"))?;
    serde_json::from_slice(&body)
        .map_err(|error| anyhow!("failed to decode {operation} response from {url}: {error}"))
}

fn http_status_error(operation: &str, url: &str, status: StatusCode) -> anyhow::Error {
    anyhow!("{operation} at {url} failed with HTTP {status}")
}

async fn process_broadcaster_subscription(
    mut stream: impl futures::Stream<Item = Result<Message, WsError>> + Unpin,
    mut processor: BroadcasterSubscriptionProcessor,
    cfg: &StreamSupervisorConfig,
) -> (SubscriptionExit, Option<VmSubscriptionRebuildState>) {
    loop {
        let next_timeout = if processor.bootstrap_complete() {
            cfg.stream_stale
        } else {
            cfg.readiness_stale
        };

        let Ok(next_message) = timeout(next_timeout, stream.next()).await else {
            let reason = if processor.bootstrap_complete() {
                "live message timeout from broadcaster"
            } else {
                "bootstrap timeout from broadcaster"
            };
            return (SubscriptionExit::error(reason), processor.vm_rebuild);
        };

        match next_message {
            None => {
                return (
                    SubscriptionExit::error("broadcaster websocket ended"),
                    processor.vm_rebuild,
                )
            }
            Some(Ok(Message::Text(text))) => {
                let envelope: BroadcasterEnvelope = match serde_json::from_str(text.as_ref()) {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        return (
                            SubscriptionExit::error(format!(
                                "failed to decode broadcaster payload: {error}"
                            )),
                            processor.vm_rebuild,
                        )
                    }
                };

                if let Err(error) = processor.observe(envelope).await {
                    return (
                        SubscriptionExit::error(error.to_string()),
                        processor.vm_rebuild,
                    );
                }
            }
            Some(Ok(Message::Binary(_))) => {
                return (
                    SubscriptionExit::error("unexpected binary broadcaster payload"),
                    processor.vm_rebuild,
                );
            }
            Some(Ok(Message::Close(_))) => {
                return (
                    SubscriptionExit::error("broadcaster websocket closed"),
                    processor.vm_rebuild,
                );
            }
            Some(Ok(_)) => {}
            Some(Err(error)) => {
                return (
                    SubscriptionExit::error(error.to_string()),
                    processor.vm_rebuild,
                )
            }
        }
    }
}

async fn handle_subscription_reset(
    controls: &BroadcasterSubscriptionControls,
    last_error: Option<String>,
    vm_rebuild: Option<VmSubscriptionRebuildState>,
) -> Option<VmSubscriptionRebuildState> {
    controls
        .broadcaster_subscription()
        .mark_disconnected(last_error.clone())
        .await;
    controls.stream_health().increment_restart().await;
    controls.stream_health().reset_bursts().await;
    controls
        .stream_health()
        .set_last_error(last_error.clone())
        .await;

    match controls {
        BroadcasterSubscriptionControls::Native(_) => {
            controls.state_store().reset().await;
            None
        }
        BroadcasterSubscriptionControls::Vm(vm_controls) => {
            {
                let mut vm_stream = vm_controls.vm_stream.write().await;
                vm_stream.last_error = last_error;
            }
            let vm_rebuild = begin_or_continue_vm_rebuild(vm_controls, vm_rebuild).await;
            controls.state_store().reset().await;
            Some(vm_rebuild)
        }
    }
}

struct VmSubscriptionRebuildState {
    guard: OwnedRwLockWriteGuard<()>,
}

async fn begin_or_continue_vm_rebuild(
    controls: &VmBroadcasterSubscriptionControls,
    vm_rebuild: Option<VmSubscriptionRebuildState>,
) -> VmSubscriptionRebuildState {
    {
        let mut vm_stream = controls.vm_stream.write().await;
        vm_stream.rebuilding = true;
        vm_stream.restart_count = vm_stream.restart_count.saturating_add(1);
        if vm_stream.rebuild_started_at.is_none() {
            vm_stream.rebuild_started_at = Some(Instant::now());
        }
    }

    if let Some(vm_rebuild) = vm_rebuild {
        return vm_rebuild;
    }

    let guard = controls.simulation_rebuild_gate.clone().write_owned().await;

    if let Err(err) = SHARED_TYCHO_DB.clear() {
        warn!(
            error = %err,
            "Failed clearing TychoDB during broadcaster-driven VM rebuild"
        );
    }

    VmSubscriptionRebuildState { guard }
}

fn snapshot_partition_update(partition: BroadcasterSnapshotPartition) -> Update {
    let mut states = HashMap::new();
    let mut new_pairs = HashMap::new();

    for entry in partition.states {
        states.insert(entry.component_id.clone(), entry.state);
        new_pairs.insert(entry.component_id, entry.component);
    }

    Update::new(partition.block_number, states, new_pairs)
}

fn live_partition_update(partition: BroadcasterUpdatePartition) -> Update {
    let block_number = partition.block_number;
    let mut states: HashMap<String, Box<dyn ProtocolSim>> = HashMap::new();
    let mut new_pairs: HashMap<String, ProtocolComponent> = HashMap::new();
    let mut removed_pairs = HashMap::new();

    for entry in partition.new_pairs {
        states.insert(entry.component_id.clone(), entry.state);
        new_pairs.insert(entry.component_id, entry.component);
    }

    for delta in partition.updated_states {
        states.insert(delta.component_id, delta.state);
    }

    for removed in partition.removed_pairs {
        removed_pairs.insert(removed.component_id, removed.component);
    }

    Update::new(block_number, states, new_pairs).set_removed_pairs(removed_pairs)
}

async fn sleep_backoff(backoff_ms: u64, memory: crate::config::MemoryConfig) {
    maybe_purge_allocator("broadcaster_subscription_restart", memory);
    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
}

fn next_backoff(current: Duration, max: Duration) -> Duration {
    current.saturating_mul(2).min(max)
}

fn jittered_backoff_ms(base: Duration, jitter_pct: f64) -> u64 {
    let base_ms = base.as_millis() as f64;
    let mut rng = rand::thread_rng();
    let jitter = rng.gen_range(-jitter_pct..=jitter_pct);
    let jittered = (base_ms * (1.0 + jitter)).max(0.0);
    jittered.round() as u64
}

struct SubscriptionExit {
    reason: &'static str,
    last_error: Option<String>,
}

impl SubscriptionExit {
    fn error(message: impl Into<String>) -> Self {
        Self {
            reason: "error",
            last_error: Some(message.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::Result;
    use num_bigint::BigUint;
    use reqwest::Client;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::RwLock,
        task::JoinHandle,
    };
    use tycho_simulation::tycho_common::dto::ProtocolStateDelta;
    use tycho_simulation::tycho_common::simulation::errors::{SimulationError, TransitionError};
    use tycho_simulation::{
        protocol::models::ProtocolComponent,
        tycho_common::{
            models::{token::Token, Chain},
            simulation::protocol_sim::{Balances, GetAmountOutResult, ProtocolSim},
            Bytes,
        },
    };

    use super::{
        bootstrap_broadcaster_snapshot, handle_subscription_reset, BroadcasterSubscriptionControls,
        BroadcasterSubscriptionProcessor, NativeBroadcasterSubscriptionControls,
        VmBroadcasterSubscriptionControls,
    };
    use crate::config::MemoryConfig;
    use crate::models::state::{BroadcasterSubscriptionStatus, StateStore, VmStreamStatus};
    use crate::models::stream_health::StreamHealth;
    use crate::models::tokens::TokenStore;
    use crate::stream::StreamSupervisorConfig;
    use simulator_core::broadcaster::{
        BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterHeartbeat,
        BroadcasterPayload, BroadcasterSnapshotChunk, BroadcasterSnapshotEnd,
        BroadcasterSnapshotPartition, BroadcasterSnapshotStart, BroadcasterStateDelta,
        BroadcasterStateEntry, BroadcasterUpdateMessage, BroadcasterUpdatePartition,
    };

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

    struct TestControls {
        token_store: Arc<TokenStore>,
        native_subscription: BroadcasterSubscriptionStatus,
        vm_subscription: BroadcasterSubscriptionStatus,
        native_state_store: Arc<StateStore>,
        vm_state_store: Arc<StateStore>,
        native_stream_health: Arc<StreamHealth>,
        vm_stream_health: Arc<StreamHealth>,
        vm_stream: Arc<RwLock<VmStreamStatus>>,
        simulation_rebuild_gate: Arc<RwLock<()>>,
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
                native_state_store: Arc::new(StateStore::new(Arc::clone(&token_store))),
                vm_state_store: Arc::new(StateStore::new(Arc::clone(&token_store))),
                native_stream_health: Arc::new(StreamHealth::new()),
                vm_stream_health: Arc::new(StreamHealth::new()),
                vm_stream: Arc::new(RwLock::new(VmStreamStatus::default())),
                simulation_rebuild_gate: Arc::new(RwLock::new(())),
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
                simulation_rebuild_gate: Arc::clone(&self.simulation_rebuild_gate),
            })
        }
    }

    async fn bootstrap(processor: &mut BroadcasterSubscriptionProcessor) -> Result<()> {
        processor.observe(snapshot_start_envelope()?).await?;
        processor.observe(snapshot_chunk_envelope()?).await?;
        processor.observe(snapshot_end_envelope()).await?;
        Ok(())
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

    fn empty_snapshot_chunk_envelope() -> Result<BroadcasterEnvelope> {
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
                        Vec::new(),
                        BTreeMap::new(),
                    ),
                    BroadcasterSnapshotPartition::new(
                        BroadcasterBackend::Vm,
                        11,
                        Vec::new(),
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

    fn update_envelope() -> Result<BroadcasterEnvelope> {
        Ok(BroadcasterEnvelope::new(
            "stream-1",
            4,
            BroadcasterPayload::Update(BroadcasterUpdateMessage::new(vec![
                BroadcasterUpdatePartition::new(
                    BroadcasterBackend::Native,
                    12,
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

    fn heartbeat_envelope() -> Result<BroadcasterEnvelope> {
        Ok(BroadcasterEnvelope::new(
            "stream-1",
            4,
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

    async fn spawn_snapshot_session_server(
        payloads: Vec<BroadcasterEnvelope>,
    ) -> Result<(String, String, JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let payloads = Arc::new(
            payloads
                .into_iter()
                .map(|payload| serde_json::to_string(&payload))
                .collect::<Result<Vec<_>, _>>()?,
        );
        let server_task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let payloads = Arc::clone(&payloads);
                tokio::spawn(async move {
                    let mut buffer = [0u8; 8192];
                    let Ok(read) = socket.read(&mut buffer).await else {
                        return;
                    };
                    let request = String::from_utf8_lossy(&buffer[..read]);
                    let first_line = request.lines().next().unwrap_or_default();
                    let (status, body) = snapshot_server_response(first_line, &payloads);
                    let response = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });

        Ok((
            format!("ws://{addr}/ws"),
            format!("http://{addr}/snapshot-sessions"),
            server_task,
        ))
    }

    fn snapshot_server_response(first_line: &str, payloads: &[String]) -> (&'static str, String) {
        if first_line == "POST /snapshot-sessions HTTP/1.1" {
            return (
                "201 Created",
                serde_json::json!({
                    "chainId": Chain::Ethereum.id(),
                    "sessionId": 7,
                    "streamId": "stream-1",
                    "snapshotId": "snapshot-1",
                    "payloadCount": payloads.len(),
                    "snapshotChunkCount": payloads
                        .iter()
                        .filter(|payload| payload.contains("\"kind\":\"snapshot_chunk\""))
                        .count(),
                    "expiresInMs": 300000
                })
                .to_string(),
            );
        }

        let Some(path) = first_line
            .strip_prefix("GET /snapshot-sessions/7/payloads/")
            .and_then(|rest| rest.strip_suffix(" HTTP/1.1"))
        else {
            return (
                "404 Not Found",
                serde_json::json!({ "error": "not found" }).to_string(),
            );
        };
        let Ok(index) = path.parse::<usize>() else {
            return (
                "416 Range Not Satisfiable",
                serde_json::json!({ "error": "bad index" }).to_string(),
            );
        };
        match payloads.get(index) {
            Some(payload) => ("200 OK", payload.clone()),
            None => (
                "416 Range Not Satisfiable",
                serde_json::json!({ "error": "bad index" }).to_string(),
            ),
        }
    }

    fn test_supervisor_config() -> StreamSupervisorConfig {
        StreamSupervisorConfig {
            readiness_stale: Duration::from_secs(1),
            stream_stale: Duration::from_secs(1),
            missing_block_burst: 3,
            missing_block_window: Duration::from_secs(60),
            error_burst: 3,
            error_window: Duration::from_secs(60),
            resync_grace: Duration::from_secs(60),
            restart_backoff_min: Duration::from_millis(10),
            restart_backoff_max: Duration::from_millis(100),
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
    async fn http_snapshot_bootstrap_populates_processor_before_live_attach() -> Result<()> {
        let controls = TestControls::new();
        let native_controls = controls.native();
        let mut processor = BroadcasterSubscriptionProcessor::new(
            Chain::Ethereum.id(),
            native_controls.clone(),
            None,
        );
        let payloads = vec![
            snapshot_start_envelope()?,
            empty_snapshot_chunk_envelope()?,
            snapshot_end_envelope(),
        ];
        let (ws_url, snapshot_sessions_url, server_task) =
            spawn_snapshot_session_server(payloads).await?;

        let session = bootstrap_broadcaster_snapshot(
            &Client::new(),
            &ws_url,
            &snapshot_sessions_url,
            &mut processor,
            &native_controls,
            &test_supervisor_config(),
        )
        .await?;
        server_task.abort();

        assert_eq!(session.session_id, 7);
        assert!(processor.bootstrap_complete());
        assert_eq!(controls.native_state_store.current_block().await, 10);
        assert!(!controls.native_state_store.has_pool("pool-native").await);
        assert!(!controls.native_state_store.has_pool("pool-vm").await);
        let snapshot = controls.native_subscription.snapshot().await;
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
        let vm_rebuild_read_guard = Arc::clone(&controls.simulation_rebuild_gate)
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
}
