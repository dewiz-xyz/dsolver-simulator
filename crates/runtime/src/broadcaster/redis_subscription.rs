use std::collections::{
    btree_map::Entry as BTreeEntry, hash_map::Entry as HashEntry, BTreeMap, HashMap,
};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use rand::Rng;
use redis::streams::{StreamId, StreamInfoStreamReply, StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use reqwest::Client;
use tokio::sync::{OwnedRwLockWriteGuard, RwLock};
use tokio::time::Instant;
use tracing::{info, warn};
use tycho_simulation::{
    evm::decoder::TychoStreamDecoder,
    evm::engine_db::SHARED_TYCHO_DB,
    protocol::models::{ProtocolComponent, Update},
    tycho_client::feed::{BlockHeader, FeedMessage},
    tycho_common::{dto::ResponseAccount, simulation::protocol_sim::ProtocolSim, Bytes},
};

use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterPayload,
    BroadcasterProtocolMessage, BroadcasterRedisReplayBoundary, BroadcasterRedisStreamEntry,
    BroadcasterSnapshotPartition, BroadcasterSnapshotSessionResponse, BroadcasterSnapshotStart,
    BroadcasterSubscriptionTracker, BroadcasterUpdatePartition,
};

use crate::config::BroadcasterRedisConfig;
use crate::memory::maybe_purge_allocator;
use crate::models::broadcaster_urls::derive_broadcaster_http_url;
use crate::models::state::{BroadcasterSubscriptionStatus, StateStore, VmStreamStatus};
use crate::models::stream_health::StreamHealth;
use crate::models::tokens::TokenStore;
use crate::services::stream_builder::build_broadcaster_subscription_decoder;
use crate::stream::StreamSupervisorConfig;

const SNAPSHOT_DOWNLOAD_CONCURRENCY: usize = 4;

#[derive(Clone)]
pub(crate) enum BroadcasterSubscriptionControls {
    Native(NativeBroadcasterSubscriptionControls),
    Vm(VmBroadcasterSubscriptionControls),
    Rfq(RfqBroadcasterSubscriptionControls),
}

#[derive(Clone)]
pub(crate) struct NativeBroadcasterSubscriptionControls {
    pub broadcaster_subscription: BroadcasterSubscriptionStatus,
    pub state_store: Arc<StateStore>,
    pub stream_health: Arc<StreamHealth>,
    pub tokens: Arc<TokenStore>,
    pub protocols: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct VmBroadcasterSubscriptionControls {
    pub broadcaster_subscription: BroadcasterSubscriptionStatus,
    pub state_store: Arc<StateStore>,
    pub stream_health: Arc<StreamHealth>,
    pub tokens: Arc<TokenStore>,
    pub protocols: Vec<String>,
    pub vm_stream: Arc<RwLock<VmStreamStatus>>,
    pub simulation_rebuild_gate: Arc<RwLock<()>>,
}

#[derive(Clone)]
pub(crate) struct RfqBroadcasterSubscriptionControls {
    pub broadcaster_subscription: BroadcasterSubscriptionStatus,
    pub state_store: Arc<StateStore>,
    pub stream_health: Arc<StreamHealth>,
    pub tokens: Arc<TokenStore>,
    pub protocols: Vec<String>,
    pub simulation_rebuild_gate: Arc<RwLock<()>>,
}

impl BroadcasterSubscriptionControls {
    fn backend(&self) -> BroadcasterBackend {
        match self {
            Self::Native(_) => BroadcasterBackend::Native,
            Self::Vm(_) => BroadcasterBackend::Vm,
            Self::Rfq(_) => BroadcasterBackend::Rfq,
        }
    }

    fn backend_label(&self) -> &'static str {
        self.backend().as_str()
    }

    fn broadcaster_subscription(&self) -> &BroadcasterSubscriptionStatus {
        match self {
            Self::Native(controls) => &controls.broadcaster_subscription,
            Self::Vm(controls) => &controls.broadcaster_subscription,
            Self::Rfq(controls) => &controls.broadcaster_subscription,
        }
    }

    fn state_store(&self) -> &Arc<StateStore> {
        match self {
            Self::Native(controls) => &controls.state_store,
            Self::Vm(controls) => &controls.state_store,
            Self::Rfq(controls) => &controls.state_store,
        }
    }

    fn stream_health(&self) -> &Arc<StreamHealth> {
        match self {
            Self::Native(controls) => &controls.stream_health,
            Self::Vm(controls) => &controls.stream_health,
            Self::Rfq(controls) => &controls.stream_health,
        }
    }

    fn tokens(&self) -> Arc<TokenStore> {
        match self {
            Self::Native(controls) => Arc::clone(&controls.tokens),
            Self::Vm(controls) => Arc::clone(&controls.tokens),
            Self::Rfq(controls) => Arc::clone(&controls.tokens),
        }
    }

    fn protocols(&self) -> &[String] {
        match self {
            Self::Native(controls) => &controls.protocols,
            Self::Vm(controls) => &controls.protocols,
            Self::Rfq(controls) => &controls.protocols,
        }
    }
}

pub(crate) async fn supervise_broadcaster_redis_subscription(
    broadcaster_url: String,
    expected_chain_id: u64,
    redis_config: BroadcasterRedisConfig,
    cfg: StreamSupervisorConfig,
    controls: Vec<BroadcasterSubscriptionControls>,
) {
    if controls.is_empty() {
        warn!("Broadcaster Redis subscription supervisor has no enabled backends");
        return;
    }

    let client = Client::new();
    let mut backoff = cfg.restart_backoff_min;
    let mut rebuilds = empty_rebuilds(controls.len());

    loop {
        let reader = match TokioRedisStreamReader::connect(&redis_config.redis_url).await {
            Ok(reader) => reader,
            Err(error) => {
                let message = format!("Failed to connect to broadcaster Redis: {error:#}");
                (rebuilds, backoff) = reset_redis_subscription_after_error(
                    &controls,
                    &cfg,
                    backoff,
                    rebuilds,
                    message,
                    false,
                    (
                        "broadcaster_redis_subscription_connect_failed",
                        "Failed to connect to broadcaster Redis",
                    ),
                )
                .await;
                continue;
            }
        };

        let prepared = match prepare_broadcaster_redis_subscription(
            &client,
            &broadcaster_url,
            expected_chain_id,
            &cfg,
            &controls,
            rebuilds,
        )
        .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                (rebuilds, backoff) = reset_redis_subscription_after_error(
                    &controls,
                    &cfg,
                    backoff,
                    error.rebuilds,
                    error.message,
                    false,
                    (error.event, error.detail),
                )
                .await;
                continue;
            }
        };

        info!(
            stream_key = prepared.replay_boundary.stream_key.as_str(),
            stream_id = prepared.replay_boundary.stream_id.as_str(),
            exclusive_entry_id = prepared.replay_boundary.exclusive_entry_id(),
            "Connected to broadcaster Redis subscription"
        );

        let (exit, next_rebuilds) =
            process_broadcaster_redis_subscription(reader, prepared, &redis_config).await;
        (rebuilds, backoff) = reset_redis_subscription_after_error(
            &controls,
            &cfg,
            backoff,
            next_rebuilds,
            exit.message,
            exit.redis_gap,
            (
                "broadcaster_redis_subscription_restart",
                "Restarting broadcaster Redis subscription",
            ),
        )
        .await;
    }
}

fn empty_rebuilds(count: usize) -> Vec<Option<SubscriptionRebuildState>> {
    std::iter::repeat_with(|| None).take(count).collect()
}

struct PreparedBroadcasterRedisSubscription {
    processors: Vec<PreparedRedisProcessor>,
    replay_boundary: BroadcasterRedisReplayBoundary,
    required_catch_up_message_seq: u64,
    expected_chain_id: u64,
}

struct PreparedRedisProcessor {
    index: usize,
    processor: BroadcasterSubscriptionProcessor,
    replay_boundary: BroadcasterRedisReplayBoundary,
}

struct PrepareBroadcasterRedisSubscriptionError {
    message: String,
    event: &'static str,
    detail: &'static str,
    rebuilds: Vec<Option<SubscriptionRebuildState>>,
}

impl PrepareBroadcasterRedisSubscriptionError {
    fn new(
        message: String,
        event: &'static str,
        detail: &'static str,
        rebuilds: Vec<Option<SubscriptionRebuildState>>,
    ) -> Self {
        Self {
            message,
            event,
            detail,
            rebuilds,
        }
    }
}

async fn prepare_broadcaster_redis_subscription(
    client: &Client,
    broadcaster_url: &str,
    expected_chain_id: u64,
    cfg: &StreamSupervisorConfig,
    controls: &[BroadcasterSubscriptionControls],
    mut rebuilds: Vec<Option<SubscriptionRebuildState>>,
) -> std::result::Result<
    PreparedBroadcasterRedisSubscription,
    PrepareBroadcasterRedisSubscriptionError,
> {
    let mut processors = Vec::with_capacity(controls.len());
    let mut last_backend_error = None;

    for (index, controls) in controls.iter().enumerate() {
        let snapshot_sessions_url = match derive_broadcaster_http_url(
            broadcaster_url,
            BROADCASTER_SNAPSHOT_SESSIONS_PATH,
        ) {
            Ok(url) => url,
            Err(error) => {
                return Err(PrepareBroadcasterRedisSubscriptionError::new(
                    format!("Invalid broadcaster URL: {error}"),
                    "broadcaster_redis_subscription_url_invalid",
                    "Failed to derive broadcaster snapshot session URL",
                    rebuilds,
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
                let message = error.to_string();
                last_backend_error = Some(message.clone());
                let rebuild = rebuilds.get_mut(index).and_then(Option::take);
                rebuilds[index] = handle_subscription_reset(controls, Some(message), rebuild).await;
                continue;
            }
        };
        let rebuild = rebuilds.get_mut(index).and_then(Option::take);
        let mut processor = BroadcasterSubscriptionProcessor::with_decoder(
            expected_chain_id,
            controls.clone(),
            decoder,
            rebuild,
        );
        let session = match bootstrap_broadcaster_snapshot(
            client,
            broadcaster_url,
            &snapshot_sessions_url,
            &mut processor,
            controls,
            cfg,
        )
        .await
        {
            Ok(session) => session,
            Err(error) => {
                let message = error.to_string();
                last_backend_error = Some(message.clone());
                rebuilds[index] =
                    handle_subscription_reset(controls, Some(message), processor.rebuild).await;
                continue;
            }
        };
        if let Err(error) = processor.align_redis_replay_boundary(&session.redis_replay_boundary) {
            let message = error.to_string();
            last_backend_error = Some(message.clone());
            rebuilds[index] =
                handle_subscription_reset(controls, Some(message), processor.rebuild).await;
            continue;
        }

        processors.push(PreparedRedisProcessor {
            index,
            processor,
            replay_boundary: session.redis_replay_boundary,
        });
    }

    finish_prepared_broadcaster_redis_subscription(
        processors,
        rebuilds,
        last_backend_error,
        expected_chain_id,
    )
}

fn finish_prepared_broadcaster_redis_subscription(
    processors: Vec<PreparedRedisProcessor>,
    rebuilds: Vec<Option<SubscriptionRebuildState>>,
    last_backend_error: Option<String>,
    expected_chain_id: u64,
) -> std::result::Result<
    PreparedBroadcasterRedisSubscription,
    PrepareBroadcasterRedisSubscriptionError,
> {
    if let Some(message) = last_backend_error {
        let rebuilds = merge_redis_processor_rebuilds(processors, rebuilds);
        return Err(PrepareBroadcasterRedisSubscriptionError::new(
            message,
            "broadcaster_redis_subscription_bootstrap_failed",
            "Failed to bootstrap broadcaster subscription from HTTP snapshot",
            rebuilds,
        ));
    }

    if processors.is_empty() {
        return Err(PrepareBroadcasterRedisSubscriptionError::new(
            "no broadcaster backends were prepared".to_string(),
            "broadcaster_redis_subscription_bootstrap_failed",
            "Failed to bootstrap broadcaster subscription from HTTP snapshot",
            rebuilds,
        ));
    }

    let required_catch_up_message_seq = processors
        .iter()
        .map(|processor| processor.replay_boundary.exclusive_message_seq)
        .max()
        .unwrap_or(0);
    let replay_boundary = match coalesce_redis_replay_boundary(
        processors
            .iter()
            .map(|processor| processor.replay_boundary.clone())
            .collect(),
    ) {
        Ok(replay_boundary) => replay_boundary,
        Err(error) => {
            return Err(PrepareBroadcasterRedisSubscriptionError::new(
                error.to_string(),
                "broadcaster_redis_subscription_boundary_invalid",
                "Failed to derive process Redis replay boundary",
                merge_redis_processor_rebuilds(processors, rebuilds),
            ));
        }
    };

    Ok(PreparedBroadcasterRedisSubscription {
        processors,
        replay_boundary,
        required_catch_up_message_seq,
        expected_chain_id,
    })
}

async fn process_broadcaster_redis_subscription(
    reader: TokioRedisStreamReader,
    mut prepared: PreparedBroadcasterRedisSubscription,
    redis_config: &BroadcasterRedisConfig,
) -> (SubscriptionExit, Vec<Option<SubscriptionRebuildState>>) {
    let mut cursor =
        RedisReplayCursor::new(prepared.replay_boundary.clone(), prepared.expected_chain_id);

    loop {
        let messages = match reader
            .read_after(
                &prepared.replay_boundary.stream_key,
                cursor.cursor_entry_id(),
                redis_config.block_ms,
                redis_config.read_count,
            )
            .await
        {
            Ok(messages) => messages,
            Err(error) => {
                return (
                    SubscriptionExit::error(format!(
                        "failed to read broadcaster Redis stream: {error:#}"
                    )),
                    redis_processor_rebuilds(prepared.processors),
                );
            }
        };

        let caught_up_after_batch = messages.len() < redis_config.read_count as usize;
        if messages.is_empty() {
            if let Err(exit) = handle_empty_redis_poll(&reader, &cursor, &prepared).await {
                return (exit, redis_processor_rebuilds(prepared.processors));
            }
            continue;
        }

        if let Err(exit) = apply_redis_message_batch(&mut prepared, &mut cursor, messages).await {
            return (exit, redis_processor_rebuilds(prepared.processors));
        }

        if caught_up_after_batch {
            if let Err(error) =
                cursor.ensure_reached_required_boundary(prepared.required_catch_up_message_seq)
            {
                return (
                    SubscriptionExit::redis_gap(error.to_string()),
                    redis_processor_rebuilds(prepared.processors),
                );
            }
            mark_redis_catch_up_cursors(&prepared.processors, cursor.cursor_entry_id()).await;
        }
    }
}

async fn handle_empty_redis_poll(
    reader: &TokioRedisStreamReader,
    cursor: &RedisReplayCursor,
    prepared: &PreparedBroadcasterRedisSubscription,
) -> std::result::Result<(), SubscriptionExit> {
    let stream_info = match reader
        .stream_info(&prepared.replay_boundary.stream_key)
        .await
    {
        Ok(stream_info) => stream_info,
        Err(error) => {
            return Err(SubscriptionExit::error(format!(
                "failed to inspect broadcaster Redis stream: {error:#}"
            )));
        }
    };

    match redis_empty_poll_action(
        cursor,
        stream_info.as_ref(),
        prepared.required_catch_up_message_seq,
    ) {
        Ok(RedisEmptyPollAction::CaughtUp) => {
            mark_redis_catch_up_cursors(&prepared.processors, cursor.cursor_entry_id()).await;
        }
        Ok(RedisEmptyPollAction::Pending) => {}
        Err(error) => return Err(SubscriptionExit::redis_gap(error.to_string())),
    }
    Ok(())
}

async fn apply_redis_message_batch(
    prepared: &mut PreparedBroadcasterRedisSubscription,
    cursor: &mut RedisReplayCursor,
    messages: Vec<RedisStreamMessage>,
) -> std::result::Result<(), SubscriptionExit> {
    let mut last_applied_entry_id = None;
    for message in messages {
        if let Err(error) = cursor.ensure_next_message(&message) {
            return Err(SubscriptionExit::redis_gap(error.to_string()));
        }

        let envelope: BroadcasterEnvelope = serde_json::from_str(&message.entry.payload_json)
            .map_err(|error| {
                SubscriptionExit::error(format!(
                    "failed to decode broadcaster Redis payload: {error}"
                ))
            })?;

        for prepared_processor in &mut prepared.processors {
            if message.entry.message_seq <= prepared_processor.replay_boundary.exclusive_message_seq
            {
                continue;
            }
            prepared_processor
                .processor
                .observe_redis_delta(&message.entry, &envelope)
                .await
                .map_err(|error| SubscriptionExit::error(error.to_string()))?;
        }

        cursor.mark_applied(&message);
        last_applied_entry_id = Some(message.entry_id);
    }
    if let Some(entry_id) = last_applied_entry_id {
        mark_redis_replay_cursors(&prepared.processors, &entry_id, cursor.last_message_seq).await;
    }
    Ok(())
}

async fn mark_redis_replay_cursors(
    processors: &[PreparedRedisProcessor],
    entry_id: &str,
    message_seq: u64,
) {
    for prepared in processors {
        let cursor = if message_seq < prepared.replay_boundary.exclusive_message_seq {
            prepared.replay_boundary.exclusive_entry_id()
        } else {
            entry_id.to_string()
        };
        prepared
            .processor
            .controls
            .broadcaster_subscription()
            .mark_redis_replay_cursor(cursor)
            .await;
    }
}

async fn mark_redis_catch_up_cursors(processors: &[PreparedRedisProcessor], cursor_entry_id: &str) {
    for prepared in processors {
        prepared
            .processor
            .controls
            .broadcaster_subscription()
            .mark_redis_catch_up_cursor(cursor_entry_id.to_string())
            .await;
    }
}

fn redis_processor_rebuilds(
    processors: Vec<PreparedRedisProcessor>,
) -> Vec<Option<SubscriptionRebuildState>> {
    let mut rebuilds = empty_rebuilds(processors.len());
    for prepared in processors {
        rebuilds[prepared.index] = prepared.processor.rebuild;
    }
    rebuilds
}

fn merge_redis_processor_rebuilds(
    processors: Vec<PreparedRedisProcessor>,
    mut rebuilds: Vec<Option<SubscriptionRebuildState>>,
) -> Vec<Option<SubscriptionRebuildState>> {
    for prepared in processors {
        rebuilds[prepared.index] = prepared.processor.rebuild;
    }
    rebuilds
}

async fn reset_redis_subscription_after_error(
    controls: &[BroadcasterSubscriptionControls],
    cfg: &StreamSupervisorConfig,
    backoff: Duration,
    rebuilds: Vec<Option<SubscriptionRebuildState>>,
    message: String,
    is_gap: bool,
    (event, detail): (&'static str, &'static str),
) -> (Vec<Option<SubscriptionRebuildState>>, Duration) {
    let mut next_rebuilds = Vec::with_capacity(controls.len());
    for (controls, rebuild) in controls.iter().zip(rebuilds) {
        let rebuild = handle_subscription_reset(controls, Some(message.clone()), rebuild).await;
        if is_gap {
            controls
                .broadcaster_subscription()
                .mark_redis_gap(message.clone())
                .await;
        }
        next_rebuilds.push(rebuild);
    }
    let backoff_ms = jittered_backoff_ms(backoff, cfg.restart_backoff_jitter_pct);
    warn!(
        event,
        backoff_ms,
        error = %message,
        "{}", detail
    );
    sleep_backoff(backoff_ms, cfg.memory).await;
    (
        next_rebuilds,
        next_backoff(backoff, cfg.restart_backoff_max),
    )
}

struct TokioRedisStreamReader {
    blocking_read_connection: redis::aio::ConnectionManager,
    inspection_connection: redis::aio::ConnectionManager,
}

impl TokioRedisStreamReader {
    async fn connect(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)
            .context("failed to create Redis client from BROADCASTER_REDIS_URL")?;
        // Keep blocking XREAD calls away from XINFO. If a blocking read times out locally, Redis
        // can still hold that command on its socket until the BLOCK window expires.
        let blocking_read_connection = client
            .get_connection_manager()
            .await
            .context("failed to connect to broadcaster Redis read connection")?;
        let inspection_connection = client
            .get_connection_manager()
            .await
            .context("failed to connect to broadcaster Redis inspection connection")?;
        Ok(Self {
            blocking_read_connection,
            inspection_connection,
        })
    }

    async fn read_after(
        &self,
        stream_key: &str,
        cursor_entry_id: &str,
        block_ms: u64,
        read_count: u64,
    ) -> Result<Vec<RedisStreamMessage>> {
        let options = StreamReadOptions::default()
            .block(block_ms as usize)
            .count(read_count as usize);
        let mut connection = self.blocking_read_connection.clone();
        let reply = connection
            .xread_options(&[stream_key], &[cursor_entry_id], &options)
            .await;
        redis_xread_messages(stream_key, reply)
    }

    async fn stream_info(&self, stream_key: &str) -> Result<Option<RedisStreamInfo>> {
        let mut connection = self.inspection_connection.clone();
        let reply = redis::cmd("XINFO")
            .arg("STREAM")
            .arg(stream_key)
            .query_async::<StreamInfoStreamReply>(&mut connection)
            .await;
        redis_stream_info(reply)
    }
}

#[derive(Debug, Clone)]
struct RedisStreamMessage {
    entry_id: String,
    entry: BroadcasterRedisStreamEntry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RedisStreamInfo {
    last_generated_entry_id: String,
    first_entry_id: Option<String>,
    last_entry_id: Option<String>,
}

fn redis_stream_messages(
    expected_stream_key: &str,
    reply: StreamReadReply,
) -> Result<Vec<RedisStreamMessage>> {
    let mut messages = Vec::new();
    for key in reply.keys {
        if key.key != expected_stream_key {
            return Err(anyhow!(
                "Redis XREAD returned stream key {}, expected {}",
                key.key,
                expected_stream_key
            ));
        }
        for stream_id in key.ids {
            messages.push(redis_stream_message(stream_id)?);
        }
    }
    Ok(messages)
}

fn redis_xread_messages(
    expected_stream_key: &str,
    reply: std::result::Result<Option<StreamReadReply>, redis::RedisError>,
) -> Result<Vec<RedisStreamMessage>> {
    match reply {
        Ok(Some(reply)) => redis_stream_messages(expected_stream_key, reply),
        Ok(None) => Ok(Vec::new()),
        Err(error) if error.is_timeout() => Ok(Vec::new()),
        Err(error) => Err(anyhow!(error).context("Redis XREAD failed")),
    }
}

fn redis_stream_info(
    reply: std::result::Result<StreamInfoStreamReply, redis::RedisError>,
) -> Result<Option<RedisStreamInfo>> {
    match reply {
        Ok(reply) => redis_stream_info_from_reply(reply).map(Some),
        Err(error) if redis_stream_missing_key(&error) => Ok(None),
        Err(error) => Err(anyhow!(error).context("Redis XINFO STREAM failed")),
    }
}

fn redis_stream_info_from_reply(reply: StreamInfoStreamReply) -> Result<RedisStreamInfo> {
    let first_entry_id = redis_stream_info_entry_id(reply.length, reply.first_entry.id)?;
    let last_entry_id = redis_stream_info_entry_id(reply.length, reply.last_entry.id)?;
    Ok(RedisStreamInfo {
        last_generated_entry_id: reply.last_generated_id,
        first_entry_id,
        last_entry_id,
    })
}

fn redis_stream_info_entry_id(length: usize, entry_id: String) -> Result<Option<String>> {
    match (length, entry_id.is_empty()) {
        (0, _) => Ok(None),
        (_, false) => Ok(Some(entry_id)),
        (_, true) => Err(anyhow!("Redis XINFO STREAM omitted a retained entry id")),
    }
}

fn redis_stream_missing_key(error: &redis::RedisError) -> bool {
    error
        .detail()
        .is_some_and(|detail| detail.contains("no such key"))
}

fn redis_stream_message(stream_id: StreamId) -> Result<RedisStreamMessage> {
    let entry_id = stream_id.id;
    let mut value = serde_json::Map::new();
    for (field, redis_value) in stream_id.map {
        let field_value: String = redis::from_redis_value(redis_value)
            .with_context(|| format!("Redis stream field {field} is not a string"))?;
        value.insert(field, serde_json::Value::String(field_value));
    }
    let entry = serde_json::from_value(serde_json::Value::Object(value))
        .context("failed to decode Redis stream entry")?;
    Ok(RedisStreamMessage { entry_id, entry })
}

fn redis_entry_scope_contains(
    entry: &BroadcasterRedisStreamEntry,
    backend: BroadcasterBackend,
) -> bool {
    entry
        .backend_scope
        .split(',')
        .any(|scope| scope == backend.as_str())
}

fn coalesce_redis_replay_boundary(
    boundaries: Vec<BroadcasterRedisReplayBoundary>,
) -> Result<BroadcasterRedisReplayBoundary> {
    let mut boundaries = boundaries.into_iter();
    let Some(mut earliest) = boundaries.next() else {
        return Err(anyhow!("no Redis replay boundaries were captured"));
    };

    for boundary in boundaries {
        ensure_same_redis_stream_boundary(&earliest, &boundary)?;
        if boundary.exclusive_message_seq < earliest.exclusive_message_seq {
            earliest = boundary;
        }
    }

    Ok(earliest)
}

fn ensure_same_redis_stream_boundary(
    left: &BroadcasterRedisReplayBoundary,
    right: &BroadcasterRedisReplayBoundary,
) -> Result<()> {
    if left.stream_key != right.stream_key
        || left.stream_id != right.stream_id
        || left.snapshot_id != right.snapshot_id
        || left.generation != right.generation
    {
        return Err(anyhow!(
            "backend HTTP snapshots returned different Redis replay boundaries: left stream_id={} generation={} snapshot_id={}, right stream_id={} generation={} snapshot_id={}",
            left.stream_id,
            left.generation,
            left.snapshot_id,
            right.stream_id,
            right.generation,
            right.snapshot_id
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RedisEmptyPollAction {
    CaughtUp,
    Pending,
}

fn redis_empty_poll_action(
    cursor: &RedisReplayCursor,
    stream_info: Option<&RedisStreamInfo>,
    required_message_seq: u64,
) -> Result<RedisEmptyPollAction> {
    cursor.ensure_reached_required_boundary(required_message_seq)?;
    let Some(stream_info) = stream_info else {
        if cursor.last_message_seq == 0 {
            return Ok(RedisEmptyPollAction::CaughtUp);
        }
        return Err(anyhow!(
            "Redis replay gap: stream disappeared after cursor {}",
            cursor.cursor_entry_id
        ));
    };

    let cursor_entry_id = parse_redis_entry_id(&cursor.cursor_entry_id)?;
    let last_generated_entry_id = parse_redis_entry_id(&stream_info.last_generated_entry_id)?;
    if last_generated_entry_id < cursor_entry_id {
        return Err(anyhow!(
            "Redis replay gap: stream moved backwards from cursor {} to last generated {}",
            cursor.cursor_entry_id,
            stream_info.last_generated_entry_id
        ));
    }
    if last_generated_entry_id == cursor_entry_id {
        return Ok(RedisEmptyPollAction::CaughtUp);
    }

    let expected_seq = cursor
        .last_message_seq
        .checked_add(1)
        .ok_or_else(|| anyhow!("Redis replay message_seq overflow"))?;
    let expected_entry_id = redis_entry_id(cursor.boundary.generation, expected_seq);
    let expected_entry_id_parts = parse_redis_entry_id(&expected_entry_id)?;
    let Some(first_entry_id) = &stream_info.first_entry_id else {
        return Err(anyhow!(
            "Redis replay gap: stream generated {} after cursor {} but retained no entries",
            stream_info.last_generated_entry_id,
            cursor.cursor_entry_id
        ));
    };
    let Some(last_entry_id) = &stream_info.last_entry_id else {
        return Err(anyhow!(
            "Redis replay gap: stream generated {} after cursor {} but retained no last entry",
            stream_info.last_generated_entry_id,
            cursor.cursor_entry_id
        ));
    };

    let first_entry_id_parts = parse_redis_entry_id(first_entry_id)?;
    if first_entry_id_parts > expected_entry_id_parts {
        return Err(anyhow!(
            "Redis replay gap: first retained entry {} is after expected entry {}",
            first_entry_id,
            expected_entry_id
        ));
    }

    let last_entry_id_parts = parse_redis_entry_id(last_entry_id)?;
    if last_entry_id_parts <= cursor_entry_id {
        return Err(anyhow!(
            "Redis replay gap: stream generated {} after cursor {} but last retained entry is {}",
            stream_info.last_generated_entry_id,
            cursor.cursor_entry_id,
            last_entry_id
        ));
    }

    Ok(RedisEmptyPollAction::Pending)
}

struct RedisReplayCursor {
    boundary: BroadcasterRedisReplayBoundary,
    cursor_entry_id: String,
    last_message_seq: u64,
    expected_chain_id: u64,
}

impl RedisReplayCursor {
    fn new(boundary: BroadcasterRedisReplayBoundary, expected_chain_id: u64) -> Self {
        Self {
            cursor_entry_id: boundary.exclusive_entry_id(),
            last_message_seq: boundary.exclusive_message_seq,
            boundary,
            expected_chain_id,
        }
    }

    fn cursor_entry_id(&self) -> &str {
        &self.cursor_entry_id
    }

    fn ensure_next_message(&self, message: &RedisStreamMessage) -> Result<()> {
        if message.entry.stream_id != self.boundary.stream_id {
            return Err(anyhow!(
                "Redis replay gap: expected stream_id {}, got {}",
                self.boundary.stream_id,
                message.entry.stream_id
            ));
        }
        if message.entry.chain_id != self.expected_chain_id {
            return Err(anyhow!(
                "Redis replay gap: expected chain_id {}, got {}",
                self.expected_chain_id,
                message.entry.chain_id
            ));
        }

        if message.entry.message_seq <= self.last_message_seq {
            return Err(anyhow!(
                "Redis replay gap: got stale message_seq {} after {} at {}",
                message.entry.message_seq,
                self.cursor_entry_id,
                message.entry_id
            ));
        }

        let expected_seq = self
            .last_message_seq
            .checked_add(1)
            .ok_or_else(|| anyhow!("Redis replay message_seq overflow"))?;
        if message.entry.message_seq != expected_seq {
            return Err(anyhow!(
                "Redis replay gap: expected message_seq {} after {}, got {} at {}",
                expected_seq,
                self.cursor_entry_id,
                message.entry.message_seq,
                message.entry_id
            ));
        }

        let expected_entry_id = redis_entry_id(self.boundary.generation, expected_seq);
        if message.entry_id != expected_entry_id {
            return Err(anyhow!(
                "Redis replay gap: expected entry id {}, got {}",
                expected_entry_id,
                message.entry_id
            ));
        }

        if let Some(snapshot_id) = &message.entry.snapshot_id {
            if snapshot_id != &self.boundary.snapshot_id {
                return Err(anyhow!(
                    "Redis replay gap: expected snapshot_id {}, got {}",
                    self.boundary.snapshot_id,
                    snapshot_id
                ));
            }
        }

        Ok(())
    }

    fn ensure_reached_required_boundary(&self, required_message_seq: u64) -> Result<()> {
        if self.last_message_seq >= required_message_seq {
            return Ok(());
        }

        Err(anyhow!(
            "Redis replay gap: stream ended at message_seq {} ({}) before required snapshot replay boundary {}",
            self.last_message_seq,
            self.cursor_entry_id,
            required_message_seq
        ))
    }

    fn mark_applied(&mut self, message: &RedisStreamMessage) {
        self.cursor_entry_id = message.entry_id.clone();
        self.last_message_seq = message.entry.message_seq;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RedisEntryIdParts {
    millis: u64,
    sequence: u64,
}

fn parse_redis_entry_id(entry_id: &str) -> Result<RedisEntryIdParts> {
    let Some((millis, sequence)) = entry_id.split_once('-') else {
        return Err(anyhow!("invalid Redis stream entry id: {entry_id}"));
    };
    Ok(RedisEntryIdParts {
        millis: millis
            .parse()
            .with_context(|| format!("invalid Redis stream entry id: {entry_id}"))?,
        sequence: sequence
            .parse()
            .with_context(|| format!("invalid Redis stream entry id: {entry_id}"))?,
    })
}

fn redis_entry_id(generation: u64, message_seq: u64) -> String {
    format!("{generation}-{message_seq}")
}

#[derive(Default)]
struct RawSnapshotReassembly {
    messages: BTreeMap<String, BroadcasterProtocolMessage>,
}

impl RawSnapshotReassembly {
    fn reset(&mut self) {
        self.messages.clear();
    }

    fn push(&mut self, message: BroadcasterProtocolMessage) -> Result<()> {
        match self.messages.entry(message.protocol.clone()) {
            BTreeEntry::Vacant(entry) => {
                entry.insert(message);
            }
            BTreeEntry::Occupied(mut entry) => {
                merge_snapshot_protocol_message(entry.get_mut(), message)?;
            }
        }
        Ok(())
    }

    fn take_messages(&mut self) -> Vec<BroadcasterProtocolMessage> {
        std::mem::take(&mut self.messages).into_values().collect()
    }
}

fn merge_snapshot_protocol_message(
    existing: &mut BroadcasterProtocolMessage,
    incoming: BroadcasterProtocolMessage,
) -> Result<()> {
    if existing.protocol != incoming.protocol {
        return Err(anyhow!(
            "broadcaster snapshot protocol mismatch: expected {}, got {}",
            existing.protocol,
            incoming.protocol
        ));
    }

    ensure_raw_snapshot_fragment_identity(existing, &incoming)?;
    ensure_raw_snapshot_fragment_conflicts(existing, &incoming)?;

    let mut merged_vm_storage = std::mem::take(&mut existing.message.snapshots.vm_storage);
    let mut incoming_message = incoming.message;
    let incoming_vm_storage = std::mem::take(&mut incoming_message.snapshots.vm_storage);
    merge_vm_storage(&mut merged_vm_storage, incoming_vm_storage)?;

    let mut merged_message = existing.message.clone().merge(incoming_message);
    merged_message.snapshots.vm_storage = merged_vm_storage;
    existing.message = merged_message;
    Ok(())
}

fn ensure_raw_snapshot_fragment_identity(
    existing: &BroadcasterProtocolMessage,
    incoming: &BroadcasterProtocolMessage,
) -> Result<()> {
    if existing.message.header != incoming.message.header {
        return Err(anyhow!(
            "broadcaster snapshot raw fragment header mismatch for protocol {}: expected {:?}, got {:?}",
            existing.protocol,
            existing.message.header,
            incoming.message.header
        ));
    }

    if existing.sync_state != incoming.sync_state {
        return Err(anyhow!(
            "broadcaster snapshot raw fragment sync_state mismatch for protocol {}: expected {:?}, got {:?}",
            existing.protocol,
            existing.sync_state,
            incoming.sync_state
        ));
    }

    Ok(())
}

fn ensure_raw_snapshot_fragment_conflicts(
    existing: &BroadcasterProtocolMessage,
    incoming: &BroadcasterProtocolMessage,
) -> Result<()> {
    ensure_no_duplicate_ids(
        &existing.protocol,
        &existing.message.snapshots.states,
        &incoming.message.snapshots.states,
        "snapshot state",
    )?;
    ensure_no_duplicate_ids(
        &existing.protocol,
        &existing.message.removed_components,
        &incoming.message.removed_components,
        "removed component",
    )?;
    ensure_no_snapshot_removal_overlap(
        &existing.protocol,
        &existing.message.snapshots.states,
        &existing.message.removed_components,
    )?;
    ensure_no_snapshot_removal_overlap(
        &existing.protocol,
        &incoming.message.snapshots.states,
        &incoming.message.removed_components,
    )?;
    ensure_no_snapshot_removal_overlap(
        &existing.protocol,
        &existing.message.snapshots.states,
        &incoming.message.removed_components,
    )?;
    ensure_no_snapshot_removal_overlap(
        &existing.protocol,
        &incoming.message.snapshots.states,
        &existing.message.removed_components,
    )?;

    Ok(())
}

fn ensure_no_duplicate_ids<Existing, Incoming>(
    protocol: &str,
    existing: &HashMap<String, Existing>,
    incoming: &HashMap<String, Incoming>,
    kind: &str,
) -> Result<()> {
    for component_id in incoming.keys() {
        if existing.contains_key(component_id) {
            return Err(anyhow!(
                "broadcaster snapshot raw fragment duplicate {kind} for protocol {protocol}: {component_id}"
            ));
        }
    }

    Ok(())
}

fn ensure_no_snapshot_removal_overlap<State, Removed>(
    protocol: &str,
    snapshots: &HashMap<String, State>,
    removals: &HashMap<String, Removed>,
) -> Result<()> {
    for component_id in snapshots.keys() {
        if removals.contains_key(component_id) {
            return Err(anyhow!(
                "broadcaster snapshot raw fragment snapshot/removal overlap for protocol {protocol}: {component_id}"
            ));
        }
    }

    Ok(())
}

fn merge_vm_storage(
    existing: &mut HashMap<Bytes, ResponseAccount>,
    incoming: HashMap<Bytes, ResponseAccount>,
) -> Result<()> {
    for (address, account) in incoming {
        match existing.entry(address.clone()) {
            HashEntry::Vacant(entry) => {
                entry.insert(account);
            }
            HashEntry::Occupied(mut entry) => {
                merge_vm_storage_account(&address, entry.get_mut(), account)?;
            }
        }
    }
    Ok(())
}

fn merge_vm_storage_account(
    address: &Bytes,
    existing: &mut ResponseAccount,
    incoming: ResponseAccount,
) -> Result<()> {
    ensure_vm_account_metadata_matches(address, existing, &incoming)?;
    for (slot, value) in incoming.slots {
        match existing.slots.entry(slot.clone()) {
            HashEntry::Vacant(entry) => {
                entry.insert(value);
            }
            HashEntry::Occupied(entry) if entry.get() == &value => {}
            HashEntry::Occupied(_) => {
                return Err(anyhow!(
                    "broadcaster snapshot VM storage slot mismatch for account {} slot {}",
                    address,
                    slot
                ));
            }
        }
    }
    Ok(())
}

#[expect(
    deprecated,
    reason = "creation_tx is deprecated but still part of the broadcaster wire DTO"
)]
fn ensure_vm_account_metadata_matches(
    address: &Bytes,
    existing: &ResponseAccount,
    incoming: &ResponseAccount,
) -> Result<()> {
    let mismatch = if existing.chain != incoming.chain {
        Some("chain")
    } else if existing.address != incoming.address {
        Some("address")
    } else if existing.title != incoming.title {
        Some("title")
    } else if existing.native_balance != incoming.native_balance {
        Some("native_balance")
    } else if existing.token_balances != incoming.token_balances {
        Some("token_balances")
    } else if existing.code != incoming.code {
        Some("code")
    } else if existing.code_hash != incoming.code_hash {
        Some("code_hash")
    } else if existing.balance_modify_tx != incoming.balance_modify_tx {
        Some("balance_modify_tx")
    } else if existing.code_modify_tx != incoming.code_modify_tx {
        Some("code_modify_tx")
    } else if existing.creation_tx != incoming.creation_tx {
        Some("creation_tx")
    } else {
        None
    };

    if let Some(field) = mismatch {
        return Err(anyhow!(
            "broadcaster snapshot VM storage metadata mismatch for account {} field {}",
            address,
            field
        ));
    }
    Ok(())
}

struct BroadcasterSubscriptionProcessor {
    expected_chain_id: u64,
    controls: BroadcasterSubscriptionControls,
    decoder: Arc<TychoStreamDecoder<BlockHeader>>,
    tracker: BroadcasterSubscriptionTracker,
    raw_snapshot: RawSnapshotReassembly,
    bootstrap_block: Option<u64>,
    bootstrap_redis_replay_boundary: Option<BroadcasterRedisReplayBoundary>,
    rebuild: Option<SubscriptionRebuildState>,
}

impl BroadcasterSubscriptionProcessor {
    #[cfg(test)]
    fn new(
        expected_chain_id: u64,
        controls: BroadcasterSubscriptionControls,
        rebuild: Option<SubscriptionRebuildState>,
    ) -> Self {
        let mut processor = Self::with_decoder(
            expected_chain_id,
            controls,
            Arc::new(TychoStreamDecoder::new()),
            rebuild,
        );
        processor.set_bootstrap_redis_replay_boundary(default_test_redis_replay_boundary());
        processor
    }

    fn with_decoder(
        expected_chain_id: u64,
        controls: BroadcasterSubscriptionControls,
        decoder: Arc<TychoStreamDecoder<BlockHeader>>,
        rebuild: Option<SubscriptionRebuildState>,
    ) -> Self {
        Self {
            expected_chain_id,
            controls,
            decoder,
            tracker: BroadcasterSubscriptionTracker::new(),
            raw_snapshot: RawSnapshotReassembly::default(),
            bootstrap_block: None,
            bootstrap_redis_replay_boundary: None,
            rebuild,
        }
    }

    fn set_bootstrap_redis_replay_boundary(&mut self, boundary: BroadcasterRedisReplayBoundary) {
        self.bootstrap_redis_replay_boundary = Some(boundary);
    }

    fn bootstrap_complete(&self) -> bool {
        matches!(
            self.tracker.state(),
            simulator_core::broadcaster::BroadcasterSubscriptionState::Live { .. }
        )
    }

    fn align_redis_replay_boundary(
        &mut self,
        boundary: &BroadcasterRedisReplayBoundary,
    ) -> Result<()> {
        self.tracker
            .align_live_replay_boundary(boundary)
            .map_err(|error| anyhow!("invalid broadcaster Redis replay boundary: {error}"))
    }

    async fn observe(&mut self, envelope: BroadcasterEnvelope) -> Result<()> {
        if let BroadcasterPayload::SnapshotStart(start) = &envelope.payload {
            self.ensure_snapshot_chain_id(start.chain_id)?;
            self.ensure_snapshot_includes_backend(start)?;
        }

        self.tracker
            .observe(&envelope)
            .map_err(|error| anyhow!("invalid broadcaster envelope: {error}"))?;

        match envelope.payload {
            BroadcasterPayload::SnapshotStart(start) => {
                self.bootstrap_block = None;
                self.raw_snapshot.reset();
                self.controls
                    .broadcaster_subscription()
                    .mark_snapshot_started(envelope.stream_id, start.snapshot_id)
                    .await;
            }
            BroadcasterPayload::SnapshotChunk(chunk) => {
                for partition in chunk.partitions {
                    if partition.backend == self.controls.backend() {
                        self.bootstrap_block = Some(partition.block_number);
                        self.buffer_snapshot_partition(partition).await?;
                    }
                }
            }
            BroadcasterPayload::SnapshotEnd(_end) => {
                self.apply_reassembled_snapshot_messages().await?;
                self.refresh_bootstrap_health().await;
                let boundary = self
                    .bootstrap_redis_replay_boundary
                    .clone()
                    .ok_or_else(|| {
                        anyhow!("HTTP snapshot completed without Redis replay boundary")
                    })?;
                self.controls
                    .broadcaster_subscription()
                    .mark_bootstrap_complete_with_redis_boundary(boundary)
                    .await;
                self.finish_rebuild().await;
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
            BroadcasterPayload::Progress(_progress) => {}
        }

        Ok(())
    }

    async fn observe_redis_delta(
        &mut self,
        entry: &BroadcasterRedisStreamEntry,
        envelope: &BroadcasterEnvelope,
    ) -> Result<()> {
        if redis_entry_scope_contains(entry, self.controls.backend()) {
            return self.observe(envelope.clone()).await;
        }

        self.tracker
            .skip_live_delta(envelope)
            .map_err(|error| anyhow!("invalid skipped broadcaster Redis envelope: {error}"))
    }

    fn ensure_snapshot_includes_backend(&self, start: &BroadcasterSnapshotStart) -> Result<()> {
        let backend = self.controls.backend();
        if !start.backends.contains(&backend) {
            return Err(anyhow!(
                "broadcaster snapshot start {} does not include {} backend",
                start.snapshot_id,
                backend
            ));
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

    async fn finish_rebuild(&mut self) {
        let Some(rebuild) = self.rebuild.take() else {
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
            return Err(anyhow!(
                "raw broadcaster snapshot messages cannot be applied without reassembly"
            ));
        }

        let update = snapshot_partition_update(partition);
        self.controls.state_store().apply_update(update).await;
        Ok(())
    }

    async fn buffer_snapshot_partition(
        &mut self,
        partition: BroadcasterSnapshotPartition,
    ) -> Result<()> {
        if partition.messages.is_empty() {
            return self.apply_snapshot_partition(partition).await;
        }
        self.ensure_raw_messages_supported()?;

        for message in partition.messages {
            self.raw_snapshot.push(message)?;
        }
        Ok(())
    }

    async fn apply_reassembled_snapshot_messages(&mut self) -> Result<()> {
        let messages = self.raw_snapshot.take_messages();
        self.apply_protocol_messages(messages).await
    }

    async fn apply_live_update_partition(
        &self,
        partition: BroadcasterUpdatePartition,
    ) -> Result<()> {
        let block_number = partition.block_number;
        if !partition.messages.is_empty() {
            self.ensure_raw_messages_supported()?;
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

    fn ensure_raw_messages_supported(&self) -> Result<()> {
        if self.controls.backend() == BroadcasterBackend::Rfq {
            return Err(anyhow!(
                "raw RFQ broadcaster messages are unsupported; expected decoded RFQ state partitions"
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
fn default_test_redis_replay_boundary() -> BroadcasterRedisReplayBoundary {
    match BroadcasterRedisReplayBoundary::new(
        "stream:test".to_string(),
        "stream-1".to_string(),
        "snapshot-1".to_string(),
        1,
        0,
    ) {
        Ok(boundary) => boundary,
        Err(error) => unreachable!("test Redis replay boundary should be valid: {error}"),
    }
}

async fn bootstrap_broadcaster_snapshot(
    client: &Client,
    broadcaster_url: &str,
    snapshot_sessions_url: &str,
    processor: &mut BroadcasterSubscriptionProcessor,
    controls: &BroadcasterSubscriptionControls,
    cfg: &StreamSupervisorConfig,
) -> Result<BroadcasterSnapshotSessionResponse> {
    let session =
        create_broadcaster_snapshot_session(client, snapshot_sessions_url, cfg.readiness_stale)
            .await?;
    processor.set_bootstrap_redis_replay_boundary(session.redis_replay_boundary.clone());
    controls.stream_health().mark_started().await;

    {
        let mut payloads = futures::stream::iter(0..session.payload_count)
            .map(|index| {
                let session = session.clone();
                async move {
                    fetch_broadcaster_snapshot_payload(
                        client,
                        broadcaster_url,
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
    broadcaster_url: &str,
    session: &BroadcasterSnapshotSessionResponse,
    index: u32,
    request_timeout: Duration,
) -> Result<BroadcasterEnvelope> {
    let payload_url = derive_broadcaster_http_url(
        broadcaster_url,
        &broadcaster_snapshot_payload_path(session.session_id, index),
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

const BROADCASTER_SNAPSHOT_SESSIONS_PATH: &str = "snapshot-sessions";

fn broadcaster_snapshot_payload_path(session_id: u64, index: u32) -> String {
    format!("{BROADCASTER_SNAPSHOT_SESSIONS_PATH}/{session_id}/payloads/{index}")
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
        return Err(anyhow!("{operation} at {url} failed with HTTP {status}"));
    }
    let body = response
        .bytes()
        .await
        .map_err(|error| anyhow!("failed to read {operation} response from {url}: {error}"))?;
    serde_json::from_slice(&body)
        .map_err(|error| anyhow!("failed to decode {operation} response from {url}: {error}"))
}

async fn handle_subscription_reset(
    controls: &BroadcasterSubscriptionControls,
    last_error: Option<String>,
    rebuild: Option<SubscriptionRebuildState>,
) -> Option<SubscriptionRebuildState> {
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
            let rebuild = begin_or_continue_vm_rebuild(vm_controls, rebuild).await;
            controls.state_store().reset().await;
            Some(rebuild)
        }
        BroadcasterSubscriptionControls::Rfq(rfq_controls) => {
            let rebuild = begin_or_continue_rfq_rebuild(rfq_controls, rebuild).await;
            controls.state_store().reset().await;
            Some(rebuild)
        }
    }
}

struct SubscriptionRebuildState {
    guard: OwnedRwLockWriteGuard<()>,
}

async fn begin_or_continue_vm_rebuild(
    controls: &VmBroadcasterSubscriptionControls,
    rebuild: Option<SubscriptionRebuildState>,
) -> SubscriptionRebuildState {
    {
        let mut vm_stream = controls.vm_stream.write().await;
        vm_stream.rebuilding = true;
        vm_stream.restart_count = vm_stream.restart_count.saturating_add(1);
        if vm_stream.rebuild_started_at.is_none() {
            vm_stream.rebuild_started_at = Some(Instant::now());
        }
    }

    if let Some(rebuild) = rebuild {
        return rebuild;
    }

    let guard = controls.simulation_rebuild_gate.clone().write_owned().await;

    if let Err(err) = SHARED_TYCHO_DB.clear() {
        warn!(
            error = %err,
            "Failed clearing TychoDB during broadcaster-driven VM rebuild"
        );
    }

    SubscriptionRebuildState { guard }
}

async fn begin_or_continue_rfq_rebuild(
    controls: &RfqBroadcasterSubscriptionControls,
    rebuild: Option<SubscriptionRebuildState>,
) -> SubscriptionRebuildState {
    if let Some(rebuild) = rebuild {
        return rebuild;
    }

    let guard = controls.simulation_rebuild_gate.clone().write_owned().await;

    SubscriptionRebuildState { guard }
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
    message: String,
    redis_gap: bool,
}

impl SubscriptionExit {
    fn error(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            redis_gap: false,
        }
    }

    fn redis_gap(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            redis_gap: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::{anyhow, Result};
    use num_bigint::BigUint;
    use redis::streams::{StreamKey, StreamReadReply};
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

    use super::{
        bootstrap_broadcaster_snapshot, coalesce_redis_replay_boundary, empty_rebuilds,
        handle_subscription_reset, mark_redis_replay_cursors,
        prepare_broadcaster_redis_subscription, redis_empty_poll_action, redis_xread_messages,
        BroadcasterSubscriptionControls, BroadcasterSubscriptionProcessor,
        NativeBroadcasterSubscriptionControls, PreparedRedisProcessor, RawSnapshotReassembly,
        RedisEmptyPollAction, RedisReplayCursor, RedisStreamInfo, RedisStreamMessage,
        VmBroadcasterSubscriptionControls,
    };
    use crate::config::MemoryConfig;
    use crate::models::state::{BroadcasterSubscriptionStatus, StateStore, VmStreamStatus};
    use crate::models::stream_health::StreamHealth;
    use crate::models::tokens::TokenStore;
    use crate::stream::StreamSupervisorConfig;
    use simulator_core::broadcaster::{
        BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterHeartbeat,
        BroadcasterPayload, BroadcasterProtocolMessage, BroadcasterProtocolSyncStatus,
        BroadcasterRedisReplayBoundary, BroadcasterRedisStreamEntry, BroadcasterSnapshotChunk,
        BroadcasterSnapshotEnd, BroadcasterSnapshotPartition, BroadcasterSnapshotStart,
        BroadcasterStateDelta, BroadcasterStateEntry, BroadcasterUpdateMessage,
        BroadcasterUpdatePartition,
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

    #[test]
    fn redis_replay_boundary_for_process_uses_earliest_backend_boundary() -> Result<()> {
        let boundary = coalesce_redis_replay_boundary(vec![
            replay_boundary(5)?,
            replay_boundary(3)?,
            replay_boundary(7)?,
        ])?;

        assert_eq!(boundary.exclusive_message_seq, 3);
        assert_eq!(boundary.exclusive_entry_id(), "1-3");
        Ok(())
    }

    #[test]
    fn redis_replay_boundary_rejects_mixed_generations() -> Result<()> {
        let first = replay_boundary(3)?;
        let second = BroadcasterRedisReplayBoundary::new(
            "dsolver:broadcaster:test:events",
            "stream-1",
            "snapshot-1",
            2,
            4,
        )?;

        let Err(error) = coalesce_redis_replay_boundary(vec![first, second]) else {
            return Err(anyhow!(
                "mixed Redis replay generations must not be coalesced"
            ));
        };

        assert!(error
            .to_string()
            .contains("different Redis replay boundaries"));
        Ok(())
    }

    #[test]
    fn redis_replay_cursor_detects_message_sequence_gap() -> Result<()> {
        let cursor = RedisReplayCursor::new(replay_boundary(0)?, Chain::Ethereum.id());
        let envelope = BroadcasterEnvelope::new(
            "stream-1",
            2,
            BroadcasterPayload::Heartbeat(BroadcasterHeartbeat::new(
                Chain::Ethereum.id(),
                "snapshot-1",
                vec![BroadcasterBackendHead::new(BroadcasterBackend::Native, 12)],
            )?),
        );
        let entry = BroadcasterRedisStreamEntry::from_envelope(
            Chain::Ethereum.id(),
            1_710_000_000_123,
            &envelope,
        )?;

        let Err(error) = cursor.ensure_next_message(&RedisStreamMessage {
            entry_id: "1-2".to_string(),
            entry,
        }) else {
            return Err(anyhow!(
                "message_seq gap should fail before applying the delta"
            ));
        };

        assert!(error.to_string().contains("Redis replay gap"));
        Ok(())
    }

    #[test]
    fn redis_replay_cursor_rejects_wrong_chain_id_before_applying_update() -> Result<()> {
        let cursor = RedisReplayCursor::new(replay_boundary(0)?, Chain::Ethereum.id());
        let envelope = BroadcasterEnvelope::new(
            "stream-1",
            1,
            BroadcasterPayload::Update(BroadcasterUpdateMessage::new(vec![
                BroadcasterUpdatePartition::new(
                    BroadcasterBackend::Native,
                    12,
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    BTreeMap::from([(
                        "uniswap_v2".to_string(),
                        BroadcasterProtocolSyncStatus::from_synchronizer_state(
                            &SynchronizerState::Ready(raw_block_header(12, 1)),
                        ),
                    )]),
                ),
            ])?),
        );
        let entry = BroadcasterRedisStreamEntry::from_envelope(
            Chain::Base.id(),
            1_710_000_000_123,
            &envelope,
        )?;

        let Err(error) = cursor.ensure_next_message(&RedisStreamMessage {
            entry_id: "1-1".to_string(),
            entry,
        }) else {
            return Err(anyhow!("wrong-chain Redis update should fail"));
        };

        assert!(error.to_string().contains("expected chain_id"));
        Ok(())
    }

    #[test]
    fn redis_replay_cursor_detects_generation_reset_before_duplicate_sequence() -> Result<()> {
        let cursor = RedisReplayCursor::new(replay_boundary(18)?, Chain::Ethereum.id());
        let envelope = BroadcasterEnvelope::new(
            "stream-2",
            1,
            BroadcasterPayload::Heartbeat(BroadcasterHeartbeat::new(
                Chain::Ethereum.id(),
                "snapshot-2",
                vec![BroadcasterBackendHead::new(BroadcasterBackend::Native, 12)],
            )?),
        );
        let entry = BroadcasterRedisStreamEntry::from_envelope(
            Chain::Ethereum.id(),
            1_710_000_000_123,
            &envelope,
        )?;

        let Err(error) = cursor.ensure_next_message(&RedisStreamMessage {
            entry_id: "2-1".to_string(),
            entry,
        }) else {
            return Err(anyhow!(
                "new generation with a low message_seq should fail before being skipped"
            ));
        };

        assert!(error.to_string().contains("Redis replay gap"));
        Ok(())
    }

    #[test]
    fn redis_replay_cursor_detects_empty_stream_before_latest_backend_boundary() -> Result<()> {
        let cursor = RedisReplayCursor::new(replay_boundary(3)?, Chain::Ethereum.id());

        let Err(error) = cursor.ensure_reached_required_boundary(7) else {
            return Err(anyhow!(
                "empty Redis poll before the newest backend boundary should fail closed"
            ));
        };

        assert!(error.to_string().contains("Redis replay gap"));
        assert!(error
            .to_string()
            .contains("required snapshot replay boundary 7"));
        Ok(())
    }

    #[test]
    fn empty_redis_poll_marks_caught_up_when_no_new_entry_was_generated() -> Result<()> {
        let cursor = RedisReplayCursor::new(replay_boundary(3)?, Chain::Ethereum.id());
        let stream_info = RedisStreamInfo {
            last_generated_entry_id: "1-3".to_string(),
            first_entry_id: Some("1-1".to_string()),
            last_entry_id: Some("1-3".to_string()),
        };

        let action = redis_empty_poll_action(&cursor, Some(&stream_info), 3)?;

        assert_eq!(action, RedisEmptyPollAction::CaughtUp);
        Ok(())
    }

    #[test]
    fn empty_redis_poll_detects_stream_recreation_behind_cursor() -> Result<()> {
        let cursor = RedisReplayCursor::new(replay_boundary(9)?, Chain::Ethereum.id());
        let stream_info = RedisStreamInfo {
            last_generated_entry_id: "1-2".to_string(),
            first_entry_id: Some("1-1".to_string()),
            last_entry_id: Some("1-2".to_string()),
        };

        let Err(error) = redis_empty_poll_action(&cursor, Some(&stream_info), 9) else {
            return Err(anyhow!(
                "Redis stream recreation behind the cursor should fail closed"
            ));
        };

        assert!(error.to_string().contains("Redis replay gap"));
        assert!(error.to_string().contains("moved backwards"));
        Ok(())
    }

    #[test]
    fn empty_redis_poll_detects_fully_trimmed_generated_entries() -> Result<()> {
        let cursor = RedisReplayCursor::new(replay_boundary(3)?, Chain::Ethereum.id());
        let stream_info = RedisStreamInfo {
            last_generated_entry_id: "1-5".to_string(),
            first_entry_id: None,
            last_entry_id: None,
        };

        let Err(error) = redis_empty_poll_action(&cursor, Some(&stream_info), 3) else {
            return Err(anyhow!(
                "trimmed Redis entries after the cursor should be a gap"
            ));
        };

        assert!(error.to_string().contains("Redis replay gap"));
        assert!(error.to_string().contains("retained no entries"));
        Ok(())
    }

    #[test]
    fn empty_redis_poll_detects_retention_gap_before_next_expected_entry() -> Result<()> {
        let cursor = RedisReplayCursor::new(replay_boundary(3)?, Chain::Ethereum.id());
        let stream_info = RedisStreamInfo {
            last_generated_entry_id: "1-5".to_string(),
            first_entry_id: Some("1-5".to_string()),
            last_entry_id: Some("1-5".to_string()),
        };

        let Err(error) = redis_empty_poll_action(&cursor, Some(&stream_info), 3) else {
            return Err(anyhow!(
                "first retained Redis entry after the expected cursor should be a gap"
            ));
        };

        assert!(error.to_string().contains("Redis replay gap"));
        assert!(error.to_string().contains("first retained entry 1-5"));
        Ok(())
    }

    #[test]
    fn empty_redis_poll_waits_when_new_entry_arrives_after_timeout() -> Result<()> {
        let cursor = RedisReplayCursor::new(replay_boundary(3)?, Chain::Ethereum.id());
        let stream_info = RedisStreamInfo {
            last_generated_entry_id: "1-4".to_string(),
            first_entry_id: Some("1-4".to_string()),
            last_entry_id: Some("1-4".to_string()),
        };

        let action = redis_empty_poll_action(&cursor, Some(&stream_info), 3)?;

        assert_eq!(action, RedisEmptyPollAction::Pending);
        Ok(())
    }

    #[test]
    fn redis_xread_timeout_returns_empty_poll() -> Result<()> {
        let timeout: redis::RedisError = std::io::Error::from(std::io::ErrorKind::TimedOut).into();

        let messages =
            redis_xread_messages("stream", Err(timeout)).map_err(|error| anyhow!("{error:?}"))?;

        assert!(messages.is_empty());
        Ok(())
    }

    #[test]
    fn redis_xread_broken_pipe_includes_command_context() -> Result<()> {
        let broken_pipe: redis::RedisError =
            std::io::Error::from(std::io::ErrorKind::BrokenPipe).into();

        let error = redis_xread_messages("stream", Err(broken_pipe))
            .err()
            .ok_or_else(|| anyhow!("broken pipe should fail"))?;

        assert!(format!("{error:#}").contains("Redis XREAD failed"));
        Ok(())
    }

    #[test]
    fn redis_xread_malformed_reply_reports_stream_key_mismatch() -> Result<()> {
        let reply = StreamReadReply {
            keys: vec![StreamKey {
                key: "other-stream".to_string(),
                ids: Vec::new(),
            }],
        };

        let error = redis_xread_messages("stream", Ok(Some(reply)))
            .err()
            .ok_or_else(|| anyhow!("malformed stream reply should fail"))?;

        assert!(error.to_string().contains("expected stream"));
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
    async fn redis_replay_status_cursor_does_not_move_behind_backend_boundary() -> Result<()> {
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

        mark_redis_replay_cursors(&processors, "1-5", 5).await;

        assert_eq!(
            controls
                .native_subscription
                .snapshot()
                .await
                .redis_catch_up_cursor
                .as_deref(),
            Some("1-5")
        );
        assert_eq!(
            controls
                .vm_subscription
                .snapshot()
                .await
                .redis_catch_up_cursor
                .as_deref(),
            Some("1-7")
        );
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
            event_time_ms: 1_710_000_000_123,
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
        Ok(BroadcasterEnvelope::new(
            "stream-1",
            message_seq,
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
            format!("http://{addr}"),
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
                    "redisReplayBoundary": {
                        "streamKey": "dsolver:broadcaster:test:events",
                        "streamId": "stream-1",
                        "snapshotId": "snapshot-1",
                        "generation": 1,
                        "exclusiveMessageSeq": 0
                    },
                    "payloadCount": payloads.len(),
                    "snapshotChunkCount": payloads
                        .iter()
                        .filter(|payload| payload.contains("\"kind\":\"snapshot_chunk\""))
                        .count(),
                    "redisReplayBoundary": {
                        "streamKey": "dsolver:broadcaster:test:1:events",
                        "streamId": "stream-1",
                        "snapshotId": "snapshot-1",
                        "generation": 1,
                        "exclusiveMessageSeq": 0
                    },
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
        let (broadcaster_url, snapshot_sessions_url, server_task) =
            spawn_snapshot_session_server(payloads).await?;

        let session = bootstrap_broadcaster_snapshot(
            &Client::new(),
            &broadcaster_url,
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
    async fn prepare_redis_subscription_retries_all_backends_when_snapshot_omits_rfq_backend(
    ) -> Result<()> {
        let controls = TestControls::new();
        let native_controls = controls.native();
        let rfq_controls = controls.rfq();
        let payloads = vec![
            snapshot_start_envelope()?,
            empty_snapshot_chunk_envelope()?,
            snapshot_end_envelope(),
        ];
        let (broadcaster_url, _snapshot_sessions_url, server_task) =
            spawn_snapshot_session_server(payloads).await?;

        let Err(error) = prepare_broadcaster_redis_subscription(
            &Client::new(),
            &broadcaster_url,
            Chain::Ethereum.id(),
            &test_supervisor_config(),
            &[native_controls, rfq_controls],
            empty_rebuilds(2),
        )
        .await
        else {
            return Err(anyhow!(
                "RFQ snapshot omission should abort this supervisor iteration"
            ));
        };
        server_task.abort();

        assert!(
            error.message.contains("does not include rfq backend"),
            "unexpected error: {}",
            error.message
        );
        assert_eq!(error.rebuilds.len(), 2);
        let rfq = controls.rfq_subscription.snapshot().await;
        assert!(!rfq.bootstrap_complete);
        assert!(
            rfq.last_error
                .as_deref()
                .is_some_and(|error| error.contains("does not include rfq backend")),
            "unexpected RFQ error: {:?}",
            rfq.last_error
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
        processor.set_bootstrap_redis_replay_boundary(super::default_test_redis_replay_boundary());
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
        let (broadcaster_url, snapshot_sessions_url, server_task) =
            spawn_snapshot_session_server(payloads).await?;

        let session = bootstrap_broadcaster_snapshot(
            &Client::new(),
            &broadcaster_url,
            &snapshot_sessions_url,
            &mut processor,
            &vm_controls,
            &test_supervisor_config(),
        )
        .await?;
        server_task.abort();

        assert_eq!(session.session_id, 7);
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

        let mut processor = BroadcasterSubscriptionProcessor::new(
            Chain::Ethereum.id(),
            controls.rfq(),
            rfq_rebuild,
        );
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

    fn raw_response_account(
        address: Bytes,
        title: &str,
        slot_values: &[(u8, u8)],
    ) -> ResponseAccount {
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
}
