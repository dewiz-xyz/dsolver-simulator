use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use rand::Rng;
use reqwest::Client;
use tokio::sync::RwLock;
use tracing::{info, warn};

use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterEnvelope, BroadcasterRedisReplayBoundary,
};

use crate::config::BroadcasterRedisConfig;
use crate::memory::maybe_purge_allocator;
use crate::models::broadcaster_urls::derive_broadcaster_http_url;
use crate::models::state::{BroadcasterSubscriptionStatus, StateStore, VmStreamStatus};
use crate::models::stream_health::StreamHealth;
use crate::models::tokens::TokenStore;
use crate::services::stream_builder::build_broadcaster_subscription_decoder;
use crate::stream::StreamSupervisorConfig;

mod checkpoint;
mod handoff;
mod processor;
mod reader;
mod snapshot;
#[cfg(test)]
mod tests;

use self::checkpoint::{redis_empty_poll_action, RedisEmptyPollAction, RedisReplayCheckpoint};
use self::handoff::{continue_redis_generation_handoff, redis_generation_handoff_candidate};
use self::processor::{
    handle_subscription_reset, BroadcasterSubscriptionProcessor, PreparedRedisProcessor,
    SubscriptionRebuildState,
};
use self::reader::{RedisStreamMessage, TokioRedisStreamReader};
use self::snapshot::{bootstrap_broadcaster_snapshot, BROADCASTER_SNAPSHOT_SESSIONS_PATH};

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
    let mut checkpoint =
        RedisReplayCheckpoint::new(prepared.replay_boundary.clone(), prepared.expected_chain_id);

    loop {
        let messages = match reader
            .read_after(
                &prepared.replay_boundary.stream_key,
                checkpoint.entry_id(),
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
            if let Err(exit) = handle_empty_redis_poll(&reader, &checkpoint, &prepared).await {
                return (exit, redis_processor_rebuilds(prepared.processors));
            }
            continue;
        }

        if let Err(exit) = apply_redis_message_batch(&mut prepared, &mut checkpoint, messages).await
        {
            return (exit, redis_processor_rebuilds(prepared.processors));
        }

        if caught_up_after_batch {
            if let Err(error) =
                checkpoint.ensure_reached_required_boundary(prepared.required_catch_up_message_seq)
            {
                return (
                    SubscriptionExit::redis_gap(error.to_string()),
                    redis_processor_rebuilds(prepared.processors),
                );
            }
            mark_redis_catch_up_checkpoints(&prepared.processors, checkpoint.entry_id()).await;
        }
    }
}

async fn handle_empty_redis_poll(
    reader: &TokioRedisStreamReader,
    checkpoint: &RedisReplayCheckpoint,
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
        checkpoint,
        stream_info.as_ref(),
        prepared.required_catch_up_message_seq,
    ) {
        Ok(RedisEmptyPollAction::CaughtUp) => {
            mark_redis_catch_up_checkpoints(&prepared.processors, checkpoint.entry_id()).await;
        }
        Ok(RedisEmptyPollAction::Pending) => {}
        Err(error) => return Err(SubscriptionExit::redis_gap(error.to_string())),
    }
    Ok(())
}

async fn apply_redis_message_batch(
    prepared: &mut PreparedBroadcasterRedisSubscription,
    checkpoint: &mut RedisReplayCheckpoint,
    messages: Vec<RedisStreamMessage>,
) -> std::result::Result<(), SubscriptionExit> {
    let mut last_applied_entry_id = None;
    for message in messages {
        let envelope: BroadcasterEnvelope = serde_json::from_str(&message.entry.payload_json)
            .map_err(|error| {
                SubscriptionExit::error(format!(
                    "failed to decode broadcaster Redis payload: {error}"
                ))
            })?;

        if let Err(error) = checkpoint.ensure_next_message(&message) {
            if redis_generation_handoff_candidate(checkpoint, &message, &envelope) {
                continue_redis_generation_handoff(prepared, checkpoint, &message, &envelope)
                    .await
                    .map_err(|error| SubscriptionExit::redis_gap(error.to_string()))?;
                last_applied_entry_id = Some(message.entry_id);
                continue;
            }
            return Err(SubscriptionExit::redis_gap(error.to_string()));
        }

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

        checkpoint.mark_applied(&message);
        last_applied_entry_id = Some(message.entry_id);
    }
    if let Some(entry_id) = last_applied_entry_id {
        mark_redis_replay_checkpoints(&prepared.processors, &entry_id, checkpoint.last_message_seq)
            .await;
    }
    Ok(())
}

async fn mark_redis_replay_checkpoints(
    processors: &[PreparedRedisProcessor],
    entry_id: &str,
    message_seq: u64,
) {
    for prepared in processors {
        let checkpoint = if message_seq < prepared.replay_boundary.exclusive_message_seq {
            prepared.replay_boundary.exclusive_entry_id()
        } else {
            entry_id.to_string()
        };
        prepared
            .processor
            .controls
            .broadcaster_subscription()
            .mark_redis_replay_checkpoint(checkpoint)
            .await;
    }
}

async fn mark_redis_catch_up_checkpoints(
    processors: &[PreparedRedisProcessor],
    checkpoint_entry_id: &str,
) {
    for prepared in processors {
        prepared
            .processor
            .controls
            .broadcaster_subscription()
            .mark_redis_catch_up_checkpoint(checkpoint_entry_id.to_string())
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
