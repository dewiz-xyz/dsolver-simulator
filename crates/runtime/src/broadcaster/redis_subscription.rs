use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use broadcaster_replay_client::{
    BroadcasterReplayClient, BroadcasterReplayClientError, BroadcasterReplayConfig,
    BroadcasterSnapshotSessionResponse, GenerationHandoffCandidate, ReplayBatchItem,
    ReplayCheckpoint, ReplayPoll,
};
use futures::StreamExt;
use rand::Rng;
use tokio::sync::RwLock;
use tracing::{info, warn};

use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterPayload,
    BroadcasterRedisReplayBoundary,
};

use crate::config::BroadcasterRedisConfig;
use crate::memory::maybe_purge_allocator;
use crate::models::state::{BroadcasterSubscriptionStatus, StateStore, VmStreamStatus};
use crate::models::stream_health::StreamHealth;
use crate::models::tokens::TokenStore;
use crate::services::stream_builder::build_broadcaster_subscription_decoder;
use crate::stream::StreamSupervisorConfig;

mod processor;
mod snapshot;
#[cfg(test)]
mod tests;

use self::processor::{
    handle_subscription_reset, BroadcasterSubscriptionProcessor, PreparedRedisProcessor,
    SubscriptionRebuildState,
};

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

    let mut backoff = cfg.restart_backoff_min;
    let mut rebuilds = empty_rebuilds(controls.len());

    loop {
        let replay_client = match BroadcasterReplayClient::connect(BroadcasterReplayConfig {
            broadcaster_url: broadcaster_url.clone(),
            redis_url: redis_config.redis_url.clone(),
            block_ms: redis_config.block_ms,
            read_count: redis_config.read_count,
            request_timeout: cfg.readiness_stale,
        })
        .await
        {
            Ok(client) => client,
            Err(error) => {
                let message = error.to_string();
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
            &replay_client,
            expected_chain_id,
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
            process_broadcaster_redis_subscription(&replay_client, prepared).await;
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
    expected_chain_id: u64,
}

struct PendingRedisProcessor {
    index: usize,
    processor: BroadcasterSubscriptionProcessor,
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
    replay_client: &BroadcasterReplayClient,
    expected_chain_id: u64,
    controls: &[BroadcasterSubscriptionControls],
    mut rebuilds: Vec<Option<SubscriptionRebuildState>>,
) -> std::result::Result<
    PreparedBroadcasterRedisSubscription,
    PrepareBroadcasterRedisSubscriptionError,
> {
    let mut processors = Vec::with_capacity(controls.len());
    let mut last_backend_error = None;

    for (index, controls) in controls.iter().enumerate() {
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
        let processor = BroadcasterSubscriptionProcessor::with_decoder(
            expected_chain_id,
            controls.clone(),
            decoder,
            rebuild,
        );
        processors.push(PendingRedisProcessor { index, processor });
    }

    finish_prepared_broadcaster_redis_subscription(
        replay_client,
        processors,
        rebuilds,
        last_backend_error,
        expected_chain_id,
    )
    .await
}

async fn finish_prepared_broadcaster_redis_subscription(
    replay_client: &BroadcasterReplayClient,
    processors: Vec<PendingRedisProcessor>,
    rebuilds: Vec<Option<SubscriptionRebuildState>>,
    last_backend_error: Option<String>,
    expected_chain_id: u64,
) -> std::result::Result<
    PreparedBroadcasterRedisSubscription,
    PrepareBroadcasterRedisSubscriptionError,
> {
    if let Some(message) = last_backend_error {
        let rebuilds = merge_pending_processor_rebuilds(processors, rebuilds);
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

    let (processors, replay_boundary) =
        bootstrap_pending_processors(replay_client, processors, rebuilds).await?;
    let processors = processors
        .into_iter()
        .map(|pending| PreparedRedisProcessor {
            index: pending.index,
            processor: pending.processor,
            replay_boundary: replay_boundary.clone(),
        })
        .collect();

    Ok(PreparedBroadcasterRedisSubscription {
        processors,
        replay_boundary,
        expected_chain_id,
    })
}

async fn bootstrap_pending_processors(
    replay_client: &BroadcasterReplayClient,
    mut processors: Vec<PendingRedisProcessor>,
    rebuilds: Vec<Option<SubscriptionRebuildState>>,
) -> std::result::Result<
    (Vec<PendingRedisProcessor>, BroadcasterRedisReplayBoundary),
    PrepareBroadcasterRedisSubscriptionError,
> {
    let session = match replay_client.create_snapshot_session().await {
        Ok(session) => session,
        Err(error) => {
            return Err(pending_processor_bootstrap_error(
                error.to_string(),
                "broadcaster_redis_subscription_bootstrap_failed",
                "Failed to create broadcaster snapshot session",
                processors,
                rebuilds,
            ));
        }
    };

    mark_pending_processors_started(&mut processors, &session.redis_replay_boundary).await;

    {
        let mut payloads = replay_client.snapshot_payloads(&session);
        while let Some(envelope) = payloads.next().await {
            let envelope = match envelope {
                Ok(envelope) => envelope,
                Err(error) => {
                    return Err(pending_processor_bootstrap_error(
                        error.to_string(),
                        "broadcaster_redis_subscription_bootstrap_failed",
                        "Failed to fetch broadcaster snapshot payload",
                        processors,
                        rebuilds,
                    ));
                }
            };
            if let Err(error) =
                apply_snapshot_payload_to_pending_processors(&mut processors, envelope).await
            {
                return Err(pending_processor_bootstrap_error(
                    error.to_string(),
                    "broadcaster_redis_subscription_bootstrap_failed",
                    "Failed to apply broadcaster snapshot payload",
                    processors,
                    rebuilds,
                ));
            }
        }
    }

    if let Err((message, event, detail)) =
        validate_pending_processors_bootstrapped(&mut processors, &session)
    {
        return Err(pending_processor_bootstrap_error(
            message, event, detail, processors, rebuilds,
        ));
    }

    Ok((processors, session.redis_replay_boundary))
}

async fn mark_pending_processors_started(
    processors: &mut [PendingRedisProcessor],
    replay_boundary: &BroadcasterRedisReplayBoundary,
) {
    for prepared in processors {
        prepared
            .processor
            .set_bootstrap_redis_replay_boundary(replay_boundary.clone());
        prepared
            .processor
            .controls
            .stream_health()
            .mark_started()
            .await;
    }
}

async fn apply_snapshot_payload_to_pending_processors(
    processors: &mut [PendingRedisProcessor],
    envelope: BroadcasterEnvelope,
) -> Result<()> {
    for prepared in processors {
        prepared.processor.observe(envelope.clone()).await?;
    }
    Ok(())
}

fn validate_pending_processors_bootstrapped(
    processors: &mut [PendingRedisProcessor],
    session: &BroadcasterSnapshotSessionResponse,
) -> std::result::Result<(), (String, &'static str, &'static str)> {
    for prepared in processors {
        if !prepared.processor.bootstrap_complete() {
            return Err((
                format!(
                    "broadcaster HTTP snapshot session {} ended before {} bootstrap completed",
                    session.session_id,
                    prepared.processor.controls.backend_label()
                ),
                "broadcaster_redis_subscription_bootstrap_failed",
                "Failed to bootstrap broadcaster subscription from HTTP snapshot",
            ));
        }
        if let Err(error) = prepared
            .processor
            .align_redis_replay_boundary(&session.redis_replay_boundary)
        {
            return Err((
                error.to_string(),
                "broadcaster_redis_subscription_boundary_invalid",
                "Failed to align broadcaster Redis replay boundary",
            ));
        }
    }
    Ok(())
}

fn pending_processor_bootstrap_error(
    message: impl Into<String>,
    event: &'static str,
    detail: &'static str,
    processors: Vec<PendingRedisProcessor>,
    rebuilds: Vec<Option<SubscriptionRebuildState>>,
) -> PrepareBroadcasterRedisSubscriptionError {
    PrepareBroadcasterRedisSubscriptionError::new(
        message.into(),
        event,
        detail,
        merge_pending_processor_rebuilds(processors, rebuilds),
    )
}

async fn process_broadcaster_redis_subscription(
    replay_client: &BroadcasterReplayClient,
    mut prepared: PreparedBroadcasterRedisSubscription,
) -> (SubscriptionExit, Vec<Option<SubscriptionRebuildState>>) {
    let mut checkpoint =
        ReplayCheckpoint::new(prepared.replay_boundary.clone(), prepared.expected_chain_id);

    loop {
        let poll = match replay_client.read_next(&checkpoint).await {
            Ok(poll) => poll,
            Err(error) => {
                return (
                    replay_error_exit(error),
                    redis_processor_rebuilds(prepared.processors),
                );
            }
        };

        match poll {
            ReplayPoll::Pending => {}
            ReplayPoll::CaughtUp {
                checkpoint: caught_up_checkpoint,
            } => {
                mark_redis_catch_up_checkpoints(
                    &prepared.processors,
                    caught_up_checkpoint.entry_id(),
                )
                .await;
                checkpoint = caught_up_checkpoint;
            }
            ReplayPoll::Batch(batch) => {
                let caught_up_after_batch = batch.caught_up_after_batch;
                if let Err(exit) =
                    apply_replay_batch(&mut prepared, &mut checkpoint, batch.items).await
                {
                    return (exit, redis_processor_rebuilds(prepared.processors));
                }
                if caught_up_after_batch {
                    mark_redis_catch_up_checkpoints(&prepared.processors, checkpoint.entry_id())
                        .await;
                }
            }
        }
    }
}

fn replay_error_exit(error: BroadcasterReplayClientError) -> SubscriptionExit {
    if matches!(error, BroadcasterReplayClientError::RedisGap { .. }) {
        SubscriptionExit::redis_gap(error.to_string())
    } else {
        SubscriptionExit::error(error.to_string())
    }
}

async fn apply_replay_batch(
    prepared: &mut PreparedBroadcasterRedisSubscription,
    checkpoint: &mut ReplayCheckpoint,
    items: Vec<ReplayBatchItem>,
) -> std::result::Result<(), SubscriptionExit> {
    for item in items {
        match item {
            ReplayBatchItem::Message(message) => {
                for prepared_processor in &mut prepared.processors {
                    if message.entry.message_seq
                        <= prepared_processor.replay_boundary.exclusive_message_seq
                    {
                        continue;
                    }
                    prepared_processor
                        .processor
                        .observe_redis_delta(&message.entry, &message.envelope)
                        .await
                        .map_err(|error| SubscriptionExit::error(error.to_string()))?;
                }
                *checkpoint = message.checkpoint_after;
            }
            ReplayBatchItem::GenerationHandoff(candidate) => {
                continue_redis_generation_handoff(prepared, &candidate)
                    .await
                    .map_err(|error| SubscriptionExit::redis_gap(error.to_string()))?;
                *checkpoint = candidate.checkpoint_after;
            }
        }
        mark_redis_replay_checkpoints(
            &prepared.processors,
            checkpoint.entry_id(),
            checkpoint.last_message_seq(),
        )
        .await;
    }
    Ok(())
}

async fn continue_redis_generation_handoff(
    prepared: &mut PreparedBroadcasterRedisSubscription,
    candidate: &GenerationHandoffCandidate,
) -> Result<()> {
    let BroadcasterPayload::Progress(progress) = &candidate.envelope.payload else {
        return Err(anyhow!(
            "Redis replay gap: generation handoff payload is not progress"
        ));
    };

    let enabled_backends = prepared_enabled_backends(prepared);
    if progress.backends != enabled_backends {
        return Err(anyhow!(
            "Redis replay gap: handoff progress backends {:?} do not match enabled backends {:?}",
            progress.backends,
            enabled_backends
        ));
    }
    let Some(handoff) = progress.handoff.as_ref() else {
        return Err(anyhow!(
            "Redis replay gap: generation handoff marker is missing handoff proof"
        ));
    };
    ensure_handoff_base_heads_match(prepared, &handoff.base_heads).await?;

    for prepared_processor in &mut prepared.processors {
        prepared_processor
            .processor
            .continue_redis_generation_handoff(&candidate.boundary)?;
        prepared_processor.replay_boundary = candidate.boundary.clone();
        prepared_processor
            .processor
            .controls
            .broadcaster_subscription()
            .mark_redis_generation_continued(candidate.boundary.clone())
            .await;
    }
    prepared.replay_boundary = candidate.boundary.clone();
    Ok(())
}

fn prepared_enabled_backends(
    prepared: &PreparedBroadcasterRedisSubscription,
) -> Vec<BroadcasterBackend> {
    let mut backends = prepared
        .processors
        .iter()
        .map(|processor| processor.processor.controls.backend())
        .collect::<Vec<_>>();
    backends.sort();
    backends
}

async fn ensure_handoff_base_heads_match(
    prepared: &PreparedBroadcasterRedisSubscription,
    base_heads: &[BroadcasterBackendHead],
) -> Result<()> {
    let mut heads_by_backend = BTreeMap::new();
    for head in base_heads {
        heads_by_backend.insert(head.backend, head.block_number);
    }

    for prepared_processor in &prepared.processors {
        let backend = prepared_processor.processor.controls.backend();
        let Some(expected_block) = heads_by_backend.remove(&backend) else {
            return Err(anyhow!(
                "Redis replay gap: handoff base heads are missing {} backend",
                backend
            ));
        };
        let current_block = prepared_processor
            .processor
            .controls
            .state_store()
            .current_block()
            .await;
        if current_block != expected_block {
            return Err(anyhow!(
                "Redis replay gap: handoff {} base head {} does not match local block {}",
                backend,
                expected_block,
                current_block
            ));
        }
    }

    if let Some((backend, _)) = heads_by_backend.into_iter().next() {
        return Err(anyhow!(
            "Redis replay gap: handoff base heads include unexpected {} backend",
            backend
        ));
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

fn merge_pending_processor_rebuilds(
    processors: Vec<PendingRedisProcessor>,
    mut rebuilds: Vec<Option<SubscriptionRebuildState>>,
) -> Vec<Option<SubscriptionRebuildState>> {
    for pending in processors {
        rebuilds[pending.index] = pending.processor.rebuild;
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
