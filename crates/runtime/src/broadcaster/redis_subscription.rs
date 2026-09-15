use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::keccak256;
use anyhow::{anyhow, Result};
use broadcaster_replay_client::{
    BroadcasterReplayClient, BroadcasterReplayClientError, BroadcasterReplayConfig,
    BroadcasterSnapshotSessionResponse, ReplayCheckpoint, ReplayMessage, ReplayPoll,
};
use futures::StreamExt;
use rand::Rng;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use simulator_core::broadcaster::{
    complete_broadcaster_partition_block, BroadcasterBackend, BroadcasterEnvelope,
    BroadcasterPayload, BroadcasterRecoveryCatchUp, BroadcasterRecoveryChunk,
    BroadcasterRecoveryCommit, BroadcasterRecoveryManifest, BroadcasterRedisReplayBoundary,
};

use crate::broadcaster::redis_publisher::replay_entry_encoded_size;
use crate::config::BroadcasterRedisConfig;
use crate::memory::maybe_purge_allocator;
use crate::metrics::{
    emit_broadcaster_redis_rebootstrap, emit_broadcaster_redis_transport_failure,
};
use crate::models::state::{BroadcasterSubscriptionStatus, StateStore, VmStreamStatus};
use crate::models::stream_health::StreamHealth;
use crate::models::tokens::TokenStore;
use crate::services::stream_builder::build_broadcaster_subscription_decoder;
use crate::stream::StreamSupervisorConfig;

#[cfg(test)]
mod live_vm_wire_e2e;
mod processor;
#[cfg(test)]
mod tests;

use self::processor::{
    begin_vm_recovery, finish_vm_recovery, handle_subscription_reset,
    BroadcasterSubscriptionProcessor, PreparedRedisProcessor, SubscriptionRebuildState,
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
    pub wire_backend: BroadcasterBackend,
    pub recovery_guard_held: bool,
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
            Self::Vm(controls) => controls.wire_backend,
            Self::Rfq(_) => BroadcasterBackend::Rfq,
        }
    }

    fn logical_backend(&self) -> BroadcasterBackend {
        match self {
            Self::Native(_) => BroadcasterBackend::Native,
            Self::Vm(_) => BroadcasterBackend::Vm,
            Self::Rfq(_) => BroadcasterBackend::Rfq,
        }
    }

    fn backend_label(&self) -> &'static str {
        self.logical_backend().as_str()
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

    async fn recovery_copy(&self) -> Self {
        let tokens = self.tokens();
        let token_snapshot = tokens.snapshot().await;
        match self {
            Self::Native(controls) => Self::Native(NativeBroadcasterSubscriptionControls {
                broadcaster_subscription: BroadcasterSubscriptionStatus::default(),
                state_store: Arc::new(StateStore::new_private_with_snapshot(
                    Arc::clone(&controls.tokens),
                    token_snapshot,
                )),
                stream_health: Arc::new(StreamHealth::new()),
                tokens: Arc::clone(&controls.tokens),
                protocols: controls.protocols.clone(),
            }),
            Self::Vm(controls) => Self::Vm(VmBroadcasterSubscriptionControls {
                broadcaster_subscription: BroadcasterSubscriptionStatus::default(),
                state_store: Arc::new(StateStore::new_private_with_snapshot(
                    Arc::clone(&controls.tokens),
                    token_snapshot,
                )),
                stream_health: Arc::new(StreamHealth::new()),
                tokens: Arc::clone(&controls.tokens),
                protocols: controls.protocols.clone(),
                vm_stream: Arc::clone(&controls.vm_stream),
                simulation_rebuild_gate: Arc::clone(&controls.simulation_rebuild_gate),
                wire_backend: controls.wire_backend,
                recovery_guard_held: true,
            }),
            Self::Rfq(controls) => Self::Rfq(RfqBroadcasterSubscriptionControls {
                broadcaster_subscription: BroadcasterSubscriptionStatus::default(),
                state_store: Arc::new(StateStore::new_private_with_snapshot(
                    Arc::clone(&controls.tokens),
                    token_snapshot,
                )),
                stream_health: Arc::new(StreamHealth::new()),
                tokens: Arc::clone(&controls.tokens),
                protocols: controls.protocols.clone(),
                simulation_rebuild_gate: Arc::clone(&controls.simulation_rebuild_gate),
            }),
        }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "subscription ownership and restart decisions stay in one supervisor loop"
)]
#[expect(
    clippy::cognitive_complexity,
    reason = "Connection, bootstrap and replay failures have distinct rebuild ownership and retry rules"
)]
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

        let mut prepared = match prepare_broadcaster_redis_subscription(
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
        prepared.redis_maxlen = redis_config.maxlen;

        info!(
            event = "broadcaster_backend_recovered",
            backends = ?controls
                .iter()
                .map(BroadcasterSubscriptionControls::backend_label)
                .collect::<Vec<_>>(),
            stream_key = prepared.replay_boundary.stream_key.as_str(),
            stream_id = prepared.replay_boundary.stream_id.as_str(),
            exclusive_entry_id = prepared.replay_boundary.exclusive_entry_id(),
            "Connected to broadcaster Redis subscription"
        );

        let (exit, next_rebuilds, caught_up_once) = process_broadcaster_redis_subscription(
            &replay_client,
            prepared,
            &cfg,
            &TokioRedisRetrySleeper,
        )
        .await;
        if !subscription_exit_requires_rebuild(exit.reason) {
            mark_redis_transport_failed(&controls, exit.message.clone()).await;
            error!(
                event = "broadcaster_redis_command_failed",
                error = %exit.message,
                "Broadcaster Redis subscription stopped after a permanent command failure"
            );
            return;
        }
        backoff = reset_outer_backoff_after_catch_up(backoff, &cfg, caught_up_once);
        emit_broadcaster_redis_rebootstrap(exit.reason.as_str());
        (rebuilds, backoff) = reset_redis_subscription_after_error(
            &controls,
            &cfg,
            backoff,
            next_rebuilds,
            exit.message,
            exit.reason == SubscriptionExitReason::RedisGap,
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
    recovery: Option<RecoveryAssembly>,
    redis_maxlen: Option<u64>,
}

struct RecoveryAssembly {
    manifest: BroadcasterRecoveryManifest,
    chunks: BTreeMap<u32, BroadcasterRecoveryChunk>,
    buffer_a: BTreeMap<u32, BroadcasterRecoveryCatchUp>,
    vm_rebuild: Option<SubscriptionRebuildState>,
    started_at: Instant,
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
        recovery: None,
        redis_maxlen: None,
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

#[expect(
    clippy::cognitive_complexity,
    reason = "Tail polling preserves separate transport retry, recovery application and catch-up outcomes"
)]
async fn process_broadcaster_redis_subscription(
    replay_client: &impl ReplayPollSource,
    mut prepared: PreparedBroadcasterRedisSubscription,
    cfg: &StreamSupervisorConfig,
    retry_sleeper: &impl RedisRetrySleeper,
) -> (
    SubscriptionExit,
    Vec<Option<SubscriptionRebuildState>>,
    bool,
) {
    let mut checkpoint =
        ReplayCheckpoint::new(prepared.replay_boundary.clone(), prepared.expected_chain_id);
    let mut transport_retry: Option<RedisTransportRetry> = None;
    let mut caught_up_once = false;

    loop {
        let poll = match replay_client.read_next(&checkpoint).await {
            Ok(poll) => {
                if let Some(retry) = transport_retry.take() {
                    mark_redis_transport_recovered(&prepared.processors).await;
                    info!(
                        event = "broadcaster_redis_transport_recovered",
                        attempts = retry.attempts,
                        recovery_ms = retry.started_at.elapsed().as_millis() as u64,
                        checkpoint = checkpoint.entry_id(),
                        "Broadcaster Redis transport recovered"
                    );
                }
                poll
            }
            Err(error) if redis_transport_error(&error) => {
                let retry = transport_retry.get_or_insert_with(|| RedisTransportRetry {
                    attempts: 0,
                    backoff: cfg.restart_backoff_min,
                    started_at: Instant::now(),
                });
                retry.attempts = retry.attempts.saturating_add(1);
                let message = error.to_string();
                let operation = redis_transport_operation(&error);
                emit_broadcaster_redis_transport_failure(operation);
                mark_redis_transport_error(&prepared.processors, message.clone()).await;
                let backoff_ms = jittered_backoff_ms(retry.backoff, cfg.restart_backoff_jitter_pct);
                warn!(
                    event = "broadcaster_redis_transport_retry",
                    operation,
                    attempt = retry.attempts,
                    backoff_ms,
                    checkpoint = checkpoint.entry_id(),
                    error = %message,
                    "Retrying broadcaster Redis transport"
                );
                retry_sleeper.sleep(Duration::from_millis(backoff_ms)).await;
                retry.backoff = next_backoff(retry.backoff, cfg.restart_backoff_max);
                continue;
            }
            Err(error) => {
                return (
                    replay_error_exit(error),
                    redis_processor_rebuilds(prepared.processors),
                    caught_up_once,
                );
            }
        };

        let caught_up = match poll {
            ReplayPoll::Pending => false,
            ReplayPoll::CaughtUp {
                checkpoint: caught_up_checkpoint,
            } => {
                checkpoint = caught_up_checkpoint;
                true
            }
            ReplayPoll::Batch(batch) => {
                let caught_up_after_batch = batch.caught_up_after_batch;
                if let Err(exit) =
                    apply_replay_batch(&mut prepared, &mut checkpoint, batch.items).await
                {
                    return (
                        exit,
                        redis_processor_rebuilds(prepared.processors),
                        caught_up_once,
                    );
                }
                if !caught_up_after_batch {
                    continue;
                }
                true
            }
        };
        if caught_up {
            caught_up_once = true;
            mark_redis_catch_up_checkpoints(&prepared.processors, checkpoint.entry_id()).await;
        }
        if let Some(message) = incomplete_recovery_timeout(&prepared, cfg) {
            return (
                SubscriptionExit::error(message),
                redis_processor_rebuilds(prepared.processors),
                caught_up_once,
            );
        }
    }
}

fn incomplete_recovery_timeout(
    prepared: &PreparedBroadcasterRedisSubscription,
    cfg: &StreamSupervisorConfig,
) -> Option<String> {
    prepared.recovery.as_ref().and_then(|recovery| {
        (recovery.started_at.elapsed() >= cfg.readiness_stale).then(|| {
            format!(
                "recovery {} remained incomplete at the Redis tail for {} ms",
                recovery.manifest.recovery_id,
                recovery.started_at.elapsed().as_millis()
            )
        })
    })
}

trait ReplayPollSource {
    fn read_next<'a>(
        &'a self,
        checkpoint: &'a ReplayCheckpoint,
    ) -> impl Future<Output = std::result::Result<ReplayPoll, BroadcasterReplayClientError>> + Send + 'a;
}

impl ReplayPollSource for BroadcasterReplayClient {
    async fn read_next<'a>(
        &'a self,
        checkpoint: &'a ReplayCheckpoint,
    ) -> std::result::Result<ReplayPoll, BroadcasterReplayClientError> {
        BroadcasterReplayClient::read_next(self, checkpoint).await
    }
}

trait RedisRetrySleeper {
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send;
}

struct TokioRedisRetrySleeper;

impl RedisRetrySleeper for TokioRedisRetrySleeper {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

struct RedisTransportRetry {
    attempts: u64,
    backoff: Duration,
    started_at: Instant,
}

fn redis_transport_error(error: &BroadcasterReplayClientError) -> bool {
    matches!(
        error,
        BroadcasterReplayClientError::RedisReadTransport { .. }
            | BroadcasterReplayClientError::RedisInspectTransport { .. }
    )
}

fn redis_transport_operation(error: &BroadcasterReplayClientError) -> &'static str {
    match error {
        BroadcasterReplayClientError::RedisReadTransport { .. } => "xread",
        BroadcasterReplayClientError::RedisInspectTransport { .. } => "xinfo",
        _ => "unknown",
    }
}

async fn mark_redis_transport_error(processors: &[PreparedRedisProcessor], error: String) {
    for prepared in processors {
        prepared
            .processor
            .controls
            .broadcaster_subscription()
            .mark_redis_transport_error(error.clone())
            .await;
    }
}

async fn mark_redis_transport_recovered(processors: &[PreparedRedisProcessor]) {
    for prepared in processors {
        prepared
            .processor
            .controls
            .broadcaster_subscription()
            .mark_redis_transport_recovered()
            .await;
    }
}

async fn mark_redis_transport_failed(controls: &[BroadcasterSubscriptionControls], error: String) {
    for controls in controls {
        controls
            .broadcaster_subscription()
            .mark_redis_transport_failed(error.clone())
            .await;
    }
}

fn replay_error_exit(error: BroadcasterReplayClientError) -> SubscriptionExit {
    match error {
        BroadcasterReplayClientError::RedisGap { .. } => {
            SubscriptionExit::redis_gap(error.to_string())
        }
        BroadcasterReplayClientError::RedisDecode { .. } => {
            SubscriptionExit::redis_decode(error.to_string())
        }
        BroadcasterReplayClientError::RedisRead { .. }
        | BroadcasterReplayClientError::RedisInspect { .. } => {
            SubscriptionExit::redis_command(error.to_string())
        }
        _ => SubscriptionExit::error(error.to_string()),
    }
}

async fn apply_replay_batch(
    prepared: &mut PreparedBroadcasterRedisSubscription,
    checkpoint: &mut ReplayCheckpoint,
    items: Vec<ReplayMessage>,
) -> std::result::Result<(), SubscriptionExit> {
    for message in items {
        if matches!(
            message.envelope.payload,
            BroadcasterPayload::RecoveryStart(_)
                | BroadcasterPayload::RecoveryChunk(_)
                | BroadcasterPayload::RecoveryCatchUp(_)
                | BroadcasterPayload::RecoveryCommit(_)
        ) || prepared.recovery.is_some()
        {
            apply_recovery_message(prepared, &message)
                .await
                .map_err(|error| SubscriptionExit::error(error.to_string()))?;
        } else {
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
        }
        *checkpoint = message.checkpoint_after;
        mark_redis_replay_checkpoints(
            &prepared.processors,
            checkpoint.entry_id(),
            checkpoint.last_message_seq(),
        )
        .await;
    }
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "all recovery message kinds are validated against one private assembly state"
)]
async fn apply_recovery_message(
    prepared: &mut PreparedBroadcasterRedisSubscription,
    message: &ReplayMessage,
) -> Result<()> {
    match &message.envelope.payload {
        BroadcasterPayload::RecoveryStart(start) => {
            start.manifest.validate()?;
            let expected_commit = message
                .entry
                .message_seq
                .checked_add(u64::from(start.manifest.chunk_count))
                .and_then(|seq| seq.checked_add(u64::from(start.manifest.buffer_a_count)))
                .and_then(|seq| seq.checked_add(1))
                .ok_or_else(|| anyhow!("recovery commit watermark overflow"))?;
            if expected_commit != start.manifest.commit_watermark {
                return Err(anyhow!(
                    "recovery {} commit watermark mismatch: expected {}, got {}",
                    start.manifest.recovery_id,
                    expected_commit,
                    start.manifest.commit_watermark
                ));
            }
            let mut vm_rebuild = if let Some(existing) = prepared.recovery.take() {
                if existing.manifest == start.manifest {
                    return Err(anyhow!(
                        "recovery {} repeated RecoveryStart in the stream",
                        start.manifest.recovery_id
                    ));
                }
                tracing::warn!(
                    event = "broadcaster_recovery_superseded",
                    old_recovery_id = existing.manifest.recovery_id,
                    new_recovery_id = start.manifest.recovery_id,
                    "Discarding uncommitted recovery assembly"
                );
                existing.vm_rebuild
            } else {
                None
            };

            for prepared_processor in &prepared.processors {
                if !start
                    .manifest
                    .backends
                    .contains(&prepared_processor.processor.controls.backend())
                {
                    continue;
                }
                let current_version = prepared_processor
                    .processor
                    .controls
                    .state_store()
                    .state_version()
                    .await;
                anyhow::ensure!(
                    start.manifest.target_state_version > current_version,
                    "recovery target state version {} must be newer than published {} state version {current_version}",
                    start.manifest.target_state_version,
                    prepared_processor.processor.controls.backend_label()
                );
                // Fence in-flight native readers before recovery closes subscription readiness.
                prepared_processor
                    .processor
                    .controls
                    .state_store()
                    .fence_requests()
                    .await;
                prepared_processor
                    .processor
                    .controls
                    .broadcaster_subscription()
                    .mark_recovery_started()
                    .await;
                if vm_rebuild.is_none() {
                    vm_rebuild = begin_vm_recovery(&prepared_processor.processor.controls).await;
                }
            }
            prepared.recovery = Some(RecoveryAssembly {
                manifest: start.manifest.clone(),
                chunks: BTreeMap::new(),
                buffer_a: BTreeMap::new(),
                vm_rebuild,
                started_at: Instant::now(),
            });
            Ok(())
        }
        BroadcasterPayload::RecoveryChunk(chunk) => {
            chunk.validate()?;
            let recovery = prepared
                .recovery
                .as_mut()
                .ok_or_else(|| anyhow!("recovery chunk arrived before RecoveryStart"))?;
            validate_recovery_identity(
                &recovery.manifest,
                &chunk.recovery_id,
                chunk.target_state_version,
            )?;
            if chunk.backends != recovery.manifest.backends {
                return Err(anyhow!(
                    "recovery chunk backend set does not match manifest"
                ));
            }
            let chunk_index = usize::try_from(chunk.chunk_index)?;
            if chunk_index != recovery.chunks.len() {
                return Err(anyhow!(
                    "recovery chunk arrived out of order: expected {}, got {}",
                    recovery.chunks.len(),
                    chunk.chunk_index
                ));
            }
            if chunk_index >= usize::try_from(recovery.manifest.chunk_count)? {
                return Err(anyhow!(
                    "recovery chunk index {} is outside manifest count {}",
                    chunk.chunk_index,
                    recovery.manifest.chunk_count
                ));
            }
            if chunk.encoded_bytes != recovery.manifest.chunk_encoded_bytes[chunk_index] {
                return Err(anyhow!(
                    "recovery chunk {} encoded size does not match manifest",
                    chunk.chunk_index
                ));
            }
            let actual_encoded_bytes = u64::try_from(replay_entry_encoded_size(
                &prepared.replay_boundary.stream_key,
                prepared.redis_maxlen,
                &message.entry,
            )?)?;
            if chunk.encoded_bytes != actual_encoded_bytes {
                return Err(anyhow!(
                    "recovery chunk {} encoded size does not match its Redis entry",
                    chunk.chunk_index
                ));
            }
            let digest = recovery_digest(chunk.payload_fragment.as_bytes());
            if digest != chunk.digest || digest != recovery.manifest.chunk_digests[chunk_index] {
                return Err(anyhow!(
                    "recovery chunk {} digest does not match manifest",
                    chunk.chunk_index
                ));
            }
            recovery.chunks.insert(chunk.chunk_index, chunk.clone());
            Ok(())
        }
        BroadcasterPayload::RecoveryCatchUp(catch_up) => {
            catch_up.validate()?;
            let recovery = prepared
                .recovery
                .as_mut()
                .ok_or_else(|| anyhow!("recovery catch-up arrived before RecoveryStart"))?;
            validate_recovery_identity(
                &recovery.manifest,
                &catch_up.recovery_id,
                catch_up.target_state_version,
            )?;
            if catch_up.block_index >= recovery.manifest.buffer_a_count {
                return Err(anyhow!(
                    "recovery catch-up index {} is outside manifest count {}",
                    catch_up.block_index,
                    recovery.manifest.buffer_a_count
                ));
            }
            if recovery.chunks.len() != usize::try_from(recovery.manifest.chunk_count)?
                || usize::try_from(catch_up.block_index)? != recovery.buffer_a.len()
            {
                return Err(anyhow!(
                    "recovery catch-up arrived out of order: expected {}, got {}",
                    recovery.buffer_a.len(),
                    catch_up.block_index
                ));
            }
            recovery
                .buffer_a
                .insert(catch_up.block_index, catch_up.clone());
            Ok(())
        }
        BroadcasterPayload::RecoveryCommit(commit) => {
            commit.validate()?;
            commit_recovery(
                prepared,
                message.entry.message_seq,
                message.entry.published_at_ms,
                commit,
            )
            .await
        }
        _ => Err(anyhow!(
            "ordinary broadcaster payload interleaved inside a recovery transaction"
        )),
    }
}

fn validate_recovery_identity(
    manifest: &BroadcasterRecoveryManifest,
    recovery_id: &str,
    target_state_version: u64,
) -> Result<()> {
    if recovery_id != manifest.recovery_id || target_state_version != manifest.target_state_version
    {
        return Err(anyhow!(
            "recovery identity does not match the active manifest"
        ));
    }
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "validation and atomic candidate publication must remain one commit boundary"
)]
async fn commit_recovery(
    prepared: &mut PreparedBroadcasterRedisSubscription,
    commit_message_seq: u64,
    published_at_ms: u64,
    commit: &BroadcasterRecoveryCommit,
) -> Result<()> {
    let mut recovery = prepared
        .recovery
        .take()
        .ok_or_else(|| anyhow!("RecoveryCommit arrived before RecoveryStart"))?;
    validate_recovery_identity(
        &recovery.manifest,
        &commit.recovery_id,
        commit.target_state_version,
    )?;
    if commit.backends != recovery.manifest.backends
        || commit.replacement_digest != recovery.manifest.replacement_digest
        || commit.buffer_a_count != recovery.manifest.buffer_a_count
        || commit.commit_watermark != recovery.manifest.commit_watermark
        || commit_message_seq != commit.commit_watermark
    {
        return Err(anyhow!(
            "RecoveryCommit does not match the active recovery manifest"
        ));
    }
    if recovery.chunks.len() != usize::try_from(recovery.manifest.chunk_count)?
        || recovery.buffer_a.len() != usize::try_from(recovery.manifest.buffer_a_count)?
    {
        return Err(anyhow!(
            "recovery {} committed before every declared fragment arrived",
            recovery.manifest.recovery_id
        ));
    }

    let replacement_json = recovery
        .chunks
        .values()
        .map(|chunk| chunk.payload_fragment.as_str())
        .collect::<String>();
    if replacement_json.len() != usize::try_from(recovery.manifest.replacement_encoded_bytes)?
        || recovery_digest(replacement_json.as_bytes()) != recovery.manifest.replacement_digest
    {
        return Err(anyhow!(
            "recovery {} replacement bytes do not match the manifest",
            recovery.manifest.recovery_id
        ));
    }
    let snapshot: Vec<BroadcasterEnvelope> = serde_json::from_str(&replacement_json)
        .map_err(|error| anyhow!("failed to decode recovery replacement snapshot: {error}"))?;
    let complete_native_block =
        committed_recovery_complete_native_block(&snapshot, &recovery.buffer_a);

    let boundary = BroadcasterRedisReplayBoundary::new(
        prepared.replay_boundary.stream_key.clone(),
        prepared.replay_boundary.stream_id.clone(),
        prepared.replay_boundary.snapshot_id.clone(),
        prepared.replay_boundary.generation,
        commit_message_seq,
        recovery.manifest.target_state_version,
    )?;
    let mut candidates = Vec::new();
    for (processor_index, prepared_processor) in prepared.processors.iter().enumerate() {
        let backend = prepared_processor.processor.controls.backend();
        if !recovery.manifest.backends.contains(&backend) {
            continue;
        }
        let mut candidate = prepared_processor
            .processor
            .recovery_copy(boundary.clone())
            .await;
        for envelope in &snapshot {
            candidate.observe(envelope.clone()).await?;
        }
        if !candidate.bootstrap_complete() {
            return Err(anyhow!(
                "recovery replacement did not complete {} snapshot bootstrap",
                backend
            ));
        }
        for catch_up in recovery.buffer_a.values() {
            candidate
                .apply_recovery_update(catch_up.update.clone())
                .await?;
        }
        candidates.push((
            processor_index,
            candidate.controls.logical_backend(),
            candidate.controls.state_store().pin().await,
            complete_native_block,
        ));
    }

    for (processor_index, backend, candidate, complete_native_block) in candidates {
        let block_number = candidate.current_block();
        let live = prepared
            .processors
            .get_mut(processor_index)
            .ok_or_else(|| anyhow!("recovery candidate lost its live backend"))?;
        live.processor
            .controls
            .state_store()
            .publish_candidate(candidate, recovery.manifest.target_state_version)
            .await?;
        match backend {
            BroadcasterBackend::Rfq => {
                live.processor
                    .controls
                    .stream_health()
                    .record_update(block_number)
                    .await;
            }
            BroadcasterBackend::Native => {
                if let Some(complete_native_block) = complete_native_block {
                    live.processor
                        .controls
                        .stream_health()
                        .record_progress(complete_native_block)
                        .await;
                }
            }
            BroadcasterBackend::Vm => {
                live.processor
                    .controls
                    .stream_health()
                    .record_progress(block_number)
                    .await;
            }
        }
        live.processor
            .controls
            .broadcaster_subscription()
            .mark_bootstrap_complete_with_redis_boundary(boundary.clone())
            .await;
        live.processor
            .controls
            .broadcaster_subscription()
            .record_publisher_to_consumer_delay(published_at_ms)
            .await;
    }
    for prepared_processor in &mut prepared.processors {
        prepared_processor
            .processor
            .align_redis_replay_boundary(&boundary)?;
        prepared_processor.replay_boundary = boundary.clone();
    }
    prepared.replay_boundary = boundary;

    if let Some(vm_rebuild) = recovery.vm_rebuild.take() {
        if let Some(vm) = prepared.processors.iter().find(|processor| {
            matches!(
                processor.processor.controls,
                BroadcasterSubscriptionControls::Vm(_)
            )
        }) {
            finish_vm_recovery(&vm.processor.controls, vm_rebuild).await;
        }
    }
    Ok(())
}

fn committed_recovery_complete_native_block(
    snapshot: &[BroadcasterEnvelope],
    buffer_a: &BTreeMap<u32, BroadcasterRecoveryCatchUp>,
) -> Option<u64> {
    let replacement_blocks = snapshot
        .iter()
        .flat_map(|envelope| match &envelope.payload {
            BroadcasterPayload::SnapshotChunk(chunk) => chunk
                .partitions
                .iter()
                .filter_map(|partition| {
                    (partition.backend == BroadcasterBackend::Native)
                        .then(|| {
                            complete_broadcaster_partition_block(
                                partition.block_number,
                                &partition.messages,
                                &partition.sync_statuses,
                            )
                        })
                        .flatten()
                })
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        });
    replacement_blocks
        .chain(
            buffer_a
                .values()
                .filter_map(|catch_up| catch_up.update.complete_native_block()),
        )
        .max()
}

fn recovery_digest(bytes: &[u8]) -> String {
    format!("{:x}", keccak256(bytes))
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

fn reset_outer_backoff_after_catch_up(
    current: Duration,
    cfg: &StreamSupervisorConfig,
    caught_up_once: bool,
) -> Duration {
    if caught_up_once {
        cfg.restart_backoff_min
    } else {
        current
    }
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
    reason: SubscriptionExitReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubscriptionExitReason {
    RedisCommand,
    RedisGap,
    RedisDecode,
    StateApply,
}

impl SubscriptionExitReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::RedisCommand => "redis_command",
            Self::RedisGap => "redis_gap",
            Self::RedisDecode => "redis_decode",
            Self::StateApply => "state_apply",
        }
    }
}

fn subscription_exit_requires_rebuild(reason: SubscriptionExitReason) -> bool {
    matches!(
        reason,
        SubscriptionExitReason::RedisGap
            | SubscriptionExitReason::RedisDecode
            | SubscriptionExitReason::StateApply
    )
}

impl SubscriptionExit {
    fn redis_command(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            reason: SubscriptionExitReason::RedisCommand,
        }
    }

    fn error(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            reason: SubscriptionExitReason::StateApply,
        }
    }

    fn redis_decode(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            reason: SubscriptionExitReason::RedisDecode,
        }
    }

    fn redis_gap(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            reason: SubscriptionExitReason::RedisGap,
        }
    }
}
