use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use rand::Rng;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::{sleep, timeout, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use tycho_simulation::{
    protocol::models::Update as TychoUpdate,
    tycho_client::feed::{BlockHeader, FeedMessage, HeaderLike, SynchronizerState},
};

use crate::broadcaster::redis_publisher::current_time_ms;
use crate::broadcaster::service::BroadcasterServiceState;
use crate::config::MemoryConfig;
use crate::memory::{maybe_log_memory_snapshot, maybe_purge_allocator};
use crate::models::stream_health::StreamHealth;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamKind {
    Broadcaster,
}

impl StreamKind {
    fn as_str(self) -> &'static str {
        match self {
            StreamKind::Broadcaster => "broadcaster",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamRestartReason {
    MissingBlock,
    Error,
    Advanced,
    SharedPublisherPaused,
    Stale,
    Ended,
    Stopped,
}

impl StreamRestartReason {
    fn as_str(self) -> &'static str {
        match self {
            StreamRestartReason::MissingBlock => "missing_block",
            StreamRestartReason::Error => "error",
            StreamRestartReason::Advanced => "advanced",
            StreamRestartReason::SharedPublisherPaused => "shared_publisher_paused",
            StreamRestartReason::Stale => "stale",
            StreamRestartReason::Ended => "ended",
            StreamRestartReason::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamExit {
    pub reason: StreamRestartReason,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StreamSupervisorConfig {
    pub readiness_stale: Duration,
    pub stream_stale: Duration,
    pub missing_block_burst: u64,
    pub missing_block_window: Duration,
    pub error_burst: u64,
    pub error_window: Duration,
    pub resync_grace: Duration,
    pub restart_backoff_min: Duration,
    pub restart_backoff_max: Duration,
    pub restart_backoff_jitter_pct: f64,
    pub memory: MemoryConfig,
}

#[derive(Debug, Clone)]
pub struct BroadcasterStreamControls {
    pub service: BroadcasterServiceState,
    pub stop: CancellationToken,
}

impl BroadcasterStreamControls {
    async fn wait_until_resumed(&self) -> bool {
        tokio::select! {
            biased;
            () = self.stop.cancelled() => false,
            () = self.service.wait_for_shared_publisher_resume() => true,
        }
    }

    async fn prepare_restart(&self, exit: &StreamExit, memory: MemoryConfig) -> bool {
        self.service
            .mark_stream_disconnected(exit.reason.as_str(), exit.last_error.clone())
            .await;
        if self.service.redis_publisher_is_retired().await {
            error!(
                event = "broadcaster_writer_fence_lost",
                "Broadcaster feed supervisor is terminating after writer fencing loss"
            );
            return false;
        }
        maybe_purge_allocator("broadcaster_restart", memory);
        true
    }

    async fn wait_backoff(&self, backoff: &mut Duration, backoff_ms: u64, max: Duration) -> bool {
        tokio::select! {
            biased;
            () = self.stop.cancelled() => return false,
            () = sleep(Duration::from_millis(backoff_ms)) => {}
        }
        *backoff = next_backoff(*backoff, max);
        true
    }

    async fn mark_disconnected_for_exit(&self, reason: &str, last_error: Option<String>) {
        self.service
            .mark_stream_disconnected_for_exit(reason, last_error)
            .await;
    }
}

enum StreamMessage<T> {
    Stale,
    Ended,
    Update(T),
    Error(String),
}

enum RawStreamCompletion {
    Stream(StreamExit),
    LifecycleTask(Result<(), JoinError>),
}

impl RawStreamCompletion {
    async fn finish(self, controls: &BroadcasterStreamControls) -> anyhow::Result<()> {
        match self {
            Self::LifecycleTask(result) => {
                let error = match result {
                    Ok(()) => "Tycho raw stream lifecycle task stopped unexpectedly".to_string(),
                    Err(error) => format!("Tycho raw stream lifecycle task failed: {error}"),
                };
                controls
                    .mark_disconnected_for_exit("lifecycle_task_ended", Some(error.clone()))
                    .await;
                error!(
                    event = "broadcaster_raw_feed_fatal",
                    stage = "lifecycle_task",
                    stream = StreamKind::Broadcaster.as_str(),
                    error,
                    "Raw broadcaster lifecycle task ended; terminating the process"
                );
                Err(anyhow::anyhow!(error))
            }
            Self::Stream(exit) if exit.reason == StreamRestartReason::Stopped => Ok(()),
            Self::Stream(exit) => {
                controls
                    .mark_disconnected_for_exit(exit.reason.as_str(), exit.last_error.clone())
                    .await;
                error!(
                    event = "broadcaster_raw_feed_fatal",
                    stage = "stream",
                    stream = StreamKind::Broadcaster.as_str(),
                    reason = exit.reason.as_str(),
                    last_error = exit.last_error.as_deref().unwrap_or_default(),
                    "Raw broadcaster stream ended; terminating the process"
                );
                let detail = exit
                    .last_error
                    .map(|error| format!("; {error}"))
                    .unwrap_or_default();
                Err(anyhow::anyhow!(
                    "Raw broadcaster stream ended: {}{}",
                    exit.reason.as_str(),
                    detail
                ))
            }
        }
    }
}

fn stream_exit(reason: StreamRestartReason, last_error: Option<String>) -> StreamExit {
    StreamExit { reason, last_error }
}

pub async fn process_broadcaster_stream(
    mut stream: impl futures::Stream<
            Item = Result<TychoUpdate, Box<dyn std::error::Error + Send + Sync + 'static>>,
        > + Unpin
        + Send,
    health: Arc<StreamHealth>,
    cfg: StreamSupervisorConfig,
    service: &BroadcasterServiceState,
    pause_epoch: u64,
    stop: &CancellationToken,
) -> StreamExit {
    info!(
        stream = StreamKind::Broadcaster.as_str(),
        "Starting stream processing"
    );
    health.mark_started().await;

    let mut ready_logged = false;

    loop {
        let message = tokio::select! {
            biased;
            () = stop.cancelled() => {
                return stream_exit(StreamRestartReason::Stopped, None);
            }
            () = service.wait_for_shared_publisher_pause_after(pause_epoch) => {
                return stream_exit(StreamRestartReason::SharedPublisherPaused, None);
            }
            message = next_stream_message(&mut stream, &health, &cfg) => {
                message
            }
        };
        match message {
            StreamMessage::Stale => return stream_exit(StreamRestartReason::Stale, None),
            StreamMessage::Ended => return stream_exit(StreamRestartReason::Ended, None),
            StreamMessage::Error(err_msg) => {
                if let Some(exit) =
                    handle_stream_error(StreamKind::Broadcaster, err_msg, &health, &cfg).await
                {
                    return exit;
                }
            }
            StreamMessage::Update(update) => {
                if let Some(exit) =
                    handle_broadcaster_update(update, service, &health, &cfg, &mut ready_logged)
                        .await
                {
                    return exit;
                }
            }
        }
    }
}

pub async fn process_broadcaster_raw_stream(
    mut stream: impl futures::Stream<
            Item = Result<
                FeedMessage<BlockHeader>,
                Box<dyn std::error::Error + Send + Sync + 'static>,
            >,
        > + Unpin
        + Send,
    health: Arc<StreamHealth>,
    cfg: StreamSupervisorConfig,
    service: &BroadcasterServiceState,
    pause_epoch: u64,
    stop: &CancellationToken,
) -> StreamExit {
    info!(
        stream = StreamKind::Broadcaster.as_str(),
        "Starting raw broadcaster stream processing"
    );
    health.mark_started_preserving_head().await;

    let mut ready_logged = false;

    loop {
        let message = tokio::select! {
            biased;
            () = stop.cancelled() => {
                return stream_exit(StreamRestartReason::Stopped, None);
            }
            () = service.wait_for_shared_publisher_pause_after(pause_epoch) => {
                return stream_exit(StreamRestartReason::SharedPublisherPaused, None);
            }
            message = next_broadcaster_raw_stream_message(&mut stream, &health, &cfg) => {
                message
            }
        };
        match message {
            StreamMessage::Stale => return stream_exit(StreamRestartReason::Stale, None),
            StreamMessage::Ended => return stream_exit(StreamRestartReason::Ended, None),
            StreamMessage::Error(err_msg) => {
                if let Some(exit) =
                    handle_stream_error(StreamKind::Broadcaster, err_msg, &health, &cfg).await
                {
                    return exit;
                }
            }
            StreamMessage::Update(update) => {
                if let Some(exit) =
                    handle_broadcaster_raw_update(update, service, &health, &cfg, &mut ready_logged)
                        .await
                {
                    return exit;
                }
            }
        }
    }
}

async fn next_stream_message(
    stream: &mut (impl futures::Stream<
        Item = Result<TychoUpdate, Box<dyn std::error::Error + Send + Sync + 'static>>,
    > + Unpin
              + Send),
    health: &StreamHealth,
    cfg: &StreamSupervisorConfig,
) -> StreamMessage<TychoUpdate> {
    let has_received_update = health.has_received_update().await;
    let stream_timeout = if has_received_update {
        cfg.stream_stale
    } else {
        cfg.readiness_stale
    };

    match timeout(stream_timeout, stream.next()).await {
        Err(_) => {
            log_stream_termination(
                health,
                has_received_update,
                "stream_stale",
                "Stream stale; triggering restart",
            )
            .await;
            StreamMessage::Stale
        }
        Ok(None) => {
            log_stream_termination(
                health,
                has_received_update,
                "stream_ended",
                "Stream ended unexpectedly",
            )
            .await;
            StreamMessage::Ended
        }
        Ok(Some(Ok(update))) => StreamMessage::Update(update),
        Ok(Some(Err(err))) => StreamMessage::Error(err.to_string()),
    }
}

async fn next_broadcaster_raw_stream_message(
    stream: &mut (impl futures::Stream<
        Item = Result<FeedMessage<BlockHeader>, Box<dyn std::error::Error + Send + Sync + 'static>>,
    > + Unpin
              + Send),
    health: &StreamHealth,
    cfg: &StreamSupervisorConfig,
) -> StreamMessage<FeedMessage<BlockHeader>> {
    let last_progress_age = health.last_update_age_ms().await.map(Duration::from_millis);
    let progress_age = if let Some(age) = last_progress_age {
        age
    } else {
        Duration::from_millis(health.started_age_ms().await.unwrap_or_default())
    };
    if progress_age >= cfg.readiness_stale {
        return StreamMessage::Stale;
    }
    let stream_timeout = cfg.readiness_stale.saturating_sub(progress_age);
    let has_received_update = health.has_received_update().await;

    match timeout(stream_timeout, stream.next()).await {
        Err(_) => {
            log_stream_termination(
                health,
                has_received_update,
                "stream_stale",
                "Raw broadcaster stream stale; triggering restart",
            )
            .await;
            StreamMessage::Stale
        }
        Ok(None) => {
            log_stream_termination(
                health,
                has_received_update,
                "stream_ended",
                "Raw broadcaster stream ended unexpectedly",
            )
            .await;
            StreamMessage::Ended
        }
        Ok(Some(Ok(update))) => StreamMessage::Update(update),
        Ok(Some(Err(err))) => StreamMessage::Error(err.to_string()),
    }
}

async fn log_stream_termination(
    health: &StreamHealth,
    has_received_update: bool,
    event: &'static str,
    message: &'static str,
) {
    let started_age_ms = health.started_age_ms().await.unwrap_or(0);
    let last_update_age_ms = health.last_update_age_ms().await.unwrap_or(0);
    let last_block = health.last_block().await;
    warn!(
        event,
        stream = StreamKind::Broadcaster.as_str(),
        started_age_ms,
        has_received_update,
        last_update_age_ms,
        last_block,
        "{message}"
    );
}

async fn check_advanced_state(
    has_advanced: bool,
    now: Instant,
    health: &StreamHealth,
    grace: Duration,
) -> Option<StreamExit> {
    if !has_advanced {
        health.clear_advanced().await;
        return None;
    }
    let advanced = health.record_advanced(now).await;
    if advanced.window_started {
        info!(
            event = "stream_advanced",
            stream = StreamKind::Broadcaster.as_str(),
            advanced_total = advanced.total_count,
            burst_count = advanced.burst_count,
            window_secs = grace.as_secs(),
            "Advanced synchronizer state detected"
        );
    }
    (advanced.elapsed >= grace).then(|| {
        stream_exit(
            StreamRestartReason::Advanced,
            Some("advanced_state_grace_exceeded".to_string()),
        )
    })
}

async fn log_broadcaster_ready(
    service: &BroadcasterServiceState,
    ready_logged: &mut bool,
    block_number: u64,
) {
    if *ready_logged {
        return;
    }
    let status = service.status_snapshot().await;
    if status.snapshot.ready {
        info!(
            stream = StreamKind::Broadcaster.as_str(),
            block = block_number,
            total_pairs = status.snapshot.total_states,
            "Broadcaster ready: snapshot cache is bootstrapped"
        );
        *ready_logged = true;
    }
}

async fn handle_broadcaster_update(
    update: TychoUpdate,
    service: &BroadcasterServiceState,
    health: &StreamHealth,
    cfg: &StreamSupervisorConfig,
    ready_logged: &mut bool,
) -> Option<StreamExit> {
    let now = Instant::now();
    let has_advanced = update
        .sync_states
        .values()
        .any(|state| matches!(state, SynchronizerState::Advanced(_)));

    if let Some(exit) = check_advanced_state(has_advanced, now, health, cfg.resync_grace).await {
        return Some(exit);
    }

    let block_number = update.block_number_or_timestamp;
    let new_pairs = update.new_pairs.len();
    if broadcaster_update_is_empty(&update) {
        health.record_update(block_number).await;
        debug!(
            event = "stream_noop_update",
            stream = StreamKind::Broadcaster.as_str(),
            block = block_number,
            "Ignoring empty decoded broadcaster update"
        );
        return None;
    }

    if let Err(error) = service.apply_update(&update).await {
        let error = error.to_string();
        health.set_last_error(Some(error.clone())).await;
        return Some(stream_exit(StreamRestartReason::Error, Some(error)));
    }

    health.record_progress(block_number).await;
    maybe_log_memory_snapshot(
        "broadcaster",
        "stream_update",
        Some(new_pairs),
        cfg.memory,
        false,
    );
    info!(
        event = "stream_update",
        stream = StreamKind::Broadcaster.as_str(),
        block = block_number,
        new_pairs,
        "Broadcaster update processed"
    );

    log_broadcaster_ready(service, ready_logged, block_number).await;

    None
}

fn broadcaster_update_is_empty(update: &TychoUpdate) -> bool {
    update.states.is_empty()
        && update.new_pairs.is_empty()
        && update.removed_pairs.is_empty()
        && update.sync_states.is_empty()
}

async fn handle_broadcaster_raw_update(
    update: FeedMessage<BlockHeader>,
    service: &BroadcasterServiceState,
    health: &StreamHealth,
    cfg: &StreamSupervisorConfig,
    ready_logged: &mut bool,
) -> Option<StreamExit> {
    let now = Instant::now();
    let received_at_ms = current_time_ms();
    let has_advanced = update
        .sync_states
        .values()
        .any(|state| matches!(state, SynchronizerState::Advanced(_)));

    if let Some(exit) = check_advanced_state(has_advanced, now, health, cfg.resync_grace).await {
        return Some(exit);
    }

    let block_number = broadcaster_raw_block_number(&update);
    let new_pairs = update
        .state_msgs
        .values()
        .map(|message| message.snapshots.states.len())
        .sum();
    let applied = match service.apply_feed_message_with_progress(&update).await {
        Ok(applied) => applied,
        Err(error) => {
            let error = error.to_string();
            health.set_last_error(Some(error.clone())).await;
            return Some(stream_exit(StreamRestartReason::Error, Some(error)));
        }
    };

    if !applied.published {
        return None;
    }

    if let Some(native_block) = applied.native_progress {
        health.record_progress(native_block).await;
    }
    let native_progress_block = applied.native_progress.unwrap_or_default();
    let block_timestamp_ms = applied
        .native_progress
        .and_then(|block| broadcaster_raw_block_timestamp_ms(&update, block));
    let broadcaster_processing_ms = now.elapsed().as_millis() as u64;
    let chain_to_broadcaster_receive_ms = block_timestamp_ms
        .map(|timestamp| received_at_ms.saturating_sub(timestamp))
        .unwrap_or_default();
    maybe_log_memory_snapshot(
        "broadcaster",
        "stream_update",
        Some(new_pairs),
        cfg.memory,
        false,
    );
    info!(
        event = "stream_update",
        stream = StreamKind::Broadcaster.as_str(),
        block = block_number,
        new_pairs,
        native_progress_advanced = applied.native_progress.is_some(),
        native_progress_block,
        native_timing_available = block_timestamp_ms.is_some(),
        block_timestamp_ms = block_timestamp_ms.unwrap_or_default(),
        broadcaster_received_at_ms = received_at_ms,
        chain_to_broadcaster_receive_ms,
        broadcaster_processing_ms,
        "Raw broadcaster update processed"
    );

    log_broadcaster_ready(service, ready_logged, block_number).await;

    None
}

fn broadcaster_raw_block_number(update: &FeedMessage<BlockHeader>) -> u64 {
    update
        .state_msgs
        .values()
        .map(|message| message.header.clone().block_number_or_timestamp())
        .max()
        .unwrap_or_default()
}

fn broadcaster_raw_block_timestamp_ms(
    update: &FeedMessage<BlockHeader>,
    block_number: u64,
) -> Option<u64> {
    update
        .state_msgs
        .values()
        .find(|message| message.header.number == block_number)
        .map(|message| message.header.timestamp.saturating_mul(1_000))
}

async fn handle_stream_error(
    kind: StreamKind,
    err_msg: String,
    health: &StreamHealth,
    cfg: &StreamSupervisorConfig,
) -> Option<StreamExit> {
    let error_kind = classify_stream_error(&err_msg);
    if is_missing_block_error(&err_msg) {
        health.set_last_error(Some(err_msg.clone())).await;
        let burst = health
            .record_missing_block(Instant::now(), cfg.missing_block_window)
            .await;
        if burst.window_started {
            info!(
                event = "stream_missing_block",
                stream = kind.as_str(),
                error_kind,
                error = %err_msg,
                missing_block_total = burst.total_count,
                burst_count = burst.burst_count,
                window_secs = cfg.missing_block_window.as_secs(),
                "Missing block detected"
            );
        }
        if burst.burst_count >= cfg.missing_block_burst {
            return Some(stream_exit(
                StreamRestartReason::MissingBlock,
                Some(err_msg),
            ));
        }
    } else {
        let burst = health.record_error(Instant::now(), cfg.error_window).await;
        health.set_last_error(Some(err_msg.clone())).await;
        warn!(
            stream = kind.as_str(),
            error_kind,
            error = %err_msg,
            error_total = burst.total_count,
            burst_count = burst.burst_count,
            "Stream error"
        );
        if burst.burst_count >= cfg.error_burst {
            return Some(stream_exit(StreamRestartReason::Error, Some(err_msg)));
        }
    }

    None
}

async fn build_stream_with_retry<F, Fut, S>(
    build_stream: &F,
    controls: &BroadcasterStreamControls,
    cfg: &StreamSupervisorConfig,
    backoff: &mut Duration,
) -> Option<(S, u64)>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = anyhow::Result<S>> + Send,
{
    while controls.wait_until_resumed().await {
        let pause_epoch = controls.service.shared_publisher_pause_epoch();
        let built_stream = tokio::select! {
            biased;
            () = controls.stop.cancelled() => return None,
            result = build_stream() => result,
        };
        match built_stream {
            Ok(stream) => {
                controls.service.mark_upstream_connected().await;
                return Some((stream, pause_epoch));
            }
            Err(err) => {
                controls.service.mark_build_failed(err.to_string()).await;
                warn!(
                    event = "stream_build_failed",
                    stream = StreamKind::Broadcaster.as_str(),
                    error = %err,
                    "Failed to build broadcaster stream"
                );
            }
        }
        let backoff_ms = jittered_backoff_ms(*backoff, cfg.restart_backoff_jitter_pct);
        if !controls
            .wait_backoff(backoff, backoff_ms, cfg.restart_backoff_max)
            .await
        {
            return None;
        }
    }
    None
}

pub async fn supervise_broadcaster_stream<F, Fut, S>(
    build_stream: F,
    health: Arc<StreamHealth>,
    cfg: StreamSupervisorConfig,
    controls: BroadcasterStreamControls,
) where
    F: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = anyhow::Result<S>> + Send,
    S: futures::Stream<
            Item = Result<TychoUpdate, Box<dyn std::error::Error + Send + Sync + 'static>>,
        > + Unpin
        + Send,
{
    let mut backoff = cfg.restart_backoff_min;

    loop {
        let Some((stream, pause_epoch)) =
            build_stream_with_retry(&build_stream, &controls, &cfg, &mut backoff).await
        else {
            return;
        };

        let exit = process_broadcaster_stream(
            stream,
            Arc::clone(&health),
            cfg.clone(),
            &controls.service,
            pause_epoch,
            &controls.stop,
        )
        .await;

        if exit.reason == StreamRestartReason::Stopped {
            return;
        }

        let restart_count = health.increment_restart().await;
        health.reset_bursts().await;
        let started_age_ms = health.started_age_ms().await.unwrap_or(0);
        let has_received_update = health.has_received_update().await;
        if has_received_update {
            backoff = cfg.restart_backoff_min;
        }
        let last_update_age_ms = health.last_update_age_ms().await.unwrap_or(0);
        let last_block = health.last_block().await;

        let backoff_ms = jittered_backoff_ms(backoff, cfg.restart_backoff_jitter_pct);
        warn!(
            event = "stream_restart",
            stream = StreamKind::Broadcaster.as_str(),
            reason = exit.reason.as_str(),
            restart_count,
            backoff_ms,
            started_age_ms,
            has_received_update,
            last_block,
            last_update_age_ms,
            "Restarting broadcaster stream"
        );

        if !controls.prepare_restart(&exit, cfg.memory).await {
            return;
        }

        if !controls
            .wait_backoff(&mut backoff, backoff_ms, cfg.restart_backoff_max)
            .await
        {
            return;
        }
    }
}

pub async fn run_broadcaster_raw_stream_once<F, Fut, S>(
    build_stream: F,
    health: Arc<StreamHealth>,
    cfg: StreamSupervisorConfig,
    controls: BroadcasterStreamControls,
) -> anyhow::Result<()>
where
    F: FnOnce() -> Fut + Send,
    Fut: std::future::Future<Output = anyhow::Result<(JoinHandle<()>, S)>> + Send,
    S: futures::Stream<
            Item = Result<
                FeedMessage<BlockHeader>,
                Box<dyn std::error::Error + Send + Sync + 'static>,
            >,
        > + Unpin
        + Send,
{
    if !controls.wait_until_resumed().await {
        return Ok(());
    }
    let pause_epoch = controls.service.shared_publisher_pause_epoch();
    let built_stream = tokio::select! {
        biased;
        () = controls.stop.cancelled() => return Ok(()),
        result = build_stream() => result,
    };
    let (mut lifecycle_task, stream) = match built_stream {
        Ok(built_stream) => {
            controls.service.mark_upstream_connected().await;
            built_stream
        }
        Err(error) => {
            controls.service.mark_build_failed(error.to_string()).await;
            error!(
                event = "broadcaster_raw_feed_fatal",
                stage = "build",
                stream = StreamKind::Broadcaster.as_str(),
                error = %error,
                "Raw broadcaster stream build failed; terminating the process"
            );
            return Err(error.context("Failed to build raw broadcaster stream"));
        }
    };

    let mut processing = Box::pin(process_broadcaster_raw_stream(
        stream,
        Arc::clone(&health),
        cfg,
        &controls.service,
        pause_epoch,
        &controls.stop,
    ));
    let completion = tokio::select! {
        biased;
        exit = &mut processing => RawStreamCompletion::Stream(exit),
        result = &mut lifecycle_task => RawStreamCompletion::LifecycleTask(result),
    };
    drop(processing);
    completion.finish(&controls).await
}

fn is_missing_block_error(message: &str) -> bool {
    message.to_ascii_lowercase().contains("missing block")
}

fn classify_stream_error(message: &str) -> &'static str {
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("statedecodingfailure") {
        "state_decoding_failure"
    } else if lowered.contains("missingcode") {
        "missing_code"
    } else if lowered.contains("missing block") {
        "missing_block"
    } else {
        "other"
    }
}

fn next_backoff(current: Duration, max: Duration) -> Duration {
    current.saturating_mul(2).min(max)
}

fn jittered_backoff_ms(base: Duration, jitter_pct: f64) -> u64 {
    let base_ms = base.as_millis().max(1) as f64;
    let clamped = jitter_pct.clamp(0.0, 1.0);
    let jitter = rand::thread_rng().gen_range(-clamped..=clamped);
    let adjusted = (base_ms * (1.0 + jitter)).max(1.0);
    adjusted.round() as u64
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Duration;

    use futures::{
        future::BoxFuture,
        stream::{BoxStream, Pending},
        StreamExt,
    };
    use tokio::sync::{mpsc, Mutex, Notify};
    use tokio::time::{advance, timeout, Instant};
    use tokio_util::sync::CancellationToken;
    use tycho_simulation::protocol::models::Update;
    use tycho_simulation::tycho_client::feed::{
        synchronizer::{Snapshot, StateSyncMessage},
        BlockHeader, FeedMessage, SynchronizerState,
    };

    use super::{
        classify_stream_error, handle_broadcaster_update, process_broadcaster_raw_stream,
        process_broadcaster_stream, run_broadcaster_raw_stream_once, BroadcasterStreamControls,
        StreamMessage, StreamRestartReason, StreamSupervisorConfig,
    };
    use crate::broadcaster::redis_publisher::{
        BroadcasterRedisPublisher, BroadcasterRedisPublisherConfig, RedisAppendCommand,
        RedisPromotionCommand, RedisPromotionResult, RedisRenewCommand, RedisStreamWriter,
    };
    use crate::broadcaster::service::BroadcasterServiceState;
    use crate::broadcaster::state::{BroadcasterSnapshotCache, BroadcasterUpstreamState};
    use crate::broadcaster::state_history::test_state_history_runtime;
    use crate::config::MemoryConfig;
    use crate::models::stream_health::StreamHealth;
    use simulator_core::broadcaster::{BroadcasterBackend, BroadcasterBackendHead};
    use tycho_simulation::tycho_common::Bytes;

    type RawTestItem = Result<FeedMessage<BlockHeader>, Box<dyn Error + Send + Sync>>;
    type TestRawStream = BoxStream<'static, RawTestItem>;

    #[tokio::test(start_paused = true)]
    async fn decoded_and_raw_streams_keep_distinct_stale_deadlines() {
        let cfg = StreamSupervisorConfig {
            readiness_stale: Duration::from_secs(10),
            stream_stale: Duration::from_secs(2),
            ..test_supervisor_config()
        };
        let health = StreamHealth::new();
        health.mark_started().await;
        health.record_progress(42).await;
        advance(Duration::from_secs(6)).await;
        let started = Instant::now();
        let mut decoded = futures::stream::pending();
        let message = super::next_stream_message(&mut decoded, &health, &cfg).await;
        assert!(matches!(message, StreamMessage::Stale));
        assert_eq!(started.elapsed(), Duration::from_secs(2));

        // A repeated native block cannot extend the raw stream's progress deadline.
        health.record_progress(42).await;
        let mut raw = futures::stream::pending();
        let message = super::next_broadcaster_raw_stream_message(&mut raw, &health, &cfg).await;
        assert!(matches!(message, StreamMessage::Stale));
        assert_eq!(started.elapsed(), Duration::from_secs(4));
    }

    #[tokio::test(start_paused = true)]
    async fn advanced_grace_expires_at_boundary_and_clears_on_normal_update() -> anyhow::Result<()>
    {
        let service = test_service(8453, BroadcasterBackend::Native);
        let health = StreamHealth::new();
        let cfg = test_supervisor_config();
        let mut ready_logged = false;
        let mut advanced_update = native_sync_update(10);
        for state in advanced_update.sync_states.values_mut() {
            if let SynchronizerState::Ready(header) = state {
                *state = SynchronizerState::Advanced(header.clone());
            }
        }
        health.record_advanced(Instant::now()).await;
        advance(cfg.resync_grace).await;
        let exit =
            handle_broadcaster_update(advanced_update, &service, &health, &cfg, &mut ready_logged)
                .await
                .ok_or_else(|| {
                    anyhow::anyhow!("Advanced state must restart at the grace boundary")
                })?;
        assert_eq!(exit.reason, StreamRestartReason::Advanced);
        assert_eq!(
            exit.last_error.as_deref(),
            Some("advanced_state_grace_exceeded")
        );

        let exit = handle_broadcaster_update(
            Update::new(11, HashMap::new(), HashMap::new()),
            &service,
            &health,
            &cfg,
            &mut ready_logged,
        )
        .await;
        assert!(exit.is_none());
        let next_window = health.record_advanced(Instant::now()).await;
        assert!(next_window.window_started);
        assert_eq!(next_window.elapsed, Duration::ZERO);
        assert_eq!(next_window.burst_count, 1);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_stream_build_failures_back_off_without_counting_restarts() {
        let controls = BroadcasterStreamControls {
            service: test_service(8453, BroadcasterBackend::Rfq),
            stop: CancellationToken::new(),
        };
        let health = Arc::new(StreamHealth::new());
        let attempts = Mutex::new(Vec::new());
        let started = Instant::now();
        let build = || async {
            let mut attempts = attempts.lock().await;
            attempts.push(started.elapsed());
            if attempts.len() == 4 {
                controls.stop.cancel();
            }
            Err::<Pending<Result<Update, Box<dyn Error + Send + Sync>>>, _>(anyhow::anyhow!(
                "planned build failure"
            ))
        };
        super::supervise_broadcaster_stream(
            build,
            Arc::clone(&health),
            test_supervisor_config(),
            controls.clone(),
        )
        .await;
        assert_eq!(
            *attempts.lock().await,
            [
                Duration::ZERO,
                Duration::from_millis(500),
                Duration::from_millis(1500),
                Duration::from_millis(3500),
            ]
        );
        assert_eq!(health.restart_count().await, 0);
    }

    #[tokio::test]
    async fn decoded_native_publication_advances_sequence_and_health() -> anyhow::Result<()> {
        let (service, writer) = active_stream_service().await?;
        let health = StreamHealth::new();
        let cfg = test_supervisor_config();
        let mut ready_logged = false;
        for block in [10, 11] {
            let exit = handle_broadcaster_update(
                native_sync_update(block),
                &service,
                &health,
                &cfg,
                &mut ready_logged,
            )
            .await;
            assert!(exit.is_none(), "decoded publication restarted: {exit:?}");
            assert_eq!(health.last_block().await, block);
            assert!(health.has_received_update().await);
        }
        assert_eq!(*writer.appends.lock().await, [(2, Some(10)), (3, Some(11))]);
        Ok(())
    }

    #[tokio::test]
    async fn raw_native_publication_advances_sequence_and_health() -> anyhow::Result<()> {
        let (service, writer) = active_stream_service().await?;
        let health = StreamHealth::new();
        let cfg = test_supervisor_config();
        let mut ready_logged = false;
        for block in [10, 11] {
            let exit = super::handle_broadcaster_raw_update(
                native_feed(block),
                &service,
                &health,
                &cfg,
                &mut ready_logged,
            )
            .await;
            assert!(exit.is_none(), "raw publication restarted: {exit:?}");
            assert_eq!(health.last_block().await, block);
            assert!(health.has_received_update().await);
        }
        assert_eq!(*writer.appends.lock().await, [(2, Some(10)), (3, Some(11))]);
        Ok(())
    }

    fn native_feed(number: u64) -> FeedMessage<BlockHeader> {
        let header = BlockHeader {
            hash: Bytes::from(number.to_be_bytes().repeat(4)),
            number,
            parent_hash: Bytes::from((number - 1).to_be_bytes().repeat(4)),
            revert: false,
            timestamp: number * 10,
            partial_block_index: None,
        };
        FeedMessage {
            state_msgs: HashMap::from([(
                "uniswap_v2".to_string(),
                StateSyncMessage {
                    header: header.clone(),
                    snapshots: Snapshot::default(),
                    deltas: None,
                    removed_components: HashMap::new(),
                },
            )]),
            sync_states: HashMap::from([(
                "uniswap_v2".to_string(),
                SynchronizerState::Ready(header),
            )]),
        }
    }

    async fn active_stream_service(
    ) -> anyhow::Result<(BroadcasterServiceState, Arc<RecordStreamAppendsRedisWriter>)> {
        let writer = Arc::new(RecordStreamAppendsRedisWriter {
            appends: Mutex::new(Vec::new()),
        });
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            test_publisher_config(8453),
            writer.clone(),
        ));
        publisher
            .promote(
                vec![BroadcasterBackendHead::new(BroadcasterBackend::Native, 0)],
                "test_active",
            )
            .await?;
        let service = test_service_with_publisher(8453, BroadcasterBackend::Native, publisher);
        service.mark_upstream_connected().await;
        Ok((service, writer))
    }

    #[derive(Debug)]
    struct RecordStreamAppendsRedisWriter {
        appends: Mutex<Vec<(u64, Option<u64>)>>,
    }

    impl RedisStreamWriter for RecordStreamAppendsRedisWriter {
        fn promote<'a>(
            &'a self,
            _command: RedisPromotionCommand<'a>,
        ) -> BoxFuture<'a, anyhow::Result<RedisPromotionResult>> {
            Box::pin(async {
                Ok(RedisPromotionResult {
                    generation: 1,
                    entry_id: "1-1".to_string(),
                })
            })
        }

        fn append_fenced<'a>(
            &'a self,
            command: RedisAppendCommand<'a>,
        ) -> BoxFuture<'a, anyhow::Result<String>> {
            Box::pin(async move {
                self.appends
                    .lock()
                    .await
                    .push((command.entry.message_seq, command.entry.block_number));
                Ok(format!(
                    "{}-{}",
                    command.generation, command.entry.message_seq
                ))
            })
        }

        fn renew_writer<'a>(
            &'a self,
            _command: RedisRenewCommand<'a>,
        ) -> BoxFuture<'a, anyhow::Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[test]
    fn classifies_state_decoding_failure_messages() {
        let kind = classify_stream_error("StateDecodingFailure: vm:curve failed to decode");
        assert_eq!(kind, "state_decoding_failure");
    }

    #[test]
    fn classifies_missing_code_messages() {
        let kind = classify_stream_error("Bad account update: MissingCode for 0xabc");
        assert_eq!(kind, "missing_code");
    }

    #[test]
    fn classifies_missing_block_messages() {
        let kind = classify_stream_error("Missing block detected");
        assert_eq!(kind, "missing_block");
    }

    #[test]
    fn classifies_other_messages() {
        let kind = classify_stream_error("stream closed by peer");
        assert_eq!(kind, "other");
    }

    #[tokio::test]
    async fn decoded_stream_stops_while_waiting_for_updates() {
        let service = test_service(8453, BroadcasterBackend::Rfq);
        let stop = CancellationToken::new();
        stop.cancel();
        let stream =
            futures::stream::pending::<Result<Update, Box<dyn std::error::Error + Send + Sync>>>();

        let exit = process_broadcaster_stream(
            stream,
            Arc::new(StreamHealth::new()),
            test_supervisor_config(),
            &service,
            service.shared_publisher_pause_epoch(),
            &stop,
        )
        .await;

        assert_eq!(exit.reason, StreamRestartReason::Stopped);
    }

    #[tokio::test]
    async fn raw_stream_stops_while_waiting_for_updates() {
        let service = test_service(8453, BroadcasterBackend::Native);
        let stop = CancellationToken::new();
        stop.cancel();
        let stream = futures::stream::pending::<
            Result<FeedMessage<BlockHeader>, Box<dyn std::error::Error + Send + Sync>>,
        >();

        let exit = process_broadcaster_raw_stream(
            stream,
            Arc::new(StreamHealth::new()),
            test_supervisor_config(),
            &service,
            service.shared_publisher_pause_epoch(),
            &stop,
        )
        .await;

        assert_eq!(exit.reason, StreamRestartReason::Stopped);
    }

    #[tokio::test]
    async fn raw_feed_build_failure_does_not_retry_in_process() -> anyhow::Result<()> {
        let service = test_service(8453, BroadcasterBackend::Native);
        let status_service = service.clone();
        let stop = CancellationToken::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let build_calls = Arc::clone(&calls);
        let result = timeout(
            Duration::from_secs(1),
            run_broadcaster_raw_stream_once(
                move || async move {
                    build_calls.fetch_add(1, Ordering::Relaxed);
                    Err::<(tokio::task::JoinHandle<()>, TestRawStream), _>(anyhow::anyhow!(
                        "planned build failure"
                    ))
                },
                Arc::new(StreamHealth::new()),
                test_supervisor_config(),
                BroadcasterStreamControls { service, stop },
            ),
        )
        .await?;
        let Err(error) = result else {
            return Err(anyhow::anyhow!(
                "build failure should end the process feed task"
            ));
        };

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(format!("{error:#}").contains("planned build failure"));
        let status = status_service.status_snapshot().await;
        assert!(!status.upstream.connected);
        assert_eq!(
            status.upstream.last_disconnect_reason.as_deref(),
            Some("build_failed")
        );
        assert_eq!(
            status.upstream.last_error.as_deref(),
            Some("planned build failure")
        );
        Ok(())
    }

    #[tokio::test]
    async fn raw_feed_end_does_not_build_replacement_in_process() -> anyhow::Result<()> {
        let lifecycle_gate = Arc::new(Mutex::new(()));
        let _gate_guard = lifecycle_gate.lock().await;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            1024,
            BroadcasterSnapshotCache::new(8453, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            test_redis_publisher(8453),
            Arc::clone(&lifecycle_gate),
        );
        let status_service = service.clone();
        let stop = CancellationToken::new();
        let lifecycle_stop = CancellationToken::new();
        let task_lifecycle_stop = lifecycle_stop.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let build_calls = Arc::clone(&calls);

        let result = timeout(
            Duration::from_secs(1),
            run_broadcaster_raw_stream_once(
                move || async move {
                    build_calls.fetch_add(1, Ordering::Relaxed);
                    Ok((
                        tokio::spawn(async move {
                            task_lifecycle_stop.cancelled().await;
                        }),
                        futures::stream::empty().boxed(),
                    ))
                },
                Arc::new(StreamHealth::new()),
                test_supervisor_config(),
                BroadcasterStreamControls { service, stop },
            ),
        )
        .await?;
        let Err(error) = result else {
            return Err(anyhow::anyhow!(
                "ended raw stream should end the process feed task"
            ));
        };
        lifecycle_stop.cancel();

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(format!("{error:#}").contains("ended"));
        let status = status_service.status_snapshot().await;
        assert_eq!(
            status.upstream.last_disconnect_reason.as_deref(),
            Some("ended")
        );
        Ok(())
    }

    #[tokio::test]
    async fn raw_lifecycle_task_completion_ends_feed_process() -> anyhow::Result<()> {
        let service = test_service(8453, BroadcasterBackend::Native);
        let status_service = service.clone();
        let stop = CancellationToken::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let build_calls = Arc::clone(&calls);
        let (sender, receiver) = mpsc::channel::<RawTestItem>(1);

        let result = timeout(
            Duration::from_secs(1),
            run_broadcaster_raw_stream_once(
                move || async move {
                    build_calls.fetch_add(1, Ordering::Relaxed);
                    Ok((
                        tokio::spawn(async {}),
                        tokio_stream::wrappers::ReceiverStream::new(receiver).boxed(),
                    ))
                },
                Arc::new(StreamHealth::new()),
                test_supervisor_config(),
                BroadcasterStreamControls { service, stop },
            ),
        )
        .await?;
        let Err(error) = result else {
            return Err(anyhow::anyhow!(
                "lifecycle task completion should end the process feed task"
            ));
        };

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(format!("{error:#}").contains("stopped unexpectedly"));
        assert!(sender.is_closed(), "raw stream receiver should be dropped");
        let status = status_service.status_snapshot().await;
        assert_eq!(
            status.upstream.last_disconnect_reason.as_deref(),
            Some("lifecycle_task_ended")
        );
        Ok(())
    }

    #[tokio::test]
    async fn raw_feed_stop_is_clean_and_does_not_rebuild() -> anyhow::Result<()> {
        let service = test_service(8453, BroadcasterBackend::Native);
        let stop = CancellationToken::new();
        let task_stop = stop.clone();
        let lifecycle_stop = CancellationToken::new();
        let task_lifecycle_stop = lifecycle_stop.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let build_calls = Arc::clone(&calls);
        let built = Arc::new(Notify::new());
        let build_notifier = Arc::clone(&built);

        let task = tokio::spawn(run_broadcaster_raw_stream_once(
            move || async move {
                build_calls.fetch_add(1, Ordering::Relaxed);
                build_notifier.notify_one();
                Ok((
                    tokio::spawn(async move {
                        task_lifecycle_stop.cancelled().await;
                    }),
                    futures::stream::pending().boxed(),
                ))
            },
            Arc::new(StreamHealth::new()),
            test_supervisor_config(),
            BroadcasterStreamControls {
                service,
                stop: task_stop,
            },
        ));

        timeout(Duration::from_secs(1), built.notified()).await?;
        stop.cancel();
        timeout(Duration::from_secs(1), task).await???;
        lifecycle_stop.cancel();

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_finishes_current_update_before_stopping() -> anyhow::Result<()> {
        let redis_writer = Arc::new(WaitAppendRedisWriter::default());
        let (state_history, _) = test_state_history_runtime(8);
        let publisher = Arc::new(
            BroadcasterRedisPublisher::new(test_publisher_config(8453), redis_writer.clone())
                .with_state_history(Arc::clone(&state_history)),
        );
        publisher
            .promote(
                vec![BroadcasterBackendHead::new(BroadcasterBackend::Native, 0)],
                "test_active",
            )
            .await?;
        let service =
            test_service_with_publisher(8453, BroadcasterBackend::Native, Arc::clone(&publisher));
        let stop = CancellationToken::new();
        let task_stop = stop.clone();
        let task_service = service.clone();
        let task = tokio::spawn(async move {
            let stream =
                futures::stream::iter([Ok(native_sync_update(10)), Ok(native_sync_update(11))])
                    .chain(futures::stream::pending());
            process_broadcaster_stream(
                stream,
                Arc::new(StreamHealth::new()),
                test_supervisor_config(),
                &task_service,
                task_service.shared_publisher_pause_epoch(),
                &task_stop,
            )
            .await
        });

        redis_writer.started.notified().await;
        stop.cancel();
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        redis_writer.release.notify_one();

        let exit = timeout(Duration::from_secs(1), task).await??;
        assert_eq!(exit.reason, StreamRestartReason::Stopped);
        assert_eq!(state_history.handle.status().enqueued_deltas, 1);
        Ok(())
    }

    #[tokio::test]
    async fn empty_decoded_broadcaster_update_is_ignored_without_restart(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let service = BroadcasterServiceState::with_lifecycle_gate(
            1024,
            BroadcasterSnapshotCache::new(8453, vec![BroadcasterBackend::Rfq]),
            BroadcasterUpstreamState::default(),
            test_redis_publisher(8453),
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        let health = StreamHealth::new();
        let mut ready_logged = false;

        let exit = handle_broadcaster_update(
            Update::new(123, HashMap::new(), HashMap::new()),
            &service,
            &health,
            &test_supervisor_config(),
            &mut ready_logged,
        )
        .await;

        assert!(exit.is_none(), "empty RFQ updates should not restart");
        assert!(health.has_received_update().await);
        let status = service.status_snapshot().await;
        assert!(status.upstream.connected);
        assert!(status.upstream.last_error.is_none());
        assert!(!status.snapshot.ready);
        Ok(())
    }

    #[tokio::test]
    async fn broadcaster_update_returns_error_when_publisher_fails(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let publisher = Arc::new(BroadcasterRedisPublisher::new(
            BroadcasterRedisPublisherConfig {
                stream_key: "stream:test".to_string(),
                chain_id: 8453,
                append_retry_window: Duration::from_millis(10),
                maxlen: None,
                writer_lease_ttl: Duration::from_secs(30),
                recovery_max_buffered_native_blocks: 64,
            },
            Arc::new(FailAppendRedisWriter),
        ));
        publisher
            .promote(
                vec![BroadcasterBackendHead::new(BroadcasterBackend::Native, 0)],
                "active_writer_promoted",
            )
            .await?;
        let service = BroadcasterServiceState::with_lifecycle_gate(
            1024,
            BroadcasterSnapshotCache::new(8453, vec![BroadcasterBackend::Native]),
            BroadcasterUpstreamState::default(),
            publisher,
            Arc::new(Mutex::new(())),
        );
        service.mark_upstream_connected().await;
        let health = StreamHealth::new();
        let mut ready_logged = false;

        let exit = handle_broadcaster_update(
            native_sync_update(10),
            &service,
            &health,
            &test_supervisor_config(),
            &mut ready_logged,
        )
        .await
        .ok_or("publisher failure should restart the broadcaster stream")?;

        assert_eq!(exit.reason, StreamRestartReason::Error);
        Ok(())
    }

    #[derive(Debug)]
    struct StreamTestRedisWriter;

    impl RedisStreamWriter for StreamTestRedisWriter {}

    #[derive(Debug)]
    struct FailAppendRedisWriter;

    #[derive(Debug, Default)]
    struct WaitAppendRedisWriter {
        started: Notify,
        release: Notify,
    }

    impl RedisStreamWriter for FailAppendRedisWriter {
        fn promote<'a>(
            &'a self,
            _command: RedisPromotionCommand<'a>,
        ) -> futures::future::BoxFuture<'a, anyhow::Result<RedisPromotionResult>> {
            Box::pin(async move {
                Ok(RedisPromotionResult {
                    generation: 1,
                    entry_id: "1-1".to_string(),
                })
            })
        }

        fn append_fenced<'a>(
            &'a self,
            _command: RedisAppendCommand<'a>,
        ) -> futures::future::BoxFuture<'a, anyhow::Result<String>> {
            Box::pin(async move { Err(anyhow::anyhow!("planned append failure")) })
        }

        fn renew_writer<'a>(
            &'a self,
            _command: RedisRenewCommand<'a>,
        ) -> futures::future::BoxFuture<'a, anyhow::Result<()>> {
            Box::pin(async move { Ok(()) })
        }
    }

    impl RedisStreamWriter for WaitAppendRedisWriter {
        fn promote<'a>(
            &'a self,
            _command: RedisPromotionCommand<'a>,
        ) -> futures::future::BoxFuture<'a, anyhow::Result<RedisPromotionResult>> {
            Box::pin(async move {
                Ok(RedisPromotionResult {
                    generation: 1,
                    entry_id: "1-1".to_string(),
                })
            })
        }

        fn append_fenced<'a>(
            &'a self,
            command: RedisAppendCommand<'a>,
        ) -> futures::future::BoxFuture<'a, anyhow::Result<String>> {
            Box::pin(async move {
                self.started.notify_one();
                self.release.notified().await;
                Ok(format!(
                    "{}-{}",
                    command.generation, command.entry.message_seq
                ))
            })
        }

        fn renew_writer<'a>(
            &'a self,
            _command: RedisRenewCommand<'a>,
        ) -> futures::future::BoxFuture<'a, anyhow::Result<()>> {
            Box::pin(async move { Ok(()) })
        }
    }

    fn test_redis_publisher(chain_id: u64) -> Arc<BroadcasterRedisPublisher> {
        Arc::new(BroadcasterRedisPublisher::new(
            test_publisher_config(chain_id),
            Arc::new(StreamTestRedisWriter),
        ))
    }

    fn test_publisher_config(chain_id: u64) -> BroadcasterRedisPublisherConfig {
        BroadcasterRedisPublisherConfig {
            stream_key: "stream:test".to_string(),
            chain_id,
            append_retry_window: Duration::from_millis(10),
            maxlen: None,
            writer_lease_ttl: Duration::from_secs(30),
            recovery_max_buffered_native_blocks: 64,
        }
    }

    fn test_service(chain_id: u64, backend: BroadcasterBackend) -> BroadcasterServiceState {
        test_service_with_publisher(chain_id, backend, test_redis_publisher(chain_id))
    }

    fn test_service_with_publisher(
        chain_id: u64,
        backend: BroadcasterBackend,
        publisher: Arc<BroadcasterRedisPublisher>,
    ) -> BroadcasterServiceState {
        BroadcasterServiceState::with_lifecycle_gate(
            1024,
            BroadcasterSnapshotCache::new(chain_id, vec![backend]),
            BroadcasterUpstreamState::default(),
            publisher,
            Arc::new(Mutex::new(())),
        )
    }

    fn native_sync_update(block_number: u64) -> Update {
        Update::new(block_number, HashMap::new(), HashMap::new()).set_sync_states(HashMap::from([
            (
                "uniswap_v2".to_string(),
                tycho_simulation::tycho_client::feed::SynchronizerState::Ready(
                    tycho_simulation::tycho_client::feed::BlockHeader {
                        hash: Bytes::from(vec![1u8; 32]),
                        number: block_number,
                        parent_hash: Bytes::from(vec![2u8; 32]),
                        revert: false,
                        timestamp: block_number * 10,
                        partial_block_index: None,
                    },
                ),
            ),
        ]))
    }

    fn test_supervisor_config() -> StreamSupervisorConfig {
        StreamSupervisorConfig {
            readiness_stale: Duration::from_secs(120),
            stream_stale: Duration::from_secs(120),
            missing_block_burst: 3,
            missing_block_window: Duration::from_secs(60),
            error_burst: 3,
            error_window: Duration::from_secs(60),
            resync_grace: Duration::from_secs(60),
            restart_backoff_min: Duration::from_millis(500),
            restart_backoff_max: Duration::from_secs(30),
            restart_backoff_jitter_pct: 0.0,
            memory: MemoryConfig {
                purge_enabled: false,
                snapshots_enabled: false,
                snapshots_min_interval_secs: 60,
                snapshots_min_new_pairs: 1000,
                snapshots_emit_emf: false,
            },
        }
    }
}
