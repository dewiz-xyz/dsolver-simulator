use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use rand::Rng;
use simulator_core::broadcaster::wire::FeedMessage;
use tokio::time::{sleep, timeout, Instant};
use tracing::{debug, error, info, warn};
use tycho_simulation::{
    protocol::models::Update as TychoUpdate,
    tycho_client::feed::{BlockHeader, HeaderLike, SynchronizerState},
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
}

impl BroadcasterStreamControls {
    async fn mark_disconnected(&self, reason: &str, last_error: Option<String>) {
        self.service
            .mark_stream_disconnected(reason, last_error)
            .await;
    }
}

enum StreamMessage {
    Stale,
    Ended,
    Update(TychoUpdate),
    Error(String),
}

enum BroadcasterRawStreamMessage {
    Stale,
    Ended,
    Update(FeedMessage<BlockHeader>),
    Error(String),
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
) -> StreamExit {
    info!(
        stream = StreamKind::Broadcaster.as_str(),
        "Starting stream processing"
    );
    health.mark_started().await;

    let mut ready_logged = false;

    loop {
        let message = tokio::select! {
            () = service.wait_for_shared_publisher_pause_after(pause_epoch) => {
                return stream_exit(StreamRestartReason::SharedPublisherPaused, None);
            }
            message = next_stream_message(StreamKind::Broadcaster, &mut stream, &health, &cfg) => {
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
) -> StreamExit {
    info!(
        stream = StreamKind::Broadcaster.as_str(),
        "Starting raw broadcaster stream processing"
    );
    health.mark_started_preserving_head().await;

    let mut ready_logged = false;

    loop {
        let message = tokio::select! {
            () = service.wait_for_shared_publisher_pause_after(pause_epoch) => {
                return stream_exit(StreamRestartReason::SharedPublisherPaused, None);
            }
            message = next_broadcaster_raw_stream_message(&mut stream, &health, &cfg) => {
                message
            }
        };
        match message {
            BroadcasterRawStreamMessage::Stale => {
                return stream_exit(StreamRestartReason::Stale, None)
            }
            BroadcasterRawStreamMessage::Ended => {
                return stream_exit(StreamRestartReason::Ended, None)
            }
            BroadcasterRawStreamMessage::Error(err_msg) => {
                if let Some(exit) =
                    handle_stream_error(StreamKind::Broadcaster, err_msg, &health, &cfg).await
                {
                    return exit;
                }
            }
            BroadcasterRawStreamMessage::Update(update) => {
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
    kind: StreamKind,
    stream: &mut (impl futures::Stream<
        Item = Result<TychoUpdate, Box<dyn std::error::Error + Send + Sync + 'static>>,
    > + Unpin
              + Send),
    health: &StreamHealth,
    cfg: &StreamSupervisorConfig,
) -> StreamMessage {
    let has_received_update = health.has_received_update().await;
    let stream_timeout = if has_received_update {
        cfg.stream_stale
    } else {
        cfg.readiness_stale
    };

    match timeout(stream_timeout, stream.next()).await {
        Err(_) => {
            let started_age_ms = health.started_age_ms().await.unwrap_or(0);
            let last_update_age_ms = health.last_update_age_ms().await.unwrap_or(0);
            let last_block = health.last_block().await;
            warn!(
                event = "stream_stale",
                stream = kind.as_str(),
                started_age_ms,
                has_received_update,
                last_update_age_ms,
                last_block,
                "Stream stale; triggering restart"
            );
            StreamMessage::Stale
        }
        Ok(None) => {
            let started_age_ms = health.started_age_ms().await.unwrap_or(0);
            let last_update_age_ms = health.last_update_age_ms().await.unwrap_or(0);
            let last_block = health.last_block().await;
            warn!(
                event = "stream_ended",
                stream = kind.as_str(),
                started_age_ms,
                has_received_update,
                last_update_age_ms,
                last_block,
                "Stream ended unexpectedly"
            );
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
) -> BroadcasterRawStreamMessage {
    let last_progress_age = health.last_update_age_ms().await.map(Duration::from_millis);
    let progress_age = if let Some(age) = last_progress_age {
        age
    } else {
        Duration::from_millis(health.started_age_ms().await.unwrap_or_default())
    };
    if progress_age >= cfg.readiness_stale {
        return BroadcasterRawStreamMessage::Stale;
    }
    let stream_timeout = cfg.readiness_stale.saturating_sub(progress_age);
    let has_received_update = health.has_received_update().await;

    match timeout(stream_timeout, stream.next()).await {
        Err(_) => {
            let started_age_ms = health.started_age_ms().await.unwrap_or(0);
            let last_update_age_ms = health.last_update_age_ms().await.unwrap_or(0);
            let last_block = health.last_block().await;
            warn!(
                event = "stream_stale",
                stream = StreamKind::Broadcaster.as_str(),
                started_age_ms,
                has_received_update,
                last_update_age_ms,
                last_block,
                "Raw broadcaster stream stale; triggering restart"
            );
            BroadcasterRawStreamMessage::Stale
        }
        Ok(None) => {
            let started_age_ms = health.started_age_ms().await.unwrap_or(0);
            let last_update_age_ms = health.last_update_age_ms().await.unwrap_or(0);
            let last_block = health.last_block().await;
            warn!(
                event = "stream_ended",
                stream = StreamKind::Broadcaster.as_str(),
                started_age_ms,
                has_received_update,
                last_update_age_ms,
                last_block,
                "Raw broadcaster stream ended unexpectedly"
            );
            BroadcasterRawStreamMessage::Ended
        }
        Ok(Some(Ok(update))) => BroadcasterRawStreamMessage::Update(update),
        Ok(Some(Err(err))) => BroadcasterRawStreamMessage::Error(err.to_string()),
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

    if has_advanced {
        let advanced = health.record_advanced(now).await;
        if advanced.window_started {
            info!(
                event = "stream_advanced",
                stream = StreamKind::Broadcaster.as_str(),
                advanced_total = advanced.total_count,
                burst_count = advanced.burst_count,
                window_secs = cfg.resync_grace.as_secs(),
                "Advanced synchronizer state detected"
            );
        }
        if advanced.elapsed >= cfg.resync_grace {
            return Some(stream_exit(
                StreamRestartReason::Advanced,
                Some("advanced_state_grace_exceeded".to_string()),
            ));
        }
    } else {
        health.clear_advanced().await;
    }

    let block_number = update.block_number;
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

    if !*ready_logged {
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

    if has_advanced {
        let advanced = health.record_advanced(now).await;
        if advanced.window_started {
            info!(
                event = "stream_advanced",
                stream = StreamKind::Broadcaster.as_str(),
                advanced_total = advanced.total_count,
                burst_count = advanced.burst_count,
                window_secs = cfg.resync_grace.as_secs(),
                "Advanced synchronizer state detected"
            );
        }
        if advanced.elapsed >= cfg.resync_grace {
            return Some(stream_exit(
                StreamRestartReason::Advanced,
                Some("advanced_state_grace_exceeded".to_string()),
            ));
        }
    } else {
        health.clear_advanced().await;
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

    if !*ready_logged {
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
        controls.service.wait_for_shared_publisher_resume().await;
        let pause_epoch = controls.service.shared_publisher_pause_epoch();
        let stream = match build_stream().await {
            Ok(stream) => {
                controls.service.mark_upstream_connected().await;
                stream
            }
            Err(err) => {
                controls.service.mark_build_failed(err.to_string()).await;
                warn!(
                    event = "stream_build_failed",
                    stream = StreamKind::Broadcaster.as_str(),
                    error = %err,
                    "Failed to build broadcaster stream"
                );
                let backoff_ms = jittered_backoff_ms(backoff, cfg.restart_backoff_jitter_pct);
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff = next_backoff(backoff, cfg.restart_backoff_max);
                continue;
            }
        };

        let exit = process_broadcaster_stream(
            stream,
            Arc::clone(&health),
            cfg.clone(),
            &controls.service,
            pause_epoch,
        )
        .await;

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

        controls
            .mark_disconnected(exit.reason.as_str(), exit.last_error.clone())
            .await;
        if controls.service.redis_publisher_is_retired().await {
            error!(
                event = "broadcaster_writer_fence_lost",
                "Broadcaster feed supervisor is terminating after writer fencing loss"
            );
            return;
        }
        maybe_purge_allocator("broadcaster_restart", cfg.memory);

        sleep(Duration::from_millis(backoff_ms)).await;
        backoff = next_backoff(backoff, cfg.restart_backoff_max);
    }
}

pub async fn supervise_broadcaster_raw_stream<F, Fut, S>(
    build_stream: F,
    health: Arc<StreamHealth>,
    cfg: StreamSupervisorConfig,
    controls: BroadcasterStreamControls,
) where
    F: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = anyhow::Result<S>> + Send,
    S: futures::Stream<
            Item = Result<
                FeedMessage<BlockHeader>,
                Box<dyn std::error::Error + Send + Sync + 'static>,
            >,
        > + Unpin
        + Send,
{
    let mut backoff = cfg.restart_backoff_min;

    loop {
        controls.service.wait_for_shared_publisher_resume().await;
        let pause_epoch = controls.service.shared_publisher_pause_epoch();
        let stream = match build_stream().await {
            Ok(stream) => {
                controls.service.mark_upstream_connected().await;
                stream
            }
            Err(err) => {
                controls.service.mark_build_failed(err.to_string()).await;
                warn!(
                    event = "stream_build_failed",
                    stream = StreamKind::Broadcaster.as_str(),
                    error = %err,
                    "Failed to build raw broadcaster stream"
                );
                let backoff_ms = jittered_backoff_ms(backoff, cfg.restart_backoff_jitter_pct);
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff = next_backoff(backoff, cfg.restart_backoff_max);
                continue;
            }
        };

        let exit = process_broadcaster_raw_stream(
            stream,
            Arc::clone(&health),
            cfg.clone(),
            &controls.service,
            pause_epoch,
        )
        .await;

        let restart_count = health.increment_restart().await;
        health.reset_bursts().await;
        let started_age_ms = health.started_age_ms().await.unwrap_or(0);
        let has_received_update = health.has_received_update().await;
        let recovery_completed = controls
            .service
            .reconnect_backoff_can_reset(cfg.readiness_stale)
            .await;
        if recovery_completed {
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
            recovery_completed,
            last_block,
            last_update_age_ms,
            "Restarting raw broadcaster stream"
        );

        controls
            .mark_disconnected(exit.reason.as_str(), exit.last_error.clone())
            .await;
        if controls.service.redis_publisher_is_retired().await {
            error!(
                event = "broadcaster_writer_fence_lost",
                "Raw broadcaster feed supervisor is terminating after writer fencing loss"
            );
            return;
        }
        maybe_purge_allocator("broadcaster_restart", cfg.memory);

        sleep(Duration::from_millis(backoff_ms)).await;
        backoff = next_backoff(backoff, cfg.restart_backoff_max);
    }
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
    let doubled = current.saturating_mul(2);
    if doubled > max {
        max
    } else {
        doubled
    }
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
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::Mutex;
    use tycho_simulation::protocol::models::Update;

    use super::{
        classify_stream_error, handle_broadcaster_update, StreamRestartReason,
        StreamSupervisorConfig,
    };
    use crate::broadcaster::redis_publisher::{
        BroadcasterRedisPublisher, BroadcasterRedisPublisherConfig, RedisAppendCommand,
        RedisPromotionCommand, RedisPromotionResult, RedisRenewCommand, RedisStreamWriter,
    };
    use crate::broadcaster::service::BroadcasterServiceState;
    use crate::broadcaster::state::{BroadcasterSnapshotCache, BroadcasterUpstreamState};
    use crate::config::MemoryConfig;
    use crate::models::stream_health::StreamHealth;
    use simulator_core::broadcaster::{BroadcasterBackend, BroadcasterBackendHead};
    use tycho_simulation::tycho_common::Bytes;

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

    fn test_redis_publisher(chain_id: u64) -> Arc<BroadcasterRedisPublisher> {
        Arc::new(BroadcasterRedisPublisher::new(
            BroadcasterRedisPublisherConfig {
                stream_key: "stream:test".to_string(),
                chain_id,
                append_retry_window: Duration::from_millis(10),
                maxlen: None,
                writer_lease_ttl: Duration::from_secs(30),
                recovery_max_buffered_native_blocks: 64,
            },
            Arc::new(StreamTestRedisWriter),
        ))
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
