use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use rand::Rng;
use tokio::sync::{RwLock, Semaphore};
use tokio::time::{sleep, timeout, Instant};
use tracing::{info, warn};
use tycho_simulation::{
    evm::engine_db::SHARED_TYCHO_DB, protocol::models::Update as TychoUpdate,
    tycho_client::feed::SynchronizerState,
};

use crate::config::MemoryConfig;
use crate::memory::{maybe_log_memory_snapshot, maybe_purge_allocator};
use crate::models::{
    state::{RfqStreamStatus, StateStore, VmStreamStatus},
    stream_health::StreamHealth,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamKind {
    Native,
    Vm,
    Rfq,
}

impl StreamKind {
    fn as_str(self) -> &'static str {
        match self {
            StreamKind::Native => "native",
            StreamKind::Vm => "vm",
            StreamKind::Rfq => "rfq",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamRestartReason {
    MissingBlock,
    Error,
    Advanced,
    Stale,
    Ended,
    BuildFailed,
}

impl StreamRestartReason {
    fn as_str(self) -> &'static str {
        match self {
            StreamRestartReason::MissingBlock => "missing_block",
            StreamRestartReason::Error => "error",
            StreamRestartReason::Advanced => "advanced",
            StreamRestartReason::Stale => "stale",
            StreamRestartReason::Ended => "ended",
            StreamRestartReason::BuildFailed => "build_failed",
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

pub struct VmStreamControls {
    pub vm_stream: Arc<RwLock<VmStreamStatus>>,
    pub vm_sim_semaphore: Arc<Semaphore>,
    pub vm_sim_concurrency: u32,
}

pub struct RfqStreamControls {
    pub rfq_stream: Arc<RwLock<RfqStreamStatus>>,
    pub rfq_sim_semaphore: Arc<Semaphore>,
    pub rfq_sim_concurrency: u32,
}

enum StreamMessage {
    Stale,
    Ended,
    Update(TychoUpdate),
    Error(String),
}

fn stream_exit(reason: StreamRestartReason, last_error: Option<String>) -> StreamExit {
    StreamExit { reason, last_error }
}

pub async fn process_stream(
    kind: StreamKind,
    mut stream: impl futures::Stream<
            Item = Result<TychoUpdate, Box<dyn std::error::Error + Send + Sync + 'static>>,
        > + Unpin
        + Send,
    state_store: Arc<StateStore>,
    health: Arc<StreamHealth>,
    cfg: StreamSupervisorConfig,
) -> StreamExit {
    info!(stream = kind.as_str(), "Starting stream processing");
    health.mark_started().await;

    let mut ready_logged = false;

    loop {
        match next_stream_message(kind, &mut stream, &health, cfg.stream_stale).await {
            StreamMessage::Stale => return stream_exit(StreamRestartReason::Stale, None),
            StreamMessage::Ended => return stream_exit(StreamRestartReason::Ended, None),
            StreamMessage::Update(update) => {
                if let Some(exit) = handle_stream_update(
                    kind,
                    update,
                    &state_store,
                    &health,
                    &cfg,
                    &mut ready_logged,
                )
                .await
                {
                    return exit;
                }
            }
            StreamMessage::Error(err_msg) => {
                if let Some(exit) = handle_stream_error(kind, err_msg, &health, &cfg).await {
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
    health: &Arc<StreamHealth>,
    stream_stale: Duration,
) -> StreamMessage {
    match timeout(stream_stale, stream.next()).await {
        Err(_) => {
            let last_update_age_ms = health.last_update_age_ms().await.unwrap_or(0);
            let last_block = health.last_block().await;
            warn!(
                event = "stream_stale",
                stream = kind.as_str(),
                last_update_age_ms,
                last_block,
                "Stream stale; triggering restart"
            );
            StreamMessage::Stale
        }
        Ok(None) => {
            warn!(
                event = "stream_ended",
                stream = kind.as_str(),
                "Stream ended unexpectedly"
            );
            StreamMessage::Ended
        }
        Ok(Some(Ok(update))) => StreamMessage::Update(update),
        Ok(Some(Err(err))) => StreamMessage::Error(err.to_string()),
    }
}

async fn handle_stream_update(
    kind: StreamKind,
    update: TychoUpdate,
    state_store: &Arc<StateStore>,
    health: &Arc<StreamHealth>,
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
                stream = kind.as_str(),
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

    let metrics = state_store.apply_update(update).await;
    health.record_update(metrics.block_number).await;
    log_stream_update(kind, &metrics);
    maybe_log_memory_snapshot(
        kind.as_str(),
        "stream_update",
        Some(metrics.new_pairs),
        cfg.memory,
        false,
    );

    if !*ready_logged && metrics.total_pairs > 0 {
        info!(
            stream = kind.as_str(),
            block = metrics.block_number,
            total_pairs = metrics.total_pairs,
            "Service ready: first pools ingested"
        );
        *ready_logged = true;
    }

    None
}

fn log_stream_update(kind: StreamKind, metrics: &crate::models::state::UpdateMetrics) {
    info!(
        event = "stream_update",
        stream = kind.as_str(),
        block = metrics.block_number,
        updated_states = metrics.updated_states,
        new_pairs = metrics.new_pairs,
        removed_pairs = metrics.removed_pairs,
        total_pairs = metrics.total_pairs,
        new_pairs_missing_state = metrics.new_pairs_missing_state,
        unknown_protocol_new_pairs = metrics.unknown_protocol_new_pairs,
        updates_for_unknown_pair = metrics.updates_for_unknown_pair,
        states_missing_in_shard = metrics.states_missing_in_shard,
        removed_unknown_pair = metrics.removed_unknown_pair,
        anomaly_samples = ?metrics.anomaly_samples,
        "Stream update processed"
    );
    if metrics.has_anomalies() {
        warn!(
            event = "stream_update_anomaly",
            stream = kind.as_str(),
            block = metrics.block_number,
            new_pairs_missing_state = metrics.new_pairs_missing_state,
            unknown_protocol_new_pairs = metrics.unknown_protocol_new_pairs,
            updates_for_unknown_pair = metrics.updates_for_unknown_pair,
            states_missing_in_shard = metrics.states_missing_in_shard,
            removed_unknown_pair = metrics.removed_unknown_pair,
            anomaly_samples = ?metrics.anomaly_samples,
            "Stream update contained ingest anomalies"
        );
    }
}

async fn handle_stream_error(
    kind: StreamKind,
    err_msg: String,
    health: &Arc<StreamHealth>,
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

pub async fn supervise_native_stream<F, Fut, S>(
    build_stream: F,
    state_store: Arc<StateStore>,
    health: Arc<StreamHealth>,
    cfg: StreamSupervisorConfig,
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
        let stream = match build_stream().await {
            Ok(stream) => stream,
            Err(err) => {
                warn!(
                    event = "stream_build_failed",
                    stream = StreamKind::Native.as_str(),
                    error = %err,
                    "Failed to build native stream"
                );
                let backoff_ms = jittered_backoff_ms(backoff, cfg.restart_backoff_jitter_pct);
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff = next_backoff(backoff, cfg.restart_backoff_max);
                continue;
            }
        };

        let exit = process_stream(
            StreamKind::Native,
            stream,
            Arc::clone(&state_store),
            Arc::clone(&health),
            cfg.clone(),
        )
        .await;

        let restart_count = health.increment_restart().await;
        health.reset_bursts().await;
        let last_update_age_ms = health.last_update_age_ms().await.unwrap_or(0);
        let last_block = health.last_block().await;

        let backoff_ms = jittered_backoff_ms(backoff, cfg.restart_backoff_jitter_pct);
        warn!(
            event = "stream_restart",
            stream = StreamKind::Native.as_str(),
            reason = exit.reason.as_str(),
            restart_count,
            backoff_ms,
            last_block,
            last_update_age_ms,
            "Restarting native stream"
        );

        state_store.reset().await;
        maybe_purge_allocator("native_restart", cfg.memory);

        sleep(Duration::from_millis(backoff_ms)).await;
        backoff = next_backoff(backoff, cfg.restart_backoff_max);
    }
}

pub async fn supervise_vm_stream<F, Fut, S>(
    build_stream: F,
    state_store: Arc<StateStore>,
    health: Arc<StreamHealth>,
    cfg: StreamSupervisorConfig,
    controls: VmStreamControls,
) where
    F: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = anyhow::Result<S>> + Send,
    S: futures::Stream<
            Item = Result<TychoUpdate, Box<dyn std::error::Error + Send + Sync + 'static>>,
        > + Unpin
        + Send,
{
    let mut backoff = cfg.restart_backoff_min;
    let mut pending_rebuild: Option<VmRebuildState> = None;

    loop {
        let stream = match build_stream().await {
            Ok(stream) => {
                if let Some(rebuild) = pending_rebuild.take() {
                    finish_vm_rebuild(&controls, rebuild, cfg.memory).await;
                }
                stream
            }
            Err(err) => {
                warn!(
                    event = "stream_build_failed",
                    stream = StreamKind::Vm.as_str(),
                    error = %err,
                    "Failed to build VM stream"
                );
                let backoff_ms = jittered_backoff_ms(backoff, cfg.restart_backoff_jitter_pct);
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff = next_backoff(backoff, cfg.restart_backoff_max);
                continue;
            }
        };

        let exit = process_stream(
            StreamKind::Vm,
            stream,
            Arc::clone(&state_store),
            Arc::clone(&health),
            cfg.clone(),
        )
        .await;

        let restart_count = health.increment_restart().await;
        health.reset_bursts().await;
        let last_update_age_ms = health.last_update_age_ms().await.unwrap_or(0);
        let last_block = health.last_block().await;

        let rebuild_state = begin_vm_rebuild(
            &controls,
            Arc::clone(&state_store),
            exit.reason,
            exit.last_error,
            cfg.memory,
        )
        .await;
        pending_rebuild = Some(rebuild_state);

        let backoff_ms = jittered_backoff_ms(backoff, cfg.restart_backoff_jitter_pct);
        warn!(
            event = "stream_restart",
            stream = StreamKind::Vm.as_str(),
            reason = exit.reason.as_str(),
            restart_count,
            backoff_ms,
            last_block,
            last_update_age_ms,
            "Restarting VM stream"
        );

        sleep(Duration::from_millis(backoff_ms)).await;
        backoff = next_backoff(backoff, cfg.restart_backoff_max);
    }
}

pub async fn supervise_rfq_stream<F, Fut, S>(
    build_stream: F,
    state_store: Arc<StateStore>,
    health: Arc<StreamHealth>,
    cfg: StreamSupervisorConfig,
    controls: RfqStreamControls,
) where
    F: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = anyhow::Result<S>> + Send,
    S: futures::Stream<
            Item = Result<TychoUpdate, Box<dyn std::error::Error + Send + Sync + 'static>>,
        > + Unpin
        + Send,
{
    let mut backoff = cfg.restart_backoff_min;
    let mut pending_rebuild: Option<RfqRebuildState> = None;

    loop {
        let stream = match build_stream().await {
            Ok(stream) => {
                if let Some(rebuild) = pending_rebuild.take() {
                    finish_rfq_rebuild(&controls, rebuild, cfg.memory).await;
                }
                stream
            }
            Err(err) => {
                warn!(
                    event = "stream_build_failed",
                    stream = StreamKind::Rfq.as_str(),
                    error = %err,
                    "Failed to build RFQ stream"
                );
                let backoff_ms = jittered_backoff_ms(backoff, cfg.restart_backoff_jitter_pct);
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff = next_backoff(backoff, cfg.restart_backoff_max);
                continue;
            }
        };

        let exit = process_stream(
            StreamKind::Rfq,
            stream,
            Arc::clone(&state_store),
            Arc::clone(&health),
            cfg.clone(),
        )
        .await;

        let restart_count = health.increment_restart().await;
        health.reset_bursts().await;
        let last_update_age_ms = health.last_update_age_ms().await.unwrap_or(0);
        let last_block = health.last_block().await;

        let rebuild_state = begin_rfq_rebuild(
            &controls,
            Arc::clone(&state_store),
            exit.reason,
            exit.last_error,
            cfg.memory,
        )
        .await;
        pending_rebuild = Some(rebuild_state);

        let backoff_ms = jittered_backoff_ms(backoff, cfg.restart_backoff_jitter_pct);
        warn!(
            event = "stream_restart",
            stream = StreamKind::Rfq.as_str(),
            reason = exit.reason.as_str(),
            restart_count,
            backoff_ms,
            last_block,
            last_update_age_ms,
            "Restarting RFQ stream"
        );

        sleep(Duration::from_millis(backoff_ms)).await;
        backoff = next_backoff(backoff, cfg.restart_backoff_max);
    }
}

struct VmRebuildState {
    guard: tokio::sync::OwnedSemaphorePermit,
    rebuild_id: u64,
    started_at: Instant,
}

struct RfqRebuildState {
    guard: tokio::sync::OwnedSemaphorePermit,
    rebuild_id: u64,
    started_at: Instant,
}

async fn begin_vm_rebuild(
    controls: &VmStreamControls,
    state_store: Arc<StateStore>,
    reason: StreamRestartReason,
    last_error: Option<String>,
    memory_cfg: MemoryConfig,
) -> VmRebuildState {
    let rebuild_id = {
        let mut status = controls.vm_stream.write().await;
        status.rebuilding = true;
        status.restart_count = status.restart_count.saturating_add(1);
        status.rebuild_started_at = Some(Instant::now());
        status.last_error = last_error.or_else(|| Some(reason.as_str().to_string()));
        status.restart_count
    };

    info!(event = "vm_rebuild_start", rebuild_id, "VM rebuild started");
    maybe_log_memory_snapshot("vm", "vm_rebuild_start", None, memory_cfg, true);

    state_store.reset().await;

    let guard = acquire_vm_rebuild_guard(controls).await;

    if let Err(err) = SHARED_TYCHO_DB.clear() {
        warn!(error = %err, "Failed clearing TychoDB during VM rebuild");
    }
    maybe_purge_allocator("vm_rebuild", memory_cfg);
    maybe_log_memory_snapshot("vm", "vm_rebuild_after_clear", None, memory_cfg, true);

    VmRebuildState {
        guard,
        rebuild_id,
        started_at: Instant::now(),
    }
}

async fn begin_rfq_rebuild(
    controls: &RfqStreamControls,
    state_store: Arc<StateStore>,
    reason: StreamRestartReason,
    last_error: Option<String>,
    memory_cfg: MemoryConfig,
) -> RfqRebuildState {
    let rebuild_id = {
        let mut status = controls.rfq_stream.write().await;
        status.rebuilding = true;
        status.restart_count = status.restart_count.saturating_add(1);
        status.rebuild_started_at = Some(Instant::now());
        status.last_error = last_error.or_else(|| Some(reason.as_str().to_string()));
        status.restart_count
    };

    info!(
        event = "rfq_rebuild_start",
        rebuild_id, "RFQ rebuild started"
    );
    maybe_log_memory_snapshot("rfq", "rfq_rebuild_start", None, memory_cfg, true);

    state_store.reset().await;

    let guard = acquire_rfq_rebuild_guard(controls).await;

    if let Err(err) = SHARED_TYCHO_DB.clear() {
        warn!(error = %err, "Failed clearing TychoDB during RFQ rebuild");
    }
    maybe_purge_allocator("rfq_rebuild", memory_cfg);
    maybe_log_memory_snapshot("rfq", "rfq_rebuild_after_clear", None, memory_cfg, true);

    RfqRebuildState {
        guard,
        rebuild_id,
        started_at: Instant::now(),
    }
}

#[expect(
    clippy::panic,
    reason = "the VM rebuild semaphore should remain available for the process lifetime"
)]
async fn acquire_vm_rebuild_guard(
    controls: &VmStreamControls,
) -> tokio::sync::OwnedSemaphorePermit {
    match controls
        .vm_sim_semaphore
        .clone()
        .acquire_many_owned(controls.vm_sim_concurrency)
        .await
    {
        Ok(guard) => guard,
        Err(_) => panic!("vm semaphore closed during rebuild"),
    }
}

#[expect(
    clippy::panic,
    reason = "the RFQ rebuild semaphore should remain available for the process lifetime"
)]
async fn acquire_rfq_rebuild_guard(
    controls: &RfqStreamControls,
) -> tokio::sync::OwnedSemaphorePermit {
    match controls
        .rfq_sim_semaphore
        .clone()
        .acquire_many_owned(controls.rfq_sim_concurrency)
        .await
    {
        Ok(guard) => guard,
        Err(_) => panic!("rfq semaphore closed during rebuild"),
    }
}

async fn finish_vm_rebuild(
    controls: &VmStreamControls,
    rebuild: VmRebuildState,
    memory_cfg: MemoryConfig,
) {
    drop(rebuild.guard);

    let duration_ms = rebuild.started_at.elapsed().as_millis() as u64;
    {
        let mut status = controls.vm_stream.write().await;
        status.rebuilding = false;
        status.rebuild_started_at = None;
    }

    info!(
        event = "vm_rebuild_success",
        rebuild_id = rebuild.rebuild_id,
        duration_ms,
        "VM rebuild completed"
    );
    maybe_log_memory_snapshot("vm", "vm_rebuild_success", None, memory_cfg, true);
}

async fn finish_rfq_rebuild(
    controls: &RfqStreamControls,
    rebuild: RfqRebuildState,
    memory_cfg: MemoryConfig,
) {
    drop(rebuild.guard);

    let duration_ms = rebuild.started_at.elapsed().as_millis() as u64;
    {
        let mut status = controls.rfq_stream.write().await;
        status.rebuilding = false;
        status.rebuild_started_at = None;
    }

    info!(
        event = "rfq_rebuild_success",
        rebuild_id = rebuild.rebuild_id,
        duration_ms,
        "RFQ rebuild completed"
    );
    maybe_log_memory_snapshot("rfq", "rfq_rebuild_success", None, memory_cfg, true);
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
    use super::classify_stream_error;

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
}
