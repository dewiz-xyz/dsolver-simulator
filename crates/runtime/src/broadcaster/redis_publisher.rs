use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use tokio::sync::Mutex;
use tokio::time::{timeout, Instant};
use tracing::{debug, warn};

use crate::metrics::{
    emit_broadcaster_redis_append_failure, emit_broadcaster_redis_generation_reset,
};
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterEnvelope, BroadcasterHeartbeat, BroadcasterPayload,
    BroadcasterProgress, BroadcasterRedisReplayBoundary, BroadcasterRedisStreamEntry,
};

mod config;
mod retry;
mod writer;

pub use config::BroadcasterRedisPublisherConfig;
use retry::{remaining_retry_window, sleep_before_retry};
pub use writer::{RedisStreamWriter, TokioRedisStreamWriter};

const APPEND_EXHAUSTED_MESSAGE: &str = "Redis broadcaster stream append retry window exhausted";

pub struct BroadcasterRedisPublisher {
    config: BroadcasterRedisPublisherConfig,
    writer: Arc<dyn RedisStreamWriter>,
    inner: Arc<Mutex<BroadcasterRedisPublisherState>>,
}

impl fmt::Debug for BroadcasterRedisPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BroadcasterRedisPublisher")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct BroadcasterRedisPublisherStatus {
    pub healthy: bool,
    pub stream_key: String,
    pub stream_id: String,
    pub snapshot_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_entry_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replay_boundary: Option<BroadcasterRedisReplayBoundary>,
    pub append_success_count: u64,
    pub append_failure_count: u64,
    pub generation_reset_count: u64,
    pub retry_exhaustion_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug)]
struct BroadcasterRedisPublisherState {
    generation: u64,
    stream_id: String,
    snapshot_id: String,
    next_message_seq: u64,
    latest_entry_id: Option<String>,
    append_success_count: u64,
    append_failure_count: u64,
    generation_reset_count: u64,
    retry_exhaustion_count: u64,
    last_error: Option<String>,
}

impl BroadcasterRedisPublisherState {
    fn record_append_success(&mut self, entry_id: String, next_message_seq: u64) {
        self.append_success_count = self.append_success_count.saturating_add(1);
        self.latest_entry_id = Some(entry_id);
        self.next_message_seq = next_message_seq;
        self.last_error = None;
    }

    fn record_append_failure(&mut self) {
        self.append_failure_count = self.append_failure_count.saturating_add(1);
    }

    fn record_unhealthy(&mut self, last_error: String, retry_exhausted: bool) {
        if retry_exhausted {
            self.retry_exhaustion_count = self.retry_exhaustion_count.saturating_add(1);
        }
        self.last_error = Some(last_error);
    }

    fn advance_generation(&mut self, chain_id: u64) -> Result<()> {
        self.generation_reset_count = self.generation_reset_count.saturating_add(1);
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("Redis broadcaster generation overflow"))?;
        self.stream_id = format_redis_stream_id(chain_id, self.generation);
        self.snapshot_id = format_redis_snapshot_id(chain_id, self.generation);
        self.next_message_seq = 1;
        self.latest_entry_id = None;
        self.last_error = None;
        Ok(())
    }
}

impl BroadcasterRedisPublisher {
    pub fn new_with_initial_generation(
        config: BroadcasterRedisPublisherConfig,
        writer: Arc<dyn RedisStreamWriter>,
        generation: u64,
    ) -> Self {
        Self::with_initial_generation(config, writer, generation)
    }

    fn with_initial_generation(
        config: BroadcasterRedisPublisherConfig,
        writer: Arc<dyn RedisStreamWriter>,
        generation: u64,
    ) -> Self {
        let stream_id = format_redis_stream_id(config.chain_id, generation);
        let snapshot_id = format_redis_snapshot_id(config.chain_id, generation);
        Self {
            config,
            writer,
            inner: Arc::new(Mutex::new(BroadcasterRedisPublisherState {
                generation,
                stream_id,
                snapshot_id,
                next_message_seq: 1,
                latest_entry_id: None,
                append_success_count: 0,
                append_failure_count: 0,
                generation_reset_count: 0,
                retry_exhaustion_count: 0,
                last_error: None,
            })),
        }
    }

    pub async fn publish_accepted_payload(&self, payload: BroadcasterPayload) -> Result<()> {
        let mut guard = self.inner.lock().await;
        if let Some(error) = &guard.last_error {
            return Err(anyhow!(
                "Redis broadcaster publisher is unhealthy; shared broadcaster generation reset is required before publishing more deltas: {error}"
            ));
        }
        let payload = normalize_live_payload(payload, &guard.snapshot_id)?;
        let append_failures_before = guard.append_failure_count;
        match self.append_payload_locked(&mut guard, payload).await {
            Ok(_) => Ok(()),
            Err(error) => {
                let message = format!("{error:#}");
                let retry_exhausted = guard.append_failure_count > append_failures_before;
                guard.record_unhealthy(message, retry_exhausted);
                Err(error)
            }
        }
    }

    pub async fn replay_boundary(&self) -> Result<BroadcasterRedisReplayBoundary> {
        let guard = self.inner.lock().await;
        if let Some(error) = &guard.last_error {
            return Err(anyhow!(
                "Redis broadcaster replay boundary is unavailable while publisher is unhealthy: {error}"
            ));
        }
        self.replay_boundary_locked(&guard)
    }

    pub async fn reset_generation(
        &self,
        reason: impl Into<String>,
        backends: Vec<BroadcasterBackend>,
    ) {
        let mut guard = self.inner.lock().await;
        let reason = reason.into();
        if let Err(error) = reset_redis_generation(&mut guard, self.config.chain_id, &reason) {
            warn!(
                event = "redis_generation_reset_failed",
                error = %error,
                "Redis broadcaster generation reset failed"
            );
            return;
        }

        let marker = match BroadcasterProgress::new(
            self.config.chain_id,
            guard.snapshot_id.clone(),
            backends,
            reason,
        ) {
            Ok(marker) => marker,
            Err(error) => {
                guard.record_unhealthy(error.to_string(), false);
                warn!(
                    event = "redis_generation_reset_marker_invalid",
                    error = %error,
                    "Redis broadcaster generation reset marker was invalid"
                );
                return;
            }
        };
        let append_failures_before = guard.append_failure_count;
        if let Err(error) = self
            .append_payload_locked(&mut guard, BroadcasterPayload::Progress(marker))
            .await
        {
            let message = format!("{error:#}");
            let retry_exhausted = guard.append_failure_count > append_failures_before;
            guard.record_unhealthy(message, retry_exhausted);
        }
    }

    pub async fn status_snapshot(&self) -> BroadcasterRedisPublisherStatus {
        let guard = self.inner.lock().await;
        let replay_boundary = if guard.last_error.is_none() {
            self.replay_boundary_locked(&guard).ok()
        } else {
            None
        };
        BroadcasterRedisPublisherStatus {
            healthy: guard.last_error.is_none(),
            stream_key: self.config.stream_key.clone(),
            stream_id: guard.stream_id.clone(),
            snapshot_id: guard.snapshot_id.clone(),
            latest_entry_id: guard.latest_entry_id.clone(),
            replay_boundary,
            append_success_count: guard.append_success_count,
            append_failure_count: guard.append_failure_count,
            generation_reset_count: guard.generation_reset_count,
            retry_exhaustion_count: guard.retry_exhaustion_count,
            last_error: guard.last_error.clone(),
        }
    }

    async fn append_payload_locked(
        &self,
        guard: &mut BroadcasterRedisPublisherState,
        payload: BroadcasterPayload,
    ) -> Result<(BroadcasterRedisStreamEntry, String)> {
        let message_seq = guard.next_message_seq;
        let next_message_seq = message_seq
            .checked_add(1)
            .ok_or_else(|| anyhow!("Redis broadcaster message_seq overflow"))?;
        let envelope = BroadcasterEnvelope::new(guard.stream_id.clone(), message_seq, payload);
        let entry = BroadcasterRedisStreamEntry::from_envelope(
            self.config.chain_id,
            current_time_ms(),
            &envelope,
        )?;
        let entry_id = self
            .append_with_retry(guard, &entry)
            .await
            .with_context(|| {
                format!(
                    "failed to append Redis broadcaster message_seq {}",
                    entry.message_seq
                )
            })?;
        guard.record_append_success(entry_id.clone(), next_message_seq);
        debug!(
            event = "redis_stream_append",
            stream_key = self.config.stream_key.as_str(),
            stream_id = entry.stream_id.as_str(),
            message_seq = entry.message_seq,
            kind = %entry.kind,
            redis_entry_id = entry_id.as_str(),
            "Redis broadcaster stream entry appended"
        );
        Ok((entry, entry_id))
    }

    fn replay_boundary_locked(
        &self,
        guard: &BroadcasterRedisPublisherState,
    ) -> Result<BroadcasterRedisReplayBoundary> {
        BroadcasterRedisReplayBoundary::new(
            self.config.stream_key.clone(),
            guard.stream_id.clone(),
            guard.snapshot_id.clone(),
            guard.generation,
            guard.next_message_seq.saturating_sub(1),
        )
        .map_err(Into::into)
    }

    async fn append_with_retry(
        &self,
        guard: &mut BroadcasterRedisPublisherState,
        entry: &BroadcasterRedisStreamEntry,
    ) -> Result<String> {
        let started_at = Instant::now();
        let mut attempts = 0u64;
        let mut last_error = None;
        loop {
            let Some(remaining) =
                remaining_retry_window(started_at, self.config.append_retry_window)
            else {
                let error =
                    anyhow!(last_error.unwrap_or_else(|| APPEND_EXHAUSTED_MESSAGE.to_string()));
                self.record_write_failure(guard, entry, attempts, &error);
                return Err(error);
            };
            attempts = attempts.saturating_add(1);
            match timeout(
                remaining,
                self.writer
                    .append(&self.config.stream_key, self.config.maxlen, entry),
            )
            .await
            {
                Err(_) => {
                    let error = anyhow!(APPEND_EXHAUSTED_MESSAGE);
                    self.record_write_failure(guard, entry, attempts, &error);
                    return Err(error);
                }
                Ok(Ok(result)) => return Ok(result),
                Ok(Err(error)) => {
                    if started_at.elapsed() >= self.config.append_retry_window {
                        self.record_write_failure(guard, entry, attempts, &error);
                        return Err(error);
                    }
                    last_error = Some(error.to_string());
                    sleep_before_retry(started_at, self.config.append_retry_window, attempts).await;
                }
            }
        }
    }

    fn record_write_failure(
        &self,
        guard: &mut BroadcasterRedisPublisherState,
        entry: &BroadcasterRedisStreamEntry,
        attempts: u64,
        error: &anyhow::Error,
    ) {
        guard.record_append_failure();
        emit_broadcaster_redis_append_failure();
        warn!(
            event = "redis_stream_append_failed",
            stream_key = self.config.stream_key.as_str(),
            stream_id = entry.stream_id.as_str(),
            message_seq = entry.message_seq,
            kind = %entry.kind,
            attempts,
            error = %error,
            "Redis broadcaster stream append retry window exhausted"
        );
    }
}

fn normalize_live_payload(
    payload: BroadcasterPayload,
    snapshot_id: &str,
) -> Result<BroadcasterPayload> {
    match payload {
        BroadcasterPayload::Update(_) => Ok(payload),
        BroadcasterPayload::Heartbeat(heartbeat) => {
            Ok(BroadcasterPayload::Heartbeat(BroadcasterHeartbeat::new(
                heartbeat.chain_id,
                snapshot_id.to_string(),
                heartbeat.backend_heads,
            )?))
        }
        BroadcasterPayload::Progress(progress) => {
            Ok(BroadcasterPayload::Progress(BroadcasterProgress::new(
                progress.chain_id,
                snapshot_id.to_string(),
                progress.backends,
                progress.reason,
            )?))
        }
        BroadcasterPayload::SnapshotStart(_)
        | BroadcasterPayload::SnapshotChunk(_)
        | BroadcasterPayload::SnapshotEnd(_) => Err(anyhow!(
            "Redis broadcaster live payload cannot be a snapshot message"
        )),
    }
}

fn format_redis_stream_id(chain_id: u64, generation: u64) -> String {
    format!("chain-{chain_id}-stream-{generation}")
}

fn format_redis_snapshot_id(chain_id: u64, generation: u64) -> String {
    format!("chain-{chain_id}-snapshot-{generation}")
}

fn reset_redis_generation(
    guard: &mut BroadcasterRedisPublisherState,
    chain_id: u64,
    reason: &str,
) -> Result<()> {
    guard.advance_generation(chain_id)?;
    emit_broadcaster_redis_generation_reset();
    warn!(
        event = "redis_generation_reset",
        stream_id = guard.stream_id.as_str(),
        snapshot_id = guard.snapshot_id.as_str(),
        generation = guard.generation,
        reason,
        "Redis broadcaster generation reset"
    );
    Ok(())
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
