use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use tokio::sync::Mutex;
use tokio::time::{timeout, Instant};
use tracing::{debug, info, warn};

use crate::metrics::{
    emit_broadcaster_redis_append_failure, emit_broadcaster_redis_generation_reset,
    emit_broadcaster_redis_pointer_write_failure, emit_broadcaster_redis_pointer_write_success,
};
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterEnvelope, BroadcasterHeartbeat, BroadcasterMessageKind,
    BroadcasterPayload, BroadcasterRedisSnapshotPointer, BroadcasterRedisStreamEntry,
    BroadcasterSnapshotEnd, BroadcasterSnapshotStart,
};

mod config;
mod retry;
mod snapshot;
mod writer;

pub use config::BroadcasterRedisPublisherConfig;
use retry::{remaining_retry_window, sleep_before_retry};
pub use snapshot::BroadcasterRedisSnapshotSource;
use snapshot::{append_snapshot_chunks, payload_backend_scope};
pub use writer::{RedisStreamWriter, TokioRedisStreamWriter};

pub struct BroadcasterRedisPublisher {
    config: BroadcasterRedisPublisherConfig,
    sources: Vec<BroadcasterRedisSnapshotSource>,
    writer: Arc<dyn RedisStreamWriter>,
    inner: Arc<Mutex<BroadcasterRedisPublisherState>>,
}

impl fmt::Debug for BroadcasterRedisPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BroadcasterRedisPublisher")
            .field("config", &self.config)
            .field("sources", &self.sources)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct BroadcasterRedisPublisherStatus {
    pub healthy: bool,
    pub stream_key: String,
    pub stream_id: String,
    pub snapshot_id: String,
    pub latest_entry_id: Option<String>,
    pub latest_snapshot_pointer: Option<BroadcasterRedisSnapshotPointer>,
    pub append_success_count: u64,
    pub append_failure_count: u64,
    pub pointer_write_success_count: u64,
    pub pointer_write_failure_count: u64,
    pub generation_reset_count: u64,
    pub retry_exhaustion_count: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
enum PublisherPhase {
    AwaitingSnapshot,
    Live {
        snapshot_pointer: BroadcasterRedisSnapshotPointer,
    },
}

impl PublisherPhase {
    fn is_live(&self) -> bool {
        matches!(self, Self::Live { .. })
    }

    fn snapshot_pointer(&self) -> Option<&BroadcasterRedisSnapshotPointer> {
        match self {
            Self::AwaitingSnapshot => None,
            Self::Live { snapshot_pointer } => Some(snapshot_pointer),
        }
    }
}

#[derive(Debug)]
struct BroadcasterRedisPublisherState {
    generation: u64,
    stream_id: String,
    snapshot_id: String,
    phase: PublisherPhase,
    next_message_seq: u64,
    latest_entry_id: Option<String>,
    append_success_count: u64,
    append_failure_count: u64,
    pointer_write_success_count: u64,
    pointer_write_failure_count: u64,
    generation_reset_count: u64,
    retry_exhaustion_count: u64,
    last_error: Option<String>,
}

impl BroadcasterRedisPublisherState {
    fn is_live(&self) -> bool {
        self.phase.is_live()
    }

    fn record_append_success(&mut self, entry_id: String, next_message_seq: u64) {
        self.append_success_count = self.append_success_count.saturating_add(1);
        self.latest_entry_id = Some(entry_id);
        self.next_message_seq = next_message_seq;
    }

    fn record_append_failure(&mut self) {
        self.append_failure_count = self.append_failure_count.saturating_add(1);
    }

    fn record_pointer_write_success(&mut self, pointer: BroadcasterRedisSnapshotPointer) {
        self.pointer_write_success_count = self.pointer_write_success_count.saturating_add(1);
        self.phase = PublisherPhase::Live {
            snapshot_pointer: pointer,
        };
        self.last_error = None;
    }

    fn record_pointer_write_failure(&mut self) {
        self.pointer_write_failure_count = self.pointer_write_failure_count.saturating_add(1);
    }

    fn advance_generation(
        &mut self,
        chain_id: u64,
        last_error: Option<String>,
        retry_exhausted: bool,
    ) -> Result<()> {
        if retry_exhausted {
            self.retry_exhaustion_count = self.retry_exhaustion_count.saturating_add(1);
        }
        self.generation_reset_count = self.generation_reset_count.saturating_add(1);
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("Redis broadcaster generation overflow"))?;
        self.stream_id = format_redis_stream_id(chain_id, self.generation);
        self.snapshot_id = format_redis_snapshot_id(chain_id, self.generation);
        self.phase = PublisherPhase::AwaitingSnapshot;
        self.next_message_seq = 1;
        self.latest_entry_id = None;
        self.last_error = last_error;
        Ok(())
    }
}

impl BroadcasterRedisPublisher {
    #[cfg(test)]
    pub fn new_for_test(
        config: BroadcasterRedisPublisherConfig,
        sources: Vec<BroadcasterRedisSnapshotSource>,
        writer: Arc<dyn RedisStreamWriter>,
    ) -> Self {
        Self::with_initial_generation(config, sources, writer, 1)
    }

    pub fn new(
        config: BroadcasterRedisPublisherConfig,
        sources: Vec<BroadcasterRedisSnapshotSource>,
        writer: Arc<dyn RedisStreamWriter>,
    ) -> Self {
        Self::with_initial_generation(config, sources, writer, initial_redis_generation())
    }

    fn with_initial_generation(
        config: BroadcasterRedisPublisherConfig,
        sources: Vec<BroadcasterRedisSnapshotSource>,
        writer: Arc<dyn RedisStreamWriter>,
        generation: u64,
    ) -> Self {
        let stream_id = format_redis_stream_id(config.chain_id, generation);
        let snapshot_id = format_redis_snapshot_id(config.chain_id, generation);
        Self {
            config,
            sources,
            writer,
            inner: Arc::new(Mutex::new(BroadcasterRedisPublisherState {
                generation,
                stream_id,
                snapshot_id,
                phase: PublisherPhase::AwaitingSnapshot,
                next_message_seq: 1,
                latest_entry_id: None,
                append_success_count: 0,
                append_failure_count: 0,
                pointer_write_success_count: 0,
                pointer_write_failure_count: 0,
                generation_reset_count: 0,
                retry_exhaustion_count: 0,
                last_error: None,
            })),
        }
    }

    async fn ensure_snapshot_published(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        if guard.is_live() {
            return Ok(());
        }

        match self.publish_snapshot_locked(&mut guard).await {
            Ok(()) => Ok(()),
            Err(error) => {
                let message = format!("{error:#}");
                reset_redis_generation(&mut guard, self.config.chain_id, Some(message), true)
                    .with_context(|| {
                        format!(
                            "failed to reset Redis broadcaster generation after snapshot publish failure: {error:#}"
                        )
                    })?;
                Err(error)
            }
        }
    }

    pub async fn publish_snapshot_if_ready(&self) -> Result<bool> {
        if !self.sources_ready().await {
            return Ok(false);
        }
        self.ensure_snapshot_published().await?;
        Ok(true)
    }

    async fn publish_snapshot_locked(
        &self,
        guard: &mut BroadcasterRedisPublisherState,
    ) -> Result<()> {
        let snapshot = self.export_combined_snapshot(&guard.snapshot_id).await?;
        let stream_id = guard.stream_id.clone();
        let snapshot_id = guard.snapshot_id.clone();
        let mut snapshot_start_entry_id = None;
        let mut snapshot_end_entry_id = None;

        for payload in snapshot {
            let (entry, entry_id) = self.append_payload_locked(guard, payload).await?;
            if matches!(entry.kind, BroadcasterMessageKind::SnapshotStart) {
                snapshot_start_entry_id = Some(entry_id.clone());
            }
            if matches!(entry.kind, BroadcasterMessageKind::SnapshotEnd) {
                snapshot_end_entry_id = Some(entry_id);
            }
        }

        let snapshot_start_entry_id = snapshot_start_entry_id
            .ok_or_else(|| anyhow!("combined Redis snapshot did not append snapshot_start"))?;
        let snapshot_end_entry_id = snapshot_end_entry_id
            .ok_or_else(|| anyhow!("combined Redis snapshot did not append snapshot_end"))?;
        let pointer = BroadcasterRedisSnapshotPointer::new(
            self.config.chain_id,
            self.config.stream_key.clone(),
            stream_id,
            snapshot_id,
            snapshot_start_entry_id,
            snapshot_end_entry_id.clone(),
            snapshot_end_entry_id,
            current_time_ms(),
        )?;
        self.set_snapshot_pointer_with_retry(guard, &pointer)
            .await
            .context("failed to write Redis broadcaster snapshot pointer")?;
        guard.record_pointer_write_success(pointer);
        info!(
            event = "redis_snapshot_published",
            stream_key = self.config.stream_key.as_str(),
            stream_id = guard.stream_id.as_str(),
            snapshot_id = guard.snapshot_id.as_str(),
            latest_entry_id = guard.latest_entry_id.as_deref().unwrap_or(""),
            "Redis broadcaster snapshot published"
        );
        Ok(())
    }

    pub async fn publish_accepted_payload(&self, payload: BroadcasterPayload) -> Result<()> {
        if !self.is_live().await {
            self.publish_snapshot_if_ready().await?;
            return Ok(());
        }

        let mut guard = self.inner.lock().await;
        let payload = normalize_live_payload(payload, &guard.snapshot_id)?;
        match self.append_payload_locked(&mut guard, payload).await {
            Ok(_) => Ok(()),
            Err(error) => {
                let message = format!("{error:#}");
                reset_redis_generation(&mut guard, self.config.chain_id, Some(message), true)
                    .with_context(|| {
                        format!(
                            "failed to reset Redis broadcaster generation after live append failure: {error:#}"
                        )
                    })?;
                Err(error)
            }
        }
    }

    pub async fn reset_generation(&self, reason: impl Into<String>) {
        let mut guard = self.inner.lock().await;
        if let Err(error) =
            reset_redis_generation(&mut guard, self.config.chain_id, Some(reason.into()), false)
        {
            warn!(
                event = "redis_generation_reset_failed",
                error = %error,
                "Redis broadcaster generation reset failed"
            );
        }
    }

    pub async fn status_snapshot(&self) -> BroadcasterRedisPublisherStatus {
        let guard = self.inner.lock().await;
        BroadcasterRedisPublisherStatus {
            healthy: guard.is_live(),
            stream_key: self.config.stream_key.clone(),
            stream_id: guard.stream_id.clone(),
            snapshot_id: guard.snapshot_id.clone(),
            latest_entry_id: guard.latest_entry_id.clone(),
            latest_snapshot_pointer: guard.phase.snapshot_pointer().cloned(),
            append_success_count: guard.append_success_count,
            append_failure_count: guard.append_failure_count,
            pointer_write_success_count: guard.pointer_write_success_count,
            pointer_write_failure_count: guard.pointer_write_failure_count,
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
        let backends = payload_backend_scope(&payload, || self.all_backends())?;
        let envelope = BroadcasterEnvelope::new(guard.stream_id.clone(), message_seq, payload);
        let entry = BroadcasterRedisStreamEntry::from_envelope(
            self.config.chain_id,
            current_time_ms(),
            &envelope,
            backends,
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
                let error = anyhow!(last_error.unwrap_or_else(|| {
                    "Redis broadcaster stream append retry window exhausted".to_string()
                }));
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
                return Err(error);
            };
            attempts = attempts.saturating_add(1);
            match timeout(
                remaining,
                self.writer.append(&self.config.stream_key, entry),
            )
            .await
            {
                Err(_) => {
                    guard.record_append_failure();
                    emit_broadcaster_redis_append_failure();
                    warn!(
                        event = "redis_stream_append_failed",
                        stream_key = self.config.stream_key.as_str(),
                        stream_id = entry.stream_id.as_str(),
                        message_seq = entry.message_seq,
                        kind = %entry.kind,
                        attempts,
                        "Redis broadcaster stream append retry window exhausted"
                    );
                    return Err(anyhow!(
                        "Redis broadcaster stream append retry window exhausted"
                    ));
                }
                Ok(Ok(entry_id)) => return Ok(entry_id),
                Ok(Err(error)) => {
                    if started_at.elapsed() >= self.config.append_retry_window {
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
                        return Err(error);
                    }
                    last_error = Some(error.to_string());
                    sleep_before_retry(started_at, self.config.append_retry_window, attempts).await;
                }
            }
        }
    }

    async fn set_snapshot_pointer_with_retry(
        &self,
        guard: &mut BroadcasterRedisPublisherState,
        pointer: &BroadcasterRedisSnapshotPointer,
    ) -> Result<()> {
        let started_at = Instant::now();
        let mut attempts = 0u64;
        let mut last_error = None;
        loop {
            let Some(remaining) =
                remaining_retry_window(started_at, self.config.append_retry_window)
            else {
                let error = anyhow!(last_error.unwrap_or_else(|| {
                    "Redis broadcaster snapshot pointer update retry window exhausted".to_string()
                }));
                guard.record_pointer_write_failure();
                emit_broadcaster_redis_pointer_write_failure();
                warn!(
                    event = "redis_snapshot_pointer_update_failed",
                    snapshot_key = self.config.snapshot_key.as_str(),
                    stream_id = pointer.stream_id.as_str(),
                    snapshot_id = pointer.snapshot_id.as_str(),
                    attempts,
                    error = %error,
                    "Redis broadcaster snapshot pointer update retry window exhausted"
                );
                return Err(error);
            };
            attempts = attempts.saturating_add(1);
            match timeout(
                remaining,
                self.writer
                    .set_snapshot_pointer(&self.config.snapshot_key, pointer),
            )
            .await
            {
                Err(_) => {
                    guard.record_pointer_write_failure();
                    emit_broadcaster_redis_pointer_write_failure();
                    warn!(
                        event = "redis_snapshot_pointer_update_failed",
                        snapshot_key = self.config.snapshot_key.as_str(),
                        stream_id = pointer.stream_id.as_str(),
                        snapshot_id = pointer.snapshot_id.as_str(),
                        attempts,
                        "Redis broadcaster snapshot pointer update retry window exhausted"
                    );
                    return Err(anyhow!(
                        "Redis broadcaster snapshot pointer update retry window exhausted"
                    ));
                }
                Ok(Ok(())) => {
                    emit_broadcaster_redis_pointer_write_success();
                    info!(
                        event = "redis_snapshot_pointer_updated",
                        snapshot_key = self.config.snapshot_key.as_str(),
                        stream_id = pointer.stream_id.as_str(),
                        snapshot_id = pointer.snapshot_id.as_str(),
                        snapshot_start_entry_id = pointer.snapshot_start_entry_id.as_str(),
                        snapshot_end_entry_id = pointer.snapshot_end_entry_id.as_str(),
                        live_cursor_entry_id = pointer.live_cursor_entry_id.as_str(),
                        "Redis broadcaster snapshot pointer updated"
                    );
                    return Ok(());
                }
                Ok(Err(error)) => {
                    if started_at.elapsed() >= self.config.append_retry_window {
                        guard.record_pointer_write_failure();
                        emit_broadcaster_redis_pointer_write_failure();
                        warn!(
                            event = "redis_snapshot_pointer_update_failed",
                            snapshot_key = self.config.snapshot_key.as_str(),
                            stream_id = pointer.stream_id.as_str(),
                            snapshot_id = pointer.snapshot_id.as_str(),
                            attempts,
                            error = %error,
                            "Redis broadcaster snapshot pointer update retry window exhausted"
                        );
                        return Err(error);
                    }
                    last_error = Some(error.to_string());
                    sleep_before_retry(started_at, self.config.append_retry_window, attempts).await;
                }
            }
        }
    }

    async fn export_combined_snapshot(&self, snapshot_id: &str) -> Result<Vec<BroadcasterPayload>> {
        let mut source_exports = Vec::with_capacity(self.sources.len());
        let mut total_chunks = 0u32;
        for source in &self.sources {
            let export = source
                .cache
                .export_snapshot(self.config.snapshot_max_payload_bytes)
                .await?;
            let chunk_count = export
                .payloads
                .iter()
                .filter(|payload| matches!(payload, BroadcasterPayload::SnapshotChunk(_)))
                .count() as u32;
            total_chunks = total_chunks.saturating_add(chunk_count);
            source_exports.push(export);
        }

        let mut payloads = Vec::with_capacity(total_chunks as usize + 2);
        payloads.push(BroadcasterPayload::SnapshotStart(
            BroadcasterSnapshotStart::new(
                snapshot_id.to_string(),
                self.config.chain_id,
                self.all_backends(),
                total_chunks,
            )?,
        ));

        let mut next_chunk_index = 0u32;
        for export in source_exports {
            append_snapshot_chunks(&mut payloads, export, snapshot_id, &mut next_chunk_index)?;
        }

        payloads.push(BroadcasterPayload::SnapshotEnd(
            BroadcasterSnapshotEnd::new(snapshot_id.to_string()),
        ));
        Ok(payloads)
    }

    fn all_backends(&self) -> Vec<BroadcasterBackend> {
        let mut backends: Vec<_> = self
            .sources
            .iter()
            .flat_map(|source| source.backends.iter().copied())
            .collect();
        backends.sort();
        backends.dedup();
        backends
    }

    async fn sources_ready(&self) -> bool {
        for source in &self.sources {
            if !source.cache.is_ready().await {
                return false;
            }
        }
        true
    }

    async fn is_live(&self) -> bool {
        self.inner.lock().await.is_live()
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
        BroadcasterPayload::SnapshotStart(_)
        | BroadcasterPayload::SnapshotChunk(_)
        | BroadcasterPayload::SnapshotEnd(_) => Err(anyhow!(
            "Redis broadcaster live payload cannot be a snapshot message"
        )),
    }
}

fn format_redis_stream_id(chain_id: u64, generation: u64) -> String {
    format!("chain-{chain_id}-redis-stream-{generation}")
}

fn format_redis_snapshot_id(chain_id: u64, generation: u64) -> String {
    format!("chain-{chain_id}-redis-snapshot-{generation}")
}

fn initial_redis_generation() -> u64 {
    current_time_ms()
        .saturating_mul(1_000_000)
        .saturating_add(rand::random::<u64>() % 1_000_000)
        .max(1)
}

fn reset_redis_generation(
    guard: &mut BroadcasterRedisPublisherState,
    chain_id: u64,
    last_error: Option<String>,
    retry_exhausted: bool,
) -> Result<()> {
    guard.advance_generation(chain_id, last_error, retry_exhausted)?;
    emit_broadcaster_redis_generation_reset();
    warn!(
        event = "redis_generation_reset",
        stream_id = guard.stream_id.as_str(),
        snapshot_id = guard.snapshot_id.as_str(),
        generation = guard.generation,
        error = guard.last_error.as_deref().unwrap_or(""),
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
