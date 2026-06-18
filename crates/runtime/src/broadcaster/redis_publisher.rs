use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use futures::future::BoxFuture;
use rand::Rng;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::time::{timeout, Instant};
use tracing::{debug, info, warn};

use crate::broadcaster::state::{BroadcasterSnapshotCache, BroadcasterSnapshotExport};
use crate::config::BroadcasterRedisConfig;
use crate::metrics::{
    emit_broadcaster_redis_append_failure, emit_broadcaster_redis_generation_reset,
    emit_broadcaster_redis_pointer_write_failure, emit_broadcaster_redis_pointer_write_success,
};
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterEnvelope, BroadcasterHeartbeat, BroadcasterMessageKind,
    BroadcasterPayload, BroadcasterRedisSnapshotPointer, BroadcasterRedisStreamEntry,
    BroadcasterSnapshotChunk, BroadcasterSnapshotEnd, BroadcasterSnapshotStart,
};

const IDEMPOTENT_XADD_SCRIPT: &str = r#"
local existing_entry_id = redis.call('GET', KEYS[2])
if existing_entry_id then
  return existing_entry_id
end
local entry_id = redis.call('XADD', KEYS[1], '*', unpack(ARGV, 2))
redis.call('PSETEX', KEYS[2], ARGV[1], entry_id)
return entry_id
"#;
const RETRY_BACKOFF_BASE: Duration = Duration::from_millis(5);
const RETRY_BACKOFF_CAP: Duration = Duration::from_millis(200);

#[derive(Debug, Clone)]
pub struct BroadcasterRedisPublisherConfig {
    pub stream_key: String,
    pub snapshot_key: String,
    pub chain_id: u64,
    pub snapshot_max_payload_bytes: usize,
    pub append_retry_window: Duration,
    pub dedupe_key_ttl_ms: u64,
}

impl BroadcasterRedisPublisherConfig {
    pub fn from_redis_config(
        redis_config: &BroadcasterRedisConfig,
        chain_id: u64,
        snapshot_max_payload_bytes: usize,
    ) -> Self {
        let append_retry_window_ms = redis_config.append_retry_window_ms;
        Self {
            stream_key: redis_config.stream_key.clone(),
            snapshot_key: redis_config.snapshot_key.clone(),
            chain_id,
            snapshot_max_payload_bytes,
            append_retry_window: Duration::from_millis(append_retry_window_ms),
            dedupe_key_ttl_ms: append_retry_window_ms.saturating_mul(3).max(1_000),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BroadcasterRedisSnapshotSource {
    cache: BroadcasterSnapshotCache,
    backends: Vec<BroadcasterBackend>,
}

impl BroadcasterRedisSnapshotSource {
    pub fn new(cache: BroadcasterSnapshotCache, mut backends: Vec<BroadcasterBackend>) -> Self {
        backends.sort();
        backends.dedup();
        Self { cache, backends }
    }
}

pub trait RedisStreamWriter: Send + Sync {
    fn append<'a>(
        &'a self,
        stream_key: &'a str,
        dedupe_key_ttl_ms: u64,
        entry: &'a BroadcasterRedisStreamEntry,
    ) -> BoxFuture<'a, Result<String>>;

    fn set_snapshot_pointer<'a>(
        &'a self,
        snapshot_key: &'a str,
        pointer: &'a BroadcasterRedisSnapshotPointer,
    ) -> BoxFuture<'a, Result<()>>;
}

pub struct TokioRedisStreamWriter {
    connection: redis::aio::ConnectionManager,
}

impl TokioRedisStreamWriter {
    pub async fn connect(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)
            .context("failed to create Redis client from BROADCASTER_REDIS_URL")?;
        let connection = client
            .get_connection_manager()
            .await
            .context("failed to connect to broadcaster Redis")?;
        Ok(Self { connection })
    }
}

impl RedisStreamWriter for TokioRedisStreamWriter {
    fn append<'a>(
        &'a self,
        stream_key: &'a str,
        dedupe_key_ttl_ms: u64,
        entry: &'a BroadcasterRedisStreamEntry,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let fields = redis_entry_fields(entry)?;
            let dedupe_key = redis_append_dedupe_key(stream_key, entry);
            let mut command = redis::cmd("EVAL");
            command
                .arg(IDEMPOTENT_XADD_SCRIPT)
                .arg(2)
                .arg(stream_key)
                .arg(dedupe_key)
                .arg(dedupe_key_ttl_ms);
            for (field, value) in fields {
                command.arg(field).arg(value);
            }
            let mut connection = self.connection.clone();
            command
                .query_async(&mut connection)
                .await
                .context("Redis XADD failed")
        })
    }

    fn set_snapshot_pointer<'a>(
        &'a self,
        snapshot_key: &'a str,
        pointer: &'a BroadcasterRedisSnapshotPointer,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let pointer_json = serde_json::to_string(pointer)
                .context("failed to serialize Redis snapshot pointer")?;
            let mut connection = self.connection.clone();
            redis::cmd("SET")
                .arg(snapshot_key)
                .arg(pointer_json)
                .query_async(&mut connection)
                .await
                .context("Redis snapshot pointer SET failed")
        })
    }
}

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

    fn record_append_success(&mut self, entry_id: String) {
        self.append_success_count = self.append_success_count.saturating_add(1);
        self.latest_entry_id = Some(entry_id);
        self.next_message_seq = self.next_message_seq.saturating_add(1);
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
    ) {
        if retry_exhausted {
            self.retry_exhaustion_count = self.retry_exhaustion_count.saturating_add(1);
        }
        self.generation_reset_count = self.generation_reset_count.saturating_add(1);
        self.generation = self.generation.saturating_add(1);
        self.stream_id = format_redis_stream_id(chain_id, self.generation);
        self.snapshot_id = format_redis_snapshot_id(chain_id, self.generation);
        self.phase = PublisherPhase::AwaitingSnapshot;
        self.next_message_seq = 1;
        self.latest_entry_id = None;
        self.last_error = last_error;
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
                reset_redis_generation(&mut guard, self.config.chain_id, Some(message), true);
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
                reset_redis_generation(&mut guard, self.config.chain_id, Some(message), true);
                Err(error)
            }
        }
    }

    pub async fn reset_generation(&self, reason: impl Into<String>) {
        let mut guard = self.inner.lock().await;
        reset_redis_generation(&mut guard, self.config.chain_id, Some(reason.into()), false);
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
        guard.record_append_success(entry_id.clone());
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
                self.writer.append(
                    &self.config.stream_key,
                    self.config.dedupe_key_ttl_ms,
                    entry,
                ),
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

fn redis_entry_fields(entry: &BroadcasterRedisStreamEntry) -> Result<Vec<(String, String)>> {
    let Value::Object(fields) =
        serde_json::to_value(entry).context("failed to serialize Redis stream entry")?
    else {
        return Err(anyhow!("Redis stream entry did not serialize as an object"));
    };

    fields
        .into_iter()
        .map(|(field, value)| {
            let value = match value {
                Value::String(value) => value,
                Value::Number(value) => value.to_string(),
                Value::Bool(value) => value.to_string(),
                Value::Null => String::new(),
                Value::Array(_) | Value::Object(_) => serde_json::to_string(&value)
                    .context("failed to serialize nested Redis stream field")?,
            };
            Ok((field, value))
        })
        .collect()
}

fn redis_append_dedupe_key(stream_key: &str, entry: &BroadcasterRedisStreamEntry) -> String {
    format!(
        "{stream_key}:append:{}:{}",
        entry.stream_id, entry.message_seq
    )
}

fn append_snapshot_chunks(
    payloads: &mut Vec<BroadcasterPayload>,
    export: BroadcasterSnapshotExport,
    snapshot_id: &str,
    next_chunk_index: &mut u32,
) -> Result<()> {
    for payload in export.payloads {
        let BroadcasterPayload::SnapshotChunk(chunk) = payload else {
            continue;
        };
        payloads.push(BroadcasterPayload::SnapshotChunk(
            BroadcasterSnapshotChunk::new(
                snapshot_id.to_string(),
                *next_chunk_index,
                chunk.partitions,
            )?,
        ));
        *next_chunk_index = next_chunk_index.saturating_add(1);
    }
    Ok(())
}

fn payload_backend_scope(
    payload: &BroadcasterPayload,
    snapshot_end_backends: impl FnOnce() -> Vec<BroadcasterBackend>,
) -> Result<Vec<BroadcasterBackend>> {
    let mut backends = match payload {
        BroadcasterPayload::SnapshotStart(start) => start.backends.clone(),
        BroadcasterPayload::SnapshotChunk(chunk) => chunk
            .partitions
            .iter()
            .map(|partition| partition.backend)
            .collect(),
        BroadcasterPayload::SnapshotEnd(_) => snapshot_end_backends(),
        BroadcasterPayload::Update(update) => update
            .partitions
            .iter()
            .map(|partition| partition.backend)
            .collect(),
        BroadcasterPayload::Heartbeat(heartbeat) => heartbeat
            .backend_heads
            .iter()
            .map(|head| head.backend)
            .collect(),
    };
    backends.sort();
    backends.dedup();
    if backends.is_empty() {
        return Err(anyhow!("Redis broadcaster payload has empty backend scope"));
    }
    Ok(backends)
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
) {
    guard.advance_generation(chain_id, last_error, retry_exhausted);
    emit_broadcaster_redis_generation_reset();
    warn!(
        event = "redis_generation_reset",
        stream_id = guard.stream_id.as_str(),
        snapshot_id = guard.snapshot_id.as_str(),
        generation = guard.generation,
        error = guard.last_error.as_deref().unwrap_or(""),
        "Redis broadcaster generation reset"
    );
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn remaining_retry_window(started_at: Instant, retry_window: Duration) -> Option<Duration> {
    let remaining = retry_window.saturating_sub(started_at.elapsed());
    (!remaining.is_zero()).then_some(remaining)
}

async fn sleep_before_retry(started_at: Instant, retry_window: Duration, attempts: u64) {
    let elapsed = started_at.elapsed();
    let remaining = retry_window.saturating_sub(elapsed);
    if remaining.is_zero() {
        return;
    }
    let backoff = retry_backoff(attempts).min(remaining);
    let max_delay_ms = backoff.as_millis().max(1) as u64;
    let delay_ms = rand::thread_rng().gen_range(1..=max_delay_ms);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
}

fn retry_backoff(attempts: u64) -> Duration {
    let multiplier = 1u32 << attempts.saturating_sub(1).min(5);
    RETRY_BACKOFF_BASE
        .saturating_mul(multiplier)
        .min(RETRY_BACKOFF_CAP)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use anyhow::{anyhow, Result};
    use num_bigint::BigUint;
    use tokio::sync::Mutex;
    use tokio::time::{sleep, Duration};
    use tycho_simulation::protocol::models::{ProtocolComponent, Update};
    use tycho_simulation::tycho_client::feed::{BlockHeader, SynchronizerState};
    use tycho_simulation::tycho_common::dto::ProtocolStateDelta;
    use tycho_simulation::tycho_common::models::{token::Token, Chain};
    use tycho_simulation::tycho_common::simulation::errors::{SimulationError, TransitionError};
    use tycho_simulation::tycho_common::simulation::protocol_sim::{
        Balances, GetAmountOutResult, ProtocolSim,
    };
    use tycho_simulation::tycho_common::Bytes;

    use super::{
        BroadcasterRedisPublisher, BroadcasterRedisPublisherConfig, BroadcasterRedisSnapshotSource,
        RedisStreamWriter,
    };
    use crate::broadcaster::state::BroadcasterSnapshotCache;
    use crate::config::BroadcasterRedisConfig;
    use simulator_core::broadcaster::{
        BroadcasterBackend, BroadcasterPayload, BroadcasterRedisSnapshotPointer,
        BroadcasterRedisStreamEntry,
    };

    #[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
    struct DummySim(u8);

    #[typetag::serde(name = "RedisPublisherDummySim")]
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

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
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

    #[tokio::test]
    async fn publishes_combined_snapshot_and_pointer_after_snapshot_end() -> Result<()> {
        let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
        let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
        let writer = FakeRedisWriter::default();
        let publisher = BroadcasterRedisPublisher::new_for_test(
            publisher_config(),
            vec![
                BroadcasterRedisSnapshotSource::new(raw_cache, vec![BroadcasterBackend::Native]),
                BroadcasterRedisSnapshotSource::new(rfq_cache, vec![BroadcasterBackend::Rfq]),
            ],
            Arc::new(writer.clone()),
        );

        publisher.ensure_snapshot_published().await?;

        let appends = writer.appends().await;
        assert_eq!(appends.len(), 4);
        assert_eq!(
            appends
                .iter()
                .map(|append| append.entry.message_seq)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert!(matches!(
            appends[0].entry.kind,
            simulator_core::broadcaster::BroadcasterMessageKind::SnapshotStart
        ));
        assert_eq!(appends[0].entry.backend_scope, "native,rfq");
        assert_eq!(appends[1].entry.backend_scope, "native");
        assert_eq!(appends[2].entry.backend_scope, "rfq");
        assert_eq!(appends[2].entry.block_number, None);
        assert!(matches!(
            appends[3].entry.kind,
            simulator_core::broadcaster::BroadcasterMessageKind::SnapshotEnd
        ));

        let pointer = writer
            .latest_pointer()
            .await
            .ok_or_else(|| anyhow!("snapshot pointer should be written"))?;
        assert_eq!(pointer.snapshot_start_entry_id, "1000-0");
        assert_eq!(pointer.snapshot_end_entry_id, "1000-3");
        assert_eq!(pointer.live_cursor_entry_id, "1000-3");
        assert_eq!(pointer.stream_id, appends[0].entry.stream_id);
        assert_eq!(
            pointer.snapshot_id,
            appends[0].entry.snapshot_id.as_deref().unwrap_or("")
        );
        Ok(())
    }

    #[tokio::test]
    async fn publishes_live_updates_and_heartbeats_after_snapshot_pointer() -> Result<()> {
        let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
        let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
        let writer = FakeRedisWriter::default();
        let publisher = BroadcasterRedisPublisher::new_for_test(
            publisher_config(),
            vec![
                BroadcasterRedisSnapshotSource::new(
                    raw_cache.clone(),
                    vec![BroadcasterBackend::Native],
                ),
                BroadcasterRedisSnapshotSource::new(
                    rfq_cache.clone(),
                    vec![BroadcasterBackend::Rfq],
                ),
            ],
            Arc::new(writer.clone()),
        );
        publisher.ensure_snapshot_published().await?;

        let native_update = raw_cache
            .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
            .await?;
        publisher
            .publish_accepted_payload(BroadcasterPayload::Update(native_update))
            .await?;
        let rfq_update = rfq_cache
            .apply_update(&update(BroadcasterBackend::Rfq, 21, "rfq-2"))
            .await?;
        publisher
            .publish_accepted_payload(BroadcasterPayload::Update(rfq_update))
            .await?;
        let heartbeat = raw_cache
            .heartbeat()
            .await?
            .ok_or_else(|| anyhow!("ready raw cache should produce heartbeat"))?;
        publisher.publish_accepted_payload(heartbeat).await?;

        let appends = writer.appends().await;
        assert_eq!(
            appends
                .iter()
                .map(|append| append.entry.message_seq)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6, 7]
        );
        assert_eq!(appends[4].entry.kind.as_str(), "update");
        assert_eq!(appends[4].entry.backend_scope, "native");
        assert_eq!(appends[4].entry.block_number, Some(11));
        assert_eq!(appends[5].entry.kind.as_str(), "update");
        assert_eq!(appends[5].entry.backend_scope, "rfq");
        assert_eq!(appends[5].entry.block_number, None);
        assert_eq!(appends[6].entry.kind.as_str(), "heartbeat");
        assert_eq!(appends[6].entry.backend_scope, "native");

        let pointer = writer
            .latest_pointer()
            .await
            .ok_or_else(|| anyhow!("snapshot pointer should be written"))?;
        assert_eq!(appends[6].entry.stream_id, pointer.stream_id);
        assert_eq!(
            appends[6].entry.snapshot_id.as_deref(),
            Some(pointer.snapshot_id.as_str())
        );
        Ok(())
    }

    #[tokio::test]
    async fn append_retry_success_preserves_message_sequence_order() -> Result<()> {
        let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
        let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
        let writer = FakeRedisWriter::default();
        writer.fail_next_appends(1).await;
        let publisher = BroadcasterRedisPublisher::new_for_test(
            publisher_config(),
            vec![
                BroadcasterRedisSnapshotSource::new(raw_cache, vec![BroadcasterBackend::Native]),
                BroadcasterRedisSnapshotSource::new(rfq_cache, vec![BroadcasterBackend::Rfq]),
            ],
            Arc::new(writer.clone()),
        );

        publisher.ensure_snapshot_published().await?;

        let appends = writer.appends().await;
        assert_eq!(writer.append_attempt_count().await, 5);
        assert_eq!(
            appends
                .iter()
                .map(|append| append.entry.message_seq)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(appends[0].entry.kind.as_str(), "snapshot_start");
        assert!(writer.latest_pointer().await.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn append_retry_after_ambiguous_accept_does_not_duplicate_entry() -> Result<()> {
        let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
        let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
        let writer = FakeRedisWriter::default();
        writer.ambiguously_accept_next_append().await;
        let publisher = BroadcasterRedisPublisher::new_for_test(
            publisher_config(),
            vec![
                BroadcasterRedisSnapshotSource::new(raw_cache, vec![BroadcasterBackend::Native]),
                BroadcasterRedisSnapshotSource::new(rfq_cache, vec![BroadcasterBackend::Rfq]),
            ],
            Arc::new(writer.clone()),
        );

        publisher.ensure_snapshot_published().await?;

        let appends = writer.appends().await;
        assert_eq!(writer.append_attempt_count().await, 5);
        assert_eq!(
            appends
                .iter()
                .map(|append| append.entry.message_seq)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        Ok(())
    }

    #[tokio::test]
    async fn readiness_triggering_update_is_only_represented_by_initial_snapshot() -> Result<()> {
        let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
        let rfq_cache =
            BroadcasterSnapshotCache::new(Chain::Ethereum.id(), vec![BroadcasterBackend::Rfq]);
        let writer = FakeRedisWriter::default();
        let publisher = BroadcasterRedisPublisher::new_for_test(
            publisher_config(),
            vec![
                BroadcasterRedisSnapshotSource::new(raw_cache, vec![BroadcasterBackend::Native]),
                BroadcasterRedisSnapshotSource::new(
                    rfq_cache.clone(),
                    vec![BroadcasterBackend::Rfq],
                ),
            ],
            Arc::new(writer.clone()),
        );
        let rfq_update = rfq_cache
            .apply_update(&update(BroadcasterBackend::Rfq, 20, "rfq-1"))
            .await?;

        publisher
            .publish_accepted_payload(BroadcasterPayload::Update(rfq_update))
            .await?;

        let appends = writer.appends().await;
        assert_eq!(appends.len(), 4);
        assert!(matches!(
            appends.last().map(|append| append.entry.kind),
            Some(simulator_core::broadcaster::BroadcasterMessageKind::SnapshotEnd)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn retry_exhaustion_marks_unhealthy_and_next_snapshot_uses_new_generation() -> Result<()>
    {
        let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
        let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
        let writer = FakeRedisWriter::default();
        writer.fail_next_appends(100).await;
        let publisher = BroadcasterRedisPublisher::new_for_test(
            publisher_config(),
            vec![
                BroadcasterRedisSnapshotSource::new(raw_cache, vec![BroadcasterBackend::Native]),
                BroadcasterRedisSnapshotSource::new(rfq_cache, vec![BroadcasterBackend::Rfq]),
            ],
            Arc::new(writer.clone()),
        );

        let Err(error) = publisher.ensure_snapshot_published().await else {
            return Err(anyhow!(
                "retry exhaustion should surface the failed publication"
            ));
        };
        assert!(error
            .to_string()
            .contains("failed to append Redis broadcaster"));

        let failed_status = publisher.status_snapshot().await;
        assert!(!failed_status.healthy);
        assert_eq!(failed_status.generation_reset_count, 1);
        assert_eq!(failed_status.retry_exhaustion_count, 1);
        assert_eq!(failed_status.stream_id, "chain-1-redis-stream-2");
        assert!(failed_status.latest_snapshot_pointer.is_none());
        assert!(failed_status
            .last_error
            .as_deref()
            .unwrap_or("")
            .contains("planned append failure"));

        writer.fail_next_appends(0).await;
        publisher.ensure_snapshot_published().await?;

        let recovered_status = publisher.status_snapshot().await;
        assert!(recovered_status.healthy);
        assert_eq!(recovered_status.stream_id, "chain-1-redis-stream-2");
        assert!(recovered_status.latest_snapshot_pointer.is_some());
        assert!(recovered_status.last_error.is_none());

        let recovered_stream_entries = writer
            .appends()
            .await
            .into_iter()
            .filter(|append| append.entry.stream_id == recovered_status.stream_id)
            .collect::<Vec<_>>();
        assert_eq!(
            recovered_stream_entries
                .iter()
                .map(|append| append.entry.message_seq)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        Ok(())
    }

    #[tokio::test]
    async fn pointer_retry_exhaustion_resets_generation_before_republishing() -> Result<()> {
        let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
        let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
        let writer = FakeRedisWriter::default();
        writer.fail_next_pointer_writes(100).await;
        let publisher = BroadcasterRedisPublisher::new_for_test(
            publisher_config(),
            vec![
                BroadcasterRedisSnapshotSource::new(raw_cache, vec![BroadcasterBackend::Native]),
                BroadcasterRedisSnapshotSource::new(rfq_cache, vec![BroadcasterBackend::Rfq]),
            ],
            Arc::new(writer.clone()),
        );

        let Err(error) = publisher.ensure_snapshot_published().await else {
            return Err(anyhow!(
                "pointer retry exhaustion should surface the failed publication"
            ));
        };
        assert!(error
            .to_string()
            .contains("failed to write Redis broadcaster snapshot pointer"));

        let failed_status = publisher.status_snapshot().await;
        assert!(!failed_status.healthy);
        assert_eq!(failed_status.generation_reset_count, 1);
        assert_eq!(failed_status.retry_exhaustion_count, 1);
        assert!(failed_status.pointer_write_failure_count > 0);
        assert_eq!(failed_status.stream_id, "chain-1-redis-stream-2");
        assert!(failed_status.latest_snapshot_pointer.is_none());

        writer.fail_next_pointer_writes(0).await;
        publisher.ensure_snapshot_published().await?;

        let recovered_status = publisher.status_snapshot().await;
        assert!(recovered_status.healthy);
        assert_eq!(recovered_status.stream_id, "chain-1-redis-stream-2");
        assert_eq!(
            recovered_status
                .latest_snapshot_pointer
                .as_ref()
                .map(|pointer| pointer.stream_id.as_str()),
            Some("chain-1-redis-stream-2")
        );
        Ok(())
    }

    #[tokio::test]
    async fn stalled_append_exhausts_retry_window() -> Result<()> {
        let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
        let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
        let writer = FakeRedisWriter::default();
        writer.delay_appends(Duration::from_millis(50)).await;
        let publisher = BroadcasterRedisPublisher::new_for_test(
            publisher_config(),
            vec![
                BroadcasterRedisSnapshotSource::new(raw_cache, vec![BroadcasterBackend::Native]),
                BroadcasterRedisSnapshotSource::new(rfq_cache, vec![BroadcasterBackend::Rfq]),
            ],
            Arc::new(writer.clone()),
        );

        let Err(error) = publisher.ensure_snapshot_published().await else {
            return Err(anyhow!("stalled append should exhaust the retry window"));
        };
        assert!(format!("{error:#}").contains("retry window exhausted"));

        let failed_status = publisher.status_snapshot().await;
        assert!(!failed_status.healthy);
        assert_eq!(failed_status.append_failure_count, 1);
        assert_eq!(failed_status.generation_reset_count, 1);
        assert_eq!(failed_status.retry_exhaustion_count, 1);
        assert!(failed_status.latest_snapshot_pointer.is_none());
        assert!(writer.appends().await.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn stalled_pointer_write_exhausts_retry_window() -> Result<()> {
        let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
        let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
        let writer = FakeRedisWriter::default();
        writer.delay_pointer_writes(Duration::from_millis(50)).await;
        let publisher = BroadcasterRedisPublisher::new_for_test(
            publisher_config(),
            vec![
                BroadcasterRedisSnapshotSource::new(raw_cache, vec![BroadcasterBackend::Native]),
                BroadcasterRedisSnapshotSource::new(rfq_cache, vec![BroadcasterBackend::Rfq]),
            ],
            Arc::new(writer.clone()),
        );

        let Err(error) = publisher.ensure_snapshot_published().await else {
            return Err(anyhow!(
                "stalled pointer write should exhaust the retry window"
            ));
        };
        assert!(format!("{error:#}").contains("retry window exhausted"));

        let failed_status = publisher.status_snapshot().await;
        assert!(!failed_status.healthy);
        assert_eq!(failed_status.pointer_write_failure_count, 1);
        assert_eq!(failed_status.generation_reset_count, 1);
        assert_eq!(failed_status.retry_exhaustion_count, 1);
        assert!(failed_status.latest_snapshot_pointer.is_none());
        assert_eq!(writer.appends().await.len(), 4);
        assert!(writer.latest_pointer().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn waits_for_all_sources_before_initial_snapshot_publish() -> Result<()> {
        let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
        let rfq_cache =
            BroadcasterSnapshotCache::new(Chain::Ethereum.id(), vec![BroadcasterBackend::Rfq]);
        let writer = FakeRedisWriter::default();
        let publisher = BroadcasterRedisPublisher::new_for_test(
            publisher_config(),
            vec![
                BroadcasterRedisSnapshotSource::new(
                    raw_cache.clone(),
                    vec![BroadcasterBackend::Native],
                ),
                BroadcasterRedisSnapshotSource::new(
                    rfq_cache.clone(),
                    vec![BroadcasterBackend::Rfq],
                ),
            ],
            Arc::new(writer.clone()),
        );

        assert!(!publisher.publish_snapshot_if_ready().await?);
        assert!(writer.appends().await.is_empty());

        rfq_cache
            .apply_update(&update(BroadcasterBackend::Rfq, 20, "rfq-1"))
            .await?;

        assert!(publisher.publish_snapshot_if_ready().await?);
        assert_eq!(writer.appends().await.len(), 4);
        assert!(writer.latest_pointer().await.is_some());
        Ok(())
    }

    #[derive(Debug, Clone)]
    struct CapturedAppend {
        entry: BroadcasterRedisStreamEntry,
    }

    #[derive(Debug, Clone, Default)]
    struct FakeRedisWriter {
        inner: Arc<Mutex<FakeRedisWriterState>>,
    }

    #[derive(Debug, Default)]
    struct FakeRedisWriterState {
        appends: Vec<CapturedAppend>,
        latest_pointer: Option<BroadcasterRedisSnapshotPointer>,
        fail_next_appends: usize,
        ambiguously_accept_next_append: bool,
        fail_next_pointer_writes: usize,
        append_delay: Option<Duration>,
        pointer_write_delay: Option<Duration>,
        append_attempt_count: usize,
    }

    impl FakeRedisWriter {
        async fn fail_next_appends(&self, count: usize) {
            self.inner.lock().await.fail_next_appends = count;
        }

        async fn ambiguously_accept_next_append(&self) {
            self.inner.lock().await.ambiguously_accept_next_append = true;
        }

        async fn fail_next_pointer_writes(&self, count: usize) {
            self.inner.lock().await.fail_next_pointer_writes = count;
        }

        async fn delay_appends(&self, delay: Duration) {
            self.inner.lock().await.append_delay = Some(delay);
        }

        async fn delay_pointer_writes(&self, delay: Duration) {
            self.inner.lock().await.pointer_write_delay = Some(delay);
        }

        async fn append_attempt_count(&self) -> usize {
            self.inner.lock().await.append_attempt_count
        }

        async fn appends(&self) -> Vec<CapturedAppend> {
            self.inner.lock().await.appends.clone()
        }

        async fn latest_pointer(&self) -> Option<BroadcasterRedisSnapshotPointer> {
            self.inner.lock().await.latest_pointer.clone()
        }
    }

    impl RedisStreamWriter for FakeRedisWriter {
        fn append<'a>(
            &'a self,
            _stream_key: &'a str,
            _dedupe_key_ttl_ms: u64,
            entry: &'a BroadcasterRedisStreamEntry,
        ) -> futures::future::BoxFuture<'a, Result<String>> {
            Box::pin(async move {
                let delay = self.inner.lock().await.append_delay;
                if let Some(delay) = delay {
                    sleep(delay).await;
                }
                let mut guard = self.inner.lock().await;
                guard.append_attempt_count = guard.append_attempt_count.saturating_add(1);
                if guard.fail_next_appends > 0 {
                    guard.fail_next_appends -= 1;
                    return Err(anyhow!("planned append failure"));
                }
                if let Some((index, _)) = guard.appends.iter().enumerate().find(|(_, append)| {
                    append.entry.stream_id == entry.stream_id
                        && append.entry.message_seq == entry.message_seq
                }) {
                    return Ok(format!("1000-{index}"));
                }
                let entry_id = format!("1000-{}", guard.appends.len());
                guard.appends.push(CapturedAppend {
                    entry: entry.clone(),
                });
                if guard.ambiguously_accept_next_append {
                    guard.ambiguously_accept_next_append = false;
                    return Err(anyhow!("ambiguous append failure"));
                }
                Ok(entry_id)
            })
        }

        fn set_snapshot_pointer<'a>(
            &'a self,
            _snapshot_key: &'a str,
            pointer: &'a BroadcasterRedisSnapshotPointer,
        ) -> futures::future::BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                let delay = self.inner.lock().await.pointer_write_delay;
                if let Some(delay) = delay {
                    sleep(delay).await;
                }
                let mut guard = self.inner.lock().await;
                if guard.fail_next_pointer_writes > 0 {
                    guard.fail_next_pointer_writes -= 1;
                    return Err(anyhow!("planned pointer failure"));
                }
                guard.latest_pointer = Some(pointer.clone());
                Ok(())
            })
        }
    }

    async fn ready_cache(
        backend: BroadcasterBackend,
        block_number: u64,
        component_id: &str,
    ) -> Result<BroadcasterSnapshotCache> {
        let cache = BroadcasterSnapshotCache::new(Chain::Ethereum.id(), vec![backend]);
        cache
            .apply_update(&update(backend, block_number, component_id))
            .await?;
        Ok(cache)
    }

    fn update(backend: BroadcasterBackend, block_number: u64, component_id: &str) -> Update {
        let protocol = match backend {
            BroadcasterBackend::Native => "uniswap_v2",
            BroadcasterBackend::Vm => "vm:balancer_v2",
            BroadcasterBackend::Rfq => "rfq:bebop",
        };
        let mut new_pairs = HashMap::new();
        new_pairs.insert(
            component_id.to_string(),
            protocol_component(protocol, block_number as u8),
        );

        let mut states = HashMap::new();
        states.insert(
            component_id.to_string(),
            Box::new(DummySim(block_number as u8)) as Box<dyn ProtocolSim>,
        );

        Update::new(block_number, states, new_pairs).set_sync_states(HashMap::from([(
            protocol.to_string(),
            SynchronizerState::Ready(BlockHeader {
                hash: Bytes::from(vec![1u8; 32]),
                number: block_number,
                parent_hash: Bytes::from(vec![2u8; 32]),
                revert: false,
                timestamp: block_number,
                partial_block_index: None,
            }),
        )]))
    }

    fn protocol_component(protocol: &str, seed: u8) -> ProtocolComponent {
        ProtocolComponent::new(
            Bytes::from([seed; 20]),
            protocol.to_string(),
            protocol.to_string(),
            Chain::Ethereum,
            vec![dummy_token(1, "TKNA"), dummy_token(2, "TKNB")],
            Vec::new(),
            HashMap::new(),
            Bytes::from([9u8; 32]),
            chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
                .unwrap_or_else(|| unreachable!("unix epoch"))
                .naive_utc(),
        )
    }

    fn dummy_token(seed: u8, symbol: &str) -> Token {
        Token::new(
            &Bytes::from([seed; 20]),
            symbol,
            18,
            0,
            &[],
            Chain::Ethereum,
            1,
        )
    }

    fn publisher_config() -> BroadcasterRedisPublisherConfig {
        BroadcasterRedisPublisherConfig::from_redis_config(
            &BroadcasterRedisConfig {
                redis_url: "redis://127.0.0.1:6379/0".to_string(),
                stream_key: "dsolver:broadcaster:test:events".to_string(),
                snapshot_key: "dsolver:broadcaster:test:snapshot".to_string(),
                block_ms: 5_000,
                read_count: 128,
                append_retry_window_ms: 10,
                retention_secs: 300,
                maxlen: None,
            },
            Chain::Ethereum.id(),
            8_388_608,
        )
    }
}
