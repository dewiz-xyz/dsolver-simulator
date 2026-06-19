use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use futures::future::BoxFuture;
use rand::Rng;
use redis::streams::{StreamInfoStreamReply, StreamRangeReply};
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::time::{timeout, Instant};
use tracing::{debug, warn};

use crate::config::BroadcasterRedisConfig;
use crate::metrics::{
    emit_broadcaster_redis_append_failure, emit_broadcaster_redis_generation_reset,
};
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterEnvelope, BroadcasterHeartbeat, BroadcasterPayload,
    BroadcasterProgress, BroadcasterRedisReplayBoundary, BroadcasterRedisStreamEntry,
};

const APPEND_EXHAUSTED_MESSAGE: &str = "Redis broadcaster stream append retry window exhausted";
const RETRY_BACKOFF_BASE: Duration = Duration::from_millis(5);
const RETRY_BACKOFF_CAP: Duration = Duration::from_millis(200);

#[derive(Debug, Clone)]
pub struct BroadcasterRedisPublisherConfig {
    pub stream_key: String,
    pub chain_id: u64,
    pub append_retry_window: Duration,
    pub maxlen: Option<u64>,
}

impl BroadcasterRedisPublisherConfig {
    pub fn from_redis_config(redis_config: &BroadcasterRedisConfig, chain_id: u64) -> Self {
        Self {
            stream_key: redis_config.stream_key.clone(),
            chain_id,
            append_retry_window: Duration::from_millis(redis_config.append_retry_window_ms),
            maxlen: redis_config.maxlen,
        }
    }
}

pub trait RedisStreamWriter: Send + Sync {
    fn append<'a>(
        &'a self,
        stream_key: &'a str,
        maxlen: Option<u64>,
        entry: &'a BroadcasterRedisStreamEntry,
    ) -> BoxFuture<'a, Result<String>>;
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

    pub async fn next_generation(&self, stream_key: &str) -> Result<u64> {
        let mut connection = self.connection.clone();
        let reply = redis::cmd("XINFO")
            .arg("STREAM")
            .arg(stream_key)
            .query_async::<StreamInfoStreamReply>(&mut connection)
            .await;
        next_generation_from_xinfo_reply(reply)
    }
}

impl RedisStreamWriter for TokioRedisStreamWriter {
    fn append<'a>(
        &'a self,
        stream_key: &'a str,
        maxlen: Option<u64>,
        entry: &'a BroadcasterRedisStreamEntry,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let entry_id = redis_entry_id(entry)?;
            let fields = redis_entry_fields(entry)?;
            let command = redis_xadd_command_from_fields(stream_key, maxlen, &entry_id, &fields);
            let mut connection = self.connection.clone();
            match command.query_async::<String>(&mut connection).await {
                Ok(entry_id) => Ok(entry_id),
                Err(error) => {
                    if redis_stream_entry_matches(&mut connection, stream_key, &entry_id, &fields)
                        .await?
                    {
                        Ok(entry_id)
                    } else {
                        Err(error).context("Redis XADD failed")
                    }
                }
            }
        })
    }
}

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

fn next_generation_from_xinfo_reply(
    reply: std::result::Result<StreamInfoStreamReply, redis::RedisError>,
) -> Result<u64> {
    match reply {
        Ok(reply) => next_generation_after_entry_id(&reply.last_generated_id),
        Err(error) if redis_stream_missing_key(&error) => Ok(1),
        Err(error) => Err(anyhow!(error).context("Redis XINFO STREAM failed")),
    }
}

pub(super) fn next_generation_after_entry_id(entry_id: &str) -> Result<u64> {
    let generation = entry_id
        .split_once('-')
        .map(|(generation, _)| generation)
        .ok_or_else(|| anyhow!("Redis stream last-generated-id is invalid: {entry_id}"))?
        .parse::<u64>()
        .with_context(|| format!("Redis stream last-generated-id is invalid: {entry_id}"))?;
    generation
        .checked_add(1)
        .ok_or_else(|| anyhow!("Redis broadcaster generation overflow"))
}

fn redis_stream_missing_key(error: &redis::RedisError) -> bool {
    error
        .detail()
        .is_some_and(|detail| detail.contains("no such key"))
}

#[cfg(test)]
pub(super) fn redis_xadd_command(
    stream_key: &str,
    maxlen: Option<u64>,
    entry: &BroadcasterRedisStreamEntry,
) -> Result<redis::Cmd> {
    let entry_id = redis_entry_id(entry)?;
    let fields = redis_entry_fields(entry)?;
    Ok(redis_xadd_command_from_fields(
        stream_key, maxlen, &entry_id, &fields,
    ))
}

fn redis_xadd_command_from_fields(
    stream_key: &str,
    maxlen: Option<u64>,
    entry_id: &str,
    fields: &[(String, String)],
) -> redis::Cmd {
    let mut command = redis::cmd("XADD");
    command.arg(stream_key);
    if let Some(maxlen) = maxlen {
        command.arg("MAXLEN").arg("~").arg(maxlen);
    }
    command.arg(entry_id);
    for (field, value) in fields {
        command.arg(field).arg(value);
    }
    command
}

pub(super) fn redis_entry_id(entry: &BroadcasterRedisStreamEntry) -> Result<String> {
    let generation = entry
        .stream_id
        .rsplit_once('-')
        .map(|(_, generation)| generation)
        .ok_or_else(|| anyhow!("Redis broadcaster stream_id is missing generation"))?;
    let generation = generation.parse::<u64>().with_context(|| {
        format!("Redis broadcaster stream_id has invalid generation: {generation}")
    })?;
    Ok(format!("{generation}-{}", entry.message_seq))
}

async fn redis_stream_entry_matches(
    connection: &mut redis::aio::ConnectionManager,
    stream_key: &str,
    entry_id: &str,
    expected_fields: &[(String, String)],
) -> Result<bool> {
    let reply = redis::cmd("XRANGE")
        .arg(stream_key)
        .arg(entry_id)
        .arg(entry_id)
        .arg("COUNT")
        .arg(1)
        .query_async::<StreamRangeReply>(connection)
        .await
        .context("Redis XRANGE failed while checking XADD result")?;
    redis_stream_entry_matches_reply(&reply, entry_id, expected_fields)
}

pub(super) fn redis_stream_entry_matches_reply(
    reply: &StreamRangeReply,
    entry_id: &str,
    expected_fields: &[(String, String)],
) -> Result<bool> {
    let Some(existing) = reply.ids.first() else {
        return Ok(false);
    };
    if reply.ids.len() != 1
        || existing.id != entry_id
        || existing.map.len() != expected_fields.len()
    {
        return Ok(false);
    }

    for (field, expected_value) in expected_fields {
        let Some(value) = existing.map.get(field) else {
            return Ok(false);
        };
        let actual_value = redis::from_redis_value::<String>(value.clone())
            .with_context(|| format!("Redis XRANGE returned invalid value for field {field}"))?;
        if actual_value != *expected_value {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn redis_entry_fields(
    entry: &BroadcasterRedisStreamEntry,
) -> Result<Vec<(String, String)>> {
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

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
