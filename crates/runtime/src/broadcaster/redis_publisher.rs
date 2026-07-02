use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use futures::future::BoxFuture;
use rand::Rng;
use redis::streams::StreamRangeReply;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::time::{timeout, Instant};
use tracing::{debug, warn};

use crate::config::BroadcasterRedisConfig;
use crate::metrics::{
    emit_broadcaster_redis_append_failure, emit_broadcaster_redis_generation_reset,
};
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterGenerationHandoff,
    BroadcasterHeartbeat, BroadcasterPayload, BroadcasterProgress, BroadcasterRedisReplayBoundary,
    BroadcasterRedisStreamEntry,
};
use state_history::StateHistoryWriter;

const APPEND_EXHAUSTED_MESSAGE: &str = "Redis broadcaster stream append retry window exhausted";
const RETRY_BACKOFF_BASE: Duration = Duration::from_millis(5);
const RETRY_BACKOFF_CAP: Duration = Duration::from_millis(200);
const MIN_WRITER_LEASE_TTL: Duration = Duration::from_secs(30);
const WRITER_LEASE_HEARTBEAT_MULTIPLIER: u32 = 3;
const STALE_WRITER_MESSAGE: &str = "stale Redis broadcaster writer";
pub(super) const GENERATION_PLACEHOLDER: &str = "__GENERATION__";
pub(super) const PREVIOUS_STREAM_ID_PLACEHOLDER: &str = "__PREVIOUS_STREAM_ID__";
pub(super) const PREVIOUS_ENTRY_ID_PLACEHOLDER: &str = "__PREVIOUS_ENTRY_ID__";

// Redis has no native "check active writer, then XADD" command. These small Lua
// scripts keep the fence and stream mutation in one Redis operation.
const PROMOTE_WRITER_SCRIPT: &str = r#"
local stream_key = KEYS[1]
local writer_key = KEYS[2]
local generation_key = KEYS[3]
local writer_token = ARGV[1]
local lease_ttl_ms = ARGV[2]
local maxlen = ARGV[3]
local expected_writer_token = ARGV[4]
local expected_generation = ARGV[5]
local normal_marker_field_count = tonumber(ARGV[6] or "0")
local handoff_marker_field_count = tonumber(ARGV[7] or "0")

-- Generation resets are only allowed by the current active writer. A passive
-- promotion passes no expectation and claims the next Redis generation.
if expected_writer_token ~= "" then
  if redis.call("GET", writer_key) ~= expected_writer_token then
    return redis.error_reply("stale Redis broadcaster writer token")
  end
  if tostring(redis.call("GET", generation_key) or "") ~= expected_generation then
    return redis.error_reply("stale Redis broadcaster writer generation")
  end
end

local previous_entry_id = ""
local previous_stream_id = ""
local tail = redis.call("XREVRANGE", stream_key, "+", "-", "COUNT", 1)
if #tail > 0 then
  previous_entry_id = tostring(tail[1][1])
  local tail_fields = tail[1][2]
  for index = 1, #tail_fields - 1, 2 do
    if tail_fields[index] == "stream_id" then
      previous_stream_id = tostring(tail_fields[index + 1])
      break
    end
  end
  if previous_stream_id == "" then
    return redis.error_reply("Redis broadcaster previous stream tail missing stream_id")
  end
end

local current_generation = tonumber(redis.call("GET", generation_key) or "0")
local stream_generation = 0
-- If the generation key was lost but the stream still exists, continue after
-- the stream's highest generation instead of reusing Redis entry ids.
local info = redis.pcall("XINFO", "STREAM", stream_key)
if type(info) == "table" and info["err"] then
  if not string.find(info["err"], "no such key") then
    return redis.error_reply(info["err"])
  end
elseif type(info) == "table" then
  for index = 1, #info - 1, 2 do
    if info[index] == "last-generated-id" then
      local last_id = tostring(info[index + 1])
      local generation = string.match(last_id, "^(%d+)-")
      if generation then
        stream_generation = tonumber(generation)
      end
      break
    end
  end
end

if current_generation < stream_generation then
  redis.call("SET", generation_key, stream_generation)
end
local generation = redis.call("INCR", generation_key)
-- The new writer token and generation marker move together. A stale writer can
-- either append before this script runs, or be rejected after it.
redis.call("SET", writer_key, writer_token, "PX", lease_ttl_ms)

local entry_id = tostring(generation) .. "-1"
local command = {"XADD", stream_key}
if maxlen ~= "" then
  table.insert(command, "MAXLEN")
  table.insert(command, "~")
  table.insert(command, maxlen)
end
table.insert(command, entry_id)

local marker_start = 8
local marker_field_count = normal_marker_field_count
if previous_entry_id ~= "" and handoff_marker_field_count > 0 then
  marker_start = 8 + (normal_marker_field_count * 2)
  marker_field_count = handoff_marker_field_count
end

for offset = 0, marker_field_count - 1 do
  local index = marker_start + (offset * 2)
  table.insert(command, ARGV[index])
  local value = string.gsub(ARGV[index + 1], "__GENERATION__", tostring(generation))
  value = string.gsub(value, "__PREVIOUS_STREAM_ID__", previous_stream_id)
  value = string.gsub(value, "__PREVIOUS_ENTRY_ID__", previous_entry_id)
  table.insert(command, value)
end

redis.call(unpack(command))
return {tostring(generation), entry_id, previous_stream_id, previous_entry_id}
"#;

const APPEND_FENCED_SCRIPT: &str = r#"
local stream_key = KEYS[1]
local writer_key = KEYS[2]
local generation_key = KEYS[3]
local writer_token = ARGV[1]
local generation = ARGV[2]
local lease_ttl_ms = ARGV[3]
local maxlen = ARGV[4]
local entry_id = ARGV[5]

-- This is the actual fence. A separate GET before XADD would still leave a
-- race, so the ownership check and append stay in the same script.
if redis.call("GET", writer_key) ~= writer_token then
  return redis.error_reply("stale Redis broadcaster writer token")
end
if tostring(redis.call("GET", generation_key) or "") ~= generation then
  return redis.error_reply("stale Redis broadcaster writer generation")
end

-- Successful writes keep the lease alive. Idle periods use the renew script.
redis.call("PEXPIRE", writer_key, lease_ttl_ms)
local command = {"XADD", stream_key}
if maxlen ~= "" then
  table.insert(command, "MAXLEN")
  table.insert(command, "~")
  table.insert(command, maxlen)
end
table.insert(command, entry_id)
for index = 6, #ARGV, 2 do
  table.insert(command, ARGV[index])
  table.insert(command, ARGV[index + 1])
end
return redis.call(unpack(command))
"#;

const RENEW_WRITER_SCRIPT: &str = r#"
-- Used by status checks, heartbeats without payloads, and snapshot serving to
-- prove this process still owns the active writer before exposing state.
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return redis.error_reply("stale Redis broadcaster writer token")
end
if tostring(redis.call("GET", KEYS[2]) or "") ~= ARGV[2] then
  return redis.error_reply("stale Redis broadcaster writer generation")
end
redis.call("PEXPIRE", KEYS[1], ARGV[3])
return "OK"
"#;

#[derive(Debug, Clone)]
pub struct BroadcasterRedisPublisherConfig {
    pub stream_key: String,
    pub chain_id: u64,
    pub append_retry_window: Duration,
    pub maxlen: Option<u64>,
    pub writer_lease_ttl: Duration,
}

impl BroadcasterRedisPublisherConfig {
    pub fn from_redis_config(
        redis_config: &BroadcasterRedisConfig,
        chain_id: u64,
        heartbeat_interval: Duration,
    ) -> Self {
        Self {
            stream_key: redis_config.stream_key.clone(),
            chain_id,
            append_retry_window: Duration::from_millis(redis_config.append_retry_window_ms),
            maxlen: redis_config.maxlen,
            writer_lease_ttl: writer_lease_ttl_for_heartbeat_interval(heartbeat_interval),
        }
    }
}

pub trait RedisStreamWriter: Send + Sync {
    fn promote<'a>(
        &'a self,
        command: RedisPromotionCommand<'a>,
    ) -> BoxFuture<'a, Result<RedisPromotionResult>> {
        Box::pin(async move {
            let _ = command;
            Err(anyhow!(
                "Redis writer does not support active writer promotion"
            ))
        })
    }

    fn append_fenced<'a>(
        &'a self,
        command: RedisAppendCommand<'a>,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let _ = command;
            Err(anyhow!("Redis writer does not support fenced appends"))
        })
    }

    fn renew_writer<'a>(&'a self, command: RedisRenewCommand<'a>) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let _ = command;
            Err(anyhow!(
                "Redis writer does not support active writer lease renewal"
            ))
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RedisPromotionCommand<'a> {
    pub stream_key: &'a str,
    pub writer_key: &'a str,
    pub writer_generation_key: &'a str,
    pub maxlen: Option<u64>,
    pub writer_token: &'a str,
    pub expected_writer_token: Option<&'a str>,
    pub expected_generation: Option<u64>,
    pub lease_ttl: Duration,
    pub normal_marker_fields: &'a [(String, String)],
    pub handoff_marker_fields: &'a [(String, String)],
}

#[derive(Debug, Clone, Copy)]
pub struct RedisAppendCommand<'a> {
    pub stream_key: &'a str,
    pub writer_key: &'a str,
    pub writer_generation_key: &'a str,
    pub maxlen: Option<u64>,
    pub writer_token: &'a str,
    pub generation: u64,
    pub lease_ttl: Duration,
    pub entry: &'a BroadcasterRedisStreamEntry,
}

#[derive(Debug, Clone, Copy)]
pub struct RedisRenewCommand<'a> {
    pub writer_key: &'a str,
    pub writer_generation_key: &'a str,
    pub writer_token: &'a str,
    pub generation: u64,
    pub lease_ttl: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisPromotionResult {
    pub generation: u64,
    pub entry_id: String,
    pub previous_stream_id: Option<String>,
    pub previous_entry_id: Option<String>,
    pub marker_entry: BroadcasterRedisStreamEntry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BroadcasterRedisPromotion {
    pub boundary: BroadcasterRedisReplayBoundary,
    pub marker_entry: BroadcasterRedisStreamEntry,
    pub marker_redis_entry_id: String,
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
    fn promote<'a>(
        &'a self,
        request: RedisPromotionCommand<'a>,
    ) -> BoxFuture<'a, Result<RedisPromotionResult>> {
        Box::pin(async move {
            let mut command = redis::cmd("EVAL");
            command
                .arg(PROMOTE_WRITER_SCRIPT)
                .arg(3)
                .arg(request.stream_key)
                .arg(request.writer_key)
                .arg(request.writer_generation_key)
                .arg(request.writer_token)
                .arg(lease_ttl_ms(request.lease_ttl))
                .arg(
                    request
                        .maxlen
                        .map(|value| value.to_string())
                        .unwrap_or_default(),
                )
                .arg(request.expected_writer_token.unwrap_or_default())
                .arg(
                    request
                        .expected_generation
                        .map(|value| value.to_string())
                        .unwrap_or_default(),
                )
                .arg(request.normal_marker_fields.len())
                .arg(request.handoff_marker_fields.len());
            for (field, value) in request.normal_marker_fields {
                command.arg(field).arg(value);
            }
            for (field, value) in request.handoff_marker_fields {
                command.arg(field).arg(value);
            }

            let mut connection = self.connection.clone();
            let reply = command
                .query_async::<Vec<String>>(&mut connection)
                .await
                .context("Redis active writer promotion failed")?;
            let [generation, entry_id, previous_stream_id, previous_entry_id] = reply.as_slice()
            else {
                return Err(anyhow!(
                    "Redis active writer promotion returned invalid reply"
                ));
            };
            let generation = generation.parse::<u64>().with_context(|| {
                format!("Redis active writer promotion returned invalid generation: {generation}")
            })?;
            let previous_stream_id = non_empty_string(previous_stream_id);
            let previous_entry_id = non_empty_string(previous_entry_id);
            let marker_entry = promotion_marker_entry_from_fields(
                request.normal_marker_fields,
                request.handoff_marker_fields,
                generation,
                previous_stream_id.as_deref(),
                previous_entry_id.as_deref(),
            )?;
            Ok(RedisPromotionResult {
                generation,
                entry_id: entry_id.clone(),
                previous_stream_id,
                previous_entry_id,
                marker_entry,
            })
        })
    }

    fn append_fenced<'a>(
        &'a self,
        request: RedisAppendCommand<'a>,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let entry_id = redis_entry_id(request.entry)?;
            let fields = redis_entry_fields(request.entry)?;
            let mut connection = self.connection.clone();
            let mut command = redis::cmd("EVAL");
            command
                .arg(APPEND_FENCED_SCRIPT)
                .arg(3)
                .arg(request.stream_key)
                .arg(request.writer_key)
                .arg(request.writer_generation_key)
                .arg(request.writer_token)
                .arg(request.generation)
                .arg(lease_ttl_ms(request.lease_ttl))
                .arg(
                    request
                        .maxlen
                        .map(|value| value.to_string())
                        .unwrap_or_default(),
                )
                .arg(&entry_id);
            for (field, value) in &fields {
                command.arg(field).arg(value);
            }
            let result = command.query_async::<String>(&mut connection).await;
            match result {
                Ok(entry_id) => Ok(entry_id),
                Err(error) => {
                    if redis_stream_entry_matches(
                        &mut connection,
                        request.stream_key,
                        &entry_id,
                        &fields,
                    )
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

    fn renew_writer<'a>(&'a self, request: RedisRenewCommand<'a>) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut connection = self.connection.clone();
            redis::cmd("EVAL")
                .arg(RENEW_WRITER_SCRIPT)
                .arg(2)
                .arg(request.writer_key)
                .arg(request.writer_generation_key)
                .arg(request.writer_token)
                .arg(request.generation)
                .arg(lease_ttl_ms(request.lease_ttl))
                .query_async::<String>(&mut connection)
                .await
                .context("Redis active writer lease renewal failed")?;
            Ok(())
        })
    }
}

pub struct BroadcasterRedisPublisher {
    config: BroadcasterRedisPublisherConfig,
    writer: Arc<dyn RedisStreamWriter>,
    writer_key: String,
    writer_generation_key: String,
    writer_token: String,
    state_history: Option<StateHistoryWriter>,
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
    pub mode: &'static str,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BroadcasterRedisPublisherMode {
    Passive,
    Active,
    Retired,
    Unhealthy,
}

impl BroadcasterRedisPublisherMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Passive => "passive",
            Self::Active => "active",
            Self::Retired => "retired",
            Self::Unhealthy => "unhealthy",
        }
    }

    const fn is_healthy(self) -> bool {
        matches!(self, Self::Passive | Self::Active)
    }
}

#[derive(Debug)]
struct BroadcasterRedisPublisherState {
    mode: BroadcasterRedisPublisherMode,
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
        self.mode = BroadcasterRedisPublisherMode::Unhealthy;
        self.last_error = Some(last_error);
    }

    fn record_retired(&mut self, last_error: String) {
        self.mode = BroadcasterRedisPublisherMode::Retired;
        self.last_error = Some(last_error);
    }

    fn activate_generation(
        &mut self,
        chain_id: u64,
        generation: u64,
        count_generation_reset: bool,
    ) {
        if count_generation_reset {
            self.generation_reset_count = self.generation_reset_count.saturating_add(1);
        }
        self.mode = BroadcasterRedisPublisherMode::Active;
        self.generation = generation;
        self.stream_id = format_redis_stream_id(chain_id, self.generation);
        self.snapshot_id = format_redis_snapshot_id(chain_id, self.generation);
        self.next_message_seq = 2;
        self.latest_entry_id = Some(format!("{}-1", self.generation));
        self.last_error = None;
    }
}

impl BroadcasterRedisPublisher {
    pub fn new(
        config: BroadcasterRedisPublisherConfig,
        writer: Arc<dyn RedisStreamWriter>,
    ) -> Self {
        Self::new_with_mode(config, writer, 1, BroadcasterRedisPublisherMode::Passive)
    }

    fn new_with_mode(
        config: BroadcasterRedisPublisherConfig,
        writer: Arc<dyn RedisStreamWriter>,
        generation: u64,
        mode: BroadcasterRedisPublisherMode,
    ) -> Self {
        let stream_id = format_redis_stream_id(config.chain_id, generation);
        let snapshot_id = format_redis_snapshot_id(config.chain_id, generation);
        let writer_key = redis_writer_key(&config.stream_key);
        let writer_generation_key = redis_writer_generation_key(&config.stream_key);
        Self {
            config,
            writer,
            writer_key,
            writer_generation_key,
            writer_token: new_writer_token(),
            state_history: None,
            inner: Arc::new(Mutex::new(BroadcasterRedisPublisherState {
                mode,
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

    pub fn with_state_history_writer(mut self, state_history: Option<StateHistoryWriter>) -> Self {
        self.state_history = state_history;
        self
    }

    pub async fn publish_accepted_payload(
        &self,
        payload: BroadcasterPayload,
    ) -> Result<Option<(BroadcasterRedisStreamEntry, String)>> {
        let mut guard = self.inner.lock().await;
        match guard.mode {
            BroadcasterRedisPublisherMode::Passive => return Ok(None),
            BroadcasterRedisPublisherMode::Retired => {
                return Err(anyhow!(
                    "Redis broadcaster publisher is retired; this process is no longer the active writer"
                ))
            }
            BroadcasterRedisPublisherMode::Unhealthy => {
                let error = guard
                    .last_error
                    .as_deref()
                    .unwrap_or("publisher is unhealthy");
                return Err(anyhow!(
                    "Redis broadcaster publisher is unhealthy; shared broadcaster generation reset is required before publishing more deltas: {error}"
                ));
            }
            BroadcasterRedisPublisherMode::Active => {}
        }
        if let Some(error) = &guard.last_error {
            return Err(anyhow!(
                "Redis broadcaster publisher is unhealthy; shared broadcaster generation reset is required before publishing more deltas: {error}"
            ));
        }
        let payload = normalize_live_payload(payload, &guard.snapshot_id)?;
        let append_failures_before = guard.append_failure_count;
        match self.append_payload_locked(&mut guard, payload).await {
            Ok((entry, entry_id)) => {
                self.enqueue_state_history(&entry, &entry_id).await;
                Ok(Some((entry, entry_id)))
            }
            Err(error) => {
                let message = format!("{error:#}");
                if is_stale_writer_error(&error) {
                    guard.record_retired(message);
                } else {
                    let retry_exhausted = guard.append_failure_count > append_failures_before;
                    guard.record_unhealthy(message, retry_exhausted);
                }
                Err(error)
            }
        }
    }

    pub async fn promote(
        &self,
        base_heads: Vec<BroadcasterBackendHead>,
        reason: impl Into<String>,
    ) -> Result<BroadcasterRedisReplayBoundary> {
        Ok(self.promote_with_marker(base_heads, reason).await?.boundary)
    }

    pub async fn promote_with_marker(
        &self,
        base_heads: Vec<BroadcasterBackendHead>,
        reason: impl Into<String>,
    ) -> Result<BroadcasterRedisPromotion> {
        let backends = backends_from_base_heads(&base_heads);
        self.promote_locked(backends, Some(base_heads), reason.into(), false, false)
            .await
    }

    pub async fn renew_lease(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        if guard.mode == BroadcasterRedisPublisherMode::Passive {
            return Ok(());
        }
        self.verify_writer_fence_locked(&mut guard, "active writer lease renewal")
            .await
    }

    pub async fn verify_active_writer(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        self.verify_writer_fence_locked(&mut guard, "active writer verification")
            .await
    }

    pub async fn replay_boundary(&self) -> Result<BroadcasterRedisReplayBoundary> {
        let mut guard = self.inner.lock().await;
        self.verify_writer_fence_locked(&mut guard, "replay boundary")
            .await?;
        self.replay_boundary_locked(&guard)
    }

    pub async fn reset_generation_boundary(
        &self,
        reason: impl Into<String>,
        backends: Vec<BroadcasterBackend>,
    ) -> Result<BroadcasterRedisReplayBoundary> {
        Ok(self
            .promote_locked(backends, None, reason.into(), true, true)
            .await?
            .boundary)
    }

    pub async fn mode(&self) -> BroadcasterRedisPublisherMode {
        self.inner.lock().await.mode
    }

    pub async fn status_snapshot(&self) -> BroadcasterRedisPublisherStatus {
        let guard = self.inner.lock().await;
        self.status_snapshot_locked(&guard)
    }

    pub async fn verified_status_snapshot(&self) -> BroadcasterRedisPublisherStatus {
        let mut guard = self.inner.lock().await;
        if guard.mode == BroadcasterRedisPublisherMode::Active && guard.last_error.is_none() {
            let _ = self
                .verify_writer_fence_locked(&mut guard, "status snapshot")
                .await;
        }
        self.status_snapshot_locked(&guard)
    }

    fn status_snapshot_locked(
        &self,
        guard: &BroadcasterRedisPublisherState,
    ) -> BroadcasterRedisPublisherStatus {
        let replay_boundary =
            if guard.mode == BroadcasterRedisPublisherMode::Active && guard.last_error.is_none() {
                self.replay_boundary_locked(guard).ok()
            } else {
                None
            };
        BroadcasterRedisPublisherStatus {
            healthy: guard.mode.is_healthy() && guard.last_error.is_none(),
            mode: guard.mode.as_str(),
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

    async fn verify_writer_fence_locked(
        &self,
        guard: &mut BroadcasterRedisPublisherState,
        context: &str,
    ) -> Result<()> {
        match guard.mode {
            BroadcasterRedisPublisherMode::Active => {}
            BroadcasterRedisPublisherMode::Passive => {
                return Err(anyhow!(
                    "Redis broadcaster {context} is unavailable while publisher mode is passive"
                ));
            }
            BroadcasterRedisPublisherMode::Retired => {
                return Err(anyhow!(
                    "Redis broadcaster publisher is retired; this process is no longer the active writer"
                ));
            }
            BroadcasterRedisPublisherMode::Unhealthy => {
                let error = guard
                    .last_error
                    .as_deref()
                    .unwrap_or("publisher is unhealthy");
                return Err(anyhow!(
                    "Redis broadcaster publisher is unhealthy; {context} is unavailable: {error}"
                ));
            }
        }
        if let Some(error) = &guard.last_error {
            return Err(anyhow!(
                "Redis broadcaster {context} is unavailable while publisher is unhealthy: {error}"
            ));
        }

        if let Err(error) = self
            .writer
            .renew_writer(RedisRenewCommand {
                writer_key: &self.writer_key,
                writer_generation_key: &self.writer_generation_key,
                writer_token: &self.writer_token,
                generation: guard.generation,
                lease_ttl: self.config.writer_lease_ttl,
            })
            .await
        {
            let message = format!("{error:#}");
            if is_stale_writer_error(&error) {
                guard.record_retired(message);
            } else {
                guard.record_unhealthy(message, false);
            }
            return Err(error);
        }

        guard.last_error = None;
        Ok(())
    }

    async fn promote_locked(
        &self,
        backends: Vec<BroadcasterBackend>,
        handoff_base_heads: Option<Vec<BroadcasterBackendHead>>,
        reason: String,
        count_generation_reset: bool,
        require_current_writer: bool,
    ) -> Result<BroadcasterRedisPromotion> {
        let mut guard = self.inner.lock().await;
        if guard.mode == BroadcasterRedisPublisherMode::Retired {
            return Err(anyhow!(
                "Redis broadcaster publisher is retired; this process cannot promote a writer generation"
            ));
        }
        if require_current_writer {
            match guard.mode {
                BroadcasterRedisPublisherMode::Active
                | BroadcasterRedisPublisherMode::Unhealthy => {}
                BroadcasterRedisPublisherMode::Passive => {
                    return Err(anyhow!(
                        "Redis broadcaster publisher is passive; this process cannot reset a writer generation"
                    ));
                }
                BroadcasterRedisPublisherMode::Retired => {
                    unreachable!("retired publisher returned above")
                }
            }
        }

        let normal_marker_fields = generation_marker_template_fields(
            self.config.chain_id,
            backends.clone(),
            reason.clone(),
            None,
        )?;
        let handoff_marker_fields = handoff_base_heads
            .map(|base_heads| {
                let handoff = BroadcasterGenerationHandoff::new(
                    PREVIOUS_STREAM_ID_PLACEHOLDER,
                    PREVIOUS_ENTRY_ID_PLACEHOLDER,
                    base_heads,
                )?;
                generation_marker_template_fields(
                    self.config.chain_id,
                    backends,
                    reason.clone(),
                    Some(handoff),
                )
            })
            .transpose()?
            .unwrap_or_default();
        let expected_writer_token = require_current_writer.then_some(self.writer_token.as_str());
        let expected_generation = require_current_writer.then_some(guard.generation);
        let promotion = self
            .writer
            .promote(RedisPromotionCommand {
                stream_key: &self.config.stream_key,
                writer_key: &self.writer_key,
                writer_generation_key: &self.writer_generation_key,
                maxlen: self.config.maxlen,
                writer_token: &self.writer_token,
                expected_writer_token,
                expected_generation,
                lease_ttl: self.config.writer_lease_ttl,
                normal_marker_fields: &normal_marker_fields,
                handoff_marker_fields: &handoff_marker_fields,
            })
            .await;

        let promotion = match promotion {
            Ok(promotion) => promotion,
            Err(error) => {
                let message = format!("{error:#}");
                if is_stale_writer_error(&error) {
                    guard.record_retired(message);
                } else {
                    guard.record_unhealthy(message, false);
                }
                return Err(error);
            }
        };

        guard.activate_generation(
            self.config.chain_id,
            promotion.generation,
            count_generation_reset,
        );
        if count_generation_reset {
            emit_broadcaster_redis_generation_reset();
        }
        warn!(
            event = "redis_generation_reset",
            stream_id = guard.stream_id.as_str(),
            snapshot_id = guard.snapshot_id.as_str(),
            generation = guard.generation,
            reason,
            "Redis broadcaster generation promoted"
        );
        let boundary = self.replay_boundary_locked(&guard)?;
        self.enqueue_state_history(&promotion.marker_entry, &promotion.entry_id)
            .await;
        Ok(BroadcasterRedisPromotion {
            boundary,
            marker_entry: promotion.marker_entry,
            marker_redis_entry_id: promotion.entry_id,
        })
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
        let entry = BroadcasterRedisStreamEntry::from_envelope(self.config.chain_id, &envelope)?;
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
                self.writer.append_fenced(RedisAppendCommand {
                    stream_key: &self.config.stream_key,
                    writer_key: &self.writer_key,
                    writer_generation_key: &self.writer_generation_key,
                    maxlen: self.config.maxlen,
                    writer_token: &self.writer_token,
                    generation: guard.generation,
                    lease_ttl: self.config.writer_lease_ttl,
                    entry,
                }),
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
                    if is_stale_writer_error(&error) {
                        self.record_write_failure(guard, entry, attempts, &error);
                        return Err(error);
                    }
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

    async fn enqueue_state_history(
        &self,
        entry: &BroadcasterRedisStreamEntry,
        redis_entry_id: &str,
    ) {
        let Some(state_history) = &self.state_history else {
            return;
        };
        if let Err(error) = state_history
            .enqueue_entry(entry.clone(), redis_entry_id.to_string())
            .await
        {
            warn!(
                error = %error,
                "State history writer did not accept Redis broadcaster entry"
            );
        }
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

fn backends_from_base_heads(base_heads: &[BroadcasterBackendHead]) -> Vec<BroadcasterBackend> {
    let mut backends = base_heads
        .iter()
        .map(|head| head.backend)
        .collect::<Vec<_>>();
    backends.sort();
    backends.dedup();
    backends
}

fn generation_marker_template_fields(
    chain_id: u64,
    backends: Vec<BroadcasterBackend>,
    reason: String,
    handoff: Option<BroadcasterGenerationHandoff>,
) -> Result<Vec<(String, String)>> {
    let stream_id = format!("chain-{chain_id}-stream-{GENERATION_PLACEHOLDER}");
    let snapshot_id = format!("chain-{chain_id}-snapshot-{GENERATION_PLACEHOLDER}");
    let marker = match handoff {
        Some(handoff) => BroadcasterProgress::new_with_handoff(
            chain_id,
            snapshot_id.clone(),
            backends,
            reason,
            handoff,
        )?,
        None => BroadcasterProgress::new(chain_id, snapshot_id.clone(), backends, reason)?,
    };
    let envelope = BroadcasterEnvelope::new(stream_id, 1, BroadcasterPayload::Progress(marker));
    let entry = BroadcasterRedisStreamEntry::from_envelope(chain_id, &envelope)?;
    redis_entry_fields(&entry)
}

fn promotion_marker_entry_from_fields(
    normal_marker_fields: &[(String, String)],
    handoff_marker_fields: &[(String, String)],
    generation: u64,
    previous_stream_id: Option<&str>,
    previous_entry_id: Option<&str>,
) -> Result<BroadcasterRedisStreamEntry> {
    let marker_fields = if previous_stream_id.is_some()
        && previous_entry_id.is_some()
        && !handoff_marker_fields.is_empty()
    {
        handoff_marker_fields
    } else {
        normal_marker_fields
    };
    let mut value = serde_json::Map::new();
    for (field, field_value) in marker_fields {
        let mut field_value = field_value.replace(GENERATION_PLACEHOLDER, &generation.to_string());
        if let Some(previous_stream_id) = previous_stream_id {
            field_value = field_value.replace(PREVIOUS_STREAM_ID_PLACEHOLDER, previous_stream_id);
        }
        if let Some(previous_entry_id) = previous_entry_id {
            field_value = field_value.replace(PREVIOUS_ENTRY_ID_PLACEHOLDER, previous_entry_id);
        }
        value.insert(field.clone(), Value::String(field_value));
    }
    let marker_entry: BroadcasterRedisStreamEntry = serde_json::from_value(Value::Object(value))
        .context("failed to build Redis promotion marker from Lua result")?;
    anyhow::ensure!(
        !marker_entry
            .payload_json
            .contains(PREVIOUS_STREAM_ID_PLACEHOLDER)
            && !marker_entry
                .payload_json
                .contains(PREVIOUS_ENTRY_ID_PLACEHOLDER),
        "Redis promotion marker still contains handoff placeholders"
    );
    Ok(marker_entry)
}

fn non_empty_string(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
}

fn redis_writer_key(stream_key: &str) -> String {
    // Keep deployment config to one public stream key; ownership keys follow it.
    format!("{stream_key}:writer")
}

fn redis_writer_generation_key(stream_key: &str) -> String {
    format!("{stream_key}:writer_generation")
}

fn new_writer_token() -> String {
    format!(
        "{}-{}-{:016x}",
        std::process::id(),
        current_time_ms(),
        rand::thread_rng().gen::<u64>()
    )
}

pub(super) fn writer_lease_ttl_for_heartbeat_interval(heartbeat_interval: Duration) -> Duration {
    heartbeat_interval
        .saturating_mul(WRITER_LEASE_HEARTBEAT_MULTIPLIER)
        .max(MIN_WRITER_LEASE_TTL)
}

fn lease_ttl_ms(lease_ttl: Duration) -> u64 {
    lease_ttl.as_millis().try_into().unwrap_or(u64::MAX)
}

fn is_stale_writer_error(error: &anyhow::Error) -> bool {
    format!("{error:#}").contains(STALE_WRITER_MESSAGE)
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
