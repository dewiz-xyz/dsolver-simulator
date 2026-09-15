use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy_primitives::keccak256;
use anyhow::{anyhow, Context, Result};
use futures::future::BoxFuture;
use rand::Rng;
use redis::streams::StreamRangeReply;
use serde_json::Value;
use state_history::StreamPosition;
use tokio::sync::{watch, Mutex};
use tokio::time::{timeout, Instant};
use tracing::{debug, warn};

use crate::broadcaster::state_history::{
    delta_entry_from_update, StateHistoryRuntime, StateHistoryUpdateProjection,
};
use crate::config::BroadcasterRedisConfig;
use crate::metrics::{
    emit_broadcaster_redis_append_failure, emit_broadcaster_writer_fence_failure,
};
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterHeartbeat,
    BroadcasterPayload, BroadcasterProgress, BroadcasterRecoveryCatchUp, BroadcasterRecoveryChunk,
    BroadcasterRecoveryCommit, BroadcasterRecoveryManifest, BroadcasterRecoveryStart,
    BroadcasterRedisReplayBoundary, BroadcasterRedisStreamEntry, BroadcasterUpdateMessage,
};

const APPEND_EXHAUSTED_MESSAGE: &str = "Redis broadcaster stream append retry window exhausted";
const RETRY_BACKOFF_BASE: Duration = Duration::from_millis(5);
const RETRY_BACKOFF_CAP: Duration = Duration::from_millis(200);
const MIN_WRITER_LEASE_TTL: Duration = Duration::from_secs(30);
const WRITER_LEASE_HEARTBEAT_MULTIPLIER: u32 = 3;
const STALE_WRITER_MESSAGE: &str = "stale Redis broadcaster writer";
// Keep this in sync with the writer fence errors returned by the Lua scripts.
pub(super) const MISSING_FENCE_MESSAGE: &str = "missing Redis broadcaster writer fence";
const WRITER_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const STATUS_VERIFY_TIMEOUT: Duration = Duration::from_secs(2);
pub(crate) const RECOVERY_REDIS_ENTRY_MAX_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const LIVE_REDIS_ENTRY_MAX_BYTES: usize = 8 * 1024 * 1024;
const RECOVERY_FRAGMENT_TARGET_BYTES: usize = 3_500_000;
const MAX_PENDING_RECOVERY_PAYLOADS: usize = 1024;
pub(crate) const STARTUP_HANDOFF_ABORT_AFTER: Duration = Duration::from_secs(60);
pub(super) const GENERATION_PLACEHOLDER: &str = "__GENERATION__";

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

-- A normal promotion may have committed even if its reply was lost. Retrying
-- with the same process token must return that generation instead of fencing it.
if expected_writer_token == "" then
  local current_writer_token = redis.call("GET", writer_key)
  local current_generation = tostring(redis.call("GET", generation_key) or "")
  if current_writer_token == writer_token and current_generation ~= "" then
    local entry_id = current_generation .. "-1"
    local marker = redis.call("XRANGE", stream_key, entry_id, entry_id, "COUNT", 1)
    if #marker == 1 then
      redis.call("PEXPIRE", writer_key, lease_ttl_ms)
      return {current_generation, entry_id}
    end
  end
end

-- Recovery may reclaim an expired fence, but it must never displace another
-- writer or reuse a generation after Redis lost its in-memory keys.
if expected_writer_token ~= "" then
  local current_writer_token = redis.call("GET", writer_key)
  local current_generation = tostring(redis.call("GET", generation_key) or "")
  if current_writer_token and current_writer_token ~= expected_writer_token then
    return redis.error_reply("stale Redis broadcaster writer token")
  end
  if current_generation ~= "" and current_generation ~= expected_generation then
    return redis.error_reply("stale Redis broadcaster writer generation")
  end
  if current_generation == "" then
    redis.call("SET", generation_key, expected_generation)
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

local marker_start = 7
for offset = 0, normal_marker_field_count - 1 do
  local index = marker_start + (offset * 2)
  table.insert(command, ARGV[index])
  local value = string.gsub(ARGV[index + 1], "__GENERATION__", tostring(generation))
  table.insert(command, value)
end

redis.call(unpack(command))
return {tostring(generation), entry_id}
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
local current_writer_token = redis.call("GET", writer_key)
if not current_writer_token then
  return redis.error_reply("missing Redis broadcaster writer fence")
end
if current_writer_token ~= writer_token then
  return redis.error_reply("stale Redis broadcaster writer token")
end
local current_generation = tostring(redis.call("GET", generation_key) or "")
if current_generation == "" then
  return redis.error_reply("missing Redis broadcaster writer generation")
end
if current_generation ~= generation then
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
local current_writer_token = redis.call("GET", KEYS[1])
if not current_writer_token then
  return redis.error_reply("missing Redis broadcaster writer fence")
end
if current_writer_token ~= ARGV[1] then
  return redis.error_reply("stale Redis broadcaster writer token")
end
local current_generation = tostring(redis.call("GET", KEYS[2]) or "")
if current_generation == "" then
  return redis.error_reply("missing Redis broadcaster writer generation")
end
if current_generation ~= ARGV[2] then
  return redis.error_reply("stale Redis broadcaster writer generation")
end
redis.call("PEXPIRE", KEYS[1], ARGV[3])
return "OK"
"#;

const VERIFY_WRITER_SCRIPT: &str = r#"
local current_writer_token = redis.call("GET", KEYS[1])
if not current_writer_token then
  return redis.error_reply("missing Redis broadcaster writer fence")
end
if current_writer_token ~= ARGV[1] then
  return redis.error_reply("stale Redis broadcaster writer token")
end
local current_generation = tostring(redis.call("GET", KEYS[2]) or "")
if current_generation == "" then
  return redis.error_reply("missing Redis broadcaster writer generation")
end
if current_generation ~= ARGV[2] then
  return redis.error_reply("stale Redis broadcaster writer generation")
end
return "OK"
"#;

const INSPECT_WRITER_SCRIPT: &str = r#"
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return redis.error_reply("stale Redis broadcaster writer token")
end
local generation = tostring(redis.call("GET", KEYS[2]) or "")
if generation ~= ARGV[2] then
  return redis.error_reply("stale Redis broadcaster writer generation")
end
local info = redis.pcall("XINFO", "STREAM", KEYS[3])
if type(info) == "table" and info["err"] then
  return redis.error_reply(info["err"])
end
local first_id = ""
local last_id = ""
for index = 1, #info - 1, 2 do
  if info[index] == "first-entry" and type(info[index + 1]) == "table" then
    first_id = tostring(info[index + 1][1] or "")
  elseif info[index] == "last-generated-id" then
    last_id = tostring(info[index + 1] or "")
  end
end
return {generation, first_id, last_id}
"#;

#[derive(Debug, Clone)]
pub struct BroadcasterRedisPublisherConfig {
    pub stream_key: String,
    pub chain_id: u64,
    pub append_retry_window: Duration,
    pub maxlen: Option<u64>,
    pub writer_lease_ttl: Duration,
    pub recovery_max_buffered_native_blocks: usize,
}

#[derive(Debug)]
pub(crate) struct BroadcasterRecoveryTransaction {
    pub(crate) recovery_id: String,
    pub(crate) backends: Vec<BroadcasterBackend>,
    pub(crate) replacement_json: String,
    pub(crate) buffer_a: Vec<BroadcasterUpdateMessage>,
}

#[derive(Debug, Clone)]
pub(crate) struct BroadcasterRecoveryPublication {
    pub(crate) recovery_id: String,
    pub(crate) target_state_version: u64,
    pub(crate) commit_entry_id: String,
    pub(crate) commit_position: StreamPosition,
    pub(crate) replay_boundary: BroadcasterRedisReplayBoundary,
    pub(crate) encoded_bytes: usize,
    pub(crate) chunk_count: usize,
    pub(crate) buffer_a_count: usize,
}

#[derive(Debug)]
pub(crate) struct OversizedRedisEntry {
    pub(crate) kind: simulator_core::broadcaster::BroadcasterMessageKind,
    pub(crate) encoded_bytes: usize,
    pub(crate) limit_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct RecoveryPublicationCancelled;

#[derive(Debug)]
pub(crate) struct RecoveryCommitAdoptionUncertain {
    message: String,
}

impl fmt::Display for RecoveryCommitAdoptionUncertain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Redis recovery commit may be authoritative but could not be adopted: {}",
            self.message
        )
    }
}

impl std::error::Error for RecoveryCommitAdoptionUncertain {}

impl fmt::Display for RecoveryPublicationCancelled {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Redis broadcaster recovery publication was cancelled")
    }
}

impl std::error::Error for RecoveryPublicationCancelled {}

impl fmt::Display for OversizedRedisEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "encoded Redis {} entry is {} bytes, above the {} byte limit",
            self.kind, self.encoded_bytes, self.limit_bytes
        )
    }
}

impl std::error::Error for OversizedRedisEntry {}

impl BroadcasterRedisPublisherConfig {
    pub fn from_redis_config(
        redis_config: &BroadcasterRedisConfig,
        chain_id: u64,
        heartbeat_interval: Duration,
        recovery_max_buffered_native_blocks: usize,
    ) -> Self {
        Self {
            stream_key: redis_config.stream_key.clone(),
            chain_id,
            append_retry_window: Duration::from_millis(redis_config.append_retry_window_ms),
            maxlen: redis_config.maxlen,
            writer_lease_ttl: writer_lease_ttl_for_heartbeat_interval(heartbeat_interval),
            recovery_max_buffered_native_blocks,
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

    fn verify_writer<'a>(&'a self, command: RedisVerifyCommand<'a>) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.renew_writer(RedisRenewCommand {
                writer_key: command.writer_key,
                writer_generation_key: command.writer_generation_key,
                writer_token: command.writer_token,
                generation: command.generation,
                lease_ttl: command.lease_ttl,
            })
            .await
        })
    }

    fn inspect_writer<'a>(
        &'a self,
        command: RedisInspectCommand<'a>,
    ) -> BoxFuture<'a, Result<RedisWriterInspection>> {
        Box::pin(async move {
            Ok(RedisWriterInspection {
                generation: command.generation,
                first_entry_id: Some(format!("{}-1", command.generation)),
                last_entry_id: None,
            })
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

#[derive(Debug, Clone, Copy)]
pub struct RedisVerifyCommand<'a> {
    pub writer_key: &'a str,
    pub writer_generation_key: &'a str,
    pub writer_token: &'a str,
    pub generation: u64,
    pub lease_ttl: Duration,
}

#[derive(Debug, Clone, Copy)]
pub struct RedisInspectCommand<'a> {
    pub stream_key: &'a str,
    pub writer_key: &'a str,
    pub writer_generation_key: &'a str,
    pub writer_token: &'a str,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisWriterInspection {
    pub generation: u64,
    pub first_entry_id: Option<String>,
    pub last_entry_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisPromotionResult {
    pub generation: u64,
    pub entry_id: String,
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

fn redis_promotion_command(request: RedisPromotionCommand<'_>) -> redis::Cmd {
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
        .arg(request.normal_marker_fields.len());
    for (field, value) in request.normal_marker_fields {
        command.arg(field).arg(value);
    }
    command
}

impl RedisStreamWriter for TokioRedisStreamWriter {
    fn promote<'a>(
        &'a self,
        request: RedisPromotionCommand<'a>,
    ) -> BoxFuture<'a, Result<RedisPromotionResult>> {
        Box::pin(async move {
            let mut connection = self.connection.clone();
            let first = redis_promotion_command(request)
                .query_async::<Vec<String>>(&mut connection)
                .await;
            let reply = match first {
                Ok(reply) => reply,
                Err(first_error) if request.expected_writer_token.is_none() => {
                    redis_promotion_command(request)
                        .query_async::<Vec<String>>(&mut connection)
                        .await
                        .with_context(|| {
                            format!(
                                "Redis active writer promotion retry failed after: {first_error}"
                            )
                        })?
                }
                Err(error) => return Err(error).context("Redis active writer promotion failed"),
            };
            let [generation, entry_id] = reply.as_slice() else {
                return Err(anyhow!(
                    "Redis active writer promotion returned invalid reply"
                ));
            };
            Ok(RedisPromotionResult {
                generation: generation.parse::<u64>().with_context(|| {
                    format!(
                        "Redis active writer promotion returned invalid generation: {generation}"
                    )
                })?,
                entry_id: entry_id.clone(),
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

    fn verify_writer<'a>(&'a self, request: RedisVerifyCommand<'a>) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut connection = self.connection.clone();
            redis::cmd("EVAL")
                .arg(VERIFY_WRITER_SCRIPT)
                .arg(2)
                .arg(request.writer_key)
                .arg(request.writer_generation_key)
                .arg(request.writer_token)
                .arg(request.generation)
                .query_async::<String>(&mut connection)
                .await
                .context("Redis active writer verification failed")?;
            Ok(())
        })
    }

    fn inspect_writer<'a>(
        &'a self,
        request: RedisInspectCommand<'a>,
    ) -> BoxFuture<'a, Result<RedisWriterInspection>> {
        Box::pin(async move {
            let mut connection = self.connection.clone();
            let reply = redis::cmd("EVAL")
                .arg(INSPECT_WRITER_SCRIPT)
                .arg(3)
                .arg(request.writer_key)
                .arg(request.writer_generation_key)
                .arg(request.stream_key)
                .arg(request.writer_token)
                .arg(request.generation)
                .query_async::<Vec<String>>(&mut connection)
                .await
                .context("Redis active writer inspection failed")?;
            let [generation, first_entry_id, last_entry_id] = reply.as_slice() else {
                return Err(anyhow!(
                    "Redis active writer inspection returned invalid reply"
                ));
            };
            Ok(RedisWriterInspection {
                generation: generation
                    .parse()
                    .context("Redis writer generation is invalid")?,
                first_entry_id: (!first_entry_id.is_empty()).then(|| first_entry_id.clone()),
                last_entry_id: (!last_entry_id.is_empty()).then(|| last_entry_id.clone()),
            })
        })
    }
}

pub struct BroadcasterRedisPublisher {
    config: BroadcasterRedisPublisherConfig,
    writer: Arc<dyn RedisStreamWriter>,
    writer_key: String,
    writer_generation_key: String,
    writer_token: String,
    inner: Arc<Mutex<BroadcasterRedisPublisherState>>,
    // Leaf-level cache lock; never hold it across an await.
    last_status: std::sync::Mutex<BroadcasterRedisPublisherStatus>,
    recovery_gate: Arc<Mutex<PublisherRecoveryGate>>,
    recovery_coordinator: Arc<Mutex<()>>,
    next_recovery_id: Arc<AtomicU64>,
    next_startup_handoff_id: Arc<AtomicU64>,
    feed_gate: watch::Sender<FeedGateState>,
    feed_pause_guard: Arc<Mutex<()>>,
    deployment_admission: watch::Sender<BroadcasterDeploymentAdmissionSnapshot>,
    state_history: Option<Arc<StateHistoryRuntime>>,
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
    pub retry_exhaustion_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BroadcasterDeploymentPhase {
    Warming,
    CandidateBuilding,
    Promoting,
    ArtifactFinalizing,
    HandoffDraining,
    Admitted,
    Retired,
    Fatal,
    ShuttingDown,
}

impl BroadcasterDeploymentPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Warming => "warming",
            Self::CandidateBuilding => "candidate_building",
            Self::Promoting => "promoting",
            Self::ArtifactFinalizing => "artifact_finalizing",
            Self::HandoffDraining => "handoff_draining",
            Self::Admitted => "admitted",
            Self::Retired => "retired",
            Self::Fatal => "fatal",
            Self::ShuttingDown => "shutting_down",
        }
    }

    pub const fn is_admitted(self) -> bool {
        matches!(self, Self::Admitted)
    }

    const fn is_terminal(self) -> bool {
        matches!(self, Self::Retired | Self::Fatal | Self::ShuttingDown)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BroadcasterDeploymentAdmissionSnapshot {
    pub admitted: bool,
    pub phase: BroadcasterDeploymentPhase,
    pub last_error: Option<String>,
}

impl BroadcasterDeploymentAdmissionSnapshot {
    fn new(phase: BroadcasterDeploymentPhase, last_error: Option<String>) -> Self {
        Self {
            admitted: phase.is_admitted(),
            phase,
            last_error,
        }
    }
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
    committed_state_version: u64,
    next_state_version: u64,
    latest_entry_id: Option<String>,
    append_success_count: u64,
    append_failure_count: u64,
    retry_exhaustion_count: u64,
    last_error: Option<String>,
    startup_handoff: Option<StartupHandoffBuffer>,
}

#[derive(Debug, Default)]
struct PublisherRecoveryGate {
    open_recovery_id: Option<String>,
    cancelled: Option<Arc<AtomicBool>>,
    complete_native_blocks: HashSet<u64>,
    pending_payloads: VecDeque<QueuedPublisherPayload>,
    max_pending_payloads: usize,
}

#[derive(Debug)]
struct QueuedPublisherPayload {
    payload: BroadcasterPayload,
    accepted_at: Instant,
}

#[derive(Debug)]
struct StartupHandoffBuffer {
    id: u64,
    started_at: Instant,
    payloads: VecDeque<BroadcasterPayload>,
    complete_native_blocks: HashSet<u64>,
    cancelled: bool,
    frozen: bool,
}

#[derive(Debug)]
pub(crate) struct FrozenStartupHandoff {
    id: u64,
    payloads: VecDeque<BroadcasterPayload>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct FeedGateState {
    pause_epoch: u64,
    paused: bool,
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

    fn activate_generation(&mut self, chain_id: u64, generation: u64) {
        self.mode = BroadcasterRedisPublisherMode::Active;
        self.generation = generation;
        self.stream_id = format_redis_stream_id(chain_id, self.generation);
        self.snapshot_id = format_redis_snapshot_id(chain_id, self.generation);
        self.next_message_seq = 2;
        self.committed_state_version = state_version_base(generation);
        self.next_state_version = self.committed_state_version;
        self.latest_entry_id = Some(format!("{}-1", self.generation));
        self.last_error = None;
    }

    fn allocate_state_version(&mut self) -> Result<u64> {
        self.next_state_version = self
            .next_state_version
            .checked_add(1)
            .ok_or_else(|| anyhow!("Redis broadcaster state version overflow"))?;
        Ok(self.next_state_version)
    }
}

impl BroadcasterRedisPublisher {
    pub fn new(
        config: BroadcasterRedisPublisherConfig,
        writer: Arc<dyn RedisStreamWriter>,
    ) -> Self {
        Self::new_with_mode(config, writer, 1, BroadcasterRedisPublisherMode::Passive)
    }

    pub(crate) fn recovery_max_buffered_native_blocks(&self) -> usize {
        self.config.recovery_max_buffered_native_blocks
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
        let (feed_gate, _) = watch::channel(FeedGateState::default());
        let deployment_phase = match mode {
            BroadcasterRedisPublisherMode::Active => BroadcasterDeploymentPhase::Admitted,
            BroadcasterRedisPublisherMode::Retired => BroadcasterDeploymentPhase::Retired,
            BroadcasterRedisPublisherMode::Passive | BroadcasterRedisPublisherMode::Unhealthy => {
                BroadcasterDeploymentPhase::Warming
            }
        };
        let (deployment_admission, _) = watch::channel(
            BroadcasterDeploymentAdmissionSnapshot::new(deployment_phase, None),
        );
        let initial_state_version = state_version_base(generation);
        let initial_status = BroadcasterRedisPublisherStatus {
            healthy: mode.is_healthy(),
            mode: mode.as_str(),
            stream_key: config.stream_key.clone(),
            stream_id: stream_id.clone(),
            snapshot_id: snapshot_id.clone(),
            latest_entry_id: None,
            replay_boundary: None,
            append_success_count: 0,
            append_failure_count: 0,
            retry_exhaustion_count: 0,
            last_error: None,
        };
        Self {
            config,
            writer,
            writer_key,
            writer_generation_key,
            writer_token: new_writer_token(),
            inner: Arc::new(Mutex::new(BroadcasterRedisPublisherState {
                mode,
                generation,
                stream_id,
                snapshot_id,
                next_message_seq: 1,
                committed_state_version: initial_state_version,
                next_state_version: initial_state_version,
                latest_entry_id: None,
                append_success_count: 0,
                append_failure_count: 0,
                retry_exhaustion_count: 0,
                last_error: None,
                startup_handoff: None,
            })),
            last_status: std::sync::Mutex::new(initial_status),
            recovery_gate: Arc::new(Mutex::new(PublisherRecoveryGate::default())),
            recovery_coordinator: Arc::new(Mutex::new(())),
            next_recovery_id: Arc::new(AtomicU64::new(1)),
            next_startup_handoff_id: Arc::new(AtomicU64::new(1)),
            feed_gate,
            feed_pause_guard: Arc::new(Mutex::new(())),
            deployment_admission,
            state_history: None,
        }
    }

    pub fn with_state_history(mut self, state_history: Arc<StateHistoryRuntime>) -> Self {
        self.state_history = Some(state_history);
        self
    }

    pub(crate) fn state_history(&self) -> Option<Arc<StateHistoryRuntime>> {
        self.state_history.as_ref().map(Arc::clone)
    }

    pub fn deployment_admission_snapshot(&self) -> BroadcasterDeploymentAdmissionSnapshot {
        self.deployment_admission.borrow().clone()
    }

    pub(crate) fn set_deployment_phase(
        &self,
        phase: BroadcasterDeploymentPhase,
        last_error: Option<String>,
    ) {
        self.deployment_admission.send_modify(|current| {
            if current.phase.is_terminal() || (current.admitted && !phase.is_terminal()) {
                return;
            }
            *current = BroadcasterDeploymentAdmissionSnapshot::new(phase, last_error);
        });
    }

    pub(crate) fn begin_shutdown(&self) {
        self.set_deployment_phase(BroadcasterDeploymentPhase::ShuttingDown, None);
    }

    pub(crate) async fn begin_startup_handoff(&self) -> Result<u64> {
        let mut guard = self.inner.lock().await;
        anyhow::ensure!(
            guard.mode == BroadcasterRedisPublisherMode::Passive,
            "startup handoff requires a passive Redis publisher"
        );
        let id = self.next_startup_handoff_id.fetch_add(1, Ordering::Relaxed);
        guard.startup_handoff = Some(StartupHandoffBuffer {
            id,
            started_at: Instant::now(),
            payloads: VecDeque::new(),
            complete_native_blocks: HashSet::new(),
            cancelled: false,
            frozen: false,
        });
        drop(guard);
        self.set_deployment_phase(BroadcasterDeploymentPhase::CandidateBuilding, None);
        Ok(id)
    }

    pub(crate) async fn abort_startup_handoff(&self, id: u64, error: impl Into<String>) {
        let error = error.into();
        let mut guard = self.inner.lock().await;
        if guard
            .startup_handoff
            .as_ref()
            .is_some_and(|buffer| buffer.id == id)
        {
            guard.startup_handoff = None;
        }
        drop(guard);
        self.set_deployment_phase(BroadcasterDeploymentPhase::Warming, Some(error));
    }

    pub(crate) async fn freeze_startup_handoff(&self, id: u64) -> Result<FrozenStartupHandoff> {
        let mut guard = self.inner.lock().await;
        anyhow::ensure!(
            guard.mode == BroadcasterRedisPublisherMode::Passive,
            "startup handoff can only freeze a passive publisher"
        );
        let buffer = guard
            .startup_handoff
            .as_mut()
            .filter(|buffer| buffer.id == id)
            .ok_or_else(|| anyhow!("startup handoff candidate is no longer current"))?;
        if buffer.started_at.elapsed() >= STARTUP_HANDOFF_ABORT_AFTER
            || buffer.complete_native_blocks.len()
                >= self.config.recovery_max_buffered_native_blocks
        {
            buffer.cancelled = true;
        }
        anyhow::ensure!(
            !buffer.cancelled,
            "startup handoff exceeded the configured {} native-block or 60-second bound",
            self.config.recovery_max_buffered_native_blocks
        );
        anyhow::ensure!(!buffer.frozen, "startup handoff is already frozen");
        buffer.frozen = true;
        Ok(FrozenStartupHandoff {
            id,
            payloads: std::mem::take(&mut buffer.payloads),
        })
    }

    pub(crate) async fn drain_startup_handoff(&self, handoff: FrozenStartupHandoff) -> Result<()> {
        self.set_deployment_phase(BroadcasterDeploymentPhase::HandoffDraining, None);
        let mut guard = self.inner.lock().await;
        anyhow::ensure!(
            guard.mode == BroadcasterRedisPublisherMode::Active,
            "startup handoff drain requires the promoted Redis writer"
        );
        anyhow::ensure!(
            guard
                .startup_handoff
                .as_ref()
                .is_some_and(|buffer| buffer.id == handoff.id && buffer.frozen),
            "startup handoff drain no longer matches the frozen candidate"
        );

        let append_failures_before = guard.append_failure_count;
        for payload in handoff.payloads {
            let payload = normalize_live_payload(payload, &guard.snapshot_id)?;
            let Err(error) = self.append_live_payload_locked(&mut guard, payload).await else {
                continue;
            };
            let message = format!("{error:#}");
            let phase = if is_stale_writer_error(&error) {
                BroadcasterDeploymentPhase::Retired
            } else {
                if guard.append_failure_count > append_failures_before {
                    guard.retry_exhaustion_count = guard.retry_exhaustion_count.saturating_add(1);
                }
                BroadcasterDeploymentPhase::Fatal
            };
            guard.record_retired(message.clone());
            self.set_deployment_phase(phase, Some(message));
            return Err(error);
        }
        guard.startup_handoff = None;
        Ok(())
    }

    pub(crate) fn admit_deployment(&self) {
        self.set_deployment_phase(BroadcasterDeploymentPhase::Admitted, None);
    }

    pub(crate) async fn begin_recovery(
        &self,
        backends: &[BroadcasterBackend],
        cancelled: Arc<AtomicBool>,
    ) -> String {
        // A new reservation cannot supersede a transaction whose commit may still land.
        let _coordinator = self.recovery_coordinator.lock().await;
        let recovery_sequence = self.next_recovery_id.fetch_add(1, Ordering::Relaxed);
        let recovery_id = format!("recovery-{recovery_sequence}");
        let mut gate = self.recovery_gate.lock().await;
        if gate.open_recovery_id.is_none() {
            gate.max_pending_payloads = 0;
        } else {
            trim_queued_target_backends(&mut gate.pending_payloads, backends);
        }
        gate.complete_native_blocks = queued_complete_native_blocks(&gate.pending_payloads);
        if gate.complete_native_blocks.len() >= self.config.recovery_max_buffered_native_blocks {
            cancelled.store(true, Ordering::Release);
        }
        gate.open_recovery_id = Some(recovery_id.clone());
        gate.cancelled = Some(cancelled);
        recovery_id
    }

    #[expect(
        clippy::too_many_lines,
        reason = "recovery queuing and active append ordering stay in one publication path"
    )]
    pub async fn publish_accepted_payload(&self, payload: BroadcasterPayload) -> Result<()> {
        let history_update = self.state_history.as_ref().and_then(|_| match &payload {
            BroadcasterPayload::Update(update) => {
                Some(StateHistoryUpdateProjection::from_update(update))
            }
            _ => None,
        });
        let mut recovery_gate = self.recovery_gate.lock().await;
        if recovery_gate.open_recovery_id.is_some() {
            if matches!(payload, BroadcasterPayload::Heartbeat(_)) {
                // The heartbeat task renews the lease while the reservation is open. Replaying
                // this later would publish stale backend heads.
                return Ok(());
            }
            let complete_block = complete_native_block(&payload);
            if let Some(block_number) = complete_block {
                recovery_gate.complete_native_blocks.insert(block_number);
            }
            if complete_block.is_some()
                && recovery_gate.complete_native_blocks.len()
                    >= self.config.recovery_max_buffered_native_blocks
            {
                if let Some(cancelled) = &recovery_gate.cancelled {
                    cancelled.store(true, Ordering::Release);
                }
            }
            anyhow::ensure!(
                recovery_gate.pending_payloads.len() < MAX_PENDING_RECOVERY_PAYLOADS,
                "Redis broadcaster recovery payload queue is full"
            );
            recovery_gate
                .pending_payloads
                .push_back(QueuedPublisherPayload {
                    payload,
                    accepted_at: Instant::now(),
                });
            recovery_gate.max_pending_payloads = recovery_gate
                .max_pending_payloads
                .max(recovery_gate.pending_payloads.len());
            return Ok(());
        }
        let mut guard = self.inner.lock().await;
        drop(recovery_gate);
        match guard.mode {
            BroadcasterRedisPublisherMode::Passive => {
                if let Some(buffer) = guard.startup_handoff.as_mut() {
                    anyhow::ensure!(
                        !buffer.frozen,
                        "startup handoff cannot accept publications after it is frozen"
                    );
                    if let Some(block_number) = complete_native_block(&payload) {
                        buffer.complete_native_blocks.insert(block_number);
                    }
                    if buffer.started_at.elapsed() >= STARTUP_HANDOFF_ABORT_AFTER
                        || buffer.complete_native_blocks.len()
                            >= self.config.recovery_max_buffered_native_blocks
                    {
                        buffer.cancelled = true;
                        buffer.payloads.clear();
                    } else if !buffer.cancelled {
                        buffer.payloads.push_back(payload);
                    }
                }
                return Ok(());
            }
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
                    "Redis broadcaster publisher is unhealthy; writer recovery is required before publishing more deltas: {error}"
                ));
            }
            BroadcasterRedisPublisherMode::Active => {}
        }
        if let Some(error) = &guard.last_error {
            return Err(anyhow!(
                "Redis broadcaster publisher is unhealthy; writer recovery is required before publishing more deltas: {error}"
            ));
        }
        let payload = normalize_live_payload(payload, &guard.snapshot_id)?;
        let append_failures_before = guard.append_failure_count;
        match self.append_live_payload_locked(&mut guard, payload).await {
            Ok((entry, _)) => {
                if let (Some(state_history), Some(update)) = (&self.state_history, history_update) {
                    enqueue_state_history_update(state_history, &guard, entry, update);
                }
                Ok(())
            }
            Err(error) => {
                if error.downcast_ref::<OversizedRedisEntry>().is_some() {
                    return Err(error);
                }
                let message = format!("{error:#}");
                if is_stale_writer_error(&error) {
                    guard.record_retired(message.clone());
                    self.set_deployment_phase(BroadcasterDeploymentPhase::Retired, Some(message));
                } else {
                    let retry_exhausted = guard.append_failure_count > append_failures_before;
                    guard.record_unhealthy(message, retry_exhausted);
                }
                Err(error)
            }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the fenced recovery transaction stays together so its contiguous append order is visible"
    )]
    pub(crate) async fn publish_recovery(
        &self,
        transaction: BroadcasterRecoveryTransaction,
        cancelled: &AtomicBool,
    ) -> Result<BroadcasterRecoveryPublication> {
        let _coordinator = self.recovery_coordinator.lock().await;
        ensure_recovery_not_cancelled(cancelled)?;
        let fragments = prepare_recovery_fragments(&transaction.replacement_json)?;
        let recovery_id = transaction.recovery_id.clone();
        let recovery_gate = self.recovery_gate.lock().await;
        anyhow::ensure!(
            recovery_gate.open_recovery_id.as_deref() == Some(recovery_id.as_str()),
            "Redis recovery reservation {recovery_id} is no longer active"
        );
        let mut guard = self.inner.lock().await;
        drop(recovery_gate);
        ensure_active_publisher(&guard)?;
        ensure_recovery_not_cancelled(cancelled)?;
        let target_state_version = guard.allocate_state_version()?;

        let chunk_count = fragments.len();
        let buffer_a_count = transaction.buffer_a.len();
        let commit_watermark = guard
            .next_message_seq
            .checked_add(u64::try_from(chunk_count)?)
            .and_then(|value| value.checked_add(u64::try_from(buffer_a_count).ok()?))
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| anyhow!("Redis broadcaster recovery message_seq overflow"))?;

        let mut chunk_entries = Vec::with_capacity(chunk_count);
        let mut chunk_encoded_bytes = Vec::with_capacity(chunk_count);
        let recovery_scope = RecoveryEntryScope {
            recovery_id: &recovery_id,
            target_state_version,
            backends: &transaction.backends,
        };
        for (chunk_index, fragment) in fragments.iter().enumerate() {
            let message_seq = guard
                .next_message_seq
                .checked_add(1)
                .and_then(|value| value.checked_add(u64::try_from(chunk_index).ok()?))
                .ok_or_else(|| anyhow!("Redis broadcaster recovery chunk sequence overflow"))?;
            let (entry, encoded_bytes) = build_recovery_chunk_entry(
                &self.config,
                &guard.stream_id,
                message_seq,
                recovery_scope,
                chunk_index,
                fragment,
            )?;
            chunk_entries.push(entry);
            chunk_encoded_bytes.push(u64::try_from(encoded_bytes)?);
        }

        let replacement_digest = digest_bytes(transaction.replacement_json.as_bytes());
        let manifest = BroadcasterRecoveryManifest::new(
            recovery_id.clone(),
            target_state_version,
            transaction.backends.clone(),
            u64::try_from(transaction.replacement_json.len())?,
            chunk_encoded_bytes,
            fragments
                .iter()
                .map(|fragment| fragment.digest.clone())
                .collect(),
            replacement_digest.clone(),
            u32::try_from(buffer_a_count)?,
            commit_watermark,
        )?;
        let start_entry = recovery_entry(
            self.config.chain_id,
            &guard.stream_id,
            guard.next_message_seq,
            BroadcasterPayload::RecoveryStart(BroadcasterRecoveryStart::new(manifest)?),
            target_state_version,
        )?;
        ensure_redis_entry_size(&self.config, &start_entry, RECOVERY_REDIS_ENTRY_MAX_BYTES)?;

        let mut buffer_a_entries = Vec::with_capacity(buffer_a_count);
        for (block_index, update) in transaction.buffer_a.iter().cloned().enumerate() {
            let message_seq = guard
                .next_message_seq
                .checked_add(1)
                .and_then(|value| value.checked_add(u64::try_from(chunk_count).ok()?))
                .and_then(|value| value.checked_add(u64::try_from(block_index).ok()?))
                .ok_or_else(|| anyhow!("Redis broadcaster Buffer A sequence overflow"))?;
            let catch_up = BroadcasterRecoveryCatchUp::new(
                recovery_id.clone(),
                target_state_version,
                u32::try_from(block_index)?,
                update,
            )?;
            let entry = recovery_entry(
                self.config.chain_id,
                &guard.stream_id,
                message_seq,
                BroadcasterPayload::RecoveryCatchUp(catch_up),
                target_state_version,
            )?;
            ensure_redis_entry_size(&self.config, &entry, RECOVERY_REDIS_ENTRY_MAX_BYTES)?;
            buffer_a_entries.push(entry);
        }

        let commit = BroadcasterRecoveryCommit::new(
            recovery_id.clone(),
            target_state_version,
            transaction.backends.clone(),
            replacement_digest,
            u32::try_from(buffer_a_count)?,
            commit_watermark,
        )?;
        let commit_entry = recovery_entry(
            self.config.chain_id,
            &guard.stream_id,
            commit_watermark,
            BroadcasterPayload::RecoveryCommit(commit),
            target_state_version,
        )?;
        ensure_redis_entry_size(&self.config, &commit_entry, RECOVERY_REDIS_ENTRY_MAX_BYTES)?;

        let mut entries = Vec::with_capacity(chunk_count + buffer_a_count + 2);
        entries.push(start_entry);
        entries.extend(chunk_entries);
        entries.extend(buffer_a_entries);
        entries.push(commit_entry);

        let encoded_bytes = entries.iter().try_fold(0usize, |total, entry| {
            redis_entry_encoded_size(&self.config, entry).and_then(|size| {
                total
                    .checked_add(size)
                    .context("recovery byte count overflow")
            })
        })?;
        let append_failures_before = guard.append_failure_count;
        let mut commit_entry_id = None;
        for entry in entries {
            let is_commit =
                entry.kind == simulator_core::broadcaster::BroadcasterMessageKind::RecoveryCommit;
            // Cancellation is cooperative between Redis commands. Once XADD starts, its
            // lost-reply reconciliation must finish before a superseding transaction begins.
            ensure_recovery_not_cancelled(cancelled)?;
            match self.append_entry_locked(&mut guard, entry).await {
                Ok(entry_id) => commit_entry_id = Some(entry_id),
                Err(error) => {
                    record_publisher_error(&mut guard, append_failures_before, &error);
                    if guard.mode == BroadcasterRedisPublisherMode::Retired {
                        self.set_deployment_phase(
                            BroadcasterDeploymentPhase::Retired,
                            Some(format!("{error:#}")),
                        );
                    }
                    if is_commit && !is_stale_writer_error(&error) {
                        return Err(RecoveryCommitAdoptionUncertain {
                            message: format!("{error:#}"),
                        }
                        .into());
                    }
                    return Err(error);
                }
            }
        }
        let commit_entry_id =
            commit_entry_id.ok_or_else(|| anyhow!("Redis recovery committed no entries"))?;
        guard.committed_state_version = target_state_version;
        self.drain_pending_payloads_locked(&mut guard).await?;
        if let Some(state_history) = &self.state_history {
            state_history.reset_rfq_high_water();
        }
        let replay_boundary = self.replay_boundary_locked(&guard)?;

        Ok(BroadcasterRecoveryPublication {
            recovery_id,
            target_state_version,
            commit_entry_id,
            commit_position: StreamPosition {
                generation: guard.generation,
                message_seq: commit_watermark,
            },
            replay_boundary,
            encoded_bytes,
            chunk_count,
            buffer_a_count,
        })
    }

    pub(crate) async fn self_fence(&self, reason: impl Into<String>) {
        let reason = reason.into();
        let mut guard = self.inner.lock().await;
        guard.record_retired(reason.clone());
        self.set_deployment_phase(BroadcasterDeploymentPhase::Fatal, Some(reason));
    }

    pub async fn promote(
        &self,
        base_heads: Vec<BroadcasterBackendHead>,
        reason: impl Into<String>,
    ) -> Result<BroadcasterRedisReplayBoundary> {
        let backends = backends_from_base_heads(&base_heads);
        self.promote_locked(backends, reason.into(), false).await
    }

    pub(crate) async fn recover_writer(
        &self,
        base_heads: Vec<BroadcasterBackendHead>,
        reason: impl Into<String>,
    ) -> Result<BroadcasterRedisReplayBoundary> {
        let backends = backends_from_base_heads(&base_heads);
        let boundary = self.promote_locked(backends, reason.into(), true).await?;
        // Everything queued behind the lost generation is superseded by the
        // fresh upstream replacements built after feeds resume.
        *self.recovery_gate.lock().await = PublisherRecoveryGate::default();
        Ok(boundary)
    }

    pub async fn renew_lease(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        match guard.mode {
            BroadcasterRedisPublisherMode::Passive => return Ok(()),
            BroadcasterRedisPublisherMode::Retired => {
                return Err(anyhow!(
                    "Redis broadcaster publisher is retired; writer lease cannot be renewed"
                ))
            }
            BroadcasterRedisPublisherMode::Active => {
                return self
                    .verify_writer_fence_locked(&mut guard, "active writer lease renewal", true)
                    .await;
            }
            BroadcasterRedisPublisherMode::Unhealthy => {}
        }
        let result = self
            .writer
            .renew_writer(RedisRenewCommand {
                writer_key: &self.writer_key,
                writer_generation_key: &self.writer_generation_key,
                writer_token: &self.writer_token,
                generation: guard.generation,
                lease_ttl: self.config.writer_lease_ttl,
            })
            .await;
        match result {
            Ok(()) => {
                self.resync_stream_tail_locked(&mut guard).await?;
                guard.mode = BroadcasterRedisPublisherMode::Active;
                guard.last_error = None;
                Ok(())
            }
            Err(error) => {
                if is_stale_writer_error(&error) {
                    let message = format!("{error:#}");
                    guard.record_retired(message.clone());
                    self.set_deployment_phase(BroadcasterDeploymentPhase::Retired, Some(message));
                }
                Err(error)
            }
        }
    }

    async fn resync_stream_tail_locked(
        &self,
        guard: &mut BroadcasterRedisPublisherState,
    ) -> Result<()> {
        let inspection = timeout(
            WRITER_PROBE_TIMEOUT,
            self.writer.inspect_writer(RedisInspectCommand {
                stream_key: &self.config.stream_key,
                writer_key: &self.writer_key,
                writer_generation_key: &self.writer_generation_key,
                writer_token: &self.writer_token,
                generation: guard.generation,
            }),
        )
        .await
        .map_err(|_| anyhow!("Redis stream tail resync timed out"))
        .and_then(|inspection| inspection)
        .context("Redis stream tail resync failed before reactivating the publisher")?;
        if inspection.generation != guard.generation {
            return Ok(());
        }
        let Some(last_entry_id) = inspection.last_entry_id else {
            return Ok(());
        };
        let (generation, sequence) = parse_redis_entry_id(&last_entry_id)?;
        if generation != guard.generation || sequence < guard.next_message_seq {
            return Ok(());
        }
        let old_next_message_seq = guard.next_message_seq;
        let next_message_seq = sequence
            .checked_add(1)
            .ok_or_else(|| anyhow!("Redis broadcaster message_seq overflow during tail resync"))?;
        warn!(
            event = "redis_writer_tail_resynced",
            stream_key = self.config.stream_key.as_str(),
            generation = guard.generation,
            old_next_message_seq,
            adopted_entry_id = last_entry_id.as_str(),
            "Redis broadcaster writer adopted the current stream tail"
        );
        guard.next_message_seq = next_message_seq;
        guard.latest_entry_id = Some(last_entry_id);
        // State versions are allocated before append and never reused. Consumers tolerate gaps.
        Ok(())
    }

    pub(crate) async fn recovery_gate_is_open(&self) -> bool {
        self.recovery_gate.lock().await.open_recovery_id.is_some()
    }

    pub(crate) async fn writer_fence_is_absent(&self) -> bool {
        let generation = {
            let guard = self.inner.lock().await;
            if guard.mode != BroadcasterRedisPublisherMode::Unhealthy {
                return false;
            }
            guard.generation
        };
        // A false positive is harmless. Promotion rechecks token and generation atomically and
        // never displaces a newer live writer.
        match timeout(
            WRITER_PROBE_TIMEOUT,
            self.writer.verify_writer(RedisVerifyCommand {
                writer_key: &self.writer_key,
                writer_generation_key: &self.writer_generation_key,
                writer_token: &self.writer_token,
                generation,
                lease_ttl: self.config.writer_lease_ttl,
            }),
        )
        .await
        {
            Ok(Ok(())) | Err(_) => false,
            Ok(Err(error)) => format!("{error:#}").contains(MISSING_FENCE_MESSAGE),
        }
    }

    pub async fn verify_active_writer(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        self.verify_writer_fence_locked(&mut guard, "active writer verification", false)
            .await
    }

    pub async fn replay_boundary(&self) -> Result<BroadcasterRedisReplayBoundary> {
        let mut guard = self.inner.lock().await;
        self.verify_writer_fence_locked(&mut guard, "replay boundary", false)
            .await?;
        self.replay_boundary_locked(&guard)
    }

    pub async fn replay_boundary_is_retained(
        &self,
        boundary: &BroadcasterRedisReplayBoundary,
    ) -> Result<bool> {
        let status = self.status_snapshot().await;
        let Some(current_boundary) = status.replay_boundary.as_ref() else {
            return Ok(false);
        };
        if status.mode != BroadcasterRedisPublisherMode::Active.as_str()
            || boundary.generation != current_boundary.generation
            || boundary.stream_id != status.stream_id
            || boundary.snapshot_id != status.snapshot_id
        {
            return Ok(false);
        }
        let inspection = timeout(
            STATUS_VERIFY_TIMEOUT,
            self.writer.inspect_writer(RedisInspectCommand {
                stream_key: &self.config.stream_key,
                writer_key: &self.writer_key,
                writer_generation_key: &self.writer_generation_key,
                writer_token: &self.writer_token,
                generation: boundary.generation,
            }),
        )
        .await
        .map_err(|_| anyhow!("Redis replay-boundary inspection timed out"))??;
        if inspection.generation != boundary.generation {
            return Ok(false);
        }
        let Some(first_entry_id) = inspection.first_entry_id else {
            return Ok(boundary.exclusive_message_seq == 0);
        };
        let first = parse_redis_entry_id(&first_entry_id)?;
        let next = (
            boundary.generation,
            boundary.exclusive_message_seq.saturating_add(1),
        );
        Ok(first <= next)
    }

    pub async fn mode(&self) -> BroadcasterRedisPublisherMode {
        self.inner.lock().await.mode
    }

    pub(crate) fn feeds_are_paused(&self) -> bool {
        self.feed_gate.borrow().paused
    }

    pub(crate) fn feed_pause_epoch(&self) -> u64 {
        self.feed_gate.borrow().pause_epoch
    }

    pub(crate) async fn wait_for_feed_pause_after(&self, pause_epoch: u64) {
        let mut feed_gate = self.feed_gate.subscribe();
        loop {
            if feed_gate.borrow_and_update().pause_epoch > pause_epoch {
                return;
            }
            if feed_gate.changed().await.is_err() {
                return;
            }
        }
    }

    pub(crate) async fn wait_for_feeds_resumed(&self) {
        let mut feed_gate = self.feed_gate.subscribe();
        while feed_gate.borrow_and_update().paused {
            if feed_gate.changed().await.is_err() {
                return;
            }
        }
    }

    pub(crate) async fn pause_feeds_until_writer_verified(&self) -> Result<()> {
        let _pause_guard = self.feed_pause_guard.lock().await;
        self.feed_gate.send_modify(|state| {
            state.pause_epoch = state.pause_epoch.saturating_add(1);
            state.paused = true;
        });
        loop {
            if self.mode().await == BroadcasterRedisPublisherMode::Retired {
                return Err(anyhow!(
                    "Redis broadcaster writer was fenced while feeds were paused"
                ));
            }
            if self.renew_lease().await.is_ok() {
                self.feed_gate.send_modify(|state| state.paused = false);
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    pub(crate) async fn pending_recovery_payload_stats(&self) -> (usize, usize, Option<u64>) {
        let gate = self.recovery_gate.lock().await;
        (
            gate.pending_payloads.len(),
            gate.max_pending_payloads,
            gate.pending_payloads
                .front()
                .map(|queued| queued.accepted_at.elapsed().as_millis() as u64),
        )
    }

    #[expect(
        clippy::unused_async,
        reason = "status snapshots keep the same future-based API as verified snapshots"
    )]
    pub async fn status_snapshot(&self) -> BroadcasterRedisPublisherStatus {
        let Ok(guard) = self.inner.try_lock() else {
            // Long appends and recoveries verify their own writer fence while holding state.
            return self.cached_status();
        };
        let status = self.status_snapshot_locked(&guard);
        self.cache_status(status.clone());
        status
    }

    pub async fn verified_status_snapshot(&self) -> BroadcasterRedisPublisherStatus {
        let Ok(mut guard) = self.inner.try_lock() else {
            // Long appends and recoveries verify their own writer fence while holding state.
            return self.cached_status();
        };
        if guard.mode == BroadcasterRedisPublisherMode::Active && guard.last_error.is_none() {
            let _ = timeout(
                STATUS_VERIFY_TIMEOUT,
                self.verify_writer_fence_locked(&mut guard, "status snapshot", false),
            )
            .await;
        }
        let status = self.status_snapshot_locked(&guard);
        self.cache_status(status.clone());
        status
    }

    fn cached_status(&self) -> BroadcasterRedisPublisherStatus {
        self.last_status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn cache_status(&self, status: BroadcasterRedisPublisherStatus) {
        *self
            .last_status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = status;
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
            retry_exhaustion_count: guard.retry_exhaustion_count,
            last_error: guard.last_error.clone(),
        }
    }

    async fn verify_writer_fence_locked(
        &self,
        guard: &mut BroadcasterRedisPublisherState,
        context: &str,
        renew_lease: bool,
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

        let verification = if renew_lease {
            self.writer.renew_writer(RedisRenewCommand {
                writer_key: &self.writer_key,
                writer_generation_key: &self.writer_generation_key,
                writer_token: &self.writer_token,
                generation: guard.generation,
                lease_ttl: self.config.writer_lease_ttl,
            })
        } else {
            self.writer.verify_writer(RedisVerifyCommand {
                writer_key: &self.writer_key,
                writer_generation_key: &self.writer_generation_key,
                writer_token: &self.writer_token,
                generation: guard.generation,
                lease_ttl: self.config.writer_lease_ttl,
            })
        };
        if let Err(error) = verification.await {
            let message = format!("{error:#}");
            if is_stale_writer_error(&error) {
                emit_broadcaster_writer_fence_failure();
                guard.record_retired(message.clone());
                self.set_deployment_phase(BroadcasterDeploymentPhase::Retired, Some(message));
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
        reason: String,
        recovering: bool,
    ) -> Result<BroadcasterRedisReplayBoundary> {
        let mut guard = self.inner.lock().await;
        if guard.mode == BroadcasterRedisPublisherMode::Retired
            || (recovering && guard.mode != BroadcasterRedisPublisherMode::Unhealthy)
        {
            return Err(anyhow!(
                "Redis broadcaster publisher cannot promote a writer generation from mode {}",
                guard.mode.as_str()
            ));
        }
        let normal_marker_fields =
            generation_marker_template_fields(self.config.chain_id, backends, reason.clone())?;
        let expected_writer_token = recovering.then_some(self.writer_token.as_str());
        let expected_generation = recovering.then_some(guard.generation);
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
            })
            .await;

        let promotion = match promotion {
            Ok(promotion) => promotion,
            Err(error) => {
                let message = format!("{error:#}");
                if is_stale_writer_error(&error) {
                    emit_broadcaster_writer_fence_failure();
                    guard.record_retired(message.clone());
                    self.set_deployment_phase(BroadcasterDeploymentPhase::Retired, Some(message));
                } else if recovering {
                    guard.record_unhealthy(message, false);
                } else {
                    guard.mode = BroadcasterRedisPublisherMode::Passive;
                    guard.last_error = Some(message);
                }
                return Err(error);
            }
        };

        guard.activate_generation(self.config.chain_id, promotion.generation);
        if let Some(state_history) = &self.state_history {
            state_history.reset_rfq_high_water();
        }
        warn!(
            event = "redis_writer_promoted",
            stream_id = guard.stream_id.as_str(),
            snapshot_id = guard.snapshot_id.as_str(),
            generation = guard.generation,
            reason,
            "Redis broadcaster generation promoted"
        );
        self.replay_boundary_locked(&guard)
    }

    async fn append_live_payload_locked(
        &self,
        guard: &mut BroadcasterRedisPublisherState,
        payload: BroadcasterPayload,
    ) -> Result<(BroadcasterRedisStreamEntry, String)> {
        let advances_state = matches!(payload, BroadcasterPayload::Update(_));
        let state_version = if advances_state {
            guard.allocate_state_version()?
        } else {
            guard.committed_state_version
        };
        let message_seq = guard.next_message_seq;
        let envelope = BroadcasterEnvelope::new(guard.stream_id.clone(), message_seq, payload);
        let entry = BroadcasterRedisStreamEntry::from_envelope_at(
            self.config.chain_id,
            &envelope,
            current_time_ms(),
            state_version,
        )?;
        let limit = if matches!(
            entry.kind,
            simulator_core::broadcaster::BroadcasterMessageKind::RecoveryStart
                | simulator_core::broadcaster::BroadcasterMessageKind::RecoveryChunk
                | simulator_core::broadcaster::BroadcasterMessageKind::RecoveryCatchUp
                | simulator_core::broadcaster::BroadcasterMessageKind::RecoveryCommit
        ) {
            RECOVERY_REDIS_ENTRY_MAX_BYTES
        } else {
            LIVE_REDIS_ENTRY_MAX_BYTES
        };
        ensure_redis_entry_size(&self.config, &entry, limit)?;
        let entry_id = self.append_entry_locked(guard, entry.clone()).await?;
        if advances_state {
            guard.committed_state_version = state_version;
        }
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

    async fn drain_pending_payloads_locked(
        &self,
        guard: &mut BroadcasterRedisPublisherState,
    ) -> Result<()> {
        loop {
            let queued = {
                let mut recovery_gate = self.recovery_gate.lock().await;
                match recovery_gate.pending_payloads.pop_front() {
                    Some(queued) => queued,
                    None => {
                        recovery_gate.open_recovery_id = None;
                        recovery_gate.cancelled = None;
                        recovery_gate.complete_native_blocks.clear();
                        return Ok(());
                    }
                }
            };
            let payload = normalize_live_payload(queued.payload, &guard.snapshot_id)?;
            if let Err(error) = self
                .append_live_payload_locked(guard, payload.clone())
                .await
            {
                self.recovery_gate.lock().await.pending_payloads.push_front(
                    QueuedPublisherPayload {
                        payload,
                        accepted_at: queued.accepted_at,
                    },
                );
                return Err(error);
            }
        }
    }

    async fn append_entry_locked(
        &self,
        guard: &mut BroadcasterRedisPublisherState,
        entry: BroadcasterRedisStreamEntry,
    ) -> Result<String> {
        if entry.message_seq != guard.next_message_seq {
            return Err(anyhow!(
                "Redis broadcaster prepared entry sequence mismatch: expected {}, found {}",
                guard.next_message_seq,
                entry.message_seq
            ));
        }
        let next_message_seq = entry
            .message_seq
            .checked_add(1)
            .ok_or_else(|| anyhow!("Redis broadcaster message_seq overflow"))?;
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
        Ok(entry_id)
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
            guard.committed_state_version,
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
        if is_stale_writer_error(error) {
            emit_broadcaster_writer_fence_failure();
        }
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

fn trim_queued_target_backends(
    pending_payloads: &mut VecDeque<QueuedPublisherPayload>,
    backends: &[BroadcasterBackend],
) {
    pending_payloads.retain_mut(|queued| {
        let BroadcasterPayload::Update(update) = &mut queued.payload else {
            return true;
        };
        update
            .partitions
            .retain(|partition| !backends.contains(&partition.backend));
        !update.partitions.is_empty()
    });
}

fn queued_complete_native_blocks(
    pending_payloads: &VecDeque<QueuedPublisherPayload>,
) -> HashSet<u64> {
    pending_payloads
        .iter()
        .filter_map(|queued| complete_native_block(&queued.payload))
        .collect()
}

fn complete_native_block(payload: &BroadcasterPayload) -> Option<u64> {
    let BroadcasterPayload::Update(update) = payload else {
        return None;
    };
    update.complete_native_block()
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
        | BroadcasterPayload::SnapshotEnd(_)
        | BroadcasterPayload::RecoveryStart(_)
        | BroadcasterPayload::RecoveryChunk(_)
        | BroadcasterPayload::RecoveryCatchUp(_)
        | BroadcasterPayload::RecoveryCommit(_) => Err(anyhow!(
            "Redis broadcaster live payload cannot be a snapshot or recovery message"
        )),
    }
}

fn enqueue_state_history_update(
    state_history: &StateHistoryRuntime,
    publisher: &BroadcasterRedisPublisherState,
    entry: BroadcasterRedisStreamEntry,
    update: Result<StateHistoryUpdateProjection>,
) {
    let result = update.and_then(|update| {
        state_history.observe_rfq_update(&update);
        delta_entry_from_update(
            entry.chain_id,
            publisher.generation,
            entry.message_seq,
            entry.published_at_ms,
            update,
            entry.payload_json,
        )
    });
    match result {
        Ok(Some(delta)) => {
            state_history.handle.enqueue_delta(delta);
        }
        Err(error) => {
            state_history.handle.record_delta_preparation_failure_gap(
                entry.chain_id,
                StreamPosition {
                    generation: publisher.generation,
                    message_seq: entry.message_seq,
                },
            );
            warn!(
                event = "state_history_delta_prepare_failed",
                chain_id = entry.chain_id,
                generation = publisher.generation,
                message_seq = entry.message_seq,
                %error,
                "State history delta could not be prepared"
            );
        }
        Ok(None) => {}
    }
}

#[derive(Debug)]
struct PreparedRecoveryFragment {
    payload: String,
    digest: String,
}

#[derive(Clone, Copy)]
struct RecoveryEntryScope<'a> {
    recovery_id: &'a str,
    target_state_version: u64,
    backends: &'a [BroadcasterBackend],
}

fn prepare_recovery_fragments(replacement_json: &str) -> Result<Vec<PreparedRecoveryFragment>> {
    if replacement_json.is_empty() {
        return Err(anyhow!(
            "broadcaster recovery replacement must not be empty"
        ));
    }

    let mut fragments = Vec::new();
    let mut offset = 0usize;
    while offset < replacement_json.len() {
        let mut end = offset
            .saturating_add(RECOVERY_FRAGMENT_TARGET_BYTES)
            .min(replacement_json.len());
        while end > offset && !replacement_json.is_char_boundary(end) {
            end -= 1;
        }
        if end == offset {
            return Err(anyhow!(
                "broadcaster recovery fragment boundary is not valid UTF-8"
            ));
        }
        let payload = replacement_json[offset..end].to_string();
        fragments.push(PreparedRecoveryFragment {
            digest: digest_bytes(payload.as_bytes()),
            payload,
        });
        offset = end;
    }
    Ok(fragments)
}

fn build_recovery_chunk_entry(
    config: &BroadcasterRedisPublisherConfig,
    stream_id: &str,
    message_seq: u64,
    recovery_scope: RecoveryEntryScope<'_>,
    chunk_index: usize,
    fragment: &PreparedRecoveryFragment,
) -> Result<(BroadcasterRedisStreamEntry, usize)> {
    let mut encoded_bytes = 1usize;
    for _ in 0..4 {
        let chunk = BroadcasterRecoveryChunk::new(
            recovery_scope.recovery_id.to_string(),
            recovery_scope.target_state_version,
            recovery_scope.backends.to_vec(),
            u32::try_from(chunk_index)?,
            u64::try_from(encoded_bytes)?,
            fragment.digest.clone(),
            fragment.payload.clone(),
        )?;
        let entry = recovery_entry(
            config.chain_id,
            stream_id,
            message_seq,
            BroadcasterPayload::RecoveryChunk(chunk),
            recovery_scope.target_state_version,
        )?;
        let measured = redis_entry_encoded_size(config, &entry)?;
        if measured == encoded_bytes {
            ensure_redis_entry_size(config, &entry, RECOVERY_REDIS_ENTRY_MAX_BYTES)?;
            return Ok((entry, measured));
        }
        encoded_bytes = measured;
    }
    Err(anyhow!(
        "broadcaster recovery chunk encoded size did not converge"
    ))
}

fn recovery_entry(
    chain_id: u64,
    stream_id: &str,
    message_seq: u64,
    payload: BroadcasterPayload,
    state_version: u64,
) -> Result<BroadcasterRedisStreamEntry> {
    BroadcasterRedisStreamEntry::from_envelope_at(
        chain_id,
        &BroadcasterEnvelope::new(stream_id.to_string(), message_seq, payload),
        current_time_ms(),
        state_version,
    )
    .map_err(Into::into)
}

fn digest_bytes(bytes: &[u8]) -> String {
    format!("{:x}", keccak256(bytes))
}

fn ensure_active_publisher(state: &BroadcasterRedisPublisherState) -> Result<()> {
    match state.mode {
        BroadcasterRedisPublisherMode::Active if state.last_error.is_none() => Ok(()),
        BroadcasterRedisPublisherMode::Passive => {
            Err(anyhow!("Redis broadcaster publisher is passive"))
        }
        BroadcasterRedisPublisherMode::Retired => Err(anyhow!(
            "Redis broadcaster publisher is retired; this process is no longer the active writer"
        )),
        BroadcasterRedisPublisherMode::Unhealthy | BroadcasterRedisPublisherMode::Active => {
            Err(anyhow!(
                "Redis broadcaster publisher is unhealthy: {}",
                state
                    .last_error
                    .as_deref()
                    .unwrap_or("publisher is unhealthy")
            ))
        }
    }
}

fn record_publisher_error(
    state: &mut BroadcasterRedisPublisherState,
    append_failures_before: u64,
    error: &anyhow::Error,
) {
    let message = format!("{error:#}");
    if is_stale_writer_error(error) {
        state.record_retired(message);
    } else {
        state.record_unhealthy(message, state.append_failure_count > append_failures_before);
    }
}

fn ensure_redis_entry_size(
    config: &BroadcasterRedisPublisherConfig,
    entry: &BroadcasterRedisStreamEntry,
    limit_bytes: usize,
) -> Result<usize> {
    let encoded_bytes = redis_entry_encoded_size(config, entry)?;
    if encoded_bytes > limit_bytes {
        return Err(OversizedRedisEntry {
            kind: entry.kind,
            encoded_bytes,
            limit_bytes,
        }
        .into());
    }
    Ok(encoded_bytes)
}

pub(super) fn redis_entry_encoded_size(
    config: &BroadcasterRedisPublisherConfig,
    entry: &BroadcasterRedisStreamEntry,
) -> Result<usize> {
    replay_entry_encoded_size(&config.stream_key, config.maxlen, entry)
}

pub(crate) fn replay_entry_encoded_size(
    stream_key: &str,
    maxlen: Option<u64>,
    entry: &BroadcasterRedisStreamEntry,
) -> Result<usize> {
    let entry_id = redis_entry_id(entry)?;
    let fields = redis_entry_fields(entry)?;
    let mut arguments = vec!["XADD".to_string(), stream_key.to_string()];
    if let Some(maxlen) = maxlen {
        arguments.extend(["MAXLEN".to_string(), "~".to_string(), maxlen.to_string()]);
    }
    arguments.push(entry_id);
    for (field, value) in fields {
        arguments.push(field);
        arguments.push(value);
    }
    Ok(resp_array_size(&arguments))
}

fn resp_array_size(arguments: &[String]) -> usize {
    let prefix = 1 + decimal_digits(arguments.len()) + 2;
    arguments.iter().fold(prefix, |total, argument| {
        total.saturating_add(resp_bulk_string_size(argument.len()))
    })
}

fn resp_bulk_string_size(length: usize) -> usize {
    1 + decimal_digits(length) + 2 + length + 2
}

const fn decimal_digits(mut value: usize) -> usize {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

pub(crate) fn format_redis_stream_id(chain_id: u64, generation: u64) -> String {
    format!("chain-{chain_id}-stream-{generation}")
}

pub(crate) fn format_redis_snapshot_id(chain_id: u64, generation: u64) -> String {
    format!("chain-{chain_id}-snapshot-{generation}")
}

fn state_version_base(generation: u64) -> u64 {
    generation.saturating_mul(1u64 << 48)
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
) -> Result<Vec<(String, String)>> {
    let stream_id = format!("chain-{chain_id}-stream-{GENERATION_PLACEHOLDER}");
    let snapshot_id = format!("chain-{chain_id}-snapshot-{GENERATION_PLACEHOLDER}");
    let marker = BroadcasterProgress::new(chain_id, snapshot_id.clone(), backends, reason)?;
    let envelope = BroadcasterEnvelope::new(stream_id, 1, BroadcasterPayload::Progress(marker));
    let entry =
        BroadcasterRedisStreamEntry::from_envelope_at(chain_id, &envelope, current_time_ms(), 0)?;
    redis_entry_fields(&entry)
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

fn ensure_recovery_not_cancelled(cancelled: &AtomicBool) -> Result<()> {
    if cancelled.load(Ordering::Acquire) {
        return Err(RecoveryPublicationCancelled.into());
    }
    Ok(())
}

fn parse_redis_entry_id(entry_id: &str) -> Result<(u64, u64)> {
    let (generation, sequence) = entry_id
        .split_once('-')
        .ok_or_else(|| anyhow!("invalid Redis stream entry id: {entry_id}"))?;
    Ok((
        generation
            .parse()
            .with_context(|| format!("invalid Redis stream generation in {entry_id}"))?,
        sequence
            .parse()
            .with_context(|| format!("invalid Redis stream sequence in {entry_id}"))?,
    ))
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

pub(crate) fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
