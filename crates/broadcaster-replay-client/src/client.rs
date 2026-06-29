use std::time::Duration;

use futures::Stream;
use reqwest::Client;
use simulator_core::broadcaster::{
    BroadcasterEnvelope, BroadcasterRedisReplayBoundary, BroadcasterRedisStreamEntry,
    BroadcasterSnapshotSessionResponse,
};

use crate::checkpoint::{redis_empty_poll_action, RedisEmptyPollAction, ReplayCheckpoint};
use crate::error::{BroadcasterReplayClientError, Result};
use crate::handoff::{redis_generation_handoff_candidate, validate_handoff_candidate};
use crate::reader::{RedisStreamMessage, TokioRedisStreamReader};
use crate::snapshot::{
    create_broadcaster_snapshot_session, fetch_broadcaster_snapshot_payload,
    fetch_broadcaster_snapshot_payloads,
};

#[derive(Debug, Clone)]
/// Configuration for the broadcaster replay client.
pub struct BroadcasterReplayConfig {
    /// Base URL for the broadcaster HTTP API.
    pub broadcaster_url: String,
    /// Redis connection URL used to read broadcaster stream entries.
    pub redis_url: String,
    /// Blocking XREAD timeout in milliseconds.
    pub block_ms: u64,
    /// Maximum Redis stream entries to read in one poll.
    pub read_count: u64,
    /// Timeout for snapshot-session HTTP requests.
    pub request_timeout: Duration,
}

/// Client that bootstraps from broadcaster snapshots and replays Redis deltas.
pub struct BroadcasterReplayClient {
    http: Client,
    redis: TokioRedisStreamReader,
    config: BroadcasterReplayConfig,
}

impl BroadcasterReplayClient {
    /// Connect to Redis and prepare the HTTP client.
    ///
    /// # Errors
    ///
    /// Returns an error when the Redis URL is invalid or the connection manager
    /// cannot be created.
    pub async fn connect(config: BroadcasterReplayConfig) -> Result<Self> {
        let redis = TokioRedisStreamReader::connect(&config.redis_url).await?;
        Ok(Self {
            http: Client::new(),
            redis,
            config,
        })
    }

    /// Create one broadcaster snapshot session.
    ///
    /// The session response contains the Redis replay boundary that callers use
    /// as the starting checkpoint after applying all snapshot payloads.
    ///
    /// # Errors
    ///
    /// Returns an error when the broadcaster URL is invalid, the request fails,
    /// the broadcaster returns a non-success status, or the response cannot be
    /// decoded.
    pub async fn create_snapshot_session(&self) -> Result<BroadcasterSnapshotSessionResponse> {
        create_broadcaster_snapshot_session(
            &self.http,
            &self.config.broadcaster_url,
            self.config.request_timeout,
        )
        .await
    }

    /// Fetch one snapshot payload from an existing session.
    ///
    /// # Errors
    ///
    /// Returns an error when the broadcaster URL is invalid, the request fails,
    /// the broadcaster returns a non-success status, or the response cannot be
    /// decoded.
    pub async fn fetch_snapshot_payload(
        &self,
        session: &BroadcasterSnapshotSessionResponse,
        index: u32,
    ) -> Result<BroadcasterEnvelope> {
        fetch_broadcaster_snapshot_payload(
            &self.http,
            &self.config.broadcaster_url,
            session,
            index,
            self.config.request_timeout,
        )
        .await
    }

    /// Fetch snapshot payloads from an existing session with bounded concurrency.
    ///
    /// The returned stream yields payloads in index order, so callers can apply
    /// envelopes directly while later payload requests are already in flight.
    pub fn snapshot_payloads<'a>(
        &'a self,
        session: &'a BroadcasterSnapshotSessionResponse,
    ) -> impl Stream<Item = Result<BroadcasterEnvelope>> + 'a {
        fetch_broadcaster_snapshot_payloads(
            &self.http,
            &self.config.broadcaster_url,
            session,
            self.config.request_timeout,
        )
    }

    /// Read and validate the next Redis replay batch after `checkpoint`.
    ///
    /// # Errors
    ///
    /// Returns an error when Redis cannot be read or inspected, a stream entry
    /// cannot be decoded, or replay continuity checks fail.
    pub async fn read_next(&self, checkpoint: &ReplayCheckpoint) -> Result<ReplayPoll> {
        let messages = self
            .redis
            .read_after(
                &checkpoint.boundary().stream_key,
                checkpoint.entry_id(),
                self.config.block_ms,
                self.config.read_count,
            )
            .await?;
        let caught_up_after_batch = messages.len() < self.config.read_count as usize;

        if messages.is_empty() {
            let stream_info = self
                .redis
                .stream_info(&checkpoint.boundary().stream_key)
                .await?;
            return match redis_empty_poll_action(checkpoint, stream_info.as_ref())? {
                RedisEmptyPollAction::CaughtUp => Ok(ReplayPoll::CaughtUp {
                    checkpoint: checkpoint.clone(),
                }),
                RedisEmptyPollAction::Pending => Ok(ReplayPoll::Pending),
            };
        }

        let batch = build_replay_batch(checkpoint, messages, caught_up_after_batch)?;
        Ok(ReplayPoll::Batch(batch))
    }
}

#[derive(Debug, Clone)]
/// Result of a Redis replay poll.
pub enum ReplayPoll {
    /// One or more entries were read and validated.
    Batch(ReplayBatch),
    /// No newer entry exists and the checkpoint has reached the required boundary.
    CaughtUp { checkpoint: ReplayCheckpoint },
    /// No entry was returned yet, but Redis shows the stream may still advance.
    Pending,
}

#[derive(Debug, Clone)]
/// Validated Redis entries from one replay poll.
pub struct ReplayBatch {
    /// Replay items in Redis order.
    pub items: Vec<ReplayBatchItem>,
    /// True when this batch drained the current stream tail.
    pub caught_up_after_batch: bool,
}

impl ReplayBatch {
    #[must_use]
    pub fn checkpoint_after(&self) -> Option<&ReplayCheckpoint> {
        self.items.last().map(ReplayBatchItem::checkpoint_after)
    }
}

#[derive(Debug, Clone)]
/// One validated replay item.
pub enum ReplayBatchItem {
    /// Normal broadcaster delta from the current generation.
    Message(ReplayMessage),
    /// First progress marker in the next Redis generation.
    GenerationHandoff(GenerationHandoffCandidate),
}

impl ReplayBatchItem {
    #[must_use]
    pub fn checkpoint_after(&self) -> &ReplayCheckpoint {
        match self {
            Self::Message(message) => &message.checkpoint_after,
            Self::GenerationHandoff(candidate) => &candidate.checkpoint_after,
        }
    }
}

#[derive(Debug, Clone)]
/// Normal broadcaster Redis delta with the checkpoint after applying it.
pub struct ReplayMessage {
    /// Redis stream entry ID.
    pub entry_id: String,
    /// Decoded Redis stream metadata.
    pub entry: BroadcasterRedisStreamEntry,
    /// Decoded broadcaster payload.
    pub envelope: BroadcasterEnvelope,
    /// Checkpoint to use after this message is applied.
    pub checkpoint_after: ReplayCheckpoint,
}

#[derive(Debug, Clone)]
/// Validated handoff marker between Redis stream generations.
pub struct GenerationHandoffCandidate {
    /// Decoded Redis stream metadata.
    pub entry: BroadcasterRedisStreamEntry,
    /// Progress envelope carrying the handoff proof.
    pub envelope: BroadcasterEnvelope,
    /// New replay boundary for the next generation.
    pub boundary: BroadcasterRedisReplayBoundary,
    /// Checkpoint to use after the handoff is accepted.
    pub checkpoint_after: ReplayCheckpoint,
}

pub(crate) fn build_replay_batch(
    checkpoint: &ReplayCheckpoint,
    messages: Vec<RedisStreamMessage>,
    caught_up_after_batch: bool,
) -> Result<ReplayBatch> {
    let mut items = Vec::with_capacity(messages.len());
    let mut next_checkpoint = checkpoint.clone();

    for message in messages {
        let envelope: BroadcasterEnvelope = serde_json::from_str(&message.entry.payload_json)
            .map_err(|error| {
                BroadcasterReplayClientError::redis_decode(format!(
                    "failed to decode broadcaster Redis payload at {}: {error}",
                    message.entry_id
                ))
            })?;

        match next_checkpoint.ensure_next_message(&message) {
            Ok(()) => {
                let checkpoint_after = next_checkpoint.after_applied(&message);
                next_checkpoint = checkpoint_after.clone();
                items.push(ReplayBatchItem::Message(ReplayMessage {
                    entry_id: message.entry_id,
                    entry: message.entry,
                    envelope,
                    checkpoint_after,
                }));
            }
            Err(_) if redis_generation_handoff_candidate(&next_checkpoint, &message, &envelope) => {
                let candidate = validate_handoff_candidate(&next_checkpoint, message, envelope)?;
                next_checkpoint = candidate.checkpoint_after.clone();
                items.push(ReplayBatchItem::GenerationHandoff(candidate));
            }
            Err(error) => return Err(error),
        }
    }

    Ok(ReplayBatch {
        items,
        caught_up_after_batch,
    })
}
