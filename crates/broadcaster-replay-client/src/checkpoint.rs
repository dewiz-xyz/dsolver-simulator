use simulator_core::broadcaster::BroadcasterRedisReplayBoundary;

use crate::error::{BroadcasterReplayClientError, Result};
use crate::reader::{RedisStreamInfo, RedisStreamMessage};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RedisEmptyPollAction {
    CaughtUp,
    Pending,
}

pub(crate) fn redis_empty_poll_action(
    checkpoint: &ReplayCheckpoint,
    stream_info: Option<&RedisStreamInfo>,
) -> Result<RedisEmptyPollAction> {
    let Some(stream_info) = stream_info else {
        if checkpoint.last_message_seq == 0 {
            return Ok(RedisEmptyPollAction::CaughtUp);
        }
        return Err(BroadcasterReplayClientError::redis_gap(format!(
            "Redis replay gap: stream disappeared after checkpoint {}",
            checkpoint.entry_id
        )));
    };

    let checkpoint_entry_id = parse_redis_entry_id(&checkpoint.entry_id)?;
    let last_generated_entry_id = parse_redis_entry_id(&stream_info.last_generated_entry_id)?;
    if last_generated_entry_id < checkpoint_entry_id {
        return Err(BroadcasterReplayClientError::redis_gap(format!(
            "Redis replay gap: stream moved backwards from checkpoint {} to last generated {}",
            checkpoint.entry_id, stream_info.last_generated_entry_id
        )));
    }
    if last_generated_entry_id == checkpoint_entry_id {
        return Ok(RedisEmptyPollAction::CaughtUp);
    }

    let expected_seq = checkpoint.last_message_seq.checked_add(1).ok_or_else(|| {
        BroadcasterReplayClientError::redis_gap("Redis replay message_seq overflow")
    })?;
    let expected_entry_id = redis_entry_id(checkpoint.boundary.generation, expected_seq);
    let expected_entry_id_parts = parse_redis_entry_id(&expected_entry_id)?;
    let Some(first_entry_id) = &stream_info.first_entry_id else {
        return Err(BroadcasterReplayClientError::redis_gap(format!(
            "Redis replay gap: stream generated {} after checkpoint {} but retained no entries",
            stream_info.last_generated_entry_id, checkpoint.entry_id
        )));
    };
    let Some(last_entry_id) = &stream_info.last_entry_id else {
        return Err(BroadcasterReplayClientError::redis_gap(format!(
            "Redis replay gap: stream generated {} after checkpoint {} but retained no last entry",
            stream_info.last_generated_entry_id, checkpoint.entry_id
        )));
    };

    let first_entry_id_parts = parse_redis_entry_id(first_entry_id)?;
    if first_entry_id_parts > expected_entry_id_parts {
        return Err(BroadcasterReplayClientError::redis_gap(format!(
            "Redis replay gap: first retained entry {first_entry_id} is after expected entry {expected_entry_id}"
        )));
    }

    let last_entry_id_parts = parse_redis_entry_id(last_entry_id)?;
    if last_entry_id_parts <= checkpoint_entry_id {
        return Err(BroadcasterReplayClientError::redis_gap(format!(
            "Redis replay gap: stream generated {} after checkpoint {} but last retained entry is {}",
            stream_info.last_generated_entry_id, checkpoint.entry_id, last_entry_id
        )));
    }

    Ok(RedisEmptyPollAction::Pending)
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Replay position inside one broadcaster Redis stream generation.
pub struct ReplayCheckpoint {
    boundary: BroadcasterRedisReplayBoundary,
    entry_id: String,
    last_message_seq: u64,
    expected_chain_id: u64,
}

impl ReplayCheckpoint {
    /// Build a checkpoint from the snapshot session's exclusive replay boundary.
    #[must_use]
    pub fn new(boundary: BroadcasterRedisReplayBoundary, expected_chain_id: u64) -> Self {
        Self {
            entry_id: boundary.exclusive_entry_id(),
            last_message_seq: boundary.exclusive_message_seq,
            boundary,
            expected_chain_id,
        }
    }

    /// Redis replay boundary for the current generation.
    #[must_use]
    pub fn boundary(&self) -> &BroadcasterRedisReplayBoundary {
        &self.boundary
    }

    /// Last Redis entry accepted by this checkpoint.
    #[must_use]
    pub fn entry_id(&self) -> &str {
        &self.entry_id
    }

    /// Last broadcaster message sequence accepted by this checkpoint.
    #[must_use]
    pub const fn last_message_seq(&self) -> u64 {
        self.last_message_seq
    }

    /// Chain ID every replay entry must match.
    #[must_use]
    pub const fn expected_chain_id(&self) -> u64 {
        self.expected_chain_id
    }

    pub(crate) fn ensure_next_message(&self, message: &RedisStreamMessage) -> Result<()> {
        if message.entry.stream_id != self.boundary.stream_id {
            return Err(BroadcasterReplayClientError::redis_gap(format!(
                "Redis replay gap: expected stream_id {}, got {}",
                self.boundary.stream_id, message.entry.stream_id
            )));
        }
        if message.entry.chain_id != self.expected_chain_id {
            return Err(BroadcasterReplayClientError::redis_gap(format!(
                "Redis replay gap: expected chain_id {}, got {}",
                self.expected_chain_id, message.entry.chain_id
            )));
        }

        if message.entry.message_seq <= self.last_message_seq {
            return Err(BroadcasterReplayClientError::redis_gap(format!(
                "Redis replay gap: got stale message_seq {} after {} at {}",
                message.entry.message_seq, self.entry_id, message.entry_id
            )));
        }

        let expected_seq = self.last_message_seq.checked_add(1).ok_or_else(|| {
            BroadcasterReplayClientError::redis_gap("Redis replay message_seq overflow")
        })?;
        if message.entry.message_seq != expected_seq {
            return Err(BroadcasterReplayClientError::redis_gap(format!(
                "Redis replay gap: expected message_seq {} after {}, got {} at {}",
                expected_seq, self.entry_id, message.entry.message_seq, message.entry_id
            )));
        }

        let expected_entry_id = redis_entry_id(self.boundary.generation, expected_seq);
        if message.entry_id != expected_entry_id {
            return Err(BroadcasterReplayClientError::redis_gap(format!(
                "Redis replay gap: expected entry id {expected_entry_id}, got {}",
                message.entry_id
            )));
        }

        if let Some(snapshot_id) = &message.entry.snapshot_id {
            if snapshot_id != &self.boundary.snapshot_id {
                return Err(BroadcasterReplayClientError::redis_gap(format!(
                    "Redis replay gap: expected snapshot_id {}, got {}",
                    self.boundary.snapshot_id, snapshot_id
                )));
            }
        }

        Ok(())
    }

    pub(crate) fn after_applied(&self, message: &RedisStreamMessage) -> Self {
        Self {
            entry_id: message.entry_id.clone(),
            last_message_seq: message.entry.message_seq,
            boundary: self.boundary.clone(),
            expected_chain_id: self.expected_chain_id,
        }
    }

    pub(crate) fn after_generation_handoff(
        &self,
        boundary: BroadcasterRedisReplayBoundary,
        entry_id: String,
    ) -> Self {
        Self {
            entry_id,
            last_message_seq: boundary.exclusive_message_seq,
            boundary,
            expected_chain_id: self.expected_chain_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RedisEntryIdParts {
    pub(crate) millis: u64,
    pub(crate) sequence: u64,
}

pub(crate) fn parse_redis_entry_id(entry_id: &str) -> Result<RedisEntryIdParts> {
    let Some((millis, sequence)) = entry_id.split_once('-') else {
        return Err(BroadcasterReplayClientError::redis_gap(format!(
            "invalid Redis stream entry id: {entry_id}"
        )));
    };
    Ok(RedisEntryIdParts {
        millis: millis.parse().map_err(|error| {
            BroadcasterReplayClientError::redis_gap(format!(
                "invalid Redis stream entry id {entry_id}: {error}"
            ))
        })?,
        sequence: sequence.parse().map_err(|error| {
            BroadcasterReplayClientError::redis_gap(format!(
                "invalid Redis stream entry id {entry_id}: {error}"
            ))
        })?,
    })
}

pub(crate) fn redis_entry_id(generation: u64, message_seq: u64) -> String {
    format!("{generation}-{message_seq}")
}
