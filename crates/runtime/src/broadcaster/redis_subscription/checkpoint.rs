use anyhow::{anyhow, Context, Result};

use simulator_core::broadcaster::BroadcasterRedisReplayBoundary;

use super::reader::{RedisStreamInfo, RedisStreamMessage};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RedisEmptyPollAction {
    CaughtUp,
    Pending,
}

pub(super) fn redis_empty_poll_action(
    checkpoint: &RedisReplayCheckpoint,
    stream_info: Option<&RedisStreamInfo>,
    required_message_seq: u64,
) -> Result<RedisEmptyPollAction> {
    checkpoint.ensure_reached_required_boundary(required_message_seq)?;
    let Some(stream_info) = stream_info else {
        if checkpoint.last_message_seq == 0 {
            return Ok(RedisEmptyPollAction::CaughtUp);
        }
        return Err(anyhow!(
            "Redis replay gap: stream disappeared after checkpoint {}",
            checkpoint.entry_id
        ));
    };

    let checkpoint_entry_id = parse_redis_entry_id(&checkpoint.entry_id)?;
    let last_generated_entry_id = parse_redis_entry_id(&stream_info.last_generated_entry_id)?;
    if last_generated_entry_id < checkpoint_entry_id {
        return Err(anyhow!(
            "Redis replay gap: stream moved backwards from checkpoint {} to last generated {}",
            checkpoint.entry_id,
            stream_info.last_generated_entry_id
        ));
    }
    if last_generated_entry_id == checkpoint_entry_id {
        return Ok(RedisEmptyPollAction::CaughtUp);
    }

    let expected_seq = checkpoint
        .last_message_seq
        .checked_add(1)
        .ok_or_else(|| anyhow!("Redis replay message_seq overflow"))?;
    let expected_entry_id = redis_entry_id(checkpoint.boundary.generation, expected_seq);
    let expected_entry_id_parts = parse_redis_entry_id(&expected_entry_id)?;
    let Some(first_entry_id) = &stream_info.first_entry_id else {
        return Err(anyhow!(
            "Redis replay gap: stream generated {} after checkpoint {} but retained no entries",
            stream_info.last_generated_entry_id,
            checkpoint.entry_id
        ));
    };
    let Some(last_entry_id) = &stream_info.last_entry_id else {
        return Err(anyhow!(
            "Redis replay gap: stream generated {} after checkpoint {} but retained no last entry",
            stream_info.last_generated_entry_id,
            checkpoint.entry_id
        ));
    };

    let first_entry_id_parts = parse_redis_entry_id(first_entry_id)?;
    if first_entry_id_parts > expected_entry_id_parts {
        return Err(anyhow!(
            "Redis replay gap: first retained entry {} is after expected entry {}",
            first_entry_id,
            expected_entry_id
        ));
    }

    let last_entry_id_parts = parse_redis_entry_id(last_entry_id)?;
    if last_entry_id_parts <= checkpoint_entry_id {
        return Err(anyhow!(
            "Redis replay gap: stream generated {} after checkpoint {} but last retained entry is {}",
            stream_info.last_generated_entry_id,
            checkpoint.entry_id,
            last_entry_id
        ));
    }

    Ok(RedisEmptyPollAction::Pending)
}

pub(super) struct RedisReplayCheckpoint {
    pub(super) boundary: BroadcasterRedisReplayBoundary,
    pub(super) entry_id: String,
    pub(super) last_message_seq: u64,
    pub(super) expected_chain_id: u64,
}

impl RedisReplayCheckpoint {
    pub(super) fn new(boundary: BroadcasterRedisReplayBoundary, expected_chain_id: u64) -> Self {
        Self {
            entry_id: boundary.exclusive_entry_id(),
            last_message_seq: boundary.exclusive_message_seq,
            boundary,
            expected_chain_id,
        }
    }

    pub(super) fn entry_id(&self) -> &str {
        &self.entry_id
    }

    pub(super) fn ensure_next_message(&self, message: &RedisStreamMessage) -> Result<()> {
        if message.entry.stream_id != self.boundary.stream_id {
            return Err(anyhow!(
                "Redis replay gap: expected stream_id {}, got {}",
                self.boundary.stream_id,
                message.entry.stream_id
            ));
        }
        if message.entry.chain_id != self.expected_chain_id {
            return Err(anyhow!(
                "Redis replay gap: expected chain_id {}, got {}",
                self.expected_chain_id,
                message.entry.chain_id
            ));
        }

        if message.entry.message_seq <= self.last_message_seq {
            return Err(anyhow!(
                "Redis replay gap: got stale message_seq {} after {} at {}",
                message.entry.message_seq,
                self.entry_id,
                message.entry_id
            ));
        }

        let expected_seq = self
            .last_message_seq
            .checked_add(1)
            .ok_or_else(|| anyhow!("Redis replay message_seq overflow"))?;
        if message.entry.message_seq != expected_seq {
            return Err(anyhow!(
                "Redis replay gap: expected message_seq {} after {}, got {} at {}",
                expected_seq,
                self.entry_id,
                message.entry.message_seq,
                message.entry_id
            ));
        }

        let expected_entry_id = redis_entry_id(self.boundary.generation, expected_seq);
        if message.entry_id != expected_entry_id {
            return Err(anyhow!(
                "Redis replay gap: expected entry id {}, got {}",
                expected_entry_id,
                message.entry_id
            ));
        }

        if let Some(snapshot_id) = &message.entry.snapshot_id {
            if snapshot_id != &self.boundary.snapshot_id {
                return Err(anyhow!(
                    "Redis replay gap: expected snapshot_id {}, got {}",
                    self.boundary.snapshot_id,
                    snapshot_id
                ));
            }
        }

        Ok(())
    }

    pub(super) fn ensure_reached_required_boundary(&self, required_message_seq: u64) -> Result<()> {
        if self.last_message_seq >= required_message_seq {
            return Ok(());
        }

        Err(anyhow!(
            "Redis replay gap: stream ended at message_seq {} ({}) before required snapshot replay boundary {}",
            self.last_message_seq,
            self.entry_id,
            required_message_seq
        ))
    }

    pub(super) fn mark_applied(&mut self, message: &RedisStreamMessage) {
        self.entry_id = message.entry_id.clone();
        self.last_message_seq = message.entry.message_seq;
    }

    pub(super) fn mark_generation_handoff(
        &mut self,
        boundary: BroadcasterRedisReplayBoundary,
        entry_id: String,
    ) {
        self.entry_id = entry_id;
        self.last_message_seq = boundary.exclusive_message_seq;
        self.boundary = boundary;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct RedisEntryIdParts {
    pub(super) millis: u64,
    pub(super) sequence: u64,
}

pub(super) fn parse_redis_entry_id(entry_id: &str) -> Result<RedisEntryIdParts> {
    let Some((millis, sequence)) = entry_id.split_once('-') else {
        return Err(anyhow!("invalid Redis stream entry id: {entry_id}"));
    };
    Ok(RedisEntryIdParts {
        millis: millis
            .parse()
            .with_context(|| format!("invalid Redis stream entry id: {entry_id}"))?,
        sequence: sequence
            .parse()
            .with_context(|| format!("invalid Redis stream entry id: {entry_id}"))?,
    })
}

pub(super) fn redis_entry_id(generation: u64, message_seq: u64) -> String {
    format!("{generation}-{message_seq}")
}
