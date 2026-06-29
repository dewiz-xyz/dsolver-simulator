use simulator_core::broadcaster::{
    BroadcasterEnvelope, BroadcasterMessageKind, BroadcasterPayload, BroadcasterProgress,
    BroadcasterRedisReplayBoundary,
};

use crate::checkpoint::{parse_redis_entry_id, ReplayCheckpoint};
use crate::client::GenerationHandoffCandidate;
use crate::error::{BroadcasterReplayClientError, Result};
use crate::reader::RedisStreamMessage;

pub(crate) fn redis_generation_handoff_candidate(
    checkpoint: &ReplayCheckpoint,
    message: &RedisStreamMessage,
    envelope: &BroadcasterEnvelope,
) -> bool {
    if message.entry.kind != BroadcasterMessageKind::Progress {
        return false;
    }
    if !matches!(envelope.payload, BroadcasterPayload::Progress(_)) {
        return false;
    }
    let Ok(entry_id) = parse_redis_entry_id(&message.entry_id) else {
        return false;
    };
    entry_id.millis == checkpoint.boundary().generation.saturating_add(1) && entry_id.sequence == 1
}

pub(crate) fn validate_handoff_candidate(
    checkpoint: &ReplayCheckpoint,
    message: RedisStreamMessage,
    envelope: BroadcasterEnvelope,
) -> Result<GenerationHandoffCandidate> {
    let progress = handoff_progress_payload(&envelope)?;
    let boundary = validate_handoff_marker(checkpoint, &message, progress)?;
    let entry = message.entry;
    let checkpoint_after = checkpoint.after_generation_handoff(boundary.clone(), message.entry_id);
    Ok(GenerationHandoffCandidate {
        entry,
        envelope,
        boundary,
        checkpoint_after,
    })
}

fn handoff_progress_payload(envelope: &BroadcasterEnvelope) -> Result<&BroadcasterProgress> {
    let BroadcasterPayload::Progress(progress) = &envelope.payload else {
        return Err(BroadcasterReplayClientError::redis_gap(
            "Redis replay gap: generation handoff payload is not progress",
        ));
    };
    Ok(progress)
}

fn validate_handoff_marker(
    checkpoint: &ReplayCheckpoint,
    message: &RedisStreamMessage,
    progress: &BroadcasterProgress,
) -> Result<BroadcasterRedisReplayBoundary> {
    if message.entry.kind != BroadcasterMessageKind::Progress {
        return Err(BroadcasterReplayClientError::redis_gap(format!(
            "Redis replay gap: generation handoff entry kind is {}",
            message.entry.kind
        )));
    }
    if message.entry.chain_id != checkpoint.expected_chain_id() {
        return Err(BroadcasterReplayClientError::redis_gap(format!(
            "Redis replay gap: expected chain_id {}, got {}",
            checkpoint.expected_chain_id(),
            message.entry.chain_id
        )));
    }
    if progress.chain_id != checkpoint.expected_chain_id() {
        return Err(BroadcasterReplayClientError::redis_gap(format!(
            "Redis replay gap: expected progress chain_id {}, got {}",
            checkpoint.expected_chain_id(),
            progress.chain_id
        )));
    }
    if message.entry.message_seq != 1 {
        return Err(BroadcasterReplayClientError::redis_gap(format!(
            "Redis replay gap: generation handoff marker must have message_seq 1, got {}",
            message.entry.message_seq
        )));
    }

    let entry_id = parse_redis_entry_id(&message.entry_id)?;
    let expected_generation = checkpoint
        .boundary()
        .generation
        .checked_add(1)
        .ok_or_else(|| {
            BroadcasterReplayClientError::redis_gap("Redis replay generation overflow")
        })?;
    if entry_id.millis != expected_generation || entry_id.sequence != 1 {
        return Err(BroadcasterReplayClientError::redis_gap(format!(
            "Redis replay gap: expected handoff entry {}-1, got {}",
            expected_generation, message.entry_id
        )));
    }

    let Some(handoff) = progress.handoff.as_ref() else {
        return Err(BroadcasterReplayClientError::redis_gap(
            "Redis replay gap: generation handoff marker is missing handoff proof",
        ));
    };
    if handoff.previous_stream_id != checkpoint.boundary().stream_id {
        return Err(BroadcasterReplayClientError::redis_gap(format!(
            "Redis replay gap: handoff previous stream_id {} does not match checkpoint stream_id {}",
            handoff.previous_stream_id,
            checkpoint.boundary().stream_id
        )));
    }
    if handoff.previous_entry_id != checkpoint.entry_id() {
        return Err(BroadcasterReplayClientError::redis_gap(format!(
            "Redis replay gap: handoff previous entry {} does not match checkpoint {}",
            handoff.previous_entry_id,
            checkpoint.entry_id()
        )));
    }
    let previous_entry_id = parse_redis_entry_id(&handoff.previous_entry_id)?;
    if previous_entry_id.millis != checkpoint.boundary().generation
        || previous_entry_id.sequence != checkpoint.last_message_seq()
    {
        return Err(BroadcasterReplayClientError::redis_gap(format!(
            "Redis replay gap: handoff previous entry {} does not match generation {} message_seq {}",
            handoff.previous_entry_id,
            checkpoint.boundary().generation,
            checkpoint.last_message_seq()
        )));
    }

    let boundary = BroadcasterRedisReplayBoundary::new(
        checkpoint.boundary().stream_key.clone(),
        message.entry.stream_id.clone(),
        progress.snapshot_id.clone(),
        entry_id.millis,
        1,
    )
    .map_err(|error| BroadcasterReplayClientError::redis_gap(error.to_string()))?;
    Ok(boundary)
}
