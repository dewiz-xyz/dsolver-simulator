use std::collections::BTreeMap;

use anyhow::{anyhow, Result};

use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterGenerationHandoff,
    BroadcasterMessageKind, BroadcasterPayload, BroadcasterProgress,
    BroadcasterRedisReplayBoundary,
};

use super::checkpoint::{parse_redis_entry_id, RedisEntryIdParts, RedisReplayCheckpoint};
use super::reader::RedisStreamMessage;
use super::PreparedBroadcasterRedisSubscription;

pub(super) fn redis_generation_handoff_candidate(
    checkpoint: &RedisReplayCheckpoint,
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
    entry_id.millis == checkpoint.boundary.generation.saturating_add(1) && entry_id.sequence == 1
}

pub(super) async fn continue_redis_generation_handoff(
    prepared: &mut PreparedBroadcasterRedisSubscription,
    checkpoint: &mut RedisReplayCheckpoint,
    message: &RedisStreamMessage,
    envelope: &BroadcasterEnvelope,
) -> Result<()> {
    let progress = handoff_progress_payload(envelope)?;
    let (entry_id, handoff) = validate_handoff_marker(checkpoint, message, progress)?;

    let enabled_backends = prepared_enabled_backends(prepared);
    if progress.backends != enabled_backends {
        return Err(anyhow!(
            "Redis replay gap: handoff progress backends {:?} do not match enabled backends {:?}",
            progress.backends,
            enabled_backends
        ));
    }
    ensure_handoff_base_heads_match(prepared, &handoff.base_heads).await?;

    let boundary = handoff_replay_boundary(checkpoint, message, progress, entry_id)?;
    for prepared_processor in &mut prepared.processors {
        prepared_processor
            .processor
            .continue_redis_generation_handoff(&boundary)?;
        prepared_processor.replay_boundary = boundary.clone();
        prepared_processor
            .processor
            .controls
            .broadcaster_subscription()
            .mark_redis_generation_continued(boundary.clone())
            .await;
    }
    prepared.replay_boundary = boundary.clone();
    prepared.required_catch_up_message_seq = boundary.exclusive_message_seq;
    checkpoint.mark_generation_handoff(boundary, message.entry_id.clone());
    Ok(())
}

fn handoff_progress_payload(envelope: &BroadcasterEnvelope) -> Result<&BroadcasterProgress> {
    let BroadcasterPayload::Progress(progress) = &envelope.payload else {
        return Err(anyhow!(
            "Redis replay gap: generation handoff payload is not progress"
        ));
    };
    Ok(progress)
}

fn validate_handoff_marker<'a>(
    checkpoint: &RedisReplayCheckpoint,
    message: &RedisStreamMessage,
    progress: &'a BroadcasterProgress,
) -> Result<(RedisEntryIdParts, &'a BroadcasterGenerationHandoff)> {
    if message.entry.kind != BroadcasterMessageKind::Progress {
        return Err(anyhow!(
            "Redis replay gap: generation handoff entry kind is {}",
            message.entry.kind
        ));
    }
    if message.entry.chain_id != checkpoint.expected_chain_id {
        return Err(anyhow!(
            "Redis replay gap: expected chain_id {}, got {}",
            checkpoint.expected_chain_id,
            message.entry.chain_id
        ));
    }
    if progress.chain_id != checkpoint.expected_chain_id {
        return Err(anyhow!(
            "Redis replay gap: expected progress chain_id {}, got {}",
            checkpoint.expected_chain_id,
            progress.chain_id
        ));
    }
    if message.entry.message_seq != 1 {
        return Err(anyhow!(
            "Redis replay gap: generation handoff marker must have message_seq 1, got {}",
            message.entry.message_seq
        ));
    }

    let entry_id = parse_redis_entry_id(&message.entry_id)?;
    let expected_generation = checkpoint
        .boundary
        .generation
        .checked_add(1)
        .ok_or_else(|| anyhow!("Redis replay generation overflow"))?;
    if entry_id.millis != expected_generation || entry_id.sequence != 1 {
        return Err(anyhow!(
            "Redis replay gap: expected handoff entry {}-1, got {}",
            expected_generation,
            message.entry_id
        ));
    }

    let Some(handoff) = progress.handoff.as_ref() else {
        return Err(anyhow!(
            "Redis replay gap: generation handoff marker is missing handoff proof"
        ));
    };
    ensure_handoff_previous_checkpoint(checkpoint, handoff)?;
    Ok((entry_id, handoff))
}

fn ensure_handoff_previous_checkpoint(
    checkpoint: &RedisReplayCheckpoint,
    handoff: &BroadcasterGenerationHandoff,
) -> Result<()> {
    if handoff.previous_stream_id != checkpoint.boundary.stream_id {
        return Err(anyhow!(
            "Redis replay gap: handoff previous stream_id {} does not match checkpoint stream_id {}",
            handoff.previous_stream_id,
            checkpoint.boundary.stream_id
        ));
    }
    if handoff.previous_entry_id != checkpoint.entry_id {
        return Err(anyhow!(
            "Redis replay gap: handoff previous entry {} does not match checkpoint {}",
            handoff.previous_entry_id,
            checkpoint.entry_id
        ));
    }
    let previous_entry_id = parse_redis_entry_id(&handoff.previous_entry_id)?;
    if previous_entry_id.millis != checkpoint.boundary.generation
        || previous_entry_id.sequence != checkpoint.last_message_seq
    {
        return Err(anyhow!(
            "Redis replay gap: handoff previous entry {} does not match generation {} message_seq {}",
            handoff.previous_entry_id,
            checkpoint.boundary.generation,
            checkpoint.last_message_seq
        ));
    }
    Ok(())
}

fn handoff_replay_boundary(
    checkpoint: &RedisReplayCheckpoint,
    message: &RedisStreamMessage,
    progress: &BroadcasterProgress,
    entry_id: RedisEntryIdParts,
) -> Result<BroadcasterRedisReplayBoundary> {
    BroadcasterRedisReplayBoundary::new(
        checkpoint.boundary.stream_key.clone(),
        message.entry.stream_id.clone(),
        progress.snapshot_id.clone(),
        entry_id.millis,
        1,
    )
    .map_err(Into::into)
}

fn prepared_enabled_backends(
    prepared: &PreparedBroadcasterRedisSubscription,
) -> Vec<BroadcasterBackend> {
    let mut backends = prepared
        .processors
        .iter()
        .map(|processor| processor.processor.controls.backend())
        .collect::<Vec<_>>();
    backends.sort();
    backends
}

async fn ensure_handoff_base_heads_match(
    prepared: &PreparedBroadcasterRedisSubscription,
    base_heads: &[BroadcasterBackendHead],
) -> Result<()> {
    let mut heads_by_backend = BTreeMap::new();
    for head in base_heads {
        heads_by_backend.insert(head.backend, head.block_number);
    }

    for prepared_processor in &prepared.processors {
        let backend = prepared_processor.processor.controls.backend();
        let Some(expected_block) = heads_by_backend.remove(&backend) else {
            return Err(anyhow!(
                "Redis replay gap: handoff base heads are missing {} backend",
                backend
            ));
        };
        let current_block = prepared_processor
            .processor
            .controls
            .state_store()
            .current_block()
            .await;
        if current_block != expected_block {
            return Err(anyhow!(
                "Redis replay gap: handoff {} base head {} does not match local block {}",
                backend,
                expected_block,
                current_block
            ));
        }
    }

    if let Some((backend, _)) = heads_by_backend.into_iter().next() {
        return Err(anyhow!(
            "Redis replay gap: handoff base heads include unexpected {} backend",
            backend
        ));
    }
    Ok(())
}
