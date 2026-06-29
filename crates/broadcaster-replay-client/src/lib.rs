//! Shared client for bootstrapping broadcaster snapshots and replaying Redis deltas.
//!
//! The current bootstrap source is the broadcaster HTTP snapshot-session API. Redis Streams
//! carry deltas after the snapshot replay boundary returned by that session.

mod checkpoint;
mod client;
mod error;
mod handoff;
mod reader;
mod snapshot;
mod url;

pub use client::{
    BroadcasterReplayClient, BroadcasterReplayConfig, GenerationHandoffCandidate, ReplayBatch,
    ReplayBatchItem, ReplayMessage, ReplayPoll,
};
pub use error::{BroadcasterReplayClientError, Result};
pub use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterGenerationHandoff,
    BroadcasterPayload, BroadcasterProgress, BroadcasterRedisReplayBoundary,
    BroadcasterRedisStreamEntry, BroadcasterSnapshotSessionResponse,
};

pub use self::checkpoint::ReplayCheckpoint;

#[cfg(test)]
mod tests {
    use anyhow::{anyhow, Result};
    use redis::streams::{StreamKey, StreamReadReply};
    use simulator_core::broadcaster::{
        BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope,
        BroadcasterGenerationHandoff, BroadcasterHeartbeat, BroadcasterPayload,
        BroadcasterProgress, BroadcasterRedisReplayBoundary, BroadcasterRedisStreamEntry,
    };

    use super::checkpoint::{redis_empty_poll_action, RedisEmptyPollAction};
    use super::client::{build_replay_batch, ReplayBatchItem};
    use super::reader::{redis_xread_messages, RedisStreamInfo, RedisStreamMessage};
    use super::ReplayCheckpoint;

    #[test]
    fn replay_checkpoint_rejects_message_sequence_gap() -> Result<()> {
        let checkpoint = ReplayCheckpoint::new(replay_boundary(0)?, ETHEREUM_CHAIN_ID);
        let envelope = BroadcasterEnvelope::new(
            "stream-1",
            2,
            BroadcasterPayload::Heartbeat(BroadcasterHeartbeat::new(
                ETHEREUM_CHAIN_ID,
                "snapshot-1",
                vec![BroadcasterBackendHead::new(BroadcasterBackend::Native, 12)],
            )?),
        );
        let entry = BroadcasterRedisStreamEntry::from_envelope(ETHEREUM_CHAIN_ID, &envelope)?;

        let Err(error) = checkpoint.ensure_next_message(&RedisStreamMessage {
            entry_id: "1-2".to_string(),
            entry,
        }) else {
            return Err(anyhow!(
                "message_seq gap should fail before applying the delta"
            ));
        };

        assert!(error.to_string().contains("Redis replay gap"));
        Ok(())
    }

    #[test]
    fn replay_checkpoint_rejects_wrong_chain_id_before_applying_update() -> Result<()> {
        let checkpoint = ReplayCheckpoint::new(replay_boundary(0)?, ETHEREUM_CHAIN_ID);
        let envelope = heartbeat_envelope("stream-1", 1, BASE_CHAIN_ID)?;
        let entry = BroadcasterRedisStreamEntry::from_envelope(BASE_CHAIN_ID, &envelope)?;

        let Err(error) = checkpoint.ensure_next_message(&RedisStreamMessage {
            entry_id: "1-1".to_string(),
            entry,
        }) else {
            return Err(anyhow!("wrong-chain Redis update should fail"));
        };

        assert!(error.to_string().contains("expected chain_id"));
        Ok(())
    }

    #[test]
    fn replay_checkpoint_detects_generation_reset_before_duplicate_sequence() -> Result<()> {
        let checkpoint = ReplayCheckpoint::new(replay_boundary(18)?, ETHEREUM_CHAIN_ID);
        let envelope = BroadcasterEnvelope::new(
            "stream-2",
            1,
            BroadcasterPayload::Progress(BroadcasterProgress::new(
                ETHEREUM_CHAIN_ID,
                "snapshot-2",
                vec![BroadcasterBackend::Native],
                "generation_reset",
            )?),
        );
        let entry = BroadcasterRedisStreamEntry::from_envelope(ETHEREUM_CHAIN_ID, &envelope)?;

        let Err(error) = checkpoint.ensure_next_message(&RedisStreamMessage {
            entry_id: "2-1".to_string(),
            entry,
        }) else {
            return Err(anyhow!(
                "new generation with a low message_seq should fail before being skipped"
            ));
        };

        assert!(error.to_string().contains("Redis replay gap"));
        Ok(())
    }

    #[test]
    fn empty_redis_poll_marks_caught_up_when_no_new_entry_was_generated() -> Result<()> {
        let checkpoint = ReplayCheckpoint::new(replay_boundary(3)?, ETHEREUM_CHAIN_ID);
        let stream_info = RedisStreamInfo {
            last_generated_entry_id: "1-3".to_string(),
            first_entry_id: Some("1-1".to_string()),
            last_entry_id: Some("1-3".to_string()),
        };

        let action = redis_empty_poll_action(&checkpoint, Some(&stream_info))?;

        assert_eq!(action, RedisEmptyPollAction::CaughtUp);
        Ok(())
    }

    #[test]
    fn empty_redis_poll_detects_stream_recreation_behind_checkpoint() -> Result<()> {
        let checkpoint = ReplayCheckpoint::new(replay_boundary(9)?, ETHEREUM_CHAIN_ID);
        let stream_info = RedisStreamInfo {
            last_generated_entry_id: "1-2".to_string(),
            first_entry_id: Some("1-1".to_string()),
            last_entry_id: Some("1-2".to_string()),
        };

        let Err(error) = redis_empty_poll_action(&checkpoint, Some(&stream_info)) else {
            return Err(anyhow!(
                "Redis stream recreation behind the checkpoint should fail closed"
            ));
        };

        assert!(error.to_string().contains("Redis replay gap"));
        assert!(error.to_string().contains("moved backwards"));
        Ok(())
    }

    #[test]
    fn empty_redis_poll_detects_fully_trimmed_generated_entries() -> Result<()> {
        let checkpoint = ReplayCheckpoint::new(replay_boundary(3)?, ETHEREUM_CHAIN_ID);
        let stream_info = RedisStreamInfo {
            last_generated_entry_id: "1-5".to_string(),
            first_entry_id: None,
            last_entry_id: None,
        };

        let Err(error) = redis_empty_poll_action(&checkpoint, Some(&stream_info)) else {
            return Err(anyhow!(
                "trimmed Redis entries after the checkpoint should be a gap"
            ));
        };

        assert!(error.to_string().contains("Redis replay gap"));
        assert!(error.to_string().contains("retained no entries"));
        Ok(())
    }

    #[test]
    fn empty_redis_poll_detects_retention_gap_before_next_expected_entry() -> Result<()> {
        let checkpoint = ReplayCheckpoint::new(replay_boundary(3)?, ETHEREUM_CHAIN_ID);
        let stream_info = RedisStreamInfo {
            last_generated_entry_id: "1-5".to_string(),
            first_entry_id: Some("1-5".to_string()),
            last_entry_id: Some("1-5".to_string()),
        };

        let Err(error) = redis_empty_poll_action(&checkpoint, Some(&stream_info)) else {
            return Err(anyhow!(
                "first retained Redis entry after the expected checkpoint should be a gap"
            ));
        };

        assert!(error.to_string().contains("Redis replay gap"));
        assert!(error.to_string().contains("first retained entry 1-5"));
        Ok(())
    }

    #[test]
    fn empty_redis_poll_waits_when_new_entry_arrives_after_timeout() -> Result<()> {
        let checkpoint = ReplayCheckpoint::new(replay_boundary(3)?, ETHEREUM_CHAIN_ID);
        let stream_info = RedisStreamInfo {
            last_generated_entry_id: "1-4".to_string(),
            first_entry_id: Some("1-4".to_string()),
            last_entry_id: Some("1-4".to_string()),
        };

        let action = redis_empty_poll_action(&checkpoint, Some(&stream_info))?;

        assert_eq!(action, RedisEmptyPollAction::Pending);
        Ok(())
    }

    #[test]
    fn redis_xread_timeout_returns_empty_poll() -> Result<()> {
        let timeout: redis::RedisError = std::io::Error::from(std::io::ErrorKind::TimedOut).into();

        let messages =
            redis_xread_messages("stream", Err(timeout)).map_err(|error| anyhow!("{error:?}"))?;

        assert!(messages.is_empty());
        Ok(())
    }

    #[test]
    fn redis_xread_broken_pipe_includes_command_context() -> Result<()> {
        let broken_pipe: redis::RedisError =
            std::io::Error::from(std::io::ErrorKind::BrokenPipe).into();

        let error = redis_xread_messages("stream", Err(broken_pipe))
            .err()
            .ok_or_else(|| anyhow!("broken pipe should fail"))?;

        assert!(format!("{error:#}").contains("Redis XREAD failed"));
        Ok(())
    }

    #[test]
    fn redis_xread_malformed_reply_reports_stream_key_mismatch() -> Result<()> {
        let reply = StreamReadReply {
            keys: vec![StreamKey {
                key: "other-stream".to_string(),
                ids: Vec::new(),
            }],
        };

        let error = redis_xread_messages("stream", Ok(Some(reply)))
            .err()
            .ok_or_else(|| anyhow!("malformed stream reply should fail"))?;

        assert!(error.to_string().contains("expected stream"));
        Ok(())
    }

    #[test]
    fn replay_batch_accepts_valid_generation_handoff() -> Result<()> {
        let checkpoint = ReplayCheckpoint::new(replay_boundary(3)?, ETHEREUM_CHAIN_ID);
        let envelope = handoff_envelope(Some(BroadcasterGenerationHandoff::new(
            "stream-1",
            "1-3",
            vec![BroadcasterBackendHead::new(BroadcasterBackend::Native, 40)],
        )?))?;
        let entry = BroadcasterRedisStreamEntry::from_envelope(ETHEREUM_CHAIN_ID, &envelope)?;

        let batch = build_replay_batch(
            &checkpoint,
            vec![RedisStreamMessage {
                entry_id: "2-1".to_string(),
                entry,
            }],
            true,
        )?;

        let [ReplayBatchItem::GenerationHandoff(candidate)] = batch.items.as_slice() else {
            return Err(anyhow!(
                "generation handoff marker should become a handoff item"
            ));
        };
        assert_eq!(candidate.boundary.stream_id, "stream-2");
        assert_eq!(candidate.boundary.snapshot_id, "snapshot-2");
        assert_eq!(candidate.checkpoint_after.entry_id(), "2-1");
        Ok(())
    }

    #[test]
    fn replay_batch_rejects_handoff_without_previous_checkpoint_proof() -> Result<()> {
        let checkpoint = ReplayCheckpoint::new(replay_boundary(3)?, ETHEREUM_CHAIN_ID);
        let envelope = handoff_envelope(None)?;
        let entry = BroadcasterRedisStreamEntry::from_envelope(ETHEREUM_CHAIN_ID, &envelope)?;

        let Err(error) = build_replay_batch(
            &checkpoint,
            vec![RedisStreamMessage {
                entry_id: "2-1".to_string(),
                entry,
            }],
            true,
        ) else {
            return Err(anyhow!("handoff marker without proof should fail closed"));
        };

        assert!(error.to_string().contains("missing handoff proof"));
        Ok(())
    }

    fn heartbeat_envelope(
        stream_id: &str,
        message_seq: u64,
        chain_id: u64,
    ) -> Result<BroadcasterEnvelope> {
        Ok(BroadcasterEnvelope::new(
            stream_id,
            message_seq,
            BroadcasterPayload::Heartbeat(BroadcasterHeartbeat::new(
                chain_id,
                "snapshot-1",
                vec![BroadcasterBackendHead::new(BroadcasterBackend::Native, 12)],
            )?),
        ))
    }

    fn handoff_envelope(
        handoff: Option<BroadcasterGenerationHandoff>,
    ) -> Result<BroadcasterEnvelope> {
        let progress = match handoff {
            Some(handoff) => BroadcasterProgress::new_with_handoff(
                ETHEREUM_CHAIN_ID,
                "snapshot-2",
                vec![BroadcasterBackend::Native],
                "active_writer_promoted",
                handoff,
            )?,
            None => BroadcasterProgress::new(
                ETHEREUM_CHAIN_ID,
                "snapshot-2",
                vec![BroadcasterBackend::Native],
                "active_writer_promoted",
            )?,
        };
        Ok(BroadcasterEnvelope::new(
            "stream-2",
            1,
            BroadcasterPayload::Progress(progress),
        ))
    }

    fn replay_boundary(exclusive_message_seq: u64) -> Result<BroadcasterRedisReplayBoundary> {
        Ok(BroadcasterRedisReplayBoundary::new(
            "dsolver:broadcaster:test:events",
            "stream-1",
            "snapshot-1",
            1,
            exclusive_message_seq,
        )?)
    }

    const ETHEREUM_CHAIN_ID: u64 = 1;
    const BASE_CHAIN_ID: u64 = 8453;
}
