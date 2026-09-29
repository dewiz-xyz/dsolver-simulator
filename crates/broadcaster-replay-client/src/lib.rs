//! Shared client for bootstrapping broadcaster snapshots and replaying Redis deltas.
//!
//! The current bootstrap source is the broadcaster HTTP snapshot-session API. Redis Streams
//! carry deltas after the snapshot replay boundary returned by that session.

mod bootstrap;
mod checkpoint;
mod client;
mod error;
mod reader;
mod snapshot;
mod url;

pub use bootstrap::{SnapshotBackend, SnapshotBootstrap};
pub use client::{
    BroadcasterReplayClient, BroadcasterReplayConfig, ReplayBatch, ReplayMessage, ReplayPoll,
};
pub use error::{BroadcasterReplayClientError, Result};
pub use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterBlockRef, BroadcasterContractError,
    BroadcasterEnvelope, BroadcasterMessageKind, BroadcasterPayload, BroadcasterProgress,
    BroadcasterProtocolMessage, BroadcasterProtocolSyncStatus, BroadcasterProtocolSyncStatusKind,
    BroadcasterRedisReplayBoundary, BroadcasterRedisStreamEntry,
    BroadcasterSnapshotSessionResponse, BroadcasterStateEntry, BroadcasterUpdateMessage,
    BroadcasterUpdatePartition,
};

pub use self::checkpoint::ReplayCheckpoint;

#[cfg(test)]
mod tests {
    use anyhow::{anyhow, Result};
    use redis::streams::{StreamKey, StreamReadReply};
    use simulator_core::broadcaster::{
        BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterHeartbeat,
        BroadcasterPayload, BroadcasterProgress, BroadcasterRedisReplayBoundary,
        BroadcasterRedisStreamEntry,
    };

    use super::checkpoint::{redis_empty_poll_action, RedisEmptyPollAction};
    use super::reader::{
        blocking_read_timeout, redis_connect_error, redis_xread_messages, RedisStreamInfo,
        RedisStreamMessage,
    };
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
        let entry = BroadcasterRedisStreamEntry::from_envelope(
            ETHEREUM_CHAIN_ID,
            &envelope,
            envelope.message_seq,
        )?;

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
        let entry = BroadcasterRedisStreamEntry::from_envelope(
            BASE_CHAIN_ID,
            &envelope,
            envelope.message_seq,
        )?;

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
    fn replay_checkpoint_detects_writer_replacement_before_duplicate_sequence() -> Result<()> {
        let checkpoint = ReplayCheckpoint::new(replay_boundary(18)?, ETHEREUM_CHAIN_ID);
        let envelope = BroadcasterEnvelope::new(
            "stream-2",
            1,
            BroadcasterPayload::Progress(BroadcasterProgress::new(
                ETHEREUM_CHAIN_ID,
                "snapshot-2",
                vec![BroadcasterBackend::Native],
                "writer_promoted",
            )?),
        );
        let entry = BroadcasterRedisStreamEntry::from_envelope(
            ETHEREUM_CHAIN_ID,
            &envelope,
            envelope.message_seq,
        )?;

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
    fn empty_redis_poll_detects_trimmed_history_gap_before_next_expected_entry() -> Result<()> {
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
    fn redis_xread_timeout_is_a_transport_failure() -> Result<()> {
        let timeout: redis::RedisError = std::io::Error::from(std::io::ErrorKind::TimedOut).into();

        let error = redis_xread_messages("stream", Err(timeout))
            .err()
            .ok_or_else(|| anyhow!("timeout should fail"))?;

        assert!(format!("{error:#}").contains("Redis XREAD transport failed"));
        Ok(())
    }

    #[test]
    fn redis_xread_timeout_exceeds_block_window() {
        assert_eq!(
            blocking_read_timeout(5_000),
            std::time::Duration::from_secs(7)
        );
    }

    #[test]
    fn redis_xread_broken_pipe_includes_command_context() -> Result<()> {
        let broken_pipe: redis::RedisError =
            std::io::Error::from(std::io::ErrorKind::BrokenPipe).into();

        let error = redis_xread_messages("stream", Err(broken_pipe))
            .err()
            .ok_or_else(|| anyhow!("broken pipe should fail"))?;

        assert!(format!("{error:#}").contains("Redis XREAD transport failed"));
        Ok(())
    }

    #[test]
    fn redis_xread_permanent_error_is_not_transport_failure() -> Result<()> {
        let auth = redis::RedisError::from((
            redis::ErrorKind::AuthenticationFailed,
            "invalid credentials",
        ));

        let error = redis_xread_messages("stream", Err(auth))
            .err()
            .ok_or_else(|| anyhow!("authentication error should fail"))?;

        assert!(matches!(
            error,
            super::BroadcasterReplayClientError::RedisRead { .. }
        ));
        Ok(())
    }

    #[test]
    fn redis_xread_retryable_server_error_uses_transport_retry() -> Result<()> {
        let try_again = redis::RedisError::from((
            redis::ErrorKind::Server(redis::ServerErrorKind::TryAgain),
            "retry the command",
        ));

        let error = redis_xread_messages("stream", Err(try_again))
            .err()
            .ok_or_else(|| anyhow!("TRYAGAIN should fail the current poll"))?;

        assert!(matches!(
            error,
            super::BroadcasterReplayClientError::RedisReadTransport { .. }
        ));
        Ok(())
    }

    #[test]
    fn redis_connect_setup_failure_is_not_transport() -> Result<()> {
        let invalid_url = redis::Client::open("not-a-redis-url")
            .err()
            .ok_or_else(|| anyhow!("an invalid Redis URL should fail"))?;
        let rejected =
            redis::RedisError::from((redis::ErrorKind::AuthenticationFailed, "invalid password"));

        for error in [invalid_url, rejected] {
            assert!(matches!(
                redis_connect_error(error),
                super::BroadcasterReplayClientError::RedisConnect { .. }
            ));
        }
        Ok(())
    }

    #[test]
    fn redis_connect_refused_or_timed_out_connection_uses_transport_retry() {
        for kind in [
            std::io::ErrorKind::ConnectionRefused,
            std::io::ErrorKind::TimedOut,
        ] {
            let error = redis::RedisError::from(std::io::Error::new(kind, "connection failed"));

            assert!(
                matches!(
                    redis_connect_error(error),
                    super::BroadcasterReplayClientError::RedisConnectTransport { .. }
                ),
                "{kind:?}"
            );
        }
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

    fn replay_boundary(exclusive_message_seq: u64) -> Result<BroadcasterRedisReplayBoundary> {
        Ok(BroadcasterRedisReplayBoundary::new(
            "dsolver:broadcaster:test:events",
            "stream-1",
            "snapshot-1",
            1,
            exclusive_message_seq,
            exclusive_message_seq,
        )?)
    }

    const ETHEREUM_CHAIN_ID: u64 = 1;
    const BASE_CHAIN_ID: u64 = 8453;
}
