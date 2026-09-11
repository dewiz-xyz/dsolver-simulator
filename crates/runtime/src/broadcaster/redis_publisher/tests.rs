use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use num_bigint::BigUint;
use redis::streams::{StreamId, StreamRangeReply};
use redis::Value;
use state_history::GapReason;
use tokio::sync::{Mutex, Notify};
use tokio::time::{sleep, timeout, Duration};
use tycho_simulation::protocol::models::{ProtocolComponent, Update};
use tycho_simulation::tycho_client::feed::{BlockHeader, SynchronizerState};
use tycho_simulation::tycho_common::dto::ProtocolStateDelta;
use tycho_simulation::tycho_common::models::{token::Token, Chain};
use tycho_simulation::tycho_common::simulation::errors::{SimulationError, TransitionError};
use tycho_simulation::tycho_common::simulation::protocol_sim::{
    Balances, GetAmountOutResult, ProtocolSim,
};
use tycho_simulation::tycho_common::Bytes;

use super::{
    redis_entry_fields, redis_entry_id, redis_stream_entry_matches_reply,
    writer_lease_ttl_for_heartbeat_interval, BroadcasterDeploymentPhase, BroadcasterRedisPublisher,
    BroadcasterRedisPublisherConfig, BroadcasterRedisPublisherMode, RedisStreamWriter,
    TokioRedisStreamWriter,
};
use crate::broadcaster::state::BroadcasterSnapshotCache;
use crate::broadcaster::state_history::test_state_history_runtime;
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterMessageKind,
    BroadcasterPayload, BroadcasterProgress, BroadcasterRedisStreamEntry, BroadcasterUpdateMessage,
};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct DummySim(u8);

#[typetag::serde(name = "RedisPublisherDummySim")]
impl ProtocolSim for DummySim {
    fn fee(&self) -> f64 {
        0.0
    }

    fn spot_price(&self, _base: &Token, _quote: &Token) -> Result<f64, SimulationError> {
        Ok(0.0)
    }

    fn get_amount_out(
        &self,
        amount_in: BigUint,
        _token_in: &Token,
        _token_out: &Token,
    ) -> Result<GetAmountOutResult, SimulationError> {
        Ok(GetAmountOutResult::new(
            amount_in,
            BigUint::from(0u8),
            self.clone_box(),
        ))
    }

    fn get_limits(
        &self,
        _sell_token: Bytes,
        _buy_token: Bytes,
    ) -> Result<(BigUint, BigUint), SimulationError> {
        Ok((BigUint::from(0u8), BigUint::from(0u8)))
    }

    fn delta_transition(
        &mut self,
        _delta: ProtocolStateDelta,
        _tokens: &HashMap<Bytes, Token>,
        _balances: &Balances,
    ) -> Result<(), TransitionError> {
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn ProtocolSim> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn eq(&self, other: &dyn ProtocolSim) -> bool {
        other
            .as_any()
            .downcast_ref::<DummySim>()
            .map(|value| value.0 == self.0)
            .unwrap_or(false)
    }
}

#[tokio::test]
async fn passive_publisher_skips_appends_and_has_no_replay_boundary() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));

    let update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(update))
        .await?;

    let status = publisher.status_snapshot().await;
    assert_eq!(status.mode, "passive");
    assert!(status.healthy);
    assert!(status.replay_boundary.is_none());
    assert!(publisher.replay_boundary().await.is_err());
    assert!(
        writer.appends().await.is_empty(),
        "passive warmup must not append Redis entries"
    );
    Ok(())
}

#[tokio::test]
async fn accepted_update_reaches_state_history_queue() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let (state_history, _) = test_state_history_runtime(8);
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer))
        .with_state_history(Arc::clone(&state_history));
    publisher
        .promote(base_heads([BroadcasterBackend::Native]), "test_active")
        .await?;
    let update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;

    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(update))
        .await?;

    assert_eq!(state_history.handle.status().enqueued_deltas, 1);
    Ok(())
}

#[tokio::test]
async fn projection_failure_records_cursorless_write_failed_gap() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Rfq, 10, "rfq-1").await?;
    let writer = FakeRedisWriter::default();
    let (state_history, probe) = test_state_history_runtime(8);
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()))
        .with_state_history(Arc::clone(&state_history));
    publisher
        .promote(base_heads([BroadcasterBackend::Rfq]), "test_active")
        .await?;
    let update = raw_cache
        .apply_update(&update(BroadcasterBackend::Rfq, u64::MAX, "rfq-overflow"))
        .await?;

    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(update))
        .await?;

    assert_eq!(writer.appends().await.len(), 2);
    assert_eq!(state_history.handle.status().enqueued_deltas, 0);
    assert_eq!(
        probe.gaps(),
        vec![state_history::GapRecord {
            chain_id: Chain::Ethereum.id(),
            generation: 1,
            from_message_seq: 2,
            to_message_seq: 2,
            reason: GapReason::WriteFailed,
            from_block_number: None,
            to_block_number: None,
            from_observed_at_ms: None,
            to_observed_at_ms: None,
        }]
    );
    Ok(())
}

#[tokio::test]
async fn heartbeat_and_progress_do_not_reach_state_history_queue() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let (state_history, _) = test_state_history_runtime(8);
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer))
        .with_state_history(Arc::clone(&state_history));
    publisher
        .promote(base_heads([BroadcasterBackend::Native]), "test_active")
        .await?;
    let heartbeat = raw_cache
        .heartbeat()
        .await?
        .ok_or_else(|| anyhow!("ready cache should produce a heartbeat"))?;
    let progress = BroadcasterPayload::Progress(BroadcasterProgress::new(
        Chain::Ethereum.id(),
        "chain-1-snapshot-1",
        vec![BroadcasterBackend::Native],
        "test",
    )?);

    publisher.publish_accepted_payload(heartbeat).await?;
    publisher.publish_accepted_payload(progress).await?;

    assert_eq!(state_history.handle.status().enqueued_deltas, 0);
    Ok(())
}

#[tokio::test]
async fn enqueue_failure_does_not_fail_publish() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let (state_history, _) = test_state_history_runtime(1);
    state_history.handle.clone().shutdown(Duration::ZERO).await;
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()))
        .with_state_history(Arc::clone(&state_history));
    publisher
        .promote(base_heads([BroadcasterBackend::Native]), "test_active")
        .await?;
    let update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;

    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(update))
        .await?;

    assert_eq!(writer.appends().await.len(), 2);
    assert_eq!(state_history.handle.status().dropped_deltas, 1);
    Ok(())
}

#[tokio::test]
async fn status_snapshot_returns_cached_state_while_append_holds_inner() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = Arc::new(BroadcasterRedisPublisher::new(
        publisher_config(),
        Arc::new(writer.clone()),
    ));
    publisher
        .promote(
            base_heads([BroadcasterBackend::Native]),
            "active_writer_promoted",
        )
        .await?;
    assert_eq!(publisher.status_snapshot().await.mode, "active");
    writer.block_message_seq(2).await;

    let update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    let publish = tokio::spawn({
        let publisher = Arc::clone(&publisher);
        async move {
            publisher
                .publish_accepted_payload(BroadcasterPayload::Update(update))
                .await
        }
    });
    writer.wait_for_blocked_append().await;

    let status = timeout(Duration::from_millis(100), publisher.status_snapshot())
        .await
        .map_err(|_| anyhow!("status snapshot blocked on an in-flight append"))?;
    assert_eq!(status.mode, "active");
    let verified = timeout(
        Duration::from_millis(100),
        publisher.verified_status_snapshot(),
    )
    .await
    .map_err(|_| anyhow!("verified status snapshot blocked on an in-flight append"))?;
    assert_eq!(verified.mode, "active");

    writer.release_blocked_append();
    publish.await??;
    assert_eq!(publisher.status_snapshot().await.append_success_count, 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn verified_status_bounds_fence_verification() -> Result<()> {
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(
            base_heads([BroadcasterBackend::Native]),
            "active_writer_promoted",
        )
        .await?;
    assert_eq!(publisher.status_snapshot().await.mode, "active");
    writer.block_next_verify().await;

    let status = publisher.verified_status_snapshot().await;

    assert_eq!(status.mode, "active");
    assert!(status.last_error.is_none());
    Ok(())
}

#[tokio::test]
async fn startup_handoff_preserves_cross_backend_publication_order_until_admission() -> Result<()> {
    let native_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));

    let handoff_id = publisher.begin_startup_handoff().await?;
    assert_eq!(
        publisher.deployment_admission_snapshot().phase,
        BroadcasterDeploymentPhase::CandidateBuilding
    );
    let native = native_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    let rfq = rfq_cache
        .apply_update(&update(BroadcasterBackend::Rfq, 21, "rfq-2"))
        .await?;
    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(native))
        .await?;
    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(rfq))
        .await?;

    let handoff = publisher.freeze_startup_handoff(handoff_id).await?;
    publisher.set_deployment_phase(BroadcasterDeploymentPhase::Promoting, None);
    publisher
        .promote(
            base_heads([BroadcasterBackend::Native, BroadcasterBackend::Rfq]),
            "startup_handoff",
        )
        .await?;
    publisher.set_deployment_phase(BroadcasterDeploymentPhase::ArtifactFinalizing, None);
    publisher.drain_startup_handoff(handoff).await?;
    assert_eq!(
        publisher.deployment_admission_snapshot().phase,
        BroadcasterDeploymentPhase::HandoffDraining
    );
    publisher.admit_deployment();

    let appends = writer.appends().await;
    assert_eq!(
        appends
            .iter()
            .map(|append| (
                append.entry.message_seq,
                append.entry.backend_scope.as_str()
            ))
            .collect::<Vec<_>>(),
        vec![(1, "native,rfq"), (2, "native"), (3, "rfq")]
    );
    assert_eq!(
        publisher.deployment_admission_snapshot(),
        super::BroadcasterDeploymentAdmissionSnapshot {
            admitted: true,
            phase: BroadcasterDeploymentPhase::Admitted,
            last_error: None,
        }
    );
    Ok(())
}

#[tokio::test]
async fn startup_handoff_overflow_discards_private_work_and_allows_retry() -> Result<()> {
    let cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let publisher =
        BroadcasterRedisPublisher::new(publisher_config(), Arc::new(FakeRedisWriter::default()));
    let handoff_id = publisher.begin_startup_handoff().await?;

    for block_number in 11..=74 {
        let message = cache
            .apply_update(&update(
                BroadcasterBackend::Native,
                block_number,
                &format!("native-{block_number}"),
            ))
            .await?;
        publisher
            .publish_accepted_payload(BroadcasterPayload::Update(message))
            .await?;
    }

    let Err(error) = publisher.freeze_startup_handoff(handoff_id).await else {
        return Err(anyhow!(
            "64 native blocks must cancel the private startup candidate"
        ));
    };
    assert!(error
        .to_string()
        .contains("configured 64 native-block or 60-second bound"));
    publisher
        .abort_startup_handoff(handoff_id, error.to_string())
        .await;
    let retry_id = publisher.begin_startup_handoff().await?;
    assert_ne!(retry_id, handoff_id);
    assert_eq!(
        publisher.mode().await,
        BroadcasterRedisPublisherMode::Passive
    );
    assert_eq!(
        publisher.deployment_admission_snapshot().phase,
        BroadcasterDeploymentPhase::CandidateBuilding
    );
    Ok(())
}

#[tokio::test]
async fn admission_latches_through_recoverable_failure_and_closes_for_shutdown() -> Result<()> {
    let cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(base_heads([BroadcasterBackend::Native]), "test_active")
        .await?;
    publisher.admit_deployment();

    writer.fail_next_appends(100).await;
    let update = cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    assert!(publisher
        .publish_accepted_payload(BroadcasterPayload::Update(update))
        .await
        .is_err());
    let admitted = publisher.deployment_admission_snapshot();
    assert!(admitted.admitted);
    assert_eq!(admitted.phase, BroadcasterDeploymentPhase::Admitted);

    publisher.begin_shutdown();
    let shutting_down = publisher.deployment_admission_snapshot();
    assert!(!shutting_down.admitted);
    assert_eq!(
        shutting_down.phase,
        BroadcasterDeploymentPhase::ShuttingDown
    );
    Ok(())
}

#[test]
fn deployment_phase_names_cover_every_public_state() {
    assert_eq!(
        [
            BroadcasterDeploymentPhase::Warming,
            BroadcasterDeploymentPhase::CandidateBuilding,
            BroadcasterDeploymentPhase::Promoting,
            BroadcasterDeploymentPhase::ArtifactFinalizing,
            BroadcasterDeploymentPhase::HandoffDraining,
            BroadcasterDeploymentPhase::Admitted,
            BroadcasterDeploymentPhase::Retired,
            BroadcasterDeploymentPhase::Fatal,
            BroadcasterDeploymentPhase::ShuttingDown,
        ]
        .map(BroadcasterDeploymentPhase::as_str),
        [
            "warming",
            "candidate_building",
            "promoting",
            "artifact_finalizing",
            "handoff_draining",
            "admitted",
            "retired",
            "fatal",
            "shutting_down",
        ]
    );
}

#[tokio::test]
#[ignore = "requires a disposable Redis provided through BROADCASTER_REDIS_HANDOFF_TEST_URL"]
#[expect(
    clippy::too_many_lines,
    reason = "the handoff assertions stay together to keep the cross-writer order visible"
)]
async fn two_broadcasters_handoff_without_loss_or_duplication_on_real_redis() -> Result<()> {
    let redis_url = std::env::var("BROADCASTER_REDIS_HANDOFF_TEST_URL")
        .map_err(|_| anyhow!("BROADCASTER_REDIS_HANDOFF_TEST_URL must be set"))?;
    let stream_key = format!(
        "dsolver:broadcaster:handoff-test:{}:events",
        std::process::id()
    );
    let config = BroadcasterRedisPublisherConfig {
        stream_key: stream_key.clone(),
        ..publisher_config()
    };
    let old = BroadcasterRedisPublisher::new(
        config.clone(),
        Arc::new(TokioRedisStreamWriter::connect(&redis_url).await?),
    );
    let new = BroadcasterRedisPublisher::new(
        config,
        Arc::new(TokioRedisStreamWriter::connect(&redis_url).await?),
    );
    let native_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;

    old.promote(
        base_heads([BroadcasterBackend::Native, BroadcasterBackend::Rfq]),
        "old_active",
    )
    .await?;
    let old_final = native_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "old-final"))
        .await?;
    old.publish_accepted_payload(BroadcasterPayload::Update(old_final))
        .await?;

    let handoff_id = new.begin_startup_handoff().await?;
    let native_handoff = native_cache
        .apply_update(&update(BroadcasterBackend::Native, 12, "new-native"))
        .await?;
    let rfq_handoff = rfq_cache
        .apply_update(&update(BroadcasterBackend::Rfq, 21, "new-rfq"))
        .await?;
    new.publish_accepted_payload(BroadcasterPayload::Update(native_handoff))
        .await?;
    new.publish_accepted_payload(BroadcasterPayload::Update(rfq_handoff))
        .await?;
    let handoff = new.freeze_startup_handoff(handoff_id).await?;
    let boundary = new
        .promote(
            base_heads([BroadcasterBackend::Native, BroadcasterBackend::Rfq]),
            "new_active",
        )
        .await?;
    let reconciled_boundary = new
        .promote(
            base_heads([BroadcasterBackend::Native, BroadcasterBackend::Rfq]),
            "new_active",
        )
        .await?;
    assert_eq!(
        reconciled_boundary, boundary,
        "repeating the same promotion token must reconcile the accepted CAS"
    );
    new.drain_startup_handoff(handoff).await?;
    new.admit_deployment();

    let stale = native_cache
        .apply_update(&update(BroadcasterBackend::Native, 13, "stale-old"))
        .await?;
    assert!(
        old.publish_accepted_payload(BroadcasterPayload::Update(stale))
            .await
            .is_err(),
        "the replaced writer must be fenced"
    );

    let client = redis::Client::open(redis_url)?;
    let mut connection = client.get_connection_manager().await?;
    let reply = redis::cmd("XRANGE")
        .arg(&stream_key)
        .arg("-")
        .arg("+")
        .query_async::<StreamRangeReply>(&mut connection)
        .await?;
    let entries = reply
        .ids
        .iter()
        .map(stream_id_to_entry)
        .collect::<Result<Vec<_>>>()?;

    assert_eq!(
        reply
            .ids
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<Vec<_>>(),
        vec!["1-1", "1-2", "2-1", "2-2", "2-3"]
    );
    assert_eq!(boundary.exclusive_entry_id(), "2-1");
    assert_eq!(
        entries
            .iter()
            .map(|entry| (entry.kind, entry.backend_scope.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (BroadcasterMessageKind::Progress, "native,rfq"),
            (BroadcasterMessageKind::Update, "native"),
            (BroadcasterMessageKind::Progress, "native,rfq"),
            (BroadcasterMessageKind::Update, "native"),
            (BroadcasterMessageKind::Update, "rfq"),
        ]
    );
    assert!(entries[3].state_version > entries[1].state_version);
    Ok(())
}

#[tokio::test]
async fn fatal_self_fence_closes_deployment_admission() {
    let publisher =
        BroadcasterRedisPublisher::new(publisher_config(), Arc::new(FakeRedisWriter::default()));
    publisher.admit_deployment();
    publisher.self_fence("planned fatal failure").await;

    let admission = publisher.deployment_admission_snapshot();
    assert!(!admission.admitted);
    assert_eq!(admission.phase, BroadcasterDeploymentPhase::Fatal);
    assert_eq!(
        admission.last_error.as_deref(),
        Some("planned fatal failure")
    );
}

#[tokio::test]
async fn promotion_allocates_generation_and_appends_marker_before_live_updates() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));

    let boundary = publisher
        .promote(
            base_heads([BroadcasterBackend::Native]),
            "active_writer_promoted",
        )
        .await?;

    assert_eq!(boundary.stream_key, "dsolver:broadcaster:test:events");
    assert_eq!(boundary.stream_id, "chain-1-stream-1");
    assert_eq!(boundary.snapshot_id, "chain-1-snapshot-1");
    assert_eq!(boundary.generation, 1);
    assert_eq!(boundary.exclusive_message_seq, 1);
    assert_eq!(boundary.exclusive_entry_id(), "1-1");

    let marker = writer
        .appends()
        .await
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("promotion should append a generation marker"))?;
    assert_eq!(marker.entry.stream_id, "chain-1-stream-1");
    assert_eq!(marker.entry.message_seq, 1);
    assert_eq!(marker.entry.kind, BroadcasterMessageKind::Progress);
    let _progress = progress_payload(&marker.entry)?;

    let update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(update))
        .await?;

    let appends = writer.appends().await;
    assert_eq!(
        appends
            .iter()
            .map(|append| append.entry.message_seq)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(publisher.status_snapshot().await.mode, "active");
    Ok(())
}

#[tokio::test]
async fn promotion_after_existing_tail_forces_new_generation_boundary() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let old = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    let new = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    old.promote(base_heads([BroadcasterBackend::Native]), "old_active")
        .await?;
    let old_update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    old.publish_accepted_payload(BroadcasterPayload::Update(old_update))
        .await?;
    let boundary = new
        .promote(base_heads([BroadcasterBackend::Native]), "new_active")
        .await?;

    assert_eq!(boundary.generation, 2);
    assert_eq!(boundary.exclusive_entry_id(), "2-1");
    let marker = writer
        .appends()
        .await
        .last()
        .cloned()
        .ok_or_else(|| anyhow!("new promotion should append a marker"))?;
    assert_eq!(redis_entry_id(&marker.entry)?, "2-1");
    assert_eq!(progress_payload(&marker.entry)?.reason, "new_active");
    Ok(())
}

#[tokio::test]
async fn second_promoted_writer_fences_old_writer_before_append() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let old = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    let new = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    old.promote(base_heads([BroadcasterBackend::Native]), "old_active")
        .await?;

    new.promote(base_heads([BroadcasterBackend::Native]), "new_active")
        .await?;
    let stale_update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    let Err(error) = old
        .publish_accepted_payload(BroadcasterPayload::Update(stale_update))
        .await
    else {
        return Err(anyhow!("old writer append should be fenced"));
    };

    assert!(
        format!("{error:#}").contains("stale Redis broadcaster writer"),
        "unexpected stale-writer error: {error:#}"
    );
    assert_eq!(old.status_snapshot().await.mode, "retired");
    assert_eq!(
        writer
            .appends()
            .await
            .iter()
            .map(|append| (append.entry.stream_id.clone(), append.entry.message_seq))
            .collect::<Vec<_>>(),
        vec![
            ("chain-1-stream-1".to_string(), 1),
            ("chain-1-stream-2".to_string(), 1)
        ],
        "the fenced writer must not append its stale update"
    );

    let active_update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 12, "native-3"))
        .await?;
    new.publish_accepted_payload(BroadcasterPayload::Update(active_update))
        .await?;
    let appends = writer.appends().await;
    assert_eq!(
        appends.last().map(|append| append.entry.message_seq),
        Some(2)
    );
    assert_eq!(
        appends.last().map(|append| append.entry.stream_id.as_str()),
        Some("chain-1-stream-2")
    );
    Ok(())
}

#[tokio::test]
async fn stale_writer_cannot_append_or_commit_recovery_transaction() -> Result<()> {
    let writer = FakeRedisWriter::default();
    let old = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    let new = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    old.promote(base_heads([BroadcasterBackend::Native]), "old_active")
        .await?;
    new.promote(base_heads([BroadcasterBackend::Native]), "new_active")
        .await?;

    let cancelled = Arc::new(AtomicBool::new(false));
    let recovery_id = old
        .begin_recovery(&[BroadcasterBackend::Native], Arc::clone(&cancelled))
        .await;
    let transaction = super::BroadcasterRecoveryTransaction {
        recovery_id,
        backends: vec![BroadcasterBackend::Native],
        replacement_json: "replacement".to_string(),
        buffer_a: Vec::new(),
    };
    let Err(error) = old.publish_recovery(transaction, cancelled.as_ref()).await else {
        return Err(anyhow!("stale writer recovery must be fenced"));
    };

    assert!(format!("{error:#}").contains("stale Redis broadcaster writer"));
    assert_eq!(old.status_snapshot().await.mode, "retired");
    assert_eq!(
        writer.appends().await.len(),
        2,
        "the stale transaction must append neither RecoveryStart nor RecoveryCommit"
    );
    Ok(())
}

#[tokio::test]
async fn writer_recovery_cannot_displace_a_newer_writer() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let old = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    let new = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    old.promote(base_heads([BroadcasterBackend::Native]), "old_active")
        .await?;
    writer.fail_next_appends(100).await;
    let failed_update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    assert!(old
        .publish_accepted_payload(BroadcasterPayload::Update(failed_update))
        .await
        .is_err());
    assert_eq!(old.status_snapshot().await.mode, "unhealthy");

    writer.fail_next_appends(0).await;
    new.promote(base_heads([BroadcasterBackend::Native]), "new_active")
        .await?;
    let Err(error) = old
        .recover_writer(
            base_heads([BroadcasterBackend::Native]),
            "old_writer_recovery",
        )
        .await
    else {
        return Err(anyhow!("old writer recovery should be fenced"));
    };
    assert!(format!("{error:#}").contains("stale Redis broadcaster writer"));
    assert_eq!(old.status_snapshot().await.mode, "retired");
    assert_eq!(new.status_snapshot().await.mode, "active");
    Ok(())
}

#[tokio::test]
async fn lease_loss_recovers_in_a_new_generation_without_reusing_the_fence() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(
            base_heads([BroadcasterBackend::Native]),
            "active_writer_promoted",
        )
        .await?;
    writer.expire_active_writer().await;

    let failed_update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    let Err(error) = publisher
        .publish_accepted_payload(BroadcasterPayload::Update(failed_update))
        .await
    else {
        return Err(anyhow!(
            "lost writer lease should fence the active publisher"
        ));
    };

    assert!(format!("{error:#}").contains("missing Redis broadcaster writer"));
    assert_eq!(publisher.status_snapshot().await.mode, "unhealthy");

    let boundary = publisher
        .recover_writer(
            base_heads([BroadcasterBackend::Native]),
            "active_writer_recovered",
        )
        .await?;
    assert_eq!(boundary.generation, 2);
    assert_eq!(boundary.stream_id, "chain-1-stream-2");
    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(
            raw_cache
                .apply_update(&update(BroadcasterBackend::Native, 12, "native-3"))
                .await?,
        ))
        .await?;

    let appends = writer.appends().await;
    assert_eq!(appends.len(), 3);
    assert_eq!(appends[1].entry.stream_id, "chain-1-stream-2");
    assert_eq!(appends[1].entry.message_seq, 1);
    assert_eq!(appends[2].entry.message_seq, 2);
    Ok(())
}

#[tokio::test]
async fn empty_redis_recovery_advances_past_the_lost_local_generation() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(
            base_heads([BroadcasterBackend::Native]),
            "active_writer_promoted",
        )
        .await?;
    writer.lose_all_data().await;

    let update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    let Err(error) = publisher
        .publish_accepted_payload(BroadcasterPayload::Update(update))
        .await
    else {
        return Err(anyhow!("empty Redis should reject the old writer fence"));
    };
    assert!(format!("{error:#}").contains("missing Redis broadcaster writer"));

    let boundary = publisher
        .recover_writer(
            base_heads([BroadcasterBackend::Native]),
            "active_writer_recovered",
        )
        .await?;
    assert_eq!(boundary.generation, 2);
    assert_eq!(boundary.exclusive_entry_id(), "2-1");
    assert_eq!(
        writer
            .appends()
            .await
            .iter()
            .map(|append| redis_entry_id(&append.entry))
            .collect::<Result<Vec<_>>>()?,
        vec!["2-1"]
    );
    Ok(())
}

#[tokio::test]
async fn configured_writer_lease_ttl_is_used_for_all_fenced_writer_commands() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let mut config = publisher_config();
    config.writer_lease_ttl = Duration::from_secs(180);
    let publisher = BroadcasterRedisPublisher::new(config, Arc::new(writer.clone()));

    publisher
        .promote(
            base_heads([BroadcasterBackend::Native]),
            "active_writer_promoted",
        )
        .await?;

    let update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(update))
        .await?;
    publisher.renew_lease().await?;

    assert_eq!(
        writer.lease_ttls().await,
        vec![
            Duration::from_secs(180),
            Duration::from_secs(180),
            Duration::from_secs(180)
        ]
    );
    Ok(())
}

#[tokio::test]
async fn recovery_transaction_appends_start_chunks_buffer_a_and_commit_contiguously() -> Result<()>
{
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(base_heads([BroadcasterBackend::Native]), "test_active")
        .await?;
    let cancelled = Arc::new(AtomicBool::new(false));
    let recovery_id = publisher
        .begin_recovery(&[BroadcasterBackend::Native], Arc::clone(&cancelled))
        .await;
    let transaction = super::BroadcasterRecoveryTransaction {
        recovery_id,
        backends: vec![BroadcasterBackend::Native],
        replacement_json: "x".repeat(3_600_000),
        buffer_a: vec![BroadcasterUpdateMessage::from_tycho_update(
            &update(BroadcasterBackend::Native, 11, "native-buffer-a"),
            &HashMap::new(),
        )?],
    };

    let publication = publisher
        .publish_recovery(transaction, cancelled.as_ref())
        .await?;
    let appends = writer.appends().await;
    let recovery_entries = &appends[1..];

    assert_eq!(publication.chunk_count, 2);
    assert_eq!(publication.buffer_a_count, 1);
    assert_eq!(
        recovery_entries
            .iter()
            .map(|append| append.entry.kind)
            .collect::<Vec<_>>(),
        vec![
            BroadcasterMessageKind::RecoveryStart,
            BroadcasterMessageKind::RecoveryChunk,
            BroadcasterMessageKind::RecoveryChunk,
            BroadcasterMessageKind::RecoveryCatchUp,
            BroadcasterMessageKind::RecoveryCommit,
        ]
    );
    assert_eq!(
        recovery_entries
            .iter()
            .map(|append| append.entry.message_seq)
            .collect::<Vec<_>>(),
        vec![2, 3, 4, 5, 6]
    );
    assert_eq!(publication.commit_entry_id, "1-6");
    Ok(())
}

#[tokio::test]
async fn recovery_versions_advance_past_intervening_live_traffic() -> Result<()> {
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(base_heads([BroadcasterBackend::Native]), "test_active")
        .await?;

    let first_cancelled = Arc::new(AtomicBool::new(false));
    let first_recovery_id = publisher
        .begin_recovery(&[BroadcasterBackend::Native], Arc::clone(&first_cancelled))
        .await;
    let first = publisher
        .publish_recovery(
            super::BroadcasterRecoveryTransaction {
                recovery_id: first_recovery_id,
                backends: vec![BroadcasterBackend::Native],
                replacement_json: "first replacement".to_string(),
                buffer_a: Vec::new(),
            },
            first_cancelled.as_ref(),
        )
        .await?;

    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let live_update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(live_update))
        .await?;

    let second_cancelled = Arc::new(AtomicBool::new(false));
    let second_recovery_id = publisher
        .begin_recovery(&[BroadcasterBackend::Native], Arc::clone(&second_cancelled))
        .await;
    let second = publisher
        .publish_recovery(
            super::BroadcasterRecoveryTransaction {
                recovery_id: second_recovery_id,
                backends: vec![BroadcasterBackend::Native],
                replacement_json: "second replacement".to_string(),
                buffer_a: Vec::new(),
            },
            second_cancelled.as_ref(),
        )
        .await?;

    assert_eq!(first.target_state_version, (1 << 48) + 1);
    assert_eq!(second.target_state_version, (1 << 48) + 3);
    let appends = writer.appends().await;
    let live_entry = appends
        .iter()
        .find(|append| append.entry.kind == BroadcasterMessageKind::Update)
        .ok_or_else(|| anyhow!("intervening live update should be present"))?;
    assert_eq!(live_entry.entry.state_version, (1 << 48) + 2);
    assert_eq!(
        publisher.replay_boundary().await?.state_version,
        second.target_state_version
    );
    Ok(())
}

#[tokio::test]
async fn partial_recovery_queues_sibling_until_superseding_commit() -> Result<()> {
    let writer = FakeRedisWriter::default();
    let mut config = publisher_config();
    config.append_retry_window = Duration::from_millis(20);
    let publisher = BroadcasterRedisPublisher::new(config, Arc::new(writer.clone()));
    publisher
        .promote(
            base_heads([BroadcasterBackend::Native, BroadcasterBackend::Rfq]),
            "test_active",
        )
        .await?;

    let first_cancelled = Arc::new(AtomicBool::new(false));
    let first_recovery_id = publisher
        .begin_recovery(&[BroadcasterBackend::Native], Arc::clone(&first_cancelled))
        .await;
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let superseded_native_update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(superseded_native_update))
        .await?;
    let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
    let sibling_update = rfq_cache
        .apply_update(&update(BroadcasterBackend::Rfq, 21, "rfq-2"))
        .await?;
    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(sibling_update))
        .await?;
    writer.fail_message_seq(3).await;

    let first_error = publisher
        .publish_recovery(
            super::BroadcasterRecoveryTransaction {
                recovery_id: first_recovery_id,
                backends: vec![BroadcasterBackend::Native],
                replacement_json: "first replacement".to_string(),
                buffer_a: Vec::new(),
            },
            first_cancelled.as_ref(),
        )
        .await
        .err()
        .ok_or_else(|| anyhow!("the first recovery should stop after RecoveryStart"))?;
    assert!(format!("{first_error:#}").contains("planned message_seq failure"));

    writer.clear_message_seq_failure().await;
    publisher.renew_lease().await?;
    let second_cancelled = Arc::new(AtomicBool::new(false));
    let second_recovery_id = publisher
        .begin_recovery(&[BroadcasterBackend::Native], Arc::clone(&second_cancelled))
        .await;
    publisher
        .publish_recovery(
            super::BroadcasterRecoveryTransaction {
                recovery_id: second_recovery_id,
                backends: vec![BroadcasterBackend::Native],
                replacement_json: "second replacement".to_string(),
                buffer_a: Vec::new(),
            },
            second_cancelled.as_ref(),
        )
        .await?;

    let appends = writer.appends().await;
    let kinds = appends
        .iter()
        .map(|append| append.entry.kind)
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        vec![
            BroadcasterMessageKind::Progress,
            BroadcasterMessageKind::RecoveryStart,
            BroadcasterMessageKind::RecoveryStart,
            BroadcasterMessageKind::RecoveryChunk,
            BroadcasterMessageKind::RecoveryCommit,
            BroadcasterMessageKind::Update,
        ]
    );
    assert_eq!(
        appends
            .last()
            .map(|append| append.entry.backend_scope.as_str()),
        Some("rfq")
    );
    assert_eq!(publisher.pending_recovery_payload_stats().await.0, 0);
    Ok(())
}

#[tokio::test]
async fn superseding_recovery_waits_for_commit_and_drain_without_deadlock() -> Result<()> {
    let writer = FakeRedisWriter::default();
    let publisher = Arc::new(BroadcasterRedisPublisher::new(
        publisher_config(),
        Arc::new(writer.clone()),
    ));
    publisher
        .promote(base_heads([BroadcasterBackend::Native]), "test_active")
        .await?;

    let first_cancelled = Arc::new(AtomicBool::new(false));
    let first_recovery_id = publisher
        .begin_recovery(&[BroadcasterBackend::Native], Arc::clone(&first_cancelled))
        .await;
    writer.block_message_seq(4).await;
    let first_task = tokio::spawn({
        let publisher = Arc::clone(&publisher);
        let first_cancelled = Arc::clone(&first_cancelled);
        async move {
            publisher
                .publish_recovery(
                    super::BroadcasterRecoveryTransaction {
                        recovery_id: first_recovery_id,
                        backends: vec![BroadcasterBackend::Native],
                        replacement_json: "first replacement".to_string(),
                        buffer_a: Vec::new(),
                    },
                    first_cancelled.as_ref(),
                )
                .await
        }
    });
    writer.wait_for_blocked_append().await;

    // The commit command is already in flight, so cancellation must let it reconcile.
    first_cancelled.store(true, Ordering::Release);
    let second_cancelled = Arc::new(AtomicBool::new(false));
    let second_begin = tokio::spawn({
        let publisher = Arc::clone(&publisher);
        let second_cancelled = Arc::clone(&second_cancelled);
        async move {
            publisher
                .begin_recovery(&[BroadcasterBackend::Native], Arc::clone(&second_cancelled))
                .await
        }
    });
    tokio::task::yield_now().await;
    assert!(
        !second_begin.is_finished(),
        "supersession must wait until the in-flight commit and drain finish"
    );

    writer.release_blocked_append();
    let first_join = timeout(Duration::from_secs(1), first_task)
        .await
        .map_err(|_| anyhow!("first recovery deadlocked during commit"))?;
    first_join.map_err(|error| anyhow!("first recovery task failed: {error}"))??;
    let second_join = timeout(Duration::from_secs(1), second_begin)
        .await
        .map_err(|_| anyhow!("superseding recovery reservation deadlocked"))?;
    let second_recovery_id =
        second_join.map_err(|error| anyhow!("superseding recovery task failed: {error}"))?;
    publisher
        .publish_recovery(
            super::BroadcasterRecoveryTransaction {
                recovery_id: second_recovery_id,
                backends: vec![BroadcasterBackend::Native],
                replacement_json: "second replacement".to_string(),
                buffer_a: Vec::new(),
            },
            second_cancelled.as_ref(),
        )
        .await?;

    let kinds = writer
        .appends()
        .await
        .into_iter()
        .map(|append| append.entry.kind)
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        vec![
            BroadcasterMessageKind::Progress,
            BroadcasterMessageKind::RecoveryStart,
            BroadcasterMessageKind::RecoveryChunk,
            BroadcasterMessageKind::RecoveryCommit,
            BroadcasterMessageKind::RecoveryStart,
            BroadcasterMessageKind::RecoveryChunk,
            BroadcasterMessageKind::RecoveryCommit,
        ]
    );
    Ok(())
}

#[tokio::test]
async fn redis_verification_pause_notifies_feeds_and_resumes_after_recovery() -> Result<()> {
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(base_heads([BroadcasterBackend::Native]), "test_active")
        .await?;
    writer.fail_next_renews(2).await;
    let pause_epoch = publisher.feed_pause_epoch();

    let ((), pause_result) = tokio::join!(
        publisher.wait_for_feed_pause_after(pause_epoch),
        publisher.pause_feeds_until_writer_verified(),
    );
    pause_result?;

    assert_eq!(publisher.feed_pause_epoch(), pause_epoch + 1);
    assert!(!publisher.feeds_are_paused());
    assert_eq!(
        publisher.mode().await,
        BroadcasterRedisPublisherMode::Active
    );
    Ok(())
}

#[tokio::test]
async fn configured_native_block_limit_cancels_publisher_recovery_queue() -> Result<()> {
    let writer = FakeRedisWriter::default();
    let mut config = publisher_config();
    config.recovery_max_buffered_native_blocks = 3;
    let publisher = BroadcasterRedisPublisher::new(config, Arc::new(writer));
    let cancelled = Arc::new(AtomicBool::new(false));
    publisher
        .begin_recovery(&[BroadcasterBackend::Rfq], Arc::clone(&cancelled))
        .await;

    for block_number in 1..=3 {
        let message = BroadcasterUpdateMessage::from_tycho_update(
            &update(
                BroadcasterBackend::Native,
                block_number,
                &format!("native-{block_number}"),
            ),
            &HashMap::new(),
        )?;
        publisher
            .publish_accepted_payload(BroadcasterPayload::Update(message))
            .await?;
    }

    assert!(cancelled.load(std::sync::atomic::Ordering::Acquire));
    let (current, maximum, oldest_age_ms) = publisher.pending_recovery_payload_stats().await;
    assert_eq!((current, maximum), (3, 3));
    assert!(oldest_age_ms.is_some());
    Ok(())
}

#[tokio::test]
async fn gate_open_heartbeats_are_not_queued() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer));
    let cancelled = Arc::new(AtomicBool::new(false));
    publisher
        .begin_recovery(&[BroadcasterBackend::Native], cancelled)
        .await;
    let heartbeat = raw_cache
        .heartbeat()
        .await?
        .ok_or_else(|| anyhow!("ready cache should produce a heartbeat"))?;

    publisher.publish_accepted_payload(heartbeat).await?;

    assert_eq!(
        publisher.pending_recovery_payload_stats().await,
        (0, 0, None)
    );
    Ok(())
}

#[tokio::test]
async fn recovery_queue_rejects_overflow() -> Result<()> {
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer));
    let cancelled = Arc::new(AtomicBool::new(false));
    publisher
        .begin_recovery(&[BroadcasterBackend::Rfq], cancelled)
        .await;
    let update = BroadcasterUpdateMessage::from_tycho_update(
        &update(BroadcasterBackend::Rfq, 10, "rfq-1"),
        &HashMap::new(),
    )?;

    for _ in 0..super::MAX_PENDING_RECOVERY_PAYLOADS {
        publisher
            .publish_accepted_payload(BroadcasterPayload::Update(update.clone()))
            .await?;
    }
    let Err(error) = publisher
        .publish_accepted_payload(BroadcasterPayload::Update(update))
        .await
    else {
        return Err(anyhow!("recovery queue overflow should be rejected"));
    };

    assert!(format!("{error:#}").contains("recovery payload queue is full"));
    assert_eq!(
        publisher.pending_recovery_payload_stats().await.0,
        super::MAX_PENDING_RECOVERY_PAYLOADS
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn identical_recovery_duplicate_is_accepted_after_lost_append_reply() -> Result<()> {
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(base_heads([BroadcasterBackend::Native]), "test_active")
        .await?;
    writer.lose_next_append_replies(1).await;

    let cancelled = Arc::new(AtomicBool::new(false));
    let recovery_id = publisher
        .begin_recovery(&[BroadcasterBackend::Native], Arc::clone(&cancelled))
        .await;
    publisher
        .publish_recovery(
            super::BroadcasterRecoveryTransaction {
                recovery_id,
                backends: vec![BroadcasterBackend::Native],
                replacement_json: "replacement".to_string(),
                buffer_a: Vec::new(),
            },
            cancelled.as_ref(),
        )
        .await?;

    let appends = writer.appends().await;
    assert_eq!(
        appends
            .iter()
            .filter(|append| append.entry.kind == BroadcasterMessageKind::RecoveryStart)
            .count(),
        1,
        "retrying the same deterministic entry must adopt the existing fields"
    );
    assert_eq!(writer.append_attempt_count().await, appends.len());
    assert_eq!(
        appends.last().map(|append| append.entry.kind),
        Some(BroadcasterMessageKind::RecoveryCommit)
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn conflicting_recovery_duplicate_is_rejected() -> Result<()> {
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(base_heads([BroadcasterBackend::Native]), "test_active")
        .await?;
    writer.conflict_next_append_replies(1).await;

    let cancelled = Arc::new(AtomicBool::new(false));
    let recovery_id = publisher
        .begin_recovery(&[BroadcasterBackend::Native], Arc::clone(&cancelled))
        .await;
    let Err(error) = publisher
        .publish_recovery(
            super::BroadcasterRecoveryTransaction {
                recovery_id,
                backends: vec![BroadcasterBackend::Native],
                replacement_json: "replacement".to_string(),
                buffer_a: Vec::new(),
            },
            cancelled.as_ref(),
        )
        .await
    else {
        return Err(anyhow!(
            "conflicting deterministic recovery entry must fail"
        ));
    };

    assert!(format!("{error:#}").contains("conflicting Redis broadcaster entry"));
    assert!(writer
        .appends()
        .await
        .iter()
        .all(|append| append.entry.kind != BroadcasterMessageKind::RecoveryCommit));
    Ok(())
}

#[test]
fn encoded_redis_entry_limits_accept_the_boundary_and_reject_the_next_payload() -> Result<()> {
    for (kind, limit) in [
        (
            BroadcasterMessageKind::RecoveryChunk,
            super::RECOVERY_REDIS_ENTRY_MAX_BYTES,
        ),
        (
            BroadcasterMessageKind::Update,
            super::LIVE_REDIS_ENTRY_MAX_BYTES,
        ),
    ] {
        let config = publisher_config();
        let mut entry = BroadcasterRedisStreamEntry {
            schema_version: "1".to_string(),
            chain_id: Chain::Ethereum.id(),
            stream_id: "chain-1-stream-1".to_string(),
            message_seq: 2,
            state_version: 1,
            kind,
            snapshot_id: Some("chain-1-snapshot-1".to_string()),
            backend_scope: "native".to_string(),
            block_number: Some(1),
            observed_timestamp_ms: None,
            published_at_ms: 1,
            payload_json: String::new(),
        };
        let mut low = 0usize;
        let mut high = limit;
        while low < high {
            let candidate = low + (high - low).div_ceil(2);
            entry.payload_json = "x".repeat(candidate);
            if super::redis_entry_encoded_size(&config, &entry)? <= limit {
                low = candidate;
            } else {
                high = candidate - 1;
            }
        }
        entry.payload_json = "x".repeat(low);
        let accepted = super::ensure_redis_entry_size(&config, &entry, limit)?;
        entry.payload_json.push('x');
        let rejected = super::redis_entry_encoded_size(&config, &entry)?;

        assert!(accepted <= limit);
        assert!(rejected > limit);
        assert!(super::ensure_redis_entry_size(&config, &entry, limit).is_err());
    }
    Ok(())
}

#[tokio::test]
async fn stale_active_writer_cannot_return_replay_boundary_after_new_promotion() -> Result<()> {
    let writer = FakeRedisWriter::default();
    let old = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    let new = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    old.promote(base_heads([BroadcasterBackend::Native]), "old_active")
        .await?;
    new.promote(base_heads([BroadcasterBackend::Native]), "new_active")
        .await?;

    let Err(error) = old.replay_boundary().await else {
        return Err(anyhow!("stale old writer must not serve a replay boundary"));
    };

    assert!(format!("{error:#}").contains("stale Redis broadcaster writer"));
    assert_eq!(old.status_snapshot().await.mode, "retired");
    assert_eq!(
        writer.appends().await.len(),
        2,
        "replay-boundary fencing must not append Redis entries"
    );
    Ok(())
}

#[test]
fn promotion_lua_stores_gsub_result_before_table_insert() {
    assert!(
        super::PROMOTE_WRITER_SCRIPT.contains("local value = string.gsub"),
        "Lua string.gsub returns value and replacement count; promotion must store only the value"
    );
    assert!(
        !super::PROMOTE_WRITER_SCRIPT.contains("table.insert(command, string.gsub"),
        "passing string.gsub directly to table.insert expands both Lua return values"
    );
}

#[test]
fn publisher_modes_serialize_as_stable_lowercase_strings() {
    assert_eq!(BroadcasterRedisPublisherMode::Passive.as_str(), "passive");
    assert_eq!(BroadcasterRedisPublisherMode::Active.as_str(), "active");
    assert_eq!(BroadcasterRedisPublisherMode::Retired.as_str(), "retired");
    assert_eq!(
        BroadcasterRedisPublisherMode::Unhealthy.as_str(),
        "unhealthy"
    );
}

#[test]
fn writer_lease_ttl_keeps_a_margin_over_the_heartbeat_interval() {
    assert_eq!(
        writer_lease_ttl_for_heartbeat_interval(Duration::from_secs(5)),
        Duration::from_secs(30)
    );
    assert_eq!(
        writer_lease_ttl_for_heartbeat_interval(Duration::from_secs(60)),
        Duration::from_secs(180)
    );
}

#[tokio::test]
async fn replay_boundary_starts_before_first_delta_without_publishing_snapshot() -> Result<()> {
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(
            base_heads([BroadcasterBackend::Native]),
            "active_writer_promoted",
        )
        .await?;

    let boundary = publisher.replay_boundary().await?;

    assert_eq!(boundary.stream_key, "dsolver:broadcaster:test:events");
    assert_eq!(boundary.stream_id, "chain-1-stream-1");
    assert_eq!(boundary.snapshot_id, "chain-1-snapshot-1");
    assert_eq!(boundary.generation, 1);
    assert_eq!(boundary.exclusive_message_seq, 1);
    assert_eq!(boundary.exclusive_entry_id(), "1-1");
    assert!(
        writer
            .appends()
            .await
            .iter()
            .all(|append| append.entry.kind == BroadcasterMessageKind::Progress),
        "HTTP snapshots are the bootstrap source; Redis must not receive full snapshot entries"
    );
    Ok(())
}

#[tokio::test]
async fn publishes_live_updates_and_heartbeats_as_deltas_after_replay_boundary() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(
            base_heads([BroadcasterBackend::Native, BroadcasterBackend::Rfq]),
            "active_writer_promoted",
        )
        .await?;
    let boundary = publisher.replay_boundary().await?;

    let native_update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(native_update))
        .await?;
    let rfq_update = rfq_cache
        .apply_update(&update(BroadcasterBackend::Rfq, 21, "rfq-2"))
        .await?;
    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(rfq_update))
        .await?;
    let heartbeat = raw_cache
        .heartbeat()
        .await?
        .ok_or_else(|| anyhow!("ready raw cache should produce heartbeat"))?;
    publisher.publish_accepted_payload(heartbeat).await?;

    let appends = writer.appends().await;
    assert_eq!(
        appends
            .iter()
            .map(|append| append.entry.message_seq)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert_eq!(appends[0].entry.kind.as_str(), "progress");
    assert_eq!(appends[1].entry.kind.as_str(), "update");
    assert_eq!(appends[1].entry.backend_scope, "native");
    assert_eq!(appends[1].entry.block_number, Some(11));
    assert_eq!(appends[2].entry.kind.as_str(), "update");
    assert_eq!(appends[2].entry.backend_scope, "rfq");
    assert_eq!(appends[2].entry.block_number, None);
    assert_eq!(appends[3].entry.kind.as_str(), "heartbeat");
    assert_eq!(appends[3].entry.backend_scope, "native");
    assert_eq!(
        appends
            .iter()
            .map(|append| append.entry.state_version)
            .collect::<Vec<_>>(),
        vec![0, (1 << 48) + 1, (1 << 48) + 2, (1 << 48) + 2]
    );
    assert_eq!(appends[3].entry.stream_id, boundary.stream_id);
    assert_eq!(
        appends[3].entry.snapshot_id.as_deref(),
        Some(boundary.snapshot_id.as_str())
    );
    assert_eq!(
        publisher.replay_boundary().await?.state_version,
        (1 << 48) + 2
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn append_retry_success_preserves_message_sequence_order() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    writer.fail_next_appends(1).await;
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(
            base_heads([BroadcasterBackend::Native]),
            "active_writer_promoted",
        )
        .await?;

    let update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(update))
        .await?;

    let appends = writer.appends().await;
    assert_eq!(writer.append_attempt_count().await, 2);
    assert_eq!(
        appends
            .iter()
            .map(|append| append.entry.message_seq)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(appends[1].entry.kind.as_str(), "update");
    Ok(())
}

#[tokio::test]
async fn readiness_triggering_update_is_published_as_a_delta() -> Result<()> {
    let rfq_cache =
        BroadcasterSnapshotCache::new(Chain::Ethereum.id(), vec![BroadcasterBackend::Rfq]);
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(
            base_heads([BroadcasterBackend::Rfq]),
            "active_writer_promoted",
        )
        .await?;
    let rfq_update = rfq_cache
        .apply_update(&update(BroadcasterBackend::Rfq, 20, "rfq-1"))
        .await?;

    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(rfq_update))
        .await?;

    let appends = writer.appends().await;
    assert_eq!(appends.len(), 2);
    assert_eq!(appends[1].entry.kind.as_str(), "update");
    assert_eq!(appends[1].entry.backend_scope, "rfq");
    Ok(())
}

#[tokio::test]
async fn retry_exhaustion_marks_publisher_unhealthy() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(
            base_heads([BroadcasterBackend::Native]),
            "active_writer_promoted",
        )
        .await?;
    writer.fail_next_appends(100).await;

    let failed_update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    let Err(error) = publisher
        .publish_accepted_payload(BroadcasterPayload::Update(failed_update))
        .await
    else {
        return Err(anyhow!(
            "retry exhaustion should surface the failed publication"
        ));
    };
    assert!(error
        .to_string()
        .contains("failed to append Redis broadcaster"));

    let failed_status = publisher.status_snapshot().await;
    assert!(!failed_status.healthy);
    assert_eq!(failed_status.retry_exhaustion_count, 1);
    assert_eq!(failed_status.stream_id, "chain-1-stream-1");
    assert!(failed_status.replay_boundary.is_none());
    assert!(failed_status
        .last_error
        .as_deref()
        .unwrap_or("")
        .contains("planned append failure"));

    Ok(())
}

#[tokio::test]
async fn stalled_append_exhausts_retry_window() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    writer.delay_appends(Duration::from_millis(50)).await;
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(
            base_heads([BroadcasterBackend::Native]),
            "active_writer_promoted",
        )
        .await?;

    let update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    let Err(error) = publisher
        .publish_accepted_payload(BroadcasterPayload::Update(update))
        .await
    else {
        return Err(anyhow!("stalled append should exhaust the retry window"));
    };
    assert!(format!("{error:#}").contains("retry window exhausted"));

    let failed_status = publisher.status_snapshot().await;
    assert!(!failed_status.healthy);
    assert_eq!(failed_status.append_failure_count, 1);
    assert_eq!(failed_status.retry_exhaustion_count, 1);
    assert!(failed_status.replay_boundary.is_none());
    assert_eq!(writer.appends().await.len(), 1);
    Ok(())
}

#[tokio::test]
async fn lost_append_that_landed_is_resynced_on_lease_renewal() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(
            base_heads([BroadcasterBackend::Native]),
            "active_writer_promoted",
        )
        .await?;
    writer.stall_after_record_message_seq(2).await;

    let update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    let Err(error) = publisher
        .publish_accepted_payload(BroadcasterPayload::Update(update))
        .await
    else {
        return Err(anyhow!("lost append reply should exhaust the retry window"));
    };
    assert!(format!("{error:#}").contains("retry window exhausted"));
    assert_eq!(
        publisher.mode().await,
        BroadcasterRedisPublisherMode::Unhealthy
    );
    assert_eq!(
        writer
            .appends()
            .await
            .iter()
            .map(|append| append.entry.message_seq)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    publisher.renew_lease().await?;
    assert_eq!(
        publisher.mode().await,
        BroadcasterRedisPublisherMode::Active
    );
    let heartbeat = raw_cache
        .heartbeat()
        .await?
        .ok_or_else(|| anyhow!("ready cache should produce a heartbeat"))?;
    publisher.publish_accepted_payload(heartbeat).await?;

    let appends = writer.appends().await;
    assert_eq!(
        appends
            .iter()
            .map(|append| append.entry.message_seq)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        publisher.status_snapshot().await.latest_entry_id.as_deref(),
        Some("1-3")
    );
    Ok(())
}

#[tokio::test]
async fn lease_renewal_without_landed_tail_keeps_sequence() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    writer.delay_appends(Duration::from_millis(50)).await;
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(
            base_heads([BroadcasterBackend::Native]),
            "active_writer_promoted",
        )
        .await?;

    let update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    let Err(error) = publisher
        .publish_accepted_payload(BroadcasterPayload::Update(update))
        .await
    else {
        return Err(anyhow!("stalled append should exhaust the retry window"));
    };
    assert!(format!("{error:#}").contains("retry window exhausted"));

    writer.delay_appends(Duration::ZERO).await;
    publisher.renew_lease().await?;
    let heartbeat = raw_cache
        .heartbeat()
        .await?
        .ok_or_else(|| anyhow!("ready cache should produce a heartbeat"))?;
    publisher.publish_accepted_payload(heartbeat).await?;

    assert_eq!(
        writer
            .appends()
            .await
            .iter()
            .map(|append| append.entry.message_seq)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    Ok(())
}

#[tokio::test]
async fn tail_resync_failure_keeps_publisher_unhealthy() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(
            base_heads([BroadcasterBackend::Native]),
            "active_writer_promoted",
        )
        .await?;
    writer.fail_next_appends(100).await;

    let update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    assert!(publisher
        .publish_accepted_payload(BroadcasterPayload::Update(update))
        .await
        .is_err());
    writer.fail_next_inspects(1).await;

    let Err(error) = publisher.renew_lease().await else {
        return Err(anyhow!("tail inspection failure should block reactivation"));
    };
    assert!(format!("{error:#}")
        .contains("Redis stream tail resync failed before reactivating the publisher"));
    assert_eq!(
        publisher.mode().await,
        BroadcasterRedisPublisherMode::Unhealthy
    );
    Ok(())
}

#[tokio::test]
async fn publish_rejects_message_sequence_overflow() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    publisher
        .promote(
            base_heads([BroadcasterBackend::Native]),
            "active_writer_promoted",
        )
        .await?;
    publisher.inner.lock().await.next_message_seq = u64::MAX;

    let update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    let Err(error) = publisher
        .publish_accepted_payload(BroadcasterPayload::Update(update))
        .await
    else {
        return Err(anyhow!("message sequence overflow should fail"));
    };

    assert!(format!("{error:#}").contains("message_seq overflow"));
    let failed_status = publisher.status_snapshot().await;
    assert!(!failed_status.healthy);
    assert_eq!(failed_status.retry_exhaustion_count, 0);
    assert_eq!(writer.appends().await.len(), 1);
    Ok(())
}

#[test]
fn redis_entry_id_uses_generation_and_message_sequence() -> Result<()> {
    let entry = BroadcasterRedisStreamEntry {
        schema_version: "1".to_string(),
        chain_id: Chain::Ethereum.id(),
        stream_id: "chain-1-stream-42".to_string(),
        message_seq: 7,
        state_version: 1,
        kind: simulator_core::broadcaster::BroadcasterMessageKind::Update,
        snapshot_id: None,
        backend_scope: "native".to_string(),
        block_number: Some(11),
        observed_timestamp_ms: None,
        published_at_ms: 1,
        payload_json: "{}".to_string(),
    };

    assert_eq!(redis_entry_id(&entry)?, "42-7");
    Ok(())
}

#[test]
fn redis_progress_entry_carries_writer_promotion_reason() -> Result<()> {
    let envelope = simulator_core::broadcaster::BroadcasterEnvelope::new(
        "chain-1-stream-2",
        1,
        BroadcasterPayload::Progress(BroadcasterProgress::new(
            Chain::Ethereum.id(),
            "chain-1-snapshot-2",
            vec![BroadcasterBackend::Native],
            "stream_restart",
        )?),
    );
    let entry = BroadcasterRedisStreamEntry::from_envelope(Chain::Ethereum.id(), &envelope, 0)?;

    assert_eq!(entry.kind, BroadcasterMessageKind::Progress);
    assert_eq!(entry.snapshot_id.as_deref(), Some("chain-1-snapshot-2"));
    assert_eq!(entry.backend_scope, "native");
    assert_eq!(entry.block_number, None);
    Ok(())
}

#[test]
fn redis_xadd_recovery_requires_existing_entry_to_match_exact_fields() -> Result<()> {
    let entry = BroadcasterRedisStreamEntry {
        schema_version: "1".to_string(),
        chain_id: 1,
        stream_id: "chain-1-stream-7".to_string(),
        message_seq: 3,
        state_version: 1,
        kind: simulator_core::broadcaster::BroadcasterMessageKind::Update,
        snapshot_id: None,
        backend_scope: "native".to_string(),
        block_number: Some(12),
        observed_timestamp_ms: None,
        published_at_ms: 1,
        payload_json: "{}".to_string(),
    };
    let entry_id = redis_entry_id(&entry)?;
    let fields = redis_entry_fields(&entry)?;
    assert!(fields.iter().all(|(field, _)| field != "event_time_ms"));
    let reply = stream_range_reply(&entry_id, &fields);

    assert!(redis_stream_entry_matches_reply(
        &reply, &entry_id, &fields
    )?);

    let mut changed_fields = fields.clone();
    let message_seq = changed_fields
        .iter_mut()
        .find(|(field, _)| field == "message_seq")
        .ok_or_else(|| anyhow!("message_seq field missing"))?;
    message_seq.1 = "4".to_string();
    assert!(!redis_stream_entry_matches_reply(
        &reply,
        &entry_id,
        &changed_fields
    )?);
    let mut stale_event_time_fields = fields.clone();
    stale_event_time_fields.push(("event_time_ms".to_string(), "1710000000000".to_string()));
    let stale_event_time_reply = stream_range_reply(&entry_id, &stale_event_time_fields);
    assert!(!redis_stream_entry_matches_reply(
        &stale_event_time_reply,
        &entry_id,
        &fields
    )?);
    assert!(!redis_stream_entry_matches_reply(&reply, "7-4", &fields)?);
    Ok(())
}

fn progress_payload(entry: &BroadcasterRedisStreamEntry) -> Result<BroadcasterProgress> {
    let envelope: BroadcasterEnvelope = serde_json::from_str(&entry.payload_json)?;
    let BroadcasterPayload::Progress(progress) = envelope.payload else {
        return Err(anyhow!("Redis stream entry payload should be progress"));
    };
    Ok(progress)
}

#[derive(Debug, Clone)]
struct CapturedAppend {
    entry: BroadcasterRedisStreamEntry,
}

fn stream_range_reply(entry_id: &str, fields: &[(String, String)]) -> StreamRangeReply {
    let map = fields
        .iter()
        .map(|(field, value)| (field.clone(), Value::BulkString(value.as_bytes().to_vec())))
        .collect();
    StreamRangeReply {
        ids: vec![StreamId {
            id: entry_id.to_string(),
            map,
            milliseconds_elapsed_from_delivery: None,
            delivered_count: None,
        }],
    }
}

fn stream_id_to_entry(stream_id: &StreamId) -> Result<BroadcasterRedisStreamEntry> {
    let fields = stream_id
        .map
        .iter()
        .map(|(field, value)| {
            redis::from_redis_value::<String>(value.clone())
                .map(|value| (field.clone(), serde_json::Value::String(value)))
                .map_err(Into::into)
        })
        .collect::<Result<serde_json::Map<String, serde_json::Value>>>()?;
    serde_json::from_value(serde_json::Value::Object(fields)).map_err(Into::into)
}

#[derive(Debug, Clone, Default)]
struct FakeRedisWriter {
    inner: Arc<Mutex<FakeRedisWriterState>>,
    append_started: Arc<Notify>,
    append_release: Arc<Notify>,
}

#[derive(Debug, Default)]
struct FakeRedisWriterState {
    appends: Vec<CapturedAppend>,
    active_token: Option<String>,
    active_generation: u64,
    lease_ttls: Vec<Duration>,
    fail_next_appends: usize,
    fail_message_seq: Option<u64>,
    fail_next_renews: usize,
    lose_next_append_replies: usize,
    conflict_next_append_replies: usize,
    append_delay: Option<Duration>,
    blocked_message_seq: Option<u64>,
    block_next_verify: bool,
    stall_after_record_message_seq: Option<u64>,
    fail_next_inspects: usize,
    append_attempt_count: usize,
}

impl FakeRedisWriter {
    async fn fail_next_appends(&self, count: usize) {
        self.inner.lock().await.fail_next_appends = count;
    }

    async fn fail_message_seq(&self, message_seq: u64) {
        self.inner.lock().await.fail_message_seq = Some(message_seq);
    }

    async fn clear_message_seq_failure(&self) {
        self.inner.lock().await.fail_message_seq = None;
    }

    async fn fail_next_renews(&self, count: usize) {
        self.inner.lock().await.fail_next_renews = count;
    }

    async fn lose_next_append_replies(&self, count: usize) {
        self.inner.lock().await.lose_next_append_replies = count;
    }

    async fn conflict_next_append_replies(&self, count: usize) {
        self.inner.lock().await.conflict_next_append_replies = count;
    }

    async fn delay_appends(&self, delay: Duration) {
        self.inner.lock().await.append_delay = Some(delay);
    }

    async fn block_message_seq(&self, message_seq: u64) {
        self.inner.lock().await.blocked_message_seq = Some(message_seq);
    }

    async fn block_next_verify(&self) {
        self.inner.lock().await.block_next_verify = true;
    }

    async fn stall_after_record_message_seq(&self, message_seq: u64) {
        self.inner.lock().await.stall_after_record_message_seq = Some(message_seq);
    }

    async fn fail_next_inspects(&self, count: usize) {
        self.inner.lock().await.fail_next_inspects = count;
    }

    async fn wait_for_blocked_append(&self) {
        self.append_started.notified().await;
    }

    fn release_blocked_append(&self) {
        self.append_release.notify_one();
    }

    async fn append_attempt_count(&self) -> usize {
        self.inner.lock().await.append_attempt_count
    }

    async fn appends(&self) -> Vec<CapturedAppend> {
        self.inner.lock().await.appends.clone()
    }

    async fn lease_ttls(&self) -> Vec<Duration> {
        self.inner.lock().await.lease_ttls.clone()
    }

    async fn expire_active_writer(&self) {
        let mut guard = self.inner.lock().await;
        guard.active_token = None;
    }

    async fn lose_all_data(&self) {
        let mut guard = self.inner.lock().await;
        guard.active_token = None;
        guard.active_generation = 0;
        guard.appends.clear();
    }
}

impl RedisStreamWriter for FakeRedisWriter {
    fn promote<'a>(
        &'a self,
        command: super::RedisPromotionCommand<'a>,
    ) -> futures::future::BoxFuture<'a, Result<super::RedisPromotionResult>> {
        Box::pin(async move {
            let mut guard = self.inner.lock().await;
            guard.lease_ttls.push(command.lease_ttl);
            if command.expected_writer_token.is_some_and(|expected_token| {
                guard.active_token.is_some()
                    && (guard.active_token.as_deref() != Some(expected_token)
                        || Some(guard.active_generation) != command.expected_generation)
            }) {
                return Err(anyhow!("stale Redis broadcaster writer token"));
            }
            if command.expected_writer_token.is_some() && guard.active_token.is_none() {
                if guard.active_generation != 0
                    && Some(guard.active_generation) != command.expected_generation
                {
                    return Err(anyhow!("stale Redis broadcaster writer generation"));
                }
                guard.active_generation = guard
                    .active_generation
                    .max(command.expected_generation.unwrap_or_default());
            }
            guard.active_generation = guard.active_generation.saturating_add(1);
            guard.active_token = Some(command.writer_token.to_string());
            let generation = guard.active_generation;
            let entry_id = format!("{generation}-1");
            guard.appends.push(CapturedAppend {
                entry: entry_from_fields(command.normal_marker_fields, generation)?,
            });
            Ok(super::RedisPromotionResult {
                generation,
                entry_id,
            })
        })
    }

    fn append_fenced<'a>(
        &'a self,
        command: super::RedisAppendCommand<'a>,
    ) -> futures::future::BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let (delay, blocked) = {
                let guard = self.inner.lock().await;
                (
                    guard.append_delay,
                    guard.blocked_message_seq == Some(command.entry.message_seq),
                )
            };
            if let Some(delay) = delay {
                sleep(delay).await;
            }
            if blocked {
                self.append_started.notify_one();
                self.append_release.notified().await;
                self.inner.lock().await.blocked_message_seq = None;
            }
            let mut guard = self.inner.lock().await;
            guard.append_attempt_count = guard.append_attempt_count.saturating_add(1);
            guard.lease_ttls.push(command.lease_ttl);
            if guard.active_token.is_none() {
                return Err(anyhow!("missing Redis broadcaster writer fence"));
            }
            if guard.active_token.as_deref() != Some(command.writer_token)
                || guard.active_generation != command.generation
            {
                return Err(anyhow!("stale Redis broadcaster writer token"));
            }
            if guard.fail_next_appends > 0 {
                guard.fail_next_appends -= 1;
                return Err(anyhow!("planned append failure"));
            }
            if guard.fail_message_seq == Some(command.entry.message_seq) {
                return Err(anyhow!("planned message_seq failure"));
            }
            let entry_id = redis_entry_id(command.entry)?;
            if let Some(existing) = guard.appends.iter().find(|append| {
                redis_entry_id(&append.entry).is_ok_and(|existing_id| existing_id == entry_id)
            }) {
                return if existing.entry == *command.entry {
                    Ok(entry_id)
                } else {
                    Err(anyhow!(
                        "conflicting Redis broadcaster entry already exists at {entry_id}"
                    ))
                };
            }
            if guard.conflict_next_append_replies > 0 {
                guard.conflict_next_append_replies -= 1;
                let mut conflicting = command.entry.clone();
                conflicting.payload_json.push('x');
                guard.appends.push(CapturedAppend { entry: conflicting });
                return Err(anyhow!("planned lost append reply"));
            }
            guard.appends.push(CapturedAppend {
                entry: command.entry.clone(),
            });
            if guard.stall_after_record_message_seq == Some(command.entry.message_seq) {
                guard.stall_after_record_message_seq = None;
                drop(guard);
                self.append_started.notify_one();
                futures::future::pending::<()>().await;
                unreachable!("pending append future only exits when cancelled");
            }
            if guard.lose_next_append_replies > 0 {
                guard.lose_next_append_replies -= 1;
                return Err(anyhow!("planned lost append reply"));
            }
            Ok(entry_id)
        })
    }

    fn renew_writer<'a>(
        &'a self,
        command: super::RedisRenewCommand<'a>,
    ) -> futures::future::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut guard = self.inner.lock().await;
            guard.lease_ttls.push(command.lease_ttl);
            if guard.active_token.is_none() {
                return Err(anyhow!("missing Redis broadcaster writer fence"));
            }
            if guard.active_token.as_deref() != Some(command.writer_token)
                || guard.active_generation != command.generation
            {
                return Err(anyhow!("stale Redis broadcaster writer token"));
            }
            if guard.fail_next_renews > 0 {
                guard.fail_next_renews -= 1;
                return Err(anyhow!("planned Redis renew failure"));
            }
            Ok(())
        })
    }

    fn verify_writer<'a>(
        &'a self,
        command: super::RedisVerifyCommand<'a>,
    ) -> futures::future::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let block = {
                let mut guard = self.inner.lock().await;
                let block = guard.block_next_verify;
                guard.block_next_verify = false;
                block
            };
            if block {
                futures::future::pending::<()>().await;
            }
            self.renew_writer(super::RedisRenewCommand {
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
        command: super::RedisInspectCommand<'a>,
    ) -> futures::future::BoxFuture<'a, Result<super::RedisWriterInspection>> {
        Box::pin(async move {
            let mut guard = self.inner.lock().await;
            if guard.active_token.is_none() {
                return Err(anyhow!(super::MISSING_FENCE_MESSAGE));
            }
            if guard.active_token.as_deref() != Some(command.writer_token)
                || guard.active_generation != command.generation
            {
                return Err(anyhow!("stale Redis broadcaster writer token"));
            }
            if guard.fail_next_inspects > 0 {
                guard.fail_next_inspects -= 1;
                return Err(anyhow!("planned Redis inspect failure"));
            }
            let first_entry_id = guard
                .appends
                .first()
                .map(|append| redis_entry_id(&append.entry))
                .transpose()?;
            let last_entry_id = guard
                .appends
                .last()
                .map(|append| redis_entry_id(&append.entry))
                .transpose()?;
            Ok(super::RedisWriterInspection {
                generation: guard.active_generation,
                first_entry_id,
                last_entry_id,
            })
        })
    }
}

fn entry_from_fields(
    fields: &[(String, String)],
    generation: u64,
) -> Result<BroadcasterRedisStreamEntry> {
    let mut value = serde_json::Map::new();
    for (field, field_value) in fields {
        let field_value =
            field_value.replace(super::GENERATION_PLACEHOLDER, &generation.to_string());
        value.insert(field.clone(), serde_json::Value::String(field_value));
    }
    serde_json::from_value(serde_json::Value::Object(value)).map_err(Into::into)
}

async fn ready_cache(
    backend: BroadcasterBackend,
    block_number: u64,
    component_id: &str,
) -> Result<BroadcasterSnapshotCache> {
    let cache = BroadcasterSnapshotCache::new(Chain::Ethereum.id(), vec![backend]);
    cache
        .apply_update(&update(backend, block_number, component_id))
        .await?;
    Ok(cache)
}

fn update(backend: BroadcasterBackend, block_number: u64, component_id: &str) -> Update {
    let protocol = match backend {
        BroadcasterBackend::Native => "uniswap_v2",
        BroadcasterBackend::Vm => "vm:balancer_v2",
        BroadcasterBackend::Rfq => "rfq:bebop",
    };
    let mut new_pairs = HashMap::new();
    new_pairs.insert(
        component_id.to_string(),
        protocol_component(protocol, block_number as u8),
    );

    let mut states = HashMap::new();
    states.insert(
        component_id.to_string(),
        Box::new(DummySim(block_number as u8)) as Box<dyn ProtocolSim>,
    );

    Update::new(block_number, states, new_pairs).set_sync_states(HashMap::from([(
        protocol.to_string(),
        SynchronizerState::Ready(BlockHeader {
            hash: Bytes::from(vec![1u8; 32]),
            number: block_number,
            parent_hash: Bytes::from(vec![2u8; 32]),
            revert: false,
            timestamp: block_number,
            partial_block_index: None,
        }),
    )]))
}

fn protocol_component(protocol: &str, seed: u8) -> ProtocolComponent {
    ProtocolComponent::new(
        Bytes::from([seed; 20]),
        protocol.to_string(),
        protocol.to_string(),
        Chain::Ethereum,
        vec![dummy_token(1, "TKNA"), dummy_token(2, "TKNB")],
        Vec::new(),
        HashMap::new(),
        Bytes::from([9u8; 32]),
        chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
            .unwrap_or_else(|| unreachable!("unix epoch"))
            .naive_utc(),
    )
}

fn dummy_token(seed: u8, symbol: &str) -> Token {
    Token::new(
        &Bytes::from([seed; 20]),
        symbol,
        18,
        0,
        &[],
        Chain::Ethereum,
        1,
    )
}

fn publisher_config() -> BroadcasterRedisPublisherConfig {
    BroadcasterRedisPublisherConfig {
        stream_key: "dsolver:broadcaster:test:events".to_string(),
        chain_id: Chain::Ethereum.id(),
        append_retry_window: Duration::from_millis(10),
        maxlen: None,
        writer_lease_ttl: Duration::from_secs(30),
        recovery_max_buffered_native_blocks: 64,
    }
}

fn base_heads<const N: usize>(backends: [BroadcasterBackend; N]) -> Vec<BroadcasterBackendHead> {
    backends
        .into_iter()
        .enumerate()
        .map(|(index, backend)| BroadcasterBackendHead::new(backend, index as u64))
        .collect()
}
