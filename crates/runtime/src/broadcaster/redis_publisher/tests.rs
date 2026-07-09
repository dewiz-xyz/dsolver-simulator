use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use num_bigint::BigUint;
use redis::streams::{StreamId, StreamRangeReply};
use redis::Value;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
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
    writer_lease_ttl_for_heartbeat_interval, BroadcasterRedisPublisher,
    BroadcasterRedisPublisherConfig, BroadcasterRedisPublisherMode, RedisStreamWriter,
};
use crate::broadcaster::state::BroadcasterSnapshotCache;
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterBackendHead, BroadcasterEnvelope, BroadcasterMessageKind,
    BroadcasterPayload, BroadcasterProgress, BroadcasterRedisStreamEntry,
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
    let progress = progress_payload(&marker.entry)?;
    assert!(
        progress.handoff.is_none(),
        "first promotion on an empty Redis stream should write a normal marker"
    );

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
async fn promotion_after_existing_tail_writes_handoff_marker() -> Result<()> {
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
    let old_tail = writer
        .appends()
        .await
        .last()
        .cloned()
        .ok_or_else(|| anyhow!("old writer should leave a Redis tail"))?;

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
    let handoff = progress_payload(&marker.entry)?
        .handoff
        .ok_or_else(|| anyhow!("promotion marker should include handoff proof"))?;
    assert_eq!(handoff.previous_stream_id, old_tail.entry.stream_id);
    assert_eq!(handoff.previous_entry_id, redis_entry_id(&old_tail.entry)?);
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
async fn lease_loss_retires_active_writer_without_appending() -> Result<()> {
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

    let update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 11, "native-2"))
        .await?;
    let Err(error) = publisher
        .publish_accepted_payload(BroadcasterPayload::Update(update))
        .await
    else {
        return Err(anyhow!(
            "lost writer lease should fence the active publisher"
        ));
    };

    assert!(format!("{error:#}").contains("stale Redis broadcaster writer"));
    assert_eq!(publisher.status_snapshot().await.mode, "retired");
    assert_eq!(writer.appends().await.len(), 1);
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
async fn retired_publisher_does_not_run_generation_reset() -> Result<()> {
    let writer = FakeRedisWriter::default();
    let old = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    let new = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    old.promote(base_heads([BroadcasterBackend::Native]), "old_active")
        .await?;
    new.promote(base_heads([BroadcasterBackend::Native]), "new_active")
        .await?;
    writer.expire_active_writer().await;
    let _ = old.renew_lease().await;

    let _ = old
        .reset_generation_boundary(
            "shared broadcaster generation reset",
            vec![BroadcasterBackend::Native],
        )
        .await;

    assert_eq!(old.status_snapshot().await.mode, "retired");
    assert_eq!(
        writer.appends().await.len(),
        2,
        "retired publishers must not append reset markers"
    );
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

#[tokio::test]
async fn stale_active_writer_cannot_reset_generation_or_repromote() -> Result<()> {
    let writer = FakeRedisWriter::default();
    let old = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    let new = BroadcasterRedisPublisher::new(publisher_config(), Arc::new(writer.clone()));
    old.promote(base_heads([BroadcasterBackend::Native]), "old_active")
        .await?;
    new.promote(base_heads([BroadcasterBackend::Native]), "new_active")
        .await?;

    let Err(error) = old
        .reset_generation_boundary(
            "shared broadcaster generation reset",
            vec![BroadcasterBackend::Native],
        )
        .await
    else {
        return Err(anyhow!(
            "stale old writer must not reset or re-promote a writer generation"
        ));
    };

    assert!(format!("{error:#}").contains("stale Redis broadcaster writer"));
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
        "stale reset must not append a new generation marker"
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
    assert_eq!(appends[3].entry.stream_id, boundary.stream_id);
    assert_eq!(
        appends[3].entry.snapshot_id.as_deref(),
        Some(boundary.snapshot_id.as_str())
    );
    Ok(())
}

#[tokio::test]
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
async fn retry_exhaustion_marks_unhealthy_until_generation_reset() -> Result<()> {
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
    assert_eq!(failed_status.generation_reset_count, 0);
    assert_eq!(failed_status.retry_exhaustion_count, 1);
    assert_eq!(failed_status.stream_id, "chain-1-stream-1");
    assert!(failed_status.replay_boundary.is_none());
    assert!(failed_status
        .last_error
        .as_deref()
        .unwrap_or("")
        .contains("planned append failure"));

    writer.fail_next_appends(0).await;
    let blocked_update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 12, "native-3"))
        .await?;
    let Err(error) = publisher
        .publish_accepted_payload(BroadcasterPayload::Update(blocked_update))
        .await
    else {
        return Err(anyhow!(
            "publisher should stay unavailable until the shared generation reset"
        ));
    };
    assert!(format!("{error:#}").contains("publisher is unhealthy"));
    assert_eq!(
        writer.appends().await.len(),
        1,
        "a blocked publisher must not append into a Redis-only generation"
    );

    publisher
        .reset_generation_boundary(
            "shared broadcaster generation reset",
            vec![BroadcasterBackend::Native],
        )
        .await?;
    let reset_status = publisher.status_snapshot().await;
    assert!(reset_status.healthy);
    assert_eq!(reset_status.generation_reset_count, 1);
    assert_eq!(reset_status.retry_exhaustion_count, 1);
    assert_eq!(reset_status.stream_id, "chain-1-stream-2");
    assert!(reset_status.replay_boundary.is_some());
    let appends_after_reset = writer.appends().await;
    let reset_marker = appends_after_reset
        .last()
        .ok_or_else(|| anyhow!("generation reset should publish a Redis progress marker"))?;
    assert_generation_reset_marker(&reset_marker.entry)?;

    let recovered_update = raw_cache
        .apply_update(&update(BroadcasterBackend::Native, 13, "native-4"))
        .await?;
    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(recovered_update))
        .await?;

    let recovered_status = publisher.status_snapshot().await;
    assert!(recovered_status.healthy);
    assert_eq!(recovered_status.stream_id, "chain-1-stream-2");
    assert!(recovered_status.replay_boundary.is_some());
    assert!(recovered_status.last_error.is_none());

    let recovered_stream_entries = writer
        .appends()
        .await
        .into_iter()
        .filter(|append| append.entry.stream_id == recovered_status.stream_id)
        .collect::<Vec<_>>();
    assert_eq!(
        recovered_stream_entries
            .iter()
            .map(|append| append.entry.message_seq)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
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
    assert_eq!(failed_status.generation_reset_count, 0);
    assert_eq!(failed_status.retry_exhaustion_count, 1);
    assert!(failed_status.replay_boundary.is_none());
    assert_eq!(writer.appends().await.len(), 1);
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
        kind: simulator_core::broadcaster::BroadcasterMessageKind::Update,
        snapshot_id: None,
        backend_scope: "native".to_string(),
        block_number: Some(11),
        observed_timestamp_ms: None,
        payload_json: "{}".to_string(),
    };

    assert_eq!(redis_entry_id(&entry)?, "42-7");
    Ok(())
}

#[test]
fn redis_progress_entry_carries_generation_reset_reason() -> Result<()> {
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
    let entry = BroadcasterRedisStreamEntry::from_envelope(Chain::Ethereum.id(), &envelope)?;

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
        kind: simulator_core::broadcaster::BroadcasterMessageKind::Update,
        snapshot_id: None,
        backend_scope: "native".to_string(),
        block_number: Some(12),
        observed_timestamp_ms: None,
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

fn assert_no_progress_handoff(entry: &BroadcasterRedisStreamEntry, context: &str) -> Result<()> {
    assert!(
        progress_payload(entry)?.handoff.is_none(),
        "{context} must stay normal progress markers"
    );
    Ok(())
}

fn assert_generation_reset_marker(entry: &BroadcasterRedisStreamEntry) -> Result<()> {
    assert_eq!(entry.stream_id, "chain-1-stream-2");
    assert_eq!(entry.message_seq, 1);
    assert_eq!(entry.kind, BroadcasterMessageKind::Progress);
    assert_eq!(entry.backend_scope, "native");
    assert_no_progress_handoff(entry, "shared generation reset markers")?;
    assert!(entry
        .payload_json
        .contains("shared broadcaster generation reset"));
    Ok(())
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

#[derive(Debug, Clone, Default)]
struct FakeRedisWriter {
    inner: Arc<Mutex<FakeRedisWriterState>>,
}

#[derive(Debug, Default)]
struct FakeRedisWriterState {
    appends: Vec<CapturedAppend>,
    active_token: Option<String>,
    active_generation: u64,
    lease_expired: bool,
    lease_ttls: Vec<Duration>,
    fail_next_appends: usize,
    append_delay: Option<Duration>,
    append_attempt_count: usize,
}

impl FakeRedisWriter {
    async fn fail_next_appends(&self, count: usize) {
        self.inner.lock().await.fail_next_appends = count;
    }

    async fn delay_appends(&self, delay: Duration) {
        self.inner.lock().await.append_delay = Some(delay);
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
        guard.lease_expired = true;
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
            if let Some(expected_token) = command.expected_writer_token {
                if guard.lease_expired {
                    return Err(anyhow!("stale Redis broadcaster writer token"));
                }
                if guard.active_token.is_some()
                    && (guard.active_token.as_deref() != Some(expected_token)
                        || Some(guard.active_generation) != command.expected_generation)
                {
                    return Err(anyhow!("stale Redis broadcaster writer token"));
                }
                if guard.active_token.is_none() {
                    guard.active_generation = guard
                        .active_generation
                        .max(command.expected_generation.unwrap_or_default());
                }
            }
            guard.active_generation = guard.active_generation.saturating_add(1);
            guard.active_token = Some(command.writer_token.to_string());
            guard.lease_expired = false;
            let generation = guard.active_generation;
            let entry_id = format!("{generation}-1");
            let previous_tail = guard.appends.last().cloned();
            let marker_fields =
                if previous_tail.is_some() && !command.handoff_marker_fields.is_empty() {
                    command.handoff_marker_fields
                } else {
                    command.normal_marker_fields
                };
            let previous_stream_id = previous_tail
                .as_ref()
                .map(|append| append.entry.stream_id.as_str())
                .unwrap_or_default();
            let previous_entry_id = previous_tail
                .as_ref()
                .map(|append| redis_entry_id(&append.entry))
                .transpose()?
                .unwrap_or_default();
            guard.appends.push(CapturedAppend {
                entry: entry_from_fields(
                    marker_fields,
                    generation,
                    previous_stream_id,
                    &previous_entry_id,
                )?,
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
            let delay = self.inner.lock().await.append_delay;
            if let Some(delay) = delay {
                sleep(delay).await;
            }
            let mut guard = self.inner.lock().await;
            guard.append_attempt_count = guard.append_attempt_count.saturating_add(1);
            guard.lease_ttls.push(command.lease_ttl);
            if guard.lease_expired
                || (guard.active_token.is_some()
                    && (guard.active_token.as_deref() != Some(command.writer_token)
                        || guard.active_generation != command.generation))
            {
                return Err(anyhow!("stale Redis broadcaster writer token"));
            }
            if guard.fail_next_appends > 0 {
                guard.fail_next_appends -= 1;
                return Err(anyhow!("planned append failure"));
            }
            let entry_id = redis_entry_id(command.entry)?;
            guard.appends.push(CapturedAppend {
                entry: command.entry.clone(),
            });
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
            if guard.lease_expired
                || (guard.active_token.is_some()
                    && (guard.active_token.as_deref() != Some(command.writer_token)
                        || guard.active_generation != command.generation))
            {
                return Err(anyhow!("stale Redis broadcaster writer token"));
            }
            Ok(())
        })
    }
}

fn entry_from_fields(
    fields: &[(String, String)],
    generation: u64,
    previous_stream_id: &str,
    previous_entry_id: &str,
) -> Result<BroadcasterRedisStreamEntry> {
    let mut value = serde_json::Map::new();
    for (field, field_value) in fields {
        let field_value = field_value
            .replace(super::GENERATION_PLACEHOLDER, &generation.to_string())
            .replace(super::PREVIOUS_STREAM_ID_PLACEHOLDER, previous_stream_id)
            .replace(super::PREVIOUS_ENTRY_ID_PLACEHOLDER, previous_entry_id);
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
    }
}

fn base_heads<const N: usize>(backends: [BroadcasterBackend; N]) -> Vec<BroadcasterBackendHead> {
    backends
        .into_iter()
        .enumerate()
        .map(|(index, backend)| BroadcasterBackendHead::new(backend, index as u64))
        .collect()
}
