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
    next_generation_after_entry_id, redis_entry_fields, redis_entry_id,
    redis_stream_entry_matches_reply, redis_xadd_command, BroadcasterRedisPublisher,
    BroadcasterRedisPublisherConfig, RedisStreamWriter,
};
use crate::broadcaster::state::BroadcasterSnapshotCache;
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterMessageKind, BroadcasterPayload, BroadcasterProgress,
    BroadcasterRedisStreamEntry,
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
async fn replay_boundary_starts_before_first_delta_without_publishing_snapshot() -> Result<()> {
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new_with_initial_generation(
        publisher_config(),
        Arc::new(writer.clone()),
        1,
    );

    let boundary = publisher.replay_boundary().await?;

    assert_eq!(boundary.stream_key, "dsolver:broadcaster:test:events");
    assert_eq!(boundary.stream_id, "chain-1-stream-1");
    assert_eq!(boundary.snapshot_id, "chain-1-snapshot-1");
    assert_eq!(boundary.generation, 1);
    assert_eq!(boundary.exclusive_message_seq, 0);
    assert_eq!(boundary.exclusive_entry_id(), "1-0");
    assert!(
        writer.appends().await.is_empty(),
        "HTTP snapshots are the bootstrap source; Redis must not receive full snapshot entries"
    );
    Ok(())
}

#[tokio::test]
async fn publishes_live_updates_and_heartbeats_as_deltas_after_replay_boundary() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new_with_initial_generation(
        publisher_config(),
        Arc::new(writer.clone()),
        1,
    );
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
        vec![1, 2, 3]
    );
    assert_eq!(appends[0].entry.kind.as_str(), "update");
    assert_eq!(appends[0].entry.backend_scope, "native");
    assert_eq!(appends[0].entry.block_number, Some(11));
    assert_eq!(appends[1].entry.kind.as_str(), "update");
    assert_eq!(appends[1].entry.backend_scope, "rfq");
    assert_eq!(appends[1].entry.block_number, None);
    assert_eq!(appends[2].entry.kind.as_str(), "heartbeat");
    assert_eq!(appends[2].entry.backend_scope, "native");
    assert_eq!(appends[2].entry.stream_id, boundary.stream_id);
    assert_eq!(
        appends[2].entry.snapshot_id.as_deref(),
        Some(boundary.snapshot_id.as_str())
    );
    Ok(())
}

#[tokio::test]
async fn append_retry_success_preserves_message_sequence_order() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    writer.fail_next_appends(1).await;
    let publisher = BroadcasterRedisPublisher::new_with_initial_generation(
        publisher_config(),
        Arc::new(writer.clone()),
        1,
    );

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
        vec![1]
    );
    assert_eq!(appends[0].entry.kind.as_str(), "update");
    Ok(())
}

#[tokio::test]
async fn readiness_triggering_update_is_published_as_a_delta() -> Result<()> {
    let rfq_cache =
        BroadcasterSnapshotCache::new(Chain::Ethereum.id(), vec![BroadcasterBackend::Rfq]);
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new_with_initial_generation(
        publisher_config(),
        Arc::new(writer.clone()),
        1,
    );
    let rfq_update = rfq_cache
        .apply_update(&update(BroadcasterBackend::Rfq, 20, "rfq-1"))
        .await?;

    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(rfq_update))
        .await?;

    let appends = writer.appends().await;
    assert_eq!(appends.len(), 1);
    assert_eq!(appends[0].entry.kind.as_str(), "update");
    assert_eq!(appends[0].entry.backend_scope, "rfq");
    Ok(())
}

#[tokio::test]
async fn retry_exhaustion_marks_unhealthy_until_generation_reset() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    writer.fail_next_appends(100).await;
    let publisher = BroadcasterRedisPublisher::new_with_initial_generation(
        publisher_config(),
        Arc::new(writer.clone()),
        1,
    );

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
        0,
        "a blocked publisher must not append into a Redis-only generation"
    );

    publisher
        .reset_generation(
            "shared broadcaster generation reset",
            vec![BroadcasterBackend::Native],
        )
        .await;
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
    assert_eq!(reset_marker.entry.stream_id, "chain-1-stream-2");
    assert_eq!(reset_marker.entry.message_seq, 1);
    assert_eq!(reset_marker.entry.kind, BroadcasterMessageKind::Progress);
    assert_eq!(reset_marker.entry.backend_scope, "native");
    assert!(reset_marker
        .entry
        .payload_json
        .contains("shared broadcaster generation reset"));

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
    let publisher = BroadcasterRedisPublisher::new_with_initial_generation(
        publisher_config(),
        Arc::new(writer.clone()),
        1,
    );

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
    assert!(writer.appends().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn publish_rejects_message_sequence_overflow() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new_with_initial_generation(
        publisher_config(),
        Arc::new(writer.clone()),
        1,
    );
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
    assert!(writer.appends().await.is_empty());
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
        event_time_ms: 1_710_000_000_123,
        payload_json: "{}".to_string(),
    };

    assert_eq!(redis_entry_id(&entry)?, "42-7");
    Ok(())
}

#[test]
fn next_generation_starts_after_retained_redis_stream_top() -> Result<()> {
    assert_eq!(next_generation_after_entry_id("42-7")?, 43);
    assert_eq!(next_generation_after_entry_id("0-0")?, 1);
    assert!(next_generation_after_entry_id("not-an-entry-id").is_err());
    Ok(())
}

#[test]
fn redis_xadd_command_uses_configured_maxlen() -> Result<()> {
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
        event_time_ms: 1_710_000_000_123,
        payload_json: "{}".to_string(),
    };

    let command = redis_xadd_command("stream:test", Some(1_000), &entry)?;
    let packed = String::from_utf8(command.get_packed_command())?;

    assert!(packed.contains("MAXLEN"));
    assert!(packed.contains("\r\n~\r\n"));
    assert!(packed.contains("\r\n1000\r\n"));
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
    let entry = BroadcasterRedisStreamEntry::from_envelope(
        Chain::Ethereum.id(),
        1_710_000_000_123,
        &envelope,
    )?;

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
        event_time_ms: 1_000,
        payload_json: "{}".to_string(),
    };
    let entry_id = redis_entry_id(&entry)?;
    let fields = redis_entry_fields(&entry)?;
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
    assert!(!redis_stream_entry_matches_reply(&reply, "7-4", &fields)?);
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
}

impl RedisStreamWriter for FakeRedisWriter {
    fn append<'a>(
        &'a self,
        _stream_key: &'a str,
        _maxlen: Option<u64>,
        entry: &'a BroadcasterRedisStreamEntry,
    ) -> futures::future::BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let delay = self.inner.lock().await.append_delay;
            if let Some(delay) = delay {
                sleep(delay).await;
            }
            let mut guard = self.inner.lock().await;
            guard.append_attempt_count = guard.append_attempt_count.saturating_add(1);
            if guard.fail_next_appends > 0 {
                guard.fail_next_appends -= 1;
                return Err(anyhow!("planned append failure"));
            }
            let entry_id = format!("1000-{}", guard.appends.len());
            guard.appends.push(CapturedAppend {
                entry: entry.clone(),
            });
            Ok(entry_id)
        })
    }
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
    }
}
