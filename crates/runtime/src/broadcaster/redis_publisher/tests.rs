use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use num_bigint::BigUint;
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
    writer::redis_entry_id, BroadcasterRedisPublisher, BroadcasterRedisPublisherConfig,
    BroadcasterRedisSnapshotSource, RedisStreamWriter,
};
use crate::broadcaster::state::BroadcasterSnapshotCache;
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterPayload, BroadcasterRedisSnapshotPointer,
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
async fn publishes_combined_snapshot_and_pointer_after_snapshot_end() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new_for_test(
        publisher_config(),
        vec![
            BroadcasterRedisSnapshotSource::new(raw_cache, vec![BroadcasterBackend::Native]),
            BroadcasterRedisSnapshotSource::new(rfq_cache, vec![BroadcasterBackend::Rfq]),
        ],
        Arc::new(writer.clone()),
    );

    publisher.ensure_snapshot_published().await?;

    let appends = writer.appends().await;
    assert_eq!(appends.len(), 4);
    assert_eq!(
        appends
            .iter()
            .map(|append| append.entry.message_seq)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert!(matches!(
        appends[0].entry.kind,
        simulator_core::broadcaster::BroadcasterMessageKind::SnapshotStart
    ));
    assert_eq!(appends[0].entry.backend_scope, "native,rfq");
    assert_eq!(appends[1].entry.backend_scope, "native");
    assert_eq!(appends[2].entry.backend_scope, "rfq");
    assert_eq!(appends[2].entry.block_number, None);
    assert!(matches!(
        appends[3].entry.kind,
        simulator_core::broadcaster::BroadcasterMessageKind::SnapshotEnd
    ));

    let pointer = writer
        .latest_pointer()
        .await
        .ok_or_else(|| anyhow!("snapshot pointer should be written"))?;
    assert_eq!(pointer.snapshot_start_entry_id, "1000-0");
    assert_eq!(pointer.snapshot_end_entry_id, "1000-3");
    assert_eq!(pointer.live_cursor_entry_id, "1000-3");
    assert_eq!(pointer.stream_id, appends[0].entry.stream_id);
    assert_eq!(
        pointer.snapshot_id,
        appends[0].entry.snapshot_id.as_deref().unwrap_or("")
    );
    Ok(())
}

#[tokio::test]
async fn publishes_live_updates_and_heartbeats_after_snapshot_pointer() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new_for_test(
        publisher_config(),
        vec![
            BroadcasterRedisSnapshotSource::new(
                raw_cache.clone(),
                vec![BroadcasterBackend::Native],
            ),
            BroadcasterRedisSnapshotSource::new(rfq_cache.clone(), vec![BroadcasterBackend::Rfq]),
        ],
        Arc::new(writer.clone()),
    );
    publisher.ensure_snapshot_published().await?;

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
        vec![1, 2, 3, 4, 5, 6, 7]
    );
    assert_eq!(appends[4].entry.kind.as_str(), "update");
    assert_eq!(appends[4].entry.backend_scope, "native");
    assert_eq!(appends[4].entry.block_number, Some(11));
    assert_eq!(appends[5].entry.kind.as_str(), "update");
    assert_eq!(appends[5].entry.backend_scope, "rfq");
    assert_eq!(appends[5].entry.block_number, None);
    assert_eq!(appends[6].entry.kind.as_str(), "heartbeat");
    assert_eq!(appends[6].entry.backend_scope, "native");

    let pointer = writer
        .latest_pointer()
        .await
        .ok_or_else(|| anyhow!("snapshot pointer should be written"))?;
    assert_eq!(appends[6].entry.stream_id, pointer.stream_id);
    assert_eq!(
        appends[6].entry.snapshot_id.as_deref(),
        Some(pointer.snapshot_id.as_str())
    );
    Ok(())
}

#[tokio::test]
async fn append_retry_success_preserves_message_sequence_order() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
    let writer = FakeRedisWriter::default();
    writer.fail_next_appends(1).await;
    let publisher = BroadcasterRedisPublisher::new_for_test(
        publisher_config(),
        vec![
            BroadcasterRedisSnapshotSource::new(raw_cache, vec![BroadcasterBackend::Native]),
            BroadcasterRedisSnapshotSource::new(rfq_cache, vec![BroadcasterBackend::Rfq]),
        ],
        Arc::new(writer.clone()),
    );

    publisher.ensure_snapshot_published().await?;

    let appends = writer.appends().await;
    assert_eq!(writer.append_attempt_count().await, 5);
    assert_eq!(
        appends
            .iter()
            .map(|append| append.entry.message_seq)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert_eq!(appends[0].entry.kind.as_str(), "snapshot_start");
    assert!(writer.latest_pointer().await.is_some());
    Ok(())
}

#[tokio::test]
async fn append_retry_after_ambiguous_accept_does_not_duplicate_entry() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
    let writer = FakeRedisWriter::default();
    writer.ambiguously_accept_next_append().await;
    let publisher = BroadcasterRedisPublisher::new_for_test(
        publisher_config(),
        vec![
            BroadcasterRedisSnapshotSource::new(raw_cache, vec![BroadcasterBackend::Native]),
            BroadcasterRedisSnapshotSource::new(rfq_cache, vec![BroadcasterBackend::Rfq]),
        ],
        Arc::new(writer.clone()),
    );

    publisher.ensure_snapshot_published().await?;

    let appends = writer.appends().await;
    assert_eq!(writer.append_attempt_count().await, 5);
    assert_eq!(
        appends
            .iter()
            .map(|append| append.entry.message_seq)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    Ok(())
}

#[tokio::test]
async fn readiness_triggering_update_is_only_represented_by_initial_snapshot() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let rfq_cache =
        BroadcasterSnapshotCache::new(Chain::Ethereum.id(), vec![BroadcasterBackend::Rfq]);
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new_for_test(
        publisher_config(),
        vec![
            BroadcasterRedisSnapshotSource::new(raw_cache, vec![BroadcasterBackend::Native]),
            BroadcasterRedisSnapshotSource::new(rfq_cache.clone(), vec![BroadcasterBackend::Rfq]),
        ],
        Arc::new(writer.clone()),
    );
    let rfq_update = rfq_cache
        .apply_update(&update(BroadcasterBackend::Rfq, 20, "rfq-1"))
        .await?;

    publisher
        .publish_accepted_payload(BroadcasterPayload::Update(rfq_update))
        .await?;

    let appends = writer.appends().await;
    assert_eq!(appends.len(), 4);
    assert!(matches!(
        appends.last().map(|append| append.entry.kind),
        Some(simulator_core::broadcaster::BroadcasterMessageKind::SnapshotEnd)
    ));
    Ok(())
}

#[tokio::test]
async fn retry_exhaustion_marks_unhealthy_and_next_snapshot_uses_new_generation() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
    let writer = FakeRedisWriter::default();
    writer.fail_next_appends(100).await;
    let publisher = BroadcasterRedisPublisher::new_for_test(
        publisher_config(),
        vec![
            BroadcasterRedisSnapshotSource::new(raw_cache, vec![BroadcasterBackend::Native]),
            BroadcasterRedisSnapshotSource::new(rfq_cache, vec![BroadcasterBackend::Rfq]),
        ],
        Arc::new(writer.clone()),
    );

    let Err(error) = publisher.ensure_snapshot_published().await else {
        return Err(anyhow!(
            "retry exhaustion should surface the failed publication"
        ));
    };
    assert!(error
        .to_string()
        .contains("failed to append Redis broadcaster"));

    let failed_status = publisher.status_snapshot().await;
    assert!(!failed_status.healthy);
    assert_eq!(failed_status.generation_reset_count, 1);
    assert_eq!(failed_status.retry_exhaustion_count, 1);
    assert_eq!(failed_status.stream_id, "chain-1-redis-stream-2");
    assert!(failed_status.latest_snapshot_pointer.is_none());
    assert!(failed_status
        .last_error
        .as_deref()
        .unwrap_or("")
        .contains("planned append failure"));

    writer.fail_next_appends(0).await;
    publisher.ensure_snapshot_published().await?;

    let recovered_status = publisher.status_snapshot().await;
    assert!(recovered_status.healthy);
    assert_eq!(recovered_status.stream_id, "chain-1-redis-stream-2");
    assert!(recovered_status.latest_snapshot_pointer.is_some());
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
        vec![1, 2, 3, 4]
    );
    Ok(())
}

#[tokio::test]
async fn pointer_retry_exhaustion_resets_generation_before_republishing() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
    let writer = FakeRedisWriter::default();
    writer.fail_next_pointer_writes(100).await;
    let publisher = BroadcasterRedisPublisher::new_for_test(
        publisher_config(),
        vec![
            BroadcasterRedisSnapshotSource::new(raw_cache, vec![BroadcasterBackend::Native]),
            BroadcasterRedisSnapshotSource::new(rfq_cache, vec![BroadcasterBackend::Rfq]),
        ],
        Arc::new(writer.clone()),
    );

    let Err(error) = publisher.ensure_snapshot_published().await else {
        return Err(anyhow!(
            "pointer retry exhaustion should surface the failed publication"
        ));
    };
    assert!(error
        .to_string()
        .contains("failed to write Redis broadcaster snapshot pointer"));

    let failed_status = publisher.status_snapshot().await;
    assert!(!failed_status.healthy);
    assert_eq!(failed_status.generation_reset_count, 1);
    assert_eq!(failed_status.retry_exhaustion_count, 1);
    assert!(failed_status.pointer_write_failure_count > 0);
    assert_eq!(failed_status.stream_id, "chain-1-redis-stream-2");
    assert!(failed_status.latest_snapshot_pointer.is_none());

    writer.fail_next_pointer_writes(0).await;
    publisher.ensure_snapshot_published().await?;

    let recovered_status = publisher.status_snapshot().await;
    assert!(recovered_status.healthy);
    assert_eq!(recovered_status.stream_id, "chain-1-redis-stream-2");
    assert_eq!(
        recovered_status
            .latest_snapshot_pointer
            .as_ref()
            .map(|pointer| pointer.stream_id.as_str()),
        Some("chain-1-redis-stream-2")
    );
    Ok(())
}

#[tokio::test]
async fn stalled_append_exhausts_retry_window() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
    let writer = FakeRedisWriter::default();
    writer.delay_appends(Duration::from_millis(50)).await;
    let publisher = BroadcasterRedisPublisher::new_for_test(
        publisher_config(),
        vec![
            BroadcasterRedisSnapshotSource::new(raw_cache, vec![BroadcasterBackend::Native]),
            BroadcasterRedisSnapshotSource::new(rfq_cache, vec![BroadcasterBackend::Rfq]),
        ],
        Arc::new(writer.clone()),
    );

    let Err(error) = publisher.ensure_snapshot_published().await else {
        return Err(anyhow!("stalled append should exhaust the retry window"));
    };
    assert!(format!("{error:#}").contains("retry window exhausted"));

    let failed_status = publisher.status_snapshot().await;
    assert!(!failed_status.healthy);
    assert_eq!(failed_status.append_failure_count, 1);
    assert_eq!(failed_status.generation_reset_count, 1);
    assert_eq!(failed_status.retry_exhaustion_count, 1);
    assert!(failed_status.latest_snapshot_pointer.is_none());
    assert!(writer.appends().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn stalled_pointer_write_exhausts_retry_window() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let rfq_cache = ready_cache(BroadcasterBackend::Rfq, 20, "rfq-1").await?;
    let writer = FakeRedisWriter::default();
    writer.delay_pointer_writes(Duration::from_millis(50)).await;
    let publisher = BroadcasterRedisPublisher::new_for_test(
        publisher_config(),
        vec![
            BroadcasterRedisSnapshotSource::new(raw_cache, vec![BroadcasterBackend::Native]),
            BroadcasterRedisSnapshotSource::new(rfq_cache, vec![BroadcasterBackend::Rfq]),
        ],
        Arc::new(writer.clone()),
    );

    let Err(error) = publisher.ensure_snapshot_published().await else {
        return Err(anyhow!(
            "stalled pointer write should exhaust the retry window"
        ));
    };
    assert!(format!("{error:#}").contains("retry window exhausted"));

    let failed_status = publisher.status_snapshot().await;
    assert!(!failed_status.healthy);
    assert_eq!(failed_status.pointer_write_failure_count, 1);
    assert_eq!(failed_status.generation_reset_count, 1);
    assert_eq!(failed_status.retry_exhaustion_count, 1);
    assert!(failed_status.latest_snapshot_pointer.is_none());
    assert_eq!(writer.appends().await.len(), 4);
    assert!(writer.latest_pointer().await.is_none());
    Ok(())
}

#[tokio::test]
async fn waits_for_all_sources_before_initial_snapshot_publish() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let rfq_cache =
        BroadcasterSnapshotCache::new(Chain::Ethereum.id(), vec![BroadcasterBackend::Rfq]);
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new_for_test(
        publisher_config(),
        vec![
            BroadcasterRedisSnapshotSource::new(
                raw_cache.clone(),
                vec![BroadcasterBackend::Native],
            ),
            BroadcasterRedisSnapshotSource::new(rfq_cache.clone(), vec![BroadcasterBackend::Rfq]),
        ],
        Arc::new(writer.clone()),
    );

    assert!(!publisher.publish_snapshot_if_ready().await?);
    assert!(writer.appends().await.is_empty());

    rfq_cache
        .apply_update(&update(BroadcasterBackend::Rfq, 20, "rfq-1"))
        .await?;

    assert!(publisher.publish_snapshot_if_ready().await?);
    assert_eq!(writer.appends().await.len(), 4);
    assert!(writer.latest_pointer().await.is_some());
    Ok(())
}

#[tokio::test]
async fn publish_rejects_message_sequence_overflow() -> Result<()> {
    let raw_cache = ready_cache(BroadcasterBackend::Native, 10, "native-1").await?;
    let writer = FakeRedisWriter::default();
    let publisher = BroadcasterRedisPublisher::new_for_test(
        publisher_config(),
        vec![BroadcasterRedisSnapshotSource::new(
            raw_cache.clone(),
            vec![BroadcasterBackend::Native],
        )],
        Arc::new(writer.clone()),
    );
    publisher.ensure_snapshot_published().await?;
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
    assert_eq!(writer.appends().await.len(), 3);
    Ok(())
}

#[test]
fn redis_entry_id_uses_generation_and_message_sequence() -> Result<()> {
    let entry = BroadcasterRedisStreamEntry {
        schema_version: "1".to_string(),
        chain_id: Chain::Ethereum.id(),
        stream_id: "chain-1-redis-stream-42".to_string(),
        message_seq: 7,
        kind: simulator_core::broadcaster::BroadcasterMessageKind::Update,
        snapshot_id: None,
        backend_scope: "native".to_string(),
        block_number: Some(11),
        event_time_ms: 1_710_000_000_123,
        payload_json: "{}".to_string(),
    };

    assert_eq!(redis_entry_id(&entry)?, "42-7");
    Ok(())
}

#[derive(Debug, Clone)]
struct CapturedAppend {
    entry: BroadcasterRedisStreamEntry,
}

#[derive(Debug, Clone, Default)]
struct FakeRedisWriter {
    inner: Arc<Mutex<FakeRedisWriterState>>,
}

#[derive(Debug, Default)]
struct FakeRedisWriterState {
    appends: Vec<CapturedAppend>,
    latest_pointer: Option<BroadcasterRedisSnapshotPointer>,
    fail_next_appends: usize,
    ambiguously_accept_next_append: bool,
    fail_next_pointer_writes: usize,
    append_delay: Option<Duration>,
    pointer_write_delay: Option<Duration>,
    append_attempt_count: usize,
}

impl FakeRedisWriter {
    async fn fail_next_appends(&self, count: usize) {
        self.inner.lock().await.fail_next_appends = count;
    }

    async fn ambiguously_accept_next_append(&self) {
        self.inner.lock().await.ambiguously_accept_next_append = true;
    }

    async fn fail_next_pointer_writes(&self, count: usize) {
        self.inner.lock().await.fail_next_pointer_writes = count;
    }

    async fn delay_appends(&self, delay: Duration) {
        self.inner.lock().await.append_delay = Some(delay);
    }

    async fn delay_pointer_writes(&self, delay: Duration) {
        self.inner.lock().await.pointer_write_delay = Some(delay);
    }

    async fn append_attempt_count(&self) -> usize {
        self.inner.lock().await.append_attempt_count
    }

    async fn appends(&self) -> Vec<CapturedAppend> {
        self.inner.lock().await.appends.clone()
    }

    async fn latest_pointer(&self) -> Option<BroadcasterRedisSnapshotPointer> {
        self.inner.lock().await.latest_pointer.clone()
    }
}

impl RedisStreamWriter for FakeRedisWriter {
    fn append<'a>(
        &'a self,
        _stream_key: &'a str,
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
            if let Some((index, _)) = guard.appends.iter().enumerate().find(|(_, append)| {
                append.entry.stream_id == entry.stream_id
                    && append.entry.message_seq == entry.message_seq
            }) {
                return Ok(format!("1000-{index}"));
            }
            let entry_id = format!("1000-{}", guard.appends.len());
            guard.appends.push(CapturedAppend {
                entry: entry.clone(),
            });
            if guard.ambiguously_accept_next_append {
                guard.ambiguously_accept_next_append = false;
                return Err(anyhow!("ambiguous append failure"));
            }
            Ok(entry_id)
        })
    }

    fn set_snapshot_pointer<'a>(
        &'a self,
        _snapshot_key: &'a str,
        pointer: &'a BroadcasterRedisSnapshotPointer,
    ) -> futures::future::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let delay = self.inner.lock().await.pointer_write_delay;
            if let Some(delay) = delay {
                sleep(delay).await;
            }
            let mut guard = self.inner.lock().await;
            if guard.fail_next_pointer_writes > 0 {
                guard.fail_next_pointer_writes -= 1;
                return Err(anyhow!("planned pointer failure"));
            }
            guard.latest_pointer = Some(pointer.clone());
            Ok(())
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
        snapshot_key: "dsolver:broadcaster:test:snapshot".to_string(),
        chain_id: Chain::Ethereum.id(),
        snapshot_max_payload_bytes: 8_388_608,
        append_retry_window: Duration::from_millis(10),
    }
}
