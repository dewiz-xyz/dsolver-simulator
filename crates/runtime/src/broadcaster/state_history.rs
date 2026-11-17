use std::collections::{btree_map::Entry, BTreeMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterPayload, BroadcasterTokenDto, BroadcasterUpdateMessage,
};
use state_history::{
    Backend, BaseFeeSource, BlockTimeObservation, CapturedState, CheckpointCapture, CheckpointKind,
    CheckpointObjectStore, DeltaBackendCursor, DeltaEntry, NoBaseFee, StateHistoryStore,
    StateHistoryWriter, WriterConfig, WriterHandle,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::broadcaster::gas_price::RpcBaseFeeSource;
use crate::broadcaster::state::BroadcasterSnapshotExport;
use crate::config::StateHistoryConfig;
use crate::models::tokens::TokenStore;

pub use state_history::{StreamPosition, WriterStatus};

pub struct StateHistoryRuntime {
    pub handle: WriterHandle,
    tokens: Arc<TokenStore>,
    token_s3_prefix: String,
    rfq_high_water_ms: AtomicU64,
    checkpoint_task: Mutex<Option<StateHistoryCheckpointTask>>,
    boundary_requests: mpsc::UnboundedSender<BoundaryCheckpointRequest>,
    boundary_request_receiver: Mutex<Option<mpsc::UnboundedReceiver<BoundaryCheckpointRequest>>>,
}

struct StateHistoryCheckpointTask {
    cancellation: CancellationToken,
    join: JoinHandle<()>,
}

pub struct StateHistoryUpdateProjection {
    backends: Vec<DeltaBackendCursor>,
    block_times: Vec<BlockTimeObservation>,
}

impl StateHistoryUpdateProjection {
    pub fn from_update(update: &BroadcasterUpdateMessage) -> Result<Self> {
        let mut backends = Vec::with_capacity(update.partitions.len());
        let mut block_times = BTreeMap::new();
        for partition in &update.partitions {
            let cursor = match partition.backend {
                BroadcasterBackend::Native | BroadcasterBackend::Vm => DeltaBackendCursor {
                    backend: backend(partition.backend),
                    block_number: Some(partition.block_number),
                    observed_at_ms: None,
                },
                BroadcasterBackend::Rfq => DeltaBackendCursor {
                    backend: Backend::Rfq,
                    block_number: None,
                    observed_at_ms: Some(seconds_to_ms(partition.block_number)?),
                },
            };
            backends.push(cursor);
            if partition.backend != BroadcasterBackend::Rfq {
                collect_message_block_times(&partition.messages, &mut block_times)?;
            }
        }
        Ok(Self {
            backends,
            block_times: block_times.into_values().collect(),
        })
    }

    fn rfq_observed_at_ms(&self) -> Option<u64> {
        self.backends
            .iter()
            .filter(|cursor| cursor.backend == Backend::Rfq)
            .filter_map(|cursor| cursor.observed_at_ms)
            .max()
    }
}

pub(crate) struct BoundaryCheckpointRequest {
    pub(crate) position: StreamPosition,
    pub(crate) complete_native_block: Option<u64>,
    pub(crate) rfq_high_water_ms: Option<u64>,
    response: oneshot::Sender<Result<Option<u64>>>,
}

impl StateHistoryRuntime {
    fn new(handle: WriterHandle, tokens: Arc<TokenStore>, token_s3_prefix: String) -> Self {
        let (boundary_requests, boundary_request_receiver) = mpsc::unbounded_channel();
        Self {
            handle,
            tokens,
            token_s3_prefix,
            rfq_high_water_ms: AtomicU64::new(0),
            checkpoint_task: Mutex::new(None),
            boundary_requests,
            boundary_request_receiver: Mutex::new(Some(boundary_request_receiver)),
        }
    }

    pub(crate) async fn clone_token_catalog(&self) -> Vec<BroadcasterTokenDto> {
        self.tokens
            .snapshot()
            .await
            .into_values()
            .map(BroadcasterTokenDto::from)
            .collect()
    }

    pub(crate) fn checkpoint_capture(
        &self,
        state: CapturedState,
        tokens: Vec<BroadcasterTokenDto>,
    ) -> CheckpointCapture {
        CheckpointCapture::new(state, tokens, self.token_s3_prefix.clone())
    }

    pub fn rfq_high_water_ms(&self) -> Option<u64> {
        match self.rfq_high_water_ms.load(Ordering::Acquire) {
            0 => None,
            value => Some(value),
        }
    }

    pub(crate) fn observe_rfq_update(&self, update: &StateHistoryUpdateProjection) {
        if let Some(observed_at_ms) = update.rfq_observed_at_ms() {
            self.rfq_high_water_ms
                .fetch_max(observed_at_ms, Ordering::AcqRel);
        }
    }

    pub fn reset_rfq_high_water(&self) {
        self.rfq_high_water_ms.store(0, Ordering::Release);
    }

    pub fn observe_rfq_export(&self, export: &BroadcasterSnapshotExport) -> Result<()> {
        if let Some(observed_at_ms) = rfq_observed_at_ms_from_export(export)? {
            self.rfq_high_water_ms
                .fetch_max(observed_at_ms, Ordering::AcqRel);
        }
        Ok(())
    }

    pub(crate) fn set_checkpoint_task(
        &self,
        cancellation: CancellationToken,
        join: JoinHandle<()>,
    ) {
        *lock(&self.checkpoint_task) = Some(StateHistoryCheckpointTask { cancellation, join });
    }

    pub(crate) fn take_boundary_request_receiver(
        &self,
    ) -> Result<mpsc::UnboundedReceiver<BoundaryCheckpointRequest>> {
        lock(&self.boundary_request_receiver)
            .take()
            .context("state history checkpoint task is already running")
    }

    pub(crate) async fn request_boundary_checkpoint(
        &self,
        position: StreamPosition,
        complete_native_block: Option<u64>,
        rfq_high_water_ms: Option<u64>,
    ) -> Result<Option<u64>> {
        let (response, receiver) = oneshot::channel();
        self.boundary_requests
            .send(BoundaryCheckpointRequest {
                position,
                complete_native_block,
                rfq_high_water_ms,
                response,
            })
            .context("state history checkpoint task is not running")?;
        receiver
            .await
            .context("state history checkpoint task stopped before responding")?
    }

    pub(crate) fn respond_to_boundary_request(
        request: BoundaryCheckpointRequest,
        result: Result<Option<u64>>,
    ) {
        let _ = request.response.send(result);
    }

    pub async fn stop_checkpoint_task(&self) {
        let task = lock(&self.checkpoint_task).take();
        if let Some(task) = task {
            task.cancellation.cancel();
            if let Err(error) = task.join.await {
                tracing::warn!(
                    event = "state_history_checkpoint_task_shutdown_failed",
                    %error,
                    "state history checkpoint task failed during shutdown"
                );
            }
        }
    }
}

pub async fn build_state_history_runtime(
    config: &StateHistoryConfig,
    rpc_url: Option<String>,
    tokens: Arc<TokenStore>,
) -> Result<Option<StateHistoryRuntime>> {
    let store = StateHistoryStore::connect(&config.database_url).await?;
    store.validate_schema().await?;
    let objects = CheckpointObjectStore::from_env_config(
        config.s3_bucket.clone(),
        config.s3_prefix.clone(),
        config.s3_region.clone(),
        config.s3_endpoint_url.clone(),
        config.s3_force_path_style,
    )
    .await;
    let base_fee: Arc<dyn BaseFeeSource> = match rpc_url {
        Some(rpc_url) => Arc::new(RpcBaseFeeSource::new(rpc_url)),
        None => Arc::new(NoBaseFee),
    };
    let handle = StateHistoryWriter::spawn(
        WriterConfig {
            queue_capacity: config.queue_capacity,
            retry_window: config.retry_window,
        },
        store,
        objects,
        base_fee,
    );
    Ok(Some(StateHistoryRuntime::new(
        handle,
        tokens,
        config.s3_prefix.clone(),
    )))
}

#[cfg(test)]
pub(crate) fn test_state_history_runtime(
    queue_capacity: usize,
) -> (Arc<StateHistoryRuntime>, state_history::TestWriterProbe) {
    test_state_history_runtime_with_tokens(queue_capacity, Vec::new())
}

#[cfg(test)]
pub(crate) fn test_state_history_runtime_with_tokens(
    queue_capacity: usize,
    tokens: Vec<tycho_simulation::tycho_common::models::token::Token>,
) -> (Arc<StateHistoryRuntime>, state_history::TestWriterProbe) {
    let tokens = Arc::new(TokenStore::local_only(
        tokens
            .into_iter()
            .map(|token| (token.address.clone(), token))
            .collect(),
        tycho_simulation::tycho_common::models::Chain::Ethereum,
        std::time::Duration::from_secs(1),
    ));
    test_state_history_runtime_with_store(queue_capacity, tokens)
}

#[cfg(test)]
pub(crate) fn test_state_history_runtime_with_store(
    queue_capacity: usize,
    tokens: Arc<TokenStore>,
) -> (Arc<StateHistoryRuntime>, state_history::TestWriterProbe) {
    let (handle, probe) = state_history::test_writer_handle(queue_capacity);
    (
        Arc::new(StateHistoryRuntime::new(
            handle,
            tokens,
            "writer".to_owned(),
        )),
        probe,
    )
}

pub fn delta_entry_from_update(
    chain_id: u64,
    generation: u64,
    message_seq: u64,
    observed_at_ms: u64,
    update: StateHistoryUpdateProjection,
    payload_json: String,
) -> Result<Option<DeltaEntry>> {
    if update.backends.is_empty() {
        return Ok(None);
    }

    Ok(Some(DeltaEntry::new(
        chain_id,
        StreamPosition {
            generation,
            message_seq,
        },
        observed_at_ms,
        payload_json,
        update.backends,
        update.block_times,
    )?))
}

pub fn captured_state_from_export(
    kind: CheckpointKind,
    export: &BroadcasterSnapshotExport,
    position: StreamPosition,
    state_version: u64,
    block_number: u64,
    rfq_observed_at_ms: Option<u64>,
) -> Result<CapturedState> {
    let mut chain_id = None;
    let mut backends = Vec::new();
    let mut backend_heads = BTreeMap::new();
    let mut block_times = BTreeMap::new();
    let mut payloads_json = Vec::with_capacity(export.payloads.len());

    for payload in &export.payloads {
        payloads_json.push(
            serde_json::to_string(payload).context("failed to serialize checkpoint payload")?,
        );
        match payload {
            BroadcasterPayload::SnapshotStart(start) => {
                chain_id = Some(start.chain_id);
                backends = start.backends.iter().copied().map(backend).collect();
            }
            BroadcasterPayload::SnapshotChunk(chunk) => {
                for partition in &chunk.partitions {
                    if !matches!(
                        partition.backend,
                        BroadcasterBackend::Native | BroadcasterBackend::Vm
                    ) {
                        continue;
                    }
                    if let Some(previous) =
                        backend_heads.insert(partition.backend, partition.block_number)
                    {
                        anyhow::ensure!(
                            previous == partition.block_number,
                            "checkpoint backend {} has inconsistent heads",
                            partition.backend
                        );
                    }
                    collect_message_block_times(&partition.messages, &mut block_times)?;
                }
            }
            BroadcasterPayload::SnapshotEnd(_) => {}
            _ => anyhow::bail!("checkpoint export contains a non-snapshot payload"),
        }
    }

    let mut chain_heads = backend_heads.values().copied();
    if let Some(first) = chain_heads.next() {
        anyhow::ensure!(
            chain_heads.all(|head| head == first) && first == block_number,
            "checkpoint requires aligned native and VM heads at block {block_number}"
        );
    }
    let chain_id = chain_id.context("checkpoint export is missing snapshot_start")?;
    let rfq_observed_at_ms = match (rfq_observed_at_ms, rfq_observed_at_ms_from_export(export)?) {
        (Some(recorded), Some(snapshot)) => Some(recorded.max(snapshot)),
        (recorded, snapshot) => recorded.or(snapshot),
    };
    Ok(CapturedState::new(
        chain_id,
        position,
        Some(state_version),
        kind,
        block_number,
        rfq_observed_at_ms,
        backends,
        payloads_json,
        block_times.into_values().collect(),
    )?)
}

fn rfq_observed_at_ms_from_export(export: &BroadcasterSnapshotExport) -> Result<Option<u64>> {
    export
        .payloads
        .iter()
        .filter_map(|payload| match payload {
            BroadcasterPayload::SnapshotChunk(chunk) => Some(&chunk.partitions),
            _ => None,
        })
        .flatten()
        .filter(|partition| partition.backend == BroadcasterBackend::Rfq)
        .map(|partition| seconds_to_ms(partition.block_number))
        .try_fold(None, |current, observed| {
            let observed = observed?;
            Ok(Some(
                current.map_or(observed, |current: u64| current.max(observed)),
            ))
        })
}

fn collect_message_block_times(
    messages: &[simulator_core::broadcaster::BroadcasterProtocolMessage],
    block_times: &mut BTreeMap<u64, BlockTimeObservation>,
) -> Result<()> {
    for message in messages {
        let header = &message.message.header;
        let observation = BlockTimeObservation {
            block_number: header.number,
            timestamp_ms: seconds_to_ms(header.timestamp)?,
            block_hash: Some(header.hash.to_string()),
            parent_hash: Some(header.parent_hash.to_string()),
        };
        match block_times.entry(header.number) {
            Entry::Vacant(entry) => {
                entry.insert(observation);
            }
            Entry::Occupied(entry) => anyhow::ensure!(
                entry.get() == &observation,
                "conflicting block metadata at height {}",
                header.number
            ),
        }
    }
    Ok(())
}

const fn backend(backend: BroadcasterBackend) -> Backend {
    match backend {
        BroadcasterBackend::Native => Backend::Native,
        BroadcasterBackend::Vm => Backend::Vm,
        BroadcasterBackend::Rfq => Backend::Rfq,
    }
}

fn seconds_to_ms(seconds: u64) -> Result<u64> {
    seconds
        .checked_mul(1_000)
        .context("timestamp seconds overflow milliseconds")
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use anyhow::{Context, Result};
    use simulator_core::broadcaster::{
        BroadcasterBackend, BroadcasterEnvelope, BroadcasterPayload,
    };
    use state_history::{Backend, CheckpointKind, StreamPosition};

    use super::{
        captured_state_from_export, delta_entry_from_update, StateHistoryUpdateProjection,
    };
    use crate::broadcaster::state::BroadcasterSnapshotExport;

    const NATIVE_UPDATE: &str =
        include_str!("../../../simulator-core/tests/fixtures/wire/native_update.json");
    const RFQ_UPDATE: &str =
        include_str!("../../../simulator-core/tests/fixtures/wire/rfq_update.json");
    const SNAPSHOT_START: &str =
        include_str!("../../../simulator-core/tests/fixtures/wire/snapshot_start.json");
    const SNAPSHOT_CHUNK: &str =
        include_str!("../../../simulator-core/tests/fixtures/wire/snapshot_chunk.json");
    const SNAPSHOT_END: &str =
        include_str!("../../../simulator-core/tests/fixtures/wire/snapshot_end.json");

    #[test]
    fn update_with_native_and_vm_partitions_yields_two_cursors_and_block_times() -> Result<()> {
        let envelope: BroadcasterEnvelope = serde_json::from_str(NATIVE_UPDATE)?;
        let BroadcasterPayload::Update(mut update) = envelope.payload else {
            unreachable!();
        };
        let mut vm = update.partitions[0].clone();
        vm.backend = BroadcasterBackend::Vm;
        vm.block_number += 1;
        vm.messages[0].message.header.timestamp = 1_700_000_000;
        update.partitions[0].messages[0].message.header.timestamp = 1_700_000_000;
        update.partitions.push(vm);

        let projection = StateHistoryUpdateProjection::from_update(&update)?;
        let entry = delta_entry_from_update(
            8453,
            3,
            9,
            1_800_000_000_000,
            projection,
            NATIVE_UPDATE.into(),
        )?
        .context("state-changing update was skipped")?;

        assert_eq!(entry.backends().len(), 2);
        assert_eq!(entry.backends()[0].backend, Backend::Native);
        assert_eq!(
            entry.backends()[0].block_number,
            Some(update.partitions[0].block_number)
        );
        assert_eq!(entry.backends()[1].backend, Backend::Vm);
        assert_eq!(
            entry.backends()[1].block_number,
            Some(update.partitions[1].block_number)
        );
        assert!(entry
            .block_times()
            .iter()
            .any(|block| block.timestamp_ms == 1_700_000_000_000));
        Ok(())
    }

    #[test]
    fn identical_block_metadata_is_deduplicated() -> Result<()> {
        let envelope: BroadcasterEnvelope = serde_json::from_str(NATIVE_UPDATE)?;
        let BroadcasterPayload::Update(mut update) = envelope.payload else {
            unreachable!();
        };
        let message_count = update.partitions[0].messages.len();
        let duplicate = update.partitions[0].messages[0].clone();
        update.partitions[0].messages.push(duplicate);

        let projection = StateHistoryUpdateProjection::from_update(&update)?;

        assert_eq!(projection.block_times.len(), message_count);
        Ok(())
    }

    #[test]
    fn conflicting_block_metadata_at_same_height_is_rejected() -> Result<()> {
        let envelope: BroadcasterEnvelope = serde_json::from_str(NATIVE_UPDATE)?;
        let BroadcasterPayload::Update(update) = envelope.payload else {
            unreachable!();
        };
        let original = update.partitions[0].messages[0].clone();
        let block_number = original.message.header.number;

        let mut conflicting_hash = original.clone();
        conflicting_hash.message.header.hash = original.message.header.parent_hash.clone();
        let mut conflicting_update = update;
        conflicting_update.partitions[0]
            .messages
            .push(conflicting_hash);

        let Err(error) = StateHistoryUpdateProjection::from_update(&conflicting_update) else {
            anyhow::bail!("conflicting block metadata was accepted");
        };
        assert!(error.to_string().contains(&format!(
            "conflicting block metadata at height {block_number}"
        )));
        Ok(())
    }

    #[test]
    fn rfq_partition_maps_to_observed_at_ms_cursor() -> Result<()> {
        let envelope: BroadcasterEnvelope = serde_json::from_str(RFQ_UPDATE)?;
        let BroadcasterPayload::Update(update) = envelope.payload else {
            unreachable!();
        };

        let projection = StateHistoryUpdateProjection::from_update(&update)?;
        let entry = delta_entry_from_update(
            8453,
            3,
            10,
            1_800_000_000_000,
            projection,
            RFQ_UPDATE.into(),
        )?
        .context("state-changing update was skipped")?;

        assert_eq!(entry.backends().len(), 1);
        assert_eq!(entry.backends()[0].backend, Backend::Rfq);
        assert_eq!(entry.backends()[0].block_number, None);
        assert_eq!(entry.backends()[0].observed_at_ms, Some(1_710_000_000_000));
        Ok(())
    }

    #[test]
    fn export_with_mismatched_block_heads_is_rejected_by_captured_state() -> Result<()> {
        let mut export = snapshot_export()?;
        let block_number = {
            let BroadcasterPayload::SnapshotChunk(chunk) = &mut export.payloads[1] else {
                unreachable!();
            };
            let mut vm = chunk.partitions[0].clone();
            vm.backend = BroadcasterBackend::Vm;
            vm.block_number += 1;
            chunk.partitions.push(vm);
            chunk.partitions[0].block_number
        };

        let Err(error) = captured_state_from_export(
            CheckpointKind::Interval,
            &export,
            StreamPosition {
                generation: 2,
                message_seq: 12,
            },
            7,
            block_number,
            Some(1_710_000_000_000),
        ) else {
            anyhow::bail!("mismatched native and VM heads were accepted");
        };

        assert!(error.to_string().contains("aligned"));
        Ok(())
    }

    #[test]
    fn snapshot_export_payloads_land_in_capture_order() -> Result<()> {
        let export = snapshot_export()?;
        let capture = captured_state_from_export(
            CheckpointKind::Boundary,
            &export,
            StreamPosition {
                generation: 2,
                message_seq: 1,
            },
            7,
            19_000_000,
            Some(1_710_000_000_000),
        )?;

        let kinds = capture
            .payloads_json()
            .iter()
            .map(|payload| serde_json::from_str::<BroadcasterPayload>(payload))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|payload| payload.kind())
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            export
                .payloads
                .iter()
                .map(BroadcasterPayload::kind)
                .collect::<Vec<_>>()
        );
        Ok(())
    }

    fn snapshot_export() -> Result<BroadcasterSnapshotExport> {
        let envelopes = [SNAPSHOT_START, SNAPSHOT_CHUNK, SNAPSHOT_END]
            .into_iter()
            .map(serde_json::from_str::<BroadcasterEnvelope>)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(BroadcasterSnapshotExport {
            stream_id: envelopes[0].stream_id.clone(),
            snapshot_id: "wire-fixture-snapshot".to_string(),
            max_payload_bytes: 8_388_608,
            payloads: envelopes
                .into_iter()
                .map(|envelope| envelope.payload)
                .collect(),
        })
    }
}
