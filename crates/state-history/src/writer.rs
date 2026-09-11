use std::{
    cmp,
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use simulator_core::broadcaster::BroadcasterTokenDto;
use sqlx::error::ErrorKind;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{Instant, MissedTickBehavior},
};

use crate::{
    encode_archive, encode_token_snapshot, token_snapshot_s3_key, ArchiveMetadata, CapturedState,
    CheckpointArchive, CheckpointKind, CheckpointObjectStore, DeltaEntry, EncodedTokenSnapshot,
    GapReason, GapRecord, StateHistoryStore, StreamPosition, TokenSnapshot, TokenSnapshotRef,
    ARCHIVE_SCHEMA_VERSION,
};

const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(100);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(2);
const GAP_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const GAP_SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

pub struct WriterConfig {
    pub queue_capacity: usize,
    pub retry_window: Duration,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 8_192,
            retry_window: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointCapture {
    state: CapturedState,
    token_snapshot: TokenSnapshot,
    token_s3_prefix: String,
}

impl CheckpointCapture {
    // The snapshot's chain_id comes from the captured state, so a state/token chain mismatch is
    // unrepresentable rather than checked.
    pub fn new(
        state: CapturedState,
        tokens: Vec<BroadcasterTokenDto>,
        token_s3_prefix: String,
    ) -> Self {
        let token_snapshot = TokenSnapshot {
            chain_id: state.chain_id(),
            tokens,
        };
        Self {
            state,
            token_snapshot,
            token_s3_prefix,
        }
    }

    pub const fn state(&self) -> &CapturedState {
        &self.state
    }

    pub const fn token_snapshot(&self) -> &TokenSnapshot {
        &self.token_snapshot
    }
}

pub trait BaseFeeSource: Send + Sync {
    fn base_fee_wei<'a>(
        &'a self,
        chain_id: u64,
        block_number: u64,
        block_hash: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>>;
}

pub struct NoBaseFee;

impl BaseFeeSource for NoBaseFee {
    fn base_fee_wei<'a>(
        &'a self,
        _chain_id: u64,
        _block_number: u64,
        _block_hash: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>> {
        Box::pin(async { None })
    }
}

pub struct StateHistoryWriter;

impl StateHistoryWriter {
    pub fn spawn(
        config: WriterConfig,
        store: StateHistoryStore,
        objects: CheckpointObjectStore,
        base_fee: Arc<dyn BaseFeeSource>,
    ) -> WriterHandle {
        #[cfg(test)]
        {
            Self::spawn_inner(config, store, objects, base_fee, None)
        }
        #[cfg(not(test))]
        {
            Self::spawn_inner(config, store, objects, base_fee)
        }
    }

    #[cfg(test)]
    fn spawn_with_token_put_probe(
        config: WriterConfig,
        store: StateHistoryStore,
        objects: CheckpointObjectStore,
        base_fee: Arc<dyn BaseFeeSource>,
        token_put_probe: Arc<TokenPutProbe>,
    ) -> WriterHandle {
        Self::spawn_inner(config, store, objects, base_fee, Some(token_put_probe))
    }

    fn spawn_inner(
        config: WriterConfig,
        store: StateHistoryStore,
        objects: CheckpointObjectStore,
        base_fee: Arc<dyn BaseFeeSource>,
        #[cfg(test)] token_put_probe: Option<Arc<TokenPutProbe>>,
    ) -> WriterHandle {
        let queue_capacity = config.queue_capacity;
        let (command_sender, command_receiver) = mpsc::channel(queue_capacity);
        let gaps = Arc::new(Mutex::new(GapAccumulator::default()));
        let (shutdown_deadline, shutdown_deadline_receiver) = watch::channel(None);
        let (status_sender, _) = watch::channel(WriterStatus::new(queue_capacity));
        let task = WriterTask {
            config,
            store,
            objects,
            base_fee,
            commands: command_receiver,
            gaps: Arc::clone(&gaps),
            status: status_sender.clone(),
            shutdown_deadline: shutdown_deadline_receiver,
            last_persisted_token: None,
            #[cfg(test)]
            token_put_probe,
        };
        let join = tokio::spawn(task.run());

        WriterHandle {
            inner: Arc::new(WriterHandleInner {
                commands: Mutex::new(Some(command_sender)),
                gaps,
                status: status_sender,
                shutdown_deadline,
                join: Mutex::new(Some(join)),
            }),
        }
    }
}

#[derive(Clone)]
pub struct WriterHandle {
    inner: Arc<WriterHandleInner>,
}

impl WriterHandle {
    pub fn enqueue_delta(&self, delta: DeltaEntry) -> bool {
        let commands = lock(&self.inner.commands);
        let Some(sender) = commands.as_ref() else {
            self.record_dropped_delta(delta, 0, GapReason::WriterStopped);
            return false;
        };

        match sender.try_send(WriterCommand::Delta(delta)) {
            Ok(()) => {
                let queue_len = queue_len(sender);
                self.inner.status.send_modify(|status| {
                    status.enqueued_deltas += 1;
                    status.queue_len = queue_len;
                });
                true
            }
            Err(error) => {
                let queue_len = queue_len(sender);
                let (command, reason) = match error {
                    mpsc::error::TrySendError::Full(command) => (command, GapReason::QueueOverflow),
                    mpsc::error::TrySendError::Closed(command) => {
                        (command, GapReason::WriterStopped)
                    }
                };
                let WriterCommand::Delta(delta) = command else {
                    return false;
                };
                self.record_dropped_delta(delta, queue_len, reason);
                false
            }
        }
    }

    pub fn request_checkpoint(&self, capture: CheckpointCapture) -> bool {
        let commands = lock(&self.inner.commands);
        let Some(sender) = commands.as_ref() else {
            self.record_checkpoint_enqueue_failure(&capture, 0, "writer_stopped");
            return false;
        };
        match sender.try_send(WriterCommand::Checkpoint(capture)) {
            Ok(()) => {
                let queue_len = queue_len(sender);
                self.inner
                    .status
                    .send_modify(|status| status.queue_len = queue_len);
                true
            }
            Err(error) => {
                let queue_len = queue_len(sender);
                let (command, reason) = match error {
                    mpsc::error::TrySendError::Full(command) => (command, "queue_overflow"),
                    mpsc::error::TrySendError::Closed(command) => (command, "writer_stopped"),
                };
                let WriterCommand::Checkpoint(capture) = command else {
                    return false;
                };
                self.record_checkpoint_enqueue_failure(&capture, queue_len, reason);
                false
            }
        }
    }

    pub fn record_boundary_failure_gap(
        &self,
        chain_id: u64,
        position: StreamPosition,
        block_number: Option<u64>,
        rfq_observed_at_ms: Option<u64>,
    ) {
        lock(&self.inner.gaps).merge(GapRecord {
            chain_id,
            generation: position.generation,
            from_message_seq: position.message_seq,
            to_message_seq: position.message_seq,
            reason: GapReason::CheckpointFailed,
            from_block_number: block_number,
            to_block_number: block_number,
            from_observed_at_ms: rfq_observed_at_ms,
            to_observed_at_ms: rfq_observed_at_ms,
        });
    }

    pub fn record_delta_preparation_failure_gap(&self, chain_id: u64, position: StreamPosition) {
        lock(&self.inner.gaps).merge(GapRecord {
            chain_id,
            generation: position.generation,
            from_message_seq: position.message_seq,
            to_message_seq: position.message_seq,
            reason: GapReason::WriteFailed,
            from_block_number: None,
            to_block_number: None,
            from_observed_at_ms: None,
            to_observed_at_ms: None,
        });
    }

    pub fn record_boundary_slip_gap(
        &self,
        chain_id: u64,
        from: StreamPosition,
        to: StreamPosition,
        block_bounds: (Option<u64>, Option<u64>),
        observed_at_ms_bounds: (Option<u64>, Option<u64>),
    ) {
        debug_assert_eq!(from.generation, to.generation);
        let (from_block_number, to_block_number) = block_bounds;
        let (from_observed_at_ms, to_observed_at_ms) = observed_at_ms_bounds;
        lock(&self.inner.gaps).merge(GapRecord {
            chain_id,
            generation: from.generation,
            from_message_seq: from.message_seq,
            to_message_seq: to.message_seq,
            reason: GapReason::BoundarySlip,
            from_block_number,
            to_block_number,
            from_observed_at_ms,
            to_observed_at_ms,
        });
    }

    pub fn status(&self) -> WriterStatus {
        self.inner.status.borrow().clone()
    }

    pub async fn shutdown(self, drain_timeout: Duration) {
        let deadline = Instant::now() + drain_timeout;
        self.inner.shutdown_deadline.send_replace(Some(deadline));
        drop(lock(&self.inner.commands).take());
        let join = lock(&self.inner.join).take();
        if let Some(join) = join {
            match join.await {
                Ok(()) => {
                    tracing::info!(
                        event = "state_history_shutdown_complete",
                        "state history writer shutdown complete"
                    );
                }
                Err(error) => {
                    tracing::error!(
                        event = "state_history_writer_shutdown_failed",
                        %error,
                        "state history writer task failed"
                    );
                    self.inner.status.send_modify(|status| {
                        status.healthy = false;
                        status.last_error = Some(error.to_string());
                    });
                }
            }
        }
    }

    fn record_dropped_delta(&self, delta: DeltaEntry, queue_len: u64, reason: GapReason) {
        // Gap recording bypasses the channel so it still works when the queue is the problem.
        let position = delta.position();
        tracing::error!(
            event = "state_history_delta_enqueue_failed",
            chain_id = delta.chain_id(),
            generation = position.generation,
            message_seq = position.message_seq,
            reason = reason.as_str(),
            queue_len,
            "state history delta enqueue failed"
        );
        lock(&self.inner.gaps).merge(delta_gap(&delta, reason));
        self.inner.status.send_modify(|status| {
            status.dropped_deltas += 1;
            status.queue_len = queue_len;
        });
    }

    fn record_checkpoint_enqueue_failure(
        &self,
        capture: &CheckpointCapture,
        queue_len: u64,
        reason: &'static str,
    ) {
        let state = capture.state();
        tracing::error!(
            event = "state_history_checkpoint_enqueue_failed",
            chain_id = state.chain_id(),
            generation = state.position().generation,
            message_seq = state.position().message_seq,
            checkpoint_kind = state.kind().as_str(),
            block_number = state.block_number(),
            reason,
            queue_len,
            "state history checkpoint enqueue failed"
        );
        self.inner.status.send_modify(|status| {
            status.healthy = false;
            status.queue_len = queue_len;
            status.checkpoints_failed += 1;
            status.last_checkpoint_status = Some("enqueue_failed".to_owned());
            status.last_error = Some(format!("checkpoint enqueue failed: {reason}"));
        });
    }
}

#[cfg(feature = "test-util")]
#[derive(Clone)]
pub struct TestWriterProbe {
    checkpoints: Arc<Mutex<Vec<CapturedState>>>,
    token_snapshots: Arc<Mutex<Vec<TokenSnapshot>>>,
    gaps: Arc<Mutex<GapAccumulator>>,
}

#[cfg(feature = "test-util")]
impl TestWriterProbe {
    pub fn checkpoints(&self) -> Vec<CapturedState> {
        lock(&self.checkpoints).clone()
    }

    pub fn token_snapshots(&self) -> Vec<TokenSnapshot> {
        lock(&self.token_snapshots).clone()
    }

    pub fn gaps(&self) -> Vec<GapRecord> {
        lock(&self.gaps).pending.clone()
    }
}

#[cfg(feature = "test-util")]
pub fn test_writer_handle(queue_capacity: usize) -> (WriterHandle, TestWriterProbe) {
    let (command_sender, mut command_receiver) = mpsc::channel(queue_capacity);
    let gaps = Arc::new(Mutex::new(GapAccumulator::default()));
    let (shutdown_deadline, _) = watch::channel(None);
    let (status, _) = watch::channel(WriterStatus::new(queue_capacity));
    let checkpoints = Arc::new(Mutex::new(Vec::new()));
    let token_snapshots = Arc::new(Mutex::new(Vec::new()));
    let task_checkpoints = Arc::clone(&checkpoints);
    let task_token_snapshots = Arc::clone(&token_snapshots);
    let probe_gaps = Arc::clone(&gaps);
    let task_status = status.clone();
    let join = tokio::spawn(async move {
        while let Some(command) = command_receiver.recv().await {
            match command {
                WriterCommand::Delta(delta) => {
                    task_status.send_modify(|status| {
                        status.queue_len =
                            u64::try_from(command_receiver.len()).unwrap_or(u64::MAX);
                        status.persisted_deltas += 1;
                        status.last_persisted = Some(delta.position());
                        status.last_persisted_at_ms = Some(unix_timestamp_ms());
                    });
                }
                WriterCommand::Checkpoint(capture) => {
                    lock(&task_checkpoints).push(capture.state);
                    lock(&task_token_snapshots).push(capture.token_snapshot);
                    task_status.send_modify(|status| {
                        status.queue_len =
                            u64::try_from(command_receiver.len()).unwrap_or(u64::MAX);
                        status.checkpoints_completed += 1;
                        status.last_checkpoint_status = Some("complete".to_string());
                        status.last_successful_checkpoint_at_ms = Some(unix_timestamp_ms());
                    });
                }
            }
        }
    });
    let handle = WriterHandle {
        inner: Arc::new(WriterHandleInner {
            commands: Mutex::new(Some(command_sender)),
            gaps,
            status,
            shutdown_deadline,
            join: Mutex::new(Some(join)),
        }),
    };
    (
        handle,
        TestWriterProbe {
            checkpoints,
            token_snapshots,
            gaps: probe_gaps,
        },
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterStatus {
    pub healthy: bool,
    pub queue_len: u64,
    pub queue_capacity: u64,
    pub enqueued_deltas: u64,
    pub persisted_deltas: u64,
    pub dropped_deltas: u64,
    pub failed_deltas: u64,
    pub recorded_gaps: u64,
    pub last_persisted: Option<StreamPosition>,
    pub last_persisted_at_ms: Option<u64>,
    pub last_error: Option<String>,
    pub checkpoints_completed: u64,
    pub checkpoints_failed: u64,
    pub last_checkpoint_status: Option<String>,
    pub last_successful_checkpoint_at_ms: Option<u64>,
    pub last_token_snapshot_sha: Option<String>,
    pub token_persistence_failures: u64,
}

impl WriterStatus {
    fn new(queue_capacity: usize) -> Self {
        Self {
            healthy: true,
            queue_len: 0,
            queue_capacity: u64::try_from(queue_capacity).unwrap_or(u64::MAX),
            enqueued_deltas: 0,
            persisted_deltas: 0,
            dropped_deltas: 0,
            failed_deltas: 0,
            recorded_gaps: 0,
            last_persisted: None,
            last_persisted_at_ms: None,
            last_error: None,
            checkpoints_completed: 0,
            checkpoints_failed: 0,
            last_checkpoint_status: None,
            last_successful_checkpoint_at_ms: None,
            last_token_snapshot_sha: None,
            token_persistence_failures: 0,
        }
    }
}

struct WriterHandleInner {
    commands: Mutex<Option<mpsc::Sender<WriterCommand>>>,
    gaps: Arc<Mutex<GapAccumulator>>,
    status: watch::Sender<WriterStatus>,
    shutdown_deadline: watch::Sender<Option<Instant>>,
    join: Mutex<Option<JoinHandle<()>>>,
}

enum WriterCommand {
    Delta(DeltaEntry),
    Checkpoint(CheckpointCapture),
}

#[cfg(test)]
#[derive(Default)]
struct TokenPutProbe {
    attempts: std::sync::atomic::AtomicU64,
    fail: std::sync::atomic::AtomicBool,
}

struct WriterTask {
    config: WriterConfig,
    store: StateHistoryStore,
    objects: CheckpointObjectStore,
    base_fee: Arc<dyn BaseFeeSource>,
    commands: mpsc::Receiver<WriterCommand>,
    gaps: Arc<Mutex<GapAccumulator>>,
    status: watch::Sender<WriterStatus>,
    shutdown_deadline: watch::Receiver<Option<Instant>>,
    last_persisted_token: Option<TokenSnapshotRef>,
    #[cfg(test)]
    token_put_probe: Option<Arc<TokenPutProbe>>,
}

impl WriterTask {
    async fn run(mut self) {
        let first_tick = Instant::now() + GAP_FLUSH_INTERVAL;
        let mut gap_tick = tokio::time::interval_at(first_tick, GAP_FLUSH_INTERVAL);
        gap_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            if self.drain_deadline_expired() {
                self.record_stopped_commands(None);
                break;
            }

            tokio::select! {
                command = self.commands.recv() => {
                    let Some(command) = command else {
                        break;
                    };
                    if self.drain_deadline_expired() {
                        self.record_stopped_commands(Some(command));
                        break;
                    }
                    self.update_queue_len();
                    self.process_command(command).await;
                    self.flush_gaps().await;
                }
                _ = gap_tick.tick() => self.flush_gaps().await,
            }
        }

        self.flush_gaps().await;
        self.status.send_modify(|status| {
            status.healthy = false;
            status.queue_len = 0;
        });
    }

    async fn process_command(&mut self, command: WriterCommand) {
        match command {
            WriterCommand::Delta(delta) => self.process_delta(delta).await,
            WriterCommand::Checkpoint(capture) => self.process_checkpoint(capture).await,
        }
    }

    async fn process_delta(&mut self, delta: DeltaEntry) {
        let position = delta.position();
        match self.persist_delta(&delta).await {
            Ok(()) => {
                self.status.send_modify(|status| {
                    status.healthy = true;
                    status.persisted_deltas += 1;
                    status.last_persisted = Some(position);
                    status.last_persisted_at_ms = Some(unix_timestamp_ms());
                    status.last_error = None;
                });
            }
            Err(error) => {
                tracing::error!(
                    event = "state_history_delta_persistence_failed",
                    chain_id = delta.chain_id(),
                    generation = position.generation,
                    message_seq = position.message_seq,
                    observed_at_ms = delta.observed_at_ms(),
                    retry_window_ms = u64::try_from(self.config.retry_window.as_millis())
                        .unwrap_or(u64::MAX),
                    %error,
                    "state history delta persistence failed"
                );
                lock(&self.gaps).merge(delta_gap(&delta, GapReason::WriteFailed));
                self.status.send_modify(|status| {
                    status.healthy = false;
                    status.failed_deltas += 1;
                    status.last_error = Some(error.to_string());
                });
            }
        }
    }

    async fn persist_delta(&mut self, delta: &DeltaEntry) -> anyhow::Result<()> {
        let base_fees = self.resolve_base_fees(delta).await;
        let started_at = Instant::now();
        let retry_deadline = started_at + self.config.retry_window;
        let shutdown_deadline = self.shutdown_deadline.clone();
        let mut delays = retry_delays(self.config.retry_window);
        let persistence = async {
            loop {
                let attempt = async {
                    let mut transaction = self
                        .store
                        .pool()
                        .begin()
                        .await
                        .context("failed to start delta transaction")?;
                    StateHistoryStore::insert_delta(&mut transaction, delta, &base_fees).await?;
                    transaction
                        .commit()
                        .await
                        .context("failed to commit delta transaction")
                };
                let result =
                    run_before_deadline(retry_deadline, "delta persistence", attempt).await;
                let Err(error) = result else {
                    return Ok(());
                };
                if is_permanent_persistence_error(&error) {
                    return Err(error);
                }
                let Some(delay) = delays.next() else {
                    return Err(error);
                };
                let Some(next_attempt_at) = Instant::now().checked_add(delay) else {
                    return Err(error);
                };
                if next_attempt_at > retry_deadline {
                    return Err(error);
                }
                tokio::time::sleep_until(next_attempt_at).await;
            }
        };
        run_until_shutdown_deadline(shutdown_deadline, "delta persistence", persistence).await
    }

    async fn resolve_base_fees(&self, delta: &DeltaEntry) -> BTreeMap<u64, String> {
        let mut resolved = BTreeMap::new();
        for observation in delta.block_times() {
            let Some(value) = run_until_shutdown_bound(
                self.shutdown_deadline.clone(),
                Duration::ZERO,
                self.base_fee.base_fee_wei(
                    delta.chain_id(),
                    observation.block_number,
                    observation.block_hash.as_deref(),
                ),
            )
            .await
            .flatten() else {
                continue;
            };
            resolved.insert(observation.block_number, value);
        }
        resolved
    }

    async fn process_checkpoint(&mut self, capture: CheckpointCapture) {
        let started_at = Instant::now();
        let chain_id = capture.state().chain_id();
        let position = capture.state().position();
        let kind = capture.state().kind();
        let block_number = capture.state().block_number();
        let state_version = capture.state().state_version();
        let rfq_observed_at_ms = capture.state().rfq_observed_at_ms();
        let boundary_gap = (capture.state().kind() == CheckpointKind::Boundary)
            .then(|| checkpoint_gap(capture.state(), GapReason::CheckpointFailed));
        match self.execute_checkpoint(capture).await {
            Ok(()) => {
                tracing::info!(
                    event = "state_history_checkpoint_completed",
                    chain_id,
                    generation = position.generation,
                    message_seq = position.message_seq,
                    checkpoint_kind = kind.as_str(),
                    ?state_version,
                    block_number,
                    ?rfq_observed_at_ms,
                    duration_ms =
                        u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "state history checkpoint completed"
                );
                self.status.send_modify(|status| {
                    status.healthy = true;
                    status.checkpoints_completed += 1;
                    status.last_checkpoint_status = Some("complete".to_owned());
                    status.last_successful_checkpoint_at_ms = Some(unix_timestamp_ms());
                    status.last_error = None;
                });
            }
            Err(error) => {
                tracing::error!(
                    event = "state_history_checkpoint_failed",
                    chain_id,
                    generation = position.generation,
                    message_seq = position.message_seq,
                    checkpoint_kind = kind.as_str(),
                    ?state_version,
                    block_number,
                    ?rfq_observed_at_ms,
                    duration_ms = u64::try_from(started_at.elapsed().as_millis())
                        .unwrap_or(u64::MAX),
                    %error,
                    "state history checkpoint failed"
                );
                if let Some(gap) = boundary_gap {
                    lock(&self.gaps).merge(gap);
                }
                self.status.send_modify(|status| {
                    status.healthy = false;
                    status.checkpoints_failed += 1;
                    status.last_checkpoint_status = Some("failed".to_owned());
                    status.last_error = Some(error.to_string());
                });
            }
        }
    }

    async fn execute_checkpoint(&mut self, capture: CheckpointCapture) -> anyhow::Result<()> {
        let CheckpointCapture {
            state,
            token_snapshot,
            token_s3_prefix,
        } = capture;
        let shutdown_deadline = self.shutdown_deadline.clone();
        let checkpoint_id =
            run_until_shutdown_deadline(shutdown_deadline.clone(), "checkpoint creation", async {
                let mut connection = self
                    .store
                    .pool()
                    .acquire()
                    .await
                    .context("failed to acquire checkpoint connection")?;
                StateHistoryStore::create_checkpoint(&mut connection, &state).await
            })
            .await?;

        let result = self
            .finish_checkpoint(
                checkpoint_id,
                &state,
                token_snapshot,
                &token_s3_prefix,
                shutdown_deadline.clone(),
            )
            .await;
        if let Err(error) = &result {
            let error_message = error.to_string();
            match run_until_shutdown_bound(
                shutdown_deadline,
                Duration::ZERO,
                StateHistoryStore::fail_checkpoint(
                    self.store.pool(),
                    checkpoint_id,
                    &error_message,
                ),
            )
            .await
            {
                Some(Ok(())) => {}
                Some(Err(mark_error)) => {
                    tracing::error!(
                        event = "state_history_checkpoint_status_update_failed",
                        checkpoint_id,
                        target_status = "failed",
                        %mark_error,
                        "failed to mark state history checkpoint as failed"
                    );
                }
                None => {
                    tracing::error!(
                        event = "state_history_checkpoint_status_update_failed",
                        checkpoint_id,
                        target_status = "failed",
                        reason = "timeout",
                        "timed out while marking state history checkpoint as failed"
                    );
                }
            }
        }
        result
    }

    async fn finish_checkpoint(
        &mut self,
        checkpoint_id: i64,
        capture: &CapturedState,
        token_snapshot: TokenSnapshot,
        token_s3_prefix: &str,
        shutdown_deadline: watch::Receiver<Option<Instant>>,
    ) -> anyhow::Result<()> {
        let archive = CheckpointArchive {
            metadata: ArchiveMetadata {
                schema_version: ARCHIVE_SCHEMA_VERSION,
                chain_id: capture.chain_id(),
                position: capture.position(),
                state_version: capture.state_version(),
                kind: capture.kind(),
                block_number: capture.block_number(),
                rfq_observed_at_ms: capture.rfq_observed_at_ms(),
                backends: capture.backends().to_vec(),
            },
            payloads_json: capture.payloads_json().to_vec(),
        };
        let (encoded, encoded_token_snapshot) =
            run_until_shutdown_deadline(shutdown_deadline.clone(), "checkpoint encoding", async {
                tokio::task::spawn_blocking(move || {
                    let encoded_archive = encode_archive(archive)?;
                    let encoded_token_snapshot = encode_token_snapshot(token_snapshot);
                    Ok::<_, anyhow::Error>((encoded_archive, encoded_token_snapshot))
                })
                .await
                .context("checkpoint encoding task failed")?
            })
            .await?;
        let key = self
            .objects
            .key_for(capture.chain_id(), capture.position(), capture.kind());
        let archive_info = encoded.info;
        run_until_shutdown_deadline(
            shutdown_deadline.clone(),
            "checkpoint upload",
            self.objects.put(&key, encoded.bytes),
        )
        .await?;
        let token_reference = self
            .resolve_token_reference(
                capture,
                token_s3_prefix,
                encoded_token_snapshot,
                shutdown_deadline.clone(),
            )
            .await;
        run_until_shutdown_deadline(shutdown_deadline, "checkpoint completion", async {
            let mut connection = self
                .store
                .pool()
                .acquire()
                .await
                .context("failed to acquire checkpoint completion connection")?;
            StateHistoryStore::complete_checkpoint(
                &mut connection,
                checkpoint_id,
                &key,
                &archive_info,
                token_reference.as_ref(),
                capture.block_times(),
                capture.position(),
                capture.chain_id(),
            )
            .await
        })
        .await
    }

    async fn resolve_token_reference(
        &mut self,
        capture: &CapturedState,
        token_s3_prefix: &str,
        encoded: anyhow::Result<EncodedTokenSnapshot>,
        shutdown_deadline: watch::Receiver<Option<Instant>>,
    ) -> Option<TokenSnapshotRef> {
        let encoded = match encoded {
            Ok(encoded) => encoded,
            Err(error) => {
                self.record_token_persistence_failure(capture, "encode", &error);
                return None;
            }
        };
        let reference = TokenSnapshotRef {
            s3_key: token_snapshot_s3_key(
                token_s3_prefix,
                capture.chain_id(),
                &encoded.info.sha256,
            ),
            sha256: encoded.info.sha256.clone(),
            token_count: encoded.info.token_count,
            token_bytes: encoded.info.token_bytes,
        };
        // Only the most recent successful PUT is memoized. The full key is compared,
        // not just the hash: a capture with a different S3 prefix must not reuse an
        // object persisted under the old one. A mismatch always writes.
        if self
            .last_persisted_token
            .as_ref()
            .is_some_and(|persisted| persisted.s3_key == reference.s3_key)
        {
            return Some(reference);
        }

        // A draining writer skips the PUT: the remaining deadline belongs to checkpoint
        // completion, and token snapshots are enrichment the next capture recreates.
        // Memo hits above still resolve because they cost nothing. Not a failure, so
        // the memo and failure counter stay untouched.
        if shutdown_deadline.borrow().is_some() {
            tracing::debug!(
                chain_id = capture.chain_id(),
                generation = capture.position().generation,
                message_seq = capture.position().message_seq,
                "skipped token snapshot persistence during writer drain"
            );
            return None;
        }

        match run_until_shutdown_bound(
            shutdown_deadline,
            Duration::ZERO,
            self.put_token_snapshot(&reference.s3_key, encoded.bytes),
        )
        .await
        {
            Some(Ok(())) => {}
            Some(Err(error)) => {
                self.record_token_persistence_failure(capture, "upload", &error);
                return None;
            }
            None => {
                tracing::debug!(
                    chain_id = capture.chain_id(),
                    generation = capture.position().generation,
                    message_seq = capture.position().message_seq,
                    "stopped token snapshot persistence during writer drain"
                );
                return None;
            }
        }

        self.last_persisted_token = Some(reference.clone());
        self.status.send_modify(|status| {
            status.last_token_snapshot_sha = Some(reference.sha256.clone());
        });
        Some(reference)
    }

    async fn put_token_snapshot(&self, key: &str, bytes: Vec<u8>) -> anyhow::Result<()> {
        #[cfg(test)]
        if let Some(probe) = &self.token_put_probe {
            probe
                .attempts
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if probe.fail.load(std::sync::atomic::Ordering::Relaxed) {
                anyhow::bail!("injected token snapshot PUT failure");
            }
        }
        self.objects.put(key, bytes).await
    }

    fn record_token_persistence_failure(
        &mut self,
        capture: &CapturedState,
        stage: &'static str,
        error: &anyhow::Error,
    ) {
        // A failed attempt invalidates the memo so identical content must prove durability again.
        self.last_persisted_token = None;
        tracing::warn!(
            event = "state_history_token_persistence_failed",
            chain_id = capture.chain_id(),
            generation = capture.position().generation,
            message_seq = capture.position().message_seq,
            stage,
            %error,
            "state history token snapshot persistence failed"
        );
        self.status.send_modify(|status| {
            status.token_persistence_failures += 1;
        });
    }

    async fn flush_gaps(&self) {
        let pending = lock(&self.gaps).drain();
        for gap in pending {
            self.persist_gap(gap).await;
        }
    }

    #[expect(
        clippy::excessive_nesting,
        reason = "The shutdown-bounded retry loop updates shared status inside each terminal outcome"
    )]
    async fn persist_gap(&self, gap: GapRecord) {
        // A lost gap record turns a known hole into an invisible one, so normal operation retries forever.
        let persistence = async {
            let mut delay = INITIAL_RETRY_DELAY;
            let mut reported_retry = false;
            loop {
                let result = async {
                    let mut connection = self
                        .store
                        .pool()
                        .acquire()
                        .await
                        .context("failed to acquire gap connection")?;
                    StateHistoryStore::record_gap(&mut connection, &gap).await
                }
                .await;
                let Err(error) = result else {
                    tracing::warn!(
                        event = "state_history_gap_recorded",
                        chain_id = gap.chain_id,
                        generation = gap.generation,
                        from_message_seq = gap.from_message_seq,
                        to_message_seq = gap.to_message_seq,
                        reason = gap.reason.as_str(),
                        "state history gap recorded"
                    );
                    self.status.send_modify(|status| {
                        status.healthy = true;
                        status.recorded_gaps += 1;
                        status.last_error = None;
                    });
                    return;
                };
                if is_permanent_persistence_error(&error) {
                    tracing::error!(
                        event = "state_history_gap_persistence_failed",
                        chain_id = gap.chain_id,
                        generation = gap.generation,
                        from_message_seq = gap.from_message_seq,
                        to_message_seq = gap.to_message_seq,
                        reason = gap.reason.as_str(),
                        %error,
                        "permanent state history gap persistence failure"
                    );
                    self.status.send_modify(|status| {
                        status.healthy = false;
                        status.last_error = Some(error.to_string());
                    });
                    return;
                }
                if !reported_retry {
                    tracing::warn!(
                        event = "state_history_gap_persistence_retrying",
                        chain_id = gap.chain_id,
                        generation = gap.generation,
                        from_message_seq = gap.from_message_seq,
                        to_message_seq = gap.to_message_seq,
                        reason = gap.reason.as_str(),
                        %error,
                        "state history gap persistence will retry"
                    );
                    reported_retry = true;
                }
                tokio::time::sleep(delay).await;
                delay = cmp::min(delay.saturating_mul(2), MAX_RETRY_DELAY);
            }
        };
        if run_until_shutdown_bound(
            self.shutdown_deadline.clone(),
            GAP_SHUTDOWN_GRACE,
            persistence,
        )
        .await
        .is_none()
        {
            tracing::error!(
                event = "state_history_gap_shutdown_dropped",
                chain_id = gap.chain_id,
                generation = gap.generation,
                from_message_seq = gap.from_message_seq,
                to_message_seq = gap.to_message_seq,
                reason = gap.reason.as_str(),
                "state history gap dropped after shutdown persistence deadline"
            );
        }
    }

    fn record_stopped_commands(&mut self, first: Option<WriterCommand>) {
        let mut stopped = GapAccumulator::default();
        // The first command was already received, so include it with the commands still queued.
        if let Some(command) = first {
            if let Some(gap) = command_gap(&command, GapReason::WriterStopped) {
                stopped.merge(gap);
            }
        }
        while let Ok(command) = self.commands.try_recv() {
            if let Some(gap) = command_gap(&command, GapReason::WriterStopped) {
                stopped.merge(gap);
            }
        }
        let mut gaps = lock(&self.gaps);
        for gap in stopped.drain() {
            gaps.merge(gap);
        }
        self.update_queue_len();
    }

    fn update_queue_len(&self) {
        let queue_len = u64::try_from(self.commands.len()).unwrap_or(u64::MAX);
        self.status
            .send_modify(|status| status.queue_len = queue_len);
    }

    fn shutdown_deadline(&self) -> Option<Instant> {
        *self.shutdown_deadline.borrow()
    }

    fn drain_deadline_expired(&self) -> bool {
        self.shutdown_deadline()
            .is_some_and(|deadline| Instant::now() >= deadline)
    }
}

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn queue_len(sender: &mpsc::Sender<WriterCommand>) -> u64 {
    let len = sender.max_capacity().saturating_sub(sender.capacity());
    u64::try_from(len).unwrap_or(u64::MAX)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn command_gap(command: &WriterCommand, reason: GapReason) -> Option<GapRecord> {
    match command {
        WriterCommand::Delta(delta) => Some(delta_gap(delta, reason)),
        // Interval checkpoints are replay accelerators, not lineage. Abandoning one
        // loses no data, so a gap here would fail ranges that replay fine, matching
        // the boundary-only rule in process_checkpoint.
        WriterCommand::Checkpoint(capture)
            if capture.state().kind() == CheckpointKind::Interval =>
        {
            None
        }
        WriterCommand::Checkpoint(capture) => Some(checkpoint_gap(capture.state(), reason)),
    }
}

fn checkpoint_gap(capture: &CapturedState, reason: GapReason) -> GapRecord {
    GapRecord {
        chain_id: capture.chain_id(),
        generation: capture.position().generation,
        from_message_seq: capture.position().message_seq,
        to_message_seq: capture.position().message_seq,
        reason,
        from_block_number: Some(capture.block_number()),
        to_block_number: Some(capture.block_number()),
        from_observed_at_ms: capture.rfq_observed_at_ms(),
        to_observed_at_ms: capture.rfq_observed_at_ms(),
    }
}

fn delta_gap(delta: &DeltaEntry, reason: GapReason) -> GapRecord {
    let position = delta.position();
    let from_block_number = delta
        .backends()
        .iter()
        .filter_map(|cursor| cursor.block_number)
        .min();
    let to_block_number = delta
        .backends()
        .iter()
        .filter_map(|cursor| cursor.block_number)
        .max();
    let from_observed_at_ms = delta
        .backends()
        .iter()
        .filter(|cursor| cursor.backend == crate::Backend::Rfq)
        .filter_map(|cursor| cursor.observed_at_ms)
        .min();
    let to_observed_at_ms = delta
        .backends()
        .iter()
        .filter(|cursor| cursor.backend == crate::Backend::Rfq)
        .filter_map(|cursor| cursor.observed_at_ms)
        .max();
    GapRecord {
        chain_id: delta.chain_id(),
        generation: position.generation,
        from_message_seq: position.message_seq,
        to_message_seq: position.message_seq,
        reason,
        from_block_number,
        to_block_number,
        from_observed_at_ms,
        to_observed_at_ms,
    }
}

async fn run_before_deadline<T>(
    deadline: Instant,
    operation: &str,
    future: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    tokio::time::timeout_at(deadline, future)
        .await
        .with_context(|| format!("{operation} exceeded its retry deadline"))?
}

async fn run_until_shutdown_deadline<T>(
    shutdown_deadline: watch::Receiver<Option<Instant>>,
    operation: &str,
    future: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    run_until_shutdown_bound(shutdown_deadline, Duration::ZERO, future)
        .await
        .with_context(|| format!("{operation} exceeded the shutdown drain timeout"))?
}

async fn run_until_shutdown_bound<T>(
    mut shutdown_deadline: watch::Receiver<Option<Instant>>,
    grace: Duration,
    future: impl Future<Output = T>,
) -> Option<T> {
    tokio::pin!(future);
    let deadline = *shutdown_deadline.borrow_and_update();
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline + grace, future).await.ok(),
        None => {
            tokio::select! {
                value = &mut future => Some(value),
                changed = shutdown_deadline.changed() => {
                    if changed.is_err() {
                        return Some(future.await);
                    }
                    let deadline = *shutdown_deadline.borrow_and_update();
                    match deadline {
                        Some(deadline) => tokio::time::timeout_at(deadline + grace, future)
                            .await
                            .ok(),
                        None => Some(future.await),
                    }
                }
            }
        }
    }
}

#[derive(Default)]
struct GapAccumulator {
    pending: Vec<GapRecord>,
}

impl GapAccumulator {
    fn merge(&mut self, gap: GapRecord) {
        if let Some(last) = self.pending.last_mut() {
            if last.chain_id == gap.chain_id
                && last.generation == gap.generation
                && last.reason == gap.reason
                && has_cursor_bounds(last) == has_cursor_bounds(&gap)
                && gap.from_message_seq <= last.to_message_seq.saturating_add(1)
                && last.from_message_seq <= gap.to_message_seq.saturating_add(1)
            {
                last.from_message_seq = last.from_message_seq.min(gap.from_message_seq);
                last.to_message_seq = last.to_message_seq.max(gap.to_message_seq);
                last.from_block_number = option_min(last.from_block_number, gap.from_block_number);
                last.to_block_number = option_max(last.to_block_number, gap.to_block_number);
                last.from_observed_at_ms =
                    option_min(last.from_observed_at_ms, gap.from_observed_at_ms);
                last.to_observed_at_ms = option_max(last.to_observed_at_ms, gap.to_observed_at_ms);
                return;
            }
        }
        self.pending.push(gap);
    }

    fn drain(&mut self) -> Vec<GapRecord> {
        std::mem::take(&mut self.pending)
    }
}

fn has_cursor_bounds(gap: &GapRecord) -> bool {
    gap.from_block_number.is_some()
        || gap.to_block_number.is_some()
        || gap.from_observed_at_ms.is_some()
        || gap.to_observed_at_ms.is_some()
}

fn retry_delays(window: Duration) -> impl Iterator<Item = Duration> {
    let mut elapsed = Duration::ZERO;
    let mut next = INITIAL_RETRY_DELAY;
    std::iter::from_fn(move || {
        let after_delay = elapsed.checked_add(next)?;
        if after_delay > window {
            return None;
        }
        let delay = next;
        elapsed = after_delay;
        next = cmp::min(next.saturating_mul(2), MAX_RETRY_DELAY);
        Some(delay)
    })
}

fn is_permanent_persistence_error(error: &anyhow::Error) -> bool {
    let Some(sqlx_error) = error.downcast_ref::<sqlx::Error>() else {
        return true;
    };
    let sqlx::Error::Database(database_error) = sqlx_error else {
        return false;
    };

    matches!(
        database_error.kind(),
        ErrorKind::UniqueViolation
            | ErrorKind::ForeignKeyViolation
            | ErrorKind::NotNullViolation
            | ErrorKind::CheckViolation
    ) || database_error
        .code()
        .is_some_and(|code| code.starts_with("23"))
}

fn option_min(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn option_max(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        borrow::Cow, error::Error, fmt, future::Future, pin::Pin, sync::Arc, time::Duration,
    };

    use sqlx::{
        error::{DatabaseError, ErrorKind},
        ConnectOptions,
    };

    use super::*;
    use crate::{
        Backend, BlockTimeObservation, CapturedState, CheckpointKind, CheckpointObjectStore,
        DeltaBackendCursor, DeltaEntry, GapReason, StateHistoryStore, StreamPosition,
    };

    #[test]
    fn stopped_interval_checkpoint_leaves_no_gap() {
        assert!(command_gap(
            &WriterCommand::Checkpoint(test_capture()),
            GapReason::WriterStopped
        )
        .is_none());
        let boundary = command_gap(
            &WriterCommand::Checkpoint(test_boundary_capture()),
            GapReason::WriterStopped,
        );
        assert!(boundary.is_some_and(|gap| gap.reason == GapReason::WriterStopped));
        let delta = command_gap(
            &WriterCommand::Delta(test_delta(3)),
            GapReason::WriterStopped,
        );
        assert!(delta.is_some_and(|gap| gap.reason == GapReason::WriterStopped));
    }

    #[test]
    fn gap_accumulator_coalesces_adjacent_and_overlapping() {
        let mut acc = GapAccumulator::default();
        acc.merge(test_gap(8453, 3, 11, 13, GapReason::QueueOverflow));
        acc.merge(test_gap(8453, 3, 10, 10, GapReason::QueueOverflow)); // reverse adjacent → extends
        acc.merge(test_gap(8453, 3, 11, 11, GapReason::QueueOverflow)); // contained → unchanged
        acc.merge(test_gap(8453, 4, 3, 3, GapReason::QueueOverflow)); // new generation → new entry
        acc.merge(test_gap(8453, 4, 1, 1, GapReason::QueueOverflow)); // reverse hole → new entry
        let pending = acc.drain();
        assert_eq!(
            pending
                .iter()
                .map(|g| (g.generation, g.from_message_seq, g.to_message_seq))
                .collect::<Vec<_>>(),
            vec![(3, 10, 13), (4, 3, 3), (4, 1, 1)]
        );
    }

    #[test]
    fn gap_accumulator_does_not_scope_cursorless_gap_by_merging() {
        let cursorless = test_gap(8453, 3, 10, 10, GapReason::WriteFailed);
        let mut bounded = test_gap(8453, 3, 11, 11, GapReason::WriteFailed);
        bounded.from_block_number = Some(100);
        bounded.to_block_number = Some(100);
        for (first, second) in [(cursorless.clone(), bounded.clone()), (bounded, cursorless)] {
            let mut acc = GapAccumulator::default();
            acc.merge(first);
            acc.merge(second);

            assert_eq!(acc.drain().len(), 2);
        }
    }

    #[test]
    fn retry_backoff_caps_and_respects_window() {
        let delays: Vec<_> = retry_delays(Duration::from_secs(30)).take(7).collect();
        assert_eq!(delays[0], Duration::from_millis(100));
        assert!(delays.iter().all(|d| *d <= Duration::from_secs(2)));
    }

    #[test]
    fn constraint_violations_are_permanent_errors() {
        assert!(is_permanent_persistence_error(&sqlx_unique_violation()));
        assert!(is_permanent_persistence_error(&anyhow::anyhow!(
            "not a db error"
        )));
        assert!(!is_permanent_persistence_error(&sqlx_io_error()));
    }

    #[test]
    fn native_only_delta_gap_has_block_bounds_without_observed_bounds() {
        let gap = delta_gap(&test_delta(1), GapReason::QueueOverflow);

        assert_eq!(gap.from_block_number, Some(100));
        assert_eq!(gap.to_block_number, Some(100));
        assert_eq!(gap.from_observed_at_ms, None);
        assert_eq!(gap.to_observed_at_ms, None);
    }

    #[test]
    #[expect(
        clippy::expect_used,
        reason = "The valid RFQ fixture must construct before its provider-time gap bounds are asserted"
    )]
    fn rfq_delta_gap_uses_provider_observation_time() {
        let publication_time = 2_000;
        let provider_observation_time = 1_000;
        let delta = DeltaEntry::new(
            8453,
            StreamPosition {
                generation: 7,
                message_seq: 1,
            },
            publication_time,
            r#"{"rfq":true}"#.to_owned(),
            vec![DeltaBackendCursor {
                backend: Backend::Rfq,
                block_number: None,
                observed_at_ms: Some(provider_observation_time),
            }],
            Vec::new(),
        )
        .expect("test delta should be valid");

        assert_ne!(delta.observed_at_ms(), provider_observation_time);
        let gap = delta_gap(&delta, GapReason::WriteFailed);
        assert_eq!(gap.from_observed_at_ms, Some(provider_observation_time));
        assert_eq!(gap.to_observed_at_ms, Some(provider_observation_time));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn writer_persists_deltas_in_order_and_reports_status(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        let store = writer_store(&pool).await?;
        let objects = test_object_store("state-history-writer-unused").await;
        let writer = StateHistoryWriter::spawn(
            WriterConfig {
                queue_capacity: 8,
                retry_window: Duration::from_secs(1),
            },
            store,
            objects,
            Arc::new(TestBaseFee),
        );

        for message_seq in 1..=3 {
            assert!(writer.enqueue_delta(test_delta(message_seq)));
        }
        wait_for_status(&writer, |status| status.persisted_deltas == 3).await?;

        let persisted: Vec<i64> = sqlx::query_scalar(
            "SELECT message_seq FROM state_history.deltas
             WHERE chain_id = 8453 AND generation = 7 ORDER BY message_seq",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(persisted, vec![1, 2, 3]);
        let base_fee: String = sqlx::query_scalar(
            "SELECT base_fee_wei::text FROM state_history.block_times
             WHERE chain_id = 8453 AND block_number = 100",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(base_fee, TEST_BASE_FEE_WEI);

        let status = writer.status();
        assert!(status.healthy);
        assert_eq!(status.enqueued_deltas, 3);
        assert_eq!(status.persisted_deltas, 3);
        assert_eq!(
            status.last_persisted,
            Some(StreamPosition {
                generation: 7,
                message_seq: 3,
            })
        );
        assert!(status.last_persisted_at_ms.is_some());
        writer.shutdown(Duration::from_secs(5)).await;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn queue_overflow_records_coalesced_gap(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let writer = StateHistoryWriter::spawn(
            WriterConfig {
                queue_capacity: 1,
                retry_window: Duration::from_secs(1),
            },
            writer_store(&pool).await?,
            test_object_store("state-history-writer-unused").await,
            Arc::new(NoBaseFee),
        );

        for message_seq in 1..=50 {
            writer.enqueue_delta(test_delta(message_seq));
        }
        wait_for_status(&writer, |status| {
            status.dropped_deltas > 0 && status.recorded_gaps > 0
        })
        .await?;

        let coalesced_gaps: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM state_history.gaps
             WHERE reason = 'queue_overflow' AND to_message_seq > from_message_seq",
        )
        .fetch_one(&pool)
        .await?;
        assert!(coalesced_gaps >= 1);
        assert!(writer.status().dropped_deltas > 0);
        writer.shutdown(Duration::from_secs(5)).await;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn write_failure_past_window_records_gap(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let writer = StateHistoryWriter::spawn(
            WriterConfig {
                queue_capacity: 4,
                retry_window: Duration::from_millis(250),
            },
            writer_store(&pool).await?,
            test_object_store("state-history-writer-unused").await,
            Arc::new(NoBaseFee),
        );
        sqlx::query("DROP TABLE state_history.deltas CASCADE")
            .execute(&pool)
            .await?;

        assert!(writer.enqueue_delta(test_delta(1)));
        wait_for_status(&writer, |status| {
            status.failed_deltas == 1 && status.recorded_gaps == 1
        })
        .await?;

        let failed_gap_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM state_history.gaps
             WHERE reason = 'write_failed' AND from_message_seq = 1 AND to_message_seq = 1",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(failed_gap_count, 1);
        writer.shutdown(Duration::from_secs(5)).await;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn checkpoint_command_runs_full_lifecycle(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let objects = test_minio_store().await?;
        let writer = StateHistoryWriter::spawn(
            WriterConfig::default(),
            writer_store(&pool).await?,
            objects,
            Arc::new(NoBaseFee),
        );
        assert!(writer.request_checkpoint(test_capture()));
        wait_for_status(&writer, |status| status.checkpoints_completed == 1).await?;

        let (status, key): (String, String) = sqlx::query_as(
            "SELECT status, s3_key FROM state_history.checkpoints
             WHERE chain_id = 8453 AND generation = 7 AND message_seq = 10",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(status, "complete");
        let fetched = test_object_store(TEST_MINIO_BUCKET)
            .await
            .fetch(&key)
            .await?;
        assert!(!fetched.is_empty());
        let block_time_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM state_history.block_times
             WHERE chain_id = 8453 AND block_number = 100",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(block_time_count, 1);
        assert_eq!(
            writer.status().last_checkpoint_status.as_deref(),
            Some("complete")
        );
        assert!(writer.status().last_successful_checkpoint_at_ms.is_some());
        writer.shutdown(Duration::from_secs(5)).await;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn unchanged_catalog_reuses_one_token_object_for_both_checkpoint_kinds(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-writer-token-dedup";
        let probe = Arc::new(TokenPutProbe::default());
        let writer = StateHistoryWriter::spawn_with_token_put_probe(
            WriterConfig::default(),
            writer_store(&pool).await?,
            test_minio_store_for_bucket(BUCKET).await?,
            Arc::new(NoBaseFee),
            Arc::clone(&probe),
        );
        let token_snapshot = test_token_snapshot(11);

        assert!(writer.request_checkpoint(test_capture_at(
            20,
            CheckpointKind::Interval,
            token_snapshot.clone(),
        )));
        wait_for_status(&writer, |status| status.checkpoints_completed == 1).await?;
        assert!(writer.request_checkpoint(test_capture_at(
            21,
            CheckpointKind::Boundary,
            token_snapshot,
        )));
        wait_for_status(&writer, |status| status.checkpoints_completed == 2).await?;

        let interval = checkpoint_token_reference(&pool, 20)
            .await?
            .context("interval checkpoint is missing its token reference")?;
        let boundary = checkpoint_token_reference(&pool, 21)
            .await?
            .context("boundary checkpoint is missing its token reference")?;
        assert_eq!(interval, boundary);
        assert_eq!(interval.token_count, 1);
        assert!(interval.token_bytes > 0);
        assert!(interval
            .s3_key
            .ends_with(&format!("{}.zst", interval.sha256)));
        assert_eq!(probe.attempts.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(token_object_count(BUCKET).await?, 1);
        assert_eq!(
            writer.status().last_token_snapshot_sha.as_deref(),
            Some(interval.sha256.as_str())
        );
        assert_eq!(writer.status().token_persistence_failures, 0);
        writer.shutdown(Duration::from_secs(5)).await;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn changed_catalog_is_referenced_only_by_later_checkpoints(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-writer-token-change";
        let probe = Arc::new(TokenPutProbe::default());
        let writer = StateHistoryWriter::spawn_with_token_put_probe(
            WriterConfig::default(),
            writer_store(&pool).await?,
            test_minio_store_for_bucket(BUCKET).await?,
            Arc::new(NoBaseFee),
            Arc::clone(&probe),
        );

        assert!(writer.request_checkpoint(test_capture_at(
            30,
            CheckpointKind::Interval,
            test_token_snapshot(12),
        )));
        wait_for_status(&writer, |status| status.checkpoints_completed == 1).await?;
        assert!(writer.request_checkpoint(test_capture_at(
            31,
            CheckpointKind::Interval,
            test_token_snapshot(13),
        )));
        wait_for_status(&writer, |status| status.checkpoints_completed == 2).await?;

        let first = checkpoint_token_reference(&pool, 30)
            .await?
            .context("first checkpoint is missing its token reference")?;
        let second = checkpoint_token_reference(&pool, 31)
            .await?
            .context("second checkpoint is missing its token reference")?;
        assert_ne!(first.sha256, second.sha256);
        assert_ne!(first.s3_key, second.s3_key);
        assert_eq!(probe.attempts.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(token_object_count(BUCKET).await?, 2);
        writer.shutdown(Duration::from_secs(5)).await;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn token_put_failure_is_enrichment_only_and_clears_the_memo(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-writer-token-failure";
        let probe = Arc::new(TokenPutProbe::default());
        let writer = StateHistoryWriter::spawn_with_token_put_probe(
            WriterConfig::default(),
            writer_store(&pool).await?,
            test_minio_store_for_bucket(BUCKET).await?,
            Arc::new(NoBaseFee),
            Arc::clone(&probe),
        );

        assert!(writer.request_checkpoint(test_capture_at(
            40,
            CheckpointKind::Interval,
            test_token_snapshot(14),
        )));
        wait_for_status(&writer, |status| status.checkpoints_completed == 1).await?;
        let first = checkpoint_token_reference(&pool, 40)
            .await?
            .context("first checkpoint is missing its token reference")?;

        probe.fail.store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(writer.request_checkpoint(test_capture_at(
            41,
            CheckpointKind::Interval,
            test_token_snapshot(15),
        )));
        wait_for_status(&writer, |status| {
            status.checkpoints_completed == 2 && status.token_persistence_failures == 1
        })
        .await?;
        assert_eq!(checkpoint_token_reference(&pool, 41).await?, None);
        assert_eq!(writer.status().checkpoints_failed, 0);
        assert!(writer.status().healthy);
        let gap_count: i64 = sqlx::query_scalar("SELECT count(*) FROM state_history.gaps")
            .fetch_one(&pool)
            .await?;
        assert_eq!(gap_count, 0);

        probe
            .fail
            .store(false, std::sync::atomic::Ordering::Relaxed);
        assert!(writer.request_checkpoint(test_capture_at(
            42,
            CheckpointKind::Interval,
            test_token_snapshot(14),
        )));
        wait_for_status(&writer, |status| status.checkpoints_completed == 3).await?;
        let recovered = checkpoint_token_reference(&pool, 42)
            .await?
            .context("recovered checkpoint is missing its token reference")?;
        assert_eq!(recovered, first);
        assert_eq!(
            probe.attempts.load(std::sync::atomic::Ordering::Relaxed),
            3,
            "the failed catalog must clear the earlier memo"
        );
        assert_eq!(writer.status().token_persistence_failures, 1);
        assert_eq!(
            writer.status().last_token_snapshot_sha.as_deref(),
            Some(first.sha256.as_str())
        );
        assert_eq!(token_object_count(BUCKET).await?, 1);
        writer.shutdown(Duration::from_secs(5)).await;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn draining_writer_skips_token_persistence_without_recording_failure(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-writer-token-drain";
        let probe = Arc::new(TokenPutProbe::default());
        let writer = StateHistoryWriter::spawn_with_token_put_probe(
            WriterConfig::default(),
            writer_store(&pool).await?,
            test_minio_store_for_bucket(BUCKET).await?,
            Arc::new(NoBaseFee),
            Arc::clone(&probe),
        );

        writer
            .inner
            .shutdown_deadline
            .send_replace(Some(Instant::now() + Duration::from_secs(60)));
        assert!(writer.request_checkpoint(test_capture_at(
            50,
            CheckpointKind::Interval,
            test_token_snapshot(16),
        )));
        wait_for_status(&writer, |status| status.checkpoints_completed == 1).await?;
        assert_eq!(checkpoint_token_reference(&pool, 50).await?, None);
        assert_eq!(probe.attempts.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(writer.status().token_persistence_failures, 0);
        assert_eq!(writer.status().checkpoints_failed, 0);

        writer.inner.shutdown_deadline.send_replace(None);
        assert!(writer.request_checkpoint(test_capture_at(
            51,
            CheckpointKind::Interval,
            test_token_snapshot(16),
        )));
        wait_for_status(&writer, |status| status.checkpoints_completed == 2).await?;
        checkpoint_token_reference(&pool, 51)
            .await?
            .context("post-drain checkpoint is missing its token reference")?;
        assert_eq!(probe.attempts.load(std::sync::atomic::Ordering::Relaxed), 1);
        writer.shutdown(Duration::from_secs(5)).await;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn boundary_checkpoint_creation_failure_records_bounded_gap(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        let writer = StateHistoryWriter::spawn(
            WriterConfig::default(),
            writer_store(&pool).await?,
            test_object_store("state-history-writer-unused").await,
            Arc::new(NoBaseFee),
        );
        sqlx::query("DROP TABLE state_history.checkpoints")
            .execute(&pool)
            .await?;

        let capture = test_boundary_capture();
        assert!(writer.request_checkpoint(capture));
        wait_for_status(&writer, |status| {
            status.checkpoints_failed == 1 && status.recorded_gaps == 1
        })
        .await?;

        let gap: (String, i64, i64, Option<i64>, Option<i64>) = sqlx::query_as(
            "SELECT reason, from_message_seq, to_message_seq,
                    from_block_number, to_block_number
             FROM state_history.gaps
             WHERE chain_id = 8453 AND generation = 7",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            gap,
            ("checkpoint_failed".to_owned(), 10, 10, Some(100), Some(100))
        );
        writer.shutdown(Duration::from_secs(5)).await;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn shutdown_drains_queued_deltas(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let writer = StateHistoryWriter::spawn(
            WriterConfig {
                queue_capacity: 32,
                retry_window: Duration::from_secs(1),
            },
            writer_store(&pool).await?,
            test_object_store("state-history-writer-unused").await,
            Arc::new(NoBaseFee),
        );
        for message_seq in 1..=20 {
            assert!(writer.enqueue_delta(test_delta(message_seq)));
        }
        let status_handle = writer.clone();
        writer.shutdown(Duration::from_secs(5)).await;

        let persisted: i64 =
            sqlx::query_scalar("SELECT count(*) FROM state_history.deltas WHERE chain_id = 8453")
                .fetch_one(&pool)
                .await?;
        assert_eq!(persisted, 20);
        assert_eq!(status_handle.status().persisted_deltas, 20);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn shutdown_records_gap_for_write_already_in_progress(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        let mut table_lock = pool.begin().await?;
        sqlx::query("LOCK TABLE state_history.deltas IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *table_lock)
            .await?;
        let writer = StateHistoryWriter::spawn(
            WriterConfig {
                queue_capacity: 8,
                retry_window: Duration::from_secs(30),
            },
            writer_store(&pool).await?,
            test_object_store("state-history-writer-unused").await,
            Arc::new(NoBaseFee),
        );

        assert!(writer.enqueue_delta(test_delta(1)));
        wait_for_query_waiting_on_lock(&pool, "INSERT INTO state_history.deltas").await?;
        writer.shutdown(Duration::from_millis(100)).await;

        let reason: String = sqlx::query_scalar(
            "SELECT reason FROM state_history.gaps
             WHERE chain_id = 8453 AND generation = 7 AND from_message_seq = 1",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(reason, "write_failed");
        table_lock.rollback().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn shutdown_records_gap_for_checkpoint_already_in_progress(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        let mut table_lock = pool.begin().await?;
        sqlx::query("LOCK TABLE state_history.checkpoints IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *table_lock)
            .await?;
        let writer = StateHistoryWriter::spawn(
            WriterConfig {
                queue_capacity: 8,
                retry_window: Duration::from_secs(30),
            },
            writer_store(&pool).await?,
            test_object_store("state-history-writer-unused").await,
            Arc::new(NoBaseFee),
        );

        assert!(writer.request_checkpoint(test_boundary_capture()));
        wait_for_query_waiting_on_lock(&pool, "INSERT INTO state_history.checkpoints").await?;
        writer.shutdown(Duration::from_millis(100)).await;

        let reason: String = sqlx::query_scalar(
            "SELECT reason FROM state_history.gaps
             WHERE chain_id = 8453 AND generation = 7 AND from_message_seq = 10",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(reason, "checkpoint_failed");
        table_lock.rollback().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn shutdown_returns_within_deadline_when_gap_flush_cannot_persist(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        let writer = StateHistoryWriter::spawn(
            WriterConfig {
                queue_capacity: 1,
                retry_window: Duration::from_secs(1),
            },
            writer_store(&pool).await?,
            test_object_store("state-history-writer-unused").await,
            Arc::new(HangingBaseFee),
        );
        assert!(writer.enqueue_delta(test_delta(1)));
        wait_for_status(&writer, |status| status.queue_len == 0).await?;
        for message_seq in 2..=50 {
            writer.enqueue_delta(test_delta(message_seq));
        }
        assert!(writer.status().dropped_deltas > 0);

        sqlx::query("DROP TABLE state_history.gaps")
            .execute(&pool)
            .await?;

        let shutdown = tokio::time::timeout(
            Duration::from_secs(6),
            writer.shutdown(Duration::from_secs(2)),
        )
        .await;
        assert!(shutdown.is_ok(), "shutdown exceeded its deadline and grace");
        Ok(())
    }

    const TEST_BASE_FEE_WEI: &str = "12345678901234567890";
    const TEST_MINIO_BUCKET: &str = "state-history-writer-test";

    struct HangingBaseFee;

    impl BaseFeeSource for HangingBaseFee {
        fn base_fee_wei<'a>(
            &'a self,
            _chain_id: u64,
            _block_number: u64,
            _block_hash: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>> {
            Box::pin(std::future::pending())
        }
    }

    struct TestBaseFee;

    impl BaseFeeSource for TestBaseFee {
        fn base_fee_wei<'a>(
            &'a self,
            _chain_id: u64,
            block_number: u64,
            _block_hash: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>> {
            Box::pin(async move { (block_number == 100).then(|| TEST_BASE_FEE_WEI.to_owned()) })
        }
    }

    #[expect(
        clippy::expect_used,
        reason = "Fixed fixture inputs must satisfy the validated constructor before the replay scenario runs"
    )]
    fn test_delta(message_seq: u64) -> DeltaEntry {
        DeltaEntry::new(
            8453,
            StreamPosition {
                generation: 7,
                message_seq,
            },
            1_000 + message_seq,
            format!(r#"{{"message_seq":{message_seq}}}"#),
            vec![DeltaBackendCursor {
                backend: Backend::Native,
                block_number: Some(100),
                observed_at_ms: None,
            }],
            vec![test_block_time()],
        )
        .expect("test delta should be valid")
    }

    fn test_capture() -> CheckpointCapture {
        test_capture_with_kind(CheckpointKind::Interval)
    }

    fn test_boundary_capture() -> CheckpointCapture {
        test_capture_with_kind(CheckpointKind::Boundary)
    }

    fn test_capture_with_kind(kind: CheckpointKind) -> CheckpointCapture {
        test_capture_at(10, kind, test_token_snapshot(1))
    }

    #[expect(
        clippy::expect_used,
        reason = "Fixed fixture inputs must satisfy the validated constructor before the replay scenario runs"
    )]
    fn test_capture_at(
        message_seq: u64,
        kind: CheckpointKind,
        token_snapshot: TokenSnapshot,
    ) -> CheckpointCapture {
        let state = CapturedState::new(
            8453,
            StreamPosition {
                generation: 7,
                message_seq,
            },
            Some(42),
            kind,
            100,
            None,
            vec![Backend::Native],
            vec![r#"{"state":true}"#.to_owned()],
            vec![test_block_time()],
        )
        .expect("test capture should be valid");
        CheckpointCapture::new(state, token_snapshot.tokens, "writer".to_owned())
    }

    #[expect(
        clippy::expect_used,
        reason = "The fixed token JSON must decode before checkpoint persistence is exercised"
    )]
    fn test_token_snapshot(seed: u8) -> TokenSnapshot {
        let token = serde_json::from_value(serde_json::json!({
            "address": format!("0x{seed:040x}"),
            "symbol": format!("TOKEN-{seed}"),
            "decimals": 18,
            "tax": 0,
            "gas": [],
            "chainId": 8453,
            "quality": 100,
        }))
        .expect("test token should be valid");
        TokenSnapshot {
            chain_id: 8453,
            tokens: vec![token],
        }
    }

    fn test_block_time() -> BlockTimeObservation {
        BlockTimeObservation {
            block_number: 100,
            timestamp_ms: 1_000,
            block_hash: Some("block".to_owned()),
            parent_hash: Some("parent".to_owned()),
        }
    }

    async fn writer_store(pool: &sqlx::PgPool) -> anyhow::Result<StateHistoryStore> {
        let database_url = pool.connect_options().to_url_lossy();
        StateHistoryStore::connect(database_url.as_str()).await
    }

    async fn checkpoint_token_reference(
        pool: &sqlx::PgPool,
        message_seq: i64,
    ) -> anyhow::Result<Option<TokenSnapshotRef>> {
        let columns: (Option<String>, Option<String>, Option<i64>, Option<i64>) = sqlx::query_as(
            "SELECT token_s3_key, token_sha256, token_count, token_bytes
             FROM state_history.checkpoints
             WHERE chain_id = 8453 AND generation = 7 AND message_seq = $1",
        )
        .bind(message_seq)
        .fetch_one(pool)
        .await?;
        match columns {
            (Some(s3_key), Some(sha256), Some(token_count), Some(token_bytes)) => {
                Ok(Some(TokenSnapshotRef {
                    s3_key,
                    sha256,
                    token_count: u64::try_from(token_count)?,
                    token_bytes: u64::try_from(token_bytes)?,
                }))
            }
            (None, None, None, None) => Ok(None),
            _ => anyhow::bail!("checkpoint token reference columns are partially populated"),
        }
    }

    async fn wait_for_status(
        writer: &WriterHandle,
        predicate: impl Fn(&WriterStatus) -> bool,
    ) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let status = writer.status();
            if predicate(&status) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("writer status did not reach the expected state: {status:?}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_query_waiting_on_lock(
        pool: &sqlx::PgPool,
        query_fragment: &str,
    ) -> anyhow::Result<()> {
        let pattern = format!("%{query_fragment}%");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                    SELECT 1 FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND wait_event_type = 'Lock'
                      AND query LIKE $1
                )",
            )
            .bind(&pattern)
            .fetch_one(pool)
            .await?;
            if waiting {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("writer query did not start waiting on the table lock");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn test_object_store(bucket: &str) -> CheckpointObjectStore {
        CheckpointObjectStore::from_env_config(
            bucket.to_owned(),
            "writer".to_owned(),
            "eu-central-1".to_owned(),
            Some("http://127.0.0.1:59000".to_owned()),
            true,
        )
        .await
    }

    async fn test_minio_store() -> anyhow::Result<CheckpointObjectStore> {
        test_minio_store_for_bucket(TEST_MINIO_BUCKET).await
    }

    async fn test_minio_store_for_bucket(bucket: &str) -> anyhow::Result<CheckpointObjectStore> {
        let client = test_minio_client().await;
        if client.create_bucket().bucket(bucket).send().await.is_err() {
            client.head_bucket().bucket(bucket).send().await?;
        }
        Ok(test_object_store(bucket).await)
    }

    async fn test_minio_client() -> aws_sdk_s3::Client {
        let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("eu-central-1"))
            .load()
            .await;
        let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
            .force_path_style(true)
            .endpoint_url("http://127.0.0.1:59000")
            .build();
        aws_sdk_s3::Client::from_conf(s3_config)
    }

    async fn token_object_count(bucket: &str) -> anyhow::Result<usize> {
        let output = test_minio_client()
            .await
            .list_objects_v2()
            .bucket(bucket)
            .prefix("writer/chain=8453/tokens/")
            .send()
            .await?;
        Ok(output.contents().len())
    }

    fn test_gap(
        chain_id: u64,
        generation: u64,
        from_message_seq: u64,
        to_message_seq: u64,
        reason: GapReason,
    ) -> GapRecord {
        GapRecord {
            chain_id,
            generation,
            from_message_seq,
            to_message_seq,
            reason,
            from_block_number: None,
            to_block_number: None,
            from_observed_at_ms: None,
            to_observed_at_ms: None,
        }
    }

    fn sqlx_unique_violation() -> anyhow::Error {
        sqlx::Error::Database(Box::new(TestDatabaseError)).into()
    }

    fn sqlx_io_error() -> anyhow::Error {
        sqlx::Error::Io(std::io::Error::other("connection reset")).into()
    }

    #[derive(Debug)]
    struct TestDatabaseError;

    impl fmt::Display for TestDatabaseError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("unique violation")
        }
    }

    impl Error for TestDatabaseError {}

    impl DatabaseError for TestDatabaseError {
        fn message(&self) -> &str {
            "unique violation"
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed("23505"))
        }

        fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> ErrorKind {
            ErrorKind::UniqueViolation
        }
    }
}
