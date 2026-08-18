use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use historical_quote::api::{
    JobCounts, JobEnvelope, JobFailure, JobFailureCode, JobResult, JobState, JobSubmission, JobType,
};
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{JobLimits, JobRequest, ProgressCounters, ProgressReporter, ScheduledJob};

#[derive(Clone)]
pub struct JobRegistry {
    inner: Arc<RegistryInner>,
}

struct RegistryInner {
    limits: JobLimits,
    state: Mutex<RegistryState>,
    notify: Notify,
}

#[derive(Default)]
struct RegistryState {
    records: HashMap<Uuid, JobRecord>,
    request_ids: HashMap<Uuid, RequestMapping>,
    queue: VecDeque<Uuid>,
    running: usize,
    reserved_decoded_bytes: u64,
    admission_open: bool,
    draining: bool,
    next_terminal_order: u64,
}

struct RequestMapping {
    job_id: Uuid,
}

struct JobRecord {
    job_id: Uuid,
    request_id: Uuid,
    fingerprint: [u8; 32],
    request: JobRequest,
    job_type: JobType,
    state: JobState,
    submitted_at: DateTime<Utc>,
    deadline_at: DateTime<Utc>,
    deadline_instant: Instant,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    finished_instant: Option<Instant>,
    terminal_order: Option<u64>,
    estimated_decoded_bytes: u64,
    progress: Arc<ProgressCounters>,
    cancellation: CancellationToken,
    cancellation_requested: bool,
    result: Option<JobResult>,
    failure: Option<JobFailure>,
    terminal_body: Option<Bytes>,
    terminal_delivery_in_progress: bool,
}

#[derive(Debug, Clone)]
pub enum SubmitOutcome {
    Accepted(JobSubmission),
    Reused(JobSubmission),
    RequestIdConflict,
    QueueFull,
    ServiceUnavailable,
    InternalError,
}

impl SubmitOutcome {
    #[must_use]
    pub fn job_id(&self) -> Option<Uuid> {
        match self {
            Self::Accepted(submission) | Self::Reused(submission) => Some(submission.job_id),
            Self::RequestIdConflict
            | Self::QueueFull
            | Self::ServiceUnavailable
            | Self::InternalError => None,
        }
    }
}

pub enum PollOutcome {
    Pending(Box<JobEnvelope>),
    Terminal(TerminalDelivery),
    NotFound,
}

pub enum CancelOutcome {
    Found(Box<JobEnvelope>),
    NotFound,
}

pub struct TerminalDelivery {
    registry: JobRegistry,
    job_id: Uuid,
    bytes: Bytes,
}

impl TerminalDelivery {
    #[must_use]
    pub fn bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    #[must_use]
    pub fn job_id(&self) -> Uuid {
        self.job_id
    }

    pub async fn commit(self) {
        self.registry.consume(self.job_id).await;
    }

    pub async fn release(self) {
        self.registry.release_delivery(self.job_id).await;
    }

    pub(crate) fn into_parts(self) -> (JobRegistry, Uuid, Bytes) {
        (self.registry, self.job_id, self.bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobSnapshot {
    pub counts: JobCounts,
    pub reserved_decoded_bytes: u64,
    pub draining: bool,
}

pub(crate) struct RunnableJob {
    pub job_id: Uuid,
    pub job_type: JobType,
    pub request: JobRequest,
    pub deadline: Instant,
    pub reserved_decoded_bytes: u64,
    pub cancellation: CancellationToken,
    pub progress: ProgressReporter,
}

pub(crate) struct ScheduleBatch {
    pub jobs: Vec<RunnableJob>,
    pub next_deadline: Option<Instant>,
    pub stopped: bool,
}

pub(crate) enum FinishedJob {
    Completed(JobResult),
    Failed(JobFailure),
    Cancelled,
}

impl JobRegistry {
    #[must_use]
    pub fn new(limits: JobLimits) -> Self {
        let state = RegistryState {
            admission_open: true,
            ..RegistryState::default()
        };
        Self {
            inner: Arc::new(RegistryInner {
                limits,
                state: Mutex::new(state),
                notify: Notify::new(),
            }),
        }
    }

    pub async fn submit(&self, job: ScheduledJob) -> SubmitOutcome {
        let Ok(fingerprint) = job.request.fingerprint() else {
            return SubmitOutcome::InternalError;
        };
        let mut state = self.inner.state.lock().await;
        prune_expired(&mut state, &self.inner.limits, Instant::now());
        let request_id = job.request.request_id();
        if let Some(mapping) = state.request_ids.get(&request_id) {
            let Some(record) = state.records.get(&mapping.job_id) else {
                return SubmitOutcome::InternalError;
            };
            if record.fingerprint != fingerprint {
                return SubmitOutcome::RequestIdConflict;
            }
            return SubmitOutcome::Reused(record.submission(true, &state.queue));
        }
        if !state.admission_open
            || job.estimated_decoded_bytes == 0
            || job.estimated_decoded_bytes > self.inner.limits.decoded_byte_budget
        {
            return SubmitOutcome::ServiceUnavailable;
        }
        if state.queue.len() >= self.inner.limits.max_waiting {
            return SubmitOutcome::QueueFull;
        }

        let submitted_at = Utc::now();
        let submitted_instant = Instant::now();
        let timeout = std::time::Duration::from_millis(job.request.timeout_ms());
        let Ok(deadline_delta) = chrono::Duration::from_std(timeout) else {
            return SubmitOutcome::ServiceUnavailable;
        };
        let Some(deadline_at) = submitted_at.checked_add_signed(deadline_delta) else {
            return SubmitOutcome::ServiceUnavailable;
        };
        let Some(deadline_instant) = submitted_instant.checked_add(timeout) else {
            return SubmitOutcome::ServiceUnavailable;
        };
        let job_id = Uuid::new_v4();
        let record = JobRecord {
            job_id,
            request_id,
            fingerprint,
            job_type: job.request.job_type(),
            progress: job.request.progress(),
            request: job.request,
            state: JobState::Queued,
            submitted_at,
            deadline_at,
            deadline_instant,
            started_at: None,
            finished_at: None,
            finished_instant: None,
            terminal_order: None,
            estimated_decoded_bytes: job.estimated_decoded_bytes,
            cancellation: CancellationToken::new(),
            cancellation_requested: false,
            result: None,
            failure: None,
            terminal_body: None,
            terminal_delivery_in_progress: false,
        };
        state
            .request_ids
            .insert(request_id, RequestMapping { job_id });
        state.queue.push_back(job_id);
        state.records.insert(job_id, record);
        let Some(submission) = state
            .records
            .get(&job_id)
            .map(|record| record.submission(false, &state.queue))
        else {
            return SubmitOutcome::InternalError;
        };
        drop(state);
        self.inner.notify.notify_one();
        SubmitOutcome::Accepted(submission)
    }

    pub async fn inspect(&self, job_id: Uuid) -> Option<JobEnvelope> {
        let mut state = self.inner.state.lock().await;
        prune_expired(&mut state, &self.inner.limits, Instant::now());
        state
            .records
            .get(&job_id)
            .map(|record| record.envelope(false, &state.queue))
    }

    pub async fn poll(&self, job_id: Uuid) -> PollOutcome {
        let mut state = self.inner.state.lock().await;
        prune_expired(&mut state, &self.inner.limits, Instant::now());
        let queue = state.queue.clone();
        let Some(record) = state.records.get_mut(&job_id) else {
            return PollOutcome::NotFound;
        };
        if !is_terminal(record.state) {
            return PollOutcome::Pending(Box::new(record.envelope(false, &queue)));
        }
        if record.terminal_delivery_in_progress {
            return PollOutcome::NotFound;
        }
        let Some(bytes) = record.terminal_body.clone() else {
            return PollOutcome::NotFound;
        };
        record.terminal_delivery_in_progress = true;
        PollOutcome::Terminal(TerminalDelivery {
            registry: self.clone(),
            job_id,
            bytes,
        })
    }

    pub async fn cancel(&self, job_id: Uuid) -> CancelOutcome {
        let mut state = self.inner.state.lock().await;
        prune_expired(&mut state, &self.inner.limits, Instant::now());
        let Some(current_state) = state.records.get(&job_id).map(|record| record.state) else {
            return CancelOutcome::NotFound;
        };
        if current_state == JobState::Queued {
            state.queue.retain(|queued| *queued != job_id);
            finish_locked(
                &mut state,
                &self.inner.limits,
                job_id,
                FinishedJob::Cancelled,
            );
        } else if current_state == JobState::Running {
            if let Some(record) = state.records.get_mut(&job_id) {
                record.cancellation_requested = true;
                record.cancellation.cancel();
            }
        }
        let envelope = state
            .records
            .get(&job_id)
            .map(|record| record.envelope(false, &state.queue));
        drop(state);
        self.inner.notify.notify_one();
        envelope.map_or(CancelOutcome::NotFound, |envelope| {
            CancelOutcome::Found(Box::new(envelope))
        })
    }

    pub async fn snapshot(&self) -> JobSnapshot {
        let mut state = self.inner.state.lock().await;
        prune_expired(&mut state, &self.inner.limits, Instant::now());
        snapshot_locked(&state)
    }

    pub async fn begin_shutdown(&self) {
        let mut state = self.inner.state.lock().await;
        state.admission_open = false;
        state.draining = true;
        let queued = state.queue.drain(..).collect::<Vec<_>>();
        for job_id in queued {
            finish_locked(
                &mut state,
                &self.inner.limits,
                job_id,
                FinishedJob::Cancelled,
            );
        }
        for record in state.records.values_mut() {
            if record.state == JobState::Running {
                record.cancellation_requested = true;
                record.cancellation.cancel();
            }
        }
        drop(state);
        self.inner.notify.notify_waiters();
    }

    #[must_use]
    pub async fn is_ready(&self) -> bool {
        let state = self.inner.state.lock().await;
        state.admission_open && !state.draining
    }

    pub(crate) async fn schedule(&self) -> ScheduleBatch {
        let mut state = self.inner.state.lock().await;
        let now = Instant::now();
        expire_queued_deadlines(&mut state, &self.inner.limits, now);
        prune_expired(&mut state, &self.inner.limits, now);
        let mut jobs = Vec::new();
        while state.running < self.inner.limits.max_running {
            let Some(job_id) = state.queue.front().copied() else {
                break;
            };
            let Some(record) = state.records.get(&job_id) else {
                state.queue.pop_front();
                continue;
            };
            let estimated_decoded_bytes = record.estimated_decoded_bytes;
            let Some(reserved) = state
                .reserved_decoded_bytes
                .checked_add(estimated_decoded_bytes)
            else {
                break;
            };
            if reserved > self.inner.limits.decoded_byte_budget {
                break;
            }
            state.queue.pop_front();
            state.running += 1;
            state.reserved_decoded_bytes = reserved;
            let Some(record) = state.records.get_mut(&job_id) else {
                state.running = state.running.saturating_sub(1);
                state.reserved_decoded_bytes = state
                    .reserved_decoded_bytes
                    .saturating_sub(estimated_decoded_bytes);
                continue;
            };
            record.state = JobState::Running;
            record.started_at = Some(Utc::now());
            jobs.push(RunnableJob {
                job_id,
                job_type: record.job_type,
                request: record.request.clone(),
                deadline: record.deadline_instant,
                reserved_decoded_bytes: estimated_decoded_bytes,
                cancellation: record.cancellation.clone(),
                progress: ProgressReporter::new(Arc::clone(&record.progress)),
            });
        }
        let next_deadline = state
            .queue
            .iter()
            .filter_map(|job_id| {
                state
                    .records
                    .get(job_id)
                    .map(|record| record.deadline_instant)
            })
            .min();
        let stopped = state.draining && state.running == 0;
        ScheduleBatch {
            jobs,
            next_deadline,
            stopped,
        }
    }

    pub(crate) async fn finish(&self, job_id: Uuid, outcome: FinishedJob) {
        let mut state = self.inner.state.lock().await;
        finish_locked(&mut state, &self.inner.limits, job_id, outcome);
        drop(state);
        self.inner.notify.notify_one();
    }

    pub(crate) async fn notified(&self) {
        self.inner.notify.notified().await;
    }

    pub(crate) async fn consume(&self, job_id: Uuid) {
        let mut state = self.inner.state.lock().await;
        if state
            .records
            .get(&job_id)
            .is_some_and(|record| record.terminal_delivery_in_progress)
        {
            remove_job(&mut state, job_id);
        }
    }

    pub(crate) async fn release_delivery(&self, job_id: Uuid) {
        let mut state = self.inner.state.lock().await;
        if let Some(record) = state.records.get_mut(&job_id) {
            record.terminal_delivery_in_progress = false;
        }
    }
}

impl JobRecord {
    fn submission(&self, reused: bool, queue: &VecDeque<Uuid>) -> JobSubmission {
        JobSubmission {
            job_id: self.job_id,
            request_id: self.request_id,
            job_type: self.job_type,
            state: self.state,
            submitted_at: self.submitted_at,
            deadline_at: self.deadline_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
            queue_position: queue_position(queue, self.job_id),
            cancellation_requested: self.cancellation_requested,
            progress: self.progress.snapshot(self.state == JobState::Completed),
            reused,
        }
    }

    fn envelope(&self, include_terminal: bool, queue: &VecDeque<Uuid>) -> JobEnvelope {
        JobEnvelope {
            job_id: self.job_id,
            request_id: self.request_id,
            job_type: self.job_type,
            state: self.state,
            submitted_at: self.submitted_at,
            deadline_at: self.deadline_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
            queue_position: queue_position(queue, self.job_id),
            cancellation_requested: self.cancellation_requested,
            progress: self.progress.snapshot(self.state == JobState::Completed),
            result: include_terminal.then(|| self.result.clone()).flatten(),
            failure: include_terminal.then(|| self.failure.clone()).flatten(),
        }
    }
}

fn finish_locked(
    state: &mut RegistryState,
    limits: &JobLimits,
    job_id: Uuid,
    outcome: FinishedJob,
) {
    let Some(record) = state.records.get_mut(&job_id) else {
        return;
    };
    if is_terminal(record.state) {
        return;
    }
    if record.state == JobState::Running {
        state.running = state.running.saturating_sub(1);
        state.reserved_decoded_bytes = state
            .reserved_decoded_bytes
            .saturating_sub(record.estimated_decoded_bytes);
    }
    let state_value = match outcome {
        FinishedJob::Completed(result) => {
            record.result = Some(result);
            JobState::Completed
        }
        FinishedJob::Failed(failure) => {
            record.failure = Some(failure);
            JobState::Failed
        }
        FinishedJob::Cancelled => JobState::Cancelled,
    };
    record.state = state_value;
    let finished_at = Utc::now();
    let finished_instant = Instant::now();
    record.finished_at = Some(finished_at);
    record.finished_instant = Some(finished_instant);
    record.terminal_order = Some(state.next_terminal_order);
    state.next_terminal_order = state.next_terminal_order.saturating_add(1);
    let envelope = record.envelope(true, &state.queue);
    match serde_json::to_vec(&envelope) {
        Ok(bytes) => record.terminal_body = Some(Bytes::from(bytes)),
        Err(_) => {
            record.state = JobState::Failed;
            record.result = None;
            record.failure = Some(internal_failure());
            if let Ok(bytes) = serde_json::to_vec(&record.envelope(true, &state.queue)) {
                record.terminal_body = Some(Bytes::from(bytes));
            }
        }
    }
    enforce_terminal_limit(state, limits.max_terminal_jobs);
}

fn expire_queued_deadlines(state: &mut RegistryState, limits: &JobLimits, now: Instant) {
    let expired = state
        .queue
        .iter()
        .filter_map(|job_id| {
            state
                .records
                .get(job_id)
                .filter(|record| record.deadline_instant <= now)
                .map(|_| *job_id)
        })
        .collect::<Vec<_>>();
    for job_id in expired {
        state.queue.retain(|queued| *queued != job_id);
        finish_locked(
            state,
            limits,
            job_id,
            FinishedJob::Failed(JobFailure {
                code: JobFailureCode::DeadlineExceeded,
                message: "job deadline exceeded".to_owned(),
                retryable: false,
                details: None,
            }),
        );
    }
}

fn prune_expired(state: &mut RegistryState, limits: &JobLimits, now: Instant) {
    let expired = state
        .records
        .iter()
        .filter_map(|(job_id, record)| {
            record
                .finished_instant
                .filter(|finished| *finished + limits.terminal_ttl <= now)
                .map(|_| *job_id)
        })
        .collect::<Vec<_>>();
    for job_id in expired {
        remove_job(state, job_id);
    }
}

fn enforce_terminal_limit(state: &mut RegistryState, limit: usize) {
    while state
        .records
        .values()
        .filter(|record| is_terminal(record.state))
        .count()
        > limit
    {
        let oldest = state
            .records
            .values()
            .filter(|record| is_terminal(record.state))
            .min_by_key(|record| record.terminal_order)
            .map(|record| record.job_id);
        let Some(job_id) = oldest else {
            break;
        };
        remove_job(state, job_id);
    }
}

fn remove_job(state: &mut RegistryState, job_id: Uuid) {
    if let Some(record) = state.records.remove(&job_id) {
        state.request_ids.remove(&record.request_id);
    }
    state.queue.retain(|queued| *queued != job_id);
}

fn snapshot_locked(state: &RegistryState) -> JobSnapshot {
    let queued = u64::try_from(state.queue.len()).unwrap_or(u64::MAX);
    let running = u64::try_from(state.running).unwrap_or(u64::MAX);
    let retained_terminal = u64::try_from(
        state
            .records
            .values()
            .filter(|record| is_terminal(record.state))
            .count(),
    )
    .unwrap_or(u64::MAX);
    JobSnapshot {
        counts: JobCounts {
            queued,
            running,
            retained_terminal,
        },
        reserved_decoded_bytes: state.reserved_decoded_bytes,
        draining: state.draining,
    }
}

fn queue_position(queue: &VecDeque<Uuid>, job_id: Uuid) -> Option<u64> {
    queue
        .iter()
        .position(|queued| *queued == job_id)
        .and_then(|position| u64::try_from(position + 1).ok())
}

fn is_terminal(state: JobState) -> bool {
    matches!(
        state,
        JobState::Completed | JobState::Failed | JobState::Cancelled
    )
}

fn internal_failure() -> JobFailure {
    JobFailure {
        code: JobFailureCode::InternalError,
        message: "job result could not be prepared".to_owned(),
        retryable: false,
        details: None,
    }
}
