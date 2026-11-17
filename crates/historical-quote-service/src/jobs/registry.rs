use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use historical_quote::api::{
    JobCounts, JobEnvelope, JobFailure, JobFailureCode, JobResult, JobState, JobSubmission, JobType,
};
use tokio::sync::futures::Notified;
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::result_body::{result_body, ResultBodyError, TerminalBody};
use super::{
    JobLimits, JobRequest, ProgressCounters, ProgressReporter, ScheduledJob, READ_LIMIT_EXCEEDED,
};

#[derive(Clone)]
pub struct JobRegistry {
    inner: Arc<RegistryInner>,
}

struct RegistryInner {
    limits: JobLimits,
    state: Mutex<RegistryState>,
    notify: Notify,
    /// Wakes the jobs whose result waits for room when a delivery ends.
    room: Notify,
}

#[derive(Default)]
struct RegistryState {
    records: HashMap<Uuid, JobRecord>,
    request_ids: HashMap<Uuid, Uuid>,
    queue: VecDeque<Uuid>,
    running: usize,
    reserved_decoded_bytes: u64,
    retained_terminal_bytes: u64,
    admission_open: bool,
    draining: bool,
    next_terminal_order: u64,
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
    terminal_body: Option<TerminalBody>,
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
    body: TerminalBody,
}

impl TerminalDelivery {
    #[must_use]
    pub fn bytes(&self) -> Bytes {
        self.body.to_bytes()
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

    pub(crate) fn into_parts(self) -> (JobRegistry, Uuid, Vec<Bytes>) {
        (self.registry, self.job_id, self.body.into_parts())
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

pub(crate) struct ScheduleBatch<'a> {
    pub jobs: Vec<RunnableJob>,
    pub next_deadline: Option<Instant>,
    pub stopped: bool,
    pub notified: Notified<'a>,
}

pub(crate) enum FinishedJob {
    /// The result, written as JSON.
    Completed(Bytes),
    Failed(JobFailure),
    Cancelled,
}

impl FinishedJob {
    /// The end of a job that gave `result`, failed when the result passes
    /// `max_bytes` or cannot be written.
    pub(crate) fn completed(result: &JobResult, max_bytes: u64) -> Self {
        match result_body(result, max_bytes) {
            Ok(body) => Self::Completed(body),
            Err(ResultBodyError::TooLarge) => Self::Failed(read_limit_failure()),
            Err(ResultBodyError::Unwritable) => Self::Failed(internal_failure()),
        }
    }
}

/// Whether a job's end was published.
pub(crate) enum Finish<'a> {
    /// The job ended as `state`, with the code of its failure when it failed.
    Ended {
        state: JobState,
        failure_code: Option<JobFailureCode>,
    },
    /// The job had ended already, or is gone.
    Gone,
    /// The result does not fit beside the results being delivered. `room`
    /// wakes once a delivery ends.
    NoRoom { result: Bytes, room: Notified<'a> },
}

/// What [`finish_locked`] did with a job's end.
enum Ending {
    Ended(JobState, Option<JobFailureCode>),
    Gone,
    NoRoom(Bytes),
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
                room: Notify::new(),
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
        if let Some(job_id) = state.request_ids.get(&request_id) {
            let Some(record) = state.records.get(job_id) else {
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
            terminal_body: None,
            terminal_delivery_in_progress: false,
        };
        state.request_ids.insert(request_id, job_id);
        state.queue.push_back(job_id);
        let submission = record.submission(false, &state.queue);
        state.records.insert(job_id, record);
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
            .map(|record| record.envelope(&state.queue))
    }

    pub async fn poll(&self, job_id: Uuid) -> PollOutcome {
        let mut state = self.inner.state.lock().await;
        prune_expired(&mut state, &self.inner.limits, Instant::now());
        let RegistryState { records, queue, .. } = &mut *state;
        let Some(record) = records.get_mut(&job_id) else {
            return PollOutcome::NotFound;
        };
        if !is_terminal(record.state) {
            return PollOutcome::Pending(Box::new(record.envelope(queue)));
        }
        if record.terminal_delivery_in_progress {
            return PollOutcome::NotFound;
        }
        let Some(body) = record.terminal_body.clone() else {
            return PollOutcome::NotFound;
        };
        record.terminal_delivery_in_progress = true;
        PollOutcome::Terminal(TerminalDelivery {
            registry: self.clone(),
            job_id,
            body,
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
            .map(|record| record.envelope(&state.queue));
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

    pub(crate) async fn schedule(&self) -> ScheduleBatch<'_> {
        // Capture broadcasts before checking state so shutdown cannot fall between the check and wait.
        let notified = self.inner.notify.notified();
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
            notified,
        }
    }

    /// Ends the job with `outcome`, unless its result does not fit beside the
    /// results being delivered. A running job keeps its slot and its
    /// reservation until [`Self::release`], since its work may outlive it.
    pub(crate) async fn finish(&self, job_id: Uuid, outcome: FinishedJob) -> Finish<'_> {
        let room = self.inner.room.notified();
        let mut state = self.inner.state.lock().await;
        let ending = finish_locked(&mut state, &self.inner.limits, job_id, outcome);
        drop(state);
        self.inner.notify.notify_one();
        match ending {
            Ending::Ended(state, failure_code) => Finish::Ended {
                state,
                failure_code,
            },
            Ending::Gone => Finish::Gone,
            Ending::NoRoom(result) => Finish::NoRoom { result, room },
        }
    }

    pub(crate) fn max_terminal_bytes(&self) -> u64 {
        self.inner.limits.max_terminal_bytes
    }

    /// Frees the slot and the `reserved_decoded_bytes` of a running job whose
    /// work has stopped.
    pub(crate) async fn release(&self, reserved_decoded_bytes: u64) {
        let mut state = self.inner.state.lock().await;
        state.running = state.running.saturating_sub(1);
        state.reserved_decoded_bytes = state
            .reserved_decoded_bytes
            .saturating_sub(reserved_decoded_bytes);
        drop(state);
        self.inner.notify.notify_one();
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
        drop(state);
        self.inner.room.notify_waiters();
    }

    pub(crate) async fn release_delivery(&self, job_id: Uuid) {
        let mut state = self.inner.state.lock().await;
        if let Some(record) = state.records.get_mut(&job_id) {
            record.terminal_delivery_in_progress = false;
        }
        drop(state);
        self.inner.room.notify_waiters();
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

    fn envelope(&self, queue: &VecDeque<Uuid>) -> JobEnvelope {
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
            result: None,
            failure: None,
        }
    }
}

/// Ends the job with `outcome`, and gives its result back unpublished when it
/// does not fit beside the bodies being delivered. A body over the kept bytes
/// fails the job, and a failure or cancellation body, a few hundred bytes, is
/// always kept.
fn finish_locked(
    state: &mut RegistryState,
    limits: &JobLimits,
    job_id: Uuid,
    outcome: FinishedJob,
) -> Ending {
    let Some(record) = state.records.get(&job_id) else {
        return Ending::Gone;
    };
    if is_terminal(record.state) {
        return Ending::Gone;
    }
    let (job_state, result, failure) = match outcome {
        FinishedJob::Completed(result) => (JobState::Completed, Some(result), None),
        FinishedJob::Failed(failure) => (JobState::Failed, None, Some(failure)),
        FinishedJob::Cancelled => (JobState::Cancelled, None, None),
    };
    let mut envelope = record.envelope(&state.queue);
    envelope.state = job_state;
    envelope.finished_at = Some(Utc::now());
    envelope.progress = record.progress.snapshot(job_state == JobState::Completed);
    envelope.failure = failure;
    let failed_progress = record.progress.snapshot(false);
    let body = match TerminalBody::new(&envelope, result.clone()) {
        Ok(body) if result.is_none() || body.len() <= limits.max_terminal_bytes => Ok(body),
        Ok(_) => Err(read_limit_failure()),
        Err(_) => Err(internal_failure()),
    };
    if let (Ok(body), Some(result)) = (&body, result) {
        if !make_room(state, limits, job_id, body.len()) {
            return Ending::NoRoom(result);
        }
    }
    let body = match body {
        Ok(body) => Some(body),
        Err(failure) => {
            envelope.progress = failed_progress;
            failed_body(&mut envelope, failure).ok()
        }
    };
    state.retained_terminal_bytes = state
        .retained_terminal_bytes
        .saturating_add(body_bytes(body.as_ref()));
    let terminal_order = state.next_terminal_order;
    state.next_terminal_order = state.next_terminal_order.saturating_add(1);
    if let Some(record) = state.records.get_mut(&job_id) {
        record.state = envelope.state;
        record.finished_at = envelope.finished_at;
        record.finished_instant = Some(Instant::now());
        record.terminal_order = Some(terminal_order);
        record.terminal_body = body;
    }
    enforce_terminal_limit(state, limits, job_id);
    Ending::Ended(
        envelope.state,
        envelope.failure.as_ref().map(|failure| failure.code),
    )
}

/// The body of `envelope` failed with `failure` instead.
fn failed_body(
    envelope: &mut JobEnvelope,
    failure: JobFailure,
) -> serde_json::Result<TerminalBody> {
    envelope.state = JobState::Failed;
    envelope.failure = Some(failure);
    TerminalBody::new(envelope, None)
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
        .filter(|(_, record)| !record.terminal_delivery_in_progress)
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

/// Drops the oldest kept bodies other than `kept` and those being delivered
/// while more terminal jobs, or more of their bytes, are kept than `limits`
/// allow.
fn enforce_terminal_limit(state: &mut RegistryState, limits: &JobLimits, kept: Uuid) {
    while terminal_jobs(state) > limits.max_terminal_jobs
        || state.retained_terminal_bytes > limits.max_terminal_bytes
    {
        let Some(job_id) = oldest_evictable(state, kept) else {
            break;
        };
        remove_job(state, job_id);
    }
}

/// Drops the oldest kept bodies other than `kept` and those being delivered
/// until one more body of `needed` bytes fits, and whether it does. Nothing is
/// dropped when the bodies being delivered leave too little room.
fn make_room(state: &mut RegistryState, limits: &JobLimits, kept: Uuid, needed: u64) -> bool {
    let fits = |jobs: usize, bytes: u64| {
        jobs < limits.max_terminal_jobs && bytes.saturating_add(needed) <= limits.max_terminal_bytes
    };
    let (evictable_jobs, evictable_bytes) = state
        .records
        .values()
        .filter(|record| evictable(record, kept))
        .fold((0_usize, 0_u64), |(jobs, bytes), record| {
            (
                jobs.saturating_add(1),
                bytes.saturating_add(body_bytes(record.terminal_body.as_ref())),
            )
        });
    if !fits(
        terminal_jobs(state).saturating_sub(evictable_jobs),
        state
            .retained_terminal_bytes
            .saturating_sub(evictable_bytes),
    ) {
        return false;
    }
    while !fits(terminal_jobs(state), state.retained_terminal_bytes) {
        let Some(job_id) = oldest_evictable(state, kept) else {
            break;
        };
        remove_job(state, job_id);
    }
    true
}

fn terminal_jobs(state: &RegistryState) -> usize {
    state
        .records
        .values()
        .filter(|record| is_terminal(record.state))
        .count()
}

fn oldest_evictable(state: &RegistryState, kept: Uuid) -> Option<Uuid> {
    state
        .records
        .values()
        .filter(|record| evictable(record, kept))
        .min_by_key(|record| record.terminal_order)
        .map(|record| record.job_id)
}

fn evictable(record: &JobRecord, kept: Uuid) -> bool {
    is_terminal(record.state) && !record.terminal_delivery_in_progress && record.job_id != kept
}

fn remove_job(state: &mut RegistryState, job_id: Uuid) {
    if let Some(record) = state.records.remove(&job_id) {
        state.request_ids.remove(&record.request_id);
        state.retained_terminal_bytes = state
            .retained_terminal_bytes
            .saturating_sub(body_bytes(record.terminal_body.as_ref()));
    }
    state.queue.retain(|queued| *queued != job_id);
}

fn snapshot_locked(state: &RegistryState) -> JobSnapshot {
    let queued = u64::try_from(state.queue.len()).unwrap_or(u64::MAX);
    let running = u64::try_from(state.running).unwrap_or(u64::MAX);
    let retained_terminal = u64::try_from(terminal_jobs(state)).unwrap_or(u64::MAX);
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

fn body_bytes(body: Option<&TerminalBody>) -> u64 {
    body.map_or(0, TerminalBody::len)
}

fn read_limit_failure() -> JobFailure {
    JobFailure {
        code: JobFailureCode::ReadLimitExceeded,
        message: READ_LIMIT_EXCEEDED.to_owned(),
        retryable: false,
        details: None,
    }
}

pub(super) fn internal_failure() -> JobFailure {
    JobFailure {
        code: JobFailureCode::InternalError,
        message: "job result could not be prepared".to_owned(),
        retryable: false,
        details: None,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::{Context, Result};
    use tokio::time::timeout;

    use super::{
        FinishedJob, JobLimits, JobRegistry, JobRequest, JobState, ScheduledJob, SubmitOutcome,
    };

    #[tokio::test(start_paused = true)]
    async fn shutdown_after_an_empty_schedule_wakes_every_waiter() -> Result<()> {
        let registry = JobRegistry::new(JobLimits::default());
        let first = registry.schedule().await;
        let second = registry.schedule().await;
        assert!(first.jobs.is_empty() && second.jobs.is_empty());
        assert!(!first.stopped && !second.stopped);
        assert!(first.next_deadline.is_none() && second.next_deadline.is_none());
        let first_notification = first.notified;
        let second_notification = second.notified;

        // Shutdown lands after state inspection, before either scheduler polls its wait.
        registry.begin_shutdown().await;
        assert!(registry.snapshot().await.draining);
        timeout(Duration::from_secs(1), async {
            first_notification.await;
            second_notification.await;
        })
        .await
        .context("shutdown must wake every scheduler that already checked its state")?;
        assert!(registry.schedule().await.stopped);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn submission_and_completion_wake_a_scheduler_before_it_polls() -> Result<()> {
        let registry = JobRegistry::new(JobLimits::default());
        let empty = registry.schedule().await;
        assert!(empty.jobs.is_empty());
        let admission_notification = empty.notified;
        let request = serde_json::from_value(serde_json::json!({
            "requestId": "00000000-0000-4000-8000-000000000001",
            "apiRevision": 1,
            "timeoutMs": 60000,
            "chainId": 8453,
            "pool": {
                "backend": "native",
                "protocol": "uniswap_v3",
                "componentId": "pool-1",
                "tokenIn": "0x01",
                "tokenOut": "0x02"
            },
            "blocks": {"start": 100, "endInclusive": 100, "step": 1},
            "lags": [1],
            "amountsIn": ["1"]
        }))?;
        let submitted = registry
            .submit(ScheduledJob::new(JobRequest::HistoricalQuote(request), 1))
            .await;
        assert!(matches!(submitted, SubmitOutcome::Accepted(_)));
        timeout(Duration::from_secs(1), admission_notification)
            .await
            .context("submission must wake the scheduler")?;

        let running = registry.schedule().await;
        assert_eq!(running.jobs.len(), 1);
        let job_id = running.jobs[0].job_id;
        let completion_notification = running.notified;
        assert_eq!(registry.snapshot().await.reserved_decoded_bytes, 1);
        registry.finish(job_id, FinishedJob::Cancelled).await;
        timeout(Duration::from_secs(1), completion_notification)
            .await
            .context("completion must wake the scheduler")?;
        assert_eq!(registry.snapshot().await.reserved_decoded_bytes, 1);
        registry.release(1).await;
        assert_eq!(registry.snapshot().await.reserved_decoded_bytes, 0);
        assert_eq!(
            registry
                .inspect(job_id)
                .await
                .context("job must remain retained")?
                .state,
            JobState::Cancelled
        );
        assert!(registry.schedule().await.jobs.is_empty());
        Ok(())
    }
}
