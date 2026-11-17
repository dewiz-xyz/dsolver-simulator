//! Observed catch-up episodes against the existing RPC observer, independent of readiness.

use std::collections::BTreeSet;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use simulator_core::broadcaster::BlockIdentity;
use tracing::info;

use crate::chain_head::{ChainHeadAgreement, ChainHeadSnapshot};

pub(crate) const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const SUMMARY_INTERVAL: Duration = Duration::from_secs(15);
const MAX_SAMPLE_SPACING: Duration = Duration::from_millis(200);

#[derive(Clone, Debug)]
pub(crate) struct Observation {
    pub(crate) local: Option<BlockIdentity>,
    pub(crate) observed: Option<BlockIdentity>,
    pub(crate) agreement: ChainHeadAgreement,
    pub(crate) unavailable_reason: Option<&'static str>,
    pub(crate) readiness: Option<&'static str>,
}

impl Observation {
    pub(crate) fn new(
        local: Option<BlockIdentity>,
        observer: &ChainHeadSnapshot,
        unavailable_reason: Option<&'static str>,
        readiness: Option<&'static str>,
    ) -> Self {
        Self {
            agreement: observer.agreement(local.as_ref()),
            local,
            observed: observer.observed_head.clone(),
            unavailable_reason,
            readiness,
        }
    }

    fn gap(&self) -> Option<u64> {
        if self.unavailable_reason.is_some() || self.agreement != ChainHeadAgreement::ObserverAhead
        {
            return None;
        }
        Some(self.observed.as_ref()?.number - self.local.as_ref()?.number)
    }

    fn reason(&self) -> Option<&'static str> {
        self.unavailable_reason.or(match self.agreement {
            ChainHeadAgreement::Matches | ChainHeadAgreement::ObserverAhead => None,
            agreement => Some(agreement.as_str()),
        })
    }
}

#[derive(Clone, Debug)]
struct Episode {
    sequence: u64,
    started: Instant,
    started_at_unix_ms: u64,
    start_observed: bool,
    continuous_observations: bool,
    peak_gap_blocks: u64,
    observations: u64,
    max_spacing: Duration,
    reasons: BTreeSet<&'static str>,
    initial: Observation,
    peak: Observation,
}

#[derive(Default)]
struct Coverage {
    matching: u64,
    behind: u64,
    unclassifiable: u64,
    hash_mismatch: u64,
    delayed: u64,
    max_spacing: Duration,
    reasons: BTreeSet<&'static str>,
}

struct Tracker {
    previous_at: Option<Instant>,
    matched: bool,
    episode: Option<Episode>,
    sequence: u64,
    coverage: Coverage,
}

struct EpisodeEvent {
    kind: &'static str,
    episode: Episode,
}

impl Tracker {
    fn new() -> Self {
        Self {
            previous_at: None,
            matched: false,
            episode: None,
            sequence: 0,
            coverage: Coverage::default(),
        }
    }

    fn observe(
        &mut self,
        now: Instant,
        unix_ms: u64,
        observation: &Observation,
    ) -> Option<EpisodeEvent> {
        let spacing = self
            .previous_at
            .map_or(Duration::ZERO, |previous| now - previous);
        self.previous_at = Some(now);
        self.coverage.max_spacing = self.coverage.max_spacing.max(spacing);
        let delayed = spacing > MAX_SAMPLE_SPACING;
        if delayed {
            self.coverage.delayed += 1;
            self.coverage.reasons.insert("observation_delayed");
            self.matched = false;
        }
        let reason = observation.reason();
        // A later valid gap cannot prove what happened during an unknown interval.
        if let Some(episode) = &mut self.episode {
            episode.observations += 1;
            episode.max_spacing = episode.max_spacing.max(spacing);
            if delayed {
                episode.continuous_observations = false;
                episode.reasons.insert("observation_delayed");
            }
            if let Some(reason) = reason {
                episode.continuous_observations = false;
                episode.reasons.insert(reason);
            }
        }
        if let Some(reason) = reason {
            self.coverage.unclassifiable += 1;
            self.coverage.hash_mismatch +=
                u64::from(observation.agreement == ChainHeadAgreement::HashMismatch);
            self.coverage.reasons.insert(reason);
            self.matched = false;
            return None;
        }
        if let Some(gap) = observation.gap() {
            self.coverage.behind += 1;
            return self.observe_gap(now, unix_ms, observation, gap, spacing);
        }
        self.coverage.matching += 1;
        self.matched = true;
        self.episode.take().map(|episode| EpisodeEvent {
            kind: "chain_tip_episode_completed",
            episode,
        })
    }

    fn observe_gap(
        &mut self,
        now: Instant,
        unix_ms: u64,
        observation: &Observation,
        gap: u64,
        spacing: Duration,
    ) -> Option<EpisodeEvent> {
        if let Some(episode) = &mut self.episode {
            if gap > episode.peak_gap_blocks {
                episode.peak_gap_blocks = gap;
                episode.peak = observation.clone();
            }
            return None;
        }
        self.sequence += 1;
        let delayed = spacing > MAX_SAMPLE_SPACING;
        let episode = Episode {
            sequence: self.sequence,
            started: now,
            started_at_unix_ms: unix_ms,
            start_observed: self.matched,
            continuous_observations: !delayed,
            peak_gap_blocks: gap,
            observations: 1,
            max_spacing: spacing,
            reasons: if delayed {
                BTreeSet::from(["observation_delayed"])
            } else {
                BTreeSet::new()
            },
            initial: observation.clone(),
            peak: observation.clone(),
        };
        self.matched = false;
        self.episode = Some(episode.clone());
        Some(EpisodeEvent {
            kind: "chain_tip_episode_started",
            episode,
        })
    }
}

/// Own one instance per service/backend, outside stream and subscription attempts.
pub(crate) struct Telemetry {
    chain_id: u64,
    service: &'static str,
    backend: &'static str,
    tracker_run_id: String,
    tracker: Tracker,
    summary_started: Instant,
    summary_started_at_unix_ms: u64,
    latest: Option<Observation>,
}

impl Telemetry {
    pub(crate) fn new(chain_id: u64, service: &'static str, backend: &'static str) -> Self {
        let wall_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Self {
            chain_id,
            service,
            backend,
            tracker_run_id: format!("{}-{}", std::process::id(), wall_time.as_nanos()),
            tracker: Tracker::new(),
            summary_started: Instant::now(),
            summary_started_at_unix_ms: millis(wall_time),
            latest: None,
        }
    }

    pub(crate) fn observe(&mut self, observation: Observation) {
        let now = Instant::now();
        let unix_ms = unix_ms();
        if let Some(event) = self.tracker.observe(now, unix_ms, &observation) {
            self.emit(event.kind, Some(&event.episode), &observation, now, unix_ms);
        }
        self.latest = Some(observation);
        if now - self.summary_started >= SUMMARY_INTERVAL {
            self.summarize(now, unix_ms);
        }
    }

    pub(crate) fn finish(&mut self) {
        let now = Instant::now();
        let unix_ms = unix_ms();
        if let (Some(episode), Some(observation)) =
            (self.tracker.episode.as_ref(), self.latest.as_ref())
        {
            self.emit(
                "chain_tip_episode_unfinished",
                Some(episode),
                observation,
                now,
                unix_ms,
            );
        }
        self.summarize(now, unix_ms);
    }

    fn summarize(&mut self, now: Instant, unix_ms: u64) {
        if let Some(observation) = &self.latest {
            self.emit(
                "chain_tip_observation_summary",
                self.tracker.episode.as_ref(),
                observation,
                now,
                unix_ms,
            );
        }
        self.tracker.coverage = Coverage::default();
        self.summary_started = now;
        self.summary_started_at_unix_ms = unix_ms;
    }

    fn emit(
        &self,
        event: &'static str,
        episode: Option<&Episode>,
        observation: &Observation,
        now: Instant,
        unix_ms: u64,
    ) {
        let coverage = &self.tracker.coverage;
        let completed = event == "chain_tip_episode_completed";
        let summary = event == "chain_tip_observation_summary";
        let initial_local = episode.and_then(|value| value.initial.local.as_ref());
        let initial_observed = episode.and_then(|value| value.initial.observed.as_ref());
        let peak_local = episode.and_then(|value| value.peak.local.as_ref());
        let peak_observed = episode.and_then(|value| value.peak.observed.as_ref());
        let local = observation.local.as_ref();
        let observed = observation.observed.as_ref();
        info!(
            event,
            schema_version = 1,
            chain_id = self.chain_id,
            service = self.service,
            backend = self.backend,
            head_stage = if self.service == "broadcaster" {
                "published"
            } else {
                "applied"
            },
            tracker_run_id = self.tracker_run_id,
            observed_at_unix_ms = unix_ms,
            target_interval_ms = millis(SAMPLE_INTERVAL),
            delayed_observation_threshold_ms = millis(MAX_SAMPLE_SPACING),
            agreement = observation.agreement.as_str(),
            unavailable_reason = observation.unavailable_reason,
            readiness = observation.readiness,
            episode_sequence = episode.map(|value| value.sequence),
            episode_started_at_unix_ms = episode.map(|value| value.started_at_unix_ms),
            start_observed = episode.map(|value| value.start_observed),
            continuous_observations = episode.map(|value| value.continuous_observations),
            peak_gap_blocks = episode.map(|value| value.peak_gap_blocks),
            observations = episode.map(|value| value.observations),
            duration_ms = episode
                .filter(|_| completed)
                .map(|value| millis(now - value.started)),
            elapsed_ms = episode
                .filter(|_| !completed)
                .map(|value| millis(now - value.started)),
            max_observation_interval_ms = episode.map(|value| millis(value.max_spacing)),
            uncertainty_reasons = episode.map(|value| format!("{:?}", value.reasons)),
            initial_local_number = initial_local.map(|head| head.number),
            initial_local_hash = initial_local.map(|head| head.hash.to_string()),
            initial_observed_number = initial_observed.map(|head| head.number),
            initial_observed_hash = initial_observed.map(|head| head.hash.to_string()),
            peak_local_number = peak_local.map(|head| head.number),
            peak_local_hash = peak_local.map(|head| head.hash.to_string()),
            peak_observed_number = peak_observed.map(|head| head.number),
            peak_observed_hash = peak_observed.map(|head| head.hash.to_string()),
            local_number = local.map(|head| head.number),
            local_hash = local.map(|head| head.hash.to_string()),
            observed_number = observed.map(|head| head.number),
            observed_hash = observed.map(|head| head.hash.to_string()),
            summary_started_at_unix_ms = summary.then_some(self.summary_started_at_unix_ms),
            summary_elapsed_ms = summary.then(|| millis(now - self.summary_started)),
            matching_observations = summary.then_some(coverage.matching),
            behind_observations = summary.then_some(coverage.behind),
            unclassifiable_observations = summary.then_some(coverage.unclassifiable),
            hash_mismatch_observations = summary.then_some(coverage.hash_mismatch),
            delayed_observations = summary.then_some(coverage.delayed),
            summary_max_observation_interval_ms = summary.then(|| millis(coverage.max_spacing)),
            coverage_reasons = summary.then(|| format!("{:?}", coverage.reasons)),
            "Observed chain tip catch-up telemetry"
        );
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn unix_ms() -> u64 {
    millis(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default(),
    )
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    reason = "deterministic test fixtures and event assertions"
)]
mod tests;
