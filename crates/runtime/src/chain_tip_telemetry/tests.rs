use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use serde_json::Value;
use tycho_common::Bytes;

use super::*;
use crate::chain_head::{ChainHeadConfig, ChainHeadObserver};

fn head(number: u64, hash: u8) -> BlockIdentity {
    BlockIdentity {
        number,
        hash: Bytes::from(vec![hash; 32]),
    }
}

fn gap(blocks: u64) -> Observation {
    let observer = ChainHeadObserver::ready_for_test(head(100, 1));
    Observation::new(
        Some(head(100 - blocks, 1)),
        &observer.snapshot(),
        None,
        Some("ready"),
    )
}

fn observe(
    tracker: &mut Tracker,
    start: Instant,
    tick: u64,
    observation: &Observation,
) -> Option<EpisodeEvent> {
    tracker.observe(
        start + Duration::from_millis(tick * 100),
        1_000 + tick * 100,
        observation,
    )
}

#[test]
fn episodes_keep_peak_until_number_and_hash_match() {
    for gaps in [vec![0, 1, 0], vec![0, 1, 50, 20, 0], vec![0, 10, 5, 1, 0]] {
        let mut tracker = Tracker::new();
        let start = Instant::now();
        let events: Vec<_> = gaps
            .iter()
            .enumerate()
            .filter_map(|(index, blocks)| observe(&mut tracker, start, index as u64, &gap(*blocks)))
            .collect();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, "chain_tip_episode_started");
        assert_eq!(events[1].kind, "chain_tip_episode_completed");
        let episode = &events[1].episode;
        assert_eq!(episode.peak_gap_blocks, *gaps.iter().max().unwrap());
        assert_eq!(episode.observations, gaps.len() as u64 - 1);
        assert!(episode.start_observed && episode.continuous_observations);
        assert_eq!(episode.started, start + SAMPLE_INTERVAL);
        assert_eq!(
            (start + SAMPLE_INTERVAL * (gaps.len() as u32 - 1)) - episode.started,
            SAMPLE_INTERVAL * (gaps.len() as u32 - 2)
        );
        assert!(observe(&mut tracker, start, gaps.len() as u64, &gap(0)).is_none());
        assert!(tracker.episode.is_none());
    }
}

#[test]
fn initially_behind_is_left_censored_until_a_new_matching_boundary() {
    let start = Instant::now();
    let mut tracker = Tracker::new();
    let first = observe(&mut tracker, start, 0, &gap(10)).unwrap();
    assert!(!first.episode.start_observed);
    let completed = observe(&mut tracker, start, 1, &gap(0)).unwrap();
    assert!(!completed.episode.start_observed);
    assert!(completed.episode.continuous_observations);
    let next = observe(&mut tracker, start, 2, &gap(1)).unwrap();
    assert!(next.episode.start_observed);
    assert_eq!(next.episode.sequence, 2);
}

fn uncertain_observations() -> Vec<Observation> {
    let observer = ChainHeadObserver::ready_for_test(head(100, 1));
    let unavailable = ChainHeadObserver::new(ChainHeadConfig {
        poll_interval: Duration::from_secs(1),
        rpc_request_timeout: Duration::from_secs(2),
        observation_max_age: Duration::from_secs(5),
    });
    vec![
        Observation::new(Some(head(100, 2)), &observer.snapshot(), None, None),
        Observation::new(Some(head(101, 1)), &observer.snapshot(), None, None),
        Observation::new(None, &observer.snapshot(), None, None),
        Observation::new(Some(head(100, 1)), &unavailable.snapshot(), None, None),
        Observation::new(
            Some(head(100, 1)),
            &observer.snapshot(),
            Some("passive"),
            None,
        ),
    ]
}

#[test]
fn uncertainty_censors_an_episode_and_invalidates_a_prior_match() {
    for uncertain in uncertain_observations() {
        let start = Instant::now();
        let mut tracker = Tracker::new();
        observe(&mut tracker, start, 0, &gap(0));
        observe(&mut tracker, start, 1, &gap(1));
        assert!(observe(&mut tracker, start, 2, &uncertain).is_none());
        assert!(observe(&mut tracker, start, 3, &gap(5)).is_none());
        let completed = observe(&mut tracker, start, 4, &gap(0)).unwrap();
        assert_eq!(completed.episode.peak_gap_blocks, 5);
        assert!(completed.episode.start_observed);
        assert!(!completed.episode.continuous_observations);
        assert!(completed
            .episode
            .reasons
            .contains(uncertain.reason().unwrap()));
        assert_eq!(tracker.coverage.unclassifiable, 1);
        assert_eq!(tracker.coverage.matching, 2);
        assert_eq!(
            tracker.coverage.hash_mismatch,
            u64::from(uncertain.agreement == ChainHeadAgreement::HashMismatch)
        );
        observe(&mut tracker, start, 5, &uncertain);
        assert!(
            !observe(&mut tracker, start, 6, &gap(1))
                .unwrap()
                .episode
                .start_observed
        );
    }
}

#[tokio::test(start_paused = true)]
async fn expired_rpc_observation_is_unknown_even_when_heads_match() {
    let observer = ChainHeadObserver::ready_for_test(head(100, 1));
    tokio::time::advance(Duration::from_secs(121)).await;
    let observation = Observation::new(Some(head(100, 1)), &observer.snapshot(), None, None);
    assert_eq!(
        observation.agreement,
        ChainHeadAgreement::ObservationUnavailable
    );
    let mut tracker = Tracker::new();
    tracker.observe(Instant::now(), 1000, &observation);
    assert_eq!(tracker.coverage.matching, 0);
    assert_eq!(tracker.coverage.unclassifiable, 1);
}

#[test]
fn delayed_samples_censor_boundaries_and_active_episodes() {
    for spacing in [Duration::from_millis(200), Duration::from_millis(201)] {
        let start = Instant::now();
        let mut tracker = Tracker::new();
        tracker.observe(start, 1000, &gap(0));
        let event = tracker.observe(start + spacing, 1200, &gap(1)).unwrap();
        assert_eq!(event.episode.start_observed, spacing <= MAX_SAMPLE_SPACING);
        assert_eq!(
            event.episode.continuous_observations,
            spacing <= MAX_SAMPLE_SPACING
        );
        let event = tracker.observe(start + spacing * 2, 1400, &gap(0)).unwrap();
        assert_eq!(event.episode.max_spacing, spacing);
        assert_eq!(
            event.episode.continuous_observations,
            spacing <= MAX_SAMPLE_SPACING
        );
    }
    let start = Instant::now();
    let mut tracker = Tracker::new();
    observe(&mut tracker, start, 0, &gap(0));
    observe(&mut tracker, start, 1, &gap(1));
    let completed = observe(&mut tracker, start, 5, &gap(0)).unwrap();
    assert!(completed.episode.start_observed);
    assert!(!completed.episode.continuous_observations);
    assert_eq!(tracker.coverage.delayed, 1);
}

#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl Write for CapturedLogs {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl CapturedLogs {
    fn records(&self) -> Result<Vec<Value>> {
        let bytes = self.0.lock().unwrap();
        let text = std::str::from_utf8(&bytes)?;
        text.lines()
            .map(|line| serde_json::from_str(line).map_err(Into::into))
            .collect()
    }
}

fn emit_observation(
    telemetry: &mut Telemetry,
    now: Instant,
    wall_ms: u64,
    observation: Observation,
) {
    if let Some(event) = telemetry.tracker.observe(now, wall_ms, &observation) {
        telemetry.emit(event.kind, Some(&event.episode), &observation, now, wall_ms);
    }
    telemetry.latest = Some(observation);
}

#[test]
fn json_records_support_episode_cohorts_and_coverage_queries() -> Result<()> {
    let logs = CapturedLogs::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        let mut telemetry = Telemetry::new(8453, "simulator", "native");
        let start = Instant::now();
        for (tick, blocks) in [0, 1, 50, 20, 0].into_iter().enumerate() {
            emit_observation(
                &mut telemetry,
                start + SAMPLE_INTERVAL * tick as u32,
                1000 + tick as u64 * 100,
                gap(blocks),
            );
        }
        emit_observation(&mut telemetry, start + SAMPLE_INTERVAL * 5, 1500, gap(1));
        emit_observation(
            &mut telemetry,
            start + SAMPLE_INTERVAL * 6,
            1600,
            uncertain_observations().remove(0),
        );
        emit_observation(&mut telemetry, start + SAMPLE_INTERVAL * 7, 1700, gap(0));
        emit_observation(&mut telemetry, start + SAMPLE_INTERVAL * 8, 1800, gap(10));
        telemetry.summarize(start + SAMPLE_INTERVAL * 9, 1900);
        let episode = telemetry.tracker.episode.as_ref().unwrap();
        telemetry.emit(
            "chain_tip_episode_unfinished",
            Some(episode),
            telemetry.latest.as_ref().unwrap(),
            start + SAMPLE_INTERVAL * 10,
            2000,
        );
    });
    let records = logs.records()?;
    let completed: Vec<_> = records
        .iter()
        .map(|record| &record["fields"])
        .filter(|fields| {
            fields["event"] == "chain_tip_episode_completed"
                && fields["start_observed"] == true
                && fields["continuous_observations"] == true
                && fields["episode_started_at_unix_ms"].as_u64().unwrap() >= 1000
        })
        .collect();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0]["peak_gap_blocks"], 50);
    assert_eq!(completed[0]["duration_ms"], 300);
    assert_eq!(completed[0]["peak_local_number"], 50);
    assert_eq!(completed[0]["local_number"], 100);
    assert_eq!(completed[0]["head_stage"], "applied");
    let summary = &records[5]["fields"];
    assert_eq!(summary["event"], "chain_tip_observation_summary");
    assert_eq!(summary["matching_observations"], 3);
    assert_eq!(summary["behind_observations"], 5);
    assert_eq!(summary["unclassifiable_observations"], 1);
    assert_eq!(summary["peak_gap_blocks"], 10);
    assert!(summary.get("duration_ms").is_none());
    assert_eq!(
        records[6]["fields"]["event"],
        "chain_tip_episode_unfinished"
    );
    assert!(records[6]["fields"].get("duration_ms").is_none());
    Ok(())
}

#[test]
fn local_log_volume_is_per_episode_and_summary_not_per_sample() -> Result<()> {
    let logs = CapturedLogs::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        let mut telemetry = Telemetry::new(8453, "broadcaster", "native");
        let start = Instant::now();
        for tick in 0..=600 {
            emit_observation(
                &mut telemetry,
                start + SAMPLE_INTERVAL * tick,
                1000 + u64::from(tick) * 100,
                gap(0),
            );
            if tick > 0 && tick % 150 == 0 {
                telemetry.summarize(start + SAMPLE_INTERVAL * tick, 1000 + u64::from(tick) * 100);
            }
        }
    });
    assert_eq!(logs.records()?.len(), 4);
    println!(
        "steady minute: 601 observations, 4 records, {} bytes",
        logs.0.lock().unwrap().len()
    );
    let logs = CapturedLogs::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        let mut telemetry = Telemetry::new(8453, "broadcaster", "native");
        let start = Instant::now();
        for tick in 0..=600 {
            emit_observation(
                &mut telemetry,
                start + SAMPLE_INTERVAL * tick,
                1000 + u64::from(tick) * 100,
                gap(u64::from(tick % 2)),
            );
            if tick > 0 && tick % 150 == 0 {
                telemetry.summarize(start + SAMPLE_INTERVAL * tick, 1000 + u64::from(tick) * 100);
            }
        }
    });
    assert_eq!(logs.records()?.len(), 604);
    println!(
        "alternating minute: 601 observations, 604 records, {} bytes",
        logs.0.lock().unwrap().len()
    );
    Ok(())
}
