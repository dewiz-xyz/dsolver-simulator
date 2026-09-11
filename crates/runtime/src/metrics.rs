use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Map, Value};
use tracing::warn;

use crate::broadcaster::state::{BroadcasterReadiness, BroadcasterStatusSnapshot};
use crate::models::messages::{QuoteResultQuality, QuoteStatus};
use crate::models::state::{
    SimulatorBackendKind, SimulatorBackendReadiness, SimulatorBackendStatusSnapshot,
    SimulatorStatusSnapshot,
};
use simulator_core::broadcaster::BroadcasterBackend;

const METRIC_NAMESPACE: &str = "Tycho/Simulation";
const METRIC_SIMULATE_COMPLETION: &str = "SimulateCompletion";
const METRIC_SIMULATE_RESULT_QUALITY: &str = "SimulateResultQuality";
const METRIC_SIMULATE_TIMEOUT: &str = "SimulateRequestTimeout";
const METRIC_BROADCASTER_REDIS_APPEND_FAILURE: &str = "BroadcasterRedisAppendFailure";
const METRIC_BROADCASTER_WRITER_FENCE_FAILURE: &str = "BroadcasterWriterFenceFailure";
const METRIC_BROADCASTER_RECOVERY_OUTCOME: &str = "BroadcasterRecoveryOutcome";
const METRIC_BROADCASTER_RECOVERY_DURATION: &str = "BroadcasterRecoveryDuration";
const METRIC_BROADCASTER_RECOVERY_ENCODED_BYTES: &str = "BroadcasterRecoveryEncodedBytes";
const METRIC_BROADCASTER_RECOVERY_CHUNK_COUNT: &str = "BroadcasterRecoveryChunkCount";
const METRIC_BROADCASTER_RECOVERY_BUFFER_A_COUNT: &str = "BroadcasterRecoveryBufferACount";
const METRIC_BROADCASTER_RECOVERY_BUFFER_B_COUNT: &str = "BroadcasterRecoveryBufferBCount";
const METRIC_BROADCASTER_SNAPSHOT_EXPORT_FAILURE: &str = "BroadcasterSnapshotExportFailure";
const METRIC_BROADCASTER_REDIS_TRANSPORT_FAILURE: &str = "BroadcasterRedisTransportFailure";
const METRIC_BROADCASTER_REDIS_REBOOTSTRAP: &str = "BroadcasterRedisRebootstrap";
const METRIC_JEMALLOC_ALLOCATED_BYTES: &str = "JemallocAllocatedBytes";
const METRIC_JEMALLOC_RESIDENT_BYTES: &str = "JemallocResidentBytes";
const DIM_STATUS: &str = "Status";
const DIM_TIMED_OUT: &str = "TimedOut";
const DIM_TIMEOUT_KIND: &str = "TimeoutKind";
const DIM_RESULT_QUALITY: &str = "ResultQuality";
const DIM_LABEL: &str = "Label";
const DIM_OPERATION: &str = "Operation";
const DIM_REASON: &str = "Reason";
const DIM_OUTCOME: &str = "Outcome";

const UNIT_COUNT: &str = "Count";
const UNIT_MILLISECONDS: &str = "Milliseconds";

#[derive(Debug, Clone, Copy)]
pub enum TimeoutKind {
    RequestGuard,
    RouterBoundary,
}

impl TimeoutKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::RequestGuard => "request_guard",
            Self::RouterBoundary => "router_boundary",
        }
    }
}

pub fn emit_simulate_completion(status: QuoteStatus, timed_out: bool) {
    emit_count_metric(
        METRIC_SIMULATE_COMPLETION,
        &[
            (DIM_STATUS, json!(status)),
            (
                DIM_TIMED_OUT,
                json!(if timed_out { "true" } else { "false" }),
            ),
        ],
    );
}

pub fn emit_simulate_timeout(kind: TimeoutKind) {
    emit_count_metric(
        METRIC_SIMULATE_TIMEOUT,
        &[(DIM_TIMEOUT_KIND, json!(kind.as_str()))],
    );
}

pub fn emit_simulate_result_quality(quality: QuoteResultQuality) {
    emit_count_metric(
        METRIC_SIMULATE_RESULT_QUALITY,
        &[(DIM_RESULT_QUALITY, json!(quality))],
    );
}

pub fn emit_broadcaster_redis_append_failure() {
    emit_count_metric(METRIC_BROADCASTER_REDIS_APPEND_FAILURE, &[]);
}

pub fn emit_broadcaster_writer_fence_failure() {
    emit_count_metric(METRIC_BROADCASTER_WRITER_FENCE_FAILURE, &[]);
}

pub fn emit_broadcaster_snapshot_export_failure() {
    emit_count_metric(METRIC_BROADCASTER_SNAPSHOT_EXPORT_FAILURE, &[]);
}

pub fn emit_broadcaster_recovery_outcome(
    outcome: &'static str,
    duration_ms: u64,
    encoded_bytes: usize,
    chunk_count: usize,
    buffer_a_count: usize,
    buffer_b_count: usize,
) {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0);
    let aws = json!({
        "Timestamp": timestamp_ms,
        "CloudWatchMetrics": [{
            "Namespace": METRIC_NAMESPACE,
            "Dimensions": [[DIM_OUTCOME]],
            "Metrics": [
                { "Name": METRIC_BROADCASTER_RECOVERY_OUTCOME, "Unit": "Count" },
                { "Name": METRIC_BROADCASTER_RECOVERY_DURATION, "Unit": "Milliseconds" },
                { "Name": METRIC_BROADCASTER_RECOVERY_ENCODED_BYTES, "Unit": "Bytes" },
                { "Name": METRIC_BROADCASTER_RECOVERY_CHUNK_COUNT, "Unit": "Count" },
                { "Name": METRIC_BROADCASTER_RECOVERY_BUFFER_A_COUNT, "Unit": "Count" },
                { "Name": METRIC_BROADCASTER_RECOVERY_BUFFER_B_COUNT, "Unit": "Count" },
            ],
        }],
    });
    let mut event = Map::new();
    event.insert("_aws".to_string(), aws);
    event.insert(DIM_OUTCOME.to_string(), json!(outcome));
    event.insert(METRIC_BROADCASTER_RECOVERY_OUTCOME.to_string(), json!(1));
    event.insert(
        METRIC_BROADCASTER_RECOVERY_DURATION.to_string(),
        json!(duration_ms),
    );
    event.insert(
        METRIC_BROADCASTER_RECOVERY_ENCODED_BYTES.to_string(),
        json!(encoded_bytes as u64),
    );
    event.insert(
        METRIC_BROADCASTER_RECOVERY_CHUNK_COUNT.to_string(),
        json!(chunk_count as u64),
    );
    event.insert(
        METRIC_BROADCASTER_RECOVERY_BUFFER_A_COUNT.to_string(),
        json!(buffer_a_count as u64),
    );
    event.insert(
        METRIC_BROADCASTER_RECOVERY_BUFFER_B_COUNT.to_string(),
        json!(buffer_b_count as u64),
    );

    emit_emf_event(event, "broadcaster_recovery_outcome");
}

pub fn emit_broadcaster_redis_transport_failure(operation: &'static str) {
    emit_count_metric(
        METRIC_BROADCASTER_REDIS_TRANSPORT_FAILURE,
        &[(DIM_OPERATION, json!(operation))],
    );
}

pub fn emit_broadcaster_redis_rebootstrap(reason: &'static str) {
    emit_count_metric(
        METRIC_BROADCASTER_REDIS_REBOOTSTRAP,
        &[(DIM_REASON, json!(reason))],
    );
}

pub fn emit_broadcaster_health_snapshot(snapshot: &BroadcasterStatusSnapshot) {
    let native = snapshot.backends.get(&BroadcasterBackend::Native);
    let vm = snapshot.backends.get(&BroadcasterBackend::Vm);
    let rfq = snapshot.backends.get(&BroadcasterBackend::Rfq);
    let mut metrics = vec![
        (
            "BroadcasterNativeReady",
            u64::from(snapshot.readiness == BroadcasterReadiness::Ready),
            UNIT_COUNT,
        ),
        (
            "BroadcasterTychoConnected",
            u64::from(snapshot.upstream.connected),
            UNIT_COUNT,
        ),
        (
            "BroadcasterTychoRestartCount",
            snapshot.upstream.restart_count,
            UNIT_COUNT,
        ),
        (
            "BroadcasterRecoveryActive",
            u64::from(snapshot.recovery.active),
            UNIT_COUNT,
        ),
        (
            "BroadcasterRecoveryBufferACount",
            snapshot.recovery.buffer_a_count as u64,
            UNIT_COUNT,
        ),
        (
            "BroadcasterRecoveryBufferBCount",
            snapshot.recovery.buffer_b_count as u64,
            UNIT_COUNT,
        ),
        (
            "BroadcasterNativeAvailable",
            u64::from(native.and_then(|status| status.block_number).is_some()),
            UNIT_COUNT,
        ),
        (
            "BroadcasterVmAvailable",
            u64::from(vm.and_then(|status| status.block_number).is_some()),
            UNIT_COUNT,
        ),
        (
            "BroadcasterRfqAvailable",
            u64::from(rfq.and_then(|status| status.block_number).is_some()),
            UNIT_COUNT,
        ),
    ];
    push_optional_metric(
        &mut metrics,
        "BroadcasterExposedStateAge",
        snapshot.upstream.last_update_age_ms,
        UNIT_MILLISECONDS,
    );
    push_optional_metric(
        &mut metrics,
        "BroadcasterPublishedNativeHead",
        native.and_then(|status| status.block_number),
        UNIT_COUNT,
    );
    push_optional_metric(
        &mut metrics,
        "BroadcasterRecoveryAge",
        snapshot.recovery.age_ms,
        UNIT_MILLISECONDS,
    );
    push_optional_metric(
        &mut metrics,
        "BroadcasterRecoveryOldestBufferedAge",
        snapshot.recovery.oldest_buffered_age_ms,
        UNIT_MILLISECONDS,
    );
    let mut properties = vec![
        ("Readiness", json!(snapshot.readiness.as_str())),
        ("RecoveryPhase", json!(snapshot.recovery.phase)),
    ];
    if let Some(state_history) = &snapshot.state_history {
        metrics.extend(state_history_health_metrics(
            state_history,
            current_timestamp_ms(),
        ));
        if let Some(status) = &state_history.last_checkpoint_status {
            properties.push(("StateHistoryCheckpointStatus", json!(status)));
        }
    }
    emit_gauge_snapshot("broadcaster_health", &properties, &metrics);
}

fn state_history_health_metrics(
    status: &state_history::WriterStatus,
    now_ms: u64,
) -> Vec<(&'static str, u64, &'static str)> {
    let queue_utilization_bps = if status.queue_capacity == 0 {
        0
    } else {
        status
            .queue_len
            .saturating_mul(10_000)
            .saturating_div(status.queue_capacity)
            .min(10_000)
    };
    let mut metrics = vec![
        ("StateHistoryHealthy", u64::from(status.healthy), UNIT_COUNT),
        ("StateHistoryQueueLength", status.queue_len, UNIT_COUNT),
        (
            "StateHistoryQueueCapacity",
            status.queue_capacity,
            UNIT_COUNT,
        ),
        (
            "StateHistoryQueueUtilizationBps",
            queue_utilization_bps,
            UNIT_COUNT,
        ),
        (
            "StateHistoryEnqueuedDeltaCount",
            status.enqueued_deltas,
            UNIT_COUNT,
        ),
        (
            "StateHistoryPersistedDeltaCount",
            status.persisted_deltas,
            UNIT_COUNT,
        ),
        (
            "StateHistoryDroppedDeltaCount",
            status.dropped_deltas,
            UNIT_COUNT,
        ),
        (
            "StateHistoryFailedDeltaCount",
            status.failed_deltas,
            UNIT_COUNT,
        ),
        (
            "StateHistoryRecordedGapCount",
            status.recorded_gaps,
            UNIT_COUNT,
        ),
        (
            "StateHistoryCheckpointCompletedCount",
            status.checkpoints_completed,
            UNIT_COUNT,
        ),
        (
            "StateHistoryCheckpointFailedCount",
            status.checkpoints_failed,
            UNIT_COUNT,
        ),
        (
            "StateHistoryLastCheckpointSucceeded",
            u64::from(status.last_checkpoint_status.as_deref() == Some("complete")),
            UNIT_COUNT,
        ),
        (
            "StateHistoryTokenPersistenceFailureCount",
            status.token_persistence_failures,
            UNIT_COUNT,
        ),
    ];
    if let Some(position) = status.last_persisted {
        metrics.push((
            "StateHistoryLastPersistedGeneration",
            position.generation,
            UNIT_COUNT,
        ));
        metrics.push((
            "StateHistoryLastPersistedMessageSequence",
            position.message_seq,
            UNIT_COUNT,
        ));
    }
    push_optional_metric(
        &mut metrics,
        "StateHistoryPersistedDeltaAge",
        status
            .last_persisted_at_ms
            .map(|timestamp| timestamp_age_ms(timestamp, now_ms)),
        UNIT_MILLISECONDS,
    );
    push_optional_metric(
        &mut metrics,
        "StateHistorySuccessfulCheckpointAge",
        status
            .last_successful_checkpoint_at_ms
            .map(|timestamp| timestamp_age_ms(timestamp, now_ms)),
        UNIT_MILLISECONDS,
    );
    metrics
}

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

const fn timestamp_age_ms(timestamp_ms: u64, now_ms: u64) -> u64 {
    now_ms.saturating_sub(timestamp_ms)
}

pub fn emit_simulator_health_snapshot(snapshot: &SimulatorStatusSnapshot) {
    let backend = |kind| {
        snapshot
            .backends
            .iter()
            .find(|backend| backend.kind == kind)
    };
    let native = backend(SimulatorBackendKind::Native);
    let vm = backend(SimulatorBackendKind::Vm);
    let rfq = backend(SimulatorBackendKind::Rfq);
    let is_available = |backend: Option<&SimulatorBackendStatusSnapshot>| {
        backend.is_some_and(|status| status.readiness == SimulatorBackendReadiness::Ready)
    };
    let mut metrics = vec![
        (
            "SimulatorNativeReady",
            u64::from(is_available(native)),
            UNIT_COUNT,
        ),
        (
            "SimulatorNativeAvailable",
            u64::from(is_available(native)),
            UNIT_COUNT,
        ),
        (
            "SimulatorVmAvailable",
            u64::from(is_available(vm)),
            UNIT_COUNT,
        ),
        (
            "SimulatorRfqAvailable",
            u64::from(is_available(rfq)),
            UNIT_COUNT,
        ),
    ];
    push_optional_metric(
        &mut metrics,
        "SimulatorExposedStateAge",
        native.and_then(|status| status.last_update_age_ms),
        UNIT_MILLISECONDS,
    );
    push_optional_metric(
        &mut metrics,
        "SimulatorAppliedNativeHead",
        native.and_then(|status| status.block_number),
        UNIT_COUNT,
    );
    push_optional_metric(
        &mut metrics,
        "BroadcasterPublisherToConsumerDelay",
        native.and_then(|status| status.publisher_to_consumer_delay_ms),
        UNIT_MILLISECONDS,
    );
    emit_gauge_snapshot(
        "simulator_health",
        &[("Status", json!(snapshot.status.label()))],
        &metrics,
    );
}

fn push_optional_metric(
    metrics: &mut Vec<(&'static str, u64, &'static str)>,
    name: &'static str,
    value: Option<u64>,
    unit: &'static str,
) {
    if let Some(value) = value {
        metrics.push((name, value, unit));
    }
}

fn emit_gauge_snapshot(
    event_name: &'static str,
    properties: &[(&'static str, Value)],
    metrics: &[(&'static str, u64, &'static str)],
) {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0);
    let metric_definitions = metrics
        .iter()
        .map(|(name, _, unit)| json!({ "Name": name, "Unit": unit }))
        .collect::<Vec<_>>();
    let aws = json!({
        "Timestamp": timestamp_ms,
        "CloudWatchMetrics": [{
            "Namespace": METRIC_NAMESPACE,
            "Dimensions": [[]],
            "Metrics": metric_definitions,
        }],
    });
    let mut event = Map::new();
    event.insert("_aws".to_string(), aws);
    event.insert("event".to_string(), json!(event_name));
    for (name, value) in properties {
        event.insert((*name).to_string(), value.clone());
    }
    for (name, value, _) in metrics {
        event.insert((*name).to_string(), json!(value));
    }

    emit_emf_event(event, event_name);
}

pub fn emit_jemalloc_snapshot(label: &str, allocated_bytes: usize, resident_bytes: usize) {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0);

    let aws = json!({
        "Timestamp": timestamp_ms,
        "CloudWatchMetrics": [{
            "Namespace": METRIC_NAMESPACE,
            "Dimensions": [[DIM_LABEL]],
            "Metrics": [
                { "Name": METRIC_JEMALLOC_ALLOCATED_BYTES, "Unit": "Bytes" },
                { "Name": METRIC_JEMALLOC_RESIDENT_BYTES, "Unit": "Bytes" },
            ],
        }],
    });

    let mut event = Map::new();
    event.insert("_aws".to_string(), aws);
    event.insert(DIM_LABEL.to_string(), json!(label));
    event.insert(
        METRIC_JEMALLOC_ALLOCATED_BYTES.to_string(),
        json!(allocated_bytes as u64),
    );
    event.insert(
        METRIC_JEMALLOC_RESIDENT_BYTES.to_string(),
        json!(resident_bytes as u64),
    );

    emit_emf_event(event, "jemalloc_snapshot");
}

fn emit_count_metric(metric_name: &str, dimensions: &[(&str, Value)]) {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0);

    let dimension_names: Vec<&str> = dimensions.iter().map(|(name, _)| *name).collect();
    let aws = json!({
        "Timestamp": timestamp_ms,
        "CloudWatchMetrics": [{
            "Namespace": METRIC_NAMESPACE,
            "Dimensions": [dimension_names],
            "Metrics": [{
                "Name": metric_name,
                "Unit": "Count",
            }],
        }],
    });

    let mut event = Map::new();
    event.insert("_aws".to_string(), aws);
    event.insert(metric_name.to_string(), json!(1));
    for (name, value) in dimensions {
        event.insert((*name).to_string(), value.clone());
    }

    emit_emf_event(event, metric_name);
}

fn emit_emf_event(event: Map<String, Value>, metric_name: &str) {
    // Tracing's JSON wrapper prevents CloudWatch from extracting these metrics.
    match serde_json::to_string(&Value::Object(event)) {
        Ok(line) => println!("{line}"),
        Err(error) => warn!(error = %error, metric = metric_name, "Failed to serialize EMF metric"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use state_history::{StreamPosition, WriterStatus};

    use super::*;

    #[test]
    fn state_history_metrics_capture_progress_pressure_and_age() {
        let status = WriterStatus {
            healthy: false,
            queue_len: 2,
            queue_capacity: 8,
            enqueued_deltas: 20,
            persisted_deltas: 17,
            dropped_deltas: 1,
            failed_deltas: 2,
            recorded_gaps: 3,
            last_persisted: Some(StreamPosition {
                generation: 4,
                message_seq: 99,
            }),
            last_persisted_at_ms: Some(8_000),
            last_error: Some("failure".to_owned()),
            checkpoints_completed: 5,
            checkpoints_failed: 1,
            last_checkpoint_status: Some("failed".to_owned()),
            last_successful_checkpoint_at_ms: Some(7_000),
            last_token_snapshot_sha: None,
            token_persistence_failures: 1,
        };
        let metrics = state_history_health_metrics(&status, 10_000)
            .into_iter()
            .map(|(name, value, _)| (name, value))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(metrics.get("StateHistoryQueueUtilizationBps"), Some(&2_500));
        assert_eq!(metrics.get("StateHistoryPersistedDeltaCount"), Some(&17));
        assert_eq!(metrics.get("StateHistoryLastPersistedGeneration"), Some(&4));
        assert_eq!(
            metrics.get("StateHistoryLastPersistedMessageSequence"),
            Some(&99)
        );
        assert_eq!(metrics.get("StateHistoryPersistedDeltaAge"), Some(&2_000));
        assert_eq!(
            metrics.get("StateHistorySuccessfulCheckpointAge"),
            Some(&3_000)
        );
        assert_eq!(metrics.get("StateHistoryDroppedDeltaCount"), Some(&1));
        assert_eq!(metrics.get("StateHistoryFailedDeltaCount"), Some(&2));
        assert_eq!(metrics.get("StateHistoryRecordedGapCount"), Some(&3));
        assert_eq!(
            metrics.get("StateHistoryTokenPersistenceFailureCount"),
            Some(&1)
        );
    }
}
