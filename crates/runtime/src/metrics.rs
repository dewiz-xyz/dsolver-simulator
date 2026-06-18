use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Map, Value};
use tracing::warn;

use crate::models::messages::{QuoteResultQuality, QuoteStatus};

const METRIC_NAMESPACE: &str = "Tycho/Simulation";
const METRIC_SIMULATE_COMPLETION: &str = "SimulateCompletion";
const METRIC_SIMULATE_RESULT_QUALITY: &str = "SimulateResultQuality";
const METRIC_SIMULATE_TIMEOUT: &str = "SimulateRequestTimeout";
const METRIC_BROADCASTER_REDIS_APPEND_FAILURE: &str = "BroadcasterRedisAppendFailure";
const METRIC_BROADCASTER_REDIS_POINTER_WRITE_SUCCESS: &str = "BroadcasterRedisPointerWriteSuccess";
const METRIC_BROADCASTER_REDIS_POINTER_WRITE_FAILURE: &str = "BroadcasterRedisPointerWriteFailure";
const METRIC_BROADCASTER_REDIS_GENERATION_RESET: &str = "BroadcasterRedisGenerationReset";
const METRIC_JEMALLOC_ALLOCATED_BYTES: &str = "JemallocAllocatedBytes";
const METRIC_JEMALLOC_RESIDENT_BYTES: &str = "JemallocResidentBytes";
const DIM_STATUS: &str = "Status";
const DIM_TIMED_OUT: &str = "TimedOut";
const DIM_TIMEOUT_KIND: &str = "TimeoutKind";
const DIM_RESULT_QUALITY: &str = "ResultQuality";
const DIM_LABEL: &str = "Label";

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

pub fn emit_broadcaster_redis_pointer_write_success() {
    emit_count_metric(METRIC_BROADCASTER_REDIS_POINTER_WRITE_SUCCESS, &[]);
}

pub fn emit_broadcaster_redis_pointer_write_failure() {
    emit_count_metric(METRIC_BROADCASTER_REDIS_POINTER_WRITE_FAILURE, &[]);
}

pub fn emit_broadcaster_redis_generation_reset() {
    emit_count_metric(METRIC_BROADCASTER_REDIS_GENERATION_RESET, &[]);
}

pub fn emit_jemalloc_snapshot(label: &str, allocated_bytes: usize, resident_bytes: usize) {
    // Emit CloudWatch Embedded Metric Format as a raw JSON log line.
    // Tracing's JSON wrapper would prevent EMF extraction, so we write directly to stdout.
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

    match serde_json::to_string(&Value::Object(event)) {
        Ok(line) => println!("{line}"),
        Err(err) => warn!(
            error = %err,
            metric = "jemalloc_snapshot",
            "Failed to serialize EMF metric"
        ),
    }
}

fn emit_count_metric(metric_name: &str, dimensions: &[(&str, Value)]) {
    // Emit CloudWatch Embedded Metric Format as a raw JSON log line.
    // Tracing's JSON wrapper would prevent EMF extraction, so we write directly to stdout.
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

    match serde_json::to_string(&Value::Object(event)) {
        Ok(line) => println!("{line}"),
        Err(err) => warn!(error = %err, metric = metric_name, "Failed to serialize EMF metric"),
    }
}
