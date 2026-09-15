use std::collections::BTreeMap;

use serde::Serialize;

use runtime::broadcaster::redis_publisher::{
    BroadcasterDeploymentAdmissionSnapshot, BroadcasterRedisPublisherStatus,
};
use runtime::broadcaster::state::{
    BroadcasterBackendStatus, BroadcasterRecoveryStatus, BroadcasterSnapshotSessionsSnapshot,
    BroadcasterSnapshotStatus, BroadcasterStatusSnapshot, BroadcasterUpstreamSnapshot,
};
use runtime::broadcaster::state_history::{StreamPosition, WriterStatus};
use simulator_core::broadcaster::{BroadcasterBackend, BroadcasterProtocolSyncStatus};

#[derive(Debug, Clone, Serialize)]
pub struct BroadcasterStatusPayload {
    pub status: &'static str,
    pub chain_id: u64,
    pub upstream: BroadcasterUpstreamPayload,
    pub recovery: BroadcasterRecoveryPayload,
    pub snapshot: BroadcasterSnapshotPayload,
    pub snapshot_sessions: BroadcasterSnapshotSessionsPayload,
    pub backends: BTreeMap<BroadcasterBackend, BroadcasterBackendPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redis_publisher: Option<BroadcasterRedisPublisherStatus>,
    pub deployment_admission: BroadcasterDeploymentAdmissionPayload,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_history: Option<BroadcasterStateHistoryStatus>,
}

impl From<BroadcasterStatusSnapshot> for BroadcasterStatusPayload {
    fn from(snapshot: BroadcasterStatusSnapshot) -> Self {
        Self {
            status: snapshot.readiness.as_str(),
            chain_id: snapshot.chain_id,
            upstream: snapshot.upstream.into(),
            recovery: snapshot.recovery.into(),
            snapshot: snapshot.snapshot.into(),
            snapshot_sessions: snapshot.snapshot_sessions.into(),
            backends: snapshot
                .backends
                .into_iter()
                .map(|(backend, status)| {
                    (
                        backend,
                        BroadcasterBackendPayload::from_backend_status(backend, status),
                    )
                })
                .collect(),
            redis_publisher: snapshot.redis_publisher,
            deployment_admission: snapshot.deployment_admission.into(),
            state_history: snapshot.state_history.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterStateHistoryStatus {
    pub healthy: bool,
    pub queue_len: u64,
    pub queue_capacity: u64,
    pub enqueued_deltas: u64,
    pub persisted_deltas: u64,
    pub dropped_deltas: u64,
    pub failed_deltas: u64,
    pub recorded_gaps: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_persisted: Option<BroadcasterStateHistoryPosition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_persisted_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub checkpoints_completed: u64,
    pub checkpoints_failed: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_checkpoint_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_successful_checkpoint_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_token_snapshot_sha: Option<String>,
    pub token_persistence_failures: u64,
}

impl From<WriterStatus> for BroadcasterStateHistoryStatus {
    fn from(status: WriterStatus) -> Self {
        Self {
            healthy: status.healthy,
            queue_len: status.queue_len,
            queue_capacity: status.queue_capacity,
            enqueued_deltas: status.enqueued_deltas,
            persisted_deltas: status.persisted_deltas,
            dropped_deltas: status.dropped_deltas,
            failed_deltas: status.failed_deltas,
            recorded_gaps: status.recorded_gaps,
            last_persisted: status.last_persisted.map(Into::into),
            last_persisted_at_ms: status.last_persisted_at_ms,
            last_error: status.last_error,
            checkpoints_completed: status.checkpoints_completed,
            checkpoints_failed: status.checkpoints_failed,
            last_checkpoint_status: status.last_checkpoint_status,
            last_successful_checkpoint_at_ms: status.last_successful_checkpoint_at_ms,
            last_token_snapshot_sha: status.last_token_snapshot_sha,
            token_persistence_failures: status.token_persistence_failures,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcasterStateHistoryPosition {
    pub generation: u64,
    pub message_seq: u64,
}

impl From<StreamPosition> for BroadcasterStateHistoryPosition {
    fn from(position: StreamPosition) -> Self {
        Self {
            generation: position.generation,
            message_seq: position.message_seq,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BroadcasterDeploymentAdmissionPayload {
    pub admitted: bool,
    pub phase: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl From<BroadcasterDeploymentAdmissionSnapshot> for BroadcasterDeploymentAdmissionPayload {
    fn from(snapshot: BroadcasterDeploymentAdmissionSnapshot) -> Self {
        Self {
            admitted: snapshot.admitted,
            phase: snapshot.phase.as_str(),
            last_error: snapshot.last_error,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BroadcasterRecoveryPayload {
    pub active: bool,
    pub phase: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_ms: Option<u64>,
    pub attempt: u8,
    pub buffer_a_count: usize,
    pub buffer_b_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_buffered_age_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_outcome: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_encoded_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_chunk_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl From<BroadcasterRecoveryStatus> for BroadcasterRecoveryPayload {
    fn from(status: BroadcasterRecoveryStatus) -> Self {
        Self {
            active: status.active,
            phase: status.phase,
            age_ms: status.age_ms,
            attempt: status.attempt,
            buffer_a_count: status.buffer_a_count,
            buffer_b_count: status.buffer_b_count,
            oldest_buffered_age_ms: status.oldest_buffered_age_ms,
            last_outcome: status.last_outcome,
            last_duration_ms: status.last_duration_ms,
            last_encoded_bytes: status.last_encoded_bytes,
            last_chunk_count: status.last_chunk_count,
            last_error: status.last_error,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BroadcasterUpstreamPayload {
    pub connected: bool,
    pub restart_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_disconnect_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_update_age_ms: Option<u64>,
}

impl From<BroadcasterUpstreamSnapshot> for BroadcasterUpstreamPayload {
    fn from(snapshot: BroadcasterUpstreamSnapshot) -> Self {
        Self {
            connected: snapshot.connected,
            restart_count: snapshot.restart_count,
            last_error: snapshot.last_error,
            last_disconnect_reason: snapshot.last_disconnect_reason,
            last_update_age_ms: snapshot.last_update_age_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BroadcasterSnapshotPayload {
    pub ready: bool,
    pub stream_id: String,
    pub snapshot_id: String,
    pub configured_backends: Vec<BroadcasterBackend>,
    pub total_states: usize,
    pub max_payload_bytes: usize,
    pub exportable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_export_check_age_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_export_success_age_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_export_duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_export_payload_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub largest_payload_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_limit_utilization_bps: Option<u16>,
    pub recovery_pending: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_export_error: Option<String>,
}

impl From<BroadcasterSnapshotStatus> for BroadcasterSnapshotPayload {
    fn from(snapshot: BroadcasterSnapshotStatus) -> Self {
        Self {
            ready: snapshot.ready,
            stream_id: snapshot.stream_id,
            snapshot_id: snapshot.snapshot_id,
            configured_backends: snapshot.configured_backends,
            total_states: snapshot.total_states,
            max_payload_bytes: snapshot.max_payload_bytes,
            exportable: snapshot.exportable,
            last_export_check_age_ms: snapshot.last_export_check_age_ms,
            last_export_success_age_ms: snapshot.last_export_success_age_ms,
            last_export_duration_ms: snapshot.last_export_duration_ms,
            last_export_payload_count: snapshot.last_export_payload_count,
            largest_payload_bytes: snapshot.largest_payload_bytes,
            payload_limit_utilization_bps: snapshot.payload_limit_utilization_bps,
            recovery_pending: snapshot.recovery_pending,
            recovery_id: snapshot.recovery_id,
            recovery_error: snapshot.recovery_error,
            last_export_error: snapshot.last_export_error,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BroadcasterSnapshotSessionsPayload {
    pub active: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl From<BroadcasterSnapshotSessionsSnapshot> for BroadcasterSnapshotSessionsPayload {
    fn from(snapshot: BroadcasterSnapshotSessionsSnapshot) -> Self {
        Self {
            active: snapshot.active,
            last_error: snapshot.last_error,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BroadcasterBackendPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_timestamp: Option<u64>,
    pub pool_count: usize,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub sync_statuses: BTreeMap<String, BroadcasterProtocolSyncStatus>,
}

impl BroadcasterBackendPayload {
    fn from_backend_status(backend: BroadcasterBackend, status: BroadcasterBackendStatus) -> Self {
        let (block_number, update_timestamp) = match backend {
            BroadcasterBackend::Native | BroadcasterBackend::Vm => (status.block_number, None),
            BroadcasterBackend::Rfq => (None, status.block_number),
        };

        Self {
            block_number,
            update_timestamp,
            pool_count: status.pool_count,
            sync_statuses: status.sync_statuses,
        }
    }
}

#[cfg(test)]
mod tests {
    use runtime::broadcaster::{
        redis_publisher::BroadcasterDeploymentPhase,
        state::{BroadcasterReadiness, BroadcasterSnapshotSessionsSnapshot},
    };

    use super::*;

    #[test]
    fn state_history_token_failure_is_visible_without_changing_readiness() -> anyhow::Result<()> {
        let payload = BroadcasterStatusPayload::from(BroadcasterStatusSnapshot {
            readiness: BroadcasterReadiness::Ready,
            chain_id: 8453,
            upstream: BroadcasterUpstreamSnapshot {
                connected: true,
                restart_count: 0,
                last_error: None,
                last_disconnect_reason: None,
                last_update_age_ms: Some(1),
            },
            recovery: BroadcasterRecoveryStatus::default(),
            snapshot: BroadcasterSnapshotStatus {
                ready: true,
                stream_id: "stream".to_owned(),
                snapshot_id: "snapshot".to_owned(),
                configured_backends: Vec::new(),
                total_states: 1,
                max_payload_bytes: 1,
                exportable: true,
                last_export_check_age_ms: Some(1),
                last_export_success_age_ms: Some(1),
                last_export_duration_ms: Some(1),
                last_export_payload_count: Some(1),
                largest_payload_bytes: Some(1),
                payload_limit_utilization_bps: Some(1),
                last_export_error: None,
                recovery_pending: false,
                recovery_id: None,
                recovery_error: None,
            },
            snapshot_sessions: BroadcasterSnapshotSessionsSnapshot::default(),
            backends: BTreeMap::new(),
            redis_publisher: None,
            deployment_admission: BroadcasterDeploymentAdmissionSnapshot {
                admitted: true,
                phase: BroadcasterDeploymentPhase::Admitted,
                last_error: None,
            },
            state_history: Some(WriterStatus {
                healthy: true,
                queue_len: 0,
                queue_capacity: 8,
                enqueued_deltas: 1,
                persisted_deltas: 1,
                dropped_deltas: 0,
                failed_deltas: 0,
                recorded_gaps: 0,
                last_persisted: Some(StreamPosition {
                    generation: 2,
                    message_seq: 3,
                }),
                last_persisted_at_ms: Some(1_000),
                last_error: None,
                checkpoints_completed: 2,
                checkpoints_failed: 0,
                last_checkpoint_status: Some("complete".to_owned()),
                last_successful_checkpoint_at_ms: Some(2_000),
                last_token_snapshot_sha: Some("token-sha".to_owned()),
                token_persistence_failures: 1,
            }),
        });

        let json = serde_json::to_value(payload)?;
        assert_eq!(json["status"], "ready");
        assert_eq!(json["state_history"]["lastTokenSnapshotSha"], "token-sha");
        assert_eq!(json["state_history"]["lastPersistedAtMs"], 1_000);
        assert_eq!(json["state_history"]["lastSuccessfulCheckpointAtMs"], 2_000);
        assert_eq!(json["state_history"]["tokenPersistenceFailures"], 1);
        assert_eq!(json["state_history"]["healthy"], true);
        Ok(())
    }
}
