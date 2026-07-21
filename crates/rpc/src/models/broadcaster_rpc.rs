use std::collections::BTreeMap;

use serde::Serialize;

use runtime::broadcaster::redis_publisher::BroadcasterRedisPublisherStatus;
use runtime::broadcaster::state::{
    BroadcasterBackendStatus, BroadcasterSnapshotSessionsSnapshot, BroadcasterSnapshotStatus,
    BroadcasterStateHistoryStatus, BroadcasterStatusSnapshot, BroadcasterUpstreamSnapshot,
};
use simulator_core::broadcaster::{BroadcasterBackend, BroadcasterProtocolSyncStatus};

#[derive(Debug, Clone, Serialize)]
pub struct BroadcasterStatusPayload {
    pub status: &'static str,
    pub chain_id: u64,
    pub upstream: BroadcasterUpstreamPayload,
    pub snapshot: BroadcasterSnapshotPayload,
    pub snapshot_sessions: BroadcasterSnapshotSessionsPayload,
    pub backends: BTreeMap<BroadcasterBackend, BroadcasterBackendPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redis_publisher: Option<BroadcasterRedisPublisherStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_history: Option<BroadcasterStateHistoryPayload>,
}

impl From<BroadcasterStatusSnapshot> for BroadcasterStatusPayload {
    fn from(snapshot: BroadcasterStatusSnapshot) -> Self {
        Self {
            status: snapshot.readiness.as_str(),
            chain_id: snapshot.chain_id,
            upstream: snapshot.upstream.into(),
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
            state_history: snapshot.state_history.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BroadcasterStateHistoryPayload {
    pub healthy: bool,
    pub queue_capacity: usize,
    pub retry_window_ms: u64,
    pub enqueued_deltas: u64,
    pub persisted_deltas: u64,
    pub recorded_gaps: u64,
    pub dropped_deltas: u64,
    pub failed_deltas: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_persisted_stream_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_persisted_redis_entry_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_persisted_message_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoints: Option<BroadcasterStateHistoryCheckpointPayload>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BroadcasterStateHistoryCheckpointPayload {
    pub healthy: bool,
    pub attempted_checkpoints: u64,
    pub completed_checkpoints: u64,
    pub failed_checkpoints: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_checkpoint_block_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_checkpoint_s3_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl From<BroadcasterStateHistoryStatus> for BroadcasterStateHistoryPayload {
    fn from(status: BroadcasterStateHistoryStatus) -> Self {
        Self {
            healthy: status.healthy,
            queue_capacity: status.queue_capacity,
            retry_window_ms: status.retry_window_ms,
            enqueued_deltas: status.enqueued_deltas,
            persisted_deltas: status.persisted_deltas,
            recorded_gaps: status.recorded_gaps,
            dropped_deltas: status.dropped_deltas,
            failed_deltas: status.failed_deltas,
            last_persisted_stream_id: status.last_persisted_stream_id,
            last_persisted_redis_entry_id: status.last_persisted_redis_entry_id,
            last_persisted_message_seq: status.last_persisted_message_seq,
            last_error: status.last_error,
            checkpoints: status.checkpoints.map(|checkpoints| {
                BroadcasterStateHistoryCheckpointPayload {
                    healthy: checkpoints.healthy,
                    attempted_checkpoints: checkpoints.attempted_checkpoints,
                    completed_checkpoints: checkpoints.completed_checkpoints,
                    failed_checkpoints: checkpoints.failed_checkpoints,
                    last_checkpoint_block_number: checkpoints.last_checkpoint_block_number,
                    last_checkpoint_s3_key: checkpoints.last_checkpoint_s3_key,
                    last_error: checkpoints.last_error,
                }
            }),
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
