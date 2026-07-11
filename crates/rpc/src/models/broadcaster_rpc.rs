use std::collections::BTreeMap;

use serde::Serialize;

use runtime::broadcaster::redis_publisher::BroadcasterRedisPublisherStatus;
use runtime::broadcaster::state::{
    BroadcasterBackendStatus, BroadcasterSnapshotSessionsSnapshot, BroadcasterSnapshotStatus,
    BroadcasterStatusSnapshot, BroadcasterUpstreamSnapshot,
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
