use std::collections::BTreeMap;

use serde::Serialize;

use runtime::broadcaster::state::{
    BroadcasterBackendStatus, BroadcasterSnapshotStatus, BroadcasterStatusSnapshot,
    BroadcasterSubscriberSnapshot, BroadcasterUpstreamSnapshot,
};
use simulator_core::broadcaster::{BroadcasterBackend, BroadcasterProtocolSyncStatus};

#[derive(Debug, Clone, Serialize)]
pub struct BroadcasterStatusPayload {
    pub status: &'static str,
    pub chain_id: u64,
    pub upstream: BroadcasterUpstreamPayload,
    pub snapshot: BroadcasterSnapshotPayload,
    pub subscribers: BroadcasterSubscribersPayload,
    pub backends: BTreeMap<BroadcasterBackend, BroadcasterBackendPayload>,
}

impl From<BroadcasterStatusSnapshot> for BroadcasterStatusPayload {
    fn from(snapshot: BroadcasterStatusSnapshot) -> Self {
        Self {
            status: snapshot.readiness.as_str(),
            chain_id: snapshot.chain_id,
            upstream: snapshot.upstream.into(),
            snapshot: snapshot.snapshot.into(),
            subscribers: snapshot.subscribers.into(),
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
pub struct BroadcasterSubscribersPayload {
    pub active: usize,
    pub lag_disconnects: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl From<BroadcasterSubscriberSnapshot> for BroadcasterSubscribersPayload {
    fn from(snapshot: BroadcasterSubscriberSnapshot) -> Self {
        Self {
            active: snapshot.active,
            lag_disconnects: snapshot.lag_disconnects,
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
