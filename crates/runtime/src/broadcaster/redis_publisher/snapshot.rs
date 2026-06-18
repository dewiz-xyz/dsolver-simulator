use anyhow::{anyhow, Result};

use crate::broadcaster::state::{BroadcasterSnapshotCache, BroadcasterSnapshotExport};
use simulator_core::broadcaster::{
    BroadcasterBackend, BroadcasterPayload, BroadcasterSnapshotChunk,
};

#[derive(Debug, Clone)]
pub struct BroadcasterRedisSnapshotSource {
    pub(super) cache: BroadcasterSnapshotCache,
    pub(super) backends: Vec<BroadcasterBackend>,
}

impl BroadcasterRedisSnapshotSource {
    pub fn new(cache: BroadcasterSnapshotCache, mut backends: Vec<BroadcasterBackend>) -> Self {
        backends.sort();
        backends.dedup();
        Self { cache, backends }
    }
}

pub(super) fn append_snapshot_chunks(
    payloads: &mut Vec<BroadcasterPayload>,
    export: BroadcasterSnapshotExport,
    snapshot_id: &str,
    next_chunk_index: &mut u32,
) -> Result<()> {
    for payload in export.payloads {
        let BroadcasterPayload::SnapshotChunk(chunk) = payload else {
            continue;
        };
        payloads.push(BroadcasterPayload::SnapshotChunk(
            BroadcasterSnapshotChunk::new(
                snapshot_id.to_string(),
                *next_chunk_index,
                chunk.partitions,
            )?,
        ));
        *next_chunk_index = next_chunk_index.saturating_add(1);
    }
    Ok(())
}

pub(super) fn payload_backend_scope(
    payload: &BroadcasterPayload,
    snapshot_end_backends: impl FnOnce() -> Vec<BroadcasterBackend>,
) -> Result<Vec<BroadcasterBackend>> {
    let mut backends = match payload {
        BroadcasterPayload::SnapshotStart(start) => start.backends.clone(),
        BroadcasterPayload::SnapshotChunk(chunk) => chunk
            .partitions
            .iter()
            .map(|partition| partition.backend)
            .collect(),
        BroadcasterPayload::SnapshotEnd(_) => snapshot_end_backends(),
        BroadcasterPayload::Update(update) => update
            .partitions
            .iter()
            .map(|partition| partition.backend)
            .collect(),
        BroadcasterPayload::Heartbeat(heartbeat) => heartbeat
            .backend_heads
            .iter()
            .map(|head| head.backend)
            .collect(),
    };
    backends.sort();
    backends.dedup();
    if backends.is_empty() {
        return Err(anyhow!("Redis broadcaster payload has empty backend scope"));
    }
    Ok(backends)
}
