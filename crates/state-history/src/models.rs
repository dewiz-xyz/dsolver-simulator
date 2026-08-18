use std::str::FromStr;

use serde::{Deserialize, Serialize};

pub const DELTA_PAYLOAD_FORMAT_VERSION: i16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StreamPosition {
    pub generation: u64,
    pub message_seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Backend {
    Native,
    Vm,
    Rfq,
}

impl Backend {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Vm => "vm",
            Self::Rfq => "rfq",
        }
    }
}

impl FromStr for Backend {
    type Err = ModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "native" => Ok(Self::Native),
            "vm" => Ok(Self::Vm),
            "rfq" => Ok(Self::Rfq),
            _ => Err(ModelError::InvalidBackend(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaBackendCursor {
    pub backend: Backend,
    pub block_number: Option<u64>,
    pub observed_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockTimeObservation {
    pub block_number: u64,
    pub timestamp_ms: u64,
    pub block_hash: Option<String>,
    pub parent_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedDeltaEntry")]
pub struct DeltaEntry {
    chain_id: u64,
    position: StreamPosition,
    observed_at_ms: u64,
    payload_json: String,
    backends: Vec<DeltaBackendCursor>,
    block_times: Vec<BlockTimeObservation>,
}

impl DeltaEntry {
    pub fn new(
        chain_id: u64,
        position: StreamPosition,
        observed_at_ms: u64,
        payload_json: String,
        backends: Vec<DeltaBackendCursor>,
        block_times: Vec<BlockTimeObservation>,
    ) -> Result<Self, ModelError> {
        if payload_json.is_empty() {
            return Err(ModelError::EmptyDeltaPayload);
        }
        if backends.is_empty() {
            return Err(ModelError::EmptyBackends);
        }

        Ok(Self {
            chain_id,
            position,
            observed_at_ms,
            payload_json,
            backends,
            block_times,
        })
    }

    pub const fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub const fn position(&self) -> StreamPosition {
        self.position
    }

    pub const fn observed_at_ms(&self) -> u64 {
        self.observed_at_ms
    }

    pub fn payload_json(&self) -> &str {
        &self.payload_json
    }

    pub fn backends(&self) -> &[DeltaBackendCursor] {
        &self.backends
    }

    pub fn block_times(&self) -> &[BlockTimeObservation] {
        &self.block_times
    }
}

#[derive(Deserialize)]
struct UncheckedDeltaEntry {
    chain_id: u64,
    position: StreamPosition,
    observed_at_ms: u64,
    payload_json: String,
    backends: Vec<DeltaBackendCursor>,
    block_times: Vec<BlockTimeObservation>,
}

impl TryFrom<UncheckedDeltaEntry> for DeltaEntry {
    type Error = ModelError;

    fn try_from(entry: UncheckedDeltaEntry) -> Result<Self, Self::Error> {
        Self::new(
            entry.chain_id,
            entry.position,
            entry.observed_at_ms,
            entry.payload_json,
            entry.backends,
            entry.block_times,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GapReason {
    QueueOverflow,
    WriteFailed,
    WriterStopped,
    CheckpointFailed,
    BoundarySlip,
}

impl GapReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QueueOverflow => "queue_overflow",
            Self::WriteFailed => "write_failed",
            Self::WriterStopped => "writer_stopped",
            Self::CheckpointFailed => "checkpoint_failed",
            Self::BoundarySlip => "boundary_slip",
        }
    }
}

impl FromStr for GapReason {
    type Err = ModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queue_overflow" => Ok(Self::QueueOverflow),
            "write_failed" => Ok(Self::WriteFailed),
            "writer_stopped" => Ok(Self::WriterStopped),
            "checkpoint_failed" => Ok(Self::CheckpointFailed),
            "boundary_slip" => Ok(Self::BoundarySlip),
            _ => Err(ModelError::InvalidGapReason(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GapRecord {
    pub chain_id: u64,
    pub generation: u64,
    pub from_message_seq: u64,
    pub to_message_seq: u64,
    pub reason: GapReason,
    pub from_block_number: Option<u64>,
    pub to_block_number: Option<u64>,
    pub from_observed_at_ms: Option<u64>,
    pub to_observed_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointKind {
    Boundary,
    Interval,
}

impl CheckpointKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Boundary => "boundary",
            Self::Interval => "interval",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointStatus {
    Writing,
    Complete,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedCapturedState")]
pub struct CapturedState {
    chain_id: u64,
    position: StreamPosition,
    state_version: Option<u64>,
    kind: CheckpointKind,
    block_number: u64,
    rfq_observed_at_ms: Option<u64>,
    backends: Vec<Backend>,
    payloads_json: Vec<String>,
    block_times: Vec<BlockTimeObservation>,
}

impl CapturedState {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        chain_id: u64,
        position: StreamPosition,
        state_version: Option<u64>,
        kind: CheckpointKind,
        block_number: u64,
        rfq_observed_at_ms: Option<u64>,
        mut backends: Vec<Backend>,
        payloads_json: Vec<String>,
        block_times: Vec<BlockTimeObservation>,
    ) -> Result<Self, ModelError> {
        if backends.is_empty() {
            return Err(ModelError::EmptyBackends);
        }
        if payloads_json.is_empty() {
            return Err(ModelError::EmptyCheckpointPayloads);
        }

        backends.sort_unstable();
        backends.dedup();

        if backends.binary_search(&Backend::Rfq).is_ok() && rfq_observed_at_ms.is_none() {
            return Err(ModelError::MissingRfqObservedAt);
        }

        Ok(Self {
            chain_id,
            position,
            state_version,
            kind,
            block_number,
            rfq_observed_at_ms,
            backends,
            payloads_json,
            block_times,
        })
    }

    pub const fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub const fn position(&self) -> StreamPosition {
        self.position
    }

    pub const fn state_version(&self) -> Option<u64> {
        self.state_version
    }

    pub const fn kind(&self) -> CheckpointKind {
        self.kind
    }

    pub const fn block_number(&self) -> u64 {
        self.block_number
    }

    pub const fn rfq_observed_at_ms(&self) -> Option<u64> {
        self.rfq_observed_at_ms
    }

    pub fn backends(&self) -> &[Backend] {
        &self.backends
    }

    pub fn payloads_json(&self) -> &[String] {
        &self.payloads_json
    }

    pub fn block_times(&self) -> &[BlockTimeObservation] {
        &self.block_times
    }
}

#[derive(Deserialize)]
struct UncheckedCapturedState {
    chain_id: u64,
    position: StreamPosition,
    state_version: Option<u64>,
    kind: CheckpointKind,
    block_number: u64,
    rfq_observed_at_ms: Option<u64>,
    backends: Vec<Backend>,
    payloads_json: Vec<String>,
    block_times: Vec<BlockTimeObservation>,
}

impl TryFrom<UncheckedCapturedState> for CapturedState {
    type Error = ModelError;

    fn try_from(state: UncheckedCapturedState) -> Result<Self, Self::Error> {
        Self::new(
            state.chain_id,
            state.position,
            state.state_version,
            state.kind,
            state.block_number,
            state.rfq_observed_at_ms,
            state.backends,
            state.payloads_json,
            state.block_times,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenSnapshotRef {
    pub s3_key: String,
    pub sha256: String,
    pub token_count: u64,
    pub token_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointManifest {
    pub id: i64,
    pub chain_id: u64,
    pub position: StreamPosition,
    pub state_version: Option<u64>,
    pub kind: CheckpointKind,
    pub block_number: u64,
    pub rfq_observed_at_ms: Option<u64>,
    pub backends: Vec<Backend>,
    pub s3_key: Option<String>,
    pub archive_sha256: Option<String>,
    pub archive_bytes: Option<u64>,
    pub compressed_bytes: Option<u64>,
    pub token_reference: Option<TokenSnapshotRef>,
    pub status: CheckpointStatus,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ModelError {
    #[error("unsupported backend `{0}`")]
    InvalidBackend(String),
    #[error("unsupported gap reason `{0}`")]
    InvalidGapReason(String),
    #[error("backends must not be empty")]
    EmptyBackends,
    #[error("target blocks must not be empty")]
    EmptyTargetBlocks,
    #[error("RFQ target block {0} has no representable next block")]
    RfqTargetHasNoSuccessor(u64),
    #[error("range start block {start} must not exceed end block {end}")]
    InvalidBlockRange { start: u64, end: u64 },
    #[error("RFQ range start timestamp {start_ms} must not exceed end timestamp {end_ms}")]
    InvalidRfqRange { start_ms: u64, end_ms: u64 },
    #[error("RFQ ranges require timestamp bounds")]
    MissingRfqBounds,
    #[error("delta payload must not be empty")]
    EmptyDeltaPayload,
    #[error("checkpoint payloads must not be empty")]
    EmptyCheckpointPayloads,
    #[error("RFQ checkpoints require an observation timestamp")]
    MissingRfqObservedAt,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_position_orders_by_generation_then_seq() {
        let a = StreamPosition {
            generation: 1,
            message_seq: 900,
        };
        let b = StreamPosition {
            generation: 2,
            message_seq: 1,
        };
        assert!(a < b);
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn backend_round_trips_db_values() {
        for (backend, s) in [
            (Backend::Native, "native"),
            (Backend::Vm, "vm"),
            (Backend::Rfq, "rfq"),
        ] {
            assert_eq!(backend.as_str(), s);
            assert_eq!(s.parse::<Backend>().unwrap(), backend);
        }
        assert!("stream_header".parse::<Backend>().is_err());
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn gap_reason_round_trips_db_values() {
        for (reason, value) in [
            (GapReason::QueueOverflow, "queue_overflow"),
            (GapReason::WriteFailed, "write_failed"),
            (GapReason::WriterStopped, "writer_stopped"),
            (GapReason::CheckpointFailed, "checkpoint_failed"),
            (GapReason::BoundarySlip, "boundary_slip"),
        ] {
            assert_eq!(reason.as_str(), value);
            assert_eq!(value.parse::<GapReason>().unwrap(), reason);
        }
        assert!("unknown".parse::<GapReason>().is_err());
    }

    #[test]
    fn captured_state_requires_rfq_cursor_when_rfq_in_scope() {
        let err = CapturedState::new(
            8453,
            StreamPosition {
                generation: 1,
                message_seq: 10,
            },
            None,
            CheckpointKind::Interval,
            100,
            None,
            vec![Backend::Native, Backend::Rfq],
            vec!["{}".into()],
            vec![],
        );
        assert!(err.is_err());
    }

    #[test]
    fn delta_entry_rejects_backendless_updates() {
        let err = DeltaEntry::new(
            8453,
            StreamPosition {
                generation: 1,
                message_seq: 2,
            },
            1_000,
            "{}".into(),
            vec![],
            vec![],
        );
        assert!(err.is_err());
    }
}
