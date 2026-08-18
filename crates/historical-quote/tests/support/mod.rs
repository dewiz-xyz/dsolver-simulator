#![expect(
    dead_code,
    reason = "this fixture module is shared by separate integration test binaries"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};

use historical_quote::api::{
    Backend as PublicBackend, BlockRange, ConsistencyCheckRequest, ConsistencySelection,
    PoolSelector, QuoteComparisonRequest, QuoteJobRequest, StreamPosition as PublicPosition,
    UnsignedAmount,
};
use historical_quote::{HistoricalError, HistorySource};
use serde_json::value::RawValue;
use serde_json::{json, Value};
use state_history::{
    ArchiveMetadata, Backend, BlockInterval, BlockTimeObservation, CheckpointArchive,
    CheckpointKind, CheckpointManifest, CheckpointPair, CheckpointPairQuery,
    CheckpointPairReplayPlan, CheckpointStatus, CoverageQuery, CoverageSnapshot,
    DeltaBackendCursor, RangeGap, RangeGapKind, RangeLeg, RawTokenSnapshot, ReadLimits,
    StoredDelta, StreamPosition, TargetPlan, TargetPlanQuery, TokenSnapshotRef,
    ARCHIVE_SCHEMA_VERSION, DELTA_PAYLOAD_FORMAT_VERSION, TOKEN_SNAPSHOT_SCHEMA_VERSION,
};
use uuid::Uuid;

pub const NATIVE_COMPONENT: &str = "native:uniswap-v2:fixture";
pub const TOKEN_A: &str = "0x1111111111111111111111111111111111111111";
pub const TOKEN_B: &str = "0x2222222222222222222222222222222222222222";
pub const RFQ_COMPONENT: &str = "rfq:bebop:WETH-USDC-fixture";
pub const WETH: &str = "0x3333333333333333333333333333333333333333";
pub const USDC: &str = "0x4444444444444444444444444444444444444444";

const NATIVE_CHECKPOINT: &str =
    include_str!("../../../simulator-replay/tests/fixtures/native_checkpoint_v1.json");
const NATIVE_DELTA: &str =
    include_str!("../../../simulator-replay/tests/fixtures/native_delta_v1.json");
const RFQ_CHECKPOINT: &str =
    include_str!("../../../simulator-replay/tests/fixtures/rfq_checkpoint_v1.json");
const RFQ_DELTA: &str = include_str!("../../../simulator-replay/tests/fixtures/rfq_delta_v1.json");

#[derive(Debug)]
pub struct FixtureSource {
    pub target_plan: TargetPlan,
    pub coverage_snapshot: CoverageSnapshot,
    pub archives: BTreeMap<i64, CheckpointArchive>,
    pub token_snapshots: BTreeMap<i64, RawTokenSnapshot>,
    pub pairs: Vec<CheckpointPair>,
    pub pair_plan: Option<CheckpointPairReplayPlan>,
    pub checkpoint_fetches: AtomicUsize,
    pub token_fetches: AtomicUsize,
}

impl FixtureSource {
    pub fn native() -> Result<Self, Box<dyn std::error::Error>> {
        let anchor = manifest(1, 100, position(1), Backend::Native, true, None);
        let delta = native_delta(2, 101)?;
        let target_plan = TargetPlan {
            visible_through_block: 103,
            block_times: block_times(100, 104, 1_000),
            missing_block_times: BTreeSet::new(),
            legs: vec![RangeLeg {
                checkpoint: anchor.clone(),
                deltas: vec![delta.clone()],
            }],
            gaps: Vec::new(),
            lineage_invalid_intervals: Vec::new(),
            estimated_decoded_bytes: 1_024,
        };
        let mut archives = BTreeMap::new();
        archives.insert(anchor.id, native_archive(&anchor, false)?);
        let mut token_snapshots = BTreeMap::new();
        token_snapshots.insert(anchor.id, native_tokens()?);
        Ok(Self {
            target_plan,
            coverage_snapshot: CoverageSnapshot {
                visible_through_block: 103,
                continuous_intervals: vec![BlockInterval::new(100, 103)?],
                known_gaps: Vec::new(),
            },
            archives,
            token_snapshots,
            pairs: Vec::new(),
            pair_plan: None,
            checkpoint_fetches: AtomicUsize::new(0),
            token_fetches: AtomicUsize::new(0),
        })
    }

    pub fn native_consistency(direct_updated: bool) -> Result<Self, Box<dyn std::error::Error>> {
        let mut source = Self::native()?;
        let earlier = source.target_plan.legs[0].checkpoint.clone();
        let target = manifest(2, 101, position(3), Backend::Native, true, None);
        source
            .archives
            .insert(target.id, native_archive(&target, direct_updated)?);
        source.token_snapshots.insert(target.id, native_tokens()?);
        let pair = CheckpointPair { earlier, target };
        source.pairs = vec![pair.clone()];
        source.pair_plan = Some(CheckpointPairReplayPlan {
            pair,
            deltas: vec![native_delta(2, 101)?],
            gaps: Vec::new(),
            estimated_decoded_bytes: 2_048,
        });
        Ok(source)
    }

    pub fn native_with_component_added_at_target() -> Result<Self, Box<dyn std::error::Error>> {
        let mut source = Self::native()?;
        let anchor = source.target_plan.legs[0].checkpoint.clone();
        let checkpoint: Value = serde_json::from_str(NATIVE_CHECKPOINT)?;
        let mut empty_partition = checkpoint["partition"].clone();
        let state_entry = empty_partition["states"][0].clone();
        empty_partition["states"] = json!([]);
        source
            .archives
            .insert(anchor.id, archive(&anchor, "native", empty_partition));
        let update = json!({
            "partitions": [{
                "backend": "native",
                "blockNumber": 101,
                "newPairs": [state_entry]
            }]
        });
        source.target_plan.legs[0].deltas =
            vec![delta(2, Backend::Native, Some(101), None, update)?];
        Ok(source)
    }

    pub fn native_with_apply_anomaly() -> Result<Self, Box<dyn std::error::Error>> {
        let mut source = Self::native()?;
        let fixture: Value = serde_json::from_str(NATIVE_DELTA)?;
        let mut update = fixture["update"].clone();
        update["partitions"][0]["updatedStates"][0]["componentId"] = json!("native:missing");
        source.target_plan.legs[0].deltas =
            vec![delta(2, Backend::Native, Some(101), None, update)?];
        Ok(source)
    }

    pub fn rfq() -> Result<Self, Box<dyn std::error::Error>> {
        let anchor = manifest(10, 100, position(10), Backend::Rfq, false, Some(1_000));
        let delta = rfq_delta(11, 5_000)?;
        let mut block_times = BTreeMap::new();
        for (block, timestamp) in [(100, 4_000), (101, 5_000), (102, 5_001)] {
            block_times.insert(block, block_time(block, timestamp));
        }
        let target_plan = TargetPlan {
            visible_through_block: 101,
            block_times,
            missing_block_times: BTreeSet::new(),
            legs: vec![RangeLeg {
                checkpoint: anchor.clone(),
                deltas: vec![delta],
            }],
            gaps: Vec::new(),
            lineage_invalid_intervals: Vec::new(),
            estimated_decoded_bytes: 1_024,
        };
        let mut archives = BTreeMap::new();
        archives.insert(anchor.id, rfq_archive(&anchor)?);
        Ok(Self {
            target_plan,
            coverage_snapshot: CoverageSnapshot {
                visible_through_block: 101,
                continuous_intervals: vec![BlockInterval::new(100, 101)?],
                known_gaps: Vec::new(),
            },
            archives,
            token_snapshots: BTreeMap::new(),
            pairs: Vec::new(),
            pair_plan: None,
            checkpoint_fetches: AtomicUsize::new(0),
            token_fetches: AtomicUsize::new(0),
        })
    }

    pub fn rfq_consistency(with_tokens: bool) -> Result<Self, Box<dyn std::error::Error>> {
        let mut source = Self::rfq()?;
        let earlier = manifest(10, 100, position(1), Backend::Rfq, with_tokens, Some(1_000));
        let target = manifest(11, 101, position(3), Backend::Rfq, with_tokens, Some(5_000));
        source.archives.clear();
        source.archives.insert(earlier.id, rfq_archive(&earlier)?);
        source.archives.insert(target.id, rfq_archive(&target)?);
        source.token_snapshots.clear();
        if with_tokens {
            source.token_snapshots.insert(earlier.id, native_tokens()?);
            source.token_snapshots.insert(target.id, native_tokens()?);
        }
        let pair = CheckpointPair { earlier, target };
        source.pairs = vec![pair.clone()];
        source.pair_plan = Some(CheckpointPairReplayPlan {
            pair,
            deltas: vec![rfq_delta(2, 3_000)?],
            gaps: Vec::new(),
            estimated_decoded_bytes: 2_048,
        });
        Ok(source)
    }

    pub fn checkpoint_fetch_count(&self) -> usize {
        self.checkpoint_fetches.load(Ordering::SeqCst)
    }

    pub fn token_fetch_count(&self) -> usize {
        self.token_fetches.load(Ordering::SeqCst)
    }
}

impl HistorySource for FixtureSource {
    fn plan_targets(
        &self,
        _query: TargetPlanQuery,
    ) -> impl Future<Output = Result<TargetPlan, HistoricalError>> + Send {
        std::future::ready(Ok(self.target_plan.clone()))
    }

    fn coverage(
        &self,
        _query: CoverageQuery,
    ) -> impl Future<Output = Result<CoverageSnapshot, HistoricalError>> + Send {
        std::future::ready(Ok(self.coverage_snapshot.clone()))
    }

    fn fetch_checkpoint(
        &self,
        manifest: CheckpointManifest,
        _limits: ReadLimits,
    ) -> impl Future<Output = Result<CheckpointArchive, HistoricalError>> + Send {
        self.checkpoint_fetches.fetch_add(1, Ordering::SeqCst);
        std::future::ready(self.archives.get(&manifest.id).cloned().ok_or_else(|| {
            HistoricalError::HistoricalDataInvalid(format!(
                "fixture checkpoint {} is missing",
                manifest.id
            ))
        }))
    }

    fn fetch_checkpoint_token_snapshot(
        &self,
        manifest: CheckpointManifest,
        _limits: ReadLimits,
    ) -> impl Future<Output = Result<Option<RawTokenSnapshot>, HistoricalError>> + Send {
        self.token_fetches.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(self.token_snapshots.get(&manifest.id).cloned()))
    }

    fn resolve_checkpoint_pairs(
        &self,
        _query: CheckpointPairQuery,
    ) -> impl Future<Output = Result<Vec<CheckpointPair>, HistoricalError>> + Send {
        std::future::ready(Ok(self.pairs.clone()))
    }

    fn plan_checkpoint_pair(
        &self,
        _pair: CheckpointPair,
        _backends: Vec<Backend>,
        _limits: ReadLimits,
    ) -> impl Future<Output = Result<CheckpointPairReplayPlan, HistoricalError>> + Send {
        std::future::ready(self.pair_plan.clone().ok_or_else(|| {
            HistoricalError::HistoricalDataInvalid(
                "fixture checkpoint pair plan is missing".to_owned(),
            )
        }))
    }
}

pub fn native_quote_request(start: u64, end: u64) -> QuoteJobRequest {
    QuoteJobRequest {
        request_id: Uuid::new_v4(),
        api_revision: 1,
        timeout_ms: 10_000,
        chain_id: 8453,
        pool: native_pool(),
        blocks: BlockRange {
            start,
            end_inclusive: end,
            step: 1,
        },
        lags: vec![2, 1],
        amounts_in: vec![amount("2000000"), amount("1000000")],
    }
}

pub fn rfq_quote_request() -> QuoteJobRequest {
    QuoteJobRequest {
        request_id: Uuid::new_v4(),
        api_revision: 1,
        timeout_ms: 10_000,
        chain_id: 8453,
        pool: PoolSelector {
            backend: PublicBackend::Rfq,
            protocol: "rfq:bebop".to_owned(),
            component_id: RFQ_COMPONENT.to_owned(),
            token_in: WETH.to_owned(),
            token_out: USDC.to_owned(),
        },
        blocks: BlockRange {
            start: 100,
            end_inclusive: 100,
            step: 1,
        },
        lags: vec![1],
        amounts_in: vec![amount("1000000000000000000")],
    }
}

pub fn consistency_request(pool: PoolSelector) -> ConsistencyCheckRequest {
    ConsistencyCheckRequest {
        request_id: Uuid::new_v4(),
        api_revision: 1,
        timeout_ms: 10_000,
        chain_id: 8453,
        backends: vec![pool.backend],
        selection: ConsistencySelection::CheckpointPairs {
            pairs: vec![historical_quote::api::CheckpointPair {
                earlier_position: public_position(1),
                target_position: public_position(3),
            }],
        },
        quotes_to_compare: vec![QuoteComparisonRequest {
            pool,
            amounts_in: vec![amount("1000000")],
        }],
    }
}

pub fn native_pool() -> PoolSelector {
    PoolSelector {
        backend: PublicBackend::Native,
        protocol: "uniswap_v2".to_owned(),
        component_id: NATIVE_COMPONENT.to_owned(),
        token_in: TOKEN_A.to_owned(),
        token_out: TOKEN_B.to_owned(),
    }
}

pub fn amount(value: &str) -> UnsignedAmount {
    #[expect(
        clippy::expect_used,
        reason = "fixture amounts are compile-time constants"
    )]
    value.parse().expect("fixture amount must be valid")
}

pub fn recorded_gap(from: u64, to: u64, from_block: u64, to_block: u64) -> RangeGap {
    RangeGap {
        kind: RangeGapKind::Recorded,
        generation: Some(1),
        from_message_seq: Some(from),
        to_message_seq: Some(to),
        from_position: Some(position(from)),
        to_position: Some(position(to)),
        from_block: Some(from_block),
        to_block_inclusive: Some(to_block),
        from_observed_at_ms: None,
        to_observed_at_ms: None,
        reason: "write_failed".to_owned(),
    }
}

pub const fn position(message_seq: u64) -> StreamPosition {
    StreamPosition {
        generation: 1,
        message_seq,
    }
}

pub const fn public_position(message_seq: u64) -> PublicPosition {
    PublicPosition {
        generation: 1,
        message_seq,
    }
}

pub fn manifest(
    id: i64,
    block_number: u64,
    position: StreamPosition,
    backend: Backend,
    with_tokens: bool,
    rfq_observed_at_ms: Option<u64>,
) -> CheckpointManifest {
    #[expect(
        clippy::expect_used,
        reason = "fixture checkpoint ids are positive constants"
    )]
    let state_version = u64::try_from(id).expect("fixture id must be positive");
    CheckpointManifest {
        id,
        chain_id: 8453,
        position,
        state_version: Some(state_version),
        kind: CheckpointKind::Interval,
        block_number,
        rfq_observed_at_ms,
        backends: vec![backend],
        s3_key: Some(format!("fixture/{id}.json.zst")),
        archive_sha256: Some(format!("archive-{id}")),
        archive_bytes: Some(1_024),
        compressed_bytes: Some(512),
        token_reference: with_tokens.then(|| TokenSnapshotRef {
            s3_key: format!("fixture/{id}-tokens.json.zst"),
            sha256: format!("tokens-{id}"),
            token_count: 2,
            token_bytes: 512,
        }),
        status: CheckpointStatus::Complete,
        error: None,
    }
}

fn native_archive(
    manifest: &CheckpointManifest,
    updated: bool,
) -> Result<CheckpointArchive, Box<dyn std::error::Error>> {
    let checkpoint: Value = serde_json::from_str(NATIVE_CHECKPOINT)?;
    let mut partition = checkpoint["partition"].clone();
    partition["blockNumber"] = json!(manifest.block_number);
    if updated {
        let delta: Value = serde_json::from_str(NATIVE_DELTA)?;
        partition["states"][0]["state"] =
            delta["update"]["partitions"][0]["updatedStates"][0]["state"].clone();
    }
    Ok(archive(manifest, "native", partition))
}

fn rfq_archive(
    manifest: &CheckpointManifest,
) -> Result<CheckpointArchive, Box<dyn std::error::Error>> {
    let checkpoint: Value = serde_json::from_str(RFQ_CHECKPOINT)?;
    Ok(archive(manifest, "rfq", checkpoint["partition"].clone()))
}

pub fn archive(
    manifest: &CheckpointManifest,
    backend: &str,
    partition: Value,
) -> CheckpointArchive {
    let snapshot_id = format!("snapshot-{}", manifest.id);
    CheckpointArchive {
        metadata: ArchiveMetadata {
            schema_version: ARCHIVE_SCHEMA_VERSION,
            chain_id: manifest.chain_id,
            position: manifest.position,
            state_version: manifest.state_version,
            kind: manifest.kind,
            block_number: manifest.block_number,
            rfq_observed_at_ms: manifest.rfq_observed_at_ms,
            backends: manifest.backends.clone(),
        },
        payloads_json: vec![
            json!({
                "kind": "snapshot_start",
                "snapshotId": snapshot_id.clone(),
                "chainId": manifest.chain_id,
                "backends": [backend],
                "totalChunks": 1
            })
            .to_string(),
            json!({
                "kind": "snapshot_chunk",
                "snapshotId": snapshot_id.clone(),
                "chunkIndex": 0,
                "partitions": [partition]
            })
            .to_string(),
            json!({
                "kind": "snapshot_end",
                "snapshotId": snapshot_id
            })
            .to_string(),
        ],
    }
}

pub fn native_tokens() -> Result<RawTokenSnapshot, Box<dyn std::error::Error>> {
    let checkpoint: Value = serde_json::from_str(NATIVE_CHECKPOINT)?;
    raw_tokens(checkpoint["tokens"].clone())
}

fn raw_tokens(tokens: Value) -> Result<RawTokenSnapshot, Box<dyn std::error::Error>> {
    let token_count = tokens
        .as_array()
        .map(Vec::len)
        .ok_or("fixture tokens must be an array")?;
    Ok(RawTokenSnapshot {
        schema_version: TOKEN_SNAPSHOT_SCHEMA_VERSION,
        chain_id: 8453,
        token_count: u64::try_from(token_count)?,
        tokens: RawValue::from_string(tokens.to_string())?,
    })
}

fn native_delta(
    message_seq: u64,
    block_number: u64,
) -> Result<StoredDelta, Box<dyn std::error::Error>> {
    let fixture: Value = serde_json::from_str(NATIVE_DELTA)?;
    delta(
        message_seq,
        Backend::Native,
        Some(block_number),
        None,
        fixture["update"].clone(),
    )
}

pub fn rfq_delta(
    message_seq: u64,
    observed_at_ms: u64,
) -> Result<StoredDelta, Box<dyn std::error::Error>> {
    let fixture: Value = serde_json::from_str(RFQ_DELTA)?;
    delta(
        message_seq,
        Backend::Rfq,
        None,
        Some(observed_at_ms),
        fixture["update"].clone(),
    )
}

fn delta(
    message_seq: u64,
    backend: Backend,
    block_number: Option<u64>,
    observed_at_ms: Option<u64>,
    update: Value,
) -> Result<StoredDelta, Box<dyn std::error::Error>> {
    let raw_payload = RawValue::from_string(
        json!({
            "stream_id": "fixture-stream",
            "message_seq": message_seq,
            "kind": "update",
            "partitions": update["partitions"].clone()
        })
        .to_string(),
    )?;
    Ok(StoredDelta {
        position: position(message_seq),
        observed_at_ms: observed_at_ms.unwrap_or(1_000 + message_seq),
        payload_format_version: DELTA_PAYLOAD_FORMAT_VERSION,
        raw_payload,
        applicable_backends: vec![DeltaBackendCursor {
            backend,
            block_number,
            observed_at_ms,
        }],
    })
}

fn block_times(start: u64, end: u64, base_ms: u64) -> BTreeMap<u64, BlockTimeObservation> {
    (start..=end)
        .map(|block| (block, block_time(block, base_ms + (block - start) * 1_000)))
        .collect()
}

fn block_time(block: u64, timestamp_ms: u64) -> BlockTimeObservation {
    BlockTimeObservation {
        block_number: block,
        timestamp_ms,
        block_hash: Some(format!("0x{block:064x}")),
        parent_hash: Some(format!("0x{:064x}", block.saturating_sub(1))),
    }
}
