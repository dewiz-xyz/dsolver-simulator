use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    ops::{Deref, DerefMut},
};

use anyhow::{ensure, Context};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use simulator_core::broadcaster::BroadcasterTokenDto;
use sqlx::{
    pool::PoolConnection, postgres::PgRow, Connection, PgConnection, PgPool, Postgres, Row,
    Transaction,
};

use crate::{
    archive::decode_archive_with_info, tokens::decode_token_snapshot_raw_with_info, Backend,
    CheckpointArchive, CheckpointKind, CheckpointManifest, CheckpointObjectStore, CheckpointStatus,
    DeltaBackendCursor, ModelError, RawTokenSnapshot, StateHistoryStore, StreamPosition,
    TokenSnapshotRef,
};

mod checkpoints;
mod coverage;
mod limits;

pub use checkpoints::*;
pub use coverage::*;
pub use limits::*;

pub trait ReadConnectionProvider: Send + Sync + 'static {
    type Connection<'a>: Deref<Target = PgConnection> + DerefMut + Send + 'a
    where
        Self: 'a;

    fn acquire(&self) -> impl Future<Output = Result<Self::Connection<'_>, sqlx::Error>> + Send;
}

impl ReadConnectionProvider for PgPool {
    type Connection<'a> = PoolConnection<Postgres>;

    fn acquire(&self) -> impl Future<Output = Result<Self::Connection<'_>, sqlx::Error>> + Send {
        PgPool::acquire(self)
    }
}

impl ReadConnectionProvider for StateHistoryStore {
    type Connection<'a> = PoolConnection<Postgres>;

    fn acquire(&self) -> impl Future<Output = Result<Self::Connection<'_>, sqlx::Error>> + Send {
        self.pool().acquire()
    }
}

pub struct StateHistoryReader<P: ReadConnectionProvider = PgPool> {
    pub(super) connections: P,
    pub(super) objects: CheckpointObjectStore,
}

impl<P: ReadConnectionProvider> StateHistoryReader<P> {
    pub fn new(connections: P, objects: CheckpointObjectStore) -> Self {
        Self {
            connections,
            objects,
        }
    }

    /// Returns the latest block that is safe for every requested backend.
    ///
    /// This is the bounded cutoff query used by service status. It does not
    /// calculate retained intervals or load state-history objects.
    ///
    /// # Errors
    ///
    /// Returns an error when the replica cannot provide a cutoff for every
    /// requested backend.
    pub async fn visible_through_block(
        &self,
        chain_id: u64,
        backends: &[Backend],
    ) -> anyhow::Result<u64> {
        let mut connection = self
            .connections
            .acquire()
            .await
            .context("failed to acquire state history read connection")?;
        let mut transaction = begin_range_transaction(&mut connection).await?;
        let cutoff = coverage::visible_through_block(&mut transaction, chain_id, backends).await?;
        transaction
            .commit()
            .await
            .context("failed to commit state history cutoff transaction")?;
        Ok(cutoff)
    }

    pub async fn resolve_range(&self, request: &RangeRequest) -> anyhow::Result<RangePlan> {
        let rfq_bounds = request.rfq_bounds()?;
        let mut connection = self
            .connections
            .acquire()
            .await
            .context("failed to acquire state history read connection")?;
        let mut transaction = begin_range_transaction(&mut connection).await?;
        let plan = resolve_range_in_transaction(
            &mut transaction,
            request,
            rfq_bounds,
            ReadLimits::unbounded(),
        )
        .await?
        .plan;
        transaction
            .commit()
            .await
            .context("failed to commit state history range transaction")?;
        Ok(plan)
    }

    /// Resolves block timestamps and the replay plan for a historical backtest.
    ///
    /// Native and VM timestamp fields are informational. Missing rows resolve to
    /// zero unless RFQ data makes complete block metadata mandatory.
    ///
    /// # Errors
    ///
    /// Returns an error when required RFQ block metadata is missing or
    /// inconsistent, or when range resolution fails.
    pub async fn resolve_backtest_range(
        &self,
        request: &BacktestRequest,
    ) -> anyhow::Result<BacktestPlan> {
        let mut connection = self
            .connections
            .acquire()
            .await
            .context("failed to acquire state history read connection")?;
        let mut transaction = begin_range_transaction(&mut connection).await?;
        if !request.includes_rfq() {
            let range = resolve_range_in_transaction(
                &mut transaction,
                &request.range_request(None),
                None,
                ReadLimits::unbounded(),
            )
            .await?
            .plan;
            let (start_block_timestamp_ms, end_block_timestamp_ms) =
                fetch_informational_timestamps(
                    &mut transaction,
                    request.chain_id,
                    request.start_block,
                    request.end_block,
                )
                .await?;
            transaction
                .commit()
                .await
                .context("failed to commit state history backtest transaction")?;
            return Ok(BacktestPlan {
                start_block_timestamp_ms,
                end_block_timestamp_ms,
                rfq_bounds: None,
                range,
            });
        }

        let end_plus_one = request.end_block + 1;
        let start = require_block_time(
            fetch_block_time(&mut transaction, request.chain_id, request.start_block).await?,
            RequiredBlockTime::Start {
                block_number: request.start_block,
            },
        )?;
        let end = require_block_time(
            fetch_block_time(&mut transaction, request.chain_id, request.end_block).await?,
            RequiredBlockTime::End {
                block_number: request.end_block,
            },
        )?;
        let next = require_block_time(
            fetch_block_time(&mut transaction, request.chain_id, end_plus_one).await?,
            RequiredBlockTime::EndPlusOne {
                block_number: end_plus_one,
            },
        )?;
        let span = fetch_block_time_span(
            &mut transaction,
            request.chain_id,
            request.start_block,
            end_plus_one,
        )
        .await?;
        validate_block_time_span(&span, request.start_block, end_plus_one)?;

        let rfq_bounds = (start.timestamp_ms, next.timestamp_ms - 1);
        let range_request = request.range_request(Some(rfq_bounds));
        let range = resolve_range_in_transaction(
            &mut transaction,
            &range_request,
            Some(rfq_bounds),
            ReadLimits::unbounded(),
        )
        .await?
        .plan;
        transaction
            .commit()
            .await
            .context("failed to commit state history range transaction")?;

        Ok(BacktestPlan {
            start_block_timestamp_ms: start.timestamp_ms,
            end_block_timestamp_ms: end.timestamp_ms,
            rfq_bounds: Some(rfq_bounds),
            range,
        })
    }

    pub async fn fetch_checkpoint(
        &self,
        manifest: &CheckpointManifest,
    ) -> anyhow::Result<CheckpointArchive> {
        ensure!(
            manifest.status == CheckpointStatus::Complete,
            "checkpoint manifest {} is not complete",
            manifest.id
        );
        let key = manifest
            .s3_key
            .as_deref()
            .context("complete checkpoint manifest is missing its S3 key")?;
        let expected_sha256 = manifest
            .archive_sha256
            .as_deref()
            .context("complete checkpoint manifest is missing its archive sha256")?;
        let expected_archive_bytes = manifest
            .archive_bytes
            .context("complete checkpoint manifest is missing its archive byte length")?;
        let expected_compressed_bytes = manifest
            .compressed_bytes
            .context("complete checkpoint manifest is missing its compressed byte length")?;
        let expected_key =
            self.objects
                .key_for(manifest.chain_id, manifest.position, manifest.kind);
        ensure!(
            key == expected_key,
            "checkpoint manifest {} object key mismatch: expected {expected_key}, got {key}",
            manifest.id
        );
        let bytes = self.objects.fetch(key).await?;
        let decoded = decode_archive_with_info(&bytes, expected_sha256)?;
        ensure!(
            decoded.archive_bytes == expected_archive_bytes,
            "checkpoint manifest {} archive length mismatch: expected {expected_archive_bytes}, got {}",
            manifest.id,
            decoded.archive_bytes
        );
        ensure!(
            decoded.compressed_bytes == expected_compressed_bytes,
            "checkpoint manifest {} compressed length mismatch: expected {expected_compressed_bytes}, got {}",
            manifest.id,
            decoded.compressed_bytes
        );
        let metadata = &decoded.archive.metadata;
        ensure!(
            metadata.chain_id == manifest.chain_id
                && metadata.position == manifest.position
                && metadata.state_version == manifest.state_version
                && metadata.kind == manifest.kind
                && metadata.block_number == manifest.block_number
                && metadata.rfq_observed_at_ms == manifest.rfq_observed_at_ms
                && metadata.backends == manifest.backends,
            "checkpoint manifest {} metadata does not match its archive",
            manifest.id
        );
        ensure!(
            !decoded.archive.payloads_json.is_empty(),
            "checkpoint manifest {} archive contains no payloads",
            manifest.id
        );
        Ok(decoded.archive)
    }

    /// Fetches and verifies the token snapshot referenced by a checkpoint.
    ///
    /// The token snapshot is content-addressed, so callers can cache one fetch
    /// by sha256 across many replay legs.
    ///
    /// # Errors
    ///
    /// Returns an error when the referenced object cannot be fetched, or when
    /// decompression, sha256 verification, envelope parsing, or schema
    /// validation fails.
    pub async fn fetch_checkpoint_token_snapshot(
        &self,
        manifest: &CheckpointManifest,
    ) -> anyhow::Result<Option<RawTokenSnapshot>> {
        ensure!(
            manifest.status == CheckpointStatus::Complete,
            "checkpoint manifest {} is not complete",
            manifest.id
        );
        let Some(reference) = manifest.token_reference.as_ref() else {
            return Ok(None);
        };
        let expected_key = self
            .objects
            .token_key_for(manifest.chain_id, &reference.sha256);
        ensure!(
            reference.s3_key == expected_key,
            "checkpoint manifest {} token object key mismatch: expected {expected_key}, got {}",
            manifest.id,
            reference.s3_key
        );
        let snapshot = self.fetch_token_snapshot(reference).await?;
        ensure!(
            snapshot.chain_id == manifest.chain_id,
            "checkpoint manifest {} token chain mismatch: expected {}, got {}",
            manifest.id,
            manifest.chain_id,
            snapshot.chain_id
        );
        Ok(Some(snapshot))
    }

    pub async fn fetch_token_snapshot(
        &self,
        reference: &TokenSnapshotRef,
    ) -> anyhow::Result<RawTokenSnapshot> {
        // The object store's own context says "checkpoint object"; name the token
        // snapshot here so a missing or corrupt sidecar is not triaged as an archive.
        let bytes = self
            .objects
            .fetch(&reference.s3_key)
            .await
            .with_context(|| format!("failed to fetch token snapshot {}", reference.s3_key))?;
        let decoded = decode_token_snapshot_raw_with_info(&bytes, &reference.sha256)?;
        let expected_key = self
            .objects
            .token_key_for(decoded.snapshot.chain_id, &reference.sha256);
        ensure!(
            reference.s3_key == expected_key,
            "token snapshot object key mismatch: expected {expected_key}, got {}",
            reference.s3_key
        );
        ensure!(
            decoded.token_bytes == reference.token_bytes,
            "token snapshot byte length mismatch: expected {}, got {}",
            reference.token_bytes,
            decoded.token_bytes
        );
        ensure!(
            decoded.snapshot.token_count == reference.token_count,
            "token snapshot count mismatch: expected {}, got {}",
            reference.token_count,
            decoded.snapshot.token_count
        );
        let tokens: Vec<BroadcasterTokenDto> = serde_json::from_str(decoded.snapshot.tokens.get())
            .context("failed to parse token snapshot token array")?;
        let actual_token_count =
            u64::try_from(tokens.len()).context("token snapshot token count exceeds u64")?;
        ensure!(
            actual_token_count == decoded.snapshot.token_count,
            "token snapshot envelope count mismatch: expected {}, got {actual_token_count}",
            decoded.snapshot.token_count
        );
        ensure!(
            tokens
                .iter()
                .all(|token| token.chain_id == decoded.snapshot.chain_id),
            "token snapshot contains a token from a different chain"
        );
        ensure!(
            tokens
                .windows(2)
                .all(|pair| pair[0].address < pair[1].address),
            "token snapshot addresses are not strictly sorted and unique"
        );
        Ok(decoded.snapshot)
    }
}

async fn begin_range_transaction(
    connection: &mut PgConnection,
) -> anyhow::Result<Transaction<'_, Postgres>> {
    let mut transaction = connection
        .begin()
        .await
        .context("failed to start state history range transaction")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *transaction)
        .await
        .context("failed to configure state history range transaction")?;
    Ok(transaction)
}

/// A validated block range and backend set for backtest resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestRequest {
    chain_id: u64,
    start_block: u64,
    end_block: u64,
    backends: Vec<Backend>,
}

impl BacktestRequest {
    /// Creates a backtest request.
    ///
    /// # Errors
    ///
    /// Returns an error for an inverted range, an empty backend set, or an end
    /// block whose successor cannot be represented.
    pub fn new(
        chain_id: u64,
        start_block: u64,
        end_block: u64,
        mut backends: Vec<Backend>,
    ) -> anyhow::Result<Self> {
        if start_block > end_block {
            return Err(ModelError::InvalidBlockRange {
                start: start_block,
                end: end_block,
            }
            .into());
        }
        if backends.is_empty() {
            return Err(ModelError::EmptyBackends.into());
        }
        ensure!(
            end_block != u64::MAX,
            "backtest end block must be less than u64::MAX because its successor is required"
        );

        backends.sort_unstable();
        backends.dedup();
        Ok(Self {
            chain_id,
            start_block,
            end_block,
            backends,
        })
    }

    fn includes_rfq(&self) -> bool {
        self.backends.binary_search(&Backend::Rfq).is_ok()
    }

    fn range_request(&self, rfq_bounds: Option<(u64, u64)>) -> RangeRequest {
        let (rfq_start_ms, rfq_end_ms) = rfq_bounds
            .map(|(start_ms, end_ms)| (Some(start_ms), Some(end_ms)))
            .unwrap_or((None, None));
        RangeRequest {
            chain_id: self.chain_id,
            start_block: self.start_block,
            end_block: self.end_block,
            backends: self.backends.clone(),
            rfq_start_ms,
            rfq_end_ms,
        }
    }
}

/// Timestamps, derived RFQ bounds, and replay work for a backtest.
#[derive(Debug, Clone)]
pub struct BacktestPlan {
    pub start_block_timestamp_ms: u64,
    pub end_block_timestamp_ms: u64,
    pub rfq_bounds: Option<(u64, u64)>,
    pub range: RangePlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeRequest {
    chain_id: u64,
    start_block: u64,
    end_block: u64,
    backends: Vec<Backend>,
    rfq_start_ms: Option<u64>,
    rfq_end_ms: Option<u64>,
}

impl RangeRequest {
    pub fn new(
        chain_id: u64,
        start_block: u64,
        end_block: u64,
        mut backends: Vec<Backend>,
    ) -> Result<Self, ModelError> {
        if start_block > end_block {
            return Err(ModelError::InvalidBlockRange {
                start: start_block,
                end: end_block,
            });
        }
        if backends.is_empty() {
            return Err(ModelError::EmptyBackends);
        }
        backends.sort_unstable();
        backends.dedup();

        Ok(Self {
            chain_id,
            start_block,
            end_block,
            backends,
            rfq_start_ms: None,
            rfq_end_ms: None,
        })
    }

    pub fn with_rfq_bounds(mut self, start_ms: u64, end_ms: u64) -> Result<Self, ModelError> {
        if start_ms > end_ms {
            return Err(ModelError::InvalidRfqRange { start_ms, end_ms });
        }
        self.rfq_start_ms = Some(start_ms);
        self.rfq_end_ms = Some(end_ms);
        Ok(self)
    }

    fn rfq_bounds(&self) -> Result<Option<(u64, u64)>, ModelError> {
        if self.backends.binary_search(&Backend::Rfq).is_err() {
            return Ok(None);
        }
        match (self.rfq_start_ms, self.rfq_end_ms) {
            (Some(start_ms), Some(end_ms)) => Ok(Some((start_ms, end_ms))),
            _ => Err(ModelError::MissingRfqBounds),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RangePlan {
    pub legs: Vec<RangeLeg>,
    pub gaps: Vec<RangeGap>,
}

pub(super) struct RangeResolution {
    pub(super) plan: RangePlan,
    pub(super) estimated_decoded_bytes: u64,
}

impl RangePlan {
    pub fn ensure_gap_free(&self) -> Result<(), GapFreeError> {
        if self.gaps.is_empty() {
            return Ok(());
        }

        Err(GapFreeError {
            gaps: self
                .gaps
                .iter()
                .map(|gap| (gap.kind, gap.reason.clone()))
                .collect(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RangeLeg {
    pub checkpoint: CheckpointManifest,
    pub deltas: Vec<StoredDelta>,
}

#[derive(Debug, Clone)]
pub struct StoredDelta {
    pub position: StreamPosition,
    pub observed_at_ms: u64,
    pub payload_format_version: i16,
    /// Exact stored payload. Apply only partitions listed in `applicable_backends`.
    pub raw_payload: Box<RawValue>,
    pub applicable_backends: Vec<DeltaBackendCursor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeGapKind {
    MissingCheckpoint,
    Recorded,
    IncompleteTail,
}

impl fmt::Display for RangeGapKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::MissingCheckpoint => "missing_checkpoint",
            Self::Recorded => "recorded",
            Self::IncompleteTail => "incomplete_tail",
        };
        formatter.write_str(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeGap {
    pub kind: RangeGapKind,
    pub generation: Option<u64>,
    pub from_message_seq: Option<u64>,
    pub to_message_seq: Option<u64>,
    pub from_position: Option<StreamPosition>,
    pub to_position: Option<StreamPosition>,
    pub from_block: Option<u64>,
    pub to_block_inclusive: Option<u64>,
    pub from_observed_at_ms: Option<u64>,
    pub to_observed_at_ms: Option<u64>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapFreeError {
    pub gaps: Vec<(RangeGapKind, String)>,
}

impl fmt::Display for GapFreeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pairs = self
            .gaps
            .iter()
            .map(|(kind, reason)| format!("{kind}: {reason}"))
            .collect::<Vec<_>>()
            .join(", ");
        write!(formatter, "state history range has gaps: {pairs}")
    }
}

impl std::error::Error for GapFreeError {}

#[derive(Debug, Clone)]
struct DeltaRow {
    position: StreamPosition,
    observed_at_ms: u64,
    payload_format_version: i16,
    payload: Box<RawValue>,
    backends: Vec<DeltaBackendCursor>,
}

struct DeltaFetch {
    rows: Vec<DeltaRow>,
    decoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ManifestRow {
    manifest: CheckpointManifest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GapRow {
    generation: u64,
    from_message_seq: u64,
    to_message_seq: u64,
    reason: String,
    from_block_number: Option<u64>,
    to_block_number: Option<u64>,
    from_observed_at_ms: Option<u64>,
    to_observed_at_ms: Option<u64>,
}

struct EncodedDeltaRow {
    id: i64,
    position: StreamPosition,
    observed_at_ms: u64,
    payload_format_version: i16,
    payload: Vec<u8>,
    payload_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoredPayloadError {
    #[error("unsupported delta payload format {0}")]
    UnsupportedDeltaFormat(i16),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PositionSpan {
    generation: u64,
    from_message_seq: u64,
    to_message_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockTimeRow {
    block_number: u64,
    timestamp_ms: u64,
    block_hash: Option<String>,
    parent_hash: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequiredBlockTime {
    Start { block_number: u64 },
    End { block_number: u64 },
    EndPlusOne { block_number: u64 },
}

pub(super) async fn resolve_range_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    request: &RangeRequest,
    rfq_bounds: Option<(u64, u64)>,
    limits: ReadLimits,
) -> anyhow::Result<RangeResolution> {
    let Some(mut anchor) = fetch_anchor(
        &mut *transaction,
        request,
        rfq_bounds.map(|bounds| bounds.0),
    )
    .await?
    else {
        return Ok(RangeResolution {
            plan: RangePlan {
                legs: vec![],
                gaps: vec![RangeGap {
                    kind: RangeGapKind::MissingCheckpoint,
                    generation: None,
                    from_message_seq: None,
                    to_message_seq: None,
                    from_position: None,
                    to_position: None,
                    from_block: Some(request.start_block),
                    to_block_inclusive: Some(request.end_block),
                    from_observed_at_ms: rfq_bounds.map(|bounds| bounds.0),
                    to_observed_at_ms: rfq_bounds.map(|bounds| bounds.1),
                    reason: format!(
                        "no complete checkpoint at or before block {}",
                        request.start_block
                    ),
                }],
            },
            estimated_decoded_bytes: 0,
        });
    };

    let needs_token_snapshot = request
        .backends
        .iter()
        .any(|backend| matches!(backend, Backend::Native | Backend::Vm));
    if needs_token_snapshot && anchor.token_reference.is_none() {
        if let Some(fallback) = checkpoints::token_fallback_in_segment(transaction, &anchor).await?
        {
            anchor = fallback;
        }
    }

    let boundaries = fetch_boundaries(&mut *transaction, &anchor).await?;
    let (checkpoint_compressed_bytes, checkpoint_decoded_bytes) =
        declared_checkpoint_bytes(&mut *transaction, &anchor, &boundaries, request).await?;
    limits.check_compressed(checkpoint_compressed_bytes)?;
    limits.check_decoded(checkpoint_decoded_bytes)?;
    let delta_limits = ReadLimits {
        max_compressed_bytes: limits
            .max_compressed_bytes
            .saturating_sub(checkpoint_compressed_bytes),
        max_decoded_bytes: limits
            .max_decoded_bytes
            .saturating_sub(checkpoint_decoded_bytes),
    };
    let delta_fetch = fetch_deltas(
        &mut *transaction,
        request,
        &anchor,
        rfq_bounds,
        delta_limits,
    )
    .await?;
    let gaps = fetch_gap_rows(&mut *transaction, request, &anchor, rfq_bounds).await?;
    let (legs, gaps) = build_legs(anchor, delta_fetch.rows, boundaries, gaps, request);
    Ok(RangeResolution {
        plan: RangePlan { legs, gaps },
        estimated_decoded_bytes: checkpoint_decoded_bytes
            .checked_add(delta_fetch.decoded_bytes)
            .context("state history decoded byte estimate overflowed")?,
    })
}

async fn declared_checkpoint_bytes(
    connection: &mut PgConnection,
    anchor: &CheckpointManifest,
    boundaries: &[ManifestRow],
    request: &RangeRequest,
) -> anyhow::Result<(u64, u64)> {
    let manifests =
        std::iter::once(anchor).chain(boundaries.iter().map(|row| &row.manifest).filter(
            |manifest| {
                manifest.status == CheckpointStatus::Complete
                    && boundary_fits_request_end(manifest, request)
                    && request
                        .backends
                        .iter()
                        .all(|backend| manifest.backends.binary_search(backend).is_ok())
            },
        ));
    let mut compressed_bytes = 0_u64;
    let mut decoded_bytes = 0_u64;
    let mut token_hashes = BTreeSet::new();
    for manifest in manifests {
        compressed_bytes = compressed_bytes
            .checked_add(
                manifest
                    .compressed_bytes
                    .context("complete checkpoint is missing its compressed byte length")?,
            )
            .context("checkpoint compressed byte estimate overflowed")?;
        decoded_bytes = decoded_bytes
            .checked_add(
                manifest
                    .archive_bytes
                    .context("complete checkpoint is missing its archive byte length")?,
            )
            .context("checkpoint decoded byte estimate overflowed")?;
        let token_reference = match manifest.token_reference.clone() {
            Some(reference) => Some(reference),
            None => checkpoints::token_fallback_in_segment(connection, manifest)
                .await?
                .and_then(|checkpoint| checkpoint.token_reference),
        };
        if let Some(reference) =
            token_reference.filter(|reference| token_hashes.insert(reference.sha256.clone()))
        {
            decoded_bytes = decoded_bytes
                .checked_add(reference.token_bytes)
                .context("token snapshot decoded byte estimate overflowed")?;
        }
    }
    Ok((compressed_bytes, decoded_bytes))
}

async fn fetch_informational_timestamps(
    connection: &mut PgConnection,
    chain_id: u64,
    start_block: u64,
    end_block: u64,
) -> anyhow::Result<(u64, u64)> {
    let rows = sqlx::query(
        "SELECT block_number, timestamp_ms
         FROM state_history.block_times
         WHERE chain_id = $1
           AND block_number = ANY($2::bigint[])",
    )
    .bind(database_i64(chain_id, "backtest chain_id")?)
    .bind([
        database_i64(start_block, "backtest start_block")?,
        database_i64(end_block, "backtest end_block")?,
    ])
    .fetch_all(connection)
    .await
    .context("failed to select informational backtest block timestamps")?;

    let mut start_timestamp_ms = 0;
    let mut end_timestamp_ms = 0;
    for row in rows {
        let block_number = database_u64(row.try_get("block_number")?, "block time block_number")?;
        let timestamp_ms = database_u64(row.try_get("timestamp_ms")?, "block time timestamp_ms")?;
        if block_number == start_block {
            start_timestamp_ms = timestamp_ms;
        }
        if block_number == end_block {
            end_timestamp_ms = timestamp_ms;
        }
    }
    Ok((start_timestamp_ms, end_timestamp_ms))
}

async fn fetch_block_time(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    block_number: u64,
) -> anyhow::Result<Option<BlockTimeRow>> {
    let row = sqlx::query(
        "SELECT block_number, timestamp_ms, block_hash, parent_hash
         FROM state_history.block_times
         WHERE chain_id = $1
           AND block_number = $2",
    )
    .bind(database_i64(chain_id, "backtest chain_id")?)
    .bind(database_i64(block_number, "backtest block_number")?)
    .fetch_optional(connection)
    .await
    .context("failed to select required backtest block time")?;
    row.as_ref().map(block_time_from_row).transpose()
}

async fn fetch_block_time_span(
    connection: &mut sqlx::PgConnection,
    chain_id: u64,
    start_block: u64,
    end_block: u64,
) -> anyhow::Result<Vec<BlockTimeRow>> {
    let rows = sqlx::query(
        "SELECT block_number, timestamp_ms, block_hash, parent_hash
         FROM state_history.block_times
         WHERE chain_id = $1
           AND block_number BETWEEN $2 AND $3
         ORDER BY block_number",
    )
    .bind(database_i64(chain_id, "backtest chain_id")?)
    .bind(database_i64(start_block, "backtest start_block")?)
    .bind(database_i64(end_block, "backtest end plus one block")?)
    .fetch_all(connection)
    .await
    .context("failed to select backtest block time span")?;
    rows.iter().map(block_time_from_row).collect()
}

fn block_time_from_row(row: &PgRow) -> anyhow::Result<BlockTimeRow> {
    Ok(BlockTimeRow {
        block_number: database_u64(row.try_get("block_number")?, "block time block_number")?,
        timestamp_ms: database_u64(row.try_get("timestamp_ms")?, "block time timestamp_ms")?,
        block_hash: row.try_get("block_hash")?,
        parent_hash: row.try_get("parent_hash")?,
    })
}

fn require_block_time(
    row: Option<BlockTimeRow>,
    requirement: RequiredBlockTime,
) -> anyhow::Result<BlockTimeRow> {
    row.with_context(|| match requirement {
        RequiredBlockTime::Start { block_number } => {
            format!("missing block_times row for backtest start block {block_number}")
        }
        RequiredBlockTime::End { block_number } => {
            format!("missing block_times row for backtest end block {block_number}")
        }
        RequiredBlockTime::EndPlusOne { block_number } => format!(
            "missing block_times row for block {block_number}; ranges ending at the recorded head resolve after the next block lands"
        ),
    })
}

fn validate_block_time_span(
    rows: &[BlockTimeRow],
    start_block: u64,
    end_block: u64,
) -> anyhow::Result<()> {
    let expected_count = end_block
        .checked_sub(start_block)
        .and_then(|count| count.checked_add(1))
        .context("backtest block time span is too large")?;
    ensure!(
        u64::try_from(rows.len()).context("backtest block time row count exceeds u64")?
            == expected_count,
        "incomplete block_times metadata for blocks {start_block} through {end_block}: expected {expected_count} rows, found {}",
        rows.len()
    );

    for (offset, row) in rows.iter().enumerate() {
        let expected_block = start_block
            + u64::try_from(offset).context("backtest block time offset exceeds u64")?;
        ensure!(
            row.block_number == expected_block,
            "incomplete block_times metadata: expected block {expected_block}, found block {}",
            row.block_number
        );
    }

    for pair in rows.windows(2) {
        let [previous, current] = pair else {
            anyhow::bail!("block time lineage window must contain two rows");
        };
        let previous_hash = previous.block_hash.as_deref().with_context(|| {
            format!(
                "missing block hash metadata for block {}",
                previous.block_number
            )
        })?;
        let current_parent = current.parent_hash.as_deref().with_context(|| {
            format!(
                "missing parent hash metadata for block {}",
                current.block_number
            )
        })?;
        ensure!(
            current_parent == previous_hash,
            "block_times lineage break between blocks {} and {}",
            previous.block_number,
            current.block_number
        );
    }

    let [.., end, next] = rows else {
        anyhow::bail!("backtest block time span must include the end block and its successor");
    };
    ensure!(
        next.timestamp_ms > end.timestamp_ms,
        "block {} timestamp {} must be greater than block {} timestamp {}",
        next.block_number,
        next.timestamp_ms,
        end.block_number,
        end.timestamp_ms
    );
    Ok(())
}

async fn fetch_anchor(
    connection: &mut sqlx::PgConnection,
    request: &RangeRequest,
    rfq_start_ms: Option<u64>,
) -> anyhow::Result<Option<CheckpointManifest>> {
    let requested_backends = database_backends(&request.backends);
    let row = sqlx::query(
        "SELECT id, chain_id, generation, message_seq, state_version, kind, block_number,
                rfq_observed_at_ms, backends, s3_key, archive_sha256, archive_bytes,
                compressed_bytes, token_s3_key, token_sha256, token_count, token_bytes,
                status, error
         FROM state_history.checkpoints
         WHERE chain_id = $1
           AND status = 'complete'
           AND block_number <= $2
           AND backends @> $3::text[]
           AND ($4::boolean = false OR rfq_observed_at_ms <= $5)
         ORDER BY generation DESC, message_seq DESC
         LIMIT 1",
    )
    .bind(database_i64(request.chain_id, "range chain_id")?)
    .bind(database_i64(request.start_block, "range start_block")?)
    .bind(requested_backends)
    .bind(rfq_start_ms.is_some())
    .bind(
        rfq_start_ms
            .map(|value| database_i64(value, "range RFQ start_ms"))
            .transpose()?,
    )
    .fetch_optional(connection)
    .await
    .context("failed to select state history range anchor")?;
    row.as_ref().map(manifest_from_row).transpose()
}

async fn fetch_deltas(
    connection: &mut sqlx::PgConnection,
    request: &RangeRequest,
    anchor: &CheckpointManifest,
    rfq_bounds: Option<(u64, u64)>,
    limits: ReadLimits,
) -> anyhow::Result<DeltaFetch> {
    preflight_delta_payloads(connection, request, anchor, rfq_bounds, limits).await?;
    let encoded = fetch_encoded_deltas(connection, request, anchor, rfq_bounds).await?;
    let backend_cursors = fetch_delta_backends(
        connection,
        &encoded.iter().map(|row| row.id).collect::<Vec<_>>(),
    )
    .await?;
    let mut decoded_bytes = 0_u64;
    let mut decoded = Vec::with_capacity(encoded.len());
    for row in encoded {
        let remaining = limits.max_decoded_bytes.saturating_sub(decoded_bytes);
        let (row, row_decoded_bytes) =
            decode_delta_row_with_limit(row, &backend_cursors, remaining)?;
        decoded_bytes = decoded_bytes
            .checked_add(row_decoded_bytes)
            .context("delta decoded byte total overflowed")?;
        decoded.push(row);
    }
    Ok(DeltaFetch {
        rows: decoded,
        decoded_bytes,
    })
}

async fn preflight_delta_payloads(
    connection: &mut PgConnection,
    request: &RangeRequest,
    anchor: &CheckpointManifest,
    rfq_bounds: Option<(u64, u64)>,
    limits: ReadLimits,
) -> anyhow::Result<()> {
    let (compressed_bytes, payload_formats): (i64, Vec<i16>) = sqlx::query_as(
        "SELECT COALESCE(sum(octet_length(d.payload)), 0)::bigint,
                COALESCE(
                    array_agg(DISTINCT d.payload_format_version ORDER BY d.payload_format_version),
                    ARRAY[]::smallint[]
                )
         FROM state_history.deltas d
         WHERE d.chain_id = $1
           AND (d.generation, d.message_seq) > ($2, $3)
           AND EXISTS (
               SELECT 1
               FROM state_history.delta_backends matched
               WHERE matched.delta_id = d.id
                 AND matched.backend = ANY($4::text[])
                 AND (
                     (matched.backend <> 'rfq' AND matched.block_number <= $5)
                     OR
                     (matched.backend = 'rfq' AND matched.observed_at_ms <= $6)
                 )
           )",
    )
    .bind(database_i64(request.chain_id, "range chain_id")?)
    .bind(database_i64(
        anchor.position.generation,
        "anchor generation",
    )?)
    .bind(database_i64(
        anchor.position.message_seq,
        "anchor message_seq",
    )?)
    .bind(database_backends(&request.backends))
    .bind(database_i64(request.end_block, "range end_block")?)
    .bind(
        rfq_bounds
            .map(|bounds| database_i64(bounds.1, "range RFQ end_ms"))
            .transpose()?,
    )
    .fetch_one(&mut *connection)
    .await
    .context("failed to preflight state history delta bytes")?;
    let compressed_bytes = database_u64(compressed_bytes, "delta compressed byte total")?;
    limits.check_compressed(compressed_bytes)?;
    if let Some(format) = payload_formats
        .into_iter()
        .find(|format| *format != crate::DELTA_PAYLOAD_FORMAT_VERSION)
    {
        return Err(StoredPayloadError::UnsupportedDeltaFormat(format).into());
    }
    Ok(())
}

async fn fetch_encoded_deltas(
    connection: &mut PgConnection,
    request: &RangeRequest,
    anchor: &CheckpointManifest,
    rfq_bounds: Option<(u64, u64)>,
) -> anyhow::Result<Vec<EncodedDeltaRow>> {
    let rows = sqlx::query(
        "SELECT d.id, d.generation, d.message_seq, d.observed_at_ms,
                d.payload_format_version, d.payload, d.payload_sha256
         FROM state_history.deltas d
         WHERE d.chain_id = $1
           AND (d.generation, d.message_seq) > ($2, $3)
           AND EXISTS (
               SELECT 1
               FROM state_history.delta_backends matched
               WHERE matched.delta_id = d.id
                 AND matched.backend = ANY($4::text[])
                 AND (
                     (matched.backend <> 'rfq' AND matched.block_number <= $5)
                     OR
                     (matched.backend = 'rfq' AND matched.observed_at_ms <= $6)
                 )
           )
         ORDER BY d.generation, d.message_seq",
    )
    .bind(database_i64(request.chain_id, "range chain_id")?)
    .bind(database_i64(
        anchor.position.generation,
        "anchor generation",
    )?)
    .bind(database_i64(
        anchor.position.message_seq,
        "anchor message_seq",
    )?)
    .bind(database_backends(&request.backends))
    .bind(database_i64(request.end_block, "range end_block")?)
    .bind(
        rfq_bounds
            .map(|bounds| database_i64(bounds.1, "range RFQ end_ms"))
            .transpose()?,
    )
    .fetch_all(&mut *connection)
    .await
    .context("failed to select state history range deltas")?;
    rows.iter().map(encoded_delta_from_row).collect()
}

async fn fetch_delta_backends(
    connection: &mut sqlx::PgConnection,
    delta_ids: &[i64],
) -> anyhow::Result<BTreeMap<i64, Vec<DeltaBackendCursor>>> {
    if delta_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows = sqlx::query(
        "SELECT delta_id, backend, block_number, observed_at_ms
         FROM state_history.delta_backends
         WHERE delta_id = ANY($1::bigint[])
         ORDER BY delta_id, backend",
    )
    .bind(delta_ids)
    .fetch_all(connection)
    .await
    .context("failed to select state history delta backend cursors")?;
    let mut cursors = BTreeMap::<i64, Vec<DeltaBackendCursor>>::new();
    for row in rows {
        let delta_id: i64 = row.try_get("delta_id")?;
        let backend: String = row.try_get("backend")?;
        cursors
            .entry(delta_id)
            .or_default()
            .push(DeltaBackendCursor {
                backend: backend.parse()?,
                block_number: optional_database_u64(row.try_get("block_number")?, "block_number")?,
                observed_at_ms: optional_database_u64(
                    row.try_get("observed_at_ms")?,
                    "observed_at_ms",
                )?,
            });
    }
    Ok(cursors)
}

async fn fetch_boundaries(
    connection: &mut sqlx::PgConnection,
    anchor: &CheckpointManifest,
) -> anyhow::Result<Vec<ManifestRow>> {
    let rows = sqlx::query(
        "SELECT id, chain_id, generation, message_seq, state_version, kind, block_number,
                rfq_observed_at_ms, backends, s3_key, archive_sha256, archive_bytes,
                compressed_bytes, token_s3_key, token_sha256, token_count, token_bytes,
                status, error
         FROM state_history.checkpoints
         WHERE chain_id = $1
           AND kind = 'boundary'
           AND (generation, message_seq) > ($2, $3)
         ORDER BY generation, message_seq",
    )
    .bind(database_i64(anchor.chain_id, "anchor chain_id")?)
    .bind(database_i64(
        anchor.position.generation,
        "anchor generation",
    )?)
    .bind(database_i64(
        anchor.position.message_seq,
        "anchor message_seq",
    )?)
    .fetch_all(connection)
    .await
    .context("failed to select state history boundary checkpoints")?;
    rows.iter()
        .map(|row| {
            Ok(ManifestRow {
                manifest: manifest_from_row(row)?,
            })
        })
        .collect()
}

fn boundary_fits_request_end(manifest: &CheckpointManifest, request: &RangeRequest) -> bool {
    request.backends.iter().all(|backend| {
        if manifest.backends.binary_search(backend).is_err() {
            return true;
        }
        match backend {
            Backend::Native | Backend::Vm => manifest.block_number <= request.end_block,
            Backend::Rfq => request.rfq_end_ms.is_some_and(|end| {
                manifest
                    .rfq_observed_at_ms
                    .is_some_and(|cursor| cursor <= end)
            }),
        }
    })
}

async fn fetch_gap_rows(
    connection: &mut sqlx::PgConnection,
    request: &RangeRequest,
    anchor: &CheckpointManifest,
    rfq_bounds: Option<(u64, u64)>,
) -> anyhow::Result<Vec<GapRow>> {
    let includes_block_backend = request
        .backends
        .iter()
        .any(|backend| matches!(backend, Backend::Native | Backend::Vm));
    let anchor_rfq_observed_at_ms = rfq_bounds
        .map(|_| {
            anchor
                .rfq_observed_at_ms
                .context("RFQ range anchor is missing its observed-at cursor")
        })
        .transpose()?;
    let rows = sqlx::query(
        "SELECT generation, from_message_seq, to_message_seq, reason,
                from_block_number, to_block_number, from_observed_at_ms, to_observed_at_ms
         FROM state_history.gaps
         WHERE chain_id = $1
           AND generation >= $2
           AND (generation > $2 OR to_message_seq > $3)
           AND (
               ($4::boolean
                AND to_block_number >= $5
                AND from_block_number <= $6)
               OR
               ($7::boolean
                AND to_observed_at_ms >= $8
                AND from_observed_at_ms <= $9)
               OR
               (from_block_number IS NULL
                AND to_block_number IS NULL
                AND from_observed_at_ms IS NULL
                AND to_observed_at_ms IS NULL)
               OR
               (reason = 'checkpoint_failed')
           )
         ORDER BY generation, from_message_seq, to_message_seq, id",
    )
    .bind(database_i64(anchor.chain_id, "anchor chain_id")?)
    .bind(database_i64(
        anchor.position.generation,
        "anchor generation",
    )?)
    .bind(database_i64(
        anchor.position.message_seq,
        "anchor message_seq",
    )?)
    .bind(includes_block_backend)
    .bind(database_i64(anchor.block_number, "anchor block_number")?)
    .bind(database_i64(request.end_block, "range end_block")?)
    .bind(rfq_bounds.is_some())
    .bind(
        anchor_rfq_observed_at_ms
            .map(|value| database_i64(value, "anchor RFQ observed_at_ms"))
            .transpose()?,
    )
    .bind(
        rfq_bounds
            .map(|bounds| database_i64(bounds.1, "range RFQ end_ms"))
            .transpose()?,
    )
    .fetch_all(connection)
    .await
    .context("failed to select recorded state history gaps")?;
    rows.iter().map(gap_from_row).collect()
}

fn encoded_delta_from_row(row: &PgRow) -> anyhow::Result<EncodedDeltaRow> {
    Ok(EncodedDeltaRow {
        id: row.try_get("id")?,
        position: StreamPosition {
            generation: database_u64(row.try_get("generation")?, "delta generation")?,
            message_seq: database_u64(row.try_get("message_seq")?, "delta message_seq")?,
        },
        observed_at_ms: database_u64(row.try_get("observed_at_ms")?, "delta observed_at_ms")?,
        payload_format_version: row.try_get("payload_format_version")?,
        payload: row.try_get("payload")?,
        payload_sha256: row.try_get("payload_sha256")?,
    })
}

fn decode_delta_row_with_limit(
    row: EncodedDeltaRow,
    backend_cursors: &BTreeMap<i64, Vec<DeltaBackendCursor>>,
    max_decoded_bytes: u64,
) -> anyhow::Result<(DeltaRow, u64)> {
    let payload_json = match row.payload_format_version {
        crate::DELTA_PAYLOAD_FORMAT_VERSION => {
            decode_zstd_with_limit(row.payload.as_slice(), max_decoded_bytes, "delta format 1")?
        }
        other => return Err(StoredPayloadError::UnsupportedDeltaFormat(other).into()),
    };
    let decoded_bytes =
        u64::try_from(payload_json.len()).context("delta decoded byte length exceeds u64")?;
    let actual_sha256 = hex::encode(Sha256::digest(&payload_json));
    ensure!(
        actual_sha256 == row.payload_sha256,
        "delta payload sha256 mismatch at generation {} message sequence {}",
        row.position.generation,
        row.position.message_seq
    );
    let payload_json =
        String::from_utf8(payload_json).context("delta payload is not valid UTF-8")?;
    let payload = RawValue::from_string(payload_json).context("delta payload is not valid JSON")?;
    Ok((
        DeltaRow {
            position: row.position,
            observed_at_ms: row.observed_at_ms,
            payload_format_version: row.payload_format_version,
            payload,
            backends: backend_cursors.get(&row.id).cloned().unwrap_or_default(),
        },
        decoded_bytes,
    ))
}

fn manifest_from_row(row: &PgRow) -> anyhow::Result<CheckpointManifest> {
    let backend_values: Vec<String> = row.try_get("backends")?;
    Ok(CheckpointManifest {
        id: row.try_get("id")?,
        chain_id: database_u64(row.try_get("chain_id")?, "checkpoint chain_id")?,
        position: StreamPosition {
            generation: database_u64(row.try_get("generation")?, "checkpoint generation")?,
            message_seq: database_u64(row.try_get("message_seq")?, "checkpoint message_seq")?,
        },
        state_version: optional_database_u64(
            row.try_get("state_version")?,
            "checkpoint state_version",
        )?,
        kind: checkpoint_kind_from_database(&row.try_get::<String, _>("kind")?)?,
        block_number: database_u64(row.try_get("block_number")?, "checkpoint block_number")?,
        rfq_observed_at_ms: optional_database_u64(
            row.try_get("rfq_observed_at_ms")?,
            "checkpoint rfq_observed_at_ms",
        )?,
        backends: backend_values
            .into_iter()
            .map(|backend| backend.parse())
            .collect::<Result<Vec<_>, _>>()?,
        s3_key: row.try_get("s3_key")?,
        archive_sha256: row.try_get("archive_sha256")?,
        archive_bytes: optional_database_u64(
            row.try_get("archive_bytes")?,
            "checkpoint archive_bytes",
        )?,
        compressed_bytes: optional_database_u64(
            row.try_get("compressed_bytes")?,
            "checkpoint compressed_bytes",
        )?,
        token_reference: token_reference_from_row(row)?,
        status: checkpoint_status_from_database(&row.try_get::<String, _>("status")?)?,
        error: row.try_get("error")?,
    })
}

fn token_reference_from_row(row: &PgRow) -> anyhow::Result<Option<TokenSnapshotRef>> {
    let s3_key: Option<String> = row.try_get("token_s3_key")?;
    let sha256: Option<String> = row.try_get("token_sha256")?;
    let token_count: Option<i64> = row.try_get("token_count")?;
    let token_bytes: Option<i64> = row.try_get("token_bytes")?;

    match (s3_key, sha256, token_count, token_bytes) {
        (Some(s3_key), Some(sha256), Some(token_count), Some(token_bytes)) => {
            Ok(Some(TokenSnapshotRef {
                s3_key,
                sha256,
                token_count: database_u64(token_count, "checkpoint token_count")?,
                token_bytes: database_u64(token_bytes, "checkpoint token_bytes")?,
            }))
        }
        (None, None, None, None) => Ok(None),
        _ => anyhow::bail!("checkpoint token reference columns must be all set or all null"),
    }
}

fn gap_from_row(row: &PgRow) -> anyhow::Result<GapRow> {
    Ok(GapRow {
        generation: database_u64(row.try_get("generation")?, "gap generation")?,
        from_message_seq: database_u64(row.try_get("from_message_seq")?, "gap from_message_seq")?,
        to_message_seq: database_u64(row.try_get("to_message_seq")?, "gap to_message_seq")?,
        reason: row.try_get("reason")?,
        from_block_number: optional_database_u64(
            row.try_get("from_block_number")?,
            "gap from_block_number",
        )?,
        to_block_number: optional_database_u64(
            row.try_get("to_block_number")?,
            "gap to_block_number",
        )?,
        from_observed_at_ms: optional_database_u64(
            row.try_get("from_observed_at_ms")?,
            "gap from_observed_at_ms",
        )?,
        to_observed_at_ms: optional_database_u64(
            row.try_get("to_observed_at_ms")?,
            "gap to_observed_at_ms",
        )?,
    })
}

fn checkpoint_kind_from_database(value: &str) -> anyhow::Result<CheckpointKind> {
    match value {
        "boundary" => Ok(CheckpointKind::Boundary),
        "interval" => Ok(CheckpointKind::Interval),
        _ => anyhow::bail!("unsupported checkpoint kind `{value}`"),
    }
}

fn checkpoint_status_from_database(value: &str) -> anyhow::Result<CheckpointStatus> {
    match value {
        "writing" => Ok(CheckpointStatus::Writing),
        "complete" => Ok(CheckpointStatus::Complete),
        "failed" => Ok(CheckpointStatus::Failed),
        _ => anyhow::bail!("unsupported checkpoint status `{value}`"),
    }
}

fn database_backends(backends: &[Backend]) -> Vec<&'static str> {
    backends.iter().map(|backend| backend.as_str()).collect()
}

fn database_i64(value: u64, field: &str) -> anyhow::Result<i64> {
    i64::try_from(value).with_context(|| format!("{field} exceeds PostgreSQL BIGINT"))
}

fn database_u64(value: i64, field: &str) -> anyhow::Result<u64> {
    u64::try_from(value).with_context(|| format!("{field} is negative"))
}

fn optional_database_u64(value: Option<i64>, field: &str) -> anyhow::Result<Option<u64>> {
    value.map(|value| database_u64(value, field)).transpose()
}

fn build_legs(
    anchor: CheckpointManifest,
    mut deltas: Vec<DeltaRow>,
    boundaries: Vec<ManifestRow>,
    gaps: Vec<GapRow>,
    request: &RangeRequest,
) -> (Vec<RangeLeg>, Vec<RangeGap>) {
    deltas.sort_by_key(|delta| delta.position);
    let anchor_block_number = anchor.block_number;
    let anchor_rfq_observed_at_ms = anchor.rfq_observed_at_ms;
    let breaks = break_positions(&anchor, &deltas, &boundaries, &gaps, request);
    let mut legs = Vec::with_capacity(breaks.len() + 1);
    let mut range_gaps = Vec::new();

    let first_break = breaks.first().copied();
    let anchor_deltas = deltas
        .iter()
        .filter(|delta| first_break.is_none_or(|position| delta.position < position))
        .collect::<Vec<_>>();
    push_leg(&mut legs, anchor, &anchor_deltas, request);

    for (index, position) in breaks.iter().copied().enumerate() {
        let next_break = breaks.get(index + 1).copied();
        let segment = deltas
            .iter()
            .filter(|delta| {
                delta.position >= position && next_break.is_none_or(|next| delta.position < next)
            })
            .collect::<Vec<_>>();
        let checkpoint = complete_boundary_at(&boundaries, position, request);
        if let Some(checkpoint) = checkpoint {
            let replayable = segment
                .into_iter()
                .filter(|delta| delta.position > checkpoint.position)
                .collect::<Vec<_>>();
            push_leg(&mut legs, checkpoint, &replayable, request);
        } else {
            let span = missing_span(position, &segment);
            range_gaps.push(missing_checkpoint_gap(span));
        }
    }

    range_gaps.extend(recorded_range_gaps(
        &gaps,
        request,
        anchor_block_number,
        anchor_rfq_observed_at_ms,
    ));
    range_gaps.extend(incomplete_tail_gaps(request, &legs, &deltas));

    (legs, range_gaps)
}

fn break_positions(
    anchor: &CheckpointManifest,
    deltas: &[DeltaRow],
    boundaries: &[ManifestRow],
    gaps: &[GapRow],
    request: &RangeRequest,
) -> Vec<StreamPosition> {
    let all_boundary_positions = boundaries
        .iter()
        .map(|row| &row.manifest)
        .filter(|manifest| {
            manifest.kind == crate::CheckpointKind::Boundary && manifest.position > anchor.position
        })
        .map(|manifest| manifest.position)
        .collect::<BTreeSet<_>>();
    let last_delta_position = deltas.last().map(|delta| delta.position);
    // A boundary past every selected delta must not fail a range that ends before it.
    let mut positions = boundaries
        .iter()
        .map(|row| &row.manifest)
        .filter(|manifest| {
            manifest.kind == crate::CheckpointKind::Boundary
                && manifest.position > anchor.position
                && (last_delta_position.is_some_and(|last_delta| manifest.position <= last_delta)
                    || boundary_fits_request_end(manifest, request))
        })
        .map(|manifest| manifest.position)
        .collect::<BTreeSet<_>>();

    if let Some(last_delta_position) = last_delta_position {
        positions.extend(
            gaps.iter()
                // Overlapping failures already fail as Recorded gaps.
                // This cut keeps hidden bounded failures from being bridged by later deltas.
                // The writer merges adjacent failures into one span, so the row's width
                // does not matter: any checkpoint_failed row marks a boundary that never
                // landed, and the cut goes at the span start.
                .filter(|gap| {
                    gap.reason == "checkpoint_failed"
                        && gap_has_cursor_bounds(gap)
                        && (gap.generation > anchor.position.generation
                            || gap.to_message_seq > anchor.position.message_seq)
                        && !gap_overlaps_request(
                            gap,
                            request,
                            anchor.block_number,
                            anchor.rfq_observed_at_ms,
                        )
                })
                .map(|gap| {
                    // A merged span can straddle the anchor, so the cut clamps to the
                    // first post-anchor position instead of the raw span start.
                    let message_seq = if gap.generation == anchor.position.generation {
                        gap.from_message_seq.max(anchor.position.message_seq + 1)
                    } else {
                        gap.from_message_seq
                    };
                    StreamPosition {
                        generation: gap.generation,
                        message_seq,
                    }
                })
                .filter(|position| *position > anchor.position && *position <= last_delta_position),
        );
    }

    let mut previous_generation = anchor.position.generation;
    for delta in deltas {
        // Promotion markers have no delta row, so the manifest is authoritative.
        // A generation jump only proxies for a missing manifest.
        let generation_has_leading_boundary = all_boundary_positions.iter().any(|position| {
            position.generation == delta.position.generation && *position <= delta.position
        });
        if delta.position.generation > anchor.position.generation
            && delta.position.generation != previous_generation
            && !generation_has_leading_boundary
        {
            positions.insert(delta.position);
        }
        previous_generation = delta.position.generation;
    }

    positions.into_iter().collect()
}

fn complete_boundary_at(
    boundaries: &[ManifestRow],
    position: StreamPosition,
    request: &RangeRequest,
) -> Option<CheckpointManifest> {
    boundaries
        .iter()
        .map(|row| &row.manifest)
        .find(|manifest| {
            manifest.position == position
                && manifest.kind == crate::CheckpointKind::Boundary
                && manifest.status == crate::CheckpointStatus::Complete
                && boundary_fits_request_end(manifest, request)
                && request
                    .backends
                    .iter()
                    .all(|backend| manifest.backends.binary_search(backend).is_ok())
        })
        .cloned()
}

fn push_leg(
    legs: &mut Vec<RangeLeg>,
    checkpoint: CheckpointManifest,
    deltas: &[&DeltaRow],
    request: &RangeRequest,
) {
    legs.push(RangeLeg {
        checkpoint,
        deltas: deltas
            .iter()
            .map(|delta| stored_delta(delta, request))
            .collect(),
    });
}

fn stored_delta(row: &DeltaRow, request: &RangeRequest) -> StoredDelta {
    StoredDelta {
        position: row.position,
        observed_at_ms: row.observed_at_ms,
        payload_format_version: row.payload_format_version,
        raw_payload: row.payload.clone(),
        applicable_backends: row
            .backends
            .iter()
            .filter(|cursor| {
                request.backends.binary_search(&cursor.backend).is_ok()
                    && match cursor.backend {
                        Backend::Native | Backend::Vm => cursor
                            .block_number
                            .is_none_or(|block_number| block_number <= request.end_block),
                        Backend::Rfq => cursor
                            .observed_at_ms
                            .zip(request.rfq_end_ms)
                            .is_none_or(|(observed_at_ms, end_ms)| observed_at_ms <= end_ms),
                    }
            })
            .cloned()
            .collect(),
    }
}

fn missing_span(position: StreamPosition, deltas: &[&DeltaRow]) -> PositionSpan {
    let to_message_seq = deltas
        .iter()
        .rev()
        .find(|delta| delta.position.generation == position.generation)
        .map_or(position.message_seq, |delta| delta.position.message_seq);
    PositionSpan {
        generation: position.generation,
        from_message_seq: position.message_seq,
        to_message_seq,
    }
}

fn missing_checkpoint_gap(span: PositionSpan) -> RangeGap {
    let from_position = StreamPosition {
        generation: span.generation,
        message_seq: span.from_message_seq,
    };
    let to_position = StreamPosition {
        generation: span.generation,
        message_seq: span.to_message_seq,
    };
    RangeGap {
        kind: RangeGapKind::MissingCheckpoint,
        generation: Some(span.generation),
        from_message_seq: Some(span.from_message_seq),
        to_message_seq: Some(span.to_message_seq),
        from_position: Some(from_position),
        to_position: Some(to_position),
        from_block: None,
        to_block_inclusive: None,
        from_observed_at_ms: None,
        to_observed_at_ms: None,
        reason: format!(
            "no complete boundary checkpoint at generation {} message sequence {}",
            span.generation, span.from_message_seq
        ),
    }
}

fn recorded_range_gaps(
    gaps: &[GapRow],
    request: &RangeRequest,
    anchor_block_number: u64,
    anchor_rfq_observed_at_ms: Option<u64>,
) -> Vec<RangeGap> {
    gaps.iter()
        .filter(|gap| {
            !gap_has_cursor_bounds(gap)
                || gap_overlaps_request(
                    gap,
                    request,
                    anchor_block_number,
                    anchor_rfq_observed_at_ms,
                )
        })
        .map(|gap| RangeGap {
            kind: RangeGapKind::Recorded,
            generation: Some(gap.generation),
            from_message_seq: Some(gap.from_message_seq),
            to_message_seq: Some(gap.to_message_seq),
            from_position: Some(StreamPosition {
                generation: gap.generation,
                message_seq: gap.from_message_seq,
            }),
            to_position: Some(StreamPosition {
                generation: gap.generation,
                message_seq: gap.to_message_seq,
            }),
            from_block: gap.from_block_number,
            to_block_inclusive: gap.to_block_number,
            from_observed_at_ms: gap.from_observed_at_ms,
            to_observed_at_ms: gap.to_observed_at_ms,
            reason: gap.reason.clone(),
        })
        .collect()
}

fn gap_overlaps_request(
    gap: &GapRow,
    request: &RangeRequest,
    anchor_block_number: u64,
    anchor_rfq_observed_at_ms: Option<u64>,
) -> bool {
    let block_overlap = request
        .backends
        .iter()
        .any(|backend| matches!(backend, Backend::Native | Backend::Vm))
        && ranges_overlap(
            gap.from_block_number,
            gap.to_block_number,
            anchor_block_number,
            request.end_block,
        );
    let rfq_overlap = anchor_rfq_observed_at_ms
        .zip(request.rfq_end_ms)
        .is_some_and(|(anchor_start, request_end)| {
            ranges_overlap(
                gap.from_observed_at_ms,
                gap.to_observed_at_ms,
                anchor_start,
                request_end,
            )
        });
    block_overlap || rfq_overlap
}

const fn gap_has_cursor_bounds(gap: &GapRow) -> bool {
    gap.from_block_number.is_some()
        || gap.to_block_number.is_some()
        || gap.from_observed_at_ms.is_some()
        || gap.to_observed_at_ms.is_some()
}

fn ranges_overlap(
    stored_start: Option<u64>,
    stored_end: Option<u64>,
    request_start: u64,
    request_end: u64,
) -> bool {
    stored_start
        .zip(stored_end)
        .is_some_and(|(stored_start, stored_end)| {
            stored_end >= request_start && stored_start <= request_end
        })
}

fn incomplete_tail_gaps(
    request: &RangeRequest,
    legs: &[RangeLeg],
    deltas: &[DeltaRow],
) -> Vec<RangeGap> {
    request
        .backends
        .iter()
        .filter_map(|backend| {
            let covered = covered_cursor(*backend, legs, deltas);
            let requested_end = match backend {
                Backend::Native | Backend::Vm => Some(request.end_block),
                Backend::Rfq => request.rfq_end_ms,
            }?;
            (covered.is_none_or(|head| head < requested_end)).then(|| RangeGap {
                kind: RangeGapKind::IncompleteTail,
                generation: None,
                from_message_seq: None,
                to_message_seq: None,
                from_position: None,
                to_position: None,
                from_block: matches!(backend, Backend::Native | Backend::Vm)
                    .then_some(covered.map_or(request.start_block, |head| head.saturating_add(1))),
                to_block_inclusive: matches!(backend, Backend::Native | Backend::Vm)
                    .then_some(request.end_block),
                from_observed_at_ms: (*backend == Backend::Rfq).then_some(
                    covered.map_or(request.rfq_start_ms.unwrap_or_default(), |head| {
                        head.saturating_add(1)
                    }),
                ),
                to_observed_at_ms: (*backend == Backend::Rfq).then_some(requested_end),
                reason: incomplete_tail_reason(*backend, covered, requested_end),
            })
        })
        .collect()
}

fn covered_cursor(backend: Backend, legs: &[RangeLeg], deltas: &[DeltaRow]) -> Option<u64> {
    let checkpoint_coverage = legs
        .iter()
        .filter(|leg| leg.checkpoint.backends.binary_search(&backend).is_ok())
        .filter_map(|leg| checkpoint_cursor(&leg.checkpoint, backend));
    let delta_coverage = deltas.iter().flat_map(|delta| {
        delta
            .backends
            .iter()
            .filter(move |cursor| cursor.backend == backend)
            .filter_map(move |cursor| delta_cursor(cursor, backend))
    });
    checkpoint_coverage.chain(delta_coverage).max()
}

const fn checkpoint_cursor(manifest: &CheckpointManifest, backend: Backend) -> Option<u64> {
    match backend {
        Backend::Native | Backend::Vm => Some(manifest.block_number),
        Backend::Rfq => manifest.rfq_observed_at_ms,
    }
}

const fn delta_cursor(cursor: &DeltaBackendCursor, backend: Backend) -> Option<u64> {
    match backend {
        Backend::Native | Backend::Vm => cursor.block_number,
        Backend::Rfq => cursor.observed_at_ms,
    }
}

fn incomplete_tail_reason(backend: Backend, covered: Option<u64>, requested_end: u64) -> String {
    let cursor_kind = match backend {
        Backend::Native | Backend::Vm => "block",
        Backend::Rfq => "timestamp",
    };
    match covered {
        Some(head) => format!(
            "{} coverage ends at {cursor_kind} {head} before requested end {cursor_kind} {requested_end}",
            backend.as_str()
        ),
        None => format!(
            "{} has no covered {cursor_kind} before requested end {cursor_kind} {requested_end}",
            backend.as_str()
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use anyhow::Context;
    use aws_sdk_s3::config::Region;
    use serde_json::json;
    use simulator_core::broadcaster::BroadcasterTokenDto;
    use sqlx::PgPool;

    use super::*;
    use crate::{
        encode_archive, encode_token_snapshot, token_snapshot_s3_key, ArchiveMetadata,
        CapturedState, CheckpointArchive, CheckpointKind, CheckpointStatus, DeltaEntry,
        EncodedArchiveInfo, GapReason, GapRecord, StateHistoryStore, TokenSnapshot,
        TokenSnapshotCodecError, TokenSnapshotRef, TOKEN_SNAPSHOT_SCHEMA_VERSION,
    };

    mod backtest {
        use super::*;

        #[test]
        #[expect(clippy::expect_used)]
        fn lineage_break_is_a_hard_error() {
            let rows = vec![
                block_time_row(100, "block-100", "block-99"),
                block_time_row(101, "block-101", "wrong-parent"),
            ];

            let error =
                validate_block_time_span(&rows, 100, 101).expect_err("broken lineage must fail");

            assert!(error.to_string().contains("lineage"));
            assert!(error.to_string().contains("100"));
            assert!(error.to_string().contains("101"));
        }

        #[test]
        #[expect(clippy::expect_used)]
        fn missing_end_plus_one_names_the_head_problem() {
            let error =
                require_block_time(None, RequiredBlockTime::EndPlusOne { block_number: 111 })
                    .expect_err("missing end plus one metadata must fail");

            assert!(error.to_string().contains("block 111"));
            assert!(error
                .to_string()
                .contains("ranges ending at the recorded head resolve after the next block lands"));
        }

        fn block_time_row(block_number: u64, block_hash: &str, parent_hash: &str) -> BlockTimeRow {
            BlockTimeRow {
                block_number,
                timestamp_ms: timestamp_ms(block_number),
                block_hash: Some(block_hash.to_owned()),
                parent_hash: Some(parent_hash.to_owned()),
            }
        }
    }

    #[test]
    fn single_segment_single_leg() {
        let (legs, gaps) = build_legs(
            manifest(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![delta(1, 1, 90), delta(1, 2, 110)],
            vec![],
            vec![],
            &request(100, 110),
        );

        assert_eq!(legs.len(), 1);
        assert_eq!(legs[0].deltas.len(), 2);
        assert_eq!(
            legs[0].deltas[0].applicable_backends[0].block_number,
            Some(90)
        );
        assert!(gaps.is_empty());
    }

    #[test]
    #[expect(clippy::expect_used)]
    fn stored_delta_keeps_raw_payload_and_lists_only_in_range_backends() {
        let request = RangeRequest::new(8453, 100, 100, vec![Backend::Native, Backend::Vm])
            .expect("test request should be valid");
        let mut row = delta(1, 1, 101);
        row.backends.push(DeltaBackendCursor {
            backend: Backend::Vm,
            block_number: Some(100),
            observed_at_ms: None,
        });

        let stored = stored_delta(&row, &request);

        assert_eq!(stored.position, position(1, 1));
        assert_eq!(stored.raw_payload.get(), row.payload.get());
        assert_eq!(
            stored.applicable_backends,
            vec![DeltaBackendCursor {
                backend: Backend::Vm,
                block_number: Some(100),
                observed_at_ms: None,
            }]
        );
    }

    #[test]
    fn promotion_with_complete_boundary_checkpoint_yields_two_legs() {
        let boundary = manifest(
            2,
            1,
            110,
            CheckpointKind::Boundary,
            CheckpointStatus::Complete,
        );
        let (legs, gaps) = build_legs(
            manifest(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![delta(1, 1, 105), delta(2, 1, 110), delta(2, 2, 120)],
            vec![ManifestRow {
                manifest: boundary.clone(),
            }],
            vec![],
            &request(100, 120),
        );

        assert_eq!(legs.len(), 2);
        assert_eq!(legs[0].deltas[0].position, position(1, 1));
        assert_eq!(legs[1].checkpoint, boundary);
        assert_eq!(legs[1].deltas[0].position, position(2, 2));
        assert!(gaps.is_empty());
    }

    #[test]
    fn promotion_checkpoint_position_precedes_first_delta() {
        let boundary = manifest(
            2,
            1,
            110,
            CheckpointKind::Boundary,
            CheckpointStatus::Complete,
        );
        let (legs, gaps) = build_legs(
            manifest(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![delta(1, 1, 105), delta(2, 2, 115), delta(2, 3, 120)],
            vec![ManifestRow {
                manifest: boundary.clone(),
            }],
            vec![],
            &request(100, 120),
        );
        let plan = RangePlan { legs, gaps };
        let generation_two_legs = plan
            .legs
            .iter()
            .filter(|leg| leg.checkpoint.position.generation == 2)
            .collect::<Vec<_>>();

        assert_eq!(plan.legs.len(), 2);
        assert_eq!(generation_two_legs.len(), 1);
        assert_eq!(generation_two_legs[0].checkpoint, boundary);
        assert_eq!(
            generation_two_legs[0]
                .deltas
                .iter()
                .map(|delta| delta.position)
                .collect::<Vec<_>>(),
            vec![position(2, 2), position(2, 3)]
        );
        assert!(plan
            .gaps
            .iter()
            .all(|gap| gap.kind != RangeGapKind::MissingCheckpoint));
        assert!(plan.ensure_gap_free().is_ok());
    }

    #[test]
    #[expect(clippy::expect_used)]
    fn boundary_checkpoint_after_first_generation_delta_reports_missing_head_span() {
        let boundary = manifest(
            2,
            2,
            115,
            CheckpointKind::Boundary,
            CheckpointStatus::Complete,
        );
        let (legs, gaps) = build_legs(
            manifest(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![delta(1, 1, 105), delta(2, 1, 110), delta(2, 3, 120)],
            vec![ManifestRow {
                manifest: boundary.clone(),
            }],
            vec![],
            &request(100, 120),
        );
        let plan = RangePlan { legs, gaps };

        assert_eq!(plan.legs.len(), 2);
        assert_eq!(plan.legs[1].checkpoint, boundary);
        assert_eq!(
            plan.legs[1]
                .deltas
                .iter()
                .map(|delta| delta.position)
                .collect::<Vec<_>>(),
            vec![position(2, 3)]
        );
        assert!(plan.legs.iter().all(|leg| {
            leg.deltas
                .iter()
                .all(|delta| delta.position != position(2, 1))
        }));
        assert_eq!(
            plan.gaps,
            vec![RangeGap {
                kind: RangeGapKind::MissingCheckpoint,
                generation: Some(2),
                from_message_seq: Some(1),
                to_message_seq: Some(1),
                from_position: Some(position(2, 1)),
                to_position: Some(position(2, 1)),
                from_block: None,
                to_block_inclusive: None,
                from_observed_at_ms: None,
                to_observed_at_ms: None,
                reason: "no complete boundary checkpoint at generation 2 message sequence 1"
                    .to_owned(),
            }]
        );
        plan.ensure_gap_free()
            .expect_err("slipped head delta must remain an explicit gap");
    }

    #[test]
    fn promotion_without_checkpoint_yields_missing_checkpoint_gap() {
        let (legs, gaps) = build_legs(
            manifest(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![delta(1, 1, 105), delta(2, 1, 110), delta(2, 2, 120)],
            vec![ManifestRow {
                manifest: manifest(
                    2,
                    1,
                    110,
                    CheckpointKind::Boundary,
                    CheckpointStatus::Failed,
                ),
            }],
            vec![GapRow {
                generation: 2,
                from_message_seq: 2,
                to_message_seq: 2,
                reason: "queue_overflow".to_owned(),
                from_block_number: None,
                to_block_number: None,
                from_observed_at_ms: None,
                to_observed_at_ms: None,
            }],
            &request(100, 120),
        );

        assert_eq!(legs.len(), 1);
        assert_eq!(gaps.len(), 2);
        assert_eq!(gaps[0].kind, RangeGapKind::MissingCheckpoint);
        assert_eq!(gaps[0].generation, Some(2));
        assert_eq!(gaps[0].from_message_seq, Some(1));
        assert_eq!(gaps[0].to_message_seq, Some(2));
        assert_eq!(gaps[1].kind, RangeGapKind::Recorded);
        assert_eq!(gaps[1].reason, "queue_overflow");
    }

    #[test]
    fn generation_jump_without_manifest_is_a_break() {
        let (legs, gaps) = build_legs(
            manifest(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![delta(1, 1, 105), delta(2, 1, 120)],
            vec![],
            vec![],
            &request(100, 120),
        );

        assert_eq!(legs.len(), 1);
        assert_eq!(legs[0].deltas.len(), 1);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].kind, RangeGapKind::MissingCheckpoint);
        assert_eq!(gaps[0].generation, Some(2));
    }

    #[test]
    fn failed_recovery_boundary_is_not_bridged() {
        let promotion = manifest(
            2,
            1,
            110,
            CheckpointKind::Boundary,
            CheckpointStatus::Complete,
        );
        let (legs, gaps) = build_legs(
            manifest(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![
                delta(1, 1, 105),
                delta(2, 1, 110),
                delta(2, 2, 115),
                delta(2, 400, 120),
                delta(2, 501, 125),
                delta(2, 600, 130),
            ],
            vec![
                ManifestRow {
                    manifest: promotion.clone(),
                },
                ManifestRow {
                    manifest: manifest(
                        2,
                        500,
                        120,
                        CheckpointKind::Boundary,
                        CheckpointStatus::Failed,
                    ),
                },
            ],
            vec![],
            &request(100, 130),
        );

        assert_eq!(legs.len(), 2);
        assert_eq!(legs[1].checkpoint, promotion);
        assert_eq!(
            legs[1]
                .deltas
                .iter()
                .map(|delta| delta.position)
                .collect::<Vec<_>>(),
            vec![position(2, 2), position(2, 400)]
        );
        assert_eq!(
            gaps,
            vec![RangeGap {
                kind: RangeGapKind::MissingCheckpoint,
                generation: Some(2),
                from_message_seq: Some(500),
                to_message_seq: Some(600),
                from_position: Some(position(2, 500)),
                to_position: Some(position(2, 600)),
                from_block: None,
                to_block_inclusive: None,
                from_observed_at_ms: None,
                to_observed_at_ms: None,
                reason: "no complete boundary checkpoint at generation 2 message sequence 500"
                    .to_owned(),
            }]
        );
        assert!(legs
            .iter()
            .flat_map(|leg| &leg.deltas)
            .all(|delta| delta.position < position(2, 500)));
    }

    #[test]
    #[expect(clippy::expect_used)]
    fn boundary_past_native_end_breaks_before_selected_rfq_delta() {
        let request = RangeRequest::new(8453, 100, 110, vec![Backend::Native, Backend::Rfq])
            .expect("test request should be valid")
            .with_rfq_bounds(1_000, 2_000)
            .expect("test RFQ bounds should be valid");
        let (legs, gaps) = build_legs(
            rfq_manifest(
                1,
                0,
                100,
                1_000,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![delta(1, 1, 110), rfq_delta(1, 3, 2_000)],
            vec![ManifestRow {
                manifest: rfq_manifest(
                    1,
                    2,
                    111,
                    1_500,
                    CheckpointKind::Boundary,
                    CheckpointStatus::Complete,
                ),
            }],
            vec![],
            &request,
        );
        let plan = RangePlan { legs, gaps };

        assert_eq!(plan.legs.len(), 1);
        assert_eq!(
            plan.legs[0]
                .deltas
                .iter()
                .map(|delta| delta.position)
                .collect::<Vec<_>>(),
            vec![position(1, 1)]
        );
        assert_eq!(
            plan.gaps,
            vec![RangeGap {
                kind: RangeGapKind::MissingCheckpoint,
                generation: Some(1),
                from_message_seq: Some(2),
                to_message_seq: Some(3),
                from_position: Some(position(1, 2)),
                to_position: Some(position(1, 3)),
                from_block: None,
                to_block_inclusive: None,
                from_observed_at_ms: None,
                to_observed_at_ms: None,
                reason: "no complete boundary checkpoint at generation 1 message sequence 2"
                    .to_owned(),
            }]
        );
        plan.ensure_gap_free()
            .expect_err("a boundary past the native end must fail closed");
    }

    #[test]
    #[expect(clippy::expect_used)]
    fn boundary_past_rfq_end_breaks_before_selected_native_delta() {
        let request = RangeRequest::new(8453, 100, 110, vec![Backend::Native, Backend::Rfq])
            .expect("test request should be valid")
            .with_rfq_bounds(1_000, 2_000)
            .expect("test RFQ bounds should be valid");
        let (legs, gaps) = build_legs(
            rfq_manifest(
                1,
                0,
                100,
                1_000,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![rfq_delta(1, 1, 2_000), delta(1, 3, 110)],
            vec![ManifestRow {
                manifest: rfq_manifest(
                    1,
                    2,
                    110,
                    2_500,
                    CheckpointKind::Boundary,
                    CheckpointStatus::Complete,
                ),
            }],
            vec![],
            &request,
        );
        let plan = RangePlan { legs, gaps };

        assert_eq!(plan.legs.len(), 1);
        assert_eq!(
            plan.legs[0]
                .deltas
                .iter()
                .map(|delta| delta.position)
                .collect::<Vec<_>>(),
            vec![position(1, 1)]
        );
        assert_eq!(plan.gaps.len(), 1);
        assert_eq!(plan.gaps[0].kind, RangeGapKind::MissingCheckpoint);
        assert_eq!(plan.gaps[0].from_message_seq, Some(2));
        assert_eq!(plan.gaps[0].to_message_seq, Some(3));
        plan.ensure_gap_free()
            .expect_err("a boundary past the RFQ end must fail closed");
    }

    #[test]
    #[expect(clippy::expect_used)]
    fn checkpoint_failure_position_breaks_rfq_replay_without_manifest() {
        let request = RangeRequest::new(8453, 100, 100, vec![Backend::Rfq])
            .expect("test request should be valid")
            .with_rfq_bounds(1_000, 2_000)
            .expect("test RFQ bounds should be valid");
        let (legs, gaps) = build_legs(
            rfq_manifest(
                1,
                0,
                100,
                1_000,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![rfq_delta(1, 2, 2_000)],
            vec![],
            vec![GapRow {
                generation: 1,
                from_message_seq: 1,
                to_message_seq: 1,
                reason: "checkpoint_failed".to_owned(),
                from_block_number: Some(105),
                to_block_number: Some(105),
                from_observed_at_ms: None,
                to_observed_at_ms: None,
            }],
            &request,
        );
        let plan = RangePlan { legs, gaps };

        assert_eq!(plan.legs.len(), 1);
        assert!(plan.legs[0].deltas.is_empty());
        assert_eq!(plan.gaps.len(), 1);
        assert_eq!(plan.gaps[0].kind, RangeGapKind::MissingCheckpoint);
        assert_eq!(plan.gaps[0].from_message_seq, Some(1));
        assert_eq!(plan.gaps[0].to_message_seq, Some(2));
        plan.ensure_gap_free()
            .expect_err("a positional checkpoint failure must fail RFQ replay");
    }

    #[test]
    #[expect(clippy::expect_used)]
    fn merged_checkpoint_failure_span_breaks_rfq_replay_without_manifest() {
        let request = RangeRequest::new(8453, 100, 100, vec![Backend::Rfq])
            .expect("test request should be valid")
            .with_rfq_bounds(1_000, 2_000)
            .expect("test RFQ bounds should be valid");
        let (legs, gaps) = build_legs(
            rfq_manifest(
                1,
                0,
                100,
                1_000,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![rfq_delta(1, 3, 2_000)],
            vec![],
            vec![GapRow {
                generation: 1,
                from_message_seq: 1,
                to_message_seq: 2,
                reason: "checkpoint_failed".to_owned(),
                from_block_number: Some(105),
                to_block_number: Some(106),
                from_observed_at_ms: None,
                to_observed_at_ms: None,
            }],
            &request,
        );
        let plan = RangePlan { legs, gaps };

        assert_eq!(plan.legs.len(), 1);
        assert!(plan.legs[0].deltas.is_empty());
        assert_eq!(plan.gaps.len(), 1);
        assert_eq!(plan.gaps[0].kind, RangeGapKind::MissingCheckpoint);
        assert_eq!(plan.gaps[0].from_message_seq, Some(1));
        assert_eq!(plan.gaps[0].to_message_seq, Some(3));
        plan.ensure_gap_free()
            .expect_err("a merged checkpoint failure span must fail RFQ replay");
    }

    #[test]
    #[expect(clippy::expect_used)]
    fn anchor_straddling_checkpoint_failure_span_breaks_at_first_post_anchor_position() {
        let request = RangeRequest::new(8453, 100, 100, vec![Backend::Rfq])
            .expect("test request should be valid")
            .with_rfq_bounds(1_000, 2_000)
            .expect("test RFQ bounds should be valid");
        let (legs, gaps) = build_legs(
            rfq_manifest(
                1,
                10,
                100,
                1_000,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![rfq_delta(1, 12, 2_000)],
            vec![],
            vec![GapRow {
                generation: 1,
                from_message_seq: 9,
                to_message_seq: 11,
                reason: "checkpoint_failed".to_owned(),
                from_block_number: Some(99),
                to_block_number: Some(101),
                from_observed_at_ms: None,
                to_observed_at_ms: None,
            }],
            &request,
        );
        let plan = RangePlan { legs, gaps };

        assert_eq!(plan.legs.len(), 1);
        assert!(plan.legs[0].deltas.is_empty());
        assert_eq!(plan.gaps.len(), 1);
        assert_eq!(plan.gaps[0].kind, RangeGapKind::MissingCheckpoint);
        assert_eq!(plan.gaps[0].from_message_seq, Some(11));
        assert_eq!(plan.gaps[0].to_message_seq, Some(12));
        plan.ensure_gap_free()
            .expect_err("a failure span straddling the anchor must fail closed");
    }

    #[test]
    fn pre_anchor_checkpoint_failure_span_does_not_create_a_break() {
        let request = RangeRequest::new(8453, 100, 100, vec![Backend::Rfq])
            .unwrap_or_else(|_| unreachable!("test request should be valid"))
            .with_rfq_bounds(1_000, 2_000)
            .unwrap_or_else(|_| unreachable!("test RFQ bounds should be valid"));
        let (legs, gaps) = build_legs(
            rfq_manifest(
                1,
                10,
                100,
                1_000,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![rfq_delta(1, 12, 2_000)],
            vec![],
            vec![GapRow {
                generation: 1,
                from_message_seq: 5,
                to_message_seq: 9,
                reason: "checkpoint_failed".to_owned(),
                from_block_number: Some(90),
                to_block_number: Some(95),
                from_observed_at_ms: None,
                to_observed_at_ms: None,
            }],
            &request,
        );
        let plan = RangePlan { legs, gaps };

        assert_eq!(plan.legs.len(), 1);
        assert_eq!(plan.legs[0].deltas.len(), 1);
        assert!(plan.gaps.is_empty());
        assert!(plan.ensure_gap_free().is_ok());
    }

    #[test]
    fn checkpoint_failure_past_last_delta_does_not_create_a_break() {
        let request = RangeRequest::new(8453, 100, 100, vec![Backend::Rfq])
            .unwrap_or_else(|_| unreachable!("test request should be valid"))
            .with_rfq_bounds(1_000, 2_000)
            .unwrap_or_else(|_| unreachable!("test RFQ bounds should be valid"));
        let (legs, gaps) = build_legs(
            rfq_manifest(
                1,
                0,
                100,
                1_000,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![rfq_delta(1, 1, 2_000)],
            vec![],
            vec![GapRow {
                generation: 1,
                from_message_seq: 2,
                to_message_seq: 2,
                reason: "checkpoint_failed".to_owned(),
                from_block_number: Some(105),
                to_block_number: Some(105),
                from_observed_at_ms: None,
                to_observed_at_ms: None,
            }],
            &request,
        );
        let plan = RangePlan { legs, gaps };

        assert_eq!(plan.legs.len(), 1);
        assert_eq!(plan.legs[0].deltas.len(), 1);
        assert!(plan.gaps.is_empty());
        assert!(plan.ensure_gap_free().is_ok());
    }

    #[test]
    fn fitting_boundary_anchors_post_recovery_rfq_delta() {
        let request = RangeRequest::new(8453, 100, 111, vec![Backend::Native, Backend::Rfq])
            .unwrap_or_else(|_| unreachable!("test request should be valid"))
            .with_rfq_bounds(1_000, 2_000)
            .unwrap_or_else(|_| unreachable!("test RFQ bounds should be valid"));
        let boundary = rfq_manifest(
            1,
            2,
            111,
            1_500,
            CheckpointKind::Boundary,
            CheckpointStatus::Complete,
        );
        let (legs, gaps) = build_legs(
            rfq_manifest(
                1,
                0,
                100,
                1_000,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![delta(1, 1, 110), rfq_delta(1, 3, 2_000)],
            vec![ManifestRow {
                manifest: boundary.clone(),
            }],
            vec![],
            &request,
        );
        let plan = RangePlan { legs, gaps };

        assert_eq!(plan.legs.len(), 2);
        assert_eq!(plan.legs[0].deltas[0].position, position(1, 1));
        assert_eq!(plan.legs[1].checkpoint, boundary);
        assert_eq!(plan.legs[1].deltas[0].position, position(1, 3));
        assert!(plan.gaps.is_empty());
        assert!(plan.ensure_gap_free().is_ok());
    }

    #[test]
    fn interval_checkpoints_do_not_split_legs() {
        let (legs, gaps) = build_legs(
            manifest(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![delta(1, 1, 105), delta(1, 2, 110)],
            vec![ManifestRow {
                manifest: manifest(
                    1,
                    1,
                    105,
                    CheckpointKind::Interval,
                    CheckpointStatus::Complete,
                ),
            }],
            vec![],
            &request(100, 110),
        );

        assert_eq!(legs.len(), 1);
        assert_eq!(legs[0].deltas.len(), 2);
        assert!(gaps.is_empty());
    }

    #[test]
    fn recorded_gap_surfaces_with_reason() {
        let (legs, gaps) = build_legs(
            manifest(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![delta(1, 3, 110)],
            vec![],
            vec![GapRow {
                generation: 1,
                from_message_seq: 2,
                to_message_seq: 2,
                reason: "write_failed".to_owned(),
                from_block_number: None,
                to_block_number: None,
                from_observed_at_ms: None,
                to_observed_at_ms: None,
            }],
            &request(100, 110),
        );

        assert_eq!(legs.len(), 1);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].kind, RangeGapKind::Recorded);
        assert_eq!(gaps[0].reason, "write_failed");
    }

    #[test]
    #[expect(clippy::expect_used)]
    fn bounded_gap_between_anchor_and_request_start_fails_the_range() {
        let (legs, gaps) = build_legs(
            manifest(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![delta(1, 2, 106), delta(1, 3, 120)],
            vec![],
            vec![GapRow {
                generation: 1,
                from_message_seq: 1,
                to_message_seq: 1,
                reason: "checkpoint_failed".to_owned(),
                from_block_number: Some(105),
                to_block_number: Some(105),
                from_observed_at_ms: None,
                to_observed_at_ms: None,
            }],
            &request(110, 120),
        );
        let plan = RangePlan { legs, gaps };

        assert_eq!(plan.gaps.len(), 1);
        assert_eq!(plan.gaps[0].kind, RangeGapKind::Recorded);
        assert_eq!(plan.gaps[0].reason, "checkpoint_failed");
        plan.ensure_gap_free()
            .expect_err("loss after the anchor must fail a later requested range");
    }

    #[test]
    fn bounded_gap_before_anchor_cursor_stays_excluded() {
        let (legs, gaps) = build_legs(
            manifest(
                1,
                10,
                100,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![delta(1, 11, 110), delta(1, 12, 120)],
            vec![],
            vec![GapRow {
                generation: 1,
                from_message_seq: 9,
                to_message_seq: 9,
                reason: "checkpoint_failed".to_owned(),
                from_block_number: Some(95),
                to_block_number: Some(95),
                from_observed_at_ms: None,
                to_observed_at_ms: None,
            }],
            &request(110, 120),
        );
        let plan = RangePlan { legs, gaps };

        assert!(plan.gaps.is_empty());
        assert!(plan.ensure_gap_free().is_ok());
    }

    #[test]
    #[expect(clippy::expect_used)]
    fn boundary_missing_requested_backend_does_not_anchor_a_leg() {
        let mut anchor = manifest(
            1,
            0,
            100,
            CheckpointKind::Boundary,
            CheckpointStatus::Complete,
        );
        anchor.backends = vec![Backend::Native, Backend::Rfq];
        anchor.backends.sort_unstable();
        anchor.rfq_observed_at_ms = Some(timestamp_ms(100));
        let boundary = manifest(
            1,
            1,
            110,
            CheckpointKind::Boundary,
            CheckpointStatus::Complete,
        );
        let request = RangeRequest::new(8453, 100, 120, vec![Backend::Native, Backend::Rfq])
            .expect("test request should be valid")
            .with_rfq_bounds(timestamp_ms(100), timestamp_ms(120))
            .expect("test RFQ bounds should be valid");
        let (legs, gaps) = build_legs(
            anchor,
            vec![delta(1, 2, 120)],
            vec![ManifestRow { manifest: boundary }],
            vec![],
            &request,
        );
        let plan = RangePlan { legs, gaps };

        assert_eq!(plan.legs.len(), 1);
        assert!(plan
            .gaps
            .iter()
            .any(|gap| gap.kind == RangeGapKind::MissingCheckpoint));
        plan.ensure_gap_free()
            .expect_err("a boundary missing RFQ coverage must fail closed");
    }

    #[test]
    #[expect(clippy::expect_used)]
    fn boundary_slip_inside_previous_leg_fails_the_range() {
        let boundary = manifest(
            1,
            4,
            110,
            CheckpointKind::Boundary,
            CheckpointStatus::Complete,
        );
        let (legs, gaps) = build_legs(
            manifest(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![delta(1, 1, 105), delta(1, 2, 110), delta(1, 3, 110)],
            vec![ManifestRow { manifest: boundary }],
            vec![GapRow {
                generation: 1,
                from_message_seq: 2,
                to_message_seq: 3,
                reason: "boundary_slip".to_owned(),
                from_block_number: Some(110),
                to_block_number: Some(110),
                from_observed_at_ms: None,
                to_observed_at_ms: None,
            }],
            &request(100, 110),
        );
        let plan = RangePlan { legs, gaps };

        assert_eq!(plan.legs.len(), 2);
        assert_eq!(
            plan.legs[0]
                .deltas
                .iter()
                .map(|delta| delta.position)
                .collect::<Vec<_>>(),
            vec![position(1, 1), position(1, 2), position(1, 3)]
        );
        assert_eq!(plan.gaps.len(), 1);
        assert_eq!(plan.gaps[0].kind, RangeGapKind::Recorded);
        assert_eq!(plan.gaps[0].generation, Some(1));
        assert_eq!(plan.gaps[0].from_message_seq, Some(2));
        assert_eq!(plan.gaps[0].to_message_seq, Some(3));
        assert_eq!(plan.gaps[0].reason, "boundary_slip");
        plan.ensure_gap_free()
            .expect_err("a boundary slip must fail the range");
    }

    #[test]
    #[expect(clippy::expect_used)]
    fn recorded_gap_without_surviving_deltas_fails_checkpoint_covered_range() {
        let (legs, gaps) = build_legs(
            manifest(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![],
            vec![],
            vec![GapRow {
                generation: 1,
                from_message_seq: 1,
                to_message_seq: 3,
                reason: "queue_overflow".to_owned(),
                from_block_number: Some(100),
                to_block_number: Some(100),
                from_observed_at_ms: Some(100_000),
                to_observed_at_ms: Some(100_000),
            }],
            &request(100, 100),
        );
        let plan = RangePlan { legs, gaps };

        assert_eq!(plan.legs.len(), 1);
        assert_eq!(plan.gaps.len(), 1);
        assert_eq!(plan.gaps[0].kind, RangeGapKind::Recorded);
        assert_eq!(plan.gaps[0].reason, "queue_overflow");
        plan.ensure_gap_free()
            .expect_err("recorded same-block loss must fail the range");
    }

    #[test]
    #[expect(clippy::expect_used)]
    fn fully_dropped_generation_gap_surfaces_between_replay_legs() {
        let generation_three_boundary = manifest(
            3,
            1,
            115,
            CheckpointKind::Boundary,
            CheckpointStatus::Complete,
        );
        let (legs, gaps) = build_legs(
            manifest(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![delta(1, 1, 105), delta(3, 2, 120)],
            vec![ManifestRow {
                manifest: generation_three_boundary.clone(),
            }],
            vec![GapRow {
                generation: 2,
                from_message_seq: 1,
                to_message_seq: 9,
                reason: "write_failed".to_owned(),
                from_block_number: Some(110),
                to_block_number: Some(110),
                from_observed_at_ms: Some(110_000),
                to_observed_at_ms: Some(110_000),
            }],
            &request(100, 120),
        );
        let plan = RangePlan { legs, gaps };

        assert_eq!(plan.legs.len(), 2);
        assert_eq!(plan.legs[1].checkpoint, generation_three_boundary);
        assert_eq!(plan.gaps.len(), 1);
        assert_eq!(plan.gaps[0].kind, RangeGapKind::Recorded);
        assert_eq!(plan.gaps[0].generation, Some(2));
        assert_eq!(plan.gaps[0].reason, "write_failed");
        plan.ensure_gap_free()
            .expect_err("fully dropped generation must fail the bridged range");
    }

    #[test]
    fn tail_short_of_end_block_reports_incomplete_tail() {
        let (legs, gaps) = build_legs(
            manifest(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![delta(1, 1, 110)],
            vec![],
            vec![],
            &request(100, 120),
        );

        assert_eq!(legs.len(), 1);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].kind, RangeGapKind::IncompleteTail);
        assert!(gaps[0].reason.contains("native"));
        assert!(gaps[0].reason.contains("110"));
    }

    #[test]
    fn trailing_complete_boundary_covers_request_without_later_deltas() {
        let boundary = manifest(
            1,
            2,
            120,
            CheckpointKind::Boundary,
            CheckpointStatus::Complete,
        );
        let (legs, gaps) = build_legs(
            manifest(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![delta(1, 1, 110)],
            vec![ManifestRow {
                manifest: boundary.clone(),
            }],
            vec![],
            &request(100, 120),
        );
        let plan = RangePlan { legs, gaps };

        assert_eq!(plan.legs.len(), 2);
        assert_eq!(plan.legs[1].checkpoint, boundary);
        assert!(plan.legs[1].deltas.is_empty());
        assert!(plan.gaps.is_empty());
        assert!(plan.ensure_gap_free().is_ok());
    }

    #[test]
    #[expect(clippy::expect_used)]
    fn trailing_failed_boundary_reports_single_position_checkpoint_gap() {
        let (legs, gaps) = build_legs(
            manifest(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                CheckpointStatus::Complete,
            ),
            vec![delta(1, 1, 110)],
            vec![ManifestRow {
                manifest: manifest(
                    1,
                    2,
                    120,
                    CheckpointKind::Boundary,
                    CheckpointStatus::Failed,
                ),
            }],
            vec![],
            &request(100, 120),
        );
        let plan = RangePlan { legs, gaps };

        assert_eq!(plan.legs.len(), 1);
        assert_eq!(
            plan.gaps[0],
            RangeGap {
                kind: RangeGapKind::MissingCheckpoint,
                generation: Some(1),
                from_message_seq: Some(2),
                to_message_seq: Some(2),
                from_position: Some(position(1, 2)),
                to_position: Some(position(1, 2)),
                from_block: None,
                to_block_inclusive: None,
                from_observed_at_ms: None,
                to_observed_at_ms: None,
                reason: "no complete boundary checkpoint at generation 1 message sequence 2"
                    .to_owned(),
            }
        );
        plan.ensure_gap_free()
            .expect_err("failed boundary must leave an explicit gap");
    }

    #[test]
    #[expect(clippy::expect_used)]
    fn ensure_gap_free_preserves_kinds() {
        let plan = RangePlan {
            legs: vec![],
            gaps: vec![
                RangeGap {
                    kind: RangeGapKind::MissingCheckpoint,
                    generation: Some(2),
                    from_message_seq: Some(1),
                    to_message_seq: Some(2),
                    from_position: Some(position(2, 1)),
                    to_position: Some(position(2, 2)),
                    from_block: None,
                    to_block_inclusive: None,
                    from_observed_at_ms: None,
                    to_observed_at_ms: None,
                    reason: "missing boundary".to_owned(),
                },
                RangeGap {
                    kind: RangeGapKind::Recorded,
                    generation: Some(2),
                    from_message_seq: Some(2),
                    to_message_seq: Some(2),
                    from_position: Some(position(2, 2)),
                    to_position: Some(position(2, 2)),
                    from_block: None,
                    to_block_inclusive: None,
                    from_observed_at_ms: None,
                    to_observed_at_ms: None,
                    reason: "write_failed".to_owned(),
                },
            ],
        };

        let error = plan
            .ensure_gap_free()
            .expect_err("plan with gaps must fail");
        assert_eq!(
            error.gaps,
            vec![
                (
                    RangeGapKind::MissingCheckpoint,
                    "missing boundary".to_owned()
                ),
                (RangeGapKind::Recorded, "write_failed".to_owned()),
            ]
        );
        assert_eq!(
            error.to_string(),
            "state history range has gaps: missing_checkpoint: missing boundary, recorded: write_failed"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn resolved_plan_uses_same_segment_token_fallback(pool: PgPool) -> anyhow::Result<()> {
        configure_test_aws();
        let token_reference = TokenSnapshotRef {
            s3_key: "reader/chain=8453/tokens/token-sha256.zst".to_owned(),
            sha256: "token-sha256".to_owned(),
            token_count: 2,
            token_bytes: 512,
        };
        let referenced_capture = capture(
            1,
            1,
            100,
            CheckpointKind::Boundary,
            r#"{"state":"referenced"}"#,
        )?;
        let mut connection = pool.acquire().await?;
        let referenced_id =
            StateHistoryStore::create_checkpoint(&mut connection, &referenced_capture).await?;
        StateHistoryStore::complete_checkpoint(
            &mut connection,
            referenced_id,
            "checkpoints/referenced.zst",
            &test_archive_info(),
            Some(&token_reference),
            referenced_capture.block_times(),
            referenced_capture.position(),
            referenced_capture.chain_id(),
        )
        .await?;
        drop(connection);

        let reader = StateHistoryReader::new(
            StateHistoryStore::from_pool(pool.clone()),
            test_object_store("state-history-optional-reference-reader-test").await,
        );
        let referenced_plan = reader
            .resolve_range(&RangeRequest::new(8453, 100, 100, vec![Backend::Native])?)
            .await?;

        assert_eq!(referenced_plan.legs.len(), 1);
        assert_eq!(
            referenced_plan.legs[0].checkpoint.token_reference,
            Some(token_reference.clone())
        );
        assert!(referenced_plan.gaps.is_empty());
        referenced_plan.ensure_gap_free()?;

        let unreferenced_capture = capture(
            1,
            2,
            101,
            CheckpointKind::Interval,
            r#"{"state":"unreferenced"}"#,
        )?;
        let mut connection = pool.acquire().await?;
        let unreferenced_id =
            StateHistoryStore::create_checkpoint(&mut connection, &unreferenced_capture).await?;
        StateHistoryStore::complete_checkpoint(
            &mut connection,
            unreferenced_id,
            "checkpoints/unreferenced.zst",
            &test_archive_info(),
            None,
            unreferenced_capture.block_times(),
            unreferenced_capture.position(),
            unreferenced_capture.chain_id(),
        )
        .await?;
        drop(connection);
        persist_delta(&pool, database_delta(1, 2, 101, false)?).await?;

        let unreferenced_plan = reader
            .resolve_range(&RangeRequest::new(8453, 101, 101, vec![Backend::Native])?)
            .await?;

        assert_eq!(unreferenced_plan.legs.len(), 1);
        assert_eq!(
            unreferenced_plan.legs[0].checkpoint.position,
            referenced_capture.position()
        );
        assert_eq!(
            unreferenced_plan.legs[0].checkpoint.token_reference,
            Some(token_reference)
        );
        assert!(unreferenced_plan.gaps.is_empty());
        unreferenced_plan.ensure_gap_free()?;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn fetch_token_snapshot_round_trips_raw_tokens(pool: PgPool) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-token-fetch-reader-test";

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let objects = test_object_store(BUCKET).await;
        let encoded = encode_token_snapshot(test_token_snapshot()?)?;
        let key = token_snapshot_s3_key("reader", 8453, &encoded.info.sha256);
        objects.put(&key, encoded.bytes).await?;
        let reference = TokenSnapshotRef {
            s3_key: key,
            sha256: encoded.info.sha256,
            token_count: encoded.info.token_count,
            token_bytes: encoded.info.token_bytes,
        };
        let reader = StateHistoryReader::new(StateHistoryStore::from_pool(pool), objects);

        let snapshot = reader.fetch_token_snapshot(&reference).await?;
        let tokens: serde_json::Value = serde_json::from_str(snapshot.tokens.get())?;

        assert_eq!(snapshot.schema_version, TOKEN_SNAPSHOT_SCHEMA_VERSION);
        assert_eq!(snapshot.chain_id, 8453);
        assert_eq!(snapshot.token_count, 2);
        assert_eq!(tokens, expected_test_tokens());
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn fetch_token_snapshot_rejects_corrupt_object(pool: PgPool) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-token-corrupt-reader-test";

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let objects = test_object_store(BUCKET).await;
        let key = "reader/chain=8453/tokens/corrupt.zst".to_owned();
        objects.put(&key, b"not a zstd object".to_vec()).await?;
        let reference = TokenSnapshotRef {
            s3_key: key,
            sha256: "corrupt".to_owned(),
            token_count: 2,
            token_bytes: 512,
        };
        let reader = StateHistoryReader::new(StateHistoryStore::from_pool(pool), objects);

        let error = reader
            .fetch_token_snapshot(&reference)
            .await
            .err()
            .context("corrupt token snapshot must fail")?;

        assert!(error
            .to_string()
            .contains("failed to decompress token snapshot"));
        assert!(error.downcast_ref::<std::io::Error>().is_some());
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn fetch_token_snapshot_rejects_mismatched_sha256(pool: PgPool) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-token-hash-reader-test";

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let objects = test_object_store(BUCKET).await;
        let encoded = encode_token_snapshot(test_token_snapshot()?)?;
        let actual_sha256 = encoded.info.sha256.clone();
        let key = token_snapshot_s3_key("reader", 8453, &actual_sha256);
        objects.put(&key, encoded.bytes).await?;
        let reference = TokenSnapshotRef {
            s3_key: key,
            sha256: "deadbeef".to_owned(),
            token_count: encoded.info.token_count,
            token_bytes: encoded.info.token_bytes,
        };
        let reader = StateHistoryReader::new(StateHistoryStore::from_pool(pool), objects);

        let error = reader
            .fetch_token_snapshot(&reference)
            .await
            .err()
            .context("mismatched token snapshot sha256 must fail")?;

        assert_eq!(
            error.downcast_ref::<TokenSnapshotCodecError>(),
            Some(&TokenSnapshotCodecError::Sha256Mismatch {
                expected: "deadbeef".to_owned(),
                actual: actual_sha256,
            })
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn fetch_token_snapshot_rejects_missing_object(pool: PgPool) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-token-missing-reader-test";

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let objects = test_object_store(BUCKET).await;
        let key = "reader/chain=8453/tokens/missing.zst".to_owned();
        let reference = TokenSnapshotRef {
            s3_key: key.clone(),
            sha256: "missing".to_owned(),
            token_count: 2,
            token_bytes: 512,
        };
        let reader = StateHistoryReader::new(StateHistoryStore::from_pool(pool), objects);

        let error = reader
            .fetch_token_snapshot(&reference)
            .await
            .err()
            .context("missing token snapshot must fail")?;

        assert!(error
            .to_string()
            .contains(&format!("failed to fetch token snapshot {key}")));
        assert!(format!("{error:#}").contains(&format!("s3://{BUCKET}/{key}")));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn completed_checkpoint_token_reference_round_trips_through_manifest_reads(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        let capture = capture(
            1,
            1,
            100,
            CheckpointKind::Interval,
            r#"{"state":"token-reference"}"#,
        )?;
        let mut connection = pool.acquire().await?;
        let id = StateHistoryStore::create_checkpoint(&mut connection, &capture).await?;
        let archive = test_archive_info();
        let token_reference = TokenSnapshotRef {
            s3_key: "state-history/chain=8453/tokens/token-sha256.zst".to_owned(),
            sha256: "token-sha256".to_owned(),
            token_count: 42,
            token_bytes: 4_096,
        };
        StateHistoryStore::complete_checkpoint(
            &mut connection,
            id,
            "checkpoints/token-reference.zst",
            &archive,
            Some(&token_reference),
            capture.block_times(),
            capture.position(),
            capture.chain_id(),
        )
        .await?;
        drop(connection);

        let stored = sqlx::query_as::<_, (String, String, i64, i64)>(
            "SELECT token_s3_key, token_sha256, token_count, token_bytes
             FROM state_history.checkpoints
             WHERE id = $1",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            stored,
            (
                token_reference.s3_key.clone(),
                token_reference.sha256.clone(),
                42,
                4_096,
            )
        );

        let manifest = read_checkpoint_manifest(&pool, id).await?;
        assert_eq!(manifest.token_reference, Some(token_reference));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn completing_checkpoint_without_token_reference_preserves_null_columns(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        let capture = capture(
            1,
            2,
            101,
            CheckpointKind::Interval,
            r#"{"state":"no-token-reference"}"#,
        )?;
        let mut connection = pool.acquire().await?;
        let id = StateHistoryStore::create_checkpoint(&mut connection, &capture).await?;
        StateHistoryStore::complete_checkpoint(
            &mut connection,
            id,
            "checkpoints/no-token-reference.zst",
            &test_archive_info(),
            None,
            capture.block_times(),
            capture.position(),
            capture.chain_id(),
        )
        .await?;
        drop(connection);

        let stored =
            sqlx::query_as::<_, (Option<String>, Option<String>, Option<i64>, Option<i64>)>(
                "SELECT token_s3_key, token_sha256, token_count, token_bytes
             FROM state_history.checkpoints
             WHERE id = $1",
            )
            .bind(id)
            .fetch_one(&pool)
            .await?;
        assert_eq!(stored, (None, None, None, None));
        assert_eq!(
            read_checkpoint_manifest(&pool, id).await?.token_reference,
            None
        );
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn writing_and_failed_manifests_have_no_token_reference(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        let writing_capture = capture(
            1,
            3,
            102,
            CheckpointKind::Interval,
            r#"{"state":"writing"}"#,
        )?;
        let failed_capture = capture(1, 4, 103, CheckpointKind::Interval, r#"{"state":"failed"}"#)?;
        let mut connection = pool.acquire().await?;
        let writing_id =
            StateHistoryStore::create_checkpoint(&mut connection, &writing_capture).await?;
        let failed_id =
            StateHistoryStore::create_checkpoint(&mut connection, &failed_capture).await?;
        let token_reference = TokenSnapshotRef {
            s3_key: "state-history/chain=8453/tokens/failed-sha256.zst".to_owned(),
            sha256: "failed-sha256".to_owned(),
            token_count: 1,
            token_bytes: 128,
        };
        StateHistoryStore::complete_checkpoint(
            &mut connection,
            failed_id,
            "checkpoints/failed.zst",
            &test_archive_info(),
            Some(&token_reference),
            failed_capture.block_times(),
            failed_capture.position(),
            failed_capture.chain_id(),
        )
        .await?;
        drop(connection);
        StateHistoryStore::fail_checkpoint(&pool, failed_id, "archive upload failed").await?;

        let writing = read_checkpoint_manifest(&pool, writing_id).await?;
        assert_eq!(writing.status, CheckpointStatus::Writing);
        assert_eq!(writing.token_reference, None);

        let failed = read_checkpoint_manifest(&pool, failed_id).await?;
        assert_eq!(failed.status, CheckpointStatus::Failed);
        assert_eq!(failed.token_reference, None);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn checkpoint_kind_separates_objects_in_both_insertion_orders(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-checkpoint-kind-reader-test";

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let writer_objects = test_object_store(BUCKET).await;
        let reader = StateHistoryReader::new(
            StateHistoryStore::from_pool(pool.clone()),
            test_object_store(BUCKET).await,
        );

        for (order_index, kinds) in [
            [CheckpointKind::Boundary, CheckpointKind::Interval],
            [CheckpointKind::Interval, CheckpointKind::Boundary],
        ]
        .into_iter()
        .enumerate()
        {
            let message_seq = 20 + u64::try_from(order_index)?;
            let mut persisted = Vec::new();

            for (capture_index, kind) in kinds.into_iter().enumerate() {
                let payload = format!(r#"{{"order":{order_index},"capture":{capture_index}}}"#);
                let manifest = persist_checkpoint(
                    &pool,
                    &writer_objects,
                    capture(4, message_seq, 100, kind, &payload)?,
                )
                .await?;
                persisted.push((manifest, payload));
            }

            let first_key = persisted[0]
                .0
                .s3_key
                .as_deref()
                .context("first checkpoint is missing its object key")?;
            let second_key = persisted[1]
                .0
                .s3_key
                .as_deref()
                .context("second checkpoint is missing its object key")?;
            assert_ne!(first_key, second_key);

            for (manifest, expected_payload) in persisted {
                let archive = reader.fetch_checkpoint(&manifest).await?;
                assert_eq!(archive.metadata.kind, manifest.kind);
                assert_eq!(archive.payloads_json, vec![expected_payload]);
            }
        }

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn checkpoint_fetch_rejects_manifest_drift(pool: PgPool) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-checkpoint-manifest-reader-test";

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let objects = test_object_store(BUCKET).await;
        let manifest = persist_checkpoint(
            &pool,
            &objects,
            capture(3, 7, 123, CheckpointKind::Boundary, r#"{"state":"bound"}"#)?,
        )
        .await?;
        let reader = StateHistoryReader::new(StateHistoryStore::from_pool(pool), objects);

        let mut wrong_key = manifest.clone();
        wrong_key.s3_key = Some("wrong/checkpoint.zst".to_owned());
        assert!(reader
            .fetch_checkpoint(&wrong_key)
            .await
            .is_err_and(|error| error.to_string().contains("object key mismatch")));

        let mut wrong_archive_length = manifest.clone();
        wrong_archive_length.archive_bytes =
            wrong_archive_length.archive_bytes.map(|value| value + 1);
        assert!(reader
            .fetch_checkpoint(&wrong_archive_length)
            .await
            .is_err_and(|error| error.to_string().contains("archive length mismatch")));

        let mut wrong_compressed_length = manifest.clone();
        wrong_compressed_length.compressed_bytes = wrong_compressed_length
            .compressed_bytes
            .map(|value| value + 1);
        assert!(reader
            .fetch_checkpoint(&wrong_compressed_length)
            .await
            .is_err_and(|error| error.to_string().contains("compressed length mismatch")));

        let mut wrong_metadata = manifest;
        wrong_metadata.block_number += 1;
        assert!(reader
            .fetch_checkpoint(&wrong_metadata)
            .await
            .is_err_and(|error| error.to_string().contains("metadata does not match")));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn checkpoint_token_fetch_binds_manifest_and_reference(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-token-manifest-reader-test";

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let objects = test_object_store(BUCKET).await;
        let encoded = encode_token_snapshot(test_token_snapshot()?)?;
        let key = token_snapshot_s3_key("reader", 8453, &encoded.info.sha256);
        objects.put(&key, encoded.bytes).await?;
        let reference = TokenSnapshotRef {
            s3_key: key,
            sha256: encoded.info.sha256,
            token_count: encoded.info.token_count,
            token_bytes: encoded.info.token_bytes,
        };
        let mut checkpoint = manifest(
            1,
            1,
            100,
            CheckpointKind::Boundary,
            CheckpointStatus::Complete,
        );
        checkpoint.token_reference = Some(reference.clone());
        let reader = StateHistoryReader::new(StateHistoryStore::from_pool(pool), objects);

        assert!(reader
            .fetch_checkpoint_token_snapshot(&checkpoint)
            .await?
            .is_some());

        let mut missing = checkpoint.clone();
        missing.token_reference = None;
        assert!(reader
            .fetch_checkpoint_token_snapshot(&missing)
            .await?
            .is_none());

        let mut wrong_count = checkpoint.clone();
        wrong_count
            .token_reference
            .as_mut()
            .context("test token reference is missing")?
            .token_count += 1;
        assert!(reader
            .fetch_checkpoint_token_snapshot(&wrong_count)
            .await
            .is_err_and(|error| error.to_string().contains("count mismatch")));

        let mut wrong_length = checkpoint.clone();
        wrong_length
            .token_reference
            .as_mut()
            .context("test token reference is missing")?
            .token_bytes += 1;
        assert!(reader
            .fetch_checkpoint_token_snapshot(&wrong_length)
            .await
            .is_err_and(|error| error.to_string().contains("byte length mismatch")));

        let mut wrong_chain = checkpoint;
        wrong_chain.chain_id = 1;
        assert!(reader
            .fetch_checkpoint_token_snapshot(&wrong_chain)
            .await
            .is_err_and(|error| error.to_string().contains("token object key mismatch")));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn token_fetch_rejects_noncanonical_catalogs(pool: PgPool) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-token-canonical-reader-test";

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let objects = test_object_store(BUCKET).await;
        let reader = StateHistoryReader::new(
            StateHistoryStore::from_pool(pool),
            test_object_store(BUCKET).await,
        );
        let canonical_tokens = expected_test_tokens();

        let envelope_count = put_raw_token_snapshot(
            &objects,
            json!({
                "schema_version": TOKEN_SNAPSHOT_SCHEMA_VERSION,
                "chain_id": 8453,
                "token_count": 3,
                "tokens": canonical_tokens,
            }),
            3,
        )
        .await?;
        assert!(reader
            .fetch_token_snapshot(&envelope_count)
            .await
            .is_err_and(|error| error.to_string().contains("envelope count mismatch")));

        let mut reversed = expected_test_tokens()
            .as_array()
            .context("test tokens are not an array")?
            .clone();
        reversed.reverse();
        let reversed = put_raw_token_snapshot(
            &objects,
            json!({
                "schema_version": TOKEN_SNAPSHOT_SCHEMA_VERSION,
                "chain_id": 8453,
                "token_count": 2,
                "tokens": reversed,
            }),
            2,
        )
        .await?;
        assert!(reader
            .fetch_token_snapshot(&reversed)
            .await
            .is_err_and(|error| error.to_string().contains("strictly sorted and unique")));

        let mut wrong_chain_tokens = expected_test_tokens();
        wrong_chain_tokens[0]["chainId"] = json!(1);
        let wrong_chain = put_raw_token_snapshot(
            &objects,
            json!({
                "schema_version": TOKEN_SNAPSHOT_SCHEMA_VERSION,
                "chain_id": 8453,
                "token_count": 2,
                "tokens": wrong_chain_tokens,
            }),
            2,
        )
        .await?;
        assert!(reader
            .fetch_token_snapshot(&wrong_chain)
            .await
            .is_err_and(|error| error.to_string().contains("different chain")));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn resolve_range_uses_complete_boundary_when_range_has_no_deltas(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-zero-delta-boundary-reader-test";

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let writer_objects = test_object_store(BUCKET).await;
        persist_checkpoint(
            &pool,
            &writer_objects,
            capture(1, 0, 100, CheckpointKind::Boundary, r#"{"state":"anchor"}"#)?,
        )
        .await?;
        let boundary = persist_checkpoint(
            &pool,
            &writer_objects,
            capture(1, 1, 120, CheckpointKind::Boundary, r#"{"state":"tail"}"#)?,
        )
        .await?;

        let reader = StateHistoryReader::new(
            StateHistoryStore::from_pool(pool),
            test_object_store(BUCKET).await,
        );
        let plan = reader
            .resolve_range(&RangeRequest::new(8453, 100, 120, vec![Backend::Native])?)
            .await?;

        assert_eq!(plan.legs.len(), 2);
        assert!(plan.legs[0].deltas.is_empty());
        assert_eq!(plan.legs[1].checkpoint, boundary);
        assert!(plan.legs[1].deltas.is_empty());
        assert!(plan.gaps.is_empty());
        plan.ensure_gap_free()?;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn resolve_range_excludes_boundary_past_requested_rfq_end(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-rfq-boundary-end-reader-test";
        const RFQ_END_MS: u64 = 120_000;

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let writer_objects = test_object_store(BUCKET).await;
        persist_checkpoint(
            &pool,
            &writer_objects,
            rfq_capture(
                1,
                0,
                100,
                timestamp_ms(100),
                CheckpointKind::Boundary,
                r#"{"state":"anchor"}"#,
            )?,
        )
        .await?;
        persist_delta(&pool, database_delta(1, 1, 120, false)?).await?;
        persist_checkpoint(
            &pool,
            &writer_objects,
            rfq_capture(
                1,
                2,
                120,
                RFQ_END_MS + 1,
                CheckpointKind::Boundary,
                r#"{"state":"past-rfq-end"}"#,
            )?,
        )
        .await?;

        let reader = StateHistoryReader::new(
            StateHistoryStore::from_pool(pool),
            test_object_store(BUCKET).await,
        );
        let request = RangeRequest::new(8453, 100, 120, vec![Backend::Native, Backend::Rfq])?
            .with_rfq_bounds(timestamp_ms(100), RFQ_END_MS)?;
        let plan = reader.resolve_range(&request).await?;

        assert_eq!(plan.legs.len(), 1);
        assert_eq!(plan.legs[0].deltas.len(), 1);
        assert_eq!(plan.gaps.len(), 1);
        assert_eq!(plan.gaps[0].kind, RangeGapKind::IncompleteTail);
        assert!(plan.gaps[0].reason.contains("rfq"));
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn backtest_resolves_same_plan_as_explicit_rfq_bounds(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-backtest-reader-test";

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let writer_objects = test_object_store(BUCKET).await;

        persist_checkpoint(
            &pool,
            &writer_objects,
            rfq_capture(
                1,
                0,
                100,
                timestamp_ms(100),
                CheckpointKind::Boundary,
                r#"{"state":"backtest"}"#,
            )?,
        )
        .await?;
        persist_delta(&pool, database_delta_with_rfq(1, 1, 110)?).await?;
        seed_block_times(&pool, 100, 111).await?;

        let reader = StateHistoryReader::new(
            StateHistoryStore::from_pool(pool),
            test_object_store(BUCKET).await,
        );
        let backtest_request =
            BacktestRequest::new(8453, 100, 110, vec![Backend::Native, Backend::Rfq])?;
        let backtest_plan = reader.resolve_backtest_range(&backtest_request).await?;
        let expected_bounds = (timestamp_ms(100), timestamp_ms(111) - 1);
        let explicit_request =
            RangeRequest::new(8453, 100, 110, vec![Backend::Native, Backend::Rfq])?
                .with_rfq_bounds(expected_bounds.0, expected_bounds.1)?;
        let explicit_plan = reader.resolve_range(&explicit_request).await?;

        assert_eq!(backtest_plan.start_block_timestamp_ms, timestamp_ms(100));
        assert_eq!(backtest_plan.end_block_timestamp_ms, timestamp_ms(110));
        assert_eq!(backtest_plan.rfq_bounds, Some(expected_bounds));
        assert_range_plans_structurally_eq(&backtest_plan.range, &explicit_plan);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn native_only_backtest_skips_block_time_requirements(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-native-backtest-reader-test";

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let writer_objects = test_object_store(BUCKET).await;

        persist_checkpoint(
            &pool,
            &writer_objects,
            capture(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                r#"{"state":"native-backtest"}"#,
            )?,
        )
        .await?;
        persist_delta(&pool, database_delta(1, 1, 110, false)?).await?;

        let reader = StateHistoryReader::new(
            StateHistoryStore::from_pool(pool),
            test_object_store(BUCKET).await,
        );
        let request = BacktestRequest::new(8453, 100, 110, vec![Backend::Native])?;
        let plan = reader.resolve_backtest_range(&request).await?;

        assert_eq!(plan.start_block_timestamp_ms, 0);
        assert_eq!(plan.end_block_timestamp_ms, 0);
        assert_eq!(plan.rfq_bounds, None);
        plan.range.ensure_gap_free()?;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    async fn resolve_range_ignores_recorded_gap_before_anchor_generation(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-pre-anchor-gap-reader-test";

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let writer_objects = test_object_store(BUCKET).await;
        persist_checkpoint(
            &pool,
            &writer_objects,
            capture(
                2,
                0,
                100,
                CheckpointKind::Boundary,
                r#"{"state":"rebased"}"#,
            )?,
        )
        .await?;
        let mut connection = pool.acquire().await?;
        StateHistoryStore::record_gap(
            &mut connection,
            &GapRecord {
                chain_id: 8453,
                generation: 1,
                from_message_seq: 40,
                to_message_seq: 50,
                reason: GapReason::QueueOverflow,
                from_block_number: Some(100),
                to_block_number: Some(100),
                from_observed_at_ms: Some(timestamp_ms(100)),
                to_observed_at_ms: Some(timestamp_ms(100)),
            },
        )
        .await?;
        drop(connection);

        let reader = StateHistoryReader::new(
            StateHistoryStore::from_pool(pool),
            test_object_store(BUCKET).await,
        );
        let plan = reader
            .resolve_range(&RangeRequest::new(8453, 100, 100, vec![Backend::Native])?)
            .await?;

        assert_eq!(plan.legs.len(), 1);
        assert_eq!(plan.legs[0].checkpoint.position, position(2, 0));
        assert!(plan.gaps.is_empty());
        plan.ensure_gap_free()?;
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    #[expect(clippy::expect_used)]
    async fn resolve_range_surfaces_unbounded_boundary_gap_without_surviving_deltas(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-gap-reader-test";

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let writer_objects = test_object_store(BUCKET).await;
        persist_checkpoint(
            &pool,
            &writer_objects,
            capture(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                r#"{"state":"before-loss"}"#,
            )?,
        )
        .await?;
        let mut connection = pool.acquire().await?;
        StateHistoryStore::record_gap(
            &mut connection,
            &GapRecord {
                chain_id: 8453,
                generation: 1,
                from_message_seq: 1,
                to_message_seq: 3,
                reason: GapReason::CheckpointFailed,
                from_block_number: None,
                to_block_number: None,
                from_observed_at_ms: None,
                to_observed_at_ms: None,
            },
        )
        .await?;
        drop(connection);

        let reader = StateHistoryReader::new(
            StateHistoryStore::from_pool(pool),
            test_object_store(BUCKET).await,
        );
        let plan = reader
            .resolve_range(&RangeRequest::new(8453, 100, 100, vec![Backend::Native])?)
            .await?;

        assert_eq!(plan.legs.len(), 1);
        assert_eq!(plan.legs[0].checkpoint.position, position(1, 0));
        assert!(plan.legs[0].deltas.is_empty());
        assert_eq!(plan.gaps.len(), 1);
        assert_eq!(plan.gaps[0].kind, RangeGapKind::Recorded);
        assert_eq!(plan.gaps[0].reason, "checkpoint_failed");
        plan.ensure_gap_free()
            .expect_err("recorded boundary loss must fail the range");
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    #[expect(clippy::expect_used)]
    async fn resolve_range_surfaces_recovery_discard_before_request_start(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-recovery-discard-reader-test";

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let writer_objects = test_object_store(BUCKET).await;
        persist_checkpoint(
            &pool,
            &writer_objects,
            capture(
                1,
                0,
                100,
                CheckpointKind::Boundary,
                r#"{"state":"pre-recovery"}"#,
            )?,
        )
        .await?;
        let mut connection = pool.acquire().await?;
        StateHistoryStore::record_gap(
            &mut connection,
            &GapRecord {
                chain_id: 8453,
                generation: 1,
                from_message_seq: 1,
                to_message_seq: 1,
                reason: GapReason::CheckpointFailed,
                from_block_number: Some(105),
                to_block_number: Some(105),
                from_observed_at_ms: None,
                to_observed_at_ms: None,
            },
        )
        .await?;
        drop(connection);
        persist_delta(&pool, database_delta(1, 2, 106, false)?).await?;
        persist_delta(&pool, database_delta(1, 3, 120, false)?).await?;

        let reader = StateHistoryReader::new(
            StateHistoryStore::from_pool(pool),
            test_object_store(BUCKET).await,
        );
        let plan = reader
            .resolve_range(&RangeRequest::new(8453, 110, 120, vec![Backend::Native])?)
            .await?;

        assert_eq!(plan.legs.len(), 1);
        assert_eq!(
            plan.legs[0]
                .deltas
                .iter()
                .map(|delta| delta.position)
                .collect::<Vec<_>>(),
            vec![position(1, 2), position(1, 3)]
        );
        assert_eq!(plan.gaps.len(), 1);
        assert_eq!(plan.gaps[0].kind, RangeGapKind::Recorded);
        assert_eq!(plan.gaps[0].reason, "checkpoint_failed");
        plan.ensure_gap_free()
            .expect_err("recovery discard after the anchor must fail a later range");
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    #[expect(clippy::expect_used)]
    async fn resolve_rfq_range_breaks_at_block_bounded_checkpoint_failure(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-rfq-checkpoint-failure-reader-test";

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let writer_objects = test_object_store(BUCKET).await;
        persist_checkpoint(
            &pool,
            &writer_objects,
            rfq_capture(
                1,
                0,
                100,
                timestamp_ms(100),
                CheckpointKind::Boundary,
                r#"{"state":"pre-recovery"}"#,
            )?,
        )
        .await?;
        let mut connection = pool.acquire().await?;
        StateHistoryStore::record_gap(
            &mut connection,
            &GapRecord {
                chain_id: 8453,
                generation: 1,
                from_message_seq: 1,
                to_message_seq: 1,
                reason: GapReason::CheckpointFailed,
                from_block_number: Some(105),
                to_block_number: Some(105),
                from_observed_at_ms: None,
                to_observed_at_ms: None,
            },
        )
        .await?;
        drop(connection);
        persist_delta(&pool, database_delta_with_rfq(1, 2, 110)?).await?;

        let reader = StateHistoryReader::new(
            StateHistoryStore::from_pool(pool),
            test_object_store(BUCKET).await,
        );
        let request = RangeRequest::new(8453, 100, 100, vec![Backend::Rfq])?
            .with_rfq_bounds(timestamp_ms(100), timestamp_ms(111) - 1)?;
        let plan = reader.resolve_range(&request).await?;

        assert_eq!(plan.legs.len(), 1);
        assert!(plan.legs[0].deltas.is_empty());
        assert_eq!(plan.gaps.len(), 1);
        assert_eq!(plan.gaps[0].kind, RangeGapKind::MissingCheckpoint);
        assert_eq!(plan.gaps[0].from_message_seq, Some(1));
        assert_eq!(plan.gaps[0].to_message_seq, Some(2));
        plan.ensure_gap_free()
            .expect_err("the SQL path must retain the positional checkpoint failure");
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    #[expect(clippy::expect_used)]
    async fn resolve_rfq_range_breaks_at_anchor_straddling_checkpoint_failure(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-rfq-straddling-failure-reader-test";

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let writer_objects = test_object_store(BUCKET).await;
        persist_checkpoint(
            &pool,
            &writer_objects,
            rfq_capture(
                1,
                10,
                100,
                timestamp_ms(100),
                CheckpointKind::Boundary,
                r#"{"state":"pre-recovery"}"#,
            )?,
        )
        .await?;
        let mut connection = pool.acquire().await?;
        StateHistoryStore::record_gap(
            &mut connection,
            &GapRecord {
                chain_id: 8453,
                generation: 1,
                from_message_seq: 9,
                to_message_seq: 11,
                reason: GapReason::CheckpointFailed,
                from_block_number: Some(99),
                to_block_number: Some(101),
                from_observed_at_ms: None,
                to_observed_at_ms: None,
            },
        )
        .await?;
        drop(connection);
        persist_delta(&pool, database_delta_with_rfq(1, 12, 110)?).await?;

        let reader = StateHistoryReader::new(
            StateHistoryStore::from_pool(pool),
            test_object_store(BUCKET).await,
        );
        let request = RangeRequest::new(8453, 100, 100, vec![Backend::Rfq])?
            .with_rfq_bounds(timestamp_ms(100), timestamp_ms(111) - 1)?;
        let plan = reader.resolve_range(&request).await?;

        assert_eq!(plan.legs.len(), 1);
        assert!(plan.legs[0].deltas.is_empty());
        assert_eq!(plan.gaps.len(), 1);
        assert_eq!(plan.gaps[0].kind, RangeGapKind::MissingCheckpoint);
        assert_eq!(plan.gaps[0].from_message_seq, Some(11));
        assert_eq!(plan.gaps[0].to_message_seq, Some(12));
        plan.ensure_gap_free()
            .expect_err("a failure span straddling the anchor must fail closed");
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore]
    #[expect(clippy::expect_used)]
    async fn resolve_range_replays_across_a_promotion(pool: PgPool) -> anyhow::Result<()> {
        const BUCKET: &str = "state-history-reader-test";

        configure_test_aws();
        create_test_bucket(BUCKET).await?;
        let writer_objects = test_object_store(BUCKET).await;

        persist_checkpoint(
            &pool,
            &writer_objects,
            capture(1, 0, 100, CheckpointKind::Boundary, r#"{"state":"gen-1"}"#)?,
        )
        .await?;
        persist_delta(&pool, database_delta(1, 1, 105, true)?).await?;
        let boundary = persist_checkpoint(
            &pool,
            &writer_objects,
            capture(2, 1, 110, CheckpointKind::Boundary, r#"{"state":"gen-2"}"#)?,
        )
        .await?;
        persist_delta(&pool, database_delta(2, 2, 120, false)?).await?;

        let reader = StateHistoryReader::new(
            StateHistoryStore::from_pool(pool.clone()),
            test_object_store(BUCKET).await,
        );
        let request = RangeRequest::new(8453, 100, 120, vec![Backend::Native])?;
        let plan = reader.resolve_range(&request).await?;

        assert_eq!(plan.legs.len(), 2);
        assert_eq!(plan.legs[0].checkpoint.position, position(1, 0));
        assert_eq!(plan.legs[0].deltas[0].position, position(1, 1));
        assert_eq!(plan.legs[0].deltas[0].applicable_backends.len(), 1);
        assert_eq!(
            plan.legs[0].deltas[0].applicable_backends[0].backend,
            Backend::Native
        );
        assert!(plan.legs[0].deltas[0]
            .raw_payload
            .get()
            .contains(r#""vm_marker":true"#));
        assert_eq!(plan.legs[1].checkpoint.position, position(2, 1));
        assert_eq!(plan.legs[1].deltas[0].position, position(2, 2));
        plan.ensure_gap_free()?;
        let fetched = reader.fetch_checkpoint(&boundary).await?;
        assert_eq!(fetched.payloads_json, vec![r#"{"state":"gen-2"}"#]);

        sqlx::query(
            "UPDATE state_history.checkpoints
             SET status = 'failed', error = 'capture failed'
             WHERE id = $1",
        )
        .bind(boundary.id)
        .execute(&pool)
        .await?;

        let plan = reader.resolve_range(&request).await?;
        assert_eq!(plan.legs.len(), 1);
        assert_eq!(
            plan.gaps
                .iter()
                .filter(|gap| gap.kind == RangeGapKind::MissingCheckpoint)
                .count(),
            1
        );
        assert!(plan
            .legs
            .iter()
            .all(|leg| leg.checkpoint.position.generation == 1));

        let mut failed_boundary = boundary;
        failed_boundary.status = CheckpointStatus::Failed;
        let error = reader
            .fetch_checkpoint(&failed_boundary)
            .await
            .expect_err("failed checkpoint fetch must be refused");
        assert!(error.to_string().contains("not complete"));
        Ok(())
    }

    #[expect(clippy::expect_used)]
    fn request(start_block: u64, end_block: u64) -> RangeRequest {
        RangeRequest::new(8453, start_block, end_block, vec![Backend::Native])
            .expect("test request should be valid")
    }

    #[expect(clippy::expect_used)]
    fn delta(generation: u64, message_seq: u64, block_number: u64) -> DeltaRow {
        DeltaRow {
            position: position(generation, message_seq),
            observed_at_ms: block_number * 1_000,
            payload_format_version: crate::DELTA_PAYLOAD_FORMAT_VERSION,
            payload: RawValue::from_string(format!(
                r#"{{"generation":{generation},"message_seq":{message_seq}}}"#
            ))
            .expect("test delta payload should be valid JSON"),
            backends: vec![DeltaBackendCursor {
                backend: Backend::Native,
                block_number: Some(block_number),
                observed_at_ms: None,
            }],
        }
    }

    #[expect(clippy::expect_used)]
    fn rfq_delta(generation: u64, message_seq: u64, observed_at_ms: u64) -> DeltaRow {
        DeltaRow {
            position: position(generation, message_seq),
            observed_at_ms,
            payload_format_version: crate::DELTA_PAYLOAD_FORMAT_VERSION,
            payload: RawValue::from_string(format!(
                r#"{{"generation":{generation},"message_seq":{message_seq}}}"#
            ))
            .expect("test delta payload should be valid JSON"),
            backends: vec![DeltaBackendCursor {
                backend: Backend::Rfq,
                block_number: None,
                observed_at_ms: Some(observed_at_ms),
            }],
        }
    }

    fn manifest(
        generation: u64,
        message_seq: u64,
        block_number: u64,
        kind: CheckpointKind,
        status: CheckpointStatus,
    ) -> CheckpointManifest {
        CheckpointManifest {
            id: i64::try_from(generation * 100 + message_seq).unwrap_or(i64::MAX),
            chain_id: 8453,
            position: position(generation, message_seq),
            state_version: Some(generation),
            kind,
            block_number,
            rfq_observed_at_ms: None,
            backends: vec![Backend::Native],
            s3_key: (status == CheckpointStatus::Complete).then(|| "checkpoint.zst".to_owned()),
            archive_sha256: (status == CheckpointStatus::Complete).then(|| "sha256".to_owned()),
            archive_bytes: None,
            compressed_bytes: None,
            token_reference: None,
            status,
            error: None,
        }
    }

    fn rfq_manifest(
        generation: u64,
        message_seq: u64,
        block_number: u64,
        rfq_observed_at_ms: u64,
        kind: CheckpointKind,
        status: CheckpointStatus,
    ) -> CheckpointManifest {
        let mut manifest = manifest(generation, message_seq, block_number, kind, status);
        manifest.rfq_observed_at_ms = Some(rfq_observed_at_ms);
        manifest.backends.push(Backend::Rfq);
        manifest.backends.sort_unstable();
        manifest
    }

    const fn position(generation: u64, message_seq: u64) -> StreamPosition {
        StreamPosition {
            generation,
            message_seq,
        }
    }

    fn configure_test_aws() {
        std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
        std::env::set_var("AWS_REGION", "eu-central-1");
    }

    const fn timestamp_ms(block_number: u64) -> u64 {
        block_number * 1_000
    }

    async fn test_object_store(bucket: &str) -> CheckpointObjectStore {
        CheckpointObjectStore::from_env_config(
            bucket.to_owned(),
            "reader".to_owned(),
            "eu-central-1".to_owned(),
            Some("http://127.0.0.1:59000".to_owned()),
            true,
        )
        .await
    }

    async fn create_test_bucket(bucket: &str) -> anyhow::Result<()> {
        let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new("eu-central-1"))
            .load()
            .await;
        let config = aws_sdk_s3::config::Builder::from(&shared_config)
            .endpoint_url("http://127.0.0.1:59000")
            .force_path_style(true)
            .build();
        let client = aws_sdk_s3::Client::from_conf(config);
        if let Err(error) = client.create_bucket().bucket(bucket).send().await {
            let exists = error.as_service_error().is_some_and(|service_error| {
                service_error.is_bucket_already_exists()
                    || service_error.is_bucket_already_owned_by_you()
            });
            if !exists {
                return Err(error).context("failed to create reader test bucket");
            }
        }
        Ok(())
    }

    fn test_token_snapshot() -> anyhow::Result<TokenSnapshot> {
        Ok(TokenSnapshot {
            chain_id: 8453,
            tokens: vec![
                test_token("0x2222222222222222222222222222222222222222", "TWO", 18, 90)?,
                test_token("0x1111111111111111111111111111111111111111", "ONE", 6, 80)?,
            ],
        })
    }

    fn test_token(
        address: &str,
        symbol: &str,
        decimals: u32,
        quality: u32,
    ) -> anyhow::Result<BroadcasterTokenDto> {
        Ok(BroadcasterTokenDto {
            address: serde_json::from_value(json!(address))?,
            symbol: symbol.to_owned(),
            decimals,
            tax: 7,
            gas: vec![Some(21_000), None],
            chain_id: 8453,
            quality,
        })
    }

    fn expected_test_tokens() -> serde_json::Value {
        json!([
            {
                "address": "0x1111111111111111111111111111111111111111",
                "symbol": "ONE",
                "decimals": 6,
                "tax": 7,
                "gas": [21_000, null],
                "chainId": 8453,
                "quality": 80,
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "symbol": "TWO",
                "decimals": 18,
                "tax": 7,
                "gas": [21_000, null],
                "chainId": 8453,
                "quality": 90,
            }
        ])
    }

    async fn put_raw_token_snapshot(
        objects: &CheckpointObjectStore,
        envelope: serde_json::Value,
        token_count: u64,
    ) -> anyhow::Result<TokenSnapshotRef> {
        let snapshot_json = serde_json::to_vec(&envelope)?;
        let sha256 = hex::encode(Sha256::digest(&snapshot_json));
        let token_bytes = u64::try_from(snapshot_json.len())?;
        let compressed = zstd::stream::encode_all(snapshot_json.as_slice(), 3)?;
        let s3_key = token_snapshot_s3_key("reader", 8453, &sha256);
        objects.put(&s3_key, compressed).await?;
        Ok(TokenSnapshotRef {
            s3_key,
            sha256,
            token_count,
            token_bytes,
        })
    }

    async fn seed_block_times(
        pool: &PgPool,
        start_block: u64,
        end_block: u64,
    ) -> anyhow::Result<()> {
        for block_number in start_block..=end_block {
            sqlx::query(
                "INSERT INTO state_history.block_times (
                    chain_id, block_number, timestamp_ms, block_hash, parent_hash,
                    source_generation, source_message_seq
                 )
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(8_453_i64)
            .bind(database_i64(block_number, "test block_number")?)
            .bind(database_i64(
                timestamp_ms(block_number),
                "test timestamp_ms",
            )?)
            .bind(format!("block-{block_number}"))
            .bind(format!("block-{}", block_number - 1))
            .bind(1_i64)
            .bind(database_i64(block_number, "test source_message_seq")?)
            .execute(pool)
            .await?;
        }
        Ok(())
    }

    fn test_archive_info() -> EncodedArchiveInfo {
        EncodedArchiveInfo {
            sha256: "archive-sha256".to_owned(),
            archive_bytes: 1_024,
            compressed_bytes: 512,
        }
    }

    async fn read_checkpoint_manifest(
        pool: &PgPool,
        checkpoint_id: i64,
    ) -> anyhow::Result<CheckpointManifest> {
        let row = sqlx::query(
            "SELECT id, chain_id, generation, message_seq, state_version, kind, block_number,
                    rfq_observed_at_ms, backends, s3_key, archive_sha256, archive_bytes,
                    compressed_bytes, token_s3_key, token_sha256, token_count, token_bytes,
                    status, error
             FROM state_history.checkpoints
             WHERE id = $1",
        )
        .bind(checkpoint_id)
        .fetch_one(pool)
        .await?;
        manifest_from_row(&row)
    }

    async fn persist_checkpoint(
        pool: &PgPool,
        objects: &CheckpointObjectStore,
        capture: CapturedState,
    ) -> anyhow::Result<CheckpointManifest> {
        let mut connection = pool.acquire().await?;
        let id = StateHistoryStore::create_checkpoint(&mut connection, &capture).await?;
        let archive = CheckpointArchive {
            metadata: ArchiveMetadata {
                schema_version: 1,
                chain_id: capture.chain_id(),
                position: capture.position(),
                state_version: capture.state_version(),
                kind: capture.kind(),
                block_number: capture.block_number(),
                rfq_observed_at_ms: capture.rfq_observed_at_ms(),
                backends: capture.backends().to_vec(),
            },
            payloads_json: capture.payloads_json().to_vec(),
        };
        let encoded = encode_archive(archive)?;
        let key = objects.key_for(capture.chain_id(), capture.position(), capture.kind());
        objects.put(&key, encoded.bytes).await?;
        StateHistoryStore::complete_checkpoint(
            &mut connection,
            id,
            &key,
            &encoded.info,
            None,
            capture.block_times(),
            capture.position(),
            capture.chain_id(),
        )
        .await?;

        Ok(CheckpointManifest {
            id,
            chain_id: capture.chain_id(),
            position: capture.position(),
            state_version: capture.state_version(),
            kind: capture.kind(),
            block_number: capture.block_number(),
            rfq_observed_at_ms: capture.rfq_observed_at_ms(),
            backends: capture.backends().to_vec(),
            s3_key: Some(key),
            archive_sha256: Some(encoded.info.sha256),
            archive_bytes: Some(encoded.info.archive_bytes),
            compressed_bytes: Some(encoded.info.compressed_bytes),
            token_reference: None,
            status: CheckpointStatus::Complete,
            error: None,
        })
    }

    async fn persist_delta(pool: &PgPool, delta: DeltaEntry) -> anyhow::Result<()> {
        let mut connection = pool.acquire().await?;
        StateHistoryStore::insert_delta(&mut connection, &delta, &BTreeMap::new()).await?;
        Ok(())
    }

    fn capture(
        generation: u64,
        message_seq: u64,
        block_number: u64,
        kind: CheckpointKind,
        payload: &str,
    ) -> Result<CapturedState, ModelError> {
        CapturedState::new(
            8453,
            position(generation, message_seq),
            Some(generation),
            kind,
            block_number,
            None,
            vec![Backend::Native],
            vec![payload.to_owned()],
            vec![],
        )
    }

    fn rfq_capture(
        generation: u64,
        message_seq: u64,
        block_number: u64,
        rfq_observed_at_ms: u64,
        kind: CheckpointKind,
        payload: &str,
    ) -> Result<CapturedState, ModelError> {
        CapturedState::new(
            8453,
            position(generation, message_seq),
            Some(generation),
            kind,
            block_number,
            Some(rfq_observed_at_ms),
            vec![Backend::Native, Backend::Rfq],
            vec![payload.to_owned()],
            vec![],
        )
    }

    fn database_delta(
        generation: u64,
        message_seq: u64,
        block_number: u64,
        include_vm: bool,
    ) -> Result<DeltaEntry, ModelError> {
        let mut backends = vec![DeltaBackendCursor {
            backend: Backend::Native,
            block_number: Some(block_number),
            observed_at_ms: None,
        }];
        if include_vm {
            backends.push(DeltaBackendCursor {
                backend: Backend::Vm,
                block_number: Some(block_number),
                observed_at_ms: None,
            });
        }
        DeltaEntry::new(
            8453,
            position(generation, message_seq),
            block_number * 1_000,
            format!(
                r#"{{"generation":{generation},"message_seq":{message_seq},"vm_marker":{include_vm}}}"#
            ),
            backends,
            vec![],
        )
    }

    fn database_delta_with_rfq(
        generation: u64,
        message_seq: u64,
        block_number: u64,
    ) -> Result<DeltaEntry, ModelError> {
        let rfq_observed_at_ms = timestamp_ms(block_number + 1) - 1;
        DeltaEntry::new(
            8453,
            position(generation, message_seq),
            rfq_observed_at_ms,
            format!(r#"{{"generation":{generation},"message_seq":{message_seq},"rfq":true}}"#),
            vec![
                DeltaBackendCursor {
                    backend: Backend::Native,
                    block_number: Some(block_number),
                    observed_at_ms: None,
                },
                DeltaBackendCursor {
                    backend: Backend::Rfq,
                    block_number: None,
                    observed_at_ms: Some(rfq_observed_at_ms),
                },
            ],
            vec![],
        )
    }

    fn assert_range_plans_structurally_eq(actual: &RangePlan, expected: &RangePlan) {
        assert_eq!(actual.gaps, expected.gaps);
        assert_eq!(actual.legs.len(), expected.legs.len());
        for (actual_leg, expected_leg) in actual.legs.iter().zip(&expected.legs) {
            assert_eq!(actual_leg.checkpoint, expected_leg.checkpoint);
            assert_eq!(actual_leg.deltas.len(), expected_leg.deltas.len());
            for (actual_delta, expected_delta) in actual_leg.deltas.iter().zip(&expected_leg.deltas)
            {
                assert_eq!(actual_delta.position, expected_delta.position);
                assert_eq!(actual_delta.observed_at_ms, expected_delta.observed_at_ms);
                assert_eq!(
                    actual_delta.raw_payload.get(),
                    expected_delta.raw_payload.get()
                );
                assert_eq!(
                    actual_delta.applicable_backends,
                    expected_delta.applicable_backends
                );
            }
        }
    }
}
