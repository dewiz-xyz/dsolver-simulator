use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;
use uuid::Uuid;

use super::{
    deserialize_uuid_v4, validate_api_revision, validate_chain_id, validate_timeout, BlockRange,
    GapReference, PoolSelector, StreamPosition, UnsignedAmount,
};

/// Request for one historical quote job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(
    rename_all = "camelCase",
    deny_unknown_fields,
    try_from = "UncheckedQuoteJobRequest"
)]
pub struct QuoteJobRequest {
    #[serde(deserialize_with = "deserialize_uuid_v4")]
    pub request_id: Uuid,
    #[schema(minimum = 1, maximum = 1)]
    pub api_revision: u32,
    #[schema(minimum = 1)]
    pub timeout_ms: u64,
    #[schema(minimum = 8453, maximum = 8453)]
    pub chain_id: u64,
    pub pool: PoolSelector,
    pub blocks: BlockRange,
    #[schema(schema_with = crate::api::common::positive_unique_lags_schema)]
    pub lags: Vec<u64>,
    #[schema(schema_with = crate::api::common::unique_amounts_schema)]
    pub amounts_in: Vec<UnsignedAmount>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UncheckedQuoteJobRequest {
    #[serde(deserialize_with = "deserialize_uuid_v4")]
    request_id: Uuid,
    api_revision: u32,
    timeout_ms: u64,
    chain_id: u64,
    pool: PoolSelector,
    blocks: BlockRange,
    lags: Vec<u64>,
    amounts_in: Vec<UnsignedAmount>,
}

impl TryFrom<UncheckedQuoteJobRequest> for QuoteJobRequest {
    type Error = String;

    fn try_from(value: UncheckedQuoteJobRequest) -> Result<Self, Self::Error> {
        validate_api_revision(value.api_revision)?;
        validate_timeout(value.timeout_ms)?;
        validate_chain_id(value.chain_id)?;
        validate_lags(&value.lags)?;
        validate_amounts(&value.amounts_in)?;

        Ok(Self {
            request_id: value.request_id,
            api_revision: value.api_revision,
            timeout_ms: value.timeout_ms,
            chain_id: value.chain_id,
            pool: value.pool,
            blocks: value.blocks,
            lags: value.lags,
            amounts_in: value.amounts_in,
        })
    }
}

/// Canonical request content used for idempotency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedQuoteRequest {
    pub api_revision: u32,
    pub timeout_ms: u64,
    pub chain_id: u64,
    pub pool: PoolSelector,
    pub blocks: BlockRange,
    pub lags: Vec<u64>,
    pub amounts_in: Vec<UnsignedAmount>,
}

impl NormalizedQuoteRequest {
    /// Returns a SHA-256 fingerprint of the canonical request JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if canonical JSON serialization fails.
    pub fn fingerprint(&self) -> Result<[u8; 32], serde_json::Error> {
        let json = serde_json::to_vec(self)?;
        Ok(Sha256::digest(json).into())
    }
}

/// Canonicalizes a validated quote request for idempotency comparison.
#[must_use]
pub fn normalize_quote_request(request: &QuoteJobRequest) -> NormalizedQuoteRequest {
    let mut lags = request.lags.clone();
    lags.sort_unstable();
    let mut amounts_in = request.amounts_in.clone();
    amounts_in.sort_unstable();

    NormalizedQuoteRequest {
        api_revision: request.api_revision,
        timeout_ms: request.timeout_ms,
        chain_id: request.chain_id,
        pool: request.pool.clone(),
        blocks: request.blocks,
        lags,
        amounts_in,
    }
}

/// Result for a completed historical quote job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HistoricalQuoteResult {
    #[serde(deserialize_with = "deserialize_uuid_v4")]
    pub request_id: Uuid,
    #[schema(minimum = 8453, maximum = 8453)]
    pub chain_id: u64,
    pub pool: PoolSelector,
    pub visible_through_block: u64,
    pub gaps: Vec<GapReference>,
    pub results: Vec<HistoricalQuoteEntry>,
}

/// One result for a start block and input amount.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HistoricalQuoteEntry {
    pub start_block: u64,
    pub amount_in: UnsignedAmount,
    pub start_outcome: QuoteOutcome,
    pub targets: Vec<HistoricalQuoteTarget>,
}

/// One lagged target in a grouped quote result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HistoricalQuoteTarget {
    pub lag: u64,
    pub target_block: u64,
    pub outcome: QuoteOutcome,
}

/// Result of one directed pool quote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(
    tag = "outcome",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum QuoteOutcome {
    Ok {
        #[serde(rename = "amountOut")]
        amount_out: UnsignedAmount,
        provenance: QuoteProvenance,
    },
    Unavailable {
        reason: UnavailableReason,
        #[serde(rename = "gapRefs", default, skip_serializing_if = "Vec::is_empty")]
        #[schema(required = false)]
        gap_refs: Vec<String>,
    },
    QuoteFailed {
        reason: QuoteFailureReason,
    },
}

/// Safe reason why historical state is unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableReason {
    Gap,
    ReplicaCutoff,
    TokenSnapshotMissing,
    RfqObservationMissing,
    BlockBoundaryMissing,
    CanonicalLineageInvalid,
    StateApplicationInvalid,
}

/// Safe reason why existing state could not produce a quote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum QuoteFailureReason {
    ComponentNotFound,
    DirectionUnsupported,
    InsufficientLiquidity,
    ZeroOutput,
    SimulationFailed,
}

/// State provenance for one successful historical quote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QuoteProvenance {
    pub block_hash: String,
    pub final_position: StreamPosition,
    pub anchor_checkpoint_position: StreamPosition,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_snapshot_digest: Option<String>,
    pub service_revision: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rfq_observation_cursor: Option<StreamPosition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rfq_observed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rfq_observation_age_ms: Option<u64>,
}

pub(crate) fn validate_amounts(amounts: &[UnsignedAmount]) -> Result<(), String> {
    if amounts.is_empty() {
        return Err("amountsIn must be non-empty".to_owned());
    }
    let distinct = amounts.iter().collect::<BTreeSet<_>>();
    if distinct.len() != amounts.len() {
        return Err("duplicate numeric input amount in amountsIn".to_owned());
    }
    Ok(())
}

fn validate_lags(lags: &[u64]) -> Result<(), String> {
    let distinct = lags.iter().copied().collect::<BTreeSet<_>>();
    if lags.is_empty() || lags.contains(&0) || distinct.len() != lags.len() {
        return Err("lags must contain distinct positive integers".to_owned());
    }
    Ok(())
}
