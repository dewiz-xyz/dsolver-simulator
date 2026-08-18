use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;
use uuid::Uuid;

use super::{
    deserialize_uuid_v4, validate_amounts, validate_api_revision, validate_backends,
    validate_chain_id, validate_timeout, Backend, PoolSelector, QuoteFailureReason, StreamPosition,
    UnsignedAmount,
};

/// Request for a historical state consistency check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(
    rename_all = "camelCase",
    deny_unknown_fields,
    try_from = "UncheckedConsistencyCheckRequest"
)]
pub struct ConsistencyCheckRequest {
    #[serde(deserialize_with = "deserialize_uuid_v4")]
    pub request_id: Uuid,
    #[schema(minimum = 1, maximum = 1)]
    pub api_revision: u32,
    #[schema(minimum = 1)]
    pub timeout_ms: u64,
    #[schema(minimum = 8453, maximum = 8453)]
    pub chain_id: u64,
    #[schema(schema_with = crate::api::common::unique_backends_schema)]
    pub backends: Vec<Backend>,
    pub selection: ConsistencySelection,
    #[schema(min_items = 1)]
    pub quotes_to_compare: Vec<QuoteComparisonRequest>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UncheckedConsistencyCheckRequest {
    #[serde(deserialize_with = "deserialize_uuid_v4")]
    request_id: Uuid,
    api_revision: u32,
    timeout_ms: u64,
    chain_id: u64,
    backends: Vec<Backend>,
    selection: ConsistencySelection,
    quotes_to_compare: Vec<QuoteComparisonRequest>,
}

impl TryFrom<UncheckedConsistencyCheckRequest> for ConsistencyCheckRequest {
    type Error = String;

    fn try_from(value: UncheckedConsistencyCheckRequest) -> Result<Self, Self::Error> {
        validate_api_revision(value.api_revision)?;
        validate_timeout(value.timeout_ms)?;
        validate_chain_id(value.chain_id)?;
        validate_backends(&value.backends)?;
        value.selection.validate()?;
        if value.quotes_to_compare.is_empty() {
            return Err("quotesToCompare must be non-empty".to_owned());
        }
        for quote in &value.quotes_to_compare {
            validate_amounts(&quote.amounts_in)?;
            if !value.backends.contains(&quote.pool.backend) {
                return Err("each quotesToCompare backend must be present in backends".to_owned());
            }
        }

        Ok(Self {
            request_id: value.request_id,
            api_revision: value.api_revision,
            timeout_ms: value.timeout_ms,
            chain_id: value.chain_id,
            backends: value.backends,
            selection: value.selection,
            quotes_to_compare: value.quotes_to_compare,
        })
    }
}

/// Checkpoint pair or block range selected for a consistency check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum ConsistencySelection {
    CheckpointPairs {
        #[schema(min_items = 1)]
        pairs: Vec<CheckpointPair>,
    },
    BlockRange {
        start: u64,
        #[serde(rename = "endInclusive")]
        end_inclusive: u64,
    },
}

impl ConsistencySelection {
    fn validate(&self) -> Result<(), String> {
        match self {
            Self::CheckpointPairs { pairs } => {
                if pairs.is_empty() {
                    return Err("selection.pairs must be non-empty".to_owned());
                }
                for pair in pairs {
                    if pair.earlier_position >= pair.target_position {
                        return Err(
                            "earlierPosition must precede targetPosition in each pair".to_owned()
                        );
                    }
                }
            }
            Self::BlockRange {
                start,
                end_inclusive,
            } => {
                if start > end_inclusive {
                    return Err("selection.start must not exceed selection.endInclusive".to_owned());
                }
            }
        }
        Ok(())
    }
}

/// Explicit earlier and target checkpoint positions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckpointPair {
    pub earlier_position: StreamPosition,
    pub target_position: StreamPosition,
}

/// Directed pool quotes compared for each selected checkpoint pair.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QuoteComparisonRequest {
    pub pool: PoolSelector,
    #[schema(schema_with = crate::api::common::unique_amounts_schema)]
    pub amounts_in: Vec<UnsignedAmount>,
}

/// Canonical consistency request content used for idempotency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedConsistencyCheckRequest {
    pub api_revision: u32,
    pub timeout_ms: u64,
    pub chain_id: u64,
    pub backends: Vec<Backend>,
    pub selection: ConsistencySelection,
    pub quotes_to_compare: Vec<QuoteComparisonRequest>,
}

impl NormalizedConsistencyCheckRequest {
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

/// Canonicalizes a validated consistency request for idempotency comparison.
#[must_use]
pub fn normalize_consistency_request(
    request: &ConsistencyCheckRequest,
) -> NormalizedConsistencyCheckRequest {
    let mut backends = request.backends.clone();
    backends.sort_unstable();

    let selection = match &request.selection {
        ConsistencySelection::CheckpointPairs { pairs } => {
            let mut pairs = pairs.clone();
            pairs.sort_unstable();
            ConsistencySelection::CheckpointPairs { pairs }
        }
        ConsistencySelection::BlockRange {
            start,
            end_inclusive,
        } => ConsistencySelection::BlockRange {
            start: *start,
            end_inclusive: *end_inclusive,
        },
    };

    let mut quotes_to_compare = request.quotes_to_compare.clone();
    for quote in &mut quotes_to_compare {
        quote.amounts_in.sort_unstable();
    }
    quotes_to_compare.sort_unstable();

    NormalizedConsistencyCheckRequest {
        api_revision: request.api_revision,
        timeout_ms: request.timeout_ms,
        chain_id: request.chain_id,
        backends,
        selection,
        quotes_to_compare,
    }
}

/// Aggregate status for one checkpoint pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum PairStatus {
    Pass,
    Mismatch,
    Gap,
    NotComparable,
}

/// Selection-level status for a consistency check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum ConsistencySelectionStatus {
    Compared,
    NotComparable,
}

/// Summary of the canonical simulator state on one side of a comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CanonicalStateSummary {
    pub sha256: String,
    pub component_count: u64,
    pub backend_counts: BTreeMap<Backend, u64>,
}

/// Quote result used only for direct-versus-replayed comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(
    tag = "outcome",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ConsistencyQuoteOutcome {
    Ok {
        #[serde(rename = "amountOut")]
        amount_out: UnsignedAmount,
    },
    QuoteFailed {
        reason: QuoteFailureReason,
    },
}

/// One quote comparison within a checkpoint pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConsistencyQuoteComparison {
    pub pool: PoolSelector,
    pub amount_in: UnsignedAmount,
    pub direct_outcome: ConsistencyQuoteOutcome,
    pub replayed_outcome: ConsistencyQuoteOutcome,
    pub status: PairStatus,
}

/// One bounded difference between canonical state representations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConsistencyDifference {
    pub backend: Backend,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub component_id: Option<String>,
    pub field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direct_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replayed_sha256: Option<String>,
}

/// Direct-versus-replayed result for one checkpoint pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConsistencyPairReport {
    pub earlier_position: StreamPosition,
    pub target_position: StreamPosition,
    pub status: PairStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direct_state: Option<CanonicalStateSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replayed_state: Option<CanonicalStateSummary>,
    pub affected_backends: Vec<Backend>,
    pub affected_component_ids: Vec<String>,
    pub quote_comparisons: Vec<ConsistencyQuoteComparison>,
    pub total_difference_count: u64,
    pub differences: Vec<ConsistencyDifference>,
}

/// Pair counts for a consistency report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConsistencySummary {
    pub selected_checkpoint_pairs: u64,
    pub compared_checkpoint_pairs: u64,
    pub pass: u64,
    pub mismatch: u64,
    pub gap: u64,
    pub not_comparable: u64,
}

/// Storage validation completed before state and quote comparison.
#[expect(
    clippy::struct_excessive_bools,
    reason = "each field is a separate check in the verification wire contract"
)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StorageVerificationSummary {
    pub range_plan_valid: bool,
    pub checkpoint_objects_valid: bool,
    pub token_objects_valid: bool,
    pub delta_order_valid: bool,
    pub replay_plans_verified: u64,
    pub checkpoint_objects_verified: u64,
    pub token_objects_verified: u64,
    pub deltas_verified: u64,
}

/// Completed report for a historical state consistency check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConsistencyCheckReport {
    #[serde(deserialize_with = "deserialize_uuid_v4")]
    pub request_id: Uuid,
    #[schema(minimum = 8453, maximum = 8453)]
    pub chain_id: u64,
    pub backends: Vec<Backend>,
    pub selection: ConsistencySelection,
    pub selection_status: ConsistencySelectionStatus,
    pub storage: StorageVerificationSummary,
    pub summary: ConsistencySummary,
    pub pairs: Vec<ConsistencyPairReport>,
}
