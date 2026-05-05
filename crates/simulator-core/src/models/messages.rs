use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde skip_serializing_if predicates receive borrowed values"
)]
fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AmountOutRequest {
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auction_id: Option<String>,
    pub token_in: String,
    pub token_out: String,
    pub amounts: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AmountOutResponse {
    pub pool: String,
    pub pool_name: String,
    pub pool_address: String,
    pub amounts_out: Vec<String>,
    #[serde(default)]
    pub slippage: Vec<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_max_in: Option<String>,
    pub gas_used: Vec<u64>,
    pub block_number: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuoteStatus {
    Ready,
    WarmingUp,
    TokenMissing,
    NoLiquidity,
    InvalidRequest,
    InternalError,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuoteResultQuality {
    Complete,
    Partial,
    NoResults,
    RequestLevelFailure,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuotePartialKind {
    AmountLadders,
    PoolCoverage,
    Mixed,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PoolOutcomeKind {
    PartialOutput,
    ZeroOutput,
    TimedOut,
    SimulatorError,
    InternalError,
}

#[derive(Debug, Clone, Copy)]
pub enum QuoteFailureKind {
    WarmUp,
    TokenValidation,
    TokenCoverage,
    Timeout,
    Overflow,
    Simulator,
    NoPools,
    InconsistentResult,
    Internal,
    InvalidRequest,
}

impl QuoteFailureKind {
    pub const fn label(self) -> &'static str {
        match self {
            QuoteFailureKind::WarmUp => "warm_up",
            QuoteFailureKind::TokenValidation => "token_validation",
            QuoteFailureKind::TokenCoverage => "token_coverage",
            QuoteFailureKind::Timeout => "timeout",
            QuoteFailureKind::Overflow => "overflow",
            QuoteFailureKind::Simulator => "simulator",
            QuoteFailureKind::NoPools => "no_pools",
            QuoteFailureKind::InconsistentResult => "inconsistent_result",
            QuoteFailureKind::Internal => "internal",
            QuoteFailureKind::InvalidRequest => "invalid_request",
        }
    }
}

impl Serialize for QuoteFailureKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.label())
    }
}

impl<'de> Deserialize<'de> for QuoteFailureKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        match value.as_str() {
            "warm_up" => Ok(Self::WarmUp),
            "token_validation" => Ok(Self::TokenValidation),
            "token_coverage" => Ok(Self::TokenCoverage),
            "timeout" => Ok(Self::Timeout),
            "overflow" => Ok(Self::Overflow),
            "simulator" => Ok(Self::Simulator),
            "no_pools" => Ok(Self::NoPools),
            "inconsistent_result" => Ok(Self::InconsistentResult),
            "internal" => Ok(Self::Internal),
            "invalid_request" => Ok(Self::InvalidRequest),
            _ => Err(D::Error::custom(format!(
                "unknown quote failure kind: {value}"
            ))),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QuoteFailure {
    pub kind: QuoteFailureKind,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PoolSimulationOutcome {
    pub pool: String,
    pub pool_name: String,
    pub pool_address: String,
    pub protocol: String,
    pub outcome: PoolOutcomeKind,
    pub reported_steps: usize,
    pub expected_steps: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QuoteMeta {
    pub status: QuoteStatus,
    pub result_quality: QuoteResultQuality,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial_kind: Option<QuotePartialKind>,
    pub block_number: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vm_block_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rfq_block_number: Option<u64>,
    pub matching_pools: usize,
    pub candidate_pools: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_pools: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auction_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pool_results: Vec<PoolSimulationOutcome>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub vm_unavailable: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub rfq_unavailable: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<QuoteFailure>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QuoteResult {
    pub request_id: String,
    pub data: Vec<AmountOutResponse>,
    pub meta: QuoteMeta,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PoolRef {
    pub protocol: String,
    pub component_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_address: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PoolSwapDraft {
    pub pool: PoolRef,
    pub token_in: String,
    pub token_out: String,
    pub split_bps: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HopDraft {
    pub token_in: String,
    pub token_out: String,
    pub swaps: Vec<PoolSwapDraft>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SegmentDraft {
    pub kind: SwapKind,
    pub share_bps: u32,
    pub hops: Vec<HopDraft>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum SwapKind {
    SimpleSwap,
    MultiSwap,
    MegaSwap,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RouteEncodeRequest {
    pub chain_id: u64,
    pub token_in: String,
    pub token_out: String,
    pub amount_in: String,
    pub min_amount_out: String,
    pub settlement_address: String,
    pub tycho_router_address: String,
    pub swap_kind: SwapKind,
    pub segments: Vec<SegmentDraft>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_amount_in: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum InteractionKind {
    #[serde(rename = "ERC20_APPROVE")]
    Erc20Approve,
    #[serde(rename = "CALL")]
    Call,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Interaction {
    pub kind: InteractionKind,
    pub target: String,
    pub value: String,
    pub calldata: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ResimulationDebug {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tycho_state_tag: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RouteDebug {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resimulation: Option<ResimulationDebug>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RouteEncodeResponse {
    pub interactions: Vec<Interaction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub debug: Option<RouteDebug>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EncodeErrorResponse {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{
        AmountOutResponse, PoolOutcomeKind, QuoteMeta, QuotePartialKind, QuoteResultQuality,
        QuoteStatus, RouteEncodeRequest,
    };
    use anyhow::Result;

    #[test]
    fn quote_result_quality_serializes_as_snake_case() -> Result<()> {
        assert_eq!(
            serde_json::to_string(&QuoteResultQuality::RequestLevelFailure)?,
            "\"request_level_failure\""
        );
        assert_eq!(
            serde_json::to_string(&QuoteResultQuality::NoResults)?,
            "\"no_results\""
        );
        Ok(())
    }

    #[test]
    fn quote_status_serializes_without_partial_success() -> Result<()> {
        assert_eq!(serde_json::to_string(&QuoteStatus::Ready)?, "\"ready\"");
        assert_eq!(
            serde_json::to_string(&QuoteStatus::InternalError)?,
            "\"internal_error\""
        );
        Ok(())
    }

    #[test]
    fn quote_partial_kind_serializes_as_snake_case() -> Result<()> {
        assert_eq!(
            serde_json::to_string(&QuotePartialKind::AmountLadders)?,
            "\"amount_ladders\""
        );
        assert_eq!(
            serde_json::to_string(&QuotePartialKind::PoolCoverage)?,
            "\"pool_coverage\""
        );
        assert_eq!(
            serde_json::to_string(&QuotePartialKind::Mixed)?,
            "\"mixed\""
        );
        Ok(())
    }

    #[test]
    fn pool_outcome_kind_serializes_as_snake_case() -> Result<()> {
        assert_eq!(
            serde_json::to_string(&PoolOutcomeKind::PartialOutput)?,
            "\"partial_output\""
        );
        Ok(())
    }

    #[test]
    fn quote_meta_omits_partial_kind_when_absent() -> Result<()> {
        let meta = QuoteMeta {
            status: QuoteStatus::Ready,
            result_quality: QuoteResultQuality::Complete,
            partial_kind: None,
            block_number: 1,
            vm_block_number: None,
            rfq_block_number: None,
            matching_pools: 0,
            candidate_pools: 0,
            total_pools: None,
            auction_id: None,
            pool_results: Vec::new(),
            vm_unavailable: false,
            rfq_unavailable: false,
            failures: Vec::new(),
        };
        let value = serde_json::to_value(meta)?;
        assert!(value.get("partial_kind").is_none());
        Ok(())
    }

    #[test]
    fn amount_out_response_drops_limit_max_out_from_public_serialization() -> Result<()> {
        let response: AmountOutResponse = serde_json::from_value(serde_json::json!({
            "pool": "pool-1",
            "pool_name": "Pool 1",
            "pool_address": "0x0000000000000000000000000000000000000001",
            "amounts_out": ["10"],
            "slippage": [3],
            "limit_max_in": "100",
            "limit_max_out": "99",
            "gas_used": [1],
            "block_number": 1
        }))?;

        let value = serde_json::to_value(response)?;

        assert!(value.get("limit_max_out").is_none());
        Ok(())
    }

    #[test]
    fn route_encode_request_accepts_estimated_amount_in() -> Result<()> {
        let request: RouteEncodeRequest = serde_json::from_value(serde_json::json!({
            "chainId": 1,
            "tokenIn": "0x0000000000000000000000000000000000000001",
            "tokenOut": "0x0000000000000000000000000000000000000002",
            "amountIn": "10",
            "minAmountOut": "8",
            "settlementAddress": "0x0000000000000000000000000000000000000003",
            "tychoRouterAddress": "0x0000000000000000000000000000000000000004",
            "swapKind": "SimpleSwap",
            "segments": [],
            "estimatedAmountIn": "10"
        }))?;

        assert_eq!(request.estimated_amount_in.as_deref(), Some("10"));
        Ok(())
    }
}
