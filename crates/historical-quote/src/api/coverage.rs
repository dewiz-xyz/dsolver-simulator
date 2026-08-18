use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    validate_api_revision, validate_backends, validate_chain_id, Backend, BlockBounds, KnownGap,
};

/// Request for continuous state reconstruction coverage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(
    rename_all = "camelCase",
    deny_unknown_fields,
    try_from = "UncheckedCoverageRequest"
)]
pub struct CoverageRequest {
    #[schema(minimum = 1, maximum = 1)]
    pub api_revision: u32,
    #[schema(minimum = 8453, maximum = 8453)]
    pub chain_id: u64,
    #[schema(schema_with = crate::api::common::unique_backends_schema)]
    pub backends: Vec<Backend>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bounds: Option<BlockBounds>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UncheckedCoverageRequest {
    api_revision: u32,
    chain_id: u64,
    backends: Vec<Backend>,
    bounds: Option<BlockBounds>,
}

impl TryFrom<UncheckedCoverageRequest> for CoverageRequest {
    type Error = String;

    fn try_from(value: UncheckedCoverageRequest) -> Result<Self, Self::Error> {
        validate_api_revision(value.api_revision)?;
        validate_chain_id(value.chain_id)?;
        validate_backends(&value.backends)?;

        Ok(Self {
            api_revision: value.api_revision,
            chain_id: value.chain_id,
            backends: value.backends,
            bounds: value.bounds,
        })
    }
}

/// Continuous reconstructible intervals visible on the read replica.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CoverageResponse {
    #[schema(minimum = 1, maximum = 1)]
    pub api_revision: u32,
    #[schema(minimum = 8453, maximum = 8453)]
    pub chain_id: u64,
    pub backends: Vec<Backend>,
    pub visible_through_block: u64,
    pub continuous_intervals: Vec<BlockBounds>,
    pub known_gaps: Vec<KnownGap>,
}
