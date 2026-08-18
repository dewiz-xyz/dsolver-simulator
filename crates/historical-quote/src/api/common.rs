use std::{cmp::Ordering, collections::BTreeSet, fmt, str::FromStr};

use num_bigint::BigUint;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use utoipa::openapi::{
    schema::{ArrayBuilder, ObjectBuilder, Schema, SchemaFormat, Type},
    KnownFormat, RefOr,
};
use utoipa::{PartialSchema, ToSchema};
use uuid::{Uuid, Version};

/// The only public API revision served by V1.
pub const API_REVISION: u32 = 1;

pub(crate) const BASE_CHAIN_ID: u64 = 8453;

/// Simulator state partition used by a historical request.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Backend {
    /// Native Tycho pool state.
    Native,
    /// Tycho VM state. V1 keeps this disabled in production.
    Vm,
    /// Stored RFQ price levels.
    Rfq,
}

/// One ordered position in state history.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ToSchema,
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamPosition {
    pub generation: u64,
    pub message_seq: u64,
}

/// Exact pool and token direction selected by the caller.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PoolSelector {
    pub backend: Backend,
    #[schema(min_length = 1)]
    pub protocol: String,
    #[schema(min_length = 1)]
    pub component_id: String,
    #[schema(min_length = 1)]
    pub token_in: String,
    #[schema(min_length = 1)]
    pub token_out: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UncheckedPoolSelector {
    backend: Backend,
    protocol: String,
    component_id: String,
    token_in: String,
    token_out: String,
}

impl<'de> Deserialize<'de> for PoolSelector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = UncheckedPoolSelector::deserialize(deserializer)?;
        for (field, selector) in [
            ("protocol", value.protocol.as_str()),
            ("componentId", value.component_id.as_str()),
            ("tokenIn", value.token_in.as_str()),
            ("tokenOut", value.token_out.as_str()),
        ] {
            if selector.is_empty() {
                return Err(de::Error::custom(format_args!(
                    "{field} must be a non-empty exact selector"
                )));
            }
        }

        Ok(Self {
            backend: value.backend,
            protocol: value.protocol,
            component_id: value.component_id,
            token_in: value.token_in,
            token_out: value.token_out,
        })
    }
}

/// Inclusive block range used by quote requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BlockRange {
    pub start: u64,
    pub end_inclusive: u64,
    #[schema(required = false, default = 1, minimum = 1)]
    pub step: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UncheckedBlockRange {
    start: u64,
    end_inclusive: u64,
    #[serde(default = "default_block_step")]
    step: u64,
}

const fn default_block_step() -> u64 {
    1
}

impl<'de> Deserialize<'de> for BlockRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = UncheckedBlockRange::deserialize(deserializer)?;
        if value.start > value.end_inclusive {
            return Err(de::Error::custom(
                "blocks.start must not exceed blocks.endInclusive",
            ));
        }
        if value.step == 0 {
            return Err(de::Error::custom("blocks.step must be a positive integer"));
        }

        Ok(Self {
            start: value.start,
            end_inclusive: value.end_inclusive,
            step: value.step,
        })
    }
}

impl BlockRange {
    /// Expands the inclusive start block sequence.
    pub fn start_blocks(self) -> impl Iterator<Item = u64> {
        std::iter::successors(Some(self.start), move |block| {
            block
                .checked_add(self.step)
                .filter(|next| *next <= self.end_inclusive)
        })
    }
}

/// Optional inclusive bounds used by coverage requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BlockBounds {
    pub start: u64,
    pub end_inclusive: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UncheckedBlockBounds {
    start: u64,
    end_inclusive: u64,
}

impl<'de> Deserialize<'de> for BlockBounds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = UncheckedBlockBounds::deserialize(deserializer)?;
        if value.start > value.end_inclusive {
            return Err(de::Error::custom(
                "bounds.start must not exceed bounds.endInclusive",
            ));
        }

        Ok(Self {
            start: value.start,
            end_inclusive: value.end_inclusive,
        })
    }
}

/// Canonical unsigned integer in token smallest units.
#[derive(Debug, Clone, PartialEq, Eq, Hash, ToSchema)]
#[schema(value_type = String, pattern = r"^[0-9]+$", example = "1000000")]
pub struct UnsignedAmount(String);

impl UnsignedAmount {
    /// Returns the canonical base-10 representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for UnsignedAmount {
    type Err = UnsignedAmountError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(UnsignedAmountError);
        }
        let amount = BigUint::from_str(value).map_err(|_| UnsignedAmountError)?;
        Ok(Self(amount.to_str_radix(10)))
    }
}

impl fmt::Display for UnsignedAmount {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for UnsignedAmount {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for UnsignedAmount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(de::Error::custom)
    }
}

impl PartialOrd for UnsignedAmount {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for UnsignedAmount {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .len()
            .cmp(&other.0.len())
            .then_with(|| self.0.cmp(&other.0))
    }
}

/// Error returned for a non-decimal unsigned amount.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("input amount must be an unsigned base-10 integer string")]
pub struct UnsignedAmountError;

/// Reason state history has a known unsafe interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum GapKind {
    MissingCheckpoint,
    Recorded,
    IncompleteTail,
}

/// Stored cause for a recorded gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum GapCause {
    QueueOverflow,
    WriteFailed,
    WriterStopped,
    CheckpointFailed,
    BoundarySlip,
}

/// Gap details shared by quote and coverage responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KnownGap {
    pub kind: GapKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<GapCause>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_position: Option<StreamPosition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_position: Option<StreamPosition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_block: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_block_inclusive: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_observed_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_observed_at_ms: Option<u64>,
}

/// Response-local gap reference in a quote result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GapReference {
    #[serde(rename = "ref")]
    pub reference: String,
    pub kind: GapKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<GapCause>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_position: Option<StreamPosition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_position: Option<StreamPosition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_block: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_block_inclusive: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_observed_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_observed_at_ms: Option<u64>,
}

pub(crate) fn validate_api_revision(api_revision: u32) -> Result<(), String> {
    if api_revision == API_REVISION {
        Ok(())
    } else {
        Err(format!("apiRevision must equal {API_REVISION}"))
    }
}

pub(crate) fn validate_chain_id(chain_id: u64) -> Result<(), String> {
    if chain_id == BASE_CHAIN_ID {
        Ok(())
    } else {
        Err(format!("chainId must equal {BASE_CHAIN_ID}"))
    }
}

pub(crate) fn validate_timeout(timeout_ms: u64) -> Result<(), String> {
    if timeout_ms > 0 {
        Ok(())
    } else {
        Err("timeoutMs must be a positive integer".to_owned())
    }
}

pub(crate) fn validate_backends(backends: &[Backend]) -> Result<(), String> {
    if backends.is_empty() {
        return Err("backends must be non-empty".to_owned());
    }
    let distinct = backends.iter().copied().collect::<BTreeSet<_>>();
    if distinct.len() != backends.len() {
        return Err("backends must be duplicate-free".to_owned());
    }
    Ok(())
}

pub(crate) fn positive_unique_lags_schema() -> RefOr<Schema> {
    ArrayBuilder::new()
        .items(
            ObjectBuilder::new()
                .schema_type(Type::Integer)
                .format(Some(SchemaFormat::KnownFormat(KnownFormat::Int64)))
                .minimum(Some(1)),
        )
        .min_items(Some(1))
        .unique_items(true)
        .into()
}

pub(crate) fn unique_backends_schema() -> RefOr<Schema> {
    ArrayBuilder::new()
        .items(Backend::schema())
        .min_items(Some(1))
        .unique_items(true)
        .into()
}

pub(crate) fn unique_amounts_schema() -> RefOr<Schema> {
    ArrayBuilder::new()
        .items(UnsignedAmount::schema())
        .min_items(Some(1))
        .unique_items(true)
        .description(Some(
            "Input amounts must also be numerically distinct after canonical unsigned-integer parsing.",
        ))
        .into()
}

pub(crate) fn deserialize_uuid_v4<'de, D>(deserializer: D) -> Result<Uuid, D::Error>
where
    D: Deserializer<'de>,
{
    let uuid = Uuid::deserialize(deserializer)?;
    if uuid.get_version() == Some(Version::Random) {
        Ok(uuid)
    } else {
        Err(de::Error::custom("value must be a UUID version 4"))
    }
}
