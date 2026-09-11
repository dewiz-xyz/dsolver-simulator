use std::str::FromStr;

use alloy_primitives::hex::encode_prefixed;
use num_bigint::BigUint;
use tycho_simulation::tycho_common::Bytes;

use super::EncodeError;

pub(super) fn parse_amount(value: &str) -> Result<BigUint, EncodeError> {
    BigUint::from_str(value).map_err(|_| EncodeError::invalid(format!("Invalid amount: {}", value)))
}

pub(super) fn parse_address(value: &str) -> Result<Bytes, EncodeError> {
    let trimmed = value.trim();
    let bytes = Bytes::from_str(trimmed)
        .map_err(|err| EncodeError::invalid(format!("Invalid address {}: {}", value, err)))?;
    if bytes.len() != 20 {
        return Err(EncodeError::invalid(format!(
            "Invalid address length for {}",
            value
        )));
    }
    Ok(bytes)
}

pub(super) fn format_address(bytes: &Bytes) -> String {
    bytes.to_string()
}

pub(super) fn format_calldata(data: &[u8]) -> String {
    encode_prefixed(data)
}

pub(super) fn biguint_to_u256_checked(
    value: &BigUint,
    label: &str,
) -> Result<alloy_primitives::U256, EncodeError> {
    let bytes = value.to_bytes_be();
    if bytes.len() > 32 {
        return Err(EncodeError::invalid(format!("{} must fit uint256", label)));
    }

    Ok(alloy_primitives::U256::from_be_slice(&bytes))
}
