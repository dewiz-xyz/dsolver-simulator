use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};

use alloy_primitives::U256;
use num_bigint::BigUint;
use thiserror::Error;
use tycho_simulation::{
    protocol::models::ProtocolComponent,
    tycho_common::{
        models::{token::Token, Chain},
        simulation::{
            errors::SimulationError,
            protocol_sim::{GetAmountOutResult, ProtocolSim},
        },
        Bytes,
    },
};

use crate::{ReplayBackend, StatePoint};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactPoolQuote {
    pub backend: ReplayBackend,
    pub protocol: String,
    pub component_id: String,
    pub token_in: Bytes,
    pub token_out: Bytes,
    pub amount_in: U256,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ReplayQuoteError {
    #[error("component not found: {0}")]
    ComponentNotFound(String),
    #[error("component {component_id} belongs to {actual}, not {expected}")]
    BackendMismatch {
        component_id: String,
        expected: &'static str,
        actual: &'static str,
    },
    #[error("component {component_id} belongs to protocol {actual}, not {expected}")]
    ProtocolMismatch {
        component_id: String,
        expected: String,
        actual: String,
    },
    #[error("component {0} does not support the requested token direction")]
    DirectionUnsupported(String),
    #[error("token metadata is missing for {0}")]
    TokenMissing(Bytes),
    #[error("pool simulation failed: {0}")]
    SimulationFailed(String),
    #[error("pool simulation panicked: {0}")]
    SimulationPanicked(String),
    #[error("pool simulation returned zero output")]
    ZeroOutput,
    #[error("pool simulation output exceeds 256 bits")]
    AmountOverflow,
}

impl StatePoint {
    pub fn quote(&self, request: &ExactPoolQuote) -> Result<U256, ReplayQuoteError> {
        let Some((kind, (pool_state, component))) =
            self.pool_by_id_with_kind(&request.component_id)
        else {
            return Err(ReplayQuoteError::ComponentNotFound(
                request.component_id.clone(),
            ));
        };
        let actual_backend = ReplayBackend::from_protocol_kind(kind);
        if actual_backend != request.backend {
            return Err(ReplayQuoteError::BackendMismatch {
                component_id: request.component_id.clone(),
                expected: request.backend.as_str(),
                actual: actual_backend.as_str(),
            });
        }
        if component.protocol_system != request.protocol {
            return Err(ReplayQuoteError::ProtocolMismatch {
                component_id: request.component_id.clone(),
                expected: request.protocol.clone(),
                actual: component.protocol_system.clone(),
            });
        }
        let requested_token_in = self
            .token(&request.token_in)
            .ok_or_else(|| ReplayQuoteError::TokenMissing(request.token_in.clone()))?;
        let requested_token_out = self
            .token(&request.token_out)
            .ok_or_else(|| ReplayQuoteError::TokenMissing(request.token_out.clone()))?;
        let token_in = remap_native_token_for_component(requested_token_in, component.as_ref());
        let token_out = remap_native_token_for_component(requested_token_out, component.as_ref());
        if !component_supports_direction(component.as_ref(), &token_in.address, &token_out.address)
        {
            return Err(ReplayQuoteError::DirectionUnsupported(
                request.component_id.clone(),
            ));
        }

        let amount_in = BigUint::from_bytes_be(&request.amount_in.to_be_bytes::<32>());
        let result = catch_unwind(AssertUnwindSafe(|| {
            simulate_amount_out(pool_state.as_ref(), amount_in, &token_in, &token_out)
        }))
        .map_err(|payload| {
            ReplayQuoteError::SimulationPanicked(panic_payload_message(payload.as_ref()))
        })?
        .map_err(|error| ReplayQuoteError::SimulationFailed(error.to_string()))?;
        if result.amount == BigUint::from(0_u8) {
            return Err(ReplayQuoteError::ZeroOutput);
        }
        let bytes = result.amount.to_bytes_be();
        if bytes.len() > 32 {
            return Err(ReplayQuoteError::AmountOverflow);
        }
        Ok(U256::from_be_slice(&bytes))
    }
}

fn remap_native_token_for_component(requested: Token, component: &ProtocolComponent) -> Token {
    // Custom chains still need the registry lookup, which can fail after direct deserialization.
    if !matches!(requested.chain, Chain::Custom(_))
        && component
            .tokens
            .iter()
            .any(|token| token.address == requested.address)
    {
        return requested;
    }

    let native = requested.chain.native_token();
    let wrapped = requested.chain.wrapped_native_token();
    if native.address == wrapped.address {
        return requested;
    }

    let has_native = component
        .tokens
        .iter()
        .any(|token| token.address == native.address);
    let has_wrapped = component
        .tokens
        .iter()
        .any(|token| token.address == wrapped.address);
    if requested.address == wrapped.address && has_native && !has_wrapped {
        native
    } else if requested.address == native.address && has_wrapped && !has_native {
        wrapped
    } else {
        requested
    }
}

fn component_supports_direction(
    component: &ProtocolComponent,
    token_in: &Bytes,
    token_out: &Bytes,
) -> bool {
    if token_in == token_out {
        return false;
    }

    if is_directional_rfq_component(component) {
        return component
            .tokens
            .first()
            .zip(component.tokens.get(1))
            .is_some_and(|(expected_in, expected_out)| {
                expected_in.address == *token_in && expected_out.address == *token_out
            });
    }

    component
        .tokens
        .iter()
        .any(|token| token.address == *token_in)
        && component
            .tokens
            .iter()
            .any(|token| token.address == *token_out)
}

fn is_directional_rfq_component(component: &ProtocolComponent) -> bool {
    matches!(
        (
            component.protocol_system.as_str(),
            component.protocol_type_name.as_str()
        ),
        ("rfq:hashflow", _) | (_, "hashflow_pool") | ("rfq:liquorice", _) | (_, "liquorice_pool")
    )
}

pub fn simulate_amount_out(
    pool_state: &dyn ProtocolSim,
    amount_in: BigUint,
    token_in: &Token,
    token_out: &Token,
) -> Result<GetAmountOutResult, SimulationError> {
    pool_state.get_amount_out(amount_in, token_in, token_out)
}

fn panic_payload_message(payload: &(dyn Any + Send + 'static)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "unknown panic payload".to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::NaiveDateTime;
    use tycho_simulation::tycho_common::models::Chain;

    use super::*;

    fn component(tokens: Vec<Token>) -> ProtocolComponent {
        ProtocolComponent::new(
            Bytes::from([1; 20]),
            "uniswap_v2".to_owned(),
            "uniswap_v2_pool".to_owned(),
            Chain::Ethereum,
            tokens,
            Vec::new(),
            HashMap::new(),
            Bytes::new(),
            NaiveDateTime::default(),
        )
    }

    fn distinctive_metadata(mut token: Token) -> Token {
        token.symbol = "REQUESTED".to_owned();
        token.decimals = 7;
        token.tax = 13;
        token.gas = vec![None, Some(31), Some(47)];
        token.quality = 59;
        token
    }

    #[test]
    fn remapping_preserves_metadata_without_alias_substitution() -> Result<(), serde_json::Error> {
        let native = Chain::Ethereum.native_token();
        let wrapped = Chain::Ethereum.wrapped_native_token();
        let ordinary = Token::new(&Bytes::from([2; 20]), "OTHER", 18, 0, &[], Chain::Base, 100);
        let starknet_native = Chain::Starknet.native_token();
        let cases = [
            ("ordinary present", ordinary.clone(), vec![ordinary.clone()]),
            ("ordinary absent", ordinary, Vec::new()),
            ("native present", native.clone(), vec![native.clone()]),
            ("wrapped present", wrapped.clone(), vec![wrapped.clone()]),
            (
                "both aliases native",
                native.clone(),
                vec![native.clone(), wrapped.clone()],
            ),
            (
                "both aliases wrapped",
                wrapped.clone(),
                vec![native.clone(), wrapped.clone()],
            ),
            (
                "duplicate native",
                native.clone(),
                vec![native.clone(), native],
            ),
            (
                "duplicate wrapped",
                wrapped.clone(),
                vec![wrapped.clone(), wrapped],
            ),
            (
                "equal alias present",
                starknet_native.clone(),
                vec![starknet_native.clone()],
            ),
            ("equal alias absent", starknet_native, Vec::new()),
        ];

        for (name, requested, component_tokens) in cases {
            let requested = distinctive_metadata(requested);
            let expected = serde_json::to_value(&requested)?;
            let actual = remap_native_token_for_component(requested, &component(component_tokens));
            assert_eq!(serde_json::to_value(actual)?, expected, "{name}");
        }
        Ok(())
    }

    #[test]
    fn alias_substitution_uses_canonical_chain_metadata() -> Result<(), serde_json::Error> {
        let native = Chain::Ethereum.native_token();
        let wrapped = Chain::Ethereum.wrapped_native_token();
        for (requested, expected) in [(wrapped.clone(), native.clone()), (native, wrapped)] {
            let requested = distinctive_metadata(requested);
            let component = component(vec![distinctive_metadata(expected.clone())]);
            let actual = remap_native_token_for_component(requested, &component);
            assert_eq!(
                serde_json::to_value(actual)?,
                serde_json::to_value(expected)?
            );
        }
        Ok(())
    }
}
