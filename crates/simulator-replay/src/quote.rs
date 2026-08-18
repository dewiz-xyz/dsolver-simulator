use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};

use alloy_primitives::U256;
use num_bigint::BigUint;
use thiserror::Error;
use tycho_simulation::{
    protocol::models::ProtocolComponent,
    tycho_common::{
        models::token::Token,
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
        if !component_supports_direction(component.as_ref(), &request.token_in, &request.token_out)
        {
            return Err(ReplayQuoteError::DirectionUnsupported(
                request.component_id.clone(),
            ));
        }

        let token_in = self
            .token(&request.token_in)
            .ok_or_else(|| ReplayQuoteError::TokenMissing(request.token_in.clone()))?;
        let token_out = self
            .token(&request.token_out)
            .ok_or_else(|| ReplayQuoteError::TokenMissing(request.token_out.clone()))?;
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
