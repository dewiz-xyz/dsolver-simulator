use num_bigint::BigUint;
use num_traits::Zero;
use std::collections::HashMap;
use tycho_execution::encoding::errors::EncodingError;
use tycho_execution::encoding::models::{EncodedSolution, Solution, Transaction};
use tycho_execution::encoding::tycho_encoder::TychoEncoder;
use tycho_simulation::tycho_common::models::token::Token;
use tycho_simulation::tycho_common::simulation::protocol_sim::ProtocolSim;
use tycho_simulation::tycho_common::Bytes;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub(super) struct MockProtocolSim;

#[typetag::serde]
impl ProtocolSim for MockProtocolSim {
    fn fee(&self) -> f64 {
        0.0
    }

    fn spot_price(
        &self,
        _base: &Token,
        _quote: &Token,
    ) -> Result<f64, tycho_simulation::tycho_common::simulation::errors::SimulationError> {
        Ok(0.0)
    }

    fn get_amount_out(
        &self,
        amount_in: BigUint,
        _token_in: &Token,
        _token_out: &Token,
    ) -> Result<
        tycho_simulation::tycho_common::simulation::protocol_sim::GetAmountOutResult,
        tycho_simulation::tycho_common::simulation::errors::SimulationError,
    > {
        Ok(
            tycho_simulation::tycho_common::simulation::protocol_sim::GetAmountOutResult::new(
                amount_in,
                BigUint::zero(),
                self.clone_box(),
            ),
        )
    }

    fn get_limits(
        &self,
        _sell_token: Bytes,
        _buy_token: Bytes,
    ) -> Result<
        (BigUint, BigUint),
        tycho_simulation::tycho_common::simulation::errors::SimulationError,
    > {
        Ok((BigUint::zero(), BigUint::zero()))
    }

    fn delta_transition(
        &mut self,
        _delta: tycho_simulation::tycho_common::dto::ProtocolStateDelta,
        _tokens: &HashMap<Bytes, Token>,
        _balances: &tycho_simulation::tycho_common::simulation::protocol_sim::Balances,
    ) -> Result<(), tycho_simulation::tycho_common::simulation::errors::TransitionError> {
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn ProtocolSim> {
        Box::new(Self {})
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn eq(&self, _other: &dyn ProtocolSim) -> bool {
        true
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub(super) struct StepProtocolSim {
    pub(super) multiplier: u32,
}

#[typetag::serde]
impl ProtocolSim for StepProtocolSim {
    fn fee(&self) -> f64 {
        0.0
    }

    fn spot_price(
        &self,
        _base: &Token,
        _quote: &Token,
    ) -> Result<f64, tycho_simulation::tycho_common::simulation::errors::SimulationError> {
        Ok(0.0)
    }

    fn get_amount_out(
        &self,
        amount_in: BigUint,
        _token_in: &Token,
        _token_out: &Token,
    ) -> Result<
        tycho_simulation::tycho_common::simulation::protocol_sim::GetAmountOutResult,
        tycho_simulation::tycho_common::simulation::errors::SimulationError,
    > {
        let amount_out = amount_in * BigUint::from(self.multiplier);
        let next = StepProtocolSim {
            multiplier: self.multiplier + 1,
        };
        Ok(
            tycho_simulation::tycho_common::simulation::protocol_sim::GetAmountOutResult::new(
                amount_out,
                BigUint::zero(),
                Box::new(next),
            ),
        )
    }

    fn get_limits(
        &self,
        _sell_token: Bytes,
        _buy_token: Bytes,
    ) -> Result<
        (BigUint, BigUint),
        tycho_simulation::tycho_common::simulation::errors::SimulationError,
    > {
        Ok((BigUint::zero(), BigUint::zero()))
    }

    fn delta_transition(
        &mut self,
        _delta: tycho_simulation::tycho_common::dto::ProtocolStateDelta,
        _tokens: &HashMap<Bytes, Token>,
        _balances: &tycho_simulation::tycho_common::simulation::protocol_sim::Balances,
    ) -> Result<(), tycho_simulation::tycho_common::simulation::errors::TransitionError> {
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn ProtocolSim> {
        Box::new(Self {
            multiplier: self.multiplier,
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn eq(&self, other: &dyn ProtocolSim) -> bool {
        other
            .as_any()
            .downcast_ref::<StepProtocolSim>()
            .is_some_and(|rhs| rhs.multiplier == self.multiplier)
    }
}

#[expect(
    clippy::expect_used,
    reason = "Tests only call this helper with StepProtocolSim states."
)]
pub(super) fn step_multiplier(state: &dyn ProtocolSim) -> u32 {
    state
        .as_any()
        .downcast_ref::<StepProtocolSim>()
        .expect("Expected StepProtocolSim")
        .multiplier
}

pub(super) struct MockTychoEncoder {
    signature: String,
    router: Bytes,
    swaps: Vec<u8>,
}

impl MockTychoEncoder {
    pub(super) fn new(signature: &str, router: Bytes) -> Self {
        Self {
            signature: signature.to_string(),
            router,
            swaps: vec![0x01, 0x02],
        }
    }
}

impl TychoEncoder for MockTychoEncoder {
    fn encode_solutions(
        &self,
        _solutions: Vec<Solution>,
    ) -> Result<Vec<EncodedSolution>, EncodingError> {
        Ok(vec![EncodedSolution {
            swaps: self.swaps.clone(),
            interacting_with: self.router.clone(),
            function_signature: self.signature.clone(),
            n_tokens: 0,
            permit: None,
        }])
    }

    fn encode_full_calldata(
        &self,
        _solutions: Vec<Solution>,
    ) -> Result<Vec<Transaction>, EncodingError> {
        Err(EncodingError::FatalError(
            "encode_full_calldata not supported in tests".to_string(),
        ))
    }

    fn validate_solution(&self, _solution: &Solution) -> Result<(), EncodingError> {
        Ok(())
    }
}
