use std::sync::Arc;

use alloy_primitives::Keccak256;
use alloy_sol_types::SolValue;
use num_bigint::BigUint;
use num_traits::Zero;
use tycho_execution::encoding::{errors::EncodingError, evm::utils::bytes_to_address};
use tycho_execution::encoding::{
    evm::encoder_builders::TychoRouterEncoderBuilder,
    evm::swap_encoder::swap_encoder_registry::SwapEncoderRegistry,
    models::{EncodedSolution, Solution, UserTransferType},
    tycho_encoder::TychoEncoder,
};
use tycho_simulation::tycho_common::{models::Chain, Bytes};

use crate::models::messages::{Interaction, InteractionKind, RouteEncodeRequest};

use super::error::map_encoding_error;
use super::model::ResimulatedRouteInternal;
use super::tycho_swaps::build_route_swaps;
use super::wire::{biguint_to_u256_checked, format_address, format_calldata};
use super::EncodeError;

pub(super) struct CallDataPayload {
    target: Bytes,
    value: BigUint,
    calldata: Vec<u8>,
}

pub(super) struct RouteContext<'a> {
    pub(super) request: &'a RouteEncodeRequest,
    pub(super) token_in: &'a Bytes,
    pub(super) token_out: &'a Bytes,
    pub(super) amount_in: &'a BigUint,
    pub(super) router_address: &'a Bytes,
    pub(super) is_native_input: bool,
}

pub(super) fn build_encoder(
    chain: Chain,
    router_address: Bytes,
) -> Result<Arc<dyn TychoEncoder>, EncodeError> {
    // TODO: we can probably optimize this in the feature to initialize the encoder registry + add default encoders during the server's startup.
    let swap_encoder_registry = SwapEncoderRegistry::new(chain)
        .add_default_encoders(None)
        .map_err(|err| {
            EncodeError::encoding(format!(
                "Failed to build swap encoder registry for {}: {}",
                chain, err
            ))
        })?;

    TychoRouterEncoderBuilder::new()
        .chain(chain)
        .user_transfer_type(UserTransferType::TransferFrom)
        .swap_encoder_registry(swap_encoder_registry)
        .router_address(router_address)
        .build()
        .map(Arc::from)
        .map_err(|err| EncodeError::encoding(format!("Failed to build Tycho encoder: {}", err)))
}

pub(super) fn build_route_calldata_tx(
    context: &RouteContext<'_>,
    resimulated: &ResimulatedRouteInternal,
    encoder: &dyn TychoEncoder,
    min_amount_out: &BigUint,
) -> Result<CallDataPayload, EncodeError> {
    let swaps = build_route_swaps(resimulated)?;
    let sender = super::wire::parse_address(&context.request.settlement_address)?;
    let receiver = sender.clone();

    let solution = Solution {
        sender,
        receiver: receiver.clone(),
        given_token: context.token_in.clone(),
        given_amount: context.amount_in.clone(),
        checked_token: context.token_out.clone(),
        exact_out: false,
        checked_amount: min_amount_out.clone(),
        swaps,
        native_action: None,
    };

    let encoded_solution = encoder
        .encode_solutions(vec![solution.clone()])
        .map_err(|err| match err {
            EncodingError::InvalidInput(_) => {
                EncodeError::invalid(format!("Route encoding failed: {}", err))
            }
            _ => EncodeError::encoding(format!("Route encoding failed: {}", err)),
        })?
        .into_iter()
        .next()
        .ok_or_else(|| EncodeError::encoding("Route encoding returned no solutions"))?;

    if encoded_solution.permit.is_some()
        || encoded_solution.function_signature.contains("Permit2")
        || encoded_solution.function_signature.contains("permit2")
    {
        return Err(EncodeError::encoding(
            "Permit2 encodings are not supported for route calldata",
        ));
    }

    if &encoded_solution.interacting_with != context.router_address {
        return Err(EncodeError::encoding(
            "Encoded router address does not match request tychoRouterAddress",
        ));
    }

    let is_transfer_from_allowed = !context.is_native_input;
    let calldata = encode_route_calldata(
        &encoded_solution,
        &solution,
        &receiver,
        is_transfer_from_allowed,
    )?;
    let value = if context.is_native_input {
        context.amount_in.clone()
    } else {
        BigUint::zero()
    };

    Ok(CallDataPayload {
        target: encoded_solution.interacting_with,
        value,
        calldata,
    })
}

pub(super) fn build_settlement_interactions(
    token_in: &Bytes,
    amount_in: &BigUint,
    router_call: CallDataPayload,
    reset_approval: bool,
    is_native_input: bool,
) -> Result<Vec<Interaction>, EncodeError> {
    let mut interactions = Vec::with_capacity(if is_native_input {
        1
    } else if reset_approval {
        3
    } else {
        2
    });
    if !is_native_input {
        if reset_approval {
            // Reset-then-approve is required for allowance-reset tokens like USDT.
            interactions.push(build_erc20_approve_interaction(
                token_in,
                &router_call.target,
                &BigUint::zero(),
            )?);
        }
        let set_approval =
            build_erc20_approve_interaction(token_in, &router_call.target, amount_in)?;
        interactions.push(set_approval);
    }
    let router_interaction = Interaction {
        kind: InteractionKind::Call,
        target: format_address(&router_call.target),
        value: router_call.value.to_str_radix(10),
        calldata: format_calldata(&router_call.calldata),
    };
    interactions.push(router_interaction);
    Ok(interactions)
}

const ERC20_APPROVE_SIGNATURE: &str = "approve(address,uint256)";

fn build_erc20_approve_interaction(
    token: &Bytes,
    spender: &Bytes,
    amount: &BigUint,
) -> Result<Interaction, EncodeError> {
    let calldata = encode_erc20_approve_calldata(spender, amount)?;
    Ok(Interaction {
        kind: InteractionKind::Erc20Approve,
        target: format_address(token),
        value: "0".to_string(),
        calldata: format_calldata(&calldata),
    })
}

fn encode_erc20_approve_calldata(
    spender: &Bytes,
    amount: &BigUint,
) -> Result<Vec<u8>, EncodeError> {
    let spender = bytes_to_address(spender).map_err(map_encoding_error)?;
    let amount = biguint_to_u256_checked(amount, "approve amount")?;
    let calldata_args = (spender, amount).abi_encode();
    encode_function_call(ERC20_APPROVE_SIGNATURE, calldata_args)
}

fn encode_route_calldata(
    encoded_solution: &EncodedSolution,
    solution: &Solution,
    receiver: &Bytes,
    is_transfer_from_allowed: bool,
) -> Result<Vec<u8>, EncodeError> {
    let signature = encoded_solution.function_signature.as_str();
    // `/encode` preserves route token semantics and does not inject wrap/unwrap steps.
    // Routes must already be protocol-compatible, and native-input routes must disable router
    // `transferFrom`.
    let is_wrap = false;
    let is_unwrap = false;
    let given_amount = biguint_to_u256_checked(&solution.given_amount, "amountIn")?;
    let checked_amount = biguint_to_u256_checked(&solution.checked_amount, "minAmountOut")?;

    let calldata_args = if signature.contains("splitSwap") {
        (
            given_amount,
            bytes_to_address(&solution.given_token).map_err(map_encoding_error)?,
            bytes_to_address(&solution.checked_token).map_err(map_encoding_error)?,
            checked_amount,
            is_wrap,
            is_unwrap,
            alloy_primitives::U256::from(encoded_solution.n_tokens),
            bytes_to_address(receiver).map_err(map_encoding_error)?,
            is_transfer_from_allowed,
            encoded_solution.swaps.clone(),
        )
            .abi_encode_params()
    } else if signature.contains("singleSwap") || signature.contains("sequentialSwap") {
        (
            given_amount,
            bytes_to_address(&solution.given_token).map_err(map_encoding_error)?,
            bytes_to_address(&solution.checked_token).map_err(map_encoding_error)?,
            checked_amount,
            is_wrap,
            is_unwrap,
            bytes_to_address(receiver).map_err(map_encoding_error)?,
            is_transfer_from_allowed,
            encoded_solution.swaps.clone(),
        )
            .abi_encode_params()
    } else {
        return Err(EncodeError::encoding(format!(
            "Unsupported Tycho function signature: {}",
            signature
        )));
    };

    encode_function_call(signature, calldata_args)
}

fn encode_function_call(signature: &str, encoded_args: Vec<u8>) -> Result<Vec<u8>, EncodeError> {
    let selector = function_selector(signature)?;
    let mut call_data = Vec::with_capacity(4 + encoded_args.len());
    call_data.extend_from_slice(&selector);
    call_data.extend(encoded_args);
    Ok(call_data)
}

fn function_selector(signature: &str) -> Result<[u8; 4], EncodeError> {
    let normalized = signature.trim();
    if !normalized.contains('(') {
        return Err(EncodeError::encoding(format!(
            "Invalid function signature: {}",
            signature
        )));
    }
    let mut hasher = Keccak256::new();
    hasher.update(normalized.as_bytes());
    let hash = hasher.finalize();
    Ok([hash[0], hash[1], hash[2], hash[3]])
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    reason = "deterministic encode fixtures use hard-coded addresses and selectors"
)]
#[expect(
    clippy::expect_used,
    reason = "deterministic encode fixtures use hard-coded response assertions"
)]
#[expect(
    clippy::panic,
    reason = "negative test branches are expressed as explicit test invariants"
)]
#[expect(
    clippy::manual_let_else,
    reason = "negative test assertions read more clearly in match form here"
)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::services::encode::backend::PoolBackend;
    use crate::services::encode::fixtures::{dummy_component, pool_ref};
    use crate::services::encode::mocks::{MockProtocolSim, MockTychoEncoder};
    use crate::services::encode::model::{
        ResimulatedHopInternal, ResimulatedSegmentInternal, ResimulatedSwapInternal,
    };

    fn decode_transfer_from_allowed(calldata: &[u8]) -> bool {
        const SELECTOR_BYTES: usize = 4;
        const ABI_WORD_BYTES: usize = 32;
        const TRANSFER_FROM_ALLOWED_INDEX: usize = 7;

        let value_offset =
            SELECTOR_BYTES + (TRANSFER_FROM_ALLOWED_INDEX * ABI_WORD_BYTES) + (ABI_WORD_BYTES - 1);
        calldata
            .get(value_offset)
            .copied()
            .expect("single-swap calldata should include transferFrom flag")
            == 1
    }

    #[test]
    fn build_settlement_interactions_with_reset_approval() {
        let token_in = Bytes::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let router = Bytes::from_str("0x0000000000000000000000000000000000000002").unwrap();
        let amount_in = BigUint::from(10u32);
        let router_call = CallDataPayload {
            target: router.clone(),
            value: BigUint::zero(),
            calldata: vec![0x11, 0x22, 0x33],
        };

        let interactions =
            build_settlement_interactions(&token_in, &amount_in, router_call, true, false).unwrap();

        assert_eq!(interactions.len(), 3);
        assert_eq!(interactions[0].kind, InteractionKind::Erc20Approve);
        assert_eq!(interactions[1].kind, InteractionKind::Erc20Approve);
        assert_eq!(interactions[2].kind, InteractionKind::Call);
        assert_eq!(interactions[0].target, format_address(&token_in));
        assert_eq!(interactions[2].target, format_address(&router));

        let selector = function_selector(ERC20_APPROVE_SIGNATURE).unwrap();
        let expected_prefix = format!("0x{}", alloy_primitives::hex::encode(selector));
        assert!(
            interactions[0].calldata.starts_with(&expected_prefix),
            "approval calldata should start with selector"
        );
        assert!(
            interactions[1].calldata.starts_with(&expected_prefix),
            "approval calldata should start with selector"
        );
        assert_eq!(
            interactions[2].calldata,
            format_calldata(&[0x11, 0x22, 0x33])
        );
    }

    #[test]
    fn build_settlement_interactions_without_reset_approval() {
        let token_in = Bytes::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let router = Bytes::from_str("0x0000000000000000000000000000000000000002").unwrap();
        let amount_in = BigUint::from(10u32);
        let router_call = CallDataPayload {
            target: router.clone(),
            value: BigUint::zero(),
            calldata: vec![0xaa, 0xbb, 0xcc],
        };

        let interactions =
            build_settlement_interactions(&token_in, &amount_in, router_call, false, false)
                .unwrap();

        assert_eq!(interactions.len(), 2);
        assert_eq!(interactions[0].kind, InteractionKind::Erc20Approve);
        assert_eq!(interactions[1].kind, InteractionKind::Call);
        assert_eq!(interactions[0].target, format_address(&token_in));
        assert_eq!(interactions[1].target, format_address(&router));
    }

    #[test]
    fn build_settlement_interactions_skips_approvals_for_native_input() {
        let token_in = Bytes::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let router = Bytes::from_str("0x0000000000000000000000000000000000000002").unwrap();
        let amount_in = BigUint::from(10u32);
        let router_call = CallDataPayload {
            target: router.clone(),
            value: amount_in.clone(),
            calldata: vec![0xde, 0xad, 0xbe, 0xef],
        };

        let interactions =
            build_settlement_interactions(&token_in, &amount_in, router_call, true, true).unwrap();

        assert_eq!(interactions.len(), 1);
        assert_eq!(interactions[0].kind, InteractionKind::Call);
        assert_eq!(interactions[0].target, format_address(&router));
        assert_eq!(interactions[0].value, "10");
    }

    #[test]
    fn build_route_calldata_tx_encodes_router_call() {
        let router = Bytes::from_str("0x0000000000000000000000000000000000000004").unwrap();
        let encoder = MockTychoEncoder::new(
            "singleSwap(uint256,address,address,uint256,bool,bool,address,bool,bytes)",
            router.clone(),
        );

        let request = RouteEncodeRequest {
            chain_id: 1,
            token_in: "0x0000000000000000000000000000000000000001".to_string(),
            token_out: "0x0000000000000000000000000000000000000002".to_string(),
            amount_in: "10".to_string(),
            min_amount_out: "8".to_string(),
            settlement_address: "0x0000000000000000000000000000000000000003".to_string(),
            tycho_router_address: format_address(&router),
            swap_kind: crate::models::messages::SwapKind::SimpleSwap,
            segments: Vec::new(),
            request_id: None,
            estimated_amount_in: None,
        };

        let resimulated = ResimulatedRouteInternal {
            segments: vec![ResimulatedSegmentInternal {
                share_bps: 10_000,
                amount_in: BigUint::from(10u32),
                expected_amount_out: BigUint::from(9u32),
                hops: vec![ResimulatedHopInternal {
                    token_in: Bytes::from_str("0x0000000000000000000000000000000000000001")
                        .unwrap(),
                    token_out: Bytes::from_str("0x0000000000000000000000000000000000000002")
                        .unwrap(),
                    amount_in: BigUint::from(10u32),
                    expected_amount_out: BigUint::from(9u32),
                    swaps: vec![ResimulatedSwapInternal {
                        pool: pool_ref("p1"),
                        backend: PoolBackend::Native,
                        token_in: Bytes::from_str("0x0000000000000000000000000000000000000001")
                            .unwrap(),
                        token_out: Bytes::from_str("0x0000000000000000000000000000000000000002")
                            .unwrap(),
                        split_bps: 10_000,
                        amount_in: BigUint::from(10u32),
                        expected_amount_out: BigUint::from(9u32),
                        pool_state: Arc::new(MockProtocolSim {}),
                        component: Arc::new(dummy_component()),
                    }],
                }],
            }],
        };

        let token_in = super::super::wire::parse_address(&request.token_in).unwrap();
        let token_out = super::super::wire::parse_address(&request.token_out).unwrap();
        let amount_in = super::super::wire::parse_amount(&request.amount_in).unwrap();

        let context = RouteContext {
            request: &request,
            token_in: &token_in,
            token_out: &token_out,
            amount_in: &amount_in,
            router_address: &router,
            is_native_input: false,
        };

        let calldata =
            build_route_calldata_tx(&context, &resimulated, &encoder, &BigUint::from(8u32))
                .unwrap();

        assert_eq!(calldata.target, router);
        assert_eq!(calldata.value, BigUint::zero());
        let selector = function_selector(
            "singleSwap(uint256,address,address,uint256,bool,bool,address,bool,bytes)",
        )
        .unwrap();
        assert_eq!(&calldata.calldata[..4], &selector);
        assert!(
            decode_transfer_from_allowed(&calldata.calldata),
            "ERC20 input should allow router transferFrom"
        );
    }

    #[test]
    fn build_route_calldata_tx_sets_call_value_for_native_input() {
        let native = Chain::Ethereum.native_token().address;
        let token_out = Bytes::from_str("0x0000000000000000000000000000000000000002").unwrap();
        let router = Bytes::from_str("0x0000000000000000000000000000000000000004").unwrap();
        let encoder = MockTychoEncoder::new(
            "singleSwap(uint256,address,address,uint256,bool,bool,address,bool,bytes)",
            router.clone(),
        );

        let request = RouteEncodeRequest {
            chain_id: 1,
            token_in: format_address(&native),
            token_out: format_address(&token_out),
            amount_in: "10".to_string(),
            min_amount_out: "8".to_string(),
            settlement_address: "0x0000000000000000000000000000000000000003".to_string(),
            tycho_router_address: format_address(&router),
            swap_kind: crate::models::messages::SwapKind::SimpleSwap,
            segments: Vec::new(),
            request_id: None,
            estimated_amount_in: None,
        };

        let resimulated = ResimulatedRouteInternal {
            segments: vec![ResimulatedSegmentInternal {
                share_bps: 10_000,
                amount_in: BigUint::from(10u32),
                expected_amount_out: BigUint::from(9u32),
                hops: vec![ResimulatedHopInternal {
                    token_in: native.clone(),
                    token_out: token_out.clone(),
                    amount_in: BigUint::from(10u32),
                    expected_amount_out: BigUint::from(9u32),
                    swaps: vec![ResimulatedSwapInternal {
                        pool: pool_ref("p1"),
                        backend: PoolBackend::Native,
                        token_in: native.clone(),
                        token_out: token_out.clone(),
                        split_bps: 10_000,
                        amount_in: BigUint::from(10u32),
                        expected_amount_out: BigUint::from(9u32),
                        pool_state: Arc::new(MockProtocolSim {}),
                        component: Arc::new(dummy_component()),
                    }],
                }],
            }],
        };

        let token_in = super::super::wire::parse_address(&request.token_in).unwrap();
        let token_out = super::super::wire::parse_address(&request.token_out).unwrap();
        let amount_in = super::super::wire::parse_amount(&request.amount_in).unwrap();

        let context = RouteContext {
            request: &request,
            token_in: &token_in,
            token_out: &token_out,
            amount_in: &amount_in,
            router_address: &router,
            is_native_input: true,
        };

        let calldata =
            build_route_calldata_tx(&context, &resimulated, &encoder, &BigUint::from(8u32))
                .unwrap();

        assert_eq!(calldata.target, router);
        assert_eq!(calldata.value, amount_in);
        assert!(
            !decode_transfer_from_allowed(&calldata.calldata),
            "native input should disable router transferFrom"
        );
    }

    #[test]
    fn build_route_calldata_tx_rejects_permit2_signature() {
        let router = Bytes::from_str("0x0000000000000000000000000000000000000004").unwrap();
        let encoder = MockTychoEncoder::new("Permit2", router.clone());

        let request = RouteEncodeRequest {
            chain_id: 1,
            token_in: "0x0000000000000000000000000000000000000001".to_string(),
            token_out: "0x0000000000000000000000000000000000000002".to_string(),
            amount_in: "10".to_string(),
            min_amount_out: "8".to_string(),
            settlement_address: "0x0000000000000000000000000000000000000003".to_string(),
            tycho_router_address: format_address(&router),
            swap_kind: crate::models::messages::SwapKind::SimpleSwap,
            segments: Vec::new(),
            request_id: None,
            estimated_amount_in: None,
        };

        let resimulated = ResimulatedRouteInternal {
            segments: vec![ResimulatedSegmentInternal {
                share_bps: 10_000,
                amount_in: BigUint::from(10u32),
                expected_amount_out: BigUint::from(9u32),
                hops: vec![ResimulatedHopInternal {
                    token_in: Bytes::from_str("0x0000000000000000000000000000000000000001")
                        .unwrap(),
                    token_out: Bytes::from_str("0x0000000000000000000000000000000000000002")
                        .unwrap(),
                    amount_in: BigUint::from(10u32),
                    expected_amount_out: BigUint::from(9u32),
                    swaps: vec![ResimulatedSwapInternal {
                        pool: pool_ref("p1"),
                        backend: PoolBackend::Native,
                        token_in: Bytes::from_str("0x0000000000000000000000000000000000000001")
                            .unwrap(),
                        token_out: Bytes::from_str("0x0000000000000000000000000000000000000002")
                            .unwrap(),
                        split_bps: 10_000,
                        amount_in: BigUint::from(10u32),
                        expected_amount_out: BigUint::from(9u32),
                        pool_state: Arc::new(MockProtocolSim {}),
                        component: Arc::new(dummy_component()),
                    }],
                }],
            }],
        };

        let token_in = super::super::wire::parse_address(&request.token_in).unwrap();
        let token_out = super::super::wire::parse_address(&request.token_out).unwrap();
        let amount_in = super::super::wire::parse_amount(&request.amount_in).unwrap();
        let context = RouteContext {
            request: &request,
            token_in: &token_in,
            token_out: &token_out,
            amount_in: &amount_in,
            router_address: &router,
            is_native_input: false,
        };

        let err =
            match build_route_calldata_tx(&context, &resimulated, &encoder, &BigUint::from(8u32)) {
                Ok(_) => panic!("Expected Permit2 signature to be rejected"),
                Err(err) => err,
            };
        assert_eq!(
            err.kind(),
            crate::services::encode::EncodeErrorKind::Encoding
        );
        assert!(
            err.message().contains("Permit2"),
            "unexpected error: {}",
            err.message()
        );
    }

    #[test]
    fn encode_function_call_appends_params() {
        let signature = "singleSwap(uint256,address,address,uint256,bool,bool,address,bool,bytes)";
        let calldata_args = (
            alloy_primitives::U256::from(1u32),
            alloy_primitives::Address::from([0x11; 20]),
            alloy_primitives::Address::from([0x22; 20]),
            alloy_primitives::U256::from(1u32),
            false,
            false,
            alloy_primitives::Address::from([0x33; 20]),
            false,
            vec![0x01u8, 0x02u8],
        )
            .abi_encode_params();

        let calldata = encode_function_call(signature, calldata_args.clone()).unwrap();
        assert_eq!(&calldata[4..], calldata_args.as_slice());
    }

    #[test]
    fn encode_route_calldata_uses_param_encoding() {
        let signature = "singleSwap(uint256,address,address,uint256,bool,bool,address,bool,bytes)";
        let encoded_solution = EncodedSolution {
            swaps: vec![0x01, 0x02],
            interacting_with: Bytes::from_str("0x0000000000000000000000000000000000000009")
                .unwrap(),
            function_signature: signature.to_string(),
            n_tokens: 0,
            permit: None,
        };
        let solution = Solution {
            sender: Bytes::from_str("0x0000000000000000000000000000000000000011").unwrap(),
            receiver: Bytes::from_str("0x0000000000000000000000000000000000000022").unwrap(),
            given_token: Bytes::from_str("0x0000000000000000000000000000000000000001").unwrap(),
            given_amount: BigUint::from(1u32),
            checked_token: Bytes::from_str("0x0000000000000000000000000000000000000002").unwrap(),
            exact_out: false,
            checked_amount: BigUint::from(2u32),
            swaps: Vec::new(),
            native_action: None,
        };
        let receiver = Bytes::from_str("0x0000000000000000000000000000000000000033").unwrap();

        let calldata = encode_route_calldata(&encoded_solution, &solution, &receiver, true)
            .expect("route calldata");
        let expected = alloy_primitives::U256::from(1u32).to_be_bytes::<32>();

        assert!(calldata.len() >= 36);
        assert_eq!(&calldata[..4], &function_selector(signature).unwrap());
        assert_eq!(&calldata[4..36], expected.as_slice());
    }

    #[test]
    fn encode_route_calldata_rejects_amount_over_uint256() {
        let signature = "singleSwap(uint256,address,address,uint256,bool,bool,address,bool,bytes)";
        let router = Bytes::from_str("0x0000000000000000000000000000000000000004").unwrap();
        let encoded_solution = EncodedSolution {
            swaps: vec![0x00],
            interacting_with: router.clone(),
            function_signature: signature.to_string(),
            n_tokens: 0,
            permit: None,
        };

        let too_large = BigUint::from(1u32) << 256;
        let solution = Solution {
            sender: Bytes::from_str("0x0000000000000000000000000000000000000011").unwrap(),
            receiver: Bytes::from_str("0x0000000000000000000000000000000000000022").unwrap(),
            given_token: Bytes::from_str("0x0000000000000000000000000000000000000001").unwrap(),
            given_amount: too_large,
            checked_token: Bytes::from_str("0x0000000000000000000000000000000000000002").unwrap(),
            exact_out: false,
            checked_amount: BigUint::from(1u32),
            swaps: Vec::new(),
            native_action: None,
        };
        let receiver = Bytes::from_str("0x0000000000000000000000000000000000000033").unwrap();

        let err = encode_route_calldata(&encoded_solution, &solution, &receiver, true).unwrap_err();
        assert_eq!(
            err.kind(),
            crate::services::encode::EncodeErrorKind::InvalidRequest
        );
        assert!(
            err.message().contains("uint256"),
            "unexpected error message: {:?}",
            err.message()
        );
    }

    #[test]
    fn default_registry_supports_ekubo_v3() {
        let registry = SwapEncoderRegistry::new(Chain::Ethereum)
            .add_default_encoders(None)
            .expect("default encoder registry should initialize");

        assert!(
            registry.get_encoder("ekubo_v3").is_some(),
            "expected ekubo_v3 encoder in default registry"
        );
    }

    #[test]
    fn default_registry_supports_aerodrome_slipstreams() {
        let registry = SwapEncoderRegistry::new(Chain::Base)
            .add_default_encoders(None)
            .expect("default encoder registry should initialize");

        assert!(
            registry.get_encoder("aerodrome_slipstreams").is_some(),
            "expected aerodrome_slipstreams encoder in default registry"
        );
    }
}
