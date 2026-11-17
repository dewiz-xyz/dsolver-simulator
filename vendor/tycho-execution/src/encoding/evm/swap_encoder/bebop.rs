use std::{collections::HashMap, str::FromStr, sync::Arc};

use alloy::{primitives::Address, sol_types::SolValue};
use tokio::{
    runtime::{Handle, Runtime},
    task::block_in_place,
};
use tycho_common::{
    models::{protocol::GetAmountOutParams, Chain},
    Bytes,
};

use crate::encoding::{
    errors::EncodingError,
    evm::{
        approvals::protocol_approvals_manager::ProtocolApprovalsManager,
        utils::{biguint_to_u256, bytes_to_address, get_runtime},
    },
    models::{EncodingContext, Swap},
    swap_encoder::SwapEncoder,
};

fn parse_partial_fill_offset(partial_fill_offset: &Bytes) -> Result<u8, EncodingError> {
    let partial_fill_offset_bytes: [u8; 8] = partial_fill_offset
        .as_ref()
        .try_into()
        .map_err(|_| {
            EncodingError::FatalError("Bebop partial_fill_offset must be a u64".to_string())
        })?;
    let partial_fill_offset_u64 = u64::from_be_bytes(partial_fill_offset_bytes);
    u8::try_from(partial_fill_offset_u64)
        .map_err(|_| EncodingError::FatalError("Bebop partial_fill_offset exceeds u8".to_string()))
}

/// Encodes a swap on Bebop (PMM RFQ) through the given executor address.
///
/// Bebop uses a Request-for-Quote model where quotes are obtained off-chain
/// and settled on-chain. This encoder supports PMM RFQ execution.
///
/// # Fields
/// * `executor_address` - The address of the executor contract that will perform the swap.
/// * `settlement_address` - The address of the Bebop settlement contract.
#[derive(Clone)]
pub struct BebopSwapEncoder {
    executor_address: Bytes,
    settlement_address: Bytes,
    native_token_bebop_address: Bytes,
    native_token_address: Bytes,
    runtime_handle: Handle,
    #[allow(dead_code)]
    runtime: Option<Arc<Runtime>>,
}

impl SwapEncoder for BebopSwapEncoder {
    fn new(
        executor_address: Bytes,
        chain: Chain,
        config: Option<HashMap<String, String>>,
    ) -> Result<Self, EncodingError> {
        let config = config.ok_or(EncodingError::FatalError(
            "Missing bebop specific addresses in config".to_string(),
        ))?;
        let settlement_address = config
            .get("bebop_settlement_address")
            .map(|s| {
                Bytes::from_str(s).map_err(|_| {
                    EncodingError::FatalError("Invalid bebop settlement address".to_string())
                })
            })
            .ok_or(EncodingError::FatalError(
                "Missing bebop settlement address in config".to_string(),
            ))
            .flatten()?;
        let native_token_bebop_address = config
            .get("native_token_address")
            .map(|s| {
                Bytes::from_str(s).map_err(|_| {
                    EncodingError::FatalError("Invalid native token bebop address".to_string())
                })
            })
            .ok_or(EncodingError::FatalError(
                "Missing native token bebop address in config".to_string(),
            ))
            .flatten()?;
        let (runtime_handle, runtime) = get_runtime()?;
        Ok(Self {
            executor_address,
            settlement_address,
            runtime_handle,
            runtime,
            native_token_bebop_address,
            native_token_address: chain.native_token().address,
        })
    }

    fn encode_swap(
        &self,
        swap: &Swap,
        encoding_context: &EncodingContext,
    ) -> Result<Vec<u8>, EncodingError> {
        let token_in = bytes_to_address(swap.token_in())?;
        let token_out = bytes_to_address(swap.token_out())?;
        let sender = encoding_context
            .router_address
            .clone()
            .ok_or(EncodingError::FatalError(
                "The router address is needed to perform a Hashflow swap".to_string(),
            ))?;
        let approval_needed = if *swap.token_in() == self.native_token_address {
            false
        } else {
            let tycho_router_address = bytes_to_address(&sender)?;
            let settlement_address = Address::from_str(&self.settlement_address.to_string())
                .map_err(|_| {
                    EncodingError::FatalError("Invalid bebop settlement address".to_string())
                })?;
            ProtocolApprovalsManager::new()?.approval_needed(
                token_in,
                tycho_router_address,
                settlement_address,
            )?
        };

        let protocol_state = swap
            .get_protocol_state()
            .as_ref()
            .ok_or_else(|| {
                EncodingError::FatalError("protocol_state is required for Bebop".to_string())
            })?;
        let (partial_fill_offset, original_filled_taker_amount, bebop_calldata) = {
            let indicatively_priced_state = protocol_state
                .as_indicatively_priced()
                .map_err(|e| {
                    EncodingError::FatalError(format!("State is not indicatively priced {e}"))
                })?;
            let estimated_amount_in = swap
                .get_estimated_amount_in()
                .clone()
                .ok_or(EncodingError::FatalError(
                    "Estimated amount in is mandatory for a Bebop swap".to_string(),
                ))?;
            // Bebop uses another address for the native token than the zero address
            let mut token_in = swap.token_in().clone();
            if *swap.token_in() == self.native_token_address {
                token_in = self.native_token_bebop_address.clone()
            }
            let mut token_out = swap.token_out().clone();
            if *swap.token_out() == self.native_token_address {
                token_out = self.native_token_bebop_address.clone()
            }

            let params = GetAmountOutParams {
                origin: None,
                amount_in: estimated_amount_in,
                token_in,
                token_out,
                sender: encoding_context
                    .router_address
                    .clone()
                    .ok_or(EncodingError::FatalError(
                        "The router address is needed to perform a Bebop swap".to_string(),
                    ))?,
                receiver: encoding_context.receiver.clone(),
            };
            let signed_quote = block_in_place(|| {
                self.runtime_handle.block_on(async {
                    indicatively_priced_state
                        .request_signed_quote(params)
                        .await
                })
            })?;
            // The executor calls the settlement contract only, so calldata for any other
            // target, such as the Bebop router, would revert.
            let tx_to = signed_quote
                .quote_attributes
                .get("tx_to")
                .ok_or(EncodingError::FatalError(
                    "Bebop quote must have a tx_to attribute".to_string(),
                ))?;
            if *tx_to != self.settlement_address {
                return Err(EncodingError::FatalError(format!(
                    "Bebop quote targets {tx_to} while the executor calls only the settlement \
                     contract {}",
                    self.settlement_address
                )));
            }
            let bebop_calldata = signed_quote
                .quote_attributes
                .get("calldata")
                .ok_or(EncodingError::FatalError(
                    "Bebop quote must have a calldata attribute".to_string(),
                ))?;
            let partial_fill_offset = signed_quote
                .quote_attributes
                .get("partial_fill_offset")
                .ok_or(EncodingError::FatalError(
                    "Bebop quote must have a partial_fill_offset attribute".to_string(),
                ))?;
            let partial_fill_offset = parse_partial_fill_offset(partial_fill_offset)?;
            let original_filled_taker_amount = biguint_to_u256(&signed_quote.amount_in);
            (partial_fill_offset, original_filled_taker_amount, bebop_calldata.to_vec())
        };

        let receiver = bytes_to_address(&encoding_context.receiver)?;

        // Encode packed data for the executor
        // Format: token_in | token_out | transfer_type | partial_fill_offset |
        //         original_filled_taker_amount | approval_needed | receiver | bebop_calldata
        let args = (
            token_in,
            token_out,
            (encoding_context.transfer_type as u8).to_be_bytes(),
            partial_fill_offset.to_be_bytes(),
            original_filled_taker_amount.to_be_bytes::<32>(),
            (approval_needed as u8).to_be_bytes(),
            receiver,
            &bebop_calldata[..],
        );

        Ok(args.abi_encode_packed())
    }

    fn executor_address(&self) -> &Bytes {
        &self.executor_address
    }

    fn clone_box(&self) -> Box<dyn SwapEncoder> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use alloy::hex::encode;
    use num_bigint::BigUint;
    use tycho_common::models::protocol::ProtocolComponent;

    use super::*;
    use crate::encoding::{
        evm::{swap_encoder::bebop::BebopSwapEncoder, testing_utils::MockRFQState},
        models::TransferType,
    };

    const SETTLEMENT: &str = "0xbbbbbBB520d69a9775E85b458C58c648259FAD5F";

    fn bebop_config() -> HashMap<String, String> {
        HashMap::from([
            ("bebop_settlement_address".to_string(), SETTLEMENT.to_string()),
            (
                "native_token_address".to_string(),
                "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE".to_string(),
            ),
        ])
    }

    fn encoded_partial_fill_offset(partial_fill_offset: u64) -> Bytes {
        Bytes::from(
            partial_fill_offset
                .to_be_bytes()
                .to_vec(),
        )
    }

    #[test]
    fn test_parse_partial_fill_offset_accepts_u8_values() {
        assert_eq!(parse_partial_fill_offset(&encoded_partial_fill_offset(12)).unwrap(), 12);
        assert_eq!(parse_partial_fill_offset(&encoded_partial_fill_offset(2)).unwrap(), 2);
    }

    #[test]
    fn test_parse_partial_fill_offset_rejects_values_over_u8() {
        let err = parse_partial_fill_offset(&encoded_partial_fill_offset(256)).unwrap_err();
        assert!(matches!(
            err,
            EncodingError::FatalError(message) if message == "Bebop partial_fill_offset exceeds u8"
        ));
    }

    #[test]
    fn test_parse_partial_fill_offset_rejects_non_u64_values() {
        let err = parse_partial_fill_offset(&Bytes::from(vec![12u8])).unwrap_err();
        assert!(matches!(
            err,
            EncodingError::FatalError(message) if message == "Bebop partial_fill_offset must be a u64"
        ));
    }

    #[test]
    fn test_encode_bebop_single_with_protocol_state() {
        // Native input skips live approval lookup; the amounts still use different token units.
        let bebop_calldata = Bytes::from_str("0x123456").unwrap();
        let partial_fill_offset = 12u64;
        let estimated_amount_in = BigUint::from_str("2999999999").unwrap();
        let quote_amount_in = BigUint::from_str("3000000000").unwrap();
        let quote_amount_out = BigUint::from_str("1000000000000000000").unwrap();

        let bebop_component = ProtocolComponent {
            id: String::from("bebop-rfq"),
            protocol_system: String::from("rfq:bebop"),
            ..Default::default()
        };
        let bebop_state = MockRFQState {
            quote_amount_in: Some(quote_amount_in),
            quote_amount_out,
            quote_data: HashMap::from([
                ("tx_to".to_string(), Bytes::from(SETTLEMENT)),
                ("calldata".to_string(), bebop_calldata.clone()),
                (
                    "partial_fill_offset".to_string(),
                    encoded_partial_fill_offset(partial_fill_offset),
                ),
            ]),
        };

        let token_in = Bytes::zero(20); // native ETH
        let token_out = Bytes::from("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"); // WETH

        let swap = Swap::new(bebop_component, token_in.clone(), token_out.clone())
            .estimated_amount_in(estimated_amount_in)
            .protocol_state(Arc::new(bebop_state));

        let encoding_context = EncodingContext {
            receiver: Bytes::from("0xc5564C13A157E6240659fb81882A28091add8670"),
            exact_out: false,
            router_address: Some(Bytes::zero(20)),
            group_token_in: token_in.clone(),
            group_token_out: token_out.clone(),
            transfer_type: TransferType::Transfer,
            historical_trade: false,
        };

        let encoder = BebopSwapEncoder::new(
            Bytes::from("0x543778987b293C7E8Cf0722BB2e935ba6f4068D4"),
            Chain::Ethereum,
            Some(bebop_config()),
        )
        .unwrap();

        let encoded_swap = encoder
            .encode_swap(&swap, &encoding_context)
            .unwrap();
        let hex_swap = encode(&encoded_swap);

        let expected_swap = String::from(concat!(
            // token in
            "0000000000000000000000000000000000000000",
            // token out
            "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            // transfer type
            "01",
            // partiall filled offset
            "0c",
            //  original taker amount
            "00000000000000000000000000000000000000000000000000000000b2d05e00",
            // approval needed
            "00",
            //receiver,
            "c5564c13a157e6240659fb81882a28091add8670",
        ));
        assert_eq!(hex_swap, expected_swap + &bebop_calldata.to_string()[2..]);
    }

    #[test]
    fn test_encode_bebop_aggregate_uses_signed_amount_in() {
        let bebop_calldata = Bytes::from_str("0xa2f74893").unwrap();
        let partial_fill_offset = 2u64;
        let estimated_amount_in = BigUint::from_str("19999999999").unwrap();
        let quote_amount_in = BigUint::from_str("20000000000").unwrap();
        let quote_amount_out = BigUint::from_str("58891757454250876735929").unwrap();

        let bebop_component = ProtocolComponent {
            id: String::from("bebop-rfq"),
            protocol_system: String::from("rfq:bebop"),
            ..Default::default()
        };
        let bebop_state = MockRFQState {
            quote_amount_in: Some(quote_amount_in),
            quote_amount_out,
            quote_data: HashMap::from([
                ("tx_to".to_string(), Bytes::from(SETTLEMENT)),
                ("calldata".to_string(), bebop_calldata.clone()),
                (
                    "partial_fill_offset".to_string(),
                    encoded_partial_fill_offset(partial_fill_offset),
                ),
            ]),
        };

        let token_in = Bytes::zero(20); // native ETH
        let token_out = Bytes::from("0xfAbA6f8e4a5E8Ab82F62fe7C39859FA577269BE3"); // ONDO

        let swap = Swap::new(bebop_component, token_in.clone(), token_out.clone())
            .estimated_amount_in(estimated_amount_in)
            .protocol_state(Arc::new(bebop_state));

        let encoding_context = EncodingContext {
            receiver: Bytes::from("0xc5564C13A157E6240659fb81882A28091add8670"),
            exact_out: false,
            router_address: Some(Bytes::zero(20)),
            group_token_in: token_in.clone(),
            group_token_out: token_out.clone(),
            transfer_type: TransferType::Transfer,
            historical_trade: false,
        };

        let encoder = BebopSwapEncoder::new(
            Bytes::from("0x543778987b293C7E8Cf0722BB2e935ba6f4068D4"),
            Chain::Ethereum,
            Some(bebop_config()),
        )
        .unwrap();

        let encoded_swap = encoder
            .encode_swap(&swap, &encoding_context)
            .unwrap();
        let hex_swap = encode(&encoded_swap);

        let expected_swap = String::from(concat!(
            // token in
            "0000000000000000000000000000000000000000",
            // token out
            "faba6f8e4a5e8ab82f62fe7c39859fa577269be3",
            // transfer type
            "01",
            // partial filled offset
            "02",
            // original taker amount
            "00000000000000000000000000000000000000000000000000000004a817c800",
            // approval needed
            "00",
            // receiver
            "c5564c13a157e6240659fb81882a28091add8670",
        ));
        assert_eq!(hex_swap, expected_swap + &bebop_calldata.to_string()[2..]);
    }

    fn encode_quote(quote_data: HashMap<String, Bytes>) -> Result<Vec<u8>, EncodingError> {
        let bebop_component = ProtocolComponent {
            id: String::from("bebop-rfq"),
            protocol_system: String::from("rfq:bebop"),
            ..Default::default()
        };
        let bebop_state = MockRFQState {
            quote_amount_in: Some(BigUint::from_str("3000000000").unwrap()),
            quote_amount_out: BigUint::from_str("1000000000000000000").unwrap(),
            quote_data,
        };

        let token_in = Bytes::zero(20); // native ETH
        let token_out = Bytes::from("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"); // WETH
        let swap = Swap::new(bebop_component, token_in.clone(), token_out.clone())
            .estimated_amount_in(BigUint::from_str("2999999999").unwrap())
            .protocol_state(Arc::new(bebop_state));
        let encoding_context = EncodingContext {
            receiver: Bytes::from("0xc5564C13A157E6240659fb81882A28091add8670"),
            exact_out: false,
            router_address: Some(Bytes::zero(20)),
            group_token_in: token_in,
            group_token_out: token_out,
            transfer_type: TransferType::Transfer,
            historical_trade: false,
        };
        let encoder = BebopSwapEncoder::new(
            Bytes::from("0x543778987b293C7E8Cf0722BB2e935ba6f4068D4"),
            Chain::Ethereum,
            Some(bebop_config()),
        )
        .unwrap();
        encoder.encode_swap(&swap, &encoding_context)
    }

    #[test]
    fn test_encode_bebop_refuses_a_quote_for_another_contract() {
        let router = Bytes::from("0x1111111111111111111111111111111111111111");
        let err = encode_quote(HashMap::from([
            ("tx_to".to_string(), router.clone()),
            ("calldata".to_string(), Bytes::from_str("0x123456").unwrap()),
            ("partial_fill_offset".to_string(), encoded_partial_fill_offset(12)),
        ]))
        .unwrap_err();
        assert!(matches!(
            err,
            EncodingError::FatalError(message) if message.contains(&router.to_string())
        ));
    }

    #[test]
    fn test_encode_bebop_refuses_a_quote_without_tx_to() {
        let err = encode_quote(HashMap::from([
            ("calldata".to_string(), Bytes::from_str("0x123456").unwrap()),
            ("partial_fill_offset".to_string(), encoded_partial_fill_offset(12)),
        ]))
        .unwrap_err();
        assert!(matches!(
            err,
            EncodingError::FatalError(message) if message == "Bebop quote must have a tx_to attribute"
        ));
    }
}
