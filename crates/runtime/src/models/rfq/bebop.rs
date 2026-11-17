use std::{collections::HashMap, io, str::FromStr};

use serde::Deserialize;
use tycho_simulation::tycho_common::{
    models::{token::Token, Chain},
    Bytes,
};

#[derive(Deserialize, Debug)]
pub struct BebopResponse {
    pub tokens: HashMap<String, TokenBebop>,
    pub metadata: Option<Metadata>, // optional if sometimes missing
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub last_update: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenBebop {
    pub name: String,
    pub ticker: String,
    pub availability: Availability,
    pub price_usd: Option<f64>,
    pub cid: String,
    pub display_decimals: Option<u8>,
    pub colour: Option<String>,
    pub tags: Vec<String>,
    pub icon_url: String,
    pub chain_info: Vec<ChainInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Availability {
    pub is_available: bool,
    pub can_buy: bool,
    pub can_sell: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainInfo {
    pub chain_id: u64,
    pub contract_address: String,
    pub decimals: u8,
}

impl TokenBebop {
    pub fn to_tycho_token(&self, chain: Chain) -> Result<Option<Token>, io::Error> {
        self.chain_info
            .iter()
            // Bebop can return the same token across multiple chains in one payload.
            .find(|chain_info| chain_info.chain_id == chain.id())
            .map(|chain_info| {
                let address =
                    Bytes::from_str(chain_info.contract_address.as_str()).map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "invalid Bebop contract address {}: {err}",
                                chain_info.contract_address
                            ),
                        )
                    })?;
                Ok(Token {
                    address,
                    symbol: self.ticker.clone(),
                    decimals: chain_info.decimals as u32,
                    tax: 0,
                    gas: vec![],
                    chain,
                    quality: Default::default(),
                })
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ETH_ADDRESS: &str = "0x1111111111111111111111111111111111111111";
    const BASE_ADDRESS: &str = "0x4200000000000000000000000000000000000006";

    fn sample_token() -> TokenBebop {
        TokenBebop {
            name: "Wrapped Ether".to_string(),
            ticker: "WETH".to_string(),
            availability: Availability {
                is_available: true,
                can_buy: true,
                can_sell: true,
            },
            price_usd: Some(2000.0),
            cid: "weth".to_string(),
            display_decimals: Some(4),
            colour: Some("#fff".to_string()),
            tags: vec!["core".to_string()],
            icon_url: "https://example.com/weth.png".to_string(),
            chain_info: vec![
                ChainInfo {
                    chain_id: Chain::Ethereum.id(),
                    contract_address: ETH_ADDRESS.to_string(),
                    decimals: 18,
                },
                ChainInfo {
                    chain_id: Chain::Base.id(),
                    contract_address: BASE_ADDRESS.to_string(),
                    decimals: 18,
                },
            ],
        }
    }

    #[test]
    fn to_tycho_token_selects_requested_chain_info() -> Result<(), Box<dyn std::error::Error>> {
        for (chain, address) in [(Chain::Ethereum, ETH_ADDRESS), (Chain::Base, BASE_ADDRESS)] {
            let token = sample_token()
                .to_tycho_token(chain)?
                .ok_or_else(|| io::Error::other(format!("expected {chain} token")))?;

            assert_eq!(token.address, Bytes::from_str(address)?, "{chain}");
            assert_eq!(token.symbol, "WETH", "{chain}");
            assert_eq!(token.decimals, 18, "{chain}");
            assert_eq!(token.chain, chain, "{chain}");
        }
        Ok(())
    }
}
