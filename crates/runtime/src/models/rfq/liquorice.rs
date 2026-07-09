use std::{io, str::FromStr};

use serde::Deserialize;
use tycho_simulation::tycho_common::{
    models::{token::Token, Chain},
    Bytes,
};

#[derive(Debug, Deserialize)]
pub struct TokenLiquorice {
    pub symbol: String,
    pub address: String,
    pub decimals: u32,
}

impl TokenLiquorice {
    pub fn to_tycho_token(&self, chain: Chain) -> Result<Token, io::Error> {
        if !liquorice_chain_supported(chain) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported Liquorice chain {chain}"),
            ));
        }

        let address = Bytes::from_str(self.address.as_str()).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid Liquorice contract address {}: {err}", self.address),
            )
        })?;

        Ok(Token {
            address,
            symbol: self.symbol.clone(),
            decimals: self.decimals,
            tax: 0,
            gas: vec![],
            chain,
            quality: Default::default(),
        })
    }
}

fn liquorice_chain_supported(chain: Chain) -> bool {
    matches!(chain, Chain::Ethereum | Chain::Arbitrum)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ARBITRUM_USDC_ADDRESS: &str = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831";
    const MAINNET_USDC_ADDRESS: &str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";

    fn sample_token(address: &str) -> TokenLiquorice {
        TokenLiquorice {
            symbol: "USDC".to_string(),
            address: address.to_string(),
            decimals: 6,
        }
    }

    #[test]
    fn to_tycho_token_maps_mainnet_supported_token() -> Result<(), Box<dyn std::error::Error>> {
        let token = sample_token(MAINNET_USDC_ADDRESS).to_tycho_token(Chain::Ethereum)?;

        assert_eq!(token.address, Bytes::from_str(MAINNET_USDC_ADDRESS)?);
        assert_eq!(token.symbol, "USDC");
        assert_eq!(token.decimals, 6);
        assert_eq!(token.chain, Chain::Ethereum);
        Ok(())
    }

    #[test]
    fn to_tycho_token_maps_arbitrum_supported_token() -> Result<(), Box<dyn std::error::Error>> {
        let token = sample_token(ARBITRUM_USDC_ADDRESS).to_tycho_token(Chain::Arbitrum)?;

        assert_eq!(token.address, Bytes::from_str(ARBITRUM_USDC_ADDRESS)?);
        assert_eq!(token.symbol, "USDC");
        assert_eq!(token.decimals, 6);
        assert_eq!(token.chain, Chain::Arbitrum);
        Ok(())
    }

    #[test]
    fn to_tycho_token_rejects_unsupported_chain() {
        let Err(err) = sample_token(MAINNET_USDC_ADDRESS).to_tycho_token(Chain::Base) else {
            unreachable!("expected unsupported Liquorice chain to fail");
        };

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("unsupported Liquorice chain"));
    }

    #[test]
    fn to_tycho_token_rejects_invalid_address() {
        let token = TokenLiquorice {
            symbol: "BAD".to_string(),
            address: "not-an-address".to_string(),
            decimals: 18,
        };

        let Err(err) = token.to_tycho_token(Chain::Ethereum) else {
            unreachable!("expected invalid Liquorice address to fail");
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
