use tycho_simulation::{protocol::models::ProtocolComponent, tycho_common::Bytes};

use super::protocol::ProtocolKind;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Erc4626PairPolicy {
    pub asset: Bytes,
    pub share: Bytes,
    pub allow_asset_to_share: bool,
    pub allow_share_to_asset: bool,
}

impl Erc4626PairPolicy {
    fn supports_direction(
        &self,
        token_in: &Bytes,
        token_out: &Bytes,
        deposits_enabled: bool,
    ) -> bool {
        (deposits_enabled
            && self.allow_asset_to_share
            && token_in == &self.asset
            && token_out == &self.share)
            || (self.allow_share_to_asset && token_in == &self.share && token_out == &self.asset)
    }

    fn component_matches(&self, component: &ProtocolComponent) -> bool {
        component.id == self.share
            && component.tokens.len() == 2
            && component
                .tokens
                .iter()
                .any(|token| token.address == self.asset)
            && component
                .tokens
                .iter()
                .any(|token| token.address == self.share)
    }
}

pub(crate) fn component_is_erc4626(component: &ProtocolComponent) -> bool {
    ProtocolKind::from_component(component) == Some(ProtocolKind::ERC4626)
}

pub(crate) fn component_direction_supported(
    component: &ProtocolComponent,
    token_in: &Bytes,
    token_out: &Bytes,
    deposits_enabled: bool,
    pair_policies: &[Erc4626PairPolicy],
) -> bool {
    !component_is_erc4626(component)
        || pair_policies.iter().any(|pair| {
            pair.supports_direction(token_in, token_out, deposits_enabled)
                && pair.component_matches(component)
        })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::str::FromStr;

    use chrono::NaiveDateTime;
    use tycho_simulation::tycho_common::models::{token::Token, Chain};

    use super::*;

    #[expect(
        clippy::unwrap_used,
        reason = "ERC4626 support tests use fixed addresses with deterministic parsing"
    )]
    fn known_address(address: &str) -> Bytes {
        Bytes::from_str(address).unwrap()
    }

    fn pair_policies() -> Vec<Erc4626PairPolicy> {
        vec![
            Erc4626PairPolicy {
                asset: known_address("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
                share: known_address("0xa3931d71877c0e7a3148cb7eb4463524fec27fbd"),
                allow_asset_to_share: true,
                allow_share_to_asset: true,
            },
            Erc4626PairPolicy {
                asset: known_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
                share: known_address("0xBc65ad17c5C0a2A4D159fa5a503f4992c7B545FE"),
                allow_asset_to_share: true,
                allow_share_to_asset: true,
            },
            Erc4626PairPolicy {
                asset: known_address("0x6c3ea9036406852006290770BEdFcAbA0e23A0e8"),
                share: known_address("0x80128DbB9f07b93DDE62A6daeadb69ED14a7D354"),
                allow_asset_to_share: true,
                allow_share_to_asset: true,
            },
        ]
    }

    fn token(address: &str, symbol: &str, decimals: u32) -> Token {
        Token::new(
            &known_address(address),
            symbol,
            decimals,
            0,
            &[],
            Chain::Ethereum,
            100,
        )
    }

    fn component(
        share: &str,
        protocol_system: &str,
        protocol_type_name: &str,
        tokens: Vec<Token>,
    ) -> ProtocolComponent {
        ProtocolComponent::new(
            known_address(share),
            protocol_system.to_string(),
            protocol_type_name.to_string(),
            Chain::Ethereum,
            tokens,
            Vec::new(),
            HashMap::new(),
            Bytes::default(),
            NaiveDateTime::default(),
        )
    }

    #[test]
    fn component_support_requires_matching_share_token() {
        let pair_policies = pair_policies();
        let usds = token("0xdC035D45d973E3EC169d2276DDab16f1e407384F", "USDS", 18);
        let susds = token("0xa3931d71877c0e7a3148cb7eb4463524fec27fbd", "sUSDS", 18);
        let wrong_share = token("0x9d39a5de30e57443bff2a8307a4256c8797a3497", "sUSDe", 18);
        let token_in = usds.address.clone();
        let token_out = susds.address.clone();

        let supported_component = component(
            "0xa3931d71877c0e7a3148cb7eb4463524fec27fbd",
            "erc4626",
            "erc4626_pool",
            vec![usds.clone(), susds],
        );
        assert!(component_direction_supported(
            &supported_component,
            &token_in,
            &token_out,
            true,
            &pair_policies,
        ));

        let unsupported_component = component(
            "0x9d39a5de30e57443bff2a8307a4256c8797a3497",
            "erc4626",
            "erc4626_pool",
            vec![usds, wrong_share],
        );
        assert!(!component_direction_supported(
            &unsupported_component,
            &token_in,
            &token_out,
            true,
            &pair_policies,
        ));
    }

    #[test]
    fn recognizes_erc4626_component_by_protocol_type_name() {
        let pair_policies = pair_policies();
        let usds = token("0xdC035D45d973E3EC169d2276DDab16f1e407384F", "USDS", 18);
        let susds = token("0xa3931d71877c0e7a3148cb7eb4463524fec27fbd", "sUSDS", 18);
        let token_in = usds.address.clone();
        let token_out = susds.address.clone();

        let component = component(
            "0xa3931d71877c0e7a3148cb7eb4463524fec27fbd",
            "",
            "erc4626_pool",
            vec![usds, susds],
        );

        assert!(component_is_erc4626(&component));
        assert!(component_direction_supported(
            &component,
            &token_in,
            &token_out,
            true,
            &pair_policies,
        ));
        assert!(!component_direction_supported(
            &component,
            &token_in,
            &token_out,
            false,
            &pair_policies,
        ));
    }
}
