use std::fmt;

use tycho_simulation::protocol::models::ProtocolComponent;

pub const NATIVE: &str = "native";
pub const VM: &str = "vm";
pub const RFQ: &str = "rfq";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProtocolKind {
    UniswapV2,
    UniswapV3,
    UniswapV4,
    AerodromeSlipstreams,
    AerodromeV1,
    Curve,
    PancakeswapV2,
    PancakeswapV3,
    EkuboV2,
    SushiswapV2,
    MaverickV2,
    BalancerV2,
    FluidV1,
    Rocketpool,
    EkuboV3,
    ERC4626,
    Hashflow,
    Bebop,
    Liquorice,
    AeroBasic,
    AeroCl,
    PancakeswapInfinityCl,
    PancakeswapInfinityBin,
    LunarBase,
    Biconomy,
    NativeRfq,
    Metric,
    UniswapV4Hooks,
    RingSwapV2,
    Sky,
    LidoV4,
    Etherfi,
    BalancerV3,
    LiquidityParty,
    TitanFermiSwap,
    TitanKipseli,
    TitanBebop,
    TitanTaurusFi,
    TitanTempest,
    TitanGoliath,
    TitanStelaxis,
    TitanElZorro,
    TitanWilbrr,
    Euclid,
    Baibai,
}

impl ProtocolKind {
    pub const ALL: [ProtocolKind; 45] = [
        ProtocolKind::UniswapV2,
        ProtocolKind::UniswapV3,
        ProtocolKind::UniswapV4,
        ProtocolKind::AerodromeSlipstreams,
        ProtocolKind::AerodromeV1,
        ProtocolKind::Curve,
        ProtocolKind::PancakeswapV2,
        ProtocolKind::PancakeswapV3,
        ProtocolKind::EkuboV2,
        ProtocolKind::SushiswapV2,
        ProtocolKind::MaverickV2,
        ProtocolKind::BalancerV2,
        ProtocolKind::FluidV1,
        ProtocolKind::Rocketpool,
        ProtocolKind::EkuboV3,
        ProtocolKind::ERC4626,
        ProtocolKind::Hashflow,
        ProtocolKind::Bebop,
        ProtocolKind::Liquorice,
        ProtocolKind::AeroBasic,
        ProtocolKind::AeroCl,
        ProtocolKind::PancakeswapInfinityCl,
        ProtocolKind::PancakeswapInfinityBin,
        ProtocolKind::LunarBase,
        ProtocolKind::Biconomy,
        ProtocolKind::NativeRfq,
        ProtocolKind::Metric,
        ProtocolKind::UniswapV4Hooks,
        ProtocolKind::RingSwapV2,
        ProtocolKind::Sky,
        ProtocolKind::LidoV4,
        ProtocolKind::Etherfi,
        ProtocolKind::BalancerV3,
        ProtocolKind::LiquidityParty,
        ProtocolKind::TitanFermiSwap,
        ProtocolKind::TitanKipseli,
        ProtocolKind::TitanBebop,
        ProtocolKind::TitanTaurusFi,
        ProtocolKind::TitanTempest,
        ProtocolKind::TitanGoliath,
        ProtocolKind::TitanStelaxis,
        ProtocolKind::TitanElZorro,
        ProtocolKind::TitanWilbrr,
        ProtocolKind::Euclid,
        ProtocolKind::Baibai,
    ];

    /// A named venue on Titan's Ethereum price-level stream.
    pub fn titan_venue(self) -> Option<&'static str> {
        self.as_str().strip_prefix("pricelevelstream:")
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ProtocolKind::UniswapV2 => "uniswap_v2",
            ProtocolKind::UniswapV3 => "uniswap_v3",
            ProtocolKind::UniswapV4 => "uniswap_v4",
            ProtocolKind::AerodromeSlipstreams => "aerodrome_slipstreams",
            ProtocolKind::AerodromeV1 => "aerodrome_v1",
            ProtocolKind::Curve => "vm:curve",
            ProtocolKind::PancakeswapV2 => "pancakeswap_v2",
            ProtocolKind::PancakeswapV3 => "pancakeswap_v3",
            ProtocolKind::EkuboV2 => "ekubo_v2",
            ProtocolKind::SushiswapV2 => "sushiswap_v2",
            ProtocolKind::MaverickV2 => "vm:maverick_v2",
            ProtocolKind::BalancerV2 => "vm:balancer_v2",
            ProtocolKind::FluidV1 => "fluid_v1",
            ProtocolKind::Rocketpool => "rocketpool",
            ProtocolKind::EkuboV3 => "ekubo_v3",
            ProtocolKind::ERC4626 => "erc4626",
            ProtocolKind::Hashflow => "rfq:hashflow",
            ProtocolKind::Bebop => "rfq:bebop",
            ProtocolKind::Liquorice => "rfq:liquorice",
            ProtocolKind::AeroBasic => "aero_basic",
            ProtocolKind::AeroCl => "aero_cl",
            ProtocolKind::PancakeswapInfinityCl => "pancakeswap_infinity_cl",
            ProtocolKind::PancakeswapInfinityBin => "pancakeswap_infinity_bin",
            ProtocolKind::LunarBase => "lunarbase",
            ProtocolKind::Biconomy => "biconomy",
            ProtocolKind::NativeRfq => "rfq:native",
            ProtocolKind::Metric => "rfq:metric",
            ProtocolKind::UniswapV4Hooks => "uniswap_v4_hooks",
            ProtocolKind::RingSwapV2 => "ring_swap_v2",
            ProtocolKind::Sky => "sky",
            ProtocolKind::LidoV4 => "lido_v4",
            ProtocolKind::Etherfi => "etherfi",
            ProtocolKind::BalancerV3 => "vm:balancer_v3",
            ProtocolKind::LiquidityParty => "vm:liquidityparty",
            ProtocolKind::TitanFermiSwap => "pricelevelstream:fermiswap",
            ProtocolKind::TitanKipseli => "pricelevelstream:kipseli",
            ProtocolKind::TitanBebop => "pricelevelstream:bebop",
            ProtocolKind::TitanTaurusFi => "pricelevelstream:taurusfi",
            ProtocolKind::TitanTempest => "pricelevelstream:tempest",
            ProtocolKind::TitanGoliath => "pricelevelstream:goliath",
            ProtocolKind::TitanStelaxis => "pricelevelstream:stelaxis",
            ProtocolKind::TitanElZorro => "pricelevelstream:elzorro",
            ProtocolKind::TitanWilbrr => {
                "pricelevelstream:0x6e27bb694393840e253262361b79187d4899e9cf"
            }
            ProtocolKind::Euclid => "rfq:euclid",
            ProtocolKind::Baibai => "baibai",
        }
    }

    /// Accepts an exact Tycho protocol system name without normalizing aliases.
    /// Use this at selection boundaries so a different wire identity is not selected.
    pub fn from_canonical_protocol_system(name: &str) -> Option<Self> {
        Self::from_protocol_system(name)
    }

    pub fn from_component(component: &ProtocolComponent) -> Option<Self> {
        let system_name = normalize_proto_name(&component.protocol_system);
        if let Some(kind) = Self::from_protocol_system(system_name.as_str()) {
            return Some(kind);
        }

        let type_name = normalize_proto_name(&component.protocol_type_name);
        Self::from_protocol_type_name(type_name.as_str())
    }

    pub fn from_sync_state_key(protocol: &str) -> Option<Self> {
        let name = normalize_proto_name(protocol);
        Self::from_protocol_system(name.as_str())
    }

    pub fn from_protocol_hint(protocol: &str) -> Option<Self> {
        let name = normalize_proto_name(protocol);
        Self::from_protocol_system(name.as_str())
            .or_else(|| Self::from_protocol_type_name(name.as_str()))
    }

    fn from_protocol_type_name(name: &str) -> Option<Self> {
        match name {
            "uniswap_v2_pool" => Some(ProtocolKind::UniswapV2),
            "sushiswap_v2_pool" => Some(ProtocolKind::SushiswapV2),
            "pancakeswap_v2_pool" => Some(ProtocolKind::PancakeswapV2),
            "uniswap_v3_pool" => Some(ProtocolKind::UniswapV3),
            "pancakeswap_v3_pool" => Some(ProtocolKind::PancakeswapV3),
            "uniswap_v4_pool" => Some(ProtocolKind::UniswapV4),
            "ekubo_v2_pool" => Some(ProtocolKind::EkuboV2),
            "ekubo_v3_pool" => Some(ProtocolKind::EkuboV3),
            "fluid_dex_pool" => Some(ProtocolKind::FluidV1),
            "curve_pool" => Some(ProtocolKind::Curve),
            "balancer_v2_pool" => Some(ProtocolKind::BalancerV2),
            "maverick_v2_pool" => Some(ProtocolKind::MaverickV2),
            "erc4626_pool" => Some(ProtocolKind::ERC4626),
            "hashflow_pool" => Some(ProtocolKind::Hashflow),
            "bebop_pool" => Some(ProtocolKind::Bebop),
            "native_relay_pool" => Some(ProtocolKind::NativeRfq),
            "metric_pool" => Some(ProtocolKind::Metric),
            "euclid_pool" => Some(ProtocolKind::Euclid),
            "baibai_pair" => Some(ProtocolKind::Baibai),
            "pancakeswap_infinity_bin_pool" => Some(ProtocolKind::PancakeswapInfinityBin),
            "lunarbase" => Some(ProtocolKind::LunarBase),
            "liquorice_pool" => Some(ProtocolKind::Liquorice),
            _ => None,
        }
    }

    fn from_protocol_system(name: &str) -> Option<Self> {
        match name {
            "uniswap_v2" => Some(ProtocolKind::UniswapV2),
            "sushiswap_v2" => Some(ProtocolKind::SushiswapV2),
            "pancakeswap_v2" => Some(ProtocolKind::PancakeswapV2),
            "uniswap_v3" => Some(ProtocolKind::UniswapV3),
            "pancakeswap_v3" => Some(ProtocolKind::PancakeswapV3),
            "uniswap_v4" => Some(ProtocolKind::UniswapV4),
            "aerodrome_slipstreams" => Some(ProtocolKind::AerodromeSlipstreams),
            "aerodrome_v1" => Some(ProtocolKind::AerodromeV1),
            "ekubo_v2" => Some(ProtocolKind::EkuboV2),
            "ekubo_v3" => Some(ProtocolKind::EkuboV3),
            "fluid_v1" => Some(ProtocolKind::FluidV1),
            "rocketpool" => Some(ProtocolKind::Rocketpool),
            "vm:curve" => Some(ProtocolKind::Curve),
            "vm:balancer_v2" => Some(ProtocolKind::BalancerV2),
            "vm:maverick_v2" => Some(ProtocolKind::MaverickV2),
            "erc4626" => Some(ProtocolKind::ERC4626),
            "rfq:hashflow" => Some(ProtocolKind::Hashflow),
            "rfq:bebop" => Some(ProtocolKind::Bebop),
            "rfq:liquorice" => Some(ProtocolKind::Liquorice),
            "aero_basic" => Some(ProtocolKind::AeroBasic),
            "aero_cl" => Some(ProtocolKind::AeroCl),
            "pancakeswap_infinity_cl" => Some(ProtocolKind::PancakeswapInfinityCl),
            "pancakeswap_infinity_bin" => Some(ProtocolKind::PancakeswapInfinityBin),
            "lunarbase" => Some(ProtocolKind::LunarBase),
            "biconomy" => Some(ProtocolKind::Biconomy),
            "rfq:native" => Some(ProtocolKind::NativeRfq),
            "rfq:metric" => Some(ProtocolKind::Metric),
            "uniswap_v4_hooks" => Some(ProtocolKind::UniswapV4Hooks),
            "ring_swap_v2" => Some(ProtocolKind::RingSwapV2),
            "sky" => Some(ProtocolKind::Sky),
            "lido_v4" => Some(ProtocolKind::LidoV4),
            "etherfi" => Some(ProtocolKind::Etherfi),
            "vm:balancer_v3" => Some(ProtocolKind::BalancerV3),
            "vm:liquidityparty" => Some(ProtocolKind::LiquidityParty),
            "pricelevelstream:fermiswap" => Some(ProtocolKind::TitanFermiSwap),
            "pricelevelstream:kipseli" => Some(ProtocolKind::TitanKipseli),
            "pricelevelstream:bebop" => Some(ProtocolKind::TitanBebop),
            "pricelevelstream:taurusfi" => Some(ProtocolKind::TitanTaurusFi),
            "pricelevelstream:tempest" => Some(ProtocolKind::TitanTempest),
            "pricelevelstream:goliath" => Some(ProtocolKind::TitanGoliath),
            "pricelevelstream:stelaxis" => Some(ProtocolKind::TitanStelaxis),
            "pricelevelstream:elzorro" => Some(ProtocolKind::TitanElZorro),
            "pricelevelstream:0x6e27bb694393840e253262361b79187d4899e9cf" => {
                Some(ProtocolKind::TitanWilbrr)
            }
            "rfq:euclid" => Some(ProtocolKind::Euclid),
            "baibai" => Some(ProtocolKind::Baibai),
            _ => None,
        }
    }
}

impl fmt::Display for ProtocolKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

fn normalize_proto_name(name: &str) -> String {
    name.to_ascii_lowercase().replace(['-', ' '], "_")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tycho_simulation::tycho_common::models::{token::Token, Chain};
    use tycho_simulation::tycho_common::Bytes;

    use super::*;

    const CANONICAL_PROTOCOL_SYSTEM_CASES: [(&str, ProtocolKind); 45] = [
        ("uniswap_v2", ProtocolKind::UniswapV2),
        ("sushiswap_v2", ProtocolKind::SushiswapV2),
        ("pancakeswap_v2", ProtocolKind::PancakeswapV2),
        ("uniswap_v3", ProtocolKind::UniswapV3),
        ("pancakeswap_v3", ProtocolKind::PancakeswapV3),
        ("uniswap_v4", ProtocolKind::UniswapV4),
        ("aerodrome_slipstreams", ProtocolKind::AerodromeSlipstreams),
        ("aerodrome_v1", ProtocolKind::AerodromeV1),
        ("ekubo_v2", ProtocolKind::EkuboV2),
        ("ekubo_v3", ProtocolKind::EkuboV3),
        ("fluid_v1", ProtocolKind::FluidV1),
        ("rocketpool", ProtocolKind::Rocketpool),
        ("vm:curve", ProtocolKind::Curve),
        ("vm:balancer_v2", ProtocolKind::BalancerV2),
        ("vm:maverick_v2", ProtocolKind::MaverickV2),
        ("erc4626", ProtocolKind::ERC4626),
        ("rfq:hashflow", ProtocolKind::Hashflow),
        ("rfq:bebop", ProtocolKind::Bebop),
        ("rfq:liquorice", ProtocolKind::Liquorice),
        ("aero_basic", ProtocolKind::AeroBasic),
        ("aero_cl", ProtocolKind::AeroCl),
        (
            "pancakeswap_infinity_cl",
            ProtocolKind::PancakeswapInfinityCl,
        ),
        (
            "pancakeswap_infinity_bin",
            ProtocolKind::PancakeswapInfinityBin,
        ),
        ("lunarbase", ProtocolKind::LunarBase),
        ("biconomy", ProtocolKind::Biconomy),
        ("rfq:native", ProtocolKind::NativeRfq),
        ("rfq:metric", ProtocolKind::Metric),
        ("uniswap_v4_hooks", ProtocolKind::UniswapV4Hooks),
        ("ring_swap_v2", ProtocolKind::RingSwapV2),
        ("sky", ProtocolKind::Sky),
        ("lido_v4", ProtocolKind::LidoV4),
        ("etherfi", ProtocolKind::Etherfi),
        ("vm:balancer_v3", ProtocolKind::BalancerV3),
        ("vm:liquidityparty", ProtocolKind::LiquidityParty),
        ("pricelevelstream:fermiswap", ProtocolKind::TitanFermiSwap),
        ("pricelevelstream:kipseli", ProtocolKind::TitanKipseli),
        ("pricelevelstream:bebop", ProtocolKind::TitanBebop),
        ("pricelevelstream:taurusfi", ProtocolKind::TitanTaurusFi),
        ("pricelevelstream:tempest", ProtocolKind::TitanTempest),
        ("pricelevelstream:goliath", ProtocolKind::TitanGoliath),
        ("pricelevelstream:stelaxis", ProtocolKind::TitanStelaxis),
        ("pricelevelstream:elzorro", ProtocolKind::TitanElZorro),
        (
            "pricelevelstream:0x6e27bb694393840e253262361b79187d4899e9cf",
            ProtocolKind::TitanWilbrr,
        ),
        ("rfq:euclid", ProtocolKind::Euclid),
        ("baibai", ProtocolKind::Baibai),
    ];

    const CANONICAL_PROTOCOL_TYPE_CASES: [(&str, ProtocolKind); 22] = [
        ("uniswap_v2_pool", ProtocolKind::UniswapV2),
        ("sushiswap_v2_pool", ProtocolKind::SushiswapV2),
        ("pancakeswap_v2_pool", ProtocolKind::PancakeswapV2),
        ("uniswap_v3_pool", ProtocolKind::UniswapV3),
        ("pancakeswap_v3_pool", ProtocolKind::PancakeswapV3),
        ("uniswap_v4_pool", ProtocolKind::UniswapV4),
        ("ekubo_v2_pool", ProtocolKind::EkuboV2),
        ("ekubo_v3_pool", ProtocolKind::EkuboV3),
        ("fluid_dex_pool", ProtocolKind::FluidV1),
        ("curve_pool", ProtocolKind::Curve),
        ("balancer_v2_pool", ProtocolKind::BalancerV2),
        ("maverick_v2_pool", ProtocolKind::MaverickV2),
        ("erc4626_pool", ProtocolKind::ERC4626),
        ("hashflow_pool", ProtocolKind::Hashflow),
        ("bebop_pool", ProtocolKind::Bebop),
        ("native_relay_pool", ProtocolKind::NativeRfq),
        ("metric_pool", ProtocolKind::Metric),
        ("euclid_pool", ProtocolKind::Euclid),
        ("baibai_pair", ProtocolKind::Baibai),
        (
            "pancakeswap_infinity_bin_pool",
            ProtocolKind::PancakeswapInfinityBin,
        ),
        ("lunarbase", ProtocolKind::LunarBase),
        ("liquorice_pool", ProtocolKind::Liquorice),
    ];

    const NON_CANONICAL_ALIASES: [&str; 16] = [
        "uniswapv2",
        "uniswapv3",
        "uniswapv4",
        "aerodromeslipstreams",
        "aerodromev1",
        "curve",
        "balancer_v2",
        "maverick_v2",
        "ekubov3",
        "fluidv1",
        "balancerv2_pool",
        "rocketpool_pool",
        "base_pool",
        "hashflow",
        "bebop",
        "liquorice",
    ];

    fn protocol_component(protocol_system: &str, protocol_type_name: &str) -> ProtocolComponent {
        ProtocolComponent::new(
            Bytes::default(),
            protocol_system.to_string(),
            protocol_type_name.to_string(),
            Chain::Ethereum,
            Vec::<Token>::new(),
            Vec::<Bytes>::new(),
            HashMap::<String, Bytes>::new(),
            Bytes::default(),
            Default::default(),
        )
    }

    fn assert_rejected_everywhere(name: &str) {
        let by_system = protocol_component(name, "");
        let by_type = protocol_component("", name);
        assert_eq!(ProtocolKind::from_component(&by_system), None);
        assert_eq!(ProtocolKind::from_component(&by_type), None);
        assert_eq!(ProtocolKind::from_sync_state_key(name), None);
    }

    #[test]
    fn as_str_returns_canonical_protocol_systems() {
        for (expected, kind) in CANONICAL_PROTOCOL_SYSTEM_CASES {
            assert_eq!(kind.as_str(), expected);
            assert_eq!(kind.to_string(), expected);
        }
    }

    #[test]
    fn canonical_protocol_system_cases_cover_all_protocol_kinds() {
        assert_eq!(
            CANONICAL_PROTOCOL_SYSTEM_CASES.len(),
            ProtocolKind::ALL.len()
        );
        for kind in ProtocolKind::ALL {
            let count = CANONICAL_PROTOCOL_SYSTEM_CASES
                .iter()
                .filter(|(_, listed_kind)| *listed_kind == kind)
                .count();
            assert_eq!(count, 1, "missing or duplicate canonical system case");
        }
    }

    #[test]
    fn exact_protocol_system_parser_preserves_wire_identity() {
        for (name, kind) in CANONICAL_PROTOCOL_SYSTEM_CASES {
            assert_eq!(
                ProtocolKind::from_canonical_protocol_system(name),
                Some(kind)
            );
        }
        for name in ["Uniswap_V2", "uniswap-v2", "uniswap v2", "VM:CURVE"] {
            assert!(ProtocolKind::from_sync_state_key(name).is_some());
            assert_eq!(ProtocolKind::from_canonical_protocol_system(name), None);
        }
        for name in NON_CANONICAL_ALIASES
            .into_iter()
            .chain(["future_protocol", "uniswap_v2_pool"])
        {
            assert_eq!(ProtocolKind::from_canonical_protocol_system(name), None);
        }
    }

    #[test]
    fn recognizes_canonical_protocol_systems_from_component() {
        for (protocol_system, expected) in CANONICAL_PROTOCOL_SYSTEM_CASES {
            let component = protocol_component(protocol_system, "");
            assert_eq!(ProtocolKind::from_component(&component), Some(expected));
        }
    }

    #[test]
    fn recognizes_canonical_protocol_type_names_from_component() {
        for (protocol_type_name, expected) in CANONICAL_PROTOCOL_TYPE_CASES {
            let component = protocol_component("", protocol_type_name);
            assert_eq!(ProtocolKind::from_component(&component), Some(expected));
        }
    }

    #[test]
    fn from_component_prefers_protocol_system_over_protocol_type_name() {
        let component = protocol_component("uniswap_v3", "uniswap_v2_pool");

        assert_eq!(
            ProtocolKind::from_component(&component),
            Some(ProtocolKind::UniswapV3)
        );
    }

    #[test]
    fn from_sync_state_key_recognizes_canonical_protocol_systems_only() {
        for (name, expected) in CANONICAL_PROTOCOL_SYSTEM_CASES {
            assert_eq!(ProtocolKind::from_sync_state_key(name), Some(expected));
        }
    }

    #[test]
    fn rejects_non_canonical_aliases() {
        for alias in NON_CANONICAL_ALIASES {
            assert_rejected_everywhere(alias);
        }
    }

    #[test]
    fn from_sync_state_key_rejects_type_only_names() {
        for (type_name, _) in CANONICAL_PROTOCOL_TYPE_CASES {
            if CANONICAL_PROTOCOL_SYSTEM_CASES
                .iter()
                .any(|(name, _)| *name == type_name)
            {
                continue;
            }
            assert_eq!(ProtocolKind::from_sync_state_key(type_name), None);
        }
    }

    #[test]
    fn from_protocol_hint_accepts_systems_and_type_names() {
        assert_eq!(
            ProtocolKind::from_protocol_hint("rfq:hashflow"),
            Some(ProtocolKind::Hashflow)
        );
        assert_eq!(
            ProtocolKind::from_protocol_hint("hashflow_pool"),
            Some(ProtocolKind::Hashflow)
        );
        assert_eq!(
            ProtocolKind::from_protocol_hint("curve_pool"),
            Some(ProtocolKind::Curve)
        );
        assert_eq!(
            ProtocolKind::from_protocol_hint("uniswap_v2"),
            Some(ProtocolKind::UniswapV2)
        );
    }
}
