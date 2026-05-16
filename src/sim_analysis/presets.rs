use anyhow::{anyhow, Result};

#[derive(Clone, Debug)]
pub struct SimulateScenarioPreset {
    pub label: &'static str,
    pub token_in_symbol: &'static str,
    pub token_out_symbol: &'static str,
    pub amounts: &'static [&'static str],
    pub tags: &'static [&'static str],
    pub expect_rfq_visibility: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodeRouteKind {
    GenericMultihop,
    BebopPartialFill,
}

#[derive(Clone, Debug)]
pub struct EncodeRoutePreset {
    pub label: &'static str,
    pub kind: EncodeRouteKind,
    pub token_in_symbol: &'static str,
    pub mid_symbol: &'static str,
    pub token_out_symbol: &'static str,
    pub amounts: &'static [&'static str],
    pub settlement_address: &'static str,
    pub tycho_router_address: &'static str,
}

#[derive(Clone, Debug)]
pub struct BalancedProfilePreset {
    pub simulate_scenarios: Vec<SimulateScenarioPreset>,
    pub latency_scenarios: Vec<SimulateScenarioPreset>,
    pub stress_scenarios: Vec<SimulateScenarioPreset>,
    pub encode_routes: Vec<EncodeRoutePreset>,
    pub latency_requests: usize,
    pub latency_concurrency: usize,
    pub stress_requests: usize,
    pub stress_concurrency: usize,
}

const ETHEREUM_TOKENS: &[(&str, &str)] = &[
    ("ETH", "0x0000000000000000000000000000000000000000"),
    ("WETH", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
    ("WBTC", "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"),
    ("USDC", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
    ("USDT", "0xdac17f958d2ee523a2206206994597c13d831ec7"),
    ("DAI", "0x6b175474e89094c44da98b954eedeac495271d0f"),
    ("STETH", "0xae7ab96520de3a18e5e111b5eaab095312d7fe84"),
    ("LINK", "0x514910771af9ca656af840dff83e8264ecf986ca"),
    ("LDO", "0x5a98fcbea516cf06857215779fd812ca3bef1b32"),
    ("RETH", "0xae78736cd615f374d3085123a210448e74fc6393"),
];

const BASE_TOKENS: &[(&str, &str)] = &[
    ("ETH", "0x0000000000000000000000000000000000000000"),
    ("WETH", "0x4200000000000000000000000000000000000006"),
    ("USDC", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
    ("AERO", "0x940181a94a35a4569e4529a3cdfb74e38fd98631"),
    ("DAI", "0x50c5725949a6f0c72e6c4a641f24049a917db0cb"),
];

const ETH_LARGE_AMOUNTS: &[&str] = &[
    "100000000000000000",
    "500000000000000000",
    "1000000000000000000",
    "2000000000000000000",
];

const STABLE_AMOUNTS: &[&str] = &["1000000", "5000000", "10000000", "50000000"];
const LINK_AMOUNTS: &[&str] = &[
    "1000000000000000000",
    "5000000000000000000",
    "10000000000000000000",
    "50000000000000000000",
];
const ETH_RETH_AMOUNTS: &[&str] = &[
    "100000000000000000",
    "500000000000000000",
    "1000000000000000000",
    "2000000000000000000",
];
const BASE_ETH_AMOUNTS: &[&str] = &[
    "10000000000000000",
    "50000000000000000",
    "100000000000000000",
    "500000000000000000",
];

pub fn resolve_token(chain_id: u64, symbol: &str) -> Result<&'static str> {
    let token = tokens_for_chain(chain_id)
        .iter()
        .find_map(|(known_symbol, address)| (*known_symbol == symbol).then_some(*address));
    token.ok_or_else(|| anyhow!("unknown token {symbol} for chain {chain_id}"))
}

pub fn chain_label(chain_id: u64) -> &'static str {
    match chain_id {
        1 => "ethereum",
        8453 => "base",
        _ => "unknown",
    }
}

pub fn balanced_profile(chain_id: u64, vm_enabled: bool) -> Result<BalancedProfilePreset> {
    match chain_id {
        1 => Ok(ethereum_balanced_profile(vm_enabled)),
        8453 => Ok(base_balanced_profile()),
        _ => Err(anyhow!(
            "unsupported chain id {chain_id}; supported values are 1 and 8453"
        )),
    }
}

fn ethereum_balanced_profile(vm_enabled: bool) -> BalancedProfilePreset {
    BalancedProfilePreset {
        simulate_scenarios: ethereum_simulate_scenarios(),
        latency_scenarios: ethereum_latency_scenarios(),
        stress_scenarios: ethereum_stress_scenarios(),
        encode_routes: vec![ethereum_encode_preset()],
        latency_requests: 36,
        latency_concurrency: if vm_enabled { 4 } else { 6 },
        stress_requests: 72,
        stress_concurrency: if vm_enabled { 6 } else { 10 },
    }
}

fn base_balanced_profile() -> BalancedProfilePreset {
    BalancedProfilePreset {
        simulate_scenarios: base_simulate_scenarios(),
        latency_scenarios: base_latency_scenarios(),
        stress_scenarios: base_stress_scenarios(),
        encode_routes: base_encode_presets(),
        latency_requests: 36,
        latency_concurrency: 6,
        stress_requests: 72,
        stress_concurrency: 10,
    }
}

fn ethereum_simulate_scenarios() -> Vec<SimulateScenarioPreset> {
    vec![
        scenario(
            "stable-dai-usdc",
            "DAI",
            "USDC",
            STABLE_AMOUNTS,
            &["stables"],
        ),
        scenario(
            "stable-usdc-usdt",
            "USDC",
            "USDT",
            STABLE_AMOUNTS,
            &["stables"],
        ),
        rfq_scenario(
            "rfq-usdc-weth",
            "USDC",
            "WETH",
            STABLE_AMOUNTS,
            &["stables"],
        ),
        rfq_scenario(
            "rfq-weth-usdc",
            "WETH",
            "USDC",
            ETH_LARGE_AMOUNTS,
            &["native"],
        ),
        scenario(
            "lst-steth-weth",
            "STETH",
            "WETH",
            ETH_LARGE_AMOUNTS,
            &["lst", "vm-sensitive"],
        ),
        scenario(
            "governance-link-weth",
            "LINK",
            "WETH",
            LINK_AMOUNTS,
            &["governance"],
        ),
        scenario(
            "native-eth-reth",
            "ETH",
            "RETH",
            ETH_RETH_AMOUNTS,
            &["native", "lst"],
        ),
    ]
}

fn ethereum_latency_scenarios() -> Vec<SimulateScenarioPreset> {
    vec![
        scenario(
            "latency-dai-usdc",
            "DAI",
            "USDC",
            STABLE_AMOUNTS,
            &["latency"],
        ),
        scenario(
            "latency-steth-weth",
            "STETH",
            "WETH",
            ETH_LARGE_AMOUNTS,
            &["latency", "vm-sensitive"],
        ),
        scenario(
            "latency-link-weth",
            "LINK",
            "WETH",
            LINK_AMOUNTS,
            &["latency"],
        ),
    ]
}

fn ethereum_stress_scenarios() -> Vec<SimulateScenarioPreset> {
    vec![
        scenario(
            "stress-dai-usdc",
            "DAI",
            "USDC",
            STABLE_AMOUNTS,
            &["stress"],
        ),
        scenario(
            "stress-usdc-usdt",
            "USDC",
            "USDT",
            STABLE_AMOUNTS,
            &["stress"],
        ),
    ]
}

fn ethereum_encode_preset() -> EncodeRoutePreset {
    EncodeRoutePreset {
        label: "generic-multihop-encode",
        kind: EncodeRouteKind::GenericMultihop,
        token_in_symbol: "DAI",
        mid_symbol: "USDC",
        token_out_symbol: "USDT",
        amounts: &[
            "1000000000000000000",
            "5000000000000000000",
            "10000000000000000000",
            "50000000000000000000",
        ],
        settlement_address: "0x9008D19f58AAbD9eD0D60971565AA8510560ab41",
        tycho_router_address: "0xfD0b31d2E955fA55e3fa641Fe90e08b677188d35",
    }
}

fn base_simulate_scenarios() -> Vec<SimulateScenarioPreset> {
    vec![
        rfq_scenario(
            "stable-usdc-weth",
            "USDC",
            "WETH",
            STABLE_AMOUNTS,
            &["base-core"],
        ),
        rfq_scenario(
            "stable-weth-usdc",
            "WETH",
            "USDC",
            BASE_ETH_AMOUNTS,
            &["base-core"],
        ),
        scenario(
            "native-eth-usdc",
            "ETH",
            "USDC",
            BASE_ETH_AMOUNTS,
            &["native"],
        ),
        scenario(
            "governance-aero-usdc",
            "AERO",
            "USDC",
            LINK_AMOUNTS,
            &["governance"],
        ),
        scenario(
            "governance-usdc-aero",
            "USDC",
            "AERO",
            STABLE_AMOUNTS,
            &["governance"],
        ),
    ]
}

fn base_latency_scenarios() -> Vec<SimulateScenarioPreset> {
    vec![
        scenario(
            "latency-usdc-weth",
            "USDC",
            "WETH",
            STABLE_AMOUNTS,
            &["latency"],
        ),
        scenario(
            "latency-weth-usdc",
            "WETH",
            "USDC",
            BASE_ETH_AMOUNTS,
            &["latency"],
        ),
        scenario(
            "latency-aero-usdc",
            "AERO",
            "USDC",
            LINK_AMOUNTS,
            &["latency"],
        ),
    ]
}

fn base_stress_scenarios() -> Vec<SimulateScenarioPreset> {
    vec![
        scenario(
            "stress-usdc-weth",
            "USDC",
            "WETH",
            STABLE_AMOUNTS,
            &["stress"],
        ),
        scenario(
            "stress-eth-usdc",
            "ETH",
            "USDC",
            BASE_ETH_AMOUNTS,
            &["stress"],
        ),
    ]
}

fn base_encode_presets() -> Vec<EncodeRoutePreset> {
    vec![
        EncodeRoutePreset {
            label: "generic-multihop-encode",
            kind: EncodeRouteKind::GenericMultihop,
            token_in_symbol: "USDC",
            mid_symbol: "WETH",
            token_out_symbol: "USDC",
            amounts: &["1000000", "5000000", "10000000", "50000000"],
            settlement_address: "0x9008D19f58AAbD9eD0D60971565AA8510560ab41",
            tycho_router_address: "0xea3207778e39EB02D72C9D3c4Eac7E224ac5d369",
        },
        EncodeRoutePreset {
            label: "bebop-partial-fill-encode",
            kind: EncodeRouteKind::BebopPartialFill,
            token_in_symbol: "USDC",
            mid_symbol: "WETH",
            token_out_symbol: "USDC",
            amounts: &["1000000", "5000000", "10000000", "50000000"],
            settlement_address: "0x9008D19f58AAbD9eD0D60971565AA8510560ab41",
            tycho_router_address: "0xea3207778e39EB02D72C9D3c4Eac7E224ac5d369",
        },
    ]
}

fn scenario(
    label: &'static str,
    token_in_symbol: &'static str,
    token_out_symbol: &'static str,
    amounts: &'static [&'static str],
    tags: &'static [&'static str],
) -> SimulateScenarioPreset {
    SimulateScenarioPreset {
        label,
        token_in_symbol,
        token_out_symbol,
        amounts,
        tags,
        expect_rfq_visibility: false,
    }
}

fn rfq_scenario(
    label: &'static str,
    token_in_symbol: &'static str,
    token_out_symbol: &'static str,
    amounts: &'static [&'static str],
    tags: &'static [&'static str],
) -> SimulateScenarioPreset {
    SimulateScenarioPreset {
        label,
        token_in_symbol,
        token_out_symbol,
        amounts,
        tags: rfq_tags(tags),
        expect_rfq_visibility: true,
    }
}

fn tokens_for_chain(chain_id: u64) -> &'static [(&'static str, &'static str)] {
    match chain_id {
        1 => ETHEREUM_TOKENS,
        8453 => BASE_TOKENS,
        _ => &[],
    }
}

fn rfq_tags(tags: &'static [&'static str]) -> &'static [&'static str] {
    match tags {
        ["stables"] => &["stables", "rfq-targeted"],
        ["native"] => &["native", "rfq-targeted"],
        ["base-core"] => &["base-core", "rfq-targeted"],
        _ => &["rfq-targeted"],
    }
}

#[cfg(test)]
mod tests {
    use super::{balanced_profile, LINK_AMOUNTS, STABLE_AMOUNTS};

    #[test]
    fn ethereum_balanced_profile_includes_rfq_targeted_scenarios() {
        let profile_result = balanced_profile(1, false);
        assert!(profile_result.is_ok());
        let Some(profile) = profile_result.ok() else {
            return;
        };

        assert!(profile
            .simulate_scenarios
            .iter()
            .any(|scenario| scenario.expect_rfq_visibility));
        assert!(profile
            .simulate_scenarios
            .iter()
            .filter(|scenario| scenario.expect_rfq_visibility)
            .all(|scenario| scenario.tags.contains(&"rfq-targeted")));
    }

    #[test]
    fn base_balanced_profile_includes_rfq_targeted_scenarios() {
        let profile_result = balanced_profile(8453, false);
        assert!(profile_result.is_ok());
        let Some(profile) = profile_result.ok() else {
            return;
        };

        assert!(profile
            .simulate_scenarios
            .iter()
            .any(|scenario| scenario.expect_rfq_visibility));
        assert!(profile
            .simulate_scenarios
            .iter()
            .filter(|scenario| scenario.expect_rfq_visibility)
            .all(|scenario| scenario.tags.contains(&"rfq-targeted")));

        assert!(matches!(
            profile
                .simulate_scenarios
                .iter()
                .find(|scenario| scenario.label == "governance-aero-usdc"),
            Some(scenario)
                if scenario.token_in_symbol == "AERO"
                    && scenario.token_out_symbol == "USDC"
                    && scenario.amounts == LINK_AMOUNTS
                    && scenario.tags.contains(&"governance")
        ));

        assert!(matches!(
            profile
                .simulate_scenarios
                .iter()
                .find(|scenario| scenario.label == "governance-usdc-aero"),
            Some(scenario)
                if scenario.token_in_symbol == "USDC"
                    && scenario.token_out_symbol == "AERO"
                    && scenario.amounts == STABLE_AMOUNTS
                    && scenario.tags.contains(&"governance")
        ));

        assert!(matches!(
            profile
                .latency_scenarios
                .iter()
                .find(|scenario| scenario.label == "latency-aero-usdc"),
            Some(scenario)
                if scenario.token_in_symbol == "AERO"
                    && scenario.token_out_symbol == "USDC"
                    && scenario.amounts == LINK_AMOUNTS
        ));

        assert!(!profile
            .simulate_scenarios
            .iter()
            .chain(profile.latency_scenarios.iter())
            .any(|scenario| {
                matches!(
                    scenario.label,
                    "stable-dai-usdc" | "stable-usdc-dai" | "latency-dai-usdc"
                ) || scenario.token_in_symbol == "DAI"
                    || scenario.token_out_symbol == "DAI"
            }));
    }
}
