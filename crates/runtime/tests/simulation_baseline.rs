use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    env, fs,
    path::PathBuf,
};

use anyhow::{anyhow, bail, Context, Result};
use num_bigint::BigUint;
use serde::{Deserialize, Serialize};
use tycho_simulation::{
    evm::protocol::{
        aerodrome_slipstreams::state::AerodromeSlipstreamsState, uniswap_v2::state::UniswapV2State,
        uniswap_v3::state::UniswapV3State, uniswap_v4::state::UniswapV4State,
    },
    protocol::models::{DecoderContext, TryFromWithBlock},
    tycho_client::feed::{
        dto::ComponentWithState as DtoComponentWithState, synchronizer::ComponentWithState,
        BlockHeader,
    },
    tycho_common::{
        models::{token::Token, Chain},
        simulation::protocol_sim::ProtocolSim,
    },
};

const UPDATE_ENV: &str = "UPDATE_SIM_BASELINE";
const SNAPSHOTS_JSON: &str = include_str!("fixtures/simulation_baseline_snapshots.json");
const BASELINE_JSON: &str = include_str!("baselines/tycho_simulation.json");

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
struct BaselineEntry {
    amount_out: String,
    gas: String,
}

type Baseline = BTreeMap<String, BTreeMap<String, BaselineEntry>>;

#[tokio::test]
#[ignore = "run explicitly when comparing Tycho simulation versions"]
async fn tycho_simulation_amount_and_gas_baseline() -> Result<()> {
    let snapshots: BTreeMap<String, DtoComponentWithState> =
        serde_json::from_str(SNAPSHOTS_JSON).context("parse simulation snapshots")?;
    let mut actual = Baseline::new();

    for (protocol, snapshot) in snapshots {
        let amounts = input_amounts(&protocol)?;
        let (state, token_in, token_out) = decode_state(&protocol, snapshot.into()).await?;
        let entries = amounts
            .into_iter()
            .map(|amount| {
                let result = state
                    .get_amount_out(amount.clone(), &token_in, &token_out)
                    .with_context(|| format!("simulate {protocol} amount {amount}"))?;
                Ok((
                    amount.to_string(),
                    BaselineEntry {
                        amount_out: result.amount.to_string(),
                        gas: result.gas.to_string(),
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        actual.insert(protocol, entries);
    }

    if env::var(UPDATE_ENV).as_deref() == Ok("1") {
        let path = baseline_path();
        let mut json = serde_json::to_string_pretty(&actual).context("serialize baseline")?;
        json.push('\n');
        fs::write(&path, json).with_context(|| format!("write {}", path.display()))?;
        return Ok(());
    }

    let expected: Baseline =
        serde_json::from_str(BASELINE_JSON).context("parse committed simulation baseline")?;
    let differences = baseline_differences(&expected, &actual);
    if !differences.is_empty() {
        bail!(
            "Tycho simulation baseline mismatch:\n{}\n\
             Regenerate intentionally with {UPDATE_ENV}=1.",
            differences.join("\n")
        );
    }
    Ok(())
}

fn input_amounts(protocol: &str) -> Result<[BigUint; 3]> {
    let decimal_amounts = match protocol {
        "uniswap_v2" | "uniswap_v3" | "pancakeswap_v3" | "aerodrome_slipstreams" => {
            ["1000000000000", "1000000000000000", "100000000000000000"]
        }
        "uniswap_v4" => ["1000000000000", "10000000000000000", "100000000000000000"],
        other => bail!("missing pinned amounts for {other}"),
    };
    let [small, medium, large] = decimal_amounts.map(|amount| {
        amount
            .parse()
            .map_err(|error| anyhow!("invalid pinned amount {amount}: {error}"))
    });
    Ok([small?, medium?, large?])
}

async fn decode_state(
    protocol: &str,
    snapshot: ComponentWithState,
) -> Result<(Box<dyn ProtocolSim>, Token, Token)> {
    let tokens = tokens_for(&snapshot)?;
    let token_in = tokens
        .first()
        .cloned()
        .context("snapshot is missing token 0")?;
    let token_out = tokens
        .get(1)
        .cloned()
        .context("snapshot is missing token 1")?;
    let all_tokens = tokens
        .into_iter()
        .map(|token| (token.address.clone(), token))
        .collect::<HashMap<_, _>>();
    let block = BlockHeader {
        number: 20_000_000,
        timestamp: 1_700_000_000,
        ..Default::default()
    };
    let account_balances = HashMap::new();
    let context = DecoderContext::new();

    let state: Box<dyn ProtocolSim> = match protocol {
        "uniswap_v2" => Box::new(
            UniswapV2State::try_from_with_header(
                snapshot,
                block,
                &account_balances,
                &all_tokens,
                &context,
            )
            .await?,
        ),
        "uniswap_v3" | "pancakeswap_v3" => Box::new(
            UniswapV3State::try_from_with_header(
                snapshot,
                block,
                &account_balances,
                &all_tokens,
                &context,
            )
            .await?,
        ),
        "uniswap_v4" => Box::new(
            UniswapV4State::try_from_with_header(
                snapshot,
                block,
                &account_balances,
                &all_tokens,
                &context,
            )
            .await?,
        ),
        "aerodrome_slipstreams" => Box::new(
            AerodromeSlipstreamsState::try_from_with_header(
                snapshot,
                block,
                &account_balances,
                &all_tokens,
                &context,
            )
            .await?,
        ),
        other => bail!("missing decoder for {other}"),
    };
    Ok((state, token_in, token_out))
}

fn tokens_for(snapshot: &ComponentWithState) -> Result<Vec<Token>> {
    snapshot
        .component
        .tokens
        .iter()
        .enumerate()
        .map(|(index, address)| {
            if address.len() != 20 {
                bail!("token {index} has {} bytes, expected 20", address.len());
            }
            Ok(Token::new(
                address,
                &format!("TOKEN{index}"),
                18,
                0,
                &[Some(10_000)],
                Chain::Base,
                100,
            ))
        })
        .collect()
}

fn baseline_differences(expected: &Baseline, actual: &Baseline) -> Vec<String> {
    let protocols = expected
        .keys()
        .chain(actual.keys())
        .collect::<BTreeSet<_>>();
    let mut differences = Vec::new();

    for protocol in protocols {
        let expected_entries = expected.get(protocol);
        let actual_entries = actual.get(protocol);
        let amounts = expected_entries
            .into_iter()
            .flat_map(|entries| entries.keys())
            .chain(
                actual_entries
                    .into_iter()
                    .flat_map(|entries| entries.keys()),
            )
            .collect::<BTreeSet<_>>();

        for amount in amounts {
            let expected_entry = expected_entries.and_then(|entries| entries.get(amount));
            let actual_entry = actual_entries.and_then(|entries| entries.get(amount));
            if expected_entry != actual_entry {
                differences.push(format!(
                    "{protocol} amount {amount}: expected {}, actual {}",
                    display_entry(expected_entry),
                    display_entry(actual_entry)
                ));
            }
        }
    }
    differences
}

fn display_entry(entry: Option<&BaselineEntry>) -> String {
    entry.map_or_else(
        || "<missing>".to_string(),
        |entry| format!("amount_out={} gas={}", entry.amount_out, entry.gas),
    )
}

fn baseline_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/baselines/tycho_simulation.json")
}
