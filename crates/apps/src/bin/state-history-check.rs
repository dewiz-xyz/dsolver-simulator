use std::{collections::BTreeSet, env, process::ExitCode, str::FromStr};

use anyhow::{bail, ensure, Context};
use serde_json::json;
use state_history::{
    Backend, BacktestRequest, CheckpointObjectStore, StateHistoryReader, StateHistoryStore,
};

const DEFAULT_CHAIN_ID: u64 = 8_453;
const DEFAULT_BLOCK_COUNT: u64 = 64;
const DEFAULT_S3_PREFIX: &str = "state-history";
const DEFAULT_S3_REGION: &str = "eu-central-1";

struct Arguments {
    chain_id: u64,
    block_count: u64,
    backends: Vec<Backend>,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(summary) => {
            println!("{summary}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("state-history-check failed: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> anyhow::Result<String> {
    let arguments = Arguments::parse(env::args().skip(1))?;
    let database_url = required_env("STATE_HISTORY_DATABASE_URL")?;
    let bucket = required_env("STATE_HISTORY_S3_BUCKET")?;

    let store = StateHistoryStore::connect(&database_url).await?;
    store.validate_schema().await?;
    let latest_recorded_block = store
        .latest_block_number(arguments.chain_id)
        .await?
        .context("state history has no recorded block times for the requested chain")?;
    let end_block = latest_recorded_block
        .checked_sub(1)
        .context("state history needs a successor block to close the recent range")?;
    let start_block = end_block.saturating_sub(arguments.block_count - 1);

    let objects = checkpoint_object_store_from_env(bucket).await?;
    let reader = StateHistoryReader::new(store.pool().clone(), objects);
    let plan = reader
        .resolve_backtest_range(&BacktestRequest::new(
            arguments.chain_id,
            start_block,
            end_block,
            arguments.backends.clone(),
        )?)
        .await?;
    plan.range.ensure_gap_free()?;
    ensure!(
        !plan.range.legs.is_empty(),
        "gap-free range has no replay legs"
    );

    let mut checkpoint_ids = BTreeSet::new();
    let mut token_hashes = BTreeSet::new();
    let mut delta_count = 0_u64;
    for leg in &plan.range.legs {
        reader.fetch_checkpoint(&leg.checkpoint).await?;
        let token_sha = leg
            .checkpoint
            .token_reference
            .as_ref()
            .with_context(|| {
                format!(
                    "complete checkpoint manifest {} has no token snapshot reference",
                    leg.checkpoint.id
                )
            })?
            .sha256
            .clone();
        let snapshot = reader
            .fetch_checkpoint_token_snapshot(&leg.checkpoint)
            .await?
            .with_context(|| {
                format!(
                    "complete checkpoint manifest {} has no token snapshot reference",
                    leg.checkpoint.id
                )
            })?;
        checkpoint_ids.insert(leg.checkpoint.id);
        token_hashes.insert(token_sha);
        ensure!(
            snapshot.chain_id == arguments.chain_id,
            "token snapshot chain differs from the requested chain"
        );
        delta_count = delta_count
            .checked_add(u64::try_from(leg.deltas.len())?)
            .context("state history delta count overflow")?;
    }

    serde_json::to_string(&json!({
        "event": "state_history_check_complete",
        "chainId": arguments.chain_id,
        "startBlock": start_block,
        "endBlock": end_block,
        "latestRecordedBlock": latest_recorded_block,
        "backends": arguments
            .backends
            .iter()
            .map(|backend| backend.as_str())
            .collect::<Vec<_>>(),
        "replayLegs": plan.range.legs.len(),
        "checkpointsVerified": checkpoint_ids.len(),
        "tokenSnapshotsVerified": token_hashes.len(),
        "deltasVerified": delta_count,
        "startBlockTimestampMs": plan.start_block_timestamp_ms,
        "endBlockTimestampMs": plan.end_block_timestamp_ms,
    }))
    .context("failed to serialize state history check summary")
}

async fn checkpoint_object_store_from_env(bucket: String) -> anyhow::Result<CheckpointObjectStore> {
    let prefix =
        env::var("STATE_HISTORY_S3_PREFIX").unwrap_or_else(|_| DEFAULT_S3_PREFIX.to_owned());
    let region =
        env::var("STATE_HISTORY_S3_REGION").unwrap_or_else(|_| DEFAULT_S3_REGION.to_owned());
    let endpoint_url = optional_env("STATE_HISTORY_S3_ENDPOINT_URL");
    let force_path_style = optional_env("STATE_HISTORY_S3_FORCE_PATH_STYLE")
        .map(|value| parse_bool(&value, "STATE_HISTORY_S3_FORCE_PATH_STYLE"))
        .transpose()?
        .unwrap_or(false);
    Ok(CheckpointObjectStore::from_env_config(
        bucket,
        prefix,
        region,
        endpoint_url,
        force_path_style,
    )
    .await)
}

impl Arguments {
    fn parse(arguments: impl Iterator<Item = String>) -> anyhow::Result<Self> {
        let mut chain_id = DEFAULT_CHAIN_ID;
        let mut block_count = DEFAULT_BLOCK_COUNT;
        let mut backends = vec![Backend::Native, Backend::Vm, Backend::Rfq];
        let mut arguments = arguments;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--chain-id" => {
                    chain_id = next_value(&mut arguments, "--chain-id")?
                        .parse()
                        .context("--chain-id must be a valid u64")?;
                }
                "--blocks" => {
                    block_count = next_value(&mut arguments, "--blocks")?
                        .parse()
                        .context("--blocks must be a valid u64")?;
                }
                "--backends" => {
                    backends = next_value(&mut arguments, "--backends")?
                        .split(',')
                        .map(Backend::from_str)
                        .collect::<Result<Vec<_>, _>>()?;
                    backends.sort_unstable();
                    backends.dedup();
                }
                "-h" | "--help" => {
                    println!(
                        "Usage: state-history-check [--chain-id <id>] [--blocks <count>] [--backends native,vm,rfq]"
                    );
                    std::process::exit(0);
                }
                _ => bail!("unknown argument {argument}"),
            }
        }
        ensure!(block_count > 0, "--blocks must be greater than zero");
        ensure!(!backends.is_empty(), "--backends must not be empty");
        Ok(Self {
            chain_id,
            block_count,
            backends,
        })
    }
}

fn next_value(arguments: &mut impl Iterator<Item = String>, flag: &str) -> anyhow::Result<String> {
    arguments
        .next()
        .with_context(|| format!("{flag} requires a value"))
}

fn required_env(name: &str) -> anyhow::Result<String> {
    let value = env::var(name)
        .with_context(|| format!("required environment variable {name} is missing"))?;
    ensure!(
        !value.trim().is_empty(),
        "required environment variable {name} is empty"
    );
    Ok(value)
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn parse_bool(value: &str, name: &str) -> anyhow::Result<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => bail!("{name} must be true or false"),
    }
}
