use std::time::Duration;

use historical_quote::api::{BlockBounds, CoverageRequest, API_REVISION};
use serde_json::json;
use tokio::time::Instant;

use crate::args::CoverageArgs;
use crate::client::HistoryClient;
use crate::exit::EXIT_SUCCESS;
use crate::output::OutputOptions;

use super::CliError;

const REQUEST_DEADLINE: Duration = Duration::from_secs(30);

pub async fn execute(
    client: &HistoryClient,
    args: CoverageArgs,
    output: &OutputOptions,
) -> Result<u8, CliError> {
    let bounds = match (args.start_block, args.end_block_inclusive) {
        (Some(start), Some(end_inclusive)) => Some(BlockBounds {
            start,
            end_inclusive,
        }),
        (None, None) => None,
        _ => {
            return Err(CliError::usage(
                "coverage bounds must include both endpoints",
            ))
        }
    };
    let request: CoverageRequest = serde_json::from_value(json!({
        "apiRevision": API_REVISION,
        "chainId": args.chain_id,
        "backends": args.backend.into_iter().map(historical_quote::api::Backend::from).collect::<Vec<_>>(),
        "bounds": bounds,
    }))
    .map_err(|error| CliError::usage(error.to_string()))?;
    let response = client
        .coverage(&request, Instant::now() + REQUEST_DEADLINE)
        .await
        .map_err(CliError::from_client)?;
    output.write(&response).map_err(CliError::from_output)?;
    Ok(EXIT_SUCCESS)
}
