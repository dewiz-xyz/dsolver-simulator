use std::time::Duration;

use tokio::time::Instant;

use crate::args::{ServiceArgs, ServiceCommand};
use crate::client::HistoryClient;
use crate::exit::EXIT_SUCCESS;
use crate::output::OutputOptions;

use super::CliError;

const REQUEST_DEADLINE: Duration = Duration::from_secs(30);

pub async fn execute(
    client: &HistoryClient,
    args: ServiceArgs,
    output: &OutputOptions,
) -> Result<u8, CliError> {
    match args.command {
        ServiceCommand::Status => {
            let status = client
                .status(Instant::now() + REQUEST_DEADLINE)
                .await
                .map_err(CliError::from_client)?;
            output.write(&status).map_err(CliError::from_output)?;
            Ok(EXIT_SUCCESS)
        }
    }
}
