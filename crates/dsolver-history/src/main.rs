mod args;
mod client;
mod commands;
mod exit;
mod output;
mod retry;

use std::process::ExitCode;

use clap::Parser;

use args::Args;
use client::HistoryClient;
use commands::CliError;

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    match run(args).await {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(error.exit_code())
        }
    }
}

async fn run(args: Args) -> Result<u8, CliError> {
    let url = args.url.as_deref().ok_or_else(|| {
        CliError::usage("DSOLVER_HISTORY_URL is missing and --url was not supplied")
    })?;
    let token = std::env::var("DSOLVER_HISTORY_TOKEN")
        .map_err(|_| CliError::usage("DSOLVER_HISTORY_TOKEN is missing"))?;
    if token.trim().is_empty() {
        return Err(CliError::usage("DSOLVER_HISTORY_TOKEN is empty"));
    }
    let client = HistoryClient::new(url, token).map_err(CliError::usage)?;
    commands::execute(args, client).await
}
