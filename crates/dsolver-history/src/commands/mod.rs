mod coverage;
mod job;
mod quotes;
mod service;
mod verify;

use std::path::Path;

use serde::de::DeserializeOwned;

use crate::args::{Args, Command};
use crate::client::{ClientError, HistoryClient};
use crate::exit::{EXIT_INTERRUPTED, EXIT_OPERATION_FAILED, EXIT_USAGE_OR_ADMISSION};
use crate::output::{OutputError, OutputOptions};

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("{0}")]
    Usage(String),
    #[error("{0}")]
    Operation(String),
    #[error("interrupted")]
    Interrupted,
}

impl CliError {
    pub fn usage(message: impl Into<String>) -> Self {
        Self::Usage(message.into())
    }

    pub fn operation(message: impl Into<String>) -> Self {
        Self::Operation(message.into())
    }

    pub fn from_client(error: ClientError) -> Self {
        if error.is_admission_or_authentication() {
            Self::Usage(error.to_string())
        } else {
            Self::Operation(error.to_string())
        }
    }

    pub fn from_output(error: OutputError) -> Self {
        match error {
            OutputError::AlreadyExists(_) | OutputError::MissingParent(_) => {
                Self::Usage(error.to_string())
            }
            OutputError::Io(_) | OutputError::Serialize(_) => Self::Operation(error.to_string()),
        }
    }

    #[must_use]
    pub const fn exit_code(&self) -> u8 {
        match self {
            Self::Usage(_) => EXIT_USAGE_OR_ADMISSION,
            Self::Operation(_) => EXIT_OPERATION_FAILED,
            Self::Interrupted => EXIT_INTERRUPTED,
        }
    }
}

pub async fn execute(args: Args, client: HistoryClient) -> Result<u8, CliError> {
    let output = OutputOptions {
        pretty: args.pretty,
        path: args.output,
        force: args.force,
    };
    output.validate().map_err(CliError::from_output)?;
    match args.command {
        Command::Quotes(args) => quotes::execute(&client, args, &output).await,
        Command::Coverage(args) => coverage::execute(&client, args, &output).await,
        Command::Verify(args) => verify::execute(&client, args, &output).await,
        Command::Job(args) => job::execute(&client, args, &output).await,
        Command::Service(args) => service::execute(&client, args, &output).await,
    }
}

fn read_request<T: DeserializeOwned>(path: &Path) -> Result<T, CliError> {
    let text = if path == Path::new("-") {
        std::io::read_to_string(std::io::stdin()).map_err(|error| {
            CliError::usage(format!("failed to read request from stdin: {error}"))
        })?
    } else {
        std::fs::read_to_string(path).map_err(|error| {
            CliError::usage(format!(
                "failed to read request {}: {error}",
                path.display()
            ))
        })?
    };
    serde_json::from_str(&text)
        .map_err(|error| CliError::usage(format!("request JSON is invalid: {error}")))
}

fn required<T>(value: Option<T>, flag: &str) -> Result<T, CliError> {
    value.ok_or_else(|| CliError::usage(format!("{flag} is required with direct request flags")))
}
