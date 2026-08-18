use std::path::PathBuf;
use std::str::FromStr;

use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use historical_quote::api::{Backend, CheckpointPair, StreamPosition};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(
    name = "dsolver-history",
    version,
    about = "HTTP client for historical pool quotes"
)]
pub struct Args {
    #[arg(long, env = "DSOLVER_HISTORY_URL", global = true, value_name = "URL")]
    pub url: Option<String>,

    #[arg(long, global = true)]
    pub pretty: bool,

    #[arg(long, global = true, value_name = "PATH")]
    pub output: Option<PathBuf>,

    #[arg(long, global = true, requires = "output")]
    pub force: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Quotes(QuotesArgs),
    Coverage(CoverageArgs),
    Verify(VerifyArgs),
    Job(JobArgs),
    Service(ServiceArgs),
}

#[derive(Debug, ClapArgs)]
pub struct QuotesArgs {
    #[arg(long, value_name = "PATH")]
    pub request: Option<PathBuf>,

    #[arg(long)]
    pub submit_only: bool,

    #[arg(long)]
    pub job_envelope: bool,

    #[command(flatten)]
    pub direct: DirectQuoteArgs,
}

#[derive(Debug, Default, ClapArgs)]
pub struct DirectQuoteArgs {
    #[arg(long)]
    pub request_id: Option<Uuid>,
    #[arg(long)]
    pub timeout_ms: Option<u64>,
    #[arg(long)]
    pub chain_id: Option<u64>,
    #[arg(long)]
    pub backend: Option<BackendArg>,
    #[arg(long)]
    pub protocol: Option<String>,
    #[arg(long)]
    pub component_id: Option<String>,
    #[arg(long)]
    pub token_in: Option<String>,
    #[arg(long)]
    pub token_out: Option<String>,
    #[arg(long)]
    pub start_block: Option<u64>,
    #[arg(long)]
    pub end_block_inclusive: Option<u64>,
    #[arg(long)]
    pub step: Option<u64>,
    #[arg(long, value_delimiter = ',', action = clap::ArgAction::Append)]
    pub lag: Vec<u64>,
    #[arg(long, value_delimiter = ',', action = clap::ArgAction::Append)]
    pub amount_in: Vec<String>,
}

impl DirectQuoteArgs {
    pub fn has_any(&self) -> bool {
        self.request_id.is_some()
            || self.timeout_ms.is_some()
            || self.chain_id.is_some()
            || self.backend.is_some()
            || self.protocol.is_some()
            || self.component_id.is_some()
            || self.token_in.is_some()
            || self.token_out.is_some()
            || self.start_block.is_some()
            || self.end_block_inclusive.is_some()
            || self.step.is_some()
            || !self.lag.is_empty()
            || !self.amount_in.is_empty()
    }
}

#[derive(Debug, ClapArgs)]
pub struct CoverageArgs {
    #[arg(long)]
    pub chain_id: u64,
    #[arg(long, value_delimiter = ',', action = clap::ArgAction::Append, required = true)]
    pub backend: Vec<BackendArg>,
    #[arg(long, requires = "end_block_inclusive")]
    pub start_block: Option<u64>,
    #[arg(long, requires = "start_block")]
    pub end_block_inclusive: Option<u64>,
}

#[derive(Debug, ClapArgs)]
pub struct VerifyArgs {
    #[arg(long, value_name = "PATH")]
    pub request: Option<PathBuf>,
    #[arg(long)]
    pub job_envelope: bool,
    #[command(flatten)]
    pub direct: DirectVerifyArgs,
}

#[derive(Debug, Default, ClapArgs)]
pub struct DirectVerifyArgs {
    #[arg(long)]
    pub request_id: Option<Uuid>,
    #[arg(long)]
    pub timeout_ms: Option<u64>,
    #[arg(long)]
    pub chain_id: Option<u64>,
    #[arg(long, value_delimiter = ',', action = clap::ArgAction::Append)]
    pub backend: Vec<BackendArg>,
    #[arg(long, value_name = "GEN:SEQ..GEN:SEQ", action = clap::ArgAction::Append)]
    pub checkpoint_pair: Vec<CheckpointPairArg>,
    #[arg(long, requires = "end_block_inclusive")]
    pub start_block: Option<u64>,
    #[arg(long, requires = "start_block")]
    pub end_block_inclusive: Option<u64>,
    #[arg(long)]
    pub quote_backend: Option<BackendArg>,
    #[arg(long)]
    pub protocol: Option<String>,
    #[arg(long)]
    pub component_id: Option<String>,
    #[arg(long)]
    pub token_in: Option<String>,
    #[arg(long)]
    pub token_out: Option<String>,
    #[arg(long, value_delimiter = ',', action = clap::ArgAction::Append)]
    pub amount_in: Vec<String>,
}

impl DirectVerifyArgs {
    pub fn has_any(&self) -> bool {
        self.request_id.is_some()
            || self.timeout_ms.is_some()
            || self.chain_id.is_some()
            || !self.backend.is_empty()
            || !self.checkpoint_pair.is_empty()
            || self.start_block.is_some()
            || self.end_block_inclusive.is_some()
            || self.quote_backend.is_some()
            || self.protocol.is_some()
            || self.component_id.is_some()
            || self.token_in.is_some()
            || self.token_out.is_some()
            || !self.amount_in.is_empty()
    }
}

#[derive(Debug, ClapArgs)]
pub struct JobArgs {
    #[command(subcommand)]
    pub command: JobCommand,
}

#[derive(Debug, Subcommand)]
pub enum JobCommand {
    Get(JobReadArgs),
    Wait(JobReadArgs),
    Cancel(JobIdArgs),
}

#[derive(Debug, ClapArgs)]
pub struct JobReadArgs {
    pub job_id: Uuid,
    #[arg(long)]
    pub job_envelope: bool,
}

#[derive(Debug, ClapArgs)]
pub struct JobIdArgs {
    pub job_id: Uuid,
}

#[derive(Debug, ClapArgs)]
pub struct ServiceArgs {
    #[command(subcommand)]
    pub command: ServiceCommand,
}

#[derive(Debug, Subcommand)]
pub enum ServiceCommand {
    Status,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum BackendArg {
    Native,
    Vm,
    Rfq,
}

impl From<BackendArg> for Backend {
    fn from(value: BackendArg) -> Self {
        match value {
            BackendArg::Native => Self::Native,
            BackendArg::Vm => Self::Vm,
            BackendArg::Rfq => Self::Rfq,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CheckpointPairArg(pub CheckpointPair);

impl FromStr for CheckpointPairArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (earlier, target) = value
            .split_once("..")
            .ok_or_else(|| "checkpoint pair must use GEN:SEQ..GEN:SEQ".to_owned())?;
        Ok(Self(CheckpointPair {
            earlier_position: parse_position(earlier)?,
            target_position: parse_position(target)?,
        }))
    }
}

fn parse_position(value: &str) -> Result<StreamPosition, String> {
    let (generation, message_seq) = value
        .split_once(':')
        .ok_or_else(|| "stream position must use GEN:SEQ".to_owned())?;
    Ok(StreamPosition {
        generation: generation
            .parse()
            .map_err(|_| "stream generation must be a valid u64".to_owned())?,
        message_seq: message_seq
            .parse()
            .map_err(|_| "message sequence must be a valid u64".to_owned())?,
    })
}
