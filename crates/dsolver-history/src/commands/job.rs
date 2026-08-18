use std::time::Duration;

use historical_quote::api::{JobEnvelope, JobProgress, JobResult, JobState};
use tokio::time::Instant;
use uuid::Uuid;

use crate::args::{JobArgs, JobCommand, JobReadArgs};
use crate::client::HistoryClient;
use crate::exit::{EXIT_OPERATION_FAILED, EXIT_SUCCESS};
use crate::output::OutputOptions;
use crate::retry::sleep_before;

use super::CliError;

const JOB_COMMAND_DEADLINE: Duration = Duration::from_secs(300);
const DEFAULT_POLL_DELAY: Duration = Duration::from_secs(2);
const CANCEL_DEADLINE: Duration = Duration::from_secs(2);

pub async fn execute(
    client: &HistoryClient,
    args: JobArgs,
    output: &OutputOptions,
) -> Result<u8, CliError> {
    match args.command {
        JobCommand::Get(args) => get(client, args, output).await,
        JobCommand::Wait(args) => wait_interruptibly(client, args, output).await,
        JobCommand::Cancel(args) => {
            let envelope = client
                .cancel(args.job_id, Instant::now() + JOB_COMMAND_DEADLINE)
                .await
                .map_err(CliError::from_client)?;
            output.write(&envelope).map_err(CliError::from_output)?;
            Ok(EXIT_SUCCESS)
        }
    }
}

async fn get(
    client: &HistoryClient,
    args: JobReadArgs,
    output: &OutputOptions,
) -> Result<u8, CliError> {
    let deadline = Instant::now() + JOB_COMMAND_DEADLINE;
    let response = client
        .poll(args.job_id, deadline)
        .await
        .map_err(CliError::from_client)?;
    emit_polled(response.envelope, args.job_envelope, output)
}

async fn wait_interruptibly(
    client: &HistoryClient,
    args: JobReadArgs,
    output: &OutputOptions,
) -> Result<u8, CliError> {
    let deadline = Instant::now() + JOB_COMMAND_DEADLINE;
    let wait_client = client.clone();
    let job_id = args.job_id;
    tokio::select! {
        result = wait(&wait_client, args, output, deadline) => result,
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(|error| CliError::operation(format!("failed to listen for Ctrl-C: {error}")))?;
            cancel_after_interrupt(client, job_id).await;
            Err(CliError::Interrupted)
        }
    }
}

async fn wait(
    client: &HistoryClient,
    args: JobReadArgs,
    output: &OutputOptions,
    deadline: Instant,
) -> Result<u8, CliError> {
    loop {
        let response = client
            .poll(args.job_id, deadline)
            .await
            .map_err(CliError::from_client)?;
        print_progress(&response.envelope);
        if terminal(response.envelope.state) {
            return emit_polled(response.envelope, args.job_envelope, output);
        }
        let delay = response.retry_after.unwrap_or(DEFAULT_POLL_DELAY);
        if !sleep_before(deadline, delay).await {
            return Err(CliError::operation("job wait deadline expired"));
        }
    }
}

pub async fn cancel_after_interrupt(client: &HistoryClient, job_id: Uuid) {
    eprintln!("interrupt received; requesting cancellation for job {job_id}");
    match client
        .cancel(job_id, Instant::now() + CANCEL_DEADLINE)
        .await
    {
        Ok(envelope) => eprintln!("cancellation acknowledged in state {:?}", envelope.state),
        Err(error) => eprintln!("cancellation acknowledgement failed: {error}"),
    }
}

pub fn terminal(state: JobState) -> bool {
    matches!(
        state,
        JobState::Completed | JobState::Failed | JobState::Cancelled
    )
}

pub fn print_progress(envelope: &JobEnvelope) {
    let percent = match envelope.progress {
        JobProgress::HistoricalQuote(progress) => progress.percent_complete,
        JobProgress::HistoricalStateConsistencyCheck(progress) => progress.percent_complete,
    };
    eprintln!(
        "job {}: {:?}, {}% complete",
        envelope.job_id, envelope.state, percent
    );
}

pub fn emit_polled(
    envelope: JobEnvelope,
    job_envelope: bool,
    output: &OutputOptions,
) -> Result<u8, CliError> {
    if !terminal(envelope.state) || job_envelope || envelope.result.is_none() {
        output.write(&envelope).map_err(CliError::from_output)?;
    } else if let Some(result) = &envelope.result {
        output.write(result).map_err(CliError::from_output)?;
    }
    if envelope.state == JobState::Completed && envelope.result.is_some() {
        Ok(EXIT_SUCCESS)
    } else if terminal(envelope.state) {
        if let Some(failure) = &envelope.failure {
            eprintln!("job failed: {:?}: {}", failure.code, failure.message);
        }
        Ok(EXIT_OPERATION_FAILED)
    } else {
        Ok(EXIT_SUCCESS)
    }
}

pub fn quote_result(
    envelope: &JobEnvelope,
) -> Option<&historical_quote::api::HistoricalQuoteResult> {
    match envelope.result.as_ref() {
        Some(JobResult::HistoricalQuote(result)) => Some(result),
        _ => None,
    }
}

pub fn consistency_result(
    envelope: &JobEnvelope,
) -> Option<&historical_quote::api::ConsistencyCheckReport> {
    match envelope.result.as_ref() {
        Some(JobResult::HistoricalStateConsistencyCheck(result)) => Some(result),
        _ => None,
    }
}
