use std::sync::Arc;
use std::time::Duration;

use historical_quote::api::{JobState, QuoteJobRequest, API_REVISION};
use serde_json::json;
use tokio::sync::Mutex;
use tokio::time::Instant;
use uuid::Uuid;

use crate::args::{DirectQuoteArgs, QuotesArgs};
use crate::client::{ClientError, HistoryClient};
use crate::exit::{EXIT_OPERATION_FAILED, EXIT_SUCCESS};
use crate::output::OutputOptions;
use crate::retry::sleep_before;

use super::job::{cancel_after_interrupt, print_progress, quote_result, terminal};
use super::{read_request, required, CliError};

const DEFAULT_POLL_DELAY: Duration = Duration::from_secs(2);

pub async fn execute(
    client: &HistoryClient,
    args: QuotesArgs,
    output: &OutputOptions,
) -> Result<u8, CliError> {
    let request = quote_request(args.request.as_deref(), args.direct)?;
    let deadline = request_deadline(request.timeout_ms)?;
    let active_job = Arc::new(Mutex::new(None));
    let flow_client = client.clone();
    let flow_active_job = Arc::clone(&active_job);
    tokio::select! {
        result = quote_flow(
            &flow_client,
            request,
            args.submit_only,
            args.job_envelope,
            output,
            deadline,
            flow_active_job,
        ) => result,
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(|error| CliError::operation(format!("failed to listen for Ctrl-C: {error}")))?;
            if let Some(job_id) = *active_job.lock().await {
                cancel_after_interrupt(client, job_id).await;
            }
            Err(CliError::Interrupted)
        }
    }
}

async fn quote_flow(
    client: &HistoryClient,
    mut request: QuoteJobRequest,
    submit_only: bool,
    job_envelope: bool,
    output: &OutputOptions,
    deadline: Instant,
    active_job: Arc<Mutex<Option<Uuid>>>,
) -> Result<u8, CliError> {
    let mut recomputed = false;
    loop {
        let submission = client
            .submit_quote(&request, deadline)
            .await
            .map_err(CliError::from_client)?;
        *active_job.lock().await = Some(submission.job_id);
        eprintln!("job {}", submission.job_id);
        if submit_only {
            output.write(&submission).map_err(CliError::from_output)?;
            return Ok(EXIT_SUCCESS);
        }

        match wait_for_quote(client, submission.job_id, deadline).await {
            Ok(envelope) => {
                if envelope.state == JobState::Completed {
                    let result = quote_result(&envelope).ok_or_else(|| {
                        CliError::operation("completed quote job did not contain a quote result")
                    })?;
                    if job_envelope {
                        output.write(&envelope).map_err(CliError::from_output)?;
                    } else {
                        output.write(result).map_err(CliError::from_output)?;
                    }
                    return Ok(EXIT_SUCCESS);
                }
                output.write(&envelope).map_err(CliError::from_output)?;
                if let Some(failure) = envelope.failure {
                    eprintln!("job failed: {:?}: {}", failure.code, failure.message);
                }
                return Ok(EXIT_OPERATION_FAILED);
            }
            Err(error) if error.is_job_not_found() && !recomputed => {
                let remaining = remaining_millis(deadline)?;
                request.timeout_ms = remaining;
                recomputed = true;
                eprintln!(
                    "job result was not found; recomputing once with request {}",
                    request.request_id
                );
            }
            Err(error) => return Err(CliError::from_client(error)),
        }
    }
}

async fn wait_for_quote(
    client: &HistoryClient,
    job_id: Uuid,
    deadline: Instant,
) -> Result<historical_quote::api::JobEnvelope, ClientError> {
    loop {
        let response = client.poll(job_id, deadline).await?;
        print_progress(&response.envelope);
        if terminal(response.envelope.state) {
            return Ok(response.envelope);
        }
        let delay = response.retry_after.unwrap_or(DEFAULT_POLL_DELAY);
        if !sleep_before(deadline, delay).await {
            return Err(ClientError::DeadlineExpired);
        }
    }
}

fn quote_request(
    request_path: Option<&std::path::Path>,
    direct: DirectQuoteArgs,
) -> Result<QuoteJobRequest, CliError> {
    match request_path {
        Some(_) if direct.has_any() => Err(CliError::usage(
            "--request cannot be mixed with direct quote request flags",
        )),
        Some(path) => read_request(path),
        None => direct_quote_request(direct),
    }
}

fn direct_quote_request(args: DirectQuoteArgs) -> Result<QuoteJobRequest, CliError> {
    let request_id = args.request_id.unwrap_or_else(Uuid::new_v4);
    let timeout_ms = required(args.timeout_ms, "--timeout-ms")?;
    let chain_id = required(args.chain_id, "--chain-id")?;
    let backend = historical_quote::api::Backend::from(required(args.backend, "--backend")?);
    let protocol = required(args.protocol, "--protocol")?;
    let component_id = required(args.component_id, "--component-id")?;
    let token_in = required(args.token_in, "--token-in")?;
    let token_out = required(args.token_out, "--token-out")?;
    let start = required(args.start_block, "--start-block")?;
    let end_inclusive = required(args.end_block_inclusive, "--end-block-inclusive")?;
    if args.lag.is_empty() {
        return Err(CliError::usage(
            "--lag is required with direct request flags",
        ));
    }
    if args.amount_in.is_empty() {
        return Err(CliError::usage(
            "--amount-in is required with direct request flags",
        ));
    }
    serde_json::from_value(json!({
        "requestId": request_id,
        "apiRevision": API_REVISION,
        "timeoutMs": timeout_ms,
        "chainId": chain_id,
        "pool": {
            "backend": backend,
            "protocol": protocol,
            "componentId": component_id,
            "tokenIn": token_in,
            "tokenOut": token_out,
        },
        "blocks": {
            "start": start,
            "endInclusive": end_inclusive,
            "step": args.step.unwrap_or(1),
        },
        "lags": args.lag,
        "amountsIn": args.amount_in,
    }))
    .map_err(|error| CliError::usage(error.to_string()))
}

fn remaining_millis(deadline: Instant) -> Result<u64, CliError> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| CliError::operation("request deadline expired before recomputation"))?;
    let millis = u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX);
    if millis == 0 {
        Err(CliError::operation(
            "request deadline expired before recomputation",
        ))
    } else {
        Ok(millis)
    }
}

fn request_deadline(timeout_ms: u64) -> Result<Instant, CliError> {
    Instant::now()
        .checked_add(Duration::from_millis(timeout_ms))
        .ok_or_else(|| CliError::usage("timeoutMs is too large"))
}
