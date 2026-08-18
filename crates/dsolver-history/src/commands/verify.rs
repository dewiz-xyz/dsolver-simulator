use std::sync::Arc;
use std::time::Duration;

use historical_quote::api::{
    ConsistencyCheckReport, ConsistencyCheckRequest, JobState, PairStatus, API_REVISION,
};
use serde_json::json;
use tokio::sync::Mutex;
use tokio::time::Instant;
use uuid::Uuid;

use crate::args::{DirectVerifyArgs, VerifyArgs};
use crate::client::{ClientError, HistoryClient};
use crate::exit::{EXIT_OPERATION_FAILED, EXIT_SUCCESS};
use crate::output::OutputOptions;
use crate::retry::sleep_before;

use super::job::{cancel_after_interrupt, consistency_result, print_progress, terminal};
use super::{read_request, required, CliError};

const DEFAULT_POLL_DELAY: Duration = Duration::from_secs(2);

pub async fn execute(
    client: &HistoryClient,
    args: VerifyArgs,
    output: &OutputOptions,
) -> Result<u8, CliError> {
    let request = verify_request(args.request.as_deref(), args.direct)?;
    let deadline = request_deadline(request.timeout_ms)?;
    let active_job = Arc::new(Mutex::new(None));
    let flow_client = client.clone();
    let flow_active_job = Arc::clone(&active_job);
    tokio::select! {
        result = verify_flow(
            &flow_client,
            request,
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

async fn verify_flow(
    client: &HistoryClient,
    mut request: ConsistencyCheckRequest,
    job_envelope: bool,
    output: &OutputOptions,
    deadline: Instant,
    active_job: Arc<Mutex<Option<Uuid>>>,
) -> Result<u8, CliError> {
    let mut recomputed = false;
    loop {
        let submission = client
            .submit_consistency(&request, deadline)
            .await
            .map_err(CliError::from_client)?;
        *active_job.lock().await = Some(submission.job_id);
        eprintln!("job {}", submission.job_id);
        match wait_for_verify(client, submission.job_id, deadline).await {
            Ok(envelope) => {
                if envelope.state != JobState::Completed {
                    output.write(&envelope).map_err(CliError::from_output)?;
                    if let Some(failure) = envelope.failure {
                        eprintln!(
                            "verification job failed: {:?}: {}",
                            failure.code, failure.message
                        );
                    }
                    return Ok(EXIT_OPERATION_FAILED);
                }
                let report = consistency_result(&envelope).ok_or_else(|| {
                    CliError::operation(
                        "completed verification job did not contain a consistency report",
                    )
                })?;
                print_summary(report);
                if job_envelope {
                    output.write(&envelope).map_err(CliError::from_output)?;
                } else {
                    output.write(report).map_err(CliError::from_output)?;
                }
                return Ok(verify_exit(report));
            }
            Err(error) if error.is_job_not_found() && !recomputed => {
                request.timeout_ms = remaining_millis(deadline)?;
                recomputed = true;
                eprintln!(
                    "verification result was not found; recomputing once with request {}",
                    request.request_id
                );
            }
            Err(error) => return Err(CliError::from_client(error)),
        }
    }
}

async fn wait_for_verify(
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

fn verify_request(
    request_path: Option<&std::path::Path>,
    direct: DirectVerifyArgs,
) -> Result<ConsistencyCheckRequest, CliError> {
    match request_path {
        Some(_) if direct.has_any() => Err(CliError::usage(
            "--request cannot be mixed with direct verification request flags",
        )),
        Some(path) => read_request(path),
        None => direct_verify_request(direct),
    }
}

fn direct_verify_request(args: DirectVerifyArgs) -> Result<ConsistencyCheckRequest, CliError> {
    let selection = direct_selection(&args)?;
    if args.backend.is_empty() {
        return Err(CliError::usage(
            "--backend is required with direct request flags",
        ));
    }
    if args.amount_in.is_empty() {
        return Err(CliError::usage(
            "--amount-in is required with direct request flags",
        ));
    }
    let quote_backend =
        historical_quote::api::Backend::from(required(args.quote_backend, "--quote-backend")?);
    serde_json::from_value(json!({
        "requestId": args.request_id.unwrap_or_else(Uuid::new_v4),
        "apiRevision": API_REVISION,
        "timeoutMs": required(args.timeout_ms, "--timeout-ms")?,
        "chainId": required(args.chain_id, "--chain-id")?,
        "backends": args.backend.into_iter().map(historical_quote::api::Backend::from).collect::<Vec<_>>(),
        "selection": selection,
        "quotesToCompare": [{
            "pool": {
                "backend": quote_backend,
                "protocol": required(args.protocol, "--protocol")?,
                "componentId": required(args.component_id, "--component-id")?,
                "tokenIn": required(args.token_in, "--token-in")?,
                "tokenOut": required(args.token_out, "--token-out")?,
            },
            "amountsIn": args.amount_in,
        }],
    }))
    .map_err(|error| CliError::usage(error.to_string()))
}

fn direct_selection(args: &DirectVerifyArgs) -> Result<serde_json::Value, CliError> {
    match (
        args.checkpoint_pair.is_empty(),
        args.start_block,
        args.end_block_inclusive,
    ) {
        (false, None, None) => Ok(json!({
            "type": "checkpointPairs",
            "pairs": args.checkpoint_pair.iter().map(|pair| &pair.0).collect::<Vec<_>>(),
        })),
        (true, Some(start), Some(end_inclusive)) => Ok(json!({
            "type": "blockRange",
            "start": start,
            "endInclusive": end_inclusive,
        })),
        (false, Some(_), Some(_)) => Err(CliError::usage(
            "--checkpoint-pair cannot be mixed with block-range selection",
        )),
        _ => Err(CliError::usage(
            "direct verification needs --checkpoint-pair or both block-range endpoints",
        )),
    }
}

fn print_summary(report: &ConsistencyCheckReport) {
    eprintln!(
        "verification: {} selected, {} compared, {} passed, {} mismatched, {} gaps, {} not comparable",
        report.summary.selected_checkpoint_pairs,
        report.summary.compared_checkpoint_pairs,
        report.summary.pass,
        report.summary.mismatch,
        report.summary.gap,
        report.summary.not_comparable,
    );
}

fn verify_exit(report: &ConsistencyCheckReport) -> u8 {
    if report.pairs.is_empty()
        || report
            .pairs
            .iter()
            .any(|pair| pair.status != PairStatus::Pass)
    {
        EXIT_OPERATION_FAILED
    } else {
        EXIT_SUCCESS
    }
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
