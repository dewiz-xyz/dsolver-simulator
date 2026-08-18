use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use historical_quote::api::{
    ConsistencyCheckRequest, HistoricalQuoteResult, JobResult, QuoteJobRequest,
};
use historical_quote_service::jobs::{
    ExecutionContext, JobExecutionError, JobExecutor, JobRequest, ProgressReporter,
};
use tokio::sync::{mpsc, oneshot, Mutex};
use uuid::Uuid;

pub fn quote_request(request_id: Uuid, timeout_ms: u64) -> QuoteJobRequest {
    serde_json::from_value(serde_json::json!({
        "requestId": request_id,
        "apiRevision": 1,
        "timeoutMs": timeout_ms,
        "chainId": 8453,
        "pool": {
            "backend": "native",
            "protocol": "uniswap_v3",
            "componentId": "pool-1",
            "tokenIn": "0x01",
            "tokenOut": "0x02"
        },
        "blocks": {"start": 100, "endInclusive": 100, "step": 1},
        "lags": [1],
        "amountsIn": ["1"]
    }))
    .expect("quote request fixture must be valid")
}

pub fn consistency_request(request_id: Uuid, timeout_ms: u64) -> ConsistencyCheckRequest {
    serde_json::from_value(serde_json::json!({
        "requestId": request_id,
        "apiRevision": 1,
        "timeoutMs": timeout_ms,
        "chainId": 8453,
        "backends": ["native"],
        "selection": {
            "type": "checkpointPairs",
            "pairs": [{
                "earlierPosition": {"generation": 1, "messageSeq": 1},
                "targetPosition": {"generation": 1, "messageSeq": 2}
            }]
        },
        "quotesToCompare": [{
            "pool": {
                "backend": "native",
                "protocol": "uniswap_v3",
                "componentId": "pool-1",
                "tokenIn": "0x01",
                "tokenOut": "0x02"
            },
            "amountsIn": ["1"]
        }]
    }))
    .expect("consistency request fixture must be valid")
}

type ExecutionResult = Result<JobResult, JobExecutionError>;

pub struct StartedJob {
    pub job_id: Uuid,
    pub progress: ProgressReporter,
    request: JobRequest,
    finish: oneshot::Sender<ExecutionResult>,
}

impl StartedJob {
    pub fn complete_quote(self) {
        let JobRequest::HistoricalQuote(request) = self.request else {
            panic!("started job must be a quote");
        };
        let result: HistoricalQuoteResult = serde_json::from_value(serde_json::json!({
            "requestId": request.request_id,
            "chainId": 8453,
            "pool": request.pool,
            "visibleThroughBlock": 101,
            "gaps": [],
            "results": []
        }))
        .expect("quote result fixture must be valid");
        self.finish
            .send(Ok(JobResult::HistoricalQuote(result)))
            .expect("job runner must still await the result");
    }
}

#[derive(Clone)]
pub struct ControlledExecutor {
    started_tx: mpsc::UnboundedSender<StartedJob>,
    started_rx: Arc<Mutex<mpsc::UnboundedReceiver<StartedJob>>>,
}

impl Default for ControlledExecutor {
    fn default() -> Self {
        let (started_tx, started_rx) = mpsc::unbounded_channel();
        Self {
            started_tx,
            started_rx: Arc::new(Mutex::new(started_rx)),
        }
    }
}

impl ControlledExecutor {
    pub async fn next_started(&self) -> StartedJob {
        self.started_rx
            .lock()
            .await
            .recv()
            .await
            .expect("executor must report a started job")
    }

    pub fn try_next_started(&self) -> Option<StartedJob> {
        self.started_rx.try_lock().ok()?.try_recv().ok()
    }
}

impl JobExecutor for ControlledExecutor {
    fn execute(
        &self,
        context: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = ExecutionResult> + Send + 'static>> {
        let (finish, result) = oneshot::channel();
        self.started_tx
            .send(StartedJob {
                job_id: context.job_id,
                progress: context.progress,
                request: context.request,
                finish,
            })
            .expect("test receiver must remain open");
        Box::pin(async move { result.await.unwrap_or(Err(JobExecutionError::Cancelled)) })
    }
}
