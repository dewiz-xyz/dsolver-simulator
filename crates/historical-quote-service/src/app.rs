use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::middleware;
use axum::routing::{get, post};
use axum::Router;
use historical_quote::api::{
    Backend, CoverageRequest, CoverageResponse, DependencyHealth, DependencyState, JobFailure,
    JobFailureCode, JobResult,
};
use historical_quote::{
    ConsistencyChecker, CoverageService, HistoricalError, HistoricalQuoteExecutor,
    HistoricalReconstructor, HistorySource,
};
use state_history::{ReadConnectionProvider, ReadLimits, StateHistoryReader};

use crate::auth::{require_bearer, BearerKeySetHandle};
use crate::config::ServiceConfig;
use crate::http::handlers;
use crate::jobs::{ExecutionContext, JobExecutionError, JobExecutor, JobRegistry, JobRequest};

type ServiceFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ServiceDependencyError>> + Send + 'a>>;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<ServiceConfig>,
    pub bearer_keys: BearerKeySetHandle,
    pub registry: JobRegistry,
    pub coverage: Arc<dyn CoverageResolver>,
    pub status: Arc<dyn StatusProbe>,
}

pub trait CoverageResolver: Send + Sync + 'static {
    fn resolve(&self, request: CoverageRequest) -> ServiceFuture<'_, CoverageResponse>;
}

pub trait StatusProbe: Send + Sync + 'static {
    fn probe(&self) -> ServiceFuture<'_, StatusDependencies>;
}

#[derive(Debug, Clone)]
pub struct StatusDependencies {
    pub visible_through_block: Option<u64>,
    pub database: DependencyHealth,
    pub object_store: DependencyHealth,
}

#[derive(Debug, thiserror::Error)]
#[error("service dependency is unavailable")]
pub struct ServiceDependencyError;

pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/jobs/quote", post(handlers::submit_quote))
        .route(
            "/jobs/consistency-check",
            post(handlers::submit_consistency_check),
        )
        .route("/jobs/:job_id", get(handlers::get_job))
        .route("/jobs/:job_id/cancel", post(handlers::cancel_job))
        .route("/coverage", post(handlers::coverage))
        .route("/status", get(handlers::status))
        .route_layer(middleware::from_fn_with_state(
            state.bearer_keys.clone(),
            require_bearer,
        ));
    Router::new()
        .route("/ready", get(handlers::ready))
        .merge(protected)
        .with_state(state)
}

pub struct EngineJobExecutor<S> {
    source: Arc<S>,
    service_revision: String,
    read_limits: ReadLimits,
    max_structured_differences: usize,
    max_consistency_pairs: usize,
}

impl<S> EngineJobExecutor<S> {
    pub fn new(source: Arc<S>, config: &ServiceConfig) -> Result<Self, ServiceDependencyError> {
        let read_limits = ReadLimits::new(
            config.job_decoded_byte_reservation,
            config.job_decoded_byte_reservation,
        )
        .map_err(|_| ServiceDependencyError)?;
        Ok(Self {
            source,
            service_revision: config.service_revision.clone(),
            read_limits,
            max_structured_differences: config.max_structured_differences,
            max_consistency_pairs: config.max_consistency_pairs,
        })
    }
}

impl<S> JobExecutor for EngineJobExecutor<S>
where
    S: HistorySource,
{
    fn execute(
        &self,
        context: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = Result<JobResult, JobExecutionError>> + Send + 'static>> {
        let source = Arc::clone(&self.source);
        let service_revision = self.service_revision.clone();
        let read_limits = self.read_limits;
        let max_structured_differences = self.max_structured_differences;
        let max_consistency_pairs = self.max_consistency_pairs;
        Box::pin(async move {
            match context.request {
                JobRequest::HistoricalQuote(request) => {
                    let reconstructor = HistoricalReconstructor::new(source, read_limits);
                    let executor = HistoricalQuoteExecutor::new(reconstructor, service_revision);
                    executor
                        .execute(&request, &context.cancellation, &context.progress)
                        .await
                        .map(JobResult::HistoricalQuote)
                        .map_err(map_engine_error)
                }
                JobRequest::HistoricalStateConsistencyCheck(request) => {
                    let checker = ConsistencyChecker::with_limits(
                        source,
                        read_limits,
                        max_structured_differences,
                        max_consistency_pairs,
                    );
                    checker
                        .execute(&request, &context.cancellation, &context.progress)
                        .await
                        .map(JobResult::HistoricalStateConsistencyCheck)
                        .map_err(map_engine_error)
                }
            }
        })
    }
}

pub struct EngineCoverageResolver<S> {
    service: CoverageService<S>,
}

impl<S> EngineCoverageResolver<S> {
    #[must_use]
    pub fn new(source: Arc<S>) -> Self {
        Self {
            service: CoverageService::new(source),
        }
    }
}

impl<S> CoverageResolver for EngineCoverageResolver<S>
where
    S: HistorySource,
{
    fn resolve(&self, request: CoverageRequest) -> ServiceFuture<'_, CoverageResponse> {
        Box::pin(async move {
            self.service
                .resolve(&request)
                .await
                .map_err(|_| ServiceDependencyError)
        })
    }
}

pub struct ReaderStatusProbe<P>
where
    P: ReadConnectionProvider,
{
    reader: Arc<StateHistoryReader<P>>,
    chain_id: u64,
    backends: Vec<state_history::Backend>,
}

impl<P> ReaderStatusProbe<P>
where
    P: ReadConnectionProvider,
{
    #[must_use]
    pub fn new(reader: Arc<StateHistoryReader<P>>, chain_id: u64, backends: &[Backend]) -> Self {
        Self {
            reader,
            chain_id,
            backends: backends.iter().copied().map(history_backend).collect(),
        }
    }
}

impl<P> StatusProbe for ReaderStatusProbe<P>
where
    P: ReadConnectionProvider,
{
    fn probe(&self) -> ServiceFuture<'_, StatusDependencies> {
        Box::pin(async move {
            let (visible_through_block, database) = match self
                .reader
                .visible_through_block(self.chain_id, &self.backends)
                .await
            {
                Ok(cutoff) => (
                    Some(cutoff),
                    DependencyHealth {
                        state: DependencyState::Healthy,
                        message: None,
                    },
                ),
                Err(_) => (
                    None,
                    DependencyHealth {
                        state: DependencyState::Unavailable,
                        message: Some("read replica cutoff is unavailable".to_owned()),
                    },
                ),
            };
            Ok(StatusDependencies {
                visible_through_block,
                database,
                object_store: DependencyHealth {
                    state: DependencyState::Degraded,
                    message: Some("object reads are checked during jobs".to_owned()),
                },
            })
        })
    }
}

fn history_backend(backend: Backend) -> state_history::Backend {
    match backend {
        Backend::Native => state_history::Backend::Native,
        Backend::Vm => state_history::Backend::Vm,
        Backend::Rfq => state_history::Backend::Rfq,
    }
}

fn map_engine_error(error: HistoricalError) -> JobExecutionError {
    let retryable = matches!(&error, HistoricalError::HistoryRead { .. });
    let (code, message) = match error {
        HistoricalError::Cancelled => return JobExecutionError::Cancelled,
        HistoricalError::HistoryRead { .. } => (
            JobFailureCode::HistoryReadFailed,
            "historical state could not be read",
        ),
        HistoricalError::HistoricalDataInvalid(_) => (
            JobFailureCode::HistoricalDataInvalid,
            "retained historical data is invalid",
        ),
        HistoricalError::StateReconstructionFailed(_) | HistoricalError::UnsupportedCanonicalVm => {
            (
                JobFailureCode::StateReconstructionFailed,
                "historical state could not be reconstructed",
            )
        }
        HistoricalError::CheckBudgetExceeded => (
            JobFailureCode::CheckBudgetExceeded,
            "checkpoint pair limit was exceeded",
        ),
        HistoricalError::InvalidSelector(_) => (
            JobFailureCode::InternalError,
            "historical job could not be completed",
        ),
    };
    JobExecutionError::Failed(JobFailure {
        code,
        message: message.to_owned(),
        retryable,
        details: None,
    })
}

#[cfg(test)]
mod tests {
    use super::{map_engine_error, HistoricalError, JobExecutionError, JobFailureCode};

    #[test]
    #[expect(
        clippy::panic,
        reason = "unexpected engine outcomes must fail the mapping test"
    )]
    fn engine_errors_preserve_public_codes_messages_and_retryability() {
        assert!(matches!(
            map_engine_error(HistoricalError::Cancelled),
            JobExecutionError::Cancelled
        ));
        let private_detail = "private replica address and credentials";
        for (error, code, message, retryable) in [
            (
                HistoricalError::HistoryRead {
                    operation: private_detail,
                    source: anyhow::anyhow!(private_detail),
                },
                JobFailureCode::HistoryReadFailed,
                "historical state could not be read",
                true,
            ),
            (
                HistoricalError::HistoricalDataInvalid(private_detail.to_owned()),
                JobFailureCode::HistoricalDataInvalid,
                "retained historical data is invalid",
                false,
            ),
            (
                HistoricalError::StateReconstructionFailed(private_detail.to_owned()),
                JobFailureCode::StateReconstructionFailed,
                "historical state could not be reconstructed",
                false,
            ),
            (
                HistoricalError::UnsupportedCanonicalVm,
                JobFailureCode::StateReconstructionFailed,
                "historical state could not be reconstructed",
                false,
            ),
            (
                HistoricalError::CheckBudgetExceeded,
                JobFailureCode::CheckBudgetExceeded,
                "checkpoint pair limit was exceeded",
                false,
            ),
            (
                HistoricalError::InvalidSelector(private_detail.to_owned()),
                JobFailureCode::InternalError,
                "historical job could not be completed",
                false,
            ),
        ] {
            let JobExecutionError::Failed(failure) = map_engine_error(error) else {
                panic!("engine failure must remain a failed job");
            };
            assert_eq!(failure.code, code);
            assert_eq!(failure.message, message);
            assert_eq!(failure.retryable, retryable);
            assert!(failure.details.is_none());
        }
    }
}
