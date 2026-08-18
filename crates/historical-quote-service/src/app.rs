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

use crate::auth::{require_bearer, BearerTokenVerifier};
use crate::config::ServiceConfig;
use crate::http::handlers;
use crate::jobs::{ExecutionContext, JobExecutionError, JobExecutor, JobRegistry, JobRequest};

type ServiceFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ServiceDependencyError>> + Send + 'a>>;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<ServiceConfig>,
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
    let verifier = BearerTokenVerifier::new(state.config.expected_bearer_digest);
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
        .route_layer(middleware::from_fn_with_state(verifier, require_bearer));
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
            match self
                .reader
                .visible_through_block(self.chain_id, &self.backends)
                .await
            {
                Ok(cutoff) => Ok(StatusDependencies {
                    visible_through_block: Some(cutoff),
                    database: healthy_dependency(),
                    object_store: DependencyHealth {
                        state: DependencyState::Degraded,
                        message: Some("object reads are checked during jobs".to_owned()),
                    },
                }),
                Err(_) => Ok(StatusDependencies {
                    visible_through_block: None,
                    database: DependencyHealth {
                        state: DependencyState::Unavailable,
                        message: Some("read replica cutoff is unavailable".to_owned()),
                    },
                    object_store: DependencyHealth {
                        state: DependencyState::Degraded,
                        message: Some("object reads are checked during jobs".to_owned()),
                    },
                }),
            }
        })
    }
}

fn healthy_dependency() -> DependencyHealth {
    DependencyHealth {
        state: DependencyState::Healthy,
        message: None,
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
    let failure = match error {
        HistoricalError::Cancelled => return JobExecutionError::Cancelled,
        HistoricalError::HistoryRead { .. } => JobFailure {
            code: JobFailureCode::HistoryReadFailed,
            message: "historical state could not be read".to_owned(),
            retryable: true,
            details: None,
        },
        HistoricalError::HistoricalDataInvalid(_) => JobFailure {
            code: JobFailureCode::HistoricalDataInvalid,
            message: "retained historical data is invalid".to_owned(),
            retryable: false,
            details: None,
        },
        HistoricalError::StateReconstructionFailed(_) | HistoricalError::UnsupportedCanonicalVm => {
            JobFailure {
                code: JobFailureCode::StateReconstructionFailed,
                message: "historical state could not be reconstructed".to_owned(),
                retryable: false,
                details: None,
            }
        }
        HistoricalError::CheckBudgetExceeded => JobFailure {
            code: JobFailureCode::CheckBudgetExceeded,
            message: "checkpoint pair limit was exceeded".to_owned(),
            retryable: false,
            details: None,
        },
        HistoricalError::InvalidSelector(_) => JobFailure {
            code: JobFailureCode::InternalError,
            message: "historical job could not be completed".to_owned(),
            retryable: false,
            details: None,
        },
    };
    JobExecutionError::Failed(failure)
}
