use std::future::Future;

use state_history::{
    CheckpointArchive, CheckpointManifest, CheckpointPairQuery, CheckpointPairReplayPlan,
    CoverageQuery, CoverageSnapshot, RawTokenSnapshot, ReadConnectionProvider, ReadLimitError,
    ReadLimits, StateHistoryReader, TargetPlan, TargetPlanQuery,
};

use crate::HistoricalError;

pub trait EngineProgress: Send + Sync {
    fn quote_comparison_completed(&self) {}

    fn checkpoint_pair_total(&self, _total: u64) {}

    fn checkpoint_pair_completed(&self) {}
}

impl EngineProgress for () {}

pub trait HistorySource: Send + Sync + 'static {
    fn plan_targets(
        &self,
        query: TargetPlanQuery,
    ) -> impl Future<Output = Result<TargetPlan, HistoricalError>> + Send;

    fn coverage(
        &self,
        query: CoverageQuery,
    ) -> impl Future<Output = Result<CoverageSnapshot, HistoricalError>> + Send;

    fn fetch_checkpoint(
        &self,
        manifest: CheckpointManifest,
        limits: ReadLimits,
    ) -> impl Future<Output = Result<CheckpointArchive, HistoricalError>> + Send;

    fn fetch_checkpoint_token_snapshot(
        &self,
        manifest: CheckpointManifest,
        limits: ReadLimits,
    ) -> impl Future<Output = Result<Option<RawTokenSnapshot>, HistoricalError>> + Send;

    fn resolve_checkpoint_pairs(
        &self,
        query: CheckpointPairQuery,
    ) -> impl Future<Output = Result<Vec<state_history::CheckpointPair>, HistoricalError>> + Send;

    fn plan_checkpoint_pair(
        &self,
        pair: state_history::CheckpointPair,
        backends: Vec<state_history::Backend>,
        limits: ReadLimits,
    ) -> impl Future<Output = Result<CheckpointPairReplayPlan, HistoricalError>> + Send;
}

impl<P> HistorySource for StateHistoryReader<P>
where
    P: ReadConnectionProvider,
{
    async fn plan_targets(&self, query: TargetPlanQuery) -> Result<TargetPlan, HistoricalError> {
        StateHistoryReader::plan_targets(self, &query)
            .await
            .map_err(|error| HistoricalError::history_read("target planning", error))
    }

    async fn coverage(&self, query: CoverageQuery) -> Result<CoverageSnapshot, HistoricalError> {
        StateHistoryReader::coverage(self, &query)
            .await
            .map_err(|error| HistoricalError::history_read("coverage resolution", error))
    }

    async fn fetch_checkpoint(
        &self,
        manifest: CheckpointManifest,
        limits: ReadLimits,
    ) -> Result<CheckpointArchive, HistoricalError> {
        StateHistoryReader::fetch_checkpoint_with_limit(self, &manifest, limits)
            .await
            .map_err(|error| storage_fetch_error("checkpoint fetch", error))
    }

    async fn fetch_checkpoint_token_snapshot(
        &self,
        manifest: CheckpointManifest,
        limits: ReadLimits,
    ) -> Result<Option<RawTokenSnapshot>, HistoricalError> {
        StateHistoryReader::fetch_checkpoint_token_snapshot_with_limit(self, &manifest, limits)
            .await
            .map_err(|error| storage_fetch_error("token snapshot fetch", error))
    }

    async fn resolve_checkpoint_pairs(
        &self,
        query: CheckpointPairQuery,
    ) -> Result<Vec<state_history::CheckpointPair>, HistoricalError> {
        StateHistoryReader::resolve_checkpoint_pairs(self, &query)
            .await
            .map_err(|error| HistoricalError::history_read("checkpoint pair resolution", error))
    }

    async fn plan_checkpoint_pair(
        &self,
        pair: state_history::CheckpointPair,
        backends: Vec<state_history::Backend>,
        limits: ReadLimits,
    ) -> Result<CheckpointPairReplayPlan, HistoricalError> {
        StateHistoryReader::plan_checkpoint_pair(self, &pair, backends, limits)
            .await
            .map_err(checkpoint_pair_plan_error)
    }
}

fn checkpoint_pair_plan_error(error: anyhow::Error) -> HistoricalError {
    let invalid = error.chain().any(|cause| {
        cause
            .downcast_ref::<state_history::StoredPayloadError>()
            .is_some()
            || matches!(
                cause.downcast_ref::<ReadLimitError>(),
                Some(ReadLimitError::Read(_))
            )
    });
    if invalid {
        HistoricalError::HistoricalDataInvalid(
            "checkpoint pair storage plan failed integrity validation".to_owned(),
        )
    } else {
        HistoricalError::history_read("checkpoint pair planning", error)
    }
}

fn storage_fetch_error(operation: &'static str, error: ReadLimitError) -> HistoricalError {
    match error {
        ReadLimitError::ObjectRead(source) => HistoricalError::history_read(operation, source),
        ReadLimitError::Read(_) => HistoricalError::HistoricalDataInvalid(format!(
            "{operation} failed storage integrity validation"
        )),
        limit @ (ReadLimitError::ZeroLimit
        | ReadLimitError::CompressedBytesExceeded { .. }
        | ReadLimitError::DecodedBytesExceeded { .. }) => {
            HistoricalError::history_read(operation, limit)
        }
    }
}
