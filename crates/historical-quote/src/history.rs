use std::future::Future;

use state_history::{
    CheckpointArchive, CheckpointManifest, CheckpointPairQuery, CheckpointPairReplayPlan,
    CoverageQuery, CoverageSnapshot, PositionRange, PositionRangeQuery, RawSnapshot,
    RawSnapshotError, RawSnapshotQuery, RawTokenSnapshot, ReadConnectionProvider, ReadLimitError,
    ReadLimits, StateHistoryReader, TargetPlan, TargetPlanQuery, TokenAnchor,
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

    fn select_token_anchor(
        &self,
        manifest: CheckpointManifest,
    ) -> impl Future<Output = Result<TokenAnchor, HistoricalError>> + Send;

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

    fn raw_snapshot(
        &self,
        query: RawSnapshotQuery,
        limits: ReadLimits,
    ) -> impl Future<Output = Result<RawSnapshot, HistoricalError>> + Send;

    fn position_range(
        &self,
        query: PositionRangeQuery,
        limits: ReadLimits,
    ) -> impl Future<Output = Result<PositionRange, HistoricalError>> + Send;
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

    async fn select_token_anchor(
        &self,
        manifest: CheckpointManifest,
    ) -> Result<TokenAnchor, HistoricalError> {
        StateHistoryReader::select_token_anchor(self, &manifest)
            .await
            .map_err(|error| HistoricalError::history_read("token anchor selection", error))
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

    async fn raw_snapshot(
        &self,
        query: RawSnapshotQuery,
        limits: ReadLimits,
    ) -> Result<RawSnapshot, HistoricalError> {
        StateHistoryReader::read_raw_snapshot(self, &query, limits)
            .await
            .map_err(|error| match error {
                RawSnapshotError::NotYetStored { .. } => HistoricalError::NotYetStored,
                RawSnapshotError::NotRaw(backend) => HistoricalError::InvalidSelector(format!(
                    "backend {} is not kept as raw messages",
                    backend.as_str()
                )),
                unavailable @ (RawSnapshotError::NoCheckpoint(_)
                | RawSnapshotError::Gaps { .. }
                | RawSnapshotError::Boundaries { .. }) => {
                    HistoricalError::HistoryUnavailable(unavailable.to_string())
                }
                RawSnapshotError::Read(error) => {
                    raw_read_error("raw snapshot read", "raw snapshot", error)
                }
            })
    }

    async fn position_range(
        &self,
        query: PositionRangeQuery,
        limits: ReadLimits,
    ) -> Result<PositionRange, HistoricalError> {
        StateHistoryReader::read_position_range(self, &query, limits)
            .await
            .map_err(|error| raw_read_error("stored message read", "stored messages", error))
    }
}

fn checkpoint_pair_plan_error(error: anyhow::Error) -> HistoricalError {
    classified_read_error(
        "checkpoint pair planning",
        "checkpoint pair storage plan",
        error,
    )
}

/// A failed raw history read during `operation`, a read beyond the job's
/// limits when one was refused for its size, classified as any read otherwise.
/// The quote and consistency jobs keep their failure codes, which their
/// clients parse strictly.
fn raw_read_error(operation: &'static str, subject: &str, error: anyhow::Error) -> HistoricalError {
    let over_limit = error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<ReadLimitError>(),
            Some(
                ReadLimitError::CompressedBytesExceeded { .. }
                    | ReadLimitError::DecodedBytesExceeded { .. }
            )
        )
    });
    if over_limit {
        HistoricalError::ReadLimitExceeded { operation }
    } else {
        classified_read_error(operation, subject, error)
    }
}

/// A failed read during `operation`, invalid data when a stored payload or
/// its size check failed, a read failure otherwise.
fn classified_read_error(
    operation: &'static str,
    subject: &str,
    error: anyhow::Error,
) -> HistoricalError {
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
        HistoricalError::HistoricalDataInvalid(format!("{subject} failed integrity validation"))
    } else {
        HistoricalError::history_read(operation, error)
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

#[cfg(test)]
mod tests {
    use state_history::ReadLimitError;

    use super::raw_read_error;
    use crate::HistoricalError;

    /// A raw history read refused for its size is told apart from a failed
    /// read and from invalid data, through any context.
    #[test]
    fn a_raw_read_over_its_limits_is_its_own_error() {
        let over_limits = [
            ReadLimitError::CompressedBytesExceeded {
                declared: 2,
                limit: 1,
            },
            ReadLimitError::DecodedBytesExceeded {
                declared: 2,
                limit: 1,
            },
        ];
        for limit in over_limits {
            let read = anyhow::Error::new(limit).context("the range read");
            assert!(matches!(
                raw_read_error("stored message read", "stored messages", read),
                HistoricalError::ReadLimitExceeded { .. }
            ));
        }
        assert!(matches!(
            raw_read_error(
                "stored message read",
                "stored messages",
                ReadLimitError::Read(anyhow::anyhow!("a corrupt payload")).into()
            ),
            HistoricalError::HistoricalDataInvalid(_)
        ));
        assert!(matches!(
            raw_read_error(
                "stored message read",
                "stored messages",
                anyhow::anyhow!("the replica went away")
            ),
            HistoricalError::HistoryRead { .. }
        ));
    }
}
