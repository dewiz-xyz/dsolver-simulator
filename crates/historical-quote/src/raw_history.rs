use std::sync::Arc;

use state_history::{PositionRange, PositionRangeQuery, RawSnapshotQuery, ReadLimits};
use tokio_util::sync::CancellationToken;

use crate::api::{
    Backend, RawJson, RawSnapshotRequest, RawSnapshotResult, StoredMessage, StoredMessagesRequest,
    StoredMessagesResult,
};
use crate::replay::{check_cancelled, history_backend, history_position, public_position};
use crate::{HistoricalError, HistorySource};

/// Reads the raw snapshot at a position and the stored messages in a range,
/// as the broadcaster kept them.
pub struct RawHistoryReader<S> {
    source: Arc<S>,
    limits: ReadLimits,
}

impl<S> RawHistoryReader<S> {
    pub fn new(source: Arc<S>, limits: ReadLimits) -> Self {
        Self { source, limits }
    }
}

impl<S: HistorySource> RawHistoryReader<S> {
    /// The raw snapshot `request` names.
    pub async fn raw_snapshot(
        &self,
        request: &RawSnapshotRequest,
        cancellation: &CancellationToken,
    ) -> Result<RawSnapshotResult, HistoricalError> {
        check_cancelled(cancellation)?;
        let query = RawSnapshotQuery::new(
            request.chain_id,
            history_position(request.position),
            history_backends(&request.backends),
        );
        let snapshot = self.source.raw_snapshot(query, self.limits).await?;
        check_cancelled(cancellation)?;
        let partitions = snapshot
            .partitions
            .iter()
            .map(|partition| {
                serde_json::value::to_raw_value(partition)
                    .map(RawJson::from)
                    .map_err(|_| {
                        HistoricalError::StateReconstructionFailed(
                            "a rebuilt partition could not be rendered".to_owned(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RawSnapshotResult {
            position: request.position,
            checkpoint_position: public_position(snapshot.checkpoint.position),
            partitions,
        })
    }

    /// The stored messages `request` names. A range with a recorded gap is
    /// refused, since its messages would come back without the lost ones. A
    /// range not stored through its end is refused as not stored yet.
    pub async fn stored_messages(
        &self,
        request: &StoredMessagesRequest,
        cancellation: &CancellationToken,
    ) -> Result<StoredMessagesResult, HistoricalError> {
        check_cancelled(cancellation)?;
        let query = PositionRangeQuery::new(
            request.chain_id,
            history_position(request.after),
            history_position(request.through),
            history_backends(&request.backends),
        );
        let range = self.source.position_range(query, self.limits).await?;
        check_cancelled(cancellation)?;
        stored_messages(range, request)
    }
}

fn stored_messages(
    range: PositionRange,
    request: &StoredMessagesRequest,
) -> Result<StoredMessagesResult, HistoricalError> {
    if !range.boundaries.is_empty() {
        return Err(HistoricalError::HistoryUnavailable(
            "a boundary checkpoint lies in the range".to_owned(),
        ));
    }
    // Raw requests never name RFQ, so a gap that lost only RFQ updates loses none here.
    if range.gaps.iter().any(|gap| !gap.rfq_only()) {
        return Err(HistoricalError::HistoryUnavailable(
            "the history lost messages in the range".to_owned(),
        ));
    }
    let through = history_position(request.through);
    if range.last_stored.is_none_or(|last| last < through) {
        return Err(HistoricalError::NotYetStored);
    }
    Ok(StoredMessagesResult {
        after: request.after,
        through: request.through,
        messages: range
            .deltas
            .into_iter()
            .map(|delta| StoredMessage {
                position: public_position(delta.position),
                envelope: RawJson::from(delta.raw_payload),
            })
            .collect(),
    })
}

fn history_backends(backends: &[Backend]) -> Vec<state_history::Backend> {
    backends.iter().copied().map(history_backend).collect()
}
