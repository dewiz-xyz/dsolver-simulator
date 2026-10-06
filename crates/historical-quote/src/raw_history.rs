use std::sync::Arc;

use state_history::{PositionRange, PositionRangeQuery, RangeGap, RawSnapshotQuery, ReadLimits};
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

    /// The stored messages `request` names. A range not stored through its end
    /// is refused as not stored yet, unless a recorded gap holds the message
    /// at its end, which the history lost.
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
    let through = history_position(request.through);
    if range.last_stored.is_none_or(|last| last < through) {
        if range.gaps.iter().any(|gap| covers(gap, through)) {
            return Err(HistoricalError::HistoryUnavailable(
                "the history lost the message at the end of the range".to_owned(),
            ));
        }
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

/// Whether `gap` holds the message at `position`. A gap without position
/// bounds proves nothing about one position.
fn covers(gap: &RangeGap, position: state_history::StreamPosition) -> bool {
    gap.from_position
        .zip(gap.to_position)
        .is_some_and(|(from, to)| from <= position && position <= to)
}

fn history_backends(backends: &[Backend]) -> Vec<state_history::Backend> {
    backends.iter().copied().map(history_backend).collect()
}
