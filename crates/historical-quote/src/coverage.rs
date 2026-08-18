use std::sync::Arc;

use state_history::{BlockInterval, CoverageQuery, RangeGap, RangeGapKind};

use crate::api::{BlockBounds, CoverageRequest, CoverageResponse, GapCause, GapKind, KnownGap};
use crate::replay::{history_backend, public_position};
use crate::{HistoricalError, HistorySource};

pub struct CoverageService<S> {
    source: Arc<S>,
}

impl<S> CoverageService<S> {
    pub fn new(source: Arc<S>) -> Self {
        Self { source }
    }
}

impl<S: HistorySource> CoverageService<S> {
    pub async fn resolve(
        &self,
        request: &CoverageRequest,
    ) -> Result<CoverageResponse, HistoricalError> {
        let mut backends = request.backends.clone();
        backends.sort_unstable();
        let bounds = request
            .bounds
            .map(|bounds| BlockInterval::new(bounds.start, bounds.end_inclusive))
            .transpose()
            .map_err(|error| HistoricalError::InvalidSelector(error.to_string()))?;
        let query = CoverageQuery::new(
            request.chain_id,
            backends.iter().copied().map(history_backend).collect(),
            bounds,
        )
        .map_err(|error| HistoricalError::InvalidSelector(error.to_string()))?;
        let snapshot = self.source.coverage(query).await?;
        Ok(CoverageResponse {
            api_revision: request.api_revision,
            chain_id: request.chain_id,
            backends,
            visible_through_block: snapshot.visible_through_block,
            continuous_intervals: snapshot
                .continuous_intervals
                .into_iter()
                .map(|interval| BlockBounds {
                    start: interval.start,
                    end_inclusive: interval.end_inclusive,
                })
                .collect(),
            known_gaps: snapshot
                .known_gaps
                .iter()
                .map(known_gap)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

fn known_gap(gap: &RangeGap) -> Result<KnownGap, HistoricalError> {
    Ok(KnownGap {
        kind: match gap.kind {
            RangeGapKind::MissingCheckpoint => GapKind::MissingCheckpoint,
            RangeGapKind::Recorded => GapKind::Recorded,
            RangeGapKind::IncompleteTail => GapKind::IncompleteTail,
        },
        cause: (gap.kind == RangeGapKind::Recorded)
            .then(|| gap_cause(&gap.reason))
            .transpose()?,
        from_position: gap.from_position.map(public_position),
        to_position: gap.to_position.map(public_position),
        from_block: gap.from_block,
        to_block_inclusive: gap.to_block_inclusive,
        from_observed_at_ms: gap.from_observed_at_ms,
        to_observed_at_ms: gap.to_observed_at_ms,
    })
}

fn gap_cause(reason: &str) -> Result<GapCause, HistoricalError> {
    match reason {
        "queue_overflow" => Ok(GapCause::QueueOverflow),
        "write_failed" => Ok(GapCause::WriteFailed),
        "writer_stopped" => Ok(GapCause::WriterStopped),
        "checkpoint_failed" => Ok(GapCause::CheckpointFailed),
        "boundary_slip" => Ok(GapCause::BoundarySlip),
        other => Err(HistoricalError::HistoricalDataInvalid(format!(
            "unknown recorded gap cause {other}"
        ))),
    }
}
