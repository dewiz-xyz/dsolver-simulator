mod support;

use std::sync::Arc;

use historical_quote::api::{Backend, RawSnapshotRequest, StoredMessagesRequest};
use historical_quote::{HistoricalError, RawHistoryReader};
use simulator_core::broadcaster::{BroadcasterBackend, BroadcasterSnapshotPartition};
use state_history::{
    Backend as HistoryBackend, PositionRange, PositionRangeQuery, RawSnapshot, RawSnapshotQuery,
    ReadLimits,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use support::{
    manifest, native_delta, position, public_position, recorded_gap, skipped_v4_message,
    FixtureSource,
};

/// Read limits small enough to show they reach the history as given.
const LIMITS: ReadLimits = ReadLimits {
    max_compressed_bytes: 1_000,
    max_decoded_bytes: 2_000,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn messages_request(after: u64, through: u64) -> StoredMessagesRequest {
    StoredMessagesRequest {
        request_id: Uuid::new_v4(),
        api_revision: 1,
        timeout_ms: 10_000,
        chain_id: 8453,
        backends: vec![Backend::Native],
        after: public_position(after),
        through: public_position(through),
    }
}

fn snapshot_request(at: u64) -> RawSnapshotRequest {
    RawSnapshotRequest {
        request_id: Uuid::new_v4(),
        api_revision: 1,
        timeout_ms: 10_000,
        chain_id: 8453,
        backends: vec![Backend::Native],
        position: public_position(at),
    }
}

fn reader(source: &Arc<FixtureSource>) -> RawHistoryReader<FixtureSource> {
    RawHistoryReader::new(Arc::clone(source), LIMITS)
}

fn range(last_stored: Option<u64>, gaps: Vec<(u64, u64)>) -> PositionRange {
    PositionRange {
        deltas: Vec::new(),
        gaps: gaps
            .into_iter()
            .map(|(from, to)| recorded_gap(from, to, 100, 101))
            .collect(),
        boundaries: Vec::new(),
        last_stored: last_stored.map(position),
        estimated_decoded_bytes: 0,
    }
}

/// A stored range gives every stored message in stream order, each envelope
/// exactly as stored, a gap that lost only RFQ updates included, and reads
/// the range it names within the reader's limits.
#[tokio::test]
async fn a_stored_range_gives_its_messages_as_stored() -> TestResult {
    let mut source = FixtureSource::native()?;
    let stored = vec![native_delta(2, 101)?, native_delta(3, 102)?];
    let mut stored_range = range(Some(5), Vec::new());
    stored_range.deltas = stored.clone();
    let mut rfq_only = recorded_gap(4, 4, 0, 0);
    rfq_only.from_block = None;
    rfq_only.to_block_inclusive = None;
    rfq_only.from_observed_at_ms = Some(1_000);
    rfq_only.to_observed_at_ms = Some(1_000);
    stored_range.gaps = vec![rfq_only];
    source.position_range = Some(stored_range);
    let source = Arc::new(source);
    let request = messages_request(1, 5);

    let result = reader(&source)
        .stored_messages(&request, &CancellationToken::new())
        .await?;

    assert_eq!(
        (result.after, result.through),
        (request.after, request.through)
    );
    assert_eq!(result.messages.len(), stored.len());
    for (message, delta) in result.messages.iter().zip(&stored) {
        assert_eq!(
            message.position,
            public_position(delta.position.message_seq)
        );
        assert_eq!(message.envelope.get(), delta.raw_payload.get());
    }
    let reads = source
        .position_range_reads
        .lock()
        .map_err(|_| "a poisoned read log")?;
    assert_eq!(
        *reads,
        vec![(
            PositionRangeQuery::new(8453, position(1), position(5), vec![HistoryBackend::Native]),
            LIMITS
        )]
    );
    Ok(())
}

/// A range with a recorded gap anywhere in it, or a boundary checkpoint, is
/// lost, whether or not it is stored through its end. A range with neither
/// that is not stored through its end lags.
#[tokio::test]
async fn a_gap_or_boundary_loses_the_range_and_an_unstored_end_lags() -> TestResult {
    let mut boundary = range(Some(5), Vec::new());
    boundary.boundaries = vec![manifest(
        7,
        100,
        position(3),
        HistoryBackend::Native,
        false,
        None,
    )];
    for (stored, lag) in [
        (range(Some(3), Vec::new()), true),
        (range(Some(5), vec![(2, 2)]), false),
        (range(None, vec![(2, 2)]), false),
        (range(Some(3), vec![(4, 5)]), false),
        (range(Some(3), vec![(5, 6)]), false),
        (boundary, false),
    ] {
        let mut source = FixtureSource::native()?;
        source.position_range = Some(stored);

        let Err(error) = reader(&Arc::new(source))
            .stored_messages(&messages_request(1, 5), &CancellationToken::new())
            .await
        else {
            return Err("each of these ranges must be refused".into());
        };

        assert_eq!(
            matches!(error, HistoricalError::NotYetStored),
            lag,
            "{error}"
        );
        assert_eq!(
            matches!(error, HistoricalError::HistoryUnavailable(_)),
            !lag,
            "{error}"
        );
    }
    Ok(())
}

/// A raw snapshot comes back at its position as the broadcaster's partition
/// wire form, raw messages included, with the checkpoint it was rebuilt from,
/// and reads the position it names within the reader's limits.
#[tokio::test]
async fn a_raw_snapshot_renders_its_partitions() -> TestResult {
    let partition = BroadcasterSnapshotPartition::with_messages(
        BroadcasterBackend::Native,
        101,
        vec![skipped_v4_message(101, false)],
        Default::default(),
    );
    let mut source = FixtureSource::native()?;
    source.raw_snapshot = Some(RawSnapshot {
        position: position(5),
        checkpoint: manifest(7, 100, position(2), HistoryBackend::Native, false, None),
        partitions: vec![partition.clone()],
    });
    let source = Arc::new(source);
    let request = snapshot_request(5);

    let result = reader(&source)
        .raw_snapshot(&request, &CancellationToken::new())
        .await?;

    assert_eq!(result.position, request.position);
    assert_eq!(result.checkpoint_position, public_position(2));
    assert_eq!(result.partitions.len(), 1);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(result.partitions[0].get())?,
        serde_json::to_value(&partition)?
    );
    let reads = source
        .raw_snapshot_reads
        .lock()
        .map_err(|_| "a poisoned read log")?;
    assert_eq!(
        *reads,
        vec![(
            RawSnapshotQuery::new(8453, position(5), vec![HistoryBackend::Native]),
            LIMITS
        )]
    );
    Ok(())
}

/// A cancelled job reads nothing, on either read.
#[tokio::test]
async fn a_cancelled_job_reads_nothing() -> TestResult {
    let source = Arc::new(FixtureSource::native()?);
    let cancellation = CancellationToken::new();
    cancellation.cancel();

    let messages = reader(&source)
        .stored_messages(&messages_request(1, 5), &cancellation)
        .await;
    let snapshot = reader(&source)
        .raw_snapshot(&snapshot_request(5), &cancellation)
        .await;

    assert!(matches!(messages, Err(HistoricalError::Cancelled)));
    assert!(matches!(snapshot, Err(HistoricalError::Cancelled)));
    let range_reads = source
        .position_range_reads
        .lock()
        .map_err(|_| "a poisoned read log")?
        .len();
    let snapshot_reads = source
        .raw_snapshot_reads
        .lock()
        .map_err(|_| "a poisoned read log")?
        .len();
    assert_eq!((range_reads, snapshot_reads), (0, 0));
    Ok(())
}
