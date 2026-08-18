mod support;

use std::sync::Arc;

use historical_quote::api::{QuoteFailureReason, QuoteOutcome, UnavailableReason};
use historical_quote::{HistoricalQuoteExecutor, HistoricalReconstructor};
use simulator_replay::acquire_vm_world_lease;
use state_history::ReadLimits;
use tokio_util::sync::CancellationToken;

use support::{native_quote_request, rfq_quote_request, FixtureSource};

#[tokio::test]
async fn grouped_quotes_are_sorted_and_replay_each_leg_once(
) -> Result<(), Box<dyn std::error::Error>> {
    let source = Arc::new(FixtureSource::native()?);
    let executor = executor(Arc::clone(&source));
    let result = executor
        .execute(
            &native_quote_request(100, 100),
            &CancellationToken::new(),
            &(),
        )
        .await?;

    assert_eq!(source.checkpoint_fetch_count(), 1);
    assert_eq!(source.token_fetch_count(), 1);
    assert_eq!(result.results.len(), 2);
    assert_eq!(result.results[0].amount_in.as_str(), "1000000");
    assert_eq!(result.results[1].amount_in.as_str(), "2000000");
    assert_eq!(result.results[0].targets[0].lag, 1);
    assert_eq!(result.results[0].targets[0].target_block, 101);
    assert_eq!(result.results[0].targets[1].lag, 2);
    assert_eq!(result.results[0].targets[1].target_block, 102);
    assert!(matches!(
        result.results[0].start_outcome,
        QuoteOutcome::Ok { .. }
    ));
    assert!(result.results[0]
        .targets
        .iter()
        .all(|target| matches!(target.outcome, QuoteOutcome::Ok { .. })));
    Ok(())
}

#[tokio::test]
async fn only_target_crossing_gap_is_unavailable() -> Result<(), Box<dyn std::error::Error>> {
    let mut source = FixtureSource::native()?;
    source
        .target_plan
        .gaps
        .push(support::recorded_gap(3, 3, 102, 102));
    let result = executor(Arc::new(source))
        .execute(
            &native_quote_request(100, 100),
            &CancellationToken::new(),
            &(),
        )
        .await?;

    assert!(matches!(
        result.results[0].targets[0].outcome,
        QuoteOutcome::Ok { .. }
    ));
    assert!(matches!(
        result.results[0].targets[1].outcome,
        QuoteOutcome::Unavailable {
            reason: UnavailableReason::Gap,
            ..
        }
    ));
    Ok(())
}

#[tokio::test]
async fn position_only_gap_after_target_does_not_invalidate_target(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut source = FixtureSource::native()?;
    source.target_plan.gaps.push(state_history::RangeGap {
        kind: state_history::RangeGapKind::MissingCheckpoint,
        generation: Some(1),
        from_message_seq: Some(3),
        to_message_seq: None,
        from_position: Some(support::position(3)),
        to_position: None,
        from_block: None,
        to_block_inclusive: None,
        from_observed_at_ms: None,
        to_observed_at_ms: None,
        reason: "missing_checkpoint".to_owned(),
    });
    let mut request = native_quote_request(100, 100);
    request.lags = vec![1];
    request.amounts_in = vec![support::amount("1000000")];
    let result = executor(Arc::new(source))
        .execute(&request, &CancellationToken::new(), &())
        .await?;

    assert!(matches!(
        result.results[0].targets[0].outcome,
        QuoteOutcome::Ok { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn start_quote_failure_does_not_suppress_safe_targets(
) -> Result<(), Box<dyn std::error::Error>> {
    let source = Arc::new(FixtureSource::native_with_component_added_at_target()?);
    let mut request = native_quote_request(100, 100);
    request.lags = vec![1];
    request.amounts_in = vec![support::amount("1000000")];
    let result = executor(source)
        .execute(&request, &CancellationToken::new(), &())
        .await?;

    assert!(matches!(
        result.results[0].start_outcome,
        QuoteOutcome::QuoteFailed {
            reason: QuoteFailureReason::ComponentNotFound
        }
    ));
    assert!(matches!(
        result.results[0].targets[0].outcome,
        QuoteOutcome::Ok { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn rfq_uses_latest_observation_strictly_before_next_block(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut source = FixtureSource::rfq()?;
    source.target_plan.legs[0]
        .deltas
        .push(support::rfq_delta(12, 5_000)?);
    let result = executor(Arc::new(source))
        .execute(&rfq_quote_request(), &CancellationToken::new(), &())
        .await?;
    let QuoteOutcome::Ok {
        amount_out: start_amount,
        provenance: start_provenance,
    } = &result.results[0].start_outcome
    else {
        return Err("RFQ start quote must succeed".into());
    };
    let QuoteOutcome::Ok {
        amount_out: target_amount,
        provenance: target_provenance,
    } = &result.results[0].targets[0].outcome
    else {
        return Err("RFQ target quote must succeed".into());
    };

    assert_eq!(start_amount.as_str(), "2000000000");
    assert_eq!(target_amount.as_str(), "1995000000");
    assert_eq!(
        start_provenance.rfq_observation_cursor,
        Some(support::public_position(10))
    );
    assert_eq!(
        target_provenance.rfq_observation_cursor,
        Some(support::public_position(12))
    );
    assert_eq!(target_provenance.rfq_observation_age_ms, Some(1));
    Ok(())
}

#[tokio::test]
async fn unavailable_cells_keep_their_distinct_reasons() -> Result<(), Box<dyn std::error::Error>> {
    let mut source = FixtureSource::native()?;
    source.target_plan.visible_through_block = 101;
    source
        .target_plan
        .lineage_invalid_intervals
        .push(state_history::BlockInterval::new(101, 101)?);
    let mut request = native_quote_request(100, 100);
    request.lags = vec![1, 2];
    request.amounts_in = vec![support::amount("1000000")];
    let result = executor(Arc::new(source))
        .execute(&request, &CancellationToken::new(), &())
        .await?;

    assert!(matches!(
        result.results[0].start_outcome,
        QuoteOutcome::Ok { .. }
    ));
    assert!(matches!(
        result.results[0].targets[0].outcome,
        QuoteOutcome::Unavailable {
            reason: UnavailableReason::CanonicalLineageInvalid,
            ..
        }
    ));
    assert!(matches!(
        result.results[0].targets[1].outcome,
        QuoteOutcome::Unavailable {
            reason: UnavailableReason::ReplicaCutoff,
            ..
        }
    ));
    Ok(())
}

#[tokio::test]
async fn rfq_without_an_observation_before_the_boundary_is_unavailable(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut source = FixtureSource::rfq()?;
    source.target_plan.legs[0].checkpoint.rfq_observed_at_ms = Some(5_000);
    source.target_plan.legs[0].deltas.clear();
    let result = executor(Arc::new(source))
        .execute(&rfq_quote_request(), &CancellationToken::new(), &())
        .await?;

    assert!(matches!(
        result.results[0].start_outcome,
        QuoteOutcome::Unavailable {
            reason: UnavailableReason::RfqObservationMissing,
            ..
        }
    ));
    assert!(matches!(
        result.results[0].targets[0].outcome,
        QuoteOutcome::Unavailable {
            reason: UnavailableReason::RfqObservationMissing,
            ..
        }
    ));
    Ok(())
}

#[tokio::test]
async fn missing_token_anchor_and_apply_anomaly_fail_closed(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut missing_tokens = FixtureSource::native()?;
    missing_tokens.target_plan.legs[0]
        .checkpoint
        .token_reference = None;
    let mut request = native_quote_request(100, 100);
    request.lags = vec![1];
    request.amounts_in = vec![support::amount("1000000")];
    let missing_result = executor(Arc::new(missing_tokens))
        .execute(&request, &CancellationToken::new(), &())
        .await?;
    assert!(matches!(
        missing_result.results[0].start_outcome,
        QuoteOutcome::Unavailable {
            reason: UnavailableReason::TokenSnapshotMissing,
            ..
        }
    ));

    let anomaly_result = executor(Arc::new(FixtureSource::native_with_apply_anomaly()?))
        .execute(&request, &CancellationToken::new(), &())
        .await?;
    assert!(matches!(
        anomaly_result.results[0].targets[0].outcome,
        QuoteOutcome::Unavailable {
            reason: UnavailableReason::StateApplicationInvalid,
            ..
        }
    ));
    Ok(())
}

#[tokio::test]
async fn vm_world_lease_serializes_complete_world_lifetimes(
) -> Result<(), Box<dyn std::error::Error>> {
    let first = acquire_vm_world_lease().await;
    let waiting = tokio::spawn(acquire_vm_world_lease());
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    drop(first);
    let second = waiting.await?;
    drop(second);
    Ok(())
}

fn executor(source: Arc<FixtureSource>) -> HistoricalQuoteExecutor<FixtureSource> {
    HistoricalQuoteExecutor::new(
        HistoricalReconstructor::new(source, ReadLimits::unbounded()),
        "phase-4-test",
    )
}
