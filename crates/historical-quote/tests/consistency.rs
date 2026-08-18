mod support;

use std::sync::Arc;

use historical_quote::api::{
    Backend, PairStatus, PoolSelector, QuoteFailureReason, StreamPosition,
};
use historical_quote::{ConsistencyChecker, HistoricalError};
use state_history::ReadLimits;
use tokio_util::sync::CancellationToken;

use support::{consistency_request, native_pool, FixtureSource};

#[tokio::test]
async fn direct_target_matches_earlier_checkpoint_plus_deltas(
) -> Result<(), Box<dyn std::error::Error>> {
    let report = checker(Arc::new(FixtureSource::native_consistency(true)?), 100)
        .execute(
            &consistency_request(native_pool()),
            &CancellationToken::new(),
            &(),
        )
        .await?;

    assert_eq!(report.pairs[0].status, PairStatus::Pass);
    assert_eq!(report.summary.pass, 1);
    assert_eq!(
        report.pairs[0].quote_comparisons[0].status,
        PairStatus::Pass
    );
    assert_eq!(report.pairs[0].total_difference_count, 0);
    assert_eq!(report.pairs[0].direct_state, report.pairs[0].replayed_state);
    Ok(())
}

#[tokio::test]
async fn canonical_and_exact_quote_mismatches_are_reported(
) -> Result<(), Box<dyn std::error::Error>> {
    let report = checker(Arc::new(FixtureSource::native_consistency(false)?), 100)
        .execute(
            &consistency_request(native_pool()),
            &CancellationToken::new(),
            &(),
        )
        .await?;

    assert_eq!(report.pairs[0].status, PairStatus::Mismatch);
    assert_eq!(report.summary.mismatch, 1);
    assert_eq!(
        report.pairs[0].quote_comparisons[0].status,
        PairStatus::Mismatch
    );
    assert!(report.pairs[0].total_difference_count > 0);
    assert_eq!(report.pairs[0].affected_backends, vec![Backend::Native]);
    assert_eq!(
        report.pairs[0].affected_component_ids,
        vec![support::NATIVE_COMPONENT]
    );
    Ok(())
}

#[tokio::test]
async fn matching_quote_failures_are_not_comparable() -> Result<(), Box<dyn std::error::Error>> {
    let missing_pool = PoolSelector {
        component_id: "native:missing".to_owned(),
        ..native_pool()
    };
    let report = checker(Arc::new(FixtureSource::native_consistency(true)?), 100)
        .execute(
            &consistency_request(missing_pool),
            &CancellationToken::new(),
            &(),
        )
        .await?;

    assert_eq!(report.pairs[0].status, PairStatus::NotComparable);
    assert_eq!(report.summary.not_comparable, 1);
    assert!(matches!(
        report.pairs[0].quote_comparisons[0].direct_outcome,
        historical_quote::api::ConsistencyQuoteOutcome::QuoteFailed {
            reason: QuoteFailureReason::ComponentNotFound
        }
    ));
    Ok(())
}

#[tokio::test]
async fn gap_has_precedence_over_state_and_quote_comparison(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut source = FixtureSource::native_consistency(false)?;
    let Some(pair_plan) = source.pair_plan.as_mut() else {
        return Err("fixture pair plan must exist".into());
    };
    pair_plan.gaps.push(support::recorded_gap(2, 2, 101, 101));
    let report = checker(Arc::new(source), 100)
        .execute(
            &consistency_request(native_pool()),
            &CancellationToken::new(),
            &(),
        )
        .await?;

    assert_eq!(report.pairs[0].status, PairStatus::Gap);
    assert_eq!(report.summary.gap, 1);
    assert!(report.pairs[0].direct_state.is_none());
    assert!(report.pairs[0].quote_comparisons.is_empty());
    Ok(())
}

#[tokio::test]
async fn no_eligible_pair_is_a_completed_not_comparable_selection(
) -> Result<(), Box<dyn std::error::Error>> {
    let report = checker(Arc::new(FixtureSource::native()?), 100)
        .execute(
            &consistency_request(native_pool()),
            &CancellationToken::new(),
            &(),
        )
        .await?;

    assert_eq!(
        report.selection_status,
        historical_quote::api::ConsistencySelectionStatus::NotComparable
    );
    assert!(report.pairs.is_empty());
    assert_eq!(report.summary.selected_checkpoint_pairs, 0);
    Ok(())
}

#[tokio::test]
async fn structured_differences_are_bounded_without_losing_total_count(
) -> Result<(), Box<dyn std::error::Error>> {
    let report = checker(Arc::new(FixtureSource::native_consistency(false)?), 0)
        .execute(
            &consistency_request(native_pool()),
            &CancellationToken::new(),
            &(),
        )
        .await?;

    assert!(report.pairs[0].total_difference_count > 0);
    assert!(report.pairs[0].differences.is_empty());
    Ok(())
}

#[tokio::test]
async fn resolved_checkpoint_pairs_cannot_exceed_the_worker_limit(
) -> Result<(), Box<dyn std::error::Error>> {
    let checker = ConsistencyChecker::with_limits(
        Arc::new(FixtureSource::native_consistency(true)?),
        ReadLimits::unbounded(),
        100,
        0,
    );
    let result = checker
        .execute(
            &consistency_request(native_pool()),
            &CancellationToken::new(),
            &(),
        )
        .await;

    assert!(matches!(result, Err(HistoricalError::CheckBudgetExceeded)));
    Ok(())
}

#[tokio::test]
async fn canonical_vm_comparison_is_rejected_and_cancellation_is_observed(
) -> Result<(), Box<dyn std::error::Error>> {
    let source = Arc::new(FixtureSource::native()?);
    let mut vm_request = consistency_request(PoolSelector {
        backend: Backend::Vm,
        ..native_pool()
    });
    vm_request.backends = vec![Backend::Vm];
    let vm_result = checker(Arc::clone(&source), 100)
        .execute(&vm_request, &CancellationToken::new(), &())
        .await;
    let Err(vm_error) = vm_result else {
        return Err("VM comparison must fail".into());
    };
    assert!(matches!(vm_error, HistoricalError::UnsupportedCanonicalVm));

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancellation_result = checker(source, 100)
        .execute(&consistency_request(native_pool()), &cancellation, &())
        .await;
    let Err(cancellation_error) = cancellation_result else {
        return Err("cancelled check must stop".into());
    };
    assert!(matches!(cancellation_error, HistoricalError::Cancelled));
    Ok(())
}

#[test]
fn explicit_positions_remain_exact_and_ordered() -> Result<(), Box<dyn std::error::Error>> {
    let request = consistency_request(native_pool());
    let historical_quote::api::ConsistencySelection::CheckpointPairs { pairs } = request.selection
    else {
        return Err("fixture must use explicit pairs".into());
    };
    assert_eq!(
        pairs[0].earlier_position,
        StreamPosition {
            generation: 1,
            message_seq: 1,
        }
    );
    assert_eq!(pairs[0].target_position.message_seq, 3);
    Ok(())
}

fn checker(
    source: Arc<FixtureSource>,
    max_differences: usize,
) -> ConsistencyChecker<FixtureSource> {
    ConsistencyChecker::new(source, ReadLimits::unbounded(), max_differences)
}
