mod support;

use std::sync::Arc;

use historical_quote::api::{Backend, BlockBounds, CoverageRequest, GapCause, GapKind};
use historical_quote::CoverageService;
use state_history::{BlockInterval, CoverageSnapshot};

use support::FixtureSource;

#[tokio::test]
async fn coverage_is_the_intersection_for_every_requested_backend(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut source = FixtureSource::native()?;
    source.coverage_snapshot = CoverageSnapshot {
        visible_through_block: 118,
        continuous_intervals: vec![BlockInterval::new(102, 107)?, BlockInterval::new(111, 118)?],
        known_gaps: vec![support::recorded_gap(7, 9, 108, 110)],
    };
    let service = CoverageService::new(Arc::new(source));
    let response = service
        .resolve(&CoverageRequest {
            api_revision: 1,
            chain_id: 8453,
            backends: vec![Backend::Rfq, Backend::Native],
            bounds: Some(BlockBounds {
                start: 100,
                end_inclusive: 120,
            }),
        })
        .await?;

    assert_eq!(response.backends, vec![Backend::Native, Backend::Rfq]);
    assert_eq!(response.visible_through_block, 118);
    assert_eq!(
        response.continuous_intervals,
        vec![
            BlockBounds {
                start: 102,
                end_inclusive: 107,
            },
            BlockBounds {
                start: 111,
                end_inclusive: 118,
            },
        ]
    );
    assert_eq!(response.known_gaps.len(), 1);
    assert_eq!(response.known_gaps[0].kind, GapKind::Recorded);
    assert_eq!(response.known_gaps[0].cause, Some(GapCause::WriteFailed));
    Ok(())
}

#[tokio::test]
async fn replica_cutoff_is_not_reported_as_a_gap() -> Result<(), Box<dyn std::error::Error>> {
    let mut source = FixtureSource::native()?;
    source.coverage_snapshot = CoverageSnapshot {
        visible_through_block: 118,
        continuous_intervals: vec![BlockInterval::new(100, 118)?],
        known_gaps: Vec::new(),
    };
    let response = CoverageService::new(Arc::new(source))
        .resolve(&CoverageRequest {
            api_revision: 1,
            chain_id: 8453,
            backends: vec![Backend::Native],
            bounds: Some(BlockBounds {
                start: 100,
                end_inclusive: 120,
            }),
        })
        .await?;

    assert_eq!(response.visible_through_block, 118);
    assert!(response.known_gaps.is_empty());
    assert_eq!(response.continuous_intervals[0].end_inclusive, 118);
    Ok(())
}

#[tokio::test]
async fn unbounded_coverage_has_no_pool_existence_claim() -> Result<(), Box<dyn std::error::Error>>
{
    let response = CoverageService::new(Arc::new(FixtureSource::native()?))
        .resolve(&CoverageRequest {
            api_revision: 1,
            chain_id: 8453,
            backends: vec![Backend::Native],
            bounds: None,
        })
        .await?;
    let json = serde_json::to_value(response)?;

    assert!(json.get("bounds").is_none());
    assert!(json.get("pool").is_none());
    assert!(json.get("componentId").is_none());
    Ok(())
}

#[test]
fn duplicate_backends_are_rejected_at_the_wire_boundary() {
    let request = serde_json::from_value::<CoverageRequest>(serde_json::json!({
        "apiRevision": 1,
        "chainId": 8453,
        "backends": ["native", "native"]
    }));

    assert!(request.is_err());
}
