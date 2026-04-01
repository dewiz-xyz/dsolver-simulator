use crate::models::messages::{
    EncodeErrorResponse, QuoteFailure, QuoteFailureKind, QuoteMeta, QuoteResult,
    QuoteResultQuality, QuoteStatus,
};

/// Constructs a router-level timeout response using sentinel values.
/// - request_id: empty
/// - block_number: 0
/// - total_pools: None
pub fn router_timeout_result() -> QuoteResult {
    QuoteResult {
        request_id: String::new(),
        data: Vec::new(),
        meta: simulate_timeout_meta(
            0,
            None,
            None,
            None,
            None,
            "Simulate request timed out".to_string(),
        ),
    }
}

pub fn simulate_timeout_meta(
    block_number: u64,
    vm_block_number: Option<u64>,
    rfq_block_number: Option<u64>,
    total_pools: Option<usize>,
    auction_id: Option<String>,
    message: String,
) -> QuoteMeta {
    let failure = QuoteFailure {
        kind: QuoteFailureKind::Timeout,
        message,
        pool: None,
        pool_name: None,
        pool_address: None,
        protocol: None,
    };

    QuoteMeta {
        status: QuoteStatus::Ready,
        result_quality: QuoteResultQuality::RequestLevelFailure,
        partial_kind: None,
        block_number,
        vm_block_number,
        rfq_block_number,
        matching_pools: 0,
        candidate_pools: 0,
        total_pools,
        auction_id,
        pool_results: Vec::new(),
        vm_unavailable: false,
        rfq_unavailable: false,
        failures: vec![failure],
    }
}

/// Constructs an encode router-level timeout response using sentinel values.
pub fn encode_router_timeout_result() -> EncodeErrorResponse {
    EncodeErrorResponse {
        error: "Encode request timed out".to_string(),
        request_id: None,
    }
}

#[cfg(test)]
mod tests {
    use super::{encode_router_timeout_result, router_timeout_result, simulate_timeout_meta};
    use crate::models::messages::{QuoteResultQuality, QuoteStatus};

    #[test]
    fn router_timeout_result_sets_request_level_failure_quality() {
        let result = router_timeout_result();
        assert_eq!(result.request_id, "");
        assert!(result.data.is_empty());
        assert!(matches!(result.meta.status, QuoteStatus::Ready));
        assert_eq!(
            result.meta.result_quality,
            QuoteResultQuality::RequestLevelFailure
        );
        assert!(result.meta.partial_kind.is_none());
        assert!(result.meta.pool_results.is_empty());
        assert!(!result.meta.vm_unavailable);
        assert!(!result.meta.rfq_unavailable);
        assert_eq!(result.meta.failures.len(), 1);
    }

    #[test]
    fn simulate_timeout_meta_sets_ready_request_level_failure() {
        let meta = simulate_timeout_meta(
            42,
            Some(41),
            Some(42),
            Some(12),
            Some("auction-1".to_string()),
            "Simulate request timed out after 123ms".to_string(),
        );
        assert!(matches!(meta.status, QuoteStatus::Ready));
        assert_eq!(meta.result_quality, QuoteResultQuality::RequestLevelFailure);
        assert!(meta.partial_kind.is_none());
        assert_eq!(meta.block_number, 42);
        assert_eq!(meta.vm_block_number, Some(41));
        assert_eq!(meta.rfq_block_number, Some(42));
        assert_eq!(meta.total_pools, Some(12));
        assert_eq!(meta.auction_id.as_deref(), Some("auction-1"));
        assert_eq!(meta.failures.len(), 1);
    }

    #[test]
    fn encode_router_timeout_result_uses_sentinel_values() {
        let result = encode_router_timeout_result();
        assert_eq!(result.error, "Encode request timed out");
        assert!(result.request_id.is_none());
    }
}
