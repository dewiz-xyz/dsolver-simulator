mod support;

use std::sync::Arc;

use historical_quote::api::{
    Backend, PairStatus, PoolSelector, QuoteFailureReason, StreamPosition,
};
use historical_quote::{ConsistencyChecker, HistoricalError};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use state_history::{
    checkpoint_s3_key, encode_archive, token_snapshot_s3_key, validate_checkpoint_object_bytes,
    validate_checkpoint_token_object_bytes, CheckpointArchive, CheckpointKind, CheckpointManifest,
    EncodedArchive, ReadLimitError, ReadLimits, TokenSnapshotRef,
};
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
async fn verification_report_covers_storage_object_integrity(
) -> Result<(), Box<dyn std::error::Error>> {
    let report = checker(Arc::new(FixtureSource::native_consistency(true)?), 100)
        .execute(
            &consistency_request(native_pool()),
            &CancellationToken::new(),
            &(),
        )
        .await?;

    assert!(report.storage.range_plan_valid);
    assert!(report.storage.checkpoint_objects_valid);
    assert!(report.storage.token_objects_valid);
    assert!(report.storage.delta_order_valid);
    assert_eq!(report.storage.replay_plans_verified, 1);
    assert_eq!(report.storage.checkpoint_objects_verified, 2);
    assert_eq!(report.storage.token_objects_verified, 2);
    assert_eq!(report.storage.deltas_verified, 1);
    Ok(())
}

#[tokio::test]
async fn rfq_verification_reads_token_objects_and_rejects_missing_references(
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = support::rfq_quote_request().pool;
    let source = Arc::new(FixtureSource::rfq_consistency(true)?);
    let report = checker(Arc::clone(&source), 100)
        .execute(
            &consistency_request(pool.clone()),
            &CancellationToken::new(),
            &(),
        )
        .await?;
    assert_eq!(source.token_fetch_count(), 2);
    assert_eq!(report.storage.token_objects_verified, 2);

    let missing = checker(Arc::new(FixtureSource::rfq_consistency(false)?), 100)
        .execute(&consistency_request(pool), &CancellationToken::new(), &())
        .await;
    assert!(matches!(
        missing,
        Err(HistoricalError::HistoricalDataInvalid(_))
    ));
    Ok(())
}

#[tokio::test]
async fn invalid_delta_order_or_backend_metadata_fails_storage_verification(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut unordered = FixtureSource::native_consistency(true)?;
    unordered
        .pair_plan
        .as_mut()
        .ok_or("fixture pair plan must exist")?
        .deltas[0]
        .position = state_history::StreamPosition {
        generation: 1,
        message_seq: 1,
    };
    let result = checker(Arc::new(unordered), 100)
        .execute(
            &consistency_request(native_pool()),
            &CancellationToken::new(),
            &(),
        )
        .await;
    assert!(matches!(
        result,
        Err(HistoricalError::HistoricalDataInvalid(_))
    ));

    let mut missing_backend = FixtureSource::native_consistency(true)?;
    missing_backend
        .pair_plan
        .as_mut()
        .ok_or("fixture pair plan must exist")?
        .deltas[0]
        .applicable_backends
        .clear();
    let result = checker(Arc::new(missing_backend), 100)
        .execute(
            &consistency_request(native_pool()),
            &CancellationToken::new(),
            &(),
        )
        .await;
    assert!(matches!(
        result,
        Err(HistoricalError::HistoricalDataInvalid(_))
    ));
    Ok(())
}

#[test]
fn concrete_storage_validation_covers_the_removed_checker_matrix(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut manifest = support::manifest(
        42,
        100,
        support::position(1),
        state_history::Backend::Native,
        false,
        None,
    );
    let archive = support::archive(&manifest, "native", json!({"states": []}));
    let encoded = encode_archive(archive.clone())?;
    let checkpoint_key = checkpoint_s3_key(
        "state-history",
        manifest.chain_id,
        manifest.position,
        CheckpointKind::Interval,
    );
    manifest.s3_key = Some(checkpoint_key.clone());
    manifest.archive_sha256 = Some(encoded.info.sha256.clone());
    manifest.archive_bytes = Some(encoded.info.archive_bytes);
    manifest.compressed_bytes = Some(encoded.info.compressed_bytes);
    assert_checkpoint_validation_matrix(&manifest, &checkpoint_key, &encoded, archive)?;

    let raw_tokens = support::native_tokens()?;
    let tokens: Value = serde_json::from_str(raw_tokens.tokens.get())?;
    let token_fixture = encode_token_fixture(manifest.chain_id, raw_tokens.token_count, &tokens)?;
    let token_key =
        token_snapshot_s3_key("state-history", manifest.chain_id, &token_fixture.sha256);
    manifest.token_reference = Some(TokenSnapshotRef {
        s3_key: token_key.clone(),
        sha256: token_fixture.sha256.clone(),
        token_count: raw_tokens.token_count,
        token_bytes: token_fixture.decoded_bytes,
    });
    validate_checkpoint_token_object_bytes(
        &manifest,
        &token_key,
        &token_fixture.bytes,
        ReadLimits::unbounded(),
    )?
    .ok_or("valid token object must resolve")?;

    assert_token_reference_validation_matrix(&manifest, &token_key, &token_fixture)?;
    assert_token_semantics_validation_matrix(manifest, raw_tokens.token_count, tokens)?;
    Ok(())
}

fn assert_checkpoint_validation_matrix(
    manifest: &CheckpointManifest,
    checkpoint_key: &str,
    encoded: &EncodedArchive,
    archive: CheckpointArchive,
) -> Result<(), Box<dyn std::error::Error>> {
    validate_checkpoint_object_bytes(
        manifest,
        checkpoint_key,
        &encoded.bytes,
        ReadLimits::unbounded(),
    )?;

    let mut wrong_key = manifest.clone();
    wrong_key.s3_key = Some("wrong/checkpoint.zst".to_owned());
    assert_integrity_error(
        "checkpoint key",
        validate_checkpoint_object_bytes(
            &wrong_key,
            checkpoint_key,
            &encoded.bytes,
            ReadLimits::unbounded(),
        ),
    );
    let mut wrong_digest = manifest.clone();
    wrong_digest.archive_sha256 = Some("deadbeef".to_owned());
    assert_integrity_error(
        "checkpoint digest",
        validate_checkpoint_object_bytes(
            &wrong_digest,
            checkpoint_key,
            &encoded.bytes,
            ReadLimits::unbounded(),
        ),
    );
    let mut wrong_compressed_size = manifest.clone();
    wrong_compressed_size.compressed_bytes = Some(encoded.info.compressed_bytes + 1);
    assert_integrity_error(
        "checkpoint compressed size",
        validate_checkpoint_object_bytes(
            &wrong_compressed_size,
            checkpoint_key,
            &encoded.bytes,
            ReadLimits::unbounded(),
        ),
    );
    let mut wrong_decoded_size = manifest.clone();
    wrong_decoded_size.archive_bytes = Some(encoded.info.archive_bytes + 1);
    assert_integrity_error(
        "checkpoint decoded size",
        validate_checkpoint_object_bytes(
            &wrong_decoded_size,
            checkpoint_key,
            &encoded.bytes,
            ReadLimits::unbounded(),
        ),
    );
    let mut wrong_metadata = manifest.clone();
    wrong_metadata.block_number += 1;
    assert_integrity_error(
        "checkpoint metadata",
        validate_checkpoint_object_bytes(
            &wrong_metadata,
            checkpoint_key,
            &encoded.bytes,
            ReadLimits::unbounded(),
        ),
    );
    let empty = encode_archive(state_history::CheckpointArchive {
        metadata: archive.metadata,
        payloads_json: Vec::new(),
    })?;
    let mut empty_manifest = manifest.clone();
    empty_manifest.archive_sha256 = Some(empty.info.sha256.clone());
    empty_manifest.archive_bytes = Some(empty.info.archive_bytes);
    empty_manifest.compressed_bytes = Some(empty.info.compressed_bytes);
    assert_integrity_error(
        "checkpoint payloads",
        validate_checkpoint_object_bytes(
            &empty_manifest,
            checkpoint_key,
            &empty.bytes,
            ReadLimits::unbounded(),
        ),
    );

    Ok(())
}

fn assert_token_reference_validation_matrix(
    manifest: &CheckpointManifest,
    token_key: &str,
    fixture: &TokenFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut wrong_token_key = manifest.clone();
    wrong_token_key
        .token_reference
        .as_mut()
        .ok_or("token reference must exist")?
        .s3_key = "wrong/tokens.zst".to_owned();
    assert_integrity_error(
        "token key",
        validate_checkpoint_token_object_bytes(
            &wrong_token_key,
            token_key,
            &fixture.bytes,
            ReadLimits::unbounded(),
        ),
    );
    let mut wrong_token_digest = manifest.clone();
    wrong_token_digest
        .token_reference
        .as_mut()
        .ok_or("token reference must exist")?
        .sha256 = "deadbeef".to_owned();
    assert_integrity_error(
        "token digest",
        validate_checkpoint_token_object_bytes(
            &wrong_token_digest,
            token_key,
            &fixture.bytes,
            ReadLimits::unbounded(),
        ),
    );
    let mut wrong_token_count = manifest.clone();
    wrong_token_count
        .token_reference
        .as_mut()
        .ok_or("token reference must exist")?
        .token_count += 1;
    assert_integrity_error(
        "token count",
        validate_checkpoint_token_object_bytes(
            &wrong_token_count,
            token_key,
            &fixture.bytes,
            ReadLimits::unbounded(),
        ),
    );
    let mut wrong_token_size = manifest.clone();
    wrong_token_size
        .token_reference
        .as_mut()
        .ok_or("token reference must exist")?
        .token_bytes += 1;
    assert_integrity_error(
        "token decoded size",
        validate_checkpoint_token_object_bytes(
            &wrong_token_size,
            token_key,
            &fixture.bytes,
            ReadLimits::unbounded(),
        ),
    );
    Ok(())
}

fn assert_token_semantics_validation_matrix(
    manifest: CheckpointManifest,
    token_count: u64,
    tokens: Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut reversed_tokens = tokens.clone();
    reversed_tokens
        .as_array_mut()
        .ok_or("tokens must be an array")?
        .reverse();
    let reversed = encode_token_fixture(manifest.chain_id, token_count, &reversed_tokens)?;
    let reversed_key = token_snapshot_s3_key("state-history", manifest.chain_id, &reversed.sha256);
    let mut wrong_token_order = manifest.clone();
    wrong_token_order.token_reference = Some(TokenSnapshotRef {
        s3_key: reversed_key.clone(),
        sha256: reversed.sha256,
        token_count,
        token_bytes: reversed.decoded_bytes,
    });
    assert_integrity_error(
        "token order",
        validate_checkpoint_token_object_bytes(
            &wrong_token_order,
            &reversed_key,
            &reversed.bytes,
            ReadLimits::unbounded(),
        ),
    );

    let mut foreign_tokens = tokens;
    for token in foreign_tokens
        .as_array_mut()
        .ok_or("tokens must be an array")?
    {
        token["chainId"] = json!(1);
    }
    let foreign = encode_token_fixture(1, token_count, &foreign_tokens)?;
    let foreign_key = token_snapshot_s3_key("state-history", manifest.chain_id, &foreign.sha256);
    let mut wrong_token_chain = manifest;
    wrong_token_chain.token_reference = Some(TokenSnapshotRef {
        s3_key: foreign_key.clone(),
        sha256: foreign.sha256,
        token_count,
        token_bytes: foreign.decoded_bytes,
    });
    assert_integrity_error(
        "token chain",
        validate_checkpoint_token_object_bytes(
            &wrong_token_chain,
            &foreign_key,
            &foreign.bytes,
            ReadLimits::unbounded(),
        ),
    );
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

fn assert_integrity_error<T>(label: &str, result: Result<T, ReadLimitError>) {
    assert!(
        matches!(result, Err(ReadLimitError::Read(_))),
        "{label} must fail as stored-data integrity"
    );
}

fn encode_token_fixture(
    chain_id: u64,
    token_count: u64,
    tokens: &Value,
) -> Result<TokenFixture, Box<dyn std::error::Error>> {
    let decoded = serde_json::to_vec(&json!({
        "schema_version": state_history::TOKEN_SNAPSHOT_SCHEMA_VERSION,
        "chain_id": chain_id,
        "token_count": token_count,
        "tokens": tokens,
    }))?;
    let sha256 = hex::encode(Sha256::digest(&decoded));
    let decoded_bytes = u64::try_from(decoded.len())?;
    let bytes = zstd::stream::encode_all(decoded.as_slice(), 3)?;
    Ok(TokenFixture {
        bytes,
        sha256,
        decoded_bytes,
    })
}

struct TokenFixture {
    bytes: Vec<u8>,
    sha256: String,
    decoded_bytes: u64,
}
