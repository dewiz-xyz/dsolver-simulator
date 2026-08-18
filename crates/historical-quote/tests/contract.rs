use historical_quote::api::{
    normalize_consistency_request, normalize_quote_request, ApiError, ApiErrorCode, Backend,
    BlockRange, ConsistencyCheckReport, ConsistencyCheckRequest, ConsistencySelectionStatus,
    CoverageRequest, CoverageResponse, HistoricalQuoteResult, JobEnvelope, QuoteJobRequest,
    QuoteOutcome, API_REVISION,
};
use serde_json::{json, Value};

const REQUEST_ID: &str = "3bbd9a8c-d942-4f9d-b96d-7ee63b9308aa";
const CONSISTENCY_REPORT_GOLDEN: &str = r#"{
  "requestId": "8021caa1-0257-4d42-9690-6d7ebd10b8f8",
  "chainId": 8453,
  "backends": [
    "native"
  ],
  "selection": {
    "type": "blockRange",
    "start": 34000000,
    "endInclusive": 34100000
  },
  "selectionStatus": "notComparable",
  "storage": {
    "rangePlanValid": true,
    "checkpointObjectsValid": true,
    "tokenObjectsValid": true,
    "deltaOrderValid": true,
    "replayPlansVerified": 0,
    "checkpointObjectsVerified": 0,
    "tokenObjectsVerified": 0,
    "deltasVerified": 0
  },
  "summary": {
    "selectedCheckpointPairs": 0,
    "comparedCheckpointPairs": 0,
    "pass": 0,
    "mismatch": 0,
    "gap": 0,
    "notComparable": 0
  },
  "pairs": []
}"#;
const HISTORICAL_QUOTE_RESULT_GOLDEN: &str = r#"{
  "requestId": "3bbd9a8c-d942-4f9d-b96d-7ee63b9308aa",
  "chainId": 8453,
  "pool": {
    "backend": "native",
    "protocol": "uniswap_v3",
    "componentId": "0xcomponent",
    "tokenIn": "0xtoken-in",
    "tokenOut": "0xtoken-out"
  },
  "visibleThroughBlock": 13,
  "gaps": [],
  "results": [
    {
      "startBlock": 10,
      "amountIn": "1",
      "startOutcome": {
        "outcome": "quote_failed",
        "reason": "component_not_found"
      },
      "targets": [
        {
          "lag": 1,
          "targetBlock": 11,
          "outcome": {
            "outcome": "unavailable",
            "reason": "gap",
            "gapRefs": [
              "gap-1"
            ]
          }
        },
        {
          "lag": 3,
          "targetBlock": 13,
          "outcome": {
            "outcome": "quote_failed",
            "reason": "insufficient_liquidity"
          }
        }
      ]
    },
    {
      "startBlock": 10,
      "amountIn": "5",
      "startOutcome": {
        "outcome": "quote_failed",
        "reason": "component_not_found"
      },
      "targets": [
        {
          "lag": 1,
          "targetBlock": 11,
          "outcome": {
            "outcome": "quote_failed",
            "reason": "zero_output"
          }
        },
        {
          "lag": 3,
          "targetBlock": 13,
          "outcome": {
            "outcome": "quote_failed",
            "reason": "simulation_failed"
          }
        }
      ]
    }
  ]
}"#;
const JOB_ENVELOPE_GOLDEN: &str = r#"{
  "jobId": "997b4517-2377-4c83-91a8-577e919ce72a",
  "requestId": "3bbd9a8c-d942-4f9d-b96d-7ee63b9308aa",
  "jobType": "historicalQuote",
  "state": "running",
  "submittedAt": "2026-08-13T15:00:00Z",
  "deadlineAt": "2026-08-13T15:02:00Z",
  "startedAt": "2026-08-13T15:00:01Z",
  "cancellationRequested": false,
  "progress": {
    "completedComparisons": 12,
    "totalComparisons": 30,
    "percentComplete": 40
  }
}"#;

fn quote_request_value() -> Value {
    json!({
        "requestId": REQUEST_ID,
        "apiRevision": 1,
        "timeoutMs": 120_000,
        "chainId": 8453,
        "pool": {
            "backend": "native",
            "protocol": "uniswap_v3",
            "componentId": "0xcomponent",
            "tokenIn": "0xtoken-in",
            "tokenOut": "0xtoken-out"
        },
        "blocks": {"start": 10, "endInclusive": 20, "step": 5},
        "lags": [5, 1, 3],
        "amountsIn": ["5000000", "0001000000"]
    })
}

#[test]
fn api_revision_is_one() {
    assert_eq!(API_REVISION, 1);
}

#[test]
#[expect(clippy::unwrap_used, reason = "the fixture must be valid JSON")]
fn request_requires_uuid_version_four() {
    let mut value = quote_request_value();
    value["requestId"] = json!("00000000-0000-1000-8000-000000000000");

    let error = serde_json::from_value::<QuoteJobRequest>(value).unwrap_err();
    assert!(error.to_string().contains("UUID version 4"));
}

#[test]
#[expect(clippy::unwrap_used, reason = "the test needs the validation error")]
fn numeric_duplicate_amounts_are_rejected() {
    let mut value = quote_request_value();
    value["amountsIn"] = json!(["01", "1"]);

    let error = serde_json::from_value::<QuoteJobRequest>(value).unwrap_err();
    assert!(error.to_string().contains("duplicate numeric input amount"));
}

#[test]
#[expect(clippy::unwrap_used, reason = "the test needs the validation error")]
fn duplicate_lags_are_rejected() {
    let mut value = quote_request_value();
    value["lags"] = json!([1, 1]);

    let error = serde_json::from_value::<QuoteJobRequest>(value).unwrap_err();
    assert!(error.to_string().contains("distinct positive integers"));
}

#[test]
#[expect(clippy::unwrap_used, reason = "the request fixture must be valid")]
fn block_expansion_is_inclusive_and_step_defaults_to_one() {
    let blocks: BlockRange = serde_json::from_value(json!({
        "start": 8,
        "endInclusive": 10
    }))
    .unwrap();

    assert_eq!(blocks.start_blocks().collect::<Vec<_>>(), vec![8, 9, 10]);
}

#[test]
#[expect(clippy::unwrap_used, reason = "the request fixture must be valid")]
fn quote_normalization_is_deterministic_and_excludes_only_request_id() {
    let request: QuoteJobRequest = serde_json::from_value(quote_request_value()).unwrap();
    let normalized = normalize_quote_request(&request);

    assert_eq!(
        serde_json::to_value(normalized).unwrap(),
        json!({
            "apiRevision": 1,
            "timeoutMs": 120000,
            "chainId": 8453,
            "pool": {
                "backend": "native",
                "protocol": "uniswap_v3",
                "componentId": "0xcomponent",
                "tokenIn": "0xtoken-in",
                "tokenOut": "0xtoken-out"
            },
            "blocks": {"start": 10, "endInclusive": 20, "step": 5},
            "lags": [1, 3, 5],
            "amountsIn": ["1000000", "5000000"]
        })
    );
}

#[test]
#[expect(clippy::unwrap_used, reason = "the request fixtures must be valid")]
fn quote_fingerprint_ignores_accepted_order_but_includes_timeout() {
    let first: QuoteJobRequest = serde_json::from_value(quote_request_value()).unwrap();
    let mut reordered = quote_request_value();
    reordered["requestId"] = json!("8021caa1-0257-4d42-9690-6d7ebd10b8f8");
    reordered["lags"].as_array_mut().unwrap().reverse();
    reordered["amountsIn"].as_array_mut().unwrap().reverse();
    let second: QuoteJobRequest = serde_json::from_value(reordered).unwrap();
    let mut different_timeout = quote_request_value();
    different_timeout["timeoutMs"] = json!(119_999);
    let third: QuoteJobRequest = serde_json::from_value(different_timeout).unwrap();

    let first = normalize_quote_request(&first);
    let second = normalize_quote_request(&second);
    let third = normalize_quote_request(&third);
    assert_eq!(first.fingerprint().unwrap(), second.fingerprint().unwrap());
    assert_ne!(first.fingerprint().unwrap(), third.fingerprint().unwrap());
}

#[test]
#[expect(clippy::unwrap_used, reason = "the test needs each validation error")]
fn quote_request_rejects_every_boundary_violation() {
    let invalid = [
        ("apiRevision", json!(2), "apiRevision must equal 1"),
        (
            "timeoutMs",
            json!(0),
            "timeoutMs must be a positive integer",
        ),
        ("chainId", json!(1), "chainId must equal 8453"),
        ("lags", json!([]), "distinct positive integers"),
        ("lags", json!([0]), "distinct positive integers"),
        ("amountsIn", json!([]), "amountsIn must be non-empty"),
        (
            "amountsIn",
            json!(["-1"]),
            "unsigned base-10 integer string",
        ),
    ];

    for (field, replacement, expected) in invalid {
        let mut value = quote_request_value();
        value[field] = replacement;
        let error = serde_json::from_value::<QuoteJobRequest>(value).unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }

    let mut empty_selector = quote_request_value();
    empty_selector["pool"]["componentId"] = json!("");
    assert!(serde_json::from_value::<QuoteJobRequest>(empty_selector)
        .unwrap_err()
        .to_string()
        .contains("componentId must be a non-empty exact selector"));

    let mut zero_step = quote_request_value();
    zero_step["blocks"]["step"] = json!(0);
    assert!(serde_json::from_value::<QuoteJobRequest>(zero_step)
        .unwrap_err()
        .to_string()
        .contains("blocks.step must be a positive integer"));

    let mut unknown = quote_request_value();
    unknown["unexpected"] = json!(true);
    assert!(serde_json::from_value::<QuoteJobRequest>(unknown)
        .unwrap_err()
        .to_string()
        .contains("unknown field"));
}

#[test]
#[expect(clippy::unwrap_used, reason = "the request fixture must be valid")]
fn consistency_normalization_ignores_accepted_array_order() {
    let first: ConsistencyCheckRequest = serde_json::from_value(json!({
        "requestId": "8021caa1-0257-4d42-9690-6d7ebd10b8f8",
        "apiRevision": 1,
        "timeoutMs": 300000,
        "chainId": 8453,
        "backends": ["rfq", "native"],
        "selection": {
            "type": "checkpointPairs",
            "pairs": [
                {
                    "earlierPosition": {"generation": 12, "messageSeq": 1000},
                    "targetPosition": {"generation": 12, "messageSeq": 1800}
                },
                {
                    "earlierPosition": {"generation": 12, "messageSeq": 900},
                    "targetPosition": {"generation": 12, "messageSeq": 1200}
                }
            ]
        },
        "quotesToCompare": [
            {
                "pool": {
                    "backend": "rfq",
                    "protocol": "bebop",
                    "componentId": "rfq-1",
                    "tokenIn": "0x02",
                    "tokenOut": "0x01"
                },
                "amountsIn": ["02", "1"]
            },
            {
                "pool": {
                    "backend": "native",
                    "protocol": "uniswap_v3",
                    "componentId": "pool-1",
                    "tokenIn": "0x01",
                    "tokenOut": "0x02"
                },
                "amountsIn": ["5"]
            }
        ]
    }))
    .unwrap();
    let mut second_value = serde_json::to_value(&first).unwrap();
    second_value["backends"].as_array_mut().unwrap().reverse();
    second_value["selection"]["pairs"]
        .as_array_mut()
        .unwrap()
        .reverse();
    second_value["quotesToCompare"]
        .as_array_mut()
        .unwrap()
        .reverse();
    let second: ConsistencyCheckRequest = serde_json::from_value(second_value).unwrap();

    assert_eq!(
        normalize_consistency_request(&first),
        normalize_consistency_request(&second)
    );
}

#[test]
#[expect(clippy::unwrap_used, reason = "the coverage fixtures must be valid")]
fn coverage_contract_is_strict_and_keeps_replica_cutoff_separate() {
    let request: CoverageRequest = serde_json::from_value(json!({
        "apiRevision": 1,
        "chainId": 8453,
        "backends": ["native", "rfq"],
        "bounds": {"start": 100, "endInclusive": 120}
    }))
    .unwrap();
    assert_eq!(request.backends, vec![Backend::Native, Backend::Rfq]);

    let response: CoverageResponse = serde_json::from_value(json!({
        "apiRevision": 1,
        "chainId": 8453,
        "backends": ["native", "rfq"],
        "visibleThroughBlock": 120,
        "continuousIntervals": [
            {"start": 100, "endInclusive": 107},
            {"start": 111, "endInclusive": 120}
        ],
        "knownGaps": [{
            "kind": "recorded",
            "cause": "boundary_slip",
            "fromBlock": 108,
            "toBlockInclusive": 110
        }]
    }))
    .unwrap();
    assert_eq!(response.visible_through_block, 120);
    assert_eq!(response.continuous_intervals.len(), 2);

    let duplicate_backends = json!({
        "apiRevision": 1,
        "chainId": 8453,
        "backends": ["native", "native"]
    });
    assert!(
        serde_json::from_value::<CoverageRequest>(duplicate_backends)
            .unwrap_err()
            .to_string()
            .contains("duplicate-free")
    );
}

#[test]
#[expect(
    clippy::unwrap_used,
    reason = "the consistency report fixture must be valid"
)]
fn consistency_report_golden_pins_not_comparable_selection() {
    let report: ConsistencyCheckReport = serde_json::from_str(CONSISTENCY_REPORT_GOLDEN).unwrap();

    assert_eq!(
        report.selection_status,
        ConsistencySelectionStatus::NotComparable
    );
    assert!(report.pairs.is_empty());
    assert_eq!(
        serde_json::to_string_pretty(&report).unwrap(),
        CONSISTENCY_REPORT_GOLDEN
    );
}

#[test]
#[expect(clippy::unwrap_used, reason = "the response fixture must be valid")]
fn outcome_union_uses_one_exact_discriminator() {
    let outcome: QuoteOutcome = serde_json::from_value(json!({
        "outcome": "unavailable",
        "reason": "gap",
        "gapRefs": ["gap-1"]
    }))
    .unwrap();

    assert!(matches!(outcome, QuoteOutcome::Unavailable { .. }));
    assert_eq!(
        serde_json::to_value(outcome).unwrap()["outcome"],
        "unavailable"
    );
}

#[test]
#[expect(clippy::unwrap_used, reason = "the result fixture must be valid")]
fn grouped_result_golden_keeps_start_amount_and_lag_order() {
    let result: HistoricalQuoteResult =
        serde_json::from_str(HISTORICAL_QUOTE_RESULT_GOLDEN).unwrap();

    assert_eq!(result.results.len(), 2);
    assert_eq!(result.results[0].amount_in.as_str(), "1");
    assert_eq!(result.results[1].amount_in.as_str(), "5");
    assert_eq!(
        result.results[0]
            .targets
            .iter()
            .map(|target| target.lag)
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
    assert_eq!(
        serde_json::to_string_pretty(&result).unwrap(),
        HISTORICAL_QUOTE_RESULT_GOLDEN
    );
}

#[test]
#[expect(clippy::unwrap_used, reason = "the envelope fixture must be valid")]
fn job_envelope_golden_pins_progress_and_lifecycle_fields() {
    let envelope: JobEnvelope = serde_json::from_str(JOB_ENVELOPE_GOLDEN).unwrap();

    assert_eq!(
        serde_json::to_string_pretty(&envelope).unwrap(),
        JOB_ENVELOPE_GOLDEN
    );
}

#[test]
#[expect(clippy::unwrap_used, reason = "the error fixture must be valid")]
fn shared_error_shape_is_exact() {
    let error: ApiError = serde_json::from_value(json!({
        "code": "invalid_request",
        "message": "lags must contain distinct positive integers",
        "retryable": false,
        "details": {"field": "lags"}
    }))
    .unwrap();

    assert_eq!(error.code, ApiErrorCode::InvalidRequest);
    assert!(!error.retryable);
}

#[test]
#[expect(clippy::unwrap_used, reason = "the enum must serialize")]
fn backend_wire_names_are_exact() {
    assert_eq!(
        serde_json::to_value([Backend::Native, Backend::Vm, Backend::Rfq]).unwrap(),
        json!(["native", "vm", "rfq"])
    );
}

#[test]
#[expect(
    clippy::unwrap_used,
    reason = "the checked-in contract must be valid JSON"
)]
fn checked_in_openapi_matches_rust_types() {
    let generated = historical_quote::openapi::document_value();
    let checked_in: Value = serde_json::from_str(include_str!(
        "../../../openapi/historical-pool-quote-api.json"
    ))
    .unwrap();

    assert_eq!(generated, checked_in);
}

#[test]
#[expect(
    clippy::expect_used,
    reason = "the generated document must contain these contract objects"
)]
fn openapi_covers_exact_routes_security_headers_and_union_fields() {
    let document = historical_quote::openapi::document_value();
    assert_eq!(document["openapi"], "3.1.0");

    let paths = document["paths"]
        .as_object()
        .expect("paths must be an object");
    assert_eq!(
        paths.keys().map(String::as_str).collect::<Vec<_>>(),
        vec![
            "/coverage",
            "/jobs/consistency-check",
            "/jobs/quote",
            "/jobs/{jobId}",
            "/jobs/{jobId}/cancel",
            "/ready",
            "/status",
        ]
    );
    assert_eq!(
        document["components"]["securitySchemes"]["bearerAuth"]["scheme"],
        "bearer"
    );
    assert!(paths["/ready"]["get"].get("security").is_none());

    for (path, method) in [
        ("/coverage", "post"),
        ("/jobs/consistency-check", "post"),
        ("/jobs/quote", "post"),
        ("/jobs/{jobId}", "get"),
        ("/jobs/{jobId}/cancel", "post"),
        ("/status", "get"),
    ] {
        assert_eq!(paths[path][method]["security"][0]["bearerAuth"], json!([]));
    }

    for (path, method) in [
        ("/jobs/{jobId}", "get"),
        ("/jobs/{jobId}/cancel", "post"),
        ("/status", "get"),
    ] {
        let parameters = paths[path][method]["parameters"]
            .as_array()
            .expect("bodyless protected request must declare parameters");
        assert!(parameters.iter().any(|parameter| {
            parameter["name"] == "X-DSolver-History-API-Revision"
                && parameter["in"] == "header"
                && parameter["required"] == true
        }));
    }

    let outcomes = document["components"]["schemas"]["QuoteOutcome"]["oneOf"]
        .as_array()
        .expect("quote outcome must be a oneOf union");
    assert!(outcomes
        .iter()
        .any(|schema| schema["properties"].get("amountOut").is_some()));
    assert!(outcomes
        .iter()
        .any(|schema| schema["properties"].get("gapRefs").is_some()));
    assert!(outcomes.iter().all(|schema| {
        schema["properties"].get("amount_out").is_none()
            && schema["properties"].get("gap_refs").is_none()
    }));
}

#[test]
#[expect(
    clippy::expect_used,
    reason = "the generated document must contain these contract objects"
)]
fn openapi_validation_matches_boundary_rules() {
    let document = historical_quote::openapi::document_value();
    let schemas = &document["components"]["schemas"];
    let block_range = &schemas["BlockRange"];
    assert_eq!(block_range["properties"]["step"]["default"], 1);
    assert_eq!(block_range["properties"]["step"]["minimum"], 1);
    assert!(!block_range["required"]
        .as_array()
        .expect("BlockRange.required must be an array")
        .iter()
        .any(|field| field == "step"));

    let quote_request = &schemas["QuoteJobRequest"]["properties"];
    assert_eq!(quote_request["lags"]["minItems"], 1);
    assert_eq!(quote_request["lags"]["uniqueItems"], true);
    assert_eq!(quote_request["lags"]["items"]["minimum"], 1);
    assert_eq!(quote_request["amountsIn"]["minItems"], 1);
    assert_eq!(quote_request["amountsIn"]["uniqueItems"], true);

    for schema in ["CoverageRequest", "ConsistencyCheckRequest"] {
        let backends = &schemas[schema]["properties"]["backends"];
        assert_eq!(backends["minItems"], 1);
        assert_eq!(backends["uniqueItems"], true);
    }
    assert_eq!(
        schemas["QuoteComparisonRequest"]["properties"]["amountsIn"]["uniqueItems"],
        true
    );

    for schema in [
        "ConsistencyQuoteOutcome",
        "ConsistencySelection",
        "QuoteOutcome",
    ] {
        assert!(schemas[schema]["oneOf"]
            .as_array()
            .expect("tagged contract type must be a oneOf union")
            .iter()
            .all(|variant| variant["additionalProperties"] == false));
    }
    let selections = schemas["ConsistencySelection"]["oneOf"]
        .as_array()
        .expect("consistency selection must be a oneOf union");
    assert_eq!(selections[0]["properties"]["pairs"]["minItems"], 1);

    for schema in ["BlockBounds", "BlockRange", "PoolSelector"] {
        assert_eq!(schemas[schema]["additionalProperties"], false);
    }
}
