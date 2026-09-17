# Quote service

This document describes the implemented quote-service workflow behind `/simulate` and how the rest of the repo depends on it.

## Runtime contract

`/simulate` returns `QuoteResult { request_id, data, meta }`.

The request shape is defined by `AmountOutRequest`:

- `request_id`
- optional `auction_id`
- `token_in`
- `token_out`
- `amounts`

The per-pool response shape is `AmountOutResponse`:

- `pool`
- `pool_name`
- `pool_address`
- `amounts_out`
- `gas_used`
- `block_number`

The request-level metadata shape is `QuoteMeta`:

- `status`
- `result_quality`
- optional `partial_kind`
- `block_number`
- optional `vm_block_number`
- optional `rfq_update_timestamp`
- `matching_pools`
- `candidate_pools`
- optional `total_pools`
- optional `auction_id`
- optional `pool_results`
- optional `vm_unavailable`
- optional `rfq_unavailable`
- optional `failures`

Live enum values:

- `QuoteStatus`: `ready`, `warming_up`, `token_missing`, `no_liquidity`, `invalid_request`, `internal_error`
- `QuoteResultQuality`: `complete`, `partial`, `no_results`, `request_level_failure`
- `QuotePartialKind`: `amount_ladders`, `pool_coverage`, `mixed`
- `PoolOutcomeKind`: `partial_output`, `zero_output`, `timed_out`, `simulator_error`, `internal_error`

Contract invariants:

- `partial_success` is not part of the public contract
- `status=ready` can pair with `result_quality=complete`, `partial`, or selected `request_level_failure`
- `status=no_liquidity` pairs with `result_quality=no_results`
- `partial_kind` appears only when `result_quality=partial`
- `no_liquidity` never appears alongside usable quote data
- request-relevant fatal failures remain visible in `meta.failures` even when some usable amount outputs survive
- `block_number` is the native stream block; `vm_block_number` is the VM stream block when VM state is ready; `rfq_update_timestamp` is the current RFQ update timestamp/cursor when RFQ state is ready
- requested-amount order inside each pool row is part of the quote contract; `amounts_out[i]` matches the requested `amounts[i]`
- emitted pool rows preserve request order and request length for usable partial results; failed or timed-out requested amounts are serialized in place as `"0"` with matching `gas_used=0`
- `"0"` in `amounts_out` means that requested amount did not produce a usable quote for that pool; only positive outputs are usable quotes
- `data[]` contains only pools with at least one positive output across the requested amounts; fully-zero rows stay visible only through `meta.failures` and `meta.pool_results`
- `data[]` is stabilized for reproducibility, but row position is not a ranking signal; clients should not treat `data[0]` as "best pool"
- same-token and direct native/wrapped requests are rejected before readiness checks, token lookup, or candidate selection

## Request lifecycle

`POST /simulate` flows through these stages:

1. The handler logs the request and wraps quote computation in a request-level timeout guard.
2. The quote runner parses token addresses and rejects identical-token and direct native/wrapped pairs.
3. Native readiness is checked before quoting. If native state is still warming up or stale, the request exits as `warming_up + request_level_failure`.
4. Token metadata is loaded for both sides, then request amounts are parsed. Missing or timed-out token coverage exits as `token_missing + request_level_failure`; invalid amounts exit as `invalid_request + request_level_failure`.
5. Candidate pools are loaded from native state and, when available, VM state.
6. Unsupported ERC4626 candidates are filtered before execution.
7. Pool tasks run until they complete or the request-level timeout guard ends the computation.
8. Per-pool execution results are aggregated into `data`, `meta.failures`, and `meta.pool_results`.
9. The runner classifies the final exit as `complete`, `partial`, `no_results`, or `request_level_failure`.
10. Before returning, native and VM freshness are checked again. Results from a backend that lost freshness are removed, and the response classification is updated.

The current classification logic is:

- usable responses with no failures and no pool anomalies => `ready + complete`
- usable responses with failures or anomalies => `ready + partial`
- no usable responses plus liquidity-like failure classification => `no_liquidity + no_results`
- no usable responses plus non-liquidity degradation => `internal_error + request_level_failure`
- hard request gates keep their own top-level status and use `request_level_failure`

## Scenario matrix

| Scenario | `status` | `result_quality` | `partial_kind` | Notes |
| --- | --- | --- | --- | --- |
| Complete quote, all relevant requested amounts returned | `ready` | `complete` | omitted | `meta.failures` and `meta.pool_results` stay empty or omitted |
| Usable quotes returned, but at least one returned pool has partial requested-amount coverage | `ready` | `partial` | `amount_ladders` | emitted pool rows stay full-length and zero-fill failed amount positions; only positive outputs are usable quotes; `meta.pool_results` includes `partial_output` |
| Usable quotes returned, but some matching pools failed or timed out | `ready` | `partial` | `pool_coverage` | simulator anomalies remain visible |
| Both partial requested-amount coverage and incomplete pool coverage occurred | `ready` | `partial` | `mixed` | both partiality sources are present |
| Matching pools exist, but none produce a usable quote because liquidity is absent or exhausted | `no_liquidity` | `no_results` | omitted | includes cases where candidate rows would otherwise be fully zero; `meta.failures` explains the no-liquidity reason |
| No matching pools exist | `no_liquidity` | `no_results` | omitted | `meta.failures` includes `no_pools` |
| Request times out or otherwise degrades with no usable quote surviving | `internal_error` or `ready` | `request_level_failure` | omitted | depends on where the timeout or degradation was surfaced |
| Warm-up, token coverage, or invalid request problem | gate-specific status | `request_level_failure` | omitted | request never reaches a normal usable-quote path |

## Readiness and gating

`GET /status` is the liveness view and always returns HTTP `200`. `GET /ready` is the readiness contract used by scripts and deploy checks.

Service health and native readiness:

- `/ready` returns HTTP `200` with `status="ready"` when native traffic can be served
- `/ready` returns HTTP `503` while native state is warming, recovering, disconnected, or stale
- `backends.native.status="ready"` means bootstrap is complete, the subscription is live, and the complete applied native head matches a fresh RPC observation
- `backends.native.status="warming_up"` means the native subscriber, snapshot bootstrap, or state store is still loading
- `backends.native.status="stale"` means the complete applied native head cannot be confirmed against a fresh RPC observation

VM readiness:

- `backends.vm.status="disabled"` when VM pools are configured but turned off
- `backends.vm.status="warming_up"` while VM subscriber bootstrap or VM state is still loading
- `backends.vm.status="rebuilding"` during VM rebuilds
- `backends.vm.status="stale"` when the complete applied VM head cannot be confirmed against a fresh RPC observation
- `backends.vm.status="ready"` when VM bootstrap is complete, the subscription is live, and the complete applied VM head matches a fresh RPC observation

Both endpoints expose `observation_age_ms` on native and VM backend entries after the first
successful RPC observation. The age starts when that observation's request began, includes request
duration, and keeps increasing after failures or expiry. It comes from the same snapshot as
`chain_head_agreement`. The field is omitted before any successful observation and for RFQ.

The observation age limit is 5 seconds on Base and 15 seconds on Ethereum. It bounds how long the
last successful observation may justify serving during RPC uncertainty. A fresh observation that
disagrees with an applied block number or hash closes that backend's readiness immediately. The
broadcaster's separate 60-second recovery window keeps the existing Tycho stream alive to catch up;
it does not permit serving stale state. RPC uncertainty alone does not trigger a restart.

RFQ readiness:

- `backends.rfq.status="disabled"` when RFQ pools are configured but turned off
- `backends.rfq.status="warming_up"` while RFQ subscriber bootstrap or RFQ state is still loading
- `backends.rfq.status="stale"` when RFQ updates are past the readiness freshness window
- `backends.rfq.status="ready"` when RFQ state is usable
- `backends.rfq.update_timestamp` is the current Tycho RFQ update timestamp/cursor when RFQ state is ready; RFQ backend status does not expose `block_number`
- when RFQ is enabled, the simulator requires credentials for every configured RFQ provider at startup so `/encode` can request firm signed quotes

Quote-path implications:

- native readiness still gates the whole request
- VM readiness does not gate the whole request when native pools are still available
- when VM pools are enabled but not ready, the runner skips VM candidates and sets `meta.vm_unavailable=true`
- RFQ readiness does not gate the whole request when native pools are still available
- when RFQ pools are enabled but not ready, the runner skips RFQ candidates and sets `meta.rfq_unavailable=true`

## Candidate selection and execution

Candidate discovery:

- native candidates come from the native state store
- VM candidates come from the VM state store only when VM state is ready
- RFQ candidates come from the RFQ state store only when RFQ state is ready

Execution rules:

- per-request quote execution uses the request deadline
- usable outputs are preserved even when some pools degrade

Partiality sources:

- `amount_ladders`: a returned pool produced at least one usable quote, but one or more requested amounts failed or timed out
- `pool_coverage`: one or more matching pools timed out or failed before returning a usable quote
- `mixed`: both conditions happened in one response

Advisory `get_limits` signals do not define success on their own. `get_amount_out` remains the source of truth for quote success or failure.

Practical client rule:

- trust requested-amount alignment inside each returned row
- choose pools by the amount position you care about, not by row position
- treat `"0"` as "no usable quote for that requested amount," not as a usable quote
- a row can still be usable overall when some amount positions are `"0"`, but a fully-zero row is not usable and should not appear in `data[]`

## Failures and pool outcomes

`meta.failures` is the request-level explanation layer.

Important failure kinds include:

- `warm_up`
- `stale_native_state`
- `token_validation`
- `token_coverage`
- `timeout`
- `overflow`
- `simulator`
- `no_pools`
- `inconsistent_result`
- `internal`
- `invalid_request`

Before returning `/simulate` results, the service rechecks native and VM chain freshness
separately. An expired observation, head change, or loss of backend readiness removes the affected
backend's rows, even if state recovers before the request finishes. Removed native rows are reported
through `stale_native_state`; removed VM rows set `vm_unavailable=true` and add a `simulator` failure.
RFQ rows do not use this chain-head freshness check.

The separate native pool identity check belongs to `/encode`: it compares the route's own pools
against current published state and may retry once if those pools changed. Updating unrelated pools
does not trigger that retry, but native and VM chain freshness must still hold for the encoded route.

`meta.pool_results` is the per-pool anomaly layer.

Use it to understand:

- which pools returned partial requested-amount coverage while still yielding at least one usable quote
- which pools returned zero output across all requested amounts and were therefore filtered out of `data[]`
- which pools timed out or failed inside the simulator

The two layers are intentionally redundant in some degraded cases. Material request-visible failures should not be visible only through per-pool anomaly rows.

## Timeouts

Handler-level timeout behavior:

- the `/simulate` handler wraps the quote runner in a request-level timeout guard
- when that guard fires, the response is still `200 OK`
- the payload is contract-valid and uses `status=ready`, `result_quality=request_level_failure`, and a timeout failure entry

Router-level timeout behavior:

- `/simulate` also sits behind a router timeout layer with extra headroom
- when the router boundary fires, the endpoint still returns `200 OK` with `result_quality=request_level_failure`
- logs mark those cases with `scope="router_timeout"`

`/encode` is intentionally different:

- router timeouts return `408 Request Timeout`
- the payload shape is `{ error }`, with `requestId` included when available

## Observability contract

The log and metric surfaces use the live quote contract fields, not deprecated status shortcuts.

Important log fields:

- `quote_status`
- `quote_result_quality`
- `partial_kind`
- `failures`
- `pool_results`
- request identifiers, token pair, latency, and best first-amount completion-log fields on completion logs

Observability note:

- API row order and completion-log `top_*` fields are different signals
- `data[]` is deterministic presentation output, not solver ranking
- completion-log `top_*` fields summarize the strongest quote seen for the first requested amount in the response set; they are useful for ops triage, but they are not a general "best pool overall" answer

Operational guidance:

- group dashboards by `quote_status` for hard request-state monitoring
- group by `quote_result_quality` for completeness and degradation monitoring
- use `partial_kind` to split partial requested-amount coverage from incomplete pool coverage
- treat `simulate-successes` style queries as `ready + (complete|partial)` only

`/encode` observability:

- `/encode` still does not expose `QuoteMeta`; its API contract stays success/error oriented
- the handler emits one structured completion event per request instead of relying on per-hop `info` logs
- summary logs include route shape fields such as `segments`, `hops`, `swaps`, `route_protocols`, `swap_kind`, request amounts, and whether the route uses VM pools
- failure logs also include stable `encode_error_kind` and `failure_stage` fields
- current `failure_stage` values are `validation`, `readiness`, `normalization`, `resimulation`, `min_amount_out_guard`, `encoding`, `interaction_build`, `internal`, `handler_timeout`, and `router_timeout`
- per-segment, per-hop, and per-swap resimulation traces are emitted at `debug`, not `info`

## Integrations

`/encode` integration:

- `/encode` does not expose `QuoteMeta`
- `/encode` clients and the local analyzer depend on `/simulate` to find candidate pools
- `/encode` supports RFQ pools when RFQ state is ready; it rebuilds the selected request-scoped RFQ client with simulator credentials because broadcaster serialization omits them
- pool selection should stay strict: `ready + complete|partial` is usable, `request_level_failure` and `no_results` are not
- clients should filter returned rows explicitly for the amount position they need instead of relying on `data[0]`

Repo analysis workflow:

- `cargo run -p apps --bin sim-analysis -- ...` is intentionally reporting-first and summarizes healthy, degraded, and errored outcomes instead of acting like a strict branch gate
- the analyzer still evaluates `result_quality`, `partial_kind`, `meta.failures`, and protocol visibility rather than looking only at HTTP status or `meta.status`
- saved artifacts under `logs/simulation-reports/` make it easier to compare local runs and investigate odd protocol-specific behavior without hard-coding business assertions

CloudWatch and query presets:

- completion logs already emit `quote_status`, `quote_result_quality`, and `partial_kind`
- preset filters distinguish usable successes from degraded but contract-valid responses
- query docs and presets should stay aligned with this contract when log fields evolve

## Observed chain tip catch-up episodes

The broadcaster and simulator inspect existing in-memory heads every 100 ms. This
adds no RPC requests and does not log each check. Each service keeps native and VM
series separate; disabled VM and RFQ do not produce series. The broadcaster compares
**published** complete heads with its own RPC observer, only while the publisher is
active. The simulator compares **applied** complete heads with its own observer.
Simulator readiness is context, so a head can match while another readiness gate
is closed. A broadcaster match does not prove that a subscriber has applied it.

An observed catch-up episode begins at the first positive block gap and completes
only when number **and hash** match again. For `0, 1, 50, 20, 0`, one completion
records a peak of 50 blocks and the monotonic duration from the first `1` to the
last `0`. Decreasing gaps stay in the same episode. Detection timestamps and peaks
are sampled observations, not exact mutation times or the maximum distance from
the live chain. Short episodes and peaks between checks or RPC polls can be missed.

A first observation already behind has `start_observed=false`; its elapsed duration
is only a lower bound. Expired RPC observations, incomplete local state, hash
conflicts, an observer behind local state, publisher ineligibility, and publisher
lock contention are unclassifiable, never healthy zero-gap samples. An active
episode stays open through these states with `continuous_observations=false` and
retains the reasons. A later matching observation completes that censored record,
but cannot prove the backend stayed behind throughout the unknown interval.
Spacing **greater than 200 ms** also censors affected episodes and invalidates the
previous matching boundary. Timers skip missed ticks rather than inventing checks.

Structured JSON fields live under `fields`. Schema version 1 emits:

| `fields.event` | Meaning |
| --- | --- |
| `chain_tip_episode_started` | First positive gap, including whether the starting boundary was observed |
| `chain_tip_episode_completed` | One detection-to-match interval with `duration_ms` and `peak_gap_blocks` |
| `chain_tip_observation_summary` | Coverage since the previous summary, every 15 seconds; active episode checkpoint if present |
| `chain_tip_episode_unfinished` | Best-effort broadcaster graceful shutdown record; never a completion |

Every record identifies `chain_id`, `service`, `backend`, `head_stage`, and
`tracker_run_id`. Episodes also have `episode_sequence`,
`episode_started_at_unix_ms`, initial and peak local/observed numbers and hashes,
`observations`, and `max_observation_interval_ms`. The current `local_*` and
`observed_*` fields on a completion identify the matching heads. Checkpoints and
unfinished records use `elapsed_ms`, not `duration_ms`. Unknown values are omitted.
`uncertainty_reasons` and `coverage_reasons` are diagnostic strings containing the
observed reason labels. Use the boolean fields to select the complete cohort.

Trackers survive internal feed/subscription restarts. They do not persist across
processes. The simulator has no graceful supervisor shutdown hook; a start or
checkpoint without a completion remains unfinished. Abrupt broadcaster termination
has the same rule. Keep CloudWatch log stream identity alongside the tracker run
and sequence, and never stitch different processes into one episode.

### Query episode distributions

Select the explicit UTC report window in CloudWatch Logs Insights and the relevant
log groups. In both queries below, replace `WINDOW_START_UNIX_MS` with that window's
start in Unix milliseconds. The query time range filters completion timestamps;
the additional start filter prevents episodes crossing the left boundary from
entering the complete cohort. Grouping below counts backend episodes across all
selected replicas, not distinct chain incidents. Add `@logStream` for task-level
results. Keep chains separate too.

Peak block-gap distribution, with one row per observed maximum:

```text
fields fields.chain_id as chain_id, fields.service as service,
       fields.backend as backend, fields.head_stage as head_stage,
       fields.peak_gap_blocks as peak_gap_blocks
| filter fields.schema_version = 1
    and fields.event = "chain_tip_episode_completed"
    and fields.start_observed = true
    and fields.continuous_observations = true
    and fields.episode_started_at_unix_ms >= WINDOW_START_UNIX_MS
| stats count(*) as episodes by chain_id, service, backend, head_stage, peak_gap_blocks
| sort peak_gap_blocks asc
```

Elapsed catch-up distribution and peak quantiles for the same cohort:

```text
fields fields.chain_id as chain_id, fields.service as service,
       fields.backend as backend, fields.head_stage as head_stage,
       fields.peak_gap_blocks as peak_gap_blocks, fields.duration_ms as duration_ms
| filter fields.schema_version = 1
    and fields.event = "chain_tip_episode_completed"
    and fields.start_observed = true
    and fields.continuous_observations = true
    and fields.episode_started_at_unix_ms >= WINDOW_START_UNIX_MS
| stats count(*) as episodes,
        pct(peak_gap_blocks, 50) as peak_blocks_p50,
        pct(peak_gap_blocks, 90) as peak_blocks_p90,
        pct(peak_gap_blocks, 99) as peak_blocks_p99,
        pct(duration_ms, 50) as catch_up_ms_p50,
        pct(duration_ms, 90) as catch_up_ms_p90,
        pct(duration_ms, 99) as catch_up_ms_p99
  by chain_id, service, backend, head_stage
```

For a final 12-hour report, export all four event types with explicit UTC bounds
and retain all fields and `@logStream`. Check query completion and result limits,
splitting bounded windows as needed. Deduplicate lifecycle events by log stream,
tracker run, episode sequence, and event kind. For periodic summaries include
`observed_at_unix_ms` in the key so multiple checkpoints are retained. Compute
percentiles from individual completed episodes using a stated percentile convention,
never percentiles of time-bin percentiles.

Report left-boundary episodes, censored completions, and right-boundary or otherwise
unfinished episodes separately. Active checkpoints can reveal an episode whose
start predates the window. Fetch adjacent summaries around the boundaries to
identify partial coverage intervals; do not assign an entire straddling summary to
the report window. Fetch preceding starts when needed for drill-down. An unmatched
start at the right edge means unfinished **as of that edge**, even if it later
completes. An abrupt process loss can hide a start before the next summary, so
absence of an unfinished record is not evidence of completeness.

Use `matching_observations / (matching_observations + behind_observations)` from
summaries for the caught-up share of classified checks. Report
`unclassifiable_observations`, `hash_mismatch_observations`, `delayed_observations`,
`summary_max_observation_interval_ms`, interval bounds and expected task coverage
alongside it. This is approximately time-weighted under regular sampling, not an
exact percentage of elapsed time. A zero denominator is unknown. Missing summaries,
process restarts, and unknown intervals cannot be counted healthy; empty episode
results alone never establish no lag.

### Output volume

Steady state emits four summaries per minute per enabled backend. Each episode
adds one start and one completion (or a best-effort unfinished record). At 10 checks
per second, alternating match/gap observations produce at most five completed
episodes and ten lifecycle records per second per backend, plus summaries. Ordinary
one-block episodes are not filtered or rate-limited. The focused telemetry tests
measure actual JSON bytes for a controlled steady minute and this alternating
upper-bound sequence. A local run produced 4 records / 3,836 bytes for 601 steady
checks, and 604 records / 831,741 bytes for 601 alternating checks (300 episodes),
per backend. This is roughly 14 KB/s at the alternating upper bound. Production
bytes depend on hashes, reasons and task identity.
