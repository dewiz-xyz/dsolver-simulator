<p align="center">
  <img src=".github/assets/dsolver-logo.png" alt="DSolver Simulator logo" width="180">
</p>

<h1 align="center">DSolver Simulator</h1>

<p align="center">
  High-throughput DeFi swap simulation and route encoding on live Tycho-backed state.
</p>

<p align="center">
  <a href="https://github.com/dewiz-xyz/dsolver-simulator/actions/workflows/ci.yml">
    <img src="https://github.com/dewiz-xyz/dsolver-simulator/actions/workflows/ci.yml/badge.svg" alt="CI status">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/github/license/dewiz-xyz/dsolver-simulator" alt="MIT license">
  </a>
  <a href="https://github.com/dewiz-xyz/dsolver-simulator/stargazers">
    <img src="https://img.shields.io/github/stars/dewiz-xyz/dsolver-simulator?logo=github" alt="GitHub stars">
  </a>
  <a href="https://github.com/dewiz-xyz/dsolver-simulator/network/members">
    <img src="https://img.shields.io/github/forks/dewiz-xyz/dsolver-simulator?logo=github" alt="GitHub forks">
  </a>
  <a href="https://github.com/dewiz-xyz/dsolver-simulator/issues">
    <img src="https://img.shields.io/github/issues/dewiz-xyz/dsolver-simulator?logo=github" alt="GitHub issues">
  </a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Rust-1.91%2B-000000?logo=rust" alt="Rust 1.91+">
  <img src="https://img.shields.io/badge/Axum-0.7-111827" alt="Axum 0.7">
  <img src="https://img.shields.io/badge/Chains-Ethereum%20%7C%20Base-2563eb" alt="Supported chains: Ethereum and Base">
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> ·
  <a href="#api-surface">API</a> ·
  <a href="#verification">Verification</a> ·
  <a href="#docs-map">Docs</a>
</p>

DSolver Simulator is a Rust service for DeFi quote simulation and route encoding. It subscribes to the internal Tycho broadcaster for native and VM state, maintains an in-memory view of pool state, and exposes fast HTTP endpoints for quoting, route settlement encoding, and readiness checks.

## What It Is

- Live-state simulation service for swap and routing workloads.
- Built on Axum and Tokio, with `tycho-simulation` and `tycho-execution` handling pricing and route execution details.
- Designed for solver and routing integrations that need both quote coverage and deterministic route encoding behavior.

## Why Teams Use It

- Continuous ingestion of native and optional VM-backed pool state through the internal broadcaster service.
- Structured `/simulate` responses that distinguish usable quotes, partial coverage, warmup states, and request-level failures.
- `/encode` route resimulation before calldata generation, so settlement interactions are built from the same runtime view used for quoting.
- Reporting-first local analysis tooling for readiness, latency, sampled evidence, and quick regression investigation.

## At A Glance

| Category | Details |
| --- | --- |
| Language | Rust 2021 |
| Runtime | Axum + Tokio |
| Binary | `dsolver-simulator-service` |
| Endpoints | `GET /status`, `POST /simulate`, `POST /encode` |
| Supported chains | Defined in `simulator-manifest.toml` |
| Required inputs | `TYCHO_API_KEY`, `CHAIN_ID`, `TYCHO_BROADCASTER_WS_URL` |
| Common optional inputs | `RPC_URL`, `ENABLE_VM_POOLS`, `ENABLE_RFQ_POOLS`, `HOST`, `PORT` |
| License | MIT |

## Quick Start

```bash
cp .env.example .env
PORT=3001 cargo run -p apps --bin dsolver-tycho-broadcaster-service --release
cargo run -p apps --bin dsolver-simulator-service --release
```

Use the explicit `-p ... --bin ...` form for local runs and builds so it's always clear which
workspace binary you're targeting.

Required runtime inputs:

- `TYCHO_API_KEY` for Tycho access
- `CHAIN_ID` for chain selection from `simulator-manifest.toml`
- `TYCHO_BROADCASTER_WS_URL` pointing at the broadcaster websocket, for example `ws://127.0.0.1:3001/ws`

Common optional inputs:

- `RPC_URL` to enable on-chain helpers that need JSON-RPC access, such as ERC4626 deposits
- `ENABLE_VM_POOLS` to enable or disable VM-backed pool feeds
- `ENABLE_RFQ_POOLS` to enable or disable RFQ-backed pool feeds (defaults to `false`)
- `BEBOP_USER`, `BEBOP_KEY`, `HASHFLOW_USER`, `HASHFLOW_KEY`, `LIQUORICE_USER`, and `LIQUORICE_KEY` only when `ENABLE_RFQ_POOLS=true` for chains that enable those RFQ providers
- `HOST` and `PORT` to change the bind address
- timeout and stream-health knobs from `crates/runtime/src/config/mod.rs`

`crates/runtime/src/config/mod.rs` is the authoritative source for runtime defaults. `.env.example`
is an example setup, not the source of truth for every default.

## API Surface

- `POST /simulate` returns per-pool quotes across the requested amounts plus `meta` describing quote completeness, failures, and readiness-adjacent request outcomes.
- `POST /encode` accepts a client-provided route, re-simulates the swaps internally, and returns ordered settlement `interactions[]`.
- `GET /status` reports overall service health plus nested backend readiness, block progress, and VM or RFQ rebuild or restart context for pollers and deploy scripts.

`/encode` keeps its current HTTP control flow, but the server emits one structured completion log per request with route shape, protocol summary, and failure-stage fields. Detailed resimulation traces stay available at `debug`.

Detailed integration docs:

- [docs/simulate_example.md](docs/simulate_example.md) for `/simulate` request and response shape
- [docs/encode_example.md](docs/encode_example.md) for `/encode` request and response shape

## Quote Outcome Semantics

`/simulate` uses a structured response contract. Clients should not treat HTTP `200 OK` as quote success on its own.

`QuoteStatus` is the top-level request state:

- `ready`
- `warming_up`
- `token_missing`
- `no_liquidity`
- `invalid_request`
- `internal_error`

`QuoteResultQuality` describes result completeness:

- `complete`
- `partial`
- `no_results`
- `request_level_failure`

`meta.partial_kind` appears only when `result_quality=partial`:

- `amount_ladders`
- `pool_coverage`
- `mixed`

Summary matrix:

| Situation | `meta.status` | `meta.result_quality` | Usable For Quoting |
| --- | --- | --- | --- |
| Usable quotes with full coverage | `ready` | `complete` | yes |
| Usable quotes with partial requested-amount coverage or incomplete pool coverage | `ready` | `partial` | yes |
| No usable quote because liquidity is absent or exhausted | `no_liquidity` | `no_results` | no |
| Request completed with valid payload but request-level failure details | `ready` or `internal_error` | `request_level_failure` | no |
| Warm-up, token coverage, or request validation problem | `warming_up`, `token_missing`, or `invalid_request` | `request_level_failure` | no |

`/simulate` results are usable for quoting when:

- `meta.status=ready`
- `meta.result_quality=complete` or `meta.result_quality=partial`

`meta.failures` is the request-level failure summary. It captures timeouts, cancellations, token coverage problems, no-pools reasons, and request-relevant simulation failures.

`meta.pool_results` is the per-pool outcome summary. It captures pool-local outcomes such as `partial_output`, `zero_output`, `timed_out`, `simulator_error`, and `internal_error`.

For partial results, emitted pool rows keep the original request order and request length in `amounts_out`. Requested amounts that fail are serialized in place as `"0"`, and the matching `gas_used` entries are `0`.

Treat `"0"` in `amounts_out` as "this requested amount did not produce a usable quote for that pool," not as a real quote. `data[]` only contains pools that produced at least one positive output across the requested amounts. Fully-zero rows are filtered out of `data[]` and stay visible only through `meta.failures` and `meta.pool_results`.

`data[]` order is deterministic, but it is not a best-to-worst ranking. Clients should rely on requested-amount alignment within each row and evaluate returned pools explicitly instead of inferring meaning from `data[0]`.

`meta.vm_unavailable=true` means VM pools were skipped because VM state was not ready. When usable native quotes still exist, that typically surfaces as a `ready + partial` response rather than a hard non-ready status.

## Readiness And Timeouts

`GET /status` is the readiness source of truth:

- `200 OK` with `status="ready"` when the service is healthy
- `503 Service Unavailable` with `status="warming_up"` while native readiness is not ready
- `backends.native.status="ready"` when the broadcaster subscription is live, bootstrap is complete, and native state is ready and not stale
- `backends.native.status="warming_up"` while initial native state is still loading
- `backends.native.status="stale"` when native updates are past the readiness freshness window

`backends.vm.status` and `backends.rfq.status` are one of:

- `disabled`
- `warming_up`
- `rebuilding`
- `stale`
- `ready`

Timeout behavior differs by endpoint:

- `/simulate` request-guard timeouts return `200 OK` with a contract-valid payload whose `meta.status=ready`, `meta.result_quality=request_level_failure`, and `meta.failures` includes a `timeout`
- `/simulate` router-boundary timeouts also return `200 OK` with `result_quality=request_level_failure`
- `/encode` router-boundary timeouts return `408 Request Timeout` with `{ "error": "..." }`, plus `requestId` when it is available
- `/status` is not wrapped in the router timeout layer

For ops, `/encode` timeout and failure logs include stable `encode_error_kind` and `failure_stage` fields so CloudWatch queries can separate validation, readiness, normalization, resimulation, encoding, `handler_timeout`, and `router_timeout` paths.

If you are integrating against `/simulate`, inspect `meta` on every successful HTTP response. If you are integrating against `/encode`, normal HTTP success and error handling is still the right control flow.

## Verification

CI-equivalent commands:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace
cargo build -p apps --bin dsolver-simulator-service --release
cargo build -p apps --bin dsolver-tycho-broadcaster-service --release
```

Local analysis harness:

```bash
cargo run -p apps --bin sim-analysis -- --chain-id 1 --stop
cargo run -p apps --bin sim-analysis -- --chain-id 8453 --stop
```

Container builds:

```bash
docker build -f Dockerfile.simulator-service -t dsolver-simulator-service .
docker build -f Dockerfile.broadcaster-service -t dsolver-tycho-broadcaster-service .
```

Useful helpers:

- `scripts/start_server.sh` to start the server with repo-local PID and log files
- `scripts/wait_ready.sh` to poll `/status` and enforce chain, native, VM, and RFQ readiness expectations; native readiness remains the default gate
- `scripts/stop_server.sh` to stop a server started by the repo helper
- `cargo run -p apps --bin sim-analysis -- ...` to generate a JSON and markdown local behavior report

The analyzer is intentionally reporting-first. It exercises representative `/simulate` and `/encode` flows, plus latency and light stress probes, then writes artifacts under `logs/simulation-reports/` so agents can inspect anomalies, compare against previous local runs, and decide what matters instead of relying on a rigid pass or fail harness.

## Docs Map

- [docs/simulate_example.md](docs/simulate_example.md): `/simulate` API examples and integration notes
- [docs/encode_example.md](docs/encode_example.md): `/encode` API examples and route-shape notes
- [docs/quote_service.md](docs/quote_service.md): maintainer deep dive for quote lifecycle, classification, observability, and integrations
- [STRESS_TEST_README.md](STRESS_TEST_README.md): local simulation analysis workflow and report artifacts
- `skills/simulation-service-analysis/SKILL.md`: repo-local analysis skill
- `skills/tycho-cloudwatch-logs/SKILL.md`: CloudWatch log triage workflow

## License

This project is licensed under the [MIT License](LICENSE).
