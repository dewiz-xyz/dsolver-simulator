# DSolver Simulator (repo context)

## What this service does
- DSolver Simulator is a fast simulation API for DeFi swaps and routing, built on Tycho state.
- Rust Axum service that consumes a Tycho broadcaster stream, keeps an in-memory pool state, and exposes HTTP endpoints for quote simulation and route encoding.
- Main local binaries: `dsolver-tycho-broadcaster-service` feeds the local stream, and `dsolver-simulator-service` exposes `/status`, `/simulate`, and `/encode`.

## Local run
- Create `.env` from `.env.example` and set `TYCHO_API_KEY` (required).
- Keep `TYCHO_BROADCASTER_URL` pointed at the broadcaster HTTP base URL and configure `BROADCASTER_REDIS_URL` plus `BROADCASTER_REDIS_STREAM_KEY` for Redis deltas. The local default lets the lifecycle helper start the broadcaster on port `3001` before the simulator.
- RFQ feeds default to off. For RFQ analysis, set `ENABLE_RFQ_POOLS=true`. Ethereum and Base currently need the Bebop and Hashflow credential pairs; Liquorice credentials are only needed after `rfq:liquorice` is added to an active chain profile.
- Set `CHAIN_ID` (`1` for Ethereum, `8453` for Base) or pass `--chain-id` to the analyzer.
- Tycho health checks expect `Authorization: <TYCHO_API_KEY>` (no `Bearer` prefix).
- Start the local stack:
  ```bash
  scripts/start_server.sh --repo . --chain-id 1
  ```
- Default bind: `127.0.0.1:3000` (override with `HOST`/`PORT`).

## Readiness
- `GET /status` returns:
  - `200 OK` with `{ "status": "ready", "chain_id": <u64>, "backends": { "native": { "status": "ready", ... } } }` when the service is healthy
  - `503 Service Unavailable` with nested backend status details while native readiness is not ready.
- `backends.native.status` carries native readiness; `backends.vm.status` and `backends.rfq.status` keep backend-specific readiness separate when those backends are configured.
- Cold starts can take several minutes (3–5+ mins; VM or RFQ pools can take up to roughly 10 minutes on a fresh warmup).
- `scripts/wait_ready.sh --expect-chain-id <id>` is still the manual guard if you want the native readiness gate directly.
- When VM pools matter, prefer `scripts/wait_ready.sh --url http://localhost:3000/status --expect-chain-id <id> --require-vm-ready --timeout 600`.
- When RFQ pools matter, prefer `scripts/wait_ready.sh --url http://localhost:3000/status --expect-chain-id <id> --require-rfq-ready --timeout 600`.
- When both VM and RFQ backends matter on Ethereum, prefer `scripts/wait_ready.sh --url http://localhost:3000/status --expect-chain-id 1 --require-vm-ready --require-rfq-ready --timeout 600`.

## Local analysis workflow (recommended)
- Run the reporting-first analyzer:
  - `cargo run -p apps --bin sim-analysis -- --chain-id 1 --stop`
  - `cargo run -p apps --bin sim-analysis -- --chain-id 8453 --stop`
- The analyzer starts or reuses the local broadcaster plus simulator stack, waits for service health, confirms native readiness first, auto-checks VM and RFQ backends when they are enabled, runs representative `/simulate` probes and the balanced `/encode` route matrix, executes latency and light stress sweeps, then writes artifacts under `logs/simulation-reports/`.
- The `/encode` matrix uses live `/simulate` prep hops to assemble 3 SimpleSwap routes, 3 MultiSwap routes, and 2 MegaSwap routes per supported chain. Prep hops are reported separately as `encode-prep` scenarios.
- Default output root:
  - `logs/simulation-reports/<chain-id>/balanced/<timestamp>/`
- Main artifacts:
  - `summary.md` for the narrative overview
  - `report.json` for exact metrics and scenario breakdowns
  - `evidence/` for sampled request/response bodies, readiness snapshots, and simulator/broadcaster log excerpts
- RFQ-enabled Ethereum runs should also surface RFQ readiness and any RFQ-visibility findings in `summary.md` and `report.json`.
- Baseline comparison is meant to be flexible. Use the latest saved run when it helps, disable it with `--baseline none` when you want a clean one-off read.
- The analyzer does not act like a branch gate. It reports healthy, degraded, and errored behavior so the agent can investigate.

## Useful commands
- Format: `cargo fmt --all`
- Lint: `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- Test: `cargo nextest run --workspace`
- Build simulator: `cargo build -p apps --bin dsolver-simulator-service --release`
- Build broadcaster: `cargo build -p apps --bin dsolver-tycho-broadcaster-service --release`
- Manual lifecycle:
  - `scripts/start_server.sh --repo . --chain-id 1`
  - `scripts/wait_ready.sh --url http://localhost:3000/status --expect-chain-id 1`
  - `scripts/wait_ready.sh --url http://localhost:3000/status --expect-chain-id 1 --require-rfq-ready --timeout 600`
  - `scripts/stop_server.sh --repo .`

## API docs
- [docs/simulate_example.md](../../../docs/simulate_example.md)
- [docs/encode_example.md](../../../docs/encode_example.md)
- [docs/quote_service.md](../../../docs/quote_service.md)
