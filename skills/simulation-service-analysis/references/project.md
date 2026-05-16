# DSolver Simulator (repo context)

## What this service does
- DSolver Simulator is a fast simulation API for DeFi swaps and routing, built on Tycho state.
- Rust Axum service that ingests Tycho protocol streams, keeps an in-memory pool state, and exposes HTTP endpoints for quote simulation and route encoding.
- Main binary: `dsolver-simulator-service` (see `src/main.rs`).

## Local run
- Create `.env` from `.env.example` and set `TYCHO_API_KEY` (required).
- RFQ feeds default to off. For Ethereum runs with `ENABLE_RFQ_POOLS=true`, also set `BEBOP_USER`, `BEBOP_KEY`, `HASHFLOW_USER`, and `HASHFLOW_KEY`.
- Set `CHAIN_ID` (`1` for Ethereum, `8453` for Base) or pass `--chain-id` to the analyzer.
- Tycho health checks expect `Authorization: <TYCHO_API_KEY>` (no `Bearer` prefix).
- Start the server:
  ```bash
  cargo run --release
  ```
- `cargo run --release` resolves to `dsolver-simulator-service` via Cargo `default-run`.
- Default bind: `127.0.0.1:3000` (override with `HOST`/`PORT`).

## Readiness
- `GET /status` returns:
  - `200 OK` with `{ "status": "ready", "native_status": "ready", "chain_id": <u64>, "block": <u64>, "pools": <usize>, ... }` when the service is healthy
  - `503 Service Unavailable` with `{ "status": "warming_up", "native_status": "...", ... }` while no backend is ready yet
- `native_status` carries native readiness; `vm_status` and `rfq_status` keep backend-specific readiness separate.
- Cold starts can take several minutes (3–5+ mins; VM or RFQ pools can take up to roughly 10 minutes on a fresh warmup).
- `scripts/wait_ready.sh --expect-chain-id <id>` is still the manual guard if you want the native readiness gate directly.
- When VM pools matter, prefer `scripts/wait_ready.sh --url http://localhost:3000/status --expect-chain-id <id> --require-vm-ready --timeout 600`.
- When RFQ pools matter on Ethereum, prefer `scripts/wait_ready.sh --url http://localhost:3000/status --expect-chain-id 1 --require-rfq-ready --timeout 600`.
- When both VM and RFQ backends matter on Ethereum, prefer `scripts/wait_ready.sh --url http://localhost:3000/status --expect-chain-id 1 --require-vm-ready --require-rfq-ready --timeout 600`.

## Local analysis workflow (recommended)
- Run the reporting-first analyzer:
  - `cargo run --bin sim-analysis -- --chain-id 1 --stop`
  - `cargo run --bin sim-analysis -- --chain-id 8453 --stop`
- The analyzer starts or reuses the local server, waits for service health, confirms native readiness first, auto-checks VM and RFQ backends when they are enabled, runs representative `/simulate` and `/encode` probes, executes latency and light stress sweeps, then writes artifacts under `logs/simulation-reports/`.
- On Base with RFQ enabled and ready, the encode probes include a Bebop partial-fill diagnostic that checks router calldata for the packed `originalFilledTakerAmount` token-in invariant.
- Default output root:
  - `logs/simulation-reports/<chain-id>/balanced/<timestamp>/`
- Main artifacts:
  - `summary.md` for the narrative overview
  - `report.json` for exact metrics and scenario breakdowns
  - `evidence/` for sampled request/response bodies, readiness snapshots, and log excerpts
- RFQ-enabled runs should also surface RFQ readiness, RFQ-visibility findings, and any Bebop encode-inspection findings in `summary.md` and `report.json`.
- Baseline comparison is meant to be flexible. Use the latest saved run when it helps, disable it with `--baseline none` when you want a clean one-off read.
- The analyzer does not act like a branch gate. It reports healthy, degraded, and errored behavior so the agent can investigate.

## Useful commands
- Format: `cargo fmt`
- Lint: `cargo clippy --all-targets --all-features -- -D warnings`
- Test: `cargo nextest run`
- Build: `cargo build --release`
- Manual lifecycle:
  - `scripts/start_server.sh --repo . --chain-id 1`
  - `scripts/wait_ready.sh --url http://localhost:3000/status --expect-chain-id 1`
  - `scripts/wait_ready.sh --url http://localhost:3000/status --expect-chain-id 1 --require-rfq-ready --timeout 600`
  - `scripts/stop_server.sh --repo .`

## API docs
- [docs/simulate_example.md](../../../docs/simulate_example.md)
- [docs/encode_example.md](../../../docs/encode_example.md)
- [docs/quote_service.md](../../../docs/quote_service.md)
