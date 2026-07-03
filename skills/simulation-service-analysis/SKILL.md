---
name: simulation-service-analysis
description: Analyze the local DSolver Simulator service in this repo with a reporting-first Rust CLI. Use when you want to start or reuse the local broadcaster plus simulator stack, wait for /status health and native readiness, exercise representative /simulate and /encode flows, run latency and light stress probes, save standardized JSON/markdown reports, compare against previous local runs, and investigate anomalies without relying on strict pass/fail business assertions.
metadata:
  short-description: DSolver Simulator local analysis
---

# Simulation Service Analysis

## Quick start

1. Confirm the repo root (expect `Cargo.toml` and `crates/`).
2. Ensure `.env` exists and contains `TYCHO_API_KEY`, `TYCHO_BROADCASTER_URL`, `BROADCASTER_REDIS_URL`, and `BROADCASTER_REDIS_STREAM_KEY`.
   The default loopback broadcaster URL lets the lifecycle helper start the broadcaster before the simulator, while Redis carries deltas after each HTTP snapshot replay boundary.
   RFQ feeds default to off. For RFQ analysis, set `ENABLE_RFQ_POOLS=true`. Ethereum and Base currently need the Bebop and Hashflow credential pairs; Liquorice credentials are only needed after `rfq:liquorice` is added to an active chain profile.
3. Pick a chain context for the run (`--chain-id 1` for Ethereum, `--chain-id 8453` for Base).
4. Run the analyzer:
   ```bash
   cargo run -p apps --bin sim-analysis -- --chain-id 1 --stop
   ```
5. For a Redis replay self-check, keep the services running, verify the replay path, then stop them:
   ```bash
   cargo run -p apps --bin sim-analysis -- --chain-id 1 --baseline none
   scripts/verify_broadcaster_redis.sh --repo .
   scripts/stop_server.sh --repo .
   ```
6. Read:
   - `logs/simulation-reports/<chain-id>/balanced/<timestamp>/summary.md`
   - `logs/simulation-reports/<chain-id>/balanced/<timestamp>/report.json`

## What the analyzer does

- Reuses the existing local simulator if it is already responding, otherwise starts the local broadcaster plus simulator stack with the repo lifecycle scripts.
- Starts helper-managed Redis first when `BROADCASTER_REDIS_URL` is loopback `redis://`, then starts `dsolver-tycho-broadcaster-service` when `TYCHO_BROADCASTER_URL` points at local loopback, then starts `dsolver-simulator-service`; non-local broadcaster or Redis URLs are treated as externally managed.
- Waits for `/status` service health, then confirms native readiness first and adds VM and RFQ readiness checks when those pool backends are enabled.
- Fresh VM-pool or RFQ warmups can take much longer than native readiness. Budget up to 10 minutes before treating either backend as stuck.
- Runs a balanced `/simulate` sweep across representative pairs.
- Builds the balanced `/encode` route matrix from live `/simulate` prep hops, covering 3 SimpleSwap routes, 3 MultiSwap routes, and 2 MegaSwap routes per supported chain.
- On Base with RFQ enabled and ready, runs a Bebop partial-fill `/encode` diagnostic and inspects router calldata for the packed `originalFilledTakerAmount` token-in invariant.
- Runs latency and light stress sweeps.
- Saves sampled request/response artifacts plus simulator and broadcaster log excerpts.
- Optionally compares the current run against the latest compatible saved report.
- Top-level `/status.status` is service health; nested `backends.*.status` carries backend readiness.

## Behavior model

- Non-zero exit codes are reserved for harness/runtime failures such as startup failures, readiness timeouts, transport failures that prevent analysis, or report-writing failures.
- Degraded protocol behavior, request-level failures, odd pool visibility, and latency regressions are reported as findings, not hard failures.
- The analyzer is meant to help local reviewers investigate behavior, not to decide prod-readiness by itself.

## Useful commands

Base run:
```bash
cargo run -p apps --bin sim-analysis -- --chain-id 8453 --stop
```

Keep the helper-managed local services running:
```bash
cargo run -p apps --bin sim-analysis -- --chain-id 1
```

Disable baseline comparison:
```bash
cargo run -p apps --bin sim-analysis -- --chain-id 1 --baseline none --stop
```

Verify the current broadcaster HTTP snapshot plus Redis delta replay path while services are still running:
```bash
scripts/verify_broadcaster_redis.sh --repo .
```

Manual VM-ready wait when you want to confirm the service itself before rerunning the analyzer:
```bash
scripts/wait_ready.sh --url http://localhost:3000/status --expect-chain-id 1 --require-vm-ready --timeout 600
```

`scripts/wait_ready.sh` still waits for native readiness by default. Use the VM and RFQ flags only when those backends also matter.

Manual RFQ-ready wait when RFQ pools are enabled:
```bash
scripts/wait_ready.sh --url http://localhost:3000/status --expect-chain-id 8453 --require-rfq-ready --timeout 600
```

Manual combined VM and RFQ wait for Ethereum when both backends matter:
```bash
scripts/wait_ready.sh --url http://localhost:3000/status --expect-chain-id 1 --require-vm-ready --require-rfq-ready --timeout 600
```

Write to a custom directory:
```bash
cargo run -p apps --bin sim-analysis -- --chain-id 1 --out logs/simulation-reports/manual-check --stop
```

Target a different local base URL:
```bash
cargo run -p apps --bin sim-analysis -- --chain-id 1 --base-url http://127.0.0.1:3000 --stop
```

## Investigation flow

After the analyzer runs:

1. Read `summary.md` first for the high-level picture.
2. Use `report.json` for exact counts, latencies, status/result-quality splits, protocol visibility, encode inspections, and any RFQ readiness or RFQ-visibility findings.
3. Open the files under `evidence/` for sampled request/response bodies, readiness snapshots, and simulator/broadcaster log excerpts.
4. If the current behavior looks suspicious, compare it with the saved baseline before deciding whether the change is actually novel.
5. If something still looks off, continue with targeted manual requests, log inspection, or deeper domain research.

## References

- `references/project.md` – repo commands, outputs, and analysis flow.
- `references/encode.md` – `/encode` schema and route-probe notes.
- `references/protocols.md` – chain protocol context and VM notes that help interpret findings.
- `references/tycho-deps.md` – Tycho/Propeller Heads context and docs.
