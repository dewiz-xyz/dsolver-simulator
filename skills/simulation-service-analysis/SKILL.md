---
name: simulation-service-analysis
description: Analyze the local DSolver Simulator service in this repo with a reporting-first Rust CLI. Use when you want to start or reuse the server, wait for /status health and native readiness, exercise representative /simulate and /encode flows, run latency and light stress probes, save standardized JSON/markdown reports, compare against previous local runs, and investigate anomalies without relying on strict pass/fail business assertions.
metadata:
  short-description: DSolver Simulator local analysis
---

# Simulation Service Analysis

## Quick start

1. Confirm the repo root (expect `Cargo.toml` and `src/`).
2. Ensure `.env` exists and contains `TYCHO_API_KEY`.
   RFQ feeds default to off. For RFQ analysis on Ethereum or Base, set `ENABLE_RFQ_POOLS=true` and also set `BEBOP_USER`, `BEBOP_KEY`, `HASHFLOW_USER`, and `HASHFLOW_KEY`.
3. Pick a chain context for the run (`--chain-id 1` for Ethereum, `--chain-id 8453` for Base).
4. Run the analyzer:
   ```bash
   cargo run --bin sim-analysis -- --chain-id 1 --stop
   ```
5. Read:
   - `logs/simulation-reports/<chain-id>/balanced/<timestamp>/summary.md`
   - `logs/simulation-reports/<chain-id>/balanced/<timestamp>/report.json`

## What the analyzer does

- Reuses the existing local server if it is already responding, otherwise starts it with the repo lifecycle scripts.
- Waits for `/status` service health, then confirms native readiness first and adds VM and RFQ readiness checks when those pool backends are enabled.
- Fresh VM-pool or RFQ warmups can take much longer than native readiness. Budget up to 10 minutes before treating either backend as stuck.
- Runs a balanced `/simulate` sweep across representative pairs.
- Builds a generic 2-hop `/encode` probe from live `/simulate` results.
- On Base with RFQ enabled and ready, runs a Bebop partial-fill `/encode` diagnostic and inspects router calldata for the packed `originalFilledTakerAmount` token-in invariant.
- Runs latency and light stress sweeps.
- Saves sampled request/response artifacts plus log excerpts.
- Optionally compares the current run against the latest compatible saved report.
- Top-level `/status.status` is service health; `native_status` carries native readiness separately.

## Behavior model

- Non-zero exit codes are reserved for harness/runtime failures such as startup failures, readiness timeouts, transport failures that prevent analysis, or report-writing failures.
- Degraded protocol behavior, request-level failures, odd pool visibility, and latency regressions are reported as findings, not hard failures.
- The analyzer is meant to help the agent investigate local behavior, not to decide prod-readiness by itself.

## Useful commands

Base run:
```bash
cargo run --bin sim-analysis -- --chain-id 8453 --stop
```

Keep the server running:
```bash
cargo run --bin sim-analysis -- --chain-id 1
```

Disable baseline comparison:
```bash
cargo run --bin sim-analysis -- --chain-id 1 --baseline none --stop
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
cargo run --bin sim-analysis -- --chain-id 1 --out logs/simulation-reports/manual-check --stop
```

Target a different local base URL:
```bash
cargo run --bin sim-analysis -- --chain-id 1 --base-url http://127.0.0.1:3000 --stop
```

## Investigation flow

After the analyzer runs:

1. Read `summary.md` first for the high-level picture.
2. Use `report.json` for exact counts, latencies, status/result-quality splits, protocol visibility, encode inspections, and any RFQ readiness or RFQ-visibility findings.
3. Open the files under `evidence/` for sampled request/response bodies, readiness snapshots, and log excerpts.
4. If the current behavior looks suspicious, compare it with the saved baseline before deciding whether the change is actually novel.
5. If something still looks off, continue with targeted manual requests, log inspection, or deeper domain research.

## References

- `references/project.md` – repo commands, outputs, and analysis flow.
- `references/encode.md` – `/encode` schema and route-probe notes.
- `references/protocols.md` – chain protocol context and VM notes that help interpret findings.
- `references/tycho-deps.md` – Tycho/Propeller Heads context and docs.
