# DSolver Simulator Local Analysis

This repo ships a reporting-first Rust CLI for local DSolver Simulator analysis. It starts or reuses the local Tycho broadcaster plus simulator stack, waits for simulator readiness, exercises representative `/simulate` and `/encode` flows, runs latency and light stress probes, captures sampled evidence, and writes a structured report instead of enforcing a strict pass/fail gate.

## Quick start

Ethereum:

```bash
cargo run -p apps --bin sim-analysis -- --chain-id 1 --stop
```

Base:

```bash
cargo run -p apps --bin sim-analysis -- --chain-id 8453 --stop
```

Keep the helper-managed local services running after the analysis:

```bash
cargo run -p apps --bin sim-analysis -- --chain-id 1
```

Disable baseline comparison for a one-off run:

```bash
cargo run -p apps --bin sim-analysis -- --chain-id 1 --baseline none --stop
```

## What the analyzer does

- reuses the existing local simulator if it is already responding, otherwise starts the local stack with the repo lifecycle helper
- starts `dsolver-tycho-broadcaster-service` first when `TYCHO_BROADCASTER_WS_URL` points at local loopback, then starts `dsolver-simulator-service`; non-local broadcaster URLs are treated as externally managed
- waits for `/status` service health, then confirms native readiness first and includes VM and RFQ readiness when those backends are enabled
- allows longer VM or RFQ warmups on fresh starts; budget up to about 10 minutes before assuming either backend is stuck
- runs a balanced `/simulate` sweep across representative pairs
- builds the balanced `/encode` route matrix from live `/simulate` prep hops: 3 SimpleSwap routes, 3 MultiSwap routes, and 2 MegaSwap routes per supported chain
- runs latency and light stress sweeps
- saves a JSON report, markdown summary, sampled request/response artifacts, and simulator/broadcaster log excerpts
- optionally compares the current run with the most recent compatible saved report

## Output

Default output root:

```text
logs/simulation-reports/<chain-id>/balanced/<timestamp>/
```

Main artifacts:

- `report.json`: machine-readable run summary
- `summary.md`: human-readable findings and investigation hints
- `evidence/`: readiness snapshots, sampled request/response bodies, and simulator/broadcaster log excerpts

## Behavior model

- Non-zero exit codes are reserved for harness/runtime failures such as startup failures, readiness timeouts, transport failures that prevent analysis, or report-writing issues.
- Degraded protocol behavior, request-level failures, odd pool visibility, and latency regressions are reported as findings, not hard failures.
- The analyzer is meant to help agents investigate, not to decide prod-readiness by itself.

## Investigation flow

After a run:

1. Read `summary.md` for the high-level picture.
2. Inspect `report.json` for exact counts, latencies, status/result-quality splits, protocol visibility, and RFQ findings.
3. Open any sampled artifacts in `evidence/` that look suspicious.
4. Compare against the saved baseline when the current behavior looks off.
5. Follow up with targeted manual requests or deeper log analysis when a protocol-specific anomaly needs explanation.
