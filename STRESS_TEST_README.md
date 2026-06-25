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

Verify the broadcaster HTTP snapshot plus Redis delta replay path while services are still running:

```bash
scripts/verify_broadcaster_redis.sh --repo .
```

The replay contract is: simulators bootstrap from the active broadcaster HTTP snapshot session, then read Redis deltas after the returned replay boundary. Only the active broadcaster should append deltas or serve snapshot sessions; passive, retired, and unhealthy broadcasters fail closed for those operations.

Disable baseline comparison for a one-off run:

```bash
cargo run -p apps --bin sim-analysis -- --chain-id 1 --baseline none --stop
```

## What the analyzer does

- reuses the existing local simulator if it is already responding, otherwise starts the local stack with the repo lifecycle helper
- starts helper-managed Redis first when `BROADCASTER_REDIS_URL` is loopback `redis://`, then starts `dsolver-tycho-broadcaster-service` when `TYCHO_BROADCASTER_URL` points at local loopback, then starts `dsolver-simulator-service`; non-local broadcaster or Redis URLs are treated as externally managed, and Redis carries active-broadcaster deltas after each HTTP snapshot replay boundary
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
- Redis replay status from simulator `/status` subscriptions is preserved in `report.json` and summarized in `summary.md` when present.
- Replay gaps should surface as readiness issues and fresh active-broadcaster snapshot bootstrap attempts. Redis generation crossings may continue only through a valid active-handoff marker; resets after append failure remain fail-closed.

## Behavior model

- Non-zero exit codes are reserved for harness/runtime failures such as startup failures, readiness timeouts, transport failures that prevent analysis, or report-writing issues.
- Degraded protocol behavior, request-level failures, odd pool visibility, and latency regressions are reported as findings, not hard failures.
- The analyzer is meant to help local reviewers investigate, not to decide prod-readiness by itself.

## Investigation flow

After a run:

1. Read `summary.md` for the high-level picture.
2. Inspect `report.json` for exact counts, latencies, status/result-quality splits, protocol visibility, and RFQ findings.
3. Open any sampled artifacts in `evidence/` that look suspicious.
4. Compare against the saved baseline when the current behavior looks off.
5. Follow up with targeted manual requests or deeper log analysis when a protocol-specific anomaly needs explanation.
