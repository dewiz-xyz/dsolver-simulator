# State History Storage

State history is an opt-in long-term store for accepted broadcaster state changes. Redis and the broadcaster cache stay the live handoff path; PostgreSQL and S3 keep the historical log and checkpoints used by external backtesting tools.

## What Is Stored

- PostgreSQL stores accepted state-changing broadcaster Redis entries, per-backend indexes, checkpoint manifests, and explicit ingestion gaps.
- S3 stores compressed combined checkpoints built from the broadcaster snapshot payload model.
- Native and VM history use block-number cursors.
- RFQ history uses the RFQ update timestamp cursor. This stores RFQ state for historical indicative `/simulate` reconstruction. It does not store signed quote replay material for `/encode`.

## Runtime Contract

State history is disabled unless `STATE_HISTORY_ENABLED=true`.

When enabled, broadcaster startup connects to PostgreSQL, validates that migrations have already been applied, builds the S3 checkpoint store, and starts the async history writer. Live serving does not wait for history storage. If the queue fills or a write exhausts its retry window, the writer records an explicit gap when PostgreSQL is available and reports the failure in `/status.state_history`.

Checkpoints are captured by block interval. A checkpoint contains the combined raw/RFQ snapshot payloads from the broadcaster cache and is anchored to the active Redis replay boundary. Upload failure marks the checkpoint manifest `failed`; delta history continues.

## Configuration

Required when state history is enabled:

- `STATE_HISTORY_DATABASE_URL`
- `STATE_HISTORY_S3_BUCKET`
- `STATE_HISTORY_CHECKPOINT_BLOCK_INTERVAL`

Optional:

- `STATE_HISTORY_S3_PREFIX`, default `state-history`
- `STATE_HISTORY_S3_REGION`, default `eu-central-1`
- `STATE_HISTORY_S3_ENDPOINT_URL`, for MinIO or another S3-compatible local service
- `STATE_HISTORY_S3_FORCE_PATH_STYLE`, for MinIO
- `STATE_HISTORY_CHECKPOINT_POLL_INTERVAL_SECS`, default `30`
- `STATE_HISTORY_QUEUE_CAPACITY`, default `8192`
- `STATE_HISTORY_WRITE_RETRY_WINDOW_MS`, default `30000`

Operators must run the migrations in `crates/state-history/migrations/` before enabling the broadcaster. The service validates the schema on startup but does not mutate production databases by itself.

## Local Validation

Run the opt-in local stack and storage harness:

```bash
scripts/verify_state_history.sh --repo .
```

The script starts Postgres and MinIO from `docker-compose.state-history.yml`, runs `cargo run -p apps --bin state-history-analysis`, and stops the storage stack when the harness exits. Use `--keep-services` to keep Postgres and MinIO running for manual inspection.

The harness:

- applies the repo-owned migrations;
- creates the local S3 bucket if it is missing;
- writes synthetic native, VM, and RFQ deltas;
- writes and fetches a combined checkpoint;
- resolves a history range through the reader API;
- verifies the gap-free helper;
- records one synthetic gap and verifies that the reader reports it.

## Reader Contract

The `state-history` crate exposes the reader API for external harnesses:

- `StateHistoryReader::resolve_range` selects the latest complete checkpoint and ordered deltas for a requested block/timestamp range.
- `StateHistoryReader::fetch_checkpoint` fetches and verifies the S3 object for a complete manifest.
- `HistoryRangePlan::ensure_gap_free` turns recorded or missing-checkpoint gaps into a hard error for callers that require complete ranges.

The reader does not materialize simulator state. External backtesting tools apply the checkpoint payloads and streamed deltas.
