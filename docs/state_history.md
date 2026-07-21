# State History Storage

State history is an opt-in long-term store for accepted broadcaster state changes. Redis and the broadcaster cache stay the live handoff path; PostgreSQL and S3 keep the historical log and checkpoints used by external backtesting tools.

## What Is Stored

- PostgreSQL stores accepted state-changing broadcaster Redis entries, per-backend indexes, stream cursors, native/VM block timestamp metadata, checkpoint manifests, and explicit ingestion gaps.
- S3 stores compressed combined checkpoints built from the broadcaster snapshot payload model.
- Block timestamp metadata stays in PostgreSQL; S3 checkpoint metadata and `checkpoint.zst` payloads keep their existing shape.
- Block timestamp rows are recorded from accepted update deltas and from completed checkpoint snapshots. Checkpoint rows land in the same transaction that marks the checkpoint complete, so every complete checkpoint has a row at its boundary block.
- Reorgs are handled by newest-source supersession. A newer sequence on the same stream or a write from another stream overwrites the stored row, while stale same-stream writes are kept out. `updated_at` moves only when the stored content changes.
- Native and VM history use block-number cursors.
- RFQ history uses the RFQ update timestamp cursor. This stores RFQ state for historical indicative `/simulate` reconstruction. It does not store signed quote replay material for `/encode`.
- Block-range backtests can request native, VM, and RFQ together by block number. The reader resolves the start, end, and end+1 blocks through PostgreSQL block timestamp metadata. The RFQ range runs from the start block timestamp to the end+1 block timestamp minus 1 ms, so it captures RFQ updates observed while the end block was head. Ranges ending at the recorded head are unresolvable until the next block is stored.

## Runtime Contract

State history is disabled unless `STATE_HISTORY_ENABLED=true`.

When enabled, broadcaster startup connects to PostgreSQL, validates that migrations have already been applied, builds the S3 checkpoint store, and starts the async history writer. Live serving does not wait for history storage. After Redis accepts an update, heartbeat, or generation-progress marker, the Redis publisher sends it to the async history writer in the same order. If the queue fills or a write exhausts its retry window, the writer records an explicit gap when PostgreSQL is available and reports the failure in `/status.state_history`.

Checkpoints are captured by block interval. A checkpoint contains the combined raw/RFQ snapshot payloads from the broadcaster cache and is anchored to the active Redis replay boundary. Passive and stale writers do not export checkpoints. Native and VM checkpoints are only captured when their block cursors are aligned. Upload failure marks the checkpoint manifest `failed`; delta history continues.

The reader fails closed. A range with no complete checkpoint returns no deltas and reports `MissingCheckpoint`. A range with a checkpoint also needs a persisted delta chain and stream cursor proof. If PostgreSQL cannot prove every persistable message was written, every closed handoff stream was observed through its tail, and the open stream is proven through the requested native, VM, and RFQ heads, the plan reports `UnprovenIngestion`. Recorded `ingestion_gaps` remain telemetry and are still surfaced when they overlap the requested range.

There is one accepted residual window: if the last command on an otherwise idle stream is lost and no later command or heartbeat arrives, the reader cannot observe the loss until the next heartbeat interval advances the stream cursor. Strict PostgreSQL-before-live-serving is intentionally out of scope.

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

The script starts Postgres and MinIO from `docker-compose.state-history.yml`, passes their connection settings directly to `state-history-analysis`, and stops the storage stack when the harness exits. It ignores inherited state-history storage and AWS session settings so this local verifier cannot target external storage. Bind and port environment variables remain available for avoiding local conflicts. Use `--keep-services` to keep Postgres and MinIO running for manual inspection.

The harness:

- applies the repo-owned migrations;
- creates the local S3 bucket if it is missing;
- writes synthetic native, VM, and RFQ deltas, then replays duplicate, reorg, stale-writer, and cross-stream block timestamp writes and reads the stored rows back to verify supersession, kept stale writes, and untouched `updated_at` on verbatim redelivery;
- writes and fetches a combined checkpoint whose snapshot chunk seeds the start boundary timestamp row with the checkpoint's replay-boundary provenance;
- rejects a checkpoint archive with a conflicted boundary height before any manifest or S3 object is created;
- resolves a history range through the reader API;
- verifies the gap-free helper;
- records one synthetic gap and verifies that the reader reports it;
- verifies that the reader reports unproven ingestion when synthetic cursors do not prove the open stream through the requested head.
- resolves the same replay plan through the block-range backtest API and verifies the RFQ end bound equals the end+1 block timestamp minus 1 ms;
- verifies that a backtest range ending at the recorded head is a hard error;
- records one synthetic gap and one stale-stream delta, then verifies that the reader reports the visible recorded gap and the generation-switch gap.

## Reader Contract

The `state-history` crate exposes the reader API for external harnesses:

- `StateHistoryReader::resolve_range` selects the latest complete checkpoint and ordered deltas for a requested block/timestamp range. The checkpoint's RFQ high-water timestamp controls eligibility, while replay starts at the first message after its recorded source sequence. Every later arrival is replayed even if a block or RFQ provider timestamp decreased; the requested end block and timestamp remain upper bounds.
- `StateHistoryReader::resolve_backtest_range` accepts only block bounds and rejects an end block of `u64::MAX`. If RFQ is in scope, the start, end, and end+1 blocks must already exist in `state_history.block_timestamps`, and the end+1 timestamp must be strictly greater than the end timestamp. Missing metadata or a non-increasing next timestamp is a hard error before the reader builds the lower-level history request. Native/VM-only requests skip the timestamp lookup and delegate directly to `resolve_range`.
- `StateHistoryReader::fetch_checkpoint` fetches and verifies the S3 object for a complete manifest.
- `HistoryRangePlan::ensure_gap_free` turns recorded gaps, missing checkpoints, generation switches, or unproven ingestion into a hard error for callers that require complete ranges.

The reader does not materialize simulator state. External backtesting tools apply the checkpoint payloads and streamed deltas.
