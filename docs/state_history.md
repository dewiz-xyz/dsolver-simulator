# State History

State history records the broadcaster as ordered segments. A stream position is the pair `(generation, message_seq)`. PostgreSQL stores accepted update payloads as deltas and stores checkpoint metadata with optional token references. S3 stores compressed full-state checkpoint archives and token snapshot sidecar objects.

A reader starts from the newest complete checkpoint at or before the requested start block, then selects deltas using each backend's own block or timestamp cursor. A replay plan can contain multiple legs. Each leg starts from one checkpoint and applies its deltas in `(generation, message_seq)` order.

Boundary checkpoints start new segments after writer promotion and recovery commits. Interval checkpoints reduce replay work but do not split an existing segment. A missing or failed boundary stops replay across that boundary and appears as a gap. Updates drained from the recovery buffer at a recovery commit are appended directly to Redis and do not become delta rows. The boundary checkpoint's exported cache state covers them.

## Terminology

- **Token catalog.** The broadcaster's live in-memory `TokenStore` map. It is mutable and append-only. Known entries are not replaced.
- **Token snapshot.** An immutable canonical serialization of the token catalog captured with a checkpoint. It is stored as an S3 sidecar object.
- **Token reference.** The nullable checkpoint manifest fields that identify a token snapshot. Many checkpoint manifests can reference the same token snapshot.
- **Content-addressed.** The object key contains the SHA-256 digest of the object's uncompressed canonical bytes, so the content defines the object's identity.

The terms "version" and "token history" are avoided for token snapshots. Token snapshots have no ordering, lineage, or diff model. Token metadata also has no block-indexed change timeline.

## S3 objects

### Checkpoint archives

With a non-empty `STATE_HISTORY_S3_PREFIX`, checkpoint archive keys use `{prefix}/chain={chain_id}/gen={generation}/seq={message_seq}/kind={boundary|interval}/checkpoint.zst`. With an empty prefix, the key starts at the `chain=` segment.

Checkpoint archives are position-addressed because each archive anchors replay at one checkpoint kind and stream position. Its manifest records the SHA-256 digest of the uncompressed archive JSON for read verification.

### Token snapshots

Every boundary and interval checkpoint capture attempts to persist a token snapshot. With a non-empty `STATE_HISTORY_S3_PREFIX`, token snapshot keys use `{prefix}/chain={chain_id}/tokens/{sha256}.zst`. With an empty prefix, the key is `chain={chain_id}/tokens/{sha256}.zst`.

The uncompressed JSON envelope has this shape:

```json
{
  "schema_version": 1,
  "chain_id": 8453,
  "token_count": 1,
  "tokens": [
    {
      "address": "0x1111111111111111111111111111111111111111",
      "symbol": "TOKEN",
      "decimals": 18,
      "tax": 0,
      "gas": [21000],
      "chainId": 8453,
      "quality": 100
    }
  ]
}
```

`TOKEN_SNAPSHOT_SCHEMA_VERSION` is the token snapshot schema constant. It is independent of `ARCHIVE_SCHEMA_VERSION` and the delta payload format. All three retained formats are currently `1`.

Canonical encoding sorts tokens by address and rejects duplicate addresses. Serde emits deterministic JSON from the fixed envelope and token DTO fields. The writer computes SHA-256 over those uncompressed canonical bytes, then compresses them with Zstandard level 3. The manifest `token_bytes` column records the uncompressed canonical JSON length. `TokenSnapshotCodecError` provides the typed `Sha256Mismatch`, `UnsupportedSchemaVersion`, and `DuplicateTokenAddress` variants.

Token snapshots are content-addressed because an unchanged token catalog can be shared by checkpoint manifests at different positions. The writer memoizes the most recent successfully persisted SHA-256 digest. A matching capture reuses its token reference without another S3 PUT. A changed catalog produces a new object key. This also lets readers cache one fetch by SHA-256 across replay legs.

## PostgreSQL schema

The append-only migrations under `crates/state-history/migrations/` are the source of truth for this schema. The initial migration creates schema version 1. `20260813000100_add_delta_payload_format_version.sql` upgrades it to version 2 without rewriting the initial migration.

### `state_history.schema_meta`

| Column | Meaning |
| --- | --- |
| `singleton` | Primary key fixed to `TRUE`, which keeps this table to one schema metadata row. |
| `version` | State history schema version. The current migration catalog requires version `2`. |

### `state_history.deltas`

One row represents one accepted update. `(chain_id, generation, message_seq)` is both the idempotency key and replay order.

| Column | Meaning |
| --- | --- |
| `id` | Generated row identifier. |
| `chain_id` | Chain that produced the update. |
| `generation` | Redis writer generation for the update. |
| `message_seq` | Update sequence within the generation. |
| `block_number` | Highest block cursor carried by the update, when any backend has a block cursor. |
| `observed_at_ms` | Time the update was observed, in Unix milliseconds. |
| `payload` | Zstandard-compressed serialized update payload. |
| `payload_sha256` | SHA-256 digest of the uncompressed serialized payload. |
| `payload_format_version` | Complete retained delta payload and decoder profile version. Existing and new rows default to version `1`. |
| `created_at` | Time PostgreSQL inserted the row. |

### Retained payload formats

Checkpoint archives, token snapshots, and deltas dispatch through explicit stored-version matches. Unknown versions fail closed. Delta payload format 1 means the current broadcaster update envelope plus the fixed `DecoderConfig::retained_v1()` profile. That profile registers every protocol implementation a format 1 writer could emit and pins token quality to `0`; it never reads the current simulator manifest.

When state history is enabled, broadcaster startup refuses `BROADCASTER_TOKEN_MIN_QUALITY` values other than `0`. Any JSON-shape, protocol-decoder, or decoder-setting change that changes retained decoding requires a new delta payload format before its writer can be enabled.

Deploy reader support for format `N + 1` before any writer emits `N + 1`. Keep each decoder while matching retained rows or objects exist. This is a stored-data retention rule, not public API backward compatibility.

### `state_history.delta_backends`

One row maps a delta to one affected backend. Range queries use these backend cursors instead of the envelope cursor.

| Column | Meaning |
| --- | --- |
| `delta_id` | Referenced `deltas.id`. Deleting the delta deletes this row. |
| `chain_id` | Chain copied from the parent delta for indexed range queries. |
| `backend` | Affected backend. Allowed values are `native`, `vm`, and `rfq`. |
| `generation` | Writer generation copied from the parent delta. |
| `message_seq` | Message sequence copied from the parent delta. |
| `block_number` | Backend block cursor, when the backend advances by block. |
| `observed_at_ms` | Backend observation cursor in Unix milliseconds, when the backend advances by time. |

At least one of `block_number` and `observed_at_ms` is present. A delta has at most one row for each backend.

### `state_history.checkpoints`

One row is the PostgreSQL manifest for a full-state checkpoint archive in S3.

| Column | Meaning |
| --- | --- |
| `id` | Generated checkpoint identifier. |
| `chain_id` | Chain captured by the checkpoint. |
| `generation` | Writer generation at capture. |
| `message_seq` | Inclusive stream position covered by the checkpoint. |
| `state_version` | Redis committed state version for the capture, when available. |
| `kind` | Checkpoint purpose. Allowed values are `boundary` and `interval`. |
| `block_number` | Aligned native and VM head captured by the checkpoint. |
| `rfq_observed_at_ms` | Highest RFQ observation time covered by the checkpoint, in Unix milliseconds. |
| `backends` | Backend names included in the checkpoint archive. |
| `s3_key` | Object key for the completed archive. |
| `archive_sha256` | SHA-256 digest of the uncompressed checkpoint archive JSON. |
| `archive_bytes` | Uncompressed checkpoint archive size in bytes. |
| `compressed_bytes` | Stored Zstandard archive size in bytes. |
| `token_s3_key` | Nullable `TEXT` object key for the token snapshot. |
| `token_sha256` | Nullable `TEXT` SHA-256 digest of the uncompressed canonical token snapshot JSON. |
| `token_count` | Nullable `BIGINT` token count from the token snapshot envelope. |
| `token_bytes` | Nullable `BIGINT` uncompressed canonical token snapshot JSON size in bytes. |
| `status` | Lifecycle state. Allowed values are `writing`, `complete`, and `failed`. |
| `error` | Checkpoint failure detail, when the checkpoint failed. |
| `created_at` | Time PostgreSQL created the manifest. |
| `completed_at` | Time the checkpoint reached `complete` or `failed` status. |

`(chain_id, generation, message_seq, kind)` is unique.

The four token reference columns are nullable. The `checkpoints_token_reference_all_or_none` CHECK constraint requires all four to be set together or all four to be `NULL`.

`StateHistoryStore::complete_checkpoint` writes the archive fields, optional token reference, `status='complete'`, and block-time updates in one database transaction. The token object is durable, or known durable through the writer memo, before that transaction publishes its reference. `StateHistoryStore::fail_checkpoint` clears all four token reference columns while setting `status='failed'`. This clearing is required because complete-then-failed is a real writer transition. Writing and failed manifests therefore expose no token reference.

### `state_history.gaps`

Rows record losses known to the writer.

| Column | Meaning |
| --- | --- |
| `id` | Generated gap identifier. |
| `chain_id` | Chain affected by the gap. |
| `generation` | Writer generation containing the gap. |
| `from_message_seq` | First missing or unusable message sequence, inclusive. |
| `to_message_seq` | Last missing or unusable message sequence, inclusive. |
| `reason` | Known cause. Allowed values are `queue_overflow`, `write_failed`, `writer_stopped`, `checkpoint_failed`, and `boundary_slip`. |
| `from_block_number` | First affected block when a block bound is known. |
| `to_block_number` | Last affected block when a block bound is known. |
| `from_observed_at_ms` | First affected observation time in Unix milliseconds, when known. |
| `to_observed_at_ms` | Last affected observation time in Unix milliseconds, when known. |
| `created_at` | Time PostgreSQL inserted the row. |

`from_message_seq` cannot exceed `to_message_seq`. The full gap identity is unique, including nullable block and time bounds.

Block bounds come only from block backend cursors. Observation time bounds come only from RFQ provider cursors, never from Redis publication time. A bound stays `NULL` when the lost payload has no corresponding cursor. Boundary checkpoint failures use the capture block number and RFQ high water for both sides of their bounds. Boundary slip bounds start at the request-time cursors and end at the capture cursors. Rows with all four bounds `NULL` are still selected by stream position.

`boundary_slip` records a same generation recovery boundary checkpoint that pins after its requested commit position. Its stream span starts at the requested message sequence plus one and ends at the pinned capture position, so replay across any slipped position fails closed.

### `state_history.block_times`

This table joins block number, block time, hashes, and base fee for direct analysis and RFQ range resolution.

| Column | Meaning |
| --- | --- |
| `chain_id` | Chain containing the block. |
| `block_number` | Block number. Together with `chain_id`, this is the primary key. |
| `timestamp_ms` | Block timestamp in Unix milliseconds. |
| `block_hash` | Block hash, when supplied by the update. |
| `parent_hash` | Parent block hash, when supplied by the update. |
| `base_fee_wei` | Base fee in wei, when an RPC source resolved it. |
| `source_generation` | Writer generation of the latest accepted observation. |
| `source_message_seq` | Message sequence of the latest accepted observation. |
| `created_at` | Time PostgreSQL created the row. |
| `updated_at` | Time a newer observation changed the block metadata. |

A row is superseded only by a newer `(source_generation, source_message_seq)` observation.

When a newer observation keeps the same block hash and supplies no base fee, the stored fee is preserved. When it changes the hash and supplies no base fee, the stored fee is cleared rather than inherited from the orphaned block.

## Token catalog capture

At startup, the broadcaster admits tokens using `BROADCASTER_TOKEN_MIN_QUALITY`, which defaults to `0`. It also extracts `new_tokens` while staging each accepted Tycho stream update. At commit time, after the cache write guard is released, it filters those tokens with the same quality floor and inserts them into the token catalog. `TokenStore::insert_batch` uses `or_insert`, so a stream update never replaces metadata for a known address. This fold also runs for accepted updates buffered during same-generation recovery. RFQ updates are outside the token catalog because RFQ payloads embed their own full token values.

Checkpoint capture pins every component state source under the shared lifecycle gate. It then clones the token catalog under the same gate and only after every state pin. Accepted update paths hold that gate through staging, state commit, and the token fold. A captured token snapshot therefore contains every token address referenced by the captured component payloads when Tycho supplied that token at or above `BROADCASTER_TOKEN_MIN_QUALITY`. A token below the floor never enters the catalog, so its pools are skipped by live simulators and historical replay alike; the deployed floor of 0 admits everything Tycho sends. Catalog appends between a state pin and the catalog clone can only add harmless extra entries. If the pinned world changes during export, the state export and token catalog clone are discarded together.

Token snapshot persistence is optional enrichment. An encoding or token object upload failure increments the token persistence failure counter and clears the writer's persistence memo. The writer proceeds with checkpoint completion and all four token reference columns set to `NULL`. The failure does not record a gap and does not change broadcaster readiness. A draining writer skips the token object upload outright so the remaining shutdown deadline stays available to checkpoint completion; memoized references still resolve because they need no upload, and the skip is neither a failure nor a memo reset. A later checkpoint attempts token persistence again.

Token metadata is not block-versioned in Tycho. The catalog does not refresh metadata for a known address, and a later broadcaster process can load different quality, tax, or gas metadata. A token snapshot records what the decoder would have had at checkpoint capture. It is reference data rather than chain state, so reorg handling does not apply.

## Example queries

The examples use Base chain ID `8453`.

### Blocks per hour

```sql
SELECT
    date_trunc('hour', to_timestamp(timestamp_ms / 1000.0)) AS hour,
    count(*) AS blocks
FROM state_history.block_times
WHERE chain_id = 8453
GROUP BY hour
ORDER BY hour;
```

### Gap inspection

```sql
SELECT
    generation,
    from_message_seq,
    to_message_seq,
    reason,
    from_block_number,
    to_block_number,
    from_observed_at_ms,
    to_observed_at_ms,
    created_at
FROM state_history.gaps
WHERE chain_id = 8453
ORDER BY generation, from_message_seq, to_message_seq;
```

### Checkpoint inventory

```sql
SELECT
    kind,
    status,
    count(*) AS checkpoints,
    min(block_number) AS first_block,
    max(block_number) AS last_block,
    sum(compressed_bytes) AS compressed_bytes
FROM state_history.checkpoints
WHERE chain_id = 8453
GROUP BY kind, status
ORDER BY kind, status;
```

### Base-fee join

```sql
SELECT
    d.generation,
    d.message_seq,
    db.backend,
    db.block_number,
    bt.timestamp_ms,
    bt.base_fee_wei
FROM state_history.delta_backends AS db
JOIN state_history.deltas AS d
    ON d.id = db.delta_id
JOIN state_history.block_times AS bt
    ON bt.chain_id = db.chain_id
   AND bt.block_number = db.block_number
WHERE db.chain_id = 8453
  AND bt.base_fee_wei IS NOT NULL
ORDER BY d.generation, d.message_seq, db.backend;
```

### Delta volume by backend

```sql
SELECT
    db.backend,
    count(*) AS backend_entries,
    count(DISTINCT db.delta_id) AS deltas,
    sum(octet_length(d.payload)) AS compressed_payload_bytes
FROM state_history.delta_backends AS db
JOIN state_history.deltas AS d
    ON d.id = db.delta_id
WHERE db.chain_id = 8453
GROUP BY db.backend
ORDER BY db.backend;
```

## Reader API

`StateHistoryReader::resolve_backtest_range(&BacktestRequest)` returns a `BacktestPlan`. Native and VM requests get informational start and end timestamps when rows exist. Requests that include RFQ require continuous `block_times` rows through `end_block + 1`, which the reader uses to derive inclusive RFQ timestamp bounds.

Each `RangeLeg` contains a `CheckpointManifest` and zero or more ordered deltas to apply after that checkpoint. Boundary discovery uses the request end rather than the last stored delta. For every requested backend represented by a boundary, its checkpoint cursor must be at or below that backend's requested end. A trailing boundary can therefore close a range without a later delta, producing a leg with zero deltas.

`StoredDelta::raw_payload` is stored and returned exactly as published. It is never truncated or rewritten. `StoredDelta::applicable_backends` is filtered to the requested backends and range end, and replay consumers must apply only the partitions listed there. A delta can contain one backend cursor inside the range and another beyond its end, so applying its whole raw payload would advance state past the requested end.

`StateHistoryReader::fetch_checkpoint(&CheckpointManifest)` accepts only a complete manifest, derives its expected S3 key, downloads the object, and verifies the digest, compressed and uncompressed byte lengths, and every decoded metadata field against the PostgreSQL manifest before returning the `CheckpointArchive`.

Each checkpoint manifest exposes `token_reference: Option<TokenSnapshotRef>`. A checkpoint captured without a token reference returns `None`. It remains a valid replay anchor and never creates a gap. Range selection, `ensure_gap_free`, and backtest admission do not check token references.

Use the manifest-aware token method for replay verification:

```rust
pub async fn fetch_checkpoint_token_snapshot(
    &self,
    manifest: &CheckpointManifest,
) -> anyhow::Result<Option<RawTokenSnapshot>>
```

The method derives the expected key from the checkpoint chain and token digest, then verifies the digest, uncompressed byte length, reference count, envelope count, array length, chain, embedded token chains, and strict address ordering. It returns `None` when the complete manifest has no token reference. The raw token array is returned without re-serialization, and no token values are logged. Missing or misassociated objects and every metadata mismatch return errors.

Gap machinery covers stream data, which consists of checkpoint state and ordered deltas. A token snapshot is optional reference metadata in the same enrichment class as `base_fee_wei`. An absent reference is a caller decision, not evidence of missing stream data.

```rust
use state_history::{Backend, BacktestRequest, StateHistoryReader};

async fn load_range(reader: &StateHistoryReader) -> anyhow::Result<()> {
    let request = BacktestRequest::new(
        8453,
        25_000_000,
        25_001_000,
        vec![Backend::Native, Backend::Vm, Backend::Rfq],
    )?;
    let plan = reader.resolve_backtest_range(&request).await?;
    plan.range.ensure_gap_free()?;

    for leg in &plan.range.legs {
        let checkpoint = reader.fetch_checkpoint(&leg.checkpoint).await?;
        let tokens = reader
            .fetch_checkpoint_token_snapshot(&leg.checkpoint)
            .await?;
        // Restore checkpoint.payloads_json, then apply leg.deltas in order.
        process_leg(checkpoint, tokens, &leg.deltas).await?;
    }

    Ok(())
}
```

Construct the reader with `StateHistoryReader::new(PgPool, CheckpointObjectStore)`. `StateHistoryReader<P>` accepts any service-owned `ReadConnectionProvider`, so the historical service can supply an observer or replica pool without giving the reader a primary fallback. Every range, coverage, target-planning, token-anchor, and checkpoint-pair query uses one repeatable-read, read-only transaction.

`coverage` returns continuous safe block intervals and `visible_through_block` separately. Replica delay only lowers that cutoff; it never becomes a stored gap. Stored gaps retain their complete stream-position, block, and RFQ observation-time bounds. A gap without block or time cursors fails closed from the prior compatible boundary through the next compatible boundary.

`plan_targets` returns observations for every unique target block and, for RFQ, each target's next block. It validates canonical parent/hash lineage across the requested span and reports unsafe lineage intervals separately from stored gaps. `resolve_checkpoint_pairs` returns adjacent complete checkpoints without crossing a boundary, while explicit pairs must resolve to exact, ordered positions in one segment. `select_token_anchor` uses the selected checkpoint's token snapshot or the newest earlier token snapshot after the same segment boundary; it never attaches a token snapshot across a boundary.

Use `ReadLimits` with target planning and the bounded object fetch methods. Planning sums manifest sizes and SQL `octet_length(payload)` before selecting compressed delta bodies. Object fetches reject an oversized manifest or S3 content length before reading the body, and all Zstandard decoders use counted readers capped at the remaining decoded-byte limit.

`state-history-check` is the read-only operational wrapper around this flow. It reads the observer database URL and S3 settings from the state-history environment variables, selects a closed recent Base range, requires `ensure_gap_free()`, and verifies every replay-leg checkpoint and token object. It never runs migrations or writes PostgreSQL or S3. The production image intentionally does not contain this local tool.

When replaying format 1 deltas, use `DecoderConfig::retained_v1()`. Its token quality is fixed at `0`; do not derive retained decoding from the live manifest or current runtime tuning.

## Operations

State history is inert when `STATE_HISTORY_ENABLED=false`. When it is disabled, the writer is not created and the `state_history` property is absent from `/status` JSON. Enabling it requires PostgreSQL, S3, and the checkpoint block interval. Deployment configuration remains in solver-iac.

### Environment variables

| Variable | Meaning and default |
| --- | --- |
| `STATE_HISTORY_ENABLED` | Enables the writer. Defaults to `false`. |
| `STATE_HISTORY_DATABASE_URL` | PostgreSQL connection URL. Required when enabled. |
| `STATE_HISTORY_S3_BUCKET` | S3 bucket for checkpoint archives and token snapshots. Required when enabled. |
| `STATE_HISTORY_S3_PREFIX` | Checkpoint archive and token snapshot object key prefix. Defaults to `state-history`. One trailing slash is removed. |
| `STATE_HISTORY_S3_REGION` | S3 region. Defaults to `eu-central-1`. |
| `STATE_HISTORY_S3_ENDPOINT_URL` | Optional custom S3 endpoint for compatible local services. |
| `STATE_HISTORY_S3_FORCE_PATH_STYLE` | Forces path-style S3 requests. Defaults to `false`. |
| `STATE_HISTORY_CHECKPOINT_BLOCK_INTERVAL` | Required positive interval between checkpoint targets. Use `1800` in production. |
| `STATE_HISTORY_CHECKPOINT_POLL_INTERVAL_SECS` | Seconds between checkpoint eligibility checks. Defaults to `30`. |
| `STATE_HISTORY_QUEUE_CAPACITY` | In-memory writer command queue capacity. Defaults to `8192`. |
| `STATE_HISTORY_WRITE_RETRY_WINDOW_MS` | Maximum delta write retry window in milliseconds. Defaults to `30000`. |

`.env.example` contains the complete local example block. The recommended production value is `STATE_HISTORY_CHECKPOINT_BLOCK_INTERVAL=1800`.

Token snapshots add no environment variables. They use the existing state history S3 settings. Stream token admission uses the existing `BROADCASTER_TOKEN_MIN_QUALITY` setting.

`state-history-migrate` is a one-shot task. It requires `STATE_HISTORY_MIGRATION_DATABASE_URL` for the migration master and `STATE_HISTORY_WRITER_PASSWORD` for `state_history_writer`. It runs and verifies the exact embedded SQLx catalog, validates schema version 2, and converges `state_history_writer` plus the passwordless, `rds_iam`-enabled `state_history_observer`. It does not construct an S3 client or write application rows. The migration task must override the broadcaster image entrypoint with `/usr/local/bin/state-history-migrate`.

### `/status.state_history`

The object mirrors the writer's 17 status fields. Its fields use camelCase. Optional fields are omitted when they have no value.

| Field | Meaning |
| --- | --- |
| `healthy` | Whether the writer's latest persistence operation is healthy. It becomes false after a failed delta, checkpoint, shutdown, or permanent gap write. |
| `queueLen` | Current number of queued writer commands. |
| `queueCapacity` | Configured writer command queue capacity. |
| `enqueuedDeltas` | Accepted deltas added to the writer queue. |
| `persistedDeltas` | Deltas successfully persisted. |
| `droppedDeltas` | Deltas rejected because the queue was unavailable or full. |
| `failedDeltas` | Deltas that exhausted persistence attempts or failed permanently. |
| `recordedGaps` | Gap rows successfully persisted. |
| `lastPersisted` | Last persisted stream position as `{ "generation", "messageSeq" }`. Omitted before the first persisted delta. |
| `lastPersistedAtMs` | Unix timestamp in milliseconds when the latest delta transaction committed. |
| `lastError` | Latest writer error text. Omitted when there is no current error. |
| `checkpointsCompleted` | Checkpoints whose archive and manifest completed successfully. |
| `checkpointsFailed` | Checkpoints whose archive or manifest lifecycle failed. |
| `lastCheckpointStatus` | Latest checkpoint result: `complete`, `failed`, or `enqueue_failed`. Omitted before the first result. |
| `lastSuccessfulCheckpointAtMs` | Unix timestamp in milliseconds when the latest successful checkpoint manifest transaction committed. |
| `lastTokenSnapshotSha` | SHA-256 digest from the latest token snapshot successfully uploaded by this writer. Omitted before the first successful token snapshot upload. |
| `tokenPersistenceFailures` | Token snapshot encoding or upload failures. The writer omits the token reference and proceeds with checkpoint completion. These failures do not change readiness. |

The five-second broadcaster EMF snapshot adds no-dimension `Tycho/Simulation` metrics for queue length, capacity and utilization basis points; enqueued, persisted, dropped, and failed delta counts; the last persisted generation and message sequence; persisted-delta age; recorded gaps; completed and failed checkpoints; successful-checkpoint age; and token-persistence failures. Counter values are cumulative for the lifetime of the writer task.

Gaps are honest reporting. They record losses the writer knows about, but their absence is not proof that a range is complete. Consumers should call `RangePlan::ensure_gap_free()` before using a replay plan.

Retention is keep everything. There is no state history TTL or cleanup policy for PostgreSQL rows, S3 checkpoint archives, or token snapshots.
