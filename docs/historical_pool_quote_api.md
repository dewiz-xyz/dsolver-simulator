# Historical Pool Quote API

The Historical Pool Quote API reconstructs retained Base state and returns directed pool quotes for one exact component and token direction. It also returns the raw history a replay consumer rebuilds a market session from. This document covers the service boundary, reconstruction semantics, HTTP contract, configuration, sizing, and operator workflow.

Client users should start with the [Historical Pool Quote Client Guide](historical_pool_quote_client.md). Agents helping with client requests should use the repo-local [`historical-pool-quote-cli` skill](../skills/historical-pool-quote-cli/SKILL.md).

The service serves bounded interactive analysis and the raw history a replay consumer needs. It is not a pool discovery service, a public endpoint, or a bulk export system, and every raw history read stays within a job's decoded-byte reservation.

## Mental model

A historical quote job selects one exact pool, one token direction, start blocks, lags, and input amounts. The service reconstructs each required end-of-block state, quotes the start once for each `(start block, input amount)`, and returns one ordered target outcome for each lag.

End-of-block state contains all accepted on-chain updates for the block. RFQ state uses the latest accepted observation strictly before the next block starts. An RFQ result is an indicative RFQ quote. It is not signed or binding.

State reconstruction coverage reports continuous block intervals that are safe for all requested backends. Coverage does not mean that a component exists or can quote. Replica delay lowers `visibleThroughBlock`; it does not create a stored gap.

Removing a pool that is already absent remains a replay diagnostic and does not make historical state unavailable. Missing state, unknown updates, and other state integrity failures still return `state_application_invalid`. A removed pool cannot produce a quote from the later state.

## Security boundary

The service runs as a separate process and task. It can read a dedicated PostgreSQL replica as `state_history_observer` and read the existing state-history objects. It has no primary database address, Redis endpoint, broadcaster URL, or object write permission.

Clients use the stable Tailscale Service URL and a bearer token issued from the configured key set. Each credential has a trusted key ID for audit attribution. Clients do not receive a database URL, AWS credential, object-store credential, internal load balancer address, or direct state-history access.

`GET /ready` is open only for the private load balancer health check. Every other endpoint requires the bearer token. Bodyless protected requests also require `X-DSolver-History-API-Revision: 1`.

The service logs request and job identifiers, normalized dimensions, work counts, duration, decoded bytes, state, and outcome counts. Successful authentication adds only the trusted `bearer_key_id`, request method, and path. It does not log authorization headers, tokens or token digests, request bodies, full responses, SQL text, storage object names, raw state, or dependency error details.

## Job lifecycle and delivery

Jobs are in memory. A deployment or task restart can lose them.

The first terminal `GET /jobs/{jobId}` selects the terminal body for delivery. The service removes the retained result only after it finishes writing that body. If the write fails, it releases the delivery lease so a later poll can retry. A successful terminal poll consumes the result. Later polls return `job_not_found`.

A cancelled job, or one past its deadline, ends at once, but its running slot and its decoded-byte reservation stay taken until its work stops. Terminal bodies are kept within a count and a byte limit, the oldest dropped first, but a body being delivered is never dropped or expired. A result that does not fit beside the bodies being delivered, by count or by bytes, waits, its job still running, until a delivery ends, and fails at its deadline if none does. A result written after its job's deadline or cancellation is not published as a success. A result larger than the byte limit fails its job with `read_limit_exceeded`. A failure or cancellation body is always kept, since it is a few hundred bytes.

The high-level `quotes` and `verify` CLI commands can recompute once after `job_not_found` when they still own the original request and time remains. Generic `job get` and `job wait` cannot recompute because they do not have the request body. See the [client guide](historical_pool_quote_client.md) for command behavior, interruption, output, and exit codes.

## Coverage and status semantics

Coverage returns `visibleThroughBlock`, continuous safe intervals, and known gaps. Protected status returns revisions, enabled backends, safe dependency summaries, queue and terminal counts, decoded-byte reservation, draining state, and configured limits.

Coverage describes safe reconstruction for the requested backends. It does not prove that an exact component exists or can quote. Replica delay lowers `visibleThroughBlock`; it does not create a stored gap.

Use bounded coverage as the normal preflight for a quote or consistency-check range. `knownGaps` returns an original, unclipped gap only when its projected unsafe interval intersects the effective requested range. Complete block bounds apply to native and VM state; complete observation-time bounds apply to RFQ state. One-sided and entirely cursorless gaps project conservatively. A missing bound domain is ignored when the other domain completely identifies the gap.

Unbounded coverage remains available for retained-history inventory and diagnosis. It scans retained history and should not be used as the routine per-request health check.

## Raw history

A replay consumer rebuilds a market session from the raw messages the broadcaster kept, so it reads them here rather than from the state-history store. Both reads are jobs and share the job lifecycle, its deadline, cancellation, decoded-byte reservation and single delivery. They accept only backends kept as raw messages, native and VM.

`POST /jobs/raw-snapshot` returns the snapshot at `position`, rebuilt from the latest complete checkpoint in the position's segment and the stored updates after it with the broadcaster's own merge rules. The result names the checkpoint it started from and carries one partition per requested backend in the broadcaster's snapshot wire form. A position the history has not stored yet fails with `history_not_yet_stored`, which is retryable. A recorded gap or a boundary checkpoint between the checkpoint and the position, or no checkpoint in its segment, fails with `history_unavailable`. A successful rebuild does not prove the history complete. A gap or a recovery boundary stored late or never can make it differ from the snapshot the broadcaster served.

`POST /jobs/stored-messages` returns the stored messages in `(after, through]` that carry a requested backend, in stream order, each envelope exactly as the broadcaster stored it. Only updates are stored, so positions between them may be messages that were never stored. A range not stored through `through` fails with `history_not_yet_stored`, unless a recorded gap holds the message at `through`, which fails with `history_unavailable`. A boundary checkpoint in the range also fails with `history_unavailable`. A gap elsewhere in the range does not fail the read, so a consumer checks the messages it expects against what came back.

A long session is read as several `stored-messages` jobs. A read that needs more than one job may read fails with `read_limit_exceeded`, which is not retryable, so the consumer narrows the range.

## Manual historical state consistency check

`dsolver-history verify` replaces the old `state-history-check` binary. The CLI sends the request to the service and needs no database URL or AWS credential.

```bash
dsolver-history verify \
  --request crates/dsolver-history/tests/fixtures/verify_request.json \
  --pretty
```

The service checks the selected storage plan and objects, restores the target checkpoint directly, reconstructs the same target from an earlier checkpoint plus deltas, compares canonical state, and compares the selected directed pool quotes.

A pair result is `pass`, `mismatch`, `gap`, or `notComparable`. Matching quote failures are `notComparable`, not a pass. The command writes a short human summary to stderr and JSON to stdout. It exits `0` only when at least one pair was compared and every pair passed. A selected passing range is not proof that all retained history is healthy.

The check is manual. Quote admission, readiness, deployment, and continuous integration do not call it.

## HTTP surface

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/jobs/quote` | Submit a historical quote job. |
| `POST` | `/jobs/consistency-check` | Submit a manual consistency check. |
| `POST` | `/jobs/raw-snapshot` | Submit a raw snapshot read at one stream position. |
| `POST` | `/jobs/stored-messages` | Submit a read of the stored messages in a position range. |
| `GET` | `/jobs/{jobId}` | Poll and possibly consume a terminal result. |
| `POST` | `/jobs/{jobId}/cancel` | Request cancellation without consuming a result. |
| `POST` | `/coverage` | Read reconstruction coverage synchronously. |
| `GET` | `/status` | Read protected service and dependency status. |
| `GET` | `/ready` | Read private-network readiness. |

The checked OpenAPI 3.1 contract is `openapi/historical-pool-quote-api.json`.

## Configuration

The service supports Base chain ID `8453`. The first production backend list is `native,rfq`; `vm` remains disabled until exact VM state export and process isolation are proven.

Required service and authentication settings are `DSOLVER_HISTORY_SERVICE_REVISION`, `DSOLVER_HISTORY_API_REVISION`, `DSOLVER_HISTORY_CHAIN_ID`, `DSOLVER_HISTORY_ENABLED_BACKENDS`, and `DSOLVER_HISTORY_BEARER_KEY_SET_SECRET_ARN`. The last setting is the full base ARN of a Secrets Manager secret, without a version suffix. Its `AWSCURRENT` version contains digests, not bearer tokens.

```json
{"schemaVersion":1,"keys":[{"id":"edson","sha256":"<64 hexadecimal characters>"},{"id":"pedro","sha256":"<64 hexadecimal characters>"}]}
```

The document accepts 1 to 32 keys. IDs are arbitrary non-secret audit and revocation labels supplied entirely by configuration; `edson` and `pedro` are examples, not reserved names. The server derives the ID from the matching digest, so clients never send it. Plaintext tokens remain outside AWS. Adding, rotating, or removing a key publishes a new secret version; the running service reloads it without a deployment.

IDs use lowercase letters, digits, and interior hyphens, are 1 to 63 characters long, and must be unique. SHA-256 values decode to exactly 32 bytes and must also be unique. Unknown fields, unsupported schema versions, malformed IDs or digests, and duplicate IDs or digests stop the service before it listens.

The service reads `AWSCURRENT` before opening the listener, then every 30 seconds. Each read has a 10-second total deadline, including SDK retries. The task role needs `secretsmanager:GetSecretValue` for this one secret. Reads never happen on the request path. A new document replaces the complete active set only after validation; in-flight requests keep the snapshot that authenticated them.

After startup, failed reads or invalid documents keep the last valid set indefinitely. This preserves existing access during an AWS outage, but also delays revocations until a valid refresh succeeds. Restarting during that outage cannot load keys and fails closed. Authentication failures remain `401`; refresh degradation does not change `/ready` or cancel accepted jobs.

Successful authenticated `/status` responses add these headers without changing revision-1 JSON.

| Header | Meaning |
| --- | --- |
| `X-DSolver-History-Auth-Version` | Secret version that authenticated this request. |
| `X-DSolver-History-Auth-Refresh-Age-Seconds` | Whole seconds since that snapshot's last successful refresh. |
| `X-DSolver-History-Auth-Refresh-State` | `degraded` after a failed read or at least 60 seconds without a successful refresh; otherwise `healthy`. |

Unauthorized responses and `/ready` expose none of this metadata. A request admitted just before a replacement may complete with the previous version in its response; a fresh request checks the new snapshot.

### Key management

Follow the [key management guide in solver-iac](https://github.com/dewiz-xyz/solver-iac/blob/main/docs/historical-pool-quote-keys.md). Generate tokens locally, retain the token file privately, and dispatch `Manage Historical Quote API Keys` with only the operation, audit ID, and digest. `list` returns IDs and the current version; `create` adds an ID, `rotate` replaces its digest, and `revoke` removes it. The last key cannot be revoked. The workflow uses OIDC and publishes only digests; no token download or service deployment is involved.

Workflow success proves publication, not application. Use the local probe through the Tailscale Service URL to confirm the expected version, healthy refresh state, and intended token acceptance. Under healthy reads, allow the 30-second polling interval plus read latency; this is not a guaranteed revocation deadline. For rotation without a client interruption, create a second ID, switch the client, then revoke the original ID.

The initial runtime cutover still needs one task deployment. Keep key edits paused, allow active jobs to finish and consume their results, and retain the old task execution role's secret-read permission until the new runtime is verified. Remove that obsolete permission in a separate IaC apply. A later rollback to the old binary must restore its permission first and inject the latest approved valid document, not an earlier version containing revoked credentials.

Replica settings are `DSOLVER_HISTORY_REPLICA_DB_HOST`, `DSOLVER_HISTORY_REPLICA_DB_NAME`, and `DSOLVER_HISTORY_REPLICA_DB_USER`. The user must be `state_history_observer`. The optional port defaults to `5432`, the physical connection limit defaults to `5`, and connection lifetime defaults to one hour. Four connections cover the running-job limit; the fifth keeps protected status and coverage reads available while every job is planning. Each new physical connection gets a fresh RDS IAM login token.

Object-store settings are `DSOLVER_HISTORY_S3_BUCKET`, `DSOLVER_HISTORY_S3_PREFIX`, and `AWS_REGION`. `DSOLVER_HISTORY_S3_ENDPOINT_URL` and `DSOLVER_HISTORY_S3_FORCE_PATH_STYLE` are for compatible local fixtures.

Measured application defaults are listed below. Every setting has a matching `DSOLVER_HISTORY_*` environment override.

| Setting | Default |
| --- | ---: |
| Running jobs | `4` |
| Waiting jobs | `16` |
| Retained terminal jobs | `20` |
| Retained terminal bytes | `1024 MiB` |
| Terminal retention | `3600` seconds |
| Comparison count | `20000` |
| Requested block span | `1800` blocks |
| Per-job decoded-byte reservation | `1024 MiB` |
| Global decoded-byte budget | `4096 MiB` |
| Default deadline | `600000` ms |
| Maximum deadline | `900000` ms |
| Consistency pairs | `100` |
| Quote selectors in a consistency check | `20` |
| Amounts per consistency quote | `20` |
| Structured differences | `100` |
| Replica connections | `5` |

The comparison budget is `startBlockCount * lagCount * inputAmountCount`. A comparison rejection includes all three dimensions, the calculated total, and the maximum. Span and deadline checks run before job creation.

## Benchmark evidence and sizing

The reproducible release benchmark uses checked sanitized native and Bebop RFQ fixtures. It varies start blocks `1, 10, 100`; lags `1, 3, 5, 10`; amounts `1, 5, 20`; delta spans `10, 100, 1800`; and concurrent jobs `1, 2, 4`. The maximum case retains all `1,100` required `StatePoint` values while it applies `1,800` deltas and runs `22,000` quotes, including start quotes.

Run it with these commands.

```bash
cargo bench -p historical-quote --bench replay
cargo bench -p historical-quote-service --bench scheduler
```

The 2026-08-14 run on the local Apple M5 Max produced these bounds.

| Case | Median or range |
| --- | ---: |
| Native, 1,800 fixture deltas | `0.510 ms` |
| RFQ, 1,800 fixture deltas | `0.921 ms` |
| Native, 100 starts x 10 lags x 20 amounts | `7.52 ms` |
| RFQ, 100 starts x 10 lags x 20 amounts | `11.95 ms` |
| Four native maximum-shape retained timelines | `19.35 ms` |
| Four RFQ maximum-shape retained timelines | `31.33 ms` |
| Four scheduler admissions | `9.50 us` |
| Full 16-job waiting queue admission | `38.63 us` |
| One-byte-over-budget rejection | `2.13 us` |
| Four completed and consumed scheduler lifecycles | `75.46 us` |
| Sanitized four-job retained-timeline peak | `79,216,640` resident bytes |

Criterion writes machine-readable estimates under each package's `target/criterion` directory. The benchmark reports fixture input bytes, database rows, checkpoint bytes, retained states, and quote calls. It no longer models replay memory with unrelated byte vectors.

Authorized read-only production measurements on 2026-08-14 used `dsolver-prod` in `eu-central-1`. Aggregate PostgreSQL queries selected byte counts only. One native and one RFQ `1,800`-block workload were fetched and decoded transiently through the production reader; no raw payload, token value, storage object name, block number, or database identifier was retained in the report. Production was still on retained format 1, so the one-off measurement build selected the known format explicitly. That build was removed; the release service keeps the approved hard cutover to format 2.

| Production input or reconstruction | Measured value |
| --- | ---: |
| Largest decoded checkpoint archive | `45,439,526` bytes |
| Largest compressed checkpoint object | `12,550,625` bytes |
| Largest decoded token snapshot | `1,860,923` bytes |
| Largest aligned 1,800-block native delta window | `1,800` rows, `10,060,148` compressed bytes |
| Native plan decoded input | `89,658,411` bytes |
| Native reconstructed states | `1,100` |
| Native planning, object read/decompression, and reconstruction | `87.602 s` |
| Native one-job maximum resident set | `7,817,150,464` bytes |
| Native one-job peak memory footprint | `8,046,403,144` bytes |
| RFQ plan decoded input | `685,408,018` bytes |
| RFQ retained states | `1,099`, plus one gap partial outcome |
| RFQ planning, object read/decompression, and reconstruction | `239.224 s` |
| RFQ one-job maximum resident set | `2,058,043,392` bytes |
| RFQ one-job peak memory footprint | `1,667,680,176` bytes |

The `1,024 MiB` job reservation leaves 36.2% of the reservation unused by the larger measured decoded plan. Four reservations exactly fill the `4,096 MiB` scheduler budget. Actual state retention is bounded separately by the four-job limit and task memory: four times the larger measured one-job footprint is `32,185,612,576` bytes. A `49,152 MiB` task leaves 37.6% headroom over that conservative no-sharing bound. The RFQ sample exercised production retention for 1,099 real `StatePoint` values; its one known-gap outcome is an approved partial result and carries no quote facts.

The deployment sizing input is therefore `8192` CPU units and `49152 MiB` task memory. AWS Fargate pairs 48 GiB with the 8-vCPU tier; the 4-vCPU tier stops at 30 GiB. See the [AWS Fargate CPU and memory combinations](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-tasks-services.html#fargate-task-defs-cpu-memory). The measured reconstruction used `2.75 s` of user CPU and `0.96 s` of system CPU, so memory, rather than CPU time, selects the tier.

The `600000` ms default deadline is 2.5 times the slower completed production reconstruction, and the `900000` ms maximum allows a caller to spend more of the end-to-end budget on queueing or variable replica and object-store latency. Deadlines include queue time, replica planning, object reads, decompression, reconstruction, quotes, and terminal serialization. A separate interior RFQ measurement did not return before the 900-second measurement stop and was excluded from the completed-run ratios; the service fails such work with `deadline_exceeded` rather than extending its resource ownership. Four job planners can each own one SQL connection; the fifth connection is reserved for protected status and coverage reads.

## Image and workflow

Build the unprivileged service image locally.

```bash
docker build \
  -f Dockerfile.historical-quote-service \
  -t historical-quote-service:test .
```

The final image runs as user ID `10001` and contains only the service binary plus TLS runtime packages. The service dependency graph has no private Git dependency, so the image build receives no Git token.

`.github/workflows/build-historical-quote-service.yml` is manual and build only. It uses the dedicated `AWS_HISTORICAL_QUOTE_ECR_ROLE_ARN` GitHub secret and further restricts the assumed session to ECR login plus image pushes to this service's repository. It pushes only the immutable `${{ github.sha }}` tag, then stops. The workflow cannot update ECS, change IAM, run Pulumi, run a consistency check, or deploy the image.

## First rollout checklist

This is a human checklist. No code gate consumes the consistency result.

1. Confirm the deployed image digest and service revision.
2. Run the schema migration on the primary while the old writer still uses the format 1 default.
3. Confirm the read replica sees schema version 2, its cutoff, and `ReplicaLag`.
4. Confirm no primary, Redis, or broadcaster route or credential exists.
5. Deploy the shared replay broadcaster revision, then the API revision, through their approved workflows.
6. Run bounded coverage for the selected Base native and RFQ range.
7. Select at least one recent adjacent checkpoint pair in one segment.
8. Run `dsolver-history verify` for selected native and Bebop pools and amounts.
9. Record `pass`, `mismatch`, `gap`, or `notComparable` with the selected range.
10. Run one grouped quote request with several starts, lags, and amounts.
11. Confirm partial gap behavior and RFQ provenance.
12. Continue the rollout assessment by human judgment.
