# Redis Streams integration

The goal is to make the broadcaster the owner of Tycho and RFQ ingestion, publish a durable stream contract for consumers, and keep the simulator focused on rebuilding local quote state from broadcaster-owned feeds.

## Runtime shape

The broadcaster owns upstream ingestion:

- native and VM pool state comes from Tycho raw stream ingestion
- RFQ pool state comes from the RFQ provider stream builders
- token metadata is loaded by the broadcaster and exposed through `/tokens/snapshot` and `/tokens/lookup`
- snapshots, live updates, and heartbeats are serialized with the shared broadcaster wire contract

The simulator owns local quote state:

- it subscribes to broadcaster sessions for enabled backends
- it applies snapshot chunks before treating a backend as bootstrapped
- it applies live updates to the matching local state store
- quote and encode paths continue to read from in-memory native, VM, and RFQ stores

Redis Streams sits between those two service responsibilities. It gives us a durable append log for broadcaster envelopes and a pointer to the latest complete snapshot segment. The existing HTTP snapshot-session and websocket contract remains the service-facing shape while we introduce the Redis transport behind it.

## Broadcaster contract

Each stream entry represents one serialized `BroadcasterEnvelope`.

The Redis entry fields are:

- `schema_version`
- `chain_id`
- `stream_id`
- `message_seq`
- `kind`
- optional `snapshot_id`
- `backend_scope`
- optional `block_number`
- `event_time_ms`
- `payload_json`

Contract invariants:

- `schema_version` must match the supported Redis stream schema version
- `stream_id` and `message_seq` must match the serialized envelope in `payload_json`
- `message_seq` starts at `1`, and only a `snapshot_start` entry can carry sequence `1` as the first message in a generation
- snapshot and heartbeat entries carry `snapshot_id`
- `backend_scope` must match the backends represented by the payload
- native and VM chain block numbers stay attached to the backend partition that produced them
- RFQ partitions carry Tycho RFQ update timestamps/cursors through the existing partition progress field; those values are not chain blocks
- entry `block_number` is a chain-block summary only: it is present only when native/VM payload partitions provide one unambiguous chain block, and when present it must match that chain block
- RFQ-only entries, heartbeat entries, snapshot boundary entries, and divergent native/VM payloads omit entry `block_number`
- payload kind, snapshot id, chain id, and backend scope are validated before an entry is accepted

### Payload boundary

Redis currently stores `payload_json` as a serialized `BroadcasterEnvelope`. That is intentional for this PR because it preserves the existing HTTP snapshot-session and websocket envelope contract while the Redis transport is introduced behind it.

The trade-off is that snapshot chunks and live updates can still carry `BroadcasterStateEntry` or `BroadcasterStateDelta` values with `state: Box<dyn ProtocolSim>`. Redis consumers that deserialize `payload_json` are therefore coupled to the broadcaster's current trait-object serialization shape. Before Redis Streams becomes a live consumer contract, evaluate replacing `payload_json` with a stable typed payload, DTO payload, or per-protocol payload encoding so consumers do not depend on `ProtocolSim` trait-object serialization.

The snapshot pointer is stored separately from the event stream. It records:

- `schema_version`
- `chain_id`
- `stream_key`
- `stream_id`
- `snapshot_id`
- `snapshot_start_entry_id`
- `snapshot_end_entry_id`
- `live_cursor_entry_id`
- `completed_at_ms`

The pointer only moves after a complete snapshot range has been written. `live_cursor_entry_id` must match the snapshot end entry, so a consumer can bootstrap from the pointed range and then continue reading live entries after that cursor.

## Redis lifecycle

For a healthy generation, the broadcaster writes:

1. `snapshot_start`
2. zero or more `snapshot_chunk` entries
3. `snapshot_end`
4. the snapshot pointer for that complete range
5. live `update` and `heartbeat` entries for the same `stream_id`

Redis appends are part of the broadcaster publication path. If append fails, the broadcaster retries inside `BROADCASTER_REDIS_APPEND_RETRY_WINDOW_MS`. During that retry window, later deltas for the same generation must wait behind the failed append so the stream cannot skip a message sequence.

The same publication gate protects cache mutation, Redis append ordering, and subscriber broadcast. That means root websocket delivery also inherits Redis append latency for the current publisher path; a later background publisher queue can relax this coupling if operational data says it is too expensive.

If the retry window is exhausted, the broadcaster treats the Redis publisher generation as unhealthy, reports root `/status` as unavailable, starts a fresh Redis generation with a new `stream_id` and `snapshot_id`, exports a new snapshot from the current in-memory cache, and resumes publication from the new stream position. Redis failures do not clear the broadcaster cache.

Redis append retries use explicit Redis Stream entry IDs derived from the publisher generation and `message_seq`. If an append is accepted but the client sees an error, the retry reuses the same entry ID and checks that entry before treating the append as failed.

Retention must preserve the latest complete snapshot range. Before a consumer trusts the snapshot pointer, it checks that the oldest retained Redis entry is not newer than the pointer's `snapshot_start_entry_id`. If retention has already trimmed past that start entry, the consumer rejects the pointer and waits for a fresh snapshot.

The broadcaster reads and validates the Redis retention knobs, but it does not trim the stream or apply `XADD MAXLEN`; retention cleanup is deferred to follow-up Redis operations work.

## Broadcaster service changes

The broadcaster keeps separate native/VM and RFQ ingestion internally, but public broadcaster status is rooted at `/status`:

- root `/status` reports native, VM, RFQ, and Redis publisher health
- root `/snapshot-sessions` and `/ws` remain native/VM-only for this publisher path
- broadcaster `/rfq/status`, `/rfq/snapshot-sessions`, and `/rfq/ws` are intentionally removed

Root `/status` reports `503` until every enabled backend is snapshot-ready and the Redis publisher is healthy. Root snapshot sessions can still be created from the native/VM cache when that cache is ready.

RFQ ingestion moves from the simulator into the broadcaster:

- broadcaster config loads RFQ provider URLs, credentials, and token sources
- the broadcaster builds the RFQ stream when `ENABLE_RFQ_POOLS=true` and the chain has RFQ protocols
- RFQ updates are timestamped by Tycho's RFQ stream and stored in a broadcaster cache configured only for the `rfq` backend
- RFQ snapshots and heartbeats use the same envelope lifecycle as native and VM snapshots

The broadcaster still serves token metadata as the authority for the simulator. RFQ-only tokens are merged into the broadcaster token view before RFQ stream construction, so the simulator does not need provider-specific token bootstrap logic.

## Simulator service changes

The simulator no longer builds RFQ provider streams directly. It subscribes to the broadcaster for every enabled backend:

- native is always planned
- VM is added when VM pools are effectively enabled
- RFQ is added when RFQ pools are effectively enabled

Native and VM continue to use the root broadcaster session paths. The broadcaster RFQ session paths are removed before the simulator runtime handoff is updated, so simulator RFQ websocket handoff is intentionally broken until the follow-up runtime work lands.

Readiness follows the backend source:

- native readiness requires the native broadcaster subscription to be connected, bootstrapped, and fresh
- VM readiness requires its broadcaster subscription, local state readiness, rebuild state, and freshness
- RFQ readiness requires its broadcaster subscription, local RFQ state readiness, and freshness
- root service readiness remains native-first; optional VM and RFQ degradation is reported at the backend level

The simulator also splits VM and RFQ rebuild gates. A VM-only quote blocks VM rebuilds but does not block RFQ rebuilds. An RFQ-only quote blocks RFQ rebuilds but does not block VM rebuilds. Mixed routes acquire both guards.

## Simulator implementation specification

The simulator change is a consumer-side refactor. The simulator should stop owning RFQ provider ingestion and should consume all enabled state through broadcaster subscriptions. Native and VM keep their current state-store behavior, but their subscription code becomes the pattern for RFQ as well.

### Scope

Historical simulator handoff requirements:

- add an RFQ broadcaster subscription status to `AppState`
- remove simulator-owned RFQ stream construction and RFQ provider token bootstrap
- keep the token store served by the broadcaster as the simulator's only token metadata source
- update simulator RFQ subscription handoff for the new broadcaster surface when RFQ pools are effectively enabled
- hydrate `rfq_state_store` from decoded RFQ snapshot partitions and live update partitions
- report RFQ readiness from broadcaster subscription state plus local RFQ store freshness
- keep native readiness as the service-level readiness gate
- keep quote behavior for RFQ pools, but mark RFQ unavailable unless the RFQ broadcaster subscription is ready
- reject RFQ encode/resimulation paths until signed RFQ quote generation is supported from broadcaster RFQ snapshots
- split VM and RFQ rebuild guards so one backend's reconnect does not block routes that only use the other backend

Non-goals:

- do not change `/simulate` or `/encode` response shapes except for existing RFQ readiness metadata
- do not add compatibility paths for the old simulator-local RFQ stream
- do not keep provider-specific RFQ token stores in the simulator
- do not restore broadcaster `/rfq/*` routes on this publisher branch

### App state

`crates/runtime/src/models/state.rs` should carry separate subscription and rebuild state for each optional backend.

Add:

- `rfq_broadcaster_subscription: BroadcasterSubscriptionStatus`
- `rfq_simulation_rebuild_gate: Arc<RwLock<()>>`
- `vm_simulation_rebuild_gate: Arc<RwLock<()>>`

Remove:

- `rfq_stream: Arc<RwLock<RfqStreamStatus>>`
- `RfqStreamStatus`
- the shared `simulation_rebuild_gate`

`SimulationRebuildGuard` should hold independent optional read guards:

- VM read guard when a request uses VM pools
- RFQ read guard when a request uses RFQ pools
- both guards when a request uses both
- no guard for native-only requests

The guard should expose a helper that verifies it blocks the required rebuilds for a route. Encode resimulation uses that check before looking up route pools.

### Simulator startup

`crates/runtime/src/simulator_service.rs` should build only the shared token store served by the broadcaster.

Remove simulator-side RFQ bootstrap work:

- Bebop token loading
- Hashflow CSV token loading
- Liquorice token loading
- `RFQTokenStores` construction
- `RFQConfig` construction
- `spawn_rfq_stream_task`
- calls to `supervise_rfq_stream`

`StreamResources` should still include:

- `native_state_store`
- `vm_state_store`
- `rfq_state_store`
- `native_stream_health`
- `vm_stream_health`
- `rfq_stream_health`
- `vm_stream`

It should not include `rfq_stream`.

`build_app_state` should initialize:

- `native_broadcaster_subscription`
- `vm_broadcaster_subscription`
- `rfq_broadcaster_subscription`
- separate VM and RFQ rebuild gates
- `configured_backends.rfq` from the chain manifest
- `enable_rfq_pools` from the effective runtime flag and manifest support

### Subscription planning

`spawn_broadcaster_subscription_task` should derive a small plan from `AppState`:

1. Always spawn native.
2. Spawn VM when `enable_vm_pools` is true.
3. Spawn RFQ when `enable_rfq_pools` is true.

Use a local enum for the plan, for example:

- `Native`
- `Vm`
- `Rfq`

Each plan entry should call one backend-specific constructor for `BroadcasterSubscriptionControls`. The RFQ constructor should use:

- `app_state.rfq_broadcaster_subscription`
- `resources.rfq_state_store`
- `resources.rfq_stream_health`
- `app_state.tokens`
- `config.chain_profile.rfq_protocols`
- `app_state.rfq_simulation_rebuild_gate()`

All three backends should continue to use the same `supervise_broadcaster_subscription` loop. The backend controls decide which paths and rebuild behavior apply.

### Subscription URLs

`crates/runtime/src/models/broadcaster_urls.rs` should continue deriving paths from `TYCHO_BROADCASTER_WS_URL`.

Root native and VM subscriptions use:

- HTTP create: `snapshot-sessions`
- HTTP payload: `snapshot-sessions/{session_id}/payloads/{index}`
- websocket attach: original `/ws?sessionId=...`

RFQ broadcaster subscription URL handling is deferred to the follow-up runtime handoff work. This branch intentionally does not expose RFQ snapshot-session or websocket routes.

### Subscription processor

`crates/runtime/src/services/broadcaster_subscription.rs` should add `BroadcasterSubscriptionControls::Rfq`.

RFQ controls should provide the same common dependencies as native and VM:

- subscription status
- state store
- stream health
- token store
- protocol list
- rebuild gate

Backend-specific behavior:

- native uses root snapshot paths and root websocket attach
- VM uses root snapshot paths and root websocket attach
- RFQ subscription path selection is part of the follow-up runtime handoff; this publisher branch does not define live RFQ snapshot or websocket routes
- VM resets acquire the VM rebuild gate and reset only VM state
- RFQ resets acquire the RFQ rebuild gate and reset only RFQ state
- native resets do not acquire VM or RFQ rebuild gates

Snapshot bootstrap should work the same way for RFQ as for native and VM:

1. Create a snapshot session.
2. Fetch all HTTP payloads with the existing bounded concurrency.
3. Feed payloads through `BroadcasterSubscriptionTracker`.
4. Apply snapshot partitions to the backend's state store.
5. Connect the session websocket and process live messages.

RFQ partitions must be decoded state partitions. Raw RFQ broadcaster protocol-message partitions are unsupported and should fail explicitly. That avoids silently accepting raw RFQ payloads that the simulator cannot reassemble into RFQ state.

On any subscription failure:

- mark the backend subscription disconnected with the last error
- increment the backend subscription restart count
- hold the backend rebuild gate until the next successful bootstrap completes
- reset only that backend's state store
- preserve other backend stores and route guards

When a rebuild is already pending, the next reset should reuse the existing rebuild state instead of acquiring a second write guard.

### Readiness and status

Native readiness:

- `warming_up` until the native broadcaster subscription is connected and snapshot bootstrap is complete
- `warming_up` until native state store is ready
- `stale` when native updates exceed the readiness freshness window
- `ready` otherwise

VM readiness:

- `disabled` when VM pools are disabled
- `rebuilding` when the VM rebuild flag is set
- `warming_up` when VM broadcaster subscription is disconnected or snapshot bootstrap is incomplete
- `warming_up` when VM state store is not ready
- `stale` when VM updates exceed the readiness freshness window
- `ready` otherwise

RFQ readiness:

- `disabled` when RFQ pools are disabled
- `warming_up` when RFQ broadcaster subscription is disconnected or snapshot bootstrap is incomplete
- `warming_up` when RFQ state store is not ready
- `stale` when RFQ updates exceed the readiness freshness window
- `ready` otherwise

RFQ no longer has a separate simulator-local `rebuilding` state. RFQ reconnects are represented as subscription warming-up with restart count and last error on the subscription/status payload.

`GET /status` should include RFQ backend status when RFQ is configured for the chain. The RFQ backend status should include:

- `enabled`
- `readiness`
- `reason`
- `update_timestamp`
- `pool_count`
- `restart_count`
- `last_error`
- `last_update_age_ms`
- `subscription`

For RFQ status, `update_timestamp` is the current RFQ update timestamp/cursor. RFQ backend status does not expose `block_number`.

The top-level simulator status remains native-first. Optional VM and RFQ backend degradation should not turn a native-ready service into top-level warming-up unless native itself is warming or stale.

### Quote path

`crates/runtime/src/services/quotes.rs` should keep RFQ as an optional candidate source.

Required quote behavior:

- compute `initial_rfq_ready` through `AppState::rfq_readiness`
- skip RFQ candidates when RFQ pools are enabled but RFQ readiness is not `ready`
- set `meta.rfq_unavailable=true` when RFQ was enabled and skipped
- set `meta.rfq_update_timestamp` only when RFQ readiness is `ready`; the field carries the current RFQ update timestamp/cursor, not a chain block
- keep native readiness as the hard request gate
- keep VM and RFQ degradation as optional backend degradation when native can still quote
- keep directional RFQ filtering unchanged
- continue reporting RFQ scheduling and gas metrics only for RFQ candidates actually scheduled

The quote service should not call any RFQ provider directly. RFQ components in `rfq_state_store` are the only RFQ source visible to quote computation.

### Encode path

RFQ route encoding is not supported by this simulator change.

Required encode behavior:

- detect RFQ route usage from protocol hints and component metadata
- return a clear unavailable error before resimulation for routes that explicitly use RFQ
- return the same unavailable error if pool lookup discovers that a route pool belongs to `rfq_state_store`
- keep VM encode behavior unchanged
- use the split rebuild guard check for VM/RFQ route requirements before pool lookup

The error message should be explicit that RFQ signed quote generation is not supported from broadcaster RFQ snapshots yet. This is a deliberate execution boundary, not a readiness failure.

### Pool metadata

Pool descriptors should use canonical protocol names when possible.

For RFQ components that only carry a type-name signal, derive the canonical RFQ protocol from `ProtocolKind` rather than passing through an empty or ambiguous protocol string. This keeps reports and quote logs readable after RFQ state comes from broadcaster snapshots.

### Tests

Add focused tests around repo-owned behavior:

- subscription planning includes native plus RFQ for a Base profile with RFQ effectively enabled
- app state initializes RFQ broadcaster subscription status
- RFQ snapshot partitions hydrate `rfq_state_store`
- RFQ live update partitions advance `rfq_state_store` and RFQ stream health
- raw RFQ snapshot message partitions fail explicitly
- RFQ subscription reset waits on the RFQ rebuild gate and resets only RFQ state
- VM subscription reset does not wait on RFQ route guards
- rebuild guards block only the backend used by the route, and block both for mixed VM/RFQ routes
- RFQ readiness requires connected and bootstrapped RFQ broadcaster subscription
- RFQ status reports subscription restart count and last error
- quote path marks RFQ unavailable when RFQ is enabled but not ready
- encode path rejects RFQ routes with the unsupported signed-quote message
- RFQ pool descriptors use canonical RFQ protocol names

Validation commands for the implementation PR should include:

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets --all-features`
- `cargo test --workspace --all-features`
- the repo's normal local simulator smoke path if the branch changes runtime startup or service wiring

## Config surface

Redis configuration is explicit and fail-fast:

- `BROADCASTER_REDIS_URL`
- `BROADCASTER_REDIS_STREAM_KEY`
- `BROADCASTER_REDIS_SNAPSHOT_KEY`
- `BROADCASTER_REDIS_BLOCK_MS`
- `BROADCASTER_REDIS_READ_COUNT`
- `BROADCASTER_REDIS_APPEND_RETRY_WINDOW_MS`
- `BROADCASTER_REDIS_RETENTION_SECS`
- optional `BROADCASTER_REDIS_MAXLEN`

Defaults:

- `BROADCASTER_REDIS_BLOCK_MS=5000`
- `BROADCASTER_REDIS_READ_COUNT=128`
- `BROADCASTER_REDIS_APPEND_RETRY_WINDOW_MS=5000`
- `BROADCASTER_REDIS_RETENTION_SECS=300`

The stream and snapshot keys must be different. Redis URLs must use `redis://` or `rediss://`, include a host, and use a valid port when a port is present. `rediss://` is supported through Redis' Tokio rustls transport.

`BROADCASTER_REDIS_RETENTION_SECS` and `BROADCASTER_REDIS_MAXLEN` are validated in this branch so configuration mistakes fail early, but publisher-side stream trimming remains deferred.

The broadcaster service treats Redis as a required dependency at startup. After startup, the publisher uses Redis connection management so ordinary connection drops can reconnect without restarting the broadcaster process.

## Operational contract

Important status surfaces:

- broadcaster root `/status`: native, VM, RFQ, and Redis publisher readiness; RFQ backend progress is `update_timestamp`, not `block_number`
- simulator `/status`: consumer readiness for native, VM, and RFQ local state

Important identifiers:

- `stream_id` identifies one process-unique broadcaster generation
- `snapshot_id` identifies the complete snapshot inside that generation
- `message_seq` is the ordered broadcaster sequence for a generation
- Redis entry IDs identify persisted stream positions, not the service-level message order

Operational guidance:

- alert on repeated Redis append retry exhaustion, because that causes a new Redis generation
- alert when the snapshot pointer is unusable because retention trimmed past the latest snapshot start
- if Redis data is flushed while the broadcaster process stays up, restart the broadcaster or force a generation reset so it publishes a fresh pointer
- treat RFQ readiness as part of root broadcaster `/status` until follow-up runtime work restores end-to-end simulator RFQ consumption
- use `stream_id`, `snapshot_id`, and `message_seq` together when correlating Redis entries, websocket messages, and simulator logs
