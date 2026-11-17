# Tycho 0.525.0: Base broadcasting upgrade

This upgrade targets Tycho **0.525.0** and Base broadcasting.

The branch is based directly on simulator master `45a984c4fd78fdc091e65a8fb313b4b2cd83de8a`, which includes dsolver's required baseline `c94491de6a9a8b48d5dc9e2e0933091d009b3da7`. This retains the current bootstrap API, raw snapshot reassembly, lifecycle cancellation, chain-head/freshness checks, historical services, and Base replay buffer settings. Tycho simulation and common are pinned exactly to `=0.525.0`; the lockfile also resolves client and ethereum to 0.525.0. The agreed execution version remains a workspace pin, with no runtime dependency after encoding removal.

## Client compatibility

`broadcaster-replay-client` exports `SnapshotBootstrap` and `SnapshotBackend`, including `SnapshotBootstrap::into_parts`. `BroadcasterReplayClient::bootstrap` and `token_catalog` retain the current simulator API. Public raw feed messages retain upstream **model** types; the baseline's serde adapters convert them to the upstream feed DTOs only at the wire boundary. No custom substitute DTO API is introduced.

Validation uses an isolated copy of the active dsolver `codex/tycho-0525-router-migration` checkout at `dc353aebbb41eec6fbf01a7fa3de3c242ee074f7`, including its uncommitted migration changes. Only the copy's `broadcaster-replay-client` dependency is redirected to this branch. The user's dsolver checkout is not edited.

Source compatibility does not imply mixed-version state compatibility. Tycho's serialized RFQ states now contain books instead of the former `price_data` and serialized client fields. Deploy matching 0.525.0 consumers and broadcaster together and bootstrap a fresh snapshot on a separate Redis stream. Existing retained RFQ records from older Tycho releases need their matching old reader; this branch explicitly tests rejection of the legacy Bebop state. No automatic historical-data migration is included.

## Base sources

The Base broadcaster subscribes to all 12 on-chain systems advertised by the authenticated Base endpoint during discovery:

- Existing simulator sources: Uniswap V2/V3/V4, PancakeSwap V3, Aerodrome Slipstreams.
- Broadcast-only sources: Aerodrome V1, SushiSwap V2, Aero Basic, Aero CL, PancakeSwap Infinity CL, LunarBase, Ekubo V3.

Aerodrome V1 was already broadcast-only in the master baseline. The other six expand that baseline's coverage. Infinity Bin and Biconomy have canonical identities and decoders registered but remain disabled because the hosted Base endpoint did not advertise them. Ethereum's selected sources are unchanged. The standalone simulator's quoting selection remains explicit in the manifest; downstream consumers own router/executor support.

Base RFQ broadcasting enables **Bebop, Native, and Metric**. Metric uses Tycho's pAMM book feed but travels through the same decoded-state snapshot and Redis contract as the RFQ providers.

The adapter publishes complete serialized states and diffs consecutive complete provider books. Missing pairs are removed. Withdrawal and terminal feed events remove only that provider's liquidity; other providers remain in the snapshot. Tests check replacement, empty books, idempotent withdrawal, provider isolation, and exact quote preservation after serialization.

## Configuration and encoding

Set `CHAIN_ID=8453` and `ENABLE_RFQ_POOLS=true` on the broadcaster. Supply `BEBOP_KEY`, `NATIVE_API_KEY`, and `METRIC_API_KEY` through the runtime secret environment. Native/Metric keys are required only when their broadcaster feeds are enabled. No supplied credentials are committed.

`RFQ_MIN_TVL_USD` defaults to `100` and must be finite and nonnegative. It is independent of the on-chain `TVL_THRESHOLD`, whose units are the native token.

The removed upstream `RFQStreamBuilder` is replaced with public provider feed builders and `BookFeedStreams`. Simulation uses the serialized broadcast books directly. The simulator requires no RFQ provider credentials; only the broadcaster connects to provider feeds.

`POST /encode` is removed and returns `404 Not Found`. Its service, request/response models, firm-quote hydration, route-only state fences, calldata fixtures, and analyzer probes are removed. `/simulate`, `/status`, `/ready`, and broadcaster contracts remain supported. The analyzer writes schema version 8 without encode inspection fields.

The workspace retains `tycho-execution = "=0.525.0"` as the agreed compatibility pin. No package depends on it after removing encoding, so it and its otherwise-unused transitive dependencies are absent from the build graph and lockfile. The private execution fork, vendored source, Cargo patch, and private Git credential plumbing in CI/Docker builds are removed. The historical service builds directly against the shared lockfile. Consumers are responsible for settlement encoding; no executor or router deployment is included.

The cleanup also removes encoder-only allowance resets, native-token permissions, ERC4626 display labels, and the unused publication generation counter. Simulator configuration no longer stores unused provider URLs, and simulator startup no longer validates broadcaster TVL settings. Broadcaster credential and TVL validation remain intact.

Use the updated `simulator-manifest.toml` when deploying. Custom manifests must remove `reset_allowance_tokens` and `native_token_protocol_allowlist` from route policies, and `asset_symbol`/`share_symbol` from ERC4626 pairs; strict unknown-field validation is retained. Addresses and direction permissions still control ERC4626 quote eligibility.

## Verification

Validated after removing `/encode` and cleaning up its remaining dependencies/configuration on 2026-11-17 with Rust 1.95.0:

- `cargo fmt --all -- --check`: passed.
- `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`: passed.
- `cargo nextest run --locked --workspace --all-features --no-fail-fast --test-threads 4`: **926 passed, 102 skipped**. Includes the removed endpoint's 404 contract, RFQ-enabled simulator startup without provider credentials on Base/Ethereum, independent broadcaster TVL validation, and broadcaster/replay coverage.
- `cargo test --locked --workspace --all-features --doc`: passed (no doctests defined).
- Lockfile inspection: simulation/common/client/ethereum each resolve once to crates.io `0.525.0`; no execution package, Git dependency, or local execution patch remains.
- The earlier `cargo build --locked --workspace --bins --release` attempt was blocked by local disk exhaustion (`No space left on device`), including after removing this checkout's generated debug artifacts. Test-profile binaries compiled and all active tests passed; the release build still needs CI validation.

Additional verification recorded before the encoding removal for the broadcasting upgrade:

- Opt-in simulation amount/gas baseline: passed unchanged for Uniswap V2/V3/V4, PancakeSwap V3, and Aerodrome Slipstreams.
- Opt-in live Base Native/Metric test: passed. Three Metric books (WETH/USDC, cbBTC/USDC, WETH/cbBTC) and two Native books (WETH/USDC, USDC/cbBTC) preserve spot prices exactly through serialization. Counts describe this test token universe, not total provider coverage. No firm quote or transaction was requested.
- Isolated downstream workspace compile with all targets/features, formatting, and public documentation checks: passed. Its lockfile contains one version of simulation/common/client/ethereum, all 0.525.0.

The downstream full script is not clean: an existing arithmetic-side-effects lint fails at `dsolver-cow-adapter/src/infra/encode/fork.rs:142`. Its recorded Base route selection and settlement-calldata golden tests also fail identically with the original vendored client, so replacing the replay client does not introduce those failures. The isolated evaluator test harness initially lacked Git metadata; a local empty validation commit supplies it without touching the active dsolver checkout, after which all 25 evaluator tests pass (one skipped).

Docker is unavailable locally, so container-backed PostgreSQL/MinIO storage checks are not run. No production deployment or configuration change is part of this branch update.

## Primary sources

The published Tycho 0.525.0 crate identifies upstream commit `3158f791d3372f7534459d10179d1290dd520328`:

- [Book feeds and lifecycle](https://github.com/propeller-heads/tycho/tree/3158f791d3372f7534459d10179d1290dd520328/crates/tycho-simulation/src/book)
- [Provider RFQ implementations](https://github.com/propeller-heads/tycho/tree/3158f791d3372f7534459d10179d1290dd520328/crates/tycho-simulation/src/rfq/protocols)
- [Metric implementation](https://github.com/propeller-heads/tycho/tree/3158f791d3372f7534459d10179d1290dd520328/crates/tycho-simulation/src/pamm/protocols/metric)
- [Upstream feed DTO boundary](https://github.com/propeller-heads/tycho/blob/3158f791d3372f7534459d10179d1290dd520328/crates/tycho-client/src/feed/dto.rs)
