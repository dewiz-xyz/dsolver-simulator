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

Aerodrome V1 was already broadcast-only in the master baseline. The other six expand that baseline's coverage. Infinity Bin and Biconomy have canonical identities and decoders registered but remain disabled because the hosted Base endpoint did not advertise them. The standalone simulator's quoting selection remains explicit in the manifest; downstream consumers own router/executor support.

Base RFQ broadcasting enables **Bebop, Native, and Metric**. Metric uses Tycho's pAMM book feed but travels through the same decoded-state snapshot and Redis contract as the RFQ providers.

The adapter publishes complete serialized states and diffs consecutive complete provider books. Missing pairs are removed. Temporary withdrawals remove only that provider's liquidity. A terminal feed event withdraws that provider’s books and reconnects its feed after a delay. Peers continue diffing their own books throughout the restart. Tests check replacement, empty books, idempotent withdrawal, provider isolation, and exact quote preservation after serialization.

## Ethereum sources

The authenticated Ethereum beta endpoint advertised 23 on-chain systems during discovery. With Balancer V2 and V3 disabled, the broadcaster subscribes to 21 of them: 18 native systems and three VM systems (Curve, Maverick V2, and LiquidityParty). The nine enabled additions are `uniswap_v4`, `uniswap_v4_hooks`, `ring_swap_v2`, `sky`, `lido_v4`, `etherfi`, `aero_basic`, `aero_cl`, and `vm:liquidityparty`. Replay decoder registration includes the hybrid Balancer V3 state and LiquidityParty's killed-pool filter. Balancer V2 is also removed from the standalone simulator's selected VM set. New sources use the manifest's broadcaster-only lists.

Direct book providers are **Bebop, Native, Metric, and Hashflow**. Hashflow uses the existing provider token CSV and keeps each maker's book identity. Its credentials are checked at broadcaster startup.

The **eight Titan propAMMs** are **FermiSwap, Kipseli, Bebop, TaurusFi, Tempest, Goliath, Stelaxis, and ElZorro**. They are selected explicitly from Tycho 0.525.0's named registry. Metric is omitted from Titan because it is already subscribed directly. Unknown/new Titan venues are not automatically enabled. Fermi uses Titan's current router `0xf6d5b2fefec7b420d9990b95790adfd3d4c4f977`, replacing the old address in the pinned crate. Gas estimates otherwise remain the upstream estimates; no new execution-gas calibration is claimed.

Titan's incremental updates are multiplexed with the complete-book provider adapter. They retain `pricelevelstream:<venue>` identities and explicit upstream removals, use observed Unix milliseconds for the shared RFQ cursor, and preserve the upstream simulation overrides. Fallback-router identities are disabled because downstream consumers choose the execution path.

Tycho skips its process-local Titan quote deadline during ordinary serde. The broadcaster's state wire adapter adds `quote_expires_at_unix_ms` and reconstructs the remaining monotonic lifetime on receipt, capped at the upstream quote TTL. Delayed snapshots and replay therefore cannot renew expired liquidity. A Titan state with no deadline is rejected. Broadcaster and consumer hosts need synchronized clocks, and consumers must use this updated replay client; older readers do not enforce the new wire deadline. New history checkpoints and deltas use format 2, which registers the added protocols for historical reconstruction. Format 1 retains its original on-chain decoder selection.

## Configuration and encoding

Set `CHAIN_ID=8453` and `ENABLE_RFQ_POOLS=true` on the broadcaster. Supply `BEBOP_KEY`, `NATIVE_API_KEY`, and `METRIC_API_KEY` through the runtime secret environment. For Ethereum, set `CHAIN_ID=1` and also supply `HASHFLOW_USER` and `HASHFLOW_KEY`. Native/Metric keys are required only when their broadcaster feeds are enabled. Titan needs no API key; its two public WebSocket URLs can be overridden with `TITAN_PAMM_PRICE_LEVEL_URL` and `TITAN_PAMM_QUOTE_STREAM_URL`. No supplied credentials are committed.

`RFQ_MIN_TVL_USD` defaults to `100` and must be finite and nonnegative. It applies to direct book providers, not Titan, and is independent of the on-chain `TVL_THRESHOLD`, whose units are the native token.

The removed upstream `RFQStreamBuilder` is replaced with public provider feed builders and a `BookFeedStream` per provider. Simulation uses the serialized broadcast books directly. The simulator requires no RFQ provider credentials; only the broadcaster connects to provider feeds.

`POST /encode` is removed and returns `404 Not Found`. Its service, request/response models, firm-quote hydration, route-only state fences, calldata fixtures, and analyzer probes are removed. `/simulate`, `/status`, `/ready`, and broadcaster contracts remain supported. The analyzer writes schema version 8 without encode inspection fields.

The workspace retains `tycho-execution = "=0.525.0"` as the agreed compatibility pin. No package depends on it after removing encoding, so it and its otherwise-unused transitive dependencies are absent from the build graph and lockfile. The private execution fork, vendored source, Cargo patch, and private Git credential plumbing in CI/Docker builds are removed. The historical service builds directly against the shared lockfile. Consumers are responsible for settlement encoding; no executor or router deployment is included.

The cleanup also removes encoder-only allowance resets, native-token permissions, ERC4626 display labels, and the unused publication generation counter. Simulator configuration no longer stores unused provider URLs, and simulator startup no longer validates broadcaster TVL settings. Broadcaster credential and TVL validation remain intact.

Use the updated `simulator-manifest.toml` when deploying. Custom manifests must remove `reset_allowance_tokens` and `native_token_protocol_allowlist` from route policies, and `asset_symbol`/`share_symbol` from ERC4626 pairs; strict unknown-field validation is retained. Addresses and direction permissions still control ERC4626 quote eligibility.

## Verification

Validated after adding Ethereum broadcasting on 2026-11-18 with Rust 1.95.0:

- Balancer-disable follow-up: 68 existing configuration, decoder-registration, and simulator-startup tests passed.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`: passed.
- `cargo nextest run --locked --workspace --all-features --no-fail-fast --test-threads 4` passed all 935 active tests after review, with 105 skipped. Coverage includes retained V2 checkpoints and deltas, independent RFQ provider restarts, standalone subscription filtering, expired Titan books, and the removed endpoint's 404 contract.
- `cargo test --locked --workspace --all-features --doc`: passed (no doctests defined).
- Lockfile inspection: simulation/common/client/ethereum each resolve once to crates.io `0.525.0`; no execution package, Git dependency, or local execution patch remains.
- The local all-binaries release build ran out of disk space. GitHub Actions built the simulator, broadcaster, migration, historical quote service, and history CLI release targets on the preceding revision. [Build log](https://github.com/dewiz-xyz/dsolver-simulator/actions/runs/44275680962/job/134703061543)

Live Ethereum read-only checks on 2026-11-18:

- Native/Metric: passed. With a WETH/USDC/WBTC token universe, Native published USDC/WETH, while Metric published WETH/USDC, WBTC/USDC, and WBTC/WETH books. Spot prices survive serialization exactly.
- Hashflow: passed using runtime-only credentials. Maker books survive the RFQ broadcast wire path with unchanged spot prices. No firm quote or transaction was requested.
- Titan: seven venues passed live spot-price, amount, and gas parity checks through serialized updates in the final 45-second window: FermiSwap, Kipseli, Bebop, Tempest, Goliath, Stelaxis, ElZorro. The strict eight-venue live test timed out waiting for TaurusFi; it was listed in `titan_getPammVenues` but absent from `titan_getPammBooks` on follow-up. An earlier run observed TaurusFi spot-price parity, then exposed a test assumption that every book had liquidity in both directions. The test now checks both directions and preserves refusals as well as available quotes. All eight remain explicitly registered; live availability is provider-dependent.
- No full live Tycho on-chain bootstrap or production deployment was performed. On-chain coverage is verified against discovery and decoder registration tests.

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

- [Titan venue registry and public feeds](https://docs.titanbuilder.xyz/propamms)
- [Pinned Titan stream configuration](https://github.com/propeller-heads/tycho/blob/3158f791d3372f7534459d10179d1290dd520328/crates/tycho-simulation/src/price_level_stream/config.rs)
