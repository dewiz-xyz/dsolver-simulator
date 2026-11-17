# Tycho simulation upgrade research

Checked 2026-11-17 against baseline `97a2d50`, the live crates.io API, published crate sources, and the authenticated Base endpoint. The Base broadcasting implementation described below is now present in this checkout; no deployment has been made.

## Implemented Base scope

- Pin simulation/common/client/ethereum to 0.526.0. Preserve the execution fork with a documented local compatibility patch at `vendor/tycho-execution`.
- Enable all 12 advertised Base on-chain systems. The seven additions are Sushi V2, Aerodrome V1, Aero Basic, Aero CL, Pancake Infinity CL, LunarBase, and Ekubo V3. Infinity Bin and Biconomy are registered but not enabled.
- Enable Native and Metric alongside Bebop in the Base RFQ profile. Use complete serialized protocol-state books, the same snapshot cache and Redis publishing contract as Bebop, and per-provider removals on replacement, withdrawal, and termination.
- Require broadcaster-only `NATIVE_API_KEY` and `METRIC_API_KEY` when those providers are enabled. Use `RFQ_MIN_TVL_USD=100` by default, independent of on-chain TVL units.
- Preserve existing Bebop and Hashflow signing through public book assemblers, keeping original pricing state separate from the local signing client. Native/Metric settlement remains explicitly unsupported in `/encode`.
- Preserve the raw feed's JSON DTO shape through explicit conversions to upstream internal models. Serialized protocol states changed across this dependency upgrade, so downstream consumers must upgrade together and bootstrap fresh snapshots; the outer transport schema alone does not establish compatibility.

Authenticated Base discovery returned 12 of 12 systems in one page: `uniswap_v2`, `uniswap_v3`, `uniswap_v4`, `pancakeswap_v3`, `aerodrome_slipstreams`, `aerodrome_v1`, `sushiswap_v2`, `aero_basic`, `aero_cl`, `lunarbase`, `pancakeswap_infinity_cl`, `ekubo_v3`. It returned no DCI protocols.

The opt-in live test passed with real Base feeds using a WETH/USDC/cbBTC token universe: three Metric books (WETH/USDC, cbBTC/USDC, WETH/cbBTC) and two Native books (WETH/USDC, USDC/cbBTC) survived broadcaster serialization with identical spot prices. These are probe counts for that token universe, not total venue coverage. No firm quotes or transactions were requested; credentials are not stored in this checkout.

Validation completed locally on macOS with Rust 1.97.1:

- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed.
- `cargo nextest run --workspace --no-fail-fast`: 656 passed, 3 opt-in tests skipped.
- Opt-in `live_base_native_and_metric_broadcast`: passed separately with provider credentials.
- `cargo build -p apps --bin dsolver-simulator-service --bin dsolver-tycho-broadcaster-service --release --locked`: passed.

The two existing opt-in external tests and Linux Docker images were not exercised. No live on-chain decode sweep or settlement validation was performed for newly added venues.

The remaining sections record the broader upgrade research and distinguish implemented scope from future work.

## Target release and dependency alignment

The latest non-yanked stable release returned by crates.io is **`tycho-simulation 0.526.0`**, published **2026-11-17 08:28:09 UTC**. The baseline requested `0.258.0`. The upstream project moved into the `propeller-heads/tycho` monorepo: the old `propeller-heads/tycho-simulation` repository still reports `0.258.0` as its latest GitHub release, so checking that repository alone gives the wrong upgrade target. [Registry metadata](https://crates.io/api/v1/crates/tycho-simulation), [current release](https://github.com/propeller-heads/tycho/releases/tag/0.526.0), [old release](https://github.com/propeller-heads/tycho-simulation/releases/tag/0.258.0).

Published source pins:

| Package | Source commit from `.cargo_vcs_info.json` | Crate checksum |
| --- | --- | --- |
| `tycho-simulation 0.258.0` | `dc2b7fb2d609d14f6b02bb4081096de0305a5d6e` in `propeller-heads/tycho-simulation` | `fbb7576a7147305f8478b3a116828ba0444922a811c124a3cd574b89f5b3554f` |
| `tycho-simulation 0.526.0` | `d0de8be62b40b63c26455acc8667954f38107446` in `propeller-heads/tycho`, `crates/tycho-simulation` | `7bcb70db4017d4b95cfc4aeadfa4a46c22acf3aaa00622846d843783db8f0d05` |

Sources: [baseline package](https://crates.io/api/v1/crates/tycho-simulation/0.258.0/download), [target package](https://crates.io/api/v1/crates/tycho-simulation/0.526.0/download).

`0.526.0` declares Rust **1.91.0**, already the workspace's declared minimum. It depends on `tycho-common`, `tycho-client`, and `tycho-ethereum` **0.526.0**. Its default features are `evm`, `book-feeds`, and `price-level-stream`. Therefore the project's direct `tycho-common = "0.158.0"` must also migrate anywhere values cross the simulation boundary; allowing two common versions to coexist does not make their `Chain`, `Token`, `Bytes`, or simulation traits interchangeable. [Published manifest](https://docs.rs/crate/tycho-simulation/0.526.0/source/Cargo.toml).

The baseline execution dependency was a separate Git fork at `46596c8`. The fork is accessible at the renamed `dewiz-xyz/dsolver-execution`; its next commit, `f4d444414c43d2cecf626139fd1749e11d15cba6`, includes the Bebop settlement-target validation needed by newer simulation. That revision still needs a common-version update, optional wrapped-token handling, and `origin: None` in three quote-parameter constructors. The local source override applies those changes while preserving the fork's router/executor configuration. See [compatibility patch](../vendor/tycho-execution/COMPATIBILITY.md).

## What to add on the two configured chains

The tables below compare against this checkout's `simulator-manifest.toml`, rather than implying every missing venue was absent from the old crate. Availability in source, support by the hosted Tycho service, and executable settlement support are different checks.

### Ethereum: documented missing registrations

| System | Decoder/state | Difference from this checkout |
| --- | --- | --- |
| `uniswap_v4` | `UniswapV4State` | Recognized locally but deliberately disabled for Ethereum; resolve the existing shared-database subscription constraint first. |
| `uniswap_v4_hooks` | `UniswapV4State` | Separate hybrid subscription; currently explicitly rejected by the local protocol classifier. |
| `vm:balancer_v3` | `BalancerV3State` | Add classifier and registration. The old crate already shipped a VM adapter, but the latest release supplies a hybrid implementation. |
| `vm:liquidityparty` | `EVMPoolState<PreCachedDB>` | Add registration with `liquidityparty_killed_pools_filter`. |
| `ring_swap_v2` | `RingSwapV2State` | New native registration. |
| `sky` | `SkyState` | New native registration. |
| `lido_v4` | `LidoV4State` | New native registration. |
| `etherfi` | `EtherfiState` | Missing locally, but its state implementation already exists in `0.258.0`. |
| `aero_basic` | `AeroBasicState` | New native registration. |
| `aero_cl` | `AerodromeSlipstreamsState` | Separate system reusing the Slipstreams implementation. |

Chain/system mapping: [pinned supported-protocol table and registration example][protocol-doc]. State implementations: [target protocol modules][protocol-mod], [baseline protocol modules][old-protocol-mod].

### Base: documented missing registrations

| System | Decoder/state | Difference from this checkout |
| --- | --- | --- |
| `sushiswap_v2` | `UniswapV2State` | Already recognized by the project; add to the Base allowlist. |
| `aerodrome_v1` | `AerodromeV1State` | Add classifier/registration; implementation already exists in `0.258.0`. |
| `pancakeswap_infinity_cl` | `UniswapV4State` | New system with its own pool semantics; use the upstream decoder, not a hand-made V4 alias. |
| `lunarbase` | `LunarBaseState` | New native registration. |
| `aero_basic` | `AeroBasicState` | New native registration. |
| `aero_cl` | `AerodromeSlipstreamsState` | New system reusing the Slipstreams implementation. |

Chain/system mapping: [pinned supported-protocol table and registration example][protocol-doc]. The published [price-printer example][price-printer] also demonstrates Base registrations. Decoder modules are in [the target protocol module index][protocol-mod].

### Further published capabilities requiring explicit availability checks

These would be missed by relying only on the prose supported-protocol table:

| System | Chain evidence | State/feed | Remaining gate |
| --- | --- | --- | --- |
| `pancakeswap_infinity_bin` | Published example registers Base and BSC | `PancakeswapInfinityBinState` | Confirm hosted Base subscription. The latest upstream encoder reuses the Infinity CL executor with a configured BinPoolManager; settlement through our fork remains unverified. The configured Base endpoint does not advertise this system. |
| `biconomy` | Base substreams, Base source tests, Base executor entry | `BiconomyState` | The configured Base endpoint does not advertise this system; leave disabled. |
| `vm:fermiswap` | Ethereum example, adapter, executor entry | `EVMPoolState<PreCachedDB>` | Confirm hosted feed and required price/oracle overrides; deduplicate against Titan's representation of the same venue. |
| `vm:bopamm` | Ethereum example, adapter, executor entry | `EVMPoolState<PreCachedDB>` | Same availability/override/deduplication checks; this is the on-chain Bebop pAMM representation. |
| `ekubo_v3` on Base | Base substreams/test and executor entry | `EkuboV3State` | Existing local decoder, but current supported-protocol documentation does not advertise Base. Authenticated Base discovery confirms this system; it is enabled with the upstream extension filter. |
| `vm:balancer_v3` on Base | Base substreams/test | `BalancerV3State` | Documentation only advertises Ethereum, and target executor registry has no Base Balancer V3 entry. |

Sources: [Infinity Bin registration][price-printer], [Infinity Bin module][infinity-bin], [Biconomy module and Base tests][biconomy], [Base Biconomy substreams][biconomy-substreams], [VM adapters][vm-adapters], [Titan override monitor][override-example], [Base Ekubo substreams][base-ekubo], [Base Balancer substreams][base-balancer], [execution deployments][executors]. Source tests and YAML files establish implementation evidence, not proof that our configured hosted endpoint serves these systems.

`native_wrapper` is a synthetic wrap/unwrap component, not a new indexed DEX. The target `ProtocolStreamBuilder` injects it after the first successful decoded update. This project's raw-feed subscription plus custom decoder must decide whether to reproduce that behavior, and its strict classifier must understand it if a decoded-stream path is used. [Injection implementation][stream]. `cowamm` was already a module in the baseline and is absent from the current documented hosted list; it is not a new source introduced by this upgrade. [Baseline modules][old-protocol-mod].

Protocols restricted to other chains, such as Velodrome Slipstreams on Unichain, Ramses V3 and QuickSwap on Polygon, and Robinhood/Elysium-specific V3 forks, do not belong in this project's Ethereum/Base subscriptions. Adding more chains is a separate scope. [Supported chains and protocols][protocol-doc].

## Off-chain feeds and pAMMs are a separate integration

New source coverage is broader than `TychoStreamBuilder.exchange(...)` subscriptions. The old combined `RFQStreamBuilder` API has been replaced by provider feed builders and complete book snapshots. [Baseline RFQ module][old-rfq], [target RFQ module][rfq], [target book model][book], [published book quickstart][book-example].

| System/family | Scope relevant here | Integration and constraints |
| --- | --- | --- |
| `rfq:bebop`, `rfq:hashflow`, `rfq:liquorice` | Existing source implementations; project currently enables Bebop | Migrate to the corresponding `*FeedBuilder` and `BookFeedStreams`; retain per-provider credentials and current route policy. Liquorice's source feed is Ethereum here. |
| `rfq:native` | Ethereum and Base | New `NativeFeedBuilder`; requires Native API credentials and compatible execution support. |
| `rfq:metric` | Ethereum and Base feed coverage | New `MetricFeedBuilder`; a pAMM book, not a maker-signed RFQ. Requires Metric credentials; check direct versus fallback executor availability by chain. |
| `baibai` | Base | `BaibaiFeedBuilder` polls the venue through JSON-RPC rather than Tycho's block feed. Implement whole-book replacement and withdrawal. |
| `fallback:{venue}` / `pricelevelstream:{venue}` | Ethereum | Titan `PriceLevelStreamBuilder`; default path uses a fallback router, direct path is opt-in. Requires family-aware classification and a fallback route/encoder contract. |

Provider coverage and authentication: [off-chain priced venues][book-doc]. Builder and data contracts: [book quickstart][book-example], [BaiBai feed][baibai-feed], [Titan stream][pricelevel-stream].

Titan's known named venues in this release are **Bebop, ElZorro, FermiSwap, Goliath, Kipseli, Metric, Stelaxis, TaurusFi, and Tempest**. Optional auto-detection adds names based on venue addresses. A fixed enumeration of nine names alone is insufficient for that mode. Components carry `pamm_address`; use it to avoid counting the same inventory twice across VM, RFQ, and price-level paths. [Published venue configuration][pricelevel-config], [stream contract][pricelevel-doc].

Book feeds publish a provider's entire current book set. Withdrawal or feed termination means the provider is no longer servable, and missing books in the replacement snapshot must be removed. Titan instead tracks expiry per component and emits removals; an individual Titan message is not a full venue universe. Keeping these semantics separate is necessary for correct broadcaster replay and stale-liquidity removal. [Book model][book], [snapshot feed contract][snapshot-feed], [Titan tracker][pricelevel-tracker].

BaiBai quotes exclude its priority-fee surcharge, and its reader ends the feed when an implementation proxy no longer matches its pinned implementation. Treat those as actual integration constraints, not a generic AMM registration. [BaiBai source documentation][baibai].

## Existing sources need migration checks too

- **Curve:** the current recommended decoder is hybrid `CurveState`, under the same `vm:curve` system. Use `curve_filter`; it limits rate-bearing/rebasing pools to trusted cases. The hybrid needs RPC access during startup. Keeping the old generic VM decoder should be an explicit compatibility choice with separate validation, not an accidental carryover. [Curve decoder][curve], [filters][filters], [registration example][protocol-doc].
- **Balancer V3:** the new hybrid decoder rejects unsupported pool/hook types. Do not treat all indexed Balancer V3 pools as supported. The old VM `balancer_v3_pool_filter` is a different filter and is not shown on the new hybrid registration example. [Hybrid decoder][balancer-v3], [filters][filters].
- **Uniswap V4 hooks:** the upstream builder filters Angstrom when `ANGSTROM_API_KEY` is absent; custom decoder registration must preserve this policy. Encoding needs fresh attestations. The project already has a deliberate Ethereum V4/VM shared-database restriction, which must be tested before enabling both systems. [Builder][stream], [hook filters][filters], [local manifest](../simulator-manifest.toml).
- **Ekubo V3:** retain the upstream extension filter; the alternate SignedExclusiveSwap filter is only appropriate when encoding can provide the required per-swap signature. [Extension module][ekubo-v3].
- **Fluid/ERC4626/Balancer V2/LiquidityParty:** preserve the current upstream pool exclusions instead of assuming every indexed pool is quoteable. [Filters][filters].
- **Aero/Slipstreams:** target code supports block-context-sensitive fee behavior. Aero Basic/CL quotes assume no MEV tax, and the default Slipstreams decoder conservatively handles uncertain first-in-block fees. Review fee assumptions against the actual transaction submission path. [Aero Basic][aero-basic], [Slipstreams decoder][slipstreams].

## Verification boundary

The investigation inspected both published simulation packages, registry metadata, pinned upstream documentation/examples, executor configuration, and authenticated Base protocol discovery. The live provider smoke test verifies the bounded RFQ book sample described above. It does not establish full venue coverage, on-chain pool decode health, or settlement support for new venues. No production services were changed.

[protocol-doc]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/docs/for-solvers/supported-protocols.md
[protocol-mod]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/evm/protocol/mod.rs
[old-protocol-mod]: https://github.com/propeller-heads/tycho-simulation/blob/dc2b7fb2d609d14f6b02bb4081096de0305a5d6e/src/evm/protocol/mod.rs
[price-printer]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/examples/price_printer/main.rs
[infinity-bin]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/evm/protocol/pancakeswap_infinity_bin/mod.rs
[biconomy]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/evm/protocol/biconomy/mod.rs
[biconomy-substreams]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/protocols/substreams/base-biconomy/base-biconomy.yaml
[vm-adapters]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/evm/protocol/vm/constants.rs
[override-example]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/examples/titan_override_monitor/main.rs
[base-ekubo]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/protocols/substreams/ethereum-ekubo-v3/base-ekubo-v3.yaml
[base-balancer]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/protocols/substreams/ethereum-balancer-v3/base-balancer-v3.yaml
[executors]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-execution/config/executor_addresses.json
[stream]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/evm/stream.rs
[old-rfq]: https://github.com/propeller-heads/tycho-simulation/tree/dc2b7fb2d609d14f6b02bb4081096de0305a5d6e/src/rfq
[rfq]: https://github.com/propeller-heads/tycho/tree/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/rfq
[book]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/book/mod.rs
[book-example]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/examples/book_quickstart/main.rs
[book-doc]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/docs/for-solvers/request-for-quote-protocols.md
[baibai-feed]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/pamm/protocols/baibai/feed.rs
[baibai]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/pamm/protocols/baibai/mod.rs
[pricelevel-stream]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/price_level_stream/stream.rs
[pricelevel-config]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/price_level_stream/config.rs
[pricelevel-doc]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/docs/for-solvers/price-level-stream.md
[snapshot-feed]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/snapshot_feed/mod.rs
[pricelevel-tracker]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/price_level_stream/tracker.rs
[curve]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/evm/protocol/curve/decoder.rs
[filters]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/evm/protocol/filters.rs
[balancer-v3]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/evm/protocol/balancer_v3/decoder.rs
[ekubo-v3]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/evm/protocol/ekubo_v3/mod.rs
[aero-basic]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/evm/protocol/aero_basic/state.rs
[slipstreams]: https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/evm/protocol/aerodrome_slipstreams/decoder.rs

## Project-specific implementation sequence

These recommendations are based on the active checkout at `97a2d503040af1af8b6ef462ea4a829276b4fc3e`. They are a source-level migration design, not a completed or compile-tested upgrade.

### 1. Align dependencies and preserve the execution fork

Update workspace `tycho-simulation` and direct `tycho-common` to `0.526.0`, regenerate `Cargo.lock`, and update the private `tycho-execution` fork to a revision compatible with that common version. The current execution package is `0.165.1` at `46596c8fd137661161edf11a7ed3e5be50452d95` and the lockfile resolves it to `tycho-common 0.158.0`. This is a concrete type boundary: `build_route_swaps` passes simulation's `ProtocolComponent`, `Bytes`, and `Arc<dyn ProtocolSim>` directly into execution's `Swap`. Keeping two incompatible common versions cannot satisfy those calls. Preserve fork-specific encoding behavior while porting; do not silently replace the fork with the public crate. The old fork URL returned 404, but the renamed fork was located and the compatibility changes are implemented as documented above. The existing declared Rust minimum, `1.91.0`, matches simulation's declared minimum; verify the complete resolved graph with the actual CI toolchain.

Local evidence: [workspace dependencies](../Cargo.toml), [lockfile](../Cargo.lock), [execution type boundary](../crates/runtime/src/services/encode/tycho_swaps.rs), [router construction](../crates/runtime/src/services/encode/calldata.rs).

### 2. Migrate the raw feed and existing RFQ paths before adding feeds

The broadcaster subscribes with `TychoStreamBuilder.exchange(...)` and publishes raw native/VM messages. The simulator decodes them after HTTP bootstrap and Redis replay. The version upgrade therefore affects both binaries and external replay consumers, even if the initial feature request is broadcaster-only. [Subscription and decoder code](../crates/runtime/src/services/stream_builder.rs), [consumer decoding](../crates/runtime/src/broadcaster/redis_subscription/processor.rs).

Concrete changes verified in the published source:

- `TychoStreamDecoder::new()` becomes `TychoStreamDecoder::new(chain)`. Pass the selected chain through decoder factories and fixtures. [Upstream decoder](https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/evm/decoder.rs).
- `Update.block_number_or_timestamp` becomes `Update.block_number`, with a separate `is_partial` flag. Migrate state application and progress accounting deliberately; the replacement RFQ feeds have time anchors rather than an overloaded block number. [Upstream Update](https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/protocol/models.rs), [local stream](../crates/runtime/src/stream.rs), [state store](../crates/runtime/src/models/state.rs).
- `feed::synchronizer::StateSyncMessage` and its nested snapshots now contain model types and no longer implement serde. Use `tycho_client::feed::dto` at our JSON boundary, converting to model messages before decoding. Its documented purpose is to preserve the earlier JSON field names. Audit `PartialEq` derivations too: the new feed DTO wrappers do not derive it. `dto::BlockChanges` is now `dto::BlockAggregatedChanges`. Existing snapshot materialization and chunk reassembly manipulate those fields directly, so audit component/account conversions, DCI entrypoints, new tokens, merge behavior, and raw VM storage rather than merely changing imports. [Upstream serializable feed DTOs](https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-client/src/feed/dto.rs), [shared wire types](../crates/simulator-core/src/broadcaster.rs), [materialization](../crates/runtime/src/broadcaster/state.rs), [reassembly](../crates/runtime/src/broadcaster/redis_subscription/snapshot.rs).
- `RFQStreamBuilder` and the old provider `client_builder` modules have been removed. Migrate Bebop/Hashflow/Liquorice to provider feed builders and `BookFeedStreams`/`SnapshotFeed`. Published snapshots replace the provider's entire book: absent pairs must be removed; `Withdrawn`/`Ended` must invalidate that provider's state. New `BookFeedConfig.min_tvl_usd` is explicitly USD, while our shared TVL configuration is documented in native-token units. Give the new setting an explicit unit instead of copying the old number. [Upstream book API](https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/book/mod.rs), [feed lifecycle](https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/snapshot_feed/mod.rs), [local settings](../.env.example).
- Our RFQ encode path currently rehydrates deserialized state by directly assigning `state.client`. Latest provider client modules and state client fields are private; serde still omits the client. An import rename cannot restore this behavior. Recommended direction: broadcast provider pricing DTOs, then rebuild local books through public assemblers with local credentials (Bebop exposes `BebopPrices`, `BebopBookAssemblerBuilder`, and `books(prices)`). Alternatively, add a supported rehydration API to the simulation dependency. Preserve request timeout clamping and firm-quote behavior. This choice must be implemented and tested before existing Bebop support can survive the upgrade. [Current hydration](../crates/runtime/src/services/encode/resimulate.rs), [Bebop state](https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/rfq/protocols/bebop/state.rs), [Bebop assembler](https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/rfq/protocols/bebop/source.rs).

### 3. Register each supported chain/protocol combination completely

Apply the source matrix above to all of these places together:

| Location | Required change |
| --- | --- |
| `simulator-manifest.toml` | Declare canonical IDs/backends; enable them only on the appropriate Ethereum/Base chain entries. |
| `crates/simulator-core/src/models/protocol.rs` | Extend `ProtocolKind`, `ALL`, canonical names, actual upstream component-type mappings, and classification tests. Unknown names currently fail manifest validation or are rejected/dropped during ingestion. |
| `crates/runtime/src/services/stream_builder.rs` | Import/register the exact new state types and filters; raw broadcaster exchange subscriptions already follow the manifest. |
| `crates/simulator-core/src/broadcaster.rs` | Extend `backend_for_kind` so new VM/hybrid kinds do not fall through to Native. |
| `crates/runtime/src/services/encode/backend.rs` | Extend execution classification and shared database guard requirements, including hooks/VM hybrids. |
| RFQ/pAMM configuration and feed adapters | Add provider credentials, endpoints, freshness, removals, and correct backend semantics; these sources are not ordinary `.exchange()` registrations. |

Do not infer backend safety from the `vm:` prefix or from a native math implementation alone. Current `CurveState`, `BalancerV3State`, and generic Uniswap v4 hooks access `SHARED_TYCHO_DB`. The local manifest explicitly disables Ethereum core v4 pending a guarded shared-database subscription that preserves the VM set. Retain that constraint until the combined snapshot/recovery path is verified. Keep VM-dependent additions under the VM lifecycle or implement a single correctly guarded decoder path. [Current manifest](../simulator-manifest.toml), [current VM update guard](../crates/runtime/src/broadcaster/redis_subscription/processor.rs), [upstream Balancer v3](https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/evm/protocol/balancer_v3/state.rs), [upstream Curve](https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/evm/protocol/curve/state.rs).

Our code registers `TychoStreamDecoder` directly, bypassing the convenience `ProtocolStreamBuilder` policy. Copy necessary filters explicitly, including unsupported Ekubo extensions, killed LiquidityParty pools, Curve eligibility, and Angstrom exclusion when no attestation integration is available. Native-token routes also need a deliberate route-policy review for newly enabled mint/wrap protocols. [Local direct registrations](../crates/runtime/src/services/stream_builder.rs), [upstream filters](https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-simulation/src/evm/protocol/filters.rs).

### 4. Verify actual feed availability and executability

Before enabling conditional sources, query **every page** of `get_protocol_systems(ProtocolSystemsParams::new(chain))` against each configured Tycho endpoint with the actual API key, and capture component counts. The wire endpoint is `POST /v1/protocol_systems`; it returns both protocol systems and DCI protocols. Intersect those results with the published simulator support matrix. This lookup was run for Base with all 12 results returned on the first page; Ethereum discovery remains outside this implementation scope. A module, substreams manifest, or executor address in source is not proof that the configured hosted feed serves it. [Upstream discovery API](https://github.com/propeller-heads/tycho/blob/d0de8be62b40b63c26455acc8667954f38107446/crates/tycho-client/src/rpc.rs).

For routing/encoding, independently verify the chosen execution fork and deployed router support each new source; validate live router/executor configuration before exposing those sources as executable. If a source is initially broadcast/quote-only, express that capability explicitly so downstream routing cannot mistake it for an encodable route. This is especially relevant to sources with no verified executor entry and to hooks that need attestations.

### 5. Validation and rollout order

1. Run manifest/registry/decoder coverage tests, including backend classification for every canonical name and real component type.
2. Round-trip representative raw native, VM/DCI, token, snapshot, recovery, and Redis messages across old/new wire DTOs. Add fixtures for new protocols and verify merged/chunked state retains all fields. Check the schema/replay compatibility decision before any mixed-version rollout.
3. Test RFQ book replacement, provider withdrawal, stale state removal, reconstructed firm-quote clients, and timeout clamping. For Aerodrome, exercise per-block execution context even when a pool has no delta.
4. Run the repository's CI checks: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo nextest run --workspace`, and release builds for both service binaries. Validation results for the implementation are recorded below.
5. Run the existing local analyzer for `--chain-id 1` and `--chain-id 8453`, Redis replay verification, and the opt-in live mainnet VM wire test. Add representative simulate/encode cases for each newly enabled source; record included/filtered pool counts and readiness progress. Existing harness instructions are in [README](../README.md).
6. Roll out a compatibility-capable consumer/broadcaster pair first, then enable verified new feeds in stages. If wire compatibility cannot be preserved, use a separately versioned stream/bootstrap contract and explicit rebootstrap. Keep the previous accepted configuration available for rollback.

Implementation is local to this checkout. No deployed service or production configuration was changed.
