# Protocol feeds (what this server subscribes to)

The service subscribes to chain-specific Tycho exchanges at startup (see
`crates/runtime/src/config/mod.rs` and `crates/runtime/src/services/stream_builder.rs`).

## Native feeds by chain

### Ethereum (`CHAIN_ID=1`)

- `uniswap_v2`
- `sushiswap_v2`
- `pancakeswap_v2`
- `uniswap_v3`
- `pancakeswap_v3`
- `uniswap_v4`
- `ekubo_v2`
- `fluid_v1`
- `rocketpool`
- `erc4626`
- `ekubo_v3`

### Base (`CHAIN_ID=8453`)

- `uniswap_v2`
- `uniswap_v3`
- `uniswap_v4`
- `pancakeswap_v3`
- `aerodrome_slipstreams`

## VM feeds by chain

### Ethereum (`CHAIN_ID=1`)

- `vm:curve`
- `vm:balancer_v2`
- `vm:maverick_v2`

### Base (`CHAIN_ID=8453`)

- No VM protocols configured in this iteration.

## RFQ feeds by chain

### Ethereum (`CHAIN_ID=1`)

- `rfq:bebop`

> **NOTE:** Hashflow remains implemented but is disabled in the active chain profiles. Liquorice
> simulation is wired in the quote, but encoding and full support will be introduced in the future.

### Base (`CHAIN_ID=8453`)

- `rfq:bebop`

## Effective VM enablement

- Runtime VM state is `effective_vm_enabled = ENABLE_VM_POOLS && vm_protocols_not_empty`.
- This means Base omits `backends.vm` even if `ENABLE_VM_POOLS=true`.
- The local analyzer waits for VM readiness automatically on Ethereum when VM pools are enabled.

## Effective RFQ enablement

- Runtime RFQ state is `effective_rfq_enabled = ENABLE_RFQ_POOLS && rfq_protocols_not_empty`.
- Enabling RFQ analysis requires credentials for the providers on that chain. Ethereum and Base
  currently use only the Bebop pair.
- When `ENABLE_RFQ_POOLS=true` and the manifest lists `rfq_protocols`, the simulator requires every listed provider credential pair at startup and aborts if any are missing. `/encode` signs RFQ firm quotes with those credentials.
- The local analyzer waits for RFQ readiness automatically when RFQ pools are enabled on either chain.
- On Base, RFQ-enabled analyzer runs include a Bebop partial-fill encode diagnostic. It is required only when RFQ is enabled and ready, and it reports degraded evidence if `rfq:bebop` or the non-RFQ comparison leg is not visible.

## Notes that affect local analysis

- Always pass chain context to the analyzer (`--chain-id` or env `CHAIN_ID`).
- The analyzer uses representative pairs for native, stable, governance, VM-sensitive, and RFQ-targeted paths. Treat protocol visibility as evidence, not as a hard assertion that every protocol must surface on every run.
- Ethereum or Base runs can take longer when RFQ pools are enabled. If the analyzer waits on that backend, that is expected on cold starts.
- Base should keep Aerodrome visible somewhere in the balanced report, but absence is a finding to investigate rather than an automatic failure.
- ERC4626 and other edge features are still highly state-sensitive. Use the analyzer report to decide whether you need deeper focused requests rather than baking every edge path into the default run.
- TVL filtering matters: pools are included or removed based on `TVL_THRESHOLD` and `TVL_KEEP_RATIO`.
  If a protocol disappears unexpectedly, lowering `TVL_THRESHOLD` may be worth testing in a follow-up run.
- Protocol labels in the analyzer are inferred from returned pool data and `meta.pool_results`. They are useful for comparison and triage, but they are still downstream observations of live state.
- RFQ-targeted simulate scenarios are meant to make RFQ visibility intentional on Ethereum and Base. If they do not surface any `rfq:*` protocols, treat that as a finding to investigate rather than an automatic failure.
- On Base, RFQ-enabled analyzer runs include a Bebop partial-fill encode diagnostic. It is required only when RFQ is enabled and ready, and it reports degraded evidence if `rfq:bebop` or the non-RFQ comparison leg is not visible.
