# /encode testing notes

## Schema (latest)
- `/encode` uses `RouteEncodeRequest` / `RouteEncodeResponse` with camelCase fields.
- The encoder emits `singleSwap`, `sequentialSwap`, or `splitSwap` depending on route shape and splits.
- Requests include only route-level `amountIn` and `minAmountOut` plus `shareBps` and `splitBps` for splits.
- Per-hop and per-swap amounts are not accepted and not returned.
- `/encode` keeps its current success/error response contract. `/simulate` selection semantics are documented in [docs/simulate_example.md](../../../docs/simulate_example.md).

## Analyzer route probes

The local analyzer uses a default route matrix:

- resolves chain from `--chain-id` (or `CHAIN_ID`)
- calls `/simulate` for each route hop to pick a representative candidate pool
- builds SimpleSwap, MultiSwap, and MegaSwap requests and posts them to `/encode`
- records prep `/simulate` hops as separate `encode-prep` scenarios
- records each `/encode` interaction shape, router-call presence, latency, and oddities without mixing in prep-hop metrics
- uses chain-specific default routes and amounts, but treats the result as analytical evidence rather than a strict gate

The matrix is intentionally representative rather than exhaustive. If the report suggests an encode-specific issue, follow up with targeted manual routes instead of assuming the default matrix captured the full contract surface.

## Common pitfalls

- `settlementAddress` and `tychoRouterAddress` are required. Defaults come from `.env`:
  - `COW_SETTLEMENT_CONTRACT`
  - `TYCHO_ROUTER_ADDRESS`
- `/encode` fails if the resimulated route `expectedAmountOut < minAmountOut`.
- Timeout behavior differs from `/simulate`: `/encode` returns `408` with `{ error }`, plus `requestId` when it is available.
- Chain mismatch between request `chainId` and server runtime chain fails validation.
- `/simulate` rows with fully-zero `amounts_out` are not usable encode candidates. More generally, `"0"` means that requested amount did not produce a usable quote, not a valid dust quote.

## Reference docs

- `docs/encode_example.md` for schema shape examples.
- `docs/simulate_example.md` for `/simulate` usability rules used during pool selection.
