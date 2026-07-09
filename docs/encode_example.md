# /encode example (RouteEncodeRequest)

This document describes the `/encode` request and response shape.

## Terminology

- Swap: a single pool operation. It always uses one pool and one token pair.
- Hop: a single token transition. A hop can split across many pools that all do the same tokenIn to tokenOut.
- Segment: a full path from tokenIn to tokenOut. A MegaSwap contains multiple segments in parallel.

SimpleSwap uses one hop with one or more swaps where every swap is tokenA to tokenB. MultiSwap uses multiple hops with different intermediary tokens. MegaSwap runs multiple MultiSwaps in parallel, each with its own segment share.

## Workflow

- Call `/simulate` for candidate pools. Keep only results with usable quotes (`status = "ready"` with `result_quality = "complete"` or `"partial"`). Within those rows, treat `amounts_out[i] = "0"` as "no usable quote for that requested amount," and ignore fully-zero rows. See [simulate_example.md](simulate_example.md).
- Build a `RouteEncodeRequest` with `segments[] -> hops[] -> swaps[]`, using segment `shareBps` and swap `splitBps` to define splits.
- Provide only the top-level `amountIn` and `minAmountOut` as the route-level guard.
- POST to `/encode`, which re-simulates swaps internally, derives per-hop and per-swap amounts, and returns settlement `interactions[]`.
- For server-side reporting, rely on the structured summary log emitted for each `/encode` request. Detailed resimulation traces are available at `debug`.

The repo's encode smoke helper stays intentionally strict: it uses dedicated realistic amount presets for default routes on each supported chain, requires simulated hops to return usable quotes for every requested amount, and fails if any tested amount degrades to `"0"` on a required hop.

## Request body (shape)

`estimatedAmountIn` is an optional top-level request field. RFQ routes still derive the actual per-swap estimated input internally from the route `amountIn` and split allocation.

```json
{
  "chainId": 1,
  "tokenIn": "0x6b175474e89094c44da98b954eedeac495271d0f",
  "tokenOut": "0xdac17f958d2ee523a2206206994597c13d831ec7",
  "amountIn": "1000000000000000000",
  "minAmountOut": "9980000",
  "settlementAddress": "0x9008D19f58AAbD9eD0D60971565AA8510560ab41", // Cowswap's settlement contract on Ethereum mainnet
  "tychoRouterAddress": "0xfD0b31d2E955fA55e3fa641Fe90e08b677188d35", // Tycho router contract on Ethereum mainnet
  "swapKind": "MegaSwap",
  "segments": [
    {
      "kind": "MultiSwap",
      "shareBps": 6000,
      "hops": [
        {
          "tokenIn": "0x6b175474e89094c44da98b954eedeac495271d0f",
          "tokenOut": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
          "swaps": [
            {
              "pool": {
                "protocol": "uniswap_v3",
                "componentId": "pool-dai-usdc-3000",
                "poolAddress": "0x1111111111111111111111111111111111111111"
              },
              "tokenIn": "0x6b175474e89094c44da98b954eedeac495271d0f",
              "tokenOut": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
              "splitBps": 6000
            },
            {
              "pool": {
                "protocol": "curve_v2",
                "componentId": "pool-dai-usdc",
                "poolAddress": "0x2222222222222222222222222222222222222222"
              },
              "tokenIn": "0x6b175474e89094c44da98b954eedeac495271d0f",
              "tokenOut": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
              "splitBps": 0
            }
          ]
        },
        {
          "tokenIn": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
          "tokenOut": "0xdac17f958d2ee523a2206206994597c13d831ec7",
          "swaps": [
            {
              "pool": {
                "protocol": "uniswap_v3",
                "componentId": "pool-usdc-usdt-100",
                "poolAddress": "0x3333333333333333333333333333333333333333"
              },
              "tokenIn": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
              "tokenOut": "0xdac17f958d2ee523a2206206994597c13d831ec7",
              "splitBps": 0
            }
          ]
        }
      ]
    },
    {
      "kind": "SimpleSwap",
      "shareBps": 0,
      "hops": [
        {
          "tokenIn": "0x6b175474e89094c44da98b954eedeac495271d0f",
          "tokenOut": "0xdac17f958d2ee523a2206206994597c13d831ec7",
          "swaps": [
            {
              "pool": {
                "protocol": "uniswap_v3",
                "componentId": "pool-dai-usdt-100",
                "poolAddress": "0x4444444444444444444444444444444444444444"
              },
              "tokenIn": "0x6b175474e89094c44da98b954eedeac495271d0f",
              "tokenOut": "0xdac17f958d2ee523a2206206994597c13d831ec7",
              "splitBps": 5000
            },
            {
              "pool": {
                "protocol": "curve_v2",
                "componentId": "pool-dai-usdt",
                "poolAddress": "0x5555555555555555555555555555555555555555"
              },
              "tokenIn": "0x6b175474e89094c44da98b954eedeac495271d0f",
              "tokenOut": "0xdac17f958d2ee523a2206206994597c13d831ec7",
              "splitBps": 0
            }
          ]
        }
      ]
    }
  ],
  "requestId": "encode-example-split-only-1",
  "estimatedAmountIn": "1000000000000000000"
}
```

## Response body (shape)

```json
{
  "interactions": [
    {
      "kind": "ERC20_APPROVE",
      "target": "0x6b175474e89094c44da98b954eedeac495271d0f",
      "value": "0",
      "calldata": "0x095ea7b3..."
    },
    {
      "kind": "CALL",
      "target": "0xfD0b31d2E955fA55e3fa641Fe90e08b677188d35",
      "value": "0",
      "calldata": "0x..."
    }
  ],
  "debug": {
    "requestId": "encode-example-split-only-1",
    "resimulation": {
      "blockNumber": 24200000
    }
  }
}
```

## Notes

- Only route level `minAmountOut` is enforced. There are no per-hop or per-swap `minAmountOut` checks.
- `shareBps` (segment-level) and `splitBps` (swap-level) use Tycho split semantics:
  - `0` means remainder, or 100% if there is only one entry.
  - For each split set, the last entry must be `0` so it receives the remainder.
  - Non-last entries must be > 0.
  - The sum of all non-remainder shares must be <= 10000, and it must leave a positive remainder
    amount for the last entry when there are 2+ entries (prefer sum < 10000 to avoid edge cases).

### Client-side validation (recommended)

If you generate split routes client-side, validate splits using **integer math in base units** (the same way the router/server behave) to avoid building routes that deterministically fail due to rounding.

#### Allocation formulas (BPS, integer)

Given a split set with total amount `T` (base units) and `bps[0..N-1]` where `bps[N-1] == 0` is the remainder:

- For `i` in `0..N-2`:
  - `amount[i] = floor(T * bps[i] / 10000)`
- `amount[N-1] = T - sum(amount[0..N-2])`

#### Required invariants

- If `N == 1`: require `bps[0] == 0` (router interprets 0 as 100%).
- If `N > 1`:
  - `bps[N-1] == 0` (remainder must be last)
  - for all `i < N-1`: `bps[i] > 0`
  - `amount[N-1] > 0` (must leave a positive remainder for the last entry)
  - for all `i < N-1`: `amount[i] > 0` (prevents non-remainder legs that round down to 0)

#### Practical rule of thumb

To guarantee `amount[i] >= 1` for a non-remainder leg with `bps = b`:

- `T >= ceil(10000 / b)`

#### Examples (rounding pitfalls)

- Example A (bad: remainder becomes 0):
  - `T = 100`, `splitBps = [5000, 5000, 0]`
  - `amount = [50, 50, 0]` => invalid (no remainder left for the last entry)
- Example B (bad: non-remainder rounds to 0):
  - `T = 100`, `splitBps = [1, 0]`
  - `amount[0] = floor(100 * 1 / 10000) = 0`, remainder gets 100
  - invalid (the 1 bps leg would execute with 0 input)

Note: this validation is deterministic for the **first hop** (route `amountIn` and segment `shareBps` are known). For later hops, validate using your best estimate of hop amounts (typically derived from `/simulate` outputs).

- Hops are sequential in the order provided; there is no hop-level share.
- SimpleSwap has a single hop. All swaps in that hop must share the same tokenIn and tokenOut.
- MultiSwap has multiple hops. Each hop transitions between different tokens.
- MegaSwap has multiple segments. Each segment is a MultiSwap or SimpleSwap and gets a segment share.
- The encoder emits `singleSwap`, `sequentialSwap`, or `splitSwap` based on route shape and splits.
- `poolAddress` is optional and may be omitted when unavailable.
- `estimatedAmountIn` is optional and may be omitted.
- `interactions[]` are emitted in order: approve(amountIn) -> router call. For reset-allowance tokens an approve(0) is prepended.
- Native `tokenIn`/`tokenOut` is supported only for allowlisted protocols (currently `rocketpool`).
  - Native `tokenIn` routes emit a `CALL`-only interaction with `value=amountIn` (no ERC20 approvals).
  - Native usage on non-allowlisted protocols is rejected as an invalid request.
- The API shape stays success/error oriented. If you need richer server-side failure breakdowns, use logs keyed by `requestId`, `encode_error_kind`, and `failure_stage`.

### Local analysis note

The repo-local analyzer uses a SimpleSwap, MultiSwap, and MegaSwap route matrix built from `/simulate` results and records how `/encode` behaves for those routes. It is meant to surface behavior, latency, and oddities in a standardized report, not to act like a strict pass/fail encode contract test.
