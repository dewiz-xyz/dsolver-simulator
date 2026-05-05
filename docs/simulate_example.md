# /simulate example (AmountOutRequest)

This document describes the `/simulate` request and response shape.

## Terminology

- Quote request: one `POST /simulate` call with one sell token, one buy token, and an ordered list of input amounts.
- Requested amounts: the ordered `amounts[]` in the request and the matching `amounts_out[]` positions in each pool response.
- Usable quote: a pool entry in `data[]` that returned at least one positive output for the requested amounts.
- Partial result: a quote response that returned usable data, but with partial requested-amount coverage or incomplete pool coverage.
- Usable result: a response with `meta.status = "ready"` and `meta.result_quality = "complete"` or `"partial"`.

## Workflow

- POST a quote request to `/simulate`.
- Inspect `meta.status`, `meta.result_quality`, `meta.partial_kind`, `meta.failures`, and `meta.pool_results`.
- Keep only responses with usable quotes when selecting pools for execution or route building.
- If you need calldata, feed selected pools into `/encode`, which re-simulates the route internally.

## Request body (shape)

```json
{
  "request_id": "req-123",
  "auction_id": "auction-42",
  "token_in": "0x6b175474e89094c44da98b954eedeac495271d0f",
  "token_out": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
  "amounts": ["1000000000000000000", "5000000000000000000"]
}
```

## Response body (shape)

```json
{
  "request_id": "req-123",
  "data": [
    {
      "pool": "uniswapv3-1",
      "pool_name": "UniswapV3::DAI/USDC",
      "pool_address": "0x1111111111111111111111111111111111111111",
      "amounts_out": ["999000000", "4985000000"],
      "gas_used": [210000, 210000],
      "block_number": 19876543
    }
  ],
  "meta": {
    "status": "ready",
    "result_quality": "complete",
    "block_number": 19876543,
    "vm_block_number": 19876540,
    "matching_pools": 4,
    "candidate_pools": 4,
    "total_pools": 412,
    "auction_id": "auction-42"
  }
}
```

## Partial Amount Coverage Example

```json
{
  "request_id": "req-124",
  "data": [
    {
      "pool": "uniswapv3-2",
      "pool_name": "UniswapV3::DAI/USDC",
      "pool_address": "0x2222222222222222222222222222222222222222",
      "amounts_out": ["0", "215000000", "980000000"],
      "gas_used": [0, 210000, 220000],
      "block_number": 19876543
    }
  ],
  "meta": {
    "status": "ready",
    "result_quality": "partial",
    "partial_kind": "amount_ladders",
    "block_number": 19876543
  }
}
```

In this example, the smallest requested amount failed for that pool, so `amounts_out[0]` is `"0"`. The later requested amounts are still usable, and the row stays in `data[]` because the pool still produced positive outputs for later amount positions. If every entry were `"0"`, the row would be filtered out of `data[]` and reported only through `meta.failures` and `meta.pool_results`.

## Notes

- `auction_id` is optional in the request. When present, it is echoed back as `meta.auction_id`.
- `/simulate` uses `200 OK` for contract-valid complete, partial, no-result, and selected degraded outcomes. Clients must inspect `meta`, not just the HTTP status.
- For partial results, `amounts_out` stays aligned with the request `amounts[]`. Failed or timed-out requested amounts are returned in place as `"0"`, and the matching `gas_used` entries are `0`.
- `amounts_out[i] = "0"` means that requested amount did not produce a usable quote for that pool. It is not a real output amount.
- `data[]` contains only pools with at least one positive output across the requested amounts. Pools whose entire `amounts_out` row is `"0"` stay out of `data[]` and remain visible through `meta.failures` and `meta.pool_results`.
- `data[]` row order is deterministic, but it is not a best-to-worst ranking. Consumers should rely on requested-amount alignment within each row and choose pools explicitly rather than inferring semantics from `data[0]`.
- `block_number` is the native stream block. `vm_block_number` is present when VM pool support is enabled and VM state has a current block.

### How to consume `data[]`

- treat each row as one pool's quotes across the requested amounts
- match the amount position you care about to the same index in the request `amounts[]`
- only treat positive values in that slot as usable quotes
- treat `"0"` as "no usable quote for that requested amount," not a real output
- treat rows whose whole `amounts_out` array is `"0"` as failed rows; they should not survive in `data[]`
- do not infer pool quality from row position; `data[]` order is stable output, not ranking

### Quick usability check

- `meta.status = "ready"` and `meta.result_quality = "complete"` or `"partial"` means the response is usable for quoting
- `meta.result_quality = "request_level_failure"` or `"no_results"` means it is not usable for quoting
- a usable response can still contain degraded rows where some amount positions are `"0"`
- a usable response should not contain a row whose entire `amounts_out` array is `"0"`

### Outcome interpretation

- `status = "ready"` with `result_quality = "complete"` means usable quotes were returned without request-visible degradation.
- `status = "ready"` with `result_quality = "partial"` means usable quotes were returned, but the result is degraded.
- `status = "no_liquidity"` with `result_quality = "no_results"` means no usable quote survived because liquidity was absent or exhausted.
- `result_quality = "request_level_failure"` means the request ended in a degraded state that should not be used as a quote source, even if the response shape is valid.
- `partial_kind` appears only when `result_quality = "partial"`. It is `amount_ladders`, `pool_coverage`, or `mixed`.
- `vm_unavailable = true` means VM pools were skipped because VM state was not ready. If native pools still produced usable quotes, the response can still be usable for quoting.

### Interpreting `meta.failures` and `meta.pool_results`

- `meta.failures` is the request-level explanation layer. It includes timeouts, token coverage failures, no-pools reasons, and other failures that matter to the overall request outcome.
- `meta.pool_results` is the per-pool anomaly layer. It includes degraded pool-local outcomes such as `partial_output`, `zero_output`, `timed_out`, `simulator_error`, and `internal_error`.
- A partial ready response can include both usable data in `data[]` and request-visible issues in `meta.failures` or `meta.pool_results`.

### Timeout behavior

- Request-guard timeouts return `200 OK` with a valid quote payload whose `meta.status = "ready"`, `meta.result_quality = "request_level_failure"`, and `meta.failures` includes a `timeout`.
- Router-boundary timeouts for `/simulate` also return `200 OK` with `result_quality = "request_level_failure"`.
- Timeout responses are structurally valid, but they are not usable for quoting.
