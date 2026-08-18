# Historical Pool Quote Client Guide

Use this guide to request historical quotes for one exact Base pool and token direction through the `dsolver-history` CLI. For service architecture, HTTP routes, configuration, and operations, see the [Historical Pool Quote API reference](historical_pool_quote_api.md).

## What the service answers

A request selects:

- one exact pool component;
- one token direction;
- one or more start blocks;
- one or more positive block lags; and
- one or more input amounts.

For every `(start block, input amount)`, the service quotes the start state once and returns an ordered target outcome for each lag. A target block is `start block + lag`.

The service reconstructs retained historical state. It does not discover pools, replay routes or auctions, return bulk datasets, or execute live quotes.

## Inputs you need

| Input | Meaning |
| --- | --- |
| `backend` | State backend. Production initially supports `native` and `rfq`; `vm` is disabled. |
| `protocol` | Exact protocol identifier stored with the component, such as `uniswap_v3`. |
| `componentId` | Exact component identifier from the source dataset or analysis input. The service does not resolve symbols or discover components. |
| `tokenIn` / `tokenOut` | Exact token addresses in the direction to quote. Direction matters. |
| Start blocks | Inclusive sequence defined by `start`, `endInclusive`, and `step`. |
| Lags | Distinct positive block offsets from every start block. |
| Input amounts | Unsigned base-10 integer strings in the input token's smallest unit. |

For example, `"1000000"` represents one token only when that token uses six decimals. Do the decimal conversion before submitting the request.

The first API revision supports Base chain ID `8453` only.

## Install and configure the CLI

From a repository checkout, install the client:

```bash
cargo install --path crates/dsolver-history --locked
```

Connect to the approved private network, then set the service URL and bearer token supplied through the approved secret channel:

```bash
export DSOLVER_HISTORY_URL='https://historical-pool-quote-api.example.ts.net'
export DSOLVER_HISTORY_TOKEN='<bearer token>'
```

The CLI deliberately has no token command-line option. Keep the token in the environment so it does not enter the command history or process arguments. Do not print or commit it.

Confirm that the client can reach the service:

```bash
dsolver-history service status --pretty
```

## Check coverage first

Coverage tells you which block intervals can be reconstructed safely through the read replica:

```bash
dsolver-history coverage \
  --chain-id 8453 \
  --backend native,rfq \
  --pretty
```

You can restrict the response to the range you intend to query:

```bash
dsolver-history coverage \
  --chain-id 8453 \
  --backend native \
  --start-block 34000000 \
  --end-block-inclusive 34001800 \
  --pretty
```

Read these fields before choosing a request range:

- `visibleThroughBlock` is the newest block currently visible on the replica.
- `continuousIntervals` are ranges that can be reconstructed safely for all requested backends.
- `knownGaps` identifies known unsafe intervals.

Coverage does not prove that a component exists or can quote. Replica delay lowers `visibleThroughBlock`; it is not a stored gap.

## Run a small quote request

Direct flags are convenient for a one-off request. Replace every selector with the exact values for the pool you want to inspect:

```bash
dsolver-history quotes \
  --chain-id 8453 \
  --backend native \
  --protocol uniswap_v3 \
  --component-id '<component ID>' \
  --token-in '<input token address>' \
  --token-out '<output token address>' \
  --start-block 34000000 \
  --end-block-inclusive 34000010 \
  --step 1 \
  --lag 1,3,5 \
  --amount-in 1000000,5000000 \
  --timeout-ms 600000 \
  --pretty
```

With direct flags, the CLI generates a UUID v4 request ID. It submits the job, polls until a terminal result, and prints the grouped quote result.

The comparison count is:

```text
start block count * lag count * input amount count
```

The service rejects requests over its configured comparison, block-span, or deadline limits before running them. The scheduler can also reject admission when the waiting queue or global decoded-byte budget has no room.

## Use a request file for repeatable work

Use JSON when a request has several dimensions or needs to be reviewed and repeated:

```json
{
  "requestId": "3bbd9a8c-d942-4f9d-b96d-7ee63b9308aa",
  "apiRevision": 1,
  "timeoutMs": 600000,
  "chainId": 8453,
  "pool": {
    "backend": "native",
    "protocol": "uniswap_v3",
    "componentId": "<component ID>",
    "tokenIn": "<input token address>",
    "tokenOut": "<output token address>"
  },
  "blocks": {
    "start": 34000000,
    "endInclusive": 34000010,
    "step": 1
  },
  "lags": [1, 3, 5],
  "amountsIn": ["1000000", "5000000"]
}
```

Save it as `quote-request.json`, replace the selectors, and run:

```bash
dsolver-history quotes \
  --request quote-request.json \
  --pretty \
  --output quote-result.json
```

The request ID must be a UUID v4. While the request is retained, reusing it with the same normalized content is idempotent. Reusing it with different content returns a conflict, so generate a new request ID when the request changes.

`--request -` reads JSON from stdin. A request file cannot be mixed with direct request flags.

## Interpret the result

The response contains one entry for each `(startBlock, amountIn)`. Each entry has one `startOutcome` and ordered `targets`; each target includes its `lag`, calculated `targetBlock`, and `outcome`.

Every directed quote outcome is one of:

| Outcome | Meaning |
| --- | --- |
| `ok` | Historical state was reconstructed safely and the component produced a quote. `amountOut` is an integer string in the output token's smallest unit. |
| `unavailable` | The service could not reconstruct safe state for this cell. Inspect `reason` and any `gapRefs`; do not infer a quote. |
| `quote_failed` | Safe state existed, but this component and direction did not produce a quote. Inspect `reason`. |

`unavailable` reasons include `gap`, `replica_cutoff`, `token_snapshot_missing`, `rfq_observation_missing`, `block_boundary_missing`, `canonical_lineage_invalid`, and `state_application_invalid`.

`quote_failed` reasons include `component_not_found`, `direction_unsupported`, `insufficient_liquidity`, `zero_output`, and `simulation_failed`.

A completed job contains every requested cell. Partial outcomes do not fail the whole job:

- If the start state is unavailable, its start and target outcomes are unavailable.
- If the start quote fails but target states are safe, the target quotes still run.
- A gap makes only the start-to-target intervals that cross it unavailable.
- `visibleThroughBlock` is the replica cutoff used for the result.

An `ok` outcome includes provenance for the reconstructed state and service revision. RFQ quotes are indicative observations, not signed or binding quotes.

## Save and automate output safely

The CLI writes machine-readable JSON to stdout. Job IDs, progress, retry notices, and human summaries go to stderr, so piping stdout does not mix operational text into the result.

Use `--output PATH` for atomic publication. The CLI refuses to replace an existing file unless `--force` is present:

```bash
dsolver-history quotes \
  --request quote-request.json \
  --output quote-result.json
```

Exit codes are stable:

| Code | Meaning |
| ---: | --- |
| `0` | The job completed. Individual cells may still be `unavailable` or `quote_failed`. |
| `1` | Transport, service, deadline, output, or terminal job failure. |
| `2` | Invalid usage or configuration, authentication failure, or admission rejection. |
| `130` | Interrupted with Ctrl-C. |

For normal use, rely on the high-level `quotes` command. `--submit-only` returns after admission, and `job get`, `job wait`, and `job cancel` expose the lower-level lifecycle when you already have a job ID. Add `--job-envelope` when you need the final lifecycle envelope instead of only the quote result.

## Jobs, retries, and interruption

Jobs and terminal results are held in service memory. A deployment or task restart can remove them. A successful terminal poll consumes the retained result, so a later poll for the same job can return `job_not_found`.

The high-level `quotes` command retains the original request and can recompute once after `job_not_found` if time remains. Generic job commands cannot recompute because they do not have the request body.

The CLI retries uncertain HTTP transport, response-body delivery, `429`, and `503` responses within the request deadline. On Ctrl-C, it asks the service to cancel the active job, waits briefly for acknowledgement, and exits with code `130`. Cancellation is cooperative; a simulator call already in progress cannot stop midway.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| `DSOLVER_HISTORY_URL is missing` | Set the private service URL or pass the non-secret global `--url` option. |
| `DSOLVER_HISTORY_TOKEN is missing` | Set the bearer token in the environment. There is no `--token` option. |
| HTTP `401` | Confirm that the token came from the current approved channel and has not been revoked. |
| HTTP `400` | Correct the field named in the error, such as an invalid selector, block span, backend, or timeout. |
| HTTP `409` | The request ID is retained with different normalized content. Generate a new UUID v4 for the changed request. |
| HTTP `422` | Reduce the start-block, lag, or input-amount dimensions reported by the comparison-budget error. |
| Repeated HTTP `429` or `503` | The queue is full or the service cannot safely admit work. The CLI respects `Retry-After` until its deadline. |
| `unavailable` cells | Inspect the typed reason, `visibleThroughBlock`, coverage intervals, and any gap references. |
| `quote_failed` cells | Confirm the exact component ID, protocol, token direction, and liquidity at that block. |

For the raw HTTP contract or a non-CLI integration, use the checked [OpenAPI 3.1 document](../openapi/historical-pool-quote-api.json) and the [service and API reference](historical_pool_quote_api.md).
