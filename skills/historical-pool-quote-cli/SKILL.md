---
name: historical-pool-quote-cli
description: Use when requesting, inspecting, or explaining historical Base pool quotes through the dsolver-history CLI.
metadata:
  short-description: Historical Base pool quotes
---

# Historical Pool Quote CLI

Use `dsolver-history` as the normal client for the private Historical Pool Quote API.

## Start with the client contract

Read [the client guide](../../docs/historical_pool_quote_client.md) before preparing or interpreting a request. It defines the input model, command forms, outcome semantics, exit codes, and troubleshooting flow.

Establish the exact:

- backend and protocol;
- component ID;
- input and output token addresses;
- start block range and step;
- positive block lags; and
- input amounts in the input token's smallest unit.

Ask for a missing selector when guessing it could query a different pool, direction, block, or amount. The API does not discover components or convert human token amounts.

## Run the request

1. Confirm `DSOLVER_HISTORY_URL` and `DSOLVER_HISTORY_TOKEN` are set without printing the token.
2. Check `coverage` first when the requested range may be near the replica cutoff or a known gap.
3. Use direct flags for a small one-off request. Use reviewed request JSON for repeated or multidimensional work.
4. Use `--output` when the result should become an artifact; do not replace an existing file unless the user intends it.
5. Let the high-level `quotes` command own submission, polling, and its single safe recomputation after `job_not_found`.

Keep JSON stdout separate from progress and summaries on stderr. Do not pass the bearer token as a process argument, place it in a request file, include it in an artifact, or repeat it in the response.

## Interpret and report

Treat each `(start block, input amount)` independently. Its start quote appears once, followed by ordered lagged target outcomes.

- `ok` contains a usable historical directed pool quote and provenance.
- `unavailable` means the service could not reconstruct safe state for that cell. Report the typed reason and any relevant cutoff or gap.
- `quote_failed` means safe state existed but the component and direction did not quote. Report the typed reason.

A process exit code of `0` means the job completed, not that every cell is `ok`. Never convert `unavailable` or `quote_failed` into a zero quote or silently drop those cells.

Finish with the request dimensions, output location or returned JSON, outcome counts, visible cutoff, and material gaps or failures. Distinguish observations from interpretation.

## Deeper references

- Read [the service and API reference](../../docs/historical_pool_quote_api.md) for architecture, security, limits, consistency checks, configuration, or operations.
- Read the checked [OpenAPI contract](../../openapi/historical-pool-quote-api.json) for raw HTTP integrations and exact wire schemas.
