# Protocol feeds

Do not maintain a second protocol inventory in this skill. The repository source
of truth is `simulator-manifest.toml`:

- `[[protocols]]` declares every known protocol and its backend.
- `[[chains]]` selects `native_protocols`, `vm_protocols`, and `rfq_protocols`
  for each chain. The broadcaster adds the corresponding `broadcaster_only_*_protocols` entries.
- `[[route_policies]]` owns chain-specific routing details.
- `crates/runtime/src/config/manifest.rs` validates those relationships and
  builds the effective runtime configuration.

Inspect the active sets directly:

```bash
python3 - <<'PY'
import tomllib

with open("simulator-manifest.toml", "rb") as handle:
    manifest = tomllib.load(handle)

for chain in manifest["chains"]:
    print(f"chain {chain['chain_id']}")
    for backend in ("native", "vm", "rfq"):
        selected = chain[f"{backend}_protocols"]
        broadcast = selected + chain.get(f"broadcaster_only_{backend}_protocols", [])
        print(f"  {backend} simulator: {', '.join(selected) or '(none)'}")
        print(f"  {backend} broadcaster: {', '.join(broadcast) or '(none)'}")
PY
```

## Effective backend enablement

- Native readiness is always evaluated for the selected chain.
- The broadcaster subscribes to every VM system selected for broadcast in the manifest.
- `ENABLE_VM_POOLS` gates the simulator's shared database backend. It serves VM pools
  on Ethereum and Uniswap V4 on Base.
- Each service enables RFQ when `ENABLE_RFQ_POOLS=true` and its own RFQ list is non-empty.
- When RFQ is effective, broadcaster startup requires the credential set associated with
  every selected RFQ provider. Derive that mapping from runtime config and tests;
  do not copy provider assumptions into this reference.

Treat protocol visibility in analyzer output as observed live state, not a hard
assertion that every configured protocol must quote in every run. TVL policy,
token coverage, pool state, and backend readiness can all change what appears.
