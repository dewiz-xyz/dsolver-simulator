# Protocol feeds

Do not maintain a second protocol inventory in this skill. The repository source
of truth is `simulator-manifest.toml`:

- `[[protocols]]` declares every known protocol and its backend.
- `[[chains]]` selects `native_protocols`, `vm_protocols`, and `rfq_protocols`
  for each chain.
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
        print(f"  {backend}: {', '.join(chain[f'{backend}_protocols']) or '(none)'}")
PY
```

## Effective backend enablement

- Native readiness is always evaluated for the selected chain.
- VM is effective only when `ENABLE_VM_POOLS=true` and the selected chain's
  `vm_protocols` is non-empty.
- RFQ is effective only when `ENABLE_RFQ_POOLS=true` and the selected chain's
  `rfq_protocols` is non-empty.
- When RFQ is effective, startup requires the credential set associated with
  every selected RFQ provider. Derive that mapping from runtime config and tests;
  do not copy provider assumptions into this reference.

Treat protocol visibility in analyzer output as observed live state, not a hard
assertion that every configured protocol must quote in every run. TVL policy,
token coverage, pool state, and backend readiness can all change what appears.
