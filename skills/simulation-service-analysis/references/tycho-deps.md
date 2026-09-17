# Tycho and Propeller Heads dependencies

Do not pin dependency versions in this skill. Read the checked-out repository:

```bash
rg -n '^tycho-(common|simulation|execution)\s*=' Cargo.toml
rg -n '^name = "tycho-(common|simulation)"|^version = ' Cargo.lock
```

`Cargo.toml` is the declared dependency source and `Cargo.lock` is the resolved
workspace source. For Git dependencies, keep the exact `rev` from `Cargo.toml`
with the analysis evidence.

Useful upstream references:

- Tycho simulation for solvers: https://docs.propellerheads.xyz/tycho/for-solvers/simulation
- Supported protocols: https://docs.propellerheads.xyz/tycho/for-solvers/supported-protocols
- Protocol integration: https://docs.propellerheads.xyz/tycho/for-dexs/protocol-integration/simulation

Upstream documentation describes Tycho generally. The checked-out manifest,
runtime config, and lockfile decide what this simulator build actually enables.
