# Execution compatibility patch

Source: `dewiz-xyz/dsolver-execution` commit `f4d444414c43d2cecf626139fd1749e11d15cba6` (MIT, as declared by its Cargo manifest).

This source snapshot preserves the project's existing router and executor behavior while updating the broadcaster/simulator to Tycho 0.525.0. The fork does not yet compile with that common version. Local changes are limited to:

- Pin `tycho-common` to 0.525.0, avoiding multiple incompatible common types.
- Handle chains without a wrapped native token using the existing encoding error type.
- Initialize the added quote `origin` field to `None`, preserving the prior behavior.

The preceding fork commit adds the required Bebop settlement-target check for newer simulation versions. No new venue executors or router addresses were added here. Remove this source override when these compatibility changes are available in a pinned upstream fork revision.

Only the library/CLI source, embedded configuration, and package README/manifest are copied; upstream examples and integration tests are not included in this compatibility snapshot.
