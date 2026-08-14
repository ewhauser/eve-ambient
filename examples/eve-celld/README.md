# Eve + celld example

This private workspace shows the experimental full-payload celld composition.
PostgreSQL still owns definitions, runs, receipts, and audit state; a cell owns
complete mailbox payloads after its append receipt. Neither tier exposes event
lookup or replay.

See the core [celld guide](../../docs/celld.md) for deployment and the
[`@ewhauser/eve-ambient-eve` README](../../packages/eve-adapter/README.md) for
the required Eve patch.
