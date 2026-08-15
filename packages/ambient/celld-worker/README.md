# celld AttentionEngine worker

This worker is the complete celld implementation of
`@ewhauser/eve-ambient`'s `AttentionEngine`. It has no PostgreSQL dependency.

- Channel-defined partition cells durably own source admission, fan-out
  membership, and every rule workflow inside that bounded partition.
- Event keys deduplicate deliveries inside a partition; instance keys
  serialize each rule correlation workflow inside the same cell.
- Every handoff contains the complete value and its idempotency lineage.
- Terminal workflows delete event payloads while retaining bounded receipts.
- The worker exposes only admission and payload-free
  diagnostics. It has no event lookup, history, or replay route.

Create this directory with `eve-ambient init celld [directory]`, configure
the application `ATTENTION_CALLBACK_URL`, inject `ATTENTION_SECRET` through
your secret store, and deploy it with celld. The callback base URL must be
reachable from cells. The same secret authenticates client admission and the application-owned
`/ambient/prepare` and `/ambient/deliver` callbacks. Capacity, retention, lease,
and retry variables are optional overrides with bounded defaults.

Run `node build.mjs` before deployment to verify the exact package worker
bundles for the target runtime.
