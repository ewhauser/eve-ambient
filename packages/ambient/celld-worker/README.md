# celld AttentionEngine worker

This worker is the complete celld implementation of
`@ewhauser/eve-ambient`'s `AttentionEngine`. It has no PostgreSQL dependency.

- Event-key cells durably freeze source input and complete fan-out membership.
- Instance-key cells serialize correlation, buffering, batches, retries,
  cooldown, prepared outcomes, and final delivery.
- Every handoff contains the complete value and its idempotency lineage.
- Terminal workflows delete event payloads while retaining bounded receipts.
- The worker exposes only admission, internal branch append, and payload-free
  diagnostics. It has no event lookup, history, or replay route.

Copy this directory from the package, configure `wrangler.jsonc`, inject
`ATTENTION_SECRET` through your secret store, and deploy it with celld. The
fleet and application callback URLs must be reachable from cells. The same
secret authenticates client admission, internal cell handoff, and the
application-owned `/ambient/prepare` and `/ambient/deliver` callbacks.

Run `node build.mjs` before deployment to verify the exact package worker
bundles for the target runtime.
