# celld attention engine

The celld backend implements the full durable attention protocol without
PostgreSQL.

```text
publisher -> event-key cell -> instance-key cell -> prepare callback
                                             \-> deliver callback -> Eve
```

Event-key cells freeze source input and fan-out membership. Instance-key cells
serialize correlation, buffering, timers, retry leases, prepared outcomes,
delivery, cooldown, and cleanup. Handoffs contain complete payloads by value.

## Application client

```ts
import { CelldAttentionEngine } from "@ewhauser/eve-ambient/celld";

const engine = new CelldAttentionEngine({
  url: process.env.CELLD_PUBLIC_URL!,
  secret: process.env.ATTENTION_SECRET!,
});
```

Pass this engine to `createAmbientPublisher()`. `accept()` posts the complete
fan-out to the event-key cell and returns only after every frozen branch is
durably accepted by its correlation cell.

## Callback endpoint

Cells need authenticated access to the application-owned callbacks:

```ts
import { createAttentionCallbackFetchHandler } from "@ewhauser/eve-ambient/celld";

export const handleAmbientCallback = createAttentionCallbackFetchHandler(
  callbacks,
  { secret: process.env.ATTENTION_SECRET! },
);
```

Mount the handler at `/ambient/prepare` and `/ambient/deliver`, or configure
custom paths. Requests and responses carry the complete frozen batch,
prepared outcome, wake, or receipt. The callbacks never load event data from a
separate system.

## Worker

The npm package includes `celld-worker/` with the `AttentionCell` class and a
reference `wrangler.jsonc`. Configure:

- `CELLD_FLEET_URL` for cell-to-cell calls;
- `ATTENTION_PREPARE_URL` and `ATTENTION_DELIVER_URL`;
- `ATTENTION_SECRET` through the deployment secret store; and
- capacity, deduplication, lease, retry, and attempt limits.

Run `node build.mjs` before deployment to verify that the exact packaged worker
bundles for the target runtime. Do not commit a real secret in
`wrangler.jsonc`.

The public cell route requires a bearer token. Internal branch handoffs and
application callbacks use the same configured token in the reference worker.
Keep the celld administrative listener and callback endpoints on trusted
networks in addition to authentication.

## Durability and cleanup

Cells use durable state transitions and alarms, not in-process timers.
Preparation and delivery run outside the cell state mutex. The worker reloads
state before committing callback results so concurrent branch admission is not
overwritten.

After terminal completion, correlation cells remove full branch, batch, and
prepared-wake payloads and retain only bounded receipts. The worker exposes
payload-free diagnostics but no event lookup, history, or replay route.

The celld implementation runs the same conformance suite as memory and
PostgreSQL. See `examples/eve-celld` for an executable GitHub channel rule.
