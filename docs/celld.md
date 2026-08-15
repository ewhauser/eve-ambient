# celld attention engine

The celld backend implements the full durable attention protocol without
PostgreSQL.

```text
publisher -> channel-partition cell -> prepare callback
                                  \-> deliver callback -> Eve
```

The channel chooses a stable, bounded partition such as one GitHub pull request
or Slack thread. Its cell freezes source input and fan-out membership, uses
`eventKey` to deduplicate deliveries, and hosts the rule correlation workflows
for that partition. Those workflows own buffering, timers, retry leases,
prepared outcomes, delivery, cooldown, and cleanup. All custody remains by
value inside that cell.

## Application binding

```ts
import { defineAmbientApplication } from "@ewhauser/eve-ambient";
import { celld } from "@ewhauser/eve-ambient/celld";

const application = defineAmbientApplication({ applicationId, rules, routes });
const ambient = application.with(celld({
  url: process.env.CELLD_PUBLIC_URL!,
  secret: process.env.ATTENTION_SECRET!,
}));

export const handleAmbientCallback = ambient.fetch;
await ambient.publish(channel, providerEvent);
```

The binding constructs admission and callbacks from the same rule registry and
secret. `publish()` returns only after every frozen branch is durably accepted
by its partition cell.

## Callback endpoint

Partition cells need authenticated access to the application-owned callbacks:

Mount `ambient.fetch` at `/ambient/prepare` and `/ambient/deliver`. Requests and
responses carry the complete frozen batch, prepared outcome, wake, or receipt.
The callbacks never load event data from a separate system. The low-level
`CelldAttentionEngine` and `createAttentionCallbackFetchHandler()` APIs remain
available for custom frameworks.

## Worker

Create the packaged worker scaffold without copying internal files manually:

```sh
pnpm exec eve-ambient init celld ./attention-worker
```

Configure:

- `ATTENTION_CALLBACK_URL`, normally the application's `/ambient` base URL;
- `ATTENTION_SECRET` through the deployment secret store; and
- capacity, deduplication, lease, retry, and attempt limits only when overriding defaults.

Run `node build.mjs` before deployment to verify that the exact packaged worker
bundles for the target runtime. Do not commit a real secret in
`wrangler.jsonc`.

The public cell route requires a bearer token. Application callbacks use the
same configured token in the reference worker. Keep the celld administrative
listener and callback endpoints on trusted networks in addition to
authentication.

## Durability and cleanup

Cells use durable state transitions and alarms, not in-process timers.
Preparation and delivery run outside the cell state mutex. The worker reloads
state before committing callback results so concurrent branch admission is not
overwritten.

After terminal completion, partition cells remove full fan-out, branch, batch,
and prepared-wake payloads and retain only bounded event, branch, and delivery
receipts. The worker exposes
payload-free diagnostics but no event lookup, history, or replay route.

One cell may contain multiple event receipts and rule workflows. `eventKey`
therefore never addresses a cell. For the example's ten webhook deliveries
covering nine logical events on two pull requests, celld creates exactly two
partition cells.

The celld implementation runs the same conformance suite as memory and
PostgreSQL. See `examples/eve-celld` for an executable PR/CI shepherd built on
Eve's native GitHub channel.
