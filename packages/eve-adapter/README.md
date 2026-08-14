# @ewhauser/eve-ambient-eve

The official Eve delivery adapter for `@ewhauser/eve-ambient`.

```sh
pnpm add @ewhauser/eve-ambient @ewhauser/eve-ambient-eve eve@0.38.1
```

The adapter targets exactly Eve `0.38.1`. Consumers must apply the included
`patches/eve@0.38.1.patch` for `vercel/eve#1842`; the patch makes Eve's durable
session admission honor the supplied idempotency key.

## Attention route

```ts
import { createEveAttentionRoute } from "@ewhauser/eve-ambient-eve";

const eveRoute = createEveAttentionRoute({
  id: "eve",
  from: channelFrom,
  address: wake => `agent:${wake.tenantId}`,
  auth: wake => authForTenant(wake.tenantId),
});
```

Use the route with `createAttentionCallbacks({ rules, routes: [eveRoute] })`.
It serializes trusted instructions separately from untrusted evidence and maps
the prepared wake's `wakeKey` directly to Eve's `idempotencyKey`. A retry of
the same recorded wake therefore reaches the same durable Eve turn.

Customize `renderMessage` only when the replacement preserves the trust
boundary and complete lineage needed by the receiving agent.

## Direct dispatch

```ts
import { createEveDirectDispatchAdapter } from "@ewhauser/eve-ambient-eve";

const direct = createEveDirectDispatchAdapter({
  from: channelFrom,
  address: request => `agent:${request.tenantId}`,
  auth: request => authForTenant(request.tenantId),
});
```

Direct chat delivery is not part of the attention workflow. Configure it as
the publisher's optional `direct.adapter`. The adapter maps the request's
stable direct-dispatch key to Eve's admission key and returns an idempotent
receipt containing the matching key and input hash.

This package stores no events or workflow state. Durable correlation and
prepared-wake retry belong to the selected attention engine; durable final
admission belongs to Eve.
