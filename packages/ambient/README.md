# @ewhauser/eve-ambient

Durable ambient attention for Eve agents.

`@ewhauser/eve-ambient` is the provider-independent runtime for typed ingress,
correlation, bounded decisions, durable mailboxes, and delivery admission. Read
the repository's [project overview](https://github.com/ewhauser/eve-ambient#readme)
for the problem it solves, architecture, and deployment profiles.

## Install

```sh
pnpm add @ewhauser/eve-ambient
```

The package requires Node.js 24 or newer. The core deliberately has no Eve
dependency; applications can supply their own delivery channel or install the
separately versioned `@ewhauser/eve-ambient-eve` adapter.

The official Eve adapter currently targets exactly `eve@0.38.1` and requires
the application to apply its carried patch for `vercel/eve#1842`. Follow the
[adapter installation procedure](https://github.com/ewhauser/eve-ambient/tree/main/packages/eve-adapter#install-and-apply-the-patch); an unpatched Eve install does not provide the required end-to-end admission key.

For the default production configuration, apply
`migrations/001_eve_ambient.sql` and provide a `pg`-compatible pool:

```ts
import { MonitorRuntime } from "@ewhauser/eve-ambient";
import { PostgresMonitorStore } from "@ewhauser/eve-ambient/postgres";

const runtime = new MonitorRuntime({
  applicationId: "engineering-agent",
  deployment: { monitors },
  channels,
  deliveryChannels,
  store: new PostgresMonitorStore({ pool }),
});

await runtime.initialize();
```

After verifying and normalizing a provider event, publish it and run `drain()`
from short-lived workers or a frequent scheduler:

```ts
await runtime.publish(events, "alert.changed", {
  tenantId,
  installationId,
  id: providerEventId,
  occurredAt,
  data,
  origin: { kind: "external" },
});

await runtime.drain();
```

Start with the [Postgres-first guide](https://github.com/ewhauser/eve-ambient/blob/main/docs/postgres.md)
for a complete production setup. Local tests can use `MemoryMonitorStore` from
`@ewhauser/eve-ambient/memory`.

## Documentation

- [RFC 0001: Full-payload idempotent handoffs](https://github.com/ewhauser/eve-ambient/blob/main/docs/rfcs/0001-full-payload-idempotent-handoffs.md) — accepted direction for payload-by-value custody and end-to-end idempotency lineage.
- [RFC 0002: Durable attention engine](https://github.com/ewhauser/eve-ambient/blob/main/docs/rfcs/0002-durable-attention-engine.md) — proposed replacement of the public store model with one engine command and two callback stages.
- [Deployment options](https://github.com/ewhauser/eve-ambient/blob/main/docs/deployment-options.md) — choose an ingestion, event-log, and mailbox topology.
- [Monitoring model](https://github.com/ewhauser/eve-ambient/blob/main/docs/monitoring-model.md) — define channel events and monitors, then wire decisions and delivery.
- [Persistence responsibilities](https://github.com/ewhauser/eve-ambient/blob/main/docs/storage-responsibilities.md) — understand ingress, branch, mailbox, run, deployment, budget, and retention ownership.
- [Postgres-first deployment](https://github.com/ewhauser/eve-ambient/blob/main/docs/postgres.md) — run the supported default with PostgreSQL as the by-value mailbox.
- [Prefiltered ingress](https://github.com/ewhauser/eve-ambient/blob/main/docs/prefiltered-ingress.md) — connect an existing rules, detection, or stream-processing pipeline.
- [celld mailbox](https://github.com/ewhauser/eve-ambient/blob/main/docs/celld.md) — operate the experimental distributed mailbox and alarm tier.
- [Operations and security](https://github.com/ewhauser/eve-ambient/blob/main/docs/operations-and-security.md) — durability, rollout, retention, trust boundaries, and deliberate limits.
- [celld worker deployment](https://github.com/ewhauser/eve-ambient/blob/main/packages/ambient/celld-worker/README.md) — build and deploy the packaged worker.

The optional `@ewhauser/eve-ambient/ai-sdk` adapter additionally requires `ai`
and `zod`:

```sh
pnpm add ai zod
```

## Package migration

This scoped package continues the former unscoped `eve-ambient` package.
Existing applications should replace both the dependency name and import
specifiers with `@ewhauser/eve-ambient`; the runtime API is unchanged.
