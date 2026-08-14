# Postgres-first deployment

The Postgres-first profile is the supported default and the recommended place
to start. PostgreSQL stores ingress receipts, full branch handoffs, correlation
mailboxes, timers, actionable runs, decisions, dead letters, budgets, dedupe
tombstones, and deployment identity. Branch and run event bodies are ephemeral:
they are removed after their next durable handoff or terminal completion.

There is no sleeping workflow or resident actor for each active key. Short-lived
workers claim work using durable leases, process different keys concurrently,
and serialize work for the same key.

## Install and migrate

```sh
pnpm add @ewhauser/eve-ambient
```

Apply `migrations/001_eve_ambient.sql` before initializing the runtime. The
package accepts a `pg`-compatible pool without forcing a particular PostgreSQL
client dependency.

```ts
import { MonitorRuntime } from "@ewhauser/eve-ambient";
import { PostgresMonitorStore } from "@ewhauser/eve-ambient/postgres";

const runtime = new MonitorRuntime({
  applicationId: "engineering-agent",
  deployment: { monitors },
  channels,
  deliveryChannels,
  store: new PostgresMonitorStore({ pool }),
  modelInvoker,
  observer,
});

await runtime.initialize();
```

Initialization validates the deployment and its durable monitor identities. Do
not change monitor IDs or definition versions as if they were stateless labels;
see [Definition rollout](operations-and-security.md#definition-identity-and-rollout).

## Push ingress

After verifying the provider request, normalize and publish the event:

```ts
const result = await runtime.publish(datadogEvents, "alert.changed", {
  tenantId,
  installationId,
  id: deliveryId,
  occurredAt: alert.timestamp,
  data: alert,
  subjects: [{ namespace: "service", key: alert.service }],
  origin: { kind: "external" },
});
```

`publish()` returns after the event and matching subscription snapshots commit.
It does not wait for filtering, a model, or an agent turn. A pull consumer may
commit its source offset after `accepted` or `duplicate`. Retry an ambiguous
outcome with the same stable provider event ID.

## Chat direct dispatch

Use `publishChat()` for chat events. Observed subscriptions are accepted first.
Undispatched subscriptions are created only after every awaited direct handler
succeeds and none returns a durable turn receipt.

```ts
const result = await runtime.publishChat(
  slackEvents,
  "message",
  normalized,
  directHandlers.map(handler => async () => handler(normalized)),
);
```

Provider acknowledgement must remain outside this completion path: acknowledge
according to the channel deadline, then let direct dispatch finish durably. A
failed or unknown direct outcome is dead-lettered and never emits
`undispatched`.

Direct handlers must deduplicate their turn command by provider event ID.
`publishChat()` durably leases the attempt, and a duplicate resumes it after a
worker crash or `TransientMonitorError`. The result reports `pending` while a
lease or retry backoff is active and otherwise returns the persisted
`dispatched`, `undispatched`, or `failed` outcome.

## Run workers

Call `drain()` from short-lived workers or a frequent scheduler:

```ts
const result = await runtime.drain();
```

`drain()` advances accepted subscriptions through filtering and correlation,
updates mailboxes, claims due batches, runs decisions, materializes evidence,
and delivers wakes. Claims use leases so another worker can recover abandoned
work. Due scans are fair across tenants, and different correlation keys run in
parallel.

PostgreSQL uses point reads, an indexed tenant-cardinality count, and a global
sequence behind a lightweight per-domain commit-order fence. The ingress
transaction does not scan instance state or update one sequence row per tenant.

## Scaling

There is intentionally no universal events-per-second claim. Measure with the
deployment's actual:

- payload sizes and source-event rate;
- subscriptions per event and filter selectivity;
- correlation-key cardinality and hot-key distribution;
- immediate versus debounce buffer policy;
- decision and delivery latency;
- database hardware, connection limits, storage, and tuning; and
- worker, subscription, and evaluation concurrency.

Scale workers and PostgreSQL first. Consider a separate durable ingress log
when partitioned ingestion or independent source retention is a requirement. Consider
celld when the measured bottleneck is per-key serialization, due scans, or
mailbox advisory-lock traffic—not simply because the raw ingress rate is high.

See [Deployment options](deployment-options.md) for the complete comparison.

## Production checklist

- Apply the migration through the application's normal schema-change process.
- Give every provider event a stable scoped ID and retry ambiguous acceptance.
- Run enough drain workers to meet the desired filter and evaluation latency.
- Export `MonitorLifecycleEvent` telemetry and alert on delivery failures and
  dead-letter growth.
- Call `purgeExpired()` on a schedule compatible with configured retention.
- Test delivery-channel idempotency and conversation-binding conflicts.
- Exercise new canary input through the normal ingress path before a rollout.
- Back up PostgreSQL according to the decision, receipt, lineage, and audit
  recovery requirements of the application; terminal event payloads are not
  retained for recovery.

More failure and trust boundaries are documented in
[Operations and security](operations-and-security.md).
