# @ewhauser/eve-ambient

Durable ambient attention for Eve agents.

`@ewhauser/eve-ambient` sits between event ingestion and agent cognition. It
accepts typed events, deduplicates and filters them, correlates related events,
buffers them by key, makes a bounded rule or model decision, and delivers
immutable evidence to an Eve session only when the agent should wake.

```text
   Eve channels      signal pipeline      durable log
         │                  │                  │
         └──────────────────┼──────────────────┘
                            ▼
                        publish()
                            │
               ┌─ eve-ambient ──────────┐
               │            ▼           │
               │  dedupe · filter ──► ✕ │
               │            ▼           │
               │  correlate · buffer ◄──┼── [ Postgres | celld ]
               │            ▼           │
               │  decide ── ignore ─► ✕ │
               │            ▼ wake      │
               │        deliver         │
               └─────┬──────────┬───────┘
                     ▼          ▼
      Postgres: runs · audit    Eve session
```

## Why?

Agents are most useful when they can notice what is happening around them, not
only when someone sends them a direct prompt. But turning every message,
webhook, alert, or state change into an agent run is noisy, expensive, and hard
to operate safely.

The usual alternative is application-specific glue: an in-process debounce,
some queue consumers, a classifier, and enough retry state to keep the pieces
together. That works until a process restarts, an event is delivered twice, a
hot key overwhelms a worker, or a classifier succeeds but delivery fails.

Eve Ambient provides a durable attention layer for that gap. It keeps event
data separate from trusted task instructions, gives every decision and
delivery a stable identity, and makes buffering, cooldown, retry, and
retention explicit rather than incidental.

It is not a general event bus or workflow engine. Channels and external systems
still own transport and normalization; Eve Ambient decides whether normalized
events merit cognition.

## One attention model, several deployment choices

Event architectures are rarely interchangeable. A small deployment may value
one database and minimal operations. A high-volume chat system may need a
partitioned ingress log, consumer groups, and independently scalable mailboxes.
Another organization may already operate a signal-detection pipeline and want
to send Eve only actionable events.

Eve Ambient keeps the monitoring and attention semantics consistent while
allowing those deployment boundaries to change.

| Profile | What enters Eve Ambient | Payload custody | Mailbox and timers | Scale profile | Complexity | Maturity |
|---|---|---|---|---|---|---|
| **Postgres-first** | Normalized channel events | Full branch and batch values in PostgreSQL until terminal completion | PostgreSQL due scans and leased claims | Add workers horizontally and scale the database vertically first | Low | Supported; default |
| **Bring your own signal pipeline** | Events already selected by an external rules, stream-processing, or detection system | The external system before acceptance; PostgreSQL after `publish()` | PostgreSQL by default | Eve work follows the selected-event rate instead of the raw firehose | Medium | Supported through the publishing API |
| **External log + distributed mailbox** | Channel or gateway events consumed from Kafka or another durable log | External log before Eve acceptance; PostgreSQL owns the branch until celld accepts a complete copy, then celld owns the mailbox payload | celld owns full-payload per-key buffers and alarms | Horizontally partitioned ingestion and correlation | High | Full-value celld is implemented but experimental; external-log bridges are application-owned |

The celld tier does not run filters. Schema validation, dedupe, deterministic
filtering, correlation, loop prevention, and event budgets run before a
filter-surviving complete event envelope is appended to its cell. The cell can
evaluate that work after the sender's local payload copy is deleted; it never
resolves an Eve event reference.

See [Deployment options](https://github.com/ewhauser/eve-ambient/blob/main/docs/deployment-options.md)
for the full responsibility boundaries, tradeoffs, and selection guidance.

## How it works

Across every deployment profile, the runtime provides:

- typed ingress with source-event deduplication;
- synchronous deterministic filtering and exact correlation;
- immediate or bounded debounce buffering with cooldown accumulation;
- restricted rule or structured model decisions;
- separate trusted instructions and untrusted structured evidence;
- tenant, application, monitor, and key-scoped budgets;
- durable runs, retries, dead letters, retention, and shadow mode; and
- idempotent delivery through channel-owned conversation bindings.

The detailed concepts and APIs are covered in the
[Monitoring model](https://github.com/ewhauser/eve-ambient/blob/main/docs/monitoring-model.md).

## Install

```sh
pnpm add @ewhauser/eve-ambient
```

The package requires Node.js 24 or newer. It does not require a particular Eve
release at runtime; applications supply their channel, delivery, storage, and
model adapters.

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
- [Deployment options](https://github.com/ewhauser/eve-ambient/blob/main/docs/deployment-options.md) — choose an ingestion, event-log, and mailbox topology.
- [Monitoring model](https://github.com/ewhauser/eve-ambient/blob/main/docs/monitoring-model.md) — define channel events and monitors, then wire decisions and delivery.
- [Postgres-first deployment](https://github.com/ewhauser/eve-ambient/blob/main/docs/postgres.md) — run the supported default with PostgreSQL as the by-value mailbox.
- [Prefiltered ingress](https://github.com/ewhauser/eve-ambient/blob/main/docs/prefiltered-ingress.md) — connect an existing rules, detection, or stream-processing pipeline.
- [celld mailbox](https://github.com/ewhauser/eve-ambient/blob/main/docs/celld.md) — operate the experimental distributed mailbox and alarm tier.
- [Operations and security](https://github.com/ewhauser/eve-ambient/blob/main/docs/operations-and-security.md) — durability, rollout, retention, trust boundaries, and deliberate limits.
- [celld worker deployment](https://github.com/ewhauser/eve-ambient/blob/main/celld-worker/README.md) — build and deploy the packaged worker.

The optional `@ewhauser/eve-ambient/ai-sdk` adapter additionally requires `ai`
and `zod`:

```sh
pnpm add ai zod
```

## Package migration

This scoped package continues the former unscoped `eve-ambient` package.
Existing applications should replace both the dependency name and import
specifiers with `@ewhauser/eve-ambient`; the runtime API is unchanged.
