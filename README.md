# Eve Ambient

Durable ambient attention for Eve agents.

Applications define ambient rules against typed channel events. Eve Ambient
then sits between event ingestion and agent cognition: it deduplicates and
filters events, correlates related activity, buffers it by key, makes a bounded
rule or model decision, and delivers immutable evidence to an Eve session only
when the agent should wake.

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
data separate from trusted task instructions, carries stable idempotency
lineage through delivery so integrations can derive keys for final durable
actions, and makes buffering, cooldown, retry, and retention explicit rather
than incidental.

It is not a general event bus, replay system, or workflow engine. Channels and
external systems still own transport and normalization; Eve Ambient decides
whether normalized events merit cognition. Active handoffs carry complete
payloads by value. On terminal completion, Eve Ambient removes the event
payload and retains only the required lineage and receipt metadata for its
configured operational retention.

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
| **Bring your own signal pipeline** | Events already selected by an external rules, stream-processing, or detection system | The external system before acceptance; full PostgreSQL branch values after `publish()` | PostgreSQL by default | Eve work follows the selected-event rate instead of the raw firehose | Medium | Supported through the publishing API |
| **External log + distributed mailbox** | Channel or gateway events consumed from Kafka or another durable log | External log before Eve acceptance; PostgreSQL owns the branch until celld accepts a complete copy, then celld owns the mailbox payload | celld owns full-payload per-key buffers and alarms | Horizontally partitioned ingestion and correlation | High | Full-value celld is implemented but experimental; external-log bridges are application-owned |

The celld tier does not run filters. Schema validation, dedupe, deterministic
filtering, correlation, loop prevention, and event budgets run before a
filter-surviving complete event envelope is appended to its cell. The cell can
evaluate that work after the sender's local payload copy is deleted; it never
resolves an Eve event reference.

See [Deployment options](docs/deployment-options.md) for the full responsibility
boundaries, tradeoffs, and selection guidance.

## How it works

Across every deployment profile, the runtime provides:

- typed ingress with source-event deduplication;
- synchronous deterministic filtering and exact correlation;
- immediate or bounded debounce buffering with cooldown accumulation;
- restricted rule or structured model decisions;
- separate trusted instructions and untrusted structured evidence;
- tenant, application, monitor, and key-scoped budgets;
- durable runs, retries, dead letters, retention, and shadow mode; and
- stable delivery admission identity and root lineage from which integrations
  can derive idempotent final-action keys.

The detailed concepts and APIs are covered in the
[Monitoring model](docs/monitoring-model.md).

## Repository

This repository is a pnpm workspace containing the provider-independent
attention runtime, its supported Eve integration, deployment examples, and
integration conformance tests.

| Workspace | Purpose | Published |
|---|---|---|
| [`packages/ambient`](packages/ambient) | Typed monitors, idempotency lineage, PostgreSQL and celld mailboxes | `@ewhauser/eve-ambient` |
| [`packages/eve-adapter`](packages/eve-adapter) | Eve channel delivery with the carried `vercel/eve#1842` patch | `@ewhauser/eve-ambient-eve` |
| [`examples/eve-postgres`](examples/eve-postgres) | Slack incident rule with Eve delivery and the supported PostgreSQL-first runtime | No |
| [`examples/eve-celld`](examples/eve-celld) | GitHub pull-request rule with Eve delivery and the experimental full-payload celld mailbox | No |
| [`integration/eve-conformance`](integration/eve-conformance) | Exact-version patch and adapter conformance | No |

## Install

Install the core runtime when your application supplies its own delivery
channel:

```sh
pnpm add @ewhauser/eve-ambient
```

Install `@ewhauser/eve-ambient-eve` when delivering directly to Eve. The Eve
adapter supports one exact Eve version and requires the application to apply
the patch shipped in the adapter package. See the
[`@ewhauser/eve-ambient-eve` README](packages/eve-adapter/README.md) for the
copy-and-verify procedure.

Start with the [Postgres Slack incident example](examples/eve-postgres) for the
supported default or the [celld GitHub pull-request example](examples/eve-celld)
for the experimental distributed mailbox.

## Documentation

- [Full-payload idempotent handoffs RFC](docs/rfcs/0001-full-payload-idempotent-handoffs.md)
- [Deployment options](docs/deployment-options.md)
- [Monitoring model](docs/monitoring-model.md)
- [Persistence responsibilities](docs/storage-responsibilities.md)
- [Postgres-first deployment](docs/postgres.md)
- [Prefiltered ingress](docs/prefiltered-ingress.md)
- [celld mailbox](docs/celld.md)
- [Operations and security](docs/operations-and-security.md)

## Development

```sh
corepack enable pnpm
pnpm install
pnpm check
```

`pnpm check` builds and tests every workspace, verifies the installed Eve
patch, and validates both publishable npm artifacts.
