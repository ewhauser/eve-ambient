# Deployment options

Eve Ambient separates the attention model from the infrastructure used to feed
and schedule it. A deployment is a set of related choices rather than one
all-or-nothing backend selection.

## The important boundaries

Evaluate a topology along these dimensions:

| Concern | Question |
|---|---|
| Ingress | Which component receives provider events and normalizes them into channel events? |
| Selection | Does Eve Ambient see the raw normalized stream, or only events selected upstream? |
| Durable acceptance | At what point may a webhook be acknowledged or a source offset be committed? |
| Event ownership | Where do normalized payloads live for replay, dedupe, and evaluation? |
| Mailbox ownership | Which system serializes one correlation key, buffers events, and schedules debounce or cooldown expiry? |
| Ordering and replay | Which guarantees exist, and which component is responsible for preserving them? |
| Operations | How many stateful systems, workers, credentials, and failure boundaries must be operated? |

The same monitor definition can be used across the supported profiles, but
changing mailbox ownership for an existing deployment is a state migration,
not a runtime toggle.

## Comparison

| Profile | Ingress and selection | Event and payload storage | Mailbox and timers | Scaling | Operational burden | Maturity |
|---|---|---|---|---|---|---|
| **Postgres-first** | Eve channels normalize events; Eve Ambient filters after durable acceptance | PostgreSQL | PostgreSQL instance rows, due scans, and leased claims | Add workers and scale PostgreSQL vertically; validate the actual workload before introducing another stateful tier | One durable system plus workers | Supported; default |
| **Bring your own signal pipeline** | An external system selects events and calls `publish()` | External system before Eve acceptance; PostgreSQL for accepted events | PostgreSQL unless celld is selected separately | Raw-stream work scales outside Eve; Eve scales with the selected-event rate | External pipeline plus Eve and PostgreSQL | Supported through the publishing API |
| **External log + distributed mailbox** | A channel gateway writes a durable log; partitioned consumers normalize and may perform coarse selection before publishing | Kafka or a similar log owns the raw stream; PostgreSQL holds events accepted through `publish()`, runs, decisions, dead letters, and audit | celld cells hold references, per-key lifecycle state, and alarms | Partition consumers by the upstream log and add celld nodes for mailbox concurrency | Multiple stateful systems and explicit handoff recovery | Supported as a custom `publish()` bridge with experimental celld; a first-class Kafka/EventLog adapter is not shipped |

No generic events-per-second claim applies to the Postgres-first profile.
Throughput depends on payload size, subscription fan-out, correlation-key
distribution, buffer policy, database hardware and configuration, worker
concurrency, and decision frequency. Publish a number only with a reproducible
benchmark for the intended deployment shape.

## Postgres-first

This is the right starting point for most installations.

```text
provider -> channel -> publish() -> PostgreSQL -> drain() -> decision -> delivery
                                  event store     mailbox
                                  dedupe          timers
                                  audit           leases
```

`publish()` returns after the normalized event and matching subscription
snapshots commit. It does not wait for filtering, a model call, or delivery.
Workers call `drain()` to filter subscriptions, update per-key mailboxes, claim
due batches, and run decisions.

Choose this profile when minimizing operational complexity is more important
than independently scaling the event log and correlation mailbox. See
[Postgres-first deployment](postgres.md).

## Bring your own signal pipeline

Some systems already have a rules engine, CEP layer, SIEM, stream processor, or
domain-specific detector. That system can decide which signals Eve should see
and call `publish()` only for those events.

```text
raw stream -> external selection -> publish() -> PostgreSQL -> Eve attention pipeline
```

Eve Ambient still validates the channel schema, deduplicates provider IDs,
applies the monitor's deterministic filter, enforces budgets, and records the
decision and delivery lifecycle. A trivial monitor filter is acceptable when
the upstream system already performs all deterministic selection.

This profile reduces the volume entering Eve, but it also moves raw-event
replay, upstream ordering, selection explainability, and pre-acceptance loss
recovery outside the package. See [Prefiltered ingress](prefiltered-ingress.md).

## External log and distributed mailbox

The high-volume reference topology separates three stateful responsibilities:

```text
channel gateway -> Kafka -> consumer -> publish() -> PostgreSQL event and audit
                                                   -> filter and correlate
                                                   -> celld mailbox
                                                   -> evaluator -> delivery
```

Kafka, or an equivalent durable log, owns source retention, replay, ordering,
and consumer offsets. A consumer normalizes the record and calls `publish()`;
it may perform coarse selection first when PostgreSQL should not receive the
entire raw firehose. Eve Ambient then retains schema validation, phase handling,
dedupe, deterministic monitor filtering, correlation, and loop prevention.
Only filter-surviving event references are appended to celld. PostgreSQL remains
the authoritative accepted-payload, run, decision, dead-letter, budget, and
audit store.

With today's public API, the upstream consumer may commit its offset after
`publish()` returns `accepted` or `duplicate`. The event and subscription are
then durable in PostgreSQL, and the store-to-cell handoff retries under a stable
subscription ID if a process crashes or a response is lost.

A future external-reference `EventLog` integration could leave accepted
payloads in Kafka and bypass PostgreSQL event acceptance. In that different
topology, the source offset could be committed only after the mailbox append was
durably accepted.

The package does not currently ship a generic `EventLog` interface or Kafka
adapter. Applications can build this topology around the public publishing API,
but should describe the guarantees of their integration instead of implying
that ordering, replay, or payload loading is supplied automatically.

The celld mailbox is separately documented in [celld mailbox](celld.md).

## Choosing a profile

Start with Postgres-first unless an observed bottleneck or an existing platform
boundary gives a concrete reason not to.

Use prefiltered ingress when the organization already has a durable and
observable signal pipeline, or when Eve should intentionally receive only a
small selected subset of a much larger stream.

Add an external log when replayable high-volume ingestion and partitioned
consumption are requirements. Add celld only when per-key mailbox serialization
or PostgreSQL due-scan and advisory-lock traffic is the measured bottleneck.
Those are independent decisions: an external log does not require celld, and
celld does not require Kafka.
