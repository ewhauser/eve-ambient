# RFC 0001: Full-Payload Idempotent Handoffs

- Status: Accepted
- Implementation: Complete
- Scope: Durable handoffs from canonical source admission through final action
- Related: [RFC 0004](0004-correlation-world-protocol.md)

## Decision

Every durable handoff carries a self-contained value. Keys identify lineage and
deduplicate work; they are never payload references that require a later lookup.

A correctness-critical handoff contains:

1. a stable idempotency key;
2. a hash of the complete logical input bound to that key;
3. the full payload needed by the receiver; and
4. enough lineage to derive downstream keys.

Same key and same input hash returns the previously recorded result. Same key
with a different input hash is a conflict and must not be processed.

## Boundaries

```text
provider delivery
  -> canonical event
  -> correlation-stream append
  -> frozen batch
  -> prepared wake checkpoint
  -> final durable delivery
```

Each arrow passes a complete value for the next component. A component may
discard an earlier representation after the next durable boundary accepts
custody.

The current lineage is:

```text
eventKey -> occurrenceKey -> branchKey
partitionKey -> partitionCellKey -> instanceKey -> batchKey -> runKey -> wakeKey
```

`eventKey` identifies the stable provider event inside its application,
tenant, channel, and installation. `occurrenceKey` additionally binds the
canonical input hash. Branch and stream keys add rule, version, partition, and
correlation identity. Batch, run, and wake keys bind terminal processing and
the final effect.

## Source admission

Canonicalization must be deterministic. A producer retries an ambiguous result
with the same source identity and complete payload. It acknowledges or commits
its upstream delivery only after the selected durable ingress boundary accepts
custody.

Ambient may sit behind a queue, stream processor, SIEM, or rules engine. That
upstream system owns raw-event retention, offsets, and replay. Selected events
enter Ambient through the same canonical publisher contract.

## Correlation streams

Ambient groups a source event by correlation address and sends one complete
append to each selected stream. The stream stores full branches while they are
buffered, sealed, or active. Its bounded recent-message ring stores only keys,
hashes, timestamps, and receipts.

Recent-message eviction makes source deduplication best effort. Correctness at
the final action does not rely on that ring: a reprocessed identical batch
derives the same `wakeKey`, and the final durable receiver must enforce it.

## Preparation and delivery

`prepare()` is bounded computation and may repeat. Its result is not an action.
When preparation chooses to wake, the complete `PreparedAttentionWake` is
recorded before delivery begins. Delivery retries reuse that exact value.

The final route must propagate `wakeKey` into the durable side-effecting system.
A matching retry returns the original receipt; a conflicting retry fails
closed. This is effectively-once final delivery, not exactly-once execution.

## No repository or replay contract

Ambient exposes no central event repository, payload loading, history, or
replay API. A World implementation may retain snapshots, logs, transport
bodies, or backups internally, but those bytes do not become an Ambient query
surface.

Retention, encryption, erasure, and backup guarantees belong to the selected
World and final delivery systems. Removing a payload from live reducer state is
not evidence of physical erasure elsewhere.

## Authority

Event actors and origins are provenance. They do not delegate credentials or
authorization to a route. Every action executes with the application's
configured principal and must enforce tenant, target, and loop boundaries.

## Required invariants

1. No durable handoff contains a payload reference in place of required data.
2. Every stable key is checked against its logical input hash.
3. Branch and batch ordering is canonical rather than arrival-dependent.
4. Preparation is checkpointed before any final action.
5. Delivery retries reuse the same prepared wake and `wakeKey`.
6. Ambiguous transport results are retried with the original complete value.
7. Ambient makes no replay or historical-query promise.

## Historical implementations

Earlier revisions specified custom Postgres records, a celld mailbox, event
coordinators, and Workflow runs. Those implementation plans are superseded by
RFC 0004. Git history retains their full text; they are not part of the current
repository contract.
