# Attention engine protocol

Eve Ambient has one portable durable command:

```ts
interface AttentionEngine {
  accept(fanout: AcceptedFanout): Promise<AttentionAcceptanceReceipt>;
}
```

The memory engine is the reducer reference. The production engine maps the
same protocol onto Workflow World runs, hooks, steps, sleeps, and output
streams.

## Lineage

```text
eventKey -> occurrenceKey -> branchKey
partitionKey -> partitionCellKey -> instanceKey -> batchKey -> runKey -> wakeKey
```

Every key binds to a canonical input hash. Same key and hash is a duplicate;
same key with different input is an `IdempotencyConflictError`.

## World flow

```text
accept(fanout)
  |
  +-> validate and reject capacity before durable storage
  |
  +-> event hook token: eve-ambient:event:<engine>:<eventKey>
        |
        +-> freeze original membership
        +-> submit each full branch in order
              |
              +-> partitioned correlation hook token:
                    eve-ambient:correlation:<engine>:<instanceKey>
                    |
                    +-> validate + append + reply to event hook
        |
        +-> emit semantic admission receipt on a named World stream

correlation workflow
  +-> buffer immediate/debounced branches
  +-> durable sleep until due
  +-> freeze canonical batch
  +-> prepare callback step
  +-> checkpoint ignore or exact wake in workflow history
  +-> deliver callback step
  +-> retry exact wake, apply cooldown, expire idempotency state
```

Concurrent random Workflow run starts elect one owner through deterministic
hook-token conflict handling. Commands are also resumed into newly registered
owners because a World may expose hook registration before scheduling the
owner continuation. Reducer-level deduplication makes that wake-up safe.

Transport storage is not semantic acceptance. The event coordinator advances
only after the correlation workflow resumes it with an append receipt. This is
why a lost branch response can retry without repeating completed branches.

The channel-owned `partitionKey` defines the outer serialization boundary.
`instanceKey` combines its derived partition cell with rule ID, rule version,
and the rule's optional sub-correlation key. Correlation never crosses channel,
installation, tenant, application, or partition boundaries.

## Callbacks

Workflow inputs cannot serialize application functions. The World binding
therefore exposes an authenticated HTTP handler. Durable steps post complete
frozen batches and prepared wakes by value to `/ambient/prepare` and
`/ambient/deliver`.

`prepare()` is bounded and tool-less; it may repeat if its response was lost
before checkpointing. `deliver()` is the final idempotent action and must use
`wakeKey`. Delivery retries use the exact checkpointed wake bytes.

## State and history

The pure reducer drops terminal event and wake values and retains only bounded
receipts. A World may still retain those values in its append-only run event
log and hook payloads. Ambient provides no query or replay surface over that
history. Operators must set World-level retention, encryption, backup, and
deletion policy appropriate to the payloads.

## Conformance

The shared reducer suite checks lineage, membership freeze, partial handoff,
capacity, canonical ordering, debounce, cooldown, callback validation,
bounded failures, exact-wake retries, and receipt expiry. World integration
adds owner election, semantic receipt handoff, concurrent correlation streams,
autonomous timers, callback authentication, and official Postgres World
startup where configured.
