# Attention stream protocol

Ambient exposes one application-facing command and one backend-facing object.

```ts
interface AttentionEngine {
  accept(fanout: AcceptedFanout): Promise<AttentionAcceptanceReceipt>;
}

interface AttentionWorld {
  stream(key: AttentionInstanceKey): AttentionStream;
}

interface AttentionStream {
  append(input: AttentionStreamAppend): Promise<AttentionStreamAppendReceipt>;
}
```

## Admission

```text
accept(fanout)
  |
  +-> validate the complete fanout and capacity
  +-> derive instanceKey for every branch
  +-> group branches by instanceKey
  +-> concurrently call stream(instanceKey).append(group)
  +-> validate every semantic append receipt
  +-> return one payload-free application receipt
```

`stream(key)` must be local address construction; it is not a registry lookup.
The number of admission RPCs is exactly the number of distinct stream keys.
All calls are allowed to finish before an error is returned, so retrying after
a partial failure is safe and predictable.

There is no event coordinator or frozen global fanout membership. A retry may
include a newly selected stream. Existing streams deduplicate their own event
append while it remains in their recent-message ring.

## Stream address and lineage

```text
eventKey -> occurrenceKey -> branchKey
partitionKey -> partitionCellKey -> instanceKey -> batchKey -> runKey -> wakeKey
```

`instanceKey` covers application, tenant, channel, installation, channel
partition, rule ID, rule version, and correlation key. Events with the same
correlation address therefore arrive at the same serialized object without a
placement lookup.

Each key binds to a canonical input hash. A remembered key with the same hash
is a duplicate; the same key with another hash is an
`IdempotencyConflictError`.

## Stream state

One stream atomically owns:

- a fixed-size recent-message ring for best-effort admission dedup;
- open, sealed, and active full-value batches;
- the exact prepared wake checkpoint;
- retry, lease, and cooldown timestamps.

Terminal processing removes source payloads and in-flight branch entries. Ring
entries contain only keys, hashes, timestamps, and receipts. Ring eviction
means an old source append may be processed again; final effects are still
protected by the durable `wakeKey` boundary.

## Callbacks

A remote World posts complete frozen batches and prepared wakes to the
authenticated `/ambient/prepare` and `/ambient/deliver` endpoints.
`prepare()` is tool-less and may repeat. A wake is recorded atomically before
delivery, and every retry uses the exact checkpointed value. Routes must carry
`wakeKey` into the final durable system.

## Conformance

The reference suite checks grouping, same-stream reuse, receiver dedup,
conflicts, capacity, partial failures, ordering, debounce, cooldown, leases,
callback validation, exact-wake retry, bounded rings, and terminal payload
cleanup. The instrumented integration fixture asserts the measured RPC count
and concurrent fanout.
