# Correlation Workflow protocol

Ambient exposes one application-facing command and implements production
durability with standard Workflow APIs:

```ts
interface AttentionEngine {
  accept(fanout: AcceptedFanout): Promise<AttentionAcceptanceReceipt>;
}

// Conceptual Workflow APIs used by the implementation.
resumeHook(correlationToken, append);
start(correlationWorkflow, [config, instanceKey]);
```

There is no Ambient-specific World interface.

## Admission

```text
accept(fanout)
  |
  +-> validate the complete fanout and capacity
  +-> derive instanceKey for every branch
  +-> group branches by instanceKey
  +-> concurrently resume one deterministic hook per instanceKey
       |
       +-> hook exists: transport accepts the append
       |
       +-> hook missing: start candidate run
                         wait for the hook owner
                         resume the owner's hook
  +-> return one payload-free application receipt
```

`accept()` resolves when Workflow has accepted every selected hook payload. It
does not wait for the correlation reducer, prepare callback, or final delivery.
Duplicate and conflicting append outcomes are therefore asynchronous. A
conflicting idempotency value is recorded as a named step in the Workflow run
timeline; it does not terminate the correlation owner.

Two cold publishers may both start candidates because hook lookup and run
creation are not atomic. Every candidate creates the same deterministic hook
and awaits `getConflict()`. Exactly one run owns the token; losing candidates
exit, and publishers resume the owner. The integration suite exercises a
20-publisher cold race.

## Correlation address and lineage

```text
eventKey -> occurrenceKey -> branchKey
partitionKey -> partitionCellKey -> instanceKey -> batchKey -> runKey -> wakeKey
```

`instanceKey` covers application, tenant, channel, installation, channel
partition, rule ID, rule version, and correlation key. Its deterministic hook
token is globally namespaced for the selected World and includes a fingerprint
of the immutable correlation Workflow configuration.

Each key binds to a canonical input hash. A remembered key with the same hash
is a duplicate; the same key with another hash is an
`IdempotencyConflictError`.

## Owned state

One permanent correlation run owns:

- a fixed-size recent-message ring for best-effort admission dedup;
- open, sealed, and active full-value batches;
- the exact prepared wake checkpoint; and
- retry, lease, and cooldown timestamps.

Applied full-value payloads are limited by `maxPendingBranches` and
`maxPendingBytes` (1,000 and 16 MiB by default). The publisher rejects a single
correlation append that cannot fit. At capacity, the run holds at most the next
validated append and waits for its reducer timer without requesting another
hook value. Remaining values stay in Workflow's durable hook queue.

Terminal processing removes source payloads and in-flight branch entries. Ring
entries contain only keys, hashes, timestamps, and receipts. Ring eviction
means an old source append may be processed again; final effects are still
protected by the durable `wakeKey` boundary.

The reducer state is bounded, but Workflow event history is not. The run is not
rotated automatically because Workflow 5 does not yet provide a standard
continue-as-new primitive that atomically preserves hook ownership. Permanently
hot correlations must be monitored against the selected World's per-run event
and retention limits.

## Timers and callbacks

The run races its hook iterator against the next reducer deadline. A moved
debounce deadline is rechecked when the old timer fires, so stale timers cannot
close a newer batch early.

Prepare and deliver are authenticated Workflow steps that post complete frozen
batches and prepared wakes to `/ambient/prepare` and `/ambient/deliver`.
`prepare()` may repeat. A wake is recorded before delivery, and every delivery
retry uses the exact checkpointed value. Routes must carry `wakeKey` into the
final durable system. Invalid or oversized prepare results and invalid delivery
receipts terminate only the affected attention run; they do not terminate the
permanent correlation owner.

## Measured call fanout

The current Workflow 5 integration instruments public standard-World methods:

| Path | Standard World calls | Application HTTP |
|---|---:|---:|
| Warm buffer-only append | 7 | 0 |
| Append that closes, prepares, and delivers a batch | 15 | 2 |

The 7-call warm path is one hook lookup, one run read, three event writes, and
two queue publishes. These are Workflow implementation calls, not seven Ambient
RPCs: Ambient makes one high-level `resumeHook()` call. Cold creation adds a run
start and hook-registration polling, so its count varies with scheduler timing.

## Conformance

The reference and integration suites check grouping, same-correlation reuse,
bounded ring dedup, conflicts, capacity, ordering, debounce, cooldown, leases,
callback validation, exact-wake retry, reducer backpressure, configuration
cutover, concurrent cold ownership, consumer workflow discovery, permanent-run
behavior, and measured World calls.
