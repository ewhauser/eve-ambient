# RFC 0004: Correlation World Protocol

- Status: Accepted
- Implementation: Complete
- Scope: Replace Workflow runs and backend-specific engines with one
  correlation-addressed stream object
- Preserves: full-value custody, deterministic lineage, correlation
  serialization, checkpoint-before-delivery, and final `wakeKey` idempotency
- Supersedes: RFC 0003 and RFC 0002 admission-coordinator details

## Decision

One correlation address owns one durable World object. Every selected source
event is sent directly to that object with one atomic `append` RPC.

```ts
interface AttentionWorld {
  stream(key: AttentionInstanceKey): AttentionStream;
}

interface AttentionStream {
  append(input: AttentionStreamAppend): Promise<AttentionStreamAppendReceipt>;
}
```

`stream(key)` is deterministic local address construction, not a remote
lookup. Branches for the same address are grouped into one append. Appends for
different addresses run concurrently.

```text
one event -> one correlation       = 1 RPC
next event -> same correlation     = 1 RPC to the same object
one event -> N correlations        = N concurrent RPCs
empty selected fanout              = 0 RPCs
```

## Deduplication

Each stream keeps a bounded ring of recent event keys, input hashes, semantic
receipts, and timestamps. The reference size is 48. A remembered key/hash
returns `duplicate`; a remembered key with another hash conflicts. Eviction is
allowed, so admission deduplication is best effort.

In-flight branch identity remains until its payload reaches a terminal batch
outcome. Exact final effects do not depend on the source ring: `wakeKey` is
derived from frozen batch identity and must be enforced by the final durable
delivery system.

## State machine

The correlation object owns the pure reducer state: recent messages, open and
sealed batches, active claim and lease, prepared wake checkpoint, retry time,
cooldown, bounded delivery receipts, and bounded terminal outcomes.

The object calls the application by value:

```text
due batch -> POST /ambient/prepare -> atomic wake checkpoint
           -> POST /ambient/deliver -> terminal cleanup
```

`prepare` may repeat. Delivery always retries the exact checkpointed wake.

## Failure model

Ambient validates complete fanout capacity before calls start, then allows all
independent appends to settle. If any fail, the application sees failure and
retries all groups with the same canonical identity. Receivers that completed
the first attempt deduplicate locally.

There is deliberately no global event coordinator, membership freeze, pending
fanout record, or acceptance-receipt store. A retry may append a newly selected
correlation while existing streams reject duplicate work.

## Implementation boundary

Ambient ships the contract, grouping adapter, authenticated callbacks, pure
reducer, in-memory reference, conformance tests, and instrumented call-count
fixture. It has no Workflow SDK, Postgres driver, Redis client, celld client,
storage schema, poller, or global World runtime.

`world-celld` and other implementations own physical persistence, timers,
placement, snapshots, retention, encryption, deletion, and observability.
They may compose multiple infrastructure systems behind the same object
contract without changing Ambient.

## Consequences

- the normal one-correlation path is one admission RPC;
- correlated events naturally reuse the same durable object;
- partial fanout recovery becomes receiver-local and best effort;
- old source events may re-enter after ring eviction;
- final-effect safety remains strong through `wakeKey`; and
- old custom-backend and Workflow-run state cannot migrate automatically.
