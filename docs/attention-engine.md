# Attention engine protocol

Eve Ambient has one portable durable backend command:

```ts
interface AttentionEngine {
  accept(fanout: AcceptedFanout): Promise<AttentionAcceptanceReceipt>;
}
```

An application normalizes a provider delivery, evaluates deterministic rules,
and compiles the selected branches before calling `accept()`. The engine then
owns durable branch handoff, correlation, buffering, retries, and cleanup.
There is no portable store, transaction, event lookup, history, or replay API.

## Lineage

```text
channel partitionKey -> partition cell
provider identity -> eventKey -> occurrenceKey -> branchKey
                                              -> instanceKey
                                              -> batchKey -> runKey -> wakeKey
```

Each key names one logical operation. The accompanying input hash binds that
key to canonical input bytes. A retry with the same key and hash is a
duplicate; the same key with a different hash is a conflict.

`eventKey` identifies the canonical source delivery. `occurrenceKey` adds the
source input hash, so a valid canonical occurrence has a stable parent.
`branchKey` freezes rule version, phase, and correlation membership.
The channel-owned `partitionKey` is separately hashed with application,
tenant, channel, and installation identity to select a bounded durable custody
cell. `instanceKey` includes that partition, so rule correlation never crosses
its outer serialization boundary.
`batchKey` freezes the canonically ordered branch set. `runKey` identifies the
decision attempt lineage, and `wakeKey` identifies the final Ambient action.

## By-value custody

`AcceptedFanout`, every `FullAttentionBranch`, and every
`FrozenAttentionBatch` carry complete event values. No key is a reference to a
central event object. Each durable hop retains its payload until the next hop
accepts it, then may delete the prior copy.

After terminal completion, a backend deletes the event payloads. It may retain
bounded payload-free receipts through the configured deduplication horizon.
Backend-native database, log, or cell retention does not create a system-level
event interface.

## Membership freeze

The publisher evaluates all registered rule filters and compiles the complete,
canonically ordered fan-out before admission. The manifest hash commits to
every selected `branchKey` and branch input hash. `accept()` returns only after
all frozen branches have been accepted by their correlation workflows.

A retry cannot silently add, remove, or alter a branch. The first accepted
membership remains frozen; reuse of any existing key with different input
fails with an idempotency conflict.

## Channel partition contract

Every channel defines a deterministic `partitionKey(event)`. It should name
the smallest bounded entity whose events may need serialized correlation: a
GitHub pull request, Slack thread, incident, customer conversation, or similar
domain object. Provider retries for the same event must always produce the
same partition. Changing the partition algorithm is a canonicalization change
and requires a new channel version.

The partition is independent of deployed rules. `eventKey` deduplicates one
source operation inside it, while a rule's optional `correlationKey` selects a
sub-workflow inside it. Without one, the rule gets one workflow per partition.
Correlation keys are local to the partition; applications that need
correlation across a larger domain must choose that larger domain as the
channel partition. This keeps cell cardinality proportional to active domain
entities rather than deliveries without making one global hot cell.

## Durable workflow

For each correlation instance the backend:

1. appends a complete, validated branch and records a bounded branch receipt;
2. applies the immutable rule policy for immediate or debounced buffering;
3. freezes a canonical batch and assigns its batch and run keys;
4. claims the run with a renewable retry lease;
5. calls `prepare(batch)`;
6. records the resulting ignore or exact prepared wake;
7. calls `deliver(wake)` only after that checkpoint exists; and
8. records the delivery receipt, applies cooldown, and deletes terminal
   payloads.

If `prepare()` succeeds but its result is not checkpointed, it may be repeated.
It must therefore be bounded and must not perform final actions. If a delivery
response is lost, the backend retries the exact recorded wake bytes with the
same `wakeKey`.

## Application callbacks

`createAttentionCallbacks()` maps frozen batches to immutable rule versions and
prepared wakes to named routes:

```ts
import { createAttentionCallbacks } from "@ewhauser/eve-ambient/protocol";

const callbacks = createAttentionCallbacks({
  rules: [rule],
  routes: [eveRoute],
});
```

`prepare()` returns either `{ kind: "ignore", decision }` or a wake containing
`routeId`, an explicit delivery `target`, trusted `instruction`, untrusted
`evidence`, and `decision`.
`deliver()` must treat `wakeKey` as the idempotency key of the final durable
action and return the same receipt for a matching retry.

## Backend conformance

The memory, PostgreSQL, and celld engines run the same failure-oriented suite.
It checks source and branch conflicts, frozen membership, concurrent accepts,
ordering, capacity, retry leases, prepare/deliver recovery, exact prepared
wake reuse, cooldown, terminal failure, and payload cleanup.

PostgreSQL implements the reducer with private per-event and per-correlation
rows. celld co-locates event receipts and correlation workflows in one cell per
channel partition. Their storage layouts are not interchangeable contracts.
