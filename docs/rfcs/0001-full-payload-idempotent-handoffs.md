# RFC: Full-Payload, End-to-End Idempotent Event Handoffs

- Status: Accepted
- Implementation: Eve Ambient identity, both mailbox tiers, and central-ingress cleanup implemented; Eve session admission tracked by `vercel/eve#1842`; Kafka pending; SQS deferred
- Scope: Eve Ambient implementation through `wakeKey` delivery, plus a conditional conformance profile through final actions
- Related: `ewhauser/eve-ambient` issue #3

## Summary

Eve Ambient will process events through self-contained, idempotent handoffs.

Every handoff that represents durable work MUST include:

1. A stable idempotency key.
2. A hash of the logical input bound to that key.
3. The complete payload required by the receiving component.
4. Enough lineage to derive stable keys for downstream work.

Every stateful or side-effecting component MUST durably remember the result of processing an idempotency key. Receiving the same key and the same input returns the previously recorded result. Receiving the same key with different input is a conflict and MUST NOT be processed.

Eve will not define a central event repository. It will not pass event references, require later payload lookup, expose event retention as a system capability, or provide replay. Kafka, PostgreSQL, celld, and other backends may retain payloads internally according to their own operation, but Eve assigns no replay or historical-query semantics to that retention. SQS is a possible future transport realization, not part of the current implementation plan.

The intended guarantee for a conforming end-to-end deployment is:

> Eve may execute internal work more than once, but one logical action key produces at most one externally durable effect within the configured idempotency horizon.

This is effectively-once final action, not exactly-once execution. Eve Ambient
alone guarantees stable identity and complete payload custody through the
`wakeKey` delivery request. The final-action guarantee additionally requires a
session/action integration that implements the optional downstream portion of
this protocol.

## Motivation

Distributed transports and workers are at-least-once systems. A provider may retry a webhook after a lost acknowledgement. Kafka may redeliver after a rebalance or an ambiguous transaction result. SQS may deliver a message more than once. A cell or evaluator may commit state and lose its response. A durable session may retry a tool call after a process failure.

Preventing duplicate work at the first layer does not solve these failure modes. The correctness property must survive every boundary through the final durable action.

At the same time, solving this problem does not require a central copy of every event. A central event repository would introduce an unnecessary availability dependency, retention policy, privacy surface, cleanup protocol, and payload-fetch failure mode. References also make downstream work dependent on the lifetime and reachability of an upstream system.

The simpler model is custody by value:

```text
source transport
  -- full event + event key --> fan-out
  -- full event + branch key --> monitor mailbox
  -- full batch + batch key --> evaluator/run
  -- full delivery + wake key --> durable session
  -- full command + action key --> external action adapter
```

After a receiver has durably accepted the full payload, the sender may acknowledge or discard its own copy. There is no global event object and no global event-lifecycle coordinator.

## Goals

- Carry stable idempotency identity from source acceptance through the final action.
- Make every durable handoff self-contained.
- Tolerate duplicate delivery and lost responses at every boundary.
- Detect accidental reuse of a key for different input.
- Allow PostgreSQL, Kafka, celld, and future backends to own their internal persistence.
- Permit payload deletion as soon as a component has completed or durably handed off its work.
- Keep the existing monitor lifecycle semantics: filtering, correlation, batching, cooldown, evaluation, and session delivery.
- Preserve provider and transport independence.
- Define where Eve Ambient's guarantee ends and the conditional requirements for integrations that continue lineage to final actions.

## Non-goals

- Event replay.
- Event archival or historical event queries.
- A central event store, repository, or payload service.
- Public payload references, pointers, or `loadPayload(ref)` APIs.
- A global retention or garbage-collection protocol.
- Exactly-once internal execution.
- Infinite idempotency retention.
- A total order across unrelated transport partitions or correlation instances.
- Framework-owned ordering or coalescing of distinct Eve session deliveries.
- Requiring Eve core to implement `turnKey` or `actionKey` as a condition for completing Eve Ambient.
- Making a non-idempotent external service safe when it offers neither idempotency nor reconciliation.

Automatic retry is not replay. Automatic retry uses the original idempotency key and attempts to complete the original operation. Eve defines no interface for intentionally reprocessing historical events under changed definitions. A new source submission is simply a new logical input and MUST have a new source key.

## Terminology

### Logical input

The complete value a component is asked to process. At ingress and in a monitor mailbox, this is the complete canonical `ChannelEvent`. At later boundaries it may be a complete batch, monitor delivery, session turn, or action command.

"Complete" means that the receiver can finish its work after the sender disappears. A system-owned pointer to payload stored elsewhere is not complete.

### Idempotency key

A stable identifier for one logical operation. Retries reuse the same key. Distinct logical operations use distinct keys, even if their payload bytes are identical.

### Input hash

A cryptographic hash of the canonical logical input and the immutable routing/configuration fields that affect its meaning. It binds a key to exactly one operation.

### Receipt

The durable outcome remembered for an idempotency key. A receipt contains identity, status, timestamps, and the result needed to answer a retry. It does not need to retain the input payload.

### Custody

A component has custody when it has durably accepted the full payload and can resume processing without fetching that payload from the sender or a shared event repository.

### Membership freeze

A membership freeze is the durable, atomic transition that makes a collection's ordered members immutable and assigns the collection one stable operation key. Before the freeze, provisional groups may accept members or be consolidated. After the freeze, duplicates are no-ops and newly arriving members belong to a later collection.

## Core invariants

### 1. Full payload at every handoff

Every durable handoff MUST carry its complete logical input by value.

For event-processing boundaries, the mailbox receives the complete canonical `ChannelEvent`, not an event reference and not a monitor-specific projection. A claimed evaluation batch contains those complete events. The evaluator MUST NOT call back to a central store to load them.

At later workflow boundaries, the full payload means the complete value produced for that boundary. For example, an action adapter receives a complete action command. It need not receive unrelated raw events if the durable workflow has already checkpointed a complete derived command.

The following are prohibited as Eve system interfaces:

- `EventRepository`
- `EventPointer`
- `BufferedEventRef`
- `loadPayload(ref)`
- `getEvent(ref)` as an evaluator dependency
- A hidden fallback from an oversized event to shared payload storage

An event may naturally contain application-domain identifiers or URLs, such as a Slack file URL. Eve treats those as event data. Eve itself MUST NOT replace the event with an infrastructure pointer that another Eve component must resolve.

### 2. Stable identity lineage

Every downstream operation derives a stable child key from its logical parents and immutable operation identity.

```text
eventKey
  +-- directDispatchKey
  +-- branchKey per matched monitor/phase/version
        +-- batchKey
              +-- runKey
                    +-- wakeKey
                          +-- turnKey       optional downstream profile
                                +-- actionKey per durable tool/action call
```

Eve Ambient owns the lineage through `wakeKey`. An integration that advertises
the stronger final-action guarantee continues it through `turnKey` and
`actionKey`. Such a final action carries both its `actionKey` and cause lineage;
for a coalesced action, the lineage contains the stable set of contributing root
event keys.

Transport coordinates, receipt handles, attempts, leases, timestamps, process IDs, and random retry IDs MUST NOT participate in logical key derivation.

### 3. Durable duplicate detection at every stateful boundary

Every component that acknowledges input, changes durable state, or initiates a side effect MUST have a durable idempotency ledger. The ledger may be colocated with that component's normal state and need not be a separate service.

A component MAY delegate this property to one atomic downstream transaction. It MUST NOT acknowledge input based only on an in-memory cache or a transport's best-effort duplicate suppression.

Native Kafka producer idempotence, SQS FIFO deduplication, and similar facilities are useful optimizations. They are not substitutes for Eve's end-to-end lineage and receipts.

### 4. Same key, same input, same result

For a `(namespace, key)` pair:

- First use reserves the key and begins the operation.
- A concurrent matching use observes `in_progress` and waits, retries, or receives a retryable response.
- A completed matching use returns the recorded result.
- The same key with a different input hash is an idempotency conflict.

An idempotency conflict MUST be fail-closed. It MUST NOT overwrite the original receipt, create a second child key, or continue to an external action.

### 5. Membership freezes before derived work begins

Fan-out manifests and evaluation batches MUST freeze their membership before
derived work begins. Any downstream integration that coalesces session turns or
multi-input actions MUST apply the same membership-freeze rule.

The membership list, derived operation key, and transition to claimed/running MUST commit atomically. Every append concurrent with a freeze must serialize either before it and be included, or after it and enter the next collection. A crash cannot produce two operation keys for the same frozen membership.

Lifecycle-specific "sealed" state does not necessarily mean membership is frozen. In particular, a sealed monitor batch remains provisional while the lifecycle may consolidate it with other unclaimed work.

### 6. Acknowledgement follows durable custody

A sender acknowledges or commits its input only after the immediate next durable boundary has accepted the complete payload and returned a stable receipt.

For an external webhook, the immediate next boundary is the configured durable ingress backend. The provider may be acknowledged after PostgreSQL, Kafka, SQS, or another selected backend durably accepts the complete canonical event. The webhook request MUST NOT wait for every eventual monitor mailbox or celld round-trip.

When a fan-out worker consumes that durable ingress record, its immediate next boundaries are the frozen branch set. The worker commits or acknowledges its ingress record only after every branch is terminal or has durably accepted the complete event. If only some branches succeed, retry uses the same event and branch keys; successful branches return their original append receipts.

This is a hop-by-hop custody rule, not one synchronous transaction from provider webhook to final monitor mailbox.

### 7. Payload lifetime is local

No global rule requires storing an event after a component is done with it.

A component may discard a payload after either:

- It has reached a terminal local outcome; or
- Every required downstream receiver has durably accepted a complete successor payload.

The component may retain the payload longer because of its backend's ordinary retention or compaction behavior. That is an implementation detail, not an Eve guarantee.

Idempotency receipts normally outlive payloads. A receipt needs only the key, input hash, status, outcome, and relevant timestamps.

## Canonical envelopes

The following types are illustrative. Exact public naming may change during implementation.

```ts
type IdempotencyKey = string;

interface IdempotencyContext {
  /** Identity of this logical operation. */
  readonly key: IdempotencyKey;
  /** Hash of the canonical logical input. */
  readonly inputHash: string;
  /** Immediate parent operations, if any. */
  readonly parentKeys: readonly IdempotencyKey[];
  /** Root source events contributing to this operation. */
  readonly eventKeys: readonly IdempotencyKey[];
}

interface IdempotentEnvelope<T> {
  readonly idempotency: IdempotencyContext;
  /** Complete logical input, never an Eve payload reference. */
  readonly payload: T;
}
```

The canonical ingress value is:

```ts
interface AcceptedChannelEvent {
  readonly tenantId: string;
  readonly applicationId: string;
  readonly channelId: string;
  readonly installationId: string;
  readonly event: ChannelEvent;
}

type IngressDelivery = IdempotentEnvelope<AcceptedChannelEvent>;
```

Every ingress producer MUST provide or allow Eve to derive a stable source identity. For provider-originated events, the preferred identity is the provider's stable event ID scoped by tenant, application, channel, and installation. If a provider does not supply one, the channel adapter MUST mint the key before its first delivery and preserve it across retries. Hashing payload content alone is not sufficient because two distinct events may have identical content.

### Channel canonicalization contract

Input-hash correctness begins at the channel adapter. Each adapter MUST define and version a deterministic normalization contract that identifies:

- Provider fields that form stable event identity.
- Fields included in the canonical `ChannelEvent`.
- Delivery-attempt metadata, receipt handles, retry counters, gateway timestamps, signatures, and other volatile fields that are excluded before hashing.
- Provider timestamps or revisions that are semantically part of the event and therefore remain included.
- JSON normalization rules, schema version, and canonical hash version.

The input hash is computed from the validated canonical `ChannelEvent`, not from raw transport bytes. Reordered object fields or changed transport metadata therefore do not create a conflict. If the same provider event ID normalizes to meaningfully different event content, fail-closed conflict handling is intentional.

Every channel adapter MUST include conformance fixtures showing that representative provider retries normalize to the same event key and input hash. It MUST also test that a genuine semantic change under a reused provider ID produces a conflict.

## Key derivation

Keys SHOULD use domain-separated SHA-256 over a versioned canonical encoding. Examples:

```text
eventKey  = H("eve:event:v1", tenant, application, channel, installation, sourceEventId)

directDispatchKey = H("eve:direct-dispatch:v1", eventKey, acceptanceId, bindingGeneration)

branchKey = H("eve:branch:v1", eventKey, acceptanceId, monitorId, definitionVersion, phase)

batchKey  = H("eve:batch:v1", instanceId, orderedDistinctBranchKeys)

runKey    = H("eve:run:v1", batchKey, "primary")

wakeKey   = H("eve:wake:v1", runKey, routeId)

# Optional downstream conformance profile:
turnKey   = H("eve:turn:v1", bindingGeneration, orderedDistinctIngressKeys)

actionKey = H("eve:action:v1", turnKey, durableActionCallId)
```

`acceptanceId` is the durable ingress-receipt generation. Matching retries
reuse it. If the source receipt horizon has ended and the provider identity is
accepted as new work, ingress mints a new generation so descendant keys cannot
collide with still-retained receipts from an earlier acceptance. It is not a
transport attempt or payload reference.

Canonical encoding MUST be unambiguous and versioned. Concatenating strings with a delimiter is insufficient unless escaping and type boundaries are formally defined.

The `inputHash` includes the full canonical payload plus immutable fields that affect interpretation. The child key identifies the logical operation; the hash detects a caller attempting to reuse that identity for different work.

When one batch or turn combines several parents, the envelope retains the frozen ordered distinct parent keys and the distinct root event keys. Duplicate delivery of an existing parent MUST NOT change the set or create a new batch/turn identity.

## Membership freeze protocol

Before a membership freeze, a collection has no externally usable operation identity. The component may append members, deduplicate them, split provisional groups, or consolidate provisional groups according to its lifecycle rules.

The freeze is one serialized durable transition:

```ts
interface FrozenMembership {
  readonly operationKey: string;
  readonly members: readonly {
    key: string;
    inputHash: string;
  }[];
  readonly frozenAt: string;
}
```

The transition MUST atomically:

1. Select the complete ordered distinct membership.
2. Persist each member key and input hash.
3. Derive and persist the operation key.
4. Mark the collection frozen/claimed so no later append can mutate it.

Every concurrent append has exactly one outcome: it commits before the freeze and is included, or commits after the freeze and enters the next provisional collection. Recovery after a lost response reads and returns the same frozen record.

Ordering is domain-specific but durable:

- Fan-out uses a canonical ordering of `(monitorId, definitionVersion, phase, branchKey)`.
- A monitor batch uses mailbox acceptance order with `branchKey` as a deterministic tie-breaker.
- A coalescing session integration uses its durable inbox order with the ingress key as a deterministic tie-breaker.

Applications of this protocol are:

- Fan-out freezes the deployment revision and complete branch manifest before dispatching the first branch.
- A monitor batch freezes when it is claimed for evaluation. Unclaimed sealed batches may still be consolidated and therefore have no `batchKey` or downstream receipt.
- A coalescing session integration freezes a turn immediately before durable turn execution begins.
- A final-action integration freezes an action when its complete durable command and durable action-call identity are checkpointed.

For an integration that deliberately coalesces a turn:

```text
W1 accepted                    provisional [W1]
W2 accepted                    provisional [W1, W2]
freeze                         T1 = H(bindingGeneration, [W1, W2])
duplicate W1 after freeze      return existing membership; no change
W3 after freeze                next provisional turn [W3]
```

`T1` is never recomputed to include `W3`. A process failure after the freeze resumes `T1` with exactly `[W1, W2]`.

## Idempotency ledger protocol

Each stateful component implements the equivalent of:

```ts
type BeginResult<R> =
  | { readonly status: "new" }
  | { readonly status: "retry"; readonly previousReceipt: R }
  | { readonly status: "in_progress"; readonly retryAt?: string }
  | { readonly status: "completed"; readonly receipt: R }
  | { readonly status: "failed"; readonly receipt: R }
  | { readonly status: "conflict"; readonly existingInputHash: string };

interface IdempotencyLedger<R> {
  begin(input: {
    namespace: string;
    key: string;
    inputHash: string;
    leaseUntil?: string;
  }): Promise<BeginResult<R>>;

  complete(input: {
    namespace: string;
    key: string;
    inputHash: string;
    receipt: R;
  }): Promise<void>;

  fail(input: {
    namespace: string;
    key: string;
    inputHash: string;
    errorClass: string;
    retryable: boolean;
  }): Promise<void>;
}
```

`begin`, the component's durable state transition, and creation of its durable result SHOULD be one transaction whenever the backend supports it. Lease expiry may allow a new worker to resume an incomplete operation, but it never creates a new logical key. A matching retryable failure may be atomically reserved again and returns `retry` with the prior failure receipt. A matching non-retryable failure returns `failed` with its terminal receipt and MUST NOT reacquire the operation.

Receipt storage is component-local:

- PostgreSQL implementations may use unique rows and transactions.
- Kafka implementations may use transactional output plus a compacted local receipt/state topic.
- SQS consumers may use a colocated durable database or target mailbox ledger.
- Celld may store append and batch receipts in cell state.
- Durable sessions may use their workflow journal or inbox.
- Action adapters may use a durable outbox and provider receipt table.

The interface does not mandate one shared idempotency database.

## End-to-end flow

### 1. Ingress

The channel adapter authenticates and normalizes the provider input into a complete canonical `ChannelEvent`. It assigns the stable `eventKey` and computes the input hash.

The ingress boundary reserves `(eventKey, inputHash)`. A duplicate with the same hash resumes or returns the prior result. A conflicting hash fails closed.

The adapter does not put the event in a central Eve event table. It hands the complete envelope to the deployment's selected durable ingress boundary:

- In PostgreSQL store mode, one transaction may freeze fan-out and write full-payload branch queue rows directly.
- In Kafka mode, the edge publishes the complete canonical event and waits for the broker's durable receipt.
- In SQS mode, the edge sends the complete canonical event and waits for queue acceptance.

The external provider is acknowledged after that first durable custody transfer, not after all later mailbox or cell operations. A consumer of the durable ingress record then applies the separate fan-out acknowledgement rule below.

### 2. Deterministic fan-out

Fan-out validates the envelope, pins the active deployment revision, and applies phase and source matching. It derives one `branchKey` per matched `(event, monitor, definition version, phase)`. Each complete branch then independently applies loop prevention, deterministic filtering, correlation, and budgets before mailbox acceptance.

Before dispatching the first branch, fan-out durably freezes the routing plan in the event receipt. For ordinary events, the plan contains the complete branch manifest. For direct chat dispatch, it contains unconditional `observed` branches, the stable direct-dispatch operation, and the predetermined `undispatched` branch candidates together with their activation condition:

```ts
interface FanoutReceipt {
  readonly eventKey: string;
  readonly inputHash: string;
  readonly deploymentRevision: string;
  readonly directDispatchKey?: string;
  readonly branches: readonly {
    branchKey: string;
    monitorId: string;
    definitionVersion: string;
    phase: MonitorPhase;
    condition: "always" | "direct-undispatched";
    status: "pending" | "accepted" | "terminal";
    appendReceipt?: string;
  }[];
}
```

This receipt is routing/idempotency metadata, not event storage. The routing plan, deployment revision, and transition to dispatching are one membership freeze. A durable direct-dispatch outcome activates or terminates the already frozen conditional candidates; it does not compute new branch identities. Freezing prevents a retry after a deployment change from producing a different plan.

Each branch append carries the full canonical event:

```ts
interface MonitorAppendPayload {
  readonly tenantId: string;
  readonly applicationId: string;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly phase: MonitorPhase;
  readonly correlationKey: string;
  readonly acceptedAt: string;
  readonly orderingKey: string;
  readonly event: ChannelEvent;
}

type MonitorAppend = IdempotentEnvelope<MonitorAppendPayload>;
```

The envelope key is `branchKey`, its parent is `eventKey`, and its root event set is `[eventKey]`.

The fan-out worker acknowledges or commits its durable ingress delivery only after every active frozen branch is terminal or has returned a durable append receipt, every inactive conditional branch has a terminal non-activation receipt, and any required direct-dispatch operation has a durable outcome.

An implementation may make fan-out atomic with transport acknowledgement. For example, Kafka may transactionally consume one canonical event, produce full-payload branch records, record routing metadata, and commit the source offset. The semantic contract is the same when implemented as retries over independent mailbox calls.

### 3. Mailbox append and batching

The monitor mailbox checks `branchKey` and `inputHash` before modifying lifecycle state.

- First append inserts the complete event and advances the lifecycle machine.
- Duplicate append returns the original append receipt.
- Duplicate append MUST NOT add bytes, change event counts, reset quiet-period timers, move `maxWait`, or increment evaluation generation.
- Conflicting append fails closed.

The mailbox owns the complete events in its open and sealed batches. It MUST NOT store `BufferedEventRef` values or rely on an evaluator to resolve payloads elsewhere.

A lifecycle-sealed batch remains provisional because cooldown or other lifecycle rules may consolidate unclaimed work. The mailbox assigns no `batchKey` and exposes no downstream receipt for provisional batches.

When the evaluator claims work, the mailbox performs a membership freeze. It atomically selects the complete ordered distinct branch membership, derives and persists `batchKey`, records the full claimed payload, and marks that membership unavailable for later consolidation. Any concurrent append belongs either to that frozen batch or the next provisional batch. The claimed batch is self-contained:

```ts
interface MonitorBatchPayload {
  readonly instanceId: string;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly closedBy: MonitorBatchClosedBy;
  readonly events: readonly {
    branchKey: string;
    eventKey: string;
    inputHash: string;
    event: ChannelEvent;
  }[];
}

type MonitorBatchDelivery = IdempotentEnvelope<MonitorBatchPayload>;
```

Unclaimed provisional batches consumed by consolidation simply cease to exist; because they were never membership-frozen, they never had durable downstream identities to revoke. The mailbox retains a frozen claimed batch until the evaluator/run boundary durably accepts that full batch and returns a receipt. It may then delete the batch payload while retaining append and batch receipts for their configured idempotency horizons.

### 4. Evaluation and runs

The evaluator reserves `runKey` before spending budget or invoking a model. Its durable run state contains the complete batch or a complete checkpointed successor state. It does not fetch source events by reference.

Budget reservations, classifier/model calls, policy decisions, evidence construction, and routes each use stable keys derived from `runKey` and their durable step identity. Retries return recorded results rather than spending or deciding twice.

If the decision is `ignore`, the run records a terminal receipt and no session wake occurs. If the decision is `wake`, the evaluator creates a complete session-delivery payload with `wakeKey`, `runKey`, and the full cause lineage.

### 5. Eve session admission and optional turn lineage

The only planned upstream Eve change required by this RFC is
[`vercel/eve#1842`](https://github.com/vercel/eve/issues/1842). An Eve delivery
adapter supplies `wakeKey` (or `directDispatchKey` for direct chat dispatch) as
the channel `send()` idempotency key and sends the complete delivery payload.
Eve durably admits that key with the turn or returns the previously recorded
disposition. A lost response therefore causes the adapter to retry the same
admission rather than create a second turn.

That issue intentionally does not define ordering or coalescing between
distinct delivery keys, and Eve Ambient does not require either behavior. An
integration may submit each distinct wake independently or provide its own
durable serialization policy.

If an integration deliberately coalesces several durable inputs into one turn,
it records their ordered distinct keys and derives one stable `turnKey`.
Re-delivery of one member does not alter the turn or start a second one. Turn
membership freezes atomically immediately before durable turn execution; wakes
accepted after that transition enter the next provisional turn.

An integration that continues lineage into final actions also checkpoints model
outputs and action calls before execution. A retry MUST NOT rerun an
uncheckpointed model step and use new output to mint unrelated action keys after
an earlier output may already have acted.

### 6. Final actions

This section is a conditional full-stack conformance profile. It is not an Eve
Ambient feature or a requirement for additional upstream Eve work.

Each durable action command contains:

```ts
interface DurableActionCommand<T> {
  readonly idempotency: {
    readonly actionKey: string;
    readonly turnKey: string;
    readonly wakeKeys: readonly string[];
    readonly runKeys: readonly string[];
    readonly eventKeys: readonly string[];
    readonly inputHash: string;
  };
  readonly command: T;
}
```

The action adapter reserves `actionKey`, validates `inputHash`, performs the complete command, and records the external receipt. Retrying the same action returns that receipt.

For an external effect, at least one of the following MUST be true:

1. The destination natively accepts `actionKey` as an idempotency key.
2. The destination can be queried or reconciled unambiguously by `actionKey`.
3. Eve writes transactionally into a destination inbox/outbox that enforces `actionKey` before producing the effect.

A local receipt written before the call can prevent concurrent calls but cannot resolve a crash after a non-idempotent remote effect and before recording its result. If the destination provides no idempotency or reconciliation mechanism, Eve cannot promise effectively-once final action. Such an adapter MUST be rejected for active durable actions or explicitly expose weaker semantics.

## Implementation ownership and guarantee boundary

This RFC defines one end-to-end protocol, but `eve-ambient` does not own every implementation boundary.

Eve Ambient owns:

- Channel canonicalization requirements.
- `eventKey`, frozen fan-out manifests, and `branchKey`.
- Full-payload mailbox appends.
- Batch membership freeze, `batchKey`, and `runKey`.
- Complete self-contained monitor delivery with `wakeKey` and root lineage.

The only planned upstream Eve dependency is
[`vercel/eve#1842`](https://github.com/vercel/eve/issues/1842). It owns the
minimum session-admission contract needed by Eve Ambient:

- Accept `wakeKey` or `directDispatchKey` as the channel-delivery idempotency
  key.
- Atomically deduplicate admission and return the prior durable disposition for
  a repeated key.
- Make no ordering or coalescing promise between distinct delivery keys.

An integration that advertises the stronger turn or final-action conformance
profile owns:

- Optional session-turn membership freeze and `turnKey` derivation when it
  coalesces inputs.
- Durable model-result and action-call checkpoints.
- Propagation of cause lineage into action commands.

Action adapters and destinations own:

- `actionKey` conflict checks and receipts.
- Provider idempotency-key propagation.
- Reconciliation after unknown remote outcomes.

Eve Ambient guarantees stable identity and complete-payload custody through the
`wakeKey` delivery request. With `vercel/eve#1842`, its Eve adapter can make
session admission idempotent as well. The stronger effectively-once
final-action guarantee exists only when an integration and the selected action
adapter implement the conditional downstream profile. No companion Eve-core
RFC or additional upstream Eve issue is required to complete Eve Ambient.

## Direct chat dispatch

Chat direct dispatch is another idempotent branch, not a mutation of the source event.

```text
directDispatchKey = H("eve:direct-dispatch:v1", eventKey, acceptanceId, bindingGeneration)
```

The direct-dispatch component receives the complete event and records one durable outcome:

- `dispatched`, with the durable session receipt;
- `undispatched`, allowing creation of `undispatched` monitor branches;
- `failed_retryable`, retaining the same key for retry; or
- `failed_terminal`.

The Eve direct-dispatch adapter passes `directDispatchKey` as the channel
delivery idempotency key defined by `vercel/eve#1842`.

`observed` branches and full-payload `undispatched` branch candidates commit with the ingress receipt. Candidate rows remain conditional and unavailable to workers until a durable `undispatched` outcome activates them. A `dispatched` or terminal failure outcome deletes them. A timeout or unknown result MUST NOT be guessed as undispatched.

The fan-out/event receipt pins both the direct-dispatch operation and resulting branch identities so a retry cannot create a different outcome path.

## Ordering

Idempotency does not establish ordering. Each mailbox requires one stable ordering domain per correlation instance.

- Store mode may assign a monotonic sequence when the branch append is accepted.
- Kafka fan-out should key full-payload branch records by the exact correlation-instance identity so all appends for that instance share one partition order.
- SQS deployments that require ordered correlation should use an appropriate FIFO message-group identity.
- Celld naturally serializes appends for one cell.

Transport sequence is metadata for mailbox ordering. It is not an idempotency key and MUST NOT make a retry look like new work.

## Backend realizations

### PostgreSQL store mode

- Do not require a canonical `events` payload table.
- Store the full canonical event in each accepted monitor branch/mailbox record.
- Use a small ingress/fan-out receipt keyed by `eventKey` to pin the revision and branch manifest.
- Use `branchKey` as the unique mailbox append identity.
- Move full events into open/sealed batches transactionally.
- Delete terminal branch/batch payloads according to local cleanup policy while retaining receipt tombstones.

This intentionally duplicates an event when it matches multiple monitors. Independence and failure isolation are preferred over shared-payload normalization.

### Kafka

- Kafka records carry the complete canonical event or complete branch append payload.
- An HTTP edge may acknowledge the provider after Kafka durably accepts the complete canonical event; it does not wait for celld fan-out.
- A fan-out topology may transactionally consume a source record and produce one complete record per branch.
- Consumers commit offsets only after the next boundary durably accepts the complete payload.
- Topic retention is a Kafka deployment concern. Eve neither depends on it for later payload lookup nor assigns it any historical-processing semantics.
- Kafka offsets and producer sequence numbers never become Eve idempotency keys.

### SQS (deferred)

No SQS adapter is planned in the current implementation sequence. If one is
added later, it must satisfy the same protocol:

- Each SQS message carries a complete self-contained Eve envelope.
- A consumer deletes the message only after the next durable boundary accepts the complete payload.
- Receipt handles are transport acknowledgements, not logical identity.
- Standard-queue duplicates and deliveries outside FIFO's deduplication interval are handled by Eve receipts.
- SQS retention and redrive configuration remain operational infrastructure with no Eve semantic contract.

### Celld

- `CelldAppendRequest` carries the complete canonical `ChannelEvent` plus `branchKey`, `eventKey`, and `inputHash`.
- Cell state stores complete events in open and sealed batches.
- The evaluation callback sends the complete claimed batch.
- Append receipts remain outside the pure XState lifecycle transition and are keyed by `branchKey` plus `inputHash`.
- The cell rejects key/hash conflicts and returns the previous append outcome for matching duplicates.
- Fleet payload and cell-state limits are validated before deployment. Limits cover both individual envelopes and total cell-resident payload bytes across open, sealed, and claimed work. Oversized events fail explicitly; there is no reference-only fallback.

### Other backends

A backend is conformant if it can:

- Accept a complete envelope.
- Durably deduplicate by key and detect hash conflicts.
- Return a stable receipt after durable custody.
- Redeliver safely after failures.
- Supply a stable ordering domain where the selected monitor semantics require one.
- Enforce declared payload-size limits without replacing payloads with Eve references.

Random reads of historical events and central retention are not required for conformance.

## Capacity and backpressure

Full-payload custody increases bytes written to mailbox backends, especially celld. This RFC does not assume that ref-based durability measurements prove full-payload capacity.

For debounce mode, `buffer.maxBytes` is the maximum total event bytes in one batch, not a per-event allowance multiplied by `maxEvents`. A configuration with `maxEvents: 20` and `maxBytes: 64_000` therefore produces a claimed event payload of approximately 64 KB plus envelope overhead, not 1.28 MB.

That per-batch bound is insufficient by itself. Multiple sealed batches may queue while evaluation is unavailable, and immediate-mode cooldown consolidation may accumulate several events. Every mailbox backend MUST additionally enforce a maximum total resident payload byte count per correlation instance, covering:

- The open provisional batch.
- All sealed provisional batches.
- Any frozen claimed batch awaiting acknowledgement.
- Receipt and envelope overhead included by the backend's size accounting.

Crossing the resident limit MUST produce configured backpressure or a terminal overflow outcome before the upstream delivery is acknowledged. It MUST NOT silently drop work or substitute payload references.

Before enabling full-payload celld in production, the spike must be rerun with representative canonical-event distributions and evaluator-outage backlogs. It must measure:

- LTX and object-store bytes per append at p50, p95, and maximum event sizes.
- Operations per append and per membership freeze.
- Maximum cell-state bytes under the configured backlog limit.
- Evaluation callback bytes at the configured batch maximum.
- Recovery latency and duplicate receipts during evaluator outages.

In PostgreSQL store mode, the common one-monitor-per-event case primarily moves the payload from a shared event row into a branch/mailbox row plus small receipts. Multi-monitor fan-out intentionally duplicates the payload once per matched branch.

## Retention and deletion

The platform defines idempotency horizons, not event retention.

Each component may use a different receipt horizon:

| Receipt | Minimum purpose |
| --- | --- |
| Event/fan-out | Cover the maximum supported automatic source-redelivery window |
| Branch append | Cover transport and mailbox recovery |
| Batch/run | Cover evaluator and durable-run recovery |
| Wake/turn | Cover session redelivery and workflow recovery |
| Final action | Cover the business duplicate-prevention horizon |

After a payload is no longer locally needed, a compact receipt may retain only:

```text
namespace, key, inputHash, status, outcome/receipt, createdAt, completedAt, expiresAt
```

There is no platform-wide "event done" transaction. Completion advances hop by hop. Once component B has durably accepted a complete successor payload, component A can consider its custody complete independently of component C.

When a receipt expires, Eve no longer promises to recognize a later delivery as a duplicate at that boundary. Horizons must therefore be explicit and long enough for the supported failure/redelivery model. Final-action receipt policy is the most important and may be longer than all internal receipts.

## Failure semantics

| Failure | Required behavior |
| --- | --- |
| Provider retries after lost acknowledgement | Same `eventKey`; fan-out resumes or returns its receipt |
| Same event key arrives with changed payload | Hash conflict; fail closed and alert |
| Worker crashes during partial fan-out | Retry immutable branch manifest; completed branches return receipts |
| Deployment changes during retry | Original pinned revision and branch identities remain in force |
| Mailbox append commits but response is lost | Retry same `branchKey`; return original append receipt |
| Duplicate append arrives later | No buffer, byte, count, or timer mutation |
| Upstream payload is deleted after mailbox receipt | No effect; mailbox owns the complete event |
| Batch callback commits but response is lost | Retry same full batch and `runKey`; return run receipt |
| Evaluator crashes after budget/model step | Resume checkpointed step key; do not spend or decide again |
| Session accepts wake but response is lost | Retry same `wakeKey`; return session receipt |
| Model/tool execution is retried | Reuse checkpointed action call and `actionKey` |
| Remote action succeeds but response is lost | Reconcile or retry using destination idempotency key |
| Remote target lacks idempotency and reconciliation | Effectively-once guarantee is impossible; adapter rejected or weakened explicitly |
| Backend deletes an old transport record | No effect on downstream work already durably accepted by value |
| Payload exceeds a backend limit | Reject explicitly before acknowledgement; never substitute a reference |

## Observability and diagnostics

Logs, metrics, traces, and dead-letter metadata SHOULD carry idempotency keys and hashes, not full event payloads by default.

Recommended dimensions include:

- Namespace and operation key.
- Root event keys.
- Duplicate, conflict, new, resumed, and completed counts.
- Fan-out branch count and incomplete branch count.
- Receipt age and expiry.
- Unknown external-action outcomes.
- Payload byte size without payload content.

A backend may offer its own payload-bearing dead-letter queue. That remains a deployment choice outside the Eve system interface. Core Eve dead-letter records need only key, hash, stage, error class, and timestamps.

Eve will not add a platform-level payload-capture flag for conflict diagnostics. A deployment may independently configure backend- or adapter-specific diagnostic capture, subject to that backend's security and retention controls.

## Security and privacy

- Payloads are copied only to components that have actual work to perform.
- There is no central historical corpus of events.
- Backend-specific encryption, access control, and deletion apply to that component's local custody.
- Hashes MUST use a canonical representation and SHOULD use a keyed construction where low-entropy sensitive inputs could otherwise be guessed.
- Conflict diagnostics MUST NOT log the conflicting payload by default.
- Definition revision and authorization context are pinned before side effects.

## Public capability model

Eve should validate concrete operational requirements rather than advertise replay-oriented booleans.

Useful backend capabilities are:

- Maximum accepted envelope bytes.
- Durable idempotency and key/hash conflict detection.
- Atomic state transition and receipt support.
- Atomic fan-out support, when available.
- Ordering domain and partition/group-key rules.
- Maximum supported automatic redelivery horizon.
- External action idempotency or reconciliation support.

The following are removed from the core capability model:

- `replayable`
- Event retention promises
- Random payload reads
- Payload repository identifiers
- Cleanup of shared event references

## Changes to the current codebase

The implementation originally stored accepted payloads in `StoredEvent`, placed `BufferedEventRef` values in celld state, and reloaded run events through `MonitorStore.getEvent(ref)`. The implementation now uses full event envelopes in both mailbox tiers, a payload-free `StoredIngressReceipt`, and complete branch-owned values. The store API and initial PostgreSQL schema contain no event repository or payload-loading contract. The remaining work in this RFC is optional external transport integration, beginning with Kafka.

Implemented conceptual changes:

1. Replace the accepted-event payload record with an ingress/fan-out idempotency receipt. It stores branch identity and status but no event body after handoff.
2. Make the subscription/branch record carry `eventKey`, `inputHash`, and the complete canonical event.
3. Rename subscription identity to the semantic `branchKey` or `appendKey` and derive it deterministically.
4. Replace `BufferedEventRef` with a full buffered-event envelope.
5. Store full event envelopes in open and sealed monitor batches.
6. Add a stable `batchKey` at the batch membership freeze and derive run identity from it.
7. Change celld append requests and cell state to carry full events.
8. Change evaluator callbacks to carry complete batches.
9. Remove `#loadRunEvents`, evaluator calls to `MonitorStore.getEvent`, and payload-expiry failures.
10. Propagate lineage through the complete monitor delivery and pass `wakeKey` or `directDispatchKey` to Eve as the channel delivery idempotency key.
11. Add local idempotency receipts and hash-conflict handling at every durable boundary.
12. Remove event repository, pointer, payload-loading, replay, and central-retention contracts from issue #3 and its implementation.

The pure XState lifecycle can remain structurally unchanged. Its event value changes from a reference record to a complete immutable event envelope; idempotency reservations and receipts remain outside the statechart.

### Clean replacement; no migration

There are no installed users or in-flight production states that require compatibility. Implementation is a clean replacement:

- No drain-before-deploy procedure.
- No dual writes.
- No backfill of old event or reference rows.
- No compatibility shim for `BufferedEventRef` cell state.
- No versioned migration namespace for old cells.
- No preservation of the existing replay API.

Obsolete types, methods, schema objects, tests, and documentation are removed directly. New tests and deployments begin with full-payload state.

## Implementation plan

### Phase 1: Identity foundation

- Define the per-channel normalization contract, canonical encoding, and domain-separated key derivation.
- Add `IdempotencyContext`, input hashes, and conflict errors.
- Define component-local receipt behavior and horizons.
- Implement the generic membership-freeze primitive and its concurrency invariants.
- Add conformance helpers for duplicate, concurrent, lost-response, and conflict cases.

### Phase 2: Store mailbox by value

- Copy full events into deterministic branch rows.
- Replace buffered references with full event envelopes.
- Make batches and runs self-contained and assign `batchKey` only at membership freeze/claim.
- Remove evaluator payload lookup.
- Remove the replay API and reduce terminal batches to lineage/completeness metadata.
- Preserve existing filtering, correlation, ordering, batching, cooldown, and deployment pinning behavior.

### Phase 3: Celld by value

- **Implemented.**
- Change the append wire contract to full payloads.
- Store full envelopes in cell state and evaluation callbacks.
- Harden append receipts with key/hash conflict detection.
- Enforce individual payload, batch, and total resident cell-size limits with explicit backpressure.
- Rerun the celld capacity spike using full payloads and evaluator-outage backlogs.

The repository regression suite now exercises full-payload evaluator-outage
backlogs and all three limit outcomes. LTX/object-store bytes, operation counts,
and throughput remain target-fleet production gates because the in-process
worker harness cannot measure them.

### Phase 4: Central ingress cleanup

- **Implemented.**
- Replace `StoredEvent` with a payload-free ingress/fan-out receipt.
- Atomically write the receipt and complete branch-owned payloads.
- Freeze direct-dispatch identity and conditional `undispatched` branches before invoking handlers.
- Require direct handlers to receive the complete event, stable `directDispatchKey`, and input hash.
- Remove central event payload APIs, storage columns, retention settings, ref-resolution tests, and documentation.

### Phase 5: External transports

- Implement Kafka full-payload consume/fan-out/commit semantics.
- Validate the adapter against the same idempotency and failure-injection suite.
- Retain backend-native retention documentation only as operational guidance.
- SQS support is explicitly deferred and is not required to complete this RFC.

### Eve integration boundary

The only planned upstream Eve change is
[`vercel/eve#1842`](https://github.com/vercel/eve/issues/1842):

- Pass `wakeKey` or `directDispatchKey` to channel `send()` as its idempotency
  key.
- Durably deduplicate session admission and return a stable disposition for a
  retry.
- Do not depend on ordering or coalescing between distinct delivery keys.
- Do not require another upstream Eve issue or companion RFC to complete this
  Eve Ambient design.

An integration may additionally implement turn/action lineage and claim the
conditional full-stack conformance profile. That extension does not change the
Eve Ambient completion boundary.

## Conformance tests

Every backend and boundary must pass, where applicable:

1. Representative provider retries with volatile transport differences normalize to the same event key and input hash.
2. A genuine semantic change under the same provider event identity produces a conflict.
3. The same key and payload delivered repeatedly produces one state transition and one stable receipt.
4. The same key with a different payload produces a conflict and no downstream work.
5. Two distinct keys with identical payloads remain two distinct events.
6. A crash after state commit but before response is recovered by the same key.
7. Concurrent matching deliveries cannot create concurrent side effects.
8. Partial fan-out resumes the originally frozen branch manifest.
9. A deployment change during retry does not change the branch set.
10. A provider webhook can be acknowledged after the selected first durable ingress backend accepts the full event; it does not wait for celld fan-out.
11. The durable-ingress consumer does not acknowledge or commit until all frozen branches are terminal or accepted.
12. Duplicate mailbox append does not change batch timing, counts, or bytes.
13. Concurrent append and membership freeze places the append in exactly the current or next batch.
14. Immediate-mode cooldown consolidation gives no identity to provisional batches and derives one new `batchKey` from the frozen union at claim.
15. A mailbox can complete evaluation after the source transport deletes its copy.
16. Evaluation uses only its delivered full batch and performs no event lookup.
17. A lost evaluator response does not create a second run or spend budget twice.
18. A coalescing integration's turn freeze retains exactly the accepted pre-freeze wakes; later wakes enter the next turn and duplicates change neither turn.
19. A lost keyed Eve session-admission response does not create a second admitted delivery.
20. An integration claiming final-action conformance reuses the checkpointed action identity when a model/tool stage is retried.
21. An integration claiming final-action conformance produces one external effect and one stable receipt for the same action key.
22. An integration claiming final-action conformance reconciles an unknown result without minting a replacement key.
23. Oversized payloads and resident-byte overflow fail or backpressure before acknowledgement and never fall back to references.
24. Backend-native payload deletion does not invalidate downstream accepted work.
25. Receipt expiry behaves according to the declared idempotency horizon.

A deployment claiming the conditional final-action guarantee must cover a lost
response at every arrow in:

```text
ingress -> fan-out -> mailbox -> evaluator -> session -> action adapter -> destination
```

Tests 1-17 and 23-25 are owned by Eve Ambient and its backends. Test 19 is also
required of the Eve delivery adapter once `vercel/eve#1842` is available. Tests
18 and 20-22 apply only to integrations advertising coalescing or final-action
conformance. For such a full-stack deployment, the primary acceptance scenario
is:

> Deliver one provider event multiple times through different transport attempts and failure/retry paths. Eve may repeat internal computation, but every matched monitor append, durable wake, and final action has stable identity, and the destination observes one durable effect for each logical action.

## Trade-offs

### Accepted: payload duplication

An event matching multiple monitors is copied into multiple mailbox records. This costs storage and bandwidth while work is active. It buys independent custody, simple deletion, predictable failure recovery, and removal of the shared payload service.

### Accepted: backend payload limits become real limits

Because there is no reference-only escape hatch, each backend must declare and enforce a maximum envelope size. Deployments must choose backends that fit their event sizes.

### Accepted: no Eve replay

Once local components delete payloads, Eve may retain only receipts and workflow outcomes. Eve provides no replay, historical reprocessing, or tuning-loop replacement in this RFC. Backend retention may exist, but Eve does not expose or assign semantics to it. Shadow evaluation applies to newly arriving traffic.

### Accepted: receipts remain durable after payload deletion

Correctness requires remembering that work occurred even when the sensitive or bulky input is gone. Receipt retention is much smaller and independently configurable.

### Accepted: final guarantees depend on the destination

No amount of upstream deduplication can make an opaque, non-idempotent remote side effect safe across an unknown outcome. The action adapter boundary must enforce this limitation.

## Decision

Eve Ambient will use full-payload, idempotent handoffs.

- No Eve component hands another Eve component an event reference in place of an event.
- No central event repository is required or exposed.
- Replay and historical reprocessing are not requirements and no replay contract is provided.
- Every stateful or side-effecting boundary durably deduplicates a stable key and binds it to an input hash.
- Every Eve Ambient multi-input operation performs one atomic membership freeze before receiving a derived operation key; optional downstream coalescing follows the same rule.
- Every receiver gets complete custody before its sender acknowledges or discards the payload.
- Eve Ambient carries stable lineage through `wakeKey`; `vercel/eve#1842` is the sole planned upstream dependency for keyed Eve session admission.
- Eve Ambient assumes no ordering or coalescing between distinct Eve delivery keys.
- Integrations claiming the conditional final-action guarantee continue lineage through `turnKey` and `actionKey`, which the destination or a reconcilable durable adapter enforces.
- Existing ref-based and replay code is replaced directly; no migration or compatibility path is required.

Backend retention remains backend retention. Eve's correctness comes from self-contained custody and idempotency receipts, not from keeping events forever or being able to fetch them later.
