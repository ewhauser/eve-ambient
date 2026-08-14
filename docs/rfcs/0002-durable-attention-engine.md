# RFC: Durable Attention Engine

- Status: Accepted
- Implementation: Protocol, v2 lineage, fan-out validation, memory reference
  engine, and shared conformance suite implemented; celld, PostgreSQL, direct
  adapter split, and clean replacement pending
- Scope: Replace the public store-and-runtime persistence model with one
  durable attention-engine command and two application callbacks
- Preserves: RFC 0001 full-payload custody, idempotency lineage, membership
  freezes, and no-replay guarantees
- Supersedes if accepted: The current `MonitorStore`, `MonitorRuntime`, public
  persistence records, and shared storage topology
- Related: [RFC 0001](0001-full-payload-idempotent-handoffs.md),
  `ewhauser/eve-ambient` issue #3

## Summary

Eve Ambient should expose an attention protocol, not a storage architecture.

The current runtime asks each backend to implement ingress receipts,
subscriptions, instances, runs, deployment records, definition pins, budgets,
dead letters, retention, transactions, and queries. That split mirrors the
PostgreSQL implementation rather than the durable behavior applications need.
It also makes celld look like an incomplete PostgreSQL adapter and keeps the
application involved in coordinating work that a durable backend can own.

This RFC replaces that model with one correctness-critical command:

```ts
interface AttentionEngine {
  accept(input: AcceptedFanout): Promise<AcceptanceReceipt>;
}
```

`accept()` freezes one source occurrence and its complete ordered branch
manifest. The selected backend then owns branch handoff, per-correlation-key
buffering, timers, retries, preparation, delivery, and payload cleanup. It may
use cells, SQL transactions, queues, or in-memory state to do so. Those choices
are backend implementation details, not system interfaces.

The application supplies two callbacks:

1. `prepare()` performs the bounded rule or model decision and constructs a
   complete delivery value. It does not deliver anything.
2. `deliver()` sends the exact recorded value using a stable `wakeKey` and
   returns an idempotent receipt.

The backend MUST durably record a successful `prepare()` result before invoking
`deliver()`. A lost `prepare()` response may cause duplicate model work, but it
cannot cause an unrecorded decision to escape. Delivery retries use the same
recorded bytes and the same `wakeKey`.

There is no central event store, event lookup, history, or replay. A backend
retains a complete event only while it needs custody to finish or hand off the
work. After terminal completion it removes the event and may retain a bounded,
payload-free receipt.

## Decision

Eve Ambient will provide:

- typed channels and ambient rules;
- canonical event validation and hashing;
- deterministic filtering and correlation;
- a pure fan-out compiler;
- one `AttentionEngine.accept()` durability boundary;
- backend-owned correlation workflows;
- separate `prepare()` and `deliver()` callbacks;
- stable lineage through `wakeKey`;
- conformance tests shared by memory, PostgreSQL, and celld backends; and
- an Eve adapter that continues idempotency into the durable session.

Eve Ambient will not provide:

- a public `MonitorStore` or transaction interface;
- a portable event, subscription, run, deployment, budget, or dead-letter
  schema;
- an event query, event reference, payload lookup, replay, or history API;
- a portable operations database;
- live migration of in-flight monitor definitions; or
- a core-owned conditional direct-dispatch workflow.

This is a clean API replacement. There are no deployed users whose durable
state must be migrated. Implementation may temporarily keep old and new code
side by side to make pull requests reviewable, but no compatibility or state
migration layer will be published.

## Why the current split is too large

The current public persistence contract has seven distinct responsibilities:

1. ingress acceptance and source deduplication;
2. frozen monitor subscriptions;
3. per-correlation-key lifecycle state;
4. claimed evaluation runs and checkpoints;
5. deployment and definition pinning;
6. usage reservations and budgets; and
7. dead-letter, retention, and audit queries.

Only two pieces of durable state are fundamental to attention correctness:

1. the frozen fan-out for an accepted source occurrence; and
2. the state machine for one correlation key.

Everything else is either a step inside one of those workflows, an optional
policy integration, or backend-specific operations data. Publishing all of it
as a store interface makes the abstraction harder to implement and harder to
change without improving the end-to-end guarantee.

The smaller boundary also reflects where transactions actually matter. A
backend must atomically freeze membership and must record a prepared outcome
before delivery. The application does not need access to those transactions.

## Goals

- Preserve end-to-end idempotency identity from source event through final
  durable action.
- Give every durable event-processing boundary a complete payload by value.
- Freeze fan-out and batch membership before deriving child work.
- Let celld run the complete attention workflow without PostgreSQL.
- Let PostgreSQL implement the same behavior without exposing its schema as the
  architecture.
- Make duplicate intermediate computation acceptable while preventing it from
  changing the final delivered value.
- Delete source payloads as soon as local custody is complete.
- Keep backend retention, retry, scheduling, and diagnostics private.
- Make the smallest useful portable contract straightforward to test.

## Non-goals

- Event replay or intentional reprocessing.
- Event history, archival, or a central event repository.
- Portable inspection of every internal workflow record.
- Exactly-once model or rule execution.
- A total order across unrelated correlation keys.
- In-place migration of active monitor state between definition versions.
- A framework-wide quota, billing, or audit system.
- Making an external action effectively once when its adapter does not support
  idempotency or reconciliation.

Automatic retry is not replay. A retry continues the original operation using
the original keys and frozen values. Reprocessing an old event under a new rule
is not a capability this system will expose.

## Public protocol

The following interfaces are illustrative but normative in shape. Exact names
may change during implementation.

```ts
type EventKey = string;
type OccurrenceKey = string;
type BranchKey = string;
type BatchKey = string;
type RunKey = string;
type WakeKey = string;

interface AcceptedFanout {
  readonly applicationId: string;
  readonly tenantId: string;
  /** Stable source identity. */
  readonly eventKey: EventKey;
  /** Derived from eventKey and the canonical source input hash. */
  readonly occurrenceKey: OccurrenceKey;
  /** Hash of the complete canonical source input. */
  readonly inputHash: string;
  /** Canonicalization contract used to compute inputHash. */
  readonly canonicalizationVersion: number;
  /** Complete canonical source event, including for an empty fan-out. */
  readonly event: CanonicalChannelEvent;
  /** Hash of the complete ordered branch manifest, including an empty one. */
  readonly manifestHash: string;
  /** Complete ordered membership proposed by this attempt. */
  readonly branches: readonly FullBranch[];
}

interface FullBranch {
  readonly applicationId: string;
  readonly tenantId: string;
  readonly eventKey: EventKey;
  readonly occurrenceKey: OccurrenceKey;
  readonly branchKey: BranchKey;
  readonly inputHash: string;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly phase?: "observed" | "undispatched";
  readonly correlationKey: string;
  /** Stable source-defined order within a correlation stream. */
  readonly orderKey: string;
  readonly mode: "active" | "shadow";
  /** Complete canonical channel event, never an Eve payload reference. */
  readonly event: CanonicalChannelEvent;
  /** Serializable lifecycle policy needed after the caller disappears. */
  readonly policy: SerializableMailboxPolicy;
}

interface AcceptanceReceipt {
  readonly eventKey: EventKey;
  readonly occurrenceKey: OccurrenceKey;
  readonly inputHash: string;
  readonly manifestHash: string;
  readonly branchKeys: readonly BranchKey[];
  readonly acceptedAt: string;
  readonly dedupeExpiresAt: string;
}

interface FrozenBatch {
  readonly instanceKey: string;
  readonly batchKey: BatchKey;
  readonly runKey: RunKey;
  readonly applicationId: string;
  readonly tenantId: string;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly correlationKey: string;
  readonly openedAt: string;
  readonly frozenAt: string;
  readonly closedBy: string;
  readonly bytes: number;
  /** Complete frozen members in canonical order. */
  readonly branches: readonly FullBranch[];
}

type PreparedOutcome =
  | {
      readonly kind: "ignore";
      readonly decision: JsonValue;
    }
  | {
      readonly kind: "wake";
      readonly decision: JsonValue;
      readonly routeId: string;
      readonly instruction: string;
      /** Complete immutable evidence, not references to source events. */
      readonly evidence: JsonValue;
    };

interface PreparedWake {
  readonly wakeKey: WakeKey;
  readonly runKey: RunKey;
  readonly batchKey: BatchKey;
  readonly instanceKey: string;
  readonly applicationId: string;
  readonly tenantId: string;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly correlationKey: string;
  readonly rootEventKeys: readonly EventKey[];
  readonly routeId: string;
  readonly instruction: string;
  readonly decision: JsonValue;
  readonly evidence: JsonValue;
  /** Hash of this complete delivery input. */
  readonly inputHash: string;
}

interface DeliveryReceipt {
  readonly wakeKey: WakeKey;
  readonly inputHash: string;
  readonly deliveredAt: string;
  readonly result: JsonValue;
}

interface AttentionCallbacks {
  prepare(batch: FrozenBatch): Promise<PreparedOutcome>;
  deliver(wake: PreparedWake): Promise<DeliveryReceipt>;
}
```

`CanonicalChannelEvent`, `SerializableMailboxPolicy`, and `JsonValue` are
values, not storage handles. The canonical event contains every stable semantic
field needed by the workflow and excludes delivery-attempt timestamps, trace
identifiers, receipt handles, and other retry-varying transport metadata. The
top-level event lets the engine verify the canonical source hash even when the
branch manifest is empty. Each non-empty branch also carries that complete
event so its handoff is self-contained. A backend MUST be able to finish after
the `accept()` caller disappears and MUST be able to invoke `prepare()` after
the source transport is gone.

`prepare()` receives all complete source events in the frozen batch. A wake
contains the complete successor value needed by its delivery adapter. The
delivery adapter MUST NOT load event payloads through an Ambient system API.
`rootEventKeys` and other lineage fields identify causes for idempotency; they
are not payload references and no Ambient API resolves them.

The protocol does not require callbacks to be in the same process as the
engine. A PostgreSQL worker may call local functions. A celld workflow may call
an authenticated application endpoint carrying the same values.

The engine MUST recompute and verify the source `eventKey`, source `inputHash`,
`occurrenceKey`, every branch key and input hash, and the ordered manifest hash.
It MUST also verify that application, tenant, and complete canonical event
values agree at the top level and in every branch. A caller cannot make an
invalid lineage valid by supplying internally consistent but unverified key
strings.

## Identity lineage

The lineage becomes:

```text
eventKey
  +-- occurrenceKey = H("eve:occurrence:v1", eventKey, inputHash)
        +-- directDispatchKey
        +-- branchKey per monitor definition and correlation key
              +-- batchKey from frozen ordered branch membership
                    +-- runKey
                          +-- wakeKey from run and route
                                +-- turnKey       downstream profile
                                      +-- actionKey per durable action
```

`occurrenceKey` replaces the random `acceptanceId`. It makes every downstream
key a deterministic function of the canonical accepted input while allowing a
reused provider identifier with different canonical bytes to produce a
different namespace after the configured conflict horizon.

The canonical derivations are domain-separated hashes of:

```text
occurrenceKey     = H("eve:occurrence:v1", eventKey, sourceInputHash)
directDispatchKey = H("eve:direct-dispatch:v2", occurrenceKey, bindingGeneration)
branchKey         = H("eve:branch:v2", occurrenceKey, monitorId,
                      definitionVersion, phase, correlationKey)
manifestHash      = H("eve:fanout:v1", occurrenceKey,
                      ordered [branchKey, branchInputHash] pairs)
instanceKey       = H("eve:instance:v2", applicationId, tenantId, monitorId,
                      definitionVersion, correlationKey)
batchKey          = H("eve:batch:v2", instanceKey, ordered branchKeys)
runKey            = H("eve:run:v2", batchKey, "primary")
wakeKey           = H("eve:wake:v2", runKey, routeId)
```

An empty manifest has a real `manifestHash`; it is not represented by a
missing value.

The input hash MUST include the canonicalization version. A `branchKey` MUST
include the occurrence, immutable monitor definition identity, phase, and
correlation key. A branch input hash MUST bind the complete event, routing
identity, ordering value, and mailbox policy. Transport attempts, timestamps
created by the worker, leases, and process identifiers MUST NOT affect keys.

Within the admission receipt horizon:

- same `eventKey` and same source `inputHash` returns the original receipt;
- same `eventKey` and different source `inputHash` is a fail-closed conflict;
- a retry cannot add, remove, or replace a frozen branch; and
- child key reuse with a different input hash is a fail-closed conflict.

After the declared horizon, a backend may forget the receipt. Reuse then has
only the guarantees offered by the newly created workflow and by downstream
idempotency horizons. Infinite deduplication is not promised.

RFC 0001 remains normative for `turnKey`, `actionKey`, and the conditional
effectively-once final-action profile.

## Fan-out and membership freeze

The caller validates and canonicalizes the event, applies deterministic
filters, computes correlation keys, and submits the complete ordered branch
manifest. The fan-out compiler rejects duplicate branch keys and orders the
manifest by `branchKey`, so monitor declaration order cannot change its hash.
The first accepted call freezes all of the following atomically:

- source `eventKey`, `occurrenceKey`, and `inputHash`;
- canonicalization version;
- ordered branch keys and their input hashes; and
- the fact that the manifest is empty, when no rule matched.

An empty manifest is a real terminal outcome. Recording it prevents a retry
under a later deployment from turning a previously ignored event into work.

A retry may propose a different branch set because code or configuration
changed between attempts. If the source input matches a live receipt, the
engine MUST return the original frozen receipt and continue only its original
branches. It SHOULD emit a membership-mismatch diagnostic. It MUST NOT append
new branches and MUST NOT replace the original manifest.

There is one exception to treating a changed proposal as a diagnostic: if a
retry reuses an original `branchKey` with a different branch `inputHash`, it is
an idempotency conflict. Definitions are immutable, so that overlap means a key
has been bound to two logical inputs. The engine MUST fail closed rather than
silently classify it as ordinary deployment membership drift.

`accept()` returns successfully only after every frozen branch is either
terminal at ingress or durably accepted, with its full value, by its
correlation workflow. A partial handoff remains backend-owned pending work.
Retries resume missing branches; already accepted branches return their stable
receipts. Once all branch handoffs are complete, the event coordinator removes
the full fan-out payload and retains only its bounded receipt.

This coordinator is not a central event repository. It is a backend-local
workflow addressed by source `eventKey`, exposes no event retrieval interface,
and retains full values only while the current occurrence's handoff is
incomplete.

## Correlation workflow

Each immutable tuple of application, tenant, monitor definition, and
correlation key owns one serialized workflow. Its backend performs this state
machine:

1. Validate `branchKey` and `inputHash` and return a prior append receipt for a
   matching duplicate.
2. Append the complete `FullBranch` to the current lifecycle state.
3. Apply immediate, debounce, maximum-wait, and cooldown policy using
   backend-owned timers.
4. Atomically freeze an ordered batch membership and derive `batchKey` and
   `runKey`.
5. Call `prepare()` with the complete `FrozenBatch`.
6. Durably record the first successful `PreparedOutcome` and its hash.
7. For `ignore`, make the batch terminal.
8. For `wake`, derive `wakeKey`, construct the complete `PreparedWake`, and
   call `deliver()`.
9. Record the stable delivery receipt, apply the terminal lifecycle transition,
   and remove source event payloads.

An append concurrent with a freeze MUST serialize entirely before or after the
freeze. An event after the freeze belongs to the next batch. Batch identity is
derived from the canonical ordered branch membership, not from a random claim
identifier.

The backend may retry `prepare()` if it loses the response before recording an
outcome. The callback may therefore perform duplicate rule evaluation or model
requests and may incur duplicate computation cost. It MUST NOT perform the
final delivery or another user-visible durable action. The first outcome the
backend records wins; later processing cannot revise its decision, evidence,
or route.

The backend MUST record a wake outcome before invoking `deliver()`. Every
delivery retry carries the exact recorded wake bytes, `inputHash`, and
`wakeKey`. The delivery destination MUST implement same-key/same-input receipt
semantics. A matching retry returns the original receipt; a different input for
the same `wakeKey` fails closed.

This split is the transactional boundary that protects the final action. We do
not need exactly-once evaluation. We need one immutable prepared value to be
the only value eligible for idempotent delivery.

## Payload lifetime and retention

Full payload storage is workflow custody, not a system feature.

The event coordinator retains its complete frozen branches until every branch
is terminal or durably appended. A correlation workflow retains complete
events until the batch is ignored or its prepared wake is durably delivered.
It may remove superseded lifecycle copies whenever the remaining state is
self-contained.

After a terminal outcome, a backend SHOULD retain only bounded data such as:

- operation key and input hash;
- frozen membership hashes or child keys;
- terminal status and timestamps;
- prepared outcome hash; and
- delivery receipt identity.

Backends may retain more because of WAL, Kafka retention, cell snapshots,
backups, or ordinary database operations. That is an operational property of
the backend. Eve Ambient exposes no API or promise for retrieving those event
bytes.

Every backend MUST document its payload deletion point, receipt horizon, and
behavior after the horizon expires.

Receipt horizons must cover their callers' retry windows. In particular, the
event admission receipt covers the maximum source-redelivery window, branch
receipts cover event-coordinator retry, wake receipts cover correlation-workflow
retry, and the final action ledger covers durable-session retry. A deployment
that configures a shorter downstream horizon no longer claims the RFC 0001
effectively-once guarantee outside that overlap. Payload retention may be much
shorter than receipt retention.

## Ordering

There is no global ingress sequence. Each correlation workflow serializes its
own appends. Frozen batch members use a canonical order:

1. the channel-provided `orderKey`, where the source has meaningful order; then
2. `eventKey`; then
3. `branchKey` as a deterministic tie-breaker.

Channel adapters are responsible for deriving `orderKey` from stable provider
coordinates such as a Slack message timestamp or Kafka partition and offset.
Wall-clock receipt time and worker scheduling MUST NOT determine identity.

The system promises deterministic order within one correlation workflow. It
does not invent a total order across unrelated keys or source partitions.

## Definition lifecycle

The engine will not persist deployments, execute store migrations for monitor
definitions, or mutate active state in place.

A monitor definition is immutable under `(monitorId, definitionVersion)`, where
the version is content-addressed or otherwise guaranteed to identify identical
executable behavior. New behavior creates a new version and therefore a new
correlation-workflow namespace. Removing a definition stops new branches while
already accepted workflows drain under their frozen version.

The application MUST keep old definition implementations callable for at least
the maximum of:

- maximum buffer wait;
- cooldown and retry duration;
- backend outage recovery objective; and
- active payload retention horizon.

The backend sends `monitorId` and `definitionVersion` to `prepare()`. An
unknown version fails retryably until it is restored or reaches a
backend-specific terminal operator policy. There is no framework-owned live
state migration.

## Direct chat dispatch

Direct Eve chat delivery and ambient attention have different semantics and
should not share a conditional subscription table.

The channel/Eve adapter performs this sequence:

1. Canonicalize the provider event and use the direct handler's own admission
   contract to bind `eventKey` to its source input hash and freeze the direct
   plan, including an explicit no-direct outcome or one immutable binding
   generation.
2. Recover `occurrenceKey` and any `directDispatchKey` from that frozen plan.
3. If the plan contains a direct Eve message, call the idempotent direct
   handler with that exact `directDispatchKey` and the complete canonical
   event.
4. Record or recover the direct handler's stable outcome.
5. Compile ambient branches from that outcome, including `observed` or
   `undispatched` phases as appropriate.
6. Call `AttentionEngine.accept()`, including an empty fan-out when no ambient
   branch remains.
7. Acknowledge the provider only after both required durable boundaries have
   returned receipts.

The direct handler's event-key admission is necessary because two different
source hashes produce two different occurrence and direct-dispatch keys. It
MUST detect same-event/different-input reuse before either key can reach a
session, and a retry MUST recover the original frozen binding rather than use a
new deployment binding. This ledger is part of the direct adapter's ordinary
durable receipt, not an Ambient event repository, and retains no event payload
after direct and attention custody are complete.

If the process crashes after direct dispatch but before attention acceptance,
provider redelivery repeats the same `directDispatchKey`, recovers the same
outcome, and calls `accept()` again. A source adapter that cannot arrange
redelivery cannot promise durable completion across that crash window.

The core attention engine therefore has no `StoredDirectDispatch`, conditional
branch rows, direct-dispatch claims, or resolution API. Direct handling is an
idempotent adapter boundary. The engine sees only the final complete frozen
ambient manifest.

## Optional policy and operations integrations

### Budgets

Portable durable budget tables are removed from the core. Applications that
need global or billable reservations may inject a semantic gate into
`prepare()`:

```ts
interface BudgetGate {
  reserve(input: {
    readonly operationKey: RunKey;
    readonly scopes: readonly string[];
    readonly units: number;
  }): Promise<BudgetReceipt>;
}
```

The gate owns its own idempotency and conflict semantics. Per-correlation-key
limits may live directly in the serialized correlation workflow. Deployments
that do not need budgets do not implement a budget database.

### Diagnostics

The engine may emit payload-free observer events for acceptance, conflict,
branch handoff, batch freeze, prepare retry, terminal outcome, and delivery.
Observer failure MUST NOT change the correctness result.

PostgreSQL or celld packages may expose backend-specific inspection and
administration tools. Those tools are not portable `AttentionEngine` methods
and are not required for conformance. A deployment may sink diagnostics into
PostgreSQL, Kafka, an observability platform, or nowhere at all.

There is no core `listRuns()`, `listDeadLetters()`, `purgeExpired()`, central
audit log, or event history. Backend maintenance and retention are backend
operations.

## Backend realizations

### Memory

The memory engine is the executable reference model. It implements both
workflow state machines and a controllable clock, failure injection, and
callback harness. It is intended for conformance and application tests, not
durability.

### celld

The celld backend uses:

- one event-admission/coordinator cell per `eventKey`; and
- one serialized correlation cell per application, tenant, monitor definition,
  and correlation key.

The event cell detects same-event/different-input conflicts, freezes the current
occurrence's fan-out, and retries complete branch appends. When its receipt
horizon expires, it may admit a later occurrence for the same source identity.
The correlation cell owns buffering, alarms, batch freezes, prepared outcomes,
delivery receipts, and retention. Cell class layout and internal keys are
implementation details.

The celld worker holds no model-provider, Eve-session, or application secrets.
It calls authenticated application-owned `prepare` and `deliver` endpoints.
The request bodies carry complete values. Callback authentication, timeout,
body-size limits, and key/hash verification fail closed.

The celld example and worker MUST have no PostgreSQL pool, `pg` dependency,
PostgreSQL migration, or PostgreSQL environment variable. A deployment may
independently export diagnostics to PostgreSQL, but celld correctness cannot
depend on it.

### PostgreSQL

The PostgreSQL backend implements the same engine protocol. SQL tables,
transactions, leases, due-work scans, and worker entry points are private to
the package. They need not reproduce the old seven-facet `MonitorStore` schema.

A PostgreSQL deployment may run callbacks in process. A backend-specific worker
may expose `runOnce(callbacks)` or a long-running equivalent, but generic
`MonitorRuntime.drain()` is not part of the engine contract.

PostgreSQL remains a useful low-operations deployment choice. It is one engine
implementation, not the reference shape every engine must emulate.

## Application shape

The intended application boundary is approximately:

```ts
const ambient = createAmbientPublisher({
  applicationId: "engineering-agent",
  channels,
  monitors,
  engine: createCelldAttentionEngine({ url, secret }),
});

await ambient.publish(events, "pull_request.changed", providerEvent);
```

The application separately registers the compiled definition registry and the
two callbacks with its chosen backend host:

```ts
const callbacks = createAttentionCallbacks({
  definitions: monitors,
  decisions,
  deliveryChannels,
});

app.post("/ambient/prepare", callbacks.handlePrepare);
app.post("/ambient/deliver", callbacks.handleDeliver);
```

A PostgreSQL worker can call the same callback object directly. The public
model does not require a scheduler in small applications or HTTP callbacks in
database-backed applications.

## Conformance requirements

Every durable backend, and every official source adapter for the applicable
admission cases, MUST pass the same failure-oriented oracle:

1. Canonicalizing the same source input produces the same source hash and
   occurrence key.
2. Same event and hash returns the original frozen fan-out even when a retry
   proposes different deployment membership.
3. Same event and different hash conflicts within the admission horizon.
4. An empty fan-out is durably frozen.
5. A crash after some branch appends resumes only missing handoffs.
6. A lost append response returns the original branch receipt without
   duplicating membership.
7. Every event coordinator, branch append, and frozen batch has the complete
   canonical event.
8. Concurrent append and freeze places a branch in exactly the current or next
   batch.
9. A lost `prepare()` response may repeat computation but cannot invoke
   delivery before an outcome is recorded.
10. A recorded prepared outcome is immutable.
11. A lost `deliver()` response retries the exact same wake payload and key and
    produces one stable receipt.
12. Terminal workflows remove source event payloads while retaining the
    declared receipt.
13. Receipt expiry behavior matches the backend's documented horizon.
14. celld conformance runs with no PostgreSQL service, driver, or network
    connection.
15. Memory, celld, and PostgreSQL produce the same semantic results under the
    same clock and injected failures.
16. A source adapter acknowledges only after its required direct and attention
    receipts exist.
17. A crash between direct dispatch and attention acceptance recovers through
    source redelivery without changing keys.
18. Same event and different source input conflicts before any direct session
    delivery occurs.
19. A retry after binding deployment drift recovers the original frozen direct
    plan and cannot dispatch to a newly selected binding.

The RFC 0001 downstream conformance profile remains required for deployments
that claim effectively-once final actions beyond `wakeKey`.

## Security and capacity

Complete values at every handoff require explicit limits. Each backend and
callback endpoint MUST enforce authenticated callers, maximum event size,
maximum branch count, maximum batch size, maximum prepared wake size, and
bounded retry policy. Over-capacity responses MUST be explicit and retry-safe;
an implementation MUST NOT replace an oversized value with a shared payload
reference.

Prepared outcomes separate trusted instructions from untrusted event evidence.
The backend stores and resends their exact serialized values but does not
reinterpret evidence as control input. Definition identity and callback
authentication prevent a cell from selecting arbitrary application code.

## Tradeoffs

This design deliberately accepts several costs:

- Backends are more opinionated internally because they own the complete
  workflow instead of exposing records to a generic runtime.
- A lost `prepare()` response may duplicate model cost. Deployments may add an
  idempotent decision ledger or budget gate when that cost justifies it.
- Operators lose a portable SQL-like view of every run and event. Diagnostics
  become backend-specific or observer-driven.
- Applications must retain old definition code until accepted work drains.
- Per-event coordination still exists to freeze fan-out. It is deliberately
  narrow, ephemeral, and non-queryable rather than a central event store.
- The API break is large. The absence of production users makes a clean break
  preferable to preserving accidental abstractions.

The benefit is that the public system has one durable command, two side-effect
stages, and one end-to-end correctness story.

## Relationship to RFC 0001

RFC 0001 remains the foundation for this proposal. Its requirements for full
payloads, stable child keys, input-hash conflicts, membership freezes,
hop-by-hop custody, local payload lifetime, bounded idempotency horizons, and
the optional final-action profile remain normative.

This RFC supersedes only RFC 0001 implementation language that assumes Ambient
must publish a shared ingress/subscription/run store or a particular PostgreSQL
topology. The event coordinator described here is a backend-internal custody
workflow, not a global event object: it has no lookup API, does not support
replay, and deletes full payloads after branch handoff.

If the two RFCs appear to conflict about a concrete Ambient storage or runtime
API, this RFC controls. If they appear to conflict about idempotency, payload
custody, or downstream action lineage, RFC 0001 controls.

## Implementation plan

This RFC should be implemented in reviewable pull requests:

1. **Protocol and reference model.** Add `occurrenceKey`, the pure fan-out
   compiler, `AttentionEngine`, callback types, a memory engine, and the shared
   conformance suite.
2. **celld engine.** Add event-coordinator and correlation workflows, the
   two-stage authenticated callback protocol, failure tests, and a celld-only
   example with no PostgreSQL dependency.
3. **PostgreSQL engine.** Implement the same protocol with private schema and
   worker APIs and run the shared conformance suite.
4. **Clean replacement.** Remove `MonitorStore`, `MonitorRuntime`, public
   persistence records, old migrations, conditional direct-dispatch state,
   global ingress sequence, built-in budgets, and portable run/dead-letter
   queries. Rewrite documentation and examples around the engine boundary.
5. **Major release.** Publish the clean API with no migration or compatibility
   promise for the unreleased durable state model.

Temporary code coexistence during these pull requests is an implementation
sequence only. The final package exports one architecture.

## Acceptance criteria

This RFC is complete when:

- the core package exports no `MonitorStore`, store transaction, or `Stored*`
  persistence record;
- no system interface can retrieve or replay a stored event;
- memory, PostgreSQL, and celld pass the same conformance suite;
- every active branch and batch is self-contained by value;
- the backend records a prepared result before any delivery attempt;
- exact wake bytes and lineage survive all delivery retries;
- terminal completion deletes source payloads according to documented backend
  policy;
- the celld worker and example contain no `pg` dependency or PostgreSQL setup;
- direct chat dispatch is an adapter-owned idempotent boundary;
- definitions are immutable and version-addressed; and
- the README describes Eve Ambient as a durable attention engine rather than a
  portable persistence framework.
