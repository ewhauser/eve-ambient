# Operations and security

Eve Ambient treats attention state as durable application state. This page
collects the guarantees and responsibilities that matter after the first
monitor is running.

## Durability

- The correlation-instance lifecycle—idle, collecting, evaluating, and
  cooldown—is an explicit XState statechart in `src/instance-machine.ts`. It is
  a pure transition table with no live actor or in-process timer requirement.
- Ingress receipts, active branch handoffs, mailboxes, timer generations,
  actionable runs, evidence snapshots, quotas, dead letters, and deployment
  identity are durable.
- Debounce closes on quiet period, mandatory maximum wait, count, or byte
  threshold. The overflowing event starts the next batch. A single oversized
  monitor event is dead-lettered rather than trimmed.
- Cooldown accumulates per key and schedules an evaluation at expiry even when
  no later event arrives.
- Transient store and target failures retry under stable leases and delivery
  keys. Model-provider failures use the declared fallback. Deterministic
  callback failures dead-letter immediately and cannot block other keys or
  tenants.
- Every branch row and mailbox batch carries a complete event envelope. Branch
  rows are deleted atomically after mailbox acceptance. An actionable run keeps
  its frozen full batch through retries; terminal completion replaces it with
  lineage and completeness metadata so event payloads do not become history.
- The ingress table is a payload-free acceptance receipt: it keeps the source
  key/hash, frozen deployment and branch manifest, and direct-dispatch outcome.
  Complete payloads live only in active branch, mailbox, or actionable-run
  custody and disappear after the next durable handoff or terminal outcome.

## Operator APIs

Use `listRuns()`, `listDeadLetters()`, and `purgeExpired()` to build operator
tooling. Terminal runs retain decisions, receipts, projected evidence, lineage
keys, and batch completeness, but not source event bodies. Replay is not a
runtime capability or retention requirement.

Lifecycle events expose separate classifier tokens or cost estimates and
delivery outcomes. Model prices remain an application or provider concern;
pass `estimatedCost` from a custom invoker when available.

## Definition identity and rollout

Monitor IDs are durable and independent of file paths. `initialize()` rejects a
missing active ID unless the deployment declares `move-state` or
`discard-state`. Keep old compiled versions inactive while they still own runs
or mailbox state.

To move idle mailbox state across a compatible code version, declare it:

```ts
compileMonitor(newDefinition, "v2", { compatibleWith: ["v1"] });
```

Use `mode: "shadow"` to run the full decision, quota, evidence, and route path
without binding or delivery. Test binding, evidence persistence, and
coalescing with new canary input through the normal ingress path before
production activation.

Mailbox ownership is durable deployment state. Switching an existing
application between the Postgres store mailbox and celld requires an explicit
fleet migration and is rejected by `initialize()` today. celld also cannot
apply monitor-ID migrations, destructive removals, or compatible-version state
moves automatically.

## Evidence trust boundary

Event text and classifier output are always untrusted evidence. The runtime
never concatenates them into task instructions, and routes cannot carry prompts
or messages. Trusted task instructions are static developer configuration.

Source actors remain provenance and are not promoted to execution identity.
Delivery executes only as the application principal. Same-application agent and
monitor origins are ignored by default; after an external platform round-trip,
applications must also filter bots and enforce wake limits because
cross-application causation cannot be guaranteed.

All keys, routes, budgets, bindings, and delivery requests are scoped by tenant
and application before monitor or correlation identity. Delivery adapters must
enforce the same boundary rather than trusting a source actor or target alone.

## Delivery guarantees

Delivery is idempotent, not exactly-once. A delivery adapter must deduplicate
the stable `eve:wake:v1:...` key derived from the run and route and return the
same durable receipt for a retry. It must resolve canonical targets through its
own conversation-binding registry and reject a conflict with a non-terminal
binding.

Human and monitor requests belong on the same durable session ingress path.
While a turn is active, a delivery channel may coalesce monitor evidence into
one pending follow-up, but it must never merge or drop human input.

## Secrets and network boundaries

Keep provider, model, session, and tool credentials outside monitor evidence and
outside celld cells. The celld evaluator route uses a shared bearer secret and
must fail closed when it is absent. Firewall celld's internal listener; the
packaged worker authenticates its public cell routes but cannot add
authentication to celld's internal administrative listener.

Use separate principals and network policies for:

- provider ingress and acknowledgement;
- PostgreSQL storage and migrations;
- the celld fleet and evaluator callback, when enabled;
- model invocation; and
- application-principal delivery.

## Deliberate version-one limits

There is no polling source, arbitrary I/O correlation, semantic re-keying,
mutable public instance state, `hold`, window or watermark processing,
source-user delegation, direct subagent invocation, multi-route wake, monitor
interruption, or cross-application loop guarantee. Each would require a
separate durable or authorization design rather than an incidental callback.
