# Channels and ambient rules

Applications own provider normalization and attention policy. Eve Ambient owns
canonical identity and durable execution after the rules select work.

## Canonical channels

A channel canonicalization contract turns a provider-specific delivery into a
`CanonicalChannelEvent` with stable source identity:

```ts
import { defineChannelCanonicalization } from "@ewhauser/eve-ambient";

export const slackChannel = defineChannelCanonicalization({
  version: 1,
  canonicalize: normalizeSlackDelivery,
});
```

Normalization must be deterministic. The event includes application-relevant
data plus source tenant, provider, stream, delivery identity, actor, origin,
and occurrence time where available. Eve Ambient canonicalizes the result,
derives `eventKey`, and hashes the complete source input.

## Ambient rules

An ambient rule is an immutable, version-addressed definition:

```ts
import { defineAmbientRule } from "@ewhauser/eve-ambient";

export const incidentRule = defineAmbientRule({
  id: "incident-escalation",
  version: "v1",
  mode: "active",
  policy: {
    buffer: {
      mode: "debounce",
      quietPeriodMs: 30_000,
      maxWaitMs: 5 * 60_000,
      maxEvents: 100,
      maxBytes: 512_000,
    },
    cooldownAfterWakeMs: 10 * 60_000,
  },
  matches: event => event.type === "incident.changed",
  correlationKey: event => event.data.incidentId,
  orderKey: event => event.occurredAt ?? event.id,
  async prepare(batch) {
    return shouldEscalate(batch)
      ? {
          kind: "wake",
          routeId: "eve",
          instruction: "Investigate the incident and report the next action.",
          decision: { reason: "severity-increased" },
          evidence: { events: batch.branches.map(branch => branch.event) },
        }
      : { kind: "ignore", decision: { reason: "no-escalation" } };
  },
});
```

`matches`, `correlationKey`, and `orderKey` run before durable admission and
must be deterministic and side-effect free. `prepare` runs later on a frozen,
complete batch. It may be repeated if its response is lost before the backend
records it, so it must not perform the final action.

`mode: "shadow"` executes preparation and records the result without delivery.
Rule IDs and versions are part of durable lineage; change the version whenever
behavior or policy changes.

## Buffer policies

`immediate` freezes each admitted branch without intentional delay.
`debounce` collects one correlation key until quiet period, maximum wait,
event-count, or byte limit closes the batch. Canonical ordering uses the rule's
`orderKey` and lineage keys, not arrival order.

Policies are pinned to a correlation workflow. Reusing the same rule identity
and correlation key with different policy bytes is an idempotency conflict,
not a live mutation.

## Publisher

```ts
const ambient = createAmbientPublisher({
  applicationId: "support-agent",
  engine,
  rules: [incidentRule],
});

const receipt = await ambient.publish(slackChannel, providerDelivery);
```

The publisher canonicalizes the event, evaluates all rules, freezes the
complete fan-out, and calls `engine.accept()`. A successful receipt means every
selected branch has reached durable backend custody. It does not mean a
decision or delivery has already completed.

Conditional direct chat dispatch is deliberately separate. Configure the
publisher's optional `direct` rule with an adapter that owns its own stable
idempotency boundary. Direct dispatch does not become part of the attention
workflow or its storage.
