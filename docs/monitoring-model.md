# Channels and ambient rules

Applications own provider normalization and attention policy. Eve Ambient owns
canonical identity and durable execution after the rules select work.

## Canonical channels

A channel canonicalization contract turns a provider-specific delivery into a
`CanonicalChannelEvent` with stable source identity:

```ts
import { defineChannel } from "@ewhauser/eve-ambient";

export const slackChannel = defineChannel({
  version: 1,
  input: slackDeliverySchema,
  map: normalizeSlackDelivery,
  partitionKey: event => event.data.threadTs,
});
```

Normalization and `partitionKey` must be deterministic. The partition names
the smallest bounded domain entity across which rules may correlate, such as a
Slack thread or GitHub pull request. The event includes application-relevant
data plus source tenant, provider, stream, delivery identity, actor, origin,
and occurrence time where available. Eve Ambient canonicalizes the result,
derives `eventKey`, and hashes the complete source input.

## Ambient rules

An ambient rule is an immutable, version-addressed definition bound to one
channel:

```ts
import { debounce, defineAmbientRule, ignore, wake } from "@ewhauser/eve-ambient";

export const incidentRule = defineAmbientRule({
  id: "incident-escalation",
  version: "v1",
  channel: slackChannel,
  policy: debounce({
    quiet: "30s",
    maxWait: "5m",
    cooldown: "10m",
    maxBytes: 512_000,
  }),
  matches: event => event.type === "incident.changed",
  decide({ events, latest }) {
    return shouldEscalate(events)
      ? wake({
          target: latest.replyTarget.address,
          instruction: "Investigate the incident and report the next action.",
          decision: { reason: "severity-increased" },
          evidence: { events },
        })
      : ignore({ reason: "no-escalation" });
  },
});
```

`matches`, the optional `correlationKey`, and the optional `orderKey` run before
durable admission and must be deterministic and side-effect free. `decide` runs later
on a convenient view of the frozen, complete batch. It may be repeated if its
response is lost before the backend records it, so it must not perform the
final action.

Rules default to active delivery. `mode: "shadow"` executes preparation and
records the result without delivery. Rule IDs and versions are part of durable
lineage; change the version whenever behavior or policy changes.

`wake()` may omit `routeId` when the application registers exactly one route.
Applications with multiple delivery routes must select one explicitly.

## Buffer policies

`immediate` freezes each admitted branch without intentional delay.
`debounce` collects one correlation key until quiet period, maximum wait,
event-count, or byte limit closes the batch. Canonical ordering uses the rule's
`orderKey` and lineage keys, not arrival order.

Policies are pinned to a correlation run. Reusing the same rule identity
and correlation key with different policy bytes is an idempotency conflict,
not a live mutation.

By default, each rule has one run per channel partition. Set
`correlationKey` only when a rule needs multiple independent runs inside
that partition. Correlation cannot cross partitions; if that is required,
enlarge the channel partition deliberately.

## Publisher

```ts
import { workflow } from "@ewhauser/eve-ambient/workflow";

const ambient = defineAmbientApplication({
  applicationId: "support-agent",
  rules: [incidentRule],
  routes: [eveRoute],
}).with(workflow({
  callbackUrl: "https://agent.example.com",
  callbackSecretEnv: "AMBIENT_CALLBACK_SECRET",
}));

const receipt = await ambient.publish(slackChannel, providerDelivery);
```

The publisher canonicalizes the event, evaluates all rules, freezes the
complete fan-out, and calls `engine.accept()`. A successful receipt means every
selected branch has reached durable backend custody. It does not mean a
decision or delivery has already completed.

An application may register rules from multiple channels with unrelated event
types. Each `defineAmbientRule()` call preserves its channel's type; each
`publish(channel, input)` call infers that channel independently. The
application registry itself has no single event generic. Requiring every rule
to name its channel is what makes that heterogeneous registry type-safe and
prevents an event from being evaluated by a rule for a different shape.

Conditional direct chat dispatch is deliberately separate. Configure the
publisher's optional `direct` rule with an adapter that owns its own stable
idempotency boundary. Direct dispatch does not become part of the attention
correlation or its Workflow history.
