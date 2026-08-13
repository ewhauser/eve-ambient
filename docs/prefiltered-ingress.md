# Prefiltered ingress

Eve Ambient does not require ownership of the raw event firehose. An existing
rules engine, stream processor, SIEM, alerting system, or domain-specific
detector can select events first and publish only the signals that may deserve
agent attention.

## Topology

```text
providers -> external durable pipeline -> selection -> Eve Ambient publish()
                                                    -> mailbox -> decision -> delivery
```

The external pipeline owns everything before Eve Ambient accepts the event.
The default Eve runtime stores accepted events and processes them through the
same durable monitoring path as channel-originated events.

## Describe the event source

The publisher still defines an inbound channel and typed event. The channel
definition is the schema and source identity; it does not require Eve Ambient
to own the provider webhook or queue consumer.

```ts
import { z } from "zod";
import {
  defineChannelEvent,
  defineInboundChannel,
} from "@ewhauser/eve-ambient";

export const selectedSignals = defineInboundChannel({
  id: "selected-signals",
  replyTarget: z.object({ targetId: z.string() }),
  inbound: {
    detected: defineChannelEvent({
      schema: z.object({
        kind: z.string(),
        summary: z.string(),
        sourceRef: z.string(),
      }),
      maxBytes: 64_000,
    }),
  },
});
```

Publish after the external pipeline has durably selected and normalized the
signal:

```ts
const result = await runtime.publish(selectedSignals, "detected", {
  tenantId,
  installationId,
  id: stableSourceEventId,
  occurredAt,
  data: signal,
  replyTarget: { targetId },
  origin: { kind: "external" },
});
```

`publish()` returns `accepted` after the event and matching subscription
snapshots commit, or `duplicate` when the same scoped source ID was accepted
previously. A pull consumer may commit its source offset after either result.
It should retry any unknown or failed outcome with the same source ID.

## Responsibility boundary

| Responsibility | External pipeline | Eve Ambient after `publish()` |
|---|---:|---:|
| Provider acknowledgement and raw source retention | Yes | No |
| Raw-stream ordering and replay | Yes | No |
| Upstream selection and its audit trail | Yes | No |
| Channel schema validation | No | Yes |
| Scoped source dedupe | No | Yes |
| Monitor filter and correlation | No | Yes |
| Buffering, cooldown, and budgets | No | Yes |
| Decision, evidence, delivery, and dead letters | No | Yes |

When the upstream selection is complete, a monitor may omit `filter` or use it
only for local safety checks. Keep `correlate`, buffer policy, decision, task
evidence, route, and wake budgets in Eve so the durable attention behavior
remains visible in one monitor definition.

## Kafka and other durable logs

The package does not currently ship a generic `EventLog` interface or a Kafka
consumer. A custom consumer can call `publish()` today, but the integration owns
its offset protocol and must preserve these rules:

1. Use a stable provider or log-record identity for `id`.
2. Retry ambiguous outcomes with that same identity.
3. Commit the source offset only after `accepted` or `duplicate`.
4. Do not claim stronger ordering or replay semantics than the source and
   consumer actually provide.
5. Preserve authoritative payload access for as long as replay or evaluation
   requires it.

For a topology that also separates the correlation mailbox from PostgreSQL,
see [celld mailbox](celld.md). The event-log and mailbox choices are independent.
