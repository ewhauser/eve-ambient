# Monitoring model

Eve Ambient has four conceptual boundaries:

1. **Channels publish typed events.** They own transport, authentication,
   normalization, provider acknowledgement, canonical targets, and conversation
   bindings.
2. **Monitors decide what merits cognition.** They filter, correlate, buffer,
   classify, project evidence, and select a registered delivery channel.
3. **The runtime makes that process durable.** It owns dedupe, leases, timers,
   budgets, retries, runs, dead letters, and retention.
4. **Sessions reason and act.** They receive trusted static instructions and a
   separate immutable evidence snapshot.

Monitors never poll, own webhooks, consume queues directly, or execute tools.

## Define a channel event

```ts
import { z } from "zod";
import {
  defineChannelEvent,
  defineInboundChannel,
} from "@ewhauser/eve-ambient";

export const slackEvents = defineInboundChannel({
  id: "slack",
  replyTarget: z.object({
    channelId: z.string(),
    threadTs: z.string(),
  }),
  inbound: {
    message: defineChannelEvent({
      schema: z.object({
        channelId: z.string(),
        ts: z.string(),
        threadTs: z.string().optional(),
        text: z.string(),
      }),
      chat: true,
      maxBytes: 128_000,
    }),
  },
});
```

`defineChannelEvent` accepts any Standard Schema v1 implementation. Publishing
validates the schema and size before durable acceptance. Source dedupe is
scoped by tenant, application, channel installation, and provider event ID.

## Define a monitor

```ts
import { z } from "zod";
import {
  compileMonitor,
  defineMonitor,
  ignore,
  modelDecision,
} from "@ewhauser/eve-ambient";

export const ambientEngineering = defineMonitor({
  id: "ambient-engineering",
  mode: "active",
  sources: [
    slackEvents.event("message", { phase: "undispatched" }),
  ],

  filter: ({ event }) =>
    !event.actor?.isBot && event.data.text.trim().length > 0,

  correlate: ({ event }) => [
    event.source.installationId,
    event.data.channelId,
    event.data.threadTs ?? event.data.ts,
  ].join(":"),

  buffer: {
    mode: "debounce",
    quietPeriod: "2s",
    maxWait: "15s",
    maxEvents: 20,
    maxBytes: 64_000,
  },

  decision: modelDecision({
    model: "openai/gpt-5-nano",
    reasoning: "none",
    instructions: "Wake only when the engineering agent can contribute.",
    input: ({ events, instance, batch }) => ({
      messages: events.map((event) => event.data.text),
      priorWakeAt: instance.lastWakeAt ?? null,
      completeness: batch,
    }),
    metadata: {
      ignore: z.object({}),
      wake: z.object({ priority: z.enum(["low", "normal", "high"]) }),
    },
    timeout: "8s",
    maxInputTokens: 4_000,
    maxOutputTokens: 250,
    onError: ignore({ reason: "classifier-unavailable", metadata: {} }),
  }),

  cooldown: { afterWake: "30s", during: "accumulate" },

  task: {
    // Trusted static configuration. Never interpolate event text here.
    instructions: "Review the attached evidence independently and respond only when useful.",
    evidence: ({ events, decision, batch }) => ({
      messages: events.map((event) => ({ ref: event.ref, text: event.data.text })),
      classifier: {
        action: decision.action,
        reason: decision.reason,
        metadata: decision.metadata ?? null,
      },
      completeness: batch,
    }),
  },

  route: ({ events }) => {
    const target = events.at(-1)?.replyTarget;
    return target
      ? { channel: slackDelivery, target, auth: "app" }
      : null;
  },

  session: { strategy: "channel", idleTimeout: "24h" },
  limits: {
    perMonitor: {
      maxEventsPerMinute: 2_000,
      maxModelCallsPerMinute: 120,
      maxModelInputTokensPerHour: 250_000,
      maxWakesPerHour: 30,
    },
    perKey: { maxWakesPerHour: 4 },
    overflow: "buffer",
  },
  retention: { decisions: "30d", dedupe: "7d" },
  metadata: { owner: "engineering-productivity", useCase: "ambient-slack" },
});

export const compiled = compileMonitor(ambientEngineering, "git:8e7b2f1");
```

`filter`, `correlate`, rule decisions, evidence projection, and routing must be
synchronous and side-effect-free. The runtime rejects returned promises and
non-JSON evidence, targets, metadata, or event payloads. Correlation produces
one exact stable string or `null`; there is no semantic join or instance merge.

The buffer closes on its quiet period, mandatory maximum wait, count, or byte
threshold. Cooldown state is maintained per correlation key and can accumulate
events for a later evaluation.

## Wire the runtime

```ts
import { MonitorRuntime } from "@ewhauser/eve-ambient";
import { createAiSdkMonitorInvoker } from "@ewhauser/eve-ambient/ai-sdk";
import { PostgresMonitorStore } from "@ewhauser/eve-ambient/postgres";

const runtime = new MonitorRuntime({
  applicationId: "engineering-agent",
  deployment: { monitors: [compiled] },
  channels: [slackEvents],
  deliveryChannels: [slackDelivery],
  store: new PostgresMonitorStore({ pool }),
  modelInvoker: createAiSdkMonitorInvoker(),
  observer: telemetryObserver,
  maxEvidenceBytes: 1_000_000,
  budgets: {
    platformId: "eve-production",
    platform: { maxModelCallsPerMinute: 5_000 },
    tenant: tenantId => tenantBudgets.get(tenantId),
    application: { maxWakesPerHour: 100 },
    overflow: "buffer",
  },
});

await runtime.initialize();
```

See [Postgres-first deployment](postgres.md) for publishing and worker
operation. See [Prefiltered ingress](prefiltered-ingress.md) when an external
system selects events before they enter the runtime. See
[Persistence responsibilities](storage-responsibilities.md) for the store
facets behind ingress, branch custody, mailboxes, runs, budgets, and retention.

## Chat phases and direct dispatch

Chat monitors can subscribe to `observed` events or to events that remain
`undispatched` after direct handlers finish. `publishChat()` atomically accepts
observed branches and full-payload conditional undispatched branches before it
calls a handler. A durable `undispatched` outcome activates those frozen
branches; a durable turn receipt cancels them.

This prevents an ambient monitor from racing a normal direct agent response.
Provider acknowledgement remains outside that completion path, and direct
handlers receive the complete canonical event plus a stable
`directDispatchKey`; they must deduplicate their turn command by that key.

## Delivery boundary

A delivery channel implements `MonitorDeliveryChannel`. It receives static
trusted task instructions and a separate untrusted `MonitorEvidenceSnapshot`.
It must:

- resolve the canonical target through its own conversation-binding registry;
- reject a non-terminal binding or target conflict;
- refresh a stale reference only after the old generation is terminal;
- idempotently admit the provided `wakeKey` and return a stable receipt;
- put human and monitor requests on the same durable session ingress queue;
- choose and document its policy for distinct delivery keys without merging or
  dropping human input; if it coalesces, it must freeze membership before the
  derived turn starts; and
- execute only as the application principal.

`MemoryConversationChannel` in `@ewhauser/eve-ambient/testing` is a binding and
coalescing conformance implementation for tests.

## Model boundary

`modelDecision()` always names its model, reasoning level, timeout, input and
output budgets, metadata schemas, optional repair, and fallback.
`createAiSdkMonitorInvoker()` performs one structured, tool-less model step. It
places source data in an untrusted user payload and keeps classifier
instructions separate.

Unknown actions, invalid confidence, long reasons, and invalid action-specific
metadata are rejected before policy or delivery. Each initial or repair attempt
consumes a separate model-call reservation. Input-token budgets reserve the
declared per-attempt maximum before the call, so the hard ceiling remains
conservative when provider tokenization differs from the runtime estimate.

For another model stack, implement `MonitorModelInvoker`. That interface has no
tool, credential, session-history, or delivery capability.
