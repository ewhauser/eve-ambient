# Eve Ambient

Durable ambient attention for Eve agents.

Agents are most useful when they can notice what is happening around them, not
only when someone sends them a direct prompt. But every message, webhook,
alert, or state change does not deserve an agent run.

Eve Ambient sits between event intake and agent cognition. Applications define
typed channels and attention rules. Ambient selects relevant events, correlates
related activity, buffers it by key, makes a bounded decision, and wakes Eve
only when attention is warranted.

```text
 channel events       signal pipeline       durable log consumer
       |                     |                       |
       +---------------------+-----------------------+
                             |
                         publish()
                             |
                 select and group by correlation
                         |             |
                         x ignore      v
                                   World stream
                             dedup / buffer / timer
                                       |
                                   prepare()
                                ignore x | wake
                                         v
                                     checkpoint
                                         |
                                     deliver()
                                         |
                                         v
                                    Eve session
```

Ambient is not an event bus, replay system, or general workflow engine. Channel
adapters and event infrastructure still own transport, raw retention, and
normalization. Ambient owns the smaller question: which normalized events merit
cognition, and how does that decision survive duplicates, restarts, and failed
delivery?

## Why?

The simple version of ambient attention is application glue: a webhook, an
in-process debounce, a classifier, and a call to an agent. It works until a
process restarts, an event arrives twice, a hot correlation key overwhelms a
worker, or the classifier succeeds and the final handoff fails.

Eve Ambient makes those boundaries explicit. Each correlation address has one
serialized stream. Active handoffs carry complete values rather than payload
references. A wake is checkpointed before delivery, and retries reuse the same
bytes and durable `wakeKey`.

That gives applications one attention model without pretending that every
event system has the same shape.

## Canonical example: message A, then message B

Suppose a bot is present in 100 Slack channels. You want one rule to watch each
channel and invoke a turn when someone says “message A” and a later message in
the same collection window says “message B.”

Everything below comes from `@ewhauser/eve-ambient` or is defined in the
example itself:

```sh
pnpm add @ewhauser/eve-ambient@^0.5.0
```

### Define the event boundary

Your Slack adapter verifies the webhook and supplies this small normalized
input. The channel contract converts it into the complete event Ambient hashes
and routes. Choosing `channelId` as `partitionKey` creates one stream per Slack
channel for this rule—not one stream per message.

```ts
import {
  defineChannelCanonicalization,
  type CanonicalChannelEvent,
} from "@ewhauser/eve-ambient";

interface SlackMessageInput {
  eventId: string;
  occurredAt: string;
  tenantId: string;
  workspaceId: string;
  channelId: string;
  userId: string;
  text: string;
}

type SlackMessageEvent = CanonicalChannelEvent<
  "slack.message",
  { workspaceId: string; channelId: string; text: string },
  string
>;

const slackMessages = defineChannelCanonicalization<
  SlackMessageInput,
  SlackMessageEvent
>({
  version: 1,
  partitionKey: event => event.data.channelId,
  canonicalize: input => ({
    id: input.eventId,
    type: "slack.message",
    version: 1,
    occurredAt: input.occurredAt,
    data: {
      workspaceId: input.workspaceId,
      channelId: input.channelId,
      text: input.text,
    },
    source: {
      channelId: "slack",
      installationId: input.workspaceId,
      tenantId: input.tenantId,
    },
    actor: { id: input.userId, principalType: "user" },
    replyTarget: `slack:${input.workspaceId}:${input.channelId}`,
    origin: { kind: "external", depth: 0 },
  }),
});
```

Authentication and transport acknowledgement remain the Slack adapter's job.
The stable `eventId` must survive provider retries.

### Listen, correlate, and decide

The rule admits only A and B messages. Its default correlation is one stream
per rule inside the channel partition. The debounce window gives related
messages time to arrive before `decide()` receives the ordered batch. Ambient
orders it by the canonical `occurredAt` value, so preserve Slack's event
timestamp during normalization.

```ts
import {
  debounce,
  defineAmbientRule,
  ignore,
  wake,
} from "@ewhauser/eve-ambient";

const text = (event: SlackMessageEvent) =>
  event.data.text.trim().toLowerCase();

const messageSequence = defineAmbientRule({
  id: "message-a-then-b",
  version: "v1",
  channel: slackMessages,
  matches: event => ["message a", "message b"].includes(text(event)),
  policy: debounce({
    quiet: "2m",
    maxWait: "10m",
    cooldown: "30m",
    maxEvents: 48,
  }),
  decide({ events, eventKeys }) {
    const firstA = events.findIndex(event => text(event) === "message a");
    const followingB = events.findIndex(
      (event, index) => index > firstA && text(event) === "message b",
    );

    if (firstA < 0 || followingB < 0) {
      return ignore({ reason: "the batch has no A-then-B sequence" });
    }

    return wake({
      routeId: "turns",
      target: events[followingB]!.replyTarget!,
      instruction:
        "Review the Slack conversation and take the configured follow-up action.",
      decision: { reason: "message A was followed by message B" },
      evidence: {
        channelId: events[followingB]!.data.channelId,
        matchedEventKeys: [eventKeys[firstA]!, eventKeys[followingB]!],
      },
    });
  },
});
```

This detects the sequence inside one frozen batch. Ambient intentionally does
not retain arbitrary rule history after a batch completes, so an unbounded
“A at any time, then B days later” rule belongs in application-owned state.

### Invoke the turn

The final side effect is an explicit application dependency. A `TurnSink` can
be backed by Eve or another durable agent queue; it must deduplicate on the
supplied `idempotencyKey`.

```ts
import {
  defineAmbientApplication,
  type JsonValue,
} from "@ewhauser/eve-ambient";

interface TurnSink {
  enqueue(request: {
    idempotencyKey: string;
    address: string;
    instruction: string;
    evidence: JsonValue;
  }): Promise<JsonValue>;
}

function address(target: JsonValue): string {
  if (typeof target !== "string") {
    throw new TypeError("the Slack turn target must be a string");
  }
  return target;
}

const definition = (turns: TurnSink) => defineAmbientApplication({
  applicationId: "slack-sequence-agent",
  rules: [messageSequence],
  routes: [{
    id: "turns",
    deliver: wake => turns.enqueue({
      idempotencyKey: wake.wakeKey,
      address: address(wake.target),
      instruction: wake.instruction,
      evidence: wake.evidence,
    }),
  }],
});
```

No `TurnSink` implementation is hidden in Ambient: the application supplies
it, and the interface above is its complete contract. The stable `wakeKey`
becomes the downstream turn admission key.

For local development, bind the definition to the included memory backend:

```ts
import { memory } from "@ewhauser/eve-ambient/memory";

export function createLocalApplication(turns: TurnSink) {
  return definition(turns).with(memory());
}
```

Publish each verified Slack input with
`ambient.publish(slackMessages, input)`. In deterministic tests, advance the
injected clock past the debounce deadline before calling
`ambient.engine.runDue()`.

In production, bind the same definition to a conforming `AttentionWorld`. The
World client is a deployment dependency supplied by the application:

```ts
import { world } from "@ewhauser/eve-ambient/world";
import type { AttentionWorld } from "@ewhauser/eve-ambient/protocol";

export function createProductionApplication(
  turns: TurnSink,
  attentionWorld: AttentionWorld,
) {
  return definition(turns).with(world({
    world: attentionWorld,
    callbackSecretEnv: "AMBIENT_CALLBACK_SECRET",
  }));
}
```

Mount the returned application's `fetch` handler at its authenticated prepare
and deliver callback paths.

The complete typechecked example is
[`examples/world-attention/src/slack-message-sequence.ts`](examples/world-attention/src/slack-message-sequence.ts).

With 100 joined channels, this produces at most 100 logical streams for this
rule version. If every channel receives A followed by B, admission makes 200
append RPCs. After the quiet period, the World makes 100 prepare callbacks and,
because every sequence matches, 100 delivery callbacks; each delivery invokes
the `TurnSink` once. That is 100 streams and up to 100 turns—not 200 workflows.

### Other patterns

The same shape applies beyond this Slack sequence:

| Listen to | Correlate by | Invoke a turn when |
|---|---|---|
| Deployments, errors, and health checks | service + deployment | a rollout remains unhealthy after its signals settle |
| Support tickets, replies, and account changes | customer + case | severity or SLA risk crosses a threshold |
| Identity, endpoint, and cloud detections | principal + investigation | several weak signals form one actionable incident |
| Orders, payments, and fulfillment events | order | the latest state needs reconciliation or human judgment |
| Scheduled snapshots and configuration changes | resource | drift persists across observations |

Some of these listeners can publish raw channel events. Others can be the
output of an existing signal pipeline. Ambient starts at whichever boundary
your application considers trustworthy and useful.

## Choose what enters Ambient

There is no single right place to reduce event volume. The publishing API is
the boundary, so applications can choose the path that fits their existing
system.

| Path | What enters Ambient | What stays outside | Good fit |
|---|---|---|---|
| **Publish channel events** | Normalized Slack, GitHub, webhook, scheduled, or application events | Provider delivery and raw retention | Applications that want Ambient rules to perform the first meaningful selection |
| **Bring your own high-signal events** | Events already selected by a SIEM, rules engine, stream processor, or domain-specific detector | Raw firehose, broad filtering, and detection pipelines | Organizations that already know what “interesting” means or cannot send the full event rate to an agent system |
| **Consume a durable log** | Events published by a Kafka or similar log consumer | Offsets, replay, long-term retention, and consumer scaling | High-volume systems that need ingestion to scale independently from correlation and cognition |

These paths can coexist. A single application might publish GitHub events
directly, consume Slack signals from Kafka, and accept incidents from an
existing detection service. Once an event crosses `publish()`, the same typed
rules, correlation protocol, and final idempotency boundary apply.

## Choose where streams live

Ambient sends each selected correlation directly to an `AttentionWorld`. The
World implementation owns deterministic stream addressing, atomic append,
the bounded dedup ring, batches, timers, leases, retries, and checkpointed
delivery. Ambient does not need to know whether those capabilities use
Postgres, Redis, celld, a managed service, or a combination.

The broader Workflow SDK ecosystem is a useful map of the available deployment
shapes:

| Option | Shape |
|---|---|
| [Vercel World](https://workflow-sdk.dev/worlds/vercel) | Fully managed storage, queuing, scaling, authentication, and observability on Vercel |
| [Postgres World](https://workflow-sdk.dev/worlds/postgres) | Official open-source, self-hosted implementation using PostgreSQL and Graphile Worker |
| [Turso, MongoDB, and Redis Worlds](https://github.com/mizzle-dev/workflow-worlds) | Community open-source implementations for several datastore choices |
| [Redis, BullMQ, Cloudflare, MySQL, Azure, NATS, and Upstash Worlds](https://github.com/vinnymac/worlds) | Community open-source implementations spanning databases, queues, and edge runtimes |

See the [full Worlds directory](https://workflow-sdk.dev/worlds) for the
maintainer-curated list of official and community implementations.

Ambient deliberately uses a smaller contract than the Workflow SDK runtime:
`world.stream(key).append(value)`. The packages above are implementation
foundations, not automatic drop-in Ambient bindings. A conforming adapter must
construct stream handles locally and durably apply the exported correlation
reducer in one atomic append.

For deterministic tests, the package includes `memory()`. It implements the
same stream reducer and explicit `runDue()` scheduling, but it is not a
production persistence backend.

## How it works

For each inbound event, Ambient:

1. canonicalizes the typed channel event and derives stable identity;
2. runs deterministic rule selection and correlation;
3. groups selected branches by correlation address;
4. calls `world.stream(key).append(...)` once per distinct address; and
5. lets each stream buffer, prepare, checkpoint, and deliver independently.

```text
0 selected correlations -> 0 append RPCs
1 selected correlation  -> 1 append RPC
N selected correlations -> N concurrent append RPCs
```

Each stream retains a bounded recent-message ring for best-effort admission
deduplication. A retry resends every append; streams that still remember the
event return `duplicate`. Ring eviction may allow an old event to be processed
again, so final effects do not depend on that cache. The durable receiver must
enforce the stable `wakeKey`.

There is no event coordinator, global fanout workflow, storage lookup, or
global stream registry in the admission path.

## Repository

| Workspace | Purpose | Published |
|---|---|---|
| [`packages/ambient`](packages/ambient) | Rules, protocol, reducer, memory reference, and World adapter | `@ewhauser/eve-ambient` |
| [`examples/world-attention`](examples/world-attention) | Typechecked support and Slack sequence definitions bound to memory or a supplied World | No |
| [`integration/attention-world`](integration/attention-world) | Executable RPC fanout and ring-dedup contract | No |

## Documentation

- [Attention stream protocol](docs/attention-engine.md)
- [World deployment](docs/deployment-options.md)
- [Monitoring and rules](docs/monitoring-model.md)
- [Operations and security](docs/operations-and-security.md)
- [Architecture decision index](docs/rfcs/README.md)
- [RFC 0004: Correlation World protocol](docs/rfcs/0004-correlation-world-protocol.md)

## Development

```sh
corepack enable pnpm
pnpm install
pnpm check
```
