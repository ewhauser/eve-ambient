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
                           correlation Workflow
                        ring / buffer / durable timer
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
serialized Workflow run. Active handoffs carry complete values rather than payload
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
pnpm add @ewhauser/eve-ambient@^0.6.0 workflow@5.0.0-beta.42
```

### Define the event boundary

Your Slack adapter verifies the webhook and supplies this small normalized
input. The channel contract converts it into the complete event Ambient hashes
and routes. Choosing `channelId` as `partitionKey` creates one correlation per
Slack channel for this rule—not one correlation per message.

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

The rule admits only A and B messages. Its default correlation is one run
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

In production, bind the same definition to Workflow. The application supplies
its public callback URL; Workflow selects the configured standard World:

```ts
import { workflow } from "@ewhauser/eve-ambient/workflow";

export function createProductionApplication(turns: TurnSink) {
  return definition(turns).with(workflow({
    callbackUrl: "https://agent.example.com",
    callbackSecretEnv: "AMBIENT_CALLBACK_SECRET",
  }));
}
```

Re-export Ambient's packaged workflow from a file in the application's
`workflows/` directory so the Workflow compiler discovers it:

```ts
// workflows/ambient.ts
export * from "@ewhauser/eve-ambient/workflows";
```

Configure Workflow for the application's framework, and mount the returned
`fetch` handler at its authenticated prepare and deliver callback paths.

The complete typechecked example is
[`examples/slack-sequence/src/slack-message-sequence.ts`](examples/slack-sequence/src/slack-message-sequence.ts).

With 100 joined channels, this produces at most 100 active correlation runs for
this rule version. If every channel receives A followed by B, admission makes
200 hook-resume operations into those same 100 runs. After the quiet period,
there are up to 100 distinct wakes—not 200 workflows. Prepare and delivery are
at-least-once steps, so callbacks may repeat; the `TurnSink` deduplicates them
by `wakeKey`.

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

## Choose the Workflow World

Ambient is a Workflow library, so it uses the standard World selected by the
Workflow runtime. There is no Ambient-specific `AttentionWorld` interface or
adapter. The chosen World owns Workflow storage, queues, streams, encryption,
retention, and observability; Ambient's correlation run owns the bounded ring,
batching state, timers, retries, and prepared wake.

Ambient 0.6 has concrete deployment paths for these Worlds:

| World | Install and select it | Operational requirement |
|---|---|---|
| [Vercel](https://workflow-sdk.dev/worlds/vercel) | Deploy the Workflow application to Vercel; the managed World is selected automatically | Enable Fluid compute; Vercel owns storage, queues, authentication, and observability |
| [Postgres](https://workflow-sdk.dev/worlds/postgres) | Install `@workflow/world-postgres@beta`, set `WORKFLOW_TARGET_WORLD=@workflow/world-postgres` and `WORKFLOW_POSTGRES_URL`, then run its idempotent `bootstrap` command | Run `world.start()` in a long-lived process so Graphile Worker can poll; this is not a serverless backend |
| [`world-celld`](https://github.com/ewhauser/world-celld) | Install `@ewhauser/world-celld@^0.3.0`, set `WORKFLOW_TARGET_WORLD=@ewhauser/world-celld`, `CELLD_FLEET_URL`, `CELLD_WORLD_SECRET`, and `WORKFLOW_BASE_URL`, then deploy its packaged worker | Operate a celld fleet and a conditional-write-capable object store; the backend is experimental |

The Postgres package must come from its `beta` npm channel while Ambient uses
Workflow 5; its npm `latest` tag is still the Workflow 4 line. The linked setup
pages include runnable examples, migrations or worker deployment, and the
required application startup hooks.

The [full Worlds directory](https://workflow-sdk.dev/worlds) remains the place
to explore other official and community implementations. Inclusion there is
not an Ambient compatibility claim: before deploying another World, verify
that its published package implements the Workflow 5 contract and that its
queue, hook, timer, and stream conformance tests pass.

For deterministic tests, the package includes `memory()`. It implements the
same stream reducer and explicit `runDue()` scheduling, but it is not a
production persistence backend.

## How it works

For each inbound event, Ambient:

1. canonicalizes the typed channel event and derives stable identity;
2. runs deterministic rule selection and correlation;
3. groups selected branches by correlation address;
4. resumes the deterministic hook for each distinct correlation;
5. starts the correlation Workflow if that hook does not exist yet; and
6. lets each run buffer, prepare, checkpoint, and deliver independently.

### Ambient protocol fanout

```text
0 selected correlations -> 0 hook resumes
1 selected correlation  -> 1 hook resume
N selected correlations -> N concurrent hook resumes
```

This is Ambient's public protocol fanout: one high-level `resumeHook()` call per
distinct selected correlation. Multiple matching rules that share a correlation
are grouped into the same append.

### Measured Workflow and storage work

The Workflow runtime expands that protocol call into internal World activity.
The checked-in Workflow 5.0.0-beta.42 integration currently observes:

| Warm path | Ambient protocol calls | Standard World method calls | Application HTTP |
|---|---:|---:|---:|
| Buffer only | 1 `resumeHook()` | 7 | 0 |
| Close, prepare, and deliver | 1 `resumeHook()` | 15 | 2 |

The 7 internal calls are one hook lookup, one run read, three event writes, and
two queue publishes. These counts describe the instrumented runtime and local
test World, not a requirement imposed on every World. A cold correlation also
needs `start()` and hook-registration polling, so its internal call count varies.

Each run retains a bounded recent-message ring for best-effort admission
deduplication. Ring eviction may allow an old event to be processed again, so
final effects do not depend on that cache. The durable receiver must enforce
the stable `wakeKey`.

Applied full-value reducer state is capped per correlation (1,000 pending
branches and 16 MiB by default). Once either limit is reached, the run holds at
most the next validated append and stops consuming the hook until due work
releases capacity; later appends remain in Workflow's durable queue. A single
append larger than either configured limit is rejected before `resumeHook()`.

Correlation runs are intentionally permanent: there is no automatic rotation
or handoff protocol. Live reducer state remains bounded, but the underlying
Workflow event history grows while a correlation stays active. Operators
should monitor per-run history limits and choose correlation keys with bounded
traffic; a future standard continue-as-new primitive can add compaction without
reintroducing a custom World contract.

The deterministic hook token includes a fingerprint of immutable Workflow
options. Changing callback routing, retry, ring, lease, or capacity options
therefore starts a new owner for subsequently admitted events instead of
silently reusing an old run's captured configuration. Reducer state does not
migrate across that cutover; drain or explicitly abandon the previous owner.

There is no event coordinator, global attention run, custom storage adapter,
or global correlation registry in the admission path.

## Repository

| Workspace | Purpose | Published |
|---|---|---|
| [`packages/ambient`](packages/ambient) | Rules, reducer, memory reference, and packaged Workflow runtime | `@ewhauser/eve-ambient` |
| [`examples/slack-sequence`](examples/slack-sequence) | Complete typechecked Slack A-then-B application | No |
| [`integration/workflow-correlation`](integration/workflow-correlation) | Consumer discovery, concurrency, retry, permanence, and call-count checks | No |

## Documentation

- [Correlation Workflow protocol](docs/attention-engine.md)
- [Workflow World deployment](docs/deployment-options.md)
- [Monitoring and rules](docs/monitoring-model.md)
- [Operations and security](docs/operations-and-security.md)
- [Architecture decision index](docs/rfcs/README.md)
- [RFC 0005: Permanent correlation Workflows](docs/rfcs/0005-permanent-correlation-workflows.md)

## Development

```sh
corepack enable pnpm
pnpm install
pnpm check
```
