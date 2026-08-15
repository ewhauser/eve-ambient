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

## Canonical example: a pull-request shepherd

Suppose you want an engineering agent to watch pull requests without starting
a turn for every push and CI update. The useful behavior is:

- listen to GitHub `pull_request` and `check_suite` webhooks;
- collect the event burst for one pull request;
- wait for CI to settle, with a maximum wait so a hot PR cannot wait forever;
- ignore a closure, a later recovery, or an all-green batch; and
- invoke one Eve turn when the latest observed check state still has a failure.

Install Ambient and its Eve adapter:

```sh
pnpm add @ewhauser/eve-ambient @ewhauser/eve-ambient-eve eve@0.38.1
```

The adapter targets that exact Eve version and requires its carried patch; see
the [adapter installation instructions](packages/eve-adapter/README.md).

### Listen, correlate, and decide

The Eve adapter supplies a typed channel that combines PR and check-suite
activity. It already partitions events by pull request, so the rule only needs
to define buffering and the condition that merits a turn:

```ts
import {
  debounce,
  defineAmbientRule,
  ignore,
  wake,
} from "@ewhauser/eve-ambient";
import {
  eveGitHubPullRequestActivity,
  type EveGitHubCheckSuiteActivityEvent,
} from "@ewhauser/eve-ambient-eve";

const failing = new Set([
  "action_required",
  "cancelled",
  "failure",
  "startup_failure",
  "timed_out",
]);

const pullRequestShepherd = defineAmbientRule({
  id: "pull-request-shepherd",
  version: "v1",
  channel: eveGitHubPullRequestActivity,
  policy: debounce({
    quiet: "2m",
    maxWait: "15m",
    cooldown: "30m",
    maxEvents: 100,
  }),
  decide({ events, eventKeys }) {
    const suites = new Map<string, EveGitHubCheckSuiteActivityEvent>();
    let closed = false;

    for (const event of events) {
      if (event.type === "github.pull-request") {
        if (["opened", "reopened", "synchronize"].includes(event.data.action)) {
          closed = false;
          suites.clear();
        } else if (event.data.action === "closed") {
          closed = true;
        }
      } else if (!closed) {
        suites.set(event.data.appSlug ?? `suite:${event.data.checkSuiteId}`, event);
      }
    }

    if (closed) return ignore({ reason: "pull request is closed" });
    const failures = [...suites.values()].filter(
      event => event.data.action === "completed" &&
        event.data.conclusion !== null &&
        failing.has(event.data.conclusion),
    );
    const failure = failures.at(-1);
    if (failure?.replyTarget === undefined) {
      return ignore({ reason: "current check suites do not show a failure" });
    }

    return wake({
      target: failure.replyTarget,
      instruction:
        "Inspect this pull request and fix the smallest safe CI blocker. " +
        "If no safe fix is possible, report the exact blocker and next action.",
      decision: { reason: "latest observed CI state is failing" },
      evidence: {
        repository: failure.data.repository.fullName,
        pullRequestNumber: failure.data.pullRequestNumber,
        failures: failures.map(event => ({
          app: event.data.appSlug,
          conclusion: event.data.conclusion,
          headSha: event.data.headSha,
        })),
        eventKeys,
      },
    });
  },
});
```

The rule receives a typed event union: TypeScript narrows PR fields and check
suite fields from `event.type`. `ignore()` records a terminal no-turn decision.
`wake()` separates the application's trusted instruction from untrusted event
evidence and carries the PR's complete Eve continuation target.

### Invoke the Eve turn

Define the route once, bind the application to a World, and wrap Eve's normal
GitHub channel. `attentionWorld`, `githubFrom`, `auth`, and `credentials` are
deployment bindings supplied by the host application.

```ts
import { defineAmbientApplication } from "@ewhauser/eve-ambient";
import { world } from "@ewhauser/eve-ambient/world";
import {
  createEveGitHubAmbientChannel,
  createEveGitHubAttentionRoute,
} from "@ewhauser/eve-ambient-eve";

const ambient = defineAmbientApplication({
  applicationId: "engineering-agent",
  rules: [pullRequestShepherd],
  routes: [createEveGitHubAttentionRoute({ from: githubFrom, auth })],
}).with(world({
  world: attentionWorld,
  callbackSecretEnv: "AMBIENT_CALLBACK_SECRET",
}));

export const github = createEveGitHubAmbientChannel({
  publisher: ambient,
  tenantId: context => context.repository.owner,
  credentials,
});

// Mount as the World's authenticated prepare/deliver callback handler.
export const POST = ambient.fetch;
```

The returned `github` value is still Eve's normal GitHub channel. Mentions and
other direct conversation events can invoke Eve normally, while PR and
check-suite hooks take the ambient path above.

The resulting path is concrete:

```text
GitHub webhook
  -> Eve verifies and normalizes pull_request / check_suite
  -> Ambient durably appends before GitHub receives 2xx
  -> one correlation stream per pull request
  -> debounce and decide
  -> checkpoint wake
  -> Eve route sends one queued turn with idempotencyKey = wakeKey
```

The final route creates or resumes the PR's Eve conversation with the stateful
GitHub target carried by the event. A lost delivery response retries the same
`wakeKey`, so Eve admits the same durable turn instead of starting another.

The complete, typechecked version lives in
[`examples/world-attention/src/github-pr-shepherd.ts`](examples/world-attention/src/github-pr-shepherd.ts).

### Other patterns

The same shape applies beyond CI:

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
reducer in one atomic append. `world-celld` is being developed directly against
that contract.

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
| [`packages/eve-adapter`](packages/eve-adapter) | Eve ingress, attention delivery, and direct dispatch | `@ewhauser/eve-ambient-eve` |
| [`examples/world-attention`](examples/world-attention) | Typechecked support and GitHub PR-shepherd definitions bound to memory or a supplied World | No |
| [`integration/attention-world`](integration/attention-world) | Executable RPC fanout and ring-dedup contract | No |
| [`integration/eve-conformance`](integration/eve-conformance) | Exact Eve patch and adapter conformance | No |

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
