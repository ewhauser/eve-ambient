# Eve Ambient

Durable ambient attention for Eve agents.

Applications define typed channel normalization and ambient rules. Eve Ambient
freezes the resulting fan-out, correlates complete events by key, buffers them,
records a decision before delivery, and wakes Eve only when attention is
warranted.

```text
channel event → eventKey → occurrenceKey → branchKey
                                      ↓
                         correlate → batchKey → runKey
                                      ↓
                                  wakeKey → Eve
```

Every durable handoff carries the complete payload by value. Keys express
lineage and idempotency; they are never payload references. When a workflow is
terminal, its event payloads are deleted and only bounded receipts remain.
There is no event repository, lookup, history, or replay API.

## Why

Sending every message, webhook, alert, or state change directly to an agent is
noisy and expensive. Application-specific debounce and retry glue also tends
to fail at exactly the awkward boundaries: duplicate delivery, process restart,
hot correlation keys, or a completed decision followed by a failed handoff.

Eve Ambient supplies one durable boundary:

```ts
interface AttentionEngine {
  accept(fanout: AcceptedFanout): Promise<AttentionAcceptanceReceipt>;
}
```

The backend owns correlation and retry. The application owns two callbacks:

- `prepare(batch)` performs bounded, tool-less decision work.
- `deliver(wake)` performs the final idempotent Ambient handoff.

The prepared outcome is durably recorded before delivery. A lost delivery
response therefore retries the exact same bytes and `wakeKey`.

## Backends

| Backend | Durable behavior | Worker model |
|---|---|---|
| Memory | Executable reference implementation and deterministic tests | Explicit `runDue()` |
| PostgreSQL | Private per-event coordinators and per-correlation workflows | Poll `runOnce()` from one or more workers |
| celld | Event-key coordinator cells and instance-key correlation cells | Cell alarms; no PostgreSQL dependency |

PostgreSQL and celld pass the same failure-oriented conformance suite as the
memory engine. Backend persistence is private implementation detail, not a
portable storage interface.

## Application shape

```ts
import { defineAmbientApplication } from "@ewhauser/eve-ambient";
import { celld } from "@ewhauser/eve-ambient/celld";
import {
  createEveGitHubAmbientChannel,
  createEveGitHubAttentionRoute,
} from "@ewhauser/eve-ambient-eve";
import { pullRequestShepherdRule } from "./rules/pull-request-shepherd.js";

const ambient = defineAmbientApplication({
  applicationId: "engineering-agent",
  rules: [pullRequestShepherdRule],
  routes: [createEveGitHubAttentionRoute({ from: githubFrom, auth })],
}).with(celld({ url: env.CELLD_URL, secret: env.CELLD_SECRET }));

export const github = createEveGitHubAmbientChannel({
  publisher: ambient,
  tenantId: context => context.repository.owner,
  credentials,
});
```

Rules and routes are defined once. Bind that same application to `memory()` in
tests, `postgres()` in a database deployment, or `celld()` in a celld
deployment. Each rule retains the event type of its own channel, so one
application can safely publish GitHub, Slack, scheduled, and other channel
events without an application-wide event union. The lower-level protocol and
backend constructors remain available for custom integrations.

The official Eve adapter maps `wakeKey` to Eve's durable admission key. It
targets exactly `eve@0.38.1` and requires consumers to apply the carried patch
for `vercel/eve#1842`.

## Repository

| Workspace | Purpose | Published |
|---|---|---|
| [`packages/ambient`](packages/ambient) | Protocol, rules, publisher, shared workflow reducer, and three backends | `@ewhauser/eve-ambient` |
| [`packages/eve-adapter`](packages/eve-adapter) | Eve GitHub ingress, attention delivery, and direct dispatch | `@ewhauser/eve-ambient-eve` |
| [`examples/eve-postgres`](examples/eve-postgres) | Slack incident rule on PostgreSQL | No |
| [`examples/eve-celld`](examples/eve-celld) | Eve GitHub PR/CI shepherd on celld, with a runnable console demo and no PostgreSQL | No |
| [`integration/eve-conformance`](integration/eve-conformance) | Exact Eve patch and adapter conformance | No |

## Documentation

- [Attention engine protocol](docs/attention-engine.md)
- [Monitoring and rules](docs/monitoring-model.md)
- [PostgreSQL deployment](docs/postgres.md)
- [celld deployment](docs/celld.md)
- [Deployment choices](docs/deployment-options.md)
- [Prefiltered ingress](docs/prefiltered-ingress.md)
- [Operations and security](docs/operations-and-security.md)
- [RFC 0001: full-payload idempotent handoffs](docs/rfcs/0001-full-payload-idempotent-handoffs.md)
- [RFC 0002: durable attention engine](docs/rfcs/0002-durable-attention-engine.md)

## Development

```sh
corepack enable pnpm
pnpm install
pnpm check
```

Run the credential-free Eve GitHub and celld example with:

```sh
pnpm --filter eve-ambient-example-celld demo
```

Set `EVE_AMBIENT_POSTGRES_URL` to run the PostgreSQL conformance suite against a
real database. `pnpm check` also verifies the Eve patch, packed npm artifacts,
celld browser bundle, and a clean consumer install.
