# Eve Ambient

Durable ambient attention for Eve agents, built on Workflow Worlds.

Applications define typed channels, correlation rules, and final routes. Eve
Ambient freezes the selected fan-out, serializes each correlation stream,
debounces event storms, checkpoints a decision, and wakes Eve only when
attention is warranted.

```text
provider event
    |
    v
publish -> event-key workflow -----------------------> semantic accept receipt
                |                                            ^
                | full branch                                | append receipt
                v                                            |
      partition + rule workflow -> timer -> prepare -> checkpoint -> deliver
                |
                +---- Queue + Storage + Streamer from the configured World
```

Ambient does not implement Redis, Postgres, or celld persistence. The Workflow
host installs one process-global World: the official Postgres World,
`world-celld`, or another conforming implementation. A composite World can use
different infrastructure internally without changing Ambient or its streams.

## Durable boundary

```ts
interface AttentionEngine {
  accept(fanout: AcceptedFanout): Promise<AttentionAcceptanceReceipt>;
}
```

`accept()` returns only after every frozen branch has produced a semantic
append receipt. A Workflow hook transport acknowledgement alone is not enough.
Lost responses, concurrent starts, partial fan-out, duplicates, and
same-key/different-input conflicts are resolved inside the durable protocol.

Each channel derives a bounded durable partition, such as a Slack thread or
pull request. Each rule then gets one long-lived Workflow run per partition,
with an optional sub-correlation key. A deterministic hook token addresses
that run. It is Ambient's internal first-class stream:
it owns ordered full-value branches, buffer policy, durable timers, retries,
prepared wake bytes, cooldown, and bounded idempotency state. It is deliberately
not a public mutable `Stream` API.

## Application shape

```ts
import { defineAmbientApplication } from "@ewhauser/eve-ambient";
import { world } from "@ewhauser/eve-ambient/world";

const ambient = defineAmbientApplication({
  applicationId: "engineering-agent",
  rules: [pullRequestShepherdRule],
  routes: [eveRoute],
}).with(world({
  engineId: "engineering-agent",
  callbackUrl: "https://agent.example.com",
  callbackSecretEnv: "AMBIENT_CALLBACK_SECRET",
}));

export const POST = ambient.fetch;
await ambient.publish(githubChannel, webhook);
```

The Workflow step runtime and application share the callback secret through
the named environment variable. Only the variable name and callback URL enter
workflow history; the secret value does not. Callback requests default to a
30-second timeout and a 16 MiB body limit; both are configurable on `world()`.

Use `memory()` for deterministic rule tests. It remains the executable reducer
reference, not a production persistence backend.

## Guarantees and retention

- Every durable handoff carries complete canonical values; keys are lineage,
  never payload references.
- Event membership and batch membership are frozen and canonically ordered.
- `prepare()` may repeat, but the exact prepared wake is recorded before
  `deliver()` and reused for delivery retries.
- `wakeKey` is the final idempotency key and must reach the final durable action.
- Reducer state drops terminal payloads after handoff, but Workflow Worlds are
  append-only event logs. Physical payload retention, encryption, and deletion
  are therefore World-level operational policies. Ambient exposes no event
  lookup, history, or replay API.

## Repository

| Workspace | Purpose | Published |
|---|---|---|
| [`packages/ambient`](packages/ambient) | Protocol, rules, reference reducer, and World workflows | `@ewhauser/eve-ambient` |
| [`packages/eve-adapter`](packages/eve-adapter) | Eve GitHub ingress, attention delivery, and direct dispatch | `@ewhauser/eve-ambient-eve` |
| [`examples/world-attention`](examples/world-attention) | One definition bound to memory and a host-supplied World | No |
| [`integration/workflow-world-spike`](integration/workflow-world-spike) | Local and Postgres World integration/conformance | No |
| [`integration/eve-conformance`](integration/eve-conformance) | Exact Eve patch and adapter conformance | No |

## Documentation

- [Attention engine protocol](docs/attention-engine.md)
- [World deployment](docs/deployment-options.md)
- [Monitoring and rules](docs/monitoring-model.md)
- [Operations and security](docs/operations-and-security.md)
- [RFC 0003: Workflow World runtime](docs/rfcs/0003-workflow-world-runtime.md)

## Development

```sh
corepack enable pnpm
pnpm install
pnpm check
```

Set `WORKFLOW_SPIKE_POSTGRES_URL` to include the official Postgres World probe.
The full check also runs the local World integration, Eve conformance, package
artifact validation, and a clean consumer install.
