# Eve Ambient

Durable, correlation-owned attention for Eve agents.

Applications define typed channels, rules, and final delivery routes. Ambient
turns each selected source event into one atomic append per distinct
correlation stream. A production World such as `world-celld` owns those
streams, their timers, and their durable state.

```text
canonical event + selected branches
              |
              | group by correlation address
              v
       +------+------------------+
       |                         |
       v                         v
world.stream(A).append(...)   world.stream(B).append(...)
       |                         |
       +-> buffer / dedup         +-> buffer / dedup
           freeze batch               freeze batch
           prepare                    prepare
           checkpoint wake            checkpoint wake
           deliver                    deliver
```

There is no event coordinator, fan-out workflow, database adapter, storage
lookup, or global stream registry. `world.stream(key)` constructs a local
handle. `append()` is the only admission RPC.

## Protocol

```ts
interface AttentionWorld {
  stream(key: AttentionInstanceKey): AttentionStream;
}

interface AttentionStream {
  append(input: AttentionStreamAppend): Promise<AttentionStreamAppendReceipt>;
}
```

For one inbound event, RPC fanout equals the number of distinct correlation
addresses selected by the application:

```text
0 selected correlations -> 0 append RPCs
1 selected correlation  -> 1 append RPC
N selected correlations -> N concurrent append RPCs
```

Each stream keeps a bounded recent-message ring (48 entries by default in the
reference engine) for best-effort duplicate suppression. A retry resends all
appends; streams that still remember the event return `duplicate`. The final
delivery boundary remains strongly idempotent through `wakeKey`.

## Application shape

```ts
import { world } from "@ewhauser/eve-ambient/world";

const ambient = application.with(world({
  world: createWorldCelld({ url: process.env.WORLD_CELLD_URL }),
  callbackSecretEnv: "AMBIENT_CALLBACK_SECRET",
}));

export const POST = ambient.fetch;
await ambient.publish(githubChannel, webhook);
```

The World implementation calls the authenticated `/ambient/prepare` and
`/ambient/deliver` endpoints with complete values. `prepare()` may repeat. Its
exact wake is checkpointed before `deliver()`, and delivery retries reuse the
same bytes and `wakeKey`.

Use `memory()` for deterministic tests. It implements the same World contract
and pure stream reducer, but is not a production persistence backend.

## Repository

| Workspace | Purpose | Published |
|---|---|---|
| [`packages/ambient`](packages/ambient) | Rules, protocol, reducer, memory reference, and World adapter | `@ewhauser/eve-ambient` |
| [`packages/eve-adapter`](packages/eve-adapter) | Eve ingress, attention delivery, and direct dispatch | `@ewhauser/eve-ambient-eve` |
| [`examples/world-attention`](examples/world-attention) | One definition bound to memory or a supplied World | No |
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
