# @ewhauser/eve-ambient

Provider-independent durable attention on Workflow Worlds.

```sh
pnpm add @ewhauser/eve-ambient workflow
```

Define channels, rules, and routes once, then bind production to the World
configured by the Workflow host:

```ts
import { world } from "@ewhauser/eve-ambient/world";

const ambient = application.with(world({
  engineId: "support-agent",
  callbackUrl: "https://agent.example.com",
  callbackSecretEnv: "AMBIENT_CALLBACK_SECRET",
  callbackTimeoutMs: 30_000,
  maxCallbackRequestBytes: 16 * 1024 * 1024,
}));

export const POST = ambient.fetch;
```

`world()` does not accept a database or World object. Workflow hook lookup and
resumption use the host's process-global World, so the host must install its
Postgres, `world-celld`, or composite World before calling Ambient.

The binding provides:

- `engine.accept()` for durable keyed admission;
- `fetch()` for authenticated `/ambient/prepare` and `/ambient/deliver`
  callbacks; and
- autonomous Workflow timers and retries—there is no Ambient poller.

The callback secret value is read from the named environment variable by both
the application handler and durable steps. It is never included in workflow
arguments.

Callback fetches are aborted after `callbackTimeoutMs` and retried through the
durable attention policy. The authenticated handler rejects request bodies
larger than `maxCallbackRequestBytes` before application code runs.

For deterministic tests:

```ts
import { memory } from "@ewhauser/eve-ambient/memory";

const ambient = application.with(memory({ clock }));
await ambient.engine.runDue();
```

The package exposes no public transaction, storage, event lookup, history, or
replay interface. Backend-author protocol types remain available from
`@ewhauser/eve-ambient/protocol`, with identity primitives from
`@ewhauser/eve-ambient/idempotency`.
