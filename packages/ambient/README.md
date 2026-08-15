# @ewhauser/eve-ambient

Provider-independent durable attention with first-class correlation streams.

```sh
pnpm add @ewhauser/eve-ambient
```

Bind an application to any conforming `AttentionWorld`:

```ts
import { world } from "@ewhauser/eve-ambient/world";

const ambient = application.with(world({
  world: createWorldCelld({ url: process.env.WORLD_CELLD_URL }),
  callbackSecretEnv: "AMBIENT_CALLBACK_SECRET",
  maxCallbackRequestBytes: 16 * 1024 * 1024,
}));

export const POST = ambient.fetch;
```

`world.stream(key)` resolves a deterministic local handle. Ambient calls
`append()` once per distinct correlation selected for an event. The World owns
atomic append, a bounded recent-message ring, batching, timers, retries, and
checkpointed delivery.

The binding provides:

- `engine.accept()` for grouped keyed admission; and
- `fetch()` for authenticated `/ambient/prepare` and `/ambient/deliver`
  callbacks.

For deterministic tests:

```ts
import { memory } from "@ewhauser/eve-ambient/memory";

const ambient = application.with(memory({ clock, maxRecentMessages: 48 }));
await ambient.engine.runDue();
```

The package has no Workflow SDK, database driver, Redis client, celld client,
public transaction, event lookup, history, or replay dependency. Backend
authors can import the stream contract and reducer from
`@ewhauser/eve-ambient/protocol`.
