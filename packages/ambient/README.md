# @ewhauser/eve-ambient

Provider-independent durable attention for typed channel events.

```sh
pnpm add @ewhauser/eve-ambient
```

The package exposes one portable persistence command,
`AttentionEngine.accept()`, plus typed channel normalization, ambient rules,
fan-out compilation, and application-owned `prepare`/`deliver` callbacks.
There is no public storage, transaction, event lookup, history, or replay API.

## PostgreSQL

Apply `migrations/001_attention_engine.sql`, then create and poll the engine:

```ts
import {
  createAmbientPublisher,
  createAttentionCallbacks,
} from "@ewhauser/eve-ambient";
import { PostgresAttentionEngine } from "@ewhauser/eve-ambient/postgres";

const callbacks = createAttentionCallbacks({ rules, routes });
const engine = new PostgresAttentionEngine({
  engineId: "engineering-agent",
  pool,
  callbacks,
});

await engine.initialize();

const ambient = createAmbientPublisher({
  applicationId: "engineering-agent",
  engine,
  rules,
});

await ambient.publish(channel, providerEvent);
await engine.runOnce();
```

## celld

```ts
import { CelldAttentionEngine } from "@ewhauser/eve-ambient/celld";

const engine = new CelldAttentionEngine({ url, secret });
```

Deploy the packaged `celld-worker/` directory and mount
`createAttentionCallbackFetchHandler(callbacks, { secret })` at the configured
prepare and deliver URLs. The celld engine has no PostgreSQL dependency.

## Memory

```ts
import { MemoryAttentionEngine } from "@ewhauser/eve-ambient/memory";

const engine = new MemoryAttentionEngine({ callbacks, clock });
await engine.runDue();
```

The memory implementation is the executable protocol reference used by the
shared backend conformance suite.

For concepts, examples, deployment details, and the Eve patch requirement,
see the [repository README](https://github.com/ewhauser/eve-ambient#readme).
