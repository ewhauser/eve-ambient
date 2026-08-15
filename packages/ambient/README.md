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

Apply `migrations/001_attention_engine.sql`, then bind and poll the application:

```ts
import {
  defineAmbientApplication,
} from "@ewhauser/eve-ambient";
import { postgres } from "@ewhauser/eve-ambient/postgres";

const ambient = defineAmbientApplication({
  applicationId: "engineering-agent",
  rules,
  routes,
}).with(postgres({
  engineId: "engineering-agent",
  pool,
}));

await ambient.engine.initialize();
await ambient.publish(channel, providerEvent);
await ambient.engine.runOnce();
```

## celld

```ts
import { celld } from "@ewhauser/eve-ambient/celld";

const ambient = application.with(celld({ url, secret }));
export const POST = ambient.fetch;
```

Run `eve-ambient init celld` to create the packaged worker configuration. Its
single callback base URL reaches the application-owned `ambient.fetch`
handler. The celld engine places one custody cell per channel-defined bounded
partition, not per event, and has no PostgreSQL dependency.

## Memory

```ts
import { memory } from "@ewhauser/eve-ambient/memory";

const ambient = application.with(memory({ clock }));
await ambient.engine.runDue();
```

The memory implementation is the executable protocol reference used by the
shared backend conformance suite.

Most consumers should stay on the root application API. Backend-author wire
types and compilers are also available from `@ewhauser/eve-ambient/protocol`;
key derivation primitives are available from `@ewhauser/eve-ambient/idempotency`.

For concepts, examples, deployment details, and the Eve patch requirement,
see the [repository README](https://github.com/ewhauser/eve-ambient#readme).
