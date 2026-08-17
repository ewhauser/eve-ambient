# @ewhauser/eve-ambient

Provider-independent durable attention with one standard Workflow run per
correlation.

```sh
pnpm add @ewhauser/eve-ambient@^0.6.0 workflow@5.0.0-beta.42
```

Bind an application to Workflow:

```ts
import { workflow } from "@ewhauser/eve-ambient/workflow";

const ambient = application.with(workflow({
  callbackUrl: "https://agent.example.com",
  callbackSecretEnv: "AMBIENT_CALLBACK_SECRET",
  maxCallbackRequestBytes: 16 * 1024 * 1024,
}));

export const POST = ambient.fetch;
```

Re-export the packaged workflow so the consumer's Workflow compiler discovers
it:

```ts
// workflows/ambient.ts
export * from "@ewhauser/eve-ambient/workflows";
```

The binding provides:

- `engine.accept()` for grouped correlation admission; and
- `fetch()` for authenticated `/ambient/prepare` and `/ambient/deliver`
  callbacks.

Workflow admission shares resolved hook-owner handles across engine instances
in a process-local 1,024-entry LRU with a 10-minute idle TTL. A missing or
inactive cached owner is evicted, and the unchanged append safely retries by
token before cold initialization. This is an advisory optimization over the
standard Workflow API and requires no additional infrastructure.

Workflow selects its standard World. Vercel uses the managed World
automatically. For Workflow 5 self-hosting, use the official Postgres package's
`beta` channel or published `@ewhauser/world-celld@^0.3.0`; both require the
startup and infrastructure documented by their linked deployment guides in the
[repository README](https://github.com/ewhauser/eve-ambient#choose-the-workflow-world).

For deterministic tests:

```ts
import { memory } from "@ewhauser/eve-ambient/memory";

const ambient = application.with(memory({ clock, maxRecentMessages: 48 }));
await ambient.engine.runDue();
```

Each correlation Workflow keeps a 48-entry recent-message ring and caps applied
full-value reducer state at 1,000 pending branches and 16 MiB by default. It
stops consuming the durable hook while at capacity, leaving overflow queued in
Workflow until due work drains state. It does not rotate automatically, so the
Workflow event history continues to grow while that correlation remains active.

Immutable Workflow options are fingerprinted into correlation ownership.
Changing them starts a new owner for new events and does not migrate the old
owner's reducer state. Final effects must deduplicate the stable `wakeKey`
because prepare and delivery are at-least-once.
