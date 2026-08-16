# @ewhauser/eve-ambient

Provider-independent durable attention with one standard Workflow run per
correlation.

```sh
pnpm add @ewhauser/eve-ambient workflow@5.0.0-beta.42
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

Workflow selects its standard World. Vercel uses the managed World
automatically; self-hosted applications can set `WORKFLOW_TARGET_WORLD` to
`@workflow/world-postgres`, `@ewhauser/world-celld`, or another conforming
implementation.

For deterministic tests:

```ts
import { memory } from "@ewhauser/eve-ambient/memory";

const ambient = application.with(memory({ clock, maxRecentMessages: 48 }));
await ambient.engine.runDue();
```

Each correlation Workflow keeps bounded reducer state and a 48-entry recent
message ring. It does not rotate automatically, so the Workflow event history
continues to grow while that correlation remains active. Final effects must
deduplicate the stable `wakeKey` because prepare and delivery are at-least-once.
