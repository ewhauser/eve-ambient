# Ambient on a correlation World

This workspace contains two typechecked application shapes:

- `application.ts` defines a small provider-independent support rule and
  exercises it with the in-memory reference engine;
- `github-pr-shepherd.ts` listens to Eve's typed GitHub PR and check-suite
  events, debounces them per pull request, and invokes one idempotent Eve turn
  for a current CI failure.

Both bind the production runtime with `world()`.

```ts
const application = createSupportWorldApplication({
  world: createWorldCelld({ url: process.env.WORLD_CELLD_URL }),
  callbackSecretEnv: "AMBIENT_CALLBACK_SECRET",
  deliver: async (target, instruction) => dispatchAgent(target, instruction),
});

export const POST = application.fetch;
```

The supplied client only needs `world.stream(key).append(input)`. Resolving the
stream handle is local; `append` is the single admission RPC. `world-celld` or
another implementation owns the durable stream state and timers.

The same `AMBIENT_CALLBACK_SECRET` value must be available to the World runtime
and the application. The secret authenticates prepare and delivery callbacks.
