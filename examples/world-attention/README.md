# Ambient on a correlation World

This example defines a support rule once, exercises it with the in-memory
reference engine, and binds the production runtime with `world()`.

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
