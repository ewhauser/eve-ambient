# Ambient on a Workflow World

This example defines a support rule once, exercises it with the in-memory
reference engine, and binds the production runtime with `world()`.

```ts
const application = createSupportWorldApplication({
  callbackUrl: "https://agent.example.com",
  callbackSecretEnv: "AMBIENT_CALLBACK_SECRET",
  deliver: async (target, instruction) => dispatchAgent(target, instruction),
});

export const POST = application.fetch;
```

The Workflow host installs one process-global World. That can be the official
Postgres World, `world-celld`, or any other conforming implementation. Ambient
does not receive a database, Redis client, or celld client and does not select
storage per stream.

The same `AMBIENT_CALLBACK_SECRET` value must be available to the Workflow step
runtime and the application. Only its environment-variable name is persisted
in workflow inputs.
