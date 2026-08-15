# World deployment

Ambient accepts one correlation-addressed World client. It does not know which
database, queue, or runtime is behind that client.

| Ambient owns | World implementation owns |
|---|---|
| canonical events, rules, keys, hashes | deterministic stream addressing |
| grouping branches by correlation | atomic append and recent-message ring |
| prepare and deliver callback contract | durable state, timers, leases, retries |
| exact final `wakeKey` | encryption, backups, retention, erasure |

```ts
const ambient = application.with(world({
  world: createWorldCelld({ url: process.env.WORLD_CELLD_URL }),
  callbackSecretEnv: "AMBIENT_CALLBACK_SECRET",
}));
```

`createWorldCelld()` is illustrative: `world-celld` is developed separately.
Any implementation is conforming if `stream(key)` constructs a local handle
and `append(input)` durably and atomically applies the exported stream reducer.

A World can use celld, Postgres, Redis, or a combination internally. That
composition remains below the stream contract; Ambient has no backend selector
per rule and ships no custom storage adapter.

## Callback endpoint

The remote World needs the application callback base URL and the same bearer
secret named by `callbackSecretEnv`. Keep the endpoint on a trusted network
where possible. Rotate the secret in both deployments together.

## Cutover

The removed custom backends and the abandoned Workflow-run spike are not state
compatible with correlation World cells. Drain or explicitly abandon old work,
deploy the new World and callback endpoint, then switch ingress. Retries must
preserve their original canonical source identity.
