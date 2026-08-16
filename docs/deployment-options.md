# Workflow World deployment

Ambient runs on the standard World selected by Workflow. It does not ship a
Postgres client, a celld client, or a custom backend interface.

| Ambient owns | Workflow and its World own |
|---|---|
| canonical events, rules, correlation keys, hashes | runs, hooks, event storage, queues, streams |
| bounded correlation reducer and durable timers | execution, replay, retries, encryption |
| prepare and deliver callback contract | deployment routing, retention, observability |
| exact final `wakeKey` | backend durability, backups, and erasure |

## Application binding

```ts
import { workflow } from "@ewhauser/eve-ambient/workflow";

const ambient = application.with(workflow({
  callbackUrl: "https://agent.example.com",
  callbackSecretEnv: "AMBIENT_CALLBACK_SECRET",
}));
```

The consumer must also configure Workflow for its framework and make the
packaged workflow discoverable:

```ts
// workflows/ambient.ts
export * from "@ewhauser/eve-ambient/workflows";
```

Without that re-export, `start()` cannot resolve the packaged workflow after a
cold start.

## Selecting a World

Vercel deployments use the managed Vercel World automatically. Outside Vercel,
set `WORKFLOW_TARGET_WORLD` when selecting a self-hosted or community World:

```sh
WORKFLOW_TARGET_WORLD=@workflow/world-postgres
# or
WORKFLOW_TARGET_WORLD=@ewhauser/world-celld
```

Useful starting points:

- [Vercel World](https://workflow-sdk.dev/worlds/vercel)
- [official Postgres World](https://workflow-sdk.dev/worlds/postgres)
- [`world-celld`](https://github.com/ewhauser/world-celld)
- [community Workflow Worlds](https://workflow-sdk.dev/worlds)

World composition remains below Workflow's standard interface. A World may use
Postgres, Redis, celld, or multiple services internally; Ambient does not select
storage per rule.

## Callback endpoint

The Workflow steps need the application's public callback base URL and the same
bearer secret named by `callbackSecretEnv`. Keep the endpoint on a trusted
network where possible. Rotate the secret in the step and application
environments together.

## Lifecycle and cutover

One Workflow run owns each active correlation. The run does not rotate, even
after its 48-entry recent-message ring wraps. This avoids the unsafe gap between
disposing one hook owner and registering another, but means run history grows
with correlation traffic.

The removed custom `AttentionWorld` protocol is not state-compatible with this
runtime. Drain or explicitly abandon old correlation work, deploy the Workflow
bundle and callback endpoint, then switch ingress. Retries must preserve their
original canonical source identity.
