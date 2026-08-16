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

## Verified deployment paths

### Vercel World

[Vercel World](https://workflow-sdk.dev/worlds/vercel) is selected automatically
when the Workflow application is deployed to Vercel. It requires no
`WORKFLOW_TARGET_WORLD` setting. Enable Fluid compute as required by the
official setup; Vercel supplies the storage, queues, authentication, and
observability service.

### Postgres World

The [official Postgres World](https://workflow-sdk.dev/worlds/postgres) is the
production self-hosted option for long-lived Node processes. Ambient targets
Workflow 5, so install the package from its `beta` channel; npm `latest` still
tracks Workflow 4.

```sh
pnpm add @ewhauser/eve-ambient@^0.6.0 \
  workflow@5.0.0-beta.42 \
  @workflow/world-postgres@beta

export WORKFLOW_TARGET_WORLD=@workflow/world-postgres
export WORKFLOW_POSTGRES_URL=postgres://user:password@host:5432/database
pnpm dlx --package @workflow/world-postgres@beta bootstrap
```

The application must call `await world.start?.()` during server startup so
Graphile Worker can poll PostgreSQL. Follow the official framework-specific
startup example. The Postgres World is not suitable for a serverless process
that cannot keep the worker running.

### celld World

[`@ewhauser/world-celld`](https://github.com/ewhauser/world-celld) is a published,
open-source Workflow 5 World with an upstream conformance suite and a runnable
demo. Version 0.3.0 is experimental and requires operating a celld fleet.

```sh
pnpm add @ewhauser/eve-ambient@^0.6.0 \
  workflow@5.0.0-beta.42 \
  @ewhauser/world-celld@^0.3.0

export WORKFLOW_TARGET_WORLD=@ewhauser/world-celld
export CELLD_FLEET_URL=http://fleet.internal:8080
export CELLD_WORLD_SECRET=replace-with-a-secret
export WORKFLOW_BASE_URL=https://workflow.example.com
```

Copy and deploy the package's `celld-worker` as described in its README. Every
celld node must reach `WORKFLOW_BASE_URL`, and the fleet's object store must
support celld's conditional-write requirements.

### Other Worlds

The [Workflow Worlds directory](https://workflow-sdk.dev/worlds) lists more
official and community implementations. It is a discovery directory, not an
Ambient compatibility guarantee. Confirm a candidate has a published package,
supports Workflow 5, passes current hook/timer/queue/stream conformance, and
documents a complete production startup path before selecting it.

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

The hook token fingerprints immutable Workflow options. Deploying a changed
callback URL or path, callback secret environment-variable name, retry or lease
setting, ring size, or payload capacity starts a new correlation owner for new
events. Existing state is not transferred. Drain or explicitly abandon the old
owner as part of that configuration cutover. Rotating only the secret value in
the same environment variable does not change ownership.

The removed custom `AttentionWorld` protocol is not state-compatible with this
runtime. Drain or explicitly abandon old correlation work, deploy the Workflow
bundle and callback endpoint, then switch ingress. Retries must preserve their
original canonical source identity.
