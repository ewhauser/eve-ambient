# World deployment

Ambient has one production runtime and many possible Worlds.

| Layer | Ambient owns | World owns |
|---|---|---|
| Protocol | keys, hashes, membership, reducer transitions | durable event storage |
| Scheduling | due timestamps and retry policy | queueing and durable sleeps |
| Stream identity | event and correlation hook tokens | atomic hook ownership and resumption |
| Receipts | semantic admission and append values | persistent output streaming |
| Operations | callback contract | database, Redis, celld, retention, backups |

Channels choose bounded semantic partitions; rules may sub-correlate inside a
partition. Those choices determine World run identity, not physical backend
placement.

The Workflow host must install one process-global World before Ambient uses
`start()`, `getHookByToken()`, or `resumeHook()`. Ambient intentionally does
not accept a World per application: Workflow's external runtime APIs all need
to resolve the same owner and storage namespace.

## Official Postgres World

Use `@workflow/world-postgres` when its Postgres storage, Graphile Worker queue,
and notification streamer fit the deployment. Ambient needs no schema,
migration, pool, advisory lock, or poller of its own.

## world-celld

A `world-celld` implementation replaces the old Ambient-specific celld worker
by implementing the standard World interfaces. Ambient sees only Queue,
Storage, and Streamer semantics. Any celld topology, alarms, Redis acceleration,
or Postgres metadata inside that World remains an infrastructure concern.

## Composite Worlds

A World may combine Redis-like scheduling with Postgres history, or route
internal World responsibilities across services. That composition is below
Ambient's correlation stream boundary. Ambient does not choose storage per
rule or expose a public stream-placement API.

If per-stream placement is eventually needed, it should be a serializable
World routing hint that is covered by conformance—not a return to separate
Ambient Postgres and celld engines.

## Callback endpoint

```ts
const ambient = application.with(world({
  engineId: "engineering-agent",
  callbackUrl: "https://agent.example.com",
  callbackSecretEnv: "AMBIENT_CALLBACK_SECRET",
}));

export const POST = ambient.fetch;
```

Make the same secret value available to the application and Workflow step
runtime. Restrict the callback URL to trusted networks where possible. The
secret value never enters workflow arguments, logs, or receipts.

## Cutover

The old custom Postgres rows and celld cells are not state-compatible with
World histories. Drain or explicitly abandon old in-flight work, configure the
World, deploy callback handling, then switch ingress to `WorldAttentionEngine`.
Retry source deliveries with their original canonical identity.
