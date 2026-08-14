# Eve + PostgreSQL example

This private workspace shows the supported default composition. An Eve route
or receive hook passes its request-scoped `from` function to
`createEvePostgresRuntime`; PostgreSQL owns complete event, batch, and run
values until their terminal outcome or durable handoff.

Apply the Eve patch exactly as documented by
[`@ewhauser/eve-ambient-eve`](../../packages/eve-adapter/README.md), apply the
core SQL migration, call `initialize()`, and then use `publish()` plus
`drain()` from the application's workers.
