# Eve + celld attention engine

This private workspace shows a typed GitHub channel event and ambient rule
running on the celld `AttentionEngine` with no PostgreSQL dependency.

1. Run `pnpm exec eve-ambient init celld ./attention-worker`, configure it, and deploy it.
2. Build the application with `createEveCelldApplication()`.
3. Mount `application.fetch` at `/ambient/prepare` and `/ambient/deliver`.
4. Pass authenticated GitHub deliveries to
   `application.publish(githubChannel, delivery)` and acknowledge only after it resolves.

Rules and routes live in `src/application.ts` and are registered exactly once.
Production binds that definition with `celld()`; the tests bind the same
definition with `memory()`.

Event-coordinator cells freeze fan-out. Correlation cells hold complete branch
and batch values, drive alarms, record prepared outcomes before delivery, and
delete terminal payloads. Their only application calls are authenticated
by-value `prepare` and `deliver` callbacks. There is no PostgreSQL pool, event
repository, payload lookup, history, or replay API.
