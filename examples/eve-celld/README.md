# Eve + celld attention engine

This private workspace shows a typed GitHub channel event and ambient rule
running on the celld `AttentionEngine` with no PostgreSQL dependency.

1. Deploy the packaged worker with the configuration in
   `packages/ambient/celld-worker/`.
2. Build the application with `createEveCelldApplication()`.
3. Mount `application.handleCallbacks` at `/ambient/prepare` and
   `/ambient/deliver`.
4. Pass authenticated GitHub deliveries to `publishPullRequest()`.

Event-coordinator cells freeze fan-out. Correlation cells hold complete branch
and batch values, drive alarms, record prepared outcomes before delivery, and
delete terminal payloads. Their only application calls are authenticated
by-value `prepare` and `deliver` callbacks. There is no PostgreSQL pool, event
repository, payload lookup, history, or replay API.
