# Eve + PostgreSQL attention engine

This private workspace shows a typed Slack channel event and an ambient rule
running on the PostgreSQL `AttentionEngine`.

1. Apply `packages/ambient/migrations/001_attention_engine.sql`.
2. Build the application with `createEvePostgresApplication()`.
3. Pass authenticated Slack deliveries to `publishSlackMessage()`.
4. Poll `application.runOnce()` from one or more workers.

PostgreSQL privately persists event coordinators, correlation workflows,
prepared outcomes, retry leases, and idempotency receipts. The application has
no event repository, payload lookup, history, or replay API. Complete payloads
are deleted when work becomes terminal; payload-free receipts expire on their
configured horizon.
