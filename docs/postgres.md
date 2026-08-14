# PostgreSQL attention engine

The PostgreSQL backend implements the durable attention protocol with private
per-event coordinator rows and per-correlation workflow rows. PostgreSQL is
required only when this backend is selected.

## Install the private schema

Apply `packages/ambient/migrations/001_attention_engine.sql` with the same
migration mechanism used by the application. It creates:

- `eve_ambient_event_coordinators`, keyed by `(engine_id, event_key)`; and
- `eve_ambient_correlation_workflows`, keyed by
  `(engine_id, instance_key)` with a due-work index.

The JSON state contains complete payloads only while work is active. These
tables are not an event repository, audit schema, history API, or replay
surface. They are private to this implementation and may change between major
versions.

## Run the backend

```ts
import { createAttentionCallbacks } from "@ewhauser/eve-ambient";
import { PostgresAttentionEngine } from "@ewhauser/eve-ambient/postgres";

const callbacks = createAttentionCallbacks({ rules, routes });
const engine = new PostgresAttentionEngine({
  engineId: "support-agent",
  pool,
  callbacks,
});

await engine.initialize();
```

`initialize()` verifies that the migration is present; it does not create or
upgrade tables. The publisher calls `accept()`. One or more workers poll due
work:

```ts
setInterval(() => {
  void engine.runOnce({ limit: 100 });
}, 250);
```

Production workers should use their normal scheduler, cancellation, error
reporting, and backoff rather than an unobserved interval.

## Transaction and callback boundary

Advisory transaction locks serialize one event coordinator or correlation
workflow. Transactions validate and checkpoint state; they do not surround
application callbacks.

The worker sequence is:

1. claim and commit a run lease;
2. call `prepare()` outside the transaction;
3. validate and commit the prepared result;
4. call `deliver()` outside the transaction; and
5. validate and commit its receipt.

This boundary is why the PostgreSQL implementation needs both migrations and a
state machine. A process may stop between any two steps. The state records
which exact operation is safe to retry while avoiding a database transaction
across network or model work.

Database checkpoint failures are not counted as rule or delivery failures.
They abort the transaction and leave the last committed workflow state for the
next worker.

## Scaling and retention

Different event and instance keys proceed independently. Multiple workers may
poll the due index; per-key advisory locks and run leases prevent concurrent
state transitions. Hot correlation keys remain serialized by design.

Completed event coordinators and branch/delivery receipts expire after
`dedupeMs`. Terminal workflow cleanup deletes complete event and prepared-wake
payloads. `diagnostics()` returns aggregate payload-free counts for tests and
operations; it cannot retrieve event values.

Set `EVE_AMBIENT_POSTGRES_URL` when running the package conformance suite to
exercise this backend against a real PostgreSQL database.
