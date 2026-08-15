# Workflow World integration

This private package is the production-substrate conformance fixture for Eve
Ambient's World runtime.

## Covered locally

- deterministic reusable hooks as event and correlation stream addresses;
- owner election under concurrent random workflow starts;
- semantic branch receipts returned through the parent event hook;
- semantic admission receipts emitted on a named World output stream;
- complete branch validation and key/hash conflict behavior;
- concurrent correlation serialization and canonical debounce ordering;
- authenticated, size-bounded application callbacks with durable timeout
  retries;
- prepared-wake checkpointing and exact delivery retry;
- replay-safe key/hash derivation in explicit Workflow steps; and
- append-only retention of payload-bearing Workflow events.

The original small attention-stream tests remain as lower-level probes of hook
transport and World history. The `WorldAttentionEngine` tests exercise the
complete Ambient protocol.

```sh
pnpm --filter eve-ambient-workflow-world-spike test
```

## Postgres World probe

The optional test uses the official `@workflow/world-postgres` storage
implementation against a disposable migrated database:

```sh
WORKFLOW_POSTGRES_URL=postgresql:///eve_ambient_world \
  pnpm --filter eve-ambient-workflow-world-spike exec bootstrap

WORKFLOW_SPIKE_POSTGRES_URL=postgresql:///eve_ambient_world \
  pnpm --filter eve-ambient-workflow-world-spike test
```

It verifies the official World's persistent run/event model. `world-celld`
should run this same World-level suite in its own repository; Ambient requires
no celld-specific integration surface.

## Confirmed constraints

Workflow hook lookup and resumption use the process-global World, so Ambient
does not support a different World object per stream in one process. Composite
storage belongs inside a World implementation.

World history is append-only and can retain full inputs and hook values after
the reducer drops terminal payload state. Physical retention and erasure are
World operational guarantees, not Ambient guarantees.

The workspace patches `builtin-modules@5.0.0` because the current Workflow
step bundler drops its JSON import attribute when externalizing the module for
Node 24. The patch inlines the package's unchanged static list.
