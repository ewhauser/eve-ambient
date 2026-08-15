# RFC 0003: Workflow World Runtime

- Status: Superseded by [RFC 0004](0004-correlation-world-protocol.md)
- Implementation: Removed
- Scope: Replace Ambient-specific Postgres and celld runtimes with one
  Workflow World implementation
- Preserves: RFC 0001 lineage and full-value custody; RFC 0002 attention
  protocol, reducer, membership freeze, checkpoint-before-delivery, and final
  idempotency boundary
- Supersedes: RFC 0002 backend, scheduling, callback, diagnostics, retention,
  migration, and deployment sections

## Decision

> Historical: the run, hook, step, sleep, and event-coordinator machinery below
> was implemented in the spike and then removed. Measured fanout was too high
> for the correlation protocol Ambient actually needs. RFC 0004 is current.

Eve Ambient builds its production machinery on the Workflow SDK and the
process-global World configured by the host. It does not ship custom Postgres
tables/pollers or a custom celld worker.

The World supplies Queue, Storage, and Streamer semantics. Infrastructure
implementations may use Postgres, Redis, celld, or a combination. Ambient does
not choose those components or expose storage placement as application policy.

The in-memory engine remains as an executable reducer reference for fast,
deterministic tests.

## Runtime mapping

```text
AcceptedFanout
  -> deterministic event hook / event coordinator run
     -> branch submit step
        -> deterministic partitioned-correlation hook / correlation run
           -> semantic branch receipt back to event hook
     -> semantic admission receipt on World output stream

correlation run
  -> append reducer state
  -> durable sleep
  -> prepare HTTP step
  -> checkpoint prepared value in run history
  -> deliver HTTP step
  -> retry/cooldown/retention
```

Random `start()` run IDs do not define ownership. A deterministic reusable hook
token elects one active run for each event key and correlation instance. The
correlation address combines the channel-owned partition cell, rule identity,
definition version, and optional rule-level sub-correlation. A
newly registered owner is explicitly resumed as well as given its initial
argument because a conforming World may expose hook registration before it
schedules the continuation. Duplicate commands are safe at the reducer layer.

`resumeHook()` confirms durable transport, not Ambient processing. The event
coordinator marks a branch accepted only when the correlation run replies with
a semantic append receipt. `AttentionEngine.accept()` similarly waits for a
named World output-stream receipt for its exact attempt ID.

## Callback boundary

Application rules and routes are functions and cannot enter durable workflow
arguments. `world()` exposes an authenticated application `fetch` handler.
Steps send complete frozen batches and prepared wakes by value.

The serialized configuration carries `callbackSecretEnv`, the name of an
environment variable. The secret value is read at step and handler execution
time and is never persisted in World input, hook payload, output receipt, or
metadata.

Callback fetch duration and request bytes are bounded. Timeouts are transient
execution failures handled by the durable retry policy; oversized or malformed
callback requests fail terminally before application code runs.

## Retention correction

The pure reducer deletes terminal payloads from its live state. Workflow run
inputs, hook payloads, and step values are append-only World history and may
remain after reducer cleanup. Therefore RFC 0002's physical payload-deletion
claims do not apply to the World implementation.

Ambient continues to expose no event query, history, or replay API. Physical
retention, encryption, backup, erasure, and key-destruction guarantees belong
to the selected World and must be verified operationally.

## Conformance

Adoption requires:

1. the existing failure-oriented reducer suite to remain green;
2. local World tests for hook ownership, semantic receipts, concurrency,
   debounce, callback delivery, and exact-wake retry;
3. official Postgres World startup and event-log retention probes;
4. packed-package and clean-consumer verification; and
5. no Ambient Postgres migration, pool, celld client, worker, or poller in the
   published artifact.

`world-celld` is conformant when it satisfies the standard World contract and
the same integration suite. It does not need an Ambient-specific API.

## Consequences

- Backend combinations are possible below the World interface without a new
  Ambient stream abstraction.
- Workflow runtime/versioning and World operations become required production
  dependencies.
- Ambient loses backend-specific diagnostics and relies on World observability.
- Existing custom-backend in-flight state cannot be migrated automatically;
  deployments must drain or abandon it during cutover.
- World event-log retention is usually broader than the old reducer-state
  retention and must be treated as a deliberate security choice.
