# celld mailbox backend

> **Experimental:** celld is alpha software and this mailbox tier has explicit
> production gates and limitations. Use the Postgres store mailbox unless a
> measured per-key serialization or due-scan bottleneck justifies the added
> system.

The correlation mailbox—the per-key buffer that accumulates post-filter events
and decides when a batch is due—can run in
[celld](https://github.com/denoland/celld) cells instead of PostgreSQL instance
rows.

One cell owns each correlation instance. It holds the same
`StoredMonitorInstance` record with complete event envelopes and runs the same
lifecycle statechart, with a durable cell alarm replacing the PostgreSQL
`nextEvaluationAt` due scan. PostgreSQL remains the system of record for
payload-free ingress receipts, runs, decisions, dead letters, budgets, and
audit. The accepted branch owns the complete event until celld returns its
durable append receipt; celld evaluation never depends on a central payload
copy. See [Persistence responsibilities](storage-responsibilities.md) for the
interface and atomicity boundaries on each side of that handoff.

## Architecture

```text
channels
    |  publish()
    v
ingress pipeline                    schema, dedupe, ingress sequence, filter,
    |                               correlate, loop prevention, event budgets
    |  append {branchKey, inputHash, event, bytes, seq, config}
    v
celld cells                         full events, statechart, buffer, cooldown,
    |                               alarms; no model credentials
    |  alarm -> evaluation {runId, complete batch, instanceView}
    v
runtime evaluator                   decision, budgets,
    |                               evidence, route, delivery, run record
    |  terminal result or durable retryAt
    v
delivery channels                   idempotency, bindings, coalescing
```

Filtering does not run in celld. Only post-filter complete event envelopes
leave the runtime. Cells persist those envelopes in open, sealed, and claimed
batches, then send the complete claimed batch to the evaluator. No event
repository or payload lookup participates in the handoff.

Appends are idempotent by durable `branchKey` and `inputHash`, including the
case where a cell commits but its HTTP response is lost. The cell validates the
event and branch identity chain and rejects key reuse with different full input.
Runs are written in the same format
in both mailbox tiers, so `listRuns()` and `listDeadLetters()` keep the same
behavior. An idle cell arms cleanup for the monitor's decision
retention expiry and then removes its instance record and expired append
receipts durably.

## When to choose it

Choose celld when the correlation mailbox is the measured bottleneck:

- many concurrent correlation keys exceed the practical store-mailbox
  throughput;
- the PostgreSQL due scan cannot meet evaluation-latency requirements; or
- advisory-lock and mailbox traffic consume too much of the database connection
  pool.

Cells scale with nodes rather than one database, timers are native instead of
swept, and idle keys cost bucket storage instead of resident process memory.

Stay on the store tier otherwise. It has lower operational complexity, is the
small-deployment answer, and remains the default conformance target. Both tiers
execute the same `dispatchLifecycle` statechart.

Mailbox ownership is durable. Changing an existing application between
`store` and `celld` requires an explicit state-migration procedure and is
rejected by `initialize()` today.

## Setup

### 1. Deploy the worker

The package ships a worker under `celld-worker/`; see the
[worker deployment guide](../packages/ambient/celld-worker/README.md). It carries no monitor
configuration. Cells learn their configuration from the first append, so one
deployment can serve every monitor.

```sh
cp -r node_modules/@ewhauser/eve-ambient/celld-worker ./mailbox
CELLD_ESBUILD=/path/to/esbuild node ./mailbox/build.mjs
celld deploy --config ./mailbox/wrangler.jsonc
```

The copied `index.ts` re-exports `@ewhauser/eve-ambient/celld-worker`, resolving
through the application's `node_modules` so the worker stays in step with the
installed package version.

### 2. Mount the evaluator

Mount an authenticated route that the fleet can reach:

```ts
import { createEvaluationFetchHandler } from "@ewhauser/eve-ambient/celld";

const evaluate = createEvaluationFetchHandler(runtime, {
  secret: process.env.MAILBOX_SECRET!,
  path: "/monitor-evaluations",
});
```

`handleEvaluation(request)` is available directly when the host is not
fetch-shaped; it accepts and returns plain objects.

The evaluator authenticates the request, validates and freezes the complete
batch, reserves budgets, runs the normal decision and delivery pipeline,
records the run, and returns either a terminal outcome or a durable future
`retryAt`.

### 3. Select the mailbox

```ts
const runtime = new MonitorRuntime({
  // ...the normal runtime options
  mailbox: {
    mode: "celld",
    fleetUrl: "http://fleet.internal:8787",
    evaluatorUrl: "https://app.internal/monitor-evaluations",
    secret: process.env.MAILBOX_SECRET!,
  },
});
```

Keep calling `drain()`. Ingress, filtering, correlation, and appends still run
there. The runtime stops sweeping due instances and due runs because claiming
them would race the cells.

`EVALUATOR_SECRET` is mandatory on the worker. When absent, every `/cells`
route fails closed with `503` instead of forwarding unauthenticated traffic.

Three fleet capacity settings are also mandatory:

| Setting | Meaning |
|---|---|
| `MAILBOX_MAX_EVENT_BYTES` | Maximum serialized full `BufferedEvent` envelope. |
| `MAILBOX_MAX_BATCH_BYTES` | Maximum serialized provisional or claimed batch; must be at least the event limit. |
| `MAILBOX_MAX_RESIDENT_BYTES` | Maximum serialized open, sealed, and claimed batches plus append receipts in one cell; must be at least the batch limit. |

Missing or inconsistent limits fail closed with `503`. A full event or batch
that can never fit returns `413` and is terminal for that branch. Resident
pressure returns `429`; the runtime retains the full branch payload and retries
with backoff. Limits never trigger a reference-only fallback.

## Tuning

| Setting | Why |
|---|---|
| `CELLD_TTL_MS=5000` | Owner takeover costs a lease TTL. Measured p95 was 9.7s, 4.7s, and 2.9s at TTL 10s, 5s, and 3s; 5s was the observed knee. |
| `CELLD_WAKER_TICK_MS=5000` | Controls orphaned-alarm adoption: 8.8s at 5s versus 56s at the 60s default in the validation environment. |
| Stable node identities | A node restarting with the same ID resumed in about 740ms. A new ID is a node loss and costs a full TTL. |
| Ingress key affinity | Churn with round-robin routing measured about 2.5 times the S3 operations of an affinity-routed warm fleet. |
| `CELLD_LTX_COMPACTION` | Without compaction, the RFC rate cap projects roughly 100 million segment objects per month. Treat it as a requirement. |
| Internal-listener firewall | celld's internal listener exposes unauthenticated `/shutdown` and `/evict`. The worker authenticates public `/cells` routes, not celld's internal listener. |

These observations came from the conformance and chaos environment used during
the celld evaluation. Re-measure them in the target infrastructure rather than
treating them as universal service levels.

## Limitations

- **Per-key correlation cardinality is not enforced.** The store tier caps
  active keys per tenant with `countInstances` under a tenant-wide lock. celld
  has no instance table to count, so that cap is inactive. Event, model-call,
  model-token, and wake budgets still apply. Cap keys upstream or size the fleet
  for unbounded key growth.
- **Mailbox and definition state do not migrate automatically.** Switching
  mailbox tiers, moving monitor IDs, removing monitors destructively, and
  applying `compatibleWith` state moves are rejected. Definition versions used
  by cells must remain available as inactive versions until the fleet is
  explicitly migrated or discarded.
- **An alarm is abandoned after six counted handler failures.** The cell keeps
  its buffered events and instance record but has no timer. Use
  `POST /cells/<instanceKey>/rearm` to derive and arm the due time again, and
  alert on overdue cells without `pendingAlarm`. Normal evaluator backoff
  returns `retryAt` and does not spend this failure ladder.
- **Deploys restart the fleet.** celld's staged rollout is not exposed. Cells
  resume from durable storage, but changing the worker does not rewrite
  configuration already pinned into existing cells.
- **The celld#144 workaround is active.** Alarm handlers can overlap
  ([celld#144](https://github.com/denoland/celld/issues/144)), so the worker
  wraps evaluation in `blockConcurrencyWhile`. Appends queue behind an
  in-flight evaluation until the workaround can be removed.
- **Sink idempotency is mandatory.** Delivery is at-least-once. A node lost
  mid-delivery can create a duplicate with the same `monitor:<runId>:0` key;
  the delivery adapter must deduplicate it.
- **celld is alpha and this tier is experimental.** Production adoption is
  gated on target-infrastructure latency measurements, formal acceptance or
  removal of the celld#144 workaround, a governance strategy for the
  dependency, and a measured full-payload throughput ceiling. Rerun capacity
  measurements with representative event distributions and evaluator-outage
  backlogs; the in-process regression suite proves the limit semantics, not
  target-fleet LTX or object-store costs.

## External event logs

celld is a mailbox, not an event log. Kafka or another durable log can own the
raw ingress stream independently, but the package does not ship a first-class
Kafka adapter today. A consumer using `publish()` may commit after `accepted`
or `duplicate` because PostgreSQL has durably recorded the subscription and
will retry the cell append. An adapter that bypasses that acceptance boundary
is not conformant. A custom external-ingress adapter
must carry the complete event and wait for the next durable full-payload
handoff before acknowledging its own input.

See [Deployment options](deployment-options.md) for the combined full-value
topology and [Operations and security](operations-and-security.md) for the
trust and delivery boundaries.
