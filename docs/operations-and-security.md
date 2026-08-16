# Operations and security

## Durability

- `accept()` returns after Workflow transport accepts every selected
  correlation append.
- Distinct correlation resumes run concurrently.
- Reducer deduplication and idempotency conflicts are asynchronous to that
  receipt.
- Prepared output is checkpointed before delivery and retried with the same
  bytes and `wakeKey`.
- Source-admission dedup is intentionally best effort and bounded by the
  recent-message ring.

Monitor hook-resume latency and errors, cold-start frequency, candidate-owner
conflicts, active correlation runs, event-history growth, due timer lag,
callback latency and status, retry exhaustion, ring capacity, and final
delivery conflicts. Workflow and the selected World provide the run and storage
signals; application callback and turn-queue signals remain application-owned.

## Permanent-run capacity

Ambient does not rotate a live correlation run. Reducer payload state remains
bounded by the configured pending-branch and pending-byte limits; hook overflow
remains durably queued until due work releases capacity. The Workflow event log
still grows with every resume, timer, and step. Choose correlation keys that
bound traffic, alert before the selected World's per-run event and queue
ceilings, and explicitly abandon or drain correlations that are no longer
needed.

## Secrets

`callbackSecretEnv` is an environment-variable name. The application handler
and Workflow step read the value at request time, and the handler compares
bearer tokens in constant time. Do not put the secret in append payloads,
decisions, evidence, receipts, or logs.

The callback URL, environment-variable name, and reducer inputs are recorded as
Workflow data. Use Workflow encryption where supported and do not place secret
values in `callbackUrl`.

## Payload retention

The reducer removes terminal event and wake payloads from live state. The World
still retains Workflow events, encrypted step inputs and outputs, logs, backups,
or transport bodies according to its own policy. Configure encryption,
retention, erasure, and administrative access there. Ambient exposes no
application event-history or replay API.

## Definition rollout

Rule ID, version, mode, policy, and correlation participate in run identity or
policy conflict checks. Workflow runtime options are fingerprinted into hook
ownership; changing one cuts new events over to a fresh owner without migrating
old reducer state. Treat released definitions and runtime configurations as
immutable, planned rollouts. Workflow IDs for packaged code include the package
version, and running work depends on the deployment that compiled it; keep
pinned deployments available according to the selected World's guarantees.

## Final action

Delivery is idempotent rather than exactly once. Every route must propagate
`wakeKey` to the final durable system and return the original receipt for a
matching retry. Source actors are provenance, not delegated authority; routes
must enforce tenant and loop boundaries with their configured principal.
