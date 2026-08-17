# Operations and security

## Durability

- `accept()` returns after Workflow transport accepts every selected
  correlation append as part of a bounded `append-many` command. Each original
  call retains an individual promise and receipt; all entries affected by a
  failed publication reject.
- Distinct correlation tokens flush and resume independently. They are never
  combined.
- Same-token arrivals in one process wait for a fixed 5 ms timer window. This
  is the nominal added latency for a lone event; event-loop load may delay the
  timer. A 2 ms window split a 20-event cold burst under CI and full-suite load;
  repeated standalone and full-check integration runs at 5 ms each produced one
  warm resume and one seeded cold start.
- Cold initialization is coalesced by hook token within one process when the
  registration timeout and local backpressure settings match. Other lanes or
  processes may still start candidates that resolve through hook ownership.
- Resolved hook handles are cached process-locally in a 1,024-entry LRU with a
  10-minute idle TTL. A missing cached owner is evicted and the same bounded
  batch retries through its deterministic token.
- Reducer deduplication and idempotency conflicts are asynchronous to that
  receipt.
- Prepared output is checkpointed before delivery and retried with the same
  bytes and `wakeKey`.
- Source-admission dedup is intentionally best effort and bounded by the
  recent-message ring.

Monitor observable `accept()` and hook-resume latency and errors, especially
retryable `WorkflowAdmissionBackpressureError`, plus cold-start frequency,
registration failures, candidate-owner conflicts, active correlation runs,
event-history growth, due timer lag, callback latency and status, retry
exhaustion, and final delivery conflicts. Ambient does not currently export
internal batch-size, split, or queue-age metrics. Workflow and the selected
World provide run and storage signals; application callback and turn-queue
signals remain application-owned.

## Permanent-run capacity

Ambient does not rotate a live correlation run. Reducer payload state remains
bounded by the configured pending-branch and pending-byte limits; hook overflow
remains durably queued until due work releases capacity. The Workflow event log
still grows with every resume, timer, and step. Choose correlation keys that
bound traffic, alert before the selected World's per-run event and queue
ceilings, and explicitly abandon or drain correlations that are no longer
needed.

Publisher batches default to 64 commands and 16 MiB of canonical serialized
bytes. Chunk construction additionally caps aggregate branches and branch
bytes at the reducer limits, so a large process-local burst cannot bypass
`maxPendingBranches` or `maxPendingBytes`. The Workflow applies entries
sequentially. If capacity fills mid-command, only that bounded remainder stays
live and later commands remain in the World's hook queue. If a chunk
publication fails, that chunk and all later unsent chunks in the same flush
reject; retry them with their original stable identities.

The separate process-local backlog defaults to 1,000 accepted appends and 64
MiB of canonical append bytes per operational lane, including queued and
currently publishing work. Exceeding either cap produces retryable
`WorkflowAdmissionBackpressureError` instead of retaining an unbounded payload
behind a stalled publication. Registration timeout and these local caps define
an operational lane but do not change the hook token. Reducer and batch limits
are immutable Workflow configuration and do change the token fingerprint;
plan changes to those values as a drain-and-cutover rather than in-place
tuning.

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
