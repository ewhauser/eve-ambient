# Operations and security

## Durability

- `accept()` returns only after every selected correlation stream returns a
  semantic append receipt.
- Independent stream appends run concurrently and all are allowed to settle.
- A retry resends the same groups; each receiver handles duplicates locally.
- Prepared output is checkpointed before delivery and retried with the same
  bytes and `wakeKey`.
- Source-admission dedup is intentionally best effort and bounded by the
  recent-message ring.

Monitor append latency and errors, active stream count, due timer lag, callback
latency and status, retry exhaustion, ring capacity, state bytes, and final
delivery conflicts. These signals belong to the selected World implementation.

## Secrets

`callbackSecretEnv` is an environment-variable name. The application handler
reads the value at request time and compares bearer tokens in constant time.
The remote World must receive the value through its own secret configuration;
do not put it in append payloads, decisions, evidence, receipts, or logs.

## Payload retention

The reducer removes terminal event and wake payloads from live stream state.
The World may still retain snapshots, logs, backups, or transport bodies.
Configure encryption, retention, erasure, and administrative access there.
Ambient exposes no event-history or replay API.

## Definition rollout

Rule ID, version, mode, policy, and correlation participate in stream identity
or policy conflict checks. Treat released definitions as immutable and keep
their callback code available until their streams drain.

## Final action

Delivery is idempotent rather than exactly once. Every route must propagate
`wakeKey` to the final durable system and return the original receipt for a
matching retry. Source actors are provenance, not delegated authority; routes
must enforce tenant and loop boundaries with their configured principal.
