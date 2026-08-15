# Operations and security

## Durability

- `accept()` succeeds only after every frozen branch emits a semantic append
  receipt.
- Deterministic hook tokens serialize event admission and correlation streams.
- Prepared output is checkpointed before delivery; retries reuse the exact
  wake and `wakeKey`.
- Capacity, retry attempts, cooldown, and deduplication horizons are bounded
  configuration carried with the workflow run.
- Callback fetches have a bounded timeout, and the authenticated application
  endpoint rejects oversized bodies before invoking application code.

Monitor World queue lag, failed runs and steps, hook conflicts, callback
latency/status, stream write errors, active sleeps, storage growth, and run
retention. Ambient no longer has a database poller or backend diagnostics API;
those signals belong to the selected World.

## Secrets

`callbackSecretEnv` is an environment variable name, not a secret value. The
durable step reads its value at callback time, which supports secret rotation
without writing the bearer credential into workflow history. The application
handler reads the same variable and uses constant-time byte comparison.

Keep provider credentials, model/session credentials, database credentials,
and callback secrets out of event payloads, decisions, evidence, and World
metadata.

## Payload retention

Ambient's reducer deletes terminal payloads from its live state. Workflow
inputs, hook events, and step values may remain in the World's append-only
history after completion. Configure encryption at rest, key rotation, backup
scope, retention, erasure, and administrative access at the World layer.

Do not describe reducer cleanup as physical erasure unless the selected World
has been tested to provide it. Ambient exposes no event history or replay API,
but absence of an API is not absence of retained bytes.

## Definition rollout

Rule identity, version, mode, policy, phase, and correlation membership
participate in durable identity. Treat released versions as immutable and keep
their callback code deployed until old workflows drain.

Workflow code also has a deployment identity. Follow the Workflow host's
versioning rules so sleeping runs can resume against compatible workflow and
step registrations.

## Final action

Delivery is idempotent rather than exactly once. Every route must carry
`wakeKey` into the final durable system and return the original receipt for a
matching retry. The Eve adapter maps it to Eve's keyed admission boundary.

Source actors are provenance, not delegated authority. The application route
always executes with its configured principal and must enforce tenant and loop
boundaries.
