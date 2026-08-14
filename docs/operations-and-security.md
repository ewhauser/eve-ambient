# Operations and security

Eve Ambient stores only the state required to finish active attention work and
bounded idempotency receipts. It deliberately does not provide an event
repository, audit database, history interface, or replay operation.

## Durability

- `accept()` succeeds only after the complete frozen fan-out reaches durable
  backend custody.
- Event, branch, batch, run, and wake keys bind stable lineage to canonical
  input hashes. Same-key/different-input retries fail closed.
- The backend records a prepared outcome before delivery. Delivery retries use
  the exact recorded bytes and `wakeKey`.
- Immediate and debounced batches have explicit capacity limits. Retry claims
  have leases and attempt limits.
- Terminal success, ignore, shadow, or failure removes event and wake payloads.
  Bounded payload-free receipts remain until the deduplication horizon.

PostgreSQL exposes aggregate `diagnostics()` counts. celld exposes a
payload-free diagnostics route. Neither can retrieve stored events.

## Definition rollout

Rule ID, version, mode, policy, phase, and correlation membership participate
in durable identity. Treat a published rule version as immutable. Deploy a new
version for behavior or policy changes and keep callback code for old versions
available until their accepted work finishes.

There is no live in-flight state migration contract. Coordinate backend or
definition cutovers at the application level after old work drains or is
explicitly abandoned.

## Trust boundary

Canonical channel data, source actor fields, decision output, and evidence are
untrusted application data. Static rule instructions are trusted application
configuration. The Eve adapter renders these fields separately so evidence is
not silently promoted to task authority.

Delivery always uses the application's configured principal. Source actors are
provenance, not delegated execution identity. Applications must filter bot and
agent loops appropriate to their provider and enforce tenant boundaries in
channel normalization, routes, and credentials.

## Final action

Delivery is idempotent rather than exactly-once. Every route must pass
`wakeKey` into the final durable action and return the same receipt for a
matching retry. The Eve adapter maps `wakeKey` to Eve's session admission key.
Consumers must apply the carried patch for `vercel/eve#1842` until that keyed
admission behavior exists in their Eve build.

Direct chat dispatch is a separate adapter-owned action. Its adapter receives a
stable direct-dispatch key and input hash and must enforce the same duplicate
and conflict behavior.

## Secrets and networks

Keep provider, model, session, tool, database, and celld credentials out of
channel event payloads and prepared evidence.

Use separate principals and network policy for:

- provider ingress and acknowledgement;
- PostgreSQL migrations and runtime access, when selected;
- celld public admission, internal fleet traffic, and callback URLs, when
  selected;
- preparation or model invocation; and
- final application-principal delivery.

The packaged celld routes and callback handler require bearer authentication.
Store `ATTENTION_SECRET` in the deployment secret manager and also restrict
administrative and callback listeners to trusted networks.

## Deliberate limits

Replay, source event queries, portable run/dead-letter databases, arbitrary I/O
correlation, semantic re-keying, mutable public workflow state, watermark
processing, source-user delegation, multi-route wakes, and cross-application
loop guarantees are outside the protocol.
