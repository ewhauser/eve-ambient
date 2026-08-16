# RFC 0004: Correlation World Protocol

- Status: Superseded by RFC 0005
- Former scope: Replace Workflow runs with an Ambient-specific
  `world.stream(key).append(value)` object

The protocol established the durable invariants still used by Ambient: one
serialized owner per correlation, a 48-entry best-effort dedup ring, full-value
batches, checkpoint-before-delivery, and final `wakeKey` idempotency.

It was removed because every production World had to implement an
Ambient-specific interface in addition to the standard Workflow World
interface. RFC 0005 keeps the reducer and ownership model but runs it inside a
standard Workflow hook owner, allowing Vercel, Postgres, celld, and community
Worlds to be selected without an Ambient adapter.
