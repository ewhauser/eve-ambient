# RFC 0002: Durable Attention Engine

- Status: Superseded by [RFC 0004](0004-correlation-world-protocol.md)
- Preserved: the `AttentionEngine.accept()` boundary, pure correlation state
  machine, prepare/checkpoint/deliver split, and no-replay contract
- Removed: backend-specific engines, public storage records, event
  coordinators, pollers, and backend conformance profiles

## Decision retained

Applications depend on one semantic command rather than a storage interface:

```ts
interface AttentionEngine {
  accept(input: AcceptedFanout): Promise<AttentionAcceptanceReceipt>;
}
```

Application code supplies two callbacks:

```ts
interface AttentionCallbacks {
  prepare(batch: FrozenAttentionBatch): Promise<PreparedAttentionOutcome>;
  deliver(wake: PreparedAttentionWake): Promise<AttentionDeliveryReceipt>;
}
```

The engine owns correlation, buffering, canonical batch freeze, leases,
checkpointing, retries, cooldown, and terminal cleanup. It exposes no generic
transaction, event lookup, history, or replay API.

## Superseded implementation

The original RFC mapped this contract separately onto memory, custom Postgres,
and custom celld backends. It also introduced a source-event coordinator to
freeze and hand off complete fanout membership. Those components were built and
then removed.

RFC 0004 sends each event directly to its correlation-owned World object.
Receiver-local bounded dedup replaces global coordinator state. The current
protocol and operational requirements live in RFC 0004 and the focused
attention-stream documentation.

Git history retains the full original implementation plan.
