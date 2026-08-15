# RFC 0003: Workflow World Runtime

- Status: Superseded by [RFC 0004](0004-correlation-world-protocol.md)
- Implementation: Removed
- Preserved: infrastructure composition belongs below Ambient's semantic
  correlation boundary
- Removed: Workflow SDK dependency, process-global World, runs, hooks, steps,
  sleeps, output streams, and Postgres World integration

## Historical decision

This RFC replaced Ambient's custom Postgres and celld engines with the Vercel
Workflow SDK and a host-installed process-global World. An event-coordinator
run submitted branches to long-lived correlation runs through deterministic
hooks and waited for semantic receipts on an output stream.

The spike proved that storage implementations could combine queue, database,
and streaming infrastructure below a common boundary. It also showed that the
Workflow execution protocol added substantially more RPC and storage activity
than Ambient's correlation model required.

## Replacement

RFC 0004 keeps the useful boundary and removes the execution machinery:

```text
event -> group by correlation -> world.stream(key).append(group)
```

There is no process-global Workflow runtime or event coordinator. The World
object owns one correlation stream directly, including its bounded dedup ring,
state machine, timers, and callbacks.

Git history retains the full spike design and measured implementation.
