# Workflow correlation integration

This consumer-style fixture re-exports Ambient's packaged workflows, runs
them through `@workflow/vitest`, and instruments the standard World interface.

It verifies that:

- a deterministic hook has one active owner under concurrent cold starts;
- every transport-accepted append reaches that owner;
- a 20-publisher same-correlation cold burst creates one run seeded with one
  bounded 20-command batch after one failed token probe;
- warm admission reuses the resolved owner without a hook-token lookup and
  matches the measured standard-World call count;
- prepare and delivery retries preserve the exact batch and `wakeKey`; and
- a correlation run remains active after its 48-entry best-effort dedup ring
  has wrapped.

The active run is intentionally not rotated. Its reducer state is bounded, but
its Workflow history grows for as long as that correlation remains active.
