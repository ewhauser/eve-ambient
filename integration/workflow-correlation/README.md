# Workflow correlation integration

This consumer-style fixture re-exports Ambient's packaged workflows, runs
them through `@workflow/vitest`, and instruments the standard World interface.

It verifies that:

- a deterministic hook has one active owner under concurrent cold starts;
- every transport-accepted append reaches that owner;
- a 20-publisher in-flight cold burst creates one run and delivers 19 follower
  resumes after one failed leader probe;
- warm admission stays within the measured standard-World call budget;
- prepare and delivery retries preserve the exact batch and `wakeKey`; and
- a correlation run remains active after its 48-entry best-effort dedup ring
  has wrapped.

The active run is intentionally not rotated. Its reducer state is bounded, but
its Workflow history grows for as long as that correlation remains active.
