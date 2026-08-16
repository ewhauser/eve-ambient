# RFC 0005: Permanent Correlation Workflows

- Status: Accepted
- Implementation: Complete
- Scope: Use one standard Workflow run per correlation and remove the custom
  `AttentionWorld` interface
- Supersedes: RFC 0004 and the transport details of RFC 0003

## Decision

Each correlation address maps to one deterministic Workflow hook token. An
event selected for that address is sent with `resumeHook()`. If the hook is
missing, the publisher starts a candidate correlation run, waits for the hook
owner, and resumes it.

```text
event 1 -> correlation K -> start owner K -> resume hook K
event 2 -> correlation K ------------------> resume hook K
event 3 -> correlation J -> start owner J -> resume hook J
```

The workflow creates the token and awaits `getConflict()` before processing
messages. Concurrent cold candidates therefore converge on one owner. Losing
candidates exit; publishers deliver to the registered owner.

## State and effects

The owner applies the existing pure reducer sequentially. It retains a bounded
recent-message ring, open and sealed batches, one active claim, the exact
prepared wake, retry timestamps, and cooldown. It races hook input against the
next durable timer.

Prepare and deliver run as authenticated Workflow steps. They may repeat.
Prepared wakes are checkpointed before delivery, and the final durable receiver
must deduplicate by `wakeKey`.

`accept()` acknowledges Workflow transport custody, not reducer completion.
Duplicates and conflicting values are resolved asynchronously inside the run.
Conflicts are recorded as a named step and do not destroy the correlation
owner.

## No automatic rotation

The run remains active after its 48-entry ring wraps. Workflow 5 does not expose
a standard continue-as-new operation that atomically transfers a deterministic
hook and reducer state. A custom handoff would recreate the backend-specific
coordination this RFC removes.

This has an explicit cost: reducer state is bounded, but Workflow event history
grows for the lifetime of a hot correlation. Operators must monitor per-run
history limits and select bounded correlation keys. Standard continue-as-new
can add compaction later without changing the public Ambient binding.

## Calls

Ambient makes one high-level `resumeHook()` call for each selected correlation.
With Workflow `5.0.0-beta.42` and the local World, the checked-in integration
measures 7 public World calls for a warm buffer-only message and 15 World calls
plus two application HTTP attempts when that message closes, prepares, and
delivers a batch. Cold creation includes variable hook-registration polling.

## Consequences

- standard Workflow Worlds are direct deployment options;
- no World is forced to implement an Ambient interface;
- one event can still fan out to multiple independent correlation resumes;
- source dedup remains bounded and best effort;
- final-effect safety remains durable through `wakeKey`;
- prepare and delivery remain at-least-once; and
- custom correlation-World state cannot migrate automatically.
