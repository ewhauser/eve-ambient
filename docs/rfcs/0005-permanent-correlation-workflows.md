# RFC 0005: Permanent Correlation Workflows

- Status: Accepted
- Implementation: Complete
- Scope: Use one standard Workflow run per correlation and remove the custom
  `AttentionWorld` interface
- Supersedes: RFC 0004 and the transport details of RFC 0003

## Decision

Each correlation address and immutable Workflow configuration map to one
deterministic Workflow hook token. An event selected for that address is sent
with `resumeHook()`. Process-local publishers first share one transient,
token-keyed probe. Its leader either finds the warm owner and accepts its append
or starts a candidate seeded with the first append and waits for the hook owner
with jittered exponential backoff. Followers resume their own appends to the
returned owner.

```text
event 1 -> correlation K -> failed resume -> start owner K with event 1
event 2 -> correlation K -> join in-flight probe -> resume owner K
event 3 -> correlation J -> failed resume -> start owner J with event 3
```

The workflow creates the token and awaits `getConflict()` before processing its
seed or hook messages. Same-process cold publications create one candidate.
Candidates from different processes still converge on one owner. Losing
candidates exit without processing their seed; their publishers deliver it to
the registered owner. Failed and completed in-flight probes are removed; this
decision does not retain a process-local owner cache.

## State and effects

The owner applies the existing pure reducer sequentially. It retains a bounded
recent-message ring, open and sealed batches, one active claim, the exact
prepared wake, retry timestamps, and cooldown. It races hook input against the
next durable timer.

Pending branch count and bytes are explicitly capped. Once applied state is at
capacity, the owner holds at most the next validated append and stops advancing
the hook iterator until a due run releases space. Remaining events stay in the
standard World's durable hook queue rather than accumulating as reducer
payloads.

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

For each selected warm correlation, Ambient makes one high-level `resumeHook()`
call. A cold leader makes a failed resume, a seeded `start()`, and variable hook
lookups; when its candidate wins, it skips the second resume. With Workflow
`5.0.0-beta.42` and the local World, the checked-in integration measures 7
public World calls for a warm buffer-only message and 15 World calls plus two
application HTTP attempts when that message closes, prepares, and delivers a
batch. Representative cold runs used 16-17 calls, including six or seven hook
lookups, down from 27 calls and 12 lookups before backoff and seeded startup. A
20-publisher winning cold burst makes 20 total resumes rather than 39: one
failed leader probe and 19 follower resumes, with one start and one polling
chain. Local before/after integration samples observed hook lookups fall from
22 to 3. Post-change total World calls ranged from 215 to 226 versus 256 in
pre-change samples; that total includes all immediate reducer and callback
scheduling and is not a latency measurement.

## Consequences

- standard Workflow Worlds are direct deployment options;
- no World is forced to implement an Ambient interface;
- one event can still fan out to multiple independent correlation resumes;
- source dedup remains bounded and best effort;
- final-effect safety remains durable through `wakeKey`;
- prepare and delivery remain at-least-once; and
- changing immutable Workflow configuration cuts new events over to a fresh
  owner without state migration; and
- custom correlation-World state cannot migrate automatically.
