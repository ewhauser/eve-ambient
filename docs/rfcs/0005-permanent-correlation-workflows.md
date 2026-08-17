# RFC 0005: Permanent Correlation Workflows

- Status: Accepted
- Implementation: Complete
- Scope: Use one standard Workflow run per correlation and remove the custom
  `AttentionWorld` interface
- Supersedes: RFC 0004 and the transport details of RFC 0003

## Decision

Each correlation address and immutable Workflow configuration map to one
deterministic Workflow hook token. Appends selected for that address enter a
process-local queue keyed by the token plus matching operational settings.
After a fixed 2 ms window, the publisher sends one bounded `append-many`
command with `resumeHook()`. A resolved owner is reused from a bounded
process-local cache when possible. Otherwise, publishers in that lane share
the initial token probe. On a miss, its leader starts a candidate seeded with
the entire first command and waits for the hook owner with jittered exponential
backoff.

```text
events 1..20 -> correlation K -> one append-many -> failed resume -> seeded start K
events 21..40 -> correlation K -> one append-many -------------> resume hook K
event J       -> correlation J -> independent append-many -> cold path J
```

The hook accepts only `append-many`. Because the protocol has no users yet,
this decision is a hard cutover: it defines no legacy command handling, dual
decode, migration, or version-negotiation machinery.

Batching never crosses a process or deterministic token. The 2 ms window was
selected by the local Workflow integration: 1 ms split the 20-event warm burst
into two public resumes, while 2 ms produced one. This adds one nominal 2 ms
timer window to a lone event; event-loop scheduling can extend it.

Commands are split in process-local queue-enrollment order by both command
count and canonical serialized bytes. They are also constrained by aggregate
branch count and branch bytes so no hook command can exceed an empty reducer's
`maxPendingBranches` or `maxPendingBytes` capacity. Defaults are 64 appends and
16 MiB serialized bytes. Each append retains its own identity and `acceptedAt`.
Each original `accept()` resolves only after the chunk containing it reaches
Workflow custody and rejects if that publication fails. A failed chunk also
fail-stops the remaining unsent chunks from that flush so later chunks cannot
overtake it.

The process-local lane is independently bounded at 1,000 queued-or-publishing
appends and 64 MiB of canonical append bytes by default. Overflow rejects with
retryable `WorkflowAdmissionBackpressureError`; an individually oversized
append remains a non-retryable capacity error. Registration timeout and these
local limits define the operational lane so a shared batch never inherits an
arbitrary publisher's timeout or backlog policy. They do not change the hook
token.

The workflow creates the token and awaits `getConflict()` before processing its
seed or hook messages. Same-process cold publications create one candidate.
Candidates from different processes still converge on one owner. Losing
candidates exit without processing their seed; their publishers deliver it to
the registered owner.

Resolved owners are retained in a process-local 1,024-entry LRU with a
10-minute idle TTL. A missing cached owner is evicted, and the unchanged batch
retries through its token before cold initialization. The cache and probe gate
remain advisory uses of standard Workflow APIs.

## State and effects

The owner applies every append inside a command sequentially with the existing
pure reducer. It retains a bounded
recent-message ring, open and sealed batches, one active claim, the exact
prepared wake, retry timestamps, and cooldown. It races hook input against the
next durable timer.

Pending branch count and bytes are explicitly capped. Once applied state is at
capacity, the owner holds only the unprocessed remainder of one bounded command
and stops advancing the hook iterator until a due run releases space. Remaining
commands stay in the standard World's durable hook queue rather than
accumulating as reducer payloads. Arrivals during a process-local active flush
are scheduled for another bounded command. Every promise in a successful,
failed, or fail-stopped chunk is settled.

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

For each bounded warm same-token chunk, Ambient makes one high-level
`resumeHook()` call. A cold leader makes a failed resume, a `start()` seeded
with the full first chunk, and variable hook lookups; when its candidate wins,
it skips the second resume. With Workflow `5.0.0-beta.42` and the local World,
the checked-in 20-event integration measures one public warm `resumeHook()`,
6 standard World calls, and 25.3-28.2 ms local admission across two combined
runs. The corresponding cold
burst measures one failed public `resumeHook()`, one seeded public `start()`,
two registration lookups, 13 standard World calls, and 40.3-59.3 ms local
admission. Cached closing, preparing, and delivering uses
14 World calls plus two application HTTP attempts. Registration timing makes
the cold lookup count variable.

## Consequences

- standard Workflow Worlds are direct deployment options;
- no World is forced to implement an Ambient interface;
- one event can still fan out to multiple independent correlation queues;
- process-local same-token bursts use bounded ordered `append-many` commands;
- resolved hook owners are cached in a bounded process-local LRU;
- local queued and publishing payloads are bounded with retryable backpressure;
- a lone event pays one nominal 2 ms batching window;
- source dedup remains bounded and best effort;
- final-effect safety remains durable through `wakeKey`;
- prepare and delivery remain at-least-once; and
- changing immutable Workflow configuration cuts new events over to a fresh
  owner without state migration; and
- custom correlation-World state cannot migrate automatically.
