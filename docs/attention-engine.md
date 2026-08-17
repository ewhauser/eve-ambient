# Correlation Workflow protocol

Ambient exposes one application-facing command and implements production
durability with standard Workflow APIs:

```ts
interface AttentionEngine {
  accept(fanout: AcceptedFanout): Promise<AttentionAcceptanceReceipt>;
}

// Conceptual Workflow APIs used by the implementation.
resumeHook(correlationToken, appendMany);
start(correlationWorkflow, [config, instanceKey, firstAppendMany]);
```

There is no Ambient-specific World interface. The correlation hook accepts
only the batched `append-many` shape; there is no legacy single-append decoder,
migration path, or dual protocol.

## Admission

```text
accept(fanout)
  |
  +-> validate the complete fanout and capacity
  +-> derive instanceKey for every branch
  +-> group branches by instanceKey
  +-> enqueue each append by complete deterministic hook token and
      matching operational queue settings
  +-> after 5 ms, split by command count, serialized bytes,
      pending branches, and pending branch bytes
  +-> publish chunks serially per token; independent tokens proceed concurrently
       |
       +-> cached owner exists: resume it with the complete chunk
       |
       +-> no cached owner: join/lead the operational-lane token probe
                         warm probe: token accepts the complete chunk
                         start one candidate with the complete first chunk
                         wait with jittered exponential backoff
                         winner: seed is already accepted
                         loser: resume the hook owner
  +-> return one payload-free application receipt
```

`accept()` resolves when Workflow has accepted every selected correlation
append, as one entry of a hook value or a seeded run argument. Every original
`accept()` keeps its own promise and receipt. Entries in a successfully
published chunk resolve together after Workflow custody; a publication failure
rejects each affected promise. It does not wait for the correlation reducer,
prepare callback, or final delivery.
Duplicate and conflicting append outcomes are therefore asynchronous. A
conflicting idempotency value is recorded as a named step in the Workflow run
timeline; it does not terminate the correlation owner.

Batching is process-local and keyed by the complete deterministic hook token
plus the publisher's registration timeout and local backpressure limits. It
never combines different correlations or operational lanes, and separate
processes publish their own chunks. A 2 ms timer window split the checked-in
20-event cold burst under CI and full-suite load. Repeated standalone and
full-check runs at the fixed 5 ms window each collapsed the warm burst to one
resume and the cold burst to one failed resume plus one seeded start. A single
event therefore incurs one nominal 5 ms scheduling window, which may be
extended by event-loop delay.

Commands default to at most 64 appends and 16 MiB of canonical serialized
bytes. The splitter also caps aggregate branches and branch bytes at
`maxPendingBranches` and `maxPendingBytes`. Oversized bursts become ordered
chunks. Arrivals during an active publication remain in the same token queue
for a later flush. Order is defined at queue enrollment. Chunks publish
serially; if one fails, its entries and every later unsent entry from that flush
reject with the same failure, and no later chunk from the snapshot is
published.

Each operational lane also caps its process-local backlog, including work in a
currently active publication, at 1,000 appends and 64 MiB of canonical append
bytes by default. An append that cannot fit by itself is a capacity error.
Otherwise, admission beyond either live-backlog limit rejects with retryable
`WorkflowAdmissionBackpressureError`; the caller can retry the same stable
input after capacity becomes available. This prevents a stalled Workflow call
from accumulating an unbounded local payload.

Cold publishers in one process and operational lane share an initialization
probe, so only one probes, starts, and polls. The winning candidate is seeded
with the entire first chunk and processes it only after `getConflict()` confirms
ownership. Other lanes or processes may still start candidates because hook
lookup and run creation are not globally atomic. Every candidate creates the
same deterministic hook and awaits `getConflict()`; losing candidates exit,
and their publishers resume the owner. The integration suite exercises both a
20-publisher local cold race and an explicit two-candidate ownership race.

Successful probes and starts place the resolved owner in a process-local
1,024-entry LRU with a 10-minute idle TTL, shared across engine instances. A
missing cached handle is evicted and the unchanged batch retries through its
token. This cache is advisory and uses only standard Workflow hook handles.

## Correlation address and lineage

```text
eventKey -> occurrenceKey -> branchKey
partitionKey -> partitionCellKey -> instanceKey -> batchKey -> runKey -> wakeKey
```

`instanceKey` covers application, tenant, channel, installation, channel
partition, rule ID, rule version, and correlation key. Its deterministic hook
token is globally namespaced for the selected World and includes a fingerprint
of the immutable correlation Workflow configuration.

Each key binds to a canonical input hash. A remembered key with the same hash
is a duplicate; the same key with another hash is an
`IdempotencyConflictError`.

## Owned state

One permanent correlation run owns:

- a fixed-size recent-message ring for best-effort admission dedup;
- open, sealed, and active full-value batches;
- the exact prepared wake checkpoint; and
- retry, lease, and cooldown timestamps.

Applied full-value payloads are limited by `maxPendingBranches` and
`maxPendingBytes` (1,000 and 16 MiB by default). The publisher rejects a single
correlation append that cannot fit. Every `append-many` command fits within the
same aggregate branch budgets as an empty reducer and within its independent
count/serialized-byte limits. The owner applies entries sequentially and never
adds the whole command atomically. At capacity, it retains only the unprocessed
remainder of that one bounded command and waits for its reducer timer without
requesting another hook value. Remaining commands stay in Workflow's durable
hook queue.

Terminal processing removes source payloads and in-flight branch entries. Ring
entries contain only keys, hashes, timestamps, and receipts. Ring eviction
means an old source append may be processed again; final effects are still
protected by the durable `wakeKey` boundary.

The reducer state is bounded, but Workflow event history is not. The run is not
rotated automatically because Workflow 5 does not yet provide a standard
continue-as-new primitive that atomically preserves hook ownership. Permanently
hot correlations must be monitored against the selected World's per-run event
and retention limits.

## Timers and callbacks

The run races its hook iterator against the next reducer deadline. A moved
debounce deadline is rechecked when the old timer fires, so stale timers cannot
close a newer batch early.

Prepare and deliver are authenticated Workflow steps that post complete frozen
batches and prepared wakes to `/ambient/prepare` and `/ambient/deliver`.
`prepare()` may repeat. A wake is recorded before delivery, and every delivery
retry uses the exact checkpointed value. Routes must carry `wakeKey` into the
final durable system. Invalid or oversized prepare results and invalid delivery
receipts terminate only the affected attention run; they do not terminate the
permanent correlation owner.

## Protocol-level call fanout

For a warm correlation, Ambient makes one high-level `resumeHook()` call per
bounded same-token chunk. No selected correlations means no Workflow call,
several matching branches from one event are grouped into one append, and
concurrent same-process appends can share one chunk. Each append retains its own
`acceptedAt`, append identity, and sequential reducer position.

A cold leader makes one failed `resumeHook()`, one `start()` seeded with the
entire first chunk, and a variable number of `getHookByToken()` registration lookups. It
does not resume again when its candidate wins. Other operational lanes or
processes can still race candidates; losers resume the owner after
deterministic ownership resolves.

This is the public admission protocol. It does not promise how many reads,
writes, or queue operations a World uses to implement that RPC.

## Measured Workflow and World calls

The current Workflow 5 integration instruments public standard-World methods:

| Path | Ambient protocol calls | Standard World calls | Application HTTP |
|---|---:|---:|---:|
| Cold 20-event buffer-only burst | 1 failed resume + 1 seeded start + 2-3 lookups | 13-14 observed | 0 |
| Cached warm 20-event buffer-only burst | 1 resume | 6 | 0 |
| Cached append that closes, prepares, and delivers a batch | 1 | 14 | 2 |

The 6-call cached warm path is one run read, three event writes, and two queue
publishes. Repeated local standalone and full-check 20-event runs took
15.8-26.9 ms warm and 58.2-68.3 ms cold; two Node 24 CI runs took 31.5-41.8 ms
warm and 99.8-129.2 ms cold with the same one-resume/seeded-start shape. These
runs used 13-14 cold World calls and 2-3 public registration lookups; the
single-append cold scenario used 16-17
World calls because registration timing varies. These are observations from
Workflow 5.0.0-beta.42 and the instrumented local test World, not calls in
Ambient's public contract and not a required implementation shape for other
Worlds. Deployed latency includes World, network, database, and region costs.

## Conformance

The reference and integration suites check grouping, same-correlation reuse,
bounded ring dedup, conflicts, capacity, ordering, debounce, cooldown, leases,
callback validation, exact-wake retry, reducer backpressure, 20-command
coalescing, token isolation, count/byte chunking, publication failures, active
flush arrivals, local count/byte backpressure, fail-stop chunk publication,
operational-lane isolation, seeded cold batches, configuration cutover,
concurrent cold ownership, cached-owner reuse/eviction/expiry, probe cleanup,
consumer workflow discovery, permanent-run behavior, and measured public
Workflow plus standard World calls.
