# Correlation Workflow protocol

Ambient exposes one application-facing command and implements production
durability with standard Workflow APIs:

```ts
interface AttentionEngine {
  accept(fanout: AcceptedFanout): Promise<AttentionAcceptanceReceipt>;
}

// Conceptual Workflow APIs used by the implementation.
resumeHook(cachedOwner ?? correlationToken, append);
start(correlationWorkflow, [config, instanceKey, firstCommand]);
```

There is no Ambient-specific World interface.

## Admission

```text
accept(fanout)
  |
  +-> validate the complete fanout and capacity
  +-> derive instanceKey for every branch
  +-> group branches by instanceKey
  +-> use the process-local cached owner when available
       +-> inactive: evict it and continue with the unchanged append
       |
       +-> active: transport accepts the append without a token lookup
  +-> on a cache miss, enter one process-local in-flight gate per instanceKey
       |
       +-> leader probes the deterministic hook
       |    +-> warm: probe accepts the leader append
       |    +-> cold: start one candidate with the leader append
       |              wait with jittered exponential backoff
       |              winner: seed is already accepted
       |              loser: resume the elected owner
       |
       +-> followers join the result and resume their own appends to the owner
  +-> return one payload-free application receipt
```

`accept()` resolves when Workflow has accepted every selected correlation
append, either as a hook value or a seeded run argument. It does not wait for
the correlation reducer, prepare callback, or final delivery.
Duplicate and conflicting append outcomes are therefore asynchronous. A
conflicting idempotency value is recorded as a named step in the Workflow run
timeline; it does not terminate the correlation owner.

Concurrent publishers in one process share a token-keyed initial-probe promise.
On a warm hit the leader's probe accepts its command; followers receive the
owner handle but must resume their own commands. On a cold miss only the leader
starts and polls. The winning candidate is seeded with that publisher's append
and processes it only after `getConflict()` confirms ownership. Other processes
may still start candidates because hook lookup and run creation are not globally
atomic. Every candidate creates the same deterministic hook and awaits
`getConflict()`; losing candidates exit, and their publishers resume the owner.
Failed and completed promises are removed so later cache misses can probe
normally. The integration suite exercises both a 20-publisher local cold race
and an explicit two-candidate ownership race.

Every successful resume returns its resolved hook owner, and cold registration
polling discovers the same standard Workflow handle. Ambient keeps those
handles in one process-local cache shared by all `WorkflowAttentionEngine`
instances. The cache is capped at 1,024 least-recently-used entries, and an
entry expires after 10 minutes without a successful resume. This retains a
substantial hot set at fixed memory. The idle TTL favors burst reuse while
periodically revalidating dormant correlations, and active permanent
correlations keep refreshing their handle. Token keys already fingerprint the
immutable Workflow configuration, so configuration cutovers cannot reuse an old
owner.

The cache is advisory. `HookNotFoundError` from a cached handle evicts that
owner and routes the unchanged append through the token-keyed probe gate. The
leader retries by deterministic token and only then uses the normal cold
initialization path if the token is also absent. Conditional eviction prevents
a late failure for an old run from removing a concurrently cached replacement.
No application storage, custom World method, or deployment dependency is
introduced.

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
correlation append that cannot fit. At capacity, the run holds at most the next
validated append and waits for its reducer timer without requesting another
hook value. Remaining values stay in Workflow's durable hook queue.

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

For a cached warm correlation, Ambient makes one high-level `resumeHook()` call
with the owner for every distinct correlation selected from an inbound event.
An uncached warm leader makes the same call by token and caches the returned
owner. No selected correlations means no Workflow call, and several matching
branches with the same correlation are grouped into one append.

A cold leader makes one failed `resumeHook()`, one `start()` seeded with the
append, and a variable number of `getHookByToken()` registration lookups. It
does not resume again when its candidate wins. Same-process followers share the
probe, start, and polling result, then resume the owner; cross-process losers do
the same after deterministic ownership resolves. A 20-publisher in-flight warm
burst therefore makes 20 resumes. A winning cold burst also makes 20 total
resumes—one failed leader probe plus 19 follower resumes—alongside one start and
one registration polling chain. The previous post-probe gate made 39 resumes.

This is the public admission protocol. It does not promise how many reads,
writes, or queue operations a World uses to implement that RPC.

## Measured Workflow and World calls

The current Workflow 5 integration instruments public standard-World methods:

| Path | Ambient protocol calls | Standard World calls | Application HTTP |
|---|---:|---:|---:|
| Cold buffer-only append | failed resume + start + polling | 16-17 observed | 0 |
| 20-publisher immediate cold burst | failed probe + 19 follower resumes + start/polling | 215-226 observed | 20 |
| Warm buffer-only append | 1 | 6 | 0 |
| Append that closes, prepares, and delivers a batch | 1 | 14 | 2 |

The 6-call warm path uses a cached owner and performs one run read, three event
writes, and two queue publishes, with no hook-token lookup. These are
observations from Workflow 5.0.0-beta.42 and the instrumented local test World,
not calls in Ambient's public contract and not a required implementation shape
for other Worlds. The cold count varied between 16 and 17 in local runs,
including six or seven hook lookups, versus 27 calls and 12 lookups with fixed
5 ms polling. Cold counts still vary with scheduler timing. In local
before/after 20-publisher samples, the initial-probe gate
reduced public resumes from 39 to 20 and World hook lookups from 22 to 3.
Post-change total World calls ranged from 215 to 226 versus 256 in pre-change
samples, but that total includes its 20 immediate reducers and prepare callbacks,
whose scheduler work can vary; no latency conclusion is drawn from this harness.

## Conformance

The reference and integration suites check grouping, same-correlation reuse,
bounded ring dedup, conflicts, capacity, ordering, debounce, cooldown, leases,
callback validation, exact-wake retry, reducer backpressure, configuration
cutover, concurrent cold ownership, consumer workflow discovery, permanent-run
behavior, and measured World calls.
