# Persistence responsibilities

Eve Ambient does not use a central event repository. Its persistence contracts
coordinate short-lived work, durable decisions, and idempotency receipts across
the attention pipeline. Complete event payloads move by value and have exactly
one active owner at each handoff.

This document names those responsibilities so they can be implemented together
today and separated without changing their correctness boundaries later.

```text
verified channel event
        |
        | accept + freeze fan-out
        v
ingress receipt (payload-free) -------- direct-dispatch coordination
        |
        +-- branch subscription (complete event, one per monitor)
                  |
                  | durable full-value handoff
                  v
        correlation mailbox
          |                 |
          | store mode      | celld mode
          | instance row    | cell state + alarm
          +--------+--------+
                   |
                   | freeze membership
                   v
             evaluation run
        decision -> evidence -> route -> delivery
                   |
                   v
        terminal lineage and receipts
```

## Responsibility map

| Responsibility | Public interfaces | Durable values | Payload rule | Used with celld |
|---|---|---|---|---|
| Source acceptance | `MonitorIngressTransaction` | `StoredIngressReceipt`, direct-dispatch state, ingress sequence | Receipt is always payload-free | Yes |
| Branch work | `MonitorSubscriptionTransaction`, `MonitorSubscriptionStore` | `StoredSubscription` | Owns one complete branch event from fan-out until mailbox acceptance | Yes |
| Store mailbox | `MonitorMailboxTransaction`, `MonitorMailboxStore` | `StoredMonitorInstance`, open/sealed batches, due times | Owns complete buffered events in store mode | No; the cell replaces it for live work |
| Evaluation and delivery | `MonitorRunTransaction`, `MonitorRunStore` | `StoredMonitorRun` | Active run owns a complete frozen batch; terminal run replaces source events with lineage/completeness and retains bounded decision, evidence, and receipt data | Yes |
| Failure reporting | `MonitorDeadLetterTransaction`, `MonitorDeadLetterStore` | `StoredDeadLetter` | Contains lineage and a bounded reason, never an event payload | Yes |
| Deployment compatibility | `MonitorDeploymentTransaction`, `MonitorDeploymentStore` | `StoredDeployment`, `StoredDefinitionPin` | No event payloads | Yes |
| Policy reservations | `MonitorBudgetTransaction` | `UsageReservation` | No event payloads | Yes |
| Operational cleanup | `MonitorRetentionStore` | Expired records across the responsibilities above | Deletes payload-bearing state only after it is terminal or safely handed off | Yes |

`StoredSubscription` is a historical name for branch work, not a reference to
an external event. It contains the complete `ChannelEvent`. In store mode it is
deleted in the same atomic operation that writes the event into the correlation
instance. In celld mode it is deleted only after celld returns a durable append
receipt for the same `branchKey` and `inputHash`.

## Required atomic boundaries

The system requires atomic state transitions, not specifically SQL
transactions.

### Acceptance and frozen fan-out

The runtime checks the source dedupe key, handles an expired dedupe horizon,
allocates the ingress sequence, writes the payload-free acceptance receipt, and
creates every complete branch subscription as one operation. A successful
`publish()` must never expose a receipt without all of its frozen branches.

### Claims and leases

Subscription and run claims compare the current status and lease before writing
the next lease. Competing workers must not both believe they own the same work.
A backend may provide this with a transaction, a conditional write, or a
single-owner execution model.

### Store-mailbox handoff

In store mode, appending the full event to `StoredMonitorInstance` and deleting
its `StoredSubscription` are one atomic operation. The commit transfers payload
custody without a gap or a duplicate active owner.

### celld mailbox handoff

There is no distributed transaction between the branch store and celld. The
runtime first sends the complete event to the cell. Only after the cell commits
and returns an idempotent append receipt does the runtime delete the branch.
If the process fails between those operations, retrying the stable
`branchKey`/`inputHash` returns the same receipt and completes the deletion.

### Membership freeze and run creation

In store mode, claiming a batch, pinning its immutable membership to a stable
run identity, updating the instance, and creating the run happen together. In
celld mode the cell owns the membership transition and sends the evaluator a
complete frozen batch; the evaluator records an idempotent run using that
identity.

### Budgets and terminal outcomes

All applicable budget scopes for one operation reserve together so a rejected
operation consumes none of them. Terminal failure writes the run outcome and
dead-letter receipt together. Store mode also updates the instance in that
operation; celld applies the corresponding statechart transition from the
evaluator response.

## Current composition

`MonitorStoreTransaction` is the compatibility composition of all seven
transactional facets. `MonitorStore` combines the transaction coordinator with
the subscription, mailbox, run, dead-letter, deployment, and retention query
facets.

`PostgresMonitorStore` implements the complete composition using a PostgreSQL
transaction plus a stable advisory lock. `MemoryMonitorStore` implements the
same semantics with a rollback-capable in-memory critical section for tests and
local development.

The composition does **not** mean all responsibilities must remain in one
physical database. It records the capabilities `MonitorRuntime` currently asks
one object to provide. In particular, celld already replaces live
`MonitorMailboxStore` ownership while PostgreSQL continues to provide the other
facets in the supported deployment.

## Next simplification seam

The responsibility split makes the remaining coupling explicit. A later change
can replace the generic `transaction(lockKey, callback)` surface with semantic
atomic commands such as:

- accept source identity and frozen fan-out;
- claim or finish one branch;
- transfer a branch into the selected mailbox;
- create, checkpoint, or finish one run;
- reserve a complete budget set; and
- apply deployment changes or retention cleanup.

Those commands can be backed by PostgreSQL transactions, celld single-owner
state, conditional writes, or another durable system. Runtime construction can
then require only the control-plane facets used in celld mode and add the
store-mailbox facets only when `mailbox.mode` is `store`.
