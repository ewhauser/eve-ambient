# Attention engine protocol

RFC 0002 replaces Eve Ambient's portable storage model with one durable
command and two application callbacks. The protocol and memory reference
engine are available alongside the legacy runtime while the celld and
PostgreSQL engines are implemented.

## Boundary

```ts
interface AttentionEngine {
  accept(input: AcceptedFanout): Promise<AttentionAcceptanceReceipt>;
}
```

`accept()` receives one complete canonical source event and a canonically
ordered set of complete monitor branches. It returns only after every frozen
branch has been durably accepted by its correlation workflow. A backend may
use cells, SQL, or memory internally; none of that state is part of the public
contract.

The application provides:

```ts
interface AttentionCallbacks {
  prepare(batch: FrozenAttentionBatch): Promise<PreparedAttentionOutcome>;
  deliver(wake: PreparedAttentionWake): Promise<AttentionDeliveryReceipt>;
}
```

`prepare()` may be repeated if its response is lost. It must not perform the
final user-visible delivery. The engine records the first successful prepared
outcome before calling `deliver()`. Every delivery retry carries the exact same
complete value, `inputHash`, and `wakeKey`.

## Compile a complete fan-out

Channel adapters first canonicalize provider input. Delivery-attempt metadata
does not enter the canonical event or its hash.

```ts
import {
  canonicalizeChannelDelivery,
  compileAcceptedFanout,
  defineChannelCanonicalization,
} from "@ewhauser/eve-ambient";

const source = await canonicalizeChannelDelivery(
  defineChannelCanonicalization({
    version: 1,
    canonicalize: (raw: ProviderMessage) => ({
      id: raw.messageId,
      type: "message.created",
      version: 1,
      occurredAt: raw.createdAt,
      data: { text: raw.text },
      source: {
        channelId: "slack",
        installationId: raw.workspaceId,
        tenantId: raw.tenantId,
      },
      origin: { kind: "external", depth: 0 },
    }),
  }),
  providerMessage,
  { applicationId: "engineering-agent" },
);

const accepted = await compileAcceptedFanout({
  source,
  branches: [
    {
      monitorId: "incident-attention",
      definitionVersion: "sha256:definition",
      correlationKey: `incident:${providerMessage.incidentId}`,
      orderKey: providerMessage.messageId,
      mode: "active",
      policy: {
        buffer: {
          mode: "debounce",
          quietPeriodMs: 2_000,
          maxWaitMs: 30_000,
          maxEvents: 100,
          maxBytes: 512_000,
        },
      },
    },
  ],
});

await engine.accept(accepted);
```

The compiler derives `occurrenceKey`, branch keys, input hashes, and the
fan-out manifest hash. The engine recomputes all of them before retaining any
state. Duplicate branch keys, unsupported wire fields, mismatched event
copies, and oversized single-branch batches fail closed.

An empty `branches` array is a durable no-work result. A matching retry cannot
turn it into work after monitor deployment changes.

## Memory reference engine

The memory engine is a deterministic executable specification for tests:

```ts
import type { AttentionCallbacks } from "@ewhauser/eve-ambient";
import { MemoryAttentionEngine } from "@ewhauser/eve-ambient/memory";

const callbacks: AttentionCallbacks = {
  async prepare(batch) {
    return {
      kind: "wake",
      decision: { reason: "actionable" },
      routeId: "eve-session",
      instruction: "Investigate the incident.",
      evidence: {
        messages: batch.branches.map((branch) => branch.event.data),
      },
    };
  },
  async deliver(wake) {
    return sessionAdapter.deliver(wake);
  },
};

const engine = new MemoryAttentionEngine({ callbacks });
await engine.accept(accepted);
await engine.runDue();
```

`runDue()` is a memory-backend worker control, not an `AttentionEngine` method.
A celld engine will drive work with cell alarms and authenticated callbacks. A
PostgreSQL engine will expose its own worker entry point.

The memory backend supports a controllable clock, bounded retries, explicit
capacity limits, injected append failures, and payload-free diagnostics. Its
shared conformance suite is the behavioral oracle for later durable backends.

## Ownership and retention

| State | Complete payload owner | Deletion point |
|---|---|---|
| Frozen fan-out | Event coordinator | Every branch is terminal or durably appended |
| Buffered branch | Correlation workflow | Its frozen batch reaches a terminal outcome |
| Active batch | Correlation workflow | Ignore, shadow completion, terminal failure, or delivery |
| Prepared wake | Correlation workflow | Stable delivery receipt or terminal failure |
| Idempotency receipt | Owning backend | Configured receipt horizon |

Keys such as `eventKey` and `rootEventKeys` are cause lineage, not payload
references. Eve Ambient exposes no operation that resolves them to an event.
There is no replay or historical event API.

## Current transition

This implementation is additive until the RFC's backend work is complete:

- `AttentionEngine`, fan-out compilation, v2 lineage, and callback values are
  exported from the package root.
- `MemoryAttentionEngine` is exported from the memory entry point.
- The existing `MonitorRuntime`, `MonitorStore`, celld mailbox, PostgreSQL
  store, migrations, and examples continue to work unchanged.

The celld engine, PostgreSQL engine, rule-to-fan-out publisher, direct-chat
adapter split, and removal of the legacy public store model are subsequent RFC
implementation pull requests. Temporary coexistence is not a compatibility
promise for the final major release.
