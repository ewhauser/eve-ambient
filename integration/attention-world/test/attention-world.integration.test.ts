import {
  canonicalizeChannelDelivery,
  defineChannelCanonicalization,
} from "@ewhauser/eve-ambient/idempotency";
import {
  applyAttentionStreamAppend,
  compileAcceptedFanout,
  type AcceptedFanout,
  type AttentionBranchPlan,
  type AttentionStreamAppend,
  type AttentionStreamAppendReceipt,
  type AttentionStreamState,
  type AttentionWorld,
} from "@ewhauser/eve-ambient/protocol";
import { WorldAttentionEngine } from "@ewhauser/eve-ambient/world";
import { describe, expect, it } from "vitest";

describe("correlation-addressed Attention World", () => {
  it("makes exactly one append call per distinct correlation stream", async () => {
    const world = new InstrumentedWorld();
    const engine = new WorldAttentionEngine({ world });

    await engine.accept(await fanout("event-1", [plan({ correlationKey: "incident-42" })]));
    await engine.accept(await fanout("event-2", [plan({ correlationKey: "incident-42" })]));
    await engine.accept(
      await fanout("event-3", [
        plan({ correlationKey: "incident-42" }),
        plan({ correlationKey: "tenant-rollup" }),
      ]),
    );

    expect(world.calls).toHaveLength(4);
    expect(new Set(world.calls.slice(0, 2).map((call) => call.streamKey)).size).toBe(1);
    expect(new Set(world.calls.slice(2).map((call) => call.streamKey)).size).toBe(2);
    expect(world.maximumConcurrentCalls).toBe(2);
  });

  it("uses a bounded recent-message ring for best-effort receiver dedup", async () => {
    const world = new InstrumentedWorld(2);
    const engine = new WorldAttentionEngine({ world });
    const first = await fanout("event-1", [plan({ correlationKey: "incident-42" })]);

    await engine.accept(first);
    await engine.accept(first);
    expect(world.calls.map((call) => call.status)).toEqual(["appended", "duplicate"]);

    await engine.accept(await fanout("event-2", [plan({ correlationKey: "incident-42" })]));
    await engine.accept(await fanout("event-3", [plan({ correlationKey: "incident-42" })]));
    expect(world.states()[0]!.recentMessages).toHaveLength(2);
  });

  it("groups multiple branches for one correlation into one atomic append", async () => {
    const world = new InstrumentedWorld();
    const engine = new WorldAttentionEngine({ world });

    await engine.accept(
      await fanout("event-1", [
        plan({ phase: "observed", orderKey: "001" }),
        plan({ phase: "undispatched", orderKey: "002" }),
      ]),
    );

    expect(world.calls).toHaveLength(1);
    expect(world.appendSizes).toEqual([2]);
  });

  it("makes no append call for an empty fan-out", async () => {
    const world = new InstrumentedWorld();
    const engine = new WorldAttentionEngine({ world });
    await engine.accept(await fanout("event-1", []));
    expect(world.calls).toHaveLength(0);
  });
});

class InstrumentedWorld implements AttentionWorld {
  readonly calls: Array<{
    streamKey: string;
    eventKey: string;
    status: AttentionStreamAppendReceipt["status"];
  }> = [];
  maximumConcurrentCalls = 0;
  readonly appendSizes: number[] = [];
  readonly #maxRecentMessages: number;
  readonly #states = new Map<string, AttentionStreamState>();
  #activeCalls = 0;

  constructor(maxRecentMessages = 48) {
    this.#maxRecentMessages = maxRecentMessages;
  }

  stream(key: AttentionStreamAppend["streamKey"]) {
    return {
      append: async (append: AttentionStreamAppend) => {
        this.#activeCalls += 1;
        this.maximumConcurrentCalls = Math.max(this.maximumConcurrentCalls, this.#activeCalls);
        await Promise.resolve();
        const applied = await applyAttentionStreamAppend(this.#states.get(key), append, {
          now: "2026-08-15T00:00:00.000Z",
          maxRecentMessages: this.#maxRecentMessages,
        });
        this.#states.set(key, applied.state);
        this.calls.push({
          streamKey: key,
          eventKey: append.eventKey,
          status: applied.receipt.status,
        });
        this.appendSizes.push(append.branches.length);
        this.#activeCalls -= 1;
        return applied.receipt;
      },
    };
  }

  states(): readonly AttentionStreamState[] {
    return [...this.#states.values()];
  }
}

const channel = defineChannelCanonicalization({
  version: 1,
  canonicalize: (event: Event) => event,
  partitionKey: () => "tenant-1",
});

interface Event {
  readonly id: string;
  readonly type: "incident";
  readonly version: 1;
  readonly occurredAt: string;
  readonly data: { readonly summary: string };
  readonly source: {
    readonly channelId: string;
    readonly installationId: string;
    readonly tenantId: string;
  };
  readonly origin: { readonly kind: "external"; readonly depth: 0 };
}

async function fanout(
  eventId: string,
  branches: readonly AttentionBranchPlan[],
): Promise<AcceptedFanout> {
  const source = await canonicalizeChannelDelivery(
    channel,
    {
      id: eventId,
      type: "incident",
      version: 1,
      occurredAt: "2026-08-15T00:00:00.000Z",
      data: { summary: eventId },
      source: {
        channelId: "incidents",
        installationId: "production",
        tenantId: "tenant-1",
      },
      origin: { kind: "external", depth: 0 },
    },
    { applicationId: "integration" },
  );
  return compileAcceptedFanout({ source, branches });
}

function plan(overrides: Partial<AttentionBranchPlan> = {}): AttentionBranchPlan {
  return {
    monitorId: "incident-attention",
    definitionVersion: "v1",
    correlationKey: "incident-42",
    orderKey: "001",
    mode: "active",
    policy: { buffer: { mode: "immediate" } },
    ...overrides,
  };
}
