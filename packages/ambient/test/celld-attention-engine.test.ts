import { describe, expect, it } from "vitest";
import { compileAcceptedFanout } from "../src/attention.js";
import { CelldAttentionEngine, createAttentionCallbackFetchHandler } from "../src/celld.js";
import {
  canonicalizeChannelDelivery,
  defineChannelCanonicalization,
} from "../src/idempotency.js";
import { defineAttentionEngineConformance } from "./attention-conformance.js";
import { FakeCelldFleet } from "./celld-harness.js";

defineAttentionEngineConformance("celld", (options) => {
  const secret = "test-secret";
  const callbacks = createAttentionCallbackFetchHandler(options.callbacks, { secret });
  const limits: Record<string, number> = {};
  if (options.dedupeMs !== undefined) limits.ATTENTION_DEDUPE_MS = options.dedupeMs;
  if (options.retryDelayMs !== undefined) {
    limits.ATTENTION_RETRY_DELAY_MS = options.retryDelayMs;
  }
  if (options.claimLeaseMs !== undefined) {
    limits.ATTENTION_CLAIM_LEASE_MS = options.claimLeaseMs;
  }
  if (options.maxAttempts !== undefined) limits.ATTENTION_MAX_ATTEMPTS = options.maxAttempts;
  if (options.maxBranches !== undefined) limits.ATTENTION_MAX_BRANCHES = options.maxBranches;
  if (options.maxFanoutBytes !== undefined) {
    limits.ATTENTION_MAX_FANOUT_BYTES = options.maxFanoutBytes;
  }
  if (options.maxPreparedWakeBytes !== undefined) {
    limits.ATTENTION_MAX_PREPARED_WAKE_BYTES = options.maxPreparedWakeBytes;
  }
  const fleet = new FakeCelldFleet({
    secret,
    clock: options.clock,
    callbacks,
    limits,
    ...(options.faults === undefined ? {} : { faults: options.faults }),
  });
  const engine = new CelldAttentionEngine({
    url: fleet.baseUrl,
    secret,
    fetch: fleet.fetch as typeof fetch,
  });
  return {
    engine,
    async runDue() {
      const start = fleet.outcomes.length;
      await fleet.fireDueAlarms();
      const outcomes = fleet.outcomes.slice(start);
      return {
        claimed: outcomes.length,
        ignored: outcomes.filter((outcome) => outcome === "ignored").length,
        shadowed: outcomes.filter((outcome) => outcome === "shadowed").length,
        delivered: outcomes.filter((outcome) => outcome === "delivered").length,
        failed: outcomes.filter((outcome) => outcome === "failed").length,
        terminalFailures: outcomes.filter((outcome) => outcome === "terminal-failure").length,
      };
    },
    diagnostics: () => fleet.diagnostics(),
  };
});

describe("celld partition placement", () => {
  it("places many events for one channel partition in one cell", async () => {
    const secret = "test-secret";
    const clock = { now: () => new Date("2026-01-01T00:00:00.000Z") };
    const callbacks = createAttentionCallbackFetchHandler({
      prepare: async () => ({ kind: "ignore", decision: null }),
      deliver: async () => {
        throw new Error("delivery is not expected in this placement test");
      },
    }, { secret });
    const fleet = new FakeCelldFleet({ secret, clock, callbacks });
    const engine = new CelldAttentionEngine({
      url: fleet.baseUrl,
      secret,
      fetch: fleet.fetch as typeof fetch,
    });

    await engine.accept(await placementFanout("event-1", "pull-20"));
    await engine.accept(await placementFanout("event-2", "pull-20"));
    await engine.accept(await placementFanout("event-3", "pull-21"));

    expect(fleet.cellNames).toHaveLength(2);
    expect(fleet.cellNames.every((name) => /^eve:partition:v1:/.test(name))).toBe(true);
    const placements = fleet.requests.filter((request) => request.action === "accept").map(
      (request) => request.name,
    );
    expect(placements[0]).toBe(placements[1]);
    expect(placements[2]).not.toBe(placements[0]);
  });
});

async function placementFanout(eventId: string, partition: string) {
  const source = await canonicalizeChannelDelivery(
    defineChannelCanonicalization<
      { readonly eventId: string; readonly partition: string },
      ReturnType<typeof placementEvent>
    >({
      version: 1,
      canonicalize: (raw) => placementEvent(raw.eventId, raw.partition),
      partitionKey: (event) => event.data.partition,
    }),
    { eventId, partition },
    { applicationId: "placement-test" },
  );
  return compileAcceptedFanout({ source, branches: [] });
}

function placementEvent(eventId: string, partition: string) {
  return {
    id: eventId,
    type: "test.event",
    version: 1,
    data: { partition },
    source: {
      channelId: "test",
      installationId: "installation-1",
      tenantId: "tenant-1",
    },
    origin: { kind: "external" as const, depth: 0 },
  };
}
