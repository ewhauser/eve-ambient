import {
  createWorkflowAttentionCallbackHandler,
  WorkflowAttentionEngine,
} from "@ewhauser/eve-ambient/workflow";
import type {
  AttentionDeliveryReceipt,
  FrozenAttentionBatch,
  PreparedAttentionWake,
} from "@ewhauser/eve-ambient/protocol";
import { getWorld, setWorld } from "workflow/runtime";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createCountingWorld, waitForStableWorldCalls } from "./counting-world.js";
import { closeServers, fanout, SECRET_ENV, serve, uniqueNamespace } from "./helpers.js";

afterEach(async () => {
  await closeServers();
  delete process.env[SECRET_ENV];
});

describe("standard Workflow correlation runtime", () => {
  it("keeps warm admission within the measured World-call budget", async () => {
    const applicationCalls = { prepare: 0, deliver: 0 };
    const callbackUrl = await serve(createWorkflowAttentionCallbackHandler({
      async prepare() {
        applicationCalls.prepare += 1;
        return {
          kind: "wake",
          routeId: "eve",
          target: "session:incident-42",
          instruction: "Investigate the events.",
          decision: null,
          evidence: null,
        };
      },
      async deliver(wake): Promise<AttentionDeliveryReceipt> {
        applicationCalls.deliver += 1;
        return {
          wakeKey: wake.wakeKey,
          inputHash: wake.inputHash,
          deliveredAt: new Date().toISOString(),
          result: null,
        };
      },
    }, { secretEnv: SECRET_ENV }));
    process.env[SECRET_ENV] = "integration-secret";

    const originalWorld = await getWorld();
    const countingWorld = createCountingWorld(originalWorld);
    setWorld(countingWorld.world);
    const engine = new WorkflowAttentionEngine({
      namespace: uniqueNamespace("cost"),
      callbackUrl,
      callbackSecretEnv: SECRET_ENV,
    });
    const policy = {
      buffer: {
        mode: "debounce" as const,
        quietPeriodMs: 5 * 60_000,
        maxWaitMs: 5 * 60_000,
        maxEvents: 2,
        maxBytes: 1_000_000,
      },
    };

    try {
      await engine.accept(await fanout("cost-cold", "01", policy));
      await waitForStableWorldCalls(countingWorld);
      const cold = countingWorld.snapshot();

      countingWorld.reset();
      await engine.accept(await fanout("cost-warm-buffer", "02", policy));
      await waitForStableWorldCalls(countingWorld);
      const warm = countingWorld.snapshot();

      countingWorld.reset();
      await engine.accept(await fanout("cost-close", "03", policy));
      await vi.waitFor(() => expect(applicationCalls).toEqual({ prepare: 1, deliver: 1 }), {
        timeout: 10_000,
      });
      await waitForStableWorldCalls(countingWorld);
      const close = countingWorld.snapshot();

      process.stdout.write(`\nWORKFLOW_CORRELATION_CALL_REPORT ${JSON.stringify({
        cold: withoutTrace(cold),
        warmBufferOnly: withoutTrace(warm),
        batchCloseAndDelivery: withoutTrace(close),
        applicationHttp: { warmBufferOnly: 0, batchCloseAndDelivery: 2 },
      }, null, 2)}\n`);

      expect(warm.total).toBeLessThanOrEqual(7);
      expect(close.total).toBeLessThanOrEqual(20);
    } finally {
      setWorld(originalWorld);
    }
  });

  it("elects one cold owner without losing concurrently accepted events", async () => {
    const preparedBatchKeys = new Set<string>();
    const callbackUrl = await serve(createWorkflowAttentionCallbackHandler({
      async prepare(batch) {
        preparedBatchKeys.add(batch.batchKey);
        return { kind: "ignore", decision: null };
      },
      async deliver() {
        throw new Error("delivery must not run");
      },
    }, { secretEnv: SECRET_ENV }));
    process.env[SECRET_ENV] = "integration-secret";
    const engine = new WorkflowAttentionEngine({
      namespace: uniqueNamespace("cold-race"),
      callbackUrl,
      callbackSecretEnv: SECRET_ENV,
    });
    const policy = { buffer: { mode: "immediate" as const } };

    const receipts = await Promise.all(Array.from({ length: 20 }, async (_, index) =>
      engine.accept(await fanout(`cold-${index}`, String(index).padStart(2, "0"), policy))));

    expect(receipts).toHaveLength(20);
    await vi.waitFor(() => expect(preparedBatchKeys.size).toBe(20), { timeout: 20_000 });
  });

  it("accepts duplicates and reducer conflicts asynchronously without duplicating work", async () => {
    let prepared = 0;
    const callbackUrl = await serve(createWorkflowAttentionCallbackHandler({
      async prepare() {
        prepared += 1;
        return { kind: "ignore", decision: null };
      },
      async deliver() {
        throw new Error("delivery must not run");
      },
    }, { secretEnv: SECRET_ENV }));
    process.env[SECRET_ENV] = "integration-secret";
    const engine = new WorkflowAttentionEngine({
      namespace: uniqueNamespace("dedup"),
      callbackUrl,
      callbackSecretEnv: SECRET_ENV,
    });
    const policy = { buffer: { mode: "immediate" as const } };
    const original = await fanout("same-event", "01", policy);

    await engine.accept(original);
    await engine.accept(original);
    await engine.accept(await fanout("same-event", "02", policy));

    await vi.waitFor(() => expect(prepared).toBe(1), { timeout: 10_000 });
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(prepared).toBe(1);
  });

  it("retries the exact prepared batch and checkpointed wake", async () => {
    const prepareCalls: FrozenAttentionBatch[] = [];
    const deliveryCalls: PreparedAttentionWake[] = [];
    let failPrepare = true;
    let failDelivery = true;
    const callbackUrl = await serve(createWorkflowAttentionCallbackHandler({
      async prepare(batch) {
        prepareCalls.push(structuredClone(batch));
        if (failPrepare) {
          failPrepare = false;
          throw new Error("lost prepare response");
        }
        return {
          kind: "wake",
          routeId: "eve",
          target: "session:incident-42",
          instruction: "Investigate.",
          decision: { stable: true },
          evidence: { stable: true },
        };
      },
      async deliver(wake): Promise<AttentionDeliveryReceipt> {
        deliveryCalls.push(structuredClone(wake));
        if (failDelivery) {
          failDelivery = false;
          throw new Error("lost delivery response");
        }
        return {
          wakeKey: wake.wakeKey,
          inputHash: wake.inputHash,
          deliveredAt: new Date().toISOString(),
          result: { delivered: true },
        };
      },
    }, { secretEnv: SECRET_ENV }));
    process.env[SECRET_ENV] = "integration-secret";
    const engine = new WorkflowAttentionEngine({
      namespace: uniqueNamespace("retry"),
      callbackUrl,
      callbackSecretEnv: SECRET_ENV,
      retryDelayMs: 5,
      maxAttempts: 3,
    });

    await engine.accept(await fanout("retry-event", "01", {
      buffer: { mode: "immediate" },
    }));

    await vi.waitFor(() => {
      expect(prepareCalls).toHaveLength(2);
      expect(deliveryCalls).toHaveLength(2);
    }, { timeout: 20_000 });
    expect(prepareCalls[1]).toEqual(prepareCalls[0]);
    expect(deliveryCalls[1]).toEqual(deliveryCalls[0]);
    expect(deliveryCalls[1]?.wakeKey).toBe(deliveryCalls[0]?.wakeKey);
  });

  it("keeps one permanent run after the 48-message ring wraps", async () => {
    const callbackUrl = await serve(createWorkflowAttentionCallbackHandler({
      async prepare() {
        return { kind: "ignore", decision: null };
      },
      async deliver() {
        throw new Error("delivery must not run");
      },
    }, { secretEnv: SECRET_ENV }));
    process.env[SECRET_ENV] = "integration-secret";
    const before = await runIds();
    const engine = new WorkflowAttentionEngine({
      namespace: uniqueNamespace("permanent"),
      callbackUrl,
      callbackSecretEnv: SECRET_ENV,
    });
    const policy = {
      buffer: {
        mode: "debounce" as const,
        quietPeriodMs: 24 * 60 * 60_000,
        maxWaitMs: 24 * 60 * 60_000,
        maxEvents: 1_000,
        maxBytes: 1_000_000,
      },
    };

    for (let index = 0; index < 50; index += 1) {
      await engine.accept(await fanout(`ring-${index}`, String(index).padStart(2, "0"), policy));
    }
    const afterWrap = await newRunIds(before);
    expect(afterWrap).toHaveLength(1);

    await engine.accept(await fanout("ring-50", "50", policy));
    expect(await newRunIds(before)).toEqual(afterWrap);
  });
});

async function runIds(): Promise<Set<string>> {
  const world = await getWorld();
  const response = await world.runs.list({
    pagination: { limit: 1_000 },
    resolveData: "none",
  });
  return new Set(response.data.map((run) => run.runId));
}

async function newRunIds(before: ReadonlySet<string>): Promise<string[]> {
  const current = await runIds();
  return [...current].filter((runId) => !before.has(runId)).sort();
}

function withoutTrace<T extends { readonly trace: readonly string[] }>(
  snapshot: T,
): Omit<T, "trace"> {
  const { trace: _trace, ...summary } = snapshot;
  return summary;
}
