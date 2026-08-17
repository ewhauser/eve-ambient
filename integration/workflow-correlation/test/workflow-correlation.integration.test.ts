import {
  createWorkflowAttentionCallbackHandler,
  WorkflowAttentionEngine,
} from "@ewhauser/eve-ambient/workflow";
import type {
  AttentionDeliveryReceipt,
  FrozenAttentionBatch,
  PreparedAttentionWake,
} from "@ewhauser/eve-ambient/protocol";
import { compileAttentionStreamAppends } from "@ewhauser/eve-ambient/protocol";
import { hashIdempotencyInput } from "@ewhauser/eve-ambient/idempotency";
import { correlationWorkflow } from "@ewhauser/eve-ambient/workflows";
import { getHookByToken, resumeHook, start } from "workflow/api";
import { HookNotFoundError } from "workflow/errors";
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

      expect(cold.eventTypes["run_created"]).toBe(1);
      expect(cold.eventTypes["hook_received"] ?? 0).toBe(0);
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
    const before = await runIds();
    const originalWorld = await getWorld();
    const countingWorld = createCountingWorld(originalWorld);
    setWorld(countingWorld.world);
    const engine = new WorkflowAttentionEngine({
      namespace: uniqueNamespace("cold-race"),
      callbackUrl,
      callbackSecretEnv: SECRET_ENV,
    });
    const policy = { buffer: { mode: "immediate" as const } };
    const inputs = await Promise.all(Array.from({ length: 20 }, (_, index) =>
      fanout(`cold-${index}`, String(index).padStart(2, "0"), policy)));

    try {
      const receipts = await Promise.all(inputs.map((input) => engine.accept(input)));

      expect(receipts).toHaveLength(20);
      await vi.waitFor(() => expect(preparedBatchKeys.size).toBe(20), { timeout: 20_000 });
      await waitForStableWorldCalls(countingWorld);
      const coldBurst = countingWorld.snapshot();
      process.stdout.write(`\nWORKFLOW_CORRELATION_COLD_BURST_REPORT ${JSON.stringify({
        publishers: 20,
        ...withoutTrace(coldBurst),
      }, null, 2)}\n`);

      expect(coldBurst.eventTypes["run_created"]).toBe(1);
      // Workflow's resilient resume path records each accepted follower input
      // from both the publisher and queue consumer; 38 writes represent 19 resumes.
      expect(coldBurst.eventTypes["hook_received"]).toBe(38);
      expect(await newRunIds(before)).toHaveLength(1);
    } finally {
      setWorld(originalWorld);
    }
  });

  it("routes a seeded append from a losing cross-process candidate to the owner", async () => {
    const preparedEventIds: string[] = [];
    const callbackUrl = await serve(createWorkflowAttentionCallbackHandler({
      async prepare(batch) {
        preparedEventIds.push(...batch.branches.map((branch) => branch.event.id));
        return { kind: "ignore", decision: null };
      },
      async deliver() {
        throw new Error("delivery must not run");
      },
    }, { secretEnv: SECRET_ENV }));
    process.env[SECRET_ENV] = "integration-secret";
    const before = await runIds();
    const config = {
      namespace: uniqueNamespace("candidate-conflict"),
      callbackUrl,
      callbackSecretEnv: SECRET_ENV,
      preparePath: "/ambient/prepare",
      deliverPath: "/ambient/deliver",
      maxRecentMessages: 48,
      claimLeaseMs: 30_000,
      retryDelayMs: 1_000,
      maxAttempts: 10,
      maxPreparedWakeBytes: 1 * 1_024 * 1_024,
      maxPendingBranches: 1_000,
      maxPendingBytes: 16 * 1_024 * 1_024,
    };
    const policy = { buffer: { mode: "immediate" as const } };
    const firstAppend = (await compileAttentionStreamAppends(
      await fanout("candidate-first", "01", policy),
    ))[0]!;
    const secondAppend = (await compileAttentionStreamAppends(
      await fanout("candidate-second", "02", policy),
    ))[0]!;
    const firstCommand = {
      kind: "append" as const,
      append: firstAppend,
      acceptedAt: new Date().toISOString(),
    };
    const secondCommand = {
      kind: "append" as const,
      append: secondAppend,
      acceptedAt: new Date().toISOString(),
    };

    const [firstCandidate, secondCandidate] = await Promise.all([
      start(correlationWorkflow, [config, firstAppend.streamKey, firstCommand]),
      start(correlationWorkflow, [config, secondAppend.streamKey, secondCommand]),
    ]);
    const configHash = await hashIdempotencyInput({ protocolVersion: 1, ...config });
    const token = `eve-ambient:correlation:${config.namespace}:${configHash}:${firstAppend.streamKey}`;
    const owner = await waitForHook(token);
    const losingCommand = owner.runId === firstCandidate.runId ? secondCommand : firstCommand;
    expect(owner.runId === firstCandidate.runId || owner.runId === secondCandidate.runId).toBe(true);

    await resumeHook(owner, losingCommand);

    await vi.waitFor(() => expect(preparedEventIds).toHaveLength(2), { timeout: 20_000 });
    expect(preparedEventIds.sort()).toEqual(["candidate-first", "candidate-second"]);
    expect(await newRunIds(before)).toHaveLength(2);
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

  it("contains an oversized prepared wake and keeps the correlation owner alive", async () => {
    let prepareCalls = 0;
    let deliverCalls = 0;
    const callbackUrl = await serve(createWorkflowAttentionCallbackHandler({
      async prepare() {
        prepareCalls += 1;
        if (prepareCalls === 1) {
          return {
            kind: "wake",
            routeId: "eve",
            target: "session:incident-42",
            instruction: "x".repeat(2_048),
            decision: null,
            evidence: null,
          };
        }
        return { kind: "ignore", decision: null };
      },
      async deliver() {
        deliverCalls += 1;
        throw new Error("oversized prepared wake must not be delivered");
      },
    }, { secretEnv: SECRET_ENV }));
    process.env[SECRET_ENV] = "integration-secret";
    const before = await runIds();
    const engine = new WorkflowAttentionEngine({
      namespace: uniqueNamespace("oversized-wake"),
      callbackUrl,
      callbackSecretEnv: SECRET_ENV,
      maxPreparedWakeBytes: 256,
    });

    await engine.accept(await fanout("oversized-wake", "01", {
      buffer: { mode: "immediate" },
    }));
    await vi.waitFor(() => expect(prepareCalls).toBe(1), { timeout: 10_000 });
    await engine.accept(await fanout("after-oversized-wake", "02", {
      buffer: { mode: "immediate" },
    }));

    await vi.waitFor(() => expect(prepareCalls).toBe(2), { timeout: 10_000 });
    expect(deliverCalls).toBe(0);
    expect(await newRunIds(before)).toHaveLength(1);
  });

  it("contains an invalid delivery receipt and processes the next message", async () => {
    let prepareCalls = 0;
    let deliverCalls = 0;
    const callbackUrl = await serve(createWorkflowAttentionCallbackHandler({
      async prepare() {
        prepareCalls += 1;
        return {
          kind: "wake",
          routeId: "eve",
          target: "session:incident-42",
          instruction: "Investigate.",
          decision: null,
          evidence: null,
        };
      },
      async deliver(wake): Promise<AttentionDeliveryReceipt> {
        deliverCalls += 1;
        return {
          wakeKey: deliverCalls === 1 ? "invalid-wake-key" : wake.wakeKey,
          inputHash: wake.inputHash,
          deliveredAt: new Date().toISOString(),
          result: null,
        } as AttentionDeliveryReceipt;
      },
    }, { secretEnv: SECRET_ENV }));
    process.env[SECRET_ENV] = "integration-secret";
    const before = await runIds();
    const engine = new WorkflowAttentionEngine({
      namespace: uniqueNamespace("invalid-receipt"),
      callbackUrl,
      callbackSecretEnv: SECRET_ENV,
    });

    await engine.accept(await fanout("invalid-receipt", "01", {
      buffer: { mode: "immediate" },
    }));
    await vi.waitFor(() => expect(deliverCalls).toBe(1), { timeout: 10_000 });
    await engine.accept(await fanout("after-invalid-receipt", "02", {
      buffer: { mode: "immediate" },
    }));

    await vi.waitFor(() => {
      expect(prepareCalls).toBe(2);
      expect(deliverCalls).toBe(2);
    }, { timeout: 10_000 });
    expect(await newRunIds(before)).toHaveLength(1);
  });

  it("leaves overflow queued in the hook while reducer payloads stay bounded", async () => {
    const batches: string[][] = [];
    const callbackUrl = await serve(createWorkflowAttentionCallbackHandler({
      async prepare(batch) {
        batches.push(batch.branches.map((branch) => branch.event.id));
        return {
          kind: "wake",
          routeId: "eve",
          target: "session:incident-42",
          instruction: "Investigate.",
          decision: null,
          evidence: null,
        };
      },
      async deliver(wake): Promise<AttentionDeliveryReceipt> {
        return {
          wakeKey: wake.wakeKey,
          inputHash: wake.inputHash,
          deliveredAt: new Date().toISOString(),
          result: null,
        };
      },
    }, { secretEnv: SECRET_ENV }));
    process.env[SECRET_ENV] = "integration-secret";
    const engine = new WorkflowAttentionEngine({
      namespace: uniqueNamespace("backpressure"),
      callbackUrl,
      callbackSecretEnv: SECRET_ENV,
      maxPendingBranches: 2,
    });
    const policy = {
      buffer: { mode: "immediate" as const },
      cooldownAfterWakeMs: 50,
    };

    await engine.accept(await fanout("bounded-0", "00", policy));
    await vi.waitFor(() => expect(batches).toHaveLength(1), { timeout: 10_000 });
    await Promise.all(Array.from({ length: 6 }, async (_, index) =>
      engine.accept(await fanout(
        `bounded-${index + 1}`,
        String(index + 1).padStart(2, "0"),
        policy,
      ))));

    const expectedEventIds = new Set(Array.from(
      { length: 7 },
      (_, index) => `bounded-${index}`,
    ));
    await vi.waitFor(() => {
      expect(new Set(batches.flat())).toEqual(expectedEventIds);
    }, { timeout: 20_000 });
    expect(batches.every((batch) => batch.length <= 2)).toBe(true);
  });

  it("starts a new correlation owner when immutable Workflow configuration changes", async () => {
    let firstCallbacks = 0;
    let secondCallbacks = 0;
    const firstCallbackUrl = await serve(createWorkflowAttentionCallbackHandler({
      async prepare() {
        firstCallbacks += 1;
        return { kind: "ignore", decision: null };
      },
      async deliver() {
        throw new Error("delivery must not run");
      },
    }, { secretEnv: SECRET_ENV }));
    const secondCallbackUrl = await serve(createWorkflowAttentionCallbackHandler({
      async prepare() {
        secondCallbacks += 1;
        return { kind: "ignore", decision: null };
      },
      async deliver() {
        throw new Error("delivery must not run");
      },
    }, { secretEnv: SECRET_ENV }));
    process.env[SECRET_ENV] = "integration-secret";
    const before = await runIds();
    const namespace = uniqueNamespace("config-cutover");
    const first = new WorkflowAttentionEngine({
      namespace,
      callbackUrl: firstCallbackUrl,
      callbackSecretEnv: SECRET_ENV,
    });
    const second = new WorkflowAttentionEngine({
      namespace,
      callbackUrl: secondCallbackUrl,
      callbackSecretEnv: SECRET_ENV,
    });

    await first.accept(await fanout("before-cutover", "01", {
      buffer: { mode: "immediate" },
    }));
    await vi.waitFor(() => expect(firstCallbacks).toBe(1), { timeout: 10_000 });
    await second.accept(await fanout("after-cutover", "02", {
      buffer: { mode: "immediate" },
    }));

    await vi.waitFor(() => expect(secondCallbacks).toBe(1), { timeout: 10_000 });
    expect(firstCallbacks).toBe(1);
    expect(await newRunIds(before)).toHaveLength(2);
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

async function waitForHook(token: string): Promise<Awaited<ReturnType<typeof getHookByToken>>> {
  const deadline = Date.now() + 10_000;
  for (;;) {
    try {
      return await getHookByToken(token);
    } catch (error) {
      if (!HookNotFoundError.is(error) || Date.now() >= deadline) throw error;
    }
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function withoutTrace<T extends { readonly trace: readonly string[] }>(
  snapshot: T,
): Omit<T, "trace"> {
  const { trace: _trace, ...summary } = snapshot;
  return summary;
}
