import {
  compileAcceptedFanout,
  type AttentionBranchPlan,
  type AttentionDeliveryReceipt,
  type FrozenAttentionBatch,
  type PreparedAttentionOutcome,
  type PreparedAttentionWake,
} from "@ewhauser/eve-ambient/protocol";
import {
  canonicalizeChannelDelivery,
  defineChannelCanonicalization,
  IdempotencyConflictError,
} from "@ewhauser/eve-ambient/idempotency";
import {
  createWorldAttentionCallbackHandler,
  WorldAttentionEngine,
} from "@ewhauser/eve-ambient/world";
import { createServer, type Server } from "node:http";
import { getHookByToken, getRun } from "workflow/api";
import { getWorld, setWorld } from "workflow/runtime";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createCountingWorld, waitForStableWorldCalls } from "./counting-world.js";

const SECRET_ENV = "AMBIENT_WORLD_TEST_SECRET";
const MAX_COLD_MESSAGE_WORLD_CALLS = 160;
const MAX_WARM_MESSAGE_WORLD_CALLS = 120;
const servers: Server[] = [];

afterEach(async () => {
  await Promise.all(
    servers.splice(0).map(
      (server) =>
        new Promise<void>((resolve, reject) =>
          server.close((error) => (error === undefined ? resolve() : reject(error))),
        ),
    ),
  );
  delete process.env[SECRET_ENV];
});

describe("WorldAttentionEngine", () => {
  it("measures the World boundary cost of cold and warm inbound messages", async () => {
    const applicationCalls = { prepare: 0, deliver: 0 };
    const callbackUrl = await serve(
      createWorldAttentionCallbackHandler(
        {
          async prepare(): Promise<PreparedAttentionOutcome> {
            applicationCalls.prepare += 1;
            return {
              kind: "wake",
              routeId: "eve",
              target: "session:incident-42",
              instruction: "Investigate the event.",
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
        },
        { secretEnv: SECRET_ENV },
      ),
    );
    process.env[SECRET_ENV] = "world-test-secret";
    const originalWorld = getWorld();
    const countingWorld = createCountingWorld(originalWorld);
    setWorld(countingWorld.world);

    try {
      const engine = new WorldAttentionEngine({
        engineId: uniqueEngineId(),
        callbackUrl,
        callbackSecretEnv: SECRET_ENV,
        receiptTimeoutMs: 10_000,
      });

      await engine.accept(await fanout("cost-event-cold", "first", [plan()]));
      await vi.waitFor(() => expect(applicationCalls.deliver).toBe(1), { timeout: 10_000 });
      await waitForStableWorldCalls(countingWorld);
      const coldCorrelation = countingWorld.snapshot();
      const coldApplicationCalls = { ...applicationCalls };

      countingWorld.reset();
      applicationCalls.prepare = 0;
      applicationCalls.deliver = 0;
      await engine.accept(await fanout("cost-event-warm", "second", [plan()]));
      await vi.waitFor(() => expect(applicationCalls.deliver).toBe(1), { timeout: 10_000 });
      await waitForStableWorldCalls(countingWorld);
      const warmCorrelation = countingWorld.snapshot();
      const warmApplicationCalls = { ...applicationCalls };

      const report = {
        firstMessageColdCorrelation: {
          world: withoutTrace(coldCorrelation),
          applicationHttp: coldApplicationCalls,
          endToEndCalls:
            coldCorrelation.total + coldApplicationCalls.prepare + coldApplicationCalls.deliver,
        },
        subsequentMessageWarmCorrelation: {
          world: withoutTrace(warmCorrelation),
          applicationHttp: warmApplicationCalls,
          endToEndCalls:
            warmCorrelation.total + warmApplicationCalls.prepare + warmApplicationCalls.deliver,
        },
      };
      process.stdout.write(`\nWORLD_CALL_REPORT ${JSON.stringify(report, null, 2)}\n`);

      // These are intentionally ceilings rather than exact values. Cold owner
      // election can schedule one extra replay depending on queue timing.
      expect(coldCorrelation.total).toBeLessThanOrEqual(MAX_COLD_MESSAGE_WORLD_CALLS);
      expect(warmCorrelation.total).toBeLessThanOrEqual(MAX_WARM_MESSAGE_WORLD_CALLS);
      expect(coldApplicationCalls).toEqual({ prepare: 1, deliver: 1 });
      expect(warmApplicationCalls).toEqual({ prepare: 1, deliver: 1 });
    } finally {
      setWorld(originalWorld);
    }
  });

  it("admits, executes, deduplicates, and conflicts through semantic World receipts", async () => {
    const prepareCalls: FrozenAttentionBatch[] = [];
    const deliveryCalls: PreparedAttentionWake[] = [];
    const effects = new Map<string, AttentionDeliveryReceipt>();
    const callbacks = {
      async prepare(batch: FrozenAttentionBatch): Promise<PreparedAttentionOutcome> {
        prepareCalls.push(structuredClone(batch));
        return {
          kind: "wake",
          routeId: "eve",
          target: "session:incident-42",
          instruction: "Investigate the event.",
          decision: { answer: "wake" },
          evidence: { complete: true },
        };
      },
      async deliver(wake: PreparedAttentionWake): Promise<AttentionDeliveryReceipt> {
        deliveryCalls.push(structuredClone(wake));
        const prior = effects.get(wake.wakeKey);
        const receipt =
          prior ??
          ({
            wakeKey: wake.wakeKey,
            inputHash: wake.inputHash,
            deliveredAt: new Date().toISOString(),
            result: { effect: `effect-${effects.size + 1}` },
          } satisfies AttentionDeliveryReceipt);
        effects.set(wake.wakeKey, receipt);
        return structuredClone(receipt);
      },
    };
    const callbackUrl = await serve(createWorldAttentionCallbackHandler(callbacks, { secretEnv: SECRET_ENV }));
    process.env[SECRET_ENV] = "world-test-secret";
    const engine = new WorldAttentionEngine({
      engineId: uniqueEngineId(),
      callbackUrl,
      callbackSecretEnv: SECRET_ENV,
      receiptTimeoutMs: 10_000,
    });
    const input = await fanout("event-1", "original", [plan()]);

    const [receipt, concurrentReceipt] = await Promise.all([
      engine.accept(input),
      engine.accept(input),
    ]);
    await vi.waitFor(() => expect(deliveryCalls).toHaveLength(1), { timeout: 10_000 });
    expect(concurrentReceipt).toEqual(receipt);
    await expect(engine.accept(input)).resolves.toEqual(receipt);
    await expect(
      engine.accept(await fanout("event-1", "changed", [plan()])),
    ).rejects.toBeInstanceOf(IdempotencyConflictError);
    await expect(
      engine.accept(
        await fanout("event-1", "original", [
          plan({
            policy: {
              buffer: { mode: "immediate" },
              cooldownAfterWakeMs: 1_000,
            },
          }),
        ]),
      ),
    ).rejects.toBeInstanceOf(IdempotencyConflictError);

    expect(receipt.branchKeys).toEqual([input.branches[0]!.branchKey]);
    expect(prepareCalls).toHaveLength(1);
    expect(deliveryCalls).toHaveLength(1);
    expect(effects).toHaveLength(1);
  });

  it("serializes concurrent events into one debounced correlation batch", async () => {
    const prepareCalls: FrozenAttentionBatch[] = [];
    const deliveryCalls: PreparedAttentionWake[] = [];
    const callbacks = {
      async prepare(batch: FrozenAttentionBatch): Promise<PreparedAttentionOutcome> {
        prepareCalls.push(structuredClone(batch));
        return {
          kind: "wake",
          routeId: "eve",
          target: "session:incident-42",
          instruction: "Investigate the events.",
          decision: null,
          evidence: null,
        };
      },
      async deliver(wake: PreparedAttentionWake): Promise<AttentionDeliveryReceipt> {
        deliveryCalls.push(structuredClone(wake));
        return {
          wakeKey: wake.wakeKey,
          inputHash: wake.inputHash,
          deliveredAt: new Date().toISOString(),
          result: null,
        };
      },
    };
    const callbackUrl = await serve(createWorldAttentionCallbackHandler(callbacks, { secretEnv: SECRET_ENV }));
    process.env[SECRET_ENV] = "world-test-secret";
    const engine = new WorldAttentionEngine({
      engineId: uniqueEngineId(),
      callbackUrl,
      callbackSecretEnv: SECRET_ENV,
    });
    const policy = {
      buffer: {
        mode: "debounce" as const,
        quietPeriodMs: 250,
        maxWaitMs: 2_000,
        maxEvents: 100,
        maxBytes: 1_000_000,
      },
    };

    await Promise.all([
      engine.accept(await fanout("event-z", "last", [plan({ orderKey: "z", policy })])),
      engine.accept(await fanout("event-a", "first", [plan({ orderKey: "a", policy })])),
    ]);
    await vi.waitFor(() => expect(deliveryCalls).toHaveLength(1), { timeout: 10_000 });

    expect(prepareCalls).toHaveLength(1);
    expect(prepareCalls[0]!.branches.map((branch) => branch.orderKey)).toEqual(["a", "z"]);
    expect(prepareCalls[0]!.branches.map((branch) => branch.event.data)).toEqual([
      { body: "first" },
      { body: "last" },
    ]);
  });

  it("retries delivery with the exact checkpointed wake after a lost callback response", async () => {
    const deliveryCalls: PreparedAttentionWake[] = [];
    const effects = new Map<string, AttentionDeliveryReceipt>();
    let loseResponse = true;
    const callbacks = {
      async prepare(): Promise<PreparedAttentionOutcome> {
        return {
          kind: "wake",
          routeId: "eve",
          target: "session:incident-42",
          instruction: "Investigate the event.",
          decision: { original: true },
          evidence: { stable: true },
        };
      },
      async deliver(wake: PreparedAttentionWake): Promise<AttentionDeliveryReceipt> {
        deliveryCalls.push(structuredClone(wake));
        const prior = effects.get(wake.wakeKey);
        const receipt =
          prior ??
          ({
            wakeKey: wake.wakeKey,
            inputHash: wake.inputHash,
            deliveredAt: new Date().toISOString(),
            result: { effect: "once" },
          } satisfies AttentionDeliveryReceipt);
        effects.set(wake.wakeKey, receipt);
        if (loseResponse) {
          loseResponse = false;
          throw new Error("delivery response lost");
        }
        return structuredClone(receipt);
      },
    };
    const callbackUrl = await serve(createWorldAttentionCallbackHandler(callbacks, { secretEnv: SECRET_ENV }));
    process.env[SECRET_ENV] = "world-test-secret";
    const engine = new WorldAttentionEngine({
      engineId: uniqueEngineId(),
      callbackUrl,
      callbackSecretEnv: SECRET_ENV,
      retryDelayMs: 25,
      maxAttempts: 2,
    });

    await engine.accept(await fanout("event-retry", "original", [plan()]));
    await vi.waitFor(() => expect(deliveryCalls).toHaveLength(2), { timeout: 10_000 });

    expect(deliveryCalls[1]).toEqual(deliveryCalls[0]);
    expect(effects).toHaveLength(1);
  });

  it("bounds callback duration and retries timed-out preparation", async () => {
    let prepareCalls = 0;
    let deliveryCalls = 0;
    const callbackUrl = await serve(
      createWorldAttentionCallbackHandler(
        {
          async prepare(): Promise<PreparedAttentionOutcome> {
            prepareCalls += 1;
            await new Promise((resolve) => setTimeout(resolve, 100));
            return { kind: "ignore", decision: null };
          },
          async deliver(): Promise<AttentionDeliveryReceipt> {
            deliveryCalls += 1;
            throw new Error("delivery must not run");
          },
        },
        { secretEnv: SECRET_ENV },
      ),
    );
    process.env[SECRET_ENV] = "world-test-secret";
    const engine = new WorldAttentionEngine({
      engineId: uniqueEngineId(),
      callbackUrl,
      callbackSecretEnv: SECRET_ENV,
      callbackTimeoutMs: 20,
      retryDelayMs: 10,
      maxAttempts: 2,
    });

    await engine.accept(await fanout("event-timeout", "original", [plan()]));
    await vi.waitFor(() => expect(prepareCalls).toBe(2), { timeout: 10_000 });
    expect(deliveryCalls).toBe(0);
  });

  it("recreates a cancelled event-expiry sleep after duplicate input", async () => {
    const callbackUrl = await serve(
      createWorldAttentionCallbackHandler(
        {
          async prepare(): Promise<PreparedAttentionOutcome> {
            return { kind: "ignore", decision: null };
          },
          async deliver(): Promise<AttentionDeliveryReceipt> {
            throw new Error("delivery must not run");
          },
        },
        { secretEnv: SECRET_ENV },
      ),
    );
    process.env[SECRET_ENV] = "world-test-secret";
    const engineId = uniqueEngineId();
    const engine = new WorldAttentionEngine({
      engineId,
      callbackUrl,
      callbackSecretEnv: SECRET_ENV,
      dedupeMs: 1_000,
    });
    const input = await fanout("event-expiry", "original", [plan()]);

    await engine.accept(input);
    const owner = await getHookByToken(
      `eve-ambient:event:${engineId}:${input.eventKey}`,
    );
    await engine.accept(input);

    await vi.waitFor(
      async () => expect(await getRun(owner.runId).status).toBe("completed"),
      { timeout: 10_000 },
    );
  });

  it("recreates a cancelled hook read after a cooldown timer wins", async () => {
    const deliveryCalls: PreparedAttentionWake[] = [];
    const callbackUrl = await serve(
      createWorldAttentionCallbackHandler(
        {
          async prepare(): Promise<PreparedAttentionOutcome> {
            return {
              kind: "wake",
              routeId: "eve",
              target: "session:incident-42",
              instruction: "Investigate the event.",
              decision: null,
              evidence: null,
            };
          },
          async deliver(wake): Promise<AttentionDeliveryReceipt> {
            deliveryCalls.push(structuredClone(wake));
            return {
              wakeKey: wake.wakeKey,
              inputHash: wake.inputHash,
              deliveredAt: new Date().toISOString(),
              result: null,
            };
          },
        },
        { secretEnv: SECRET_ENV },
      ),
    );
    process.env[SECRET_ENV] = "world-test-secret";
    const engine = new WorldAttentionEngine({
      engineId: uniqueEngineId(),
      callbackUrl,
      callbackSecretEnv: SECRET_ENV,
      dedupeMs: 2_000,
    });
    const policy = {
      buffer: { mode: "immediate" as const },
      cooldownAfterWakeMs: 100,
    };

    await engine.accept(await fanout("event-before-cooldown", "first", [plan({ policy })]));
    await vi.waitFor(() => expect(deliveryCalls).toHaveLength(1), { timeout: 10_000 });
    await new Promise((resolve) => setTimeout(resolve, 250));
    await engine.accept(await fanout("event-after-cooldown", "second", [plan({ policy })]));
    await vi.waitFor(() => expect(deliveryCalls).toHaveLength(2), { timeout: 10_000 });
  });
});

async function serve(handler: (request: Request) => Promise<Response>): Promise<string> {
  const server = createServer(async (incoming, outgoing) => {
    const chunks: Buffer[] = [];
    for await (const chunk of incoming) chunks.push(Buffer.from(chunk));
    const address = server.address();
    if (address === null || typeof address === "string") throw new Error("test server has no port");
    const request = new Request(`http://127.0.0.1:${address.port}${incoming.url ?? "/"}`, {
      method: incoming.method ?? "GET",
      headers: incoming.headers as HeadersInit,
      ...(chunks.length === 0 ? {} : { body: Buffer.concat(chunks) }),
    });
    const response = await handler(request);
    outgoing.statusCode = response.status;
    response.headers.forEach((value, name) => outgoing.setHeader(name, value));
    outgoing.end(Buffer.from(await response.arrayBuffer()));
  });
  servers.push(server);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("test server has no port");
  return `http://127.0.0.1:${address.port}`;
}

async function fanout(
  eventId: string,
  body: string,
  branches: readonly AttentionBranchPlan[],
) {
  const source = await canonicalizeChannelDelivery(
    defineChannelCanonicalization<
      { readonly eventId: string; readonly body: string },
      ReturnType<typeof canonicalEvent>
    >({
      version: 1,
      canonicalize: (raw) => canonicalEvent(raw.eventId, raw.body),
      partitionKey: () => "incident-42",
    }),
    { eventId, body },
    { applicationId: "engineering-agent" },
  );
  return compileAcceptedFanout({ source, branches });
}

function canonicalEvent(eventId: string, body: string) {
  return {
    id: eventId,
    type: "channel.message",
    version: 1,
    occurredAt: "2026-08-14T00:00:00.000Z",
    data: { body },
    source: {
      channelId: "slack",
      installationId: "workspace-1",
      tenantId: "tenant-1",
    },
    origin: { kind: "external" as const, depth: 0 },
  };
}

function plan(overrides: Partial<AttentionBranchPlan> = {}): AttentionBranchPlan {
  return {
    monitorId: "monitor",
    definitionVersion: "1",
    correlationKey: "incident-42",
    orderKey: "a",
    mode: "active",
    policy: { buffer: { mode: "immediate" } },
    ...overrides,
  };
}

function uniqueEngineId(): string {
  return `test-${crypto.randomUUID()}`;
}

function withoutTrace<T extends { readonly trace: readonly string[] }>(
  snapshot: T,
): Omit<T, "trace"> {
  const { trace: _trace, ...summary } = snapshot;
  return summary;
}
