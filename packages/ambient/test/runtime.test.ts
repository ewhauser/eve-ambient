import { z } from "zod";
import { describe, expect, it, vi } from "vitest";
import {
  compileMonitor,
  defineChannelEvent,
  defineInboundChannel,
  defineMonitor,
  ignore,
  IdempotencyConflictError,
  modelDecision,
  MonitorRuntime,
  TransientMonitorError,
  wake,
  type ChannelEvent,
  type DirectDispatchOptions,
  type DirectDispatchRequest,
  type MonitorModelInvoker,
} from "../src/index.js";
import { MemoryMonitorStore } from "../src/memory.js";
import {
  MemoryConversationChannel,
  RecordingMonitorObserver,
  VirtualMonitorClock,
} from "../src/testing.js";

const messageSchema = z.object({
  channelId: z.string(),
  ts: z.string(),
  threadTs: z.string().optional(),
  text: z.string(),
});

const slack = defineInboundChannel({
  id: "slack",
  replyTarget: z.object({ channel: z.string(), thread: z.string() }),
  inbound: {
    message: defineChannelEvent({ schema: messageSchema, chat: true }),
  },
});

type MessageEvent = ChannelEvent<"message", z.infer<typeof messageSchema>, { channel: string; thread: string }>;

function eventInput(id: string, text = "please investigate") {
  return {
    tenantId: "tenant-a",
    installationId: "workspace-a",
    id,
    data: { channelId: "C1", ts: id, text },
    replyTarget: { channel: "C1", thread: id },
    actor: { id: "U1", principalType: "user" as const },
    origin: { kind: "external" as const },
  };
}

function direct(
  handlers: DirectDispatchOptions["handlers"] = [],
  bindingGeneration = "test-binding-v1",
): DirectDispatchOptions {
  return { bindingGeneration, handlers };
}

describe("MonitorRuntime", () => {
  it("deduplicates ingress and creates no delivery for ignores", async () => {
    const clock = new VirtualMonitorClock();
    const delivery = new MemoryConversationChannel({ id: "slack-delivery", clock });
    const monitor = defineMonitor<MessageEvent>({
      id: "ambient-engineering",
      sources: [slack.event("message", { phase: "undispatched" })],
      correlate: ({ event }) => `${event.source.installationId}:${event.data.channelId}`,
      decision: () => ignore({ reason: "not-useful" }),
      task: { instructions: "Review evidence.", evidence: ({ events }) => ({ count: events.length }) },
      route: ({ events }) => ({ channel: delivery, target: events.at(-1)!.replyTarget!, auth: "app" }),
      metadata: { owner: "test", useCase: "ambient-slack" },
    });
    const store = new MemoryMonitorStore();
    const runtime = new MonitorRuntime({
      applicationId: "app-a",
      deployment: { monitors: [compileMonitor(monitor, "v1")] },
      channels: [slack],
      deliveryChannels: [delivery],
      store,
      clock,
    });
    await runtime.initialize();

    const first = await runtime.publishChat(slack, "message", eventInput("1"), direct());
    const duplicate = await runtime.publishChat(slack, "message", eventInput("1"), direct());
    await runtime.drain();

    expect(first.status).toBe("accepted");
    expect(first.directDispatch).toBe("undispatched");
    expect(duplicate.status).toBe("duplicate");
    expect(duplicate.directDispatch).toBe("undispatched");
    await expect(
      runtime.publishChat(slack, "message", eventInput("1", "different payload"), direct()),
    ).rejects.toBeInstanceOf(IdempotencyConflictError);
    expect(delivery.deliveries).toHaveLength(0);
    expect((await runtime.listRuns())[0]?.status).toBe("ignored");
  });

  it("freezes full-payload conditional fan-out before direct dispatch settles", async () => {
    const clock = new VirtualMonitorClock();
    const monitor = defineMonitor<MessageEvent>({
      id: "conditional-ambient",
      sources: [slack.event("message", { phase: "undispatched" })],
      decision: () => ignore({ reason: "not-useful" }),
      task: { instructions: "Review.", evidence: () => ({}) },
      route: () => null,
      metadata: { owner: "test", useCase: "conditional-fanout" },
    });
    const store = new MemoryMonitorStore();
    const runtime = new MonitorRuntime({
      applicationId: "app-a",
      deployment: { monitors: [compileMonitor(monitor, "v1")] },
      channels: [slack],
      store,
      clock,
    });
    await runtime.initialize();

    let presented!: DirectDispatchRequest;
    let markStarted!: () => void;
    const started = new Promise<void>((resolve) => { markStarted = resolve; });
    let release!: (receipt: null) => void;
    const completion = new Promise<null>((resolve) => { release = resolve; });
    const handler = vi.fn((request: DirectDispatchRequest) => {
      presented = request;
      markStarted();
      return completion;
    });
    const publishing = runtime.publishChat(
      slack,
      "message",
      eventInput("conditional"),
      direct([handler]),
    );
    await started;

    const receipt = await store.transaction("inspect:conditional", (tx) =>
      tx.getIngressReceiptByDedupeKey(presented.eventKey)
    );
    expect(receipt).not.toBeNull();
    expect(receipt).not.toHaveProperty("event");
    expect(receipt?.directDispatch).toMatchObject({
      directDispatchKey: presented.idempotencyKey,
      inputHash: presented.inputHash,
      bindingGeneration: "test-binding-v1",
      status: "processing",
    });
    const conditional = await store.listSubscriptions({
      applicationId: "app-a",
      statuses: ["conditional"],
      availableBefore: clock.now().toISOString(),
      limit: 10,
    });
    expect(conditional).toHaveLength(1);
    expect(conditional[0]).toMatchObject({
      branchKey: receipt?.branches[0]?.branchKey,
      inputHash: receipt?.branches[0]?.inputHash,
      event: { data: { text: "please investigate" } },
    });

    release(null);
    const result = await publishing;
    expect(result).toMatchObject({
      status: "accepted",
      directDispatchKey: presented.idempotencyKey,
      directDispatch: "undispatched",
    });
    expect((await store.listSubscriptions({
      applicationId: "app-a",
      statuses: ["pending"],
      availableBefore: clock.now().toISOString(),
      limit: 10,
    }))[0]?.branchKey).toBe(conditional[0]?.branchKey);
    await expect(
      runtime.publishChat(
        slack,
        "message",
        eventInput("conditional"),
        direct([], "test-binding-v2"),
      ),
    ).rejects.toBeInstanceOf(IdempotencyConflictError);
  });

  it("resumes transient direct dispatch from its durable duplicate state", async () => {
    const clock = new VirtualMonitorClock();
    const monitor = defineMonitor<MessageEvent>({
      id: "durable-direct-dispatch",
      sources: [slack.event("message")],
      decision: () => ignore({ reason: "not-useful" }),
      task: { instructions: "Review.", evidence: () => ({}) },
      route: () => null,
      metadata: { owner: "test", useCase: "direct-dispatch" },
    });
    const runtime = new MonitorRuntime({
      applicationId: "app-a",
      deployment: { monitors: [compileMonitor(monitor, "v1")] },
      channels: [slack],
      store: new MemoryMonitorStore(),
      clock,
    });
    await runtime.initialize();
    const handler = vi.fn()
      .mockRejectedValueOnce(new TransientMonitorError("temporary dispatch outage"))
      .mockResolvedValueOnce({ turnId: "durable-turn" });

    const first = await runtime.publishChat(
      slack,
      "message",
      eventInput("dispatch-retry"),
      direct([handler]),
    );
    const earlyDuplicate = await runtime.publishChat(
      slack,
      "message",
      eventInput("dispatch-retry"),
      direct([handler]),
    );
    clock.advance(1_000);
    const resumed = await runtime.publishChat(
      slack,
      "message",
      eventInput("dispatch-retry"),
      direct([handler]),
    );

    expect(first.directDispatch).toBe("pending");
    expect(earlyDuplicate.directDispatch).toBe("pending");
    expect(resumed).toMatchObject({ status: "duplicate", directDispatch: "dispatched" });
    expect(handler).toHaveBeenCalledTimes(2);
    expect(handler.mock.calls[0]?.[0]).toMatchObject({
      idempotencyKey: first.directDispatchKey,
      eventKey: expect.stringMatching(/^eve:event:v1:/),
      event: { data: { text: "please investigate" } },
    });
    expect(handler.mock.calls[1]?.[0]).toEqual(handler.mock.calls[0]?.[0]);
  });

  it("assigns a new direct-dispatch key after the ingress dedupe horizon", async () => {
    const clock = new VirtualMonitorClock();
    const monitor = defineMonitor<MessageEvent>({
      id: "direct-horizon",
      sources: [slack.event("message", { phase: "observed" })],
      decision: () => ignore({ reason: "not-useful" }),
      task: { instructions: "Review.", evidence: () => ({}) },
      route: () => null,
      retention: { decisions: "1h", dedupe: "1s" },
      metadata: { owner: "test", useCase: "direct-horizon" },
    });
    const runtime = new MonitorRuntime({
      applicationId: "app-a",
      deployment: { monitors: [compileMonitor(monitor, "v1")] },
      channels: [slack],
      store: new MemoryMonitorStore(),
      clock,
    });
    await runtime.initialize();
    const handler = vi.fn(async (request: DirectDispatchRequest) => ({
      turnId: request.idempotencyKey,
    }));

    const first = await runtime.publishChat(
      slack,
      "message",
      eventInput("reused-direct", "first"),
      direct([handler]),
    );
    await runtime.drain();
    clock.advance(1_000);
    await runtime.purgeExpired();
    const second = await runtime.publishChat(
      slack,
      "message",
      eventInput("reused-direct", "second"),
      direct([handler]),
    );

    expect(first.status).toBe("accepted");
    expect(second.status).toBe("accepted");
    expect(second.directDispatchKey).not.toBe(first.directDispatchKey);
    expect(handler).toHaveBeenCalledTimes(2);
  });

  it("recovers an expired direct-dispatch lease without accepting a stale outcome", async () => {
    const clock = new VirtualMonitorClock();
    const monitor = defineMonitor<MessageEvent>({
      id: "leased-direct-dispatch",
      sources: [slack.event("message")],
      decision: () => ignore({ reason: "not-useful" }),
      task: { instructions: "Review.", evidence: () => ({}) },
      route: () => null,
      metadata: { owner: "test", useCase: "direct-dispatch-lease" },
    });
    const runtime = new MonitorRuntime({
      applicationId: "app-a",
      deployment: { monitors: [compileMonitor(monitor, "v1")] },
      channels: [slack],
      store: new MemoryMonitorStore(),
      clock,
    });
    await runtime.initialize();
    let markStarted!: () => void;
    const started = new Promise<void>((resolve) => { markStarted = resolve; });
    let releaseStale!: (receipt: { turnId: string } | null) => void;
    const staleHandler = vi.fn(() => {
      markStarted();
      return new Promise<{ turnId: string } | null>((resolve) => { releaseStale = resolve; });
    });
    const first = runtime.publishChat(
      slack,
      "message",
      eventInput("dispatch-lease"),
      direct([staleHandler]),
    );
    await started;
    clock.advance(30_000);

    const recovered = await runtime.publishChat(
      slack,
      "message",
      eventInput("dispatch-lease"),
      direct([async () => ({ turnId: "recovered-turn" })]),
    );
    releaseStale(null);

    expect(recovered).toMatchObject({ status: "duplicate", directDispatch: "dispatched" });
    await expect(first).resolves.toMatchObject({ directDispatch: "dispatched" });
    expect(staleHandler).toHaveBeenCalledOnce();
  });

  it("never drains another application's work from a shared store", async () => {
    const clock = new VirtualMonitorClock();
    const store = new MemoryMonitorStore();
    const definition = (id: string) => defineMonitor<MessageEvent>({
      id,
      sources: [slack.event("message")],
      decision: () => ignore({ reason: "not-useful" }),
      task: { instructions: "Review.", evidence: () => ({}) },
      route: () => null,
      metadata: { owner: "test", useCase: "application-isolation" },
    });
    const appA = new MonitorRuntime({
      applicationId: "app-a",
      deployment: { monitors: [compileMonitor(definition("monitor-a"), "v1")] },
      channels: [slack],
      store,
      clock,
    });
    const appB = new MonitorRuntime({
      applicationId: "app-b",
      deployment: { monitors: [compileMonitor(definition("monitor-b"), "v1")] },
      channels: [slack],
      store,
      clock,
    });
    await appA.initialize();
    await appB.initialize();
    await appB.publishChat(slack, "message", eventInput("owned-by-b"), direct());

    await expect(appA.drain()).resolves.toMatchObject({
      subscriptions: 0,
      evaluations: 0,
      runs: 0,
      remaining: false,
    });
    expect(await appA.listRuns()).toHaveLength(0);
    await appB.drain();
    expect(await appB.listRuns()).toHaveLength(1);
  });

  it("validates canonical reply targets before durable acceptance", async () => {
    const clock = new VirtualMonitorClock();
    const delivery = new MemoryConversationChannel({ id: "slack-delivery", clock });
    const monitor = defineMonitor<MessageEvent>({
      id: "target-validation",
      sources: [slack.event("message")],
      decision: () => ignore({ reason: "not-useful" }),
      task: { instructions: "Review.", evidence: () => ({}) },
      route: () => null,
      metadata: { owner: "test", useCase: "target-validation" },
    });
    const store = new MemoryMonitorStore();
    const runtime = new MonitorRuntime({
      applicationId: "app-a",
      deployment: { monitors: [compileMonitor(monitor, "v1")] },
      channels: [slack],
      deliveryChannels: [delivery],
      store,
      clock,
    });
    await runtime.initialize();

    await expect(
      runtime.publishChat(
        slack,
        "message",
        { ...eventInput("invalid-target"), replyTarget: { channel: "C1" } } as never,
        direct(),
      ),
    ).rejects.toThrow("channel replyTarget failed schema validation");
    expect(await runtime.listRuns()).toHaveLength(0);
  });

  it("flushes a continuous debounce stream at maxWait", async () => {
    const clock = new VirtualMonitorClock();
    const delivery = new MemoryConversationChannel({ id: "slack-delivery", clock });
    const monitor = defineMonitor<MessageEvent>({
      id: "debounced",
      sources: [slack.event("message")],
      correlate: () => "thread:one",
      buffer: { mode: "debounce", quietPeriod: "2s", maxWait: "5s", maxEvents: 20, maxBytes: 10_000 },
      decision: () => wake({ reason: "useful" }),
      task: { instructions: "Review evidence.", evidence: ({ events, batch }) => ({ count: events.length, closedBy: batch.closedBy }) },
      route: () => ({ channel: delivery, target: { channel: "C1", thread: "T1" }, auth: "app" }),
      metadata: { owner: "test", useCase: "debounce" },
    });
    const runtime = new MonitorRuntime({
      applicationId: "app-a",
      deployment: { monitors: [compileMonitor(monitor, "v1")] },
      channels: [slack],
      deliveryChannels: [delivery],
      store: new MemoryMonitorStore(),
      clock,
    });
    await runtime.initialize();
    for (let index = 0; index < 6; index += 1) {
      await runtime.publishChat(slack, "message", eventInput(String(index)), direct());
      await runtime.drain();
      if (index < 5) clock.advance(1_000);
    }
    await runtime.drain();
    expect(delivery.deliveries).toHaveLength(1);
    expect(delivery.deliveries[0]?.evidence.completeness.closedBy).toBe("max-wait");
    expect(delivery.deliveries[0]?.evidence.sourceEventKeys).toHaveLength(6);
  });

  it("uses explicit model settings, repairs once, and validates action metadata", async () => {
    const clock = new VirtualMonitorClock();
    const delivery = new MemoryConversationChannel({ id: "slack-delivery", clock });
    const invoker = vi.fn<MonitorModelInvoker>()
      .mockResolvedValueOnce({ output: { action: "invented" }, usage: { inputTokens: 4, outputTokens: 2 } })
      .mockResolvedValueOnce({
        output: { action: "wake", reason: "helpful", metadata: { priority: "high" } },
        usage: { inputTokens: 5, outputTokens: 3 },
      });
    const monitor = defineMonitor<MessageEvent, Record<string, never>, { priority: "high" | "low" }>({
      id: "model-backed",
      sources: [slack.event("message")],
      decision: modelDecision({
        model: "openai/gpt-5-nano",
        reasoning: "none",
        instructions: "Classify relevance.",
        input: ({ events }) => ({ text: events[0]!.data.text }),
        metadata: {
          ignore: z.object({}),
          wake: z.object({ priority: z.enum(["high", "low"]) }),
        },
        timeout: "8s",
        maxInputTokens: 100,
        maxOutputTokens: 50,
        onError: ignore({ reason: "classifier-unavailable", metadata: {} }),
      }),
      task: {
        instructions: "Review evidence.",
        evidence: ({ decision }) => ({ action: decision.action, reason: decision.reason }),
      },
      route: () => ({ channel: delivery, target: { channel: "C1", thread: "T1" }, auth: "app" }),
      metadata: { owner: "test", useCase: "classifier" },
    });
    const runtime = new MonitorRuntime({
      applicationId: "app-a",
      deployment: { monitors: [compileMonitor(monitor, "v1")] },
      channels: [slack],
      deliveryChannels: [delivery],
      store: new MemoryMonitorStore(),
      modelInvoker: invoker,
      clock,
    });
    await runtime.initialize();
    await runtime.publishChat(slack, "message", eventInput("1"), direct());
    await runtime.drain();

    expect(invoker).toHaveBeenCalledTimes(2);
    expect(invoker.mock.calls[0]?.[0]).toMatchObject({
      model: "openai/gpt-5-nano",
      reasoning: "none",
      timeoutMs: 8_000,
      maxOutputTokens: 50,
      repairAttempt: 0,
    });
    expect(invoker.mock.calls[1]?.[0].repairAttempt).toBe(1);
    expect(delivery.deliveries).toHaveLength(1);
  });

  it.each([
    ["call", { maxModelCallsPerMinute: 1 }],
    ["input-token", { maxModelInputTokensPerHour: 10 }],
  ] as const)("charges every classifier repair against the %s budget", async (_name, limits) => {
    const clock = new VirtualMonitorClock();
    const invoker = vi.fn<MonitorModelInvoker>().mockResolvedValue({
      output: { action: "invalid" },
      usage: { inputTokens: 5, outputTokens: 1 },
    });
    const monitor = defineMonitor<MessageEvent>({
      id: `repair-${_name}`,
      sources: [slack.event("message")],
      decision: modelDecision({
        model: "openai/gpt-5-nano",
        reasoning: "none",
        instructions: "Classify.",
        input: () => ({ text: "small" }),
        timeout: "1s",
        maxInputTokens: 10,
        maxOutputTokens: 10,
        repairAttempts: 1,
        onError: ignore({ reason: "repair-budget-exhausted" }),
      }),
      task: { instructions: "Review.", evidence: () => ({}) },
      route: () => null,
      limits: { perMonitor: limits, overflow: "drop" },
      metadata: { owner: "test", useCase: "repair-budget" },
    });
    const runtime = new MonitorRuntime({
      applicationId: "app-a",
      deployment: { monitors: [compileMonitor(monitor, "v1")] },
      channels: [slack],
      store: new MemoryMonitorStore(),
      modelInvoker: invoker,
      clock,
    });
    await runtime.initialize();
    await runtime.publishChat(slack, "message", eventInput(`repair-${_name}`), direct());
    await runtime.drain();

    expect(invoker).toHaveBeenCalledTimes(1);
    expect((await runtime.listRuns())[0]).toMatchObject({
      status: "ignored",
      decisionSource: "fallback",
      decision: { action: "ignore", reason: "repair-budget-exhausted" },
    });
  });

  it("uses classifier fallback without treating provider failures as schema repairs", async () => {
    const clock = new VirtualMonitorClock();
    const invoker = vi.fn<MonitorModelInvoker>().mockRejectedValue(
      new TransientMonitorError("provider timeout"),
    );
    const monitor = defineMonitor<MessageEvent>({
      id: "provider-fallback",
      sources: [slack.event("message")],
      decision: modelDecision({
        model: "openai/gpt-5-nano",
        reasoning: "none",
        instructions: "Classify.",
        input: () => ({ text: "small" }),
        timeout: "1s",
        maxInputTokens: 10,
        maxOutputTokens: 10,
        repairAttempts: 1,
        onError: ignore({ reason: "provider-unavailable" }),
      }),
      task: { instructions: "Review.", evidence: () => ({}) },
      route: () => null,
      metadata: { owner: "test", useCase: "provider-fallback" },
    });
    const runtime = new MonitorRuntime({
      applicationId: "app-a",
      deployment: { monitors: [compileMonitor(monitor, "v1")] },
      channels: [slack],
      store: new MemoryMonitorStore(),
      modelInvoker: invoker,
      clock,
    });
    await runtime.initialize();
    await runtime.publishChat(slack, "message", eventInput("provider-failure"), direct());
    await runtime.drain();

    expect(invoker).toHaveBeenCalledOnce();
    expect((await runtime.listRuns())[0]).toMatchObject({
      status: "ignored",
      decisionSource: "fallback",
      decision: { action: "ignore", reason: "provider-unavailable" },
    });
  });

  it("rejects phaseless chat sources instead of subscribing to both phases", () => {
    const monitor = defineMonitor<MessageEvent>({
      id: "phaseless-chat",
      sources: [{ kind: "channel-event-source", channelId: "slack", eventType: "message" }],
      decision: () => ignore({ reason: "not-useful" }),
      task: { instructions: "Review.", evidence: () => ({}) },
      route: () => null,
      metadata: { owner: "test", useCase: "chat-phase" },
    });

    expect(
      () => new MonitorRuntime({
        applicationId: "app-a",
        deployment: { monitors: [compileMonitor(monitor, "v1")] },
        channels: [slack],
        store: new MemoryMonitorStore(),
      }),
    ).toThrow("must select observed or undispatched");
  });

  it("does not let a failing telemetry observer change durable outcomes", async () => {
    const clock = new VirtualMonitorClock();
    const delivery = new MemoryConversationChannel({ id: "slack-delivery", clock });
    const monitor = defineMonitor<MessageEvent>({
      id: "telemetry-safe",
      sources: [slack.event("message")],
      decision: () => wake({ reason: "useful" }),
      task: { instructions: "Review.", evidence: () => ({ ok: true }) },
      route: () => ({ channel: delivery, target: { channel: "C1", thread: "T1" }, auth: "app" }),
      metadata: { owner: "test", useCase: "telemetry" },
    });
    const runtime = new MonitorRuntime({
      applicationId: "app-a",
      deployment: { monitors: [compileMonitor(monitor, "v1")] },
      channels: [slack],
      deliveryChannels: [delivery],
      store: new MemoryMonitorStore(),
      clock,
      observer: { emit: () => { throw new Error("telemetry unavailable"); } },
    });
    await runtime.initialize();
    await runtime.publishChat(slack, "message", eventInput("telemetry"), direct());
    await runtime.drain();
    expect(delivery.deliveries).toHaveLength(1);
  });
});
