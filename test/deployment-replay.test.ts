import { z } from "zod";
import { describe, expect, it } from "vitest";
import {
  compileMonitor,
  defineChannelEvent,
  defineInboundChannel,
  defineMonitor,
  MonitorRuntime,
  wake,
  type ChannelEvent,
  type MonitorDeliveryRequest,
  type MonitorDefinition,
} from "../src/index.js";
import { MemoryMonitorStore } from "../src/memory.js";
import { MemoryConversationChannel, VirtualMonitorClock } from "../src/testing.js";
import { TransientMonitorError } from "../src/types.js";

class RetryOnceConversationChannel extends MemoryConversationChannel {
  attempts = 0;

  override async deliver(request: MonitorDeliveryRequest) {
    this.attempts += 1;
    if (this.attempts === 1) throw new TransientMonitorError("delivery unavailable");
    return super.deliver(request);
  }
}

const source = defineInboundChannel({
  id: "events",
  replyTarget: z.object({ id: z.string() }),
  inbound: { changed: defineChannelEvent({ schema: z.object({ key: z.string(), value: z.string() }) }) },
});
type Event = ChannelEvent<"changed", { key: string; value: string }, { id: string }>;

function monitor(
  id: string,
  delivery: MemoryConversationChannel,
  overrides: Partial<MonitorDefinition<Event>> = {},
) {
  return defineMonitor<Event>({
    id,
    sources: [source.event("changed")],
    correlate: ({ event }) => event.data.key,
    decision: () => wake({ reason: "changed" }),
    task: {
      instructions: "Review the change.",
      evidence: ({ events }) => ({ values: events.map((event) => event.data.value) }),
    },
    route: ({ events }) => ({ channel: delivery, target: events.at(-1)!.replyTarget!, auth: "app" }),
    metadata: { owner: "test", useCase: "deployment" },
    ...overrides,
  });
}

function event(id: string, value = "v1") {
  return {
    tenantId: "tenant",
    installationId: "installation",
    id,
    data: { key: "key", value },
    replyTarget: { id: "target" },
    origin: { kind: "external" as const },
  };
}

describe("deployment identity and retention", () => {
  it("refuses mailbox ownership changes that would strand durable work", async () => {
    const clock = new VirtualMonitorClock();
    const store = new MemoryMonitorStore();
    const delivery = new MemoryConversationChannel({ id: "delivery", clock });
    const compiled = compileMonitor(monitor("stable", delivery), "v1");
    const storeRuntime = new MonitorRuntime({
      applicationId: "app",
      deployment: { monitors: [compiled] },
      channels: [source],
      deliveryChannels: [delivery],
      store,
      clock,
    });
    await storeRuntime.initialize();

    const celldRuntime = new MonitorRuntime({
      applicationId: "app",
      deployment: { monitors: [compiled] },
      channels: [source],
      deliveryChannels: [delivery],
      store,
      clock,
      mailbox: {
        mode: "celld",
        fleetUrl: "http://fleet.test",
        evaluatorUrl: "http://app.test/evaluate",
        secret: "secret",
        fetch: async () => new Response(null, { status: 503 }),
      },
    });

    await expect(celldRuntime.initialize()).rejects.toThrow(
      "mailbox mode cannot change from store to celld",
    );

    const otherStore = new MemoryMonitorStore();
    const firstCelld = new MonitorRuntime({
      applicationId: "app",
      deployment: { monitors: [compiled] },
      channels: [source],
      deliveryChannels: [delivery],
      store: otherStore,
      clock,
      mailbox: {
        mode: "celld",
        fleetUrl: "http://fleet.test",
        evaluatorUrl: "http://app.test/evaluate",
        secret: "secret",
        fetch: async () => new Response(null, { status: 503 }),
      },
    });
    await firstCelld.initialize();
    const backToStore = new MonitorRuntime({
      applicationId: "app",
      deployment: { monitors: [compiled] },
      channels: [source],
      deliveryChannels: [delivery],
      store: otherStore,
      clock,
    });
    await expect(backToStore.initialize()).rejects.toThrow(
      "mailbox mode cannot change from celld to store",
    );
  });

  it("rejects state migrations that cannot update celld cells", async () => {
    const clock = new VirtualMonitorClock();
    const store = new MemoryMonitorStore();
    const delivery = new MemoryConversationChannel({ id: "delivery", clock });
    const runtime = new MonitorRuntime({
      applicationId: "app",
      deployment: {
        monitors: [compileMonitor(monitor("new", delivery), "v1")],
        monitorMigrations: [{ from: "old", to: "new", mode: "move-state" }],
      },
      channels: [source],
      deliveryChannels: [delivery],
      store,
      clock,
      mailbox: {
        mode: "celld",
        fleetUrl: "http://fleet.test",
        evaluatorUrl: "http://app.test/evaluate",
        secret: "secret",
        fetch: async () => new Response(null, { status: 503 }),
      },
    });

    await expect(runtime.initialize()).rejects.toThrow(
      "celld mailbox does not support monitor migrations or removals",
    );
  });

  it("requires an explicit migration or destructive removal for a missing monitor ID", async () => {
    const clock = new VirtualMonitorClock();
    const store = new MemoryMonitorStore();
    const delivery = new MemoryConversationChannel({ id: "delivery", clock });
    const first = new MonitorRuntime({
      applicationId: "app",
      deployment: { monitors: [compileMonitor(monitor("old", delivery), "v1")] },
      channels: [source],
      deliveryChannels: [delivery],
      store,
      clock,
    });
    await first.initialize();
    await first.publish(source, "changed", event("queued-before-rename", "queued"));

    const missing = new MonitorRuntime({
      applicationId: "app",
      deployment: { monitors: [compileMonitor(monitor("new", delivery), "v1")] },
      channels: [source],
      deliveryChannels: [delivery],
      store,
      clock,
    });
    await expect(missing.initialize()).rejects.toThrow("monitor old disappeared");

    const migrated = new MonitorRuntime({
      applicationId: "app",
      deployment: {
        monitors: [compileMonitor(monitor("new", delivery), "v1")],
        monitorMigrations: [{ from: "old", to: "new", mode: "move-state" }],
      },
      channels: [source],
      deliveryChannels: [delivery],
      store,
      clock,
    });
    await migrated.initialize();
    await migrated.drain();
    expect(delivery.deliveries).toHaveLength(1);
    expect((await migrated.listRuns())[0]).toMatchObject({ monitorId: "new", definitionVersion: "v1" });
  });

  it("destructive removal discards queued subscriptions", async () => {
    const clock = new VirtualMonitorClock();
    const store = new MemoryMonitorStore();
    const delivery = new MemoryConversationChannel({ id: "delivery", clock });
    const first = new MonitorRuntime({
      applicationId: "app",
      deployment: { monitors: [compileMonitor(monitor("removed", delivery), "v1")] },
      channels: [source],
      deliveryChannels: [delivery],
      store,
      clock,
    });
    await first.initialize();
    await first.publish(source, "changed", event("discard-me"));

    const removed = new MonitorRuntime({
      applicationId: "app",
      deployment: {
        monitors: [],
        monitorRemovals: [{ id: "removed", mode: "discard-state" }],
      },
      channels: [source],
      deliveryChannels: [delivery],
      store,
      clock,
    });
    await removed.initialize();
    await removed.drain();
    expect(delivery.deliveries).toHaveLength(0);
    expect(await store.listDefinitionPins("app")).toHaveLength(0);
  });

  it("moves idle mailbox state only across explicitly compatible versions", async () => {
    const clock = new VirtualMonitorClock();
    const store = new MemoryMonitorStore();
    const delivery = new MemoryConversationChannel({ id: "delivery", clock });
    const v1Definition = monitor("stable", delivery, {
      buffer: { mode: "debounce", quietPeriod: "10s", maxWait: "20s", maxEvents: 10, maxBytes: 1_000 },
    });
    const v1 = new MonitorRuntime({
      applicationId: "app",
      deployment: { monitors: [compileMonitor(v1Definition, "v1")] },
      channels: [source],
      deliveryChannels: [delivery],
      store,
      clock,
    });
    await v1.initialize();
    await v1.publish(source, "changed", event("one"));
    await v1.drain();

    const v2 = new MonitorRuntime({
      applicationId: "app",
      deployment: {
        monitors: [
          compileMonitor(v1Definition, "v1", { active: false }),
          compileMonitor(monitor("stable", delivery, {
            buffer: { mode: "debounce", quietPeriod: "10s", maxWait: "20s", maxEvents: 10, maxBytes: 1_000 },
          }), "v2", { compatibleWith: ["v1"] }),
        ],
      },
      channels: [source],
      deliveryChannels: [delivery],
      store,
      clock,
    });
    await v2.initialize();
    clock.advance(10_000);
    await v2.drain();
    expect(delivery.deliveries).toHaveLength(1);
    expect((await v2.listRuns())[0]?.definitionVersion).toBe("v2");
  });

  it("requires queued subscriptions to retain their pinned definition version", async () => {
    const clock = new VirtualMonitorClock();
    const store = new MemoryMonitorStore();
    const delivery = new MemoryConversationChannel({ id: "delivery", clock });
    const v1Definition = monitor("queued", delivery);
    const v1 = new MonitorRuntime({
      applicationId: "app",
      deployment: { monitors: [compileMonitor(v1Definition, "v1")] },
      channels: [source],
      deliveryChannels: [delivery],
      store,
      clock,
    });
    await v1.initialize();
    await v1.publish(source, "changed", event("queued"));

    const missing = new MonitorRuntime({
      applicationId: "app",
      deployment: { monitors: [compileMonitor(monitor("queued", delivery), "v2")] },
      channels: [source],
      deliveryChannels: [delivery],
      store,
      clock,
    });
    await expect(missing.initialize()).rejects.toThrow("requires pinned definition queued@v1");

    const retained = new MonitorRuntime({
      applicationId: "app",
      deployment: {
        monitors: [
          compileMonitor(v1Definition, "v1", { active: false }),
          compileMonitor(monitor("queued", delivery), "v2"),
        ],
      },
      channels: [source],
      deliveryChannels: [delivery],
      store,
      clock,
    });
    await retained.initialize();
    await retained.drain();
    expect(delivery.deliveries).toHaveLength(1);
    expect((await retained.listRuns())[0]?.definitionVersion).toBe("v1");
  });

  it("redacts ingress payload before dedupe expiry while the frozen run remains self-contained", async () => {
    const clock = new VirtualMonitorClock();
    const store = new MemoryMonitorStore();
    const delivery = new RetryOnceConversationChannel({ id: "delivery", clock });
    const definition = monitor("retention", delivery, {
      retention: { payload: "1s", decisions: "1h", dedupe: "2s" },
    });
    const runtime = new MonitorRuntime({
      applicationId: "app",
      deployment: { monitors: [compileMonitor(definition, "v1")] },
      channels: [source],
      deliveryChannels: [delivery],
      store,
      clock,
    });
    await runtime.initialize();
    const accepted = await runtime.publish(source, "changed", event("one", "secret"));
    await runtime.drain();
    const pending = (await runtime.listRuns())[0]!;
    expect(pending.status).toBe("retry");
    if (!("events" in pending.batch)) throw new Error("retry run lost its actionable payload");
    expect(pending.batch.events[0]?.event.data).toEqual({ key: "key", value: "secret" });
    clock.advance(1_000);
    await runtime.purgeExpired();
    expect((await store.getEvent(accepted.eventId))?.event).toBeUndefined();
    await runtime.drain();
    expect(delivery.deliveries[0]?.evidence.projectedEvidence).toEqual({ values: ["secret"] });
    const run = (await runtime.listRuns())[0]!;
    expect(run.batch).not.toHaveProperty("events");
    expect(run.batch).toMatchObject({ eventCount: 1 });
    expect(run.batch.batchKey).toMatch(/^eve:batch:v1:/);
    expect(run.runKey).toMatch(/^eve:run:v1:/);
    expect((await runtime.publish(source, "changed", event("one", "secret"))).status).toBe("duplicate");
    clock.advance(1_000);
    await runtime.purgeExpired();
    expect((await runtime.publish(source, "changed", event("one", "new"))).status).toBe("accepted");
  });

  it("rejects provider-ID reuse while its earlier branch is still active", async () => {
    const clock = new VirtualMonitorClock();
    const store = new MemoryMonitorStore();
    const delivery = new MemoryConversationChannel({ id: "delivery", clock });
    const definition = monitor("dedupe-reuse", delivery, {
      retention: { payload: "1s", decisions: "1h", dedupe: "1s" },
    });
    const runtime = new MonitorRuntime({
      applicationId: "app",
      deployment: { monitors: [compileMonitor(definition, "v1")] },
      channels: [source],
      deliveryChannels: [delivery],
      store,
      clock,
    });
    await runtime.initialize();
    const first = await runtime.publish(source, "changed", event("reused", "old"));
    clock.advance(1_000);

    await expect(
      runtime.publish(source, "changed", event("reused", "new")),
    ).rejects.toMatchObject({ name: "IdempotencyConflictError" });

    await runtime.drain();
    const second = await runtime.publish(source, "changed", event("reused", "new"));

    expect(second.status).toBe("accepted");
    expect(second.eventId).not.toBe(first.eventId);
    expect(await store.getEvent(first.eventId)).not.toBeNull();
  });

  it("keeps an unfinished branch self-contained after ingress dedupe retention expires", async () => {
    const clock = new VirtualMonitorClock();
    const store = new MemoryMonitorStore();
    const delivery = new MemoryConversationChannel({ id: "delivery", clock });
    const definition = monitor("retention-dead-letter", delivery, {
      retention: { payload: "1s", decisions: "1h", dedupe: "1s" },
    });
    const runtime = new MonitorRuntime({
      applicationId: "app",
      deployment: { monitors: [compileMonitor(definition, "v1")] },
      channels: [source],
      deliveryChannels: [delivery],
      store,
      clock,
    });
    await runtime.initialize();
    await runtime.publish(source, "changed", event("unfinished"));
    clock.advance(1_000);

    await runtime.purgeExpired();

    expect(await runtime.listDeadLetters()).toEqual([]);
    await expect(store.listSubscriptionsForMonitor({
      applicationId: "app",
      monitorId: definition.id,
    })).resolves.toMatchObject([{ event: { data: { value: "v1" } } }]);

    await runtime.drain();
    expect(delivery.deliveries).toHaveLength(1);
    await expect(store.listSubscriptionsForMonitor({
      applicationId: "app",
      monitorId: definition.id,
    })).resolves.toEqual([]);
  });
});
