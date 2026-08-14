import { readFile } from "node:fs/promises";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { z } from "zod";
import {
  compileMonitor,
  defineChannelEvent,
  defineInboundChannel,
  defineMonitor,
  ignore,
  MonitorRuntime,
  TransientMonitorError,
  type ChannelEvent,
  type PublishResult,
} from "../src/index.js";
import { PostgresMonitorStore, type PostgresPool } from "../src/postgres.js";
import { VirtualMonitorClock } from "../src/testing.js";

const connectionString = process.env.EVE_AMBIENT_POSTGRES_URL;
const postgresDescribe = connectionString === undefined ? describe.skip : describe;

const source = defineInboundChannel({
  id: "postgres-events",
  inbound: {
    changed: defineChannelEvent({ schema: z.object({ key: z.string() }) }),
  },
});
type Event = ChannelEvent<"changed", { key: string }>;
const chat = defineInboundChannel({
  id: "postgres-chat",
  inbound: {
    message: defineChannelEvent({ schema: z.object({ key: z.string() }), chat: true }),
  },
});
type ChatEvent = ChannelEvent<"message", { key: string }>;

postgresDescribe("PostgresMonitorStore integration", () => {
  const schema = `eve_ambient_test_${process.pid}_${Date.now()}`;
  let pool: Pool;
  let store: PostgresMonitorStore;

  beforeAll(async () => {
    pool = new Pool({ connectionString, max: 8 });
    const migration = await readFile(
      new URL("../migrations/001_eve_ambient.sql", import.meta.url),
      "utf8",
    );
    const client = await pool.connect();
    try {
      await client.query(`CREATE SCHEMA "${schema}"`);
      await client.query(`SET search_path TO "${schema}"`);
      await client.query(migration);
    } finally {
      client.release();
    }
    store = new PostgresMonitorStore({
      pool: pool as unknown as PostgresPool,
      schema,
    });
  });

  afterAll(async () => {
    if (pool === undefined) return;
    await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
    await pool.end();
  });

  it("runs ordered mailboxes, indexed cardinality, and retention on real PostgreSQL", async () => {
    const clock = new VirtualMonitorClock();
    const definition = defineMonitor<Event>({
      id: "postgres-conformance",
      sources: [source.event("changed")],
      correlate: ({ event }) => event.data.key,
      decision: () => ignore({ reason: "recorded" }),
      task: { instructions: "Review.", evidence: ({ events }) => ({ count: events.length }) },
      route: () => null,
      retention: { decisions: "1h", dedupe: "1s" },
      metadata: { owner: "test", useCase: "postgres-conformance" },
    });
    const runtime = new MonitorRuntime({
      applicationId: "app",
      deployment: { monitors: [compileMonitor(definition, "v1")] },
      channels: [source],
      store,
      clock,
    });
    await runtime.initialize();
    const accepted: PublishResult[] = [];
    for (const [id, key] of [["one", "same"], ["two", "same"], ["three", "other"]] as const) {
      accepted.push(await runtime.publish(source, "changed", {
        tenantId: "tenant",
        installationId: "installation",
        id,
        data: { key },
        origin: { kind: "external" },
      }));
    }
    const ingressReceipt = await store.transaction("inspect:ingress", (tx) =>
      tx.getIngressReceipt(accepted[0]!.eventId)
    );
    expect(ingressReceipt).not.toHaveProperty("event");
    expect(ingressReceipt?.branches).toHaveLength(1);
    await runtime.drain();

    expect(await runtime.listRuns()).toHaveLength(3);
    await store.transaction("count", async (tx) => {
      await expect(tx.countInstances({ tenantId: "tenant", applicationId: "app" })).resolves.toBe(2);
    });

    await runtime.publish(source, "changed", {
      tenantId: "tenant",
      installationId: "installation",
      id: "unfinished",
      data: { key: "unfinished" },
      origin: { kind: "external" },
    });
    clock.advance(1_000);
    await runtime.purgeExpired();

    expect(await runtime.listDeadLetters()).toEqual([]);
    await expect(store.listSubscriptionsForMonitor({
      applicationId: "app",
      monitorId: definition.id,
    })).resolves.toMatchObject([{ event: { data: { key: "unfinished" } } }]);

    await runtime.drain();
    expect(await runtime.listRuns()).toHaveLength(4);
    expect((await runtime.listRuns()).every((run) => !("events" in run.batch))).toBe(true);
    await expect(store.listSubscriptionsForMonitor({
      applicationId: "app",
      monitorId: definition.id,
    })).resolves.toEqual([]);
  }, 20_000);

  it("freezes and resolves chat conditional branches in PostgreSQL", async () => {
    const clock = new VirtualMonitorClock();
    const definition = (id: string, phase: "observed" | "undispatched") =>
      defineMonitor<ChatEvent>({
        id,
        sources: [chat.event("message", { phase })],
        decision: () => ignore({ reason: "recorded" }),
        task: { instructions: "Review.", evidence: () => ({}) },
        route: () => null,
        retention: { decisions: "1h", dedupe: "1s" },
        metadata: { owner: "test", useCase: "postgres-chat" },
      });
    const runtime = new MonitorRuntime({
      applicationId: "app-chat",
      deployment: {
        monitors: [
          compileMonitor(definition("observed", "observed"), "v1"),
          compileMonitor(definition("ambient", "undispatched"), "v1"),
        ],
      },
      channels: [chat],
      store,
      clock,
    });
    await runtime.initialize();

    const dispatched = await runtime.publishChat(
      chat,
      "message",
      {
        tenantId: "tenant",
        installationId: "installation",
        id: "direct",
        data: { key: "direct" },
        origin: { kind: "external" },
      },
      {
        bindingGeneration: "postgres-binding-v1",
        handlers: [async () => ({ turnId: "turn-direct" })],
      },
    );
    await runtime.drain();

    const receipt = await store.transaction(`inspect:${dispatched.eventId}`, (tx) =>
      tx.getIngressReceipt(dispatched.eventId)
    );
    expect(receipt).not.toHaveProperty("event");
    expect(receipt?.directDispatch).toMatchObject({
      directDispatchKey: dispatched.directDispatchKey,
      status: "dispatched",
      receipts: [{ turnId: "turn-direct" }],
    });
    expect(receipt?.branches.find((branch) => branch.condition === "direct-undispatched"))
      .toMatchObject({ status: "terminal" });
    expect((await runtime.listRuns()).map((run) => run.monitorId)).toEqual(["observed"]);

    const pending = await runtime.publishChat(
      chat,
      "message",
      {
        tenantId: "tenant",
        installationId: "installation",
        id: "pending",
        data: { key: "pending" },
        origin: { kind: "external" },
      },
      {
        bindingGeneration: "postgres-binding-v1",
        handlers: [async () => {
          throw new TransientMonitorError("direct target unavailable");
        }],
      },
    );
    expect(pending.directDispatch).toBe("pending");
    await runtime.drain();
    clock.advance(1_000);
    await expect(runtime.purgeExpired()).resolves.toMatchObject({ ingressReceipts: 2 });
    expect(await runtime.listDeadLetters()).toMatchObject([{
      eventKey: expect.stringMatching(/^eve:event:v1:/),
      directDispatchKey: pending.directDispatchKey,
      stage: "direct-dispatch",
    }]);
    expect(await store.listSubscriptionsForMonitor({
      applicationId: "app-chat",
      monitorId: "ambient",
    })).toEqual([]);
  }, 20_000);
});
