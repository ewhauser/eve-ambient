import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  compileAcceptedFanout,
  type AttentionCallbacks,
} from "../src/attention.js";
import {
  canonicalizeChannelDelivery,
  defineChannelCanonicalization,
} from "../src/idempotency.js";
import {
  PostgresAttentionEngine,
  type PostgresClient,
  type PostgresPool,
  type PostgresQueryResult,
} from "../src/postgres.js";
import { VirtualMonitorClock } from "../src/testing.js";
import { defineAttentionEngineConformance } from "./attention-conformance.js";

const databaseUrl = process.env.EVE_AMBIENT_POSTGRES_URL;

describe.skipIf(databaseUrl === undefined)("PostgreSQL AttentionEngine integration", () => {
  const pool = new Pool({ connectionString: databaseUrl });

  beforeAll(async () => {
    const migration = await readFile(
      new URL("../migrations/001_attention_engine.sql", import.meta.url),
      "utf8",
    );
    await pool.query(migration);
  });

  afterAll(async () => pool.end());

  defineAttentionEngineConformance("postgres", async (options) => {
    const postgres = new PostgresAttentionEngine(
      {
        pool: pool as unknown as PostgresPool,
        callbacks: options.callbacks,
        clock: options.clock,
        engineId: `conformance-${randomUUID()}`,
        ...(options.dedupeMs === undefined ? {} : { dedupeMs: options.dedupeMs }),
        ...(options.retryDelayMs === undefined
          ? {}
          : { retryDelayMs: options.retryDelayMs }),
        ...(options.claimLeaseMs === undefined
          ? {}
          : { claimLeaseMs: options.claimLeaseMs }),
        ...(options.maxAttempts === undefined ? {} : { maxAttempts: options.maxAttempts }),
        ...(options.maxBranches === undefined ? {} : { maxBranches: options.maxBranches }),
        ...(options.maxFanoutBytes === undefined
          ? {}
          : { maxFanoutBytes: options.maxFanoutBytes }),
        ...(options.maxPreparedWakeBytes === undefined
          ? {}
          : { maxPreparedWakeBytes: options.maxPreparedWakeBytes }),
      },
      { ...(options.faults === undefined ? {} : { faults: options.faults }) },
    );
    return {
      engine: postgres,
      runDue: () => postgres.runOnce(),
      diagnostics: () => postgres.diagnostics(),
    };
  });

  it("leaves a claimed run retryable when the prepared checkpoint write fails", async () => {
    const clock = new VirtualMonitorClock();
    let failPreparedCheckpoint = false;
    let armPreparedCheckpointFault = true;
    let prepareCalls = 0;
    let deliveryCalls = 0;
    const faultingPool = new FaultingPostgresPool(
      pool as unknown as PostgresPool,
      (text) => {
        if (
          failPreparedCheckpoint &&
          text.includes("INSERT INTO eve_ambient_correlation_workflows")
        ) {
          failPreparedCheckpoint = false;
          return new Error("prepared checkpoint unavailable");
        }
        return undefined;
      },
    );
    const callbacks: AttentionCallbacks = {
      async prepare() {
        prepareCalls += 1;
        if (armPreparedCheckpointFault) {
          armPreparedCheckpointFault = false;
          failPreparedCheckpoint = true;
        }
        return {
          kind: "wake",
          decision: { answer: "wake" },
          routeId: "eve",
          target: "session:incident-42",
          instruction: "Investigate the event.",
          evidence: { summary: "evidence" },
        };
      },
      async deliver(wake) {
        deliveryCalls += 1;
        return {
          wakeKey: wake.wakeKey,
          inputHash: wake.inputHash,
          deliveredAt: clock.now().toISOString(),
          result: { delivered: true },
        };
      },
    };
    const engine = new PostgresAttentionEngine({
      pool: faultingPool,
      callbacks,
      clock,
      engineId: `checkpoint-${randomUUID()}`,
      maxAttempts: 1,
    });
    await engine.accept(await checkpointFanout());

    await expect(engine.runOnce()).rejects.toThrow("prepared checkpoint unavailable");
    expect({ prepareCalls, deliveryCalls }).toEqual({ prepareCalls: 1, deliveryCalls: 0 });

    clock.advance(30_000);
    await expect(engine.runOnce()).resolves.toMatchObject({ delivered: 1 });
    expect({ prepareCalls, deliveryCalls }).toEqual({ prepareCalls: 2, deliveryCalls: 1 });
  });
});

class FaultingPostgresPool implements PostgresPool {
  readonly #pool: PostgresPool;
  readonly #fault: (text: string) => Error | undefined;

  constructor(pool: PostgresPool, fault: (text: string) => Error | undefined) {
    this.#pool = pool;
    this.#fault = fault;
  }

  query<TRow = Record<string, unknown>>(
    text: string,
    values?: readonly unknown[],
  ): Promise<PostgresQueryResult<TRow>> {
    return this.#pool.query<TRow>(text, values);
  }

  async connect(): Promise<PostgresClient> {
    const client = await this.#pool.connect();
    return {
      query: async <TRow = Record<string, unknown>>(
        text: string,
        values?: readonly unknown[],
      ): Promise<PostgresQueryResult<TRow>> => {
        const fault = this.#fault(text);
        if (fault !== undefined) throw fault;
        return client.query<TRow>(text, values);
      },
      release: () => client.release?.(),
    };
  }
}

async function checkpointFanout() {
  const source = await canonicalizeChannelDelivery(
    defineChannelCanonicalization({
      version: 1,
      canonicalize: (raw: { readonly id: string }) => ({
        id: raw.id,
        type: "channel.message",
        version: 1,
        occurredAt: "2026-01-01T00:00:00.000Z",
        data: { body: "checkpoint test" },
        source: {
          channelId: "test",
          installationId: "installation-1",
          tenantId: "tenant-1",
        },
        origin: { kind: "external" as const, depth: 0 },
      }),
    }),
    { id: `event-${randomUUID()}` },
    { applicationId: "checkpoint-test" },
  );
  return compileAcceptedFanout({
    source,
    branches: [
      {
        monitorId: "checkpoint-monitor",
        definitionVersion: "v1",
        correlationKey: "one",
        orderKey: "one",
        mode: "active",
        policy: { buffer: { mode: "immediate" } },
      },
    ],
  });
}
