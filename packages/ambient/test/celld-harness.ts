import { AttentionCell } from "../src/celld-worker.js";
import { secretsMatch } from "../src/celld.js";
import type { MonitorClock } from "../src/types.js";
import type { FullAttentionBranch } from "../src/attention.js";
import { eventCoordinatorExpired } from "../src/coordinator.js";
import { purgeAttentionWorkflow } from "../src/workflow.js";

export interface FakeDurableObjectState {
  readonly id: { readonly name: string; toString(): string };
  readonly storage: {
    get(key: string): Promise<unknown>;
    put(key: string, value: unknown): Promise<void>;
    delete(key: string): Promise<void>;
    getAlarm(): Promise<number | null>;
    setAlarm(at: number): Promise<void>;
    deleteAlarm(): Promise<void>;
  };
  blockConcurrencyWhile<T>(callback: () => Promise<T>): Promise<T>;
  alarmAt: number | null;
  readonly map: Map<string, unknown>;
}

export function createFakeDurableObjectState(name: string): FakeDurableObjectState {
  const map = new Map<string, unknown>();
  const state: FakeDurableObjectState = {
    id: { name, toString: () => `fake:${name}` },
    storage: {
      async get(key) {
        return map.get(key);
      },
      async put(key, value) {
        map.set(key, value);
      },
      async delete(key) {
        map.delete(key);
      },
      async getAlarm() {
        return state.alarmAt;
      },
      async setAlarm(at) {
        state.alarmAt = at;
      },
      async deleteAlarm() {
        state.alarmAt = null;
      },
    },
    async blockConcurrencyWhile<T>(callback: () => Promise<T>): Promise<T> {
      return callback();
    },
    alarmAt: null,
    map,
  };
  return state;
}

export interface FakeCelldFleetOptions {
  readonly baseUrl?: string | undefined;
  readonly secret: string;
  readonly clock: MonitorClock;
  readonly callbacks: (request: Request) => Promise<Response>;
  readonly limits?: Readonly<Record<string, number>> | undefined;
  readonly faults?: {
    readonly beforeBranchAppend?: ((branch: FullAttentionBranch) => void | Promise<void>) | undefined;
    readonly afterBranchAppend?: ((branch: FullAttentionBranch) => void | Promise<void>) | undefined;
  } | undefined;
}

interface Placement {
  readonly state: FakeDurableObjectState;
  readonly cell: AttentionCell;
}

export class FakeCelldFleet {
  readonly baseUrl: string;
  readonly #secret: string;
  readonly #clock: MonitorClock;
  readonly #callbacks: (request: Request) => Promise<Response>;
  readonly #limits: Readonly<Record<string, number>>;
  readonly #faults: NonNullable<FakeCelldFleetOptions["faults"]>;
  readonly #cells = new Map<string, Placement>();
  readonly requests: Array<{ readonly name: string; readonly action: string; readonly body: unknown }> = [];
  readonly outcomes: string[] = [];

  constructor(options: FakeCelldFleetOptions) {
    this.baseUrl = options.baseUrl ?? "http://fleet.test";
    this.#secret = options.secret;
    this.#clock = options.clock;
    this.#callbacks = options.callbacks;
    this.#limits = options.limits ?? {};
    this.#faults = options.faults ?? {};
  }

  get cellNames(): readonly string[] {
    return [...this.#cells.keys()].sort();
  }

  state(name: string): FakeDurableObjectState | undefined {
    return this.#cells.get(name)?.state;
  }

  readonly fetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const request = new Request(input as never, init);
    const url = new URL(request.url);
    if (url.pathname === "/health") return jsonResponse({ ok: true }, 200);
    const parts = url.pathname.split("/").filter(Boolean);
    if (parts[0] !== "cells" || parts.length !== 3) return jsonResponse({ error: "not found" }, 404);
    const presented = /^Bearer[ ]+(.+)$/i.exec(
      request.headers.get("authorization")?.trim() ?? "",
    )?.[1] ?? "";
    if (!secretsMatch(presented, this.#secret)) return jsonResponse({ error: "unauthorized" }, 401);
    const name = decodeURIComponent(parts[1]!);
    const action = parts[2]!;
    let body: unknown = null;
    if (request.method === "POST") body = await request.clone().json();
    this.requests.push({ name, action, body });
    const placement = this.#place(name);
    const forwarded = new Request(`${this.baseUrl}/${action}`, request);
    forwarded.headers.set("x-cell-name", name);
    return placement.cell.fetch(forwarded);
  };

  async fireDueAlarms(): Promise<readonly { readonly name: string; readonly error: unknown }[]> {
    const now = this.#clock.now().getTime();
    const due = [...this.#cells.entries()]
      .filter(([, placement]) => placement.state.alarmAt !== null && placement.state.alarmAt <= now)
      .sort((left, right) => (left[1].state.alarmAt ?? 0) - (right[1].state.alarmAt ?? 0));
    const results: Array<{ readonly name: string; readonly error: unknown }> = [];
    for (const [name, placement] of due) {
      const scheduled = placement.state.alarmAt;
      try {
        await placement.cell.alarm();
        results.push({ name, error: null });
      } catch (error) {
        placement.state.alarmAt = scheduled;
        results.push({ name, error });
      }
    }
    return results;
  }

  diagnostics(): {
    eventCoordinators: number;
    pendingFanoutPayloads: number;
    acceptanceReceipts: number;
    correlationWorkflows: number;
    bufferedBranchPayloads: number;
    activeBatchPayloads: number;
    preparedWakePayloads: number;
    branchReceipts: number;
    deliveryReceipts: number;
    terminalFailures: number;
  } {
    const result = {
      eventCoordinators: 0,
      pendingFanoutPayloads: 0,
      acceptanceReceipts: 0,
      correlationWorkflows: 0,
      bufferedBranchPayloads: 0,
      activeBatchPayloads: 0,
      preparedWakePayloads: 0,
      branchReceipts: 0,
      deliveryReceipts: 0,
      terminalFailures: 0,
    };
    for (const placement of this.#cells.values()) {
      const raw = placement.state.map.get("record");
      if (raw === undefined) continue;
      const record = JSON.parse(String(raw)) as any;
      if (record.kind !== "partition") continue;
      for (const coordinator of record.coordinators) {
        if (eventCoordinatorExpired(coordinator, this.#clock.now().toISOString())) continue;
        result.eventCoordinators += 1;
        if (coordinator.pendingFanout !== undefined) result.pendingFanoutPayloads += 1;
        if (coordinator.receipt !== undefined) result.acceptanceReceipts += 1;
      }
      for (const source of record.workflows) {
        const workflow = structuredClone(source);
        if (purgeAttentionWorkflow(workflow, this.#clock.now().toISOString()) === "empty") continue;
        result.correlationWorkflows += 1;
        result.bufferedBranchPayloads +=
          (workflow.open?.branches.length ?? 0) +
          workflow.sealed.reduce((sum: number, batch: any) => sum + batch.branches.length, 0);
        result.activeBatchPayloads += workflow.active?.batch.branches.length ?? 0;
        if (workflow.active?.wake !== undefined) result.preparedWakePayloads += 1;
        result.branchReceipts += workflow.branchLedger.length;
        result.deliveryReceipts += workflow.deliveryReceipts.length;
        result.terminalFailures += workflow.terminalFailures.length;
      }
    }
    return result;
  }

  #place(name: string): Placement {
    let placement = this.#cells.get(name);
    if (placement !== undefined) return placement;
    const state = createFakeDurableObjectState(name);
    const env: Record<string, unknown> = {
      ATTENTION_SECRET: this.#secret,
      ATTENTION_CALLBACK_URL: "http://callbacks.test/ambient",
      clock: this.#clock,
      beforeBranchAppend: this.#faults.beforeBranchAppend,
      afterBranchAppend: this.#faults.afterBranchAppend,
      onOutcome: (_name: string, outcome: string) => this.outcomes.push(outcome),
      ...Object.fromEntries(
        Object.entries(this.#limits).map(([key, value]) => [key, String(value)]),
      ),
    };
    env.fetch = async (input: string, init: RequestInit): Promise<Response> => {
      const target = new URL(input);
      return target.origin === new URL(this.baseUrl).origin
        ? this.fetch(input, init)
        : this.#callbacks(new Request(input, init));
    };
    placement = { state, cell: new AttentionCell(state, env) };
    this.#cells.set(name, placement);
    return placement;
  }
}

export function jsonResponse(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}
