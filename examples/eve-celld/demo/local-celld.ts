import attentionWorker, {
  AttentionCell,
} from "@ewhauser/eve-ambient/celld-worker";
import type { VirtualMonitorClock } from "@ewhauser/eve-ambient/testing";

import { startFetchServer, type LocalFetchServer } from "./http.js";

type DemoLog = (message: string) => void;

interface LocalDurableObjectState {
  readonly id: { readonly name: string; toString(): string };
  readonly storage: {
    get(key: string): Promise<unknown>;
    put(key: string, value: unknown): Promise<void>;
    delete(key: string): Promise<void>;
    getAlarm(): Promise<number | null>;
    setAlarm(at: number): Promise<void>;
    deleteAlarm(): Promise<void>;
  };
  readonly map: Map<string, unknown>;
  alarmAt: number | null;
  blockConcurrencyWhile<T>(callback: () => Promise<T>): Promise<T>;
}

interface Placement {
  readonly cell: AttentionCell;
  readonly state: LocalDurableObjectState;
}

export interface LocalCelldAlarmResult {
  readonly cellName: string;
  readonly error?: unknown;
}

export interface LocalCelldDiagnostics {
  readonly cells: number;
  readonly payloadBearingCells: number;
  readonly receiptOnlyCells: number;
}

/**
 * Minimal local host for the production celld worker. It supplies in-memory
 * Durable Object storage and alarms; all fleet and callback handoffs still use
 * HTTP and execute the packaged worker router and AttentionCell class.
 */
export class LocalCelldFleet {
  readonly #clock: VirtualMonitorClock;
  readonly #log: DemoLog;
  readonly #placements = new Map<string, Placement>();
  readonly #secret: string;
  #callbackBaseUrl?: string;
  #server: LocalFetchServer | undefined;
  readonly outcomes: string[] = [];

  constructor(options: {
    readonly clock: VirtualMonitorClock;
    readonly log: DemoLog;
    readonly secret: string;
  }) {
    this.#clock = options.clock;
    this.#log = options.log;
    this.#secret = options.secret;
  }

  get url(): string {
    if (this.#server === undefined) throw new Error("local celld fleet has not started");
    return this.#server.url;
  }

  get cellCount(): number {
    return this.#placements.size;
  }

  async start(): Promise<void> {
    if (this.#server !== undefined) throw new Error("local celld fleet already started");
    this.#server = await startFetchServer((request) =>
      attentionWorker.fetch(request, {
        ATTENTION_SECRET: this.#secret,
        ATTENTION: {
          idFromName: (name: string) => name,
          get: (name: string) => ({
            fetch: (request: Request) => this.#placement(name).cell.fetch(request),
          }),
        },
      }),
    );
  }

  setCallbackBaseUrl(url: string): void {
    this.#callbackBaseUrl = new URL(url).toString().replace(/\/$/, "");
  }

  async fireDueAlarms(): Promise<readonly LocalCelldAlarmResult[]> {
    const now = this.#clock.now().getTime();
    const due = [...this.#placements.entries()]
      .filter(([, placement]) =>
        placement.state.alarmAt !== null && placement.state.alarmAt <= now,
      )
      .sort((left, right) =>
        (left[1].state.alarmAt ?? 0) - (right[1].state.alarmAt ?? 0),
      );
    const results: LocalCelldAlarmResult[] = [];
    for (const [cellName, placement] of due) {
      this.#log(`[celld] alarm ${shortKey(cellName)}`);
      const scheduledAt = placement.state.alarmAt;
      try {
        await placement.cell.alarm();
        results.push({ cellName });
      } catch (error) {
        placement.state.alarmAt = scheduledAt;
        results.push({ cellName, error });
      }
    }
    return results;
  }

  async diagnostics(): Promise<LocalCelldDiagnostics> {
    let payloadBearingCells = 0;
    let receiptOnlyCells = 0;
    for (const cellName of this.#placements.keys()) {
      const response = await fetch(
        `${this.url}/cells/${encodeURIComponent(cellName)}/diagnostics`,
        { headers: { authorization: `Bearer ${this.#secret}` } },
      );
      if (!response.ok) {
        throw new Error(`celld diagnostics failed with ${response.status}`);
      }
      const diagnostic = (await response.json()) as Record<string, unknown>;
      const hasPayload =
        diagnostic.pendingFanout === true ||
        positiveNumber(diagnostic.bufferedBranches) ||
        positiveNumber(diagnostic.activeBatchBranches) ||
        diagnostic.preparedWake === true;
      if (hasPayload) payloadBearingCells += 1;
      else receiptOnlyCells += 1;
    }
    return {
      cells: this.#placements.size,
      payloadBearingCells,
      receiptOnlyCells,
    };
  }

  async close(): Promise<void> {
    const server = this.#server;
    this.#server = undefined;
    if (server !== undefined) await server.close();
  }

  #placement(name: string): Placement {
    let placement = this.#placements.get(name);
    if (placement !== undefined) return placement;
    if (this.#callbackBaseUrl === undefined) {
      throw new Error("local celld callback URL has not been configured");
    }
    const state = createLocalDurableObjectState(name);
    const cell = new AttentionCell(state, {
      ATTENTION_SECRET: this.#secret,
      ATTENTION_CALLBACK_URL: this.#callbackBaseUrl,
      CELLD_FLEET_URL: this.url,
      clock: this.#clock,
      onOutcome: (_cellName: string, outcome: string) => {
        this.outcomes.push(outcome);
        this.#log(`[celld] outcome ${outcome}`);
      },
    });
    placement = { cell, state };
    this.#placements.set(name, placement);
    this.#log(`[celld] placed ${shortKey(name)}`);
    return placement;
  }
}

function createLocalDurableObjectState(name: string): LocalDurableObjectState {
  const map = new Map<string, unknown>();
  const state: LocalDurableObjectState = {
    id: { name, toString: () => `local:${name}` },
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
    map,
    alarmAt: null,
    async blockConcurrencyWhile<T>(callback: () => Promise<T>): Promise<T> {
      return callback();
    },
  };
  return state;
}

function shortKey(value: string): string {
  return value.length <= 28 ? value : `${value.slice(0, 12)}…${value.slice(-12)}`;
}

function positiveNumber(value: unknown): boolean {
  return typeof value === "number" && value > 0;
}
