import {
  AttentionCapacityError,
  attentionValueBytes,
  validateAcceptedFanout,
  type AcceptedFanout,
  type AttentionAcceptanceReceipt,
  type AttentionCallbacks,
  type AttentionEngine,
  type FullAttentionBranch,
} from "./attention.js";
import {
  IdempotencyConflictError,
  type AttentionInstanceKey,
} from "./idempotency.js";
import {
  compileAttentionStreamAppends,
  type AttentionStreamAppend,
  type AttentionStreamAppendReceipt,
} from "./stream-protocol.js";
import type { MonitorClock } from "./types.js";
import {
  AttentionCallbackValidationError,
  applyAttentionDeliveryReceipt,
  applyAttentionStreamAppend,
  applyPreparedAttentionOutcome,
  claimAttentionRun,
  failAttentionRun,
  isCurrentAttentionClaim,
  type AttentionStreamState,
} from "./stream-state.js";

const DEFAULT_MAX_RECENT_MESSAGES = 48;
const DEFAULT_RETRY_DELAY_MS = 1_000;
const DEFAULT_CLAIM_LEASE_MS = 30_000;
const DEFAULT_MAX_ATTEMPTS = 10;
const DEFAULT_MAX_BRANCHES = 1_000;
const DEFAULT_MAX_FANOUT_BYTES = 16 * 1_024 * 1_024;
const DEFAULT_MAX_PREPARED_WAKE_BYTES = 1 * 1_024 * 1_024;

export interface MemoryAttentionEngineFaults {
  readonly beforeBranchAppend?: ((branch: FullAttentionBranch) => void | Promise<void>) | undefined;
  readonly afterBranchAppend?: ((branch: FullAttentionBranch) => void | Promise<void>) | undefined;
}

export interface MemoryAttentionEngineOptions {
  readonly callbacks: AttentionCallbacks;
  readonly clock?: MonitorClock | undefined;
  readonly maxRecentMessages?: number | undefined;
  readonly retryDelayMs?: number | undefined;
  readonly claimLeaseMs?: number | undefined;
  readonly maxAttempts?: number | undefined;
  readonly maxBranches?: number | undefined;
  readonly maxFanoutBytes?: number | undefined;
  readonly maxPreparedWakeBytes?: number | undefined;
  readonly faults?: MemoryAttentionEngineFaults | undefined;
}

export interface MemoryAttentionRunResult {
  readonly claimed: number;
  readonly ignored: number;
  readonly shadowed: number;
  readonly delivered: number;
  readonly failed: number;
  readonly terminalFailures: number;
}

export interface MemoryAttentionDiagnostics {
  readonly correlationStreams: number;
  readonly recentMessages: number;
  readonly bufferedBranchPayloads: number;
  readonly activeBatchPayloads: number;
  readonly preparedWakePayloads: number;
  readonly inFlightBranches: number;
}

type ProcessOutcome =
  | "none"
  | "ignored"
  | "shadowed"
  | "delivered"
  | "failed"
  | "terminal-failure";

/** Executable in-memory World and reference correlation-stream state machine. */
export class MemoryAttentionEngine implements AttentionEngine {
  readonly #callbacks: AttentionCallbacks;
  readonly #clock: MonitorClock;
  readonly #maxRecentMessages: number;
  readonly #retryDelayMs: number;
  readonly #claimLeaseMs: number;
  readonly #maxAttempts: number;
  readonly #maxBranches: number;
  readonly #maxFanoutBytes: number;
  readonly #maxPreparedWakeBytes: number;
  readonly #faults: MemoryAttentionEngineFaults;
  readonly #streams = new Map<AttentionInstanceKey, AttentionStreamState>();
  readonly #locks = new Map<string, Promise<void>>();

  constructor(options: MemoryAttentionEngineOptions) {
    if (options === null || typeof options !== "object") {
      throw new TypeError("memory attention engine options are required");
    }
    if (
      options.callbacks === null ||
      typeof options.callbacks !== "object" ||
      typeof options.callbacks.prepare !== "function" ||
      typeof options.callbacks.deliver !== "function"
    ) {
      throw new TypeError("attention callbacks must define prepare and deliver");
    }
    this.#callbacks = options.callbacks;
    this.#clock = options.clock ?? { now: () => new Date() };
    this.#maxRecentMessages = positiveInteger(
      options.maxRecentMessages ?? DEFAULT_MAX_RECENT_MESSAGES,
      "maxRecentMessages",
    );
    this.#retryDelayMs = positiveInteger(
      options.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS,
      "retryDelayMs",
    );
    this.#claimLeaseMs = positiveInteger(
      options.claimLeaseMs ?? DEFAULT_CLAIM_LEASE_MS,
      "claimLeaseMs",
    );
    this.#maxAttempts = positiveInteger(options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS, "maxAttempts");
    this.#maxBranches = positiveInteger(
      options.maxBranches ?? DEFAULT_MAX_BRANCHES,
      "maxBranches",
    );
    this.#maxFanoutBytes = positiveInteger(
      options.maxFanoutBytes ?? DEFAULT_MAX_FANOUT_BYTES,
      "maxFanoutBytes",
    );
    this.#maxPreparedWakeBytes = positiveInteger(
      options.maxPreparedWakeBytes ?? DEFAULT_MAX_PREPARED_WAKE_BYTES,
      "maxPreparedWakeBytes",
    );
    this.#faults = options.faults ?? {};
  }

  async accept(input: AcceptedFanout): Promise<AttentionAcceptanceReceipt> {
    const fanout = await validateAcceptedFanout(input);
    if (fanout.branches.length > this.#maxBranches) {
      throw new AttentionCapacityError(
        `accepted fan-out exceeds the maximum of ${this.#maxBranches} branches`,
      );
    }
    if (attentionValueBytes(fanout) > this.#maxFanoutBytes) {
      throw new AttentionCapacityError(
        `accepted fan-out exceeds the maximum of ${this.#maxFanoutBytes} bytes`,
      );
    }

    const appends = await compileAttentionStreamAppends(fanout);
    const settled = await Promise.allSettled(
      appends.map((append) => this.#appendStream(append.streamKey, append)),
    );
    const receipts = settled.map((result) => {
      if (result.status === "rejected") throw result.reason;
      return result.value;
    });
    const acceptedAt = receipts.map((receipt) => receipt.acceptedAt).sort().at(-1) ?? this.#now();
    return Object.freeze({
      eventKey: fanout.eventKey,
      occurrenceKey: fanout.occurrenceKey,
      inputHash: fanout.inputHash,
      branchKeys: Object.freeze(fanout.branches.map((branch) => branch.branchKey)),
      acceptedAt,
    });
  }

  async runDue(
    options: { readonly limit?: number | undefined } = {},
  ): Promise<MemoryAttentionRunResult> {
    const limit = positiveInteger(options.limit ?? 100, "limit");
    const result = {
      claimed: 0,
      ignored: 0,
      shadowed: 0,
      delivered: 0,
      failed: 0,
      terminalFailures: 0,
    };
    for (const key of [...this.#streams.keys()].sort()) {
      if (result.claimed >= limit) break;
      const outcome = await this.#processOne(key);
      if (outcome === "none") continue;
      result.claimed += 1;
      if (outcome === "ignored") result.ignored += 1;
      if (outcome === "shadowed") result.shadowed += 1;
      if (outcome === "delivered") result.delivered += 1;
      if (outcome === "failed") result.failed += 1;
      if (outcome === "terminal-failure") result.terminalFailures += 1;
    }
    return result;
  }

  diagnostics(): MemoryAttentionDiagnostics {
    const streams = [...this.#streams.values()];
    return {
      correlationStreams: streams.length,
      recentMessages: streams.reduce((count, stream) => count + stream.recentMessages.length, 0),
      bufferedBranchPayloads: streams.reduce(
        (count, stream) =>
          count +
          (stream.open?.branches.length ?? 0) +
          stream.sealed.reduce((sum, batch) => sum + batch.branches.length, 0),
        0,
      ),
      activeBatchPayloads: streams.reduce(
        (count, stream) => count + (stream.active?.batch.branches.length ?? 0),
        0,
      ),
      preparedWakePayloads: streams.filter((stream) => stream.active?.wake !== undefined).length,
      inFlightBranches: streams.reduce(
        (count, stream) => count + stream.branchLedger.length,
        0,
      ),
    };
  }

  async #appendStream(
    expectedKey: AttentionInstanceKey,
    input: AttentionStreamAppend,
  ): Promise<AttentionStreamAppendReceipt> {
    if (input.streamKey !== expectedKey) {
      throw new TypeError("attention stream append was sent to the wrong stream");
    }
    for (const branch of input.branches) await this.#faults.beforeBranchAppend?.(clone(branch));
    const receipt = await this.#withLock(`stream:${expectedKey}`, async () => {
      const applied = await applyAttentionStreamAppend(this.#streams.get(expectedKey), input, {
        now: this.#now(),
        maxRecentMessages: this.#maxRecentMessages,
      });
      this.#streams.set(expectedKey, applied.state);
      return applied.receipt;
    });
    for (const branch of input.branches) await this.#faults.afterBranchAppend?.(clone(branch));
    return receipt;
  }

  async #processOne(instanceKey: AttentionInstanceKey): Promise<ProcessOutcome> {
    const claimed = await this.#withLock(`stream:${instanceKey}`, async () => {
      const stream = this.#streams.get(instanceKey);
      if (stream === undefined) return undefined;
      const active = await claimAttentionRun(stream, {
        now: this.#now(),
        leaseMs: this.#claimLeaseMs,
      });
      return active === undefined ? undefined : clone(active);
    });
    if (claimed === undefined) return "none";
    let failureStage = claimed.stage;
    try {
      if (claimed.stage === "preparing") {
        const prepared = await this.#callbacks.prepare(deepFreeze(clone(claimed.batch)));
        const transition = await this.#withLock(`stream:${instanceKey}`, async () => {
          const stream = this.#streams.get(instanceKey);
          if (!isCurrentAttentionClaim(stream, claimed, "preparing")) return "stale";
          return applyPreparedAttentionOutcome(stream, prepared, {
            now: this.#now(),
            maxPreparedWakeBytes: this.#maxPreparedWakeBytes,
          });
        });
        if (transition === "stale") return "none";
        if (transition !== "deliver") return transition;
      }
      failureStage = "delivering";
      const wake = await this.#withLock(`stream:${instanceKey}`, async () => {
        const stream = this.#streams.get(instanceKey);
        if (!isCurrentAttentionClaim(stream, claimed, "delivering")) return undefined;
        if (stream.active?.wake === undefined) throw new Error("delivery stage has no prepared wake");
        return clone(stream.active.wake);
      });
      if (wake === undefined) return "none";
      const receipt = await this.#callbacks.deliver(deepFreeze(wake));
      const committed = await this.#withLock(`stream:${instanceKey}`, async () => {
        const stream = this.#streams.get(instanceKey);
        if (!isCurrentAttentionClaim(stream, claimed, "delivering")) return false;
        applyAttentionDeliveryReceipt(stream, receipt, {
          now: this.#now(),
        });
        return true;
      });
      return committed ? "delivered" : "none";
    } catch (error) {
      return this.#withLock(`stream:${instanceKey}`, async () => {
        const stream = this.#streams.get(instanceKey);
        if (!isCurrentAttentionClaim(stream, claimed, failureStage)) return "none";
        return failAttentionRun(stream, error, {
          now: this.#now(),
          retryDelayMs: this.#retryDelayMs,
          maxAttempts: this.#maxAttempts,
          terminalError: isTerminalError,
        });
      });
    }
  }

  #now(): string {
    return this.#clock.now().toISOString();
  }

  async #withLock<T>(key: string, callback: () => Promise<T>): Promise<T> {
    const previous = this.#locks.get(key) ?? Promise.resolve();
    let release!: () => void;
    const current = new Promise<void>((resolve) => {
      release = resolve;
    });
    const queued = previous.then(() => current);
    this.#locks.set(key, queued);
    await previous;
    try {
      return await callback();
    } finally {
      release();
      if (this.#locks.get(key) === queued) this.#locks.delete(key);
    }
  }
}

function isTerminalError(error: unknown): boolean {
  return (
    error instanceof AttentionCapacityError ||
    error instanceof IdempotencyConflictError ||
    error instanceof AttentionCallbackValidationError
  );
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  return value;
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object") {
    Object.freeze(value);
    for (const nested of Object.values(value)) deepFreeze(nested);
  }
  return value;
}
