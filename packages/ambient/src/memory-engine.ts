import {
  AttentionCapacityError,
  validateAcceptedFanout,
  validateFullAttentionBranch,
  type AcceptedFanout,
  type AttentionAcceptanceReceipt,
  type AttentionCallbacks,
  type AttentionEngine,
  type FullAttentionBranch,
} from "./attention.js";
import {
  completeEventCoordinator,
  createEventCoordinator,
  eventCoordinatorExpired,
  markCoordinatorBranchAccepted,
  pendingCoordinatorBranches,
  validateEventCoordinatorRetry,
  type EventCoordinatorState,
} from "./coordinator.js";
import {
  deriveAttentionPartitionKey,
  deriveAttentionInstanceKey,
  hashIdempotencyInput,
  IdempotencyConflictError,
  type AttentionInstanceKey,
  type EventKey,
} from "./idempotency.js";
import type { MonitorClock } from "./types.js";
import {
  AttentionCallbackValidationError,
  appendAttentionBranch,
  applyAttentionDeliveryReceipt,
  applyPreparedAttentionOutcome,
  claimAttentionRun,
  createAttentionWorkflow,
  failAttentionRun,
  isCurrentAttentionClaim,
  purgeAttentionWorkflow,
  type AttentionWorkflowState,
} from "./workflow.js";

const DEFAULT_DEDUPE_MS = 7 * 24 * 60 * 60 * 1_000;
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
  readonly dedupeMs?: number | undefined;
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
  readonly eventCoordinators: number;
  readonly pendingFanoutPayloads: number;
  readonly acceptanceReceipts: number;
  readonly correlationWorkflows: number;
  readonly bufferedBranchPayloads: number;
  readonly activeBatchPayloads: number;
  readonly preparedWakePayloads: number;
  readonly branchReceipts: number;
  readonly deliveryReceipts: number;
  readonly terminalFailures: number;
}

type ProcessOutcome =
  | "none"
  | "ignored"
  | "shadowed"
  | "delivered"
  | "failed"
  | "terminal-failure";

/** Executable reference engine and shared durable workflow state machine. */
export class MemoryAttentionEngine implements AttentionEngine {
  readonly #callbacks: AttentionCallbacks;
  readonly #clock: MonitorClock;
  readonly #dedupeMs: number;
  readonly #retryDelayMs: number;
  readonly #claimLeaseMs: number;
  readonly #maxAttempts: number;
  readonly #maxBranches: number;
  readonly #maxFanoutBytes: number;
  readonly #maxPreparedWakeBytes: number;
  readonly #faults: MemoryAttentionEngineFaults;
  readonly #events = new Map<EventKey, EventCoordinatorState>();
  readonly #workflows = new Map<AttentionInstanceKey, AttentionWorkflowState>();
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
    this.#dedupeMs = positiveInteger(options.dedupeMs ?? DEFAULT_DEDUPE_MS, "dedupeMs");
    this.#retryDelayMs = positiveInteger(
      options.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS,
      "retryDelayMs",
    );
    this.#claimLeaseMs = positiveInteger(
      options.claimLeaseMs ?? DEFAULT_CLAIM_LEASE_MS,
      "claimLeaseMs",
    );
    this.#maxAttempts = positiveInteger(options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS, "maxAttempts");
    this.#maxBranches = positiveInteger(options.maxBranches ?? DEFAULT_MAX_BRANCHES, "maxBranches");
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
    const proposed = await validateAcceptedFanout(input);
    return this.#withLock(`event:${proposed.eventKey}`, async () => {
      const now = this.#now();
      const existing = this.#events.get(proposed.eventKey);
      if (existing !== undefined && eventCoordinatorExpired(existing, now)) {
        this.#events.delete(proposed.eventKey);
      }
      let coordinator = this.#events.get(proposed.eventKey);
      if (coordinator === undefined) {
        coordinator = createEventCoordinator(proposed, {
          now,
          maxBranches: this.#maxBranches,
          maxFanoutBytes: this.#maxFanoutBytes,
        });
        this.#events.set(proposed.eventKey, coordinator);
      } else {
        validateEventCoordinatorRetry(coordinator, proposed);
      }
      if (coordinator.receipt !== undefined) return clone(coordinator.receipt);
      for (const branch of pendingCoordinatorBranches(coordinator)) {
        await this.#faults.beforeBranchAppend?.(clone(branch));
        await this.#appendBranch(branch);
        await this.#faults.afterBranchAppend?.(clone(branch));
        markCoordinatorBranchAccepted(coordinator, branch.branchKey);
      }
      const receipt = completeEventCoordinator(coordinator, {
        now: this.#now(),
        dedupeMs: this.#dedupeMs,
      });
      return clone(receipt);
    });
  }

  async runDue(
    options: { readonly limit?: number | undefined } = {},
  ): Promise<MemoryAttentionRunResult> {
    const limit = positiveInteger(options.limit ?? 100, "limit");
    this.#purgeExpiredSync();
    const result = {
      claimed: 0,
      ignored: 0,
      shadowed: 0,
      delivered: 0,
      failed: 0,
      terminalFailures: 0,
    };
    for (const key of [...this.#workflows.keys()].sort()) {
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
    this.#purgeExpiredSync();
    const workflows = [...this.#workflows.values()];
    return {
      eventCoordinators: this.#events.size,
      pendingFanoutPayloads: [...this.#events.values()].filter(
        (coordinator) => coordinator.pendingFanout !== undefined,
      ).length,
      acceptanceReceipts: [...this.#events.values()].filter(
        (coordinator) => coordinator.receipt !== undefined,
      ).length,
      correlationWorkflows: workflows.length,
      bufferedBranchPayloads: workflows.reduce(
        (count, workflow) =>
          count +
          (workflow.open?.branches.length ?? 0) +
          workflow.sealed.reduce((sum, batch) => sum + batch.branches.length, 0),
        0,
      ),
      activeBatchPayloads: workflows.reduce(
        (count, workflow) => count + (workflow.active?.batch.branches.length ?? 0),
        0,
      ),
      preparedWakePayloads: workflows.filter((workflow) => workflow.active?.wake !== undefined)
        .length,
      branchReceipts: workflows.reduce(
        (count, workflow) => count + workflow.branchLedger.length,
        0,
      ),
      deliveryReceipts: workflows.reduce(
        (count, workflow) => count + workflow.deliveryReceipts.length,
        0,
      ),
      terminalFailures: workflows.reduce(
        (count, workflow) => count + workflow.terminalFailures.length,
        0,
      ),
    };
  }

  async #appendBranch(input: FullAttentionBranch): Promise<void> {
    const branch = await validateFullAttentionBranch(input);
    const partitionCellKey = await deriveAttentionPartitionKey({
      applicationId: branch.applicationId,
      tenantId: branch.tenantId,
      channelId: branch.event.source.channelId,
      installationId: branch.event.source.installationId,
      partitionKey: branch.partitionKey,
    });
    const instanceKey = await deriveAttentionInstanceKey({
      partitionCellKey,
      monitorId: branch.monitorId,
      definitionVersion: branch.definitionVersion,
      correlationKey: branch.correlationKey,
    });
    const policyHash = await hashIdempotencyInput({ mode: branch.mode, policy: branch.policy });
    await this.#withLock(`workflow:${instanceKey}`, async () => {
      const now = this.#now();
      let workflow = this.#workflows.get(instanceKey);
      if (workflow === undefined) {
        workflow = createAttentionWorkflow({ instanceKey, branch, policyHash });
        this.#workflows.set(instanceKey, workflow);
      }
      appendAttentionBranch(workflow, branch, {
        now,
        dedupeMs: this.#dedupeMs,
        policyHash,
      });
    });
  }

  async #processOne(instanceKey: AttentionInstanceKey): Promise<ProcessOutcome> {
    const claimed = await this.#withLock(`workflow:${instanceKey}`, async () => {
      const workflow = this.#workflows.get(instanceKey);
      if (workflow === undefined) return undefined;
      const active = await claimAttentionRun(workflow, {
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
        const transition = await this.#withLock(`workflow:${instanceKey}`, async () => {
          const workflow = this.#workflows.get(instanceKey);
          if (!isCurrentAttentionClaim(workflow, claimed, "preparing")) return "stale";
          const outcome = await applyPreparedAttentionOutcome(workflow, prepared, {
            now: this.#now(),
            dedupeMs: this.#dedupeMs,
            maxPreparedWakeBytes: this.#maxPreparedWakeBytes,
          });
          return outcome;
        });
        if (transition === "stale") return "none";
        if (transition !== "deliver") return transition;
      }
      failureStage = "delivering";
      const wake = await this.#withLock(`workflow:${instanceKey}`, async () => {
        const workflow = this.#workflows.get(instanceKey);
        if (!isCurrentAttentionClaim(workflow, claimed, "delivering")) return undefined;
        if (workflow.active?.wake === undefined) throw new Error("delivery stage has no prepared wake");
        return clone(workflow.active.wake);
      });
      if (wake === undefined) return "none";
      const receipt = await this.#callbacks.deliver(deepFreeze(wake));
      const committed = await this.#withLock(`workflow:${instanceKey}`, async () => {
        const workflow = this.#workflows.get(instanceKey);
        if (!isCurrentAttentionClaim(workflow, claimed, "delivering")) return false;
        applyAttentionDeliveryReceipt(workflow, receipt, {
          now: this.#now(),
          dedupeMs: this.#dedupeMs,
        });
        return true;
      });
      return committed ? "delivered" : "none";
    } catch (error) {
      return this.#withLock(`workflow:${instanceKey}`, async () => {
        const workflow = this.#workflows.get(instanceKey);
        if (!isCurrentAttentionClaim(workflow, claimed, failureStage)) {
          return "none";
        }
        const outcome = failAttentionRun(workflow, error, {
          now: this.#now(),
          dedupeMs: this.#dedupeMs,
          retryDelayMs: this.#retryDelayMs,
          maxAttempts: this.#maxAttempts,
          terminalError: isTerminalError,
        });
        return outcome;
      });
    }
  }

  #purgeExpiredSync(): boolean {
    const now = this.#now();
    let changed = false;
    for (const [key, coordinator] of this.#events) {
      if (eventCoordinatorExpired(coordinator, now)) {
        this.#events.delete(key);
        changed = true;
      }
    }
    for (const [key, workflow] of this.#workflows) {
      const before = JSON.stringify(workflow);
      if (purgeAttentionWorkflow(workflow, now) === "empty") this.#workflows.delete(key);
      if (before !== JSON.stringify(workflow) || !this.#workflows.has(key)) changed = true;
    }
    return changed;
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
