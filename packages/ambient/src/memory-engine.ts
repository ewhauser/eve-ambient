import {
  attentionValueBytes,
  AttentionCapacityError,
  compareAttentionBranches,
  validateAcceptedFanout,
  type AcceptedFanout,
  type AttentionAcceptanceReceipt,
  type AttentionCallbacks,
  type AttentionDeliveryReceipt,
  type AttentionEngine,
  type FrozenAttentionBatch,
  type FullAttentionBranch,
  type PreparedAttentionOutcome,
  type PreparedAttentionWake,
  type SerializableMailboxPolicy,
} from "./attention.js";
import { canonicalJson } from "./canonical.js";
import {
  assertIdempotencyInput,
  deriveAttentionBatchKey,
  deriveAttentionInstanceKey,
  deriveAttentionRunKey,
  deriveAttentionWakeKey,
  hashIdempotencyInput,
  IdempotencyConflictError,
  parseIdempotencyKey,
  parseInputHash,
  type AttentionInstanceKey,
  type BranchKey,
  type EventKey,
  type InputHash,
  type RunKey,
  type WakeKey,
} from "./idempotency.js";
import { addMs } from "./time.js";
import type { JsonValue, MonitorBatchClosedBy, MonitorClock } from "./types.js";

const DEFAULT_DEDUPE_MS = 7 * 24 * 60 * 60 * 1_000;
const DEFAULT_RETRY_DELAY_MS = 1_000;
const DEFAULT_MAX_ATTEMPTS = 10;
const DEFAULT_MAX_BRANCHES = 1_000;
const DEFAULT_MAX_FANOUT_BYTES = 16 * 1_024 * 1_024;
const DEFAULT_MAX_PREPARED_WAKE_BYTES = 1 * 1_024 * 1_024;

export interface MemoryAttentionEngineFaults {
  /** Fails before the correlation workflow receives the complete branch. */
  readonly beforeBranchAppend?: ((branch: FullAttentionBranch) => void | Promise<void>) | undefined;
  /** Simulates losing the response after the correlation workflow committed. */
  readonly afterBranchAppend?: ((branch: FullAttentionBranch) => void | Promise<void>) | undefined;
}

export interface MemoryAttentionEngineOptions {
  readonly callbacks: AttentionCallbacks;
  readonly clock?: MonitorClock | undefined;
  readonly dedupeMs?: number | undefined;
  readonly retryDelayMs?: number | undefined;
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

/** Payload-free, backend-specific introspection for conformance tests. */
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

interface EventCoordinatorRecord {
  readonly eventKey: EventKey;
  readonly inputHash: InputHash;
  readonly occurrenceKey: AcceptedFanout["occurrenceKey"];
  readonly manifestHash: AcceptedFanout["manifestHash"];
  readonly branchInputs: ReadonlyMap<BranchKey, InputHash>;
  readonly acceptedAt: string;
  readonly acceptedBranches: Set<BranchKey>;
  dedupeExpiresAt?: string | undefined;
  fanout?: AcceptedFanout | undefined;
  receipt?: AttentionAcceptanceReceipt | undefined;
}

interface BranchLedgerEntry {
  readonly inputHash: InputHash;
  expiresAt: string;
  terminal: boolean;
}

interface BufferedBatch {
  readonly branches: FullAttentionBranch[];
  readonly bytes: number;
  readonly openedAt: string;
  readonly updatedAt: string;
  readonly closedAt?: string | undefined;
  readonly closedBy?: MonitorBatchClosedBy | undefined;
}

interface ActiveRun {
  readonly batch: FrozenAttentionBatch;
  stage: "preparing" | "delivering";
  processing: boolean;
  retryAt: string;
  failures: number;
  prepared?: PreparedAttentionOutcome | undefined;
  wake?: PreparedAttentionWake | undefined;
}

interface RetainedDeliveryReceipt {
  readonly receipt: AttentionDeliveryReceipt;
  readonly expiresAt: string;
}

interface CorrelationWorkflow {
  readonly instanceKey: AttentionInstanceKey;
  readonly applicationId: string;
  readonly tenantId: string;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly correlationKey: string;
  readonly mode: FullAttentionBranch["mode"];
  readonly policy: SerializableMailboxPolicy;
  readonly policyHash: InputHash;
  readonly branchLedger: Map<BranchKey, BranchLedgerEntry>;
  readonly deliveryReceipts: Map<WakeKey, RetainedDeliveryReceipt>;
  readonly terminalFailures: Map<RunKey, string>;
  open?: BufferedBatch | undefined;
  sealed: BufferedBatch[];
  active?: ActiveRun | undefined;
  cooldownUntil?: string | undefined;
}

type ProcessOutcome =
  | "none"
  | "ignored"
  | "shadowed"
  | "delivered"
  | "failed"
  | "terminal-failure";

/**
 * Executable RFC 0002 reference backend.
 *
 * It deliberately exposes no event lookup. Complete source values exist only
 * in pending fan-out, buffered branches, an active batch, or a prepared wake.
 */
export class MemoryAttentionEngine implements AttentionEngine {
  readonly #callbacks: AttentionCallbacks;
  readonly #clock: MonitorClock;
  readonly #dedupeMs: number;
  readonly #retryDelayMs: number;
  readonly #maxAttempts: number;
  readonly #maxBranches: number;
  readonly #maxFanoutBytes: number;
  readonly #maxPreparedWakeBytes: number;
  readonly #faults: MemoryAttentionEngineFaults;
  readonly #events = new Map<EventKey, EventCoordinatorRecord>();
  readonly #workflows = new Map<AttentionInstanceKey, CorrelationWorkflow>();
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
    const lockKey = `event:${proposed.eventKey}`;
    return this.#withLock(lockKey, async () => {
      const now = this.#now();
      this.#purgeExpired(now);
      let record = this.#events.get(proposed.eventKey);
      if (
        record !== undefined &&
        record.receipt !== undefined &&
        record.dedupeExpiresAt !== undefined &&
        record.dedupeExpiresAt <= now
      ) {
        this.#events.delete(proposed.eventKey);
        record = undefined;
      }
      if (record === undefined) {
        if (proposed.branches.length > this.#maxBranches) {
          throw new AttentionCapacityError(
            `accepted fan-out exceeds the maximum of ${this.#maxBranches} branches`,
          );
        }
        if (attentionValueBytes(proposed) > this.#maxFanoutBytes) {
          throw new AttentionCapacityError(
            `accepted fan-out exceeds the maximum of ${this.#maxFanoutBytes} bytes`,
          );
        }
        record = {
          eventKey: proposed.eventKey,
          inputHash: proposed.inputHash,
          occurrenceKey: proposed.occurrenceKey,
          manifestHash: proposed.manifestHash,
          branchInputs: new Map(
            proposed.branches.map((branch) => [branch.branchKey, branch.inputHash] as const),
          ),
          acceptedAt: now,
          acceptedBranches: new Set(),
          fanout: proposed,
        };
        this.#events.set(proposed.eventKey, record);
      } else {
        assertIdempotencyInput({
          namespace: "attention-event-admission",
          key: proposed.eventKey,
          existingInputHash: record.inputHash,
          receivedInputHash: proposed.inputHash,
        });
        for (const branch of proposed.branches) {
          const original = record.branchInputs.get(branch.branchKey);
          if (original !== undefined) {
            assertIdempotencyInput({
              namespace: "attention-fanout-branch",
              key: branch.branchKey,
              existingInputHash: original,
              receivedInputHash: branch.inputHash,
            });
          }
        }
      }
      if (record.receipt !== undefined) return deepFreeze(clone(record.receipt));
      const frozen = record.fanout;
      if (frozen === undefined) {
        throw new Error("pending event coordinator lost its frozen fan-out");
      }
      for (const branch of frozen.branches) {
        if (record.acceptedBranches.has(branch.branchKey)) continue;
        await this.#faults.beforeBranchAppend?.(clone(branch));
        await this.#appendBranch(branch);
        await this.#faults.afterBranchAppend?.(clone(branch));
        record.acceptedBranches.add(branch.branchKey);
      }
      const completedAt = this.#now();
      record.dedupeExpiresAt = addMs(completedAt, this.#dedupeMs);
      const receipt: AttentionAcceptanceReceipt = deepFreeze({
        eventKey: record.eventKey,
        occurrenceKey: record.occurrenceKey,
        inputHash: record.inputHash,
        manifestHash: record.manifestHash,
        branchKeys: [...record.branchInputs.keys()],
        acceptedAt: record.acceptedAt,
        dedupeExpiresAt: record.dedupeExpiresAt,
      });
      record.receipt = receipt;
      delete record.fanout;
      return deepFreeze(clone(receipt));
    });
  }

  /** Runs each correlation workflow that is due at most once. */
  async runDue(
    options: { readonly limit?: number | undefined } = {},
  ): Promise<MemoryAttentionRunResult> {
    const limit = positiveInteger(options.limit ?? 100, "limit");
    const now = this.#now();
    this.#purgeExpired(now);
    const keys = [...this.#workflows.keys()].sort();
    const result = {
      claimed: 0,
      ignored: 0,
      shadowed: 0,
      delivered: 0,
      failed: 0,
      terminalFailures: 0,
    };
    for (const key of keys) {
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
    const now = this.#now();
    this.#purgeExpired(now);
    const workflows = [...this.#workflows.values()];
    return {
      eventCoordinators: this.#events.size,
      pendingFanoutPayloads: [...this.#events.values()].filter(
        (record) => record.fanout !== undefined,
      ).length,
      acceptanceReceipts: [...this.#events.values()].filter(
        (record) => record.receipt !== undefined,
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
        (count, workflow) => count + workflow.branchLedger.size,
        0,
      ),
      deliveryReceipts: workflows.reduce(
        (count, workflow) => count + workflow.deliveryReceipts.size,
        0,
      ),
      terminalFailures: workflows.reduce(
        (count, workflow) => count + workflow.terminalFailures.size,
        0,
      ),
    };
  }

  async #appendBranch(branch: FullAttentionBranch): Promise<void> {
    const instanceKey = await deriveAttentionInstanceKey({
      applicationId: branch.applicationId,
      tenantId: branch.tenantId,
      monitorId: branch.monitorId,
      definitionVersion: branch.definitionVersion,
      correlationKey: branch.correlationKey,
    });
    const policyHash = await hashIdempotencyInput({
      mode: branch.mode,
      policy: branch.policy,
    });
    await this.#withLock(`workflow:${instanceKey}`, async () => {
      const now = this.#now();
      let workflow = this.#workflows.get(instanceKey);
      if (workflow === undefined) {
        workflow = {
          instanceKey,
          applicationId: branch.applicationId,
          tenantId: branch.tenantId,
          monitorId: branch.monitorId,
          definitionVersion: branch.definitionVersion,
          correlationKey: branch.correlationKey,
          mode: branch.mode,
          policy: clone(branch.policy),
          policyHash,
          branchLedger: new Map(),
          deliveryReceipts: new Map(),
          terminalFailures: new Map(),
          sealed: [],
        };
        this.#workflows.set(instanceKey, workflow);
      } else {
        assertIdempotencyInput({
          namespace: "attention-instance-policy",
          key: instanceKey,
          existingInputHash: workflow.policyHash,
          receivedInputHash: policyHash,
        });
      }
      const prior = workflow.branchLedger.get(branch.branchKey);
      if (prior !== undefined) {
        assertIdempotencyInput({
          namespace: "attention-branch-append",
          key: branch.branchKey,
          existingInputHash: prior.inputHash,
          receivedInputHash: branch.inputHash,
        });
        if (!prior.terminal || prior.expiresAt > now) return;
        workflow.branchLedger.delete(branch.branchKey);
      }
      workflow.branchLedger.set(branch.branchKey, {
        inputHash: branch.inputHash,
        expiresAt: addMs(now, this.#dedupeMs),
        terminal: false,
      });
      this.#buffer(workflow, clone(branch), now);
    });
  }

  #buffer(workflow: CorrelationWorkflow, branch: FullAttentionBranch, now: string): void {
    const bytes = attentionValueBytes(branch);
    const policy = workflow.policy.buffer;
    if (policy.mode === "immediate" && !isFuture(workflow.cooldownUntil, now)) {
      workflow.sealed.push({
        branches: [branch],
        bytes,
        openedAt: now,
        updatedAt: now,
        closedAt: now,
        closedBy: "immediate",
      });
      return;
    }
    if (
      policy.mode === "debounce" &&
      workflow.open !== undefined &&
      (workflow.open.branches.length + 1 > policy.maxEvents ||
        workflow.open.bytes + bytes > policy.maxBytes)
    ) {
      const closedBy: MonitorBatchClosedBy =
        workflow.open.branches.length + 1 > policy.maxEvents ? "max-events" : "max-bytes";
      workflow.sealed.push({ ...workflow.open, closedAt: now, closedBy });
      delete workflow.open;
    }
    workflow.open =
      workflow.open === undefined
        ? { branches: [branch], bytes, openedAt: now, updatedAt: now }
        : {
            ...workflow.open,
            branches: [...workflow.open.branches, branch],
            bytes: workflow.open.bytes + bytes,
            updatedAt: now,
          };
  }

  async #processOne(instanceKey: AttentionInstanceKey): Promise<ProcessOutcome> {
    const claimNow = this.#now();
    const claimed = await this.#withLock(`workflow:${instanceKey}`, async () => {
      const workflow = this.#workflows.get(instanceKey);
      if (workflow === undefined) return false;
      if (workflow.active !== undefined) {
        if (workflow.active.processing || workflow.active.retryAt > claimNow) return false;
        workflow.active.processing = true;
        return true;
      }
      const batch = await this.#freezeDueBatch(workflow, claimNow);
      if (batch === undefined) return false;
      workflow.active = {
        batch,
        stage: "preparing",
        processing: true,
        retryAt: claimNow,
        failures: 0,
      };
      return true;
    });
    if (!claimed) return "none";
    try {
      const workflow = this.#workflows.get(instanceKey);
      const active = workflow?.active;
      if (workflow === undefined || active === undefined) return "none";
      if (active.stage === "preparing") {
        const callbackOutput = await this.#callbacks.prepare(
          deepFreeze(clone(active.batch)),
        );
        const prepared = validateCallbackOutput(
          () => validatePreparedOutcome(callbackOutput),
        );
        const preparedAt = this.#now();
        const preparedResult = await this.#withLock(`workflow:${instanceKey}`, async () => {
          const current = this.#requireActive(instanceKey, active.batch.runKey);
          if (prepared.kind === "ignore") {
            this.#finish(workflow, current, preparedAt, false);
            return "ignored" as const;
          }
          current.prepared = prepared;
          if (workflow.mode === "shadow") {
            this.#finish(workflow, current, preparedAt, true);
            return "shadowed" as const;
          }
          current.wake = await createPreparedWake(current.batch, prepared);
          if (attentionValueBytes(current.wake) > this.#maxPreparedWakeBytes) {
            throw new AttentionCapacityError(
              `prepared wake exceeds the maximum of ${this.#maxPreparedWakeBytes} bytes`,
            );
          }
          current.stage = "delivering";
          return "deliver" as const;
        });
        if (preparedResult !== "deliver") return preparedResult;
      }
      const current = this.#requireActive(instanceKey);
      if (current.wake === undefined) throw new Error("delivery stage has no prepared wake");
      const expectedWake = current.wake;
      const wake = deepFreeze(clone(expectedWake));
      const callbackReceipt = await this.#callbacks.deliver(wake);
      const receipt = validateCallbackOutput(
        () => validateDeliveryReceipt(callbackReceipt, expectedWake),
      );
      const completedAt = this.#now();
      await this.#withLock(`workflow:${instanceKey}`, async () => {
        const activeRun = this.#requireActive(instanceKey, wake.runKey);
        workflow.deliveryReceipts.set(wake.wakeKey, {
          receipt,
          expiresAt: addMs(completedAt, this.#dedupeMs),
        });
        this.#finish(workflow, activeRun, completedAt, true);
      });
      return "delivered";
    } catch (error) {
      const failedAt = this.#now();
      const terminal = await this.#withLock(`workflow:${instanceKey}`, async () => {
        const workflow = this.#workflows.get(instanceKey);
        if (workflow?.active === undefined) return false;
        workflow.active.processing = false;
        workflow.active.failures += 1;
        if (
          error instanceof AttentionCapacityError ||
          error instanceof IdempotencyConflictError ||
          error instanceof AttentionCallbackValidationError ||
          workflow.active.failures >= this.#maxAttempts
        ) {
          const runKey = workflow.active.batch.runKey;
          this.#finish(workflow, workflow.active, failedAt, false);
          workflow.terminalFailures.set(runKey, addMs(failedAt, this.#dedupeMs));
          return true;
        }
        workflow.active.retryAt = addMs(failedAt, this.#retryDelayMs);
        return false;
      });
      return terminal ? "terminal-failure" : "failed";
    }
  }

  async #freezeDueBatch(
    workflow: CorrelationWorkflow,
    now: string,
  ): Promise<FrozenAttentionBatch | undefined> {
    if (isFuture(workflow.cooldownUntil, now)) return undefined;
    let batch = workflow.sealed.shift();
    if (batch === undefined && workflow.open !== undefined) {
      const policy = workflow.policy.buffer;
      let closedBy: MonitorBatchClosedBy | undefined;
      if (workflow.cooldownUntil !== undefined && workflow.cooldownUntil <= now) {
        closedBy = "cooldown-expired";
      } else if (policy.mode === "immediate") {
        closedBy = "immediate";
      } else {
        const quietAt = addMs(workflow.open.updatedAt, policy.quietPeriodMs);
        const maximumAt = addMs(workflow.open.openedAt, policy.maxWaitMs);
        if (quietAt > now && maximumAt > now) return undefined;
        closedBy = maximumAt <= quietAt ? "max-wait" : "quiet-period";
      }
      batch = { ...workflow.open, closedAt: now, closedBy };
      delete workflow.open;
      delete workflow.cooldownUntil;
    }
    if (batch === undefined) return undefined;
    if (batch.closedBy === undefined) {
      throw new Error("frozen attention batch has no closure cause");
    }
    const branches = [...batch.branches].sort(compareAttentionBranches);
    const batchKey = await deriveAttentionBatchKey({
      instanceKey: workflow.instanceKey,
      orderedBranchKeys: branches.map((branch) => branch.branchKey),
    });
    const runKey = await deriveAttentionRunKey({ batchKey });
    return deepFreeze({
      instanceKey: workflow.instanceKey,
      batchKey,
      runKey,
      applicationId: workflow.applicationId,
      tenantId: workflow.tenantId,
      monitorId: workflow.monitorId,
      definitionVersion: workflow.definitionVersion,
      correlationKey: workflow.correlationKey,
      openedAt: batch.openedAt,
      frozenAt: batch.closedAt ?? now,
      closedBy: batch.closedBy,
      bytes: batch.bytes,
      branches,
    });
  }

  #finish(workflow: CorrelationWorkflow, run: ActiveRun, now: string, woke: boolean): void {
    for (const branch of run.batch.branches) {
      const receipt = workflow.branchLedger.get(branch.branchKey);
      if (receipt !== undefined) {
        receipt.terminal = true;
        receipt.expiresAt = addMs(now, this.#dedupeMs);
      }
    }
    delete workflow.active;
    if (woke && workflow.policy.cooldownAfterWakeMs !== undefined) {
      workflow.cooldownUntil = addMs(now, workflow.policy.cooldownAfterWakeMs);
      if (workflow.policy.buffer.mode !== "immediate") return;
      const buffered = [
        ...workflow.sealed.flatMap((batch) => batch.branches),
        ...(workflow.open?.branches ?? []),
      ];
      if (buffered.length > 0) {
        const openedAt = [
          ...workflow.sealed.map((batch) => batch.openedAt),
          ...(workflow.open === undefined ? [] : [workflow.open.openedAt]),
        ].sort()[0]!;
        workflow.open = {
          branches: buffered,
          bytes: buffered.reduce((sum, branch) => sum + attentionValueBytes(branch), 0),
          openedAt,
          updatedAt: now,
        };
        workflow.sealed = [];
      }
    }
  }

  #requireActive(instanceKey: AttentionInstanceKey, runKey?: RunKey): ActiveRun {
    const active = this.#workflows.get(instanceKey)?.active;
    if (active === undefined) throw new Error("attention workflow has no active run");
    if (runKey !== undefined && active.batch.runKey !== runKey) {
      throw new Error("attention workflow active run changed unexpectedly");
    }
    return active;
  }

  #purgeExpired(now: string): void {
    for (const [key, record] of this.#events) {
      if (
        record.receipt !== undefined &&
        record.dedupeExpiresAt !== undefined &&
        record.dedupeExpiresAt <= now
      ) {
        this.#events.delete(key);
      }
    }
    for (const [key, workflow] of this.#workflows) {
      for (const [branchKey, receipt] of workflow.branchLedger) {
        if (receipt.terminal && receipt.expiresAt <= now) workflow.branchLedger.delete(branchKey);
      }
      for (const [wakeKey, retained] of workflow.deliveryReceipts) {
        if (retained.expiresAt <= now) workflow.deliveryReceipts.delete(wakeKey);
      }
      for (const [runKey, expiresAt] of workflow.terminalFailures) {
        if (expiresAt <= now) workflow.terminalFailures.delete(runKey);
      }
      if (
        workflow.active === undefined &&
        workflow.open === undefined &&
        workflow.sealed.length === 0 &&
        workflow.branchLedger.size === 0 &&
        workflow.deliveryReceipts.size === 0 &&
        workflow.terminalFailures.size === 0 &&
        !isFuture(workflow.cooldownUntil, now)
      ) {
        this.#workflows.delete(key);
      }
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

class AttentionCallbackValidationError extends Error {
  constructor(cause: TypeError) {
    super(cause.message, { cause });
    this.name = "AttentionCallbackValidationError";
  }
}

function validateCallbackOutput<T>(validate: () => T): T {
  try {
    return validate();
  } catch (error) {
    if (error instanceof IdempotencyConflictError) throw error;
    if (error instanceof TypeError) throw new AttentionCallbackValidationError(error);
    throw error;
  }
}

async function createPreparedWake(
  batch: FrozenAttentionBatch,
  prepared: Extract<PreparedAttentionOutcome, { readonly kind: "wake" }>,
): Promise<PreparedAttentionWake> {
  const rootEventKeys = [...new Set(batch.branches.map((branch) => branch.eventKey))];
  const payload = {
    runKey: batch.runKey,
    batchKey: batch.batchKey,
    instanceKey: batch.instanceKey,
    applicationId: batch.applicationId,
    tenantId: batch.tenantId,
    monitorId: batch.monitorId,
    definitionVersion: batch.definitionVersion,
    correlationKey: batch.correlationKey,
    rootEventKeys,
    routeId: prepared.routeId,
    instruction: prepared.instruction,
    decision: prepared.decision,
    evidence: prepared.evidence,
  };
  const inputHash = await hashIdempotencyInput(payload);
  const wakeKey = await deriveAttentionWakeKey({
    runKey: batch.runKey,
    routeId: prepared.routeId,
  });
  return deepFreeze({ wakeKey, ...payload, inputHash });
}

function validatePreparedOutcome(outcome: PreparedAttentionOutcome): PreparedAttentionOutcome {
  const detached = clone(outcome);
  assertRecord(detached, "prepared outcome");
  if (detached.kind === "ignore") {
    assertExactKeys(detached, ["decision", "kind"], "prepared ignore outcome");
    return deepFreeze(detached);
  }
  if (detached.kind !== "wake") throw new TypeError("prepared outcome kind is invalid");
  assertExactKeys(
    detached,
    ["decision", "evidence", "instruction", "kind", "routeId"],
    "prepared wake outcome",
  );
  nonEmpty(detached.routeId, "prepared routeId");
  nonEmpty(detached.instruction, "prepared instruction");
  canonicalJson(detached.decision, "prepared decision");
  canonicalJson(detached.evidence, "prepared evidence");
  return deepFreeze(detached);
}

function validateDeliveryReceipt(
  receipt: AttentionDeliveryReceipt,
  wake: PreparedAttentionWake,
): AttentionDeliveryReceipt {
  const detached = clone(receipt);
  assertRecord(detached, "attention delivery receipt");
  assertExactKeys(
    detached,
    ["deliveredAt", "inputHash", "result", "wakeKey"],
    "attention delivery receipt",
  );
  parseIdempotencyKey("wake", detached.wakeKey);
  parseInputHash(detached.inputHash);
  if (detached.wakeKey !== wake.wakeKey) {
    throw new TypeError("delivery receipt wakeKey does not match the prepared wake");
  }
  assertIdempotencyInput({
    namespace: "attention-delivery",
    key: wake.wakeKey,
    existingInputHash: wake.inputHash,
    receivedInputHash: detached.inputHash,
  });
  canonicalTimestamp(detached.deliveredAt, "delivery receipt deliveredAt");
  canonicalJson(detached.result, "delivery receipt result");
  return deepFreeze(detached);
}

function clone<T>(value: T): T {
  return JSON.parse(canonicalJson(value)) as T;
}

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object") {
    Object.freeze(value);
    for (const nested of Object.values(value)) deepFreeze(nested);
  }
  return value;
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  return value;
}

function nonEmpty(value: string, name: string): void {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${name} must not be empty`);
  }
}

function canonicalTimestamp(value: string, name: string): void {
  const milliseconds = Date.parse(value);
  if (!Number.isFinite(milliseconds) || new Date(milliseconds).toISOString() !== value) {
    throw new TypeError(`${name} must be a canonical ISO timestamp`);
  }
}

function isFuture(value: string | undefined, now: string): boolean {
  return value !== undefined && value > now;
}

function assertRecord(value: unknown, name: string): asserts value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${name} must be an object`);
  }
}

function assertExactKeys(value: object, allowed: readonly string[], name: string): void {
  const keys = new Set(allowed);
  const unexpected = Object.keys(value).filter((key) => !keys.has(key));
  if (unexpected.length > 0) {
    throw new TypeError(`${name} contains unsupported fields: ${unexpected.sort().join(", ")}`);
  }
}
