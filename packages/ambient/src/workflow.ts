import {
  AttentionCapacityError,
  attentionValueBytes,
  compareAttentionBranches,
  createPreparedAttentionWake,
  validateAttentionDeliveryReceipt,
  validatePreparedAttentionOutcome,
  type AttentionDeliveryReceipt,
  type FrozenAttentionBatch,
  type FullAttentionBranch,
  type PreparedAttentionOutcome,
  type PreparedAttentionWake,
  type SerializableMailboxPolicy,
} from "./attention.js";
import {
  assertIdempotencyInput,
  deriveAttentionBatchKey,
  deriveAttentionRunKey,
  type AttentionInstanceKey,
  type BranchKey,
  type InputHash,
  type RunKey,
  type WakeKey,
} from "./idempotency.js";
import { addMs } from "./time.js";
import type { MonitorBatchClosedBy } from "./types.js";

export interface AttentionBranchLedgerEntry {
  readonly branchKey: BranchKey;
  readonly inputHash: InputHash;
  expiresAt: string;
  terminal: boolean;
}

export interface BufferedAttentionBatch {
  readonly branches: FullAttentionBranch[];
  readonly bytes: number;
  readonly openedAt: string;
  readonly updatedAt: string;
  readonly closedAt?: string | undefined;
  readonly closedBy?: MonitorBatchClosedBy | undefined;
}

export interface ActiveAttentionRun {
  readonly batch: FrozenAttentionBatch;
  stage: "preparing" | "delivering";
  retryAt: string;
  leaseUntil?: string | undefined;
  failures: number;
  prepared?: PreparedAttentionOutcome | undefined;
  wake?: PreparedAttentionWake | undefined;
}

export interface RetainedAttentionDeliveryReceipt {
  readonly wakeKey: WakeKey;
  readonly receipt: AttentionDeliveryReceipt;
  readonly expiresAt: string;
}

export interface RetainedAttentionFailure {
  readonly runKey: RunKey;
  readonly expiresAt: string;
}

/** Complete private state for one serialized correlation workflow. */
export interface AttentionWorkflowState {
  readonly instanceKey: AttentionInstanceKey;
  readonly applicationId: string;
  readonly tenantId: string;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly correlationKey: string;
  readonly mode: FullAttentionBranch["mode"];
  readonly policy: SerializableMailboxPolicy;
  readonly policyHash: InputHash;
  branchLedger: AttentionBranchLedgerEntry[];
  deliveryReceipts: RetainedAttentionDeliveryReceipt[];
  terminalFailures: RetainedAttentionFailure[];
  open?: BufferedAttentionBatch | undefined;
  sealed: BufferedAttentionBatch[];
  active?: ActiveAttentionRun | undefined;
  cooldownUntil?: string | undefined;
}

export type PreparedTransition = "deliver" | "ignored" | "shadowed";

export class AttentionCallbackValidationError extends Error {
  constructor(cause: TypeError) {
    super(cause.message, { cause });
    this.name = "AttentionCallbackValidationError";
  }
}

export function validateAttentionCallbackValue<T>(validate: () => T): T {
  try {
    return validate();
  } catch (error) {
    if (error instanceof TypeError) throw new AttentionCallbackValidationError(error);
    throw error;
  }
}

export function createAttentionWorkflow(input: {
  readonly instanceKey: AttentionInstanceKey;
  readonly branch: FullAttentionBranch;
  readonly policyHash: InputHash;
}): AttentionWorkflowState {
  return {
    instanceKey: input.instanceKey,
    applicationId: input.branch.applicationId,
    tenantId: input.branch.tenantId,
    monitorId: input.branch.monitorId,
    definitionVersion: input.branch.definitionVersion,
    correlationKey: input.branch.correlationKey,
    mode: input.branch.mode,
    policy: clone(input.branch.policy),
    policyHash: input.policyHash,
    branchLedger: [],
    deliveryReceipts: [],
    terminalFailures: [],
    sealed: [],
  };
}

/** Appends a complete branch exactly once and updates only provisional state. */
export function appendAttentionBranch(
  workflow: AttentionWorkflowState,
  branch: FullAttentionBranch,
  input: {
    readonly now: string;
    readonly dedupeMs: number;
    readonly policyHash: InputHash;
  },
): "appended" | "duplicate" {
  assertIdempotencyInput({
    namespace: "attention-instance-policy",
    key: workflow.instanceKey,
    existingInputHash: workflow.policyHash,
    receivedInputHash: input.policyHash,
  });
  assertWorkflowIdentity(workflow, branch);
  const priorIndex = workflow.branchLedger.findIndex(
    (entry) => entry.branchKey === branch.branchKey,
  );
  if (priorIndex >= 0) {
    const prior = workflow.branchLedger[priorIndex]!;
    assertIdempotencyInput({
      namespace: "attention-branch-append",
      key: branch.branchKey,
      existingInputHash: prior.inputHash,
      receivedInputHash: branch.inputHash,
    });
    if (!prior.terminal || prior.expiresAt > input.now) return "duplicate";
    workflow.branchLedger.splice(priorIndex, 1);
  }
  workflow.branchLedger.push({
    branchKey: branch.branchKey,
    inputHash: branch.inputHash,
    expiresAt: addMs(input.now, input.dedupeMs),
    terminal: false,
  });
  bufferBranch(workflow, clone(branch), input.now);
  return "appended";
}

/** Claims one due run and freezes its complete ordered membership. */
export async function claimAttentionRun(
  workflow: AttentionWorkflowState,
  input: { readonly now: string; readonly leaseMs: number },
): Promise<ActiveAttentionRun | undefined> {
  if (workflow.active !== undefined) {
    if (
      workflow.active.retryAt > input.now ||
      (workflow.active.leaseUntil !== undefined && workflow.active.leaseUntil > input.now)
    ) {
      return undefined;
    }
    workflow.active.leaseUntil = addMs(input.now, input.leaseMs);
    return workflow.active;
  }
  const batch = await freezeDueBatch(workflow, input.now);
  if (batch === undefined) return undefined;
  workflow.active = {
    batch,
    stage: "preparing",
    retryAt: input.now,
    leaseUntil: addMs(input.now, input.leaseMs),
    failures: 0,
  };
  return workflow.active;
}

/** True only while this exact leased claim still owns the requested stage. */
export function isCurrentAttentionClaim(
  workflow: AttentionWorkflowState | undefined,
  claim: ActiveAttentionRun,
  stage: ActiveAttentionRun["stage"],
): workflow is AttentionWorkflowState {
  return (
    workflow?.active !== undefined &&
    workflow.active.batch.runKey === claim.batch.runKey &&
    workflow.active.stage === stage &&
    workflow.active.leaseUntil !== undefined &&
    workflow.active.leaseUntil === claim.leaseUntil
  );
}

export async function applyPreparedAttentionOutcome(
  workflow: AttentionWorkflowState,
  outcome: PreparedAttentionOutcome,
  input: { readonly now: string; readonly dedupeMs: number; readonly maxPreparedWakeBytes: number },
): Promise<PreparedTransition> {
  const active = requireActive(workflow);
  if (active.stage !== "preparing") throw new Error("attention run is not preparing");
  const prepared = validateAttentionCallbackValue(() =>
    validatePreparedAttentionOutcome(outcome),
  );
  if (prepared.kind === "ignore") {
    finishAttentionRun(workflow, active, input.now, false, input.dedupeMs);
    return "ignored";
  }
  active.prepared = prepared;
  if (workflow.mode === "shadow") {
    finishAttentionRun(workflow, active, input.now, true, input.dedupeMs);
    return "shadowed";
  }
  active.wake = await createPreparedAttentionWake(active.batch, prepared);
  if (attentionValueBytes(active.wake) > input.maxPreparedWakeBytes) {
    throw new AttentionCapacityError(
      `prepared wake exceeds the maximum of ${input.maxPreparedWakeBytes} bytes`,
    );
  }
  active.stage = "delivering";
  return "deliver";
}

export function applyAttentionDeliveryReceipt(
  workflow: AttentionWorkflowState,
  receipt: AttentionDeliveryReceipt,
  input: { readonly now: string; readonly dedupeMs: number },
): AttentionDeliveryReceipt {
  const active = requireActive(workflow);
  if (active.stage !== "delivering" || active.wake === undefined) {
    throw new Error("attention run is not delivering");
  }
  const validated = validateAttentionCallbackValue(() =>
    validateAttentionDeliveryReceipt(receipt, active.wake!),
  );
  const existingIndex = workflow.deliveryReceipts.findIndex(
    (entry) => entry.wakeKey === active.wake!.wakeKey,
  );
  const retained = {
    wakeKey: active.wake.wakeKey,
    receipt: validated,
    expiresAt: addMs(input.now, input.dedupeMs),
  };
  if (existingIndex >= 0) workflow.deliveryReceipts[existingIndex] = retained;
  else workflow.deliveryReceipts.push(retained);
  finishAttentionRun(workflow, active, input.now, true, input.dedupeMs);
  return validated;
}

export function failAttentionRun(
  workflow: AttentionWorkflowState,
  error: unknown,
  input: {
    readonly now: string;
    readonly dedupeMs: number;
    readonly retryDelayMs: number;
    readonly maxAttempts: number;
    readonly terminalError: (error: unknown) => boolean;
  },
): "failed" | "terminal-failure" {
  const active = requireActive(workflow);
  delete active.leaseUntil;
  active.failures += 1;
  if (input.terminalError(error) || active.failures >= input.maxAttempts) {
    const runKey = active.batch.runKey;
    finishAttentionRun(workflow, active, input.now, false, input.dedupeMs);
    workflow.terminalFailures.push({
      runKey,
      expiresAt: addMs(input.now, input.dedupeMs),
    });
    return "terminal-failure";
  }
  active.retryAt = addMs(input.now, input.retryDelayMs);
  return "failed";
}

export function nextAttentionDueAt(workflow: AttentionWorkflowState): string | undefined {
  if (workflow.active !== undefined) {
    return maxTimestamp(workflow.active.retryAt, workflow.active.leaseUntil);
  }
  const cooldown = workflow.cooldownUntil;
  if (workflow.sealed.length > 0) return cooldown ?? workflow.sealed[0]!.closedAt;
  if (workflow.open !== undefined) {
    const policy = workflow.policy.buffer;
    const due =
      policy.mode === "immediate"
        ? workflow.open.updatedAt
        : minTimestamp(
            addMs(workflow.open.updatedAt, policy.quietPeriodMs),
            addMs(workflow.open.openedAt, policy.maxWaitMs),
          );
    return cooldown === undefined ? due : maxTimestamp(due, cooldown);
  }
  return minTimestamp(
    ...workflow.branchLedger.filter((entry) => entry.terminal).map((entry) => entry.expiresAt),
    ...workflow.deliveryReceipts.map((entry) => entry.expiresAt),
    ...workflow.terminalFailures.map((entry) => entry.expiresAt),
    workflow.cooldownUntil,
  );
}

export function purgeAttentionWorkflow(
  workflow: AttentionWorkflowState,
  now: string,
): "active" | "empty" {
  workflow.branchLedger = workflow.branchLedger.filter(
    (entry) => !entry.terminal || entry.expiresAt > now,
  );
  workflow.deliveryReceipts = workflow.deliveryReceipts.filter(
    (entry) => entry.expiresAt > now,
  );
  workflow.terminalFailures = workflow.terminalFailures.filter(
    (entry) => entry.expiresAt > now,
  );
  if (workflow.cooldownUntil !== undefined && workflow.cooldownUntil <= now) {
    delete workflow.cooldownUntil;
  }
  return workflow.active === undefined &&
    workflow.open === undefined &&
    workflow.sealed.length === 0 &&
    workflow.branchLedger.length === 0 &&
    workflow.deliveryReceipts.length === 0 &&
    workflow.terminalFailures.length === 0
    ? "empty"
    : "active";
}

function bufferBranch(
  workflow: AttentionWorkflowState,
  branch: FullAttentionBranch,
  now: string,
): void {
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

async function freezeDueBatch(
  workflow: AttentionWorkflowState,
  now: string,
): Promise<FrozenAttentionBatch | undefined> {
  if (isFuture(workflow.cooldownUntil, now)) return undefined;
  let batch = workflow.sealed.shift();
  if (batch === undefined && workflow.open !== undefined) {
    const policy = workflow.policy.buffer;
    let closedBy: MonitorBatchClosedBy;
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
  if (batch === undefined || batch.closedBy === undefined) return undefined;
  const branches = [...batch.branches].sort(compareAttentionBranches);
  const batchKey = await deriveAttentionBatchKey({
    instanceKey: workflow.instanceKey,
    orderedBranchKeys: branches.map((branch) => branch.branchKey),
  });
  const runKey = await deriveAttentionRunKey({ batchKey });
  return {
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
  };
}

function finishAttentionRun(
  workflow: AttentionWorkflowState,
  run: ActiveAttentionRun,
  now: string,
  woke: boolean,
  dedupeMs: number,
): void {
  for (const branch of run.batch.branches) {
    const receipt = workflow.branchLedger.find(
      (entry) => entry.branchKey === branch.branchKey,
    );
    if (receipt !== undefined) {
      receipt.terminal = true;
      receipt.expiresAt = addMs(now, dedupeMs);
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

function requireActive(workflow: AttentionWorkflowState): ActiveAttentionRun {
  if (workflow.active === undefined) throw new Error("attention workflow has no active run");
  return workflow.active;
}

function assertWorkflowIdentity(
  workflow: AttentionWorkflowState,
  branch: FullAttentionBranch,
): void {
  if (
    workflow.applicationId !== branch.applicationId ||
    workflow.tenantId !== branch.tenantId ||
    workflow.monitorId !== branch.monitorId ||
    workflow.definitionVersion !== branch.definitionVersion ||
    workflow.correlationKey !== branch.correlationKey ||
    workflow.mode !== branch.mode
  ) {
    throw new TypeError("attention branch does not match its correlation workflow identity");
  }
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

function isFuture(value: string | undefined, now: string): boolean {
  return value !== undefined && value > now;
}

function minTimestamp(...values: readonly (string | undefined)[]): string | undefined {
  return values.filter((value): value is string => value !== undefined).sort()[0];
}

function maxTimestamp(...values: readonly (string | undefined)[]): string | undefined {
  return values.filter((value): value is string => value !== undefined).sort().at(-1);
}
