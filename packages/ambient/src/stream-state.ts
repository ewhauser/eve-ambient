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
  hashIdempotencyInput,
  type AttentionInstanceKey,
  type BranchKey,
  type EventKey,
  type InputHash,
} from "./idempotency.js";
import {
  validateAttentionStreamAppend,
  type AttentionStreamAppend,
  type AttentionStreamAppendReceipt,
} from "./stream-protocol.js";
import { addMs } from "./time.js";
import type { MonitorBatchClosedBy } from "./types.js";

export interface AttentionBranchLedgerEntry {
  readonly branchKey: BranchKey;
  readonly inputHash: InputHash;
}

export interface AttentionRecentMessage {
  readonly eventKey: EventKey;
  readonly inputHash: InputHash;
  readonly receipt: AttentionStreamAppendReceipt;
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
  wake?: PreparedAttentionWake | undefined;
}

export type FrozenAttentionBatchDraft = Omit<
  FrozenAttentionBatch,
  "batchKey" | "runKey"
>;

export type AttentionRunClaim =
  | { readonly kind: "active"; readonly active: ActiveAttentionRun }
  | { readonly kind: "draft"; readonly draft: FrozenAttentionBatchDraft };

/** Complete backend-owned state for one serialized correlation stream. */
export interface AttentionStreamState {
  readonly instanceKey: AttentionInstanceKey;
  readonly applicationId: string;
  readonly tenantId: string;
  readonly partitionKey: string;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly correlationKey: string;
  readonly mode: FullAttentionBranch["mode"];
  readonly policy: SerializableMailboxPolicy;
  readonly policyHash: InputHash;
  /** Monotonic reducer time for the last non-duplicate append. */
  lastAcceptedAt: string;
  recentMessages: AttentionRecentMessage[];
  branchLedger: AttentionBranchLedgerEntry[];
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

function validateAttentionCallbackValue<T>(validate: () => T): T {
  try {
    return validate();
  } catch (error) {
    if (error instanceof TypeError) throw new AttentionCallbackValidationError(error);
    throw error;
  }
}

function createAttentionStream(input: {
  readonly instanceKey: AttentionInstanceKey;
  readonly branch: FullAttentionBranch;
  readonly policyHash: InputHash;
  readonly acceptedAt: string;
}): AttentionStreamState {
  return {
    instanceKey: input.instanceKey,
    applicationId: input.branch.applicationId,
    tenantId: input.branch.tenantId,
    partitionKey: input.branch.partitionKey,
    monitorId: input.branch.monitorId,
    definitionVersion: input.branch.definitionVersion,
    correlationKey: input.branch.correlationKey,
    mode: input.branch.mode,
    policy: clone(input.branch.policy),
    policyHash: input.policyHash,
    lastAcceptedAt: input.acceptedAt,
    recentMessages: [],
    branchLedger: [],
    sealed: [],
  };
}

/**
 * Applies one event append atomically to a correlation stream. The returned
 * state is detached so a backend can persist it in one transaction.
 */
export async function applyAttentionStreamAppend(
  current: AttentionStreamState | undefined,
  proposed: AttentionStreamAppend,
  input: { readonly now: string; readonly maxRecentMessages: number },
): Promise<{
  readonly state: AttentionStreamState;
  readonly receipt: AttentionStreamAppendReceipt;
}> {
  const append = await validateAttentionStreamAppend(proposed);
  const existing = current?.recentMessages.find(
    (entry) => entry.eventKey === append.eventKey,
  );
  if (existing !== undefined) {
    assertIdempotencyInput({
      namespace: "attention-stream-recent-message",
      key: append.eventKey,
      existingInputHash: existing.inputHash,
      receivedInputHash: append.inputHash,
    });
    return {
      state: clone(current!),
      receipt: { ...clone(existing.receipt), status: "duplicate" },
    };
  }

  const first = append.branches[0]!;
  const policyHash = await hashPolicy(first);
  const reducerNow = current === undefined
    ? input.now
    : maxTimestamp(current.lastAcceptedAt, input.now)!;
  const state = current === undefined
    ? createAttentionStream({
        instanceKey: append.streamKey,
        branch: first,
        policyHash,
        acceptedAt: reducerNow,
      })
    : clone(current);
  state.lastAcceptedAt = reducerNow;
  for (const branch of append.branches) {
    appendAttentionBranch(state, branch, { now: reducerNow, policyHash });
  }
  const receipt: AttentionStreamAppendReceipt = {
    streamKey: append.streamKey,
    eventKey: append.eventKey,
    inputHash: append.inputHash,
    status: "appended",
    acceptedAt: reducerNow,
  };
  state.recentMessages.push({
    eventKey: append.eventKey,
    inputHash: append.inputHash,
    receipt,
  });
  trimRing(state.recentMessages, input.maxRecentMessages);
  return { state, receipt: clone(receipt) };
}

/**
 * Checks whether an append fits in the live reducer payload budget. Workflow
 * callers can leave overflow queued in the durable hook until this becomes true.
 */
export function attentionStreamAppendFits(
  stream: AttentionStreamState | undefined,
  append: AttentionStreamAppend,
  limits: {
    readonly maxPendingBranches: number;
    readonly maxPendingBytes: number;
  },
): boolean {
  const pendingBranches = stream?.branchLedger.length ?? 0;
  const pendingBytes = stream === undefined
    ? 0
    : (stream.open?.bytes ?? 0) +
      stream.sealed.reduce((sum, batch) => sum + batch.bytes, 0) +
      (stream.active?.batch.bytes ?? 0);
  const appendBytes = append.branches.reduce(
    (sum, branch) => sum + attentionValueBytes(branch),
    0,
  );
  return (
    pendingBranches + append.branches.length <= limits.maxPendingBranches &&
    pendingBytes + appendBytes <= limits.maxPendingBytes
  );
}

/** Appends a complete branch exactly once and updates only provisional state. */
function appendAttentionBranch(
  stream: AttentionStreamState,
  branch: FullAttentionBranch,
  input: {
    readonly now: string;
    readonly policyHash: InputHash;
  },
): "appended" | "duplicate" {
  assertIdempotencyInput({
    namespace: "attention-instance-policy",
    key: stream.instanceKey,
    existingInputHash: stream.policyHash,
    receivedInputHash: input.policyHash,
  });
  assertStreamIdentity(stream, branch);
  const priorIndex = stream.branchLedger.findIndex(
    (entry) => entry.branchKey === branch.branchKey,
  );
  if (priorIndex >= 0) {
    const prior = stream.branchLedger[priorIndex]!;
    assertIdempotencyInput({
      namespace: "attention-branch-append",
      key: branch.branchKey,
      existingInputHash: prior.inputHash,
      receivedInputHash: branch.inputHash,
    });
    return "duplicate";
  }
  stream.branchLedger.push({
    branchKey: branch.branchKey,
    inputHash: branch.inputHash,
  });
  bufferBranch(stream, clone(branch), input.now);
  return "appended";
}

/** Claims one due run and freezes its complete ordered membership. */
export async function claimAttentionRun(
  stream: AttentionStreamState,
  input: { readonly now: string; readonly leaseMs: number },
): Promise<ActiveAttentionRun | undefined> {
  const claim = beginAttentionRunClaim(stream, input);
  if (claim === undefined) return undefined;
  if (claim.kind === "active") return claim.active;
  const batchKey = await deriveAttentionBatchKey({
    instanceKey: claim.draft.instanceKey,
    orderedBranchKeys: claim.draft.branches.map((branch) => branch.branchKey),
  });
  const runKey = await deriveAttentionRunKey({ batchKey });
  return activateAttentionRun(stream, claim.draft, { batchKey, runKey }, input);
}

/** Begins a claim without crossing an async boundary. */
export function beginAttentionRunClaim(
  stream: AttentionStreamState,
  input: { readonly now: string; readonly leaseMs: number },
): AttentionRunClaim | undefined {
  if (stream.active !== undefined) {
    if (
      stream.active.retryAt > input.now ||
      (stream.active.leaseUntil !== undefined && stream.active.leaseUntil > input.now)
    ) {
      return undefined;
    }
    stream.active.leaseUntil = addMs(input.now, input.leaseMs);
    return { kind: "active", active: stream.active };
  }
  const draft = freezeDueBatchDraft(stream, input.now);
  return draft === undefined ? undefined : { kind: "draft", draft };
}

/** Installs keys derived by a durable step and completes a new claim. */
export function activateAttentionRun(
  stream: AttentionStreamState,
  draft: FrozenAttentionBatchDraft,
  identity: Pick<FrozenAttentionBatch, "batchKey" | "runKey">,
  input: { readonly now: string; readonly leaseMs: number },
): ActiveAttentionRun {
  if (stream.active !== undefined) throw new Error("attention stream already has an active run");
  const batch: FrozenAttentionBatch = { ...draft, ...identity };
  stream.active = {
    batch,
    stage: "preparing",
    retryAt: input.now,
    leaseUntil: addMs(input.now, input.leaseMs),
    failures: 0,
  };
  return stream.active;
}

/** True only while this exact leased claim still owns the requested stage. */
export function isCurrentAttentionClaim(
  stream: AttentionStreamState | undefined,
  claim: ActiveAttentionRun,
  stage: ActiveAttentionRun["stage"],
): stream is AttentionStreamState {
  return (
    stream?.active !== undefined &&
    stream.active.batch.runKey === claim.batch.runKey &&
    stream.active.stage === stage &&
    stream.active.leaseUntil !== undefined &&
    stream.active.leaseUntil === claim.leaseUntil
  );
}

export async function applyPreparedAttentionOutcome(
  stream: AttentionStreamState,
  outcome: PreparedAttentionOutcome,
  input: {
    readonly now: string;
    readonly maxPreparedWakeBytes: number;
  },
): Promise<PreparedTransition> {
  const prepared = validateAttentionCallbackValue(() =>
    validatePreparedAttentionOutcome(outcome),
  );
  const active = requireActive(stream);
  const wake =
    prepared.kind === "wake" && stream.mode === "active"
      ? await createPreparedAttentionWake(active.batch, prepared)
      : undefined;
  return applyPreparedAttentionCheckpoint(stream, prepared, { ...input, wake });
}

/** Applies a validated outcome and optional wake without awaiting native promises. */
export function applyPreparedAttentionCheckpoint(
  stream: AttentionStreamState,
  prepared: PreparedAttentionOutcome,
  input: {
    readonly now: string;
    readonly maxPreparedWakeBytes: number;
    readonly wake?: PreparedAttentionWake | undefined;
  },
): PreparedTransition {
  const active = requireActive(stream);
  if (active.stage !== "preparing") throw new Error("attention run is not preparing");
  if (prepared.kind === "ignore") {
    if (input.wake !== undefined) throw new TypeError("ignored outcome must not include a wake");
    finishAttentionRun(stream, active, input.now, false);
    return "ignored";
  }
  if (stream.mode === "shadow") {
    if (input.wake !== undefined) throw new TypeError("shadow outcome must not include a wake");
    finishAttentionRun(stream, active, input.now, true);
    return "shadowed";
  }
  if (input.wake === undefined) throw new TypeError("prepared wake outcome has no checkpointed wake");
  active.wake = input.wake;
  if (attentionValueBytes(input.wake) > input.maxPreparedWakeBytes) {
    throw new AttentionCapacityError(
      `prepared wake exceeds the maximum of ${input.maxPreparedWakeBytes} bytes`,
    );
  }
  active.stage = "delivering";
  return "deliver";
}

export function applyAttentionDeliveryReceipt(
  stream: AttentionStreamState,
  receipt: AttentionDeliveryReceipt,
  input: { readonly now: string },
): AttentionDeliveryReceipt {
  const active = requireActive(stream);
  if (active.stage !== "delivering" || active.wake === undefined) {
    throw new Error("attention run is not delivering");
  }
  const validated = validateAttentionCallbackValue(() =>
    validateAttentionDeliveryReceipt(receipt, active.wake!),
  );
  finishAttentionRun(stream, active, input.now, true);
  return validated;
}

export function failAttentionRun(
  stream: AttentionStreamState,
  error: unknown,
  input: {
    readonly now: string;
    readonly retryDelayMs: number;
    readonly maxAttempts: number;
    readonly terminalError: (error: unknown) => boolean;
  },
): "failed" | "terminal-failure" {
  const active = requireActive(stream);
  delete active.leaseUntil;
  active.failures += 1;
  if (input.terminalError(error) || active.failures >= input.maxAttempts) {
    finishAttentionRun(stream, active, input.now, false);
    return "terminal-failure";
  }
  active.retryAt = addMs(input.now, input.retryDelayMs);
  return "failed";
}

export function nextAttentionDueAt(stream: AttentionStreamState): string | undefined {
  if (stream.active !== undefined) {
    return maxTimestamp(stream.active.retryAt, stream.active.leaseUntil);
  }
  const cooldown = stream.cooldownUntil;
  if (stream.sealed.length > 0) return cooldown ?? stream.sealed[0]!.closedAt;
  if (stream.open !== undefined) {
    const policy = stream.policy.buffer;
    const due =
      policy.mode === "immediate"
        ? stream.open.updatedAt
        : minTimestamp(
            addMs(stream.open.updatedAt, policy.quietPeriodMs),
            addMs(stream.open.openedAt, policy.maxWaitMs),
          );
    return cooldown === undefined ? due : maxTimestamp(due, cooldown);
  }
  return stream.cooldownUntil;
}

function bufferBranch(
  stream: AttentionStreamState,
  branch: FullAttentionBranch,
  now: string,
): void {
  const bytes = attentionValueBytes(branch);
  const policy = stream.policy.buffer;
  if (policy.mode === "immediate" && !isFuture(stream.cooldownUntil, now)) {
    stream.sealed.push({
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
    stream.open !== undefined &&
    (stream.open.branches.length + 1 > policy.maxEvents ||
      stream.open.bytes + bytes > policy.maxBytes)
  ) {
    const closedBy: MonitorBatchClosedBy =
      stream.open.branches.length + 1 > policy.maxEvents ? "max-events" : "max-bytes";
    stream.sealed.push({ ...stream.open, closedAt: now, closedBy });
    delete stream.open;
  }
  stream.open =
    stream.open === undefined
      ? { branches: [branch], bytes, openedAt: now, updatedAt: now }
      : {
          ...stream.open,
          branches: [...stream.open.branches, branch],
          bytes: stream.open.bytes + bytes,
          updatedAt: now,
        };
}

function freezeDueBatchDraft(
  stream: AttentionStreamState,
  now: string,
): FrozenAttentionBatchDraft | undefined {
  if (isFuture(stream.cooldownUntil, now)) return undefined;
  let batch = stream.sealed.shift();
  if (batch === undefined && stream.open !== undefined) {
    const policy = stream.policy.buffer;
    let closedBy: MonitorBatchClosedBy;
    if (stream.cooldownUntil !== undefined && stream.cooldownUntil <= now) {
      closedBy = "cooldown-expired";
    } else if (policy.mode === "immediate") {
      closedBy = "immediate";
    } else {
      const quietAt = addMs(stream.open.updatedAt, policy.quietPeriodMs);
      const maximumAt = addMs(stream.open.openedAt, policy.maxWaitMs);
      if (quietAt > now && maximumAt > now) return undefined;
      closedBy = maximumAt <= quietAt ? "max-wait" : "quiet-period";
    }
    batch = { ...stream.open, closedAt: now, closedBy };
    delete stream.open;
    delete stream.cooldownUntil;
  }
  if (batch === undefined || batch.closedBy === undefined) {
    if (stream.cooldownUntil !== undefined && stream.cooldownUntil <= now) {
      delete stream.cooldownUntil;
    }
    return undefined;
  }
  const branches = [...batch.branches].sort(compareAttentionBranches);
  return {
    instanceKey: stream.instanceKey,
    applicationId: stream.applicationId,
    tenantId: stream.tenantId,
    monitorId: stream.monitorId,
    definitionVersion: stream.definitionVersion,
    correlationKey: stream.correlationKey,
    openedAt: batch.openedAt,
    frozenAt: batch.closedAt ?? now,
    closedBy: batch.closedBy,
    bytes: batch.bytes,
    branches,
  };
}

function finishAttentionRun(
  stream: AttentionStreamState,
  run: ActiveAttentionRun,
  now: string,
  woke: boolean,
): void {
  const completed = new Set(run.batch.branches.map((branch) => branch.branchKey));
  stream.branchLedger = stream.branchLedger.filter(
    (entry) => !completed.has(entry.branchKey),
  );
  delete stream.active;
  if (woke && stream.policy.cooldownAfterWakeMs !== undefined) {
    stream.cooldownUntil = addMs(now, stream.policy.cooldownAfterWakeMs);
    if (stream.policy.buffer.mode !== "immediate") return;
    const buffered = [
      ...stream.sealed.flatMap((batch) => batch.branches),
      ...(stream.open?.branches ?? []),
    ];
    if (buffered.length > 0) {
      const openedAt = [
        ...stream.sealed.map((batch) => batch.openedAt),
        ...(stream.open === undefined ? [] : [stream.open.openedAt]),
      ].sort()[0]!;
      stream.open = {
        branches: buffered,
        bytes: buffered.reduce((sum, branch) => sum + attentionValueBytes(branch), 0),
        openedAt,
        updatedAt: now,
      };
      stream.sealed = [];
    }
  }
}

async function hashPolicy(branch: FullAttentionBranch): Promise<InputHash> {
  return hashIdempotencyInput({ mode: branch.mode, policy: branch.policy });
}

function trimRing<T>(values: T[], maximum: number): void {
  if (!Number.isSafeInteger(maximum) || maximum <= 0) {
    throw new TypeError("maxRecentMessages must be a positive safe integer");
  }
  if (values.length > maximum) values.splice(0, values.length - maximum);
}

function requireActive(stream: AttentionStreamState): ActiveAttentionRun {
  if (stream.active === undefined) throw new Error("attention stream has no active run");
  return stream.active;
}

function assertStreamIdentity(
  stream: AttentionStreamState,
  branch: FullAttentionBranch,
): void {
  if (
    stream.applicationId !== branch.applicationId ||
    stream.tenantId !== branch.tenantId ||
    stream.partitionKey !== branch.partitionKey ||
    stream.monitorId !== branch.monitorId ||
    stream.definitionVersion !== branch.definitionVersion ||
    stream.correlationKey !== branch.correlationKey ||
    stream.mode !== branch.mode
  ) {
    throw new TypeError("attention branch does not match its correlation stream identity");
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
