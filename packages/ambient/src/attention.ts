import { canonicalJson } from "./canonical.js";
import {
  assertIdempotencyInput,
  canonicalizeChannelDelivery,
  deriveAttentionBranchKey,
  deriveFanoutManifestHash,
  deriveOccurrenceKey,
  hashIdempotencyInput,
  parseIdempotencyKey,
  parseInputHash,
  type AcceptedChannelEvent,
  type AttentionInstanceKey,
  type BatchKey,
  type BranchKey,
  type CanonicalChannelEvent,
  type EventKey,
  type FanoutManifestHash,
  type IdempotentEnvelope,
  type InputHash,
  type OccurrenceKey,
  type RunKey,
  type WakeKey,
} from "./idempotency.js";
import type { JsonValue, MonitorBatchClosedBy, MonitorPhase } from "./types.js";

export interface ImmediateAttentionBuffer {
  readonly mode: "immediate";
}

export interface DebouncedAttentionBuffer {
  readonly mode: "debounce";
  readonly quietPeriodMs: number;
  readonly maxWaitMs: number;
  readonly maxEvents: number;
  readonly maxBytes: number;
}

/** Complete serializable lifecycle policy pinned by a correlation workflow. */
export interface SerializableMailboxPolicy {
  readonly buffer: ImmediateAttentionBuffer | DebouncedAttentionBuffer;
  readonly cooldownAfterWakeMs?: number | undefined;
}

export type AttentionMode = "active" | "shadow";

export class AttentionCapacityError extends RangeError {
  readonly retryable = false;

  constructor(message: string) {
    super(message);
    this.name = "AttentionCapacityError";
  }
}

/** Pure compiler input before lineage and hashes are assigned. */
export interface AttentionBranchPlan {
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly phase?: MonitorPhase | undefined;
  readonly correlationKey: string;
  readonly orderKey: string;
  readonly mode: AttentionMode;
  readonly policy: SerializableMailboxPolicy;
}

/** Complete branch value appended to one serialized correlation workflow. */
export interface FullAttentionBranch<
  TEvent extends CanonicalChannelEvent = CanonicalChannelEvent,
> {
  readonly applicationId: string;
  readonly tenantId: string;
  readonly eventKey: EventKey;
  readonly occurrenceKey: OccurrenceKey;
  readonly branchKey: BranchKey;
  readonly inputHash: InputHash;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly phase?: MonitorPhase | undefined;
  readonly correlationKey: string;
  readonly orderKey: string;
  readonly mode: AttentionMode;
  readonly event: TEvent;
  readonly policy: SerializableMailboxPolicy;
}

/** One complete, canonically ordered fan-out proposed to the durable engine. */
export interface AcceptedFanout<
  TEvent extends CanonicalChannelEvent = CanonicalChannelEvent,
> {
  readonly applicationId: string;
  readonly tenantId: string;
  readonly eventKey: EventKey;
  readonly occurrenceKey: OccurrenceKey;
  readonly inputHash: InputHash;
  readonly canonicalizationVersion: number;
  readonly event: TEvent;
  readonly manifestHash: FanoutManifestHash;
  readonly branches: readonly FullAttentionBranch<TEvent>[];
}

/** Payload-free result returned only after every frozen branch is accepted. */
export interface AttentionAcceptanceReceipt {
  readonly eventKey: EventKey;
  readonly occurrenceKey: OccurrenceKey;
  readonly inputHash: InputHash;
  readonly manifestHash: FanoutManifestHash;
  readonly branchKeys: readonly BranchKey[];
  readonly acceptedAt: string;
  readonly dedupeExpiresAt: string;
}

export interface FrozenAttentionBatch {
  readonly instanceKey: AttentionInstanceKey;
  readonly batchKey: BatchKey;
  readonly runKey: RunKey;
  readonly applicationId: string;
  readonly tenantId: string;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly correlationKey: string;
  readonly openedAt: string;
  readonly frozenAt: string;
  readonly closedBy: MonitorBatchClosedBy;
  readonly bytes: number;
  readonly branches: readonly FullAttentionBranch[];
}

export type PreparedAttentionOutcome =
  | {
      readonly kind: "ignore";
      readonly decision: JsonValue;
    }
  | {
      readonly kind: "wake";
      readonly decision: JsonValue;
      readonly routeId: string;
      readonly instruction: string;
      readonly evidence: JsonValue;
    };

/** Exact complete value retried at the final Ambient delivery boundary. */
export interface PreparedAttentionWake {
  readonly wakeKey: WakeKey;
  readonly runKey: RunKey;
  readonly batchKey: BatchKey;
  readonly instanceKey: AttentionInstanceKey;
  readonly applicationId: string;
  readonly tenantId: string;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly correlationKey: string;
  readonly rootEventKeys: readonly EventKey[];
  readonly routeId: string;
  readonly instruction: string;
  readonly decision: JsonValue;
  readonly evidence: JsonValue;
  readonly inputHash: InputHash;
}

export interface AttentionDeliveryReceipt {
  readonly wakeKey: WakeKey;
  readonly inputHash: InputHash;
  readonly deliveredAt: string;
  readonly result: JsonValue;
}

export interface AttentionCallbacks {
  /** Tool-less bounded computation. It may be repeated before its result is recorded. */
  prepare(batch: FrozenAttentionBatch): Promise<PreparedAttentionOutcome>;
  /** Idempotent final Ambient handoff. Matching retries return the original receipt. */
  deliver(wake: PreparedAttentionWake): Promise<AttentionDeliveryReceipt>;
}

/** The only portable durable backend command. There is deliberately no event query API. */
export interface AttentionEngine {
  accept(input: AcceptedFanout): Promise<AttentionAcceptanceReceipt>;
}

/**
 * Compiles deterministic filters/correlation output into the complete value
 * accepted by every backend. Declaration order is erased by sorting branches
 * by their derived key before the manifest is hashed.
 */
export async function compileAcceptedFanout<
  TEvent extends CanonicalChannelEvent,
>(input: {
  readonly source: IdempotentEnvelope<AcceptedChannelEvent<TEvent>, EventKey>;
  readonly branches: readonly AttentionBranchPlan[];
}): Promise<AcceptedFanout<TEvent>> {
  if (!Array.isArray(input.branches)) {
    throw new TypeError("attention branch plans must be an array");
  }
  const applicationId = input.source.payload.applicationId;
  const event = cloneCanonical(input.source.payload.event, "canonical source event");
  const tenantId = event.source.tenantId;
  const eventKey = input.source.idempotency.key;
  const sourceInputHash = input.source.idempotency.inputHash;
  const occurrenceKey = await deriveOccurrenceKey({ eventKey, inputHash: sourceInputHash });
  const branches = await Promise.all(
    input.branches.map(async (plan): Promise<FullAttentionBranch<TEvent>> => {
      assertRecord(plan, "attention branch plan");
      assertExactKeys(
        plan,
        [
          "correlationKey",
          "definitionVersion",
          "mode",
          "monitorId",
          "orderKey",
          "phase",
          "policy",
        ],
        "attention branch plan",
      );
      validateBranchPlan(plan);
      const branchKey = await deriveAttentionBranchKey({
        occurrenceKey,
        monitorId: plan.monitorId,
        definitionVersion: plan.definitionVersion,
        ...(plan.phase === undefined ? {} : { phase: plan.phase }),
        correlationKey: plan.correlationKey,
      });
      const logicalInput = branchLogicalInput({
        applicationId,
        tenantId,
        eventKey,
        occurrenceKey,
        monitorId: plan.monitorId,
        definitionVersion: plan.definitionVersion,
        ...(plan.phase === undefined ? {} : { phase: plan.phase }),
        correlationKey: plan.correlationKey,
        orderKey: plan.orderKey,
        mode: plan.mode,
        event,
        policy: plan.policy,
      });
      const inputHash = await hashIdempotencyInput(logicalInput);
      const branch = cloneCanonical(
        { ...logicalInput, branchKey, inputHash },
        `attention branch ${plan.monitorId}`,
      );
      assertBranchFitsPolicy(branch);
      return branch;
    }),
  );
  branches.sort((left, right) => compareCanonicalText(left.branchKey, right.branchKey));
  assertDistinctBranchKeys(branches);
  const manifestHash = await deriveFanoutManifestHash({
    occurrenceKey,
    orderedBranches: branches.map((branch) => ({
      branchKey: branch.branchKey,
      inputHash: branch.inputHash,
    })),
  });
  return deepFreeze({
    applicationId,
    tenantId,
    eventKey,
    occurrenceKey,
    inputHash: sourceInputHash,
    canonicalizationVersion: input.source.payload.canonicalizationVersion,
    event,
    manifestHash,
    branches,
  });
}

/** Recomputes every claimed key/hash and returns a detached immutable value. */
export async function validateAcceptedFanout(
  input: AcceptedFanout,
): Promise<AcceptedFanout> {
  const detached = cloneCanonical(input, "accepted fan-out");
  assertRecord(detached, "accepted fan-out");
  assertExactKeys(
    detached,
    [
      "applicationId",
      "branches",
      "canonicalizationVersion",
      "event",
      "eventKey",
      "inputHash",
      "manifestHash",
      "occurrenceKey",
      "tenantId",
    ],
    "accepted fan-out",
  );
  if (!Array.isArray(detached.branches)) {
    throw new TypeError("accepted fan-out branches must be an array");
  }
  const verifiedSource = await canonicalizeChannelDelivery(
    {
      version: detached.canonicalizationVersion,
      canonicalize: () => detached.event,
    },
    null,
    { applicationId: detached.applicationId },
  );
  if (detached.tenantId !== detached.event.source.tenantId) {
    throw new TypeError("fan-out tenantId does not match the canonical event");
  }
  if (detached.eventKey !== verifiedSource.idempotency.key) {
    throw new TypeError("fan-out eventKey does not match the canonical event");
  }
  assertIdempotencyInput({
    namespace: "attention-source",
    key: detached.eventKey,
    existingInputHash: verifiedSource.idempotency.inputHash,
    receivedInputHash: parseInputHash(detached.inputHash),
  });
  const occurrenceKey = await deriveOccurrenceKey({
    eventKey: detached.eventKey,
    inputHash: detached.inputHash,
  });
  if (detached.occurrenceKey !== occurrenceKey) {
    throw new TypeError("fan-out occurrenceKey does not match its source lineage");
  }
  parseIdempotencyKey("occurrence", detached.occurrenceKey);
  const canonicalEvent = canonicalJson(detached.event);
  let previousBranchKey: string | undefined;
  for (const branch of detached.branches) {
    assertRecord(branch, "full attention branch");
    assertExactKeys(
      branch,
      [
        "applicationId",
        "branchKey",
        "correlationKey",
        "definitionVersion",
        "event",
        "eventKey",
        "inputHash",
        "mode",
        "monitorId",
        "occurrenceKey",
        "orderKey",
        "phase",
        "policy",
        "tenantId",
      ],
      "full attention branch",
    );
    validateBranchPlan(branch);
    parseIdempotencyKey("branch", branch.branchKey);
    if (previousBranchKey !== undefined && previousBranchKey >= branch.branchKey) {
      throw new TypeError("fan-out branches must be distinct and ordered by branchKey");
    }
    previousBranchKey = branch.branchKey;
    if (
      branch.applicationId !== detached.applicationId ||
      branch.tenantId !== detached.tenantId ||
      branch.eventKey !== detached.eventKey ||
      branch.occurrenceKey !== detached.occurrenceKey ||
      canonicalJson(branch.event) !== canonicalEvent
    ) {
      throw new TypeError("attention branch does not match its complete source value");
    }
    const expectedKey = await deriveAttentionBranchKey({
      occurrenceKey: detached.occurrenceKey,
      monitorId: branch.monitorId,
      definitionVersion: branch.definitionVersion,
      ...(branch.phase === undefined ? {} : { phase: branch.phase }),
      correlationKey: branch.correlationKey,
    });
    if (branch.branchKey !== expectedKey) {
      throw new TypeError("branchKey does not match its attention lineage");
    }
    const expectedHash = await hashIdempotencyInput(branchLogicalInput(branch));
    assertIdempotencyInput({
      namespace: "attention-branch",
      key: branch.branchKey,
      existingInputHash: expectedHash,
      receivedInputHash: parseInputHash(branch.inputHash),
    });
  }
  const manifestHash = await deriveFanoutManifestHash({
    occurrenceKey: detached.occurrenceKey,
    orderedBranches: detached.branches.map((branch) => ({
      branchKey: branch.branchKey,
      inputHash: branch.inputHash,
    })),
  });
  if (detached.manifestHash !== manifestHash) {
    throw new TypeError("manifestHash does not match the frozen branch membership");
  }
  return deepFreeze(detached);
}

export function compareAttentionBranches(
  left: FullAttentionBranch,
  right: FullAttentionBranch,
): number {
  return (
    compareCanonicalText(left.orderKey, right.orderKey) ||
    compareCanonicalText(left.eventKey, right.eventKey) ||
    compareCanonicalText(left.branchKey, right.branchKey)
  );
}

export function attentionValueBytes(value: unknown): number {
  return new TextEncoder().encode(canonicalJson(value, "attention value")).byteLength;
}

export function validateMailboxPolicy(policy: SerializableMailboxPolicy): void {
  assertRecord(policy, "mailbox policy");
  assertExactKeys(policy, ["buffer", "cooldownAfterWakeMs"], "mailbox policy");
  assertRecord(policy.buffer, "mailbox buffer policy");
  if (policy.buffer.mode === "debounce") {
    assertExactKeys(
      policy.buffer,
      ["maxBytes", "maxEvents", "maxWaitMs", "mode", "quietPeriodMs"],
      "debounce mailbox policy",
    );
    positiveInteger(policy.buffer.quietPeriodMs, "quietPeriodMs");
    positiveInteger(policy.buffer.maxWaitMs, "maxWaitMs");
    positiveInteger(policy.buffer.maxEvents, "maxEvents");
    positiveInteger(policy.buffer.maxBytes, "maxBytes");
  } else if (policy.buffer.mode !== "immediate") {
    throw new TypeError("mailbox buffer mode must be immediate or debounce");
  } else {
    assertExactKeys(policy.buffer, ["mode"], "immediate mailbox policy");
  }
  if (policy.cooldownAfterWakeMs !== undefined) {
    positiveInteger(policy.cooldownAfterWakeMs, "cooldownAfterWakeMs");
  }
  canonicalJson(policy, "mailbox policy");
}

function validateBranchPlan(plan: AttentionBranchPlan): void {
  nonEmpty(plan.monitorId, "monitorId");
  nonEmpty(plan.definitionVersion, "definitionVersion");
  nonEmpty(plan.correlationKey, "correlationKey");
  nonEmpty(plan.orderKey, "orderKey");
  if (plan.phase !== undefined && plan.phase !== "observed" && plan.phase !== "undispatched") {
    throw new TypeError("phase must be observed or undispatched");
  }
  if (plan.mode !== "active" && plan.mode !== "shadow") {
    throw new TypeError("attention mode must be active or shadow");
  }
  validateMailboxPolicy(plan.policy);
}

function branchLogicalInput<TEvent extends CanonicalChannelEvent>(input: {
  readonly applicationId: string;
  readonly tenantId: string;
  readonly eventKey: EventKey;
  readonly occurrenceKey: OccurrenceKey;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly phase?: MonitorPhase | undefined;
  readonly correlationKey: string;
  readonly orderKey: string;
  readonly mode: AttentionMode;
  readonly event: TEvent;
  readonly policy: SerializableMailboxPolicy;
}): Omit<FullAttentionBranch<TEvent>, "branchKey" | "inputHash"> {
  return {
    applicationId: input.applicationId,
    tenantId: input.tenantId,
    eventKey: input.eventKey,
    occurrenceKey: input.occurrenceKey,
    monitorId: input.monitorId,
    definitionVersion: input.definitionVersion,
    ...(input.phase === undefined ? {} : { phase: input.phase }),
    correlationKey: input.correlationKey,
    orderKey: input.orderKey,
    mode: input.mode,
    event: input.event,
    policy: input.policy,
  };
}

function assertDistinctBranchKeys(branches: readonly FullAttentionBranch[]): void {
  for (let index = 1; index < branches.length; index += 1) {
    if (branches[index - 1]!.branchKey === branches[index]!.branchKey) {
      throw new TypeError("fan-out plans produced duplicate branch keys");
    }
  }
}

function assertBranchFitsPolicy(branch: FullAttentionBranch): void {
  const buffer = branch.policy.buffer;
  if (buffer.mode === "debounce" && attentionValueBytes(branch) > buffer.maxBytes) {
    throw new AttentionCapacityError(
      `attention branch ${branch.branchKey} exceeds its mailbox maxBytes`,
    );
  }
}

function cloneCanonical<T>(value: T, name: string): T {
  return JSON.parse(canonicalJson(value, name)) as T;
}

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object") {
    Object.freeze(value);
    for (const nested of Object.values(value)) deepFreeze(nested);
  }
  return value;
}

function nonEmpty(value: string, name: string): void {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${name} must not be empty`);
  }
}

function positiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
}

function assertRecord<T>(
  value: T,
  name: string,
): asserts value is T & Record<string, unknown> {
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

function compareCanonicalText(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}
