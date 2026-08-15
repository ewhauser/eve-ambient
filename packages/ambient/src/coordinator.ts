import {
  AttentionCapacityError,
  attentionValueBytes,
  type AcceptedFanout,
  type AttentionAcceptanceReceipt,
  type FullAttentionBranch,
} from "./attention.js";
import {
  assertIdempotencyInput,
  type BranchKey,
  type EventKey,
  type FanoutManifestHash,
  type InputHash,
  type OccurrenceKey,
} from "./idempotency.js";
import { addMs } from "./time.js";

export interface EventCoordinatorBranchInput {
  readonly branchKey: BranchKey;
  readonly inputHash: InputHash;
}

export type PendingEventFanout = Omit<AcceptedFanout, "branches"> & {
  /** Complete branch values whose custody has not moved to a workflow yet. */
  readonly branches: readonly FullAttentionBranch[];
};

/** Private durable admission state; `fanout` is deleted after all handoffs. */
export interface EventCoordinatorState {
  readonly eventKey: EventKey;
  readonly inputHash: InputHash;
  readonly occurrenceKey: OccurrenceKey;
  readonly manifestHash: FanoutManifestHash;
  readonly branchInputs: readonly EventCoordinatorBranchInput[];
  readonly acceptedAt: string;
  acceptedBranchKeys: BranchKey[];
  dedupeExpiresAt?: string | undefined;
  pendingFanout?: PendingEventFanout | undefined;
  receipt?: AttentionAcceptanceReceipt | undefined;
}

export function createEventCoordinator(
  fanout: AcceptedFanout,
  input: { readonly now: string; readonly maxBranches: number; readonly maxFanoutBytes: number },
): EventCoordinatorState {
  if (fanout.branches.length > input.maxBranches) {
    throw new AttentionCapacityError(
      `accepted fan-out exceeds the maximum of ${input.maxBranches} branches`,
    );
  }
  if (attentionValueBytes(fanout) > input.maxFanoutBytes) {
    throw new AttentionCapacityError(
      `accepted fan-out exceeds the maximum of ${input.maxFanoutBytes} bytes`,
    );
  }
  return {
    eventKey: fanout.eventKey,
    inputHash: fanout.inputHash,
    occurrenceKey: fanout.occurrenceKey,
    manifestHash: fanout.manifestHash,
    branchInputs: fanout.branches.map((branch) => ({
      branchKey: branch.branchKey,
      inputHash: branch.inputHash,
    })),
    acceptedAt: input.now,
    acceptedBranchKeys: [],
    pendingFanout: clone(fanout),
  };
}

export function validateEventCoordinatorRetry(
  coordinator: EventCoordinatorState,
  proposed: AcceptedFanout,
): void {
  assertIdempotencyInput({
    namespace: "attention-event-admission",
    key: proposed.eventKey,
    existingInputHash: coordinator.inputHash,
    receivedInputHash: proposed.inputHash,
  });
  for (const branch of proposed.branches) {
    const original = coordinator.branchInputs.find(
      (candidate) => candidate.branchKey === branch.branchKey,
    );
    if (original === undefined) continue;
    assertIdempotencyInput({
      namespace: "attention-fanout-branch",
      key: branch.branchKey,
      existingInputHash: original.inputHash,
      receivedInputHash: branch.inputHash,
    });
  }
}

export function pendingCoordinatorBranches(
  coordinator: EventCoordinatorState,
): readonly FullAttentionBranch[] {
  if (coordinator.receipt !== undefined) return [];
  if (coordinator.pendingFanout === undefined) {
    throw new Error("pending event coordinator lost its frozen fan-out");
  }
  return coordinator.pendingFanout.branches;
}

export function markCoordinatorBranchAccepted(
  coordinator: EventCoordinatorState,
  branchKey: BranchKey,
): void {
  if (!coordinator.branchInputs.some((branch) => branch.branchKey === branchKey)) {
    throw new TypeError("accepted branch does not belong to the frozen fan-out");
  }
  if (!coordinator.acceptedBranchKeys.includes(branchKey)) {
    coordinator.acceptedBranchKeys.push(branchKey);
  }
  if (coordinator.pendingFanout !== undefined) {
    coordinator.pendingFanout = {
      ...coordinator.pendingFanout,
      branches: coordinator.pendingFanout.branches.filter(
        (branch) => branch.branchKey !== branchKey,
      ),
    };
  }
}

export function completeEventCoordinator(
  coordinator: EventCoordinatorState,
  input: { readonly now: string; readonly dedupeMs: number },
): AttentionAcceptanceReceipt {
  if (coordinator.acceptedBranchKeys.length !== coordinator.branchInputs.length) {
    throw new Error("cannot complete an event coordinator with missing branch handoffs");
  }
  if (coordinator.pendingFanout?.branches.length !== 0) {
    throw new Error("cannot complete an event coordinator while it retains branch payloads");
  }
  coordinator.dedupeExpiresAt = addMs(input.now, input.dedupeMs);
  coordinator.receipt = {
    eventKey: coordinator.eventKey,
    occurrenceKey: coordinator.occurrenceKey,
    inputHash: coordinator.inputHash,
    manifestHash: coordinator.manifestHash,
    branchKeys: coordinator.branchInputs.map((branch) => branch.branchKey),
    acceptedAt: coordinator.acceptedAt,
    dedupeExpiresAt: coordinator.dedupeExpiresAt,
  };
  delete coordinator.pendingFanout;
  return clone(coordinator.receipt);
}

export function eventCoordinatorExpired(
  coordinator: EventCoordinatorState,
  now: string,
): boolean {
  return (
    coordinator.receipt !== undefined &&
    coordinator.dedupeExpiresAt !== undefined &&
    coordinator.dedupeExpiresAt <= now
  );
}

function clone<T>(value: T): T {
  return structuredClone(value);
}
