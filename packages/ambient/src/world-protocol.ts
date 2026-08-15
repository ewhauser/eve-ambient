import type {
  AcceptedFanout,
  AttentionAcceptanceReceipt,
  AttentionDeliveryReceipt,
  FullAttentionBranch,
  FrozenAttentionBatch,
  PreparedAttentionOutcome,
  PreparedAttentionWake,
} from "./attention.js";
import type { BranchKey, InputHash } from "./idempotency.js";

export const ADMISSION_STREAM = "ambient-admission-receipts";

export function eventAdmissionToken(engineId: string, eventKey: string): string {
  return `eve-ambient:event:${engineId}:${eventKey}`;
}

export function correlationToken(engineId: string, instanceKey: string): string {
  return `eve-ambient:correlation:${engineId}:${instanceKey}`;
}

export interface WorldAttentionLimits {
  readonly dedupeMs: number;
  readonly retryDelayMs: number;
  readonly claimLeaseMs: number;
  readonly maxAttempts: number;
  readonly maxBranches: number;
  readonly maxFanoutBytes: number;
  readonly maxPreparedWakeBytes: number;
  readonly callbackTimeoutMs: number;
  readonly maxCallbackRequestBytes: number;
  readonly registrationTimeoutMs: number;
  readonly receiptTimeoutMs: number;
}

/** Serializable configuration persisted with each Workflow run. */
export interface WorldAttentionConfig {
  readonly engineId: string;
  readonly callbackUrl: string;
  /** Environment variable read by callback steps. Its value is never serialized. */
  readonly callbackSecretEnv: string;
  readonly preparePath: string;
  readonly deliverPath: string;
  readonly limits: WorldAttentionLimits;
}

export interface EventAdmissionCommand {
  readonly attemptId: string;
  readonly acceptedAt: string;
  readonly fanout: AcceptedFanout;
}

export interface BranchAppendCommand {
  readonly attemptId: string;
  readonly appendedAt: string;
  readonly replyToken: string;
  readonly branch: FullAttentionBranch;
  readonly policyHash: InputHash;
}

export type EventAdmissionInput =
  | { readonly kind: "admit"; readonly command: EventAdmissionCommand }
  | { readonly kind: "branch-receipt"; readonly receipt: BranchStreamReceipt };

export interface SerializedConflict {
  readonly namespace: string;
  readonly key: string;
  readonly existingInputHash: string;
  readonly receivedInputHash: string;
}

export type WorldAttentionFailure =
  | {
      readonly kind: "capacity";
      readonly message: string;
    }
  | {
      readonly kind: "conflict";
      readonly message: string;
      readonly conflict: SerializedConflict;
    }
  | {
      readonly kind: "runtime";
      readonly message: string;
      readonly retryable: true;
    };

export type AdmissionStreamReceipt =
  | {
      readonly kind: "accepted";
      readonly attemptId: string;
      readonly receipt: AttentionAcceptanceReceipt;
    }
  | {
      readonly kind: "rejected";
      readonly attemptId: string;
      readonly failure: WorldAttentionFailure;
    };

export type BranchStreamReceipt =
  | {
      readonly kind: "accepted";
      readonly attemptId: string;
      readonly branchKey: BranchKey;
      readonly status: "appended" | "duplicate";
      readonly committedAt: string;
    }
  | {
      readonly kind: "rejected";
      readonly attemptId: string;
      readonly branchKey: BranchKey;
      readonly failure: WorldAttentionFailure;
    };

export type CallbackRequest =
  | { readonly kind: "prepare"; readonly value: FrozenAttentionBatch }
  | { readonly kind: "deliver"; readonly value: PreparedAttentionWake };

export type CallbackValue = PreparedAttentionOutcome | AttentionDeliveryReceipt;

export type CallbackEnvelope =
  | {
      readonly ok: true;
      readonly completedAt: string;
      readonly value: CallbackValue;
    }
  | {
      readonly ok: false;
      readonly completedAt: string;
      readonly error: string;
      readonly terminal: boolean;
    };
