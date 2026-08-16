import type { AttentionStreamAppend } from "./stream-protocol.js";

/** Serializable configuration retained by one correlation Workflow run. */
export interface CorrelationWorkflowConfig {
  readonly namespace: string;
  readonly callbackUrl: string;
  readonly callbackSecretEnv: string;
  readonly preparePath: string;
  readonly deliverPath: string;
  readonly maxRecentMessages: number;
  readonly claimLeaseMs: number;
  readonly retryDelayMs: number;
  readonly maxAttempts: number;
  readonly maxPreparedWakeBytes: number;
}

/** One transport-accepted message sent to a correlation Workflow hook. */
export interface CorrelationAppendCommand {
  readonly kind: "append";
  readonly acceptedAt: string;
  readonly append: AttentionStreamAppend;
}

/** An append reused an idempotency key with a different canonical value. */
export interface CorrelationReducerConflict {
  readonly namespace: string;
  readonly key: string;
  readonly existingInputHash: string;
  readonly receivedInputHash: string;
}

/** A cold-start race lost to the run that already owns the correlation hook. */
export interface CorrelationOwnerConflict {
  readonly kind: "owner-conflict";
  readonly streamKey: string;
  readonly ownerRunId: string;
}

export function correlationToken(namespace: string, streamKey: string): string {
  return `eve-ambient:correlation:${namespace}:${streamKey}`;
}
