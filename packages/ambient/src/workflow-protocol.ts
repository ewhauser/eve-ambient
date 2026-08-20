import { attentionValueBytes } from "./attention.js";
import { hashIdempotencyInput, type InputHash } from "./idempotency.js";
import type { AttentionStreamAppend } from "./stream-protocol.js";

/** Serializable configuration retained by one correlation Workflow run. */
export interface CorrelationWorkflowConfig {
  readonly namespace: string;
  readonly callbackUrl: string;
  /** Bearer-secret environment name, or null when transport authentication is authoritative. */
  readonly callbackSecretEnv: string | null;
  readonly preparePath: string;
  readonly deliverPath: string;
  readonly maxRecentMessages: number;
  readonly claimLeaseMs: number;
  readonly retryDelayMs: number;
  readonly maxAttempts: number;
  readonly maxPreparedWakeBytes: number;
  readonly maxPendingBranches: number;
  readonly maxPendingBytes: number;
  readonly maxBatchCommands: number;
  readonly maxBatchBytes: number;
}

/** One independently accepted append retained inside a shared hook command. */
export interface CorrelationAppendInput {
  readonly acceptedAt: string;
  readonly append: AttentionStreamAppend;
}

/** The only command shape accepted by a correlation Workflow hook. */
export interface CorrelationAppendManyCommand {
  readonly kind: "append-many";
  readonly commands: readonly CorrelationAppendInput[];
}

/** Returns the exact canonical serialized size of one retained append. */
export function correlationAppendInputBytes(input: CorrelationAppendInput): number {
  return attentionValueBytes(input);
}

/** Returns the exact canonical serialized size used for batching limits. */
export function correlationAppendManyBytes(
  command: CorrelationAppendManyCommand,
): number {
  return attentionValueBytes(command);
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

/** Hashes the immutable configuration portion of a correlation address. */
export function correlationConfigHash(
  config: CorrelationWorkflowConfig,
): Promise<InputHash> {
  return hashIdempotencyInput({
    protocolVersion: 1,
    ...config,
  });
}

/** Builds a correlation hook token from a previously computed configuration hash. */
export function correlationTokenFromConfigHash(
  config: Pick<CorrelationWorkflowConfig, "namespace">,
  configHash: InputHash,
  streamKey: string,
): string {
  return `eve-ambient:correlation:${config.namespace}:${configHash}:${streamKey}`;
}

export async function correlationToken(
  config: CorrelationWorkflowConfig,
  streamKey: string,
): Promise<string> {
  return correlationTokenFromConfigHash(
    config,
    await correlationConfigHash(config),
    streamKey,
  );
}
