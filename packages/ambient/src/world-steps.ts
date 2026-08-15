import {
  createPreparedAttentionWake,
  validateFullAttentionBranch,
  validatePreparedAttentionOutcome,
  type AttentionMode,
  type FrozenAttentionBatch,
  type PreparedAttentionOutcome,
  type PreparedAttentionWake,
} from "./attention.js";
import {
  deriveAttentionBatchKey,
  deriveAttentionPartitionKey,
  deriveAttentionInstanceKey,
  deriveAttentionRunKey,
  hashIdempotencyInput,
  type AttentionInstanceKey,
  type BranchKey,
} from "./idempotency.js";
import {
  ADMISSION_STREAM,
  correlationToken,
  type AdmissionStreamReceipt,
  type BranchAppendCommand,
  type BranchStreamReceipt,
  type CallbackEnvelope,
  type CallbackRequest,
  type EventAdmissionInput,
  type WorldAttentionConfig,
} from "./world-protocol.js";
import { correlationWorkflow } from "./world-workflows.js";
import { getWritable } from "workflow";
import { getHookByToken, resumeHook, start } from "workflow/api";

export async function submitBranchThroughWorld(
  config: WorldAttentionConfig,
  branch: BranchAppendCommand["branch"],
  attemptId: string,
  replyToken: string,
): Promise<void> {
  "use step";

  const validated = await validateFullAttentionBranch(branch);
  const partitionCellKey = await deriveAttentionPartitionKey({
    applicationId: validated.applicationId,
    tenantId: validated.tenantId,
    channelId: validated.event.source.channelId,
    installationId: validated.event.source.installationId,
    partitionKey: validated.partitionKey,
  });
  const instanceKey = await deriveAttentionInstanceKey({
    partitionCellKey,
    monitorId: validated.monitorId,
    definitionVersion: validated.definitionVersion,
    correlationKey: validated.correlationKey,
  });
  const policyHash = await hashIdempotencyInput({ mode: validated.mode, policy: validated.policy });
  const command: BranchAppendCommand = {
    attemptId,
    appendedAt: new Date().toISOString(),
    replyToken,
    branch: validated,
    policyHash,
  };
  const token = correlationToken(config.engineId, instanceKey);
  let owner = await findHook(token);
  if (owner === undefined) {
    await start(correlationWorkflow, [config, instanceKey, command]);
    owner = await waitForHook(token, config.limits.registrationTimeoutMs);
  }
  // Also resume a newly elected owner. Some Worlds make hook registration
  // visible before scheduling the continuation from getConflict(); the
  // duplicate command is harmless because branch append is semantic-idempotent.
  await resumeHook(token, command);
}
submitBranchThroughWorld.maxRetries = 0;

export async function validateBranchAppendCommand(
  instanceKey: AttentionInstanceKey,
  command: BranchAppendCommand,
): Promise<BranchAppendCommand> {
  "use step";

  const branch = await validateFullAttentionBranch(command.branch);
  const partitionCellKey = await deriveAttentionPartitionKey({
    applicationId: branch.applicationId,
    tenantId: branch.tenantId,
    channelId: branch.event.source.channelId,
    installationId: branch.event.source.installationId,
    partitionKey: branch.partitionKey,
  });
  const derived = await deriveAttentionInstanceKey({
    partitionCellKey,
    monitorId: branch.monitorId,
    definitionVersion: branch.definitionVersion,
    correlationKey: branch.correlationKey,
  });
  if (derived !== instanceKey) {
    throw new TypeError("correlation workflow address does not match branch identity");
  }
  const expectedPolicyHash = await hashIdempotencyInput({
    mode: branch.mode,
    policy: branch.policy,
  });
  if (expectedPolicyHash !== command.policyHash) {
    throw new TypeError("correlation policy hash does not match branch policy");
  }
  return { ...command, branch };
}
validateBranchAppendCommand.maxRetries = 0;

export async function deriveAttentionBatchIdentity(
  instanceKey: AttentionInstanceKey,
  orderedBranchKeys: readonly BranchKey[],
) {
  "use step";

  const batchKey = await deriveAttentionBatchKey({ instanceKey, orderedBranchKeys });
  const runKey = await deriveAttentionRunKey({ batchKey });
  return { batchKey, runKey };
}
deriveAttentionBatchIdentity.maxRetries = 0;

export async function prepareAttentionCheckpoint(
  batch: FrozenAttentionBatch,
  mode: AttentionMode,
  value: PreparedAttentionOutcome,
): Promise<
  | {
      readonly ok: true;
      readonly prepared: PreparedAttentionOutcome;
      readonly wake?: PreparedAttentionWake | undefined;
    }
  | { readonly ok: false; readonly error: string }
> {
  "use step";

  try {
    const prepared = validatePreparedAttentionOutcome(value);
    const wake =
      prepared.kind === "wake" && mode === "active"
        ? await createPreparedAttentionWake(batch, prepared)
        : undefined;
    return { ok: true, prepared, ...(wake === undefined ? {} : { wake }) };
  } catch (error) {
    return { ok: false, error: message(error) };
  }
}
prepareAttentionCheckpoint.maxRetries = 0;

export async function invokeCallback(
  config: WorldAttentionConfig,
  request: CallbackRequest,
): Promise<CallbackEnvelope> {
  "use step";

  const secret = process.env[config.callbackSecretEnv];
  if (secret === undefined || secret.length === 0) {
    return {
      ok: false,
      completedAt: new Date().toISOString(),
      error: `callback secret environment variable ${config.callbackSecretEnv} is not set`,
      terminal: false,
    };
  }
  const path = request.kind === "prepare" ? config.preparePath : config.deliverPath;
  try {
    const response = await fetch(new URL(path, config.callbackUrl), {
      method: "POST",
      headers: {
        authorization: `Bearer ${secret}`,
        "content-type": "application/json",
      },
      body: JSON.stringify(request.value),
      signal: AbortSignal.timeout(config.limits.callbackTimeoutMs),
    });
    const body = (await response.json()) as unknown;
    if (isCallbackEnvelope(body)) return body;
    return {
      ok: false,
      completedAt: new Date().toISOString(),
      error: `callback returned ${response.status} with an invalid envelope`,
      terminal: false,
    };
  } catch (error) {
    return {
      ok: false,
      completedAt: new Date().toISOString(),
      error: message(error),
      terminal: false,
    };
  }
}
invokeCallback.maxRetries = 0;

export async function emitAdmissionReceipt(receipt: AdmissionStreamReceipt): Promise<void> {
  "use step";
  await writeStream(ADMISSION_STREAM, receipt);
}

export async function acknowledgeBranch(
  replyToken: string,
  receipt: BranchStreamReceipt,
): Promise<boolean> {
  "use step";
  try {
    await resumeHook<EventAdmissionInput>(replyToken, { kind: "branch-receipt", receipt });
    return true;
  } catch (error) {
    // A duplicate correlation command can outlive the event coordinator's
    // receipt horizon. The branch is already committed; a missing reply hook
    // must not fail the long-lived correlation workflow.
    if (isNotFound(error)) return false;
    throw error;
  }
}

async function writeStream(namespace: string, value: unknown): Promise<void> {
  const writable = getWritable({ namespace });
  const writer = writable.getWriter();
  try {
    await writer.write(value);
  } finally {
    writer.releaseLock();
  }
}

async function findHook(token: string) {
  try {
    return await getHookByToken(token);
  } catch (error) {
    if (isNotFound(error)) return undefined;
    throw error;
  }
}

async function waitForHook(token: string, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const hook = await findHook(token);
    if (hook !== undefined) return hook;
    if (Date.now() >= deadline) throw new Error(`timed out waiting for Workflow hook ${token}`);
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}

function isCallbackEnvelope(value: unknown): value is CallbackEnvelope {
  if (value === null || typeof value !== "object") return false;
  const candidate = value as Record<string, unknown>;
  if (typeof candidate.ok !== "boolean" || typeof candidate.completedAt !== "string") return false;
  return candidate.ok
    ? "value" in candidate
    : typeof candidate.error === "string" && typeof candidate.terminal === "boolean";
}

function isNotFound(error: unknown): boolean {
  return error instanceof Error && (error.name.includes("NotFound") || /not found/i.test(error.message));
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
