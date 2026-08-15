import { getHookByToken, getRun, resumeHook, start } from "workflow/api";
import {
  attentionStreamToken,
  attentionStreamWorkflow,
  type AttentionStreamBranch,
  type AttentionStreamWorkflowResult,
} from "./attention-stream-workflow.js";

export interface WorkflowAttentionStream {
  readonly address: string;
  readonly token: string;
  readonly runId: string;
  append(branch: AttentionStreamBranch): Promise<WorkflowTransportReceipt>;
  close(): Promise<WorkflowTransportReceipt>;
  result(): Promise<AttentionStreamWorkflowResult>;
}

/** What resumeHook can acknowledge: durable transport, not Ambient processing. */
export interface WorkflowTransportReceipt {
  readonly runId: string;
  readonly hookId: string;
  readonly transportAccepted: true;
}

export interface OpenAttentionStreamOptions {
  readonly registrationTimeoutMs?: number | undefined;
}

/**
 * Opens or finds a correlation stream. Concurrent starts are resolved by the
 * workflow's deterministic hook token, not by start(), whose run IDs are random.
 */
export async function openAttentionStream(
  address: string,
  options: OpenAttentionStreamOptions = {},
): Promise<WorkflowAttentionStream> {
  const token = attentionStreamToken(address);
  const existing = await findHook(token);
  if (existing !== undefined) return stream(address, existing.runId, existing.hookId);

  await start(attentionStreamWorkflow, [address]);
  const owner = await waitForHook(token, options.registrationTimeoutMs ?? 10_000);
  return stream(address, owner.runId, owner.hookId);
}

function stream(
  address: string,
  ownerRunId: string,
  hookId: string,
): WorkflowAttentionStream {
  const token = attentionStreamToken(address);
  return Object.freeze({
    address,
    token,
    runId: ownerRunId,
    async append(branch: AttentionStreamBranch) {
      const hook = await resumeHook(token, { kind: "append", branch });
      return { runId: hook.runId, hookId: hook.hookId, transportAccepted: true as const };
    },
    async close() {
      const hook = await resumeHook(token, { kind: "close" });
      return { runId: hook.runId, hookId: hook.hookId, transportAccepted: true as const };
    },
    result: () => getRun<AttentionStreamWorkflowResult>(ownerRunId).returnValue,
  });
}

async function waitForHook(token: string, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const hook = await findHook(token);
    if (hook !== undefined) return hook;
    if (Date.now() >= deadline) {
      throw new Error(`timed out waiting for Workflow hook ${token}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
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

function isNotFound(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  return error.name.includes("NotFound") || /not found/i.test(error.message);
}
