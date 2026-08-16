import { IdempotencyConflictError } from "../idempotency.js";
import {
  applyAttentionDeliveryReceipt,
  applyPreparedAttentionOutcome,
  applyAttentionStreamAppend,
  claimAttentionRun,
  failAttentionRun,
  nextAttentionDueAt,
  type AttentionStreamState,
} from "../stream-state.js";
import {
  correlationToken,
  type CorrelationAppendCommand,
  type CorrelationOwnerConflict,
  type CorrelationReducerConflict,
  type CorrelationWorkflowConfig,
} from "../workflow-protocol.js";
import { createHook, sleep } from "workflow";
import {
  invokeDeliver,
  invokePrepare,
  reportReducerConflict,
} from "./callback-steps.js";

/**
 * Owns one correlation for its full lifetime. Live reducer state stays bounded,
 * while the Workflow event history grows until the underlying run is retired.
 */
export async function correlationWorkflow(
  config: CorrelationWorkflowConfig,
  streamKey: string,
): Promise<CorrelationOwnerConflict> {
  "use workflow";

  using inbox = createHook<CorrelationAppendCommand>({
    token: correlationToken(config.namespace, streamKey),
  });
  const conflict = await inbox.getConflict();
  if (conflict !== null) {
    return { kind: "owner-conflict", streamKey, ownerRunId: conflict.runId };
  }

  const iterator = inbox[Symbol.asyncIterator]();
  let nextInput = iterator.next();
  let state: AttentionStreamState | undefined;
  let timer: { readonly wakeAt: string; readonly promise: Promise<TimerWake> } | undefined;

  for (;;) {
    const dueAt = state === undefined ? undefined : nextAttentionDueAt(state);
    if (dueAt !== undefined && (timer === undefined || dueAt < timer.wakeAt)) {
      timer = {
        wakeAt: dueAt,
        promise: sleep(new Date(dueAt)).then(() => ({ kind: "timer" as const, wakeAt: dueAt })),
      };
    }

    const selected = timer === undefined
      ? await nextInput.then((received) => ({ kind: "input" as const, received }))
      : await Promise.race([
          nextInput.then((received) => ({ kind: "input" as const, received })),
          timer.promise,
        ]);

    if (selected.kind === "input") {
      if (selected.received.done) throw new Error("correlation hook closed unexpectedly");
      nextInput = iterator.next();
      state = await applyCommand(selected.received.value, state, config);
      const currentDueAt = nextAttentionDueAt(state);
      if (currentDueAt !== undefined && currentDueAt <= selected.received.value.acceptedAt) {
        state = await processDue(state, selected.received.value.acceptedAt, config);
      }
      continue;
    }

    timer = undefined;
    const currentDueAt = state === undefined ? undefined : nextAttentionDueAt(state);
    if (state !== undefined && currentDueAt !== undefined && currentDueAt <= selected.wakeAt) {
      state = await processDue(state, selected.wakeAt, config);
    }
  }
}

interface TimerWake {
  readonly kind: "timer";
  readonly wakeAt: string;
}

async function applyCommand(
  command: CorrelationAppendCommand,
  state: AttentionStreamState | undefined,
  config: CorrelationWorkflowConfig,
): Promise<AttentionStreamState> {
  try {
    const reduced = await applyAttentionStreamAppend(state, command.append, {
      now: command.acceptedAt,
      maxRecentMessages: config.maxRecentMessages,
    });
    return reduced.state;
  } catch (error) {
    if (!(error instanceof IdempotencyConflictError)) throw error;
    const conflict: CorrelationReducerConflict = {
      namespace: error.namespace,
      key: error.key,
      existingInputHash: error.existingInputHash,
      receivedInputHash: error.receivedInputHash,
    };
    await reportReducerConflict(conflict);
    if (state === undefined) {
      throw new Error("the first correlation append cannot conflict without existing state");
    }
    return state;
  }
}

async function processDue(
  current: AttentionStreamState,
  dueAt: string,
  config: CorrelationWorkflowConfig,
): Promise<AttentionStreamState> {
  const state = structuredClone(current);
  await claimAttentionRun(state, { now: dueAt, leaseMs: config.claimLeaseMs });
  const active = state.active;
  if (active === undefined) return state;

  if (active.stage === "preparing") {
    const prepared = await invokePrepare(
      config.callbackUrl,
      config.preparePath,
      config.callbackSecretEnv,
      active.batch,
    );
    if (!prepared.ok) {
      failAttentionRun(state, new Error(prepared.error), {
        now: prepared.completedAt,
        retryDelayMs: config.retryDelayMs,
        maxAttempts: config.maxAttempts,
        terminalError: () => prepared.terminal,
      });
      return state;
    }
    await applyPreparedAttentionOutcome(state, prepared.value, {
      now: prepared.completedAt,
      maxPreparedWakeBytes: config.maxPreparedWakeBytes,
    });
  }

  const delivering = state.active;
  if (delivering?.stage !== "delivering" || delivering.wake === undefined) return state;
  const delivered = await invokeDeliver(
    config.callbackUrl,
    config.deliverPath,
    config.callbackSecretEnv,
    delivering.wake,
  );
  if (!delivered.ok) {
    failAttentionRun(state, new Error(delivered.error), {
      now: delivered.completedAt,
      retryDelayMs: config.retryDelayMs,
      maxAttempts: config.maxAttempts,
      terminalError: () => delivered.terminal,
    });
    return state;
  }
  applyAttentionDeliveryReceipt(state, delivered.value, { now: delivered.completedAt });
  return state;
}
