import { AttentionCapacityError, attentionValueBytes } from "../attention.js";
import { IdempotencyConflictError } from "../idempotency.js";
import {
  AttentionCallbackValidationError,
  applyAttentionDeliveryReceipt,
  applyPreparedAttentionOutcome,
  applyAttentionStreamAppend,
  attentionStreamAppendFits,
  claimAttentionRun,
  failAttentionRun,
  nextAttentionDueAt,
  type AttentionStreamState,
} from "../stream-state.js";
import {
  correlationAppendManyBytes,
  correlationToken,
  type CorrelationAppendInput,
  type CorrelationAppendManyCommand,
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
  initialCommand?: CorrelationAppendManyCommand,
): Promise<CorrelationOwnerConflict> {
  "use workflow";

  const token = await correlationToken(config, streamKey);
  using inbox = createHook<CorrelationAppendManyCommand>({ token });
  const conflict = await inbox.getConflict();
  if (conflict !== null) {
    return { kind: "owner-conflict", streamKey, ownerRunId: conflict.runId };
  }

  const iterator = inbox[Symbol.asyncIterator]();
  let nextInput: Promise<IteratorResult<CorrelationAppendManyCommand>> | undefined =
    initialCommand === undefined ? iterator.next() : undefined;
  let pendingCommands = initialCommand === undefined
    ? undefined
    : validateAppendMany(initialCommand, config, streamKey).commands;
  let state: AttentionStreamState | undefined;
  let timer: { readonly wakeAt: string; readonly promise: Promise<TimerWake> } | undefined;

  for (;;) {
    if (
      pendingCommands !== undefined &&
      attentionStreamAppendFits(state, pendingCommands[0]!.append, config)
    ) {
      state = await applyCommand(
        pendingCommands[0]!,
        state,
        config,
        streamKey,
      );
      const currentDueAt = nextAttentionDueAt(state);
      if (currentDueAt !== undefined && currentDueAt <= state.lastAcceptedAt) {
        state = await processDue(state, state.lastAcceptedAt, config);
      }
      pendingCommands = pendingCommands.slice(1);
      if (pendingCommands.length === 0) {
        pendingCommands = undefined;
        nextInput = iterator.next();
      }
      continue;
    }

    const dueAt = state === undefined ? undefined : nextAttentionDueAt(state);
    if (dueAt !== undefined && (timer === undefined || dueAt < timer.wakeAt)) {
      timer = {
        wakeAt: dueAt,
        promise: sleep(new Date(dueAt)).then(() => ({ kind: "timer" as const, wakeAt: dueAt })),
      };
    }

    let selected: InputWake | TimerWake;
    if (pendingCommands !== undefined) {
      if (timer === undefined) {
        throw new Error("a full correlation reducer has no due work to release capacity");
      }
      selected = await timer.promise;
    } else {
      if (nextInput === undefined) throw new Error("correlation input iterator is not pending");
      const input = nextInput.then((received) => ({ kind: "input" as const, received }));
      selected = timer === undefined ? await input : await Promise.race([input, timer.promise]);
    }

    if (selected.kind === "input") {
      if (selected.received.done) throw new Error("correlation hook closed unexpectedly");
      pendingCommands = validateAppendMany(
        selected.received.value,
        config,
        streamKey,
      ).commands;
      nextInput = undefined;
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

interface InputWake {
  readonly kind: "input";
  readonly received: IteratorResult<CorrelationAppendManyCommand>;
}

async function applyCommand(
  command: CorrelationAppendInput,
  state: AttentionStreamState | undefined,
  config: CorrelationWorkflowConfig,
  streamKey: string,
): Promise<AttentionStreamState> {
  try {
    if (command.append.streamKey !== streamKey) {
      throw new TypeError("correlation append does not match its Workflow owner");
    }
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

function validateAppendMany(
  command: CorrelationAppendManyCommand,
  config: CorrelationWorkflowConfig,
  streamKey: string,
): CorrelationAppendManyCommand {
  if (command.kind !== "append-many" || !Array.isArray(command.commands)) {
    throw new TypeError("correlation hook command must be append-many");
  }
  if (command.commands.length === 0) {
    throw new TypeError("correlation append-many command must not be empty");
  }
  if (command.commands.length > config.maxBatchCommands) {
    throw new AttentionCapacityError(
      `correlation append-many command exceeds ${config.maxBatchCommands} commands`,
    );
  }
  if (correlationAppendManyBytes(command) > config.maxBatchBytes) {
    throw new AttentionCapacityError(
      `correlation append-many command exceeds ${config.maxBatchBytes} serialized bytes`,
    );
  }

  let branchCount = 0;
  let branchBytes = 0;
  for (const input of command.commands as readonly CorrelationAppendInput[]) {
    if (input.append.streamKey !== streamKey) {
      throw new TypeError("correlation append does not match its Workflow owner");
    }
    branchCount += input.append.branches.length;
    branchBytes += input.append.branches.reduce(
      (total, branch) => total + attentionValueBytes(branch),
      0,
    );
  }
  if (
    branchCount > config.maxPendingBranches ||
    branchBytes > config.maxPendingBytes
  ) {
    throw new AttentionCapacityError(
      "correlation append-many command exceeds reducer capacity",
    );
  }
  return command;
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
    try {
      await applyPreparedAttentionOutcome(state, prepared.value, {
        now: prepared.completedAt,
        maxPreparedWakeBytes: config.maxPreparedWakeBytes,
      });
    } catch (error) {
      failAttentionRun(state, error, {
        now: prepared.completedAt,
        retryDelayMs: config.retryDelayMs,
        maxAttempts: config.maxAttempts,
        terminalError: isTerminalTransitionError,
      });
      return state;
    }
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
  try {
    applyAttentionDeliveryReceipt(state, delivered.value, {
      now: delivered.completedAt,
    });
  } catch (error) {
    failAttentionRun(state, error, {
      now: delivered.completedAt,
      retryDelayMs: config.retryDelayMs,
      maxAttempts: config.maxAttempts,
      terminalError: isTerminalTransitionError,
    });
  }
  return state;
}

function isTerminalTransitionError(error: unknown): boolean {
  return (
    error instanceof AttentionCapacityError ||
    error instanceof IdempotencyConflictError ||
    error instanceof AttentionCallbackValidationError
  );
}
