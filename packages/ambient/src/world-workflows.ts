import {
  AttentionCapacityError,
  type AttentionDeliveryReceipt,
  type PreparedAttentionOutcome,
} from "./attention.js";
import {
  completeEventCoordinator,
  createEventCoordinator,
  markCoordinatorBranchAccepted,
  pendingCoordinatorBranches,
  validateEventCoordinatorRetry,
  type EventCoordinatorState,
} from "./coordinator.js";
import {
  IdempotencyConflictError,
  type AttentionInstanceKey,
} from "./idempotency.js";
import {
  AttentionCallbackValidationError,
  activateAttentionRun,
  appendAttentionBranch,
  applyPreparedAttentionCheckpoint,
  applyAttentionDeliveryReceipt,
  beginAttentionRunClaim,
  createAttentionWorkflow,
  failAttentionRun,
  nextAttentionDueAt,
  purgeAttentionWorkflow,
  type AttentionWorkflowState,
} from "./workflow.js";
import type {
  BranchAppendCommand,
  BranchStreamReceipt,
  EventAdmissionCommand,
  EventAdmissionInput,
  SerializedConflict,
  WorldAttentionConfig,
  WorldAttentionFailure,
} from "./world-protocol.js";
import { correlationToken, eventAdmissionToken } from "./world-protocol.js";
import {
  acknowledgeBranch,
  deriveAttentionBatchIdentity,
  emitAdmissionReceipt,
  invokeCallback,
  prepareAttentionCheckpoint,
  submitBranchThroughWorld,
  validateBranchAppendCommand,
} from "./world-steps.js";
import { createHook, sleep } from "workflow";

/** One durable keyed coordinator for a canonical source event. */
export async function eventAdmissionWorkflow(
  config: WorldAttentionConfig,
  initial: EventAdmissionCommand,
): Promise<{ readonly kind: "expired" } | { readonly kind: "owner-conflict"; readonly ownerRunId: string }> {
  "use workflow";

  const token = eventAdmissionToken(config.engineId, initial.fanout.eventKey);
  using commands = createHook<EventAdmissionInput>({
    token,
    metadata: { kind: "eve-ambient-event", engineId: config.engineId },
  });
  const conflict = await commands.getConflict();
  if (conflict !== null) return { kind: "owner-conflict", ownerRunId: conflict.runId };

  let coordinator: EventCoordinatorState | undefined;
  const iterator = commands[Symbol.asyncIterator]();
  let nextInput: Promise<IteratorResult<EventAdmissionInput>> = iterator.next();
  const queuedCommands: EventAdmissionCommand[] = [];
  let command: EventAdmissionCommand | undefined = initial;
  let expiry: Promise<void> | undefined;

  for (;;) {
    if (command !== undefined) {
      try {
        if (coordinator === undefined) {
          coordinator = createEventCoordinator(command.fanout, {
            now: command.acceptedAt,
            maxBranches: config.limits.maxBranches,
            maxFanoutBytes: config.limits.maxFanoutBytes,
          });
        } else {
          validateEventCoordinatorRetry(coordinator, command.fanout);
        }
        if (coordinator.receipt !== undefined) {
          await emitAdmissionReceipt({
            kind: "accepted",
            attemptId: command.attemptId,
            receipt: coordinator.receipt,
          });
        } else {
          let completedAt = command.acceptedAt;
          let rejected = false;
          for (const branch of pendingCoordinatorBranches(coordinator)) {
            let result: BranchStreamReceipt;
            try {
              await submitBranchThroughWorld(config, branch, command.attemptId, token);
              for (;;) {
                const received = await nextInput;
                if (received.done) throw new Error("event admission hook closed during branch handoff");
                nextInput = iterator.next();
                if (received.value.kind === "admit") {
                  queuedCommands.push(received.value.command);
                  continue;
                }
                const candidate = received.value.receipt;
                if (
                  candidate.attemptId === command.attemptId &&
                  candidate.branchKey === branch.branchKey
                ) {
                  result = candidate;
                  break;
                }
              }
            } catch (error) {
              result = {
                kind: "rejected",
                attemptId: command.attemptId,
                branchKey: branch.branchKey,
                failure: serializeFailure(error),
              };
            }
            if (result.kind === "rejected") {
              await emitAdmissionReceipt({
                kind: "rejected",
                attemptId: command.attemptId,
                failure: result.failure,
              });
              rejected = true;
              break;
            }
            markCoordinatorBranchAccepted(coordinator, branch.branchKey);
            completedAt = result.committedAt;
          }
          if (!rejected) {
            const receipt = completeEventCoordinator(coordinator, {
              now: completedAt,
              dedupeMs: config.limits.dedupeMs,
            });
            await emitAdmissionReceipt({
              kind: "accepted",
              attemptId: command.attemptId,
              receipt,
            });
          }
        }
      } catch (error) {
        await emitAdmissionReceipt({
          kind: "rejected",
          attemptId: command.attemptId,
          failure: serializeFailure(error),
        });
      }
      command = undefined;
      if (coordinator?.receipt !== undefined && coordinator.dedupeExpiresAt !== undefined) {
        expiry ??= sleep(new Date(coordinator.dedupeExpiresAt));
      }
    }

    command = queuedCommands.shift();
    if (command !== undefined) continue;

    // Duplicate branch acknowledgements can arrive after their event command
    // has completed. They do not create a new command, but they must not leave
    // a cancelled expiry wait unarmed.
    if (coordinator?.receipt !== undefined && coordinator.dedupeExpiresAt !== undefined) {
      expiry ??= sleep(new Date(coordinator.dedupeExpiresAt));
    }

    if (expiry === undefined) {
      const received = await nextInput;
      if (received.done) return { kind: "expired" };
      nextInput = iterator.next();
      if (received.value.kind === "admit") command = received.value.command;
      continue;
    }

    const selected = await Promise.race([
      nextInput.then((received) => ({ kind: "input" as const, received })),
      expiry.then(() => ({ kind: "expired" as const })),
    ]);
    if (selected.kind === "expired") return { kind: "expired" };
    // Workflow cancels the losing side of Promise.race. An input therefore
    // invalidates this sleep promise even though the absolute expiry is
    // unchanged; recreate it before waiting again.
    expiry = undefined;
    if (selected.received.done) return { kind: "expired" };
    nextInput = iterator.next();
    if (selected.received.value.kind === "admit") {
      command = selected.received.value.command;
    }
  }
}

/** One durable serialized stream for a correlation identity. */
export async function correlationWorkflow(
  config: WorldAttentionConfig,
  instanceKey: AttentionInstanceKey,
  initial: BranchAppendCommand,
): Promise<{ readonly kind: "empty" } | { readonly kind: "owner-conflict"; readonly ownerRunId: string }> {
  "use workflow";

  using commands = createHook<BranchAppendCommand>({
    token: correlationToken(config.engineId, instanceKey),
    metadata: { kind: "eve-ambient-correlation", engineId: config.engineId },
  });
  const conflict = await commands.getConflict();
  if (conflict !== null) return { kind: "owner-conflict", ownerRunId: conflict.runId };

  let state: AttentionWorkflowState | undefined;
  const iterator = commands[Symbol.asyncIterator]();
  let nextCommand: Promise<IteratorResult<BranchAppendCommand>> = iterator.next();
  let command: BranchAppendCommand | undefined = initial;
  let logicalNow = initial.appendedAt;
  let timer: { readonly dueAt: string; readonly promise: Promise<void> } | undefined;

  for (;;) {
    if (command !== undefined) {
      logicalNow = command.appendedAt > logicalNow ? command.appendedAt : logicalNow;
      state = await handleBranchCommand(config, instanceKey, state, command);
      command = undefined;
    }
    if (state === undefined) throw new Error("correlation workflow has no initial state");

    logicalNow = await processDueRuns(config, state, logicalNow);
    if (purgeAttentionWorkflow(state, logicalNow) === "empty") return { kind: "empty" };

    const dueAt = nextAttentionDueAt(state);
    if (dueAt === undefined) {
      const received = await nextCommand;
      if (received.done) return { kind: "empty" };
      command = received.value;
      nextCommand = iterator.next();
      continue;
    }

    timer ??= { dueAt, promise: sleep(new Date(dueAt)) };
    const selected = await Promise.race([
      nextCommand.then((received) => ({ kind: "command" as const, received })),
      timer.promise.then(() => ({ kind: "due" as const })),
    ]);
    if (selected.kind === "due") {
      logicalNow = timer.dueAt > logicalNow ? timer.dueAt : logicalNow;
      timer = undefined;
      // The hook read lost the durable race and was cancelled with the timer
      // winner. Start a fresh read so commands arriving after this due time
      // are not stranded in the hook.
      nextCommand = iterator.next();
      continue;
    }
    // Workflow cancels the losing branch of Promise.race. Recreate the sleep
    // after a command wins; retaining that promise would retain a cancelled
    // durable wait that can never make progress.
    timer = undefined;
    if (selected.received.done) return { kind: "empty" };
    command = selected.received.value;
    nextCommand = iterator.next();
  }
}

async function handleBranchCommand(
  config: WorldAttentionConfig,
  instanceKey: AttentionInstanceKey,
  current: AttentionWorkflowState | undefined,
  command: BranchAppendCommand,
): Promise<AttentionWorkflowState> {
  let state = current;
  try {
    const validated = await validateBranchAppendCommand(instanceKey, command);
    const branch = validated.branch;
    state ??= createAttentionWorkflow({ instanceKey, branch, policyHash: validated.policyHash });
    const status = appendAttentionBranch(state, branch, {
      now: validated.appendedAt,
      dedupeMs: config.limits.dedupeMs,
      policyHash: validated.policyHash,
    });
    await acknowledgeBranch(command.replyToken, {
      kind: "accepted",
      attemptId: command.attemptId,
      branchKey: branch.branchKey,
      status,
      committedAt: validated.appendedAt,
    });
    return state;
  } catch (error) {
    await acknowledgeBranch(command.replyToken, {
      kind: "rejected",
      attemptId: command.attemptId,
      branchKey: command.branch.branchKey,
      failure: serializeFailure(error),
    });
    if (state === undefined) throw error;
    return state;
  }
}

async function processDueRuns(
  config: WorldAttentionConfig,
  state: AttentionWorkflowState,
  now: string,
): Promise<string> {
  let logicalNow = now;
  for (;;) {
    const claim = beginAttentionRunClaim(state, {
      now: logicalNow,
      leaseMs: config.limits.claimLeaseMs,
    });
    if (claim === undefined) return logicalNow;
    const claimed =
      claim.kind === "active"
        ? claim.active
        : activateAttentionRun(
            state,
            claim.draft,
            await deriveAttentionBatchIdentity(
              claim.draft.instanceKey,
              claim.draft.branches.map((branch) => branch.branchKey),
            ),
            { now: logicalNow, leaseMs: config.limits.claimLeaseMs },
          );
    let stage = claimed.stage;
    try {
      if (claimed.stage === "preparing") {
        const prepared = await invokeCallback(config, {
          kind: "prepare",
          value: claimed.batch,
        });
        logicalNow = later(logicalNow, prepared.completedAt);
        if (!prepared.ok) throw new CallbackRequestError(prepared.error, prepared.terminal);
        const checkpoint = await prepareAttentionCheckpoint(
          claimed.batch,
          state.mode,
          prepared.value as PreparedAttentionOutcome,
        );
        if (!checkpoint.ok) {
          throw new AttentionCallbackValidationError(new TypeError(checkpoint.error));
        }
        const transition = applyPreparedAttentionCheckpoint(state, checkpoint.prepared, {
          now: logicalNow,
          dedupeMs: config.limits.dedupeMs,
          maxPreparedWakeBytes: config.limits.maxPreparedWakeBytes,
          ...(checkpoint.wake === undefined ? {} : { wake: checkpoint.wake }),
        });
        if (transition !== "deliver") continue;
      }

      stage = "delivering";
      const wake = state.active?.wake;
      if (wake === undefined) throw new Error("delivery stage has no prepared wake");
      const delivered = await invokeCallback(config, { kind: "deliver", value: wake });
      logicalNow = later(logicalNow, delivered.completedAt);
      if (!delivered.ok) throw new CallbackRequestError(delivered.error, delivered.terminal);
      applyAttentionDeliveryReceipt(state, delivered.value as AttentionDeliveryReceipt, {
        now: logicalNow,
        dedupeMs: config.limits.dedupeMs,
      });
    } catch (error) {
      if (state.active === undefined || state.active.stage !== stage) throw error;
      failAttentionRun(state, error, {
        now: logicalNow,
        dedupeMs: config.limits.dedupeMs,
        retryDelayMs: config.limits.retryDelayMs,
        maxAttempts: config.limits.maxAttempts,
        terminalError: isTerminalError,
      });
      return logicalNow;
    }
  }
}

function serializeFailure(error: unknown): WorldAttentionFailure {
  if (error instanceof IdempotencyConflictError) {
    return { kind: "conflict", message: error.message, conflict: serializeConflict(error) };
  }
  if (error instanceof AttentionCapacityError) return { kind: "capacity", message: error.message };
  return { kind: "runtime", message: message(error), retryable: true };
}

function serializeConflict(error: IdempotencyConflictError): SerializedConflict {
  return {
    namespace: error.namespace,
    key: error.key,
    existingInputHash: error.existingInputHash,
    receivedInputHash: error.receivedInputHash,
  };
}

function isTerminalError(error: unknown): boolean {
  return (
    error instanceof AttentionCapacityError ||
    error instanceof IdempotencyConflictError ||
    error instanceof AttentionCallbackValidationError ||
    (error instanceof CallbackRequestError && error.terminal)
  );
}

class CallbackRequestError extends Error {
  readonly terminal: boolean;

  constructor(message: string, terminal: boolean) {
    super(message);
    this.name = "CallbackRequestError";
    this.terminal = terminal;
  }
}

function later(left: string, right: string): string {
  return left >= right ? left : right;
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
