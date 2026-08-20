import {
  AttentionCapacityError,
  attentionValueBytes,
  validateAcceptedFanout,
  type AcceptedFanout,
  type AttentionAcceptanceReceipt,
  type AttentionCallbacks,
  type AttentionEngine,
  type FrozenAttentionBatch,
  type FullAttentionBranch,
  type PreparedAttentionWake,
} from "./attention.js";
import type {
  AmbientApplicationBackend,
  AmbientBackendBinding,
} from "./application.js";
import { IdempotencyConflictError, type InputHash } from "./idempotency.js";
import { compileAttentionStreamAppends } from "./stream-protocol.js";
import { attentionStreamAppendFits } from "./stream-state.js";
import type { MonitorClock } from "./types.js";
import {
  correlationAppendInputBytes,
  correlationAppendManyBytes,
  correlationConfigHash,
  correlationTokenFromConfigHash,
  type CorrelationAppendInput,
  type CorrelationAppendManyCommand,
  type CorrelationWorkflowConfig,
} from "./workflow-protocol.js";
import { correlationWorkflow } from "./workflows/correlation.js";
import { getHookByToken, resumeHook, start } from "workflow/api";
import { HookNotFoundError } from "workflow/errors";

const DEFAULT_MAX_RECENT_MESSAGES = 48;
const DEFAULT_RETRY_DELAY_MS = 1_000;
const DEFAULT_CLAIM_LEASE_MS = 30_000;
const DEFAULT_MAX_ATTEMPTS = 10;
const DEFAULT_MAX_BRANCHES = 1_000;
const DEFAULT_MAX_FANOUT_BYTES = 16 * 1_024 * 1_024;
const DEFAULT_MAX_PREPARED_WAKE_BYTES = 1 * 1_024 * 1_024;
const DEFAULT_MAX_PENDING_BRANCHES = 1_000;
const DEFAULT_MAX_PENDING_BYTES = 16 * 1_024 * 1_024;
const DEFAULT_MAX_BATCH_COMMANDS = 64;
const DEFAULT_MAX_BATCH_BYTES = 16 * 1_024 * 1_024;
const DEFAULT_MAX_LOCAL_PENDING_COMMANDS = 1_000;
const DEFAULT_MAX_LOCAL_PENDING_BYTES = 64 * 1_024 * 1_024;
const DEFAULT_MAX_CALLBACK_REQUEST_BYTES = 16 * 1_024 * 1_024;
const DEFAULT_REGISTRATION_TIMEOUT_MS = 10_000;
const REGISTRATION_POLL_INITIAL_DELAY_MS = 5;
const REGISTRATION_POLL_MAX_DELAY_MS = 50;
const MAX_CACHED_CORRELATION_HOOKS = 1_024;
const CACHED_CORRELATION_HOOK_TTL_MS = 10 * 60_000;
const MAX_CORRELATION_PROBE_ATTEMPTS = 4;
/** A ready append cannot wait indefinitely for slower concurrent preparation. */
const BATCH_PREPARATION_MAX_WAIT_MS = 50;
/** A small bounded timer window collects async fan-outs with 5 ms nominal latency. */
const BATCH_FLUSH_WINDOW_MS = 5;
const EMPTY_APPEND_MANY_BYTES = correlationAppendManyBytes({
  kind: "append-many",
  commands: [],
});

type WorkflowHook = Awaited<ReturnType<typeof getHookByToken>>;

interface CorrelationProbeResult {
  readonly owner: WorkflowHook;
  readonly leaderCommandAccepted: boolean;
}

interface CorrelationProbeAttempt {
  readonly leader: boolean;
  readonly result: Promise<CorrelationProbeResult>;
}

interface CachedCorrelationHook {
  readonly hook: WorkflowHook;
  readonly expiresAt: number;
}

/** Process-local collapse of probes within one operational publication lane. */
const correlationProbes = new Map<string, Promise<CorrelationProbeResult>>();

/** Process-local hot set shared by every engine using the active Workflow World. */
const cachedCorrelationHooks = new Map<string, CachedCorrelationHook>();

interface QueuedCorrelationAppend {
  readonly input: CorrelationAppendInput;
  readonly branchCount: number;
  readonly branchBytes: number;
  readonly serializedBytes: number;
  readonly publish: (command: CorrelationAppendManyCommand) => Promise<void>;
  readonly resolve: () => void;
  readonly reject: (error: unknown) => void;
  settled: boolean;
}

interface CorrelationPublishQueue {
  readonly key: string;
  readonly token: string;
  readonly config: CorrelationWorkflowConfig;
  readonly maxPendingCommands: number;
  readonly maxPendingBytes: number;
  readonly queued: QueuedCorrelationAppend[];
  pendingCommands: number;
  pendingBytes: number;
  scheduled: boolean;
  flushing: boolean;
}

interface CorrelationPreparationGroup {
  pendingAccepts: number;
  readonly waitingQueues: Set<CorrelationPublishQueue>;
  releaseTimer: ReturnType<typeof setTimeout> | undefined;
  released: boolean;
}

interface CorrelationPreparationOptions {
  readonly config: CorrelationWorkflowConfig;
  readonly registrationTimeoutMs: number;
  readonly maxLocalPendingCommands: number;
  readonly maxLocalPendingBytes: number;
}

/** Process-local queues are isolated by hook token and operational settings. */
const correlationPublishQueues = new Map<string, CorrelationPublishQueue>();

/** Concurrent accept preparation is isolated by the complete raw correlation address. */
const correlationPreparations = new Map<string, CorrelationPreparationGroup>();

/** Retryable rejection when one process already holds its bounded local backlog. */
export class WorkflowAdmissionBackpressureError extends Error {
  readonly retryable = true;

  constructor(message: string) {
    super(message);
    this.name = "WorkflowAdmissionBackpressureError";
  }
}

export interface WorkflowAttentionEngineOptions {
  /** Public base URL at which this application's callback handler is mounted. */
  readonly callbackUrl: string;
  /** Optional isolation prefix when multiple deployments share one World. */
  readonly namespace?: string | undefined;
  /** Defaults to bearer authentication. Use none only behind authenticated transport policy. */
  readonly callbackAuth?: "bearer" | "none" | undefined;
  readonly callbackSecretEnv?: string | undefined;
  readonly preparePath?: string | undefined;
  readonly deliverPath?: string | undefined;
  readonly maxRecentMessages?: number | undefined;
  readonly retryDelayMs?: number | undefined;
  readonly claimLeaseMs?: number | undefined;
  readonly maxAttempts?: number | undefined;
  readonly maxBranches?: number | undefined;
  readonly maxFanoutBytes?: number | undefined;
  readonly maxPreparedWakeBytes?: number | undefined;
  /** Maximum full branch payloads applied to one correlation reducer at once. */
  readonly maxPendingBranches?: number | undefined;
  /** Maximum full branch bytes applied to one correlation reducer at once. */
  readonly maxPendingBytes?: number | undefined;
  /** Maximum independently accepted appends in one Workflow hook command. */
  readonly maxBatchCommands?: number | undefined;
  /** Maximum canonical serialized bytes in one Workflow hook command. */
  readonly maxBatchBytes?: number | undefined;
  /** Maximum accepted appends queued or publishing per process-local lane. */
  readonly maxLocalPendingCommands?: number | undefined;
  /** Maximum canonical append bytes queued or publishing per process-local lane. */
  readonly maxLocalPendingBytes?: number | undefined;
  readonly registrationTimeoutMs?: number | undefined;
  readonly clock?: MonitorClock | undefined;
}

export interface WorkflowAmbientOptions extends WorkflowAttentionEngineOptions {
  readonly maxCallbackRequestBytes?: number | undefined;
}

export interface WorkflowAmbientBinding extends AmbientBackendBinding {
  readonly engine: WorkflowAttentionEngine;
  readonly fetch: (request: Request) => Promise<Response>;
}

/** Binds Ambient to one permanent standard Workflow run per correlation. */
export function workflow(
  options: WorkflowAmbientOptions,
): AmbientApplicationBackend<WorkflowAmbientBinding> {
  return Object.freeze({
    ...(options.clock === undefined ? {} : { clock: options.clock }),
    bind(callbacks: AttentionCallbacks) {
      return Object.freeze({
        engine: new WorkflowAttentionEngine(options),
        fetch: createWorkflowAttentionCallbackHandler(callbacks, {
          ...(options.callbackAuth === undefined
            ? {}
            : { callbackAuth: options.callbackAuth }),
          ...(options.callbackSecretEnv === undefined
            ? {}
            : { secretEnv: options.callbackSecretEnv }),
          ...(options.preparePath === undefined ? {} : { preparePath: options.preparePath }),
          ...(options.deliverPath === undefined ? {} : { deliverPath: options.deliverPath }),
          ...(options.maxCallbackRequestBytes === undefined
            ? {}
            : { maxRequestBytes: options.maxCallbackRequestBytes }),
          ...(options.clock === undefined ? {} : { clock: options.clock }),
        }),
      });
    },
  });
}

/** Publishes each distinct correlation to its deterministic Workflow hook. */
export class WorkflowAttentionEngine implements AttentionEngine {
  readonly #config: CorrelationWorkflowConfig;
  #configHash: Promise<InputHash> | undefined;
  readonly #clock: MonitorClock;
  readonly #registrationTimeoutMs: number;
  readonly #maxBranches: number;
  readonly #maxFanoutBytes: number;
  readonly #maxLocalPendingCommands: number;
  readonly #maxLocalPendingBytes: number;

  constructor(options: WorkflowAttentionEngineOptions) {
    if (options === null || typeof options !== "object") {
      throw new TypeError("Workflow attention engine options are required");
    }
    const callbackAuth = callbackAuthMode(options.callbackAuth ?? "bearer");
    if (callbackAuth === "none" && options.callbackSecretEnv !== undefined) {
      throw new TypeError("callbackSecretEnv cannot be set when callbackAuth is none");
    }
    this.#config = {
      namespace: nonEmpty(options.namespace ?? "default", "namespace"),
      callbackUrl: callbackBaseUrl(options.callbackUrl),
      callbackSecretEnv: callbackAuth === "none"
        ? null
        : environmentName(options.callbackSecretEnv ?? "AMBIENT_CALLBACK_SECRET"),
      preparePath: pathName(options.preparePath ?? "/ambient/prepare", "preparePath"),
      deliverPath: pathName(options.deliverPath ?? "/ambient/deliver", "deliverPath"),
      maxRecentMessages: positiveInteger(
        options.maxRecentMessages ?? DEFAULT_MAX_RECENT_MESSAGES,
        "maxRecentMessages",
      ),
      claimLeaseMs: positiveInteger(
        options.claimLeaseMs ?? DEFAULT_CLAIM_LEASE_MS,
        "claimLeaseMs",
      ),
      retryDelayMs: positiveInteger(
        options.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS,
        "retryDelayMs",
      ),
      maxAttempts: positiveInteger(options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS, "maxAttempts"),
      maxPreparedWakeBytes: positiveInteger(
        options.maxPreparedWakeBytes ?? DEFAULT_MAX_PREPARED_WAKE_BYTES,
        "maxPreparedWakeBytes",
      ),
      maxPendingBranches: positiveInteger(
        options.maxPendingBranches ?? DEFAULT_MAX_PENDING_BRANCHES,
        "maxPendingBranches",
      ),
      maxPendingBytes: positiveInteger(
        options.maxPendingBytes ?? DEFAULT_MAX_PENDING_BYTES,
        "maxPendingBytes",
      ),
      maxBatchCommands: positiveInteger(
        options.maxBatchCommands ?? DEFAULT_MAX_BATCH_COMMANDS,
        "maxBatchCommands",
      ),
      maxBatchBytes: positiveInteger(
        options.maxBatchBytes ?? DEFAULT_MAX_BATCH_BYTES,
        "maxBatchBytes",
      ),
    };
    if (this.#config.preparePath === this.#config.deliverPath) {
      throw new TypeError("preparePath and deliverPath must be different");
    }
    this.#clock = options.clock ?? { now: () => new Date() };
    this.#registrationTimeoutMs = positiveInteger(
      options.registrationTimeoutMs ?? DEFAULT_REGISTRATION_TIMEOUT_MS,
      "registrationTimeoutMs",
    );
    this.#maxBranches = positiveInteger(
      options.maxBranches ?? DEFAULT_MAX_BRANCHES,
      "maxBranches",
    );
    this.#maxFanoutBytes = positiveInteger(
      options.maxFanoutBytes ?? DEFAULT_MAX_FANOUT_BYTES,
      "maxFanoutBytes",
    );
    this.#maxLocalPendingCommands = positiveInteger(
      options.maxLocalPendingCommands ?? DEFAULT_MAX_LOCAL_PENDING_COMMANDS,
      "maxLocalPendingCommands",
    );
    this.#maxLocalPendingBytes = positiveInteger(
      options.maxLocalPendingBytes ?? DEFAULT_MAX_LOCAL_PENDING_BYTES,
      "maxLocalPendingBytes",
    );
  }

  async accept(input: AcceptedFanout): Promise<AttentionAcceptanceReceipt> {
    const preparationOptions = {
      config: this.#config,
      registrationTimeoutMs: this.#registrationTimeoutMs,
      maxLocalPendingCommands: this.#maxLocalPendingCommands,
      maxLocalPendingBytes: this.#maxLocalPendingBytes,
    } satisfies CorrelationPreparationOptions;
    const preparations = beginCorrelationPreparations(
      correlationPreparationKeys(input, preparationOptions),
    );
    let preparationsFinished = false;
    try {
      const fanout = await validateAcceptedFanout(input);
      if (fanout.branches.length > this.#maxBranches) {
        throw new AttentionCapacityError(
          `accepted fan-out exceeds the maximum of ${this.#maxBranches} branches`,
        );
      }
      if (attentionValueBytes(fanout) > this.#maxFanoutBytes) {
        throw new AttentionCapacityError(
          `accepted fan-out exceeds the maximum of ${this.#maxFanoutBytes} bytes`,
        );
      }

      const acceptedAt = this.#clock.now().toISOString();
      const appends = await compileAttentionStreamAppends(fanout);
      for (const append of appends) {
        if (!attentionStreamAppendFits(undefined, append, this.#config)) {
          throw new AttentionCapacityError(
            `one correlation append exceeds the reducer limit of ` +
              `${this.#config.maxPendingBranches} branches or ` +
              `${this.#config.maxPendingBytes} bytes`,
          );
        }
      }
      const commands = appends.map((append): CorrelationAppendInput => ({
        append,
        acceptedAt,
      }));
      for (const command of commands) {
        const serializedBytes = correlationAppendManyBytes({
          kind: "append-many",
          commands: [command],
        });
        if (serializedBytes > this.#config.maxBatchBytes) {
          throw new AttentionCapacityError(
            `one correlation command exceeds the serialized batch limit of ` +
              `${this.#config.maxBatchBytes} bytes`,
          );
        }
        if (correlationAppendInputBytes(command) > this.#maxLocalPendingBytes) {
          throw new AttentionCapacityError(
            `one correlation command exceeds the process-local pending limit of ` +
              `${this.#maxLocalPendingBytes} bytes`,
          );
        }
      }
      const prepared = await Promise.all(commands.map(async (command) => {
        const token = await this.#correlationToken(command.append.streamKey);
        const queueKey = correlationQueueKey({
          token,
          registrationTimeoutMs: this.#registrationTimeoutMs,
          maxLocalPendingCommands: this.#maxLocalPendingCommands,
          maxLocalPendingBytes: this.#maxLocalPendingBytes,
        });
        const preparationKey = correlationPreparationKey(
          command.append.branches[0]!,
          preparationOptions,
        );
        return { command, token, queueKey, preparation: preparations.get(preparationKey) };
      }));
      const publications = prepared.map(({ command, token, queueKey, preparation }) =>
        enqueueCorrelationAppend({
          queueKey,
          token,
          config: this.#config,
          maxPendingCommands: this.#maxLocalPendingCommands,
          maxPendingBytes: this.#maxLocalPendingBytes,
          input: command,
          publish: (batch) => this.#publishBatch(queueKey, token, batch),
          preparation,
        }));
      finishCorrelationPreparations(preparations);
      preparationsFinished = true;
      await Promise.all(publications);
      return Object.freeze({
        eventKey: fanout.eventKey,
        occurrenceKey: fanout.occurrenceKey,
        inputHash: fanout.inputHash,
        branchKeys: Object.freeze(fanout.branches.map((branch) => branch.branchKey)),
        acceptedAt,
      });
    } finally {
      if (!preparationsFinished) finishCorrelationPreparations(preparations);
    }
  }

  #correlationToken(streamKey: string): Promise<string> {
    let configHash = this.#configHash;
    if (configHash === undefined) {
      configHash = correlationConfigHash(this.#config);
      this.#configHash = configHash;
      void configHash.catch(() => {
        if (this.#configHash === configHash) this.#configHash = undefined;
      });
    }
    return configHash.then((resolved) =>
      correlationTokenFromConfigHash(this.#config, resolved, streamKey));
  }

  async #publishBatch(
    publicationLaneKey: string,
    token: string,
    command: CorrelationAppendManyCommand,
  ): Promise<void> {
    const cached = cachedCorrelationHook(token);
    if (cached !== undefined) {
      try {
        const owner = await resumeHook(cached, command);
        cacheCorrelationHook(token, owner);
        return;
      } catch (error) {
        if (!isNotFound(error)) throw error;
        evictCachedCorrelationHook(token, cached);
      }
    }

    let probe = this.#probeCorrelation(publicationLaneKey, token, command);
    for (let attempt = 0; attempt < MAX_CORRELATION_PROBE_ATTEMPTS; attempt += 1) {
      const probed = await probe.result;
      cacheCorrelationHook(token, probed.owner);
      if (probe.leader && probed.leaderCommandAccepted) return;
      try {
        const owner = await resumeHook(probed.owner, command);
        cacheCorrelationHook(token, owner);
        return;
      } catch (error) {
        if (!isNotFound(error)) throw error;
        evictCachedCorrelationHook(token, probed.owner);
      }
      probe = this.#probeCorrelation(publicationLaneKey, token, command);
    }
    throw new Error(`could not publish append to correlation hook ${token}`);
  }

  #probeCorrelation(
    publicationLaneKey: string,
    token: string,
    command: CorrelationAppendManyCommand,
  ): CorrelationProbeAttempt {
    const existing = correlationProbes.get(publicationLaneKey);
    if (existing !== undefined) return { leader: false, result: existing };

    const result = this.#probeOrStartCorrelation(token, command);
    correlationProbes.set(publicationLaneKey, result);
    const clear = (): void => {
      if (correlationProbes.get(publicationLaneKey) === result) {
        correlationProbes.delete(publicationLaneKey);
      }
    };
    void result.then(clear, clear);
    return { leader: true, result };
  }

  async #probeOrStartCorrelation(
    token: string,
    command: CorrelationAppendManyCommand,
  ): Promise<CorrelationProbeResult> {
    try {
      return {
        owner: await resumeHook(token, command),
        leaderCommandAccepted: true,
      };
    } catch (error) {
      if (!isNotFound(error)) throw error;
    }
    const candidate = await start(correlationWorkflow, [
      this.#config,
      command.commands[0]!.append.streamKey,
      command,
    ]);
    const owner = await this.#waitForHook(token);
    return {
      owner,
      leaderCommandAccepted: owner.runId === candidate.runId,
    };
  }

  async #waitForHook(token: string): Promise<WorkflowHook> {
    const deadline = Date.now() + this.#registrationTimeoutMs;
    let delayMs = REGISTRATION_POLL_INITIAL_DELAY_MS;
    for (;;) {
      try {
        return await getHookByToken(token);
      } catch (error) {
        if (!isNotFound(error)) throw error;
      }
      const remainingMs = deadline - Date.now();
      if (remainingMs <= 0) {
        throw new Error(`timed out waiting for Workflow hook ${token}`);
      }
      const jitteredDelayMs = Math.max(
        1,
        Math.round(delayMs * (0.8 + Math.random() * 0.4)),
      );
      await new Promise((resolve) => {
        setTimeout(resolve, Math.min(jitteredDelayMs, remainingMs));
      });
      delayMs = Math.min(delayMs * 2, REGISTRATION_POLL_MAX_DELAY_MS);
    }
  }
}

function enqueueCorrelationAppend(input: {
  readonly queueKey: string;
  readonly token: string;
  readonly config: CorrelationWorkflowConfig;
  readonly maxPendingCommands: number;
  readonly maxPendingBytes: number;
  readonly input: CorrelationAppendInput;
  readonly publish: (command: CorrelationAppendManyCommand) => Promise<void>;
  readonly preparation: CorrelationPreparationGroup | undefined;
}): Promise<void> {
  let queue = correlationPublishQueues.get(input.queueKey);
  if (queue === undefined) {
    queue = {
      key: input.queueKey,
      token: input.token,
      config: input.config,
      maxPendingCommands: input.maxPendingCommands,
      maxPendingBytes: input.maxPendingBytes,
      queued: [],
      pendingCommands: 0,
      pendingBytes: 0,
      scheduled: false,
      flushing: false,
    };
    correlationPublishQueues.set(input.queueKey, queue);
  }
  const branchBytes = input.input.append.branches.reduce(
    (total, branch) => total + attentionValueBytes(branch),
    0,
  );
  const serializedBytes = correlationAppendInputBytes(input.input);
  if (
    queue.pendingCommands + 1 > queue.maxPendingCommands ||
    queue.pendingBytes + serializedBytes > queue.maxPendingBytes
  ) {
    return Promise.reject(new WorkflowAdmissionBackpressureError(
      `process-local correlation queue ${input.token} exceeds ` +
        `${queue.maxPendingCommands} commands or ${queue.maxPendingBytes} bytes`,
    ));
  }
  queue.pendingCommands += 1;
  queue.pendingBytes += serializedBytes;
  const result = new Promise<void>((resolve, reject) => {
    queue!.queued.push({
      input: input.input,
      branchCount: input.input.append.branches.length,
      branchBytes,
      serializedBytes,
      publish: input.publish,
      resolve,
      reject,
      settled: false,
    });
  });
  scheduleCorrelationFlushAfterPreparation(queue, input.preparation);
  return result;
}

function scheduleCorrelationFlushAfterPreparation(
  queue: CorrelationPublishQueue,
  preparation: CorrelationPreparationGroup | undefined,
): void {
  if (preparation === undefined || preparation.released) {
    scheduleCorrelationFlush(queue);
    return;
  }
  preparation.waitingQueues.add(queue);
  if (preparation.releaseTimer !== undefined) return;
  preparation.releaseTimer = setTimeout(() => {
    preparation.releaseTimer = undefined;
    releaseCorrelationPreparation(preparation);
  }, BATCH_PREPARATION_MAX_WAIT_MS);
}

function scheduleCorrelationFlush(queue: CorrelationPublishQueue): void {
  if (queue.scheduled) return;
  queue.scheduled = true;
  setTimeout(() => {
    queue.scheduled = false;
    void flushCorrelationQueue(queue);
  }, BATCH_FLUSH_WINDOW_MS);
}

async function flushCorrelationQueue(queue: CorrelationPublishQueue): Promise<void> {
  if (queue.flushing) return;
  if (queue.queued.length === 0) {
    if (correlationPublishQueues.get(queue.key) === queue) {
      correlationPublishQueues.delete(queue.key);
    }
    return;
  }

  queue.flushing = true;
  const pending = queue.queued.splice(0);
  try {
    const chunks = splitCorrelationChunks(pending, queue.config);
    for (let index = 0; index < chunks.length; index += 1) {
      const chunk = chunks[index]!;
      const command: CorrelationAppendManyCommand = Object.freeze({
        kind: "append-many",
        commands: Object.freeze(chunk.map((entry) => entry.input)),
      });
      try {
        await chunk[0]!.publish(command);
        for (const entry of chunk) settleCorrelationAppend(queue, entry, true);
      } catch (error) {
        for (const entry of chunk) settleCorrelationAppend(queue, entry, false, error);
        for (const remaining of chunks.slice(index + 1)) {
          for (const entry of remaining) {
            settleCorrelationAppend(queue, entry, false, error);
          }
        }
        break;
      }
    }
  } catch (error) {
    for (const entry of pending) settleCorrelationAppend(queue, entry, false, error);
  } finally {
    queue.flushing = false;
    if (queue.queued.length > 0) {
      scheduleCorrelationFlush(queue);
    } else if (!queue.scheduled && correlationPublishQueues.get(queue.key) === queue) {
      correlationPublishQueues.delete(queue.key);
    }
  }
}

function settleCorrelationAppend(
  queue: CorrelationPublishQueue,
  entry: QueuedCorrelationAppend,
  accepted: boolean,
  error?: unknown,
): void {
  if (entry.settled) return;
  entry.settled = true;
  queue.pendingCommands -= 1;
  queue.pendingBytes -= entry.serializedBytes;
  if (accepted) {
    entry.resolve();
  } else {
    entry.reject(error);
  }
}

function correlationPreparationKeys(
  input: AcceptedFanout,
  options: CorrelationPreparationOptions,
): readonly string[] {
  try {
    if (!Array.isArray(input.branches)) return [];
    return [...new Set(input.branches.map((branch) =>
      correlationPreparationKey(branch, options)))];
  } catch {
    // Validation remains authoritative; malformed inputs simply skip this optimization.
    return [];
  }
}

function correlationPreparationKey(
  branch: FullAttentionBranch,
  options: CorrelationPreparationOptions,
): string {
  const { config } = options;
  return JSON.stringify([
    config.namespace,
    config.callbackUrl,
    config.callbackSecretEnv,
    config.preparePath,
    config.deliverPath,
    config.maxRecentMessages,
    config.claimLeaseMs,
    config.retryDelayMs,
    config.maxAttempts,
    config.maxPreparedWakeBytes,
    config.maxPendingBranches,
    config.maxPendingBytes,
    config.maxBatchCommands,
    config.maxBatchBytes,
    options.registrationTimeoutMs,
    options.maxLocalPendingCommands,
    options.maxLocalPendingBytes,
    branch.applicationId,
    branch.tenantId,
    branch.event.source.channelId,
    branch.event.source.installationId,
    branch.partitionKey,
    branch.monitorId,
    branch.definitionVersion,
    branch.correlationKey,
  ]);
}

function beginCorrelationPreparations(
  keys: readonly string[],
): ReadonlyMap<string, CorrelationPreparationGroup> {
  const preparations = new Map<string, CorrelationPreparationGroup>();
  for (const key of keys) {
    let preparation = correlationPreparations.get(key);
    if (preparation === undefined || preparation.released) {
      preparation = {
        pendingAccepts: 0,
        waitingQueues: new Set(),
        releaseTimer: undefined,
        released: false,
      };
      correlationPreparations.set(key, preparation);
    }
    preparation.pendingAccepts += 1;
    preparations.set(key, preparation);
  }
  return preparations;
}

function finishCorrelationPreparations(
  preparations: ReadonlyMap<string, CorrelationPreparationGroup>,
): void {
  for (const [key, preparation] of preparations) {
    preparation.pendingAccepts -= 1;
    if (preparation.pendingAccepts > 0) continue;
    if (correlationPreparations.get(key) === preparation) {
      correlationPreparations.delete(key);
    }
    if (preparation.releaseTimer !== undefined) {
      clearTimeout(preparation.releaseTimer);
      preparation.releaseTimer = undefined;
    }
    releaseCorrelationPreparation(preparation);
  }
}

function releaseCorrelationPreparation(preparation: CorrelationPreparationGroup): void {
  if (preparation.released) return;
  preparation.released = true;
  const queues = [...preparation.waitingQueues];
  preparation.waitingQueues.clear();
  for (const queue of queues) scheduleCorrelationFlush(queue);
}

function correlationQueueKey(input: {
  readonly token: string;
  readonly registrationTimeoutMs: number;
  readonly maxLocalPendingCommands: number;
  readonly maxLocalPendingBytes: number;
}): string {
  return JSON.stringify([
    input.token,
    input.registrationTimeoutMs,
    input.maxLocalPendingCommands,
    input.maxLocalPendingBytes,
  ]);
}

function splitCorrelationChunks(
  pending: readonly QueuedCorrelationAppend[],
  config: CorrelationWorkflowConfig,
): readonly (readonly QueuedCorrelationAppend[])[] {
  const chunks: QueuedCorrelationAppend[][] = [];
  let chunk: QueuedCorrelationAppend[] = [];
  let branchCount = 0;
  let branchBytes = 0;
  let serializedInputBytes = 0;

  const flush = (): void => {
    if (chunk.length === 0) return;
    chunks.push(chunk);
    chunk = [];
    branchCount = 0;
    branchBytes = 0;
    serializedInputBytes = 0;
  };

  for (const entry of pending) {
    const candidateCount = chunk.length + 1;
    const candidateSerializedBytes = populatedAppendManyBytes(
      serializedInputBytes + entry.serializedBytes,
      candidateCount,
    );
    const exceeds =
      candidateCount > config.maxBatchCommands ||
      candidateSerializedBytes > config.maxBatchBytes ||
      branchCount + entry.branchCount > config.maxPendingBranches ||
      branchBytes + entry.branchBytes > config.maxPendingBytes;
    if (exceeds) flush();

    chunk.push(entry);
    branchCount += entry.branchCount;
    branchBytes += entry.branchBytes;
    serializedInputBytes += entry.serializedBytes;
    if (
      chunk.length > config.maxBatchCommands ||
      populatedAppendManyBytes(serializedInputBytes, chunk.length) > config.maxBatchBytes ||
      branchCount > config.maxPendingBranches ||
      branchBytes > config.maxPendingBytes
    ) {
      throw new AttentionCapacityError("one correlation command cannot fit a bounded batch");
    }
  }
  flush();
  return chunks;
}

function populatedAppendManyBytes(serializedInputBytes: number, count: number): number {
  return EMPTY_APPEND_MANY_BYTES + serializedInputBytes + count - 1;
}

export interface WorkflowAttentionCallbackHandlerOptions {
  /** Defaults to bearer authentication. Use none only behind authenticated transport policy. */
  readonly callbackAuth?: "bearer" | "none" | undefined;
  readonly secretEnv?: string | undefined;
  readonly preparePath?: string | undefined;
  readonly deliverPath?: string | undefined;
  readonly maxRequestBytes?: number | undefined;
  readonly clock?: MonitorClock | undefined;
}

export type WorkflowAttentionCallbackEnvelope =
  | { readonly ok: true; readonly completedAt: string; readonly value: unknown }
  | {
      readonly ok: false;
      readonly completedAt: string;
      readonly error: string;
      readonly terminal: boolean;
    };

/** Handles by-value prepare and deliver Workflow steps with explicit callback authentication. */
export function createWorkflowAttentionCallbackHandler(
  callbacks: AttentionCallbacks,
  options: WorkflowAttentionCallbackHandlerOptions = {},
): (request: Request) => Promise<Response> {
  if (
    callbacks === null ||
    typeof callbacks !== "object" ||
    typeof callbacks.prepare !== "function" ||
    typeof callbacks.deliver !== "function"
  ) {
    throw new TypeError("attention callbacks must define prepare and deliver");
  }
  const callbackAuth = callbackAuthMode(options.callbackAuth ?? "bearer");
  if (callbackAuth === "none" && options.secretEnv !== undefined) {
    throw new TypeError("secretEnv cannot be set when callbackAuth is none");
  }
  const secretEnv = callbackAuth === "none"
    ? null
    : environmentName(options.secretEnv ?? "AMBIENT_CALLBACK_SECRET");
  const preparePath = pathName(options.preparePath ?? "/ambient/prepare", "preparePath");
  const deliverPath = pathName(options.deliverPath ?? "/ambient/deliver", "deliverPath");
  if (preparePath === deliverPath) throw new TypeError("preparePath and deliverPath must be different");
  const maxRequestBytes = positiveInteger(
    options.maxRequestBytes ?? DEFAULT_MAX_CALLBACK_REQUEST_BYTES,
    "maxRequestBytes",
  );
  const clock = options.clock ?? { now: () => new Date() };

  return async (request) => {
    if (request.method !== "POST") return json({ error: "method not allowed" }, 405);
    if (secretEnv !== null) {
      const secret = process.env[secretEnv];
      if (secret === undefined || secret.length === 0) {
        return json({ error: `callback secret environment variable ${secretEnv} is not set` }, 503);
      }
      if (!secretsMatch(bearerToken(request), secret)) return json({ error: "unauthorized" }, 401);
    }
    const path = new URL(request.url).pathname;
    if (path !== preparePath && path !== deliverPath) return json({ error: "not found" }, 404);
    let body: unknown;
    try {
      body = await readJson(request, maxRequestBytes);
    } catch (error) {
      return callbackJson(
        {
          ok: false,
          completedAt: clock.now().toISOString(),
          error: message(error),
          terminal: true,
        },
        error instanceof CallbackBodyTooLargeError ? 413 : 400,
      );
    }

    try {
      const value = path === preparePath
        ? await callbacks.prepare(deepFreeze(body as FrozenAttentionBatch))
        : await callbacks.deliver(deepFreeze(body as PreparedAttentionWake));
      return callbackJson({ ok: true, completedAt: clock.now().toISOString(), value });
    } catch (error) {
      return callbackJson(
        {
          ok: false,
          completedAt: clock.now().toISOString(),
          error: message(error),
          terminal:
            error instanceof IdempotencyConflictError || error instanceof AttentionCapacityError,
        },
        503,
      );
    }
  };
}

export function secretsMatch(left: string, right: string): boolean {
  const a = new TextEncoder().encode(left);
  const b = new TextEncoder().encode(right);
  let difference = a.length ^ b.length;
  const length = Math.max(a.length, b.length);
  for (let index = 0; index < length; index += 1) {
    difference |= (a[index] ?? 0) ^ (b[index] ?? 0);
  }
  return difference === 0;
}

function callbackJson(body: WorkflowAttentionCallbackEnvelope, status = 200): Response {
  return json(body, status);
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function bearerToken(request: Request): string {
  const match = /^Bearer[ ]+(.+)$/i.exec(request.headers.get("authorization")?.trim() ?? "");
  return match?.[1] ?? "";
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  return value;
}

function callbackBaseUrl(value: string): string {
  const url = new URL(nonEmpty(value, "callbackUrl"));
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new TypeError("callbackUrl must use http or https");
  }
  if (url.username.length > 0 || url.password.length > 0 || url.search.length > 0 || url.hash.length > 0) {
    throw new TypeError("callbackUrl must not contain credentials, a query, or a fragment");
  }
  return url.toString().replace(/\/$/, "");
}

function environmentName(value: string): string {
  const normalized = nonEmpty(value, "callbackSecretEnv");
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(normalized)) {
    throw new TypeError("callbackSecretEnv must be an environment variable name");
  }
  return normalized;
}

function callbackAuthMode(value: string): "bearer" | "none" {
  if (value !== "bearer" && value !== "none") {
    throw new TypeError("callbackAuth must be bearer or none");
  }
  return value;
}

function pathName(value: string, name: string): string {
  const normalized = nonEmpty(value, name);
  if (!normalized.startsWith("/") || normalized.startsWith("//")) {
    throw new TypeError(`${name} must be an absolute URL path`);
  }
  return normalized;
}

function nonEmpty(value: string, name: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${name} must not be empty`);
  }
  return value;
}

function isNotFound(error: unknown): boolean {
  return HookNotFoundError.is(error);
}

function cachedCorrelationHook(token: string): WorkflowHook | undefined {
  const cached = cachedCorrelationHooks.get(token);
  if (cached === undefined) return undefined;
  if (cached.expiresAt <= Date.now()) {
    cachedCorrelationHooks.delete(token);
    return undefined;
  }
  cachedCorrelationHooks.delete(token);
  cachedCorrelationHooks.set(token, cached);
  return cached.hook;
}

function cacheCorrelationHook(token: string, hook: WorkflowHook): void {
  cachedCorrelationHooks.delete(token);
  cachedCorrelationHooks.set(token, {
    hook,
    expiresAt: Date.now() + CACHED_CORRELATION_HOOK_TTL_MS,
  });
  while (cachedCorrelationHooks.size > MAX_CACHED_CORRELATION_HOOKS) {
    const oldest = cachedCorrelationHooks.keys().next().value as string | undefined;
    if (oldest === undefined) break;
    cachedCorrelationHooks.delete(oldest);
  }
}

function evictCachedCorrelationHook(token: string, rejected: WorkflowHook): void {
  const cached = cachedCorrelationHooks.get(token);
  if (cached?.hook.runId === rejected.runId) cachedCorrelationHooks.delete(token);
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function readJson(request: Request, maxBytes: number): Promise<unknown> {
  const declared = request.headers.get("content-length");
  if (declared !== null && /^\d+$/.test(declared) && Number(declared) > maxBytes) {
    throw new CallbackBodyTooLargeError(maxBytes);
  }
  if (request.body === null) throw new TypeError("callback request body is empty");

  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const result = await reader.read();
      if (result.done) break;
      total += result.value.byteLength;
      if (total > maxBytes) {
        await reader.cancel().catch(() => undefined);
        throw new CallbackBodyTooLargeError(maxBytes);
      }
      chunks.push(result.value);
    }
  } finally {
    reader.releaseLock();
  }

  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  const source = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  try {
    return JSON.parse(source) as unknown;
  } catch (error) {
    throw new TypeError(`invalid callback JSON: ${message(error)}`);
  }
}

class CallbackBodyTooLargeError extends RangeError {
  constructor(maxBytes: number) {
    super(`callback request exceeds the maximum of ${maxBytes} bytes`);
    this.name = "CallbackBodyTooLargeError";
  }
}

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object") {
    Object.freeze(value);
    for (const nested of Object.values(value)) deepFreeze(nested);
  }
  return value;
}
