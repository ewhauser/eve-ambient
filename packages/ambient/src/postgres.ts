import {
  AttentionCapacityError,
  validateAcceptedFanout,
  validateFullAttentionBranch,
  type AcceptedFanout,
  type AttentionAcceptanceReceipt,
  type AttentionCallbacks,
  type AttentionEngine,
  type FullAttentionBranch,
} from "./attention.js";
import type {
  AmbientApplicationBackend,
  AmbientBackendBinding,
} from "./application.js";
import {
  completeEventCoordinator,
  createEventCoordinator,
  eventCoordinatorExpired,
  markCoordinatorBranchAccepted,
  pendingCoordinatorBranches,
  validateEventCoordinatorRetry,
  type EventCoordinatorState,
} from "./coordinator.js";
import {
  deriveAttentionPartitionKey,
  deriveAttentionInstanceKey,
  hashIdempotencyInput,
  IdempotencyConflictError,
  type AttentionInstanceKey,
  type EventKey,
} from "./idempotency.js";
import type {
  MemoryAttentionDiagnostics,
  MemoryAttentionEngineFaults,
  MemoryAttentionEngineOptions,
  MemoryAttentionRunResult,
} from "./memory-engine.js";
import type { MonitorClock } from "./types.js";
import {
  AttentionCallbackValidationError,
  appendAttentionBranch,
  applyAttentionDeliveryReceipt,
  applyPreparedAttentionOutcome,
  claimAttentionRun,
  createAttentionWorkflow,
  failAttentionRun,
  isCurrentAttentionClaim,
  nextAttentionDueAt,
  purgeAttentionWorkflow,
  type AttentionWorkflowState,
} from "./workflow.js";

const DEFAULT_DEDUPE_MS = 7 * 24 * 60 * 60 * 1_000;
const DEFAULT_RETRY_DELAY_MS = 1_000;
const DEFAULT_CLAIM_LEASE_MS = 30_000;
const DEFAULT_MAX_ATTEMPTS = 10;
const DEFAULT_MAX_BRANCHES = 1_000;
const DEFAULT_MAX_FANOUT_BYTES = 16 * 1_024 * 1_024;
const DEFAULT_MAX_PREPARED_WAKE_BYTES = 1 * 1_024 * 1_024;

export interface PostgresQueryResult<TRow = Record<string, unknown>> {
  readonly rows: readonly TRow[];
  readonly rowCount?: number | null | undefined;
}

export interface PostgresClient {
  query<TRow = Record<string, unknown>>(
    text: string,
    values?: readonly unknown[],
  ): Promise<PostgresQueryResult<TRow>>;
  release?(): void;
}

export interface PostgresPool extends PostgresClient {
  connect(): Promise<PostgresClient>;
}

export interface PostgresAttentionEngineOptions
  extends Omit<MemoryAttentionEngineOptions, "callbacks" | "faults"> {
  readonly pool: PostgresPool;
  readonly callbacks: AttentionCallbacks;
  /** Namespaces independent applications in the private backend tables. */
  readonly engineId?: string | undefined;
}

export interface PostgresAmbientBinding extends AmbientBackendBinding {
  readonly engine: PostgresAttentionEngine;
}

/** Binds an application definition to PostgreSQL without duplicating callbacks. */
export function postgres(
  options: Omit<PostgresAttentionEngineOptions, "callbacks">,
): AmbientApplicationBackend<PostgresAmbientBinding> {
  return Object.freeze({
    ...(options.clock === undefined ? {} : { clock: options.clock }),
    bind(callbacks: AttentionCallbacks) {
      return Object.freeze({
        engine: new PostgresAttentionEngine({ ...options, callbacks }),
      });
    },
  });
}

interface Limits {
  readonly dedupeMs: number;
  readonly retryDelayMs: number;
  readonly claimLeaseMs: number;
  readonly maxAttempts: number;
  readonly maxBranches: number;
  readonly maxFanoutBytes: number;
  readonly maxPreparedWakeBytes: number;
}

interface StateRow<T> {
  readonly state: T | string;
}

interface WorkflowMutation<T> {
  readonly result: T;
  readonly workflow?: AttentionWorkflowState | undefined;
  readonly delete?: boolean | undefined;
}

/** Scalable per-event/per-correlation PostgreSQL AttentionEngine. */
export class PostgresAttentionEngine implements AttentionEngine {
  readonly #pool: PostgresPool;
  readonly #callbacks: AttentionCallbacks;
  readonly #clock: MonitorClock;
  readonly #engineId: string;
  readonly #limits: Limits;
  readonly #faults: MemoryAttentionEngineFaults;

  constructor(
    options: PostgresAttentionEngineOptions,
    internal: { readonly faults?: MemoryAttentionEngineFaults | undefined } = {},
  ) {
    if (
      options?.pool === undefined ||
      typeof options.pool.query !== "function" ||
      typeof options.pool.connect !== "function"
    ) {
      throw new TypeError("PostgresAttentionEngine requires a pg-compatible pool");
    }
    if (
      options.callbacks === null ||
      typeof options.callbacks !== "object" ||
      typeof options.callbacks.prepare !== "function" ||
      typeof options.callbacks.deliver !== "function"
    ) {
      throw new TypeError("attention callbacks must define prepare and deliver");
    }
    this.#pool = options.pool;
    this.#callbacks = options.callbacks;
    this.#clock = options.clock ?? { now: () => new Date() };
    this.#engineId = nonEmpty(options.engineId ?? "default", "engineId");
    this.#limits = {
      dedupeMs: positiveInteger(options.dedupeMs ?? DEFAULT_DEDUPE_MS, "dedupeMs"),
      retryDelayMs: positiveInteger(
        options.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS,
        "retryDelayMs",
      ),
      claimLeaseMs: positiveInteger(
        options.claimLeaseMs ?? DEFAULT_CLAIM_LEASE_MS,
        "claimLeaseMs",
      ),
      maxAttempts: positiveInteger(options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS, "maxAttempts"),
      maxBranches: positiveInteger(options.maxBranches ?? DEFAULT_MAX_BRANCHES, "maxBranches"),
      maxFanoutBytes: positiveInteger(
        options.maxFanoutBytes ?? DEFAULT_MAX_FANOUT_BYTES,
        "maxFanoutBytes",
      ),
      maxPreparedWakeBytes: positiveInteger(
        options.maxPreparedWakeBytes ?? DEFAULT_MAX_PREPARED_WAKE_BYTES,
        "maxPreparedWakeBytes",
      ),
    };
    this.#faults = internal.faults ?? {};
  }

  async initialize(): Promise<void> {
    try {
      await this.#pool.query("SELECT event_key FROM eve_ambient_event_coordinators LIMIT 0");
      await this.#pool.query("SELECT instance_key FROM eve_ambient_correlation_workflows LIMIT 0");
    } catch (error) {
      throw new Error(
        `PostgreSQL attention schema is unavailable; apply migrations/001_attention_engine.sql: ${message(error)}`,
        { cause: error },
      );
    }
  }

  async accept(input: AcceptedFanout): Promise<AttentionAcceptanceReceipt> {
    const proposed = await validateAcceptedFanout(input);
    const client = await acquire(this.#pool);
    try {
      let coordinator = await this.#ensureCoordinator(client, proposed);
      if (coordinator.receipt !== undefined) return clone(coordinator.receipt);
      for (;;) {
        const branch = pendingCoordinatorBranches(coordinator)[0];
        if (branch === undefined) break;
        await this.#faults.beforeBranchAppend?.(clone(branch));
        await this.#appendBranch(client, branch);
        await this.#faults.afterBranchAppend?.(clone(branch));
        coordinator = await this.#mutateCoordinator(
          client,
          proposed.eventKey,
          (current) => {
            markCoordinatorBranchAccepted(current, branch.branchKey);
            return current;
          },
        );
      }
      coordinator = await this.#mutateCoordinator(client, proposed.eventKey, (current) => {
        if (current.receipt === undefined) {
          completeEventCoordinator(current, {
            now: this.#now(),
            dedupeMs: this.#limits.dedupeMs,
          });
        }
        return current;
      });
      if (coordinator.receipt === undefined) throw new Error("event coordinator did not complete");
      return clone(coordinator.receipt);
    } finally {
      client.release?.();
    }
  }

  async runOnce(
    options: { readonly limit?: number | undefined } = {},
  ): Promise<MemoryAttentionRunResult> {
    const limit = positiveInteger(options.limit ?? 100, "limit");
    const client = await acquire(this.#pool);
    try {
      await this.#cleanupExpiredCoordinators(client);
      const due = await client.query<{ readonly instance_key: string }>(
        `SELECT instance_key
           FROM eve_ambient_correlation_workflows
          WHERE engine_id = $1 AND next_due_at <= $2::timestamptz
          ORDER BY next_due_at, instance_key
          LIMIT $3`,
        [this.#engineId, this.#now(), limit],
      );
      const result = {
        claimed: 0,
        ignored: 0,
        shadowed: 0,
        delivered: 0,
        failed: 0,
        terminalFailures: 0,
      };
      for (const row of due.rows) {
        const outcome = await this.#processOne(client, row.instance_key as AttentionInstanceKey);
        if (outcome === "none") continue;
        result.claimed += 1;
        if (outcome === "ignored") result.ignored += 1;
        if (outcome === "shadowed") result.shadowed += 1;
        if (outcome === "delivered") result.delivered += 1;
        if (outcome === "failed") result.failed += 1;
        if (outcome === "terminal-failure") result.terminalFailures += 1;
      }
      return result;
    } finally {
      client.release?.();
    }
  }

  async diagnostics(): Promise<MemoryAttentionDiagnostics> {
    const client = await acquire(this.#pool);
    try {
      await this.#cleanupExpiredCoordinators(client);
      const coordinators = await client.query<StateRow<EventCoordinatorState>>(
        `SELECT state FROM eve_ambient_event_coordinators WHERE engine_id = $1`,
        [this.#engineId],
      );
      const workflowRows = await client.query<StateRow<AttentionWorkflowState>>(
        `SELECT state FROM eve_ambient_correlation_workflows WHERE engine_id = $1`,
        [this.#engineId],
      );
      const workflows: AttentionWorkflowState[] = [];
      for (const row of workflowRows.rows) {
        const workflow = parseState<AttentionWorkflowState>(row.state);
        if (purgeAttentionWorkflow(workflow, this.#now()) !== "empty") workflows.push(workflow);
      }
      const eventStates = coordinators.rows.map((row) => parseState<EventCoordinatorState>(row.state));
      return {
        eventCoordinators: eventStates.length,
        pendingFanoutPayloads: eventStates.filter((state) => state.pendingFanout !== undefined).length,
        acceptanceReceipts: eventStates.filter((state) => state.receipt !== undefined).length,
        correlationWorkflows: workflows.length,
        bufferedBranchPayloads: workflows.reduce(
          (count, workflow) =>
            count +
            (workflow.open?.branches.length ?? 0) +
            workflow.sealed.reduce((sum, batch) => sum + batch.branches.length, 0),
          0,
        ),
        activeBatchPayloads: workflows.reduce(
          (count, workflow) => count + (workflow.active?.batch.branches.length ?? 0),
          0,
        ),
        preparedWakePayloads: workflows.filter((workflow) => workflow.active?.wake !== undefined)
          .length,
        branchReceipts: workflows.reduce(
          (count, workflow) => count + workflow.branchLedger.length,
          0,
        ),
        deliveryReceipts: workflows.reduce(
          (count, workflow) => count + workflow.deliveryReceipts.length,
          0,
        ),
        terminalFailures: workflows.reduce(
          (count, workflow) => count + workflow.terminalFailures.length,
          0,
        ),
      };
    } finally {
      client.release?.();
    }
  }

  async #ensureCoordinator(
    client: PostgresClient,
    proposed: AcceptedFanout,
  ): Promise<EventCoordinatorState> {
    return this.#transaction(client, `event:${proposed.eventKey}`, async () => {
      let current = await this.#loadCoordinator(client, proposed.eventKey, true);
      const now = this.#now();
      if (current !== undefined && eventCoordinatorExpired(current, now)) {
        await this.#checkpointQuery(
          client,
          `DELETE FROM eve_ambient_event_coordinators WHERE engine_id = $1 AND event_key = $2`,
          [this.#engineId, proposed.eventKey],
        );
        current = undefined;
      }
      if (current === undefined) {
        current = createEventCoordinator(proposed, {
          now,
          maxBranches: this.#limits.maxBranches,
          maxFanoutBytes: this.#limits.maxFanoutBytes,
        });
        await this.#saveCoordinator(client, current);
      } else {
        validateEventCoordinatorRetry(current, proposed);
      }
      return clone(current);
    });
  }

  async #mutateCoordinator(
    client: PostgresClient,
    eventKey: EventKey,
    mutate: (state: EventCoordinatorState) => EventCoordinatorState,
  ): Promise<EventCoordinatorState> {
    return this.#transaction(client, `event:${eventKey}`, async () => {
      const current = await this.#loadCoordinator(client, eventKey, true);
      if (current === undefined) throw new Error("event coordinator disappeared before completion");
      const next = mutate(current);
      await this.#saveCoordinator(client, next);
      return clone(next);
    });
  }

  async #appendBranch(client: PostgresClient, input: FullAttentionBranch): Promise<void> {
    const branch = await validateFullAttentionBranch(input);
    const partitionCellKey = await deriveAttentionPartitionKey({
      applicationId: branch.applicationId,
      tenantId: branch.tenantId,
      channelId: branch.event.source.channelId,
      installationId: branch.event.source.installationId,
      partitionKey: branch.partitionKey,
    });
    const instanceKey = await deriveAttentionInstanceKey({
      partitionCellKey,
      monitorId: branch.monitorId,
      definitionVersion: branch.definitionVersion,
      correlationKey: branch.correlationKey,
    });
    const policyHash = await hashIdempotencyInput({ mode: branch.mode, policy: branch.policy });
    await this.#mutateWorkflow(client, instanceKey, async (current) => {
      const workflow = current ?? createAttentionWorkflow({ instanceKey, branch, policyHash });
      appendAttentionBranch(workflow, branch, {
        now: this.#now(),
        dedupeMs: this.#limits.dedupeMs,
        policyHash,
      });
      return { result: undefined, workflow };
    });
  }

  async #processOne(
    client: PostgresClient,
    instanceKey: AttentionInstanceKey,
  ): Promise<"none" | "ignored" | "shadowed" | "delivered" | "failed" | "terminal-failure"> {
    const claimed = await this.#mutateWorkflow(client, instanceKey, async (workflow) => {
      if (workflow === undefined) return { result: undefined, delete: true };
      if (purgeAttentionWorkflow(workflow, this.#now()) === "empty") {
        return { result: undefined, delete: true };
      }
      const active = await claimAttentionRun(workflow, {
        now: this.#now(),
        leaseMs: this.#limits.claimLeaseMs,
      });
      return { result: active === undefined ? undefined : clone(active), workflow };
    });
    if (claimed === undefined) return "none";
    let failureStage = claimed.stage;
    try {
      if (claimed.stage === "preparing") {
        const prepared = await this.#callbacks.prepare(deepFreeze(clone(claimed.batch)));
        const transition = await this.#mutateWorkflow(client, instanceKey, async (workflow) => {
          if (!isCurrentAttentionClaim(workflow, claimed, "preparing")) {
            return { result: "stale" as const, workflow };
          }
          const result = await applyPreparedAttentionOutcome(workflow, prepared, {
            now: this.#now(),
            dedupeMs: this.#limits.dedupeMs,
            maxPreparedWakeBytes: this.#limits.maxPreparedWakeBytes,
          });
          return { result, workflow };
        });
        if (transition === "stale") return "none";
        if (transition !== "deliver") return transition;
      }
      failureStage = "delivering";
      const wake = await this.#mutateWorkflow(client, instanceKey, async (workflow) => {
        if (!isCurrentAttentionClaim(workflow, claimed, "delivering")) {
          return { result: undefined, workflow };
        }
        if (workflow.active?.wake === undefined) throw new Error("delivery stage has no prepared wake");
        return { result: clone(workflow.active.wake), workflow };
      });
      if (wake === undefined) return "none";
      const receipt = await this.#callbacks.deliver(deepFreeze(wake));
      const committed = await this.#mutateWorkflow(client, instanceKey, async (workflow) => {
        if (!isCurrentAttentionClaim(workflow, claimed, "delivering")) {
          return { result: false, workflow };
        }
        applyAttentionDeliveryReceipt(workflow, receipt, {
          now: this.#now(),
          dedupeMs: this.#limits.dedupeMs,
        });
        return { result: true, workflow };
      });
      return committed ? "delivered" : "none";
    } catch (error) {
      if (error instanceof PostgresCheckpointError) throw error.cause;
      return this.#mutateWorkflow(client, instanceKey, async (workflow) => {
        if (!isCurrentAttentionClaim(workflow, claimed, failureStage)) {
          return { result: "none" as const, workflow };
        }
        const result = failAttentionRun(workflow, error, {
          now: this.#now(),
          dedupeMs: this.#limits.dedupeMs,
          retryDelayMs: this.#limits.retryDelayMs,
          maxAttempts: this.#limits.maxAttempts,
          terminalError: isTerminalError,
        });
        return { result, workflow };
      });
    }
  }

  async #mutateWorkflow<T>(
    client: PostgresClient,
    instanceKey: AttentionInstanceKey,
    mutate: (state: AttentionWorkflowState | undefined) => Promise<WorkflowMutation<T>>,
  ): Promise<T> {
    return this.#transaction(client, `workflow:${instanceKey}`, async () => {
      const current = await this.#loadWorkflow(client, instanceKey, true);
      const mutation = await mutate(current);
      if (mutation.delete === true || mutation.workflow === undefined) {
        await this.#checkpointQuery(
          client,
          `DELETE FROM eve_ambient_correlation_workflows WHERE engine_id = $1 AND instance_key = $2`,
          [this.#engineId, instanceKey],
        );
      } else {
        await this.#saveWorkflow(client, mutation.workflow);
      }
      return mutation.result;
    });
  }

  async #transaction<T>(
    client: PostgresClient,
    lockKey: string,
    operation: () => Promise<T>,
  ): Promise<T> {
    let phase: "database" | "operation" = "database";
    try {
      await client.query("BEGIN");
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
        `eve-ambient:${this.#engineId}:${lockKey}`,
      ]);
      phase = "operation";
      const result = await operation();
      phase = "database";
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      if (error instanceof PostgresCheckpointError) throw error;
      if (phase === "database") throw new PostgresCheckpointError(error);
      throw error;
    }
  }

  async #loadCoordinator(
    client: PostgresClient,
    eventKey: EventKey,
    forUpdate: boolean,
  ): Promise<EventCoordinatorState | undefined> {
    const result = await this.#checkpointQuery<StateRow<EventCoordinatorState>>(
      client,
      `SELECT state FROM eve_ambient_event_coordinators
        WHERE engine_id = $1 AND event_key = $2${forUpdate ? " FOR UPDATE" : ""}`,
      [this.#engineId, eventKey],
    );
    return result.rows[0] === undefined ? undefined : parseState(result.rows[0].state);
  }

  async #saveCoordinator(client: PostgresClient, state: EventCoordinatorState): Promise<void> {
    await this.#checkpointQuery(
      client,
      `INSERT INTO eve_ambient_event_coordinators
         (engine_id, event_key, state, expires_at, updated_at)
       VALUES ($1, $2, $3::jsonb, $4::timestamptz, now())
       ON CONFLICT (engine_id, event_key) DO UPDATE
       SET state = EXCLUDED.state, expires_at = EXCLUDED.expires_at, updated_at = EXCLUDED.updated_at`,
      [this.#engineId, state.eventKey, JSON.stringify(state), state.dedupeExpiresAt ?? null],
    );
  }

  async #loadWorkflow(
    client: PostgresClient,
    instanceKey: AttentionInstanceKey,
    forUpdate: boolean,
  ): Promise<AttentionWorkflowState | undefined> {
    const result = await this.#checkpointQuery<StateRow<AttentionWorkflowState>>(
      client,
      `SELECT state FROM eve_ambient_correlation_workflows
        WHERE engine_id = $1 AND instance_key = $2${forUpdate ? " FOR UPDATE" : ""}`,
      [this.#engineId, instanceKey],
    );
    return result.rows[0] === undefined ? undefined : parseState(result.rows[0].state);
  }

  async #saveWorkflow(client: PostgresClient, state: AttentionWorkflowState): Promise<void> {
    await this.#checkpointQuery(
      client,
      `INSERT INTO eve_ambient_correlation_workflows
         (engine_id, instance_key, state, next_due_at, updated_at)
       VALUES ($1, $2, $3::jsonb, $4::timestamptz, now())
       ON CONFLICT (engine_id, instance_key) DO UPDATE
       SET state = EXCLUDED.state, next_due_at = EXCLUDED.next_due_at, updated_at = EXCLUDED.updated_at`,
      [this.#engineId, state.instanceKey, JSON.stringify(state), nextAttentionDueAt(state) ?? null],
    );
  }

  async #cleanupExpiredCoordinators(client: PostgresClient): Promise<void> {
    await client.query(
      `DELETE FROM eve_ambient_event_coordinators
        WHERE engine_id = $1 AND expires_at IS NOT NULL AND expires_at <= $2::timestamptz`,
      [this.#engineId, this.#now()],
    );
  }

  async #checkpointQuery<TRow = Record<string, unknown>>(
    client: PostgresClient,
    text: string,
    values?: readonly unknown[],
  ): Promise<PostgresQueryResult<TRow>> {
    try {
      return await client.query<TRow>(text, values);
    } catch (error) {
      throw new PostgresCheckpointError(error);
    }
  }

  #now(): string {
    return this.#clock.now().toISOString();
  }
}

class PostgresCheckpointError extends Error {
  readonly cause: unknown;

  constructor(cause: unknown) {
    super("PostgreSQL attention checkpoint failed", { cause });
    this.cause = cause;
  }
}

function isTerminalError(error: unknown): boolean {
  return (
    error instanceof AttentionCapacityError ||
    error instanceof IdempotencyConflictError ||
    error instanceof AttentionCallbackValidationError
  );
}

async function acquire(pool: PostgresPool): Promise<PostgresClient> {
  return pool.connect();
}

function parseState<T>(value: T | string): T {
  return clone(typeof value === "string" ? (JSON.parse(value) as T) : value);
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  return value;
}

function nonEmpty(value: string, name: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${name} must not be empty`);
  }
  return value;
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object") {
    Object.freeze(value);
    for (const nested of Object.values(value)) deepFreeze(nested);
  }
  return value;
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
