import {
  AttentionCapacityError,
  attentionValueBytes,
  validateAcceptedFanout,
  validateFullAttentionBranch,
  type AcceptedFanout,
  type AttentionDeliveryReceipt,
  type FullAttentionBranch,
  type PreparedAttentionOutcome,
} from "./attention.js";
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
import { secretsMatch } from "./celld.js";
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
const DEFAULT_MAX_RESIDENT_BYTES = 32 * 1_024 * 1_024;

type Storage = {
  get(key: string): Promise<unknown>;
  put(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<void>;
  getAlarm(): Promise<number | null>;
  setAlarm(at: number): Promise<void>;
  deleteAlarm(): Promise<void>;
};

interface CellRecord {
  readonly kind: "partition";
  coordinators: EventCoordinatorState[];
  workflows: AttentionWorkflowState[];
}

interface Limits {
  readonly dedupeMs: number;
  readonly retryDelayMs: number;
  readonly claimLeaseMs: number;
  readonly maxAttempts: number;
  readonly maxBranches: number;
  readonly maxFanoutBytes: number;
  readonly maxPreparedWakeBytes: number;
  readonly maxResidentBytes: number;
}

/** One celld Durable Object per channel-defined custody partition. */
export class AttentionCell {
  readonly state: any;
  readonly env: any;
  #requestCellName?: string;
  #mutex: Promise<void> = Promise.resolve();

  constructor(state: any, env: any) {
    this.state = state;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const presentedName = request.headers.get("x-cell-name");
    if (presentedName !== null) this.#requestCellName = presentedName;
    return this.#withLock(() => this.#handle(request));
  }

  async alarm(): Promise<void> {
    await this.#handleAlarm();
  }

  async #handle(request: Request): Promise<Response> {
    const action = new URL(request.url).pathname.split("/").filter(Boolean).at(-1) ?? "diagnostics";
    if (action === "whoami") return json({ id: String(this.state.id) });
    if (action === "diagnostics") return json(await this.#diagnostics());
    if (request.method !== "POST") return json({ error: "method not allowed" }, 405);
    let body: unknown;
    try {
      body = await request.json();
    } catch (error) {
      return json({ error: `invalid JSON: ${message(error)}` }, 400);
    }
    try {
      if (action === "accept") return json(await this.#accept(body as AcceptedFanout));
      return json({ error: "not found" }, 404);
    } catch (error) {
      return errorResponse(error);
    }
  }

  async #accept(input: AcceptedFanout): Promise<unknown> {
    const fanout = await validateAcceptedFanout(input);
    const partitionCellKey = await deriveAttentionPartitionKey({
      applicationId: fanout.applicationId,
      tenantId: fanout.tenantId,
      channelId: fanout.event.source.channelId,
      installationId: fanout.event.source.installationId,
      partitionKey: fanout.partitionKey,
    });
    if (this.#cellName() !== partitionCellKey) {
      throw new TypeError("partition cell address does not match the accepted fan-out");
    }
    const now = this.#now();
    const limits = this.#limits();
    let record = (await this.#read()) ?? this.#emptyRecord();
    this.#purgeRecord(record, now);
    let coordinator = record.coordinators.find(
      (candidate) => candidate.eventKey === fanout.eventKey,
    );
    if (coordinator === undefined) {
      const candidate = structuredClone(record);
      coordinator = createEventCoordinator(fanout, {
        now,
        maxBranches: limits.maxBranches,
        maxFanoutBytes: limits.maxFanoutBytes,
      });
      candidate.coordinators.push(coordinator);
      this.#assertResidentCapacity(candidate, this.#recordEmpty(record) ? 413 : 429);
      await this.#write(candidate);
      await this.#armPartition(candidate);
      record = candidate;
    } else {
      validateEventCoordinatorRetry(coordinator, fanout);
    }
    if (coordinator.receipt !== undefined) {
      await this.#armPartition(record);
      return structuredClone(coordinator.receipt);
    }
    for (const pending of pendingCoordinatorBranches(coordinator)) {
      const branch = await validateFullAttentionBranch(pending);
      await this.#branchAppendHook("beforeBranchAppend", branch);
      const appended = structuredClone(record);
      await this.#appendBranch(appended, branch, limits);
      const appendedCoordinator = this.#coordinator(appended, fanout.eventKey);
      if (appendedCoordinator === undefined) {
        throw new Error("event coordinator disappeared during branch transfer");
      }
      markCoordinatorBranchAccepted(appendedCoordinator, branch.branchKey);
      this.#assertResidentCapacity(appended, 429);
      await this.#write(appended);
      await this.#armPartition(appended);
      record = appended;
      coordinator = appendedCoordinator;

      // A lost response here leaves branch custody and its coordinator marker
      // in one atomic cell checkpoint. The provider retry resumes the remaining
      // frozen branches without duplicating this member.
      await this.#branchAppendHook("afterBranchAppend", branch);
    }
    const completed = structuredClone(record);
    const completedCoordinator = this.#coordinator(completed, fanout.eventKey);
    if (completedCoordinator === undefined) {
      throw new Error("event coordinator disappeared before completion");
    }
    const receipt = completeEventCoordinator(completedCoordinator, {
      now: this.#now(),
      dedupeMs: limits.dedupeMs,
    });
    await this.#write(completed);
    await this.#armPartition(completed);
    return receipt;
  }

  async #appendBranch(
    record: CellRecord,
    branch: FullAttentionBranch,
    limits: Limits,
  ): Promise<void> {
    const partitionCellKey = await deriveAttentionPartitionKey({
      applicationId: branch.applicationId,
      tenantId: branch.tenantId,
      channelId: branch.event.source.channelId,
      installationId: branch.event.source.installationId,
      partitionKey: branch.partitionKey,
    });
    if (partitionCellKey !== this.#cellName()) {
      throw new TypeError("branch partition does not match its custody cell");
    }
    const instanceKey = await deriveAttentionInstanceKey({
      partitionCellKey,
      monitorId: branch.monitorId,
      definitionVersion: branch.definitionVersion,
      correlationKey: branch.correlationKey,
    });
    const policyHash = await hashIdempotencyInput({ mode: branch.mode, policy: branch.policy });
    let workflow = this.#workflow(record, instanceKey);
    if (workflow === undefined) {
      workflow = createAttentionWorkflow({ instanceKey, branch, policyHash });
      record.workflows.push(workflow);
    }
    appendAttentionBranch(workflow, branch, {
      now: this.#now(),
      dedupeMs: limits.dedupeMs,
      policyHash,
    });
  }

  async #handleAlarm(): Promise<void> {
    const limits = this.#limits();
    const claimed = await this.#withLock(async () => {
      const record = await this.#read();
      if (record === undefined) {
        await this.#deleteAlarm();
        return undefined;
      }
      const now = this.#now();
      this.#purgeRecord(record, now);
      let active:
        | {
            readonly instanceKey: AttentionInstanceKey;
            readonly claim: NonNullable<Awaited<ReturnType<typeof claimAttentionRun>>>;
          }
        | undefined;
      for (const workflow of [...record.workflows].sort((left, right) =>
        left.instanceKey.localeCompare(right.instanceKey),
      )) {
        const claim = await claimAttentionRun(workflow, {
          now,
          leaseMs: limits.claimLeaseMs,
        });
        if (claim !== undefined) {
          active = {
            instanceKey: workflow.instanceKey,
            claim: structuredClone(claim),
          };
          break;
        }
      }
      await this.#write(record);
      await this.#armPartition(record);
      return active;
    });
    if (claimed === undefined) return;
    const claim = claimed.claim;
    let failureStage = claim.stage;
    try {
      if (claim.stage === "preparing") {
        const prepared = (await this.#callback(
          this.#callbackUrl("prepare"),
          claim.batch,
        )) as PreparedAttentionOutcome;
        const transition = await this.#withLock(async () => {
          const record = await this.#read();
          if (record === undefined) return "stale";
          const workflow = this.#workflow(record, claimed.instanceKey);
          if (!isCurrentAttentionClaim(workflow, claim, "preparing")) return "stale";
          const result = await applyPreparedAttentionOutcome(workflow, prepared, {
            now: this.#now(),
            dedupeMs: limits.dedupeMs,
            maxPreparedWakeBytes: limits.maxPreparedWakeBytes,
          });
          await this.#write(record);
          await this.#armPartition(record);
          return result;
        });
        if (transition === "stale") return;
        if (transition !== "deliver") {
          this.#emitOutcome(transition);
          return;
        }
      }
      failureStage = "delivering";
      const wake = await this.#withLock(async () => {
        const record = await this.#read();
        const workflow = record === undefined
          ? undefined
          : this.#workflow(record, claimed.instanceKey);
        if (!isCurrentAttentionClaim(workflow, claim, "delivering")) return undefined;
        if (workflow.active?.wake === undefined) throw new Error("delivery stage has no prepared wake");
        return structuredClone(workflow.active.wake);
      });
      if (wake === undefined) return;
      const receipt = (await this.#callback(
        this.#callbackUrl("deliver"),
        wake,
      )) as AttentionDeliveryReceipt;
      const committed = await this.#withLock(async () => {
        const record = await this.#read();
        if (record === undefined) return false;
        const workflow = this.#workflow(record, claimed.instanceKey);
        if (!isCurrentAttentionClaim(workflow, claim, "delivering")) return false;
        applyAttentionDeliveryReceipt(workflow, receipt, {
          now: this.#now(),
          dedupeMs: limits.dedupeMs,
        });
        await this.#write(record);
        await this.#armPartition(record);
        return true;
      });
      if (committed) this.#emitOutcome("delivered");
    } catch (error) {
      if (error instanceof DurableCheckpointError) throw error.cause;
      const outcome = await this.#withLock(async () => {
        const record = await this.#read();
        if (record === undefined) return "none";
        const workflow = this.#workflow(record, claimed.instanceKey);
        if (!isCurrentAttentionClaim(workflow, claim, failureStage)) return "none";
        const result = failAttentionRun(workflow, error, {
          now: this.#now(),
          dedupeMs: limits.dedupeMs,
          retryDelayMs: limits.retryDelayMs,
          maxAttempts: limits.maxAttempts,
          terminalError: isTerminalError,
        });
        await this.#write(record);
        await this.#armPartition(record);
        return result;
      });
      if (outcome !== "none") this.#emitOutcome(outcome);
    }
  }

  #coordinator(
    record: CellRecord,
    eventKey: EventKey,
  ): EventCoordinatorState | undefined {
    return record.coordinators.find((candidate) => candidate.eventKey === eventKey);
  }

  #workflow(
    record: CellRecord,
    instanceKey: AttentionInstanceKey,
  ): AttentionWorkflowState | undefined {
    return record.workflows.find((candidate) => candidate.instanceKey === instanceKey);
  }

  async #callback(url: string, body: unknown): Promise<unknown> {
    const response = await this.#fetch(url, {
      method: "POST",
      headers: {
        authorization: `Bearer ${this.#secret()}`,
        "content-type": "application/json",
      },
      body: JSON.stringify(body),
    });
    const result = await safeJson(response);
    if (!response.ok) {
      throw new Error(
        typeof result.error === "string" ? result.error : `attention callback returned ${response.status}`,
      );
    }
    return result;
  }

  async #diagnostics(): Promise<unknown> {
    const record = await this.#read();
    if (record === undefined) return { kind: "empty" };
    const detached = structuredClone(record);
    this.#purgeRecord(detached, this.#now());
    return {
      kind: "partition",
      eventCoordinators: detached.coordinators.length,
      pendingFanoutPayloads: detached.coordinators.filter(
        (coordinator) => coordinator.pendingFanout !== undefined,
      ).length,
      acceptanceReceipts: detached.coordinators.filter(
        (coordinator) => coordinator.receipt !== undefined,
      ).length,
      correlationWorkflows: detached.workflows.length,
      bufferedBranches: detached.workflows.reduce(
        (count, workflow) => count +
          (workflow.open?.branches.length ?? 0) +
          workflow.sealed.reduce((sum, batch) => sum + batch.branches.length, 0),
        0,
      ),
      activeBatchBranches: detached.workflows.reduce(
        (count, workflow) => count + (workflow.active?.batch.branches.length ?? 0),
        0,
      ),
      preparedWakes: detached.workflows.filter(
        (workflow) => workflow.active?.wake !== undefined,
      ).length,
      branchReceipts: detached.workflows.reduce(
        (count, workflow) => count + workflow.branchLedger.length,
        0,
      ),
      deliveryReceipts: detached.workflows.reduce(
        (count, workflow) => count + workflow.deliveryReceipts.length,
        0,
      ),
      terminalFailures: detached.workflows.reduce(
        (count, workflow) => count + workflow.terminalFailures.length,
        0,
      ),
    };
  }

  #emptyRecord(): CellRecord {
    return { kind: "partition", coordinators: [], workflows: [] };
  }

  #recordEmpty(record: CellRecord): boolean {
    return record.coordinators.length === 0 && record.workflows.length === 0;
  }

  #purgeRecord(record: CellRecord, now: string): void {
    record.coordinators = record.coordinators.filter(
      (coordinator) => !eventCoordinatorExpired(coordinator, now),
    );
    record.workflows = record.workflows.filter(
      (workflow) => purgeAttentionWorkflow(workflow, now) !== "empty",
    );
  }

  #assertResidentCapacity(record: CellRecord, status: 413 | 429): void {
    if (attentionValueBytes(record) <= this.#limits().maxResidentBytes) return;
    throw new CelldResidentCapacityError(
      status === 413
        ? "accepted fan-out can never fit in the configured partition capacity"
        : "partition resident capacity is exhausted",
      status,
    );
  }

  async #armPartition(record: CellRecord): Promise<void> {
    if (this.#recordEmpty(record)) {
      await this.#delete();
      return;
    }
    const due = [
      ...record.coordinators.flatMap((coordinator) =>
        coordinator.dedupeExpiresAt === undefined ? [] : [coordinator.dedupeExpiresAt],
      ),
      ...record.workflows.flatMap((workflow) => {
        const workflowDue = nextAttentionDueAt(workflow);
        return workflowDue === undefined ? [] : [workflowDue];
      }),
    ].map(Date.parse);
    if (due.length === 0) {
      await this.#deleteAlarm();
      return;
    }
    await this.#setAlarm(Math.max(Math.min(...due), Date.parse(this.#now())));
  }

  async #branchAppendHook(
    name: "beforeBranchAppend" | "afterBranchAppend",
    branch: FullAttentionBranch,
  ): Promise<void> {
    const hook = this.env?.[name];
    if (typeof hook === "function") await hook(structuredClone(branch));
  }

  get #storage(): Storage {
    return this.state.storage as Storage;
  }

  async #read(): Promise<CellRecord | undefined> {
    let raw: unknown;
    try {
      raw = await this.#storage.get("record");
    } catch (error) {
      throw new DurableCheckpointError(error);
    }
    if (raw === undefined || raw === null) return undefined;
    return JSON.parse(String(raw)) as CellRecord;
  }

  async #write(record: CellRecord): Promise<void> {
    try {
      await this.#storage.put("record", JSON.stringify(record));
    } catch (error) {
      throw new DurableCheckpointError(error);
    }
  }

  async #delete(): Promise<void> {
    try {
      await this.#storage.delete("record");
      await this.#storage.deleteAlarm();
    } catch (error) {
      throw new DurableCheckpointError(error);
    }
  }

  async #setAlarm(at: number): Promise<void> {
    try {
      await this.#storage.setAlarm(at);
    } catch (error) {
      throw new DurableCheckpointError(error);
    }
  }

  async #deleteAlarm(): Promise<void> {
    try {
      await this.#storage.deleteAlarm();
    } catch (error) {
      throw new DurableCheckpointError(error);
    }
  }

  #cellName(): string {
    if (this.#requestCellName !== undefined) return this.#requestCellName;
    const value = this.state.id?.name;
    if (typeof value === "string" && value.length > 0) return value;
    throw new Error("celld did not provide the cell name");
  }

  #now(): string {
    const injected = this.env?.clock;
    return typeof injected?.now === "function"
      ? (injected.now() as Date).toISOString()
      : new Date().toISOString();
  }

  #fetch(input: string, init: RequestInit): Promise<Response> {
    const injected = this.env?.fetch;
    return typeof injected === "function"
      ? (injected(input, init) as Promise<Response>)
      : fetch(input, init);
  }

  #secret(): string {
    return nonEmpty(this.env?.ATTENTION_SECRET, "ATTENTION_SECRET");
  }

  #requiredUrl(name: string): string {
    return absoluteUrl(this.env?.[name], name);
  }

  #callbackUrl(kind: "prepare" | "deliver"): string {
    if (this.env?.ATTENTION_CALLBACK_URL !== undefined) {
      return `${absoluteUrl(this.env.ATTENTION_CALLBACK_URL, "ATTENTION_CALLBACK_URL")}/${kind}`;
    }
    return this.#requiredUrl(
      kind === "prepare" ? "ATTENTION_PREPARE_URL" : "ATTENTION_DELIVER_URL",
    );
  }

  #limits(): Limits {
    return {
      dedupeMs: positiveInteger(this.env?.ATTENTION_DEDUPE_MS, DEFAULT_DEDUPE_MS, "ATTENTION_DEDUPE_MS"),
      retryDelayMs: positiveInteger(this.env?.ATTENTION_RETRY_DELAY_MS, DEFAULT_RETRY_DELAY_MS, "ATTENTION_RETRY_DELAY_MS"),
      claimLeaseMs: positiveInteger(this.env?.ATTENTION_CLAIM_LEASE_MS, DEFAULT_CLAIM_LEASE_MS, "ATTENTION_CLAIM_LEASE_MS"),
      maxAttempts: positiveInteger(this.env?.ATTENTION_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS, "ATTENTION_MAX_ATTEMPTS"),
      maxBranches: positiveInteger(this.env?.ATTENTION_MAX_BRANCHES, DEFAULT_MAX_BRANCHES, "ATTENTION_MAX_BRANCHES"),
      maxFanoutBytes: positiveInteger(this.env?.ATTENTION_MAX_FANOUT_BYTES, DEFAULT_MAX_FANOUT_BYTES, "ATTENTION_MAX_FANOUT_BYTES"),
      maxPreparedWakeBytes: positiveInteger(this.env?.ATTENTION_MAX_PREPARED_WAKE_BYTES, DEFAULT_MAX_PREPARED_WAKE_BYTES, "ATTENTION_MAX_PREPARED_WAKE_BYTES"),
      maxResidentBytes: positiveInteger(this.env?.ATTENTION_MAX_RESIDENT_BYTES, DEFAULT_MAX_RESIDENT_BYTES, "ATTENTION_MAX_RESIDENT_BYTES"),
    };
  }

  #emitOutcome(outcome: string): void {
    const callback = this.env?.onOutcome;
    if (typeof callback === "function") callback(this.#cellName(), outcome);
  }

  async #withLock<T>(callback: () => Promise<T>): Promise<T> {
    const previous = this.#mutex;
    let release!: () => void;
    this.#mutex = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await callback();
    } finally {
      release();
    }
  }
}

class DurableCheckpointError extends Error {
  readonly cause: unknown;

  constructor(cause: unknown) {
    super("celld attention checkpoint failed", { cause });
    this.cause = cause;
  }
}

class CelldResidentCapacityError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

function isTerminalError(error: unknown): boolean {
  return (
    error instanceof AttentionCapacityError ||
    error instanceof IdempotencyConflictError ||
    error instanceof AttentionCallbackValidationError
  );
}

function errorResponse(error: unknown): Response {
  if (error instanceof IdempotencyConflictError) {
    return json(
      {
        error: error.message,
        namespace: error.namespace,
        key: error.key,
        existingInputHash: error.existingInputHash,
        receivedInputHash: error.receivedInputHash,
      },
      409,
    );
  }
  if (error instanceof AttentionCapacityError) return json({ error: error.message }, 413);
  if (error instanceof CelldResidentCapacityError) {
    return json({ error: error.message }, error.status);
  }
  if (error instanceof TypeError) return json({ error: error.message }, 400);
  return json({ error: message(error) }, 503);
}

function positiveInteger(value: unknown, fallback: number, name: string): number {
  const parsed = value === undefined ? fallback : Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  return parsed;
}

function nonEmpty(value: unknown, name: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${name} must not be empty`);
  }
  return value;
}

function absoluteUrl(value: unknown, name: string): string {
  try {
    return new URL(nonEmpty(value, name)).toString();
  } catch (error) {
    if (error instanceof TypeError && error.message.endsWith("must not be empty")) throw error;
    throw new TypeError(`${name} must be an absolute URL`);
  }
}

async function safeJson(response: Response): Promise<Record<string, unknown>> {
  try {
    const value = (await response.json()) as unknown;
    return value !== null && typeof value === "object"
      ? (value as Record<string, unknown>)
      : { error: "response body must be an object" };
  } catch {
    return { error: "response body is not valid JSON" };
  }
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export default {
  async fetch(request: Request, env: any): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/health") return json({ ok: true, app: "eve-ambient-attention" });
    const parts = url.pathname.split("/").filter(Boolean);
    if (parts[0] !== "cells" || parts.length !== 3) {
      return json({ error: "use /cells/<partition-key>/<accept|diagnostics|whoami>" }, 404);
    }
    let secret: string;
    try {
      secret = nonEmpty(env?.ATTENTION_SECRET, "ATTENTION_SECRET");
    } catch (error) {
      return json({ error: message(error) }, 503);
    }
    const presented = /^Bearer[ ]+(.+)$/i.exec(
      request.headers.get("authorization")?.trim() ?? "",
    )?.[1] ?? "";
    if (!secretsMatch(presented, secret)) return json({ error: "unauthorized" }, 401);
    const name = decodeURIComponent(parts[1]!);
    const action = parts[2]!;
    const id = env.ATTENTION.idFromName(name);
    const stub = env.ATTENTION.get(id);
    const forwarded = new Request(new URL(`/${action}`, url).toString(), request);
    forwarded.headers.set("x-cell-name", name);
    const response = await stub.fetch(forwarded);
    const output = new Response(response.body, response);
    output.headers.set("x-cell-name", name);
    return output;
  },
};
