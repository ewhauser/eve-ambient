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
  deriveAttentionInstanceKey,
  hashIdempotencyInput,
  IdempotencyConflictError,
} from "./idempotency.js";
import { cellUrl, secretsMatch } from "./celld.js";
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

type CellRecord =
  | { readonly kind: "coordinator"; readonly coordinator: EventCoordinatorState }
  | { readonly kind: "workflow"; readonly workflow: AttentionWorkflowState };

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

/** One celld Durable Object class for event coordinators and correlation workflows. */
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
      if (action === "append") return json(await this.#append(body as FullAttentionBranch));
      return json({ error: "not found" }, 404);
    } catch (error) {
      return errorResponse(error);
    }
  }

  async #accept(input: AcceptedFanout): Promise<unknown> {
    const fanout = await validateAcceptedFanout(input);
    const cellName = this.#cellName();
    if (cellName !== fanout.eventKey) {
      throw new TypeError("event coordinator address does not match eventKey");
    }
    const now = this.#now();
    const limits = this.#limits();
    let record = await this.#read();
    if (record?.kind === "workflow") throw new TypeError("cell is already a correlation workflow");
    if (record !== undefined && eventCoordinatorExpired(record.coordinator, now)) {
      await this.#delete();
      record = undefined;
    }
    let coordinator: EventCoordinatorState;
    if (record === undefined) {
      coordinator = createEventCoordinator(fanout, {
        now,
        maxBranches: limits.maxBranches,
        maxFanoutBytes: limits.maxFanoutBytes,
      });
      await this.#write({ kind: "coordinator", coordinator });
    } else {
      coordinator = record.coordinator;
      validateEventCoordinatorRetry(coordinator, fanout);
    }
    if (coordinator.receipt !== undefined) return coordinator.receipt;
    for (const branch of pendingCoordinatorBranches(coordinator)) {
      await this.#appendToCorrelationWorkflow(branch);
      markCoordinatorBranchAccepted(coordinator, branch.branchKey);
      await this.#write({ kind: "coordinator", coordinator });
    }
    const receipt = completeEventCoordinator(coordinator, {
      now: this.#now(),
      dedupeMs: limits.dedupeMs,
    });
    await this.#write({ kind: "coordinator", coordinator });
    await this.#setAlarm(Date.parse(receipt.dedupeExpiresAt));
    return receipt;
  }

  async #append(input: FullAttentionBranch): Promise<unknown> {
    const branch = await validateFullAttentionBranch(input);
    const instanceKey = await deriveAttentionInstanceKey({
      applicationId: branch.applicationId,
      tenantId: branch.tenantId,
      monitorId: branch.monitorId,
      definitionVersion: branch.definitionVersion,
      correlationKey: branch.correlationKey,
    });
    if (this.#cellName() !== instanceKey) {
      throw new TypeError("correlation workflow address does not match instanceKey");
    }
    const limits = this.#limits();
    const policyHash = await hashIdempotencyInput({ mode: branch.mode, policy: branch.policy });
    const existing = await this.#read();
    if (existing?.kind === "coordinator") throw new TypeError("cell is already an event coordinator");
    const workflow =
      existing?.workflow ?? createAttentionWorkflow({ instanceKey, branch, policyHash });
    const outcome = appendAttentionBranch(workflow, branch, {
      now: this.#now(),
      dedupeMs: limits.dedupeMs,
      policyHash,
    });
    if (attentionValueBytes(workflow) > limits.maxResidentBytes) {
      throw new CelldResidentCapacityError(
        existing === undefined
          ? "attention branch can never fit in the configured cell capacity"
          : "correlation workflow resident capacity is exhausted",
        existing === undefined ? 413 : 429,
      );
    }
    if (outcome === "appended" || existing === undefined) {
      await this.#write({ kind: "workflow", workflow });
      await this.#armWorkflow(workflow);
    }
    return {
      branchKey: branch.branchKey,
      inputHash: branch.inputHash,
      acceptedAt: this.#now(),
    };
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
      if (record.kind === "coordinator") {
        if (eventCoordinatorExpired(record.coordinator, now)) await this.#delete();
        else if (record.coordinator.dedupeExpiresAt !== undefined) {
          await this.#setAlarm(Date.parse(record.coordinator.dedupeExpiresAt));
        }
        return undefined;
      }
      const workflow = record.workflow;
      if (purgeAttentionWorkflow(workflow, now) === "empty") {
        await this.#delete();
        return undefined;
      }
      const active = await claimAttentionRun(workflow, {
        now,
        leaseMs: limits.claimLeaseMs,
      });
      await this.#write({ kind: "workflow", workflow });
      await this.#armWorkflow(workflow);
      return active === undefined ? undefined : structuredClone(active);
    });
    if (claimed === undefined) return;
    let failureStage = claimed.stage;
    try {
      if (claimed.stage === "preparing") {
        const prepared = (await this.#callback(
          this.#requiredUrl("ATTENTION_PREPARE_URL"),
          claimed.batch,
        )) as PreparedAttentionOutcome;
        const transition = await this.#withLock(async () => {
          const workflow = await this.#workflow();
          if (!isCurrentAttentionClaim(workflow, claimed, "preparing")) return "stale";
          const result = await applyPreparedAttentionOutcome(workflow, prepared, {
            now: this.#now(),
            dedupeMs: limits.dedupeMs,
            maxPreparedWakeBytes: limits.maxPreparedWakeBytes,
          });
          await this.#write({ kind: "workflow", workflow });
          await this.#armWorkflow(workflow);
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
        const workflow = await this.#workflow();
        if (!isCurrentAttentionClaim(workflow, claimed, "delivering")) return undefined;
        if (workflow.active?.wake === undefined) throw new Error("delivery stage has no prepared wake");
        return structuredClone(workflow.active.wake);
      });
      if (wake === undefined) return;
      const receipt = (await this.#callback(
        this.#requiredUrl("ATTENTION_DELIVER_URL"),
        wake,
      )) as AttentionDeliveryReceipt;
      const committed = await this.#withLock(async () => {
        const workflow = await this.#workflow();
        if (!isCurrentAttentionClaim(workflow, claimed, "delivering")) return false;
        applyAttentionDeliveryReceipt(workflow, receipt, {
          now: this.#now(),
          dedupeMs: limits.dedupeMs,
        });
        await this.#write({ kind: "workflow", workflow });
        await this.#armWorkflow(workflow);
        return true;
      });
      if (committed) this.#emitOutcome("delivered");
    } catch (error) {
      if (error instanceof DurableCheckpointError) throw error.cause;
      const outcome = await this.#withLock(async () => {
        const workflow = await this.#workflow();
        if (!isCurrentAttentionClaim(workflow, claimed, failureStage)) return "none";
        const result = failAttentionRun(workflow, error, {
          now: this.#now(),
          dedupeMs: limits.dedupeMs,
          retryDelayMs: limits.retryDelayMs,
          maxAttempts: limits.maxAttempts,
          terminalError: isTerminalError,
        });
        await this.#write({ kind: "workflow", workflow });
        await this.#armWorkflow(workflow);
        return result;
      });
      if (outcome !== "none") this.#emitOutcome(outcome);
    }
  }

  async #workflow(): Promise<AttentionWorkflowState | undefined> {
    const record = await this.#read();
    return record?.kind === "workflow" ? record.workflow : undefined;
  }

  async #appendToCorrelationWorkflow(branch: FullAttentionBranch): Promise<void> {
    const instanceKey = await deriveAttentionInstanceKey({
      applicationId: branch.applicationId,
      tenantId: branch.tenantId,
      monitorId: branch.monitorId,
      definitionVersion: branch.definitionVersion,
      correlationKey: branch.correlationKey,
    });
    const response = await this.#fetch(cellUrl(this.#requiredUrl("CELLD_FLEET_URL"), instanceKey, "append"), {
      method: "POST",
      headers: {
        authorization: `Bearer ${this.#secret()}`,
        "content-type": "application/json",
      },
      body: JSON.stringify(branch),
    });
    if (!response.ok) {
      const detail = await safeJson(response);
      throw new CelldHandoffError(
        typeof detail.error === "string" ? detail.error : `branch append returned ${response.status}`,
        response.status,
      );
    }
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
    if (record.kind === "coordinator") {
      return {
        kind: "coordinator",
        pendingFanout: record.coordinator.fanout !== undefined,
        acceptedBranches: record.coordinator.acceptedBranchKeys.length,
        complete: record.coordinator.receipt !== undefined,
      };
    }
    return {
      kind: "workflow",
      bufferedBranches:
        (record.workflow.open?.branches.length ?? 0) +
        record.workflow.sealed.reduce((sum, batch) => sum + batch.branches.length, 0),
      activeBatchBranches: record.workflow.active?.batch.branches.length ?? 0,
      preparedWake: record.workflow.active?.wake !== undefined,
      branchReceipts: record.workflow.branchLedger.length,
      deliveryReceipts: record.workflow.deliveryReceipts.length,
      terminalFailures: record.workflow.terminalFailures.length,
    };
  }

  async #armWorkflow(workflow: AttentionWorkflowState): Promise<void> {
    const due = nextAttentionDueAt(workflow);
    if (due === undefined) await this.#deleteAlarm();
    else await this.#setAlarm(Math.max(Date.parse(due), Date.parse(this.#now())));
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

class CelldHandoffError extends Error {
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
  if (error instanceof CelldResidentCapacityError || error instanceof CelldHandoffError) {
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
      return json({ error: "use /cells/<key>/<accept|append|diagnostics|whoami>" }, 404);
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
