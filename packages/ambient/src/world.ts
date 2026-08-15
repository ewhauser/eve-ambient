import {
  AttentionCapacityError,
  attentionValueBytes,
  validateAcceptedFanout,
  type AcceptedFanout,
  type AttentionAcceptanceReceipt,
  type AttentionCallbacks,
  type AttentionEngine,
  type FrozenAttentionBatch,
  type PreparedAttentionWake,
} from "./attention.js";
import type {
  AmbientApplicationBackend,
  AmbientBackendBinding,
} from "./application.js";
import { IdempotencyConflictError } from "./idempotency.js";
import type { MonitorClock } from "./types.js";
import {
  eventAdmissionWorkflow,
} from "./world-workflows.js";
import type {
  AdmissionStreamReceipt,
  CallbackEnvelope,
  CallbackValue,
  EventAdmissionCommand,
  WorldAttentionConfig,
  WorldAttentionFailure,
} from "./world-protocol.js";
import { ADMISSION_STREAM, eventAdmissionToken } from "./world-protocol.js";
import { getHookByToken, getRun, resumeHook, start } from "workflow/api";

const DEFAULT_DEDUPE_MS = 7 * 24 * 60 * 60 * 1_000;
const DEFAULT_RETRY_DELAY_MS = 1_000;
const DEFAULT_CLAIM_LEASE_MS = 30_000;
const DEFAULT_MAX_ATTEMPTS = 10;
const DEFAULT_MAX_BRANCHES = 1_000;
const DEFAULT_MAX_FANOUT_BYTES = 16 * 1_024 * 1_024;
const DEFAULT_MAX_PREPARED_WAKE_BYTES = 1 * 1_024 * 1_024;
const DEFAULT_CALLBACK_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_CALLBACK_REQUEST_BYTES = 16 * 1_024 * 1_024;
const DEFAULT_REGISTRATION_TIMEOUT_MS = 10_000;
const DEFAULT_RECEIPT_TIMEOUT_MS = 30_000;

export interface WorldAttentionEngineOptions {
  /** Namespaces deterministic event and correlation stream addresses. */
  readonly engineId?: string | undefined;
  /** Absolute application URL used by durable callback steps. */
  readonly callbackUrl: string;
  /** Environment variable containing the shared callback bearer secret. */
  readonly callbackSecretEnv?: string | undefined;
  readonly preparePath?: string | undefined;
  readonly deliverPath?: string | undefined;
  readonly clock?: MonitorClock | undefined;
  readonly dedupeMs?: number | undefined;
  readonly retryDelayMs?: number | undefined;
  readonly claimLeaseMs?: number | undefined;
  readonly maxAttempts?: number | undefined;
  readonly maxBranches?: number | undefined;
  readonly maxFanoutBytes?: number | undefined;
  readonly maxPreparedWakeBytes?: number | undefined;
  readonly callbackTimeoutMs?: number | undefined;
  readonly maxCallbackRequestBytes?: number | undefined;
  readonly registrationTimeoutMs?: number | undefined;
  readonly receiptTimeoutMs?: number | undefined;
}

export interface WorldAmbientBinding extends AmbientBackendBinding {
  readonly engine: WorldAttentionEngine;
  readonly fetch: (request: Request) => Promise<Response>;
}

/**
 * Binds Ambient to the process-global Workflow World.
 *
 * The host configures the World (Postgres, world-celld, or another conforming
 * implementation) through the Workflow runtime. Ambient never selects or
 * composes storage backends itself.
 */
export function world(
  options: WorldAttentionEngineOptions,
): AmbientApplicationBackend<WorldAmbientBinding> {
  const config = normalizeOptions(options);
  return Object.freeze({
    ...(options.clock === undefined ? {} : { clock: options.clock }),
    bind(callbacks: AttentionCallbacks) {
      return Object.freeze({
        engine: new WorldAttentionEngine(options),
        fetch: createWorldAttentionCallbackHandler(callbacks, {
          secretEnv: config.callbackSecretEnv,
          preparePath: config.preparePath,
          deliverPath: config.deliverPath,
          maxRequestBytes: config.limits.maxCallbackRequestBytes,
          ...(options.clock === undefined ? {} : { clock: options.clock }),
        }),
      });
    },
  });
}

export class WorldAttentionEngine implements AttentionEngine {
  readonly #config: WorldAttentionConfig;
  readonly #clock: MonitorClock;

  constructor(options: WorldAttentionEngineOptions) {
    this.#config = normalizeOptions(options);
    this.#clock = options.clock ?? { now: () => new Date() };
  }

  async accept(input: AcceptedFanout): Promise<AttentionAcceptanceReceipt> {
    const fanout = await validateAcceptedFanout(input);
    if (fanout.branches.length > this.#config.limits.maxBranches) {
      throw new AttentionCapacityError(
        `accepted fan-out exceeds the maximum of ${this.#config.limits.maxBranches} branches`,
      );
    }
    if (attentionValueBytes(fanout) > this.#config.limits.maxFanoutBytes) {
      throw new AttentionCapacityError(
        `accepted fan-out exceeds the maximum of ${this.#config.limits.maxFanoutBytes} bytes`,
      );
    }

    const command: EventAdmissionCommand = {
      attemptId: attemptId(),
      acceptedAt: this.#clock.now().toISOString(),
      fanout,
    };
    const receipt = await this.#submit(command);
    if (receipt.kind === "accepted") return receipt.receipt;
    throwFailure(receipt.failure);
  }

  async #submit(command: EventAdmissionCommand): Promise<AdmissionStreamReceipt> {
    const token = eventAdmissionToken(this.#config.engineId, command.fanout.eventKey);
    for (let raceAttempt = 0; raceAttempt < 3; raceAttempt += 1) {
      let owner = await findHook(token);
      let startIndex = 0;
      let candidateRunId: string | undefined;
      if (owner === undefined) {
        const candidate = await start(eventAdmissionWorkflow, [this.#config, command]);
        candidateRunId = candidate.runId;
        owner = await waitForHook(
          token,
          this.#config.limits.registrationTimeoutMs,
        );
      } else {
        startIndex = await admissionTail(owner.runId);
      }

      try {
        if (candidateRunId !== undefined && owner.runId !== candidateRunId) {
          startIndex = await admissionTail(owner.runId);
        }
        // Wake newly registered owners as well as existing ones. The workflow
        // freezes membership and deduplicates the repeated attempt itself.
        await resumeHook(token, { kind: "admit", command });
        const readable = getRun(owner.runId).getReadable({
          namespace: ADMISSION_STREAM,
          startIndex,
        }) as ReadableStream<AdmissionStreamReceipt>;
        return await waitForReceipt(
          readable,
          command.attemptId,
          this.#config.limits.receiptTimeoutMs,
        );
      } catch (error) {
        if (!isNotFound(error) || raceAttempt === 2) throw error;
      }
    }
    throw new Error("unreachable World admission retry state");
  }
}

export class WorldAttentionError extends Error {
  readonly retryable: boolean;

  constructor(message: string, retryable = true) {
    super(message);
    this.name = "WorldAttentionError";
    this.retryable = retryable;
  }
}

export interface WorldAttentionCallbackHandlerOptions {
  readonly secretEnv?: string | undefined;
  readonly preparePath?: string | undefined;
  readonly deliverPath?: string | undefined;
  readonly maxRequestBytes?: number | undefined;
  readonly clock?: MonitorClock | undefined;
}

/** Authenticated application endpoint called by durable World workflow steps. */
export function createWorldAttentionCallbackHandler(
  callbacks: AttentionCallbacks,
  options: WorldAttentionCallbackHandlerOptions = {},
): (request: Request) => Promise<Response> {
  if (
    callbacks === null ||
    typeof callbacks !== "object" ||
    typeof callbacks.prepare !== "function" ||
    typeof callbacks.deliver !== "function"
  ) {
    throw new TypeError("attention callbacks must define prepare and deliver");
  }
  const secretEnv = environmentName(options.secretEnv ?? "AMBIENT_CALLBACK_SECRET");
  const preparePath = pathName(options.preparePath ?? "/ambient/prepare", "preparePath");
  const deliverPath = pathName(options.deliverPath ?? "/ambient/deliver", "deliverPath");
  const maxRequestBytes = positiveInteger(
    options.maxRequestBytes ?? DEFAULT_MAX_CALLBACK_REQUEST_BYTES,
    "maxRequestBytes",
  );
  const clock = options.clock ?? { now: () => new Date() };

  return async (request) => {
    if (request.method !== "POST") return json({ error: "method not allowed" }, 405);
    const secret = process.env[secretEnv];
    if (secret === undefined || secret.length === 0) {
      return json({ error: `callback secret environment variable ${secretEnv} is not set` }, 503);
    }
    if (!secretsMatch(bearerToken(request), secret)) return json({ error: "unauthorized" }, 401);
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
      let value: CallbackValue;
      if (path === preparePath) {
        value = await callbacks.prepare(deepFreeze(body as FrozenAttentionBatch));
      } else {
        value = await callbacks.deliver(deepFreeze(body as PreparedAttentionWake));
      }
      return callbackJson({ ok: true, completedAt: clock.now().toISOString(), value });
    } catch (error) {
      return callbackJson(
        {
          ok: false,
          completedAt: clock.now().toISOString(),
          error: message(error),
          terminal: error instanceof IdempotencyConflictError || error instanceof AttentionCapacityError,
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

async function admissionTail(runId: string): Promise<number> {
  return getRun(runId).getReadable({ namespace: ADMISSION_STREAM }).getTailIndex();
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
    if (Date.now() >= deadline) {
      throw new WorldAttentionError(`timed out waiting for Workflow hook ${token}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}

async function waitForReceipt(
  readable: ReadableStream<AdmissionStreamReceipt>,
  expectedAttemptId: string,
  timeoutMs: number,
): Promise<AdmissionStreamReceipt> {
  const reader = readable.getReader();
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    const deadline = new Promise<never>((_, reject) => {
      timeout = setTimeout(
        () => reject(new WorldAttentionError("timed out waiting for World admission receipt")),
        timeoutMs,
      );
    });
    for (;;) {
      const result = await Promise.race([reader.read(), deadline]);
      if (result.done) {
        throw new WorldAttentionError("World admission receipt stream closed unexpectedly");
      }
      if (result.value.attemptId === expectedAttemptId) return result.value;
    }
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
    reader.releaseLock();
    await readable.cancel().catch(() => undefined);
  }
}

function normalizeOptions(options: WorldAttentionEngineOptions): WorldAttentionConfig {
  if (options === null || typeof options !== "object") {
    throw new TypeError("World attention engine options are required");
  }
  return Object.freeze({
    engineId: identifier(options.engineId ?? "default", "engineId"),
    callbackUrl: absoluteUrl(options.callbackUrl, "callbackUrl"),
    callbackSecretEnv: environmentName(
      options.callbackSecretEnv ?? "AMBIENT_CALLBACK_SECRET",
    ),
    preparePath: pathName(options.preparePath ?? "/ambient/prepare", "preparePath"),
    deliverPath: pathName(options.deliverPath ?? "/ambient/deliver", "deliverPath"),
    limits: Object.freeze({
      dedupeMs: positiveInteger(options.dedupeMs ?? DEFAULT_DEDUPE_MS, "dedupeMs"),
      retryDelayMs: positiveInteger(
        options.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS,
        "retryDelayMs",
      ),
      claimLeaseMs: positiveInteger(
        options.claimLeaseMs ?? DEFAULT_CLAIM_LEASE_MS,
        "claimLeaseMs",
      ),
      maxAttempts: positiveInteger(
        options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS,
        "maxAttempts",
      ),
      maxBranches: positiveInteger(
        options.maxBranches ?? DEFAULT_MAX_BRANCHES,
        "maxBranches",
      ),
      maxFanoutBytes: positiveInteger(
        options.maxFanoutBytes ?? DEFAULT_MAX_FANOUT_BYTES,
        "maxFanoutBytes",
      ),
      maxPreparedWakeBytes: positiveInteger(
        options.maxPreparedWakeBytes ?? DEFAULT_MAX_PREPARED_WAKE_BYTES,
        "maxPreparedWakeBytes",
      ),
      callbackTimeoutMs: positiveInteger(
        options.callbackTimeoutMs ?? DEFAULT_CALLBACK_TIMEOUT_MS,
        "callbackTimeoutMs",
      ),
      maxCallbackRequestBytes: positiveInteger(
        options.maxCallbackRequestBytes ?? DEFAULT_MAX_CALLBACK_REQUEST_BYTES,
        "maxCallbackRequestBytes",
      ),
      registrationTimeoutMs: positiveInteger(
        options.registrationTimeoutMs ?? DEFAULT_REGISTRATION_TIMEOUT_MS,
        "registrationTimeoutMs",
      ),
      receiptTimeoutMs: positiveInteger(
        options.receiptTimeoutMs ?? DEFAULT_RECEIPT_TIMEOUT_MS,
        "receiptTimeoutMs",
      ),
    }),
  });
}

function throwFailure(failure: WorldAttentionFailure): never {
  if (failure.kind === "conflict") throw new IdempotencyConflictError(failure.conflict);
  if (failure.kind === "capacity") throw new AttentionCapacityError(failure.message);
  throw new WorldAttentionError(failure.message, failure.retryable);
}

function callbackJson(body: CallbackEnvelope, status = 200): Response {
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

function identifier(value: string, name: string): string {
  const normalized = nonEmpty(value, name);
  if (normalized.length > 64 || !/^[A-Za-z0-9._-]+$/.test(normalized)) {
    throw new TypeError(`${name} must be at most 64 URL-safe identifier characters`);
  }
  return normalized;
}

function environmentName(value: string): string {
  const normalized = nonEmpty(value, "callbackSecretEnv");
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(normalized)) {
    throw new TypeError("callbackSecretEnv must be an environment variable name");
  }
  return normalized;
}

function absoluteUrl(value: string, name: string): string {
  const normalized = nonEmpty(value, name);
  try {
    return new URL(normalized).toString().replace(/\/$/, "");
  } catch {
    throw new TypeError(`${name} must be an absolute URL`);
  }
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

function attemptId(): string {
  return globalThis.crypto.randomUUID();
}

function isNotFound(error: unknown): boolean {
  return error instanceof Error && (error.name.includes("NotFound") || /not found/i.test(error.message));
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
