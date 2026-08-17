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
import { compileAttentionStreamAppends } from "./stream-protocol.js";
import { attentionStreamAppendFits } from "./stream-state.js";
import type { MonitorClock } from "./types.js";
import {
  correlationToken,
  type CorrelationAppendCommand,
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
const DEFAULT_MAX_CALLBACK_REQUEST_BYTES = 16 * 1_024 * 1_024;
const DEFAULT_REGISTRATION_TIMEOUT_MS = 10_000;
const REGISTRATION_POLL_INITIAL_DELAY_MS = 5;
const REGISTRATION_POLL_MAX_DELAY_MS = 50;

type WorkflowHook = Awaited<ReturnType<typeof getHookByToken>>;

interface CorrelationProbeResult {
  readonly owner: WorkflowHook;
  readonly leaderCommandAccepted: boolean;
}

interface CorrelationProbeAttempt {
  readonly leader: boolean;
  readonly result: Promise<CorrelationProbeResult>;
}

/** Process-local collapse of the initial probe and any cold start for one hook token. */
const correlationProbes = new Map<string, Promise<CorrelationProbeResult>>();

export interface WorkflowAttentionEngineOptions {
  /** Public base URL at which this application's callback handler is mounted. */
  readonly callbackUrl: string;
  /** Optional isolation prefix when multiple deployments share one World. */
  readonly namespace?: string | undefined;
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
  readonly #clock: MonitorClock;
  readonly #registrationTimeoutMs: number;
  readonly #maxBranches: number;
  readonly #maxFanoutBytes: number;

  constructor(options: WorkflowAttentionEngineOptions) {
    if (options === null || typeof options !== "object") {
      throw new TypeError("Workflow attention engine options are required");
    }
    this.#config = {
      namespace: nonEmpty(options.namespace ?? "default", "namespace"),
      callbackUrl: callbackBaseUrl(options.callbackUrl),
      callbackSecretEnv: environmentName(
        options.callbackSecretEnv ?? "AMBIENT_CALLBACK_SECRET",
      ),
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
  }

  async accept(input: AcceptedFanout): Promise<AttentionAcceptanceReceipt> {
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
    await Promise.all(appends.map((append) => this.#publish({
      kind: "append",
      append,
      acceptedAt,
    })));
    return Object.freeze({
      eventKey: fanout.eventKey,
      occurrenceKey: fanout.occurrenceKey,
      inputHash: fanout.inputHash,
      branchKeys: Object.freeze(fanout.branches.map((branch) => branch.branchKey)),
      acceptedAt,
    });
  }

  async #publish(command: CorrelationAppendCommand): Promise<void> {
    const token = await correlationToken(this.#config, command.append.streamKey);
    let probe = this.#probeCorrelation(token, command);
    for (let attempt = 0; attempt < 4; attempt += 1) {
      const probed = await probe.result;
      if (probe.leader && probed.leaderCommandAccepted) return;
      try {
        await resumeHook(probed.owner, command);
        return;
      } catch (error) {
        if (!isNotFound(error)) throw error;
      }
      probe = this.#probeCorrelation(token, command);
    }
    throw new Error(`could not publish append to correlation hook ${token}`);
  }

  #probeCorrelation(
    token: string,
    command: CorrelationAppendCommand,
  ): CorrelationProbeAttempt {
    const existing = correlationProbes.get(token);
    if (existing !== undefined) return { leader: false, result: existing };

    const result = this.#probeOrStartCorrelation(token, command);
    correlationProbes.set(token, result);
    const clear = (): void => {
      if (correlationProbes.get(token) === result) {
        correlationProbes.delete(token);
      }
    };
    void result.then(clear, clear);
    return { leader: true, result };
  }

  async #probeOrStartCorrelation(
    token: string,
    command: CorrelationAppendCommand,
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
      command.append.streamKey,
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

export interface WorkflowAttentionCallbackHandlerOptions {
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

/** Handles authenticated by-value prepare and deliver Workflow steps. */
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
  const secretEnv = environmentName(options.secretEnv ?? "AMBIENT_CALLBACK_SECRET");
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
