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
import {
  compileAttentionStreamAppends,
  validateAttentionStreamAppendReceipt,
  type AttentionWorld,
} from "./stream-protocol.js";
import type { MonitorClock } from "./types.js";

const DEFAULT_MAX_BRANCHES = 1_000;
const DEFAULT_MAX_FANOUT_BYTES = 16 * 1_024 * 1_024;
const DEFAULT_MAX_CALLBACK_REQUEST_BYTES = 16 * 1_024 * 1_024;

export interface WorldAttentionEngineOptions {
  /** Correlation-addressed World. Resolving a stream handle must be local. */
  readonly world: AttentionWorld;
  readonly clock?: MonitorClock | undefined;
  readonly maxBranches?: number | undefined;
  readonly maxFanoutBytes?: number | undefined;
}

export interface WorldAmbientOptions extends WorldAttentionEngineOptions {
  readonly callbackSecretEnv?: string | undefined;
  readonly preparePath?: string | undefined;
  readonly deliverPath?: string | undefined;
  readonly maxCallbackRequestBytes?: number | undefined;
}

export interface WorldAmbientBinding extends AmbientBackendBinding {
  readonly engine: WorldAttentionEngine;
  readonly fetch: (request: Request) => Promise<Response>;
}

/** Binds Ambient to one correlation-addressed append RPC per distinct stream. */
export function world(
  options: WorldAmbientOptions,
): AmbientApplicationBackend<WorldAmbientBinding> {
  return Object.freeze({
    ...(options.clock === undefined ? {} : { clock: options.clock }),
    bind(callbacks: AttentionCallbacks) {
      return Object.freeze({
        engine: new WorldAttentionEngine(options),
        fetch: createWorldAttentionCallbackHandler(callbacks, {
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

/** Fans one accepted event directly to its distinct correlation streams. */
export class WorldAttentionEngine implements AttentionEngine {
  readonly #world: AttentionWorld;
  readonly #clock: MonitorClock;
  readonly #maxBranches: number;
  readonly #maxFanoutBytes: number;

  constructor(options: WorldAttentionEngineOptions) {
    if (options === null || typeof options !== "object") {
      throw new TypeError("World attention engine options are required");
    }
    if (
      options.world === null ||
      typeof options.world !== "object" ||
      typeof options.world.stream !== "function"
    ) {
      throw new TypeError("World attention engine requires a correlation-addressed world");
    }
    this.#world = options.world;
    this.#clock = options.clock ?? { now: () => new Date() };
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

    const appends = await compileAttentionStreamAppends(fanout);
    const settled = await Promise.allSettled(
      appends.map(async (append) => {
        const stream = this.#world.stream(append.streamKey);
        if (stream === null || typeof stream !== "object" || typeof stream.append !== "function") {
          throw new TypeError(`World stream ${append.streamKey} must define append`);
        }
        return validateAttentionStreamAppendReceipt(await stream.append(append), append);
      }),
    );
    const receipts = settled.map((result) => {
      if (result.status === "rejected") throw result.reason;
      return result.value;
    });
    const acceptedAt = receipts.map((receipt) => receipt.acceptedAt).sort().at(-1) ??
      this.#clock.now().toISOString();
    return Object.freeze({
      eventKey: fanout.eventKey,
      occurrenceKey: fanout.occurrenceKey,
      inputHash: fanout.inputHash,
      branchKeys: Object.freeze(fanout.branches.map((branch) => branch.branchKey)),
      acceptedAt,
    });
  }
}

export interface WorldAttentionCallbackHandlerOptions {
  readonly secretEnv?: string | undefined;
  readonly preparePath?: string | undefined;
  readonly deliverPath?: string | undefined;
  readonly maxRequestBytes?: number | undefined;
  readonly clock?: MonitorClock | undefined;
}

export type WorldAttentionCallbackEnvelope =
  | { readonly ok: true; readonly completedAt: string; readonly value: unknown }
  | {
      readonly ok: false;
      readonly completedAt: string;
      readonly error: string;
      readonly terminal: boolean;
    };

/** Authenticated by-value prepare/deliver endpoint for remote stream cells. */
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
      const value =
        path === preparePath
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

function callbackJson(body: WorldAttentionCallbackEnvelope, status = 200): Response {
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
