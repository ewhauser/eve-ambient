import {
  validateAcceptedFanout,
  AttentionCapacityError,
  type AcceptedFanout,
  type AttentionAcceptanceReceipt,
  type AttentionCallbacks,
  type AttentionEngine,
  type FrozenAttentionBatch,
  type PreparedAttentionWake,
} from "./attention.js";
import { IdempotencyConflictError } from "./idempotency.js";

export interface CelldAttentionEngineOptions {
  readonly url: string;
  readonly secret: string;
  readonly fetch?: typeof fetch | undefined;
}

export class CelldAttentionEngine implements AttentionEngine {
  readonly #url: string;
  readonly #secret: string;
  readonly #fetch: typeof fetch;

  constructor(options: CelldAttentionEngineOptions) {
    this.#url = absoluteUrl(options.url, "celld url");
    this.#secret = nonEmpty(options.secret, "celld secret");
    this.#fetch = options.fetch ?? fetch;
  }

  async accept(input: AcceptedFanout): Promise<AttentionAcceptanceReceipt> {
    const fanout = await validateAcceptedFanout(input);
    const response = await this.#fetch(
      cellUrl(this.#url, fanout.eventKey, "accept"),
      {
        method: "POST",
        headers: {
          authorization: `Bearer ${this.#secret}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(fanout),
      },
    );
    const body = await responseJson(response);
    if (response.ok) return body as unknown as AttentionAcceptanceReceipt;
    const detail = errorDetail(body);
    if (response.status === 409) {
      throw new IdempotencyConflictError({
        namespace: detail.namespace ?? "celld-attention",
        key: detail.key ?? fanout.eventKey,
        existingInputHash: detail.existingInputHash ?? "unknown",
        receivedInputHash: detail.receivedInputHash ?? fanout.inputHash,
      });
    }
    if (response.status === 413) throw new AttentionCapacityError(detail.error);
    throw new CelldAttentionError(detail.error, response.status, response.status >= 500 || response.status === 429);
  }
}

export class CelldAttentionError extends Error {
  readonly status: number;
  readonly retryable: boolean;

  constructor(message: string, status: number, retryable: boolean) {
    super(message);
    this.name = "CelldAttentionError";
    this.status = status;
    this.retryable = retryable;
  }
}

export interface AttentionCallbackFetchHandlerOptions {
  readonly secret: string;
  readonly preparePath?: string | undefined;
  readonly deliverPath?: string | undefined;
}

/** Authenticated by-value callback endpoint used by celld correlation cells. */
export function createAttentionCallbackFetchHandler(
  callbacks: AttentionCallbacks,
  options: AttentionCallbackFetchHandlerOptions,
): (request: Request) => Promise<Response> {
  const secret = nonEmpty(options.secret, "attention callback secret");
  const preparePath = options.preparePath ?? "/ambient/prepare";
  const deliverPath = options.deliverPath ?? "/ambient/deliver";
  return async (request) => {
    if (request.method !== "POST") return json({ error: "method not allowed" }, 405);
    if (!secretsMatch(bearerToken(request), secret)) return json({ error: "unauthorized" }, 401);
    let body: unknown;
    try {
      body = await request.json();
    } catch (error) {
      return json({ error: `invalid JSON: ${message(error)}` }, 400);
    }
    try {
      const path = new URL(request.url).pathname;
      if (path === preparePath) {
        return json(await callbacks.prepare(deepFreeze(body as FrozenAttentionBatch)));
      }
      if (path === deliverPath) {
        return json(await callbacks.deliver(deepFreeze(body as PreparedAttentionWake)));
      }
      return json({ error: "not found" }, 404);
    } catch (error) {
      return json({ error: message(error) }, 503);
    }
  };
}

export function cellUrl(baseUrl: string, name: string, action: string): string {
  return `${baseUrl.replace(/\/$/, "")}/cells/${encodeURIComponent(name)}/${action}`;
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

function bearerToken(request: Request): string {
  const match = /^Bearer[ ]+(.+)$/i.exec(request.headers.get("authorization")?.trim() ?? "");
  return match?.[1] ?? "";
}

async function responseJson(response: Response): Promise<Record<string, unknown>> {
  try {
    const value = (await response.json()) as unknown;
    return value !== null && typeof value === "object"
      ? (value as Record<string, unknown>)
      : { error: "celld returned a non-object response" };
  } catch {
    return { error: `celld returned ${response.status} with invalid JSON` };
  }
}

function errorDetail(value: Record<string, unknown>): {
  readonly error: string;
  readonly namespace?: string | undefined;
  readonly key?: string | undefined;
  readonly existingInputHash?: string | undefined;
  readonly receivedInputHash?: string | undefined;
} {
  return {
    error: typeof value.error === "string" ? value.error : "celld attention request failed",
    ...(typeof value.namespace === "string" ? { namespace: value.namespace } : {}),
    ...(typeof value.key === "string" ? { key: value.key } : {}),
    ...(typeof value.existingInputHash === "string"
      ? { existingInputHash: value.existingInputHash }
      : {}),
    ...(typeof value.receivedInputHash === "string"
      ? { receivedInputHash: value.receivedInputHash }
      : {}),
  };
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function absoluteUrl(value: string, name: string): string {
  const normalized = nonEmpty(value, name);
  try {
    return new URL(normalized).toString().replace(/\/$/, "");
  } catch {
    throw new TypeError(`${name} must be an absolute URL`);
  }
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

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object") {
    Object.freeze(value);
    for (const nested of Object.values(value)) deepFreeze(nested);
  }
  return value;
}
