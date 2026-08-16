import type {
  AttentionDeliveryReceipt,
  FrozenAttentionBatch,
  PreparedAttentionOutcome,
  PreparedAttentionWake,
} from "../attention.js";
import type { CorrelationReducerConflict } from "../workflow-protocol.js";

export type PrepareCallbackResult = CallbackFailure | {
  readonly ok: true;
  readonly completedAt: string;
  readonly value: PreparedAttentionOutcome;
};

export type DeliverCallbackResult = CallbackFailure | {
  readonly ok: true;
  readonly completedAt: string;
  readonly value: AttentionDeliveryReceipt;
};

interface CallbackFailure {
  readonly ok: false;
  readonly completedAt: string;
  readonly error: string;
  readonly terminal: boolean;
}

export async function invokePrepare(
  callbackUrl: string,
  path: string,
  secretEnv: string,
  batch: FrozenAttentionBatch,
): Promise<PrepareCallbackResult> {
  "use step";

  return invoke(callbackUrl, path, secretEnv, batch) as Promise<PrepareCallbackResult>;
}

export async function invokeDeliver(
  callbackUrl: string,
  path: string,
  secretEnv: string,
  wake: PreparedAttentionWake,
): Promise<DeliverCallbackResult> {
  "use step";

  return invoke(callbackUrl, path, secretEnv, wake) as Promise<DeliverCallbackResult>;
}

/** Records an asynchronous reducer conflict in the Workflow run timeline. */
export async function reportReducerConflict(
  conflict: CorrelationReducerConflict,
): Promise<void> {
  "use step";

  console.error("eve-ambient correlation idempotency conflict", conflict);
}

async function invoke(
  callbackUrl: string,
  path: string,
  secretEnv: string,
  body: unknown,
): Promise<PrepareCallbackResult | DeliverCallbackResult> {
  const completedAt = () => new Date().toISOString();
  try {
    const secret = process.env[secretEnv];
    if (secret === undefined || secret.length === 0) {
      return {
        ok: false,
        completedAt: completedAt(),
        error: `callback secret environment variable ${secretEnv} is not set`,
        terminal: true,
      };
    }
    const response = await globalThis.fetch(`${callbackUrl}${path}`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${secret}`,
        "content-type": "application/json",
      },
      body: JSON.stringify(body),
    });
    const value = await response.json() as unknown;
    if (!isCallbackEnvelope(value)) {
      return {
        ok: false,
        completedAt: completedAt(),
        error: `callback returned an invalid envelope with status ${response.status}`,
        terminal: true,
      };
    }
    return value as PrepareCallbackResult | DeliverCallbackResult;
  } catch (error) {
    return {
      ok: false,
      completedAt: completedAt(),
      error: error instanceof Error ? error.message : String(error),
      terminal: false,
    };
  }
}

function isCallbackEnvelope(value: unknown): value is {
  readonly ok: boolean;
  readonly completedAt: string;
} {
  return value !== null &&
    typeof value === "object" &&
    typeof (value as { ok?: unknown }).ok === "boolean" &&
    typeof (value as { completedAt?: unknown }).completedAt === "string";
}
