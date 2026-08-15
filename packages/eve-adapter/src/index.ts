import type {
  AttentionRoute,
  DirectDispatchAdapter,
  DirectDispatchRequest,
  PreparedAttentionWake,
} from "@ewhauser/eve-ambient/protocol";
import type { JsonValue } from "@ewhauser/eve-ambient";
import type { ChannelFrom, ChannelSendOptions } from "eve/channels";

export const SUPPORTED_EVE_VERSION = "0.38.1" as const;
export const EVE_PATCH_FILE = "patches/eve@0.38.1.patch" as const;

export type EveChannelAuth = ChannelSendOptions["auth"];

export interface EveAttentionRouteOptions {
  readonly id?: string | undefined;
  readonly auth: EveChannelAuth | ((wake: PreparedAttentionWake) => EveChannelAuth);
  /** Defaults to the prepared wake's string target. */
  readonly address?: string | ((wake: PreparedAttentionWake) => string) | undefined;
  readonly from: ChannelFrom;
  readonly renderMessage?: ((wake: PreparedAttentionWake) => string) | undefined;
}

export interface EveDirectDispatchOptions {
  readonly auth:
    | EveChannelAuth
    | ((request: DirectDispatchRequest) => EveChannelAuth);
  readonly address: string | ((request: DirectDispatchRequest) => string);
  readonly from: ChannelFrom;
  readonly renderMessage?: ((request: DirectDispatchRequest) => string) | undefined;
  readonly now?: (() => Date) | undefined;
}

/** Renders trusted task instructions separately from untrusted evidence. */
export function renderEveAttentionMessage(wake: PreparedAttentionWake): string {
  return JSON.stringify({
    kind: "eve-ambient.attention",
    applicationId: wake.applicationId,
    tenantId: wake.tenantId,
    monitorId: wake.monitorId,
    definitionVersion: wake.definitionVersion,
    correlationKey: wake.correlationKey,
    wakeKey: wake.wakeKey,
    runKey: wake.runKey,
    batchKey: wake.batchKey,
    rootEventKeys: wake.rootEventKeys,
    target: wake.target,
    task: {
      trust: "application",
      instruction: wake.instruction,
    },
    decision: wake.decision,
    evidence: {
      trust: "untrusted",
      value: wake.evidence,
    },
  });
}

/** Final Ambient route: `wakeKey` becomes Eve's durable session admission key. */
export function createEveAttentionRoute(
  options: EveAttentionRouteOptions,
): AttentionRoute {
  return {
    id: options.id ?? "eve",
    async deliver(wake): Promise<JsonValue> {
      const address = nonEmpty(
        options.address === undefined
          ? stringTarget(wake.target)
          : resolve(options.address, wake),
        "Eve attention address",
      );
      const session = await options.from(address).send(
        (options.renderMessage ?? renderEveAttentionMessage)(wake),
        {
          auth: resolve(options.auth, wake),
          idempotencyKey: wake.wakeKey,
          turnPolicy: "queue",
        },
      );
      return { address, sessionId: session.id, turnId: wake.wakeKey };
    },
  };
}

function stringTarget(target: JsonValue): string {
  if (typeof target !== "string") {
    throw new TypeError("Eve attention target must be a string unless address is configured");
  }
  return target;
}

export function renderEveDirectDispatchMessage(
  request: DirectDispatchRequest,
): string {
  return JSON.stringify({
    kind: "eve-ambient.direct-dispatch",
    applicationId: request.applicationId,
    tenantId: request.tenantId,
    eventKey: request.eventKey,
    occurrenceKey: request.occurrenceKey,
    idempotencyKey: request.idempotencyKey,
    event: request.event,
  });
}

/** Adapter-owned direct chat boundary, independent of the attention engine. */
export function createEveDirectDispatchAdapter(
  options: EveDirectDispatchOptions,
): DirectDispatchAdapter {
  return {
    async dispatch(request) {
      const address = nonEmpty(resolve(options.address, request), "Eve direct address");
      const session = await options.from(address).send(
        (options.renderMessage ?? renderEveDirectDispatchMessage)(request),
        {
          auth: resolve(options.auth, request),
          idempotencyKey: request.idempotencyKey,
          turnPolicy: "queue",
        },
      );
      return {
        idempotencyKey: request.idempotencyKey,
        inputHash: request.inputHash,
        dispatchedAt: (options.now ?? (() => new Date()))().toISOString(),
        result: { address, sessionId: session.id, turnId: request.idempotencyKey },
      };
    },
  };
}

function resolve<TRequest, TValue>(
  value: TValue | ((request: TRequest) => TValue),
  request: TRequest,
): TValue {
  return typeof value === "function"
    ? (value as (request: TRequest) => TValue)(request)
    : value;
}

function nonEmpty(value: string, name: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${name} must not be empty`);
  }
  return value;
}
