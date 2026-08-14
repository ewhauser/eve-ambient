import {
  BindingConflictError,
  type DirectDispatchHandler,
  type DirectDispatchRequest,
  type JsonValue,
  type MonitorBindingView,
  type MonitorDeliveryChannel,
  type MonitorDeliveryRequest,
} from "@ewhauser/eve-ambient";
import type { ChannelFrom, ChannelSendOptions, Session } from "eve/channels";

export const SUPPORTED_EVE_VERSION = "0.38.1" as const;
export const EVE_PATCH_FILE = "patches/eve@0.38.1.patch" as const;

export type EveDeliveryTarget = Readonly<{
  address: string;
}>;

export type EveChannelAuth = ChannelSendOptions["auth"];

export interface EveDeliveryChannelOptions {
  readonly auth:
    | EveChannelAuth
    | ((request: MonitorDeliveryRequest<EveDeliveryTarget>) => EveChannelAuth);
  readonly binding?: (
    request: MonitorDeliveryRequest<EveDeliveryTarget>,
    session: Session,
  ) => MonitorBindingView;
  readonly from: ChannelFrom;
  readonly id?: string;
  readonly renderMessage?: (
    request: MonitorDeliveryRequest<EveDeliveryTarget>,
  ) => string;
}

export interface EveDirectDispatchOptions {
  readonly address: (request: DirectDispatchRequest) => string | undefined;
  readonly auth:
    | EveChannelAuth
    | ((request: DirectDispatchRequest) => EveChannelAuth);
  readonly from: ChannelFrom;
  readonly renderMessage?: (request: DirectDispatchRequest) => string;
}

function resolveValue<TRequest, TValue>(
  value: TValue | ((request: TRequest) => TValue),
  request: TRequest,
): TValue {
  return typeof value === "function"
    ? (value as (request: TRequest) => TValue)(request)
    : value;
}

function canonicalBindingRef(address: string): string {
  return `eve:channel-address:${address}`;
}

function assertBinding(request: MonitorDeliveryRequest<EveDeliveryTarget>): string {
  const bindingRef = canonicalBindingRef(request.target.address);
  if (request.bindingRef !== undefined && request.bindingRef !== bindingRef) {
    throw new BindingConflictError(
      `Eve address ${JSON.stringify(request.target.address)} resolves to ${JSON.stringify(bindingRef)}, not existing binding ${JSON.stringify(request.bindingRef)}`,
    );
  }
  return bindingRef;
}

/**
 * Renders one complete monitor handoff while keeping trusted task text visibly
 * separate from untrusted evidence inside Eve's user-message boundary.
 */
export function renderEveMonitorMessage(
  request: MonitorDeliveryRequest<EveDeliveryTarget>,
): string {
  return JSON.stringify({
    applicationId: request.applicationId,
    auth: request.auth,
    bindingRef: request.bindingRef,
    correlationSubject: request.correlationSubject,
    kind: "eve-ambient.monitor-delivery",
    idempotencyKey: request.idempotencyKey,
    session: request.session,
    task: {
      trust: "application",
      instructions: request.taskInstructions,
    },
    target: request.target,
    tenantId: request.tenantId,
    evidence: {
      trust: "untrusted",
      value: request.evidence,
    },
    trigger: request.trigger,
  });
}

/** Delivers wake keys into Eve's durable channel-address inbox. */
export function createEveDeliveryChannel(
  options: EveDeliveryChannelOptions,
): MonitorDeliveryChannel<EveDeliveryTarget> {
  return {
    id: options.id ?? "eve",
    async deliver(request) {
      const bindingRef = assertBinding(request);
      const session = await options.from(request.target.address).send(
        (options.renderMessage ?? renderEveMonitorMessage)(request),
        {
          auth: resolveValue(options.auth, request),
          idempotencyKey: request.idempotencyKey,
          turnPolicy: "queue",
        },
      );
      const binding =
        options.binding?.(request, session) ?? {
          agentHasParticipated: false,
          bindingRef,
          status: "active" as const,
        };
      if (binding.bindingRef !== bindingRef) {
        throw new BindingConflictError(
          `Eve binding projection returned ${JSON.stringify(binding.bindingRef)}, expected ${JSON.stringify(bindingRef)}`,
        );
      }

      return {
        binding,
        outcome: "accepted",
        sessionId: session.id,
        // Eve does not expose its internal turn id from ChannelSource.send().
        // The admission key is the stable public identity for this delivery.
        turnId: request.idempotencyKey,
      };
    },
  };
}

/** Renders the complete canonical event for direct chat dispatch. */
export function renderEveDirectDispatchMessage(request: DirectDispatchRequest): string {
  return JSON.stringify({
    applicationId: request.applicationId,
    kind: "eve-ambient.direct-dispatch",
    event: request.event,
    eventKey: request.eventKey,
    idempotencyKey: request.idempotencyKey,
    inputHash: request.inputHash,
    tenantId: request.tenantId,
  });
}

/**
 * Creates a direct-dispatch handler that returns null only when the application
 * has no Eve address for the full event payload.
 */
export function createEveDirectDispatchHandler(
  options: EveDirectDispatchOptions,
): DirectDispatchHandler {
  return async (request) => {
    const address = options.address(request);
    if (address === undefined) return null;

    await options.from(address).send(
      (options.renderMessage ?? renderEveDirectDispatchMessage)(request),
      {
        auth: resolveValue(options.auth, request),
        idempotencyKey: request.idempotencyKey,
        turnPolicy: "queue",
      },
    );
    return { turnId: request.idempotencyKey };
  };
}

// Compile-time assertion: EveDeliveryTarget is a durable JSON value accepted
// by the provider-independent core boundary.
const _targetIsJson: JsonValue = { address: "example" } satisfies EveDeliveryTarget;
void _targetIsJson;
