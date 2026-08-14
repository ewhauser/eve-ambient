import {
  compileAcceptedFanout,
  type AttentionAcceptanceReceipt,
  type AttentionBranchPlan,
  type AttentionCallbacks,
  type AttentionDeliveryReceipt,
  type AttentionEngine,
  type AttentionMode,
  type FrozenAttentionBatch,
  type PreparedAttentionOutcome,
  type PreparedAttentionWake,
  type SerializableMailboxPolicy,
} from "./attention.js";
import {
  canonicalizeChannelDelivery,
  deriveAttentionDirectDispatchKey,
  hashIdempotencyInput,
  type CanonicalChannelEvent,
  type ChannelCanonicalizationContract,
  type DirectDispatchKey,
  type EventKey,
  type InputHash,
  type OccurrenceKey,
} from "./idempotency.js";
import type { JsonValue, MonitorClock, MonitorPhase } from "./types.js";

export interface AmbientRule<TEvent extends CanonicalChannelEvent = CanonicalChannelEvent> {
  readonly id: string;
  readonly version: string;
  readonly mode: AttentionMode;
  readonly phase?: MonitorPhase | undefined;
  readonly policy: SerializableMailboxPolicy;
  readonly matches: (event: TEvent) => boolean;
  readonly correlationKey: (event: TEvent) => string;
  readonly orderKey: (event: TEvent) => string;
  readonly prepare: (batch: FrozenAttentionBatch<TEvent>) => Promise<PreparedAttentionOutcome>;
}

export function defineAmbientRule<TEvent extends CanonicalChannelEvent>(
  rule: AmbientRule<TEvent>,
): AmbientRule<TEvent> {
  nonEmpty(rule.id, "rule id");
  nonEmpty(rule.version, "rule version");
  if (rule.mode !== "active" && rule.mode !== "shadow") {
    throw new TypeError("rule mode must be active or shadow");
  }
  if (typeof rule.matches !== "function") throw new TypeError("rule matches must be a function");
  if (typeof rule.correlationKey !== "function") {
    throw new TypeError("rule correlationKey must be a function");
  }
  if (typeof rule.orderKey !== "function") throw new TypeError("rule orderKey must be a function");
  if (typeof rule.prepare !== "function") throw new TypeError("rule prepare must be a function");
  return Object.freeze({ ...rule });
}

export interface DirectDispatchRequest<
  TEvent extends CanonicalChannelEvent = CanonicalChannelEvent,
> {
  readonly idempotencyKey: DirectDispatchKey;
  readonly inputHash: InputHash;
  readonly applicationId: string;
  readonly tenantId: string;
  readonly eventKey: EventKey;
  readonly occurrenceKey: OccurrenceKey;
  readonly event: TEvent;
}

export interface DirectDispatchReceipt {
  readonly idempotencyKey: DirectDispatchKey;
  readonly inputHash: InputHash;
  readonly dispatchedAt: string;
  readonly result: JsonValue;
}

export interface DirectDispatchAdapter<
  TEvent extends CanonicalChannelEvent = CanonicalChannelEvent,
> {
  dispatch(request: DirectDispatchRequest<TEvent>): Promise<DirectDispatchReceipt>;
}

export interface DirectDispatchRule<
  TEvent extends CanonicalChannelEvent = CanonicalChannelEvent,
> {
  readonly adapter: DirectDispatchAdapter<TEvent>;
  readonly matches: (event: TEvent) => boolean;
  readonly bindingGeneration: (event: TEvent) => string;
}

export interface AmbientPublishReceipt {
  readonly attention: AttentionAcceptanceReceipt;
  readonly direct?: DirectDispatchReceipt | undefined;
}

export interface AmbientPublisher<TEvent extends CanonicalChannelEvent = CanonicalChannelEvent> {
  publish<TRaw>(
    channel: ChannelCanonicalizationContract<TRaw, TEvent>,
    raw: TRaw,
  ): Promise<AmbientPublishReceipt>;
}

export function createAmbientPublisher<TEvent extends CanonicalChannelEvent>(options: {
  readonly applicationId: string;
  readonly engine: AttentionEngine;
  readonly rules: readonly AmbientRule<TEvent>[];
  readonly direct?: DirectDispatchRule<TEvent> | undefined;
}): AmbientPublisher<TEvent> {
  nonEmpty(options.applicationId, "applicationId");
  if (options.engine === null || typeof options.engine !== "object") {
    throw new TypeError("attention engine is required");
  }
  const rules = [...options.rules];
  assertRuleRegistry(rules);
  return Object.freeze({
    async publish<TRaw>(
      channel: ChannelCanonicalizationContract<TRaw, TEvent>,
      raw: TRaw,
    ): Promise<AmbientPublishReceipt> {
      const source = await canonicalizeChannelDelivery(channel, raw, {
        applicationId: options.applicationId,
      });
      const plans: AttentionBranchPlan[] = [];
      for (const rule of rules) {
        if (!rule.matches(source.payload.event)) continue;
        const correlationKey = rule.correlationKey(source.payload.event);
        const orderKey = rule.orderKey(source.payload.event);
        nonEmpty(correlationKey, `${rule.id} correlation key`);
        nonEmpty(orderKey, `${rule.id} order key`);
        plans.push({
          monitorId: rule.id,
          definitionVersion: rule.version,
          ...(rule.phase === undefined ? {} : { phase: rule.phase }),
          correlationKey,
          orderKey,
          mode: rule.mode,
          policy: rule.policy,
        });
      }
      const fanout = await compileAcceptedFanout({ source, branches: plans });
      let direct: DirectDispatchReceipt | undefined;
      if (options.direct?.matches(source.payload.event) === true) {
        const bindingGeneration = options.direct.bindingGeneration(source.payload.event);
        nonEmpty(bindingGeneration, "direct dispatch binding generation");
        const payload = {
          applicationId: options.applicationId,
          tenantId: source.payload.event.source.tenantId,
          eventKey: source.idempotency.key,
          occurrenceKey: fanout.occurrenceKey,
          event: source.payload.event,
        };
        const inputHash = await hashIdempotencyInput(payload);
        const idempotencyKey = await deriveAttentionDirectDispatchKey({
          occurrenceKey: fanout.occurrenceKey,
          bindingGeneration,
        });
        direct = await options.direct.adapter.dispatch({
          idempotencyKey,
          inputHash,
          ...payload,
        });
        if (direct.idempotencyKey !== idempotencyKey || direct.inputHash !== inputHash) {
          throw new TypeError("direct dispatch receipt does not match its request identity");
        }
      }
      const attention = await options.engine.accept(fanout);
      return { attention, ...(direct === undefined ? {} : { direct }) };
    },
  });
}

export interface AttentionRoute {
  readonly id: string;
  deliver(wake: PreparedAttentionWake): Promise<JsonValue>;
}

export function createAttentionCallbacks<TEvent extends CanonicalChannelEvent>(options: {
  readonly rules: readonly AmbientRule<TEvent>[];
  readonly routes: readonly AttentionRoute[];
  readonly clock?: MonitorClock | undefined;
}): AttentionCallbacks {
  const rules = [...options.rules];
  assertRuleRegistry(rules);
  const routes = new Map<string, AttentionRoute>();
  for (const route of options.routes) {
    nonEmpty(route.id, "route id");
    if (typeof route.deliver !== "function") throw new TypeError("route deliver must be a function");
    if (routes.has(route.id)) throw new TypeError(`duplicate attention route ${route.id}`);
    routes.set(route.id, route);
  }
  const clock = options.clock ?? { now: () => new Date() };
  return Object.freeze({
    async prepare(batch: FrozenAttentionBatch): Promise<PreparedAttentionOutcome> {
      const rule = rules.find(
        (candidate) =>
          candidate.id === batch.monitorId && candidate.version === batch.definitionVersion,
      );
      if (rule === undefined) {
        throw new Error(
          `attention definition ${batch.monitorId}@${batch.definitionVersion} is not registered`,
        );
      }
      return rule.prepare(batch as FrozenAttentionBatch<TEvent>);
    },
    async deliver(wake: PreparedAttentionWake): Promise<AttentionDeliveryReceipt> {
      const route = routes.get(wake.routeId);
      if (route === undefined) throw new Error(`attention route ${wake.routeId} is not registered`);
      const result = await route.deliver(wake);
      return {
        wakeKey: wake.wakeKey,
        inputHash: wake.inputHash,
        deliveredAt: clock.now().toISOString(),
        result,
      };
    },
  });
}

function assertRuleRegistry<TEvent extends CanonicalChannelEvent>(
  rules: readonly AmbientRule<TEvent>[],
): void {
  const identities = new Set<string>();
  for (const rule of rules) {
    defineAmbientRule(rule);
    const identity = `${rule.id}\u0000${rule.version}`;
    if (identities.has(identity)) throw new TypeError(`duplicate ambient rule ${rule.id}@${rule.version}`);
    identities.add(identity);
  }
}

function nonEmpty(value: string, name: string): void {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${name} must not be empty`);
  }
}
