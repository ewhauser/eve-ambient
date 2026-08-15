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
  type PreparedAttentionResult,
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

declare const ambientRuleEvent: unique symbol;
const ambientRuleExecutor: unique symbol = Symbol("ambientRuleExecutor");

interface AmbientRuleExecutor {
  matches(channel: object, event: CanonicalChannelEvent): boolean;
  correlationKey(event: CanonicalChannelEvent): string;
  orderKey(event: CanonicalChannelEvent): string;
  prepare(batch: FrozenAttentionBatch): Promise<PreparedAttentionResult>;
}

/** A typed rule whose executable type erasure remains private to the library. */
export interface AmbientRule<out TEvent extends CanonicalChannelEvent = CanonicalChannelEvent> {
  readonly id: string;
  readonly version: string;
  readonly mode: AttentionMode;
  readonly phase?: MonitorPhase | undefined;
  readonly policy: SerializableMailboxPolicy;
  readonly channel: object;
  readonly [ambientRuleEvent]?: TEvent | undefined;
  readonly [ambientRuleExecutor]: AmbientRuleExecutor;
}

export interface AmbientBatch<TEvent extends CanonicalChannelEvent = CanonicalChannelEvent> {
  readonly batch: FrozenAttentionBatch<TEvent>;
  readonly events: readonly TEvent[];
  readonly latest: TEvent;
  readonly eventKeys: readonly EventKey[];
}

interface AmbientRuleDefinitionBase<TEvent extends CanonicalChannelEvent> {
  readonly id: string;
  readonly version: string;
  readonly mode?: AttentionMode | undefined;
  readonly phase?: MonitorPhase | undefined;
  readonly policy: SerializableMailboxPolicy;
  readonly channel: ChannelCanonicalizationContract<never, TEvent>;
  readonly matches?: ((event: TEvent) => boolean) | undefined;
  readonly correlationKey: (event: TEvent) => string;
  readonly orderKey?: ((event: TEvent) => string) | undefined;
}

export type AmbientRuleDefinition<TEvent extends CanonicalChannelEvent> =
  AmbientRuleDefinitionBase<TEvent> &
    (
      | {
          readonly decide: (
            batch: AmbientBatch<TEvent>,
          ) => PreparedAttentionResult | Promise<PreparedAttentionResult>;
          readonly prepare?: never;
        }
      | {
          readonly prepare: (
            batch: FrozenAttentionBatch<TEvent>,
          ) => PreparedAttentionResult | Promise<PreparedAttentionResult>;
          readonly decide?: never;
        }
    );

export function defineAmbientRule<TEvent extends CanonicalChannelEvent>(
  definition: AmbientRuleDefinition<TEvent>,
): AmbientRule<TEvent> {
  nonEmpty(definition.id, "rule id");
  nonEmpty(definition.version, "rule version");
  const mode = definition.mode ?? "active";
  if (mode !== "active" && mode !== "shadow") {
    throw new TypeError("rule mode must be active or shadow");
  }
  if (
    definition.channel === null ||
    typeof definition.channel !== "object" ||
    typeof definition.channel.canonicalize !== "function"
  ) {
    throw new TypeError("rule channel must define canonicalize");
  }
  if (definition.matches !== undefined && typeof definition.matches !== "function") {
    throw new TypeError("rule matches must be a function");
  }
  if (typeof definition.correlationKey !== "function") {
    throw new TypeError("rule correlationKey must be a function");
  }
  if (definition.orderKey !== undefined && typeof definition.orderKey !== "function") {
    throw new TypeError("rule orderKey must be a function");
  }
  const hasPrepare = typeof definition.prepare === "function";
  const hasDecide = typeof definition.decide === "function";
  if (hasPrepare === hasDecide) {
    throw new TypeError("rule must define exactly one of prepare or decide");
  }
  const prepare = hasPrepare
    ? async (batch: FrozenAttentionBatch<TEvent>) => definition.prepare!(batch)
    : async (batch: FrozenAttentionBatch<TEvent>) => definition.decide!(batchView(batch));
  const matches = definition.matches ?? (() => true);
  const orderKey = definition.orderKey ?? ((event: TEvent) => event.occurredAt ?? event.id);
  const executor: AmbientRuleExecutor = Object.freeze({
    matches(channel: object, event: CanonicalChannelEvent) {
      return definition.channel === channel && matches(event as TEvent);
    },
    correlationKey: (event: CanonicalChannelEvent) =>
      definition.correlationKey(event as TEvent),
    orderKey: (event: CanonicalChannelEvent) => orderKey(event as TEvent),
    prepare: (batch: FrozenAttentionBatch) => prepare(batch as FrozenAttentionBatch<TEvent>),
  });
  return Object.freeze({
    id: definition.id,
    version: definition.version,
    mode,
    ...(definition.phase === undefined ? {} : { phase: definition.phase }),
    policy: definition.policy,
    channel: definition.channel,
    [ambientRuleExecutor]: executor,
  });
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

export interface AmbientPublisher {
  publish<TRaw, TEvent extends CanonicalChannelEvent>(
    channel: ChannelCanonicalizationContract<TRaw, TEvent>,
    raw: TRaw,
  ): Promise<AmbientPublishReceipt>;
}

export function createAmbientPublisher(options: {
  readonly applicationId: string;
  readonly engine: AttentionEngine;
  readonly rules: readonly AmbientRule[];
  readonly direct?: DirectDispatchRule | undefined;
}): AmbientPublisher {
  nonEmpty(options.applicationId, "applicationId");
  if (options.engine === null || typeof options.engine !== "object") {
    throw new TypeError("attention engine is required");
  }
  const rules = [...options.rules];
  assertRuleRegistry(rules);
  return Object.freeze({
    async publish<TRaw, TEvent extends CanonicalChannelEvent>(
      channel: ChannelCanonicalizationContract<TRaw, TEvent>,
      raw: TRaw,
    ): Promise<AmbientPublishReceipt> {
      const source = await canonicalizeChannelDelivery(channel, raw, {
        applicationId: options.applicationId,
      });
      const plans: AttentionBranchPlan[] = [];
      for (const rule of rules) {
        const executor = rule[ambientRuleExecutor];
        if (!executor.matches(channel, source.payload.event)) continue;
        const correlationKey = executor.correlationKey(source.payload.event);
        const orderKey = executor.orderKey(source.payload.event);
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

export interface AmbientBackendBinding {
  readonly engine: AttentionEngine;
}

export interface AmbientApplicationBackend<TBinding extends AmbientBackendBinding> {
  readonly clock?: MonitorClock | undefined;
  bind(callbacks: AttentionCallbacks): TBinding;
}

export type AmbientApplication<TBinding extends AmbientBackendBinding> = TBinding & {
  publish<TRaw, TEvent extends CanonicalChannelEvent>(
    channel: ChannelCanonicalizationContract<TRaw, TEvent>,
    raw: TRaw,
  ): Promise<AmbientPublishReceipt>;
};

export interface AmbientApplicationDefinition {
  readonly applicationId: string;
  with<TBinding extends AmbientBackendBinding>(
    backend: AmbientApplicationBackend<TBinding>,
  ): AmbientApplication<TBinding>;
}

/** Defines rules and routes once, then binds them to any supported backend. */
export function defineAmbientApplication(options: {
  readonly applicationId: string;
  readonly rules: readonly AmbientRule[];
  readonly routes: readonly AttentionRoute[];
  readonly direct?: DirectDispatchRule | undefined;
}): AmbientApplicationDefinition {
  nonEmpty(options.applicationId, "applicationId");
  const rules = [...options.rules];
  const routes = [...options.routes];
  assertRuleRegistry(rules);
  return Object.freeze({
    applicationId: options.applicationId,
    with<TBinding extends AmbientBackendBinding>(
      backend: AmbientApplicationBackend<TBinding>,
    ): AmbientApplication<TBinding> {
      if (backend === null || typeof backend !== "object" || typeof backend.bind !== "function") {
        throw new TypeError("ambient application backend must define bind");
      }
      const callbacks = createAttentionCallbacks({
        rules,
        routes,
        ...(backend.clock === undefined ? {} : { clock: backend.clock }),
      });
      const binding = backend.bind(callbacks);
      const publisher = createAmbientPublisher({
        applicationId: options.applicationId,
        engine: binding.engine,
        rules,
        ...(options.direct === undefined ? {} : { direct: options.direct }),
      });
      return Object.freeze({
        ...binding,
        publish: publisher.publish.bind(publisher),
      }) as AmbientApplication<TBinding>;
    },
  });
}

export function createAttentionCallbacks(options: {
  readonly rules: readonly AmbientRule[];
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
      const prepared = await rule[ambientRuleExecutor].prepare(batch);
      if (prepared.kind === "ignore") return prepared;
      if (prepared.routeId !== undefined) return { ...prepared, routeId: prepared.routeId };
      if (routes.size !== 1) {
        throw new Error("a wake must select routeId when the application has multiple routes");
      }
      return { ...prepared, routeId: routes.keys().next().value! };
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

function assertRuleRegistry(rules: readonly AmbientRule[]): void {
  const identities = new Set<string>();
  for (const rule of rules) {
    nonEmpty(rule.id, "rule id");
    nonEmpty(rule.version, "rule version");
    if (rule.mode !== "active" && rule.mode !== "shadow") {
      throw new TypeError("rule mode must be active or shadow");
    }
    const executor = rule[ambientRuleExecutor];
    if (executor === undefined || typeof executor.matches !== "function") {
      throw new TypeError("ambient rules must be created with defineAmbientRule");
    }
    if (typeof executor.correlationKey !== "function") {
      throw new TypeError("rule correlationKey must be a function");
    }
    if (typeof executor.orderKey !== "function") throw new TypeError("rule orderKey must be a function");
    if (typeof executor.prepare !== "function") throw new TypeError("rule prepare must be a function");
    const identity = `${rule.id}\u0000${rule.version}`;
    if (identities.has(identity)) throw new TypeError(`duplicate ambient rule ${rule.id}@${rule.version}`);
    identities.add(identity);
  }
}

function batchView<TEvent extends CanonicalChannelEvent>(
  batch: FrozenAttentionBatch<TEvent>,
): AmbientBatch<TEvent> {
  const events = batch.branches.map((branch) => branch.event);
  const latest = events.at(-1);
  if (latest === undefined) throw new Error("attention batch is empty");
  return Object.freeze({
    batch,
    events: Object.freeze(events),
    latest,
    eventKeys: Object.freeze(batch.branches.map((branch) => branch.eventKey)),
  });
}

function nonEmpty(value: string, name: string): void {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${name} must not be empty`);
  }
}
