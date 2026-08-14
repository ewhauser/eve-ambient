import { canonicalJson } from "./canonical.js";
import type {
  ChannelEventActor,
  ChannelEventOrigin,
  JsonValue,
  MonitorPhase,
  SubjectRef,
} from "./types.js";

declare const idempotencyKeyBrand: unique symbol;
declare const inputHashBrand: unique symbol;

export type IdempotencyKey<TKind extends string = string> = string & {
  readonly [idempotencyKeyBrand]: TKind;
};
export type EventKey = IdempotencyKey<"event">;
export type DirectDispatchKey = IdempotencyKey<"direct-dispatch">;
export type BranchKey = IdempotencyKey<"branch">;
export type BatchKey = IdempotencyKey<"batch">;
export type RunKey = IdempotencyKey<"run">;
export type WakeKey = IdempotencyKey<"wake">;
export type InputHash = string & { readonly [inputHashBrand]: true };
export type IdempotencyKeyKind =
  | "event"
  | "direct-dispatch"
  | "branch"
  | "batch"
  | "run"
  | "wake";

export interface IdempotencyContext<TKey extends IdempotencyKey = IdempotencyKey> {
  readonly key: TKey;
  readonly inputHash: InputHash;
  readonly parentKeys: readonly IdempotencyKey[];
  readonly eventKeys: readonly EventKey[];
}

export interface IdempotentEnvelope<TPayload, TKey extends IdempotencyKey = IdempotencyKey> {
  readonly idempotency: IdempotencyContext<TKey>;
  readonly payload: TPayload;
}

export interface EventKeyInput {
  readonly tenantId: string;
  readonly applicationId: string;
  readonly channelId: string;
  readonly installationId: string;
  readonly sourceEventId: string;
}

export interface CanonicalChannelEvent<
  TType extends string = string,
  TData extends JsonValue = JsonValue,
  TReplyTarget extends JsonValue = JsonValue,
> {
  readonly id: string;
  readonly type: TType;
  readonly version: number;
  readonly occurredAt?: string | undefined;
  readonly data: TData;
  readonly source: {
    readonly channelId: string;
    readonly installationId: string;
    readonly tenantId: string;
  };
  readonly actor?: ChannelEventActor | undefined;
  readonly authRef?: string | undefined;
  readonly replyTarget?: TReplyTarget | undefined;
  readonly subjects?: readonly SubjectRef[] | undefined;
  readonly origin: ChannelEventOrigin;
}

export interface AcceptedChannelEvent<TEvent extends CanonicalChannelEvent = CanonicalChannelEvent> {
  readonly applicationId: string;
  readonly canonicalizationVersion: number;
  readonly event: TEvent;
}

/**
 * A channel-owned, deterministic conversion from provider input to the
 * complete semantic event Eve hashes and hands to durable ingress.
 *
 * Authentication and transport acknowledgement happen outside this pure
 * function. Delivery-attempt fields must be discarded here; semantically
 * meaningful provider fields must remain in the returned event.
 */
export interface ChannelCanonicalizationContract<
  TRaw,
  TEvent extends CanonicalChannelEvent = CanonicalChannelEvent,
> {
  readonly version: number;
  readonly canonicalize: (raw: TRaw) => TEvent;
}

export function defineChannelCanonicalization<
  TRaw,
  TEvent extends CanonicalChannelEvent,
>(options: ChannelCanonicalizationContract<TRaw, TEvent>): ChannelCanonicalizationContract<TRaw, TEvent> {
  assertPositiveInteger(options.version, "channel canonicalization version");
  if (typeof options.canonicalize !== "function") {
    throw new TypeError("channel canonicalization canonicalize must be a function");
  }
  return Object.freeze({ version: options.version, canonicalize: options.canonicalize });
}

/** Canonicalizes a provider delivery and assigns its root idempotency identity. */
export async function canonicalizeChannelDelivery<
  TRaw,
  TEvent extends CanonicalChannelEvent,
>(
  contract: ChannelCanonicalizationContract<TRaw, TEvent>,
  raw: TRaw,
  options: { readonly applicationId: string },
): Promise<IdempotentEnvelope<AcceptedChannelEvent<TEvent>, EventKey>> {
  assertPositiveInteger(contract.version, "channel canonicalization version");
  assertNonEmpty(options.applicationId, "applicationId");
  const event = cloneCanonicalEvent(contract.canonicalize(raw));
  const key = await deriveEventKey({
    tenantId: event.source.tenantId,
    applicationId: options.applicationId,
    channelId: event.source.channelId,
    installationId: event.source.installationId,
    sourceEventId: event.id,
  });
  const payload = deepFreeze({
    applicationId: options.applicationId,
    canonicalizationVersion: contract.version,
    event,
  }) as AcceptedChannelEvent<TEvent>;
  const inputHash = await hashIdempotencyInput(payload);
  return deepFreeze({
    idempotency: createIdempotencyContext({
      key,
      inputHash,
      parentKeys: [],
      eventKeys: [key],
    }),
    payload,
  });
}

export async function hashIdempotencyInput(value: unknown): Promise<InputHash> {
  return domainHash("eve:input:v1", [value]) as Promise<InputHash>;
}

/** Validates a key reconstructed from durable or transport data. */
export function parseIdempotencyKey<TKind extends IdempotencyKeyKind>(
  kind: TKind,
  value: string,
): IdempotencyKey<TKind> {
  assertKeyKind(value, kind);
  return value as IdempotencyKey<TKind>;
}

/** Validates an input hash reconstructed from durable or transport data. */
export function parseInputHash(value: string): InputHash {
  assertInputHash(value);
  return value;
}

export async function deriveEventKey(input: EventKeyInput): Promise<EventKey> {
  const parts = [
    nonEmpty(input.tenantId, "tenantId"),
    nonEmpty(input.applicationId, "applicationId"),
    nonEmpty(input.channelId, "channelId"),
    nonEmpty(input.installationId, "installationId"),
    nonEmpty(input.sourceEventId, "sourceEventId"),
  ];
  return domainHash("eve:event:v1", parts) as Promise<EventKey>;
}

export async function deriveDirectDispatchKey(input: {
  readonly eventKey: EventKey;
  /** Durable ingress-receipt generation; matching retries reuse it. */
  readonly acceptanceId: string;
  readonly bindingGeneration: string;
}): Promise<DirectDispatchKey> {
  assertKeyKind(input.eventKey, "event");
  return domainHash("eve:direct-dispatch:v1", [
    input.eventKey,
    nonEmpty(input.acceptanceId, "acceptanceId"),
    nonEmpty(input.bindingGeneration, "bindingGeneration"),
  ]) as Promise<DirectDispatchKey>;
}

export async function deriveBranchKey(input: {
  readonly eventKey: EventKey;
  /** Durable ingress-receipt generation; matching retries reuse it. */
  readonly acceptanceId: string;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly phase?: MonitorPhase | undefined;
}): Promise<BranchKey> {
  assertKeyKind(input.eventKey, "event");
  return domainHash("eve:branch:v1", [
    input.eventKey,
    nonEmpty(input.acceptanceId, "acceptanceId"),
    nonEmpty(input.monitorId, "monitorId"),
    nonEmpty(input.definitionVersion, "definitionVersion"),
    input.phase ?? null,
  ]) as Promise<BranchKey>;
}

export async function deriveBatchKey(input: {
  readonly instanceId: string;
  readonly orderedBranchKeys: readonly BranchKey[];
}): Promise<BatchKey> {
  const keys = distinctKeys(input.orderedBranchKeys, "orderedBranchKeys", "branch");
  if (keys.length === 0) throw new TypeError("orderedBranchKeys must not be empty");
  return domainHash("eve:batch:v1", [nonEmpty(input.instanceId, "instanceId"), keys]) as Promise<BatchKey>;
}

export async function deriveRunKey(input: {
  readonly batchKey: BatchKey;
  readonly purpose?: string | undefined;
}): Promise<RunKey> {
  assertKeyKind(input.batchKey, "batch");
  return domainHash("eve:run:v1", [
    input.batchKey,
    nonEmpty(input.purpose ?? "primary", "purpose"),
  ]) as Promise<RunKey>;
}

export async function deriveWakeKey(input: {
  readonly runKey: RunKey;
  readonly routeId: string;
}): Promise<WakeKey> {
  assertKeyKind(input.runKey, "run");
  return domainHash("eve:wake:v1", [input.runKey, nonEmpty(input.routeId, "routeId")]) as Promise<WakeKey>;
}

export function createIdempotencyContext<TKey extends IdempotencyKey>(input: {
  readonly key: TKey;
  readonly inputHash: InputHash;
  readonly parentKeys: readonly IdempotencyKey[];
  readonly eventKeys: readonly EventKey[];
}): IdempotencyContext<TKey> {
  assertNonEmpty(input.key, "idempotency key");
  assertInputHash(input.inputHash);
  const parentKeys = distinctKeys(input.parentKeys, "parentKeys");
  const eventKeys = distinctKeys(input.eventKeys, "eventKeys", "event") as EventKey[];
  if (parentKeys.includes(input.key)) {
    throw new TypeError("parentKeys must not contain the operation key");
  }
  if (eventKeys.length === 0) throw new TypeError("eventKeys must not be empty");
  return deepFreeze({
    key: input.key,
    inputHash: input.inputHash,
    parentKeys,
    eventKeys,
  });
}

export interface MembershipMember<TKey extends IdempotencyKey = IdempotencyKey> {
  readonly key: TKey;
  readonly inputHash: InputHash;
}

export interface FrozenMembership<
  TOperationKey extends IdempotencyKey = IdempotencyKey,
  TMemberKey extends IdempotencyKey = IdempotencyKey,
> {
  readonly operationKey: TOperationKey;
  readonly members: readonly MembershipMember<TMemberKey>[];
  readonly frozenAt: string;
}

/**
 * Builds the immutable value a caller persists during its membership-freeze
 * transaction. The caller is responsible for selecting the durable order and
 * atomically storing this record with the transition to claimed/running.
 *
 * Matching duplicate members collapse to their first position. Reusing one
 * member key with a different input hash fails closed.
 */
export async function freezeMembership<
  TOperationKey extends IdempotencyKey,
  TMemberKey extends IdempotencyKey,
>(input: {
  readonly namespace: string;
  readonly orderedMembers: readonly MembershipMember<TMemberKey>[];
  readonly frozenAt: string;
  readonly deriveOperationKey: (orderedKeys: readonly TMemberKey[]) => TOperationKey | Promise<TOperationKey>;
}): Promise<FrozenMembership<TOperationKey, TMemberKey>> {
  assertNonEmpty(input.namespace, "membership namespace");
  assertTimestamp(input.frozenAt, "frozenAt");
  const byKey = new Map<string, MembershipMember<TMemberKey>>();
  for (const member of input.orderedMembers) {
    assertNonEmpty(member.key, "membership key");
    assertInputHash(member.inputHash);
    const existing = byKey.get(member.key);
    if (existing === undefined) {
      byKey.set(member.key, { key: member.key, inputHash: member.inputHash });
    } else {
      assertIdempotencyInput({
        namespace: input.namespace,
        key: member.key,
        existingInputHash: existing.inputHash,
        receivedInputHash: member.inputHash,
      });
    }
  }
  const members = [...byKey.values()];
  if (members.length === 0) throw new TypeError("membership must not be empty");
  const operationKey = await input.deriveOperationKey(members.map((member) => member.key));
  assertNonEmpty(operationKey, "membership operation key");
  return deepFreeze({ operationKey, members, frozenAt: input.frozenAt });
}

export class IdempotencyConflictError extends Error {
  readonly namespace: string;
  readonly key: string;
  readonly existingInputHash: string;
  readonly receivedInputHash: string;

  constructor(input: {
    readonly namespace: string;
    readonly key: string;
    readonly existingInputHash: string;
    readonly receivedInputHash: string;
  }) {
    super(`idempotency conflict for ${input.namespace}:${input.key}`);
    this.name = "IdempotencyConflictError";
    this.namespace = input.namespace;
    this.key = input.key;
    this.existingInputHash = input.existingInputHash;
    this.receivedInputHash = input.receivedInputHash;
  }
}

export function assertIdempotencyInput(input: {
  readonly namespace: string;
  readonly key: string;
  readonly existingInputHash: string;
  readonly receivedInputHash: string;
}): void {
  if (input.existingInputHash !== input.receivedInputHash) {
    throw new IdempotencyConflictError(input);
  }
}

interface IdempotencyReceiptBase<TKey extends IdempotencyKey> {
  readonly namespace: string;
  readonly key: TKey;
  readonly inputHash: InputHash;
  readonly createdAt: string;
  /** Boundary after which this component no longer promises duplicate recognition. */
  readonly expiresAt: string;
}

export type IdempotencyReceipt<
  TResult extends JsonValue = JsonValue,
  TKey extends IdempotencyKey = IdempotencyKey,
> =
  | (IdempotencyReceiptBase<TKey> & {
      readonly status: "in_progress";
      readonly leaseUntil?: string | undefined;
    })
  | (IdempotencyReceiptBase<TKey> & {
      readonly status: "completed";
      readonly result: TResult;
      readonly completedAt: string;
    })
  | (IdempotencyReceiptBase<TKey> & {
      readonly status: "failed";
      readonly errorClass: string;
      readonly retryable: boolean;
      readonly failedAt: string;
    });

export type IdempotencyBeginResult<
  TResult extends JsonValue = JsonValue,
  TKey extends IdempotencyKey = IdempotencyKey,
> =
  | { readonly status: "new" }
  | {
      /** A matching retryable failure was atomically reserved for another attempt. */
      readonly status: "retry";
      readonly previousReceipt: Extract<
        IdempotencyReceipt<TResult, TKey>,
        { readonly status: "failed" }
      > & { readonly retryable: true };
    }
  | { readonly status: "in_progress"; readonly retryAt?: string | undefined }
  | {
      readonly status: "completed";
      readonly receipt: Extract<IdempotencyReceipt<TResult, TKey>, { readonly status: "completed" }>;
    }
  | {
      /** A matching non-retryable failure is terminal and must not run again. */
      readonly status: "failed";
      readonly receipt: Extract<
        IdempotencyReceipt<TResult, TKey>,
        { readonly status: "failed" }
      > & { readonly retryable: false };
    }
  | { readonly status: "conflict"; readonly existingInputHash: InputHash };

export interface IdempotencyLedger<
  TResult extends JsonValue = JsonValue,
  TKey extends IdempotencyKey = IdempotencyKey,
> {
  /**
   * Atomically reserves `(namespace, key)`. Matching retries observe the
   * existing state; a different input hash returns `conflict` and must not
   * replace the original reservation. Expiry ends this component's guarantee
   * and is deliberately independent of payload retention.
   *
   * `new` means the first or post-expiry attempt was reserved. `retry` means a
   * matching retryable failure was atomically reserved again under the same
   * key. `failed` replays a terminal failure without reacquiring the operation.
   */
  begin(input: {
    readonly namespace: string;
    readonly key: TKey;
    readonly inputHash: InputHash;
    readonly leaseUntil?: string | undefined;
    readonly expiresAt: string;
  }): Promise<IdempotencyBeginResult<TResult, TKey>>;

  complete(input: {
    readonly namespace: string;
    readonly key: TKey;
    readonly inputHash: InputHash;
    readonly result: TResult;
    readonly completedAt: string;
  }): Promise<void>;

  fail(input: {
    readonly namespace: string;
    readonly key: TKey;
    readonly inputHash: InputHash;
    readonly errorClass: string;
    readonly retryable: boolean;
    readonly failedAt: string;
  }): Promise<void>;
}

async function domainHash(domain: string, parts: readonly unknown[]): Promise<string> {
  const crypto = globalThis.crypto;
  if (crypto?.subtle === undefined) throw new Error("Web Crypto SHA-256 is unavailable");
  const encoded = new TextEncoder().encode(canonicalJson([domain, ...parts], `${domain} hash input`));
  const digest = await crypto.subtle.digest("SHA-256", encoded);
  const hex = Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${domain}:${hex}`;
}

function cloneCanonicalEvent<TEvent extends CanonicalChannelEvent>(event: TEvent): TEvent {
  const serialized = canonicalJson(event, "canonical channel event");
  const detached = JSON.parse(serialized) as TEvent;
  assertCanonicalEventShape(detached);
  return deepFreeze(detached);
}

function assertCanonicalEventShape(event: CanonicalChannelEvent): void {
  if (event === null || typeof event !== "object" || Array.isArray(event)) {
    throw new TypeError("canonical channel event must be an object");
  }
  assertExactKeys(event, [
    "actor",
    "authRef",
    "data",
    "id",
    "occurredAt",
    "origin",
    "replyTarget",
    "source",
    "subjects",
    "type",
    "version",
  ], "canonical channel event");
  assertRequiredKeys(event, ["data", "id", "origin", "source", "type", "version"], "canonical channel event");
  assertNonEmpty(event.id, "canonical channel event id");
  assertNonEmpty(event.type, "canonical channel event type");
  assertPositiveInteger(event.version, "canonical channel event version");
  if (event.occurredAt !== undefined) {
    assertTimestamp(event.occurredAt, "canonical channel event occurredAt");
  }
  assertRecord(event.source, "canonical channel event source");
  assertExactKeys(event.source, ["channelId", "installationId", "tenantId"], "canonical channel event source");
  assertRequiredKeys(event.source, ["channelId", "installationId", "tenantId"], "canonical channel event source");
  assertNonEmpty(event.source.channelId, "canonical channel event source.channelId");
  assertNonEmpty(event.source.installationId, "canonical channel event source.installationId");
  assertNonEmpty(event.source.tenantId, "canonical channel event source.tenantId");
  assertRecord(event.origin, "canonical channel event origin");
  assertExactKeys(event.origin, [
    "applicationId",
    "causationId",
    "depth",
    "id",
    "kind",
  ], "canonical channel event origin");
  assertRequiredKeys(event.origin, ["depth", "kind"], "canonical channel event origin");
  if (!(["external", "agent", "monitor", "schedule"] as const).includes(event.origin.kind)) {
    throw new TypeError("canonical channel event origin.kind is invalid");
  }
  if (!Number.isSafeInteger(event.origin.depth) || event.origin.depth < 0) {
    throw new TypeError("canonical channel event origin.depth must be a non-negative safe integer");
  }
  if (event.origin.applicationId !== undefined) {
    assertNonEmpty(event.origin.applicationId, "canonical channel event origin.applicationId");
  }
  if (event.origin.id !== undefined) assertNonEmpty(event.origin.id, "canonical channel event origin.id");
  if (event.origin.causationId !== undefined) {
    assertNonEmpty(event.origin.causationId, "canonical channel event origin.causationId");
  }
  if (event.actor !== undefined) {
    assertRecord(event.actor, "canonical channel event actor");
    assertExactKeys(event.actor, [
      "displayName",
      "id",
      "isBot",
      "knownAgentPrincipal",
      "principalType",
    ], "canonical channel event actor");
    assertRequiredKeys(event.actor, ["id", "principalType"], "canonical channel event actor");
    assertNonEmpty(event.actor.id, "canonical channel event actor.id");
    if (!(["user", "service", "app", "unknown"] as const).includes(event.actor.principalType)) {
      throw new TypeError("canonical channel event actor.principalType is invalid");
    }
    if (event.actor.displayName !== undefined) {
      assertNonEmpty(event.actor.displayName, "canonical channel event actor.displayName");
    }
    if (event.actor.isBot !== undefined && typeof event.actor.isBot !== "boolean") {
      throw new TypeError("canonical channel event actor.isBot must be a boolean");
    }
    if (
      event.actor.knownAgentPrincipal !== undefined &&
      typeof event.actor.knownAgentPrincipal !== "boolean"
    ) {
      throw new TypeError("canonical channel event actor.knownAgentPrincipal must be a boolean");
    }
  }
  if (event.authRef !== undefined) assertNonEmpty(event.authRef, "canonical channel event authRef");
  if (event.subjects !== undefined && !Array.isArray(event.subjects)) {
    throw new TypeError("canonical channel event subjects must be an array");
  }
  const subjects = new Set<string>();
  for (const [index, subject] of (event.subjects ?? []).entries()) {
    assertRecord(subject, `canonical channel event subjects[${index}]`);
    assertExactKeys(subject, ["key", "namespace"], `canonical channel event subjects[${index}]`);
    assertRequiredKeys(subject, ["key", "namespace"], `canonical channel event subjects[${index}]`);
    assertNonEmpty(subject.namespace, `canonical channel event subjects[${index}].namespace`);
    assertNonEmpty(subject.key, `canonical channel event subjects[${index}].key`);
    const identity = canonicalJson([subject.namespace, subject.key]);
    if (subjects.has(identity)) {
      throw new TypeError("canonical channel event subjects must contain distinct identities");
    }
    subjects.add(identity);
  }
}

function distinctKeys<TKey extends string>(
  keys: readonly TKey[],
  name: string,
  kind?: string,
): TKey[] {
  const output: TKey[] = [];
  const seen = new Set<string>();
  for (const key of keys) {
    assertNonEmpty(key, `${name} key`);
    if (kind !== undefined) assertKeyKind(key, kind);
    if (seen.has(key)) throw new TypeError(`${name} must contain distinct keys`);
    seen.add(key);
    output.push(key);
  }
  return output;
}

function assertKeyKind(value: string, kind: string): void {
  const prefix = `eve:${kind}:v1:`;
  if (!new RegExp(`^${escapeRegExp(prefix)}[0-9a-f]{64}$`).test(value)) {
    throw new TypeError(`expected ${kind} idempotency key`);
  }
}

function assertInputHash(value: string): asserts value is InputHash {
  assertKeyKind(value, "input");
}

function assertPositiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) throw new TypeError(`${name} must be a positive safe integer`);
}

function nonEmpty(value: string, name: string): string {
  assertNonEmpty(value, name);
  return value;
}

function assertNonEmpty(value: unknown, name: string): asserts value is string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${name} must not be empty`);
  }
}

function assertTimestamp(value: string, name: string): void {
  const milliseconds = Date.parse(value);
  if (!Number.isFinite(milliseconds) || new Date(milliseconds).toISOString() !== value) {
    throw new TypeError(`${name} must be a canonical ISO timestamp`);
  }
}

function assertRecord(value: unknown, name: string): asserts value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${name} must be an object`);
  }
}

function assertExactKeys(value: object, allowed: readonly string[], name: string): void {
  const allowedKeys = new Set(allowed);
  const unexpected = Object.keys(value).filter((key) => !allowedKeys.has(key));
  if (unexpected.length > 0) {
    throw new TypeError(`${name} contains unsupported fields: ${unexpected.sort().join(", ")}`);
  }
}

function assertRequiredKeys(value: object, required: readonly string[], name: string): void {
  const missing = required.filter((key) => !Object.prototype.hasOwnProperty.call(value, key));
  if (missing.length > 0) {
    throw new TypeError(`${name} is missing required fields: ${missing.sort().join(", ")}`);
  }
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object") {
    Object.freeze(value);
    for (const nested of Object.values(value)) deepFreeze(nested);
  }
  return value;
}
