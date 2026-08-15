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
declare const fanoutManifestHashBrand: unique symbol;

export type IdempotencyKey<TKind extends string = string> = string & {
  readonly [idempotencyKeyBrand]: TKind;
};
export type EventKey = IdempotencyKey<"event">;
export type OccurrenceKey = IdempotencyKey<"occurrence">;
export type DirectDispatchKey = IdempotencyKey<"direct-dispatch">;
export type BranchKey = IdempotencyKey<"branch">;
export type AttentionPartitionKey = IdempotencyKey<"partition">;
export type AttentionInstanceKey = IdempotencyKey<"instance">;
export type BatchKey = IdempotencyKey<"batch">;
export type RunKey = IdempotencyKey<"run">;
export type WakeKey = IdempotencyKey<"wake">;
export type InputHash = string & { readonly [inputHashBrand]: true };
export type FanoutManifestHash = string & { readonly [fanoutManifestHashBrand]: true };
export type IdempotencyKeyKind =
  | "event"
  | "occurrence"
  | "direct-dispatch"
  | "branch"
  | "partition"
  | "instance"
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
  /** Stable channel-owned outer serialization boundary, independent of rules. */
  readonly partitionKey: string;
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
  readonly canonicalize: (raw: TRaw) => TEvent | Promise<TEvent>;
  /** Maps every equivalent provider retry to the same bounded durable partition. */
  readonly partitionKey: (event: TEvent) => string;
}

export interface ChannelInputParser<TInput> {
  readonly parse: (value: unknown) => TInput;
}

export interface StandardChannelSchema<TInput = unknown, TOutput = TInput> {
  readonly "~standard": {
    readonly version: 1;
    readonly validate: (
      value: unknown,
    ) =>
      | { readonly value: TOutput; readonly issues?: undefined }
      | { readonly issues: readonly { readonly message: string }[] }
      | Promise<
          | { readonly value: TOutput; readonly issues?: undefined }
          | { readonly issues: readonly { readonly message: string }[] }
        >;
    readonly types?: {
      readonly input: TInput;
      readonly output: TOutput;
    } | undefined;
  };
}

type ChannelSchema = ChannelInputParser<unknown> | StandardChannelSchema<unknown, unknown>;
type ChannelSchemaInput<TSchema> = TSchema extends StandardChannelSchema<infer TInput, unknown>
  ? TInput
  : TSchema extends ChannelInputParser<infer TInput>
    ? TInput
    : never;
type ChannelSchemaOutput<TSchema> = TSchema extends StandardChannelSchema<unknown, infer TOutput>
  ? TOutput
  : TSchema extends ChannelInputParser<infer TOutput>
    ? TOutput
    : never;

export type ChannelEvent<TChannel> = TChannel extends ChannelCanonicalizationContract<
  never,
  infer TEvent
>
  ? TEvent
  : never;

/**
 * Defines a typed channel from Standard Schema (including Zod) or a parser and
 * one deterministic mapping, so consumers do not repeat the input type.
 */
export function defineChannel<
  TSchema extends ChannelSchema,
  TMap extends (input: ChannelSchemaOutput<TSchema>) => CanonicalChannelEvent,
>(options: {
  readonly version: number;
  readonly input: TSchema;
  readonly map: TMap;
  readonly partitionKey: (event: ReturnType<TMap>) => string;
}): ChannelCanonicalizationContract<ChannelSchemaInput<TSchema>, ReturnType<TMap>> {
  if (options.input === null || typeof options.input !== "object" || !isChannelSchema(options.input)) {
    throw new TypeError("channel input must implement Standard Schema or define parse");
  }
  if (typeof options.map !== "function") {
    throw new TypeError("channel map must be a function");
  }
  return defineChannelCanonicalization<
    ChannelSchemaInput<TSchema>,
    ReturnType<TMap>
  >({
    version: options.version,
    partitionKey: options.partitionKey,
    async canonicalize(raw) {
      return options.map(
        await parseChannelInput(options.input, raw),
      ) as ReturnType<TMap>;
    },
  });
}

export function defineChannelCanonicalization<
  TRaw,
  TEvent extends CanonicalChannelEvent,
>(options: ChannelCanonicalizationContract<TRaw, TEvent>): ChannelCanonicalizationContract<TRaw, TEvent> {
  assertPositiveInteger(options.version, "channel canonicalization version");
  if (typeof options.canonicalize !== "function") {
    throw new TypeError("channel canonicalization canonicalize must be a function");
  }
  if (typeof options.partitionKey !== "function") {
    throw new TypeError("channel canonicalization partitionKey must be a function");
  }
  return Object.freeze({
    version: options.version,
    canonicalize: options.canonicalize,
    partitionKey: options.partitionKey,
  });
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
  const event = cloneCanonicalEvent(await contract.canonicalize(raw));
  const partitionKey = nonEmpty(contract.partitionKey(event), "channel partitionKey");
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
    partitionKey,
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

function isChannelSchema(value: object): value is ChannelSchema {
  if ("~standard" in value) {
    const standard = value["~standard"];
    return (
      standard !== null &&
      typeof standard === "object" &&
      "validate" in standard &&
      typeof standard.validate === "function"
    );
  }
  return "parse" in value && typeof value.parse === "function";
}

async function parseChannelInput<TSchema extends ChannelSchema>(
  schema: TSchema,
  value: ChannelSchemaInput<TSchema>,
): Promise<ChannelSchemaOutput<TSchema>> {
  if ("~standard" in schema) {
    const result = await schema["~standard"].validate(value);
    if ("issues" in result && result.issues !== undefined) {
      const detail = result.issues.map((issue) => issue.message).join("; ");
      throw new TypeError(`channel input is invalid${detail.length === 0 ? "" : `: ${detail}`}`);
    }
    if (!("value" in result)) throw new TypeError("channel input validation returned no value");
    return result.value as ChannelSchemaOutput<TSchema>;
  }
  return schema.parse(value) as ChannelSchemaOutput<TSchema>;
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

/** Deterministic accepted occurrence for the canonical source input. */
export async function deriveOccurrenceKey(input: {
  readonly eventKey: EventKey;
  readonly inputHash: InputHash;
}): Promise<OccurrenceKey> {
  assertKeyKind(input.eventKey, "event");
  assertInputHash(input.inputHash);
  return domainHash("eve:occurrence:v1", [
    input.eventKey,
    input.inputHash,
  ]) as Promise<OccurrenceKey>;
}

/** Direct-dispatch identity rooted in the accepted occurrence. */
export async function deriveAttentionDirectDispatchKey(input: {
  readonly occurrenceKey: OccurrenceKey;
  readonly bindingGeneration: string;
}): Promise<DirectDispatchKey> {
  assertKeyKind(input.occurrenceKey, "occurrence");
  return domainHash("eve:direct-dispatch:v2", [
    input.occurrenceKey,
    nonEmpty(input.bindingGeneration, "bindingGeneration"),
  ]) as Promise<DirectDispatchKey>;
}

/** Channel-owned bounded durable serialization boundary. */
export async function deriveAttentionPartitionKey(input: {
  readonly applicationId: string;
  readonly tenantId: string;
  readonly channelId: string;
  readonly installationId: string;
  readonly partitionKey: string;
}): Promise<AttentionPartitionKey> {
  return domainHash("eve:partition:v1", [
    nonEmpty(input.applicationId, "applicationId"),
    nonEmpty(input.tenantId, "tenantId"),
    nonEmpty(input.channelId, "channelId"),
    nonEmpty(input.installationId, "installationId"),
    nonEmpty(input.partitionKey, "partitionKey"),
  ]) as Promise<AttentionPartitionKey>;
}

/** Full-branch identity. */
export async function deriveAttentionBranchKey(input: {
  readonly occurrenceKey: OccurrenceKey;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly phase?: MonitorPhase | undefined;
  readonly correlationKey: string;
}): Promise<BranchKey> {
  assertKeyKind(input.occurrenceKey, "occurrence");
  return domainHash("eve:branch:v2", [
    input.occurrenceKey,
    nonEmpty(input.monitorId, "monitorId"),
    nonEmpty(input.definitionVersion, "definitionVersion"),
    input.phase ?? null,
    nonEmpty(input.correlationKey, "correlationKey"),
  ]) as Promise<BranchKey>;
}

/** Serialized correlation-workflow identity. */
export async function deriveAttentionInstanceKey(input: {
  readonly partitionCellKey: AttentionPartitionKey;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly correlationKey: string;
}): Promise<AttentionInstanceKey> {
  assertKeyKind(input.partitionCellKey, "partition");
  return domainHash("eve:instance:v3", [
    input.partitionCellKey,
    nonEmpty(input.monitorId, "monitorId"),
    nonEmpty(input.definitionVersion, "definitionVersion"),
    nonEmpty(input.correlationKey, "correlationKey"),
  ]) as Promise<AttentionInstanceKey>;
}

/** Batch identity from canonical frozen membership. */
export async function deriveAttentionBatchKey(input: {
  readonly instanceKey: AttentionInstanceKey;
  readonly orderedBranchKeys: readonly BranchKey[];
}): Promise<BatchKey> {
  assertKeyKind(input.instanceKey, "instance");
  const keys = distinctKeys(input.orderedBranchKeys, "orderedBranchKeys", "branch");
  if (keys.length === 0) throw new TypeError("orderedBranchKeys must not be empty");
  return domainHash("eve:batch:v2", [input.instanceKey, keys]) as Promise<BatchKey>;
}

export async function deriveAttentionRunKey(input: {
  readonly batchKey: BatchKey;
  readonly purpose?: string | undefined;
}): Promise<RunKey> {
  assertKeyKind(input.batchKey, "batch");
  return domainHash("eve:run:v2", [
    input.batchKey,
    nonEmpty(input.purpose ?? "primary", "purpose"),
  ]) as Promise<RunKey>;
}

export async function deriveAttentionWakeKey(input: {
  readonly runKey: RunKey;
  readonly routeId: string;
}): Promise<WakeKey> {
  assertKeyKind(input.runKey, "run");
  return domainHash("eve:wake:v2", [
    input.runKey,
    nonEmpty(input.routeId, "routeId"),
  ]) as Promise<WakeKey>;
}

/** Hashes a complete ordered fan-out, including the empty-manifest outcome. */
export async function deriveFanoutManifestHash(input: {
  readonly occurrenceKey: OccurrenceKey;
  readonly orderedBranches: readonly {
    readonly branchKey: BranchKey;
    readonly inputHash: InputHash;
  }[];
}): Promise<FanoutManifestHash> {
  assertKeyKind(input.occurrenceKey, "occurrence");
  const seen = new Set<string>();
  let previousBranchKey: string | undefined;
  const branches = input.orderedBranches.map((branch) => {
    assertKeyKind(branch.branchKey, "branch");
    assertInputHash(branch.inputHash);
    if (seen.has(branch.branchKey)) {
      throw new TypeError("orderedBranches must contain distinct branch keys");
    }
    seen.add(branch.branchKey);
    if (previousBranchKey !== undefined && previousBranchKey > branch.branchKey) {
      throw new TypeError("orderedBranches must be ordered by branchKey");
    }
    previousBranchKey = branch.branchKey;
    return [branch.branchKey, branch.inputHash] as const;
  });
  return domainHash("eve:fanout:v1", [
    input.occurrenceKey,
    branches,
  ]) as Promise<FanoutManifestHash>;
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
  const versions: Readonly<Record<string, readonly number[]>> = {
    event: [1],
    occurrence: [1],
    "direct-dispatch": [2],
    branch: [2],
    partition: [1],
    instance: [3],
    batch: [2],
    run: [2],
    wake: [2],
    input: [1],
  };
  const allowed = versions[kind] ?? [1];
  const version = allowed.map(String).join("|");
  const prefix = `eve:${kind}:v`;
  if (!new RegExp(`^${escapeRegExp(prefix)}(?:${version}):[0-9a-f]{64}$`).test(value)) {
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
