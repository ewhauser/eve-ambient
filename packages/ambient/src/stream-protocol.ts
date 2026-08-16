import {
  validateAcceptedFanout,
  validateFullAttentionBranch,
  type AcceptedFanout,
  type FullAttentionBranch,
} from "./attention.js";
import {
  assertIdempotencyInput,
  deriveAttentionInstanceKey,
  deriveAttentionPartitionKey,
  hashIdempotencyInput,
  parseIdempotencyKey,
  parseInputHash,
  type AttentionInstanceKey,
  type EventKey,
  type InputHash,
} from "./idempotency.js";
import { canonicalJson } from "./canonical.js";

/** One atomic append sent to a single correlation-owned stream. */
export interface AttentionStreamAppend {
  readonly streamKey: AttentionInstanceKey;
  readonly eventKey: EventKey;
  readonly inputHash: InputHash;
  readonly branches: readonly FullAttentionBranch[];
}

/** Payload-free result retained in the stream's bounded recent-message ring. */
export interface AttentionStreamAppendReceipt {
  readonly streamKey: AttentionInstanceKey;
  readonly eventKey: EventKey;
  readonly inputHash: InputHash;
  readonly status: "appended" | "duplicate";
  readonly acceptedAt: string;
}

/** Groups one accepted event into one append per distinct correlation stream. */
export async function compileAttentionStreamAppends(
  input: AcceptedFanout,
): Promise<readonly AttentionStreamAppend[]> {
  const fanout = await validateAcceptedFanout(input);
  const groups = new Map<AttentionInstanceKey, FullAttentionBranch[]>();
  for (const branch of fanout.branches) {
    const streamKey = await streamKeyForBranch(branch);
    const group = groups.get(streamKey) ?? [];
    group.push(branch);
    groups.set(streamKey, group);
  }
  const appends = await Promise.all(
    [...groups]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(async ([streamKey, branches]) => {
        branches.sort((left, right) => left.branchKey.localeCompare(right.branchKey));
        assertOneStreamPolicy(streamKey, branches);
        const logical = {
          streamKey,
          eventKey: fanout.eventKey,
          branches,
        };
        return deepFreeze({
          ...logical,
          inputHash: await hashIdempotencyInput(logical),
        });
      }),
  );
  return deepFreeze(appends);
}

/** Recomputes the correlation address and complete append hash. */
export async function validateAttentionStreamAppend(
  input: AttentionStreamAppend,
): Promise<AttentionStreamAppend> {
  assertRecord(input, "attention stream append");
  assertExactKeys(
    input,
    ["branches", "eventKey", "inputHash", "streamKey"],
    "attention stream append",
  );
  const streamKey = parseIdempotencyKey("instance", input.streamKey);
  const eventKey = parseIdempotencyKey("event", input.eventKey);
  const claimedHash = parseInputHash(input.inputHash);
  if (!Array.isArray(input.branches) || input.branches.length === 0) {
    throw new TypeError("attention stream append branches must not be empty");
  }
  const branches = await Promise.all(
    input.branches.map(async (candidate) => {
      const branch = await validateFullAttentionBranch(candidate);
      if (branch.eventKey !== eventKey) {
        throw new TypeError("attention stream append contains a different eventKey");
      }
      if ((await streamKeyForBranch(branch)) !== streamKey) {
        throw new TypeError("attention stream append branch does not match streamKey");
      }
      return branch;
    }),
  );
  branches.sort((left, right) => left.branchKey.localeCompare(right.branchKey));
  assertDistinctBranchKeys(branches);
  assertOneStreamPolicy(streamKey, branches);
  const logical = { streamKey, eventKey, branches };
  const expectedHash = await hashIdempotencyInput(logical);
  assertIdempotencyInput({
    namespace: "attention-stream-append",
    key: eventKey,
    existingInputHash: expectedHash,
    receivedInputHash: claimedHash,
  });
  return deepFreeze({ ...logical, inputHash: expectedHash });
}

export async function streamKeyForBranch(
  branch: FullAttentionBranch,
): Promise<AttentionInstanceKey> {
  const partitionCellKey = await deriveAttentionPartitionKey({
    applicationId: branch.applicationId,
    tenantId: branch.tenantId,
    channelId: branch.event.source.channelId,
    installationId: branch.event.source.installationId,
    partitionKey: branch.partitionKey,
  });
  return deriveAttentionInstanceKey({
    partitionCellKey,
    monitorId: branch.monitorId,
    definitionVersion: branch.definitionVersion,
    correlationKey: branch.correlationKey,
  });
}

function assertOneStreamPolicy(
  streamKey: AttentionInstanceKey,
  branches: readonly FullAttentionBranch[],
): void {
  const first = branches[0];
  if (first === undefined) throw new TypeError("attention stream append branches must not be empty");
  const identity = canonicalJson({ mode: first.mode, policy: first.policy });
  for (const branch of branches.slice(1)) {
    if (canonicalJson({ mode: branch.mode, policy: branch.policy }) !== identity) {
      throw new TypeError(`attention stream ${streamKey} received multiple policies`);
    }
  }
}

function assertDistinctBranchKeys(branches: readonly FullAttentionBranch[]): void {
  const keys = new Set<string>();
  for (const branch of branches) {
    if (keys.has(branch.branchKey)) {
      throw new TypeError("attention stream append branches must have distinct branch keys");
    }
    keys.add(branch.branchKey);
  }
}

function assertRecord(value: unknown, name: string): asserts value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${name} must be an object`);
  }
}

function assertExactKeys(
  value: Record<string, unknown>,
  allowed: readonly string[],
  name: string,
): void {
  const supported = new Set(allowed);
  const extras = Object.keys(value).filter((key) => !supported.has(key));
  if (extras.length > 0) throw new TypeError(`${name} contains unsupported fields`);
  for (const key of allowed) {
    if (!(key in value)) throw new TypeError(`${name} is missing ${key}`);
  }
}

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object") {
    Object.freeze(value);
    for (const nested of Object.values(value)) deepFreeze(nested);
  }
  return value;
}
