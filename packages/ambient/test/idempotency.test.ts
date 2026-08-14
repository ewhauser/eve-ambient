import { describe, expect, it } from "vitest";
import {
  IdempotencyConflictError,
  assertIdempotencyInput,
  canonicalizeChannelDelivery,
  defineChannelCanonicalization,
  deriveAttentionBatchKey,
  deriveAttentionBranchKey,
  deriveAttentionInstanceKey,
  deriveAttentionRunKey,
  deriveAttentionWakeKey,
  deriveFanoutManifestHash,
  deriveOccurrenceKey,
  freezeMembership,
  hashIdempotencyInput,
  parseIdempotencyKey,
} from "../src/index.js";

const channel = defineChannelCanonicalization({
  version: 1,
  canonicalize: (raw: { readonly id: string; readonly text: string }) => ({
    id: raw.id,
    type: "message.changed",
    version: 1,
    data: { text: raw.text },
    source: {
      channelId: "slack",
      installationId: "installation-1",
      tenantId: "tenant-1",
    },
    origin: { kind: "external" as const, depth: 0 },
  }),
});

describe("attention idempotency lineage", () => {
  it("canonicalizes equivalent deliveries to one event and occurrence identity", async () => {
    const first = await canonicalizeChannelDelivery(channel, { id: "event-1", text: "hello" }, {
      applicationId: "app",
    });
    const retry = await canonicalizeChannelDelivery(channel, { id: "event-1", text: "hello" }, {
      applicationId: "app",
    });

    expect(retry).toEqual(first);
    await expect(
      deriveOccurrenceKey({
        eventKey: first.idempotency.key,
        inputHash: first.idempotency.inputHash,
      }),
    ).resolves.toMatch(/^eve:occurrence:v1:[0-9a-f]{64}$/);
  });

  it("keeps the source key while changing the input hash for conflicting payloads", async () => {
    const first = await canonicalizeChannelDelivery(channel, { id: "event-1", text: "hello" }, {
      applicationId: "app",
    });
    const conflict = await canonicalizeChannelDelivery(channel, { id: "event-1", text: "changed" }, {
      applicationId: "app",
    });

    expect(conflict.idempotency.key).toBe(first.idempotency.key);
    expect(conflict.idempotency.inputHash).not.toBe(first.idempotency.inputHash);
    expect(() =>
      assertIdempotencyInput({
        namespace: "source",
        key: first.idempotency.key,
        existingInputHash: first.idempotency.inputHash,
        receivedInputHash: conflict.idempotency.inputHash,
      }),
    ).toThrow(IdempotencyConflictError);
  });

  it("derives only the v2 branch through wake chain", async () => {
    const source = await canonicalizeChannelDelivery(channel, { id: "event-1", text: "hello" }, {
      applicationId: "app",
    });
    const occurrenceKey = await deriveOccurrenceKey({
      eventKey: source.idempotency.key,
      inputHash: source.idempotency.inputHash,
    });
    const branchKey = await deriveAttentionBranchKey({
      occurrenceKey,
      monitorId: "incident",
      definitionVersion: "v1",
      correlationKey: "incident-42",
    });
    const instanceKey = await deriveAttentionInstanceKey({
      applicationId: "app",
      tenantId: "tenant-1",
      monitorId: "incident",
      definitionVersion: "v1",
      correlationKey: "incident-42",
    });
    const batchKey = await deriveAttentionBatchKey({ instanceKey, orderedBranchKeys: [branchKey] });
    const runKey = await deriveAttentionRunKey({ batchKey });
    const wakeKey = await deriveAttentionWakeKey({ runKey, routeId: "eve" });

    expect(branchKey).toMatch(/^eve:branch:v2:/);
    expect(instanceKey).toMatch(/^eve:instance:v2:/);
    expect(batchKey).toMatch(/^eve:batch:v2:/);
    expect(runKey).toMatch(/^eve:run:v2:/);
    expect(wakeKey).toMatch(/^eve:wake:v2:/);
    expect(() => parseIdempotencyKey("branch", `eve:branch:v1:${"0".repeat(64)}`)).toThrow();
  });

  it("freezes canonical membership and rejects conflicting duplicate members", async () => {
    const instanceKey = await deriveAttentionInstanceKey({
      applicationId: "app",
      tenantId: "tenant-1",
      monitorId: "incident",
      definitionVersion: "v1",
      correlationKey: "incident-42",
    });
    const eventHash = await hashIdempotencyInput({ event: 1 });
    const otherHash = await hashIdempotencyInput({ event: 2 });
    const source = await canonicalizeChannelDelivery(channel, { id: "event-1", text: "hello" }, {
      applicationId: "app",
    });
    const occurrenceKey = await deriveOccurrenceKey({
      eventKey: source.idempotency.key,
      inputHash: source.idempotency.inputHash,
    });
    const branchKey = await deriveAttentionBranchKey({
      occurrenceKey,
      monitorId: "incident",
      definitionVersion: "v1",
      correlationKey: "incident-42",
    });

    await expect(
      freezeMembership({
        namespace: "batch",
        orderedMembers: [
          { key: branchKey, inputHash: eventHash },
          { key: branchKey, inputHash: otherHash },
        ],
        frozenAt: "2026-01-01T00:00:00.000Z",
        deriveOperationKey: (keys) =>
          deriveAttentionBatchKey({ instanceKey, orderedBranchKeys: keys }),
      }),
    ).rejects.toBeInstanceOf(IdempotencyConflictError);
  });

  it("hashes an empty fan-out manifest as a durable no-work result", async () => {
    const source = await canonicalizeChannelDelivery(channel, { id: "event-1", text: "hello" }, {
      applicationId: "app",
    });
    const occurrenceKey = await deriveOccurrenceKey({
      eventKey: source.idempotency.key,
      inputHash: source.idempotency.inputHash,
    });
    await expect(
      deriveFanoutManifestHash({ occurrenceKey, orderedBranches: [] }),
    ).resolves.toMatch(/^eve:fanout:v1:[0-9a-f]{64}$/);
  });
});
