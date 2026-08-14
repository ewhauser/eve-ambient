import { describe, expect, it, vi } from "vitest";
import {
  IdempotencyConflictError,
  assertIdempotencyInput,
  canonicalizeChannelDelivery,
  createIdempotencyContext,
  defineChannelCanonicalization,
  deriveBatchKey,
  deriveBranchKey,
  deriveDirectDispatchKey,
  deriveEventKey,
  deriveRunKey,
  deriveWakeKey,
  freezeMembership,
  hashIdempotencyInput,
  parseIdempotencyKey,
  parseInputHash,
  type CanonicalChannelEvent,
  type JsonValue,
} from "../src/index.js";
import { assertChannelCanonicalization } from "../src/testing.js";

describe("idempotency identity", () => {
  it("derives stable, domain-separated keys without delimiter ambiguity", async () => {
    const input = {
      tenantId: "tenant:a",
      applicationId: "app",
      channelId: "slack",
      installationId: "workspace",
      sourceEventId: "evt:1",
    };
    const eventKey = await deriveEventKey(input);

    expect(eventKey).toBe(await deriveEventKey({ ...input }));
    expect(eventKey).toMatch(/^eve:event:v1:[0-9a-f]{64}$/);
    await expect(deriveEventKey({
      ...input,
      tenantId: "tenant",
      applicationId: "a:app",
    })).resolves.not.toBe(eventKey);

    const directDispatchKey = await deriveDirectDispatchKey({
      eventKey,
      bindingGeneration: "generation-1",
    });
    const branchKey = await deriveBranchKey({
      eventKey,
      monitorId: "ambient-engineering",
      definitionVersion: "v1",
      phase: "observed",
    });
    expect(directDispatchKey).toMatch(/^eve:direct-dispatch:v1:[0-9a-f]{64}$/);
    expect(branchKey).toMatch(/^eve:branch:v1:[0-9a-f]{64}$/);
    expect(directDispatchKey).not.toBe(branchKey);
    expect(parseIdempotencyKey("event", eventKey)).toBe(eventKey);
    expect(() => parseIdempotencyKey("branch", eventKey)).toThrow(
      "expected branch idempotency key",
    );
    await expect(deriveEventKey({
      tenantId: "tenant",
      applicationId: "app",
      channelId: "slack",
      installationId: "workspace",
      sourceEventId: "event",
    })).resolves.toBe(
      "eve:event:v1:d2610cc50b0c1e5ff635bc49e379591f5cf040ee868cdc72d92eb2a51d63f517",
    );
  });

  it("binds canonical JSON semantics while ignoring object field order", async () => {
    const first = await hashIdempotencyInput({
      z: [1, { beta: true, alpha: "x" }],
      a: { right: null, left: 2 },
    });
    const reordered = await hashIdempotencyInput({
      a: { left: 2, right: null },
      z: [1, { alpha: "x", beta: true }],
    });
    const reorderedArray = await hashIdempotencyInput({
      a: { left: 2, right: null },
      z: [{ alpha: "x", beta: true }, 1],
    });

    expect(first).toBe(reordered);
    expect(first).not.toBe(reorderedArray);
    expect(first).toMatch(/^eve:input:v1:[0-9a-f]{64}$/);
    expect(parseInputHash(first)).toBe(first);
    expect(() => parseInputHash("eve:input:v1:not-a-hash")).toThrow(
      "expected input idempotency key",
    );
    await expect(hashIdempotencyInput({ a: 1, b: "two" })).resolves.toBe(
      "eve:input:v1:41d8527e0ee4e8decaa292b8886fd099a3a38a0d07a9c12b35499c19709864c2",
    );
    await expect(hashIdempotencyInput({ unsafe: undefined })).rejects.toThrow(
      "must be JSON-safe",
    );
    await expect(hashIdempotencyInput({ unsafe: Number.NaN })).rejects.toThrow(
      "finite numbers",
    );
    const prototypeKey = JSON.parse('{"__proto__":"kept-as-data"}') as JsonValue;
    await expect(hashIdempotencyInput(prototypeKey)).resolves.not.toBe(
      await hashIdempotencyInput({}),
    );
  });

  it("derives batch, run, and wake identity from frozen ordered membership", async () => {
    const eventKey = await deriveEventKey({
      tenantId: "tenant",
      applicationId: "app",
      channelId: "slack",
      installationId: "workspace",
      sourceEventId: "event",
    });
    const first = await deriveBranchKey({
      eventKey,
      monitorId: "monitor",
      definitionVersion: "v1",
    });
    const second = await deriveBranchKey({
      eventKey,
      monitorId: "monitor",
      definitionVersion: "v2",
    });
    const batchKey = await deriveBatchKey({
      instanceId: "instance",
      orderedBranchKeys: [first, second],
    });
    const reversed = await deriveBatchKey({
      instanceId: "instance",
      orderedBranchKeys: [second, first],
    });
    const runKey = await deriveRunKey({ batchKey });
    const wakeKey = await deriveWakeKey({ runKey, routeId: "route-1" });

    expect(batchKey).toMatch(/^eve:batch:v1:[0-9a-f]{64}$/);
    expect(reversed).not.toBe(batchKey);
    expect(runKey).toMatch(/^eve:run:v1:[0-9a-f]{64}$/);
    expect(wakeKey).toMatch(/^eve:wake:v1:[0-9a-f]{64}$/);
    await expect(deriveBatchKey({
      instanceId: "instance",
      orderedBranchKeys: [first, first],
    })).rejects.toThrow("must contain distinct keys");
  });

  it("fails closed when one key is rebound to a different input hash", async () => {
    const key = await deriveEventKey({
      tenantId: "tenant",
      applicationId: "app",
      channelId: "slack",
      installationId: "workspace",
      sourceEventId: "event",
    });
    const existingInputHash = await hashIdempotencyInput({ text: "original" });
    const receivedInputHash = await hashIdempotencyInput({ text: "changed" });

    expect(() => assertIdempotencyInput({
      namespace: "ingress",
      key,
      existingInputHash,
      receivedInputHash: existingInputHash,
    })).not.toThrow();
    expect(() => assertIdempotencyInput({
      namespace: "ingress",
      key,
      existingInputHash,
      receivedInputHash,
    })).toThrow(IdempotencyConflictError);
    try {
      assertIdempotencyInput({
        namespace: "ingress",
        key,
        existingInputHash,
        receivedInputHash,
      });
    } catch (error) {
      expect(error).toMatchObject({
        name: "IdempotencyConflictError",
        namespace: "ingress",
        key,
        existingInputHash,
        receivedInputHash,
      });
      expect((error as Error).message).not.toContain(receivedInputHash);
    }
  });

  it("freezes ordered distinct members into one immutable operation", async () => {
    const eventKey = await deriveEventKey({
      tenantId: "tenant",
      applicationId: "app",
      channelId: "slack",
      installationId: "workspace",
      sourceEventId: "event",
    });
    const first = await deriveBranchKey({
      eventKey,
      monitorId: "monitor-a",
      definitionVersion: "v1",
    });
    const second = await deriveBranchKey({
      eventKey,
      monitorId: "monitor-b",
      definitionVersion: "v1",
    });
    const firstHash = await hashIdempotencyInput({ branch: "a" });
    const secondHash = await hashIdempotencyInput({ branch: "b" });
    const deriveOperationKey = vi.fn((orderedBranchKeys: readonly typeof first[]) =>
      deriveBatchKey({ instanceId: "instance", orderedBranchKeys }));

    const frozen = await freezeMembership({
      namespace: "monitor-batch:instance",
      orderedMembers: [
        { key: second, inputHash: secondHash },
        { key: first, inputHash: firstHash },
        { key: second, inputHash: secondHash },
      ],
      frozenAt: "2026-08-13T22:00:00.000Z",
      deriveOperationKey,
    });

    expect(frozen.members).toEqual([
      { key: second, inputHash: secondHash },
      { key: first, inputHash: firstHash },
    ]);
    expect(deriveOperationKey).toHaveBeenCalledWith([second, first]);
    expect(Object.isFrozen(frozen)).toBe(true);
    expect(Object.isFrozen(frozen.members)).toBe(true);
    expect(Object.isFrozen(frozen.members[0])).toBe(true);

    const recovered = await freezeMembership({
      namespace: "monitor-batch:instance",
      orderedMembers: [
        { key: second, inputHash: secondHash },
        { key: first, inputHash: firstHash },
      ],
      frozenAt: "2026-08-13T22:00:00.000Z",
      deriveOperationKey,
    });
    expect(recovered).toEqual(frozen);
  });

  it("rejects conflicting duplicate membership before deriving an operation key", async () => {
    const eventKey = await deriveEventKey({
      tenantId: "tenant",
      applicationId: "app",
      channelId: "slack",
      installationId: "workspace",
      sourceEventId: "event",
    });
    const branchKey = await deriveBranchKey({
      eventKey,
      monitorId: "monitor",
      definitionVersion: "v1",
    });
    const original = await hashIdempotencyInput({ text: "original" });
    const changed = await hashIdempotencyInput({ text: "changed" });
    const deriveOperationKey = vi.fn((orderedBranchKeys: readonly typeof branchKey[]) =>
      deriveBatchKey({ instanceId: "instance", orderedBranchKeys }));

    await expect(freezeMembership({
      namespace: "monitor-batch:instance",
      orderedMembers: [
        { key: branchKey, inputHash: original },
        { key: branchKey, inputHash: changed },
      ],
      frozenAt: "2026-08-13T22:00:00.000Z",
      deriveOperationKey,
    })).rejects.toThrow(IdempotencyConflictError);
    expect(deriveOperationKey).not.toHaveBeenCalled();
  });

  it("creates immutable lineage contexts with distinct causes", async () => {
    const eventKey = await deriveEventKey({
      tenantId: "tenant",
      applicationId: "app",
      channelId: "slack",
      installationId: "workspace",
      sourceEventId: "event",
    });
    const branchKey = await deriveBranchKey({
      eventKey,
      monitorId: "monitor",
      definitionVersion: "v1",
    });
    const inputHash = await hashIdempotencyInput({ text: "hello" });
    const context = createIdempotencyContext({
      key: branchKey,
      inputHash,
      parentKeys: [eventKey],
      eventKeys: [eventKey],
    });

    expect(context).toEqual({
      key: branchKey,
      inputHash,
      parentKeys: [eventKey],
      eventKeys: [eventKey],
    });
    expect(Object.isFrozen(context.parentKeys)).toBe(true);
    expect(() => createIdempotencyContext({
      key: branchKey,
      inputHash,
      parentKeys: [eventKey, eventKey],
      eventKeys: [eventKey],
    })).toThrow("parentKeys must contain distinct keys");
    expect(() => createIdempotencyContext({
      key: branchKey,
      inputHash,
      parentKeys: [branchKey],
      eventKeys: [eventKey],
    })).toThrow("parentKeys must not contain the operation key");
    expect(() => createIdempotencyContext({
      key: branchKey,
      inputHash,
      parentKeys: [eventKey],
      eventKeys: [],
    })).toThrow("eventKeys must not be empty");
  });
});

interface ProviderDelivery {
  readonly eventId: string;
  readonly eventTimestamp: string;
  readonly channelId: string;
  readonly text: string;
  readonly attempt: number;
  readonly signature: string;
  readonly gatewayReceivedAt: string;
}

function canonicalization(version = 1) {
  return defineChannelCanonicalization<ProviderDelivery, CanonicalChannelEvent<"message">>({
    version,
    canonicalize(raw) {
      return {
        id: raw.eventId,
        type: "message",
        version: 1,
        occurredAt: raw.eventTimestamp,
        data: { channelId: raw.channelId, text: raw.text },
        source: {
          channelId: "slack",
          installationId: "workspace",
          tenantId: "tenant",
        },
        origin: { kind: "external", depth: 0 },
      };
    },
  });
}

describe("channel canonicalization", () => {
  const original: ProviderDelivery = {
    eventId: "Ev1",
    eventTimestamp: "2026-08-13T20:00:00.000Z",
    channelId: "C1",
    text: "hello",
    attempt: 1,
    signature: "signature-one",
    gatewayReceivedAt: "2026-08-13T20:00:01.000Z",
  };

  it("normalizes volatile provider retries to one key and input hash", async () => {
    const baseline = await assertChannelCanonicalization(canonicalization(), {
      applicationId: "app",
      original,
      equivalentRetries: [{
        ...original,
        attempt: 2,
        signature: "signature-two",
        gatewayReceivedAt: "2026-08-13T20:00:05.000Z",
      }],
      conflictingRetries: [{ ...original, attempt: 3, text: "meaningfully changed" }],
    });

    expect(baseline.idempotency.parentKeys).toEqual([]);
    expect(baseline.idempotency.eventKeys).toEqual([baseline.idempotency.key]);
    expect(baseline.payload).toMatchObject({
      applicationId: "app",
      canonicalizationVersion: 1,
      event: { id: "Ev1", data: { channelId: "C1", text: "hello" } },
    });
    expect(Object.isFrozen(baseline.payload.event)).toBe(true);
    expect(baseline.payload.event).not.toHaveProperty("signature");
    expect(baseline.payload.event).not.toHaveProperty("gatewayReceivedAt");
  });

  it("binds the normalization contract version into the input hash, not event identity", async () => {
    const first = await canonicalizeChannelDelivery(canonicalization(1), original, {
      applicationId: "app",
    });
    const second = await canonicalizeChannelDelivery(canonicalization(2), original, {
      applicationId: "app",
    });

    expect(second.idempotency.key).toBe(first.idempotency.key);
    expect(second.idempotency.inputHash).not.toBe(first.idempotency.inputHash);
  });

  it("reports an adapter that preserves volatile retry metadata", async () => {
    const unsafe = defineChannelCanonicalization<ProviderDelivery, CanonicalChannelEvent<"message">>({
      version: 1,
      canonicalize(raw) {
        return {
          ...canonicalization().canonicalize(raw),
          data: { channelId: raw.channelId, text: raw.text, attempt: raw.attempt },
        };
      },
    });

    await expect(assertChannelCanonicalization(unsafe, {
      applicationId: "app",
      original,
      equivalentRetries: [{ ...original, attempt: 2 }],
    })).rejects.toThrow("equivalent retry 0 changed the input hash");
  });

  it("rejects malformed canonical events and contract versions", async () => {
    expect(() => defineChannelCanonicalization({
      version: 0,
      canonicalize: () => original as unknown as CanonicalChannelEvent,
    })).toThrow("must be a positive safe integer");

    const malformed = defineChannelCanonicalization<ProviderDelivery, CanonicalChannelEvent>({
      version: 1,
      canonicalize: () => ({
        id: "",
        type: "message",
        version: 1,
        data: {},
        source: { channelId: "slack", installationId: "workspace", tenantId: "tenant" },
        origin: { kind: "external", depth: 0 },
      }),
    });
    await expect(canonicalizeChannelDelivery(malformed, original, {
      applicationId: "app",
    })).rejects.toThrow("canonical channel event id must not be empty");

    const referenceBearing = defineChannelCanonicalization<ProviderDelivery, CanonicalChannelEvent>({
      version: 1,
      canonicalize: () => ({
        ...canonicalization().canonicalize(original),
        ref: "evt_internal_reference",
      }) as CanonicalChannelEvent,
    });
    await expect(canonicalizeChannelDelivery(referenceBearing, original, {
      applicationId: "app",
    })).rejects.toThrow("contains unsupported fields: ref");

    const malformedSubjects = defineChannelCanonicalization<ProviderDelivery, CanonicalChannelEvent>({
      version: 1,
      canonicalize: () => ({
        ...canonicalization().canonicalize(original),
        subjects: { namespace: "slack-channel", key: "C1" },
      }) as unknown as CanonicalChannelEvent,
    });
    await expect(canonicalizeChannelDelivery(malformedSubjects, original, {
      applicationId: "app",
    })).rejects.toThrow("canonical channel event subjects must be an array");
  });
});
