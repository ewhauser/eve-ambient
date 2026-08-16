import { describe, expect, it } from "vitest";
import {
  AttentionCapacityError,
  compileAcceptedFanout,
  validateAcceptedFanout,
  type AcceptedFanout,
  type AttentionBranchPlan,
} from "../src/attention.js";
import {
  canonicalizeChannelDelivery,
  defineChannelCanonicalization,
  hashIdempotencyInput,
} from "../src/idempotency.js";
import { compileAttentionStreamAppends } from "../src/stream-protocol.js";
import {
  applyAttentionStreamAppend,
  nextAttentionDueAt,
} from "../src/stream-state.js";

describe("attention fan-out protocol", () => {
  it("carries the canonical event by value and erases declaration order", async () => {
    const source = await acceptedSource();
    const second = branch({ monitorId: "monitor-z" });
    const first = branch({ monitorId: "monitor-a" });
    const compiled = await compileAcceptedFanout({ source, branches: [second, first] });
    const reversed = await compileAcceptedFanout({ source, branches: [first, second] });

    expect(compiled).toEqual(reversed);
    expect(compiled.branches.map((value) => value.branchKey)).toEqual(
      [...compiled.branches.map((value) => value.branchKey)].sort(),
    );
    expect(compiled.branches.every((value) => value.event !== compiled.event)).toBe(true);
    expect(compiled.branches.map((value) => value.event)).toEqual([
      compiled.event,
      compiled.event,
    ]);
    expect(compiled.branches.map((value) => value.event.data)).toEqual([
      { body: "semantic payload" },
      { body: "semantic payload" },
    ]);
    expect(Object.isFrozen(compiled)).toBe(true);
    expect(Object.isFrozen(compiled.branches[0]!.event)).toBe(true);
    await expect(validateAcceptedFanout(compiled)).resolves.toEqual(compiled);
  });

  it("rejects claimed lineage and branch payload tampering", async () => {
    const compiled = await compileAcceptedFanout({
      source: await acceptedSource(),
      branches: [branch()],
    });
    const wrongOccurrence = clone(compiled);
    wrongOccurrence.occurrenceKey =
      wrongOccurrence.eventKey as unknown as AcceptedFanout["occurrenceKey"];
    await expect(validateAcceptedFanout(wrongOccurrence)).rejects.toThrow(
      "occurrenceKey does not match",
    );

    const wrongBranch = clone(compiled);
    wrongBranch.branches[0]!.event = {
      ...wrongBranch.branches[0]!.event,
      data: { body: "substituted" },
    };
    await expect(validateAcceptedFanout(wrongBranch)).rejects.toThrow(
      "branch does not match its complete source value",
    );
  });

  it("validates serializable mailbox bounds before durable acceptance", async () => {
    const source = await acceptedSource();
    await expect(
      compileAcceptedFanout({
        source,
        branches: [
          branch({
            policy: {
              buffer: {
                mode: "debounce",
                quietPeriodMs: 0,
                maxWaitMs: 100,
                maxEvents: 10,
                maxBytes: 1_000,
              },
            },
          }),
        ],
      }),
    ).rejects.toThrow("quietPeriodMs must be a positive safe integer");

    await expect(
      compileAcceptedFanout({
        source,
        branches: [
          branch({
            policy: {
              buffer: {
                mode: "debounce",
                quietPeriodMs: 1,
                maxWaitMs: 2,
                maxEvents: 1,
                maxBytes: 1,
              },
            },
          }),
        ],
      }),
    ).rejects.toBeInstanceOf(AttentionCapacityError);
  });

  it("rechecks branch size after validating an independently hashed fan-out", async () => {
    const compiled = await compileAcceptedFanout({
      source: await acceptedSource(),
      branches: [
        branch({
          policy: {
            buffer: {
              mode: "debounce",
              quietPeriodMs: 1,
              maxWaitMs: 2,
              maxEvents: 10,
              maxBytes: 100_000,
            },
          },
        }),
      ],
    });
    const independentlyHashed = clone(compiled);
    const value = independentlyHashed.branches[0]!;
    if (value.policy.buffer.mode !== "debounce") throw new Error("expected debounce policy");
    value.policy = {
      ...value.policy,
      buffer: { ...value.policy.buffer, maxBytes: 1 },
    };
    const logicalInput = structuredClone(value) as Record<string, unknown>;
    delete logicalInput.branchKey;
    delete logicalInput.inputHash;
    value.inputHash = await hashIdempotencyInput(logicalInput);
    await expect(validateAcceptedFanout(independentlyHashed)).rejects.toBeInstanceOf(
      AttentionCapacityError,
    );
  });

  it("rejects unsupported wire fields instead of leaving them unhashed", async () => {
    const source = await acceptedSource();
    await expect(
      compileAcceptedFanout({
        source,
        branches: [
          {
            ...branch(),
            hiddenPayloadReference: "event://payload",
          } as AttentionBranchPlan,
        ],
      }),
    ).rejects.toThrow("attention branch plan contains unsupported fields");

    const compiled = await compileAcceptedFanout({ source, branches: [branch()] });
    const injected = {
      ...compiled,
      hiddenPayloadReference: "event://payload",
    } as AcceptedFanout;
    await expect(validateAcceptedFanout(injected)).rejects.toThrow(
      "accepted fan-out contains unsupported fields",
    );
  });

  it("keeps reducer time monotonic when accepted appends arrive out of order", async () => {
    const policy = {
      buffer: {
        mode: "debounce" as const,
        quietPeriodMs: 2 * 60_000,
        maxWaitMs: 10 * 60_000,
        maxEvents: 10,
        maxBytes: 100_000,
      },
    };
    const later = await compileAcceptedFanout({
      source: await acceptedSource({ id: "later", body: "later" }),
      branches: [branch({ orderKey: "002", policy })],
    });
    const earlier = await compileAcceptedFanout({
      source: await acceptedSource({ id: "earlier", body: "earlier" }),
      branches: [branch({ orderKey: "001", policy })],
    });
    const [laterAppend] = await compileAttentionStreamAppends(later);
    const [earlierAppend] = await compileAttentionStreamAppends(earlier);
    const first = await applyAttentionStreamAppend(undefined, laterAppend!, {
      now: "2026-01-01T00:01:00.000Z",
      maxRecentMessages: 48,
    });
    const second = await applyAttentionStreamAppend(first.state, earlierAppend!, {
      now: "2026-01-01T00:00:00.000Z",
      maxRecentMessages: 48,
    });

    expect(second.state.lastAcceptedAt).toBe("2026-01-01T00:01:00.000Z");
    expect(second.state.open?.updatedAt).toBe("2026-01-01T00:01:00.000Z");
    expect(nextAttentionDueAt(second.state)).toBe("2026-01-01T00:03:00.000Z");
    expect(second.receipt.acceptedAt).toBe("2026-01-01T00:01:00.000Z");
  });
});

async function acceptedSource(
  overrides: { readonly id?: string; readonly body?: string } = {},
) {
  return canonicalizeChannelDelivery(
    defineChannelCanonicalization<
      { readonly id: string; readonly body: string; readonly attempt: number },
      ReturnType<typeof event>
    >({
      version: 1,
      partitionKey: () => "conversation-1",
      canonicalize: (raw) => event(raw.id, raw.body),
    }),
    {
      id: overrides.id ?? "event-1",
      body: overrides.body ?? "semantic payload",
      attempt: 7,
    },
    { applicationId: "engineering-agent" },
  );
}

function event(id: string, body: string) {
  return {
    id,
    type: "channel.message",
    version: 1,
    occurredAt: "2026-01-01T00:00:00.000Z",
    data: { body },
    source: {
      channelId: "slack",
      installationId: "workspace-1",
      tenantId: "tenant-1",
    },
    origin: { kind: "external" as const, depth: 0 },
  };
}

function branch(overrides: Partial<AttentionBranchPlan> = {}): AttentionBranchPlan {
  return {
    monitorId: "monitor",
    definitionVersion: "definition-v1",
    correlationKey: "incident-1",
    orderKey: "001",
    mode: "active",
    policy: { buffer: { mode: "immediate" } },
    ...overrides,
  };
}

function clone(value: AcceptedFanout): MutableAcceptedFanout {
  return structuredClone(value) as MutableAcceptedFanout;
}

type MutableAcceptedFanout = {
  -readonly [Key in keyof AcceptedFanout]: Key extends "branches"
    ? Array<{
        -readonly [BranchKey in keyof AcceptedFanout["branches"][number]]:
          AcceptedFanout["branches"][number][BranchKey];
      }>
    : AcceptedFanout[Key];
};
