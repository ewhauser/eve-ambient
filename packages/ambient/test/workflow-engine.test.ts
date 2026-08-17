import { HookNotFoundError } from "workflow/errors";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  compileAcceptedFanout,
  type AcceptedFanout,
  type AttentionBranchPlan,
} from "../src/attention.js";
import {
  canonicalizeChannelDelivery,
  defineChannelCanonicalization,
} from "../src/idempotency.js";

const workflowApi = vi.hoisted(() => ({
  getHookByToken: vi.fn(),
  resumeHook: vi.fn(),
  start: vi.fn(),
}));

vi.mock("workflow/api", () => workflowApi);

const { WorkflowAttentionEngine } = await import("../src/workflow.js");

beforeEach(() => {
  workflowApi.getHookByToken.mockReset();
  workflowApi.resumeHook.mockReset();
  workflowApi.start.mockReset();
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("Workflow attention admission", () => {
  it("singleflights a warm probe while every follower publishes its command", async () => {
    const owner = { runId: "warm-owner" };
    const probe = deferred<void>();
    workflowApi.resumeHook.mockImplementation(async (target: unknown) => {
      if (typeof target === "string") await probe.promise;
      return owner;
    });
    const first = engine("warm-singleflight");
    const second = engine("warm-singleflight");
    const inputs = await fanouts("warm", 20);

    const accepted = inputs.map((input, index) =>
      (index % 2 === 0 ? first : second).accept(input));
    await vi.waitFor(() => expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1));
    probe.resolve();
    await Promise.all(accepted);

    expect(workflowApi.start).not.toHaveBeenCalled();
    expect(workflowApi.getHookByToken).not.toHaveBeenCalled();
    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(20);
    expect(typeof workflowApi.resumeHook.mock.calls[0]?.[0]).toBe("string");
    expect(
      workflowApi.resumeHook.mock.calls.slice(1).every(([target]) => target === owner),
    ).toBe(true);
    expect(resumedEventKeys(workflowApi.resumeHook.mock.calls).sort()).toEqual(
      inputs.map((input) => input.eventKey).sort(),
    );
  });

  it("singleflights the initial cold probe, candidate start, and registration polling", async () => {
    const candidate = { runId: "candidate-run" };
    const probe = deferred<void>();
    workflowApi.resumeHook.mockImplementation(async (target: unknown) => {
      if (typeof target === "string") {
        await probe.promise;
        throw new HookNotFoundError(target);
      }
      return target;
    });
    workflowApi.start.mockResolvedValueOnce(candidate);
    workflowApi.getHookByToken.mockResolvedValue(candidate);
    const first = engine("cold-singleflight");
    const second = engine("cold-singleflight");
    const inputs = await fanouts("cold", 20);

    const accepted = inputs.map((input, index) =>
      (index % 2 === 0 ? first : second).accept(input));
    await vi.waitFor(() => expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1));
    probe.resolve();
    await Promise.all(accepted);

    expect(workflowApi.start).toHaveBeenCalledTimes(1);
    expect(workflowApi.getHookByToken).toHaveBeenCalledTimes(1);
    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(20);
    const startArguments = workflowApi.start.mock.calls[0]?.[1];
    const seededEventKey = startArguments?.[2]?.append.eventKey;
    expect(startArguments).toHaveLength(3);
    expect(startArguments?.[2]).toMatchObject({ kind: "append" });
    expect([
      seededEventKey,
      ...resumedEventKeys(workflowApi.resumeHook.mock.calls.slice(1)),
    ].sort()).toEqual(inputs.map((input) => input.eventKey).sort());
  });

  it("resumes both leader and follower commands when the seeded candidate loses", async () => {
    const candidate = { runId: "candidate-run" };
    const owner = { runId: "owner-run" };
    const probe = deferred<void>();
    workflowApi.resumeHook.mockImplementation(async (target: unknown) => {
      if (typeof target === "string") {
        await probe.promise;
        throw new HookNotFoundError(target);
      }
      return owner;
    });
    workflowApi.start.mockResolvedValueOnce(candidate);
    workflowApi.getHookByToken.mockResolvedValueOnce(owner);
    const inputs = await fanouts("losing-candidate", 2);

    const accepted = inputs.map((input) => engine("losing-candidate").accept(input));
    await vi.waitFor(() => expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1));
    probe.resolve();
    await Promise.all(accepted);

    expect(workflowApi.start).toHaveBeenCalledTimes(1);
    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(3);
    expect(
      workflowApi.resumeHook.mock.calls.slice(1).every(([target]) => target === owner),
    ).toBe(true);
    expect(resumedEventKeys(workflowApi.resumeHook.mock.calls.slice(1)).sort()).toEqual(
      inputs.map((input) => input.eventKey).sort(),
    );
  });

  it("cleans up failed cold initialization so a later publication can retry", async () => {
    const failure = new Error("start failed");
    const owner = { runId: "retry-owner" };
    const probe = deferred<void>();
    let cold = true;
    workflowApi.resumeHook.mockImplementation(async (target: unknown) => {
      if (typeof target === "string" && cold) {
        await probe.promise;
        throw new HookNotFoundError(target);
      }
      return owner;
    });
    workflowApi.start.mockRejectedValueOnce(failure);
    const admission = engine("failure-cleanup");
    const inputs = await fanouts("failure", 3);

    const failed = inputs.slice(0, 2).map((input) => admission.accept(input));
    await vi.waitFor(() => expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1));
    probe.resolve();
    const results = await Promise.allSettled(failed);

    expect(results).toEqual([
      { status: "rejected", reason: failure },
      { status: "rejected", reason: failure },
    ]);
    expect(workflowApi.start).toHaveBeenCalledTimes(1);

    cold = false;
    await admission.accept(inputs[2]!);

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2);
    expect(workflowApi.start).toHaveBeenCalledTimes(1);
  });

  it("cleans up a completed probe instead of retaining an owner cache", async () => {
    const owner = { runId: "warm-owner" };
    workflowApi.resumeHook.mockResolvedValue(owner);
    const admission = engine("completed-cleanup");
    const inputs = await fanouts("completed", 2);

    await admission.accept(inputs[0]!);
    await admission.accept(inputs[1]!);

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2);
    expect(
      workflowApi.resumeHook.mock.calls.every(([target]) => typeof target === "string"),
    ).toBe(true);
    expect(workflowApi.start).not.toHaveBeenCalled();
  });

  it("does not serialize unrelated correlation tokens", async () => {
    const blockedProbe = deferred<void>();
    let blockedToken: string | undefined;
    workflowApi.resumeHook.mockImplementation(async (target: unknown) => {
      if (typeof target !== "string") return target;
      if (blockedToken === undefined) {
        blockedToken = target;
        await blockedProbe.promise;
      }
      return { runId: `owner:${target}` };
    });
    const admission = engine("independent-tokens");
    const blocked = admission.accept(await fanout("blocked", "blocked-correlation"));
    await vi.waitFor(() => expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1));

    await admission.accept(await fanout("independent", "independent-correlation"));

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2);
    expect(workflowApi.resumeHook.mock.calls[1]?.[0]).not.toBe(blockedToken);
    blockedProbe.resolve();
    await blocked;
    expect(workflowApi.start).not.toHaveBeenCalled();
  });

  it("uses jittered exponential backoff for registration polling", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(0.5);
    const candidate = { runId: "candidate-run" };
    workflowApi.resumeHook.mockRejectedValueOnce(new HookNotFoundError("missing"));
    workflowApi.start.mockResolvedValueOnce(candidate);
    workflowApi.getHookByToken
      .mockRejectedValueOnce(new HookNotFoundError("missing"))
      .mockRejectedValueOnce(new HookNotFoundError("missing"))
      .mockRejectedValueOnce(new HookNotFoundError("missing"))
      .mockResolvedValueOnce(candidate);

    const accepted = engine("backoff").accept(await fanout("backoff"));
    await vi.waitFor(() => expect(workflowApi.getHookByToken).toHaveBeenCalledTimes(1));
    await vi.advanceTimersByTimeAsync(5);
    expect(workflowApi.getHookByToken).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(10);
    expect(workflowApi.getHookByToken).toHaveBeenCalledTimes(3);
    await vi.advanceTimersByTimeAsync(20);
    await accepted;

    expect(workflowApi.getHookByToken).toHaveBeenCalledTimes(4);
    expect(workflowApi.start).toHaveBeenCalledTimes(1);
    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1);
  });
});

function engine(namespace: string): InstanceType<typeof WorkflowAttentionEngine> {
  return new WorkflowAttentionEngine({
    namespace,
    callbackUrl: "https://application.example.test",
  });
}

async function fanouts(prefix: string, count: number): Promise<AcceptedFanout[]> {
  return Promise.all(Array.from(
    { length: count },
    (_, index) => fanout(`${prefix}-${index}`),
  ));
}

function resumedEventKeys(calls: readonly unknown[][]): string[] {
  return calls.map((call) => {
    const command = call[1] as { readonly append: { readonly eventKey: string } };
    return command.append.eventKey;
  });
}

function deferred<T>(): {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
  readonly reject: (reason?: unknown) => void;
} {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function fanout(
  eventId: string,
  correlationKey = "incident-1",
): Promise<AcceptedFanout> {
  const source = await canonicalizeChannelDelivery(
    defineChannelCanonicalization({
      version: 1,
      partitionKey: () => "conversation-1",
      canonicalize: (raw: { readonly eventId: string }) => ({
        id: raw.eventId,
        type: "channel.message",
        version: 1,
        occurredAt: "2026-01-01T00:00:00.000Z",
        data: null,
        source: {
          channelId: "slack",
          installationId: "workspace-1",
          tenantId: "tenant-1",
        },
        origin: { kind: "external" as const, depth: 0 },
      }),
    }),
    { eventId },
    { applicationId: "workflow-engine-test" },
  );
  return compileAcceptedFanout({ source, branches: [plan(correlationKey)] });
}

function plan(correlationKey = "incident-1"): AttentionBranchPlan {
  return {
    monitorId: "monitor",
    definitionVersion: "definition-v1",
    correlationKey,
    orderKey: "001",
    mode: "active",
    policy: { buffer: { mode: "immediate" } },
  };
}
