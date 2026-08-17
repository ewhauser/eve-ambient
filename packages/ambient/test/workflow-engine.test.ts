import { HookNotFoundError } from "workflow/errors";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  attentionValueBytes,
  compileAcceptedFanout,
  type AcceptedFanout,
  type AttentionBranchPlan,
} from "../src/attention.js";
import {
  canonicalizeChannelDelivery,
  defineChannelCanonicalization,
} from "../src/idempotency.js";
import { compileAttentionStreamAppends } from "../src/stream-protocol.js";
import {
  correlationAppendInputBytes,
  correlationAppendManyBytes,
} from "../src/workflow-protocol.js";

const workflowApi = vi.hoisted(() => ({
  getHookByToken: vi.fn(),
  resumeHook: vi.fn(),
  start: vi.fn(),
}));

vi.mock("workflow/api", () => workflowApi);

const {
  WorkflowAdmissionBackpressureError,
  WorkflowAttentionEngine,
} = await import("../src/workflow.js");

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
  it("coalesces 20 warm same-correlation commands into one hook resume", async () => {
    workflowApi.resumeHook.mockResolvedValue(undefined);
    const inputs = await fanouts("warm", 20);

    const receipts = await Promise.all(inputs.map((input) => engine("warm").accept(input)));

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1);
    expect(workflowApi.start).not.toHaveBeenCalled();
    const command = workflowApi.resumeHook.mock.calls[0]?.[1];
    expect(command).toMatchObject({ kind: "append-many" });
    expect(command.commands).toHaveLength(20);
    expect(command.commands.map(eventId).sort())
      .toEqual(inputs.map((input) => input.event.id).sort());
    const acceptedAtByEvent = new Map(command.commands.map(
      (entry: { readonly acceptedAt: string } & Parameters<typeof eventId>[0]) =>
        [eventId(entry), entry.acceptedAt],
    ));
    for (const [index, input] of inputs.entries()) {
      expect(acceptedAtByEvent.get(input.event.id)).toBe(receipts[index]!.acceptedAt);
    }
  });

  it("coalesces arrivals whose same-correlation preparation finishes apart", async () => {
    workflowApi.resumeHook.mockResolvedValue(undefined);
    const inputs = await fanouts("staggered-preparation", 2);
    const preparationGate = deferred<void>();
    const preparationBlocked = deferred<void>();
    const digest = crypto.subtle.digest.bind(crypto.subtle);
    let blockedOneDigest = false;
    vi.spyOn(crypto.subtle, "digest").mockImplementation((algorithm, data) => {
      if (blockedOneDigest) return digest(algorithm, data);
      blockedOneDigest = true;
      preparationBlocked.resolve();
      return preparationGate.promise.then(() => digest(algorithm, data));
    });
    const shared = engine("staggered-preparation");

    const accepted = inputs.map((input) => shared.accept(input));
    await preparationBlocked.promise;
    setTimeout(() => preparationGate.resolve(), 8);
    await Promise.all(accepted);

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1);
    expect(workflowApi.resumeHook.mock.calls[0]?.[1].commands).toHaveLength(2);
  });

  it("bounds waits for a stalled same-correlation preparation", async () => {
    workflowApi.resumeHook.mockResolvedValue(undefined);
    const inputs = await fanouts("stalled-preparation", 2);
    const preparationGate = deferred<void>();
    const preparationBlocked = deferred<void>();
    const digest = crypto.subtle.digest.bind(crypto.subtle);
    let blockedOneDigest = false;
    vi.spyOn(crypto.subtle, "digest").mockImplementation((algorithm, data) => {
      if (blockedOneDigest) return digest(algorithm, data);
      blockedOneDigest = true;
      preparationBlocked.resolve();
      return preparationGate.promise.then(() => digest(algorithm, data));
    });
    const shared = engine("stalled-preparation");

    const accepted = inputs.map((input) => shared.accept(input));
    await preparationBlocked.promise;
    await vi.waitFor(() => expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1));
    expect(workflowApi.resumeHook.mock.calls[0]?.[1].commands).toHaveLength(1);
    preparationGate.resolve();
    await Promise.all(accepted);

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2);
    expect(workflowApi.resumeHook.mock.calls[1]?.[1].commands).toHaveLength(1);
  });

  it("releases a same-correlation cohort when one preparation rejects", async () => {
    workflowApi.resumeHook.mockResolvedValue(undefined);
    const input = await fanout("rejected-preparation");
    const invalid = {
      ...input,
      inputHash: `eve:input:v1:${"0".repeat(64)}`,
    } as AcceptedFanout;
    const shared = engine("rejected-preparation");

    const accepted = shared.accept(input);
    const rejected = shared.accept(invalid);

    await expect(rejected).rejects.toThrow();
    await accepted;
    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1);
    expect(workflowApi.resumeHook.mock.calls[0]?.[1].commands).toHaveLength(1);
  });

  it("keeps independent correlation tokens in independent batches", async () => {
    workflowApi.resumeHook.mockResolvedValue(undefined);
    const shared = engine("independent");
    const left = await fanouts("left", 10, { correlationKey: "left" });
    const right = await fanouts("right", 10, { correlationKey: "right" });

    await Promise.all([
      ...left.map((input) => shared.accept(input)),
      ...right.map((input) => shared.accept(input)),
    ]);

    const calls = workflowApi.resumeHook.mock.calls.map(([token, command]) => ({
      token,
      command,
    }));
    expect(calls.length).toBeGreaterThanOrEqual(2);
    expect(new Set(calls.map(({ token }) => token)).size).toBe(2);
    expect(calls.every(({ command }) =>
      new Set(command.commands.map((entry: { append: { streamKey: string } }) =>
        entry.append.streamKey)).size === 1)).toBe(true);
    expect(calls.flatMap(({ command }) => command.commands.map(eventId)).sort())
      .toEqual([...left, ...right].map((input) => input.event.id).sort());
  });

  it("splits count-limited bursts into serial queue-ordered chunks", async () => {
    workflowApi.resumeHook.mockResolvedValue(undefined);
    const inputs = await fanouts("count", 8);
    const shared = engine("count", { maxBatchCommands: 3 });

    await Promise.all(inputs.map((input) => shared.accept(input)));

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(3);
    const commands = workflowApi.resumeHook.mock.calls.map((call) => call[1]);
    expect(commands.map((command) => command.commands.length)).toEqual([3, 3, 2]);
    expect(commands.flatMap((command) => command.commands.map(eventId)).sort())
      .toEqual(inputs.map((input) => input.event.id).sort());
  });

  it("splits bursts by exact serialized command bytes", async () => {
    workflowApi.resumeHook.mockResolvedValue(undefined);
    const inputs = await fanouts("bytes", 2);
    const acceptedAt = "2026-01-01T00:00:00.000Z";
    const appends = await Promise.all(inputs.map(async (input) =>
      (await compileAttentionStreamAppends(input))[0]!));
    const individual = appends.map((append) => ({ append, acceptedAt }));
    const oneBytes = correlationAppendManyBytes({
      kind: "append-many",
      commands: [individual[0]!],
    });
    const combined = { kind: "append-many" as const, commands: individual };
    expect(correlationAppendManyBytes(combined)).toBe(attentionValueBytes(combined));
    expect(correlationAppendManyBytes(combined)).toBeGreaterThan(oneBytes);
    const shared = engine("bytes", {
      maxBatchBytes: oneBytes,
      clock: { now: () => new Date(acceptedAt) },
    });

    await Promise.all(inputs.map((input) => shared.accept(input)));

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2);
    expect(workflowApi.resumeHook.mock.calls.map((call) => call[1].commands.length))
      .toEqual([1, 1]);
    expect(workflowApi.resumeHook.mock.calls.flatMap((call) =>
      call[1].commands.map(eventId)).sort()).toEqual(
      inputs.map((input) => input.event.id).sort(),
    );
  });

  it("seeds the winning cold candidate with the entire first batch", async () => {
    const owner = { runId: "candidate-run" };
    workflowApi.resumeHook.mockRejectedValueOnce(new HookNotFoundError("missing"));
    workflowApi.start.mockResolvedValueOnce(owner);
    workflowApi.getHookByToken.mockResolvedValueOnce(owner);
    const inputs = await fanouts("winner", 20);

    await Promise.all(inputs.map((input) => engine("winner").accept(input)));

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1);
    expect(workflowApi.start).toHaveBeenCalledTimes(1);
    const startArguments = workflowApi.start.mock.calls[0]?.[1];
    expect(startArguments).toHaveLength(3);
    expect(startArguments?.[2]).toMatchObject({
      kind: "append-many",
    });
    expect(startArguments?.[2].commands).toHaveLength(20);
    expect(startArguments?.[2].commands.map(eventId).sort())
      .toEqual(inputs.map((input) => input.event.id).sort());
  });

  it("resumes the elected owner when its seeded candidate loses", async () => {
    const candidate = { runId: "candidate-run" };
    const owner = { runId: "owner-run" };
    workflowApi.resumeHook
      .mockRejectedValueOnce(new HookNotFoundError("missing"))
      .mockResolvedValueOnce(owner);
    workflowApi.start.mockResolvedValueOnce(candidate);
    workflowApi.getHookByToken.mockResolvedValueOnce(owner);
    const input = await fanout("loser");

    await engine("loser").accept(input);

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2);
    expect(workflowApi.resumeHook.mock.calls[1]?.[0]).toBe(owner);
    expect(workflowApi.resumeHook.mock.calls[1]?.[1]).toMatchObject({
      kind: "append-many",
    });
    expect(workflowApi.resumeHook.mock.calls[1]?.[1].commands[0].append.eventKey)
      .toBe(input.eventKey);
  });

  it("coalesces cold admission across engine instances before initialization", async () => {
    const candidate = { runId: "candidate-run" };
    workflowApi.resumeHook.mockImplementation(async (target: unknown) => {
      if (typeof target === "string") throw new HookNotFoundError(target);
      return target;
    });
    workflowApi.start.mockResolvedValueOnce(candidate);
    workflowApi.getHookByToken.mockResolvedValue(candidate);
    const first = engine("singleflight");
    const second = engine("singleflight");
    const inputs = await fanouts("singleflight", 20);

    const accepted = inputs.map((input, index) =>
      (index % 2 === 0 ? first : second).accept(input));
    await Promise.all(accepted);

    expect(workflowApi.start).toHaveBeenCalledTimes(1);
    expect(workflowApi.getHookByToken).toHaveBeenCalledTimes(1);
    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1);
    expect(workflowApi.start.mock.calls[0]?.[1][2].commands).toHaveLength(20);
  });

  it("cleans up a failed cold batch probe so a later publication can retry", async () => {
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
    const inputs = await fanouts("probe-failure", 3);

    const failed = inputs.slice(0, 2).map((input) => admission.accept(input));
    await vi.waitFor(() => expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1));
    probe.resolve();

    expect(await Promise.allSettled(failed)).toEqual([
      { status: "rejected", reason: failure },
      { status: "rejected", reason: failure },
    ]);
    expect(workflowApi.start).toHaveBeenCalledTimes(1);

    cold = false;
    await admission.accept(inputs[2]!);

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2);
    expect(workflowApi.start).toHaveBeenCalledTimes(1);
  });

  it("reuses the owner returned by a completed warm batch probe", async () => {
    const owner = { runId: "warm-owner" };
    workflowApi.resumeHook.mockResolvedValue(owner);
    const admission = engine("completed-cache");
    const inputs = await fanouts("completed", 2);

    await admission.accept(inputs[0]!);
    await admission.accept(inputs[1]!);

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2);
    expect(workflowApi.resumeHook.mock.calls[0]?.[0]).toEqual(expect.any(String));
    expect(workflowApi.resumeHook.mock.calls[1]?.[0]).toBe(owner);
    expect(workflowApi.start).not.toHaveBeenCalled();
  });

  it("shares a cold owner across engine instances after initialization", async () => {
    const owner = { runId: "shared-cold-owner" };
    workflowApi.resumeHook
      .mockRejectedValueOnce(new HookNotFoundError("missing"))
      .mockResolvedValueOnce(owner);
    workflowApi.start.mockResolvedValueOnce(owner);
    workflowApi.getHookByToken.mockResolvedValueOnce(owner);
    const first = engine("cross-engine-cache");
    const second = engine("cross-engine-cache");

    await first.accept(await fanout("cross-engine-first"));
    await second.accept(await fanout("cross-engine-second"));

    expect(workflowApi.start).toHaveBeenCalledTimes(1);
    expect(workflowApi.getHookByToken).toHaveBeenCalledTimes(1);
    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2);
    expect(workflowApi.resumeHook.mock.calls[1]?.[0]).toBe(owner);
  });

  it("evicts a stale owner and retries the batch through its token", async () => {
    const staleOwner = { runId: "stale-owner" };
    const activeOwner = { runId: "active-owner" };
    workflowApi.resumeHook
      .mockResolvedValueOnce(staleOwner)
      .mockRejectedValueOnce(new HookNotFoundError("inactive"))
      .mockResolvedValueOnce(activeOwner)
      .mockResolvedValueOnce(activeOwner);
    const admission = engine("stale-token");
    const recovered = await fanout("stale-token-recovered");

    await admission.accept(await fanout("stale-token-first"));
    await admission.accept(recovered);
    await admission.accept(await fanout("stale-token-after"));

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(4);
    expect(workflowApi.resumeHook.mock.calls[1]?.[0]).toBe(staleOwner);
    expect(workflowApi.resumeHook.mock.calls[2]?.[0]).toEqual(expect.any(String));
    expect(workflowApi.resumeHook.mock.calls[3]?.[0]).toBe(activeOwner);
    expect(workflowApi.resumeHook.mock.calls[1]?.[1].commands[0].append.eventKey)
      .toBe(recovered.eventKey);
    expect(workflowApi.resumeHook.mock.calls[2]?.[1].commands[0].append.eventKey)
      .toBe(recovered.eventKey);
    expect(workflowApi.start).not.toHaveBeenCalled();
  });

  it("seeds a replacement owner when a stale handle and token are inactive", async () => {
    const staleOwner = { runId: "stale-cold-owner" };
    const replacement = { runId: "replacement-owner" };
    workflowApi.resumeHook
      .mockResolvedValueOnce(staleOwner)
      .mockRejectedValueOnce(new HookNotFoundError("inactive"))
      .mockRejectedValueOnce(new HookNotFoundError("missing"))
      .mockResolvedValueOnce(replacement);
    workflowApi.start.mockResolvedValueOnce(replacement);
    workflowApi.getHookByToken.mockResolvedValueOnce(replacement);
    const admission = engine("stale-cold");
    const recovered = await fanout("stale-cold-recovered");

    await admission.accept(await fanout("stale-cold-first"));
    await admission.accept(recovered);
    await admission.accept(await fanout("stale-cold-after"));

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(4);
    expect(workflowApi.resumeHook.mock.calls[1]?.[0]).toBe(staleOwner);
    expect(workflowApi.resumeHook.mock.calls[2]?.[0]).toEqual(expect.any(String));
    expect(workflowApi.resumeHook.mock.calls[3]?.[0]).toBe(replacement);
    expect(workflowApi.start).toHaveBeenCalledTimes(1);
    expect(workflowApi.start.mock.calls[0]?.[1]?.[2].commands[0].append.eventKey)
      .toBe(recovered.eventKey);
  });

  it("does not serialize unrelated correlation-token batches", async () => {
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
    const admission = engine("independent-token-progress");
    const blocked = admission.accept(await fanout("blocked", {
      correlationKey: "blocked-correlation",
    }));
    await vi.waitFor(() => expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1));

    await admission.accept(await fanout("independent", {
      correlationKey: "independent-correlation",
    }));

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2);
    expect(workflowApi.resumeHook.mock.calls[1]?.[0]).not.toBe(blockedToken);
    blockedProbe.resolve();
    await blocked;
    expect(workflowApi.start).not.toHaveBeenCalled();
  });

  it("rejects every affected receipt on publication failure and accepts a retry", async () => {
    const outage = new Error("injected publication outage");
    workflowApi.resumeHook.mockRejectedValueOnce(outage).mockResolvedValueOnce(undefined);
    const shared = engine("failure");
    const inputs = await fanouts("failure", 20);

    const failed = await Promise.allSettled(inputs.map((input) => shared.accept(input)));

    expect(failed).toHaveLength(20);
    expect(failed.every((result) =>
      result.status === "rejected" && result.reason === outage)).toBe(true);
    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1);

    const retry = await shared.accept(inputs[0]!);
    expect(retry.eventKey).toBe(inputs[0]!.eventKey);
    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2);
    expect(workflowApi.resumeHook.mock.calls[1]?.[1].commands).toHaveLength(1);
  });

  it("fail-stops later chunks after a middle publication failure", async () => {
    const outage = new Error("injected middle-chunk outage");
    workflowApi.resumeHook
      .mockResolvedValueOnce(undefined)
      .mockRejectedValueOnce(outage)
      .mockResolvedValue(undefined);
    const shared = engine("middle-failure", { maxBatchCommands: 2 });
    const inputs = await fanouts("middle-failure", 5);

    const failed = await Promise.allSettled(inputs.map((input) => shared.accept(input)));

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2);
    const successfulIds = new Set(
      workflowApi.resumeHook.mock.calls[0]![1].commands.map(eventId),
    );
    const failedPublishedIds = new Set(
      workflowApi.resumeHook.mock.calls[1]![1].commands.map(eventId),
    );
    expect(successfulIds.size).toBe(2);
    expect(failedPublishedIds.size).toBe(2);
    expect([...successfulIds].some((event) => failedPublishedIds.has(event))).toBe(false);
    for (const [index, result] of failed.entries()) {
      if (successfulIds.has(inputs[index]!.event.id)) {
        expect(result.status).toBe("fulfilled");
      } else {
        expect(result).toMatchObject({ status: "rejected", reason: outage });
      }
    }

    const retryInputs = inputs.filter((input) => !successfulIds.has(input.event.id));
    await Promise.all(retryInputs.map((input) => shared.accept(input)));

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(4);
    expect(workflowApi.resumeHook.mock.calls.slice(2).flatMap((call) =>
      call[1].commands.map(eventId)).sort()).toEqual(
      retryInputs.map((input) => input.event.id).sort(),
    );
  });

  it("bounds queued and in-flight commands with retryable local backpressure", async () => {
    let releaseFirst!: () => void;
    workflowApi.resumeHook
      .mockImplementationOnce(() => new Promise<void>((resolve) => {
        releaseFirst = resolve;
      }))
      .mockResolvedValue(undefined);
    const shared = engine("local-count-limit", { maxLocalPendingCommands: 3 });
    const inputs = await fanouts("local-count-limit", 4);

    const first = shared.accept(inputs[0]!);
    await vi.waitFor(() => expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1));
    const second = shared.accept(inputs[1]!);
    const third = shared.accept(inputs[2]!);
    await new Promise((resolve) => setTimeout(resolve, 5));

    const overflow = await shared.accept(inputs[3]!).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(overflow).toBeInstanceOf(WorkflowAdmissionBackpressureError);
    expect(overflow).toMatchObject({ retryable: true });
    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1);

    releaseFirst();
    await Promise.all([first, second, third]);

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2);
    expect(workflowApi.resumeHook.mock.calls[1]?.[1].commands).toHaveLength(2);
  });

  it("bounds queued and in-flight canonical append bytes", async () => {
    let releaseFirst!: () => void;
    workflowApi.resumeHook.mockImplementationOnce(() => new Promise<void>((resolve) => {
      releaseFirst = resolve;
    }));
    const acceptedAt = "2026-01-01T00:00:00.000Z";
    const inputs = await fanouts("local-byte-limit", 2);
    const firstAppend = (await compileAttentionStreamAppends(inputs[0]!))[0]!;
    const oneInputBytes = correlationAppendInputBytes({ append: firstAppend, acceptedAt });
    const shared = engine("local-byte-limit", {
      clock: { now: () => new Date(acceptedAt) },
      maxLocalPendingBytes: oneInputBytes,
    });

    const first = shared.accept(inputs[0]!);
    await vi.waitFor(() => expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1));

    await expect(shared.accept(inputs[1]!)).rejects.toBeInstanceOf(
      WorkflowAdmissionBackpressureError,
    );

    releaseFirst();
    await first;
  });

  it("does not coalesce engines with different operational queue settings", async () => {
    workflowApi.resumeHook.mockResolvedValue(undefined);
    const first = engine("operational-lanes", { registrationTimeoutMs: 10 });
    const second = engine("operational-lanes", { registrationTimeoutMs: 20 });
    const inputs = await fanouts("operational-lanes", 2);

    await Promise.all([first.accept(inputs[0]!), second.accept(inputs[1]!)]);

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2);
    expect(new Set(workflowApi.resumeHook.mock.calls.map((call) => call[0])).size).toBe(1);
    expect(workflowApi.resumeHook.mock.calls.every((call) =>
      call[1].commands.length === 1)).toBe(true);
  });

  it("schedules arrivals during an active flush without stranding either batch", async () => {
    let releaseFirst!: () => void;
    workflowApi.resumeHook
      .mockImplementationOnce(() => new Promise<void>((resolve) => {
        releaseFirst = resolve;
      }))
      .mockResolvedValueOnce(undefined);
    const shared = engine("active-flush");
    const firstInputs = await fanouts("active-first", 10);
    const secondInputs = await fanouts("active-second", 10);
    let firstSettled = false;
    const first = Promise.all(firstInputs.map((input) => shared.accept(input)))
      .then((receipts) => {
        firstSettled = true;
        return receipts;
      });
    await vi.waitFor(() => expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1));

    const second = Promise.all(secondInputs.map((input) => shared.accept(input)));
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(firstSettled).toBe(false);
    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1);
    releaseFirst();
    await Promise.all([first, second]);

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2);
    expect(workflowApi.resumeHook.mock.calls[0]?.[1].commands).toHaveLength(10);
    expect(workflowApi.resumeHook.mock.calls[1]?.[1].commands).toHaveLength(10);
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
    await vi.advanceTimersByTimeAsync(1);
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

  it("expires an idle cached owner after ten minutes", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00.000Z"));
    const owner = { runId: "expiring-owner" };
    workflowApi.resumeHook.mockResolvedValue(owner);
    const admission = engine("cache-ttl");

    const first = admission.accept(await fanout("cache-ttl-first"));
    await vi.advanceTimersByTimeAsync(1);
    await vi.waitFor(() => expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1));
    await first;
    await vi.advanceTimersByTimeAsync(10 * 60_000);
    const second = admission.accept(await fanout("cache-ttl-second"));
    await vi.advanceTimersByTimeAsync(1);
    await vi.waitFor(() => expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2));
    await second;

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2);
    expect(workflowApi.resumeHook.mock.calls[0]?.[0]).toEqual(expect.any(String));
    expect(workflowApi.resumeHook.mock.calls[1]?.[0]).toEqual(expect.any(String));
  });

  it("bounds the process-local hook cache with LRU eviction", async () => {
    workflowApi.resumeHook.mockImplementation(async (target: string | { runId: string }) =>
      typeof target === "string" ? { runId: `owner-${target}` } : target);
    const admission = engine("cache-bounds");

    await admission.accept(await fanout(
      "cache-bounds-0",
      { correlationKey: "correlation-0" },
    ));
    const concurrent = await Promise.all(Array.from(
      { length: 1_023 },
      (_, offset) => {
        const index = offset + 1;
        return fanout(
          `cache-bounds-${index}`,
          { correlationKey: `correlation-${index}` },
        );
      },
    ));
    await Promise.all(concurrent.map((input) => admission.accept(input)));
    await admission.accept(await fanout(
      "cache-bounds-1024",
      { correlationKey: "correlation-1024" },
    ));
    workflowApi.resumeHook.mockClear();

    await admission.accept(await fanout("cache-bounds-first-again", {
      correlationKey: "correlation-0",
    }));
    await admission.accept(await fanout("cache-bounds-last-again", {
      correlationKey: "correlation-1024",
    }));

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(2);
    expect(workflowApi.resumeHook.mock.calls[0]?.[0]).toEqual(expect.any(String));
    expect(workflowApi.resumeHook.mock.calls[1]?.[0]).toEqual(expect.objectContaining({
      runId: expect.any(String),
    }));
  });
});

function engine(
  namespace: string,
  options: Partial<ConstructorParameters<typeof WorkflowAttentionEngine>[0]> = {},
): InstanceType<typeof WorkflowAttentionEngine> {
  return new WorkflowAttentionEngine({
    namespace,
    callbackUrl: "https://application.example.test",
    ...options,
  });
}

async function fanouts(
  prefix: string,
  count: number,
  options: { readonly correlationKey?: string } = {},
): Promise<AcceptedFanout[]> {
  return Promise.all(Array.from(
    { length: count },
    (_, index) => fanout(`${prefix}-${String(index).padStart(2, "0")}`, options),
  ));
}

async function fanout(
  eventId: string,
  options: { readonly correlationKey?: string } = {},
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
  return compileAcceptedFanout({
    source,
    branches: [plan(options.correlationKey)],
  });
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

function eventId(input: { readonly append: { readonly branches: readonly {
  readonly event: { readonly id: string };
}[] } }): string {
  return input.append.branches[0]!.event.id;
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
