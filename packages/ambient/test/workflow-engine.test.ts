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
  it("seeds the winning cold candidate and skips a second hook resume", async () => {
    const owner = { runId: "candidate-run" };
    workflowApi.resumeHook.mockRejectedValueOnce(new HookNotFoundError("missing"));
    workflowApi.start.mockResolvedValueOnce(owner);
    workflowApi.getHookByToken.mockResolvedValueOnce(owner);
    const input = await fanout("winner");

    await engine("winner").accept(input);

    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(1);
    expect(workflowApi.start).toHaveBeenCalledTimes(1);
    const startArguments = workflowApi.start.mock.calls[0]?.[1];
    expect(startArguments).toHaveLength(3);
    expect(startArguments?.[2]).toMatchObject({
      kind: "append",
      append: { eventKey: input.eventKey },
    });
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
      kind: "append",
      append: { eventKey: input.eventKey },
    });
  });

  it("singleflights cold initialization across engine instances", async () => {
    const candidate = { runId: "candidate-run" };
    let releaseStart!: (value: typeof candidate) => void;
    workflowApi.resumeHook.mockImplementation(async (target: unknown) => {
      if (typeof target === "string") throw new HookNotFoundError(target);
      return target;
    });
    workflowApi.start.mockImplementationOnce(() =>
      new Promise<typeof candidate>((resolve) => {
        releaseStart = resolve;
      }));
    workflowApi.getHookByToken.mockResolvedValue(candidate);
    const first = engine("singleflight");
    const second = engine("singleflight");
    const inputs = await Promise.all(Array.from(
      { length: 20 },
      (_, index) => fanout(`singleflight-${index}`),
    ));

    const accepted = inputs.map((input, index) =>
      (index % 2 === 0 ? first : second).accept(input));
    await vi.waitFor(() => expect(workflowApi.resumeHook).toHaveBeenCalledTimes(20));
    releaseStart(candidate);
    await Promise.all(accepted);

    expect(workflowApi.start).toHaveBeenCalledTimes(1);
    expect(workflowApi.getHookByToken).toHaveBeenCalledTimes(1);
    expect(workflowApi.resumeHook).toHaveBeenCalledTimes(39);
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

async function fanout(eventId: string): Promise<AcceptedFanout> {
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
  return compileAcceptedFanout({ source, branches: [plan()] });
}

function plan(): AttentionBranchPlan {
  return {
    monitorId: "monitor",
    definitionVersion: "definition-v1",
    correlationKey: "incident-1",
    orderKey: "001",
    mode: "active",
    policy: { buffer: { mode: "immediate" } },
  };
}
