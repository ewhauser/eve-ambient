import type { BranchKey, InputHash } from "@ewhauser/eve-ambient/idempotency";
import { getRun, start } from "workflow/api";
import { getWorld } from "workflow/runtime";
import { waitForHook } from "@workflow/vitest";
import { describe, expect, it } from "vitest";
import { openAttentionStream } from "../src/attention-stream-client.js";
import {
  attentionStreamToken,
  attentionStreamWorkflow,
  type AttentionStreamBranch,
} from "../src/attention-stream-workflow.js";

describe("Workflow World attention-stream spike", () => {
  it("serializes a correlation stream and applies key/hash dedupe during replay", async () => {
    const stream = await openAttentionStream("tenant-1:monitor-1:thread-7");

    const firstReceipt = await stream.append(branch("branch-1", "hash-1", { text: "one" }));
    const duplicateReceipt = await stream.append(
      branch("branch-1", "hash-1", { text: "one" }),
    );
    await stream.append(branch("branch-2", "hash-2", { text: "two" }));
    const conflictingReceipt = await stream.append(
      branch("branch-1", "hash-changed", { text: "changed" }),
    );
    await stream.close();

    expect(firstReceipt).toEqual({
      runId: stream.runId,
      hookId: firstReceipt.hookId,
      transportAccepted: true,
    });
    expect(duplicateReceipt).not.toHaveProperty("duplicate");
    expect(conflictingReceipt).not.toHaveProperty("conflict");
    await expect(stream.result()).resolves.toEqual({
      kind: "closed",
      address: "tenant-1:monitor-1:thread-7",
      accepted: [
        branch("branch-1", "hash-1", { text: "one" }),
        branch("branch-2", "hash-2", { text: "two" }),
      ],
      duplicates: 1,
      conflicts: [
        {
          branchKey: "branch-1",
          existingInputHash: "hash-1",
          receivedInputHash: "hash-changed",
        },
      ],
    });
  });

  it("elects one owner when duplicate workflow runs claim the same stream token", async () => {
    const address = "same-address";
    const owner = await start(attentionStreamWorkflow, [address]);
    await waitForHook(owner, { token: attentionStreamToken(address) });

    const duplicate = await start(attentionStreamWorkflow, [address]);
    await expect(duplicate.returnValue).resolves.toEqual({
      kind: "owner-conflict",
      address,
      ownerRunId: owner.runId,
    });

    const stream = await openAttentionStream(address);
    expect(stream.runId).toBe(owner.runId);
    await stream.close();
    await expect(owner.returnValue).resolves.toMatchObject({ kind: "closed" });
  });

  it("retains payload-bearing hook events after the stream run is terminal", async () => {
    const stream = await openAttentionStream("retention-probe");
    await stream.append(branch("sensitive-branch", "sensitive-hash", { secret: "payload" }));
    await stream.close();
    await stream.result();

    const world = getWorld();
    const run = await world.runs.get(stream.runId, { resolveData: "all" });
    const events = await world.events.list({
      runId: stream.runId,
      resolveData: "all",
      pagination: { limit: 100 },
    });

    expect(run.status).toBe("completed");
    const received = events.data.filter((event) => event.eventType === "hook_received");
    expect(received).toHaveLength(2);
    for (const event of received) {
      expect(event.eventData.payload).toBeInstanceOf(Uint8Array);
      expect((event.eventData.payload as Uint8Array).byteLength).toBeGreaterThan(0);
    }
    expect(events.data.some((event) => event.eventType === "run_completed")).toBe(true);
    await expect(getRun(stream.runId).status).resolves.toBe("completed");
  });
});

function branch(
  branchKey: string,
  inputHash: string,
  payload: unknown,
): AttentionStreamBranch {
  return {
    branchKey: branchKey as BranchKey,
    inputHash: inputHash as InputHash,
    payload,
  };
}
