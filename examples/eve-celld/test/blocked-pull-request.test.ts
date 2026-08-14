import { describe, expect, it } from "vitest";
import { MonitorRuntime } from "@ewhauser/eve-ambient";
import { MemoryMonitorStore } from "@ewhauser/eve-ambient/memory";
import {
  MemoryConversationChannel,
  VirtualMonitorClock,
} from "@ewhauser/eve-ambient/testing";
import type { EveDeliveryTarget } from "@ewhauser/eve-ambient-eve";

import { githubChannel } from "../src/channels/github.js";
import { publishPullRequestChanged } from "../src/publish.js";
import { blockedPullRequestRule } from "../src/rules/blocked-pull-request.js";

describe("blockedPullRequestRule", () => {
  it("wakes Eve for the latest blocking GitHub state", async () => {
    const clock = new VirtualMonitorClock();
    const delivery = new MemoryConversationChannel<EveDeliveryTarget>({
      clock,
      id: "eve",
    });
    const runtime = new MonitorRuntime({
      applicationId: "developer-productivity-agent",
      channels: [githubChannel],
      clock,
      deliveryChannels: [delivery],
      deployment: { monitors: [blockedPullRequestRule(delivery)] },
      store: new MemoryMonitorStore(),
    });
    await runtime.initialize();

    await publishPullRequestChanged(runtime, {
      data: {
        failingChecks: ["test"],
        mergeState: "conflicting",
        number: 1842,
        repository: "vercel/eve",
        reviewDecision: "changes-requested",
        state: "open",
        title: "Carry channel delivery idempotency",
        updatedAt: "2026-08-14T18:00:00.000Z",
      },
      id: "github-delivery-123",
      installationId: "github-installation-42",
      origin: { kind: "external" },
      replyTarget: { address: "github:vercel/eve:pull:1842" },
      tenantId: "acme",
    });
    await runtime.drain();
    clock.advance(10_000);
    await runtime.drain();

    expect(delivery.deliveries).toHaveLength(1);
    expect(delivery.deliveries[0]).toMatchObject({
      target: { address: "github:vercel/eve:pull:1842" },
      evidence: {
        projectedEvidence: {
          blockers: ["changes-requested", "check:test", "merge-conflict"],
        },
      },
    });
  });
});
