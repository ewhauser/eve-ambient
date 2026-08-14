import { describe, expect, it } from "vitest";
import { MonitorRuntime } from "@ewhauser/eve-ambient";
import { MemoryMonitorStore } from "@ewhauser/eve-ambient/memory";
import {
  MemoryConversationChannel,
  VirtualMonitorClock,
} from "@ewhauser/eve-ambient/testing";
import type { EveDeliveryTarget } from "@ewhauser/eve-ambient-eve";

import { slackChannel } from "../src/channels/slack.js";
import { publishSlackMessage } from "../src/publish.js";
import { incidentEscalationRule } from "../src/rules/incident-escalation.js";

describe("incidentEscalationRule", () => {
  it("wakes Eve for a critical Slack thread with complete evidence", async () => {
    const clock = new VirtualMonitorClock();
    const delivery = new MemoryConversationChannel<EveDeliveryTarget>({
      clock,
      id: "eve",
    });
    const runtime = new MonitorRuntime({
      applicationId: "engineering-agent",
      channels: [slackChannel],
      clock,
      deliveryChannels: [delivery],
      deployment: { monitors: [incidentEscalationRule(delivery)] },
      store: new MemoryMonitorStore(),
    });
    await runtime.initialize();

    await publishSlackMessage(runtime, {
      actor: { id: "U123", principalType: "user" },
      data: {
        channelId: "C123",
        messageTs: "1723651200.000100",
        severity: "critical",
        text: "SEV-1: checkout is unavailable",
      },
      id: "slack-event-123",
      installationId: "slack-workspace-T1",
      origin: { kind: "external" },
      replyTarget: { address: "slack:C123:1723651200.000100" },
      tenantId: "acme",
    });
    await runtime.drain();
    clock.advance(2_000);
    await runtime.drain();

    expect(delivery.deliveries).toHaveLength(1);
    expect(delivery.deliveries[0]).toMatchObject({
      target: { address: "slack:C123:1723651200.000100" },
      evidence: {
        projectedEvidence: {
          signals: ["critical-severity", "incident-language"],
        },
      },
    });
  });
});
