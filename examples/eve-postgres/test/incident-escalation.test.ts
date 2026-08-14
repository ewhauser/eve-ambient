import {
  createAmbientPublisher,
  createAttentionCallbacks,
} from "@ewhauser/eve-ambient";
import { MemoryAttentionEngine } from "@ewhauser/eve-ambient/memory";
import { VirtualMonitorClock } from "@ewhauser/eve-ambient/testing";
import { createEveAttentionRoute } from "@ewhauser/eve-ambient-eve";
import type { ChannelFrom, ChannelSendOptions } from "eve/channels";
import { expect, it } from "vitest";
import { slackChannel } from "../src/channels/slack.js";
import { incidentEscalationRule } from "../src/rules/incident-escalation.js";

it("publishes a Slack rule into one idempotent Eve wake", async () => {
  const deliveries: Array<{ message: string; options: ChannelSendOptions }> = [];
  const from = (() => ({
    async send(message: string, options: ChannelSendOptions) {
      deliveries.push({ message, options });
      return { id: "session-1" };
    },
  })) as unknown as ChannelFrom;
  const route = createEveAttentionRoute({ id: "eve", auth: null, address: "incident", from });
  const clock = new VirtualMonitorClock();
  const callbacks = createAttentionCallbacks({
    rules: [incidentEscalationRule],
    routes: [route],
    clock,
  });
  const engine = new MemoryAttentionEngine({ callbacks, clock });
  const publisher = createAmbientPublisher({
    applicationId: "engineering-agent",
    engine,
    rules: [incidentEscalationRule],
  });

  const receipt = await publisher.publish(slackChannel, {
    eventId: "event-1",
    installationId: "slack-installation",
    tenantId: "tenant-1",
    occurredAt: "2026-01-01T00:00:00.000Z",
    channelId: "C123",
    incidentId: "incident-42",
    severity: "critical",
    text: "database unavailable",
    threadTs: "1700000000.000001",
  });
  clock.advance(30_000);
  await engine.runDue();

  expect(receipt.attention.branchKeys).toHaveLength(1);
  expect(deliveries).toHaveLength(1);
  expect(deliveries[0]?.options.idempotencyKey).toMatch(/^eve:wake:v2:/);
  expect(JSON.parse(deliveries[0]?.message ?? "").evidence.value.messages).toHaveLength(1);
});
