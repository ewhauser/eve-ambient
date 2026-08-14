import type {
  DirectDispatchRequest,
  PreparedAttentionWake,
} from "@ewhauser/eve-ambient";
import {
  deriveAttentionDirectDispatchKey,
  hashIdempotencyInput,
} from "@ewhauser/eve-ambient";
import type { ChannelFrom, ChannelSendOptions } from "eve/channels";
import { describe, expect, it } from "vitest";
import {
  createEveAttentionRoute,
  createEveDirectDispatchAdapter,
  renderEveAttentionMessage,
} from "../src/index.js";

function fakeFrom(
  deliveries: Array<{ address: string; message: string; options: ChannelSendOptions }>,
): ChannelFrom {
  return ((address: string) => ({
    async send(message: string, options: ChannelSendOptions) {
      deliveries.push({ address, message, options });
      return { id: "session-1" };
    },
  })) as ChannelFrom;
}

const wake = {
  wakeKey: `eve:wake:v2:${"1".repeat(64)}`,
  runKey: `eve:run:v2:${"2".repeat(64)}`,
  batchKey: `eve:batch:v2:${"3".repeat(64)}`,
  instanceKey: `eve:instance:v2:${"4".repeat(64)}`,
  applicationId: "engineering-agent",
  tenantId: "tenant-1",
  monitorId: "incident",
  definitionVersion: "v1",
  correlationKey: "incident-42",
  rootEventKeys: [`eve:event:v1:${"5".repeat(64)}`],
  routeId: "eve",
  instruction: "Investigate the incident.",
  decision: { action: "wake" },
  evidence: { text: "untrusted" },
  inputHash: `eve:input:v1:${"6".repeat(64)}`,
} as unknown as PreparedAttentionWake;

describe("Eve attention adapter", () => {
  it("passes wakeKey into Eve with the complete by-value wake", async () => {
    const deliveries: Parameters<typeof fakeFrom>[0] = [];
    const route = createEveAttentionRoute({
      auth: null,
      address: (value) => `monitor:${value.correlationKey}`,
      from: fakeFrom(deliveries),
    });

    await expect(route.deliver(wake)).resolves.toEqual({
      address: "monitor:incident-42",
      sessionId: "session-1",
      turnId: wake.wakeKey,
    });
    expect(deliveries[0]?.options).toMatchObject({
      auth: null,
      idempotencyKey: wake.wakeKey,
      turnPolicy: "queue",
    });
    expect(JSON.parse(deliveries[0]?.message ?? "")).toEqual(
      JSON.parse(renderEveAttentionMessage(wake)),
    );
  });

  it("carries occurrence identity into direct Eve dispatch", async () => {
    const deliveries: Parameters<typeof fakeFrom>[0] = [];
    const occurrenceKey = `eve:occurrence:v1:${"7".repeat(64)}` as DirectDispatchRequest["occurrenceKey"];
    const idempotencyKey = await deriveAttentionDirectDispatchKey({
      occurrenceKey,
      bindingGeneration: "thread-v1",
    });
    const inputHash = await hashIdempotencyInput({ event: "one" });
    const request: DirectDispatchRequest = {
      idempotencyKey,
      inputHash,
      applicationId: "engineering-agent",
      tenantId: "tenant-1",
      eventKey: `eve:event:v1:${"8".repeat(64)}` as DirectDispatchRequest["eventKey"],
      occurrenceKey,
      event: {
        id: "provider-1",
        type: "message.changed",
        version: 1,
        data: { text: "hello" },
        source: {
          channelId: "slack",
          installationId: "installation-1",
          tenantId: "tenant-1",
        },
        origin: { kind: "external", depth: 0 },
      },
    };
    const adapter = createEveDirectDispatchAdapter({
      auth: null,
      address: "chat:thread-7",
      from: fakeFrom(deliveries),
      now: () => new Date("2026-01-01T00:00:00.000Z"),
    });

    await expect(adapter.dispatch(request)).resolves.toMatchObject({
      idempotencyKey,
      inputHash,
      dispatchedAt: "2026-01-01T00:00:00.000Z",
    });
    expect(deliveries[0]?.options.idempotencyKey).toBe(idempotencyKey);
    expect(JSON.parse(deliveries[0]?.message ?? "").event).toEqual(request.event);
  });
});
