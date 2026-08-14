import type {
  DirectDispatchRequest,
  MonitorDeliveryRequest,
} from "@ewhauser/eve-ambient";
import {
  deriveDirectDispatchKey,
  deriveEventKey,
  hashIdempotencyInput,
} from "@ewhauser/eve-ambient";
import type { ChannelFrom, ChannelSendOptions } from "eve/channels";
import { describe, expect, it } from "vitest";

import {
  createEveDeliveryChannel,
  createEveDirectDispatchHandler,
  renderEveMonitorMessage,
  type EveDeliveryTarget,
} from "../src/index.js";

function fakeFrom(
  deliveries: Array<{
    address: string;
    message: string;
    options: ChannelSendOptions;
  }>,
): ChannelFrom {
  return ((address: string) => ({
    async send(message: string, options: ChannelSendOptions) {
      deliveries.push({ address, message, options });
      return { id: "session-1" };
    },
  })) as ChannelFrom;
}

function monitorRequest(): MonitorDeliveryRequest<EveDeliveryTarget> {
  return {
    applicationId: "engineering-agent",
    auth: "app",
    evidence: {
      completeness: {
        bytes: 128,
        closedAt: "2026-08-14T12:00:00.000Z",
        closedBy: "immediate",
        eventCount: 1,
        isPartial: false,
        omittedBytes: 0,
        omittedEventCount: 0,
        openedAt: "2026-08-14T12:00:00.000Z",
      },
      createdAt: "2026-08-14T12:00:00.000Z",
      decision: { action: "wake", reason: "needs attention" },
      id: "snapshot-1",
      projectedEvidence: { text: "untrusted payload" },
      projectionVersion: "v1",
      runId: "run-1",
      runKey: "eve:run:v1:run-1",
      sourceEventKeys: ["eve:event:v1:event-1"],
    },
    idempotencyKey: "eve:wake:v1:wake-1",
    session: { strategy: "correlation" },
    target: { address: "monitor:incident-42" },
    taskInstructions: "Assess whether the incident needs escalation.",
    tenantId: "tenant-1",
    trigger: {
      correlationKeyHash: "correlation-1",
      definitionVersion: "v1",
      evidenceSnapshotId: "snapshot-1",
      kind: "monitor",
      monitorId: "incident-monitor",
      runId: "run-1",
      runKey: "eve:run:v1:run-1",
      sourceTypes: ["incident.changed"],
    },
  };
}

describe("createEveDeliveryChannel", () => {
  it("passes the wake key to Eve with a complete by-value message", async () => {
    const deliveries: Parameters<typeof fakeFrom>[0] = [];
    const channel = createEveDeliveryChannel({
      auth: null,
      from: fakeFrom(deliveries),
    });

    const receipt = await channel.deliver(monitorRequest());

    expect(deliveries).toHaveLength(1);
    expect(deliveries[0]?.address).toBe("monitor:incident-42");
    expect(deliveries[0]?.options).toMatchObject({
      auth: null,
      idempotencyKey: "eve:wake:v1:wake-1",
      turnPolicy: "queue",
    });
    expect(JSON.parse(deliveries[0]?.message ?? "")).toEqual(
      JSON.parse(renderEveMonitorMessage(monitorRequest())),
    );
    expect(receipt).toMatchObject({
      binding: {
        bindingRef: "eve:channel-address:monitor:incident-42",
        status: "active",
      },
      sessionId: "session-1",
      turnId: "eve:wake:v1:wake-1",
    });
  });

  it("fails closed when an existing binding names another address", async () => {
    const request = { ...monitorRequest(), bindingRef: "eve:channel-address:other" };
    const channel = createEveDeliveryChannel({ auth: null, from: fakeFrom([]) });

    await expect(channel.deliver(request)).rejects.toThrow("not existing binding");
  });

  it("rejects a binding projection that changes the canonical address", async () => {
    const channel = createEveDeliveryChannel({
      auth: null,
      binding: () => ({
        agentHasParticipated: true,
        bindingRef: "eve:channel-address:other",
        status: "active",
      }),
      from: fakeFrom([]),
    });

    await expect(channel.deliver(monitorRequest())).rejects.toThrow(
      "binding projection returned",
    );
  });
});

describe("createEveDirectDispatchHandler", () => {
  it("passes the direct dispatch key and full event to Eve", async () => {
    const deliveries: Parameters<typeof fakeFrom>[0] = [];
    const handler = createEveDirectDispatchHandler({
      address: () => "chat:thread-7",
      auth: null,
      from: fakeFrom(deliveries),
    });
    const eventKey = await deriveEventKey({
      applicationId: "engineering-agent",
      channelId: "slack",
      installationId: "installation-1",
      sourceEventId: "provider-1",
      tenantId: "tenant-1",
    });
    const request: DirectDispatchRequest = {
      applicationId: "engineering-agent",
      event: {
        data: { text: "hello" },
        id: "provider-1",
        origin: { depth: 0, kind: "external" },
        receivedAt: "2026-08-14T12:00:00.000Z",
        ref: "event-1",
        source: {
          channelId: "slack",
          installationId: "installation-1",
          tenantId: "tenant-1",
        },
        trace: { traceId: "trace-1" },
        type: "message.created",
        version: 1,
      },
      eventKey,
      idempotencyKey: await deriveDirectDispatchKey({
        acceptanceId: "acceptance-1",
        bindingGeneration: "v1",
        eventKey,
      }),
      inputHash: await hashIdempotencyInput({ text: "hello" }),
      tenantId: "tenant-1",
    };

    await expect(handler(request)).resolves.toEqual({
      turnId: request.idempotencyKey,
    });
    expect(deliveries[0]?.options.idempotencyKey).toBe(request.idempotencyKey);
    expect(JSON.parse(deliveries[0]?.message ?? "").event).toEqual(request.event);
  });
});
