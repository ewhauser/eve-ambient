import { describe, expect, it } from "vitest";
import {
  debounce,
  defineAmbientApplication,
  defineAmbientRule,
  defineChannel,
  immediate,
  wake,
  type PreparedAttentionWake,
  type StandardChannelSchema,
} from "../src/index.js";
import { canonicalizeChannelDelivery } from "../src/idempotency.js";
import { memory } from "../src/memory.js";

describe("consumer application API", () => {
  it("binds heterogeneous channel rules to one application", async () => {
    const channel = testChannel("messages");
    const metrics = metricChannel();
    const rule = defineAmbientRule({
      id: "mention",
      version: "v1",
      channel,
      policy: immediate(),
      decide: ({ latest }) =>
        wake({
          target: latest.replyTarget.address,
          instruction: "Respond to the mention.",
          evidence: latest.data,
        }),
    });
    const metricRule = defineAmbientRule({
      id: "popular-repository",
      version: "v1",
      channel: metrics,
      policy: immediate(),
      decide: ({ latest }) =>
        wake({
          target: `repository:${latest.data.repository}`,
          instruction: "Review the newly popular repository.",
          evidence: { stars: latest.data.stars },
        }),
    });
    const deliveries: PreparedAttentionWake[] = [];
    const clock = { now: () => new Date("2026-01-01T00:00:00.000Z") };
    const application = defineAmbientApplication({
      applicationId: "test-application",
      rules: [rule, metricRule],
      routes: [{
        id: "custom-delivery",
        async deliver(prepared) {
          deliveries.push(prepared);
          return { accepted: true };
        },
      }],
    }).with(memory({ clock }));

    const message = await application.publish(channel, input("message-event"));
    const metric = await application.publish(metrics, {
      id: "metric-event",
      repository: "ewhauser/eve-ambient",
      stars: 100,
    });
    await application.engine.runDue();

    expect(message.attention.branchKeys).toHaveLength(1);
    expect(metric.attention.branchKeys).toHaveLength(1);
    expect(deliveries).toHaveLength(2);
    expect(deliveries.map((delivery) => delivery.target).sort()).toEqual([
      "repository:ewhauser/eve-ambient",
      "thread:42",
    ]);
    expect(deliveries.every((delivery) => delivery.routeId === "custom-delivery")).toBe(true);
    expect(deliveries.every((delivery) => delivery.correlationKey === "default")).toBe(true);
  });

  it("parses readable duration policies with bounded defaults", () => {
    expect(debounce({ quiet: "30s", maxWait: "5m", cooldown: "1h" })).toEqual({
      buffer: {
        mode: "debounce",
        quietPeriodMs: 30_000,
        maxWaitMs: 300_000,
        maxEvents: 100,
        maxBytes: 1_000_000,
      },
      cooldownAfterWakeMs: 3_600_000,
    });
    expect(() => debounce({ quiet: "0s", maxWait: "1m" })).toThrow(
      "quiet must be a positive safe integer",
    );
  });

  it("accepts asynchronous Standard Schema channel validation", async () => {
    const schema = {
      "~standard": {
        version: 1 as const,
        async validate(value: unknown) {
          return typeof value === "object" && value !== null && "id" in value
            ? { value: value as { readonly id: string } }
            : { issues: [{ message: "id is required" }] };
        },
      },
    } satisfies StandardChannelSchema<{ readonly id: string }>;
    const channel = defineChannel({
      version: 1,
      input: schema,
      map: (value) => ({
        id: value.id,
        type: "standard-schema.event" as const,
        version: 1,
        data: null,
        source: {
          channelId: "standard-schema",
          installationId: "installation-1",
          tenantId: "tenant-1",
        },
        origin: { kind: "external" as const, depth: 0 },
      }),
      partitionKey: (event) => event.id,
    });

    await expect(
      canonicalizeChannelDelivery(channel, { id: "event-1" }, { applicationId: "app" }),
    ).resolves.toMatchObject({ payload: { event: { id: "event-1" } } });
    await expect(
      canonicalizeChannelDelivery(channel, {} as { readonly id: string }, { applicationId: "app" }),
    ).rejects.toThrow("channel input is invalid: id is required");
  });
});

function testChannel(channelId: string) {
  return defineChannel({
    version: 1,
    input: {
      parse(value: unknown) {
        if (value === null || typeof value !== "object") throw new TypeError("invalid input");
        return value as ReturnType<typeof input>;
      },
    },
    map(value) {
      return {
        id: value.id,
        type: "message.changed" as const,
        version: 1,
        occurredAt: "2026-01-01T00:00:00.000Z",
        data: { thread: value.thread, text: value.text },
        source: {
          channelId,
          installationId: "installation-1",
          tenantId: "tenant-1",
        },
        replyTarget: { address: `thread:${value.thread}` },
        origin: { kind: "external" as const, depth: 0 },
      };
    },
    partitionKey: (event) => event.data.thread,
  });
}

function input(id: string) {
  return { id, thread: "42", text: "hello" };
}

function metricChannel() {
  return defineChannel({
    version: 1,
    input: {
      parse(value: unknown) {
        if (value === null || typeof value !== "object") throw new TypeError("invalid metric");
        return value as {
          readonly id: string;
          readonly repository: string;
          readonly stars: number;
        };
      },
    },
    map(value) {
      return {
        id: value.id,
        type: "repository.metric" as const,
        version: 1,
        data: { repository: value.repository, stars: value.stars },
        source: {
          channelId: "metrics",
          installationId: "installation-1",
          tenantId: "tenant-1",
        },
        origin: { kind: "external" as const, depth: 0 },
      };
    },
    partitionKey: (event) => event.data.repository,
  });
}
