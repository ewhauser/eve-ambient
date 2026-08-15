import { memory } from "@ewhauser/eve-ambient/memory";
import { VirtualMonitorClock } from "@ewhauser/eve-ambient/testing";
import type { ChannelFrom, ChannelSendOptions } from "eve/channels";
import { expect, it } from "vitest";
import { defineEngineeringApplication } from "../src/application.js";
import { githubChannel } from "../src/channels/github.js";
import { createEveCelldApplication } from "../src/runtime.js";

it("publishes a GitHub channel rule into one idempotent Eve wake", async () => {
  const { ambient, clock, deliveries } = testApplication();

  const receipt = await ambient.publish(githubChannel, {
    eventId: "delivery-1",
    installationId: "github-installation",
    tenantId: "tenant-1",
    repository: "ewhauser/eve-ambient",
    number: 20,
    title: "Replace the attention runtime",
    state: "open",
    mergeState: "clean",
    reviewDecision: "review-required",
    failingChecks: ["ci"],
    updatedAt: "2026-01-01T00:00:00.000Z",
  });
  clock.advance(60_000);
  await ambient.engine.runDue();

  expect(receipt.attention.branchKeys).toHaveLength(1);
  expect(deliveries).toHaveLength(1);
  expect(deliveries[0]?.address).toBe("github:ewhauser/eve-ambient#20");
  expect(deliveries[0]?.options.idempotencyKey).toMatch(/^eve:wake:v2:/);
  expect(JSON.parse(deliveries[0]?.message ?? "").evidence.value.failingChecks).toEqual(["ci"]);
});

it("lets a later clean channel event suppress a debounced blocked wake", async () => {
  const { ambient, clock, deliveries } = testApplication();
  const base = {
    installationId: "github-installation",
    tenantId: "tenant-1",
    repository: "ewhauser/eve-ambient",
    number: 20,
    title: "Replace the attention runtime",
    state: "open" as const,
  };

  await ambient.publish(githubChannel, {
    ...base,
    eventId: "delivery-blocked",
    mergeState: "conflicting",
    reviewDecision: "changes-requested",
    failingChecks: ["ci"],
    updatedAt: "2026-01-01T00:00:00.000Z",
  });
  await ambient.publish(githubChannel, {
    ...base,
    eventId: "delivery-clean",
    mergeState: "clean",
    reviewDecision: "approved",
    failingChecks: [],
    updatedAt: "2026-01-01T00:00:30.000Z",
  });
  clock.advance(60_000);

  await expect(ambient.engine.runDue()).resolves.toMatchObject({ ignored: 1 });
  expect(deliveries).toHaveLength(0);
});

it("binds celld admission and callbacks from one runtime configuration", async () => {
  const application = createEveCelldApplication({
    applicationId: "engineering-agent",
    celld: {
      url: "https://celld.example.test",
      secret: "test-secret",
      async fetch(_input, init) {
        const fanout = JSON.parse(String(init?.body));
        return Response.json({
          eventKey: fanout.eventKey,
          occurrenceKey: fanout.occurrenceKey,
          inputHash: fanout.inputHash,
          manifestHash: fanout.manifestHash,
          branchKeys: fanout.branches.map(
            (branch: { readonly branchKey: string }) => branch.branchKey,
          ),
          acceptedAt: "2026-01-01T00:00:00.000Z",
          dedupeExpiresAt: "2026-01-08T00:00:00.000Z",
        });
      },
    },
    eve: {
      auth: null,
      from: (() => ({ send: async () => ({ id: "unused" }) })) as unknown as ChannelFrom,
    },
  });

  await expect(
    application.publish(githubChannel, {
      eventId: "delivery-runtime",
      installationId: "github-installation",
      tenantId: "tenant-1",
      repository: "ewhauser/eve-ambient",
      number: 20,
      title: "Simplify the consumer API",
      state: "open",
      mergeState: "conflicting",
      reviewDecision: "changes-requested",
      failingChecks: ["ci"],
      updatedAt: "2026-01-01T00:00:00.000Z",
    }),
  ).resolves.toMatchObject({ attention: { branchKeys: expect.any(Array) } });

  const response = await application.fetch(
    new Request("https://app.example.test/ambient/unknown", {
      method: "POST",
      headers: { authorization: "Bearer test-secret" },
      body: "{}",
    }),
  );
  expect(response.status).toBe(404);
});

function testApplication() {
  const deliveries: Array<{
    readonly address: string;
    readonly message: string;
    readonly options: ChannelSendOptions;
  }> = [];
  const from = ((address: string) => ({
    async send(message: string, options: ChannelSendOptions) {
      deliveries.push({ address, message, options });
      return { id: "session-1" };
    },
  })) as unknown as ChannelFrom;
  const clock = new VirtualMonitorClock();
  const ambient = defineEngineeringApplication({
    applicationId: "engineering-agent",
    eve: { auth: null, from },
  }).with(memory({ clock }));
  return { ambient, clock, deliveries };
}
