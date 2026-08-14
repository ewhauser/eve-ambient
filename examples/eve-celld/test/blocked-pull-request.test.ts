import {
  createAmbientPublisher,
  createAttentionCallbacks,
} from "@ewhauser/eve-ambient";
import { MemoryAttentionEngine } from "@ewhauser/eve-ambient/memory";
import { VirtualMonitorClock } from "@ewhauser/eve-ambient/testing";
import { createEveAttentionRoute } from "@ewhauser/eve-ambient-eve";
import type { ChannelFrom, ChannelSendOptions } from "eve/channels";
import { expect, it } from "vitest";
import { githubChannel } from "../src/channels/github.js";
import { blockedPullRequestRule } from "../src/rules/blocked-pull-request.js";

it("publishes a GitHub channel rule into one idempotent Eve wake", async () => {
  const deliveries: Array<{ message: string; options: ChannelSendOptions }> = [];
  const from = (() => ({
    async send(message: string, options: ChannelSendOptions) {
      deliveries.push({ message, options });
      return { id: "session-1" };
    },
  })) as unknown as ChannelFrom;
  const clock = new VirtualMonitorClock();
  const route = createEveAttentionRoute({ id: "eve", auth: null, address: "pull-request", from });
  const callbacks = createAttentionCallbacks({
    rules: [blockedPullRequestRule],
    routes: [route],
    clock,
  });
  const engine = new MemoryAttentionEngine({ callbacks, clock });
  const publisher = createAmbientPublisher({
    applicationId: "engineering-agent",
    engine,
    rules: [blockedPullRequestRule],
  });

  const receipt = await publisher.publish(githubChannel, {
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
  await engine.runDue();

  expect(receipt.attention.branchKeys).toHaveLength(1);
  expect(deliveries).toHaveLength(1);
  expect(deliveries[0]?.options.idempotencyKey).toMatch(/^eve:wake:v2:/);
  expect(JSON.parse(deliveries[0]?.message ?? "").evidence.value.failingChecks).toEqual(["ci"]);
});

it("lets a later clean channel event suppress a debounced blocked wake", async () => {
  const deliveries: string[] = [];
  const from = (() => ({
    async send(message: string) {
      deliveries.push(message);
      return { id: "session-1" };
    },
  })) as unknown as ChannelFrom;
  const clock = new VirtualMonitorClock();
  const route = createEveAttentionRoute({ id: "eve", auth: null, address: "pull-request", from });
  const callbacks = createAttentionCallbacks({
    rules: [blockedPullRequestRule],
    routes: [route],
    clock,
  });
  const engine = new MemoryAttentionEngine({ callbacks, clock });
  const publisher = createAmbientPublisher({
    applicationId: "engineering-agent",
    engine,
    rules: [blockedPullRequestRule],
  });
  const base = {
    installationId: "github-installation",
    tenantId: "tenant-1",
    repository: "ewhauser/eve-ambient",
    number: 20,
    title: "Replace the attention runtime",
    state: "open" as const,
  };

  await publisher.publish(githubChannel, {
    ...base,
    eventId: "delivery-blocked",
    mergeState: "conflicting",
    reviewDecision: "changes-requested",
    failingChecks: ["ci"],
    updatedAt: "2026-01-01T00:00:00.000Z",
  });
  await publisher.publish(githubChannel, {
    ...base,
    eventId: "delivery-clean",
    mergeState: "clean",
    reviewDecision: "approved",
    failingChecks: [],
    updatedAt: "2026-01-01T00:00:30.000Z",
  });
  clock.advance(60_000);

  await expect(engine.runDue()).resolves.toMatchObject({ ignored: 1 });
  expect(deliveries).toHaveLength(0);
});
