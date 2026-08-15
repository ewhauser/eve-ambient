import { memory } from "@ewhauser/eve-ambient/memory";
import { WorldAttentionEngine } from "@ewhauser/eve-ambient/world";
import type { JsonValue } from "@ewhauser/eve-ambient";
import { expect, it } from "vitest";
import { defineSupportApplication, supportChannel, type SupportEvent } from "../src/application.js";
import { createSupportWorldApplication } from "../src/runtime.js";

it("defines attention once and binds it to the reference engine", async () => {
  const deliveries: JsonValue[] = [];
  const application = defineSupportApplication({
    async deliver(target) {
      deliveries.push(target);
      return { delivered: true };
    },
  }).with(memory());

  await application.publish(supportChannel, incident("incident-42"));
  await application.engine.runDue();

  expect(deliveries).toEqual(["incident:incident-42"]);
});

it("binds the same definition to a correlation World and callback endpoint", async () => {
  process.env.EXAMPLE_AMBIENT_SECRET = "test-secret";
  const application = createSupportWorldApplication({
    world: {
      stream: () => ({
        append: async () => {
          throw new Error("not used by this callback test");
        },
      }),
    },
    callbackSecretEnv: "EXAMPLE_AMBIENT_SECRET",
    async deliver() {
      return null;
    },
  });

  expect(application.engine).toBeInstanceOf(WorldAttentionEngine);
  const response = await application.fetch(
    new Request("https://application.example.test/ambient/unknown", {
      method: "POST",
      headers: { authorization: "Bearer test-secret" },
      body: "{}",
    }),
  );
  expect(response.status).toBe(404);
  delete process.env.EXAMPLE_AMBIENT_SECRET;
});

function incident(id: string): SupportEvent {
  return {
    id: `event-${id}`,
    type: "support.incident",
    version: 1,
    occurredAt: "2026-08-14T00:00:00.000Z",
    data: { incidentId: id, summary: "API error rate is elevated" },
    source: {
      channelId: "support",
      installationId: "support-production",
      tenantId: "tenant-1",
    },
    origin: { kind: "external", depth: 0 },
  };
}
