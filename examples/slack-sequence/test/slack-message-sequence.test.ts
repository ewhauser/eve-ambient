import { memory } from "@ewhauser/eve-ambient/memory";
import { VirtualMonitorClock } from "@ewhauser/eve-ambient/testing";
import { WorkflowAttentionEngine } from "@ewhauser/eve-ambient/workflow";
import { expect, it } from "vitest";
import {
  createWorkflowSlackSequenceApplication,
  defineSlackSequenceApplication,
  slackMessages,
  type SlackMessageInput,
  type TurnRequest,
} from "../src/slack-message-sequence.js";

it("delivers one turn for message A followed by message B", async () => {
  const clock = new VirtualMonitorClock();
  const turns: TurnRequest[] = [];
  const application = defineSlackSequenceApplication({
    async enqueue(request) {
      turns.push(request);
      return { queued: true };
    },
  }).with(memory({ clock }));

  await application.publish(
    slackMessages,
    message("event-a", "message A", "2026-08-15T00:00:00.000Z"),
  );
  clock.advance(60_000);
  await application.publish(
    slackMessages,
    message("event-b", "message B", "2026-08-15T00:01:00.000Z"),
  );
  clock.advance(120_000);

  await expect(application.engine.runDue()).resolves.toMatchObject({ delivered: 1 });
  expect(turns).toHaveLength(1);
  expect(turns[0]).toMatchObject({
    address: "slack:workspace-1:channel-1",
    instruction: "Review the Slack conversation and take the configured follow-up action.",
  });
  expect(turns[0]?.idempotencyKey).toMatch(/^eve:wake:v2:/);
});

it("does not deliver when message B precedes message A", async () => {
  const clock = new VirtualMonitorClock();
  const turns: TurnRequest[] = [];
  const application = defineSlackSequenceApplication({
    async enqueue(request) {
      turns.push(request);
      return { queued: true };
    },
  }).with(memory({ clock }));

  await application.publish(
    slackMessages,
    message("event-b", "message B", "2026-08-15T00:00:00.000Z"),
  );
  clock.advance(60_000);
  await application.publish(
    slackMessages,
    message("event-a", "message A", "2026-08-15T00:01:00.000Z"),
  );
  clock.advance(120_000);

  await expect(application.engine.runDue()).resolves.toMatchObject({ ignored: 1 });
  expect(turns).toEqual([]);
});

it("binds the definition to the standard Workflow runtime", async () => {
  process.env.EXAMPLE_AMBIENT_SECRET = "test-secret";
  const application = createWorkflowSlackSequenceApplication({
    async enqueue() {
      return null;
    },
  }, {
    callbackUrl: "https://application.example.test",
    callbackSecretEnv: "EXAMPLE_AMBIENT_SECRET",
  });

  expect(application.engine).toBeInstanceOf(WorkflowAttentionEngine);
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

function message(eventId: string, text: string, occurredAt: string): SlackMessageInput {
  return {
    eventId,
    occurredAt,
    tenantId: "tenant-1",
    workspaceId: "workspace-1",
    channelId: "channel-1",
    userId: "user-1",
    text,
  };
}
