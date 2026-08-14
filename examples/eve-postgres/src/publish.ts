import { slackChannel, type SlackMessageInput } from "./channels/slack.js";
import type { EvePostgresApplication } from "./runtime.js";

/** Acknowledge the Slack transport only after this promise resolves. */
export function publishSlackMessage(
  application: EvePostgresApplication,
  input: SlackMessageInput,
) {
  return application.publisher.publish(slackChannel, input);
}
