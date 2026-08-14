import {
  type DirectDispatchOptions,
  type MonitorRuntime,
} from "@ewhauser/eve-ambient";

import {
  slackChannel,
  type SlackMessageInput,
} from "./channels/slack.js";

/** Publishes one normalized Slack event after the provider request is verified. */
export function publishSlackMessage(
  runtime: MonitorRuntime,
  input: SlackMessageInput,
  direct: DirectDispatchOptions = {
    bindingGeneration: "example:no-direct-handler:v1",
    handlers: [],
  },
) {
  return runtime.publishChat(slackChannel, "message", input, direct);
}
