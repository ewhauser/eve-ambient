import { type MonitorRuntime } from "@ewhauser/eve-ambient";

import {
  githubChannel,
  type PullRequestChangedInput,
} from "./channels/github.js";

/** Publishes one normalized GitHub event after webhook verification. */
export function publishPullRequestChanged(
  runtime: MonitorRuntime,
  input: PullRequestChangedInput,
) {
  return runtime.publish(githubChannel, "pull-request-changed", input);
}
