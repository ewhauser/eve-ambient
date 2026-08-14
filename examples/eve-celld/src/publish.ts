import { githubChannel, type PullRequestInput } from "./channels/github.js";
import type { EveCelldApplication } from "./runtime.js";

/** Acknowledge the GitHub webhook only after this promise resolves. */
export function publishPullRequest(
  application: EveCelldApplication,
  input: PullRequestInput,
) {
  return application.publisher.publish(githubChannel, input);
}
