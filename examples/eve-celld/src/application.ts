import { defineAmbientApplication } from "@ewhauser/eve-ambient";
import {
  createEveGitHubAttentionRoute,
  type EveChannelAuth,
} from "@ewhauser/eve-ambient-eve";
import type { ChannelFrom } from "eve/channels";
import type { GitHubChannelState } from "eve/channels/github";

import { pullRequestShepherdRule } from "./rules/pull-request-shepherd.js";

export function defineEngineeringApplication(options: {
  readonly applicationId: string;
  readonly eve: {
    readonly auth: EveChannelAuth;
    readonly from: ChannelFrom<GitHubChannelState>;
  };
}) {
  return defineAmbientApplication({
    applicationId: options.applicationId,
    rules: [pullRequestShepherdRule],
    routes: [createEveGitHubAttentionRoute(options.eve)],
  });
}
