import { defineAmbientApplication } from "@ewhauser/eve-ambient";
import {
  createEveAttentionRoute,
  type EveChannelAuth,
} from "@ewhauser/eve-ambient-eve";
import type { ChannelFrom } from "eve/channels";

import { blockedPullRequestRule } from "./rules/blocked-pull-request.js";

export function defineEngineeringApplication(options: {
  readonly applicationId: string;
  readonly eve: {
    readonly auth: EveChannelAuth;
    readonly from: ChannelFrom;
  };
}) {
  return defineAmbientApplication({
    applicationId: options.applicationId,
    rules: [blockedPullRequestRule],
    routes: [createEveAttentionRoute(options.eve)],
  });
}
