import { celld } from "@ewhauser/eve-ambient/celld";
import type { EveChannelAuth } from "@ewhauser/eve-ambient-eve";
import type { ChannelFrom } from "eve/channels";
import type { GitHubChannelState } from "eve/channels/github";

import { defineEngineeringApplication } from "./application.js";

export interface EveCelldApplicationOptions {
  readonly applicationId: string;
  readonly celld: {
    readonly url: string;
    readonly secret: string;
    readonly fetch?: typeof fetch | undefined;
  };
  readonly eve: {
    readonly auth: EveChannelAuth;
    readonly from: ChannelFrom<GitHubChannelState>;
  };
}

/** celld owns the entire durable workflow; no PostgreSQL pool is accepted. */
export function createEveCelldApplication(
  options: EveCelldApplicationOptions,
) {
  return defineEngineeringApplication(options).with(celld(options.celld));
}
