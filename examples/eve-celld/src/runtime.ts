import {
  MonitorRuntime,
  type CelldMailboxOptions,
  type MonitorRuntimeOptions,
} from "@ewhauser/eve-ambient";
import { PostgresMonitorStore } from "@ewhauser/eve-ambient/postgres";
import {
  createEveDeliveryChannel,
  type EveChannelAuth,
} from "@ewhauser/eve-ambient-eve";
import type { ChannelFrom } from "eve/channels";

import { githubChannel } from "./channels/github.js";
import { blockedPullRequestRule } from "./rules/blocked-pull-request.js";

export interface EveCelldRuntimeOptions
  extends Omit<
    MonitorRuntimeOptions,
    "channels" | "deliveryChannels" | "deployment" | "mailbox" | "store"
  > {
  readonly eve: {
    readonly auth: EveChannelAuth;
    readonly from: ChannelFrom;
  };
  readonly mailbox: CelldMailboxOptions;
  readonly pool: ConstructorParameters<typeof PostgresMonitorStore>[0]["pool"];
}

/** Experimental: celld owns complete mailbox payloads after append receipt. */
export function createEveCelldRuntime(
  options: EveCelldRuntimeOptions,
): MonitorRuntime {
  const { eve, mailbox, pool, ...runtime } = options;
  const delivery = createEveDeliveryChannel({ ...eve, id: "eve" });
  return new MonitorRuntime({
    ...runtime,
    channels: [githubChannel],
    deliveryChannels: [delivery],
    deployment: { monitors: [blockedPullRequestRule(delivery)] },
    mailbox,
    store: new PostgresMonitorStore({ pool }),
  });
}
