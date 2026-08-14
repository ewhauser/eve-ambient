import {
  MonitorRuntime,
  type MonitorRuntimeOptions,
} from "@ewhauser/eve-ambient";
import { PostgresMonitorStore } from "@ewhauser/eve-ambient/postgres";
import {
  createEveDeliveryChannel,
  type EveChannelAuth,
} from "@ewhauser/eve-ambient-eve";
import type { ChannelFrom } from "eve/channels";
import type { Pool } from "pg";

import { slackChannel } from "./channels/slack.js";
import { incidentEscalationRule } from "./rules/incident-escalation.js";

export interface EvePostgresRuntimeOptions
  extends Omit<
    MonitorRuntimeOptions,
    "channels" | "deliveryChannels" | "deployment" | "mailbox" | "store"
  > {
  readonly eve: {
    readonly auth: EveChannelAuth;
    readonly from: ChannelFrom;
  };
  readonly pool: Pool;
}

/** Supported default: PostgreSQL owns each complete payload until handoff. */
export function createEvePostgresRuntime(
  options: EvePostgresRuntimeOptions,
): MonitorRuntime {
  const { eve, pool, ...runtime } = options;
  const delivery = createEveDeliveryChannel({ ...eve, id: "eve" });
  return new MonitorRuntime({
    ...runtime,
    channels: [slackChannel],
    deliveryChannels: [delivery],
    deployment: { monitors: [incidentEscalationRule(delivery)] },
    store: new PostgresMonitorStore({ pool }),
  });
}
