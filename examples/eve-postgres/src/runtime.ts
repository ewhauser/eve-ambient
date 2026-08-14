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

export interface EvePostgresRuntimeOptions
  extends Omit<
    MonitorRuntimeOptions,
    "deliveryChannels" | "mailbox" | "store"
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
  return new MonitorRuntime({
    ...runtime,
    deliveryChannels: [createEveDeliveryChannel(eve)],
    store: new PostgresMonitorStore({ pool }),
  });
}
