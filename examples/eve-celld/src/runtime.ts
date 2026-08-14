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

export interface EveCelldRuntimeOptions
  extends Omit<
    MonitorRuntimeOptions,
    "deliveryChannels" | "mailbox" | "store"
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
  return new MonitorRuntime({
    ...runtime,
    deliveryChannels: [createEveDeliveryChannel(eve)],
    mailbox,
    store: new PostgresMonitorStore({ pool }),
  });
}
