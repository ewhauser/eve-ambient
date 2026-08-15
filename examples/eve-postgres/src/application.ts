import { defineAmbientApplication } from "@ewhauser/eve-ambient";
import {
  createEveAttentionRoute,
  type EveChannelAuth,
} from "@ewhauser/eve-ambient-eve";
import type { ChannelFrom } from "eve/channels";

import { incidentEscalationRule } from "./rules/incident-escalation.js";

export function defineEngineeringApplication(options: {
  readonly applicationId: string;
  readonly eve: {
    readonly auth: EveChannelAuth;
    readonly from: ChannelFrom;
  };
}) {
  return defineAmbientApplication({
    applicationId: options.applicationId,
    rules: [incidentEscalationRule],
    routes: [createEveAttentionRoute(options.eve)],
  });
}
