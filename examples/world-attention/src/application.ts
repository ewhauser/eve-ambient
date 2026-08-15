import {
  defineAmbientApplication,
  defineAmbientRule,
  immediate,
  wake,
  type JsonValue,
} from "@ewhauser/eve-ambient";
import {
  defineChannelCanonicalization,
  type CanonicalChannelEvent,
} from "@ewhauser/eve-ambient/idempotency";

export interface SupportEvent extends CanonicalChannelEvent {
  readonly type: "support.incident";
  readonly data: {
    readonly incidentId: string;
    readonly summary: string;
  };
}

export const supportChannel = defineChannelCanonicalization<SupportEvent, SupportEvent>({
  version: 1,
  canonicalize: (event) => event,
  partitionKey: (event) => event.data.incidentId,
});

const incidentRule = defineAmbientRule({
  id: "support-incident",
  version: "v1",
  channel: supportChannel,
  policy: immediate({ cooldown: "5m" }),
  decide({ latest, eventKeys }) {
    return wake({
      routeId: "support-agent",
      target: `incident:${latest.data.incidentId}`,
      instruction: "Investigate the incident and report the safest next action.",
      decision: { reason: "a support incident was received" },
      evidence: { summary: latest.data.summary, eventKeys },
    });
  },
});

export function defineSupportApplication(options: {
  readonly deliver: (target: JsonValue, instruction: string) => Promise<JsonValue>;
}) {
  return defineAmbientApplication({
    applicationId: "support-agent",
    rules: [incidentRule],
    routes: [
      {
        id: "support-agent",
        deliver: (prepared) => options.deliver(prepared.target, prepared.instruction),
      },
    ],
  });
}
