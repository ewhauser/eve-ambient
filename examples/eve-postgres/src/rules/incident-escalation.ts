import { debounce, defineAmbientRule, ignore, wake } from "@ewhauser/eve-ambient";
import { slackChannel } from "../channels/slack.js";

export const incidentEscalationRule = defineAmbientRule({
  id: "incident-escalation",
  version: "v1",
  channel: slackChannel,
  policy: debounce({ quiet: "30s", maxWait: "2m", cooldown: "5m" }),
  matches: (event) => event.type === "slack.message" && event.data.severity !== "info",
  correlationKey: (event) => event.data.incidentId,
  decide({ events, latest, eventKeys }) {
    const critical = events.some((event) => event.data.severity === "critical");
    if (!critical) {
      return ignore({ reason: "no critical incident evidence" });
    }
    const address = latest.replyTarget?.address;
    if (address === undefined) throw new Error("incident event has no Slack reply target");
    return wake({
      target: address,
      instruction: "Assess the incident and coordinate the next escalation step.",
      decision: { reason: "critical incident evidence", severity: "critical" },
      evidence: {
        messages: events.map((event, index) => ({
          eventKey: eventKeys[index]!,
          occurredAt: event.occurredAt ?? null,
          text: event.data.text,
          severity: event.data.severity,
        })),
      },
    });
  },
});
