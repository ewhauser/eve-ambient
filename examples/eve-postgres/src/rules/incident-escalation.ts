import { defineAmbientRule } from "@ewhauser/eve-ambient";
import type { SlackMessageEvent } from "../channels/slack.js";

export const incidentEscalationRule = defineAmbientRule<SlackMessageEvent>({
  id: "incident-escalation",
  version: "v1",
  mode: "active",
  policy: {
    buffer: {
      mode: "debounce",
      quietPeriodMs: 30_000,
      maxWaitMs: 120_000,
      maxEvents: 100,
      maxBytes: 1_000_000,
    },
    cooldownAfterWakeMs: 300_000,
  },
  matches: (event) => event.type === "slack.message" && event.data.severity !== "info",
  correlationKey: (event) => event.data.incidentId,
  orderKey: (event) => event.occurredAt ?? event.id,
  async prepare(batch) {
    const critical = batch.branches.some(
      (branch) => branch.event.data.severity === "critical",
    );
    if (!critical) {
      return {
        kind: "ignore",
        decision: { reason: "no critical incident evidence" },
      };
    }
    const address = batch.branches.at(-1)?.event.replyTarget?.address;
    if (address === undefined) throw new Error("incident event has no Slack reply target");
    return {
      kind: "wake",
      routeId: "eve",
      instruction: "Assess the incident and coordinate the next escalation step.",
      decision: { reason: "critical incident evidence", severity: "critical" },
      evidence: {
        address,
        messages: batch.branches.map((branch) => ({
          eventKey: branch.eventKey,
          occurredAt: branch.event.occurredAt ?? null,
          text: branch.event.data.text,
          severity: branch.event.data.severity,
        })),
      },
    };
  },
});
