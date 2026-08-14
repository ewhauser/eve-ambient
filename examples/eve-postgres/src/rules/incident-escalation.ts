import {
  compileMonitor,
  defineMonitor,
  ignore,
  wake,
  type MonitorDeliveryChannel,
} from "@ewhauser/eve-ambient";
import type { EveDeliveryTarget } from "@ewhauser/eve-ambient-eve";

import {
  slackChannel,
  type SlackMessageEvent,
} from "../channels/slack.js";

type EscalationMetadata = Readonly<{ signals: readonly string[] }>;

function escalationSignals(events: readonly Readonly<SlackMessageEvent>[]): string[] {
  const signals = new Set<string>();
  for (const event of events) {
    if (event.data.severity === "critical") signals.add("critical-severity");
    if (/\b(?:sev[ -]?[01]|outage|customer impact)\b/i.test(event.data.text)) {
      signals.add("incident-language");
    }
  }
  return [...signals].sort();
}

/** One deterministic ambient rule evaluated against canonical Slack events. */
export function incidentEscalationRule(
  delivery: MonitorDeliveryChannel<EveDeliveryTarget>,
) {
  const definition = defineMonitor<
    SlackMessageEvent,
    EscalationMetadata,
    EscalationMetadata
  >({
    id: "incident-escalation",
    mode: "active",
    sources: [slackChannel.event("message", { phase: "undispatched" })],
    filter: ({ event }) =>
      event.actor?.isBot !== true && event.data.text.trim().length > 0,
    correlate: ({ event }) =>
      JSON.stringify([
        event.source.installationId,
        event.data.channelId,
        event.data.threadTs ?? event.data.messageTs,
      ]),
    buffer: {
      mode: "debounce",
      quietPeriod: "2s",
      maxWait: "20s",
      maxEvents: 50,
      maxBytes: 128_000,
    },
    decision: ({ events }) => {
      const signals = escalationSignals(events);
      return signals.length === 0
        ? ignore({ reason: "no-escalation-signal", metadata: { signals } })
        : wake({ reason: "incident-needs-attention", metadata: { signals } });
    },
    cooldown: { afterWake: "2m", during: "accumulate" },
    task: {
      instructions:
        "Assess the incident evidence, identify the next useful action, and respond only when you can materially help.",
      evidence: ({ events, decision, batch }) => ({
        messages: events.map((event) => ({
          actorId: event.actor?.id ?? null,
          messageTs: event.data.messageTs,
          ref: event.ref,
          severity: event.data.severity,
          text: event.data.text,
        })),
        signals: decision.metadata?.signals ?? [],
        completeness: batch,
      }),
    },
    route: ({ events }) => {
      const target = events.at(-1)?.replyTarget;
      return target === undefined
        ? null
        : { auth: "app", channel: delivery, target };
    },
    session: { strategy: "channel", idleTimeout: "24h" },
    limits: {
      perMonitor: { maxEventsPerMinute: 2_000, maxWakesPerHour: 30 },
      perKey: { maxWakesPerHour: 4 },
      overflow: "buffer",
    },
    retention: { decisions: "30d", dedupe: "7d" },
    metadata: {
      owner: "engineering-productivity",
      useCase: "ambient-slack-incidents",
    },
  });

  return compileMonitor(definition, "example:incident-escalation:v1");
}
