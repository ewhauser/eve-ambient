import {
  defineChannelCanonicalization,
  type CanonicalChannelEvent,
} from "@ewhauser/eve-ambient";
import { z } from "zod";

export const slackMessageInputSchema = z.object({
  eventId: z.string().min(1),
  installationId: z.string().min(1),
  tenantId: z.string().min(1),
  occurredAt: z.string().datetime(),
  channelId: z.string().min(1),
  incidentId: z.string().min(1),
  severity: z.enum(["info", "warning", "critical"]),
  text: z.string().min(1),
  threadTs: z.string().min(1),
});

export type SlackMessageInput = z.infer<typeof slackMessageInputSchema>;
export type SlackMessageEvent = CanonicalChannelEvent<
  "slack.message",
  {
    readonly channelId: string;
    readonly incidentId: string;
    readonly severity: "info" | "warning" | "critical";
    readonly text: string;
    readonly threadTs: string;
  },
  { readonly address: string }
>;

/** The authenticated Slack adapter owns this deterministic normalization. */
export const slackChannel = defineChannelCanonicalization({
  version: 1,
  canonicalize(raw: SlackMessageInput): SlackMessageEvent {
    const input = slackMessageInputSchema.parse(raw);
    return {
      id: input.eventId,
      type: "slack.message",
      version: 1,
      occurredAt: input.occurredAt,
      data: {
        channelId: input.channelId,
        incidentId: input.incidentId,
        severity: input.severity,
        text: input.text,
        threadTs: input.threadTs,
      },
      source: {
        channelId: "slack",
        installationId: input.installationId,
        tenantId: input.tenantId,
      },
      replyTarget: { address: `slack:${input.channelId}:${input.threadTs}` },
      subjects: [{ namespace: "incident", key: input.incidentId }],
      origin: { kind: "external", depth: 0 },
    };
  },
});
