import {
  defineChannelEvent,
  defineInboundChannel,
  type ChannelEvent,
  type PublishEventInput,
} from "@ewhauser/eve-ambient";
import type { EveDeliveryTarget } from "@ewhauser/eve-ambient-eve";
import { z } from "zod";

export const slackMessageSchema = z.object({
  channelId: z.string().min(1),
  messageTs: z.string().min(1),
  severity: z.enum(["info", "warning", "critical"]),
  text: z.string().min(1),
  threadTs: z.string().min(1).optional(),
});

export type SlackMessageData = z.infer<typeof slackMessageSchema>;
export type SlackMessageEvent = ChannelEvent<
  "message",
  SlackMessageData,
  EveDeliveryTarget
>;
export type SlackMessageInput = PublishEventInput<
  SlackMessageData,
  EveDeliveryTarget
>;

/** Canonical Slack events emitted by the application's authenticated adapter. */
export const slackChannel = defineInboundChannel({
  id: "slack",
  replyTarget: z.object({ address: z.string().min(1) }),
  inbound: {
    message: defineChannelEvent({
      chat: true,
      maxBytes: 128_000,
      schema: slackMessageSchema,
    }),
  },
});
