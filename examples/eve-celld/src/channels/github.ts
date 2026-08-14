import {
  defineChannelEvent,
  defineInboundChannel,
  type ChannelEvent,
  type PublishEventInput,
} from "@ewhauser/eve-ambient";
import type { EveDeliveryTarget } from "@ewhauser/eve-ambient-eve";
import { z } from "zod";

export const pullRequestChangedSchema = z.object({
  failingChecks: z.array(z.string().min(1)).max(100),
  mergeState: z.enum(["clean", "conflicting", "unknown"]),
  number: z.number().int().positive(),
  repository: z.string().min(1),
  reviewDecision: z.enum(["approved", "changes-requested", "review-required"]),
  state: z.enum(["open", "closed"]),
  title: z.string().min(1),
  updatedAt: z.string().datetime(),
});

export type PullRequestChangedData = z.infer<typeof pullRequestChangedSchema>;
export type PullRequestChangedEvent = ChannelEvent<
  "pull-request-changed",
  PullRequestChangedData,
  EveDeliveryTarget
>;
export type PullRequestChangedInput = PublishEventInput<
  PullRequestChangedData,
  EveDeliveryTarget
>;

/** Canonical GitHub events emitted after webhook verification and normalization. */
export const githubChannel = defineInboundChannel({
  id: "github",
  replyTarget: z.object({ address: z.string().min(1) }),
  inbound: {
    "pull-request-changed": defineChannelEvent({
      maxBytes: 256_000,
      schema: pullRequestChangedSchema,
    }),
  },
});
