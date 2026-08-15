import {
  defineChannel,
  type ChannelEvent,
} from "@ewhauser/eve-ambient";
import { z } from "zod";

export const pullRequestInputSchema = z.object({
  eventId: z.string().min(1),
  installationId: z.string().min(1),
  tenantId: z.string().min(1),
  repository: z.string().min(1),
  number: z.number().int().positive(),
  title: z.string().min(1),
  state: z.enum(["open", "closed"]),
  mergeState: z.enum(["clean", "conflicting", "unknown"]),
  reviewDecision: z.enum(["approved", "changes-requested", "review-required"]),
  failingChecks: z.array(z.string().min(1)).max(100),
  updatedAt: z.string().datetime(),
});

export type PullRequestInput = z.infer<typeof pullRequestInputSchema>;

export const githubChannel = defineChannel({
  version: 1,
  input: pullRequestInputSchema,
  map(input) {
    return {
      id: input.eventId,
      type: "github.pull-request.changed",
      version: 1,
      occurredAt: input.updatedAt,
      data: {
        repository: input.repository,
        number: input.number,
        title: input.title,
        state: input.state,
        mergeState: input.mergeState,
        reviewDecision: input.reviewDecision,
        failingChecks: input.failingChecks,
        updatedAt: input.updatedAt,
      },
      source: {
        channelId: "github",
        installationId: input.installationId,
        tenantId: input.tenantId,
      },
      replyTarget: { address: `github:${input.repository}#${input.number}` },
      subjects: [
        { namespace: "repository", key: input.repository },
        { namespace: "pull-request", key: `${input.repository}#${input.number}` },
      ],
      origin: { kind: "external", depth: 0 },
    };
  },
});

export type PullRequestEvent = ChannelEvent<typeof githubChannel>;
