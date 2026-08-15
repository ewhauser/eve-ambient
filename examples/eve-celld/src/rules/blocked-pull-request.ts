import { debounce, defineAmbientRule, ignore, wake } from "@ewhauser/eve-ambient";
import { githubChannel } from "../channels/github.js";

export const blockedPullRequestRule = defineAmbientRule({
  id: "blocked-pull-request",
  version: "v1",
  channel: githubChannel,
  policy: debounce({
    quiet: "1m",
    maxWait: "5m",
    cooldown: "10m",
    maxEvents: 50,
  }),
  // Admit every state change for a pull request so a later clean/closed event
  // can suppress an earlier blocked observation in the same debounce batch.
  correlationKey: (event) => `${event.data.repository}#${event.data.number}`,
  orderKey: (event) => event.data.updatedAt,
  decide({ latest }) {
    if (
      latest.data.state !== "open" ||
      (latest.data.mergeState === "clean" &&
        latest.data.reviewDecision !== "changes-requested" &&
        latest.data.failingChecks.length === 0)
    ) {
      return ignore({ reason: "pull request is unblocked" });
    }
    const address = latest.replyTarget?.address;
    if (address === undefined) throw new Error("pull request has no Eve address");
    return wake({
      target: address,
      instruction: "Review the blocked pull request and identify the smallest useful next action.",
      decision: { reason: "pull request remains blocked" },
      evidence: {
        repository: latest.data.repository,
        number: latest.data.number,
        title: latest.data.title,
        mergeState: latest.data.mergeState,
        reviewDecision: latest.data.reviewDecision,
        failingChecks: latest.data.failingChecks,
      },
    });
  },
});
