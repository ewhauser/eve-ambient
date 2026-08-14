import { defineAmbientRule } from "@ewhauser/eve-ambient";
import type { PullRequestEvent } from "../channels/github.js";

export const blockedPullRequestRule = defineAmbientRule<PullRequestEvent>({
  id: "blocked-pull-request",
  version: "v1",
  mode: "active",
  policy: {
    buffer: {
      mode: "debounce",
      quietPeriodMs: 60_000,
      maxWaitMs: 300_000,
      maxEvents: 50,
      maxBytes: 1_000_000,
    },
    cooldownAfterWakeMs: 600_000,
  },
  // Admit every state change for a pull request so a later clean/closed event
  // can suppress an earlier blocked observation in the same debounce batch.
  matches: (event) => event.type === "github.pull-request.changed",
  correlationKey: (event) => `${event.data.repository}#${event.data.number}`,
  orderKey: (event) => event.data.updatedAt,
  async prepare(batch) {
    const latest = batch.branches.at(-1)?.event;
    if (latest === undefined) throw new Error("pull-request batch is empty");
    if (
      latest.data.state !== "open" ||
      (latest.data.mergeState === "clean" &&
        latest.data.reviewDecision !== "changes-requested" &&
        latest.data.failingChecks.length === 0)
    ) {
      return { kind: "ignore", decision: { reason: "pull request is unblocked" } };
    }
    const address = latest.replyTarget?.address;
    if (address === undefined) throw new Error("pull request has no Eve address");
    return {
      kind: "wake",
      routeId: "eve",
      instruction: "Review the blocked pull request and identify the smallest useful next action.",
      decision: { reason: "pull request remains blocked" },
      evidence: {
        address,
        repository: latest.data.repository,
        number: latest.data.number,
        title: latest.data.title,
        mergeState: latest.data.mergeState,
        reviewDecision: latest.data.reviewDecision,
        failingChecks: latest.data.failingChecks,
        rootEventKeys: batch.branches.map((branch) => branch.eventKey),
      },
    };
  },
});
