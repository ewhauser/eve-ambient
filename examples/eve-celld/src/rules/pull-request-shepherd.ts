import { debounce, defineAmbientRule, ignore, wake } from "@ewhauser/eve-ambient";
import {
  eveGitHubPullRequestActivity,
  type EveGitHubCheckSuiteActivityEvent,
} from "@ewhauser/eve-ambient-eve";

const failingConclusions = new Set([
  "action_required",
  "cancelled",
  "failure",
  "startup_failure",
  "timed_out",
]);

/** Coalesces a push's PR and CI event storm into one current-state bot turn. */
export const pullRequestShepherdRule = defineAmbientRule({
  id: "pull-request-shepherd",
  version: "v1",
  channel: eveGitHubPullRequestActivity,
  policy: debounce({
    quiet: "2m",
    maxWait: "15m",
    cooldown: "30m",
    maxEvents: 100,
  }),
  correlationKey: (event) =>
    `${event.data.repository.id}#${event.data.pullRequestNumber}`,
  decide({ events, eventKeys }) {
    const suites = new Map<string, EveGitHubCheckSuiteActivityEvent>();
    let closed = false;
    for (const event of events) {
      if (event.type === "github.pull-request") {
        if (["opened", "reopened", "synchronize"].includes(event.data.action)) suites.clear();
        if (event.data.action === "closed") closed = true;
        if (["opened", "reopened", "synchronize"].includes(event.data.action)) closed = false;
        continue;
      }
      if (closed) continue;
      const key = event.data.appSlug ?? `suite:${event.data.checkSuiteId}`;
      suites.set(key, event);
    }
    if (closed) return ignore({ reason: "pull request is closed" });
    const failures = events.flatMap((event) => {
      if (event.type !== "github.check-suite") return [];
      const key = event.data.appSlug ?? `suite:${event.data.checkSuiteId}`;
      return suites.get(key) === event &&
        event.data.action === "completed" &&
        event.data.conclusion !== null &&
        failingConclusions.has(event.data.conclusion)
        ? [event]
        : [];
    });
    const failure = failures.at(-1);
    if (failure === undefined) {
      return ignore({ reason: "current check suites do not show a failure" });
    }
    const target = failure.replyTarget;
    if (target === undefined) throw new Error("GitHub activity has no Eve continuation target");
    return wake({
      target,
      instruction:
        "Inspect the current pull request and its checkout. Fix the smallest safe CI blocker, or report the exact blocker and next action if a safe fix is not possible.",
      decision: {
        reason: "one or more current check suites failed",
        apps: failures.map((event) => event.data.appSlug),
      },
      evidence: {
        repository: failure.data.repository.fullName,
        pullRequestNumber: failure.data.pullRequestNumber,
        failures: failures.map((event) => ({
          app: event.data.appSlug,
          checkSuiteId: event.data.checkSuiteId,
          conclusion: event.data.conclusion,
          headSha: event.data.headSha,
        })),
        activity: events.map((event, index) => ({
          eventKey: eventKeys[index]!,
          type: event.type,
          action: event.data.action,
          occurredAt: event.occurredAt ?? null,
          conclusion:
            event.type === "github.check-suite" ? event.data.conclusion : null,
        })),
      },
    });
  },
});
