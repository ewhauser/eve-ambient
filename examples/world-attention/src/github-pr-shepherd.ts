import {
  debounce,
  defineAmbientApplication,
  defineAmbientRule,
  ignore,
  wake,
} from "@ewhauser/eve-ambient";
import { world } from "@ewhauser/eve-ambient/world";
import type { AttentionWorld } from "@ewhauser/eve-ambient/protocol";
import {
  createEveGitHubAmbientChannel,
  createEveGitHubAttentionRoute,
  eveGitHubPullRequestActivity,
  type EveGitHubAmbientChannelOptions,
  type EveGitHubCheckSuiteActivityEvent,
} from "@ewhauser/eve-ambient-eve";

const failingConclusions = new Set([
  "action_required",
  "cancelled",
  "failure",
  "startup_failure",
  "timed_out",
]);

/** Coalesces a PR's webhook storm into one turn for its latest observed CI state. */
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
  decide({ events, eventKeys }) {
    const suites = new Map<string, EveGitHubCheckSuiteActivityEvent>();
    let closed = false;
    for (const event of events) {
      if (event.type === "github.pull-request") {
        if (["opened", "reopened", "synchronize"].includes(event.data.action)) {
          closed = false;
          suites.clear();
        } else if (event.data.action === "closed") {
          closed = true;
        }
        continue;
      }
      if (!closed) {
        suites.set(event.data.appSlug ?? `suite:${event.data.checkSuiteId}`, event);
      }
    }
    if (closed) return ignore({ reason: "pull request is closed" });

    const failures = [...suites.values()].filter(
      (event) =>
        event.data.action === "completed" &&
        event.data.conclusion !== null &&
        failingConclusions.has(event.data.conclusion),
    );
    const failure = failures.at(-1);
    if (failure === undefined) {
      return ignore({ reason: "current check suites do not show a failure" });
    }
    if (failure.replyTarget === undefined) {
      throw new Error("GitHub activity has no Eve continuation target");
    }

    return wake({
      target: failure.replyTarget,
      instruction:
        "Inspect this pull request and fix the smallest safe CI blocker. " +
        "If no safe fix is possible, report the exact blocker and next action.",
      decision: {
        reason: "one or more latest observed check suites failed",
        apps: failures.map((event) => event.data.appSlug),
      },
      evidence: {
        repository: failure.data.repository.fullName,
        pullRequestNumber: failure.data.pullRequestNumber,
        failures: failures.map((event) => ({
          app: event.data.appSlug,
          conclusion: event.data.conclusion,
          headSha: event.data.headSha,
        })),
        eventKeys,
      },
    });
  },
});

export interface PullRequestShepherdOptions {
  readonly applicationId: string;
  readonly callbackSecretEnv?: string | undefined;
  readonly eve: Parameters<typeof createEveGitHubAttentionRoute>[0];
  readonly github: Omit<EveGitHubAmbientChannelOptions, "publisher">;
  readonly world: AttentionWorld;
}

/** Creates the callback application and the Eve GitHub webhook listener. */
export function createPullRequestShepherd(options: PullRequestShepherdOptions) {
  const ambient = defineAmbientApplication({
    applicationId: options.applicationId,
    rules: [pullRequestShepherdRule],
    routes: [createEveGitHubAttentionRoute(options.eve)],
  }).with(
    world({
      world: options.world,
      ...(options.callbackSecretEnv === undefined
        ? {}
        : { callbackSecretEnv: options.callbackSecretEnv }),
    }),
  );

  const channel = createEveGitHubAmbientChannel({
    ...options.github,
    publisher: ambient,
  });
  return Object.freeze({ ambient, channel });
}
