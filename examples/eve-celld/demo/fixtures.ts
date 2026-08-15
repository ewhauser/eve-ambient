export type SyntheticGitHubEvent = {
  readonly deliveryId: string;
  readonly event: "check_suite" | "pull_request";
  readonly label: string;
  readonly payload: object;
};

export function checkSuiteEvent(options: {
  readonly appSlug: string;
  readonly checkSuiteId: number;
  readonly conclusion: string | null;
  readonly deliveryId: string;
  readonly label: string;
  readonly pullRequestNumber: number;
  readonly updatedAt: string;
}): SyntheticGitHubEvent {
  const completed = options.conclusion !== null;
  return {
    deliveryId: options.deliveryId,
    event: "check_suite",
    label: options.label,
    payload: {
      action: completed ? "completed" : "requested",
      installation: { id: 17 },
      repository: repository(),
      sender: sender(),
      check_suite: {
        id: options.checkSuiteId,
        app: { slug: options.appSlug },
        conclusion: options.conclusion,
        head_sha: `head-${options.pullRequestNumber}`,
        pull_requests: [{ number: options.pullRequestNumber }],
        status: completed ? "completed" : "queued",
        updated_at: options.updatedAt,
      },
    },
  };
}

export function pullRequestEvent(options: {
  readonly action: "closed" | "synchronize";
  readonly deliveryId: string;
  readonly label: string;
  readonly pullRequestNumber: number;
  readonly updatedAt: string;
}): SyntheticGitHubEvent {
  return {
    deliveryId: options.deliveryId,
    event: "pull_request",
    label: options.label,
    payload: {
      action: options.action,
      installation: { id: 17 },
      repository: repository(),
      sender: sender(),
      pull_request: {
        number: options.pullRequestNumber,
        head: {
          ref: `feature-${options.pullRequestNumber}`,
          sha: `head-${options.pullRequestNumber}`,
        },
        base: { ref: "main", sha: "base-123", repo: { default_branch: "main" } },
        updated_at: options.updatedAt,
      },
    },
  };
}

function repository() {
  return {
    id: 101,
    full_name: "ewhauser/eve-ambient",
    name: "eve-ambient",
    owner: { login: "ewhauser" },
    private: false,
  };
}

function sender() {
  return {
    id: 202,
    login: "octocat",
    type: "User",
    html_url: "https://github.com/octocat",
    url: "https://api.github.com/users/octocat",
  };
}
