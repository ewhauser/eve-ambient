import {
  defineChannelCanonicalization,
  type CanonicalChannelEvent,
} from "@ewhauser/eve-ambient";
import type { AmbientPublisher } from "@ewhauser/eve-ambient/protocol";
import {
  githubChannel,
  type GitHubCheckSuiteEvent,
  type GitHubChannel,
  type GitHubChannelConfig,
  type GitHubChannelState,
  type GitHubInboundContext,
  type GitHubJsonObject,
  type GitHubPullRequestEvent,
  type GitHubRepositoryRef,
} from "eve/channels/github";

type EveGitHubRepositoryData = {
  readonly fullName: string;
  readonly id: number;
  readonly name: string;
  readonly owner: string;
  readonly private: boolean;
};

export type EveGitHubPullRequestState = {
  readonly baseRef: string | null;
  readonly baseSha: string | null;
  readonly checkoutPath: null;
  readonly conversationKind: "pull_request";
  readonly defaultBranch: string | null;
  readonly headRef: string | null;
  readonly headSha: string | null;
  readonly installationId: number;
  readonly issueNumber: number;
  readonly owner: string;
  readonly pullRequestNumber: number;
  readonly repo: string;
  readonly repositoryId: number;
  readonly reviewCommentId: null;
  readonly reviewThreadRootCommentId: null;
  readonly triggeringCommentId: null;
  readonly triggeringUserLogin: string;
};

export type EveGitHubPullRequestTarget = {
  readonly address: string;
  readonly state: EveGitHubPullRequestState;
};

export type EveGitHubPullRequestActivityEvent = CanonicalChannelEvent<
  "github.pull-request",
  {
    readonly action: string;
    readonly headSha: string | null;
    readonly pullRequestNumber: number;
    readonly raw: GitHubJsonObject;
    readonly repository: EveGitHubRepositoryData;
  },
  EveGitHubPullRequestTarget
>;

export type EveGitHubCheckSuiteActivityEvent = CanonicalChannelEvent<
  "github.check-suite",
  {
    readonly action: string;
    readonly appSlug: string | null;
    readonly checkSuiteId: number;
    readonly conclusion: string | null;
    readonly headSha: string | null;
    readonly pullRequestNumber: number;
    readonly raw: GitHubJsonObject;
    readonly repository: EveGitHubRepositoryData;
    readonly status: string | null;
  },
  EveGitHubPullRequestTarget
>;

export type EveGitHubActivityEvent =
  | EveGitHubPullRequestActivityEvent
  | EveGitHubCheckSuiteActivityEvent;

interface EveGitHubActivityInputBase {
  readonly context: GitHubInboundContext;
  readonly pullRequestNumber: number;
  readonly tenantId: string;
}

export type EveGitHubPullRequestActivityInput =
  | (EveGitHubActivityInputBase & {
      readonly event: GitHubPullRequestEvent;
      readonly kind: "pull_request";
    })
  | (EveGitHubActivityInputBase & {
      readonly event: GitHubCheckSuiteEvent;
      readonly kind: "check_suite";
    });

/**
 * Canonicalizes Eve's built-in GitHub hook values without asking applications
 * to redefine provider schemas, delivery identity, repository identity, or
 * continuation routing.
 */
export const eveGitHubPullRequestActivity = defineChannelCanonicalization<
  EveGitHubPullRequestActivityInput,
  EveGitHubActivityEvent
>({
  version: 1,
  canonicalize(input) {
    const installationId = input.context.github.installationId;
    if (installationId === undefined) {
      throw new TypeError("Eve GitHub activity requires an installation id");
    }
    const tenantId = nonEmpty(input.tenantId, "Eve GitHub tenant id");
    const occurredAt = eventTimestamp(input.event.raw);
    const common = {
      version: 1,
      ...(occurredAt === undefined ? {} : { occurredAt }),
      source: {
        channelId: "eve.github",
        installationId: String(installationId),
        tenantId,
      },
      actor: {
        id: String(input.context.sender.id),
        principalType: principalType(input.context.sender.type),
        displayName: input.context.sender.login,
        isBot: input.context.sender.type === "Bot",
      },
      replyTarget: eveGitHubPullRequestTarget(input, installationId),
      subjects: [
        { namespace: "repository", key: input.context.repository.fullName },
        {
          namespace: "pull-request",
          key: `${input.context.repository.fullName}#${input.pullRequestNumber}`,
        },
      ],
      origin: { kind: "external" as const, depth: 0 },
    };
    if (input.kind === "pull_request") {
      return {
        ...common,
        id: input.context.delivery.id,
        type: "github.pull-request",
        data: {
          action: input.event.action,
          headSha: input.event.headSha,
          pullRequestNumber: input.pullRequestNumber,
          raw: input.event.raw,
          repository: repositoryData(input.context.repository),
        },
      };
    }
    return {
      ...common,
      id: `${input.context.delivery.id}:pull:${input.pullRequestNumber}`,
      type: "github.check-suite",
      data: {
        action: input.event.action,
        appSlug: input.event.app.slug,
        checkSuiteId: input.event.checkSuiteId,
        conclusion: input.event.conclusion,
        headSha: input.event.headSha,
        pullRequestNumber: input.pullRequestNumber,
        raw: input.event.raw,
        repository: repositoryData(input.context.repository),
        status: input.event.status,
      },
    };
  },
});

export type EveGitHubAmbientChannelOptions = Omit<
  GitHubChannelConfig,
  "onCheckSuite" | "onPullRequest"
> & {
  readonly publisher: AmbientPublisher;
  readonly tenantId: string | ((context: GitHubInboundContext) => string);
};

/**
 * Uses Eve's built-in GitHub webhook verification and event normalization,
 * then waits for Ambient's durable acceptance before returning Eve's 2xx
 * response. Pull-request and check-suite hooks are owned by this adapter;
 * other GitHub hooks and normal mention dispatch remain configurable.
 */
export function createEveGitHubAmbientChannel(
  options: EveGitHubAmbientChannelOptions,
): GitHubChannel {
  if (
    options.publisher === null ||
    typeof options.publisher !== "object" ||
    typeof options.publisher.publish !== "function"
  ) {
    throw new TypeError("Eve GitHub Ambient publisher is required");
  }
  const { publisher, tenantId: tenantIdOption, ...github } = options;
  const tenantId = (context: GitHubInboundContext) =>
    typeof tenantIdOption === "function" ? tenantIdOption(context) : tenantIdOption;
  const admissions = new GitHubAdmissionTracker();
  const channel = githubChannel({
    ...github,
    async onPullRequest(context, event) {
      try {
        await publisher.publish(eveGitHubPullRequestActivity, {
          context,
          event,
          kind: "pull_request",
          pullRequestNumber: event.pullRequestNumber,
          tenantId: tenantId(context),
        });
      } catch (error) {
        admissions.fail(context.delivery.id);
        throw error;
      }
      return null;
    },
    async onCheckSuite(context, event) {
      try {
        await Promise.all(
          event.pullRequests.map((pullRequestNumber) =>
            publisher.publish(eveGitHubPullRequestActivity, {
              context,
              event,
              kind: "check_suite",
              pullRequestNumber,
              tenantId: tenantId(context),
            }),
          ),
        );
      } catch (error) {
        admissions.fail(context.delivery.id);
        throw error;
      }
      return null;
    },
  });
  return awaitDurableAdmission(channel, admissions);
}

/** Eve's channel-local continuation address for a pull-request conversation. */
export function eveGitHubPullRequestAddress(input: {
  readonly repositoryId: number;
  readonly pullRequestNumber: number;
}): string {
  positiveInteger(input.repositoryId, "Eve GitHub repository id");
  positiveInteger(input.pullRequestNumber, "Eve GitHub pull-request number");
  return `repo:${input.repositoryId}:pull:${input.pullRequestNumber}`;
}

/** Complete stateful Eve destination carried by each canonical GitHub event. */
export function eveGitHubPullRequestTarget(
  input: EveGitHubPullRequestActivityInput,
  installationId = input.context.github.installationId,
): EveGitHubPullRequestTarget {
  if (installationId === undefined) {
    throw new TypeError("Eve GitHub target requires an installation id");
  }
  const pullRequest = input.kind === "pull_request" ? input.event.raw : undefined;
  const state = {
    baseRef: nestedString(pullRequest, "base", "ref"),
    baseSha: nestedString(pullRequest, "base", "sha"),
    checkoutPath: null,
    conversationKind: "pull_request" as const,
    defaultBranch: nestedString(pullRequest, "base", "repo", "default_branch"),
    headRef: nestedString(pullRequest, "head", "ref"),
    headSha: input.event.headSha,
    installationId,
    issueNumber: input.pullRequestNumber,
    owner: input.context.repository.owner,
    pullRequestNumber: input.pullRequestNumber,
    repo: input.context.repository.name,
    repositoryId: input.context.repository.id,
    reviewCommentId: null,
    reviewThreadRootCommentId: null,
    triggeringCommentId: null,
    triggeringUserLogin: input.context.sender.login,
  } satisfies GitHubChannelState;
  return {
    address: eveGitHubPullRequestAddress({
      repositoryId: input.context.repository.id,
      pullRequestNumber: input.pullRequestNumber,
    }),
    state,
  };
}

/** Validates a prepared wake target before handing it back to Eve. */
export function parseEveGitHubPullRequestTarget(value: unknown): EveGitHubPullRequestTarget {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("Eve GitHub attention target must be an object");
  }
  const target = value as { readonly address?: unknown; readonly state?: unknown };
  if (typeof target.address !== "string" || target.address.length === 0) {
    throw new TypeError("Eve GitHub attention target address must not be empty");
  }
  if (target.state === null || typeof target.state !== "object" || Array.isArray(target.state)) {
    throw new TypeError("Eve GitHub attention target state must be an object");
  }
  const state = target.state as Partial<EveGitHubPullRequestState>;
  if (
    state.conversationKind !== "pull_request" ||
    !Number.isSafeInteger(state.repositoryId) ||
    !Number.isSafeInteger(state.pullRequestNumber) ||
    typeof state.owner !== "string" ||
    typeof state.repo !== "string"
  ) {
    throw new TypeError("Eve GitHub attention target state is invalid");
  }
  return value as EveGitHubPullRequestTarget;
}

function awaitDurableAdmission(
  channel: GitHubChannel,
  admissions: GitHubAdmissionTracker,
): GitHubChannel {
  const routes = channel.routes.map((route) => {
    if (route.transport === "websocket" || route.method !== "POST") return route;
    return Object.freeze({
      ...route,
      async handler(request: Request, args: Parameters<typeof route.handler>[1]) {
        const deliveryId = request.headers.get("x-github-delivery")?.trim();
        if (deliveryId === undefined || deliveryId.length === 0) {
          return new Response("GitHub delivery id is required", { status: 400 });
        }
        admissions.begin(deliveryId);
        const tasks: Promise<unknown>[] = [];
        try {
          const response = await route.handler(request, {
            ...args,
            waitUntil(task) {
              tasks.push(task);
            },
          });
          await Promise.all(tasks);
          return admissions.failed(deliveryId)
            ? new Response("Ambient admission unavailable", { status: 503 })
            : response;
        } finally {
          admissions.end(deliveryId);
        }
      },
    });
  });
  return Object.freeze({ ...channel, routes: Object.freeze(routes) });
}

function repositoryData(repository: GitHubRepositoryRef): EveGitHubRepositoryData {
  return {
    fullName: repository.fullName,
    id: repository.id,
    name: repository.name,
    owner: repository.owner,
    private: repository.private,
  };
}

function eventTimestamp(raw: GitHubJsonObject): string | undefined {
  for (const key of ["updated_at", "completed_at", "started_at", "created_at"]) {
    const value = raw[key];
    if (typeof value !== "string" || value.length === 0) continue;
    const milliseconds = Date.parse(value);
    if (Number.isFinite(milliseconds)) return new Date(milliseconds).toISOString();
  }
  return undefined;
}

function nestedString(
  value: GitHubJsonObject | undefined,
  ...path: readonly string[]
): string | null {
  let current: unknown = value;
  for (const key of path) {
    if (current === null || typeof current !== "object" || Array.isArray(current)) return null;
    current = (current as Readonly<Record<string, unknown>>)[key];
  }
  return typeof current === "string" ? current : null;
}

class GitHubAdmissionTracker {
  readonly #entries = new Map<string, { failed: boolean; references: number }>();

  begin(deliveryId: string): void {
    const existing = this.#entries.get(deliveryId);
    if (existing === undefined) {
      this.#entries.set(deliveryId, { failed: false, references: 1 });
      return;
    }
    existing.references += 1;
  }

  fail(deliveryId: string): void {
    const existing = this.#entries.get(deliveryId);
    if (existing === undefined) {
      this.#entries.set(deliveryId, { failed: true, references: 0 });
      return;
    }
    existing.failed = true;
  }

  failed(deliveryId: string): boolean {
    return this.#entries.get(deliveryId)?.failed === true;
  }

  end(deliveryId: string): void {
    const existing = this.#entries.get(deliveryId);
    if (existing === undefined) return;
    existing.references -= 1;
    if (existing.references <= 0) this.#entries.delete(deliveryId);
  }
}

function principalType(type: string): "app" | "service" | "unknown" | "user" {
  if (type === "User") return "user";
  if (type === "Bot") return "app";
  if (type === "Organization") return "service";
  return "unknown";
}

function nonEmpty(value: string, name: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${name} must not be empty`);
  }
  return value;
}

function positiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
}
