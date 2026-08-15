import { memory } from "@ewhauser/eve-ambient/memory";
import { VirtualMonitorClock } from "@ewhauser/eve-ambient/testing";
import type {
  ChannelFrom,
  ChannelSendOptions,
  RouteHandlerArgs,
} from "eve/channels";
import type { GitHubChannel, GitHubChannelState } from "eve/channels/github";
import { expect, it } from "vitest";
import { defineEngineeringApplication } from "../src/application.js";
import { createEngineeringGitHubChannel } from "../src/channels/github.js";
import { createEveCelldApplication } from "../src/runtime.js";

it("coalesces a retried Eve check-suite webhook into one idempotent bot turn", async () => {
  const { ambient, clock, deliveries, github } = testApplication();
  const payload = checkSuitePayload({ conclusion: "failure", updatedAt: "2026-01-01T00:01:00Z" });

  await expect(deliver(github, "check_suite", "delivery-failed", payload)).resolves.toMatchObject({
    status: 200,
  });
  await expect(deliver(github, "check_suite", "delivery-failed", payload)).resolves.toMatchObject({
    status: 200,
  });
  clock.advance(120_000);
  await ambient.engine.runDue();

  expect(deliveries).toHaveLength(1);
  expect(deliveries[0]?.address).toBe("repo:101:pull:20");
  expect(deliveries[0]?.options.idempotencyKey).toMatch(/^eve:wake:v2:/);
  expect(deliveries[0]?.options.state).toMatchObject({
    conversationKind: "pull_request",
    installationId: 17,
    owner: "ewhauser",
    repo: "eve-ambient",
    repositoryId: 101,
    pullRequestNumber: 20,
  });
  const message = JSON.parse(deliveries[0]?.message ?? "");
  expect(message.task.instruction).toContain("Fix the smallest safe CI blocker");
  expect(message.evidence.value).toMatchObject({
    repository: "ewhauser/eve-ambient",
    pullRequestNumber: 20,
    failures: [{ app: "github-actions", checkSuiteId: 501, conclusion: "failure" }],
  });
});

it("does not let a successful suite hide another provider's failure", async () => {
  const { ambient, clock, deliveries, github } = testApplication();

  await deliver(
    github,
    "check_suite",
    "delivery-circle-failed",
    checkSuitePayload({
      appSlug: "circleci",
      conclusion: "failure",
      updatedAt: "2026-01-01T00:01:00Z",
    }),
  );
  await deliver(
    github,
    "check_suite",
    "delivery-actions-passed",
    checkSuitePayload({
      appSlug: "github-actions",
      checkSuiteId: 502,
      conclusion: "success",
      updatedAt: "2026-01-01T00:02:00Z",
    }),
  );
  clock.advance(120_000);
  await ambient.engine.runDue();

  expect(deliveries).toHaveLength(1);
  const evidence = JSON.parse(deliveries[0]?.message ?? "").evidence.value;
  expect(evidence.failures).toEqual([
    { app: "circleci", checkSuiteId: 501, conclusion: "failure", headSha: "abc123" },
  ]);
});

it("lets later built-in success or closure suppress stale CI failure", async () => {
  const { ambient, clock, deliveries, github } = testApplication();

  await deliver(
    github,
    "check_suite",
    "delivery-failed",
    checkSuitePayload({ conclusion: "failure", updatedAt: "2026-01-01T00:01:00Z" }),
  );
  await deliver(
    github,
    "check_suite",
    "delivery-passed",
    checkSuitePayload({
      checkSuiteId: 502,
      conclusion: "success",
      updatedAt: "2026-01-01T00:02:00Z",
    }),
  );
  clock.advance(120_000);
  await expect(ambient.engine.runDue()).resolves.toMatchObject({ ignored: 1 });
  expect(deliveries).toHaveLength(0);

  await deliver(
    github,
    "check_suite",
    "delivery-failed-again",
    checkSuitePayload({
      checkSuiteId: 503,
      conclusion: "failure",
      updatedAt: "2026-01-01T00:03:00Z",
    }),
  );
  await deliver(
    github,
    "pull_request",
    "delivery-closed",
    pullRequestPayload({ action: "closed", updatedAt: "2026-01-01T00:04:00Z" }),
  );
  clock.advance(120_000);
  await expect(ambient.engine.runDue()).resolves.toMatchObject({ ignored: 1 });
  expect(deliveries).toHaveLength(0);
});

it("binds Eve GitHub admission and celld callbacks from one runtime configuration", async () => {
  const application = createEveCelldApplication({
    applicationId: "engineering-agent",
    celld: {
      url: "https://celld.example.test",
      secret: "test-secret",
      async fetch(_input, init) {
        const fanout = JSON.parse(String(init?.body));
        return Response.json({
          eventKey: fanout.eventKey,
          occurrenceKey: fanout.occurrenceKey,
          inputHash: fanout.inputHash,
          manifestHash: fanout.manifestHash,
          branchKeys: fanout.branches.map(
            (branch: { readonly branchKey: string }) => branch.branchKey,
          ),
          acceptedAt: "2026-01-01T00:00:00.000Z",
          dedupeExpiresAt: "2026-01-08T00:00:00.000Z",
        });
      },
    },
    eve: {
      auth: null,
      from: (() => ({ send: async () => ({ id: "unused" }) })) as unknown as ChannelFrom<GitHubChannelState>,
    },
  });
  const github = createEngineeringGitHubChannel({
    publisher: application,
    tenantId: (context) => context.repository.owner,
    credentials: { webhookVerifier: () => true },
  });

  await expect(
    deliver(
      github,
      "check_suite",
      "delivery-runtime",
      checkSuitePayload({ conclusion: "failure", updatedAt: "2026-01-01T00:01:00Z" }),
    ),
  ).resolves.toMatchObject({ status: 200 });

  const response = await application.fetch(
    new Request("https://app.example.test/ambient/unknown", {
      method: "POST",
      headers: { authorization: "Bearer test-secret" },
      body: "{}",
    }),
  );
  expect(response.status).toBe(404);
});

function testApplication() {
  const deliveries: Array<{
    readonly address: string;
    readonly message: string;
    readonly options: ChannelSendOptions<GitHubChannelState>;
  }> = [];
  const from = ((address: string) => ({
    async send(message: string, options: ChannelSendOptions<GitHubChannelState>) {
      deliveries.push({ address, message, options });
      return { id: "session-1" };
    },
  })) as unknown as ChannelFrom<GitHubChannelState>;
  const clock = new VirtualMonitorClock();
  const ambient = defineEngineeringApplication({
    applicationId: "engineering-agent",
    eve: { auth: null, from },
  }).with(memory({ clock }));
  const github = createEngineeringGitHubChannel({
    publisher: ambient,
    tenantId: (context) => context.repository.owner,
    credentials: { webhookVerifier: () => true },
  });
  return { ambient, clock, deliveries, github };
}

async function deliver(
  channel: GitHubChannel,
  event: "check_suite" | "pull_request",
  deliveryId: string,
  payload: object,
): Promise<Response> {
  const route = channel.routes.find(
    (candidate) => candidate.transport !== "websocket" && candidate.method === "POST",
  );
  if (route === undefined || route.transport === "websocket") {
    throw new Error("Eve GitHub POST route is missing");
  }
  return route.handler(
    new Request("https://app.example.test/eve/v1/github", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-github-delivery": deliveryId,
        "x-github-event": event,
      },
      body: JSON.stringify(payload),
    }),
    routeArgs(),
  );
}

function routeArgs(): RouteHandlerArgs<GitHubChannelState> {
  return {
    from: (() => {
      throw new Error("Ambient GitHub hooks must not dispatch directly");
    }) as unknown as ChannelFrom<GitHubChannelState>,
    resolveSession: async () => undefined,
    attachSession() {
      throw new Error("not used");
    },
    to() {
      throw new Error("not used");
    },
    params: {},
    waitUntil() {
      throw new Error("durable admission must be awaited by the wrapped route");
    },
    requestIp: null,
  } as unknown as RouteHandlerArgs<GitHubChannelState>;
}

function checkSuitePayload(options: {
  readonly appSlug?: string;
  readonly checkSuiteId?: number;
  readonly conclusion: string;
  readonly updatedAt: string;
}) {
  return {
    action: "completed",
    installation: { id: 17 },
    repository: repository(),
    sender: sender(),
    check_suite: {
      id: options.checkSuiteId ?? 501,
      app: { slug: options.appSlug ?? "github-actions" },
      conclusion: options.conclusion,
      head_sha: "abc123",
      pull_requests: [{ number: 20 }],
      status: "completed",
      updated_at: options.updatedAt,
    },
  };
}

function pullRequestPayload(options: { readonly action: string; readonly updatedAt: string }) {
  return {
    action: options.action,
    installation: { id: 17 },
    repository: repository(),
    sender: sender(),
    pull_request: {
      number: 20,
      head: { ref: "feature", sha: "abc123" },
      base: { ref: "main", sha: "base123", repo: { default_branch: "main" } },
      updated_at: options.updatedAt,
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
