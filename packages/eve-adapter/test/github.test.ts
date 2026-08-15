import type {
  AmbientPublishReceipt,
  AmbientPublisher,
} from "@ewhauser/eve-ambient/protocol";
import type { ChannelFrom, RouteHandlerArgs } from "eve/channels";
import type { GitHubChannel, GitHubChannelState } from "eve/channels/github";
import { describe, expect, it } from "vitest";
import { createEveGitHubAmbientChannel } from "../src/index.js";

describe("Eve GitHub Ambient ingress", () => {
  it("does not return Eve's acknowledgement until Ambient accepts the delivery", async () => {
    let accept: ((receipt: AmbientPublishReceipt) => void) | undefined;
    let markStarted: (() => void) | undefined;
    const accepted = new Promise<AmbientPublishReceipt>((resolve) => {
      accept = resolve;
    });
    const started = new Promise<void>((resolve) => {
      markStarted = resolve;
    });
    const publisher: AmbientPublisher = {
      async publish() {
        markStarted?.();
        return accepted;
      },
    };
    const channel = createEveGitHubAmbientChannel({
      publisher,
      tenantId: "tenant-1",
      credentials: { webhookVerifier: () => true },
    });
    let settled = false;
    const response = deliver(channel, "delivery-1").then((value) => {
      settled = true;
      return value;
    });

    await started;
    expect(settled).toBe(false);
    accept?.({} as AmbientPublishReceipt);
    await expect(response).resolves.toMatchObject({ status: 200 });
  });

  it("turns a swallowed Eve hook failure into a retryable response", async () => {
    const publisher: AmbientPublisher = {
      async publish() {
        throw new Error("durable backend unavailable");
      },
    };
    const channel = createEveGitHubAmbientChannel({
      publisher,
      tenantId: "tenant-1",
      credentials: { webhookVerifier: () => true },
    });

    await expect(deliver(channel, "delivery-2")).resolves.toMatchObject({ status: 503 });
  });
});

async function deliver(channel: GitHubChannel, deliveryId: string): Promise<Response> {
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
        "x-github-event": "check_suite",
      },
      body: JSON.stringify({
        action: "completed",
        installation: { id: 17 },
        repository: {
          id: 101,
          full_name: "ewhauser/eve-ambient",
          name: "eve-ambient",
          owner: { login: "ewhauser" },
          private: false,
        },
        sender: { id: 202, login: "octocat", type: "User" },
        check_suite: {
          id: 501,
          app: { slug: "github-actions" },
          conclusion: "failure",
          head_sha: "abc123",
          pull_requests: [{ number: 20 }],
          status: "completed",
          updated_at: "2026-01-01T00:01:00Z",
        },
      }),
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
