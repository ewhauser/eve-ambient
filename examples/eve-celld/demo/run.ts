import assert from "node:assert/strict";
import { pathToFileURL } from "node:url";

import { VirtualMonitorClock } from "@ewhauser/eve-ambient/testing";
import type {
  ChannelFrom,
  ChannelSendOptions,
  RouteHandlerArgs,
} from "eve/channels";
import type { GitHubChannel, GitHubChannelState } from "eve/channels/github";

import { createEngineeringGitHubChannel } from "../src/channels/github.js";
import { createEveCelldApplication } from "../src/runtime.js";
import {
  checkSuiteEvent,
  pullRequestEvent,
  type SyntheticGitHubEvent,
} from "./fixtures.js";
import { startFetchServer, type LocalFetchServer } from "./http.js";
import { LocalCelldFleet } from "./local-celld.js";

const SECRET = "local-eve-ambient-demo";

type DemoLog = (message: string) => void;

export interface DemoDelivery {
  readonly address: string;
  readonly idempotencyKey: string;
  readonly message: Record<string, unknown>;
  readonly state: GitHubChannelState;
}

export interface DemoResult {
  readonly acceptedWebhooks: number;
  readonly celldCells: number;
  readonly deliveries: readonly DemoDelivery[];
  readonly outcomes: readonly string[];
  readonly payloadBearingCells: number;
}

export async function runDemo(
  options: { readonly log?: DemoLog | undefined } = {},
): Promise<DemoResult> {
  const log = options.log ?? console.log;
  const clock = new VirtualMonitorClock();
  const deliveries: DemoDelivery[] = [];
  const fleet = new LocalCelldFleet({ clock, log, secret: SECRET });
  let appServer: LocalFetchServer | undefined;

  log("Eve Ambient GitHub shepherd demo");
  log("--------------------------------");
  await fleet.start();
  log(`[boot] celld worker ${fleet.url}`);

  try {
    const from = consoleEveChannel(deliveries, log);
    const application = createEveCelldApplication({
      applicationId: "engineering-agent",
      celld: { url: fleet.url, secret: SECRET },
      eve: { auth: null, from },
    });
    const github = createEngineeringGitHubChannel({
      publisher: application,
      tenantId: (context) => context.repository.owner,
      credentials: { webhookVerifier: () => true },
    });
    appServer = await startApplicationServer(application.fetch, github);
    fleet.setCallbackBaseUrl(`${appServer.url}/ambient`);
    log(`[boot] Eve GitHub channel ${appServer.url}/eve/v1/github`);
    log("[boot] no GitHub, model, or workflow-provider credentials required\n");

    const events = scenario();
    for (const event of events) {
      const response = await sendSyntheticEvent(appServer.url, event);
      assert.equal(response.status, 200, await response.text());
      log(`[github] 200 ${event.event.padEnd(12)} ${event.deliveryId}  ${event.label}`);
    }

    log(`\n[celld] ${events.length} HTTP deliveries accepted into ${fleet.cellCount} cells`);
    log("[clock] advancing two-minute quiet period instantly");
    clock.advance(120_000);
    const alarms = await fleet.fireDueAlarms();
    const alarmFailure = alarms.find((alarm) => alarm.error !== undefined);
    if (alarmFailure?.error !== undefined) throw alarmFailure.error;

    assert.equal(fleet.cellCount, 2, "one stable partition cell must own each pull request");
    assert.equal(deliveries.length, 1, "the demo must produce exactly one Eve turn");
    assert.deepEqual(fleet.outcomes.slice().sort(), ["delivered", "ignored"]);
    const failures = deliveryFailures(deliveries[0]!);
    assert.deepEqual(failures, [
      {
        app: "circleci",
        checkSuiteId: 502,
        conclusion: "failure",
        headSha: "head-20",
      },
    ]);
    const diagnostics = await fleet.diagnostics();
    assert.equal(
      diagnostics.payloadBearingCells,
      0,
      "terminal celld workflows must delete event payloads",
    );

    log(
      `[celld] terminal storage ${diagnostics.payloadBearingCells} payload-bearing cells; ` +
      `${diagnostics.receiptOnlyCells} receipt-only cells`,
    );
    log("\n[done] 10 webhook deliveries -> 9 logical events -> 2 PR cells -> 1 Eve turn");
    log("[done] duplicate delivery was deduplicated; recovered and closed failures were suppressed");
    return {
      acceptedWebhooks: events.length,
      celldCells: fleet.cellCount,
      deliveries,
      outcomes: [...fleet.outcomes],
      payloadBearingCells: diagnostics.payloadBearingCells,
    };
  } finally {
    try {
      await appServer?.close();
    } finally {
      await fleet.close();
    }
  }
}

function scenario(): readonly SyntheticGitHubEvent[] {
  const circleFailure = checkSuiteEvent({
    appSlug: "circleci",
    checkSuiteId: 502,
    conclusion: "failure",
    deliveryId: "delivery-pr20-circle-failure",
    label: "CircleCI fails",
    pullRequestNumber: 20,
    updatedAt: "2026-01-01T00:04:00Z",
  });
  return [
    pullRequestEvent({
      action: "synchronize",
      deliveryId: "delivery-pr20-push",
      label: "PR #20 receives a new push",
      pullRequestNumber: 20,
      updatedAt: "2026-01-01T00:01:00Z",
    }),
    checkSuiteEvent({
      appSlug: "github-actions",
      checkSuiteId: 501,
      conclusion: null,
      deliveryId: "delivery-pr20-actions-requested",
      label: "GitHub Actions starts",
      pullRequestNumber: 20,
      updatedAt: "2026-01-01T00:02:00Z",
    }),
    checkSuiteEvent({
      appSlug: "github-actions",
      checkSuiteId: 501,
      conclusion: "failure",
      deliveryId: "delivery-pr20-actions-failure",
      label: "GitHub Actions fails",
      pullRequestNumber: 20,
      updatedAt: "2026-01-01T00:03:00Z",
    }),
    circleFailure,
    { ...circleFailure, label: "provider retries the identical CircleCI webhook" },
    checkSuiteEvent({
      appSlug: "github-actions",
      checkSuiteId: 503,
      conclusion: "success",
      deliveryId: "delivery-pr20-actions-success",
      label: "GitHub Actions recovers; CircleCI is still failing",
      pullRequestNumber: 20,
      updatedAt: "2026-01-01T00:05:00Z",
    }),
    pullRequestEvent({
      action: "synchronize",
      deliveryId: "delivery-pr21-push",
      label: "PR #21 receives a new push",
      pullRequestNumber: 21,
      updatedAt: "2026-01-01T00:06:00Z",
    }),
    checkSuiteEvent({
      appSlug: "github-actions",
      checkSuiteId: 504,
      conclusion: "failure",
      deliveryId: "delivery-pr21-actions-failure",
      label: "PR #21 fails",
      pullRequestNumber: 21,
      updatedAt: "2026-01-01T00:07:00Z",
    }),
    pullRequestEvent({
      action: "closed",
      deliveryId: "delivery-pr21-closed",
      label: "PR #21 closes before the quiet period",
      pullRequestNumber: 21,
      updatedAt: "2026-01-01T00:08:00Z",
    }),
    checkSuiteEvent({
      appSlug: "github-actions",
      checkSuiteId: 505,
      conclusion: "success",
      deliveryId: "delivery-pr21-late-success",
      label: "a late PR #21 success remains suppressed by closure",
      pullRequestNumber: 21,
      updatedAt: "2026-01-01T00:09:00Z",
    }),
  ];
}

function consoleEveChannel(
  deliveries: DemoDelivery[],
  log: DemoLog,
): ChannelFrom<GitHubChannelState> {
  return ((address: string) => ({
    async send(
      serialized: string,
      options: ChannelSendOptions<GitHubChannelState>,
    ) {
      if (options.idempotencyKey === undefined || options.state === undefined) {
        throw new Error("the Eve delivery must carry idempotency and GitHub state");
      }
      const message = JSON.parse(serialized) as Record<string, unknown>;
      deliveries.push({
        address,
        idempotencyKey: options.idempotencyKey,
        message,
        state: options.state,
      });
      const task = message.task as { readonly instruction?: string };
      const failures = deliveryFailures(deliveries.at(-1)!);
      log("\n[eve] durable bot turn accepted");
      log(`[eve] conversation ${address}`);
      log(`[eve] idempotency  ${options.idempotencyKey}`);
      log(`[eve] instruction  ${task.instruction ?? "<missing>"}`);
      for (const failure of failures) {
        log(
          `[eve] evidence     ${failure.app}: ${failure.conclusion} ` +
          `(suite ${failure.checkSuiteId})`,
        );
      }
      return { id: `console-session-${deliveries.length}` };
    },
  })) as unknown as ChannelFrom<GitHubChannelState>;
}

async function startApplicationServer(
  attentionCallbacks: (request: Request) => Promise<Response>,
  github: GitHubChannel,
): Promise<LocalFetchServer> {
  const route = github.routes.find(
    (candidate) => candidate.transport !== "websocket" && candidate.method === "POST",
  );
  if (route === undefined || route.transport === "websocket") {
    throw new Error("Eve GitHub POST route is missing");
  }
  return startFetchServer((request) => {
    const path = new URL(request.url).pathname;
    if (path.startsWith("/ambient/")) return attentionCallbacks(request);
    if (path === "/eve/v1/github") return route.handler(request, routeArgs());
    if (path === "/health") return Response.json({ ok: true });
    return Response.json({ error: "not found" }, { status: 404 });
  });
}

async function sendSyntheticEvent(
  appUrl: string,
  event: SyntheticGitHubEvent,
): Promise<Response> {
  return fetch(`${appUrl}/eve/v1/github`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-github-delivery": event.deliveryId,
      "x-github-event": event.event,
    },
    body: JSON.stringify(event.payload),
  });
}

function routeArgs(): RouteHandlerArgs<GitHubChannelState> {
  return {
    from: (() => {
      throw new Error("Ambient GitHub hooks must not dispatch directly");
    }) as unknown as ChannelFrom<GitHubChannelState>,
    resolveSession: async () => undefined,
    attachSession() {
      throw new Error("not used by synthetic Ambient hooks");
    },
    to() {
      throw new Error("not used by synthetic Ambient hooks");
    },
    params: {},
    waitUntil() {
      throw new Error("the adapter must replace waitUntil and await durable admission");
    },
    requestIp: "127.0.0.1",
  } as unknown as RouteHandlerArgs<GitHubChannelState>;
}

function deliveryFailures(delivery: DemoDelivery): readonly Record<string, unknown>[] {
  const evidence = delivery.message.evidence as {
    readonly value?: { readonly failures?: readonly Record<string, unknown>[] };
  };
  return evidence.value?.failures ?? [];
}

if (
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  runDemo().catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
