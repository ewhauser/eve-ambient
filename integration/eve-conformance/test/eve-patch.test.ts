import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import type { ChannelSendOptions } from "eve/channels";
import { describe, expect, it } from "vitest";

const require = createRequire(import.meta.url);
const eveRoot = dirname(require.resolve("eve/package.json"));

async function loadChannelAddress(): Promise<{
  createChannelAddress(input: Record<string, unknown>): {
    send(message: string, options: ChannelSendOptions): Promise<{ id: string }>;
  };
}> {
  return (await import(
    pathToFileURL(resolve(eveRoot, "dist/src/channel/channel-address.js")).href
  )) as never;
}

describe("carried eve patch", () => {
  it("pins the exact package and exposes the public idempotency option", () => {
    const manifest = JSON.parse(
      readFileSync(resolve(eveRoot, "package.json"), "utf8"),
    );
    expect(manifest.version).toBe("0.38.1");

    const options: ChannelSendOptions = {
      auth: null,
      idempotencyKey: "eve:wake:v1:conformance",
      turnPolicy: "queue",
    };
    expect(options.idempotencyKey).toBe("eve:wake:v1:conformance");
  });

  it("maps retries for an owned address into Eve's durable delivery ledger", async () => {
    const commands: unknown[] = [];
    const { createChannelAddress } = await loadChannelAddress();
    const address = createChannelAddress({
      adapter: {},
      channelName: "ambient",
      continuationToken: "incident-42",
      runtime: {
        async dispatchContinuation(input: unknown) {
          commands.push(input);
          return { sessionId: "session-1", status: "accepted" };
        },
      },
    });

    await address.send("complete payload", {
      auth: null,
      idempotencyKey: "eve:wake:v1:wake-1",
      turnPolicy: "queue",
    });

    expect(commands).toMatchObject([
      {
        command: {
          kind: "send",
          taskDeliveryId: "eve:wake:v1:wake-1",
        },
        continuationToken: "ambient:incident-42",
      },
    ]);
  });

  it("carries the key into first-session workflow input", async () => {
    const runs: unknown[] = [];
    const { createChannelAddress } = await loadChannelAddress();
    const address = createChannelAddress({
      adapter: {},
      channelName: "ambient",
      continuationToken: "incident-42",
      runtime: {
        async createSession(input: unknown) {
          runs.push(input);
          return { sessionId: "session-1" };
        },
        async dispatchContinuation() {
          return { status: "session_not_active" };
        },
      },
    });

    await address.send("complete payload", {
      auth: null,
      idempotencyKey: "eve:wake:v1:wake-1",
      turnPolicy: "queue",
    });

    expect(runs).toMatchObject([
      {
        idempotencyKey: "eve:wake:v1:wake-1",
        input: { message: "complete payload" },
      },
    ]);
  });

  it("seeds the durable workflow's delivery ledger before its first turn", () => {
    const workflow = readFileSync(
      resolve(eveRoot, "dist/src/execution/workflow-entry.js"),
      "utf8",
    );
    expect(workflow).toContain("taskDeliveryId:e.idempotencyKey");
    expect(workflow).toMatch(
      /initialInput\.kind===`deliver`&&.*seenTaskDeliveries|\.kind===`deliver`&&.*\.add\(/,
    );
  });
});
