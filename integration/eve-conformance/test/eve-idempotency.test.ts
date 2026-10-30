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

async function loadInputQueue(): Promise<{
  SessionInputQueue: new (admittedKeys: readonly string[]) => {
    readonly admittedDeliveryKeys: string[];
    enqueueDelivery(input: { readonly idempotencyKey?: string }): unknown;
  };
}> {
  return (await import(
    pathToFileURL(resolve(eveRoot, "dist/src/execution/session/input-queue.js")).href
  )) as never;
}

describe("native Eve channel idempotency", () => {
  it("uses the exact stable package and public idempotency option", () => {
    const manifest = JSON.parse(readFileSync(resolve(eveRoot, "package.json"), "utf8"));
    expect(manifest.version).toBe("0.90.0");

    const options: ChannelSendOptions = {
      auth: null,
      idempotencyKey: "eve:wake:v1:conformance",
      turnPolicy: "queue",
    };
    expect(options.idempotencyKey).toBe("eve:wake:v1:conformance");
  });

  it("passes a follow-up key to the durable continuation command", async () => {
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
          idempotencyKey: "eve:wake:v1:wake-1",
        },
        continuationToken: "ambient:incident-42",
      },
    ]);
  });

  it("passes the key through first-session workflow creation", async () => {
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

  it("deduplicates across reconstructed owners, scopes keys, and bounds retention", async () => {
    const { SessionInputQueue } = await loadInputQueue();
    const firstOwner = new SessionInputQueue([]);
    expect(firstOwner.enqueueDelivery({ idempotencyKey: "wake-1" })).toBeDefined();
    expect(firstOwner.enqueueDelivery({ idempotencyKey: "wake-1" })).toBeUndefined();
    expect(firstOwner.enqueueDelivery({})).toBeDefined();
    expect(firstOwner.enqueueDelivery({})).toBeDefined();

    const successor = new SessionInputQueue(firstOwner.admittedDeliveryKeys);
    expect(successor.enqueueDelivery({ idempotencyKey: "wake-1" })).toBeUndefined();
    expect(new SessionInputQueue([]).enqueueDelivery({ idempotencyKey: "wake-1" })).toBeDefined();

    for (let i = 0; i < 1024; i++) {
      expect(successor.enqueueDelivery({ idempotencyKey: `later-${i}` })).toBeDefined();
    }
    expect(successor.admittedDeliveryKeys).toHaveLength(1024);
    expect(successor.enqueueDelivery({ idempotencyKey: "later-0" })).toBeUndefined();
    expect(successor.enqueueDelivery({ idempotencyKey: "wake-1" })).toBeDefined();
    expect(successor.admittedDeliveryKeys).toHaveLength(1024);
  });

  it("seeds and carries the admitted ledger through owner handoffs", () => {
    const entry = readFileSync(resolve(eveRoot, "dist/src/execution/session/entry.js"), "utf8");
    const program = readFileSync(resolve(eveRoot, "dist/src/execution/session/program.js"), "utf8");
    expect(entry).toContain("admittedDeliveryKeys:e.idempotencyKey");
    expect(entry).toContain("admittedDeliveryKeys:i.admittedDeliveryKeys");
    expect(program).toContain("admittedDeliveryKeys:o.admittedDeliveryKeys");
  });
});
