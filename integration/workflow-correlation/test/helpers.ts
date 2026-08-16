import {
  canonicalizeChannelDelivery,
  defineChannelCanonicalization,
} from "@ewhauser/eve-ambient/idempotency";
import {
  compileAcceptedFanout,
  type AcceptedFanout,
  type AttentionBranchPlan,
} from "@ewhauser/eve-ambient/protocol";
import { createServer, type Server } from "node:http";

export const SECRET_ENV = "AMBIENT_WORKFLOW_INTEGRATION_SECRET";
const servers: Server[] = [];

export async function serve(handler: (request: Request) => Promise<Response>): Promise<string> {
  const server = createServer(async (incoming, outgoing) => {
    const chunks: Buffer[] = [];
    for await (const chunk of incoming) chunks.push(Buffer.from(chunk));
    const address = server.address();
    if (address === null || typeof address === "string") throw new Error("server has no port");
    const request = new Request(`http://127.0.0.1:${address.port}${incoming.url ?? "/"}`, {
      method: incoming.method ?? "GET",
      headers: incoming.headers as HeadersInit,
      ...(chunks.length === 0 ? {} : { body: Buffer.concat(chunks) }),
    });
    const response = await handler(request);
    outgoing.statusCode = response.status;
    response.headers.forEach((value, name) => outgoing.setHeader(name, value));
    outgoing.end(Buffer.from(await response.arrayBuffer()));
  });
  servers.push(server);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("server has no port");
  return `http://127.0.0.1:${address.port}`;
}

export async function closeServers(): Promise<void> {
  await Promise.all(servers.splice(0).map((server) => new Promise<void>((resolve, reject) => {
    server.close((error) => error === undefined ? resolve() : reject(error));
    server.closeAllConnections();
  })));
}

export async function fanout(
  eventId: string,
  body: string,
  policy: AttentionBranchPlan["policy"],
  overrides: Partial<AttentionBranchPlan> = {},
): Promise<AcceptedFanout> {
  const seconds = Number.parseInt(body, 10);
  const occurredAt = new Date(Date.UTC(2026, 7, 16, 0, 0, Number.isNaN(seconds) ? 0 : seconds))
    .toISOString();
  const source = await canonicalizeChannelDelivery(
    defineChannelCanonicalization({
      version: 1,
      canonicalize: (raw: { readonly eventId: string; readonly body: string }) => ({
        id: raw.eventId,
        type: "channel.message",
        version: 1,
        occurredAt,
        data: { body: raw.body },
        source: {
          channelId: "slack",
          installationId: "workspace-1",
          tenantId: "tenant-1",
        },
        origin: { kind: "external" as const, depth: 0 },
      }),
      partitionKey: () => "incident-42",
    }),
    { eventId, body },
    { applicationId: "workflow-correlation-integration" },
  );
  return compileAcceptedFanout({
    source,
    branches: [{
      monitorId: "monitor",
      definitionVersion: "1",
      correlationKey: "incident-42",
      orderKey: body,
      mode: "active",
      policy,
      ...overrides,
    }],
  });
}

export function uniqueNamespace(label: string): string {
  return `${label}-${crypto.randomUUID()}`;
}
