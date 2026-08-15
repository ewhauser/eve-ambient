import { createServer, type IncomingHttpHeaders, type Server } from "node:http";
import type { AddressInfo } from "node:net";

export interface LocalFetchServer {
  readonly url: string;
  close(): Promise<void>;
}

/** Exposes a Fetch-style handler on an ephemeral loopback HTTP server. */
export async function startFetchServer(
  handler: (request: Request) => Response | Promise<Response>,
): Promise<LocalFetchServer> {
  const server = createServer(async (incoming, outgoing) => {
    try {
      const origin = localOrigin(server);
      const requestBody = await readRequestBody(incoming);
      const request = new Request(new URL(incoming.url ?? "/", origin), {
        method: incoming.method ?? "GET",
        headers: requestHeaders(incoming.headers),
        ...(requestBody.byteLength === 0 ? {} : { body: requestBody }),
      });
      const response = await handler(request);
      const body = new Uint8Array(await response.arrayBuffer());
      outgoing.writeHead(response.status, Object.fromEntries(response.headers));
      outgoing.end(body);
    } catch (error) {
      outgoing.writeHead(500, { "content-type": "text/plain; charset=utf-8" });
      outgoing.end(error instanceof Error ? error.stack ?? error.message : String(error));
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  return {
    url: localOrigin(server),
    async close() {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error === undefined ? resolve() : reject(error)));
      });
    },
  };
}

function localOrigin(server: Server): string {
  const address = server.address() as AddressInfo | null;
  if (address === null || typeof address === "string") {
    throw new Error("local HTTP server is not listening on a TCP port");
  }
  return `http://127.0.0.1:${address.port}`;
}

function requestHeaders(input: IncomingHttpHeaders): Headers {
  const headers = new Headers();
  for (const [name, value] of Object.entries(input)) {
    if (value === undefined) continue;
    if (Array.isArray(value)) {
      for (const item of value) headers.append(name, item);
    } else {
      headers.set(name, value);
    }
  }
  return headers;
}

async function readRequestBody(
  request: AsyncIterable<Uint8Array>,
): Promise<Uint8Array<ArrayBuffer>> {
  const chunks: Uint8Array[] = [];
  for await (const chunk of request) chunks.push(chunk);
  const length = chunks.reduce((total, chunk) => total + chunk.byteLength, 0);
  const body = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}
