import { afterEach, describe, expect, it, vi } from "vitest";
import type { FrozenAttentionBatch } from "../src/attention.js";
import { invokePrepare } from "../src/workflows/callback-steps.js";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("Workflow callback steps", () => {
  it("omits bearer authorization when transport authentication is authoritative", async () => {
    const fetch = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({
      ok: true,
      completedAt: "2026-01-01T00:00:00.000Z",
      value: { kind: "ignore", decision: null },
    }), {
      status: 200,
      headers: { "content-type": "application/json" },
    }));

    await expect(invokePrepare(
      "http://application.internal",
      "/ambient/prepare",
      null,
      {} as FrozenAttentionBatch,
    )).resolves.toMatchObject({ ok: true });

    const headers = new Headers(fetch.mock.calls[0]?.[1]?.headers);
    expect(headers.get("content-type")).toBe("application/json");
    expect(headers.has("authorization")).toBe(false);
  });
});
