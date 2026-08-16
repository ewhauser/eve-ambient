import { afterEach, describe, expect, it, vi } from "vitest";
import { createWorkflowAttentionCallbackHandler } from "../src/workflow.js";

const SECRET_ENV = "AMBIENT_WORKFLOW_UNIT_SECRET";

afterEach(() => {
  delete process.env[SECRET_ENV];
});

describe("Workflow callback handler", () => {
  it("rejects oversized callback values before application code runs", async () => {
    process.env[SECRET_ENV] = "test-secret";
    const prepare = vi.fn(async () => ({ kind: "ignore" as const, decision: null }));
    const deliver = vi.fn();
    const handler = createWorkflowAttentionCallbackHandler(
      { prepare, deliver },
      { secretEnv: SECRET_ENV, maxRequestBytes: 8 },
    );

    const response = await handler(
      new Request("https://application.example.test/ambient/prepare", {
        method: "POST",
        headers: {
          authorization: "Bearer test-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify({ value: "too large" }),
      }),
    );

    expect(response.status).toBe(413);
    await expect(response.json()).resolves.toMatchObject({ ok: false, terminal: true });
    expect(prepare).not.toHaveBeenCalled();
    expect(deliver).not.toHaveBeenCalled();
  });

  it("authenticates before reading a callback body", async () => {
    process.env[SECRET_ENV] = "test-secret";
    const handler = createWorkflowAttentionCallbackHandler({
      async prepare() {
        return { kind: "ignore", decision: null };
      },
      async deliver() {
        throw new Error("not called");
      },
    }, { secretEnv: SECRET_ENV });

    const response = await handler(
      new Request("https://application.example.test/ambient/prepare", {
        method: "POST",
        body: "not-json",
      }),
    );

    expect(response.status).toBe(401);
  });

  it("rejects callback URLs that cannot be composed safely", () => {
    expect(() => createWorkflowAttentionCallbackHandler({
      async prepare() {
        return { kind: "ignore", decision: null };
      },
      async deliver() {
        throw new Error("not called");
      },
    }, { preparePath: "/ambient/callback", deliverPath: "/ambient/callback" }))
      .toThrow("preparePath and deliverPath must be different");
  });
});
