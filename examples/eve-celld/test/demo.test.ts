import { expect, it } from "vitest";

import { runDemo } from "../demo/run.js";

it("runs the Eve GitHub and celld console demo end to end", async () => {
  const result = await runDemo({ log: () => undefined });

  expect(result.acceptedWebhooks).toBe(10);
  expect(result.celldCells).toBe(2);
  expect(result.outcomes.slice().sort()).toEqual(["delivered", "ignored"]);
  expect(result.payloadBearingCells).toBe(0);
  expect(result.deliveries).toHaveLength(1);
  expect(result.deliveries[0]).toMatchObject({
    address: "repo:101:pull:20",
    idempotencyKey: expect.stringMatching(/^eve:wake:v2:/),
    state: {
      conversationKind: "pull_request",
      installationId: 17,
      owner: "ewhauser",
      pullRequestNumber: 20,
      repo: "eve-ambient",
      repositoryId: 101,
    },
  });
});
