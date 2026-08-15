import { createWorld } from "@workflow/world-postgres";
import { describe, expect, it } from "vitest";

const connectionString = process.env.WORKFLOW_SPIKE_POSTGRES_URL;

describe.skipIf(connectionString === undefined)("Postgres World storage probe", () => {
  it("persists the same append-only run/event model in PostgreSQL", async () => {
    const world = createWorld({ connectionString: connectionString! });
    try {
      const created = await world.events.create(null, {
        eventType: "run_created",
        eventData: {
          deploymentId: "workflow-world-spike",
          workflowName: "attentionStreamWorkflow",
          input: new Uint8Array([1, 2, 3]),
        },
        specVersion: 2,
      });
      expect(created.run).toBeDefined();
      const runId = created.run!.runId;

      await world.events.create(runId, {
        eventType: "run_completed",
        eventData: { output: new Uint8Array([4, 5, 6]) },
        specVersion: 2,
      });

      const run = await world.runs.get(runId, { resolveData: "all" });
      const events = await world.events.list({
        runId,
        resolveData: "all",
        pagination: { limit: 100 },
      });
      expect(run.status).toBe("completed");
      expect(events.data.map((event) => event.eventType)).toEqual([
        "run_created",
        "run_completed",
      ]);
    } finally {
      await world.close?.();
    }
  });
});
