import { CelldAttentionEngine, createAttentionCallbackFetchHandler } from "../src/celld.js";
import { defineAttentionEngineConformance } from "./attention-conformance.js";
import { FakeCelldFleet } from "./celld-harness.js";

defineAttentionEngineConformance("celld", (options) => {
  const secret = "test-secret";
  const callbacks = createAttentionCallbackFetchHandler(options.callbacks, { secret });
  const limits: Record<string, number> = {};
  if (options.dedupeMs !== undefined) limits.ATTENTION_DEDUPE_MS = options.dedupeMs;
  if (options.retryDelayMs !== undefined) {
    limits.ATTENTION_RETRY_DELAY_MS = options.retryDelayMs;
  }
  if (options.claimLeaseMs !== undefined) {
    limits.ATTENTION_CLAIM_LEASE_MS = options.claimLeaseMs;
  }
  if (options.maxAttempts !== undefined) limits.ATTENTION_MAX_ATTEMPTS = options.maxAttempts;
  if (options.maxBranches !== undefined) limits.ATTENTION_MAX_BRANCHES = options.maxBranches;
  if (options.maxFanoutBytes !== undefined) {
    limits.ATTENTION_MAX_FANOUT_BYTES = options.maxFanoutBytes;
  }
  if (options.maxPreparedWakeBytes !== undefined) {
    limits.ATTENTION_MAX_PREPARED_WAKE_BYTES = options.maxPreparedWakeBytes;
  }
  const fleet = new FakeCelldFleet({
    secret,
    clock: options.clock,
    callbacks,
    limits,
    ...(options.faults === undefined ? {} : { faults: options.faults }),
  });
  const engine = new CelldAttentionEngine({
    url: fleet.baseUrl,
    secret,
    fetch: fleet.fetch as typeof fetch,
  });
  return {
    engine,
    async runDue() {
      const start = fleet.outcomes.length;
      await fleet.fireDueAlarms();
      const outcomes = fleet.outcomes.slice(start);
      return {
        claimed: outcomes.length,
        ignored: outcomes.filter((outcome) => outcome === "ignored").length,
        shadowed: outcomes.filter((outcome) => outcome === "shadowed").length,
        delivered: outcomes.filter((outcome) => outcome === "delivered").length,
        failed: outcomes.filter((outcome) => outcome === "failed").length,
        terminalFailures: outcomes.filter((outcome) => outcome === "terminal-failure").length,
      };
    },
    diagnostics: () => fleet.diagnostics(),
  };
});
