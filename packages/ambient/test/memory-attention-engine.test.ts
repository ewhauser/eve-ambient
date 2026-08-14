import { MemoryAttentionEngine } from "../src/memory.js";
import { defineAttentionEngineConformance } from "./attention-conformance.js";

defineAttentionEngineConformance("memory", (options) => {
  const engine = new MemoryAttentionEngine(options);
  return {
    engine,
    runDue: () => engine.runDue(),
    diagnostics: () => engine.diagnostics(),
  };
});
