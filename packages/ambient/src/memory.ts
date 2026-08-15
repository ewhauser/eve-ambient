import type {
  AmbientApplicationBackend,
  AmbientBackendBinding,
} from "./application.js";
import type { AttentionCallbacks } from "./attention.js";
import { MemoryAttentionEngine } from "./memory-engine.js";
import type { MemoryAttentionEngineOptions } from "./memory-engine.js";

export interface MemoryAmbientBinding extends AmbientBackendBinding {
  readonly engine: MemoryAttentionEngine;
}

/** Binds an application definition to the deterministic in-memory engine. */
export function memory(
  options: Omit<MemoryAttentionEngineOptions, "callbacks"> = {},
): AmbientApplicationBackend<MemoryAmbientBinding> {
  return Object.freeze({
    ...(options.clock === undefined ? {} : { clock: options.clock }),
    bind(callbacks: AttentionCallbacks) {
      return Object.freeze({
        engine: new MemoryAttentionEngine({ ...options, callbacks }),
      });
    },
  });
}

export {
  MemoryAttentionEngine,
  type MemoryAttentionDiagnostics,
  type MemoryAttentionEngineFaults,
  type MemoryAttentionEngineOptions,
  type MemoryAttentionRunResult,
} from "./memory-engine.js";
