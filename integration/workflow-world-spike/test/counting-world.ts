import { AsyncLocalStorage } from "node:async_hooks";
import { getWorld } from "workflow/runtime";

type WorkflowWorld = ReturnType<typeof getWorld>;

export interface WorldCallSnapshot {
  readonly total: number;
  readonly categories: Readonly<Record<WorldCallCategory, number>>;
  readonly operations: Readonly<Record<string, number>>;
  readonly eventTypes: Readonly<Record<string, number>>;
  readonly queues: Readonly<Record<string, number>>;
  readonly scopes: Readonly<Record<string, WorldCallScopeSnapshot>>;
  readonly trace: readonly string[];
}

export interface WorldCallScopeSnapshot {
  readonly total: number;
  readonly categories: Readonly<Record<WorldCallCategory, number>>;
  readonly operations: Readonly<Record<string, number>>;
}

export type WorldCallCategory =
  | "storageRead"
  | "storageWrite"
  | "queuePublish"
  | "worldControl";

export interface CountingWorld {
  readonly world: WorkflowWorld;
  reset(): void;
  snapshot(): WorldCallSnapshot;
}

/**
 * Counts calls at the public World boundary. Each entry is a potential remote
 * RPC/storage operation even though the Vitest World executes it locally.
 */
export function createCountingWorld(target: WorkflowWorld): CountingWorld {
  let operations = new Map<string, number>();
  let eventTypes = new Map<string, number>();
  let queues = new Map<string, number>();
  let scopes = new Map<string, Map<string, number>>();
  let trace: string[] = [];
  const proxies = new WeakMap<object, object>();
  const activeScope = new AsyncLocalStorage<string>();

  const increment = (counts: Map<string, number>, key: string): void => {
    counts.set(key, (counts.get(key) ?? 0) + 1);
  };

  const record = (operation: string, args: readonly unknown[]): void => {
    increment(operations, operation);
    const scope = activeScope.getStore() ?? "client";
    const scopeCounts = scopes.get(scope) ?? new Map<string, number>();
    scopes.set(scope, scopeCounts);
    increment(scopeCounts, operation);
    let detail = operation;
    if (operation === "events.create") {
      const request = args[1];
      if (isRecord(request) && typeof request.eventType === "string") {
        increment(eventTypes, request.eventType);
        detail += `(${request.eventType})`;
      }
    } else if (operation === "queue" && typeof args[0] === "string") {
      const queue = normalizeQueue(args[0]);
      increment(queues, queue);
      detail += `(${queue})`;
    }
    trace.push(`${scope}: ${detail}`);
  };

  const wrap = <T extends object>(value: T, prefix = ""): T => {
    const existing = proxies.get(value);
    if (existing !== undefined) return existing as T;
    const proxy = new Proxy(value, {
      get(object, property) {
        const member = Reflect.get(object, property, object) as unknown;
        if (typeof property !== "string") return member;
        const operation = prefix.length === 0 ? property : `${prefix}.${property}`;
        if (typeof member === "function") {
          return (...args: unknown[]) => {
            if (operation === "createQueueHandler") {
              return Reflect.apply(member, object, wrapQueueHandlerArgs(args, activeScope)) as unknown;
            }
            record(operation, args);
            return Reflect.apply(member, object, args) as unknown;
          };
        }
        if (member !== null && typeof member === "object") return wrap(member, operation);
        return member;
      },
    });
    proxies.set(value, proxy);
    return proxy;
  };

  return {
    world: wrap(target),
    reset() {
      operations = new Map();
      eventTypes = new Map();
      queues = new Map();
      scopes = new Map();
      trace = [];
    },
    snapshot() {
      const operationRecord = sortedRecord(operations);
      return {
        total: Object.values(operationRecord).reduce((sum, calls) => sum + calls, 0),
        categories: categorize(operationRecord),
        operations: operationRecord,
        eventTypes: sortedRecord(eventTypes),
        queues: sortedRecord(queues),
        scopes: Object.fromEntries(
          [...scopes]
            .sort(([left], [right]) => left.localeCompare(right))
            .map(([scope, counts]) => {
              const scopeOperations = sortedRecord(counts);
              return [
                scope,
                {
                  total: Object.values(scopeOperations).reduce((sum, calls) => sum + calls, 0),
                  categories: categorize(scopeOperations),
                  operations: scopeOperations,
                },
              ];
            }),
        ),
        trace: [...trace],
      };
    },
  };
}

export async function waitForStableWorldCalls(
  countingWorld: CountingWorld,
  options: { readonly timeoutMs?: number; readonly stableMs?: number } = {},
): Promise<void> {
  const timeoutMs = options.timeoutMs ?? 10_000;
  const stableMs = options.stableMs ?? 250;
  const deadline = Date.now() + timeoutMs;
  let lastTotal = countingWorld.snapshot().total;
  let stableSince = Date.now();
  for (;;) {
    await new Promise((resolve) => setTimeout(resolve, 25));
    const total = countingWorld.snapshot().total;
    if (total !== lastTotal) {
      lastTotal = total;
      stableSince = Date.now();
    } else if (Date.now() - stableSince >= stableMs) {
      return;
    }
    if (Date.now() >= deadline) {
      throw new Error(`World calls did not stabilize within ${timeoutMs}ms`);
    }
  }
}

function categorize(operations: Readonly<Record<string, number>>): Record<WorldCallCategory, number> {
  const categories: Record<WorldCallCategory, number> = {
    storageRead: 0,
    storageWrite: 0,
    queuePublish: 0,
    worldControl: 0,
  };
  for (const [operation, calls] of Object.entries(operations)) {
    categories[category(operation)] += calls;
  }
  return categories;
}

function category(operation: string): WorldCallCategory {
  if (operation === "queue") return "queuePublish";
  if (
    operation === "events.create" ||
    operation === "writeToStream" ||
    operation === "writeToStreamMulti" ||
    operation === "closeStream"
  ) {
    return "storageWrite";
  }
  if (
    /^(runs|steps|events|hooks)\.(get|getByToken|list|listByCorrelationId)$/.test(operation) ||
    operation === "readFromStream" ||
    operation === "getStreamChunks" ||
    operation === "getStreamInfo" ||
    operation === "listStreamsByRunId"
  ) {
    return "storageRead";
  }
  return "worldControl";
}

function sortedRecord(counts: ReadonlyMap<string, number>): Record<string, number> {
  return Object.fromEntries([...counts].sort(([left], [right]) => left.localeCompare(right)));
}

function wrapQueueHandlerArgs(
  args: readonly unknown[],
  activeScope: AsyncLocalStorage<string>,
): readonly unknown[] {
  const handler = args[1];
  if (typeof handler !== "function") return args;
  const wrapped = (message: unknown, metadata: unknown): unknown => {
    const queueName =
      isRecord(metadata) && typeof metadata.queueName === "string"
        ? normalizeQueue(metadata.queueName)
        : "queue-handler";
    return activeScope.run(
      queueName,
      () => Reflect.apply(handler, undefined, [message, metadata]) as unknown,
    );
  };
  return [args[0], wrapped];
}

function normalizeQueue(queue: string): string {
  const name = queue.split("//").at(-1) ?? queue;
  if (queue.startsWith("__wkf_step_")) return `step:${name}`;
  if (queue.startsWith("__wkf_workflow_")) return `workflow:${name}`;
  return queue;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object";
}
