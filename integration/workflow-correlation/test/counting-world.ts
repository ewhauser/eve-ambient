import { AsyncLocalStorage } from "node:async_hooks";
import { getWorld } from "workflow/runtime";

type WorkflowWorld = Awaited<ReturnType<typeof getWorld>>;

export interface WorldCallSnapshot {
  readonly total: number;
  readonly operations: Readonly<Record<string, number>>;
  readonly eventTypes: Readonly<Record<string, number>>;
  readonly trace: readonly string[];
}

export interface CountingWorld {
  readonly world: WorkflowWorld;
  reset(): void;
  snapshot(): WorldCallSnapshot;
}

/** Counts calls to the public standard-World methods. */
export function createCountingWorld(target: WorkflowWorld): CountingWorld {
  let operations = new Map<string, number>();
  let eventTypes = new Map<string, number>();
  let trace: string[] = [];
  const proxies = new WeakMap<object, object>();
  const activeScope = new AsyncLocalStorage<string>();

  const increment = (counts: Map<string, number>, key: string): void => {
    counts.set(key, (counts.get(key) ?? 0) + 1);
  };
  const record = (operation: string, args: readonly unknown[]): void => {
    increment(operations, operation);
    const scope = activeScope.getStore() ?? "client";
    let detail = operation;
    if (operation === "events.create") {
      const request = args[1];
      if (isRecord(request) && typeof request.eventType === "string") {
        increment(eventTypes, request.eventType);
        detail += `(${request.eventType})`;
      }
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
      trace = [];
    },
    snapshot() {
      const operationRecord = sortedRecord(operations);
      return {
        total: Object.values(operationRecord).reduce((total, value) => total + value, 0),
        operations: operationRecord,
        eventTypes: sortedRecord(eventTypes),
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
    if (Date.now() >= deadline) throw new Error("World calls did not stabilize");
  }
}

function wrapQueueHandlerArgs(
  args: readonly unknown[],
  activeScope: AsyncLocalStorage<string>,
): readonly unknown[] {
  const handler = args[1];
  if (typeof handler !== "function") return args;
  return [args[0], (message: unknown, metadata: unknown) => {
    const queue = isRecord(metadata) && typeof metadata.queueName === "string"
      ? metadata.queueName
      : "queue-handler";
    return activeScope.run(queue, () => Reflect.apply(handler, undefined, [message, metadata]));
  }];
}

function sortedRecord(counts: ReadonlyMap<string, number>): Record<string, number> {
  return Object.fromEntries([...counts].sort(([left], [right]) => left.localeCompare(right)));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object";
}
