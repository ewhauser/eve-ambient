import type {
  MonitorStore,
  MonitorStoreTransaction,
  StoredDeadLetter,
  StoredDefinitionPin,
  StoredDeployment,
  StoredIngressReceipt,
  StoredMonitorInstance,
  StoredMonitorRun,
  StoredSubscription,
  SubscriptionStatus,
  UsageReservation,
} from "./storage.js";
export {
  MemoryAttentionEngine,
  type MemoryAttentionDiagnostics,
  type MemoryAttentionEngineFaults,
  type MemoryAttentionEngineOptions,
  type MemoryAttentionRunResult,
} from "./memory-engine.js";
import { scopedKey } from "./storage.js";
import {
  assertIdempotencyInput,
  parseIdempotencyKey,
  parseInputHash,
} from "./idempotency.js";
import { addMs, cloneJson, iso } from "./util.js";

/**
 * Co-located implementation of every `MonitorStore` responsibility for local
 * development and deterministic tests, with durable-equivalent semantics.
 */
export class MemoryMonitorStore implements MonitorStore {
  readonly #ingressReceipts = new Map<string, StoredIngressReceipt>();
  readonly #ingressDedupe = new Map<string, string>();
  readonly #subscriptions = new Map<string, StoredSubscription>();
  readonly #instances = new Map<string, StoredMonitorInstance>();
  readonly #runs = new Map<string, StoredMonitorRun>();
  readonly #deadLetters = new Map<string, StoredDeadLetter>();
  readonly #deployments = new Map<string, StoredDeployment>();
  readonly #usage = new Map<string, UsageReservation>();
  readonly #sequences = new Map<string, bigint>();
  readonly #locks = new Map<string, Promise<void>>();

  async transaction<T>(
    _lockKey: string,
    callback: (tx: MonitorStoreTransaction) => Promise<T>,
  ): Promise<T> {
    // A global mutex makes rollback atomic across the map-backed tables. The
    // production PostgreSQL store retains per-key parallelism.
    const lockKey = "__transaction__";
    const previous = this.#locks.get(lockKey) ?? Promise.resolve();
    let release!: () => void;
    const current = new Promise<void>((resolve) => {
      release = resolve;
    });
    const queued = previous.then(() => current);
    this.#locks.set(lockKey, queued);
    await previous;

    const snapshot = this.#snapshot();
    try {
      return await callback(this.#transactionView());
    } catch (error) {
      this.#restore(snapshot);
      throw error;
    } finally {
      release();
      if (this.#locks.get(lockKey) === queued) this.#locks.delete(lockKey);
    }
  }

  async listSubscriptions(input: {
    readonly applicationId: string;
    readonly statuses: readonly SubscriptionStatus[];
    readonly availableBefore: string;
    readonly limit: number;
  }): Promise<readonly StoredSubscription[]> {
    const statuses = new Set(input.statuses);
    const due = [...this.#subscriptions.values()].filter(
      (value) =>
        value.applicationId === input.applicationId &&
        statuses.has(value.status) &&
        value.availableAt <= input.availableBefore &&
        (value.status !== "processing" ||
          value.leaseExpiresAt === undefined ||
          value.leaseExpiresAt <= input.availableBefore),
    );
    return fairByTenant(
      due,
      input.limit,
      (left, right) =>
        left.availableAt.localeCompare(right.availableAt) ||
        compareSequence(left.ingressSequence, right.ingressSequence),
    ).map(clone);
  }

  async listSubscriptionsForMonitor(input: {
    readonly applicationId: string;
    readonly monitorId: string;
  }): Promise<readonly StoredSubscription[]> {
    return [...this.#subscriptions.values()]
      .filter(
        (value) =>
          value.applicationId === input.applicationId && value.monitorId === input.monitorId,
      )
      .map(clone);
  }

  async listDueInstances(input: {
    readonly applicationId: string;
    readonly availableBefore: string;
    readonly limit: number;
  }): Promise<readonly StoredMonitorInstance[]> {
    const due = [...this.#instances.values()].filter(
      (value) =>
        value.applicationId === input.applicationId &&
        value.activeRunId === undefined &&
        value.nextEvaluationAt !== undefined &&
        value.nextEvaluationAt <= input.availableBefore,
    );
    return fairByTenant(
      due,
      input.limit,
      (left, right) => left.nextEvaluationAt!.localeCompare(right.nextEvaluationAt!),
    ).map(clone);
  }

  async listDueRuns(input: {
    readonly applicationId: string;
    readonly availableBefore: string;
    readonly limit: number;
  }): Promise<readonly StoredMonitorRun[]> {
    const due = [...this.#runs.values()].filter(
      (value) =>
        value.applicationId === input.applicationId &&
        (value.status === "pending" || value.status === "retry" || value.status === "processing") &&
        value.availableAt <= input.availableBefore &&
        (value.status !== "processing" ||
          value.leaseExpiresAt === undefined ||
          value.leaseExpiresAt <= input.availableBefore),
    );
    return fairByTenant(
      due,
      input.limit,
      (left, right) => left.availableAt.localeCompare(right.availableAt),
    ).map(clone);
  }

  async listInstances(input: {
    readonly applicationId: string;
    readonly monitorId?: string | undefined;
  }): Promise<readonly StoredMonitorInstance[]> {
    return [...this.#instances.values()]
      .filter(
        (value) =>
          value.applicationId === input.applicationId &&
          (input.monitorId === undefined || value.monitorId === input.monitorId),
      )
      .map(clone);
  }

  async listRuns(input: {
    readonly applicationId: string;
    readonly monitorId?: string | undefined;
    readonly limit?: number | undefined;
  }): Promise<readonly StoredMonitorRun[]> {
    return [...this.#runs.values()]
      .filter(
        (value) =>
          value.applicationId === input.applicationId &&
          (input.monitorId === undefined || value.monitorId === input.monitorId),
      )
      .sort((left, right) => right.createdAt.localeCompare(left.createdAt))
      .slice(0, input.limit ?? 100)
      .map(clone);
  }

  async listDeadLetters(input: {
    readonly applicationId: string;
    readonly monitorId?: string | undefined;
    readonly limit?: number | undefined;
  }): Promise<readonly StoredDeadLetter[]> {
    return [...this.#deadLetters.values()]
      .filter(
        (value) =>
          value.applicationId === input.applicationId &&
          (input.monitorId === undefined || value.monitorId === input.monitorId),
      )
      .sort((left, right) => right.createdAt.localeCompare(left.createdAt))
      .slice(0, input.limit ?? 100)
      .map(clone);
  }

  async listDefinitionPins(applicationId: string): Promise<readonly StoredDefinitionPin[]> {
    return [
      ...[...this.#subscriptions.values()]
        .filter(
          (value) =>
            value.applicationId === applicationId &&
            ["conditional", "pending", "processing", "ready"].includes(value.status),
        )
        .map((value) => ({
          kind: "subscription" as const,
          id: value.id,
          monitorId: value.monitorId,
          definitionVersion: value.definitionVersion,
        })),
      ...[...this.#instances.values()]
        .filter((value) => value.applicationId === applicationId)
        .map((value) => ({
          kind: "instance" as const,
          id: value.id,
          monitorId: value.monitorId,
          definitionVersion: value.definitionVersion,
        })),
      ...[...this.#runs.values()]
        .filter(
          (value) =>
            value.applicationId === applicationId &&
            ["pending", "processing", "retry"].includes(value.status),
        )
        .map((value) => ({
          kind: "run" as const,
          id: value.id,
          monitorId: value.monitorId,
          definitionVersion: value.definitionVersion,
        })),
    ];
  }

  async getRun(id: string): Promise<StoredMonitorRun | null> {
    const value = this.#runs.get(id);
    return value === undefined ? null : clone(value);
  }

  async getInstance(id: string): Promise<StoredMonitorInstance | null> {
    const value = this.#instances.get(id);
    return value === undefined ? null : clone(value);
  }

  async purgeExpired(now: string): Promise<{
    readonly ingressReceipts: number;
    readonly runs: number;
    readonly instances: number;
    readonly usage: number;
  }> {
    let ingressReceipts = 0;
    let runs = 0;
    let instances = 0;
    let usage = 0;
    for (const [ref, receipt] of this.#ingressReceipts) {
      if (receipt.dedupeExpiresAt <= now) {
        if (
          receipt.directDispatch !== undefined &&
          ["pending", "processing"].includes(receipt.directDispatch.status)
        ) {
          this.#deadLetters.set(`purge:direct:${receipt.directDispatch.directDispatchKey}`, {
            id: `purge:direct:${receipt.directDispatch.directDispatchKey}`,
            tenantId: receipt.tenantId,
            applicationId: receipt.applicationId,
            eventKey: receipt.eventKey,
            directDispatchKey: receipt.directDispatch.directDispatchKey,
            stage: "direct-dispatch",
            reason: "ingress receipt horizon expired before direct dispatch completed",
            createdAt: now,
          });
          for (const [id, subscription] of this.#subscriptions) {
            if (
              subscription.eventKey === receipt.eventKey &&
              subscription.acceptanceId === receipt.acceptanceId &&
              subscription.status === "conditional"
            ) {
              this.#subscriptions.delete(id);
            }
          }
        }
        this.#ingressReceipts.delete(ref);
        this.#ingressDedupe.delete(receipt.dedupeKey);
        ingressReceipts += 1;
      }
    }
    for (const [id, run] of this.#runs) {
      if (run.expiresAt <= now && isTerminalRun(run.status)) {
        this.#runs.delete(id);
        runs += 1;
      }
    }
    for (const [id, instance] of this.#instances) {
      if (
        instance.expiresAt <= now &&
        instance.activeRunId === undefined &&
        instance.openBatch === undefined &&
        instance.sealedBatches.length === 0
      ) {
        this.#instances.delete(id);
        instances += 1;
      }
    }
    for (const [id, reservation] of this.#usage) {
      if (reservation.expiresAt <= now) {
        this.#usage.delete(id);
        usage += 1;
      }
    }
    return { ingressReceipts, runs, instances, usage };
  }

  #transactionView(): MonitorStoreTransaction {
    return {
      getIngressReceiptByDedupeKey: async (key) => {
        const ref = this.#ingressDedupe.get(key);
        const value = ref === undefined ? undefined : this.#ingressReceipts.get(ref);
        return value === undefined ? null : clone(value);
      },
      getIngressReceipt: async (ref) => {
        const value = this.#ingressReceipts.get(ref);
        return value === undefined ? null : clone(value);
      },
      releaseIngressDedupe: async (ref) => {
        const receipt = this.#ingressReceipts.get(ref);
        if (receipt === undefined) return;
        this.#ingressDedupe.delete(receipt.dedupeKey);
        const dedupeKey = scopedKey("expired", receipt.ref, receipt.dedupeKey);
        this.#ingressReceipts.set(ref, clone({ ...receipt, dedupeKey }));
        this.#ingressDedupe.set(dedupeKey, ref);
      },
      putIngressReceipt: async (receipt) => {
        parseIdempotencyKey("event", receipt.eventKey);
        parseInputHash(receipt.inputHash);
        parseInputHash(receipt.deploymentRevision);
        if (receipt.directDispatch !== undefined) {
          parseIdempotencyKey("direct-dispatch", receipt.directDispatch.directDispatchKey);
          parseInputHash(receipt.directDispatch.inputHash);
        }
        const previousRef = this.#ingressDedupe.get(receipt.dedupeKey);
        const previous = previousRef === undefined
          ? undefined
          : this.#ingressReceipts.get(previousRef);
        if (previous !== undefined) {
          assertIdempotencyInput({
            namespace: "memory-ingress",
            key: receipt.eventKey,
            existingInputHash: previous.inputHash,
            receivedInputHash: receipt.inputHash,
          });
        }
        if (previousRef !== undefined && previousRef !== receipt.ref) {
          this.#ingressReceipts.delete(previousRef);
        }
        this.#ingressReceipts.set(receipt.ref, clone(receipt));
        this.#ingressDedupe.set(receipt.dedupeKey, receipt.ref);
      },
      getSubscription: async (id) => {
        const value = this.#subscriptions.get(id);
        return value === undefined ? null : clone(value);
      },
      putSubscription: async (subscription) => {
        parseIdempotencyKey("branch", subscription.branchKey);
        parseIdempotencyKey("event", subscription.eventKey);
        parseInputHash(subscription.eventInputHash);
        parseInputHash(subscription.inputHash);
        if (subscription.id !== subscription.branchKey) {
          throw new TypeError("subscription id must equal branchKey");
        }
        const existing = this.#subscriptions.get(subscription.id);
        if (existing !== undefined) {
          assertIdempotencyInput({
            namespace: "memory-branch",
            key: subscription.branchKey,
            existingInputHash: existing.inputHash,
            receivedInputHash: subscription.inputHash,
          });
        }
        this.#subscriptions.set(subscription.id, clone(subscription));
      },
      deleteSubscription: async (id) => {
        this.#subscriptions.delete(id);
      },
      hasActiveSubscriptionForAcceptance: async (input) =>
        [...this.#subscriptions.values()].some(
          (subscription) =>
            subscription.eventKey === input.eventKey &&
            subscription.acceptanceId === input.acceptanceId,
        ),
      getInstance: async (id) => {
        const value = this.#instances.get(id);
        return value === undefined ? null : clone(value);
      },
      countInstances: async (input) =>
        [...this.#instances.values()].filter(
          (instance) =>
            instance.tenantId === input.tenantId &&
            instance.applicationId === input.applicationId,
        ).length,
      putInstance: async (instance) => {
        this.#instances.set(instance.id, clone(instance));
      },
      deleteInstance: async (id) => {
        this.#instances.delete(id);
      },
      getRun: async (id) => {
        const value = this.#runs.get(id);
        return value === undefined ? null : clone(value);
      },
      putRun: async (run) => {
        parseIdempotencyKey("run", run.runKey);
        parseInputHash(run.inputHash);
        const existing = this.#runs.get(run.id);
        if (existing !== undefined) {
          if (existing.runKey !== run.runKey) {
            throw new TypeError(`run ${run.id} changed runKey`);
          }
          assertIdempotencyInput({
            namespace: "memory-run",
            key: run.runKey,
            existingInputHash: existing.inputHash,
            receivedInputHash: run.inputHash,
          });
        }
        this.#runs.set(run.id, clone(run));
      },
      putDeadLetter: async (deadLetter) => {
        this.#deadLetters.set(deadLetter.id, clone(deadLetter));
      },
      getDeployment: async (applicationId) => {
        const value = this.#deployments.get(applicationId);
        return value === undefined ? null : clone(value);
      },
      putDeployment: async (deployment) => {
        this.#deployments.set(deployment.applicationId, clone(deployment));
      },
      nextIngressSequence: async (scope) => {
        const next = (this.#sequences.get(scope) ?? 0n) + 1n;
        this.#sequences.set(scope, next);
        return next.toString();
      },
      hasEarlierOpenSubscription: async (input) =>
        [...this.#subscriptions.values()].some(
          (subscription) =>
            subscription.tenantId === input.tenantId &&
            subscription.applicationId === input.applicationId &&
            subscription.monitorId === input.monitorId &&
            subscription.definitionVersion === input.definitionVersion &&
            ["pending", "processing", "ready"].includes(subscription.status) &&
            (subscription.correlationKeyHash === undefined ||
              subscription.correlationKeyHash === input.correlationKeyHash) &&
            compareSequence(subscription.ingressSequence, input.ingressSequence) < 0,
        ),
      reserveUsage: async (input) => {
        if (this.#usage.has(input.id)) return { allowed: true } as const;
        const windowStart = iso(Date.parse(input.now) - input.windowMs);
        const reservations = [...this.#usage.values()].filter(
          (value) =>
            value.scope === input.scope &&
            value.metric === input.metric &&
            value.occurredAt > windowStart &&
            value.expiresAt > input.now,
        );
        const used = reservations.reduce((sum, value) => sum + value.amount, 0);
        if (used + input.amount > input.limit) {
          const oldest = reservations.sort((left, right) =>
            left.occurredAt.localeCompare(right.occurredAt),
          )[0];
          return {
            allowed: false,
            retryAt: oldest === undefined ? addMs(input.now, input.windowMs) : addMs(oldest.occurredAt, input.windowMs),
          } as const;
        }
        this.#usage.set(input.id, {
          id: input.id,
          scope: input.scope,
          metric: input.metric,
          amount: input.amount,
          occurredAt: input.now,
          expiresAt: addMs(input.now, input.windowMs),
        });
        return { allowed: true } as const;
      },
    };
  }

  #snapshot(): MemorySnapshot {
    return {
      ingressReceipts: new Map(this.#ingressReceipts),
      ingressDedupe: new Map(this.#ingressDedupe),
      subscriptions: new Map(this.#subscriptions),
      instances: new Map(this.#instances),
      runs: new Map(this.#runs),
      deadLetters: new Map(this.#deadLetters),
      deployments: new Map(this.#deployments),
      usage: new Map(this.#usage),
      sequences: new Map(this.#sequences),
    };
  }

  #restore(snapshot: MemorySnapshot): void {
    replaceMap(this.#ingressReceipts, snapshot.ingressReceipts);
    replaceMap(this.#ingressDedupe, snapshot.ingressDedupe);
    replaceMap(this.#subscriptions, snapshot.subscriptions);
    replaceMap(this.#instances, snapshot.instances);
    replaceMap(this.#runs, snapshot.runs);
    replaceMap(this.#deadLetters, snapshot.deadLetters);
    replaceMap(this.#deployments, snapshot.deployments);
    replaceMap(this.#usage, snapshot.usage);
    replaceMap(this.#sequences, snapshot.sequences);
  }
}

interface MemorySnapshot {
  readonly ingressReceipts: Map<string, StoredIngressReceipt>;
  readonly ingressDedupe: Map<string, string>;
  readonly subscriptions: Map<string, StoredSubscription>;
  readonly instances: Map<string, StoredMonitorInstance>;
  readonly runs: Map<string, StoredMonitorRun>;
  readonly deadLetters: Map<string, StoredDeadLetter>;
  readonly deployments: Map<string, StoredDeployment>;
  readonly usage: Map<string, UsageReservation>;
  readonly sequences: Map<string, bigint>;
}

function replaceMap<TKey, TValue>(target: Map<TKey, TValue>, source: Map<TKey, TValue>): void {
  target.clear();
  for (const [key, value] of source) target.set(key, value);
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

function fairByTenant<T extends { readonly tenantId: string; readonly id: string }>(
  values: readonly T[],
  limit: number,
  compare: (left: T, right: T) => number,
): T[] {
  const queues = new Map<string, T[]>();
  for (const value of values) {
    const queue = queues.get(value.tenantId) ?? [];
    queue.push(value);
    queues.set(value.tenantId, queue);
  }
  for (const queue of queues.values()) {
    queue.sort((left, right) => compare(left, right) || left.id.localeCompare(right.id));
  }
  const tenants = [...queues.keys()].sort();
  const result: T[] = [];
  while (result.length < limit) {
    let progressed = false;
    for (const tenant of tenants) {
      const next = queues.get(tenant)?.shift();
      if (next !== undefined) {
        result.push(next);
        progressed = true;
        if (result.length === limit) break;
      }
    }
    if (!progressed) break;
  }
  return result;
}

function isTerminalRun(status: StoredMonitorRun["status"]): boolean {
  return ["ignored", "shadowed", "suppressed", "delivered", "unroutable", "dead-lettered"].includes(status);
}

function compareSequence(left: string, right: string): number {
  const leftValue = BigInt(left);
  const rightValue = BigInt(right);
  return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
}
