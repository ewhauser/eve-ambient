import type {
  ChannelEvent,
  DirectDispatchReceipt,
  JsonValue,
  MonitorBatchClosedBy,
  MonitorBindingView,
  MonitorDecision,
  MonitorDeliveryReceipt,
  MonitorEvidenceSnapshot,
  MonitorMode,
  MonitorInstanceView,
  MonitorPhase,
} from "./types.js";
import type {
  BatchKey,
  BranchKey,
  DirectDispatchKey,
  EventKey,
  InputHash,
  RunKey,
} from "./idempotency.js";

/** Payload-free source acceptance and atomically frozen fan-out receipt. */
export interface StoredIngressReceipt {
  readonly ref: string;
  readonly eventKey: EventKey;
  /** Unique durable generation for this post-horizon ingress acceptance. */
  readonly acceptanceId: string;
  readonly inputHash: InputHash;
  readonly dedupeKey: string;
  readonly tenantId: string;
  readonly applicationId: string;
  readonly channelId: string;
  readonly installationId: string;
  readonly eventId: string;
  readonly eventType: string;
  readonly traceId: string;
  readonly traceSpanId?: string | undefined;
  /** Monotonic within one tenant/application acceptance domain. */
  readonly ingressSequence: string;
  readonly bytes: number;
  readonly acceptedAt: string;
  readonly dedupeExpiresAt: string;
  /** Active definitions and conditional paths frozen with source acceptance. */
  readonly deploymentRevision: InputHash;
  readonly branches: readonly StoredFanoutBranchReceipt[];
  /** Durable coordination for chat direct-dispatch completion. */
  readonly directDispatch?: StoredDirectDispatch | undefined;
}

export interface StoredFanoutBranchReceipt {
  readonly branchKey: BranchKey;
  readonly inputHash: InputHash;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly phase?: MonitorPhase | undefined;
  readonly condition: "always" | "direct-undispatched";
  readonly status: "accepted" | "terminal";
}

export interface StoredDirectDispatch {
  readonly directDispatchKey: DirectDispatchKey;
  readonly inputHash: InputHash;
  readonly bindingGeneration: string;
  readonly status: "pending" | "processing" | "dispatched" | "undispatched" | "failed";
  readonly attempt: number;
  readonly availableAt: string;
  readonly leaseExpiresAt?: string | undefined;
  readonly error?: string | undefined;
  readonly receipts?: readonly DirectDispatchReceipt[] | undefined;
  readonly updatedAt: string;
}

export type SubscriptionStatus =
  | "conditional"
  | "pending"
  | "processing"
  | "ready";

export interface StoredSubscription {
  readonly id: string;
  readonly branchKey: BranchKey;
  readonly eventKey: EventKey;
  readonly acceptanceId: string;
  readonly eventInputHash: InputHash;
  readonly inputHash: InputHash;
  /** Complete branch-owned input; store-mode processing never loads it elsewhere. */
  readonly event: ChannelEvent<string, JsonValue, JsonValue>;
  readonly bytes: number;
  readonly acceptedAt: string;
  readonly tenantId: string;
  readonly applicationId: string;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly ingressSequence: string;
  readonly phase?: MonitorPhase | undefined;
  readonly status: SubscriptionStatus;
  readonly attempt: number;
  readonly availableAt: string;
  readonly leaseExpiresAt?: string | undefined;
  readonly correlationKeyHash?: string | undefined;
  readonly correlationKey?: string | undefined;
  readonly outcome?: string | undefined;
  readonly createdAt: string;
  readonly updatedAt: string;
}

export interface BufferedEventValue {
  readonly bytes: number;
  readonly acceptedAt: string;
  readonly ingressSequence: string;
}

/** Complete event envelope owned by a mailbox. */
export interface BufferedEvent extends BufferedEventValue {
  readonly branchKey: BranchKey;
  readonly eventKey: EventKey;
  readonly inputHash: InputHash;
  readonly event: ChannelEvent<string, JsonValue, JsonValue>;
}

export interface OpenMonitorBatch<TEvent extends BufferedEventValue = BufferedEvent> {
  readonly events: readonly TEvent[];
  readonly bytes: number;
  readonly openedAt: string;
  readonly updatedAt: string;
}

export interface StoredMonitorBatch<TEvent extends BufferedEventValue = BufferedEvent> {
  readonly events: readonly TEvent[];
  readonly bytes: number;
  readonly openedAt: string;
  readonly closedAt: string;
  readonly closedBy: MonitorBatchClosedBy;
}

/** Claimed immutable membership handed to one durable run. */
export interface FrozenMonitorBatch extends StoredMonitorBatch<BufferedEvent> {
  readonly batchKey: BatchKey;
  readonly inputHash: InputHash;
  readonly eventKeys: readonly EventKey[];
  readonly frozenAt: string;
}

/** Durable lineage retained after terminal completion; event payloads are not. */
export interface FrozenMonitorBatchSummary extends Omit<FrozenMonitorBatch, "events"> {
  readonly branchKeys: readonly BranchKey[];
  readonly eventCount: number;
}

export interface StoredLastDecision {
  readonly action: "ignore" | "wake";
  readonly reasonClass?: string | undefined;
  readonly confidence?: number | undefined;
  readonly decidedAt: string;
}

export interface StoredMonitorInstance<TEvent extends BufferedEventValue = BufferedEvent> {
  readonly id: string;
  readonly tenantId: string;
  readonly applicationId: string;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly correlationKey: string;
  readonly correlationKeyHash: string;
  readonly openBatch?: OpenMonitorBatch<TEvent> | undefined;
  readonly sealedBatches: readonly StoredMonitorBatch<TEvent>[];
  readonly activeRunId?: string | undefined;
  readonly nextEvaluationAt?: string | undefined;
  readonly evaluationGeneration: number;
  readonly lastDecision?: StoredLastDecision | undefined;
  readonly lastWakeAt?: string | undefined;
  readonly cooldownUntil?: string | undefined;
  readonly consecutiveIgnores: number;
  readonly eventsSinceLastWake: number;
  readonly binding?: MonitorBindingView | undefined;
  readonly expiresAt: string;
  readonly createdAt: string;
  readonly updatedAt: string;
}

export type MonitorRunStatus =
  | "pending"
  | "processing"
  | "retry"
  | "ignored"
  | "shadowed"
  | "suppressed"
  | "delivered"
  | "unroutable"
  | "dead-lettered";

export type MonitorRunStage =
  | "decision"
  | "policy"
  | "evidence"
  | "route"
  | "delivery"
  | "complete";

export interface StoredMonitorRun {
  readonly id: string;
  readonly runKey: RunKey;
  readonly inputHash: InputHash;
  readonly eventKeys: readonly EventKey[];
  readonly instanceId: string;
  readonly tenantId: string;
  readonly applicationId: string;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly correlationKeyHash: string;
  /** Full while actionable; reduced to lineage and completeness once terminal. */
  readonly batch: FrozenMonitorBatch | FrozenMonitorBatchSummary;
  readonly mode: MonitorMode;
  readonly instanceView: MonitorInstanceView;
  readonly status: MonitorRunStatus;
  readonly stage: MonitorRunStage;
  readonly attempt: number;
  readonly availableAt: string;
  readonly leaseExpiresAt?: string | undefined;
  readonly decision?: MonitorDecision | undefined;
  readonly decisionSource?: "rule" | "model" | "fallback" | undefined;
  readonly modelUsage?: {
    readonly inputTokens: number;
    readonly outputTokens: number;
    readonly estimatedCost?: number | undefined;
  } | undefined;
  readonly snapshot?: MonitorEvidenceSnapshot | undefined;
  readonly route?: {
    readonly channelId: string;
    readonly target: JsonValue;
  } | undefined;
  readonly receipt?: MonitorDeliveryReceipt | undefined;
  readonly suppression?: {
    readonly cause: string;
    readonly scope: string;
    readonly retryAt?: string | undefined;
  } | undefined;
  readonly error?: string | undefined;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly expiresAt: string;
}

export interface StoredDeadLetter {
  readonly id: string;
  readonly tenantId: string;
  readonly applicationId: string;
  readonly monitorId?: string | undefined;
  readonly definitionVersion?: string | undefined;
  readonly eventKey?: EventKey | undefined;
  readonly branchKey?: BranchKey | undefined;
  readonly directDispatchKey?: DirectDispatchKey | undefined;
  readonly instanceId?: string | undefined;
  readonly runId?: string | undefined;
  readonly stage: string;
  readonly reason: string;
  readonly createdAt: string;
}

export interface StoredDeployment {
  readonly applicationId: string;
  /** Mailbox ownership is durable; changing it without migration strands work. */
  readonly mailboxMode?: "store" | "celld" | undefined;
  readonly activeMonitorIds: readonly string[];
  readonly activeVersions: Readonly<Record<string, readonly string[]>>;
  /** Definition versions pinned by cells, which are not visible in store tables. */
  readonly celldDefinitionPins?: Readonly<Record<string, readonly string[]>> | undefined;
  readonly updatedAt: string;
}

export interface StoredDefinitionPin {
  readonly kind: "subscription" | "instance" | "run";
  readonly id: string;
  readonly monitorId: string;
  readonly definitionVersion: string;
}

export interface UsageReservation {
  readonly id: string;
  readonly scope: string;
  readonly metric: string;
  readonly amount: number;
  readonly occurredAt: string;
  readonly expiresAt: string;
}

/**
 * Payload-free source acceptance and direct-dispatch coordination.
 *
 * System position: provider/channel ingress, before any monitor branch runs.
 * The receipt proves one accepted source identity and freezes its fan-out, but
 * never becomes an event repository. `nextIngressSequence` orders newly
 * accepted work within one tenant/application acceptance domain.
 */
export interface MonitorIngressTransaction {
  getIngressReceiptByDedupeKey(key: string): Promise<StoredIngressReceipt | null>;
  getIngressReceipt(ref: string): Promise<StoredIngressReceipt | null>;
  releaseIngressDedupe(ref: string): Promise<void>;
  putIngressReceipt(receipt: StoredIngressReceipt): Promise<void>;

  nextIngressSequence(scope: string): Promise<string>;
}

/**
 * Complete per-monitor branch work between ingress and mailbox custody.
 *
 * System position: after schema validation and fan-out, through filtering and
 * correlation, but before the selected mailbox durably accepts the branch. A
 * subscription owns the complete event payload until either the store mailbox
 * commits its copy or celld returns an idempotent append receipt; it is then
 * deleted.
 */
export interface MonitorSubscriptionTransaction {
  getSubscription(id: string): Promise<StoredSubscription | null>;
  putSubscription(subscription: StoredSubscription): Promise<void>;
  deleteSubscription(id: string): Promise<void>;
  hasActiveSubscriptionForAcceptance(input: {
    readonly eventKey: EventKey;
    readonly acceptanceId: string;
  }): Promise<boolean>;

  hasEarlierOpenSubscription(input: {
    readonly tenantId: string;
    readonly applicationId: string;
    readonly monitorId: string;
    readonly definitionVersion: string;
    readonly correlationKeyHash: string;
    readonly ingressSequence: string;
  }): Promise<boolean>;
}

/**
 * Store-mode correlation mailbox state.
 *
 * System position: per correlation key, after preprocessing and before a run
 * is frozen. These methods own complete buffered events, debounce/cooldown
 * state, and due times only when `mailbox.mode === "store"`. In celld mode the
 * cell owns this state and the runtime must not use this facet for live work.
 */
export interface MonitorMailboxTransaction {
  getInstance(id: string): Promise<StoredMonitorInstance | null>;
  countInstances(input: {
    readonly tenantId: string;
    readonly applicationId: string;
  }): Promise<number>;
  putInstance(instance: StoredMonitorInstance): Promise<void>;
  deleteInstance(id: string): Promise<void>;
}

/**
 * Durable evaluation checkpoints.
 *
 * System position: after a mailbox freezes membership and before/through
 * decision, evidence, routing, and delivery. Active runs own their complete
 * frozen batch. Terminal runs replace source events with lineage and
 * completeness while retaining bounded decision, evidence, and receipt data.
 */
export interface MonitorRunTransaction {
  getRun(id: string): Promise<StoredMonitorRun | null>;
  putRun(run: StoredMonitorRun): Promise<void>;
}

/**
 * Durable failure receipts from any pipeline stage.
 *
 * System position: cross-cutting operator record for terminal ingress,
 * subscription, mailbox, evaluation, binding, or delivery failure. A dead
 * letter contains lineage and a bounded reason, never an event payload.
 */
export interface MonitorDeadLetterTransaction {
  putDeadLetter(deadLetter: StoredDeadLetter): Promise<void>;
}

/**
 * Deployed monitor versions and durable mailbox-ownership declarations.
 *
 * System position: runtime initialization and explicit migrations, not the hot
 * event path. Pins prevent a definition or mailbox backend from being removed
 * while durable work still depends on it.
 */
export interface MonitorDeploymentTransaction {
  getDeployment(applicationId: string): Promise<StoredDeployment | null>;
  putDeployment(deployment: StoredDeployment): Promise<void>;
}

/**
 * Rolling-window capacity and cost reservations.
 *
 * System position: policy gates around model calls, tokens, events, and wakes.
 * Multiple scope reservations may share one transaction so a denied operation
 * consumes none of its platform, tenant, application, or monitor allowances.
 */
export interface MonitorBudgetTransaction {
  /** Atomically reserves one scope and returns its next opening when denied. */
  reserveUsage(input: {
    readonly id: string;
    readonly scope: string;
    readonly metric: string;
    readonly amount: number;
    readonly limit: number;
    readonly windowMs: number;
    readonly now: string;
  }): Promise<{ readonly allowed: true } | { readonly allowed: false; readonly retryAt: string }>;
}

/**
 * The capabilities currently co-located inside one atomic unit of work.
 *
 * Current transitions cross facets at ingress receipt + frozen fan-out,
 * direct-dispatch receipt + conditional branches/failures, store-mailbox append
 * + branch deletion, store-mode instance + run transitions, and terminal run +
 * dead-letter recording. The split interfaces identify those crossings so
 * future stores can replace the generic callback with semantic atomic commands
 * rather than assuming every record belongs in one database.
 */
export interface MonitorStoreTransaction
  extends MonitorIngressTransaction,
    MonitorSubscriptionTransaction,
    MonitorMailboxTransaction,
    MonitorRunTransaction,
    MonitorDeadLetterTransaction,
    MonitorDeploymentTransaction,
    MonitorBudgetTransaction {}

/** Serializes atomic state transitions under a stable cross-process key. */
export interface MonitorTransactionCoordinator {
  transaction<T>(lockKey: string, callback: (tx: MonitorStoreTransaction) => Promise<T>): Promise<T>;
}

/** Discovers and drains complete branch work after ingress fan-out. */
export interface MonitorSubscriptionStore {
  listSubscriptions(input: {
    readonly applicationId: string;
    readonly statuses: readonly SubscriptionStatus[];
    readonly availableBefore: string;
    readonly limit: number;
  }): Promise<readonly StoredSubscription[]>;
  listSubscriptionsForMonitor(input: {
    readonly applicationId: string;
    readonly monitorId: string;
  }): Promise<readonly StoredSubscription[]>;
}

/**
 * Reads and schedules the store-backed correlation mailbox.
 *
 * This entire facet is replaced by celld for live mailbox ownership. It remains
 * in the composite store today for store-mode operation, migrations, retention,
 * and compatibility with the existing runtime constructor.
 */
export interface MonitorMailboxStore {
  listDueInstances(input: {
    readonly applicationId: string;
    readonly availableBefore: string;
    readonly limit: number;
  }): Promise<readonly StoredMonitorInstance[]>;
  listInstances(input: {
    readonly applicationId: string;
    readonly monitorId?: string | undefined;
  }): Promise<readonly StoredMonitorInstance[]>;
  getInstance(id: string): Promise<StoredMonitorInstance | null>;
}

/** Queries durable evaluation checkpoints. */
export interface MonitorRunStore {
  listDueRuns(input: {
    readonly applicationId: string;
    readonly availableBefore: string;
    readonly limit: number;
  }): Promise<readonly StoredMonitorRun[]>;
  listRuns(input: {
    readonly applicationId: string;
    readonly monitorId?: string | undefined;
    readonly limit?: number | undefined;
  }): Promise<readonly StoredMonitorRun[]>;
  getRun(id: string): Promise<StoredMonitorRun | null>;
}

/** Queries payload-free terminal failure receipts from every pipeline stage. */
export interface MonitorDeadLetterStore {
  listDeadLetters(input: {
    readonly applicationId: string;
    readonly monitorId?: string | undefined;
    readonly limit?: number | undefined;
  }): Promise<readonly StoredDeadLetter[]>;
}

/** Finds durable work that pins monitor definitions during deployment changes. */
export interface MonitorDeploymentStore {
  listDefinitionPins(applicationId: string): Promise<readonly StoredDefinitionPin[]>;
}

export interface MonitorPurgeResult {
  readonly ingressReceipts: number;
  readonly runs: number;
  readonly instances: number;
  readonly usage: number;
}

/**
 * Applies operational retention across all persistence responsibilities.
 *
 * Purging is deliberately a composite concern today: an expired direct-dispatch
 * receipt may atomically dead-letter and remove conditional branches, while
 * unrelated terminal runs, idle store-mailbox instances, and usage reservations
 * are independently eligible for deletion.
 */
export interface MonitorRetentionStore {
  purgeExpired(now: string): Promise<MonitorPurgeResult>;
}

/**
 * Compatibility composition consumed by `MonitorRuntime` today.
 *
 * This is a capability composition, not a claim that every responsibility must
 * remain in PostgreSQL. `PostgresMonitorStore` and `MemoryMonitorStore` provide
 * all facets in one object; the next simplification can narrow runtime paths to
 * only the facets required by the selected mailbox topology.
 */
export interface MonitorStore
  extends MonitorTransactionCoordinator,
    MonitorSubscriptionStore,
    MonitorMailboxStore,
    MonitorRunStore,
    MonitorDeadLetterStore,
    MonitorDeploymentStore,
    MonitorRetentionStore {}

export function instanceStoreKey(input: {
  readonly tenantId: string;
  readonly applicationId: string;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly correlationKeyHash: string;
}): string {
  return scopedKey(
    input.tenantId,
    input.applicationId,
    input.monitorId,
    input.definitionVersion,
    input.correlationKeyHash,
  );
}

const UTF8 = new TextEncoder();

/**
 * Collision-free encoding for durable compound key and lock components.
 *
 * `TextEncoder` rather than `Buffer.byteLength(part, "utf8")` — identical byte
 * counts, but no Node built-in, so this module stays bundleable for non-Node
 * hosts alongside the lifecycle statechart.
 */
export function scopedKey(...parts: readonly string[]): string {
  return parts.map((part) => `${UTF8.encode(part).length}:${part}`).join("|");
}
