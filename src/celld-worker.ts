/**
 * The eve-ambient correlation mailbox as a celld cell.
 *
 * One Durable Object class, `MonitorInstance`. One cell per correlation
 * instance; the cell name *is* the instance key (`instanceStoreKey(...)`).
 * The cell reimplements no lifecycle logic: every state transition goes
 * through `dispatchLifecycle` from the package's own statechart. What the cell
 * supplies is the two things the store-backed runtime
 * gets from its database and its sweeper — durable storage for the
 * `StoredMonitorInstance` record, and a timer for `nextEvaluationAt`.
 *
 * The cell carries no monitor configuration of its own. It learns
 * `monitorId`, `definitionVersion`, and the buffer/cooldown/retention
 * configuration from its first append and pins them; an append that disagrees
 * with the pin is rejected with `definition-version-mismatch`, which the
 * runtime dead-letters.
 *
 * The cell never calls a model and holds no provider credentials. On claim it
 * POSTs an evaluation request to the runtime's evaluator, which runs the
 * decision/budget/evidence/route/delivery pipeline and records the run.
 *
 * Routes (through any node's public listener; celld proxies to the owner):
 *
 *   GET  /health
 *   POST /cells/<name>/append   CelldAppendRequest
 *   GET  /cells/<name>/state    stored instance + alarm + transition log
 *   POST /cells/<name>/rearm    recompute nextEvaluationAt and re-arm the alarm
 *   GET  /cells/<name>/whoami   DO id (for ownership tracing)
 *
 * Everything under /cells requires `authorization: Bearer $EVALUATOR_SECRET`.
 * A missing secret is a configuration error and fails closed.
 */

import {
  computeNextEvaluationAt,
  deriveLifecycleValue,
  dispatchLifecycle,
  lifecycleConfig,
} from "./instance-machine.js";
import {
  CELLD_APPEND_CONFLICT,
  CELLD_BATCH_TOO_LARGE,
  CELLD_CELL_IDENTITY_MISMATCH,
  CELLD_DEFINITION_VERSION_MISMATCH,
  CELLD_EVENT_TOO_LARGE,
  CELLD_INVALID_CAPACITY_CONFIG,
  CELLD_MALFORMED_APPEND,
  CELLD_RESIDENT_CAPACITY_EXCEEDED,
  CELLD_UNPINNED_CELL,
  projectInstanceView,
  secretsMatch,
} from "./mailbox.js";
import type {
  CelldAppendOutcome,
  CelldAppendRequest,
  CelldAppendResponse,
  CelldCellConfig,
  EvaluationRequest,
  EvaluationResponse,
  EvaluationTerminalResponse,
} from "./mailbox.js";
import { scopedKey } from "./storage.js";
import {
  deriveBranchKey,
  deriveEventKey,
  hashIdempotencyInput,
  parseIdempotencyKey,
  parseInputHash,
} from "./idempotency.js";
import type {
  BufferedEvent,
  OpenMonitorBatch,
  StoredMonitorBatch,
  StoredMonitorInstance,
} from "./storage.js";
import { addMs, durationMs } from "./time.js";
import type {
  ChannelEvent,
  MonitorDefinition,
  MonitorInstanceView,
} from "./types.js";

const LOG_LIMIT = 60;

type CelldMonitorInstance = StoredMonitorInstance<BufferedEvent>;
type CelldMonitorBatch = StoredMonitorBatch<BufferedEvent>;

interface CelldCapacityLimits {
  readonly maxEventBytes: number;
  readonly maxBatchBytes: number;
  readonly maxResidentBytes: number;
}

/** What the first append pins into the cell, and nothing else ever changes. */
interface CellPin {
  readonly cellName: string;
  readonly monitorId: string;
  readonly definitionVersion: string;
  readonly config: CelldCellConfig;
  readonly evaluatorUrl: string;
  readonly tenantId: string;
  readonly applicationId: string;
  readonly correlationKey: string;
  readonly correlationKeyHash: string;
  readonly pinnedAt: string;
}

/**
 * The in-flight run, checkpointed before every irreversible step so an alarm
 * retry after partial progress resumes instead of redoing. This is the cell's
 * stand-in for `StoredMonitorRun.stage`; the authoritative run record, with
 * its own finer-grained stages, is written by the evaluator.
 */
interface RunCheckpoint {
  readonly runId: string;
  readonly stage: "evaluating" | "complete";
  readonly batch: CelldMonitorBatch;
  readonly instanceView: MonitorInstanceView;
  readonly claimedAt: string;
  readonly outcome?: EvaluationTerminalResponse | undefined;
}

interface AppendReceipt {
  readonly branchKey: BufferedEvent["branchKey"];
  readonly eventKey: BufferedEvent["eventKey"];
  readonly inputHash: BufferedEvent["inputHash"];
  /** Hash of the complete mailbox envelope, including runtime-only event fields. */
  readonly envelopeHash: BufferedEvent["inputHash"];
  readonly outcome: CelldAppendOutcome;
  readonly flushed: boolean;
  readonly recordedAt: string;
  readonly expiresAt: string;
}

interface LogEntry {
  readonly at: string;
  readonly kind: string;
  readonly [key: string]: unknown;
}

type Storage = any;

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

const encoder = new TextEncoder();

function jsonBytes(value: unknown): number {
  return encoder.encode(JSON.stringify(value)).byteLength;
}

function parseCapacityLimit(value: unknown, name: string): number {
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  return parsed;
}

function phaseOf(event: ChannelEvent): "observed" | "undispatched" | undefined {
  const phase = event.source.phase;
  return phase === "observed" || phase === "undispatched" ? phase : undefined;
}

/** Reconstructs the value used by ingress before runtime-only fields were added. */
function canonicalIngressEvent(event: ChannelEvent): Record<string, unknown> {
  const {
    ref: _ref,
    receivedAt: _receivedAt,
    trace: _trace,
    source,
    ...canonical
  } = event;
  const { phase: _phase, ...canonicalSource } = source;
  return { ...canonical, source: canonicalSource };
}

function prospectiveBatch(open: OpenMonitorBatch<BufferedEvent>): CelldMonitorBatch {
  return {
    ...open,
    closedAt: "9999-12-31T23:59:59.999Z",
    closedBy: "cooldown-expired",
  };
}

/**
 * Deterministic in (instance key, evaluation generation) — and identical to
 * the id the store-backed runtime derives for the same claim, so a run is the
 * same run whichever tier produced it. It is also the evaluator's idempotency
 * key, which is what makes a retried alarm safe.
 */
async function deterministicRunId(cellName: string, generation: number): Promise<string> {
  const data = new TextEncoder().encode(scopedKey(cellName, String(generation)));
  const digest = await crypto.subtle.digest("SHA-256", data);
  const hex = Array.from(new Uint8Array(digest), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
  return `run_${hex}`;
}

export class MonitorInstance {
  readonly state: any;
  readonly env: any;

  constructor(state: any, env: any) {
    this.state = state;
    this.env = env;
  }

  get #storage(): Storage {
    return this.state.storage;
  }

  /**
   * Test seams. A deployed fleet passes `vars`, which are strings, so neither
   * is ever present in production.
   */
  #now(): string {
    const clock = this.env?.clock;
    return typeof clock?.now === "function"
      ? (clock.now() as Date).toISOString()
      : new Date().toISOString();
  }

  #nowMs(): number {
    return Date.parse(this.#now());
  }

  #fetch(input: string, init: RequestInit): Promise<Response> {
    const injected = this.env?.fetch;
    return typeof injected === "function"
      ? (injected(input, init) as Promise<Response>)
      : fetch(input, init);
  }

  #capacity(): CelldCapacityLimits {
    const maxEventBytes = parseCapacityLimit(
      this.env?.MAILBOX_MAX_EVENT_BYTES,
      "MAILBOX_MAX_EVENT_BYTES",
    );
    const maxBatchBytes = parseCapacityLimit(
      this.env?.MAILBOX_MAX_BATCH_BYTES,
      "MAILBOX_MAX_BATCH_BYTES",
    );
    const maxResidentBytes = parseCapacityLimit(
      this.env?.MAILBOX_MAX_RESIDENT_BYTES,
      "MAILBOX_MAX_RESIDENT_BYTES",
    );
    if (maxEventBytes > maxBatchBytes) {
      throw new TypeError("MAILBOX_MAX_EVENT_BYTES must not exceed MAILBOX_MAX_BATCH_BYTES");
    }
    if (maxBatchBytes > maxResidentBytes) {
      throw new TypeError("MAILBOX_MAX_BATCH_BYTES must not exceed MAILBOX_MAX_RESIDENT_BYTES");
    }
    return { maxEventBytes, maxBatchBytes, maxResidentBytes };
  }

  async #validatedEnvelope(body: CelldAppendRequest): Promise<{
    readonly event: BufferedEvent;
    readonly envelopeHash: BufferedEvent["inputHash"];
  }> {
    for (const [name, value] of [
      ["monitorId", body.monitorId],
      ["definitionVersion", body.definitionVersion],
      ["evaluatorUrl", body.evaluatorUrl],
      ["tenantId", body.tenantId],
      ["applicationId", body.applicationId],
      ["correlationKey", body.correlationKey],
      ["correlationKeyHash", body.correlationKeyHash],
    ] as const) {
      if (typeof value !== "string" || value.trim().length === 0) {
        throw new TypeError(`${name} must be a non-empty string`);
      }
    }
    parseIdempotencyKey("branch", body.branchKey);
    parseIdempotencyKey("event", body.eventKey);
    parseInputHash(body.eventInputHash);
    parseInputHash(body.inputHash);
    if (typeof body.acceptanceId !== "string" || body.acceptanceId.length === 0) {
      throw new TypeError("acceptanceId must be a non-empty string");
    }
    if (body.event === null || typeof body.event !== "object") {
      throw new TypeError("event must be a complete channel event object");
    }
    if (
      typeof body.acceptedAt !== "string" ||
      !Number.isFinite(Date.parse(body.acceptedAt)) ||
      typeof body.ingressSequence !== "string" ||
      body.ingressSequence.length === 0 ||
      !Number.isSafeInteger(body.bytes) ||
      body.bytes <= 0
    ) {
      throw new TypeError("acceptedAt, ingressSequence, and bytes must be valid envelope metadata");
    }
    if (
      body.event.source?.tenantId !== body.tenantId ||
      typeof body.event.source?.channelId !== "string" ||
      body.event.source.channelId.length === 0 ||
      typeof body.event.source?.installationId !== "string" ||
      body.event.source.installationId.length === 0 ||
      (body.event.source.phase !== undefined && phaseOf(body.event) === undefined) ||
      typeof body.event.ref !== "string" ||
      body.event.ref.length === 0 ||
      typeof body.event.id !== "string" ||
      body.event.id.length === 0 ||
      typeof body.event.type !== "string" ||
      body.event.type.length === 0 ||
      !Number.isSafeInteger(body.event.version) ||
      body.event.version <= 0 ||
      body.event.receivedAt !== body.acceptedAt ||
      typeof body.event.trace?.traceId !== "string" ||
      body.event.trace.traceId.length === 0 ||
      body.event.origin === null ||
      typeof body.event.origin !== "object" ||
      !["external", "agent", "monitor", "schedule"].includes(body.event.origin.kind) ||
      !Number.isSafeInteger(body.event.origin.depth) ||
      body.event.origin.depth < 0
    ) {
      throw new TypeError("event is incomplete or its source identity does not match the append address");
    }
    if (jsonBytes(body.event.data) !== body.bytes) {
      throw new TypeError("bytes must equal the UTF-8 JSON size of event.data");
    }

    const expectedEventKey = await deriveEventKey({
      tenantId: body.tenantId,
      applicationId: body.applicationId,
      channelId: body.event.source.channelId,
      installationId: body.event.source.installationId,
      sourceEventId: body.event.id,
    });
    if (expectedEventKey !== body.eventKey) {
      throw new TypeError("eventKey does not match the complete event identity");
    }
    const expectedEventInputHash = await hashIdempotencyInput({
      applicationId: body.applicationId,
      canonicalizationVersion: 1,
      event: canonicalIngressEvent(body.event),
    });
    if (expectedEventInputHash !== body.eventInputHash) {
      throw new TypeError("eventInputHash does not match the complete canonical event");
    }
    const phase = phaseOf(body.event);
    const expectedBranchKey = await deriveBranchKey({
      eventKey: body.eventKey,
      acceptanceId: body.acceptanceId,
      monitorId: body.monitorId,
      definitionVersion: body.definitionVersion,
      ...(phase === undefined ? {} : { phase }),
    });
    if (expectedBranchKey !== body.branchKey) {
      throw new TypeError("branchKey does not match the append lineage");
    }
    const expectedInputHash = await hashIdempotencyInput({
      parentInputHash: body.eventInputHash,
      eventKey: body.eventKey,
      acceptanceId: body.acceptanceId,
      branchKey: body.branchKey,
      tenantId: body.tenantId,
      applicationId: body.applicationId,
      monitorId: body.monitorId,
      definitionVersion: body.definitionVersion,
      phase: phase ?? null,
      acceptedAt: body.acceptedAt,
      orderingKey: body.ingressSequence,
    });
    if (expectedInputHash !== body.inputHash) {
      throw new TypeError("inputHash does not match the complete branch input");
    }

    const event: BufferedEvent = {
      branchKey: body.branchKey,
      eventKey: body.eventKey,
      inputHash: body.inputHash,
      event: structuredClone(body.event),
      bytes: body.bytes,
      acceptedAt: body.acceptedAt,
      ingressSequence: body.ingressSequence,
    };
    return { event, envelopeHash: await hashIdempotencyInput(event) };
  }

  #assertBatchCapacity(instance: CelldMonitorInstance, limits: CelldCapacityLimits): void {
    const batches: CelldMonitorBatch[] = [
      ...instance.sealedBatches,
      ...(instance.openBatch === undefined ? [] : [prospectiveBatch(instance.openBatch)]),
    ];
    for (const batch of batches) {
      const bytes = jsonBytes(batch);
      if (bytes > limits.maxBatchBytes) {
        throw new CelldCapacityError(
          CELLD_BATCH_TOO_LARGE,
          `mailbox batch is ${bytes} bytes and exceeds ${limits.maxBatchBytes}`,
          413,
        );
      }
    }
  }

  async #residentBytes(
    instance: CelldMonitorInstance | undefined,
    pendingReceipt?: AppendReceipt,
  ): Promise<number> {
    let bytes =
      instance?.sealedBatches.reduce((sum, batch) => sum + jsonBytes(batch), 0) ?? 0;
    if (instance?.openBatch !== undefined) bytes += jsonBytes(prospectiveBatch(instance.openBatch));
    const run = await this.#readJson<RunCheckpoint>("run");
    if (run !== undefined) bytes += jsonBytes(run.batch);
    for (const prefix of ["append:", "append-recovery:"]) {
      const receipts = await this.#storage.list({ prefix });
      for (const [key, raw] of receipts) {
        if (
          pendingReceipt !== undefined &&
          (key === `append:${pendingReceipt.branchKey}` ||
            key === `append-recovery:${pendingReceipt.branchKey}`)
        ) {
          continue;
        }
        bytes += encoder.encode(String(raw)).byteLength;
      }
    }
    if (pendingReceipt !== undefined) {
      // The append protocol briefly holds a recovery copy until the durable
      // instance and stable receipt have both committed.
      bytes += jsonBytes(pendingReceipt) * 2;
    }
    return bytes;
  }

  // --- durable accessors -------------------------------------------------
  //
  // Everything is stored as a JSON *string*, not as a structured-cloned
  // object. That deliberately reproduces the store tier, which persists
  // `JSON.stringify(instance)` into a jsonb column: keys whose value is
  // `undefined` disappear on the round trip. `dispatchLifecycle` reads every
  // optional field with `!== undefined`, so an absent key and an explicit
  // `undefined` are indistinguishable to it — which is why the two tiers'
  // conformance runs agree.

  async #readJson<T>(key: string): Promise<T | undefined> {
    const raw = await this.#storage.get(key);
    return raw === undefined || raw === null ? undefined : (JSON.parse(raw as string) as T);
  }

  async #writeJson(key: string, value: unknown): Promise<void> {
    await this.#storage.put(key, JSON.stringify(value));
  }

  async #log(entry: LogEntry): Promise<void> {
    const log = (await this.#readJson<LogEntry[]>("log")) ?? [];
    log.push(entry);
    await this.#writeJson("log", log.slice(-LOG_LIMIT));
  }

  #definition(config: CelldCellConfig): MonitorDefinition<ChannelEvent> {
    // `lifecycleConfig()` reads exactly `buffer` and `cooldown`; everything
    // else on a definition belongs to the evaluator's pipeline.
    return config as unknown as MonitorDefinition<ChannelEvent>;
  }

  #decisionRetentionMs(config: CelldCellConfig): number {
    return durationMs(config.retention.decisions);
  }

  #dedupeRetentionMs(config: CelldCellConfig): number {
    return Math.max(
      durationMs(config.retention.dedupe),
      durationMs(config.retention.decisions),
    );
  }

  /**
   * Mirrors the runtime's `#newInstance`. The cell has no subscription record
   * to copy tenancy from, so those fields come off the pinned append.
   */
  #newInstance(pin: CellPin, now: string): CelldMonitorInstance {
    return {
      id: pin.cellName,
      tenantId: pin.tenantId,
      applicationId: pin.applicationId,
      monitorId: pin.monitorId,
      definitionVersion: pin.definitionVersion,
      correlationKey: pin.correlationKey,
      correlationKeyHash: pin.correlationKeyHash,
      sealedBatches: [],
      evaluationGeneration: 0,
      consecutiveIgnores: 0,
      eventsSinceLastWake: 0,
      expiresAt: addMs(now, this.#decisionRetentionMs(pin.config)),
      createdAt: now,
      updatedAt: now,
    };
  }

  /**
   * The instance's `nextEvaluationAt` is the alarm. Three cases:
   *   - a due time              -> setAlarm (clamped to not-in-the-past)
   *   - no due time, run active -> leave the alarm alone; it is either the
   *     alarm currently executing or celld's pending retry of it. Deleting it
   *     would strand the claimed batch.
   *   - no due time, no run     -> arm instance retention cleanup
   */
  async #arm(instance: CelldMonitorInstance): Promise<number | null> {
    if (instance.nextEvaluationAt !== undefined) {
      const at = Math.max(Date.parse(instance.nextEvaluationAt), this.#nowMs());
      await this.#storage.setAlarm(at);
      return at;
    }
    if (instance.activeRunId !== undefined) {
      return (await this.#storage.getAlarm()) ?? null;
    }
    const expiresAt = Math.max(Date.parse(instance.expiresAt), this.#nowMs());
    await this.#storage.setAlarm(expiresAt);
    return expiresAt;
  }

  #isIdle(instance: CelldMonitorInstance): boolean {
    return (
      instance.activeRunId === undefined &&
      instance.openBatch === undefined &&
      instance.sealedBatches.length === 0
    );
  }

  async #cleanupExpiredReceipts(now: string): Promise<void> {
    let nextExpiry: number | null = null;
    for (const prefix of ["append:", "append-recovery:"]) {
      const receipts = await this.#storage.list({ prefix });
      for (const [key, raw] of receipts) {
        let receipt: AppendReceipt | undefined;
        try {
          receipt = JSON.parse(String(raw)) as AppendReceipt;
        } catch {
          await this.#storage.delete(key);
          continue;
        }
        if (typeof receipt.expiresAt !== "string" || receipt.expiresAt <= now) {
          await this.#storage.delete(key);
          continue;
        }
        const expiresAt = Date.parse(receipt.expiresAt);
        if (Number.isFinite(expiresAt)) {
          nextExpiry = nextExpiry === null ? expiresAt : Math.min(nextExpiry, expiresAt);
        }
      }
    }
    if (nextExpiry === null) await this.#storage.deleteAlarm();
    else await this.#storage.setAlarm(Math.max(nextExpiry, this.#nowMs()));
  }

  /** Hot cells may never reach idle cleanup, so expiry is also enforced on append. */
  async #purgeExpiredReceipts(now: string): Promise<void> {
    for (const prefix of ["append:", "append-recovery:"]) {
      const receipts = await this.#storage.list({ prefix });
      for (const [key, raw] of receipts) {
        try {
          const receipt = JSON.parse(String(raw)) as AppendReceipt;
          if (typeof receipt.expiresAt === "string" && receipt.expiresAt > now) continue;
        } catch {
          // Malformed receipt state is not a valid idempotency tombstone.
        }
        await this.#storage.delete(key);
      }
    }
  }

  async #cellName(request?: Request): Promise<string> {
    const header = request?.headers.get("x-cell-name");
    if (header !== null && header !== undefined && header.length > 0) return header;
    const stored = await this.#storage.get("cellName");
    if (typeof stored === "string") return stored;
    return this.state.id?.name ?? String(this.state.id);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const parts = url.pathname.split("/").filter(Boolean);
    const action = parts[parts.length - 1] ?? "state";

    if (action === "whoami") {
      return json({ ok: true, id: String(this.state.id), name: this.state.id?.name ?? null });
    }
    if (action === "append") return await this.#append(request);
    if (action === "rearm") return await this.#rearm(request);
    if (action === "state") return await this.#state(request);
    return json({ ok: false, code: "unknown-action", error: `unknown cell action ${action}` }, 404);
  }

  async #append(request: Request): Promise<Response> {
    let body: CelldAppendRequest;
    try {
      body = (await request.json()) as CelldAppendRequest;
    } catch (error) {
      return json({ ok: false, code: CELLD_MALFORMED_APPEND, error: String(error) }, 400);
    }
    if (
      typeof body?.monitorId !== "string" ||
      typeof body?.definitionVersion !== "string" ||
      typeof body?.branchKey !== "string" ||
      typeof body?.eventKey !== "string" ||
      typeof body?.acceptanceId !== "string" ||
      typeof body?.eventInputHash !== "string" ||
      typeof body?.inputHash !== "string" ||
      typeof body?.event !== "object" ||
      body.event === null ||
      typeof body?.config !== "object" ||
      body.config === null ||
      typeof body.config.retention !== "object" ||
      body.config.retention === null
    ) {
      return json(
        {
          ok: false,
          code: CELLD_MALFORMED_APPEND,
          error:
            "append requires monitorId, definitionVersion, lineage, a complete event, and config.retention",
        },
        400,
      );
    }
    let limits: CelldCapacityLimits;
    try {
      limits = this.#capacity();
    } catch (error) {
      return json(
        { ok: false, code: CELLD_INVALID_CAPACITY_CONFIG, error: String(error) },
        503,
      );
    }
    let buffered: BufferedEvent;
    let envelopeHash: BufferedEvent["inputHash"];
    try {
      ({ event: buffered, envelopeHash } = await this.#validatedEnvelope(body));
    } catch (error) {
      return json({ ok: false, code: CELLD_MALFORMED_APPEND, error: String(error) }, 400);
    }
    const now = this.#now();
    const name = await this.#cellName(request);
    await this.#purgeExpiredReceipts(now);

    const storedPin = await this.#readJson<CellPin>("cell");
    const newPin = storedPin === undefined;
    let pin: CellPin;
    if (storedPin === undefined) {
      pin = {
        cellName: name,
        monitorId: body.monitorId,
        definitionVersion: body.definitionVersion,
        config: body.config,
        evaluatorUrl: body.evaluatorUrl,
        tenantId: body.tenantId,
        applicationId: body.applicationId,
        correlationKey: body.correlationKey,
        correlationKeyHash: body.correlationKeyHash,
        pinnedAt: now,
      };
    } else {
      pin = storedPin;
    }
    if (
      !newPin &&
      (pin.monitorId !== body.monitorId ||
        pin.definitionVersion !== body.definitionVersion)
    ) {
      // The instance key already carries both, so a mismatch means durable
      // state was moved across versions without the fleet following. Running
      // one version's events through another's buffer configuration would be
      // silent corruption; refuse, and let the runtime dead-letter it.
      return json(
        {
          ok: false,
          code: CELLD_DEFINITION_VERSION_MISMATCH,
          error:
            `cell ${name} is pinned to ${pin.monitorId}@${pin.definitionVersion}; ` +
            `append carried ${body.monitorId}@${body.definitionVersion}`,
        },
        409,
      );
    }
    if (
      !newPin &&
      (pin.tenantId !== body.tenantId ||
        pin.applicationId !== body.applicationId ||
        pin.correlationKey !== body.correlationKey ||
        pin.correlationKeyHash !== body.correlationKeyHash)
    ) {
      return json(
        {
          ok: false,
          code: CELLD_CELL_IDENTITY_MISMATCH,
          error: `cell ${name} received an append addressed to a different correlation instance`,
        },
        409,
      );
    }
    const definition = this.#definition(pin.config);

    const existing = await this.#readJson<CelldMonitorInstance>("instance");
    const expiredIdle =
      existing !== undefined &&
      existing.expiresAt <= now &&
      this.#isIdle(existing);
    if (expiredIdle) {
      await this.#storage.delete("run");
    }
    let instance = existing === undefined || expiredIdle ? this.#newInstance(pin, now) : existing;

    const receiptKey = `append:${body.branchKey}`;
    let receipt = await this.#readJson<AppendReceipt>(receiptKey);
    if (receipt !== undefined && receipt.expiresAt <= now) {
      await this.#storage.delete(receiptKey);
      receipt = undefined;
    }
    if (
      receipt !== undefined &&
      (receipt.branchKey !== body.branchKey ||
        receipt.eventKey !== body.eventKey ||
        receipt.inputHash !== body.inputHash ||
        receipt.envelopeHash !== envelopeHash)
    ) {
      return json(
        {
          ok: false,
          code: CELLD_APPEND_CONFLICT,
          error:
            `branch ${body.branchKey} was already appended with different input`,
        },
        409,
      );
    }
    if (receipt !== undefined) {
      await this.#storage.delete(`append-recovery:${body.branchKey}`);
      const alarmAt = await this.#arm(instance);
      await this.#log({
        at: now,
        kind: "append-duplicate",
        branchKey: body.branchKey,
        eventRef: body.event.ref,
        outcome: receipt.outcome,
      });
      return json(
        this.#appendResponse(name, pin, instance, receipt, alarmAt, now),
      );
    }
    // Heals the only commit gap in the receipt protocol: a recovery copy is
    // written before the instance and promoted after the instance commits. A
    // retry in that window sees the branch in the instance or active run and
    // must not dispatch APPEND again.
    const storedBranch = await this.#findBranch(instance, body.branchKey);
    if (storedBranch !== undefined) {
      const storedEnvelopeHash = await hashIdempotencyInput(storedBranch);
      if (
        storedBranch.eventKey !== body.eventKey ||
        storedBranch.inputHash !== body.inputHash ||
        storedEnvelopeHash !== envelopeHash
      ) {
        return json(
          {
            ok: false,
            code: CELLD_APPEND_CONFLICT,
            error:
              `branch ${body.branchKey} was already appended with different input`,
          },
          409,
        );
      }
      const prior = await this.#recordedAppendOutcome(body.branchKey);
      const recovered: AppendReceipt = {
        branchKey: body.branchKey,
        eventKey: body.eventKey,
        inputHash: body.inputHash,
        envelopeHash,
        outcome: prior?.outcome ?? "updated",
        flushed: prior?.flushed ?? false,
        recordedAt: prior?.recordedAt ?? now,
        expiresAt: prior?.expiresAt ?? addMs(now, this.#dedupeRetentionMs(pin.config)),
      };
      await this.#writeJson(receiptKey, recovered);
      await this.#storage.delete(`append-recovery:${body.branchKey}`);
      const alarmAt = await this.#arm(instance);
      await this.#log({
        at: now,
        kind: "append-duplicate",
        branchKey: body.branchKey,
        eventRef: body.event.ref,
        outcome: recovered.outcome,
        recovered: true,
      });
      return json(
        this.#appendResponse(name, pin, instance, recovered, alarmAt, now),
      );
    }

    const eventEnvelopeBytes = jsonBytes(buffered);
    if (eventEnvelopeBytes > limits.maxEventBytes) {
      return json(
        {
          ok: false,
          code: CELLD_EVENT_TOO_LARGE,
          error: `mailbox event envelope is ${eventEnvelopeBytes} bytes and exceeds ${limits.maxEventBytes}`,
        },
        413,
      );
    }

    const result = dispatchLifecycle(instance, definition, {
      type: "APPEND",
      event: buffered,
      now,
    });
    instance = {
      ...result.instance,
      expiresAt: addMs(now, this.#decisionRetentionMs(pin.config)),
      updatedAt: now,
    };

    const outcome: CelldAppendOutcome =
      existing === undefined || expiredIdle ? "opened" : result.flushed ? "flushed" : "updated";
    const nextReceipt: AppendReceipt = {
      branchKey: body.branchKey,
      eventKey: body.eventKey,
      inputHash: body.inputHash,
      envelopeHash,
      outcome,
      flushed: result.flushed,
      recordedAt: now,
      expiresAt: addMs(now, this.#dedupeRetentionMs(pin.config)),
    };
    try {
      this.#assertBatchCapacity(instance, limits);
      const residentBytes = await this.#residentBytes(instance, nextReceipt);
      if (residentBytes > limits.maxResidentBytes) {
        throw new CelldCapacityError(
          CELLD_RESIDENT_CAPACITY_EXCEEDED,
          `mailbox resident payload is ${residentBytes} bytes and exceeds ${limits.maxResidentBytes}`,
          429,
        );
      }
    } catch (error) {
      if (error instanceof CelldCapacityError) {
        return json({ ok: false, code: error.code, error: error.message }, error.status);
      }
      throw error;
    }
    if (newPin) {
      await this.#storage.put("cellName", name);
      await this.#writeJson("cell", pin);
    }
    await this.#writeJson(`append-recovery:${body.branchKey}`, nextReceipt);
    await this.#writeJson("instance", instance);
    await this.#writeJson(receiptKey, nextReceipt);
    await this.#storage.delete(`append-recovery:${body.branchKey}`);
    const alarmAt = await this.#arm(instance);
    await this.#log({
      at: now,
      kind: "append",
      branchKey: body.branchKey,
      eventRef: body.event.ref,
      eventKey: body.eventKey,
      inputHash: body.inputHash,
      phase: phaseOf(body.event) ?? null,
      flushed: result.flushed,
      outcome,
      state: deriveLifecycleValue(instance, now),
      nextEvaluationAt: instance.nextEvaluationAt ?? null,
      alarmAt,
    });

    return json(this.#appendResponse(name, pin, instance, nextReceipt, alarmAt, now));
  }

  async #findBranch(
    instance: CelldMonitorInstance,
    branchKey: BufferedEvent["branchKey"],
  ): Promise<BufferedEvent | undefined> {
    const open = instance.openBatch?.events.find((event) => event.branchKey === branchKey);
    if (open !== undefined) return open;
    for (const batch of instance.sealedBatches) {
      const sealed = batch.events.find((event) => event.branchKey === branchKey);
      if (sealed !== undefined) return sealed;
    }
    const run = await this.#readJson<RunCheckpoint>("run");
    return run?.batch.events.find((event) => event.branchKey === branchKey);
  }

  async #recordedAppendOutcome(
    branchKey: BufferedEvent["branchKey"],
  ): Promise<
    | (Pick<AppendReceipt, "outcome" | "flushed" | "recordedAt"> & {
        readonly expiresAt?: string;
      })
    | undefined
  > {
    const recovery = await this.#readJson<AppendReceipt>(`append-recovery:${branchKey}`);
    if (recovery?.branchKey === branchKey) {
      return {
        outcome: recovery.outcome,
        flushed: recovery.flushed,
        recordedAt: recovery.recordedAt,
        expiresAt: recovery.expiresAt,
      };
    }
    const log = (await this.#readJson<LogEntry[]>("log")) ?? [];
    for (let index = log.length - 1; index >= 0; index -= 1) {
      const entry = log[index]!;
      if (entry.kind !== "append" || entry.branchKey !== branchKey) continue;
      if (
        (entry.outcome === "opened" || entry.outcome === "updated" || entry.outcome === "flushed") &&
        typeof entry.flushed === "boolean"
      ) {
        return {
          outcome: entry.outcome,
          flushed: entry.flushed,
          recordedAt: typeof entry.at === "string" ? entry.at : this.#now(),
        };
      }
    }
    return undefined;
  }

  #appendResponse(
    name: string,
    pin: CellPin,
    instance: CelldMonitorInstance,
    receipt: AppendReceipt,
    alarmAt: number | null,
    now: string,
  ): CelldAppendResponse & Record<string, unknown> {
    return {
      ok: true,
      cellName: name,
      monitorId: pin.monitorId,
      definitionVersion: pin.definitionVersion,
      outcome: receipt.outcome,
      flushed: receipt.flushed,
      receipt: {
        branchKey: receipt.branchKey,
        inputHash: receipt.inputHash,
        outcome: receipt.outcome,
        flushed: receipt.flushed,
        recordedAt: receipt.recordedAt,
      },
      state: deriveLifecycleValue(instance, now),
      openBatchSize: instance.openBatch?.events.length ?? 0,
      sealedBatches: instance.sealedBatches.length,
      cooldownUntil: instance.cooldownUntil ?? null,
      nextEvaluationAt: instance.nextEvaluationAt ?? null,
      alarmAt,
      evaluationGeneration: instance.evaluationGeneration,
      now,
    };
  }

  async #state(request: Request): Promise<Response> {
    const now = this.#now();
    const instance = await this.#readJson<CelldMonitorInstance>("instance");
    const run = await this.#readJson<RunCheckpoint>("run");
    const branchKeys = [
      ...(instance?.sealedBatches.flatMap((batch) => batch.events) ?? []),
      ...(instance?.openBatch?.events ?? []),
      ...(run?.batch.events ?? []),
    ].map((event) => event.branchKey);
    return json({
      ok: true,
      cellName: await this.#cellName(request),
      pin: (await this.#readJson<CellPin>("cell")) ?? null,
      doId: String(this.state.id),
      instance: instance ?? null,
      state: instance === undefined ? null : deriveLifecycleValue(instance, now),
      pendingAlarm: (await this.#storage.getAlarm()) ?? null,
      activeRun: run ?? null,
      bufferedBranchKeys: [...new Set(branchKeys)].sort(),
      residentBytes: await this.#residentBytes(instance),
      log: (await this.#readJson<LogEntry[]>("log")) ?? [],
      now,
    });
  }

  /**
   * Recomputes `nextEvaluationAt` from the stored record and re-arms the
   * alarm — the documented mitigation for celld abandoning an alarm after six
   * counted handler failures, which otherwise leaves a cell with buffered work
   * and no timer to evaluate it.
   *
   * No transition semantics live here: `computeNextEvaluationAt` is the same
   * derivation `dispatchLifecycle` applies, over the same record. A cell with
   * an active run is re-armed for *now* instead, because the timer that would
   * have retried its evaluation is exactly the one that was abandoned.
   */
  async #rearm(request: Request): Promise<Response> {
    const now = this.#now();
    const pin = await this.#readJson<CellPin>("cell");
    if (pin === undefined) {
      return json({ ok: false, code: CELLD_UNPINNED_CELL, error: "cell has no pinned monitor" }, 409);
    }
    const instance = await this.#readJson<CelldMonitorInstance>("instance");
    if (instance === undefined) {
      return json({ ok: true, rearmed: false, reason: "no instance record", now });
    }
    if (instance.activeRunId !== undefined) {
      const at = this.#nowMs();
      await this.#storage.setAlarm(at);
      await this.#log({ at: now, kind: "rearm", mode: "resume-run", runId: instance.activeRunId, alarmAt: at });
      return json({
        ok: true,
        rearmed: true,
        mode: "resume-run",
        cellName: await this.#cellName(request),
        activeRunId: instance.activeRunId,
        alarmAt: at,
        now,
      });
    }
    const config = lifecycleConfig(this.#definition(pin.config));
    const nextEvaluationAt = computeNextEvaluationAt(
      {
        config,
        activeRunId: undefined,
        cooldownUntil: instance.cooldownUntil,
        openBatch: instance.openBatch,
        sealedBatches: instance.sealedBatches,
      },
      now,
    );
    const next: CelldMonitorInstance = {
      ...instance,
      ...(nextEvaluationAt === undefined
        ? { nextEvaluationAt: undefined }
        : { nextEvaluationAt }),
      updatedAt: now,
    };
    await this.#writeJson("instance", next);
    const alarmAt = await this.#arm(next);
    await this.#log({
      at: now,
      kind: "rearm",
      mode: "recompute",
      nextEvaluationAt: nextEvaluationAt ?? null,
      alarmAt,
    });
    return json({
      ok: true,
      rearmed: true,
      mode: "recompute",
      cellName: await this.#cellName(request),
      state: deriveLifecycleValue(next, now),
      nextEvaluationAt: nextEvaluationAt ?? null,
      alarmAt,
      now,
    });
  }

  /**
   * The evaluation tick: the store tier's due-instance sweep collapsed into a
   * timer. Claim, evaluate, complete — each step checkpointed, and any failure
   * thrown so celld's native alarm retry (2s doubling, six counted failures)
   * is the retry ladder.
   *
   * celld re-dispatches a due alarm while its handler is still running (the
   * `_cf_ALARM` row is deleted in `finish_alarm_handler`, i.e. only once the
   * handler ends), so a handler that awaits an outbound fetch reliably gets a
   * second, *concurrent* invocation. Cloudflare's Durable Objects never
   * overlap alarm handlers. `blockConcurrencyWhile` shuts the cell's input
   * gate, which restores that guarantee: the second dispatch queues, and by
   * the time it runs the run is complete and the alarm cleared.
   *
   * See https://github.com/denoland/celld/issues/144. The cost of the
   * workaround is that appends queue behind an in-flight evaluation.
   *
   * The block must not reject — a failed critical section resets the actor —
   * so failures are carried out of the block and rethrown here, where celld's
   * alarm retry ladder sees them.
   */
  async alarm(info?: { retryCount?: number }): Promise<void> {
    const attempt = async (): Promise<unknown> => {
      try {
        await this.#evaluate(info);
        return null;
      } catch (error) {
        return error;
      }
    };
    const failure =
      typeof this.state.blockConcurrencyWhile === "function"
        ? await this.state.blockConcurrencyWhile(attempt)
        : await attempt();
    if (failure !== null) throw failure;
  }

  async #evaluate(info?: { retryCount?: number }): Promise<void> {
    const retryCount = info?.retryCount ?? 0;
    // Tags every log line so concurrent invocations of the same alarm are
    // distinguishable after the fact.
    const invocation = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    let now = this.#now();
    let instance = await this.#readJson<CelldMonitorInstance>("instance");
    if (instance === undefined) {
      await this.#cleanupExpiredReceipts(now);
      return;
    }
    if (instance.expiresAt <= now && this.#isIdle(instance)) {
      await this.#storage.delete("instance");
      await this.#storage.delete("run");
      await this.#log({ at: now, kind: "instance-expired" });
      await this.#cleanupExpiredReceipts(now);
      return;
    }
    const pin = await this.#readJson<CellPin>("cell");
    if (pin === undefined) return;
    const definition = this.#definition(pin.config);

    let run = await this.#readJson<RunCheckpoint>("run");

    if (instance.activeRunId !== undefined && run?.runId === instance.activeRunId) {
      // Resuming a run whose alarm failed part-way. Skip the claim entirely:
      // the batch is already out of the buffer and recorded in the checkpoint.
      await this.#log({
        inv: invocation,
        at: now,
        kind: "resume",
        runId: run.runId,
        stage: run.stage,
        retryCount,
      });
    } else if (instance.activeRunId !== undefined) {
      // A run is active but its checkpoint is gone. Nothing can be replayed,
      // so fail the run the way the runtime's dead-letter path does.
      const failed = dispatchLifecycle(instance, definition, { type: "RUN_FAILED", now });
      instance = { ...failed.instance, updatedAt: now };
      await this.#storage.delete("run");
      await this.#writeJson("instance", instance);
      await this.#log({ inv: invocation, at: now, kind: "run-failed-orphan", retryCount });
      await this.#arm(instance);
      return;
    } else {
      const generation = instance.evaluationGeneration + 1;
      const runId = await deterministicRunId(pin.cellName, generation);
      const instanceView = projectInstanceView(instance);
      const claim = dispatchLifecycle(instance, definition, { type: "CLAIM", runId, now });
      if (claim.claimedBatch === undefined) {
        // A spurious wake is normal: an alarm can fire while a cooldown still
        // gates the batch. Persist the refreshed record and re-arm.
        instance = { ...claim.instance, updatedAt: now };
        await this.#writeJson("instance", instance);
        const alarmAt = await this.#arm(instance);
        await this.#log({
          inv: invocation,
          at: now,
          kind: "claim-empty",
          state: deriveLifecycleValue(instance, now),
          nextEvaluationAt: instance.nextEvaluationAt ?? null,
          alarmAt,
          retryCount,
        });
        return;
      }
      instance = { ...claim.instance, updatedAt: now };
      run = {
        runId,
        stage: "evaluating",
        batch: claim.claimedBatch,
        instanceView,
        claimedAt: now,
      };
      // Checkpoint first: after this pair of writes an interrupted alarm
      // resumes rather than re-claims.
      await this.#writeJson("run", run);
      await this.#writeJson("instance", instance);
      await this.#log({
        inv: invocation,
        at: now,
        kind: "claim",
        runId,
        closedBy: claim.claimedBatch.closedBy,
        branchKeys: claim.claimedBatch.events.map((event) => event.branchKey),
        retryCount,
      });
    }

    // --- evaluation stage ------------------------------------------------
    const claimed = run!;
    let outcome: EvaluationTerminalResponse | undefined = claimed.outcome;
    if (outcome === undefined) {
      const response = await this.#callEvaluator(pin, claimed, retryCount, invocation);
      if (response.status === "retry") {
        const alarmAt = Math.max(Date.parse(response.retryAt), this.#nowMs());
        await this.#storage.setAlarm(alarmAt);
        await this.#log({
          inv: invocation,
          at: this.#now(),
          kind: "evaluation-deferred",
          runId: claimed.runId,
          retryAt: response.retryAt,
          alarmAt,
          retryCount,
        });
        return;
      }
      outcome = response;
      run = { ...claimed, outcome, stage: "complete" };
      await this.#writeJson("run", run);
    }

    // --- completion ------------------------------------------------------
    now = this.#now();
    const completion =
      outcome.status === "dead-lettered"
        ? dispatchLifecycle(instance, definition, { type: "RUN_FAILED", now })
        : dispatchLifecycle(instance, definition, {
            type: "RUN_COMPLETED",
            status: outcome.status,
            ...(outcome.decision === undefined ? {} : { decision: outcome.decision }),
            ...(outcome.binding === undefined ? {} : { binding: outcome.binding }),
            now,
          });
    instance = {
      ...completion.instance,
      expiresAt: addMs(now, this.#decisionRetentionMs(pin.config)),
      updatedAt: now,
    };
    // Instance before checkpoint delete: a crash between the two leaves a
    // stale checkpoint with activeRunId already cleared, which the next claim
    // harmlessly overwrites. The reverse order left activeRunId pointing at a
    // vanished checkpoint, and the orphan path then RUN_FAILED away the
    // completed wake's cooldown and counters.
    await this.#writeJson("instance", instance);
    await this.#storage.delete("run");
    const alarmAt = await this.#arm(instance);
    await this.#log({
      inv: invocation,
      at: now,
      kind: outcome.status === "dead-lettered" ? "dead-lettered" : "completed",
      runId: claimed.runId,
      status: outcome.status,
      // decision and binding are the two RUN_COMPLETED inputs that come from
      // outside the cell; logging them is what lets the conformance oracle
      // replay this timeline through the machine and diff the result.
      decision: outcome.decision ?? null,
      binding: outcome.binding ?? null,
      closedBy: claimed.batch.closedBy,
      branchKeys: claimed.batch.events.map((event) => event.branchKey),
      state: deriveLifecycleValue(instance, now),
      cooldownUntil: instance.cooldownUntil ?? null,
      nextEvaluationAt: instance.nextEvaluationAt ?? null,
      alarmAt,
      retryCount,
    });
  }

  /**
   * Hands the complete claimed batch to the runtime's evaluator. The cell is
   * the payload custodian for this hop; evaluation never reads an event
   * repository to reconstruct the batch.
   *
   * A throw here rides celld's alarm-retry ladder, and the run is idempotent
   * by `runId`, so a retry after a lost response returns the recorded outcome
   * rather than delivering twice.
   */
  async #callEvaluator(
    pin: CellPin,
    run: RunCheckpoint,
    retryCount: number,
    invocation: string,
  ): Promise<EvaluationResponse> {
    const target = String(this.env.EVALUATOR_URL ?? pin.evaluatorUrl);
    const configuredSecret = this.env?.EVALUATOR_SECRET;
    if (typeof configuredSecret !== "string" || configuredSecret.length === 0) {
      throw new Error("EVALUATOR_SECRET is required; refusing to call the evaluator");
    }
    const secret = configuredSecret;
    const body: Omit<EvaluationRequest, "secret"> = {
      runId: run.runId,
      instanceId: pin.cellName,
      tenantId: pin.tenantId,
      applicationId: pin.applicationId,
      monitorId: pin.monitorId,
      definitionVersion: pin.definitionVersion,
      correlationKey: pin.correlationKey,
      correlationKeyHash: pin.correlationKeyHash,
      batch: run.batch,
      instanceView: run.instanceView,
      claimedAt: run.claimedAt,
    };
    let response: Response;
    try {
      response = await this.#fetch(target, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${secret}`,
        },
        body: JSON.stringify(body),
      });
    } catch (error) {
      await this.#log({
        inv: invocation,
        at: this.#now(),
        kind: "evaluation-error",
        runId: run.runId,
        retryCount,
        error: String(error),
      });
      throw new Error(`evaluation of ${run.runId} could not reach ${target}: ${String(error)}`);
    }
    if (!response.ok) {
      await this.#log({
        inv: invocation,
        at: this.#now(),
        kind: "evaluation-rejected",
        runId: run.runId,
        status: response.status,
        retryCount,
      });
      throw new Error(`evaluation of ${run.runId} returned ${response.status}`);
    }
    return validateEvaluationResponse(await response.json(), run.runId);
  }
}

class CelldCapacityError extends Error {
  readonly code: string;
  readonly status: number;

  constructor(code: string, message: string, status: number) {
    super(message);
    this.name = "CelldCapacityError";
    this.code = code;
    this.status = status;
  }
}

const TERMINAL_EVALUATION_STATUSES = new Set([
  "ignored",
  "shadowed",
  "suppressed",
  "delivered",
  "unroutable",
  "dead-lettered",
]);

function validateEvaluationResponse(value: unknown, expectedRunId: string): EvaluationResponse {
  if (value === null || typeof value !== "object") {
    throw new Error(`evaluation of ${expectedRunId} returned a malformed response`);
  }
  const response = value as Record<string, unknown>;
  if (response.runId !== expectedRunId) {
    throw new Error(
      `evaluation of ${expectedRunId} returned mismatched runId ${String(response.runId)}`,
    );
  }
  if (response.status === "retry") {
    if (
      typeof response.retryAt !== "string" ||
      !Number.isFinite(Date.parse(response.retryAt))
    ) {
      throw new Error(`evaluation of ${expectedRunId} returned an invalid retryAt`);
    }
    return value as EvaluationResponse;
  }
  if (typeof response.status !== "string" || !TERMINAL_EVALUATION_STATUSES.has(response.status)) {
    throw new Error(
      `evaluation of ${expectedRunId} returned unknown status ${String(response.status)}`,
    );
  }
  if (response.decision !== undefined) {
    if (response.decision === null || typeof response.decision !== "object") {
      throw new Error(`evaluation of ${expectedRunId} returned an invalid decision`);
    }
    const decision = response.decision as Record<string, unknown>;
    if (
      !["ignore", "wake"].includes(String(decision.action)) ||
      typeof decision.reasonClass !== "string" ||
      decision.reasonClass.length === 0 ||
      (decision.confidence !== undefined &&
        (typeof decision.confidence !== "number" ||
          !Number.isFinite(decision.confidence) ||
          decision.confidence < 0 ||
          decision.confidence > 1))
    ) {
      throw new Error(`evaluation of ${expectedRunId} returned an invalid decision`);
    }
  }
  return value as EvaluationResponse;
}

export default {
  async fetch(request: Request, env: any): Promise<Response> {
    const url = new URL(request.url);
    const parts = url.pathname.split("/").filter(Boolean);

    if (parts[0] === "health") return json({ ok: true, app: "eve-ambient-mailbox" });
    if (parts[0] !== "cells" || parts.length < 2) {
      return json(
        { ok: false, error: "use /cells/<instanceKey>/<append|state|rearm|whoami>" },
        404,
      );
    }
    const secret = env?.EVALUATOR_SECRET;
    if (typeof secret !== "string" || secret.length === 0) {
      return json(
        {
          ok: false,
          code: "missing-evaluator-secret",
          error: "EVALUATOR_SECRET must be configured before cell routes are enabled",
        },
        503,
      );
    }
    const header = request.headers.get("authorization") ?? "";
    const match = /^Bearer[ ]+(.+)$/i.exec(header.trim());
    if (!secretsMatch(match?.[1] ?? "", secret)) {
      return json({ ok: false, code: "unauthorized", error: "unauthorized" }, 401);
    }
    const name = decodeURIComponent(parts[1]!);
    const action = parts[2] ?? "state";

    const id = env.MONITOR.idFromName(name);
    const stub = env.MONITOR.get(id);
    const inner = new URL(url);
    inner.pathname = `/${action}`;
    // The cell needs its own name inside alarm(), where there is no request.
    // Pass it explicitly rather than relying on `state.id.name` surviving a
    // restore on another node.
    const forwarded = new Request(inner.toString(), request);
    forwarded.headers.set("x-cell-name", name);
    const response = await stub.fetch(forwarded);
    const out = new Response(response.body, response);
    out.headers.set("x-cell-name", name);
    return out;
  },
};
