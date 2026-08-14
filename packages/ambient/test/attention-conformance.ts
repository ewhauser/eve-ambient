import { describe, expect, it } from "vitest";
import {
  IdempotencyConflictError,
  AttentionCapacityError,
  canonicalizeChannelDelivery,
  compileAcceptedFanout,
  defineChannelCanonicalization,
  type AcceptedFanout,
  type AttentionCallbacks,
  type AttentionDeliveryReceipt,
  type AttentionEngine,
  type AttentionBranchPlan,
  type FrozenAttentionBatch,
  type FullAttentionBranch,
  type PreparedAttentionOutcome,
  type PreparedAttentionWake,
} from "../src/index.js";
import {
  VirtualMonitorClock,
} from "../src/testing.js";
export interface AttentionConformanceHarness {
  readonly engine: AttentionEngine;
  runDue(): Promise<AttentionConformanceRunResult>;
  diagnostics(): AttentionConformanceDiagnostics | Promise<AttentionConformanceDiagnostics>;
}

export interface AttentionConformanceRunResult {
  readonly claimed: number;
  readonly ignored: number;
  readonly shadowed: number;
  readonly delivered: number;
  readonly failed: number;
  readonly terminalFailures: number;
}

export interface AttentionConformanceDiagnostics {
  readonly eventCoordinators: number;
  readonly pendingFanoutPayloads: number;
  readonly acceptanceReceipts: number;
  readonly correlationWorkflows: number;
  readonly bufferedBranchPayloads: number;
  readonly activeBatchPayloads: number;
  readonly preparedWakePayloads: number;
  readonly branchReceipts: number;
  readonly deliveryReceipts: number;
  readonly terminalFailures: number;
}

export interface AttentionConformanceFactoryOptions {
  readonly callbacks: AttentionCallbacks;
  readonly clock: VirtualMonitorClock;
  readonly dedupeMs?: number | undefined;
  readonly retryDelayMs?: number | undefined;
  readonly claimLeaseMs?: number | undefined;
  readonly maxAttempts?: number | undefined;
  readonly maxBranches?: number | undefined;
  readonly maxFanoutBytes?: number | undefined;
  readonly maxPreparedWakeBytes?: number | undefined;
  readonly faults?: {
    readonly beforeBranchAppend?:
      | ((branch: FullAttentionBranch) => void | Promise<void>)
      | undefined;
    readonly afterBranchAppend?:
      | ((branch: FullAttentionBranch) => void | Promise<void>)
      | undefined;
  } | undefined;
}

export type AttentionConformanceFactory = (
  options: AttentionConformanceFactoryOptions,
) => AttentionConformanceHarness | Promise<AttentionConformanceHarness>;

export function defineAttentionEngineConformance(
  name: string,
  createHarness: AttentionConformanceFactory,
): void {
  describe(`${name} attention engine conformance`, () => {
    it("freezes an empty fan-out against later deployment membership", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      const harness = await createHarness({ callbacks, clock });
      const empty = await fanout({ branches: [] });
      const accepted = await harness.engine.accept(empty);
      const changed = await fanout({ branches: [plan()] });
      const retry = await harness.engine.accept(changed);

      expect(accepted.branchKeys).toEqual([]);
      expect(retry).toEqual(accepted);
      expect(await harness.diagnostics()).toMatchObject({
        pendingFanoutPayloads: 0,
        acceptanceReceipts: 1,
        bufferedBranchPayloads: 0,
      });
    });

    it("returns the original membership when a retry proposes different branches", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      const harness = await createHarness({ callbacks, clock, maxBranches: 1 });
      const original = await fanout({ branches: [plan({ monitorId: "monitor-a" })] });
      const accepted = await harness.engine.accept(original);
      const changed = await fanout({
        branches: [
          plan({ monitorId: "monitor-b" }),
          plan({ monitorId: "monitor-c" }),
        ],
      });

      await expect(harness.engine.accept(changed)).resolves.toEqual(accepted);
      expect((await harness.diagnostics()).bufferedBranchPayloads).toBe(1);
    });

    it("fails closed on source conflicts and overlapping branch-input conflicts", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      const harness = await createHarness({ callbacks, clock });
      await harness.engine.accept(await fanout({ branches: [plan()] }));

      await expect(
        harness.engine.accept(await fanout({ body: "changed", branches: [plan()] })),
      ).rejects.toBeInstanceOf(IdempotencyConflictError);
      await expect(
        harness.engine.accept(
          await fanout({
            branches: [
              plan({
                policy: {
                  buffer: { mode: "immediate" },
                  cooldownAfterWakeMs: 10,
                },
              }),
            ],
          }),
        ),
      ).rejects.toBeInstanceOf(IdempotencyConflictError);
    });

    it("rejects over-capacity fan-out before retaining its payload", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      const harness = await createHarness({ callbacks, clock, maxBranches: 1 });
      const input = await fanout({
        branches: [
          plan({ monitorId: "monitor-a" }),
          plan({ monitorId: "monitor-b" }),
        ],
      });

      await expect(harness.engine.accept(input)).rejects.toBeInstanceOf(
        AttentionCapacityError,
      );
      expect(await harness.diagnostics()).toMatchObject({
        eventCoordinators: 0,
        pendingFanoutPayloads: 0,
      });
    });

    it("rejects an oversized complete fan-out before retaining its payload", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      const harness = await createHarness({
        callbacks,
        clock,
        maxFanoutBytes: 1,
      });

      await expect(
        harness.engine.accept(await fanout({ branches: [] })),
      ).rejects.toBeInstanceOf(AttentionCapacityError);
      expect(await harness.diagnostics()).toMatchObject({
        eventCoordinators: 0,
        pendingFanoutPayloads: 0,
      });
    });

    it("resumes a partially handed-off fan-out without repeating completed branches", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      let appendAttempts = 0;
      const harness = await createHarness({
        callbacks,
        clock,
        faults: {
          beforeBranchAppend: () => {
            appendAttempts += 1;
            if (appendAttempts === 2) throw new Error("injected append outage");
          },
        },
      });
      const input = await fanout({
        branches: [
          plan({ monitorId: "monitor-a", correlationKey: "a" }),
          plan({ monitorId: "monitor-b", correlationKey: "b" }),
        ],
      });

      await expect(harness.engine.accept(input)).rejects.toThrow("injected append outage");
      expect(await harness.diagnostics()).toMatchObject({
        pendingFanoutPayloads: 1,
        bufferedBranchPayloads: 1,
      });
      await expect(harness.engine.accept(input)).resolves.toMatchObject({
        branchKeys: input.branches.map((branch) => branch.branchKey),
      });
      expect(appendAttempts).toBe(3);
      expect(await harness.diagnostics()).toMatchObject({
        pendingFanoutPayloads: 0,
        bufferedBranchPayloads: 2,
      });
    });

    it("recovers a lost branch-append response from the receiver receipt", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      let loseResponse = true;
      const harness = await createHarness({
        callbacks,
        clock,
        faults: {
          afterBranchAppend: () => {
            if (loseResponse) {
              loseResponse = false;
              throw new Error("append response lost");
            }
          },
        },
      });
      const input = await fanout({ branches: [plan()] });

      await expect(harness.engine.accept(input)).rejects.toThrow("append response lost");
      await expect(harness.engine.accept(input)).resolves.toMatchObject({
        branchKeys: [input.branches[0]!.branchKey],
      });
      expect((await harness.diagnostics()).bufferedBranchPayloads).toBe(1);
    });

    it("starts the admission receipt horizon after branch handoff completes", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      const harness = await createHarness({
        callbacks,
        clock,
        dedupeMs: 20,
        faults: {
          afterBranchAppend: () => clock.advance(50),
        },
      });

      const receipt = await harness.engine.accept(await fanout({ branches: [plan()] }));

      expect(receipt.acceptedAt).toBe("2026-01-01T00:00:00.000Z");
      expect(receipt.dedupeExpiresAt).toBe("2026-01-01T00:00:00.070Z");
      expect((await harness.diagnostics()).acceptanceReceipts).toBe(1);
    });

    it("timestamps each serialized branch append when it commits", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      let appendResponses = 0;
      const harness = await createHarness({
        callbacks,
        clock,
        faults: {
          afterBranchAppend: () => {
            appendResponses += 1;
            if (appendResponses === 1) clock.advance(50);
          },
        },
      });
      const policy = debouncePolicy();
      await harness.engine.accept(
        await fanout({
          branches: [
            plan({ monitorId: "monitor-a", correlationKey: "a", policy }),
            plan({ monitorId: "monitor-b", correlationKey: "b", policy }),
          ],
        }),
      );

      await expect(harness.runDue()).resolves.toMatchObject({ delivered: 1 });
      expect(callbacks.prepareCalls).toHaveLength(1);
      clock.advance(10);
      await expect(harness.runDue()).resolves.toMatchObject({ delivered: 1 });
      expect(callbacks.prepareCalls).toHaveLength(2);
    });

    it("hands prepare the complete batch in canonical source order", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      const harness = await createHarness({ callbacks, clock });
      const policy = debouncePolicy();
      await harness.engine.accept(
        await fanout({
          eventId: "event-z",
          body: "last",
          branches: [plan({ orderKey: "z", policy })],
        }),
      );
      await harness.engine.accept(
        await fanout({
          eventId: "event-a",
          body: "first",
          branches: [plan({ orderKey: "a", policy })],
        }),
      );
      clock.advance(10);

      await expect(harness.runDue()).resolves.toMatchObject({ delivered: 1 });
      expect(callbacks.prepareCalls).toHaveLength(1);
      expect(callbacks.prepareCalls[0]!.branches.map((branch) => branch.orderKey)).toEqual([
        "a",
        "z",
      ]);
      expect(callbacks.prepareCalls[0]!.branches.map((branch) => branch.event.data)).toEqual([
        { body: "first" },
        { body: "last" },
      ]);
      expect(callbacks.prepareInputsFrozen).toEqual([true]);
    });

    it("seals a full debounce batch before appending an overflowing branch", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      const harness = await createHarness({ callbacks, clock });
      const policy = {
        buffer: {
          mode: "debounce" as const,
          quietPeriodMs: 100,
          maxWaitMs: 1_000,
          maxEvents: 2,
          maxBytes: 100_000,
        },
      };
      for (const eventId of ["event-1", "event-2", "event-3"]) {
        await harness.engine.accept(
          await fanout({ eventId, branches: [plan({ orderKey: eventId, policy })] }),
        );
      }

      await expect(harness.runDue()).resolves.toMatchObject({ delivered: 1 });
      expect(callbacks.prepareCalls[0]).toMatchObject({ closedBy: "max-events" });
      expect(callbacks.prepareCalls[0]!.branches).toHaveLength(2);
      expect((await harness.diagnostics()).bufferedBranchPayloads).toBe(1);
    });

    it("preserves bounded debounce partitions when a wake starts cooldown", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      const harness = await createHarness({ callbacks, clock });
      const policy = {
        buffer: {
          mode: "debounce" as const,
          quietPeriodMs: 1,
          maxWaitMs: 1_000,
          maxEvents: 2,
          maxBytes: 100_000,
        },
        cooldownAfterWakeMs: 100,
      };
      await harness.engine.accept(
        await fanout({ eventId: "event-1", branches: [plan({ policy })] }),
      );
      clock.advance(1);
      const prepareStarted = callbacks.holdPrepare();
      const firstRun = harness.runDue();
      await prepareStarted;
      for (const eventId of ["event-2", "event-3", "event-4"]) {
        await harness.engine.accept(
          await fanout({ eventId, branches: [plan({ orderKey: eventId, policy })] }),
        );
      }
      callbacks.releasePrepare();
      await expect(firstRun).resolves.toMatchObject({ delivered: 1 });

      clock.advance(100);
      await expect(harness.runDue()).resolves.toMatchObject({ delivered: 1 });
      expect(callbacks.prepareCalls[1]).toMatchObject({ closedBy: "max-events" });
      expect(callbacks.prepareCalls[1]!.branches).toHaveLength(2);
      expect((await harness.diagnostics()).bufferedBranchPayloads).toBe(1);
    });

    it("serializes an append concurrent with freeze into the next batch", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      const harness = await createHarness({ callbacks, clock });
      await harness.engine.accept(
        await fanout({ eventId: "event-1", branches: [plan({ orderKey: "1" })] }),
      );
      const prepareStarted = callbacks.holdPrepare();
      const firstRun = harness.runDue();
      await prepareStarted;
      await harness.engine.accept(
        await fanout({ eventId: "event-2", branches: [plan({ orderKey: "2" })] }),
      );
      callbacks.releasePrepare();

      await expect(firstRun).resolves.toMatchObject({ delivered: 1 });
      await expect(harness.runDue()).resolves.toMatchObject({ delivered: 1 });
      expect(
        callbacks.prepareCalls.map((batch) =>
          batch.branches.map((branch) => branch.event.id),
        ),
      ).toEqual([["event-1"], ["event-2"]]);
    });

    it("ignores a stale prepare completion after another worker reclaims the lease", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      const harness = await createHarness({ callbacks, clock, claimLeaseMs: 5 });
      await harness.engine.accept(await fanout({ branches: [plan()] }));

      callbacks.holdPrepare();
      const staleRun = harness.runDue();
      await callbacks.waitForPrepareCalls(1);
      clock.advance(5);
      const currentRun = harness.runDue();
      await callbacks.waitForPrepareCalls(2);
      callbacks.releasePrepare();

      const outcomes = await Promise.all([staleRun, currentRun]);
      expect(outcomes.reduce((sum, outcome) => sum + outcome.delivered, 0)).toBe(1);
      expect(callbacks.prepareCalls).toHaveLength(2);
      expect(callbacks.deliveryCalls).toHaveLength(1);
      expect(callbacks.effects.size).toBe(1);
    });

    it("ignores a stale delivery receipt after another worker reclaims the lease", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      const harness = await createHarness({ callbacks, clock, claimLeaseMs: 5 });
      await harness.engine.accept(await fanout({ branches: [plan()] }));

      callbacks.holdDelivery();
      const staleRun = harness.runDue();
      await callbacks.waitForDeliveryCalls(1);
      clock.advance(5);
      const currentRun = harness.runDue();
      await callbacks.waitForDeliveryCalls(2);
      callbacks.releaseDelivery();

      const outcomes = await Promise.all([staleRun, currentRun]);
      expect(outcomes.reduce((sum, outcome) => sum + outcome.delivered, 0)).toBe(1);
      expect(callbacks.prepareCalls).toHaveLength(1);
      expect(callbacks.deliveryCalls).toHaveLength(2);
      expect(callbacks.effects.size).toBe(1);
    });

    it("may repeat prepare but never delivers an unrecorded result", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      callbacks.losePrepareResponses = 1;
      const harness = await createHarness({ callbacks, clock, retryDelayMs: 5 });
      await harness.engine.accept(await fanout({ branches: [plan()] }));

      await expect(harness.runDue()).resolves.toMatchObject({ failed: 1 });
      expect(callbacks.prepareCalls).toHaveLength(1);
      expect(callbacks.deliveryCalls).toHaveLength(0);
      clock.advance(5);
      await expect(harness.runDue()).resolves.toMatchObject({ delivered: 1 });
      expect(callbacks.prepareCalls).toHaveLength(2);
      expect(callbacks.deliveryCalls).toHaveLength(1);
    });

    it.each(["prepare", "deliver"] as const)(
      "retries a transient TypeError from %s",
      async (stage) => {
        const clock = new VirtualMonitorClock();
        const callbacks = new ControlledAttentionCallbacks(clock);
        callbacks[`${stage}Errors`].push(new TypeError("simulated fetch failure"));
        const harness = await createHarness({
          callbacks,
          clock,
          maxAttempts: 2,
          retryDelayMs: 5,
        });
        await harness.engine.accept(await fanout({ branches: [plan()] }));

        await expect(harness.runDue()).resolves.toMatchObject({ failed: 1 });
        expect((await harness.diagnostics()).activeBatchPayloads).toBe(1);
        clock.advance(5);
        await expect(harness.runDue()).resolves.toMatchObject({ delivered: 1 });
      },
    );

    it("makes a malformed callback output terminal", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      callbacks.outcome = { kind: "invalid" } as unknown as PreparedAttentionOutcome;
      const harness = await createHarness({ callbacks, clock, maxAttempts: 2 });
      await harness.engine.accept(await fanout({ branches: [plan()] }));

      await expect(harness.runDue()).resolves.toMatchObject({ terminalFailures: 1 });
      expect(await harness.diagnostics()).toMatchObject({
        activeBatchPayloads: 0,
        terminalFailures: 1,
      });
    });

    it("retries exact prepared bytes after a lost delivery response", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      callbacks.loseDeliveryResponses = 1;
      const harness = await createHarness({ callbacks, clock, retryDelayMs: 5 });
      await harness.engine.accept(await fanout({ branches: [plan()] }));

      await expect(harness.runDue()).resolves.toMatchObject({ failed: 1 });
      callbacks.outcome = {
        kind: "wake",
        decision: { answer: "changed" },
        routeId: "changed-route",
        instruction: "changed instruction",
        evidence: { changed: true },
      };
      clock.advance(5);
      await expect(harness.runDue()).resolves.toMatchObject({ delivered: 1 });

      expect(callbacks.prepareCalls).toHaveLength(1);
      expect(callbacks.deliveryCalls).toHaveLength(2);
      expect(callbacks.deliveryCalls[1]).toEqual(callbacks.deliveryCalls[0]);
      expect(callbacks.deliveryInputsFrozen).toEqual([true, true]);
      expect(callbacks.effects.size).toBe(1);
    });

    it("deletes complete source payloads after terminal delivery", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      const harness = await createHarness({ callbacks, clock });
      await harness.engine.accept(await fanout({ branches: [plan()] }));
      await harness.runDue();

      expect(await harness.diagnostics()).toMatchObject({
        pendingFanoutPayloads: 0,
        bufferedBranchPayloads: 0,
        activeBatchPayloads: 0,
        preparedWakePayloads: 0,
        acceptanceReceipts: 1,
        branchReceipts: 1,
        deliveryReceipts: 1,
      });
    });

    it("makes an ignore terminal without calling delivery", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      callbacks.outcome = { kind: "ignore", decision: { answer: "ignore" } };
      const harness = await createHarness({ callbacks, clock });
      await harness.engine.accept(await fanout({ branches: [plan()] }));

      await expect(harness.runDue()).resolves.toMatchObject({ ignored: 1 });
      expect(callbacks.deliveryCalls).toHaveLength(0);
      expect(await harness.diagnostics()).toMatchObject({
        bufferedBranchPayloads: 0,
        activeBatchPayloads: 0,
        preparedWakePayloads: 0,
      });
    });

    it("applies a bounded terminal policy to repeated callback failures", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      callbacks.losePrepareResponses = 2;
      const harness = await createHarness({
        callbacks,
        clock,
        maxAttempts: 2,
        retryDelayMs: 5,
      });
      await harness.engine.accept(await fanout({ branches: [plan()] }));

      await expect(harness.runDue()).resolves.toMatchObject({ failed: 1 });
      clock.advance(5);
      await expect(harness.runDue()).resolves.toMatchObject({ terminalFailures: 1 });
      expect(callbacks.deliveryCalls).toHaveLength(0);
      expect(await harness.diagnostics()).toMatchObject({
        activeBatchPayloads: 0,
        preparedWakePayloads: 0,
        terminalFailures: 1,
      });
    });

    it("fails an oversized prepared wake before delivery and deletes its payload", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      const harness = await createHarness({
        callbacks,
        clock,
        maxPreparedWakeBytes: 1,
      });
      await harness.engine.accept(await fanout({ branches: [plan()] }));

      await expect(harness.runDue()).resolves.toMatchObject({ terminalFailures: 1 });
      expect(callbacks.deliveryCalls).toHaveLength(0);
      expect(await harness.diagnostics()).toMatchObject({
        activeBatchPayloads: 0,
        preparedWakePayloads: 0,
        terminalFailures: 1,
      });
    });

    it("makes shadow wakes terminal without calling delivery", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      const harness = await createHarness({ callbacks, clock });
      await harness.engine.accept(
        await fanout({ branches: [plan({ mode: "shadow" })] }),
      );

      await expect(harness.runDue()).resolves.toMatchObject({ shadowed: 1 });
      expect(callbacks.deliveryCalls).toHaveLength(0);
      expect(await harness.diagnostics()).toMatchObject({
        bufferedBranchPayloads: 0,
        activeBatchPayloads: 0,
      });
    });

    it("buffers during cooldown and freezes the accumulated successor at expiry", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      const harness = await createHarness({ callbacks, clock });
      const policy = {
        buffer: { mode: "immediate" as const },
        cooldownAfterWakeMs: 100,
      };
      await harness.engine.accept(
        await fanout({ eventId: "event-1", branches: [plan({ policy })] }),
      );
      await harness.runDue();
      await harness.engine.accept(
        await fanout({ eventId: "event-2", branches: [plan({ policy })] }),
      );

      await expect(harness.runDue()).resolves.toMatchObject({ claimed: 0 });
      clock.advance(100);
      await expect(harness.runDue()).resolves.toMatchObject({ delivered: 1 });
      expect(callbacks.prepareCalls[1]!.branches.map((branch) => branch.event.id)).toEqual([
        "event-2",
      ]);
    });

    it("starts cooldown when preparation and delivery commit, not when the batch is claimed", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      callbacks.advanceOnPrepareMs = 50;
      const harness = await createHarness({ callbacks, clock });
      const policy = {
        buffer: { mode: "immediate" as const },
        cooldownAfterWakeMs: 100,
      };
      await harness.engine.accept(
        await fanout({ eventId: "event-1", branches: [plan({ policy })] }),
      );
      await harness.runDue();
      await harness.engine.accept(
        await fanout({ eventId: "event-2", branches: [plan({ policy })] }),
      );

      clock.advance(99);
      await expect(harness.runDue()).resolves.toMatchObject({ claimed: 0 });
      clock.advance(1);
      await expect(harness.runDue()).resolves.toMatchObject({ delivered: 1 });
    });

    it("expires payload-free receipts independently of terminal payload deletion", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      const harness = await createHarness({ callbacks, clock, dedupeMs: 20 });
      await harness.engine.accept(await fanout({ branches: [plan()] }));
      await harness.runDue();
      expect((await harness.diagnostics()).acceptanceReceipts).toBe(1);

      clock.advance(20);
      expect(await harness.diagnostics()).toMatchObject({
        eventCoordinators: 0,
        correlationWorkflows: 0,
        branchReceipts: 0,
        deliveryReceipts: 0,
      });
    });

    it("starts branch receipt retention after terminal workflow completion", async () => {
      const clock = new VirtualMonitorClock();
      const callbacks = new ControlledAttentionCallbacks(clock);
      callbacks.advanceOnPrepareMs = 50;
      const harness = await createHarness({ callbacks, clock, dedupeMs: 20 });
      await harness.engine.accept(await fanout({ branches: [plan()] }));

      await expect(harness.runDue()).resolves.toMatchObject({ delivered: 1 });
      expect(await harness.diagnostics()).toMatchObject({
        acceptanceReceipts: 0,
        branchReceipts: 1,
        deliveryReceipts: 1,
      });

      clock.advance(20);
      expect(await harness.diagnostics()).toMatchObject({
        correlationWorkflows: 0,
        branchReceipts: 0,
        deliveryReceipts: 0,
      });
    });
  });
}

class ControlledAttentionCallbacks implements AttentionCallbacks {
  outcome: PreparedAttentionOutcome = {
    kind: "wake",
    decision: { answer: "wake" },
    routeId: "eve-session",
    instruction: "Investigate the event.",
    evidence: { summary: "complete evidence" },
  };
  losePrepareResponses = 0;
  loseDeliveryResponses = 0;
  advanceOnPrepareMs = 0;
  readonly prepareErrors: Error[] = [];
  readonly deliverErrors: Error[] = [];
  readonly prepareCalls: FrozenAttentionBatch[] = [];
  readonly deliveryCalls: PreparedAttentionWake[] = [];
  readonly prepareInputsFrozen: boolean[] = [];
  readonly deliveryInputsFrozen: boolean[] = [];
  readonly effects = new Map<string, AttentionDeliveryReceipt>();
  readonly #clock: VirtualMonitorClock;
  #prepareGate?: Promise<void> | undefined;
  #releasePrepare?: (() => void) | undefined;
  #prepareStarted?: (() => void) | undefined;
  readonly #prepareWaiters: Array<{ count: number; resolve: () => void }> = [];
  #deliveryGate?: Promise<void> | undefined;
  #releaseDelivery?: (() => void) | undefined;
  readonly #deliveryWaiters: Array<{ count: number; resolve: () => void }> = [];

  constructor(clock: VirtualMonitorClock) {
    this.#clock = clock;
  }

  holdPrepare(): Promise<void> {
    this.#prepareGate = new Promise<void>((resolve) => {
      this.#releasePrepare = resolve;
    });
    return new Promise<void>((resolve) => {
      this.#prepareStarted = resolve;
    });
  }

  releasePrepare(): void {
    this.#releasePrepare?.();
    this.#releasePrepare = undefined;
    this.#prepareGate = undefined;
  }

  waitForPrepareCalls(count: number): Promise<void> {
    if (this.prepareCalls.length >= count) return Promise.resolve();
    return new Promise<void>((resolve) => this.#prepareWaiters.push({ count, resolve }));
  }

  holdDelivery(): void {
    this.#deliveryGate = new Promise<void>((resolve) => {
      this.#releaseDelivery = resolve;
    });
  }

  releaseDelivery(): void {
    this.#releaseDelivery?.();
    this.#releaseDelivery = undefined;
    this.#deliveryGate = undefined;
  }

  waitForDeliveryCalls(count: number): Promise<void> {
    if (this.deliveryCalls.length >= count) return Promise.resolve();
    return new Promise<void>((resolve) => this.#deliveryWaiters.push({ count, resolve }));
  }

  async prepare(batch: FrozenAttentionBatch): Promise<PreparedAttentionOutcome> {
    this.prepareInputsFrozen.push(
      Object.isFrozen(batch) &&
        Object.isFrozen(batch.branches) &&
        batch.branches.every((branch) => Object.isFrozen(branch.event)),
    );
    this.prepareCalls.push(structuredClone(batch));
    for (const waiter of [...this.#prepareWaiters]) {
      if (this.prepareCalls.length < waiter.count) continue;
      this.#prepareWaiters.splice(this.#prepareWaiters.indexOf(waiter), 1);
      waiter.resolve();
    }
    this.#prepareStarted?.();
    this.#prepareStarted = undefined;
    if (this.#prepareGate !== undefined) await this.#prepareGate;
    const error = this.prepareErrors.shift();
    if (error !== undefined) throw error;
    const outcome = structuredClone(this.outcome);
    if (this.advanceOnPrepareMs > 0) this.#clock.advance(this.advanceOnPrepareMs);
    if (this.losePrepareResponses > 0) {
      this.losePrepareResponses -= 1;
      throw new Error("prepare response lost");
    }
    return outcome;
  }

  async deliver(wake: PreparedAttentionWake): Promise<AttentionDeliveryReceipt> {
    this.deliveryInputsFrozen.push(Object.isFrozen(wake) && Object.isFrozen(wake.evidence));
    this.deliveryCalls.push(structuredClone(wake));
    for (const waiter of [...this.#deliveryWaiters]) {
      if (this.deliveryCalls.length < waiter.count) continue;
      this.#deliveryWaiters.splice(this.#deliveryWaiters.indexOf(waiter), 1);
      waiter.resolve();
    }
    if (this.#deliveryGate !== undefined) await this.#deliveryGate;
    const error = this.deliverErrors.shift();
    if (error !== undefined) throw error;
    const prior = this.effects.get(wake.wakeKey);
    if (prior !== undefined && prior.inputHash !== wake.inputHash) {
      throw new IdempotencyConflictError({
        namespace: "conformance-delivery",
        key: wake.wakeKey,
        existingInputHash: prior.inputHash,
        receivedInputHash: wake.inputHash,
      });
    }
    const receipt =
      prior ??
      ({
        wakeKey: wake.wakeKey,
        inputHash: wake.inputHash,
        deliveredAt: this.#clock.now().toISOString(),
        result: { effect: `effect-${this.effects.size + 1}` },
      } satisfies AttentionDeliveryReceipt);
    this.effects.set(wake.wakeKey, receipt);
    if (this.loseDeliveryResponses > 0) {
      this.loseDeliveryResponses -= 1;
      throw new Error("delivery response lost");
    }
    return structuredClone(receipt);
  }
}

async function fanout(options: {
  readonly eventId?: string | undefined;
  readonly body?: string | undefined;
  readonly branches: readonly AttentionBranchPlan[];
}): Promise<AcceptedFanout> {
  const source = await canonicalizeChannelDelivery(
    defineChannelCanonicalization<
      { readonly eventId: string; readonly body: string },
      ReturnType<typeof canonicalEvent>
    >({
      version: 1,
      canonicalize: (raw) => canonicalEvent(raw.eventId, raw.body),
    }),
    {
      eventId: options.eventId ?? "event-1",
      body: options.body ?? "original",
    },
    { applicationId: "engineering-agent" },
  );
  return compileAcceptedFanout({ source, branches: options.branches });
}

function canonicalEvent(eventId: string, body: string) {
  return {
    id: eventId,
    type: "channel.message",
    version: 1,
    occurredAt: "2026-01-01T00:00:00.000Z",
    data: { body },
    source: {
      channelId: "slack",
      installationId: "workspace-1",
      tenantId: "tenant-1",
    },
    origin: { kind: "external" as const, depth: 0 },
  };
}

function plan(overrides: Partial<AttentionBranchPlan> = {}): AttentionBranchPlan {
  return {
    monitorId: "monitor",
    definitionVersion: "definition-v1",
    correlationKey: "incident-1",
    orderKey: "001",
    mode: "active",
    policy: { buffer: { mode: "immediate" } },
    ...overrides,
  };
}

function debouncePolicy() {
  return {
    buffer: {
      mode: "debounce" as const,
      quietPeriodMs: 10,
      maxWaitMs: 100,
      maxEvents: 10,
      maxBytes: 100_000,
    },
  };
}
