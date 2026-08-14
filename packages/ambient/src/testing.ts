import { canonicalizeChannelDelivery } from "./idempotency.js";
import type {
  AcceptedChannelEvent,
  CanonicalChannelEvent,
  ChannelCanonicalizationContract,
  EventKey,
  IdempotentEnvelope,
} from "./idempotency.js";
import type { MonitorClock } from "./types.js";

export async function assertChannelCanonicalization<
  TRaw,
  TEvent extends CanonicalChannelEvent,
>(
  contract: ChannelCanonicalizationContract<TRaw, TEvent>,
  options: {
    readonly applicationId: string;
    readonly original: TRaw;
    readonly equivalentRetries: readonly [TRaw, ...TRaw[]];
    readonly conflictingRetries: readonly [TRaw, ...TRaw[]];
  },
): Promise<IdempotentEnvelope<AcceptedChannelEvent<TEvent>, EventKey>> {
  const baseline = await canonicalizeChannelDelivery(contract, options.original, {
    applicationId: options.applicationId,
  });
  for (const [index, retry] of options.equivalentRetries.entries()) {
    const result = await canonicalizeChannelDelivery(contract, retry, {
      applicationId: options.applicationId,
    });
    if (result.idempotency.key !== baseline.idempotency.key) {
      throw new Error(`equivalent retry ${index} changed the event key`);
    }
    if (result.idempotency.inputHash !== baseline.idempotency.inputHash) {
      throw new Error(`equivalent retry ${index} changed the input hash`);
    }
  }
  for (const [index, retry] of options.conflictingRetries.entries()) {
    const result = await canonicalizeChannelDelivery(contract, retry, {
      applicationId: options.applicationId,
    });
    if (result.idempotency.key !== baseline.idempotency.key) {
      throw new Error(`conflicting retry ${index} changed the event key instead of conflicting`);
    }
    if (result.idempotency.inputHash === baseline.idempotency.inputHash) {
      throw new Error(`conflicting retry ${index} did not change the input hash`);
    }
  }
  return baseline;
}

export class VirtualMonitorClock implements MonitorClock {
  #milliseconds: number;

  constructor(initial: string | Date = "2026-01-01T00:00:00.000Z") {
    this.#milliseconds = new Date(initial).getTime();
    if (!Number.isFinite(this.#milliseconds)) throw new TypeError("invalid initial clock time");
  }

  now(): Date {
    return new Date(this.#milliseconds);
  }

  advance(milliseconds: number): void {
    if (!Number.isFinite(milliseconds) || milliseconds < 0) {
      throw new TypeError("clock advance must be a non-negative finite number");
    }
    this.#milliseconds += milliseconds;
  }

  set(value: string | Date): void {
    const next = new Date(value).getTime();
    if (!Number.isFinite(next) || next < this.#milliseconds) {
      throw new TypeError("virtual clock cannot move backwards");
    }
    this.#milliseconds = next;
  }
}
