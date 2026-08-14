import {
  compileMonitor,
  defineMonitor,
  ignore,
  wake,
  type MonitorDeliveryChannel,
} from "@ewhauser/eve-ambient";
import type { EveDeliveryTarget } from "@ewhauser/eve-ambient-eve";

import {
  githubChannel,
  type PullRequestChangedEvent,
} from "../channels/github.js";

type BlockerMetadata = Readonly<{ blockers: readonly string[] }>;

function blockers(event: Readonly<PullRequestChangedEvent>): string[] {
  const values = new Set<string>();
  if (event.data.mergeState === "conflicting") values.add("merge-conflict");
  if (event.data.reviewDecision === "changes-requested") {
    values.add("changes-requested");
  }
  for (const check of event.data.failingChecks) values.add(`check:${check}`);
  return [...values].sort();
}

/** A high-cardinality rule whose complete batches live in celld after append. */
export function blockedPullRequestRule(
  delivery: MonitorDeliveryChannel<EveDeliveryTarget>,
) {
  const definition = defineMonitor<
    PullRequestChangedEvent,
    BlockerMetadata,
    BlockerMetadata
  >({
    id: "blocked-pull-request",
    mode: "active",
    sources: [githubChannel.event("pull-request-changed")],
    correlate: ({ event }) =>
      JSON.stringify([
        event.source.installationId,
        event.data.repository,
        String(event.data.number),
      ]),
    buffer: {
      mode: "debounce",
      quietPeriod: "10s",
      maxWait: "2m",
      maxEvents: 100,
      maxBytes: 512_000,
    },
    decision: ({ events }) => {
      const latest = events.at(-1);
      if (latest === undefined || latest.data.state === "closed") {
        return ignore({ reason: "pull-request-not-open", metadata: { blockers: [] } });
      }
      const current = blockers(latest);
      return current.length === 0
        ? ignore({ reason: "pull-request-unblocked", metadata: { blockers: current } })
        : wake({ reason: "pull-request-blocked", metadata: { blockers: current } });
    },
    cooldown: { afterWake: "15m", during: "accumulate" },
    task: {
      instructions:
        "Review the pull request blockers, determine which intervention is useful, and avoid repeating work already reflected in the evidence.",
      evidence: ({ events, decision, batch }) => ({
        latest: events.at(-1)?.data ?? null,
        observedChanges: events.map((event) => ({
          ref: event.ref,
          sourceEventId: event.id,
          updatedAt: event.data.updatedAt,
        })),
        blockers: decision.metadata?.blockers ?? [],
        completeness: batch,
      }),
    },
    route: ({ events }) => {
      const target = events.at(-1)?.replyTarget;
      return target === undefined
        ? null
        : { auth: "app", channel: delivery, target };
    },
    session: { strategy: "correlation", idleTimeout: "7d" },
    limits: {
      perMonitor: { maxEventsPerMinute: 20_000, maxWakesPerHour: 500 },
      perKey: { maxWakesPerHour: 2 },
      overflow: "buffer",
    },
    retention: { decisions: "30d", dedupe: "7d" },
    metadata: {
      owner: "developer-productivity",
      useCase: "ambient-github-pull-requests",
    },
  });

  return compileMonitor(definition, "example:blocked-pull-request:v1");
}
