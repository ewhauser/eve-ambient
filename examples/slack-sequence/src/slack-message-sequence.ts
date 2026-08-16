import {
  debounce,
  defineAmbientApplication,
  defineAmbientRule,
  defineChannelCanonicalization,
  ignore,
  wake,
  type CanonicalChannelEvent,
  type JsonValue,
} from "@ewhauser/eve-ambient";
import { memory } from "@ewhauser/eve-ambient/memory";
import {
  workflow,
  type WorkflowAmbientOptions,
} from "@ewhauser/eve-ambient/workflow";

export interface SlackMessageInput {
  readonly eventId: string;
  readonly occurredAt: string;
  readonly tenantId: string;
  readonly workspaceId: string;
  readonly channelId: string;
  readonly userId: string;
  readonly userName?: string | undefined;
  readonly text: string;
}

export type SlackMessageEvent = CanonicalChannelEvent<
  "slack.message",
  {
    readonly workspaceId: string;
    readonly channelId: string;
    readonly text: string;
  },
  string
>;

/** Converts a verified Slack delivery into Ambient's complete canonical event. */
export const slackMessages = defineChannelCanonicalization<
  SlackMessageInput,
  SlackMessageEvent
>({
  version: 1,
  partitionKey: (event) => event.data.channelId,
  canonicalize(input) {
    return {
      id: input.eventId,
      type: "slack.message",
      version: 1,
      occurredAt: input.occurredAt,
      data: {
        workspaceId: input.workspaceId,
        channelId: input.channelId,
        text: input.text,
      },
      source: {
        channelId: "slack",
        installationId: input.workspaceId,
        tenantId: input.tenantId,
      },
      actor: {
        id: input.userId,
        principalType: "user",
        ...(input.userName === undefined ? {} : { displayName: input.userName }),
      },
      replyTarget: `slack:${input.workspaceId}:${input.channelId}`,
      subjects: [{ namespace: "slack-channel", key: input.channelId }],
      origin: { kind: "external", depth: 0 },
    };
  },
});

const normalizedText = (event: SlackMessageEvent) => event.data.text.trim().toLowerCase();

/** Detects message A followed by message B inside one channel's bounded batch. */
export const messageSequenceRule = defineAmbientRule({
  id: "message-a-then-b",
  version: "v1",
  channel: slackMessages,
  matches: (event) => ["message a", "message b"].includes(normalizedText(event)),
  policy: debounce({
    quiet: "2m",
    maxWait: "10m",
    cooldown: "30m",
    maxEvents: 48,
  }),
  decide({ events, eventKeys }) {
    const firstA = events.findIndex((event) => normalizedText(event) === "message a");
    const followingB = events.findIndex(
      (event, index) => index > firstA && normalizedText(event) === "message b",
    );
    if (firstA < 0 || followingB < 0) {
      return ignore({ reason: "the batch does not contain message A followed by message B" });
    }

    const trigger = events[followingB]!;
    return wake({
      routeId: "turns",
      target: trigger.replyTarget!,
      instruction: "Review the Slack conversation and take the configured follow-up action.",
      decision: { reason: "message A was followed by message B" },
      evidence: {
        channelId: trigger.data.channelId,
        matchedEventKeys: [eventKeys[firstA]!, eventKeys[followingB]!],
      },
    });
  },
});

export interface TurnRequest {
  readonly idempotencyKey: string;
  readonly address: string;
  readonly instruction: string;
  readonly evidence: JsonValue;
}

/** Application-owned boundary to Eve or another durable turn queue. */
export interface TurnSink {
  enqueue(request: TurnRequest): Promise<JsonValue>;
}

function turnAddress(target: JsonValue): string {
  if (typeof target !== "string") {
    throw new TypeError("the Slack turn target must be a string");
  }
  return target;
}

export function defineSlackSequenceApplication(turns: TurnSink) {
  return defineAmbientApplication({
    applicationId: "slack-sequence-agent",
    rules: [messageSequenceRule],
    routes: [{
      id: "turns",
      deliver: (prepared) => turns.enqueue({
        idempotencyKey: prepared.wakeKey,
        address: turnAddress(prepared.target),
        instruction: prepared.instruction,
        evidence: prepared.evidence,
      }),
    }],
  });
}

export function createLocalSlackSequenceApplication(turns: TurnSink) {
  return defineSlackSequenceApplication(turns).with(memory());
}

export function createWorkflowSlackSequenceApplication(
  turns: TurnSink,
  options: WorkflowAmbientOptions,
) {
  return defineSlackSequenceApplication(turns).with(workflow(options));
}
