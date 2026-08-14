export type JsonPrimitive = null | boolean | number | string;
export type JsonValue =
  | JsonPrimitive
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue };

export interface SubjectRef {
  readonly namespace: string;
  readonly key: string;
}

export interface ChannelEventActor {
  readonly id: string;
  readonly principalType: "app" | "service" | "unknown" | "user";
  readonly displayName?: string | undefined;
  readonly isBot?: boolean | undefined;
  readonly knownAgentPrincipal?: boolean | undefined;
}

export interface ChannelEventOrigin {
  readonly kind: "agent" | "external" | "monitor" | "schedule";
  readonly depth: number;
  readonly applicationId?: string | undefined;
  readonly causationId?: string | undefined;
  readonly id?: string | undefined;
}

export type MonitorPhase = "observed" | "undispatched";

export type MonitorBatchClosedBy =
  | "cooldown-expired"
  | "immediate"
  | "max-bytes"
  | "max-events"
  | "max-wait"
  | "quiet-period";

export interface MonitorClock {
  now(): Date;
}
