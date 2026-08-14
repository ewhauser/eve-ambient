import {
  createAmbientPublisher,
  createAttentionCallbacks,
  type AmbientPublisher,
  type PreparedAttentionWake,
} from "@ewhauser/eve-ambient";
import {
  PostgresAttentionEngine,
  type PostgresPool,
} from "@ewhauser/eve-ambient/postgres";
import {
  createEveAttentionRoute,
  type EveChannelAuth,
} from "@ewhauser/eve-ambient-eve";
import type { ChannelFrom } from "eve/channels";

import type { SlackMessageEvent } from "./channels/slack.js";
import { incidentEscalationRule } from "./rules/incident-escalation.js";

export interface EvePostgresApplicationOptions {
  readonly applicationId: string;
  readonly pool: PostgresPool;
  readonly eve: {
    readonly auth: EveChannelAuth;
    readonly from: ChannelFrom;
  };
}

export interface EvePostgresApplication {
  readonly engine: PostgresAttentionEngine;
  readonly publisher: AmbientPublisher<SlackMessageEvent>;
  runOnce(): ReturnType<PostgresAttentionEngine["runOnce"]>;
}

/** Supported default: PostgreSQL privately owns the complete active workflow. */
export async function createEvePostgresApplication(
  options: EvePostgresApplicationOptions,
): Promise<EvePostgresApplication> {
  const route = createEveAttentionRoute({
    id: "eve",
    auth: options.eve.auth,
    from: options.eve.from,
    address: evidenceAddress,
  });
  const callbacks = createAttentionCallbacks({
    rules: [incidentEscalationRule],
    routes: [route],
  });
  const engine = new PostgresAttentionEngine({
    pool: options.pool,
    callbacks,
    engineId: options.applicationId,
  });
  await engine.initialize();
  return {
    engine,
    publisher: createAmbientPublisher({
      applicationId: options.applicationId,
      engine,
      rules: [incidentEscalationRule],
    }),
    runOnce: () => engine.runOnce(),
  };
}

function evidenceAddress(wake: PreparedAttentionWake): string {
  const evidence =
    wake.evidence !== null &&
    typeof wake.evidence === "object" &&
    !Array.isArray(wake.evidence)
      ? (wake.evidence as { readonly address?: unknown })
      : undefined;
  const address =
    typeof evidence?.address === "string"
      ? evidence.address
      : undefined;
  if (address === undefined) throw new Error("prepared incident wake has no Eve address");
  return address;
}
