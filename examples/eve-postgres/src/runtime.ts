import {
  postgres,
  type PostgresPool,
} from "@ewhauser/eve-ambient/postgres";
import type { EveChannelAuth } from "@ewhauser/eve-ambient-eve";
import type { ChannelFrom } from "eve/channels";

import { defineEngineeringApplication } from "./application.js";

export interface EvePostgresApplicationOptions {
  readonly applicationId: string;
  readonly pool: PostgresPool;
  readonly eve: {
    readonly auth: EveChannelAuth;
    readonly from: ChannelFrom;
  };
}

/** Supported default: PostgreSQL privately owns the complete active workflow. */
export async function createEvePostgresApplication(
  options: EvePostgresApplicationOptions,
) {
  const application = defineEngineeringApplication(options).with(
    postgres({
      pool: options.pool,
      engineId: options.applicationId,
    }),
  );
  await application.engine.initialize();
  return application;
}
