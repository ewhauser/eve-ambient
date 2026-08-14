import {
  createAmbientPublisher,
  createAttentionCallbacks,
  type AmbientPublisher,
  type PreparedAttentionWake,
} from "@ewhauser/eve-ambient";
import {
  CelldAttentionEngine,
  createAttentionCallbackFetchHandler,
} from "@ewhauser/eve-ambient/celld";
import {
  createEveAttentionRoute,
  type EveChannelAuth,
} from "@ewhauser/eve-ambient-eve";
import type { ChannelFrom } from "eve/channels";

import type { PullRequestEvent } from "./channels/github.js";
import { blockedPullRequestRule } from "./rules/blocked-pull-request.js";

export interface EveCelldApplicationOptions {
  readonly applicationId: string;
  readonly celld: {
    readonly url: string;
    readonly secret: string;
    readonly fetch?: typeof fetch | undefined;
  };
  readonly eve: {
    readonly auth: EveChannelAuth;
    readonly from: ChannelFrom;
  };
}

export interface EveCelldApplication {
  readonly engine: CelldAttentionEngine;
  readonly publisher: AmbientPublisher<PullRequestEvent>;
  readonly handleCallbacks: (request: Request) => Promise<Response>;
}

/** celld owns the entire durable workflow; no PostgreSQL pool is accepted. */
export function createEveCelldApplication(
  options: EveCelldApplicationOptions,
): EveCelldApplication {
  const route = createEveAttentionRoute({
    id: "eve",
    auth: options.eve.auth,
    from: options.eve.from,
    address: evidenceAddress,
  });
  const callbacks = createAttentionCallbacks({
    rules: [blockedPullRequestRule],
    routes: [route],
  });
  const engine = new CelldAttentionEngine(options.celld);
  return {
    engine,
    publisher: createAmbientPublisher({
      applicationId: options.applicationId,
      engine,
      rules: [blockedPullRequestRule],
    }),
    handleCallbacks: createAttentionCallbackFetchHandler(callbacks, {
      secret: options.celld.secret,
    }),
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
  if (address === undefined) throw new Error("prepared pull-request wake has no Eve address");
  return address;
}
