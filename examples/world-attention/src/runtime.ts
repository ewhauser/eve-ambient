import { world } from "@ewhauser/eve-ambient/world";
import type { AttentionWorld } from "@ewhauser/eve-ambient/protocol";
import { defineSupportApplication } from "./application.js";

export interface SupportWorldOptions {
  readonly world: AttentionWorld;
  readonly callbackSecretEnv?: string | undefined;
  readonly deliver: Parameters<typeof defineSupportApplication>[0]["deliver"];
}

/** Ambient sends each correlation directly to the supplied World client. */
export function createSupportWorldApplication(options: SupportWorldOptions) {
  return defineSupportApplication({ deliver: options.deliver }).with(
    world({
      world: options.world,
      ...(options.callbackSecretEnv === undefined
        ? {}
        : { callbackSecretEnv: options.callbackSecretEnv }),
    }),
  );
}
