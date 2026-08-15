import { world } from "@ewhauser/eve-ambient/world";
import { defineSupportApplication } from "./application.js";

export interface SupportWorldOptions {
  readonly callbackUrl: string;
  readonly callbackSecretEnv?: string | undefined;
  readonly deliver: Parameters<typeof defineSupportApplication>[0]["deliver"];
}

/** Ambient uses whichever process-global World the Workflow host installed. */
export function createSupportWorldApplication(options: SupportWorldOptions) {
  return defineSupportApplication({ deliver: options.deliver }).with(
    world({
      engineId: "support-agent",
      callbackUrl: options.callbackUrl,
      ...(options.callbackSecretEnv === undefined
        ? {}
        : { callbackSecretEnv: options.callbackSecretEnv }),
    }),
  );
}
