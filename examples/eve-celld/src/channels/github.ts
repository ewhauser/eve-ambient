import {
  createEveGitHubAmbientChannel,
  type EveGitHubAmbientChannelOptions,
} from "@ewhauser/eve-ambient-eve";

/** The normal Eve GitHub channel, with PR and check-suite events admitted to Ambient. */
export function createEngineeringGitHubChannel(options: EveGitHubAmbientChannelOptions) {
  return createEveGitHubAmbientChannel(options);
}
