import { checkPackage } from "./package-artifact.mjs";

const { manifest, packResult } = checkPackage();
console.log(
  `checked ${manifest.name}@${manifest.version}: ${packResult.entryCount} files, ${packResult.size} bytes packed`,
);
