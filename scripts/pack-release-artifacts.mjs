import { mkdirSync } from "node:fs";
import { resolve } from "node:path";

import { packPackage } from "./package-artifacts.mjs";

const releasePackages = new Map([
  [".", "packages/ambient"],
  ["packages/eve-adapter", "packages/eve-adapter"],
]);

const releasePaths = JSON.parse(process.env.RELEASE_PATHS ?? "[]");
if (!Array.isArray(releasePaths) || releasePaths.length === 0) {
  throw new Error("RELEASE_PATHS must be a non-empty JSON array");
}

const uniquePaths = [...new Set(releasePaths)];
for (const packagePath of uniquePaths) {
  if (!releasePackages.has(packagePath)) {
    throw new Error(`refusing unknown release path: ${packagePath}`);
  }
}

const destination = resolve("release-artifacts");
mkdirSync(destination, { recursive: false });

for (const releasePath of uniquePaths.sort()) {
  const packagePath = releasePackages.get(releasePath);
  if (packagePath === undefined) throw new Error(`missing package for ${releasePath}`);
  const { manifest, tarball } = packPackage(packagePath, destination);
  console.log(`packed ${manifest.name}@${manifest.version} at ${tarball}`);
}
