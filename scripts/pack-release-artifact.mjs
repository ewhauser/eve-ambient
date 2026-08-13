import { resolve } from "node:path";
import { createArtifactDirectory, packPackage } from "./package-artifact.mjs";

const destination = resolve("release-artifacts");
createArtifactDirectory(destination);

const { manifest, tarball } = packPackage(destination);
console.log(`packed ${manifest.name}@${manifest.version} at ${tarball}`);
