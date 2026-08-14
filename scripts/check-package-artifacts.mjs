import { checkPackage, packages } from "./package-artifacts.mjs";

for (const packagePath of packages.keys()) {
  const { manifest, packResult } = checkPackage(packagePath);
  console.log(
    `checked ${manifest.name}@${manifest.version}: ${packResult.files.length} files`,
  );
}
