import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { basename, join, resolve } from "node:path";

const packagePath = ".";
const packageName = "@ewhauser/eve-ambient";
const repositoryUrl = "git+https://github.com/ewhauser/eve-ambient.git";
const requiredFiles = [
  "dist/index.d.ts",
  "dist/index.js",
  "dist/ai-sdk.d.ts",
  "dist/ai-sdk.js",
  "dist/memory.d.ts",
  "dist/memory.js",
  "dist/postgres.d.ts",
  "dist/postgres.js",
  "dist/testing.d.ts",
  "dist/testing.js",
  "migrations/001_eve_ambient.sql",
  "LICENSE",
  "README.md",
];

function readManifest() {
  return JSON.parse(readFileSync("package.json", "utf8"));
}

function validateManifest() {
  const manifest = readManifest();
  const failures = [];

  if (manifest.name !== packageName) {
    failures.push(`expected name ${packageName}, found ${manifest.name}`);
  }
  if (manifest.private === true) {
    failures.push("publishable package is marked private");
  }
  if (manifest.publishConfig?.access !== "public") {
    failures.push("publishConfig.access must be public");
  }
  if (manifest.repository?.url !== repositoryUrl) {
    failures.push(`repository.url must be ${repositoryUrl}`);
  }
  if (manifest.repository?.directory !== undefined) {
    failures.push("repository.directory must be omitted for the root package");
  }
  if (!/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/.test(manifest.version)) {
    failures.push(`version is not publishable SemVer: ${manifest.version}`);
  }

  if (failures.length > 0) {
    throw new Error(`package.json:\n- ${failures.join("\n- ")}`);
  }

  return manifest;
}

function parsePackOutput(output) {
  const result = JSON.parse(output);
  if (!Array.isArray(result) || result.length !== 1) {
    throw new Error("npm pack returned an unexpected result");
  }
  return result[0];
}

function validateContents(packResult) {
  const paths = new Set(packResult.files.map((file) => file.path));
  const missing = requiredFiles.filter((file) => !paths.has(file));
  const forbidden = [...paths].filter(
    (file) =>
      file === ".env" ||
      file.startsWith(".env.") ||
      file.startsWith("coverage/") ||
      file.startsWith("node_modules/") ||
      file.startsWith("src/") ||
      file.startsWith("test/"),
  );

  if (missing.length > 0 || forbidden.length > 0) {
    const details = [];
    if (missing.length > 0) details.push(`missing: ${missing.join(", ")}`);
    if (forbidden.length > 0) details.push(`forbidden: ${forbidden.join(", ")}`);
    throw new Error(`invalid package contents (${details.join("; ")})`);
  }
}

export function checkPackage() {
  const manifest = validateManifest();
  const output = execFileSync(
    "npm",
    ["pack", "--json", "--dry-run", "--ignore-scripts"],
    { cwd: packagePath, encoding: "utf8" },
  );
  const packResult = parsePackOutput(output);
  validateContents(packResult);
  return { manifest, packResult };
}

export function packPackage(destination) {
  const manifest = validateManifest();
  const output = execFileSync(
    "npm",
    ["pack", "--json", "--ignore-scripts", "--pack-destination", resolve(destination)],
    { cwd: packagePath, encoding: "utf8" },
  );
  const packResult = parsePackOutput(output);
  validateContents(packResult);

  const tarball = join(destination, basename(packResult.filename));
  const digest = createHash("sha256").update(readFileSync(tarball)).digest("hex");
  writeFileSync(`${tarball}.sha256`, `${digest}  ${basename(tarball)}\n`, {
    encoding: "utf8",
    flag: "wx",
  });

  return { manifest, packResult, tarball };
}

export function createArtifactDirectory(destination) {
  mkdirSync(destination, { recursive: false });
}
