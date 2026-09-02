import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { basename, join, resolve } from "node:path";

const repositoryUrl = "git+https://github.com/ewhauser/eve-ambient.git";

export const packages = new Map([
  [
    "packages/ambient",
    {
      component: "eve-ambient",
      name: "@ewhauser/eve-ambient",
      requiredFiles: [
        "dist/index.d.ts",
        "dist/index.js",
        "dist/application.d.ts",
        "dist/application.js",
        "dist/memory.d.ts",
        "dist/memory.js",
        "dist/idempotency.d.ts",
        "dist/idempotency.js",
        "dist/protocol.d.ts",
        "dist/protocol.js",
        "dist/stream-protocol.d.ts",
        "dist/stream-protocol.js",
        "dist/testing.d.ts",
        "dist/testing.js",
        "dist/stream-state.d.ts",
        "dist/stream-state.js",
        "dist/workflow.d.ts",
        "dist/workflow.js",
        "dist/workflow-protocol.d.ts",
        "dist/workflow-protocol.js",
        "dist/workflows/index.d.ts",
        "dist/workflows/index.js",
        "dist/workflows/callback-steps.d.ts",
        "dist/workflows/callback-steps.js",
        "dist/workflows/correlation.d.ts",
        "dist/workflows/correlation.js",
        "LICENSE",
        "README.md",
      ],
    },
  ],
  [
    "packages/eve-adapter",
    {
      component: "eve-ambient-eve",
      name: "@ewhauser/eve-ambient-eve",
      requiredFiles: [
        "dist/index.d.ts",
        "dist/index.js",
        "patches/eve@0.49.0-source.patch",
        "patches/eve@0.49.0.patch",
        "LICENSE",
        "README.md",
      ],
    },
  ],
]);

const workspaceManifest = readManifest(".");
const coreManifest = readManifest("packages/ambient");
if (workspaceManifest.private !== true) {
  throw new Error("workspace root must remain private");
}
if (workspaceManifest.version !== coreManifest.version) {
  throw new Error(
    `workspace release version ${String(workspaceManifest.version)} does not match core ${String(coreManifest.version)}`,
  );
}

function readManifest(packagePath) {
  return JSON.parse(readFileSync(join(packagePath, "package.json"), "utf8"));
}

function validateManifest(packagePath, expected) {
  const manifest = readManifest(packagePath);
  const failures = [];

  if (manifest.name !== expected.name) {
    failures.push(`expected name ${expected.name}, found ${manifest.name}`);
  }
  if (manifest.private === true) failures.push("publishable package is marked private");
  if (manifest.publishConfig?.access !== "public") {
    failures.push("publishConfig.access must be public");
  }
  if (manifest.repository?.url !== repositoryUrl) {
    failures.push(`repository.url must be ${repositoryUrl}`);
  }
  if (manifest.repository?.directory !== packagePath) {
    failures.push(`repository.directory must be ${packagePath}`);
  }
  if (!/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/.test(manifest.version)) {
    failures.push(`version is not publishable SemVer: ${manifest.version}`);
  }

  if (packagePath === "packages/ambient") {
    // Workflow derives stable package IDs only for files that are direct
    // package export targets. Keep every module containing a durable directive
    // exported so pnpm and Bazel filesystem paths never leak into those IDs.
    const durableModuleExports = {
      "./workflows/callback-steps": "./dist/workflows/callback-steps",
      "./workflows/correlation": "./dist/workflows/correlation",
    };
    for (const [subpath, target] of Object.entries(durableModuleExports)) {
      const exported = manifest.exports?.[subpath];
      if (
        exported?.types !== `${target}.d.ts` ||
        exported?.import !== `${target}.js` ||
        exported?.default !== `${target}.js`
      ) {
        failures.push(
          `${subpath} must directly export its durable Workflow module`,
        );
      }
    }
    const eveDependency = [
      manifest.dependencies?.eve,
      manifest.optionalDependencies?.eve,
      manifest.peerDependencies?.eve,
    ].find((value) => value !== undefined);
    if (eveDependency !== undefined) {
      failures.push("provider-independent core must not depend on eve");
    }
    if (manifest.dependencies?.workflow !== undefined) {
      failures.push("Workflow must remain an optional peer, not a direct dependency");
    }
    if (manifest.devDependencies?.workflow !== "5.0.0-beta.42") {
      failures.push("Workflow development dependency must be exactly 5.0.0-beta.42");
    }
    if (manifest.peerDependencies?.workflow !== ">=5.0.0-beta.42 <6") {
      failures.push("Workflow peer range must cover the supported Workflow 5 runtime");
    }
    if (manifest.peerDependenciesMeta?.workflow?.optional !== true) {
      failures.push("Workflow peer dependency must be optional");
    }
  }

  if (packagePath === "packages/eve-adapter") {
    if (manifest.peerDependencies?.eve !== "0.49.0") {
      failures.push("eve peer dependency must be exactly 0.49.0");
    }
    if (manifest.devDependencies?.eve !== "0.49.0") {
      failures.push("eve development dependency must be exactly 0.49.0");
    }
  }

  if (failures.length > 0) {
    throw new Error(`${packagePath}/package.json:\n- ${failures.join("\n- ")}`);
  }

  return manifest;
}

function parsePackOutput(output, packagePath) {
  const result = JSON.parse(output);
  if (typeof result !== "object" || result === null || Array.isArray(result)) {
    throw new Error(`pnpm pack returned an unexpected result for ${packagePath}`);
  }
  return result;
}

function validateContents(packagePath, expected, packResult) {
  const paths = new Set(packResult.files.map((file) => file.path));
  const missing = expected.requiredFiles.filter((file) => !paths.has(file));
  const legacyAmbientFiles = new Set([
    "dist/ai-sdk.d.ts",
    "dist/ai-sdk.js",
    "dist/definition.d.ts",
    "dist/definition.js",
    "dist/instance-machine.d.ts",
    "dist/instance-machine.js",
    "dist/mailbox.d.ts",
    "dist/mailbox.js",
    "dist/runtime.d.ts",
    "dist/runtime.js",
    "dist/storage.d.ts",
    "dist/storage.js",
    "dist/celld.d.ts",
    "dist/celld.js",
    "dist/celld-worker.d.ts",
    "dist/celld-worker.js",
    "dist/postgres.d.ts",
    "dist/postgres.js",
    "dist/coordinator.d.ts",
    "dist/coordinator.js",
    "dist/world-protocol.d.ts",
    "dist/world-protocol.js",
    "dist/world-steps.d.ts",
    "dist/world-steps.js",
    "dist/world-workflows.d.ts",
    "dist/world-workflows.js",
    "dist/world.d.ts",
    "dist/world.js",
    "bin/eve-ambient.mjs",
    "migrations/001_attention_engine.sql",
    "migrations/001_eve_ambient.sql",
  ]);
  const forbidden = [...paths].filter(
    (file) =>
      file === ".env" ||
      file.startsWith(".env.") ||
      file.startsWith("coverage/") ||
      file.startsWith("celld-worker/build/") ||
      file.startsWith("node_modules/") ||
      file.startsWith("src/") ||
      file.startsWith("test/") ||
      (packagePath === "packages/ambient" && legacyAmbientFiles.has(file)),
  );

  if (missing.length > 0 || forbidden.length > 0) {
    const details = [];
    if (missing.length > 0) details.push(`missing: ${missing.join(", ")}`);
    if (forbidden.length > 0) details.push(`forbidden: ${forbidden.join(", ")}`);
    throw new Error(`${packagePath} has invalid package contents (${details.join("; ")})`);
  }
}

function packEnvironment() {
  return { ...process.env, PNPM_CONFIG_IGNORE_SCRIPTS: "true" };
}

export function checkPackage(packagePath) {
  const expected = packages.get(packagePath);
  if (!expected) throw new Error(`refusing unknown package path: ${packagePath}`);

  const manifest = validateManifest(packagePath, expected);
  const output = execFileSync("pnpm", ["pack", "--json", "--dry-run"], {
    cwd: packagePath,
    encoding: "utf8",
    env: packEnvironment(),
  });
  const packResult = parsePackOutput(output, packagePath);
  validateContents(packagePath, expected, packResult);
  return { expected, manifest, packResult };
}

export function packPackage(packagePath, destination) {
  const expected = packages.get(packagePath);
  if (!expected) throw new Error(`refusing unknown package path: ${packagePath}`);

  const manifest = validateManifest(packagePath, expected);
  const output = execFileSync(
    "pnpm",
    ["pack", "--json", "--pack-destination", resolve(destination)],
    { cwd: packagePath, encoding: "utf8", env: packEnvironment() },
  );
  const packResult = parsePackOutput(output, packagePath);
  validateContents(packagePath, expected, packResult);

  const tarball = join(destination, basename(packResult.filename));
  const digest = createHash("sha256").update(readFileSync(tarball)).digest("hex");
  writeFileSync(`${tarball}.sha256`, `${digest}  ${basename(tarball)}\n`, {
    encoding: "utf8",
    flag: "wx",
  });

  return { expected, manifest, packResult, tarball };
}
