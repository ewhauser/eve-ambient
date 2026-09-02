import { spawnSync } from "node:child_process";
import {
  cpSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";

const [
  baselineArgument,
  modifiedArgument,
  outputArgument,
  sourceRepositoryArgument,
  sourceOutputArgument,
] = process.argv.slice(2);
if (
  baselineArgument === undefined ||
  modifiedArgument === undefined ||
  outputArgument === undefined
) {
  throw new Error(
    "usage: node scripts/generate-eve-patch.mjs <published-package> <built-package> <output> [source-repository source-output]",
  );
}
if ((sourceRepositoryArgument === undefined) !== (sourceOutputArgument === undefined)) {
  throw new Error("source-repository and source-output must be provided together");
}

const baseline = resolve(baselineArgument);
const modified = resolve(modifiedArgument);
const output = resolve(outputArgument);
for (const packageRoot of [baseline, modified]) {
  const manifest = JSON.parse(
    readFileSync(resolve(packageRoot, "package.json"), "utf8"),
  );
  if (manifest.name !== "eve" || manifest.version !== "0.49.0") {
    throw new Error(
      `expected eve@0.49.0 at ${packageRoot}, found ${String(manifest.name)}@${String(manifest.version)}`,
    );
  }
}
const files = [
  "dist/src/channel/channel-address.d.ts",
  "dist/src/channel/channel-address.js",
  "dist/src/channel/channel-operations.d.ts",
  "dist/src/channel/types.d.ts",
  "dist/src/execution/workflow-entry.d.ts",
  "dist/src/execution/workflow-entry.js",
  "dist/src/execution/workflow-runtime.js",
];

const temporary = mkdtempSync(`${tmpdir()}/eve-ambient-eve-patch-`);
try {
  for (const file of files) {
    const oldFile = `${temporary}/old/${file}`;
    const newFile = `${temporary}/new/${file}`;
    mkdirSync(dirname(oldFile), { recursive: true });
    mkdirSync(dirname(newFile), { recursive: true });
    cpSync(`${baseline}/${file}`, oldFile);
    cpSync(`${modified}/${file}`, newFile);
  }

  const result = spawnSync(
    "git",
    [
      "diff",
      "--no-index",
      "--src-prefix=a/",
      "--dst-prefix=b/",
      "--",
      "old",
      "new",
    ],
    { cwd: temporary, encoding: "utf8" },
  );
  if (result.status !== 1 || result.stdout.length === 0) {
    throw new Error(
      `expected package differences, got status ${String(result.status)}`,
    );
  }

  const patch = result.stdout
    .replaceAll("a/old/", "a/")
    .replaceAll("b/new/", "b/");
  mkdirSync(dirname(output), { recursive: true });
  writeFileSync(output, patch, "utf8");
  console.log(`wrote ${files.length} Eve package differences to ${output}`);

  if (sourceRepositoryArgument !== undefined && sourceOutputArgument !== undefined) {
    const sourceRepository = resolve(sourceRepositoryArgument);
    const sourceHead = spawnSync("git", ["rev-parse", "HEAD"], {
      cwd: sourceRepository,
      encoding: "utf8",
    });
    if (
      sourceHead.status !== 0 ||
      sourceHead.stdout.trim() !== "78fa9046b8ad377b7fdca2c6d18cd3c10afcfc77"
    ) {
      throw new Error("Eve source repository is not at the eve@0.49.0 commit");
    }
    const sourceFiles = [
      "packages/eve/src/channel/channel-address.test.ts",
      "packages/eve/src/channel/channel-address.ts",
      "packages/eve/src/channel/channel-operations.ts",
      "packages/eve/src/channel/types.ts",
      "packages/eve/src/execution/workflow-entry.ts",
      "packages/eve/src/execution/workflow-runtime.ts",
    ];
    const sourceResult = spawnSync(
      "git",
      ["diff", "--binary", "--unified=0", "--", ...sourceFiles],
      { cwd: sourceRepository, encoding: "utf8" },
    );
    if (sourceResult.status !== 0 || sourceResult.stdout.length === 0) {
      throw new Error(
        `expected Eve source differences, got status ${String(sourceResult.status)}`,
      );
    }
    const sourceOutput = resolve(sourceOutputArgument);
    mkdirSync(dirname(sourceOutput), { recursive: true });
    writeFileSync(sourceOutput, sourceResult.stdout, "utf8");
    console.log(`wrote Eve source review patch to ${sourceOutput}`);
  }
} finally {
  rmSync(temporary, { force: true, recursive: true });
}
