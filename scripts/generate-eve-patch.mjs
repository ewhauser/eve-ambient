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

const [baselineArgument, modifiedArgument, outputArgument] = process.argv.slice(2);
if (!baselineArgument || !modifiedArgument || !outputArgument) {
  throw new Error("usage: node scripts/generate-eve-patch.mjs <published-package> <built-package> <output>");
}

const baseline = resolve(baselineArgument);
const modified = resolve(modifiedArgument);
const output = resolve(outputArgument);
for (const packageRoot of [baseline, modified]) {
  const manifest = JSON.parse(
    readFileSync(resolve(packageRoot, "package.json"), "utf8"),
  );
  if (manifest.name !== "eve" || manifest.version !== "0.71.2") {
    throw new Error(
      `expected eve@0.71.2 at ${packageRoot}, found ${String(manifest.name)}@${String(manifest.version)}`,
    );
  }
}
const files = [
  "dist/src/channel/channel-address.d.ts",
  "dist/src/channel/channel-address.js",
  "dist/src/channel/channel-operations.d.ts",
  "dist/src/channel/types.d.ts",
  "dist/src/execution/workflow-runtime.js",
  "dist/src/execution/session-inbox/inbox.d.ts",
  "dist/src/execution/session-inbox/inbox.js",
  "dist/src/execution/session/entry-input.d.ts",
  "dist/src/execution/session/entry.js",
  "dist/src/execution/session/handoff.d.ts",
  "dist/src/execution/session/program.js",
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

} finally {
  rmSync(temporary, { force: true, recursive: true });
}
