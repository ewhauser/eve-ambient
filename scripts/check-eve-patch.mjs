import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const packageRoot = resolve("packages/eve-adapter/node_modules/eve");
const manifest = JSON.parse(
  readFileSync(resolve(packageRoot, "package.json"), "utf8"),
);
if (manifest.name !== "eve" || manifest.version !== "0.49.0") {
  throw new Error(
    `expected patched eve@0.49.0, found ${String(manifest.name)}@${String(manifest.version)}`,
  );
}

const assertions = [
  [
    "dist/src/channel/channel-operations.d.ts",
    "readonly idempotencyKey?: string;",
  ],
  [
    "dist/src/channel/channel-address.js",
    "taskDeliveryId:o.idempotencyKey",
  ],
  ["dist/src/channel/channel-address.js", "idempotencyKey:o.idempotencyKey"],
  ["dist/src/execution/workflow-entry.js", "taskDeliveryId:r.idempotencyKey"],
  [
    "dist/src/execution/workflow-runtime.js",
    "a.idempotencyKey!==void 0&&(m.idempotencyKey=a.idempotencyKey)",
  ],
];

for (const [file, marker] of assertions) {
  const contents = readFileSync(resolve(packageRoot, file), "utf8");
  if (!contents.includes(marker)) {
    throw new Error(`eve@0.49.0 is missing carried patch marker in ${file}`);
  }
}

const patch = readFileSync(
  resolve("packages/eve-adapter/patches/eve@0.49.0.patch"),
  "utf8",
);
for (const marker of [
  "readonly idempotencyKey?: string;",
  "taskDeliveryId:o.idempotencyKey",
  "a.idempotencyKey!==void 0&&(m.idempotencyKey=a.idempotencyKey)",
]) {
  if (!patch.includes(marker)) {
    throw new Error(`published Eve patch is missing marker ${marker}`);
  }
}

const sourcePatch = readFileSync(
  resolve("packages/eve-adapter/patches/eve@0.49.0-source.patch"),
  "utf8",
);
for (const marker of [
  'readonly idempotencyKey?: string;',
  'seenTaskDeliveries.add(input.initialInput.taskDeliveryId);',
  'it("carries one explicit idempotency key through existing and initial sessions"',
]) {
  if (!sourcePatch.includes(marker)) {
    throw new Error(`Eve source review patch is missing marker ${marker}`);
  }
}

console.log("verified carried Eve idempotency patch for eve@0.49.0");
