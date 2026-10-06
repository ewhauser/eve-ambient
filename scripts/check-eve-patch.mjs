import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const version = "0.71.2";
const packageRoot = resolve("packages/eve-adapter/node_modules/eve");
const manifest = JSON.parse(readFileSync(resolve(packageRoot, "package.json"), "utf8"));
if (manifest.name !== "eve" || manifest.version !== version) {
  throw new Error(`expected patched eve@${version}, found ${manifest.name}@${manifest.version}`);
}

const assertions = [
  ["dist/src/channel/channel-operations.d.ts", "readonly idempotencyKey?: string;"],
  ["dist/src/channel/channel-address.js", "taskDeliveryId:i.idempotencyKey"],
  ["dist/src/execution/workflow-runtime.js", "l.idempotencyKey=t.idempotencyKey"],
  ["dist/src/execution/session/entry.js", "createSessionInbox(t,e.idempotencyKey"],
  ["dist/src/execution/session/entry.js", "e.checkpoint.seenTaskDeliveries"],
  ["dist/src/execution/session/program.js", "seenTaskDeliveries:i.seenTaskDeliveries"],
  ["dist/src/execution/session-inbox/inbox.js", "seen.has(e.value.taskDeliveryId)"],
];
for (const [file, marker] of assertions) {
  if (!readFileSync(resolve(packageRoot, file), "utf8").includes(marker)) {
    throw new Error(`eve@${version} is missing patch marker in ${file}: ${marker}`);
  }
}
console.log(`verified Eve idempotency patch for eve@${version}`);
