#!/usr/bin/env node

import { cpSync, existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const [, , command, component, requestedTarget] = process.argv;
if (command !== "init" || component !== "celld") {
  console.error("Usage: eve-ambient init celld [directory]");
  process.exitCode = 1;
} else {
  const target = resolve(requestedTarget ?? "eve-ambient-celld");
  if (existsSync(target)) {
    console.error(`Refusing to overwrite existing path: ${target}`);
    process.exitCode = 1;
  } else {
    const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
    cpSync(resolve(packageRoot, "celld-worker"), target, { recursive: true });
    console.log(`Created ${target}`);
    console.log("Set CELLD_FLEET_URL and ATTENTION_CALLBACK_URL in wrangler.jsonc.");
    console.log("Install ATTENTION_SECRET through your deployment secret store, then deploy with celld.");
  }
}
