import { execFileSync } from "node:child_process";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { basename, resolve } from "node:path";

import { packPackage } from "./package-artifacts.mjs";

const temporary = mkdtempSync(`${tmpdir()}/eve-ambient-consumer-`);
try {
  const artifacts = resolve(temporary, "artifacts");
  const consumer = resolve(temporary, "consumer");
  mkdirSync(artifacts);
  mkdirSync(consumer);

  const core = packPackage("packages/ambient", artifacts);
  const adapter = packPackage("packages/eve-adapter", artifacts);
  const patch = execFileSync(
    "tar",
    ["-xOf", adapter.tarball, "package/patches/eve@0.38.1.patch"],
    { encoding: "utf8" },
  );
  mkdirSync(resolve(consumer, "patches"));
  writeFileSync(resolve(consumer, "patches/eve@0.38.1.patch"), patch, "utf8");

  writeFileSync(
    resolve(consumer, "package.json"),
    `${JSON.stringify(
      {
        name: "eve-ambient-clean-consumer",
        version: "0.0.0",
        private: true,
        type: "module",
        dependencies: {
          "@ewhauser/eve-ambient": `file:${core.tarball}`,
          "@ewhauser/eve-ambient-eve": `file:${adapter.tarball}`,
          eve: "0.38.1",
        },
      },
      null,
      2,
    )}\n`,
    "utf8",
  );
  writeFileSync(
    resolve(consumer, "pnpm-workspace.yaml"),
    'packages:\n  - "."\n\npatchedDependencies:\n  "eve@0.38.1": patches/eve@0.38.1.patch\n',
    "utf8",
  );

  execFileSync("pnpm", ["install", "--prefer-offline", "--ignore-scripts"], {
    cwd: consumer,
    stdio: "inherit",
  });
  const generatedCelld = resolve(consumer, "generated-celld");
  execFileSync(
    "pnpm",
    ["exec", "eve-ambient", "init", "celld", generatedCelld],
    { cwd: consumer, stdio: "pipe" },
  );
  const generatedCelldConfig = readFileSync(
    resolve(generatedCelld, "wrangler.jsonc"),
    "utf8",
  );
  if (!generatedCelldConfig.includes("ATTENTION_CALLBACK_URL")) {
    throw new Error("celld initializer did not create the simplified callback configuration");
  }
  execFileSync(
    "node",
    [
      "--input-type=module",
      "--eval",
      'const core = await import("@ewhauser/eve-ambient"); const protocol = await import("@ewhauser/eve-ambient/protocol"); const idempotency = await import("@ewhauser/eve-ambient/idempotency"); const postgres = await import("@ewhauser/eve-ambient/postgres"); const celld = await import("@ewhauser/eve-ambient/celld"); const memory = await import("@ewhauser/eve-ambient/memory"); const adapter = await import("@ewhauser/eve-ambient-eve"); if (typeof core.defineAmbientApplication !== "function") throw new Error("missing ambient application definition"); if (typeof protocol.compileAcceptedFanout !== "function") throw new Error("missing protocol surface"); if (typeof idempotency.deriveEventKey !== "function") throw new Error("missing idempotency surface"); if (typeof postgres.postgres !== "function") throw new Error("missing Postgres binding"); if (typeof celld.celld !== "function") throw new Error("missing celld binding"); if (typeof memory.memory !== "function") throw new Error("missing memory binding"); for (const removed of ["MonitorRuntime", "compileMonitor", "PostgresMonitorStore"]) if (removed in core || removed in postgres) throw new Error(`legacy export remains: ${removed}`); if (typeof adapter.createEveAttentionRoute !== "function") throw new Error("missing Eve attention route"); if (adapter.SUPPORTED_EVE_VERSION !== "0.38.1") throw new Error("wrong Eve support version");',
    ],
    { cwd: consumer, stdio: "pipe" },
  );

  const installed = readFileSync(
    resolve(consumer, "node_modules/eve/dist/src/channel/channel-operations.d.ts"),
    "utf8",
  );
  if (!installed.includes("readonly idempotencyKey?: string;")) {
    throw new Error("clean consumer installed Eve without the carried patch");
  }

  console.log(
    `verified clean consumer for ${basename(core.tarball)} and ${basename(adapter.tarball)}`,
  );
} finally {
  rmSync(temporary, { force: true, recursive: true });
}
