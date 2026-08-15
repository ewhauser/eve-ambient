import { workflow } from "@workflow/vitest";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [workflow()],
  test: {
    include: ["test/**/*.integration.test.ts"],
    testTimeout: 60_000,
  },
});
