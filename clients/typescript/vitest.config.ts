import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    // The integration tests share one pgkv keyspace and one Redis database.
    fileParallelism: false,
    testTimeout: 30_000,
    // Show the skip notices and the differential test's summary on a passing run too.
    silent: false,
  },
});
