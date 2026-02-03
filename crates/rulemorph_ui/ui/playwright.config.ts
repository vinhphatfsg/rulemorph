import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 60_000,
  expect: { timeout: 10_000 },
  use: {
    viewport: { width: 1280, height: 720 }
  },
  reporter: "list",
  fullyParallel: false
});
