import { expect, test } from "@playwright/test";
import { mkdtempSync } from "fs";
import { tmpdir } from "os";
import path from "path";
import { getAvailablePort, startServer } from "../support/server";
import { writeInlineNodesTrace, writeLegacyInlineNodesTrace } from "../support/traceFixtures";

export function registerTraceConsoleNormalizationTests() {
  test("Trace Console normalizes inline nodes", async ({ page }) => {
    const dataDir = mkdtempSync(path.join(tmpdir(), "rulemorph-e2e-"));
    writeInlineNodesTrace(dataDir);
    const port = await getAvailablePort();
    const server = await startServer(dataDir, port);

    try {
      await page.goto(`http://127.0.0.1:${port}/`, { waitUntil: "domcontentloaded" });
      await page.getByRole("heading", { name: "Trace一覧" }).waitFor();
      await page.getByRole("button", { name: /inline/ }).first().click();
      await page.getByTestId("rf__node-rules/inline.yaml").click();
      await page.getByRole("heading", { name: "Records" }).waitFor();
      await page.getByTestId("rf__node-detail-rules/inline.yaml::step-0").click();
      await expect(page.getByText("mappings · inline node")).toBeVisible();
    } finally {
      server.kill("SIGTERM");
    }
  });

  test("Trace Console normalizes legacy inline nodes", async ({ page }) => {
    const dataDir = mkdtempSync(path.join(tmpdir(), "rulemorph-e2e-"));
    writeLegacyInlineNodesTrace(dataDir);
    const port = await getAvailablePort();
    const server = await startServer(dataDir, port);

    try {
      await page.goto(`http://127.0.0.1:${port}/`, { waitUntil: "domcontentloaded" });
      await page.getByRole("heading", { name: "Trace一覧" }).waitFor();
      await page.getByRole("button", { name: /legacy inline/ }).first().click();
      await page.getByTestId("rf__node-rules/legacy-inline.yaml").click();
      await page.getByRole("heading", { name: "Records" }).waitFor();
      await page.getByTestId("rf__node-detail-rules/legacy-inline.yaml::step-0").click();
      await expect(page.getByText("mappings · legacy inline")).toBeVisible();
    } finally {
      server.kill("SIGTERM");
    }
  });
}
