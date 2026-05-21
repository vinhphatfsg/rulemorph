import { expect, test } from "@playwright/test";
import { mkdtempSync } from "fs";
import { tmpdir } from "os";
import path from "path";
import { getAvailablePort, startServer } from "../support/server";
import { writeDemoTrace, writeErrorTrace, writeInlineNodesTrace } from "../support/traceFixtures";

export function registerTraceConsoleDetailTests() {
  test("Trace Console loads chunks and shows details", async ({ page }) => {
    const dataDir = mkdtempSync(path.join(tmpdir(), "rulemorph-e2e-"));
    writeDemoTrace(dataDir);
    const port = await getAvailablePort();
    const server = await startServer(dataDir, port);

    try {
      await page.goto(`http://127.0.0.1:${port}/`, { waitUntil: "domcontentloaded" });
      await page.getByRole("heading", { name: "Trace一覧" }).waitFor();

      const demoCard = page.getByRole("button", { name: /demo/ }).first();
      await demoCard.click();

      await page.getByTestId("rf__node-rules/demo.yaml").click();
      await page.getByRole("heading", { name: "Records" }).waitFor();

      await page.getByTestId("rf__node-detail-rules/demo.yaml::step-0").click();
      await expect(page.getByText('"foo": 1')).toBeVisible();
      await expect(page.getByText('"bar": 2').first()).toBeVisible();
    } finally {
      server.kill("SIGTERM");
    }
  });

  test("Trace Console filters traces", async ({ page }) => {
    const dataDir = mkdtempSync(path.join(tmpdir(), "rulemorph-e2e-"));
    writeDemoTrace(dataDir);
    writeInlineNodesTrace(dataDir);
    writeErrorTrace(dataDir);
    const port = await getAvailablePort();
    const server = await startServer(dataDir, port);
    let traceListRequestCount = 0;
    page.on("request", (request) => {
      if (request.method() === "GET" && request.url().endsWith("/api/traces")) {
        traceListRequestCount += 1;
      }
    });

    try {
      await page.goto(`http://127.0.0.1:${port}/`, { waitUntil: "domcontentloaded" });
      await page.getByRole("heading", { name: "Trace一覧" }).waitFor();
      const beforeFilterRequests = traceListRequestCount;

      const search = page.locator(".trace-filter__search");
      await search.fill("inline");
      await expect(page.getByRole("button", { name: /inline/ }).first()).toBeVisible();
      await expect(page.getByRole("button", { name: /demo/ })).toHaveCount(0);
      await page.waitForTimeout(200);
      expect(traceListRequestCount).toBe(beforeFilterRequests);

      await search.fill("");
      await page.getByRole("combobox", { name: "ステータス" }).selectOption("error");
      await expect(page.getByRole("button", { name: /error/ }).first()).toBeVisible();
      await expect(page.getByRole("button", { name: /demo/ })).toHaveCount(0);
      await page.waitForTimeout(200);
      expect(traceListRequestCount).toBe(beforeFilterRequests);
    } finally {
      server.kill("SIGTERM");
    }
  });

  test("Trace Console shows finalize details", async ({ page }) => {
    const dataDir = mkdtempSync(path.join(tmpdir(), "rulemorph-e2e-"));
    writeDemoTrace(dataDir);
    const port = await getAvailablePort();
    const server = await startServer(dataDir, port);

    try {
      await page.goto(`http://127.0.0.1:${port}/`, { waitUntil: "domcontentloaded" });
      await page.getByRole("heading", { name: "Trace一覧" }).waitFor();
      await page.getByRole("button", { name: /demo/ }).first().click();

      await page.getByTestId("rf__node-rules/demo.yaml").click();
      await page.getByRole("heading", { name: "Records" }).waitFor();
      await page.getByTestId("record-finalize").click();

      await page
        .locator(".inspector__section--finalize .inspector__section-toggle")
        .waitFor();
      await expect(page.getByText('"bar": 2').first()).toBeVisible();
    } finally {
      server.kill("SIGTERM");
    }
  });
}
