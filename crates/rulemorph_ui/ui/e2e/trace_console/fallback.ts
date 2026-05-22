import { expect, test } from "@playwright/test";
import { mkdtempSync } from "fs";
import { tmpdir } from "os";
import path from "path";
import { getAvailablePort, startServer } from "../support/server";
import { writeBasicTrace, writeBrokenTrace } from "../support/traceFixtures";

export function registerTraceConsoleBasicFallbackTest() {
  test("Trace Console shows basic detail fallback message", async ({ page }) => {
    const dataDir = mkdtempSync(path.join(tmpdir(), "rulemorph-e2e-"));
    writeBasicTrace(dataDir);
    const port = await getAvailablePort();
    const server = await startServer(dataDir, port);

    try {
      await page.goto(`http://127.0.0.1:${port}/`, { waitUntil: "domcontentloaded" });
      await page.getByRole("heading", { name: "Trace一覧" }).waitFor();
      await page.getByRole("button", { name: /basic/ }).first().click();
      await page.getByText("detail は basic です。").waitFor();
      await expect(page.getByText(/reason: budget_exceeded/)).toBeVisible();
    } finally {
      server.kill("SIGTERM");
    }
  });
}

export function registerTraceConsoleBrokenFallbackTest() {
  test("Trace Console downgrades when detail chunks fail to load", async ({ page }) => {
    const dataDir = mkdtempSync(path.join(tmpdir(), "rulemorph-e2e-"));
    writeBrokenTrace(dataDir);
    const port = await getAvailablePort();
    const server = await startServer(dataDir, port);

    try {
      await page.goto(`http://127.0.0.1:${port}/`, { waitUntil: "domcontentloaded" });
      await page.getByRole("heading", { name: "Trace一覧" }).waitFor();
      await page.getByRole("button", { name: /broken/ }).first().click();
      await page.getByText("detail は basic です。").waitFor();
      await expect(page.getByText(/reason: chunk_error/)).toBeVisible();
    } finally {
      server.kill("SIGTERM");
    }
  });
}
