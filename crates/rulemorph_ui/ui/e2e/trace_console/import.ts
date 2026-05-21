import { expect, test } from "@playwright/test";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import path from "path";
import { getAvailablePort, startServerWithOptions } from "../support/server";
import { createStoredZip } from "../support/zip";

function writeZipTrace(traceDir: string, traceId: string, name: string, rulePath: string, durationUs: number) {
  writeFileSync(
    path.join(traceDir, "trace.json"),
    JSON.stringify(
      {
        trace_schema_version: 1,
        trace_id: traceId,
        timestamp: "2026-02-03T00:30:00Z",
        status: "ok",
        rule: {
          type: "normal",
          name,
          path: rulePath,
          version: 2
        },
        input_format: "json",
        summary: {
          record_total: 1,
          record_success: 1,
          record_failed: 0,
          duration_us: durationUs
        },
        max_chunk_bytes_uncompressed: 1048576,
        detail: {
          layout: "records_nodes_split",
          status: "basic",
          reason: [],
          records: [],
          nodes: []
        }
      },
      null,
      2
    )
  );
}

export function registerTraceConsoleImportTests() {
  test("Trace Console imports ZIP bundles", async ({ page }) => {
    const dataDir = mkdtempSync(path.join(tmpdir(), "rulemorph-e2e-"));
    const bundleDir = mkdtempSync(path.join(tmpdir(), "rulemorph-bundle-"));
    const traceDir = path.join(bundleDir, "traces", "2026", "02", "03", "zip-001");
    mkdirSync(traceDir, { recursive: true });
    writeZipTrace(traceDir, "zip-001", "zip", "rules/zip.yaml", 800);
    const zipPath = path.join(bundleDir, "bundle.zip");
    createStoredZip(zipPath, [
      {
        name: "traces/2026/02/03/zip-001/trace.json",
        data: readFileSync(path.join(traceDir, "trace.json"))
      }
    ]);

    const port = await getAvailablePort();
    const server = await startServerWithOptions(dataDir, port, {
      internalApiKey: "internal-test",
      allowUnauthInternal: true
    });
    let apiImportRequests = 0;
    let internalImportZipRequests = 0;
    page.on("request", (request) => {
      if (request.method() === "POST" && request.url().endsWith("/api/import")) {
        apiImportRequests += 1;
      }
      if (request.method() === "POST" && request.url().endsWith("/internal/import-zip")) {
        internalImportZipRequests += 1;
      }
    });

    try {
      await page.goto(`http://127.0.0.1:${port}/?internal_key=internal-test`, {
        waitUntil: "domcontentloaded"
      });
      await page.getByRole("heading", { name: "Trace一覧" }).waitFor();

      await page.getByTestId("zip-import-button").click();
      await page.getByTestId("zip-import-file").setInputFiles(zipPath);
      const importResponsePromise = page.waitForResponse(
        (response) =>
          response.request().method() === "POST" && response.url().endsWith("/api/import")
      );
      await page.getByTestId("zip-import-submit").click();
      const importResponse = await importResponsePromise;
      expect(importResponse.status()).toBe(200);
      expect(importResponse.request().headers()["x-api-key"]).toBe("internal-test");
      expect(importResponse.request().headers()["x-rulemorph-import"]).toBe("zip");
      await expect(page.getByTestId("zip-import-message")).toContainText("imported 1 traces");
      expect(apiImportRequests).toBeGreaterThan(0);
      expect(internalImportZipRequests).toBe(0);
      await expect(
        page.getByRole("button", { name: /rules\/zip\.yaml/ }).first()
      ).toBeVisible();
    } finally {
      server.kill("SIGTERM");
    }
  });

  test("Trace Console imports ZIP bundles with static auth", async ({ page }) => {
    const dataDir = mkdtempSync(path.join(tmpdir(), "rulemorph-e2e-auth-"));
    const bundleDir = mkdtempSync(path.join(tmpdir(), "rulemorph-bundle-auth-"));
    const traceDir = path.join(bundleDir, "traces", "2026", "02", "03", "zip-auth-001");
    mkdirSync(traceDir, { recursive: true });
    writeZipTrace(traceDir, "zip-auth-001", "zip-auth", "rules/zip-auth.yaml", 900);
    const zipPath = path.join(bundleDir, "bundle.zip");
    createStoredZip(zipPath, [
      {
        name: "traces/2026/02/03/zip-auth-001/trace.json",
        data: readFileSync(path.join(traceDir, "trace.json"))
      }
    ]);

    const port = await getAvailablePort();
    const server = await startServerWithOptions(dataDir, port, {
      apiKey: "static-test-key",
      tenantId: "tenant-static",
      internalApiKey: "internal-test-key",
      allowUnauthInternal: false
    });
    let apiImportRequests = 0;
    let internalImportZipRequests = 0;
    page.on("request", (request) => {
      if (request.method() === "POST" && request.url().endsWith("/api/import")) {
        apiImportRequests += 1;
      }
      if (request.method() === "POST" && request.url().endsWith("/internal/import-zip")) {
        internalImportZipRequests += 1;
      }
    });

    try {
      await page.goto(
        `http://127.0.0.1:${port}/?api_key=static-test-key&internal_key=internal-test-key&tenant_id=tenant-static`,
        {
          waitUntil: "domcontentloaded"
        }
      );
      await page.getByRole("heading", { name: "Trace一覧" }).waitFor();

      await page.getByTestId("zip-import-button").click();
      await page.getByTestId("zip-import-file").setInputFiles(zipPath);
      const importResponsePromise = page.waitForResponse(
        (response) =>
          response.request().method() === "POST" && response.url().endsWith("/api/import")
      );
      await page.getByTestId("zip-import-submit").click();
      const importResponse = await importResponsePromise;
      expect(importResponse.status()).toBe(200);
      expect(importResponse.request().headers()["x-api-key"]).toBe("internal-test-key");
      expect(importResponse.request().headers()["x-rulemorph-import"]).toBe("zip");
      await expect(page.getByTestId("zip-import-message")).toContainText("imported 1 traces");
      expect(apiImportRequests).toBeGreaterThan(0);
      expect(internalImportZipRequests).toBe(0);
      await expect(
        page.getByRole("button", { name: /rules\/zip-auth\.yaml/ }).first()
      ).toBeVisible();
    } finally {
      server.kill("SIGTERM");
    }
  });
}
