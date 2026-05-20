import { test, expect } from "@playwright/test";
import { mkdtempSync, writeFileSync, mkdirSync, readFileSync } from "fs";
import { tmpdir } from "os";
import path from "path";
import { getAvailablePort, startServer, startServerWithOptions } from "./support/server";
import {
  writeBasicTrace,
  writeBrokenTrace,
  writeDemoTrace,
  writeErrorTrace,
  writeInlineNodesTrace,
  writeLegacyInlineNodesTrace
} from "./support/traceFixtures";
import { createStoredZip } from "./support/zip";

test.describe.configure({ mode: "serial" });

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

test("Trace Console imports ZIP bundles", async ({ page }) => {
  const dataDir = mkdtempSync(path.join(tmpdir(), "rulemorph-e2e-"));
  const bundleDir = mkdtempSync(path.join(tmpdir(), "rulemorph-bundle-"));
  const traceDir = path.join(bundleDir, "traces", "2026", "02", "03", "zip-001");
  mkdirSync(traceDir, { recursive: true });
  writeFileSync(
    path.join(traceDir, "trace.json"),
    JSON.stringify(
      {
        trace_schema_version: 1,
        trace_id: "zip-001",
        timestamp: "2026-02-03T00:30:00Z",
        status: "ok",
        rule: {
          type: "normal",
          name: "zip",
          path: "rules/zip.yaml",
          version: 2
        },
        input_format: "json",
        summary: {
          record_total: 1,
          record_success: 1,
          record_failed: 0,
          duration_us: 800
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
  writeFileSync(
    path.join(traceDir, "trace.json"),
    JSON.stringify(
      {
        trace_schema_version: 1,
        trace_id: "zip-auth-001",
        timestamp: "2026-02-03T00:35:00Z",
        status: "ok",
        rule: {
          type: "normal",
          name: "zip-auth",
          path: "rules/zip-auth.yaml",
          version: 2
        },
        input_format: "json",
        summary: {
          record_total: 1,
          record_success: 1,
          record_failed: 0,
          duration_us: 900
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
