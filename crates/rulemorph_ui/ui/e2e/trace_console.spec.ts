import { test, expect } from "@playwright/test";
import { execFileSync, spawn } from "child_process";
import { mkdtempSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { tmpdir } from "os";
import path from "path";
import net from "net";
import { fileURLToPath } from "url";

const repoRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../../.."
);
const uiDist = path.resolve(repoRoot, "crates/rulemorph_ui/ui/dist");
const apiRulesDir = path.resolve(repoRoot, "assets/api_rules");

function getAvailablePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on("error", reject);
    server.listen(0, () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close(() => reject(new Error("failed to acquire port")));
        return;
      }
      const { port } = address;
      server.close(() => resolve(port));
    });
  });
}

function writeDemoTrace(dataDir: string) {
  const traceDir = path.join(dataDir, "traces", "2026", "02", "03", "demo-001");
  mkdirSync(traceDir, { recursive: true });
  writeFileSync(
    path.join(traceDir, "records-0001.ndjson"),
    "{\"index\":0,\"status\":\"ok\",\"duration_us\":1000,\"input\":{\"foo\":1},\"output\":{\"bar\":2}}\n"
  );
  writeFileSync(
    path.join(traceDir, "nodes-0001.ndjson"),
    "{\"record_index\":0,\"id\":\"n1\",\"kind\":\"mappings\",\"label\":\"mappings[0]\",\"status\":\"ok\",\"input\":{\"foo\":1},\"output\":{\"bar\":2},\"children\":[{\"id\":\"n1.1\",\"kind\":\"op\",\"label\":\"add\",\"status\":\"ok\",\"input\":1,\"output\":2,\"pipe_value\":1,\"args\":[1],\"meta\":{\"op\":\"add\"}}]}\n"
  );
  writeFileSync(
    path.join(traceDir, "finalize.json"),
    "{\"status\":\"ok\",\"input\":[{\"bar\":2}],\"output\":[{\"bar\":2}],\"duration_us\":100,\"nodes\":[{\"id\":\"op-wrap\",\"kind\":\"op\",\"label\":\"wrap\",\"status\":\"ok\",\"meta\":{\"op\":\"wrap\"},\"args\":{\"wrap\":{\"key\":\"value\"}}}]}"
  );
  writeFileSync(
    path.join(traceDir, "trace.json"),
    JSON.stringify(
      {
        trace_schema_version: 1,
        trace_id: "demo-001",
        timestamp: "2026-02-03T00:00:00Z",
        status: "ok",
        rule: {
          type: "normal",
          name: "demo",
          path: "rules/demo.yaml",
          version: 2
        },
        input_format: "json",
        summary: {
          record_total: 1,
          record_success: 1,
          record_failed: 0,
          duration_us: 1000
        },
        max_chunk_bytes_uncompressed: 1048576,
        detail: {
          layout: "records_nodes_split",
          status: "full",
          reason: [],
          records: [
            {
              path: "records-0001.ndjson",
              format: "ndjson",
              compression: "none",
              record_start: 0,
              record_end: 0
            }
          ],
          nodes: [
            {
              path: "nodes-0001.ndjson",
              format: "ndjson",
              compression: "none",
              node_start: 0,
              node_end: 0
            }
          ],
          finalize: {
            path: "finalize.json",
            format: "json",
            compression: "none"
          }
        }
      },
      null,
      2
    )
  );
}

function writeInlineNodesTrace(dataDir: string) {
  const traceDir = path.join(dataDir, "traces", "2026", "02", "03", "inline-001");
  mkdirSync(traceDir, { recursive: true });
  writeFileSync(
    path.join(traceDir, "records-0001.ndjson"),
    "{\"index\":0,\"status\":\"ok\",\"duration_us\":500,\"nodes\":{\"id\":\"inline-0\",\"kind\":\"mappings\",\"label\":\"inline node\",\"status\":\"ok\",\"input\":{\"foo\":1},\"output\":{\"bar\":2}}}\n"
  );
  writeFileSync(
    path.join(traceDir, "trace.json"),
    JSON.stringify(
      {
        trace_schema_version: 1,
        trace_id: "inline-001",
        timestamp: "2026-02-03T00:05:00Z",
        status: "ok",
        rule: {
          type: "normal",
          name: "inline",
          path: "rules/inline.yaml",
          version: 2
        },
        input_format: "json",
        summary: {
          record_total: 1,
          record_success: 1,
          record_failed: 0,
          duration_us: 500
        },
        max_chunk_bytes_uncompressed: 1048576,
        detail: {
          layout: "records_inline",
          status: "full",
          reason: [],
          records: [
            {
              path: "records-0001.ndjson",
              format: "ndjson",
              compression: "none",
              record_start: 0,
              record_end: 0
            }
          ],
          nodes: []
        }
      },
      null,
      2
    )
  );
}

function writeLegacyInlineNodesTrace(dataDir: string) {
  const traceDir = path.join(dataDir, "traces", "2026", "02", "03", "legacy-inline-001");
  mkdirSync(traceDir, { recursive: true });
  writeFileSync(
    path.join(traceDir, "trace.json"),
    JSON.stringify(
      {
        trace_id: "legacy-inline-001",
        timestamp: "2026-02-03T00:08:00Z",
        status: "ok",
        rule: {
          type: "normal",
          name: "legacy inline",
          path: "rules/legacy-inline.yaml",
          version: 2
        },
        records: [
          {
            index: 0,
            status: "ok",
            duration_us: 450,
            nodes: {
              id: "legacy-inline-0",
              kind: "mappings",
              label: "legacy inline",
              status: "ok",
              input: { foo: 1 },
              output: { bar: 2 }
            }
          }
        ]
      },
      null,
      2
    )
  );
}

function writeBasicTrace(dataDir: string) {
  const traceDir = path.join(dataDir, "traces", "2026", "02", "03", "basic-001");
  mkdirSync(traceDir, { recursive: true });
  writeFileSync(
    path.join(traceDir, "trace.json"),
    JSON.stringify(
      {
        trace_schema_version: 1,
        trace_id: "basic-001",
        timestamp: "2026-02-03T00:10:00Z",
        status: "ok",
        rule: {
          type: "normal",
          name: "basic",
          path: "rules/basic.yaml",
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
          reason: ["budget_exceeded"],
          records: [],
          nodes: []
        }
      },
      null,
      2
    )
  );
}

function writeErrorTrace(dataDir: string) {
  const traceDir = path.join(dataDir, "traces", "2026", "02", "03", "error-001");
  mkdirSync(traceDir, { recursive: true });
  writeFileSync(
    path.join(traceDir, "trace.json"),
    JSON.stringify(
      {
        trace_schema_version: 1,
        trace_id: "error-001",
        timestamp: "2026-02-03T00:15:00Z",
        status: "error",
        rule: {
          type: "normal",
          name: "error",
          path: "rules/error.yaml",
          version: 2
        },
        input_format: "json",
        summary: {
          record_total: 1,
          record_success: 0,
          record_failed: 1,
          duration_us: 1200
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

function writeBrokenTrace(dataDir: string) {
  const traceDir = path.join(dataDir, "traces", "2026", "02", "03", "broken-001");
  mkdirSync(traceDir, { recursive: true });
  writeFileSync(
    path.join(traceDir, "trace.json"),
    JSON.stringify(
      {
        trace_schema_version: 1,
        trace_id: "broken-001",
        timestamp: "2026-02-03T00:20:00Z",
        status: "ok",
        rule: {
          type: "normal",
          name: "broken",
          path: "rules/broken.yaml",
          version: 2
        },
        input_format: "json",
        summary: {
          record_total: 1,
          record_success: 1,
          record_failed: 0,
          duration_us: 700
        },
        max_chunk_bytes_uncompressed: 1048576,
        detail: {
          layout: "records_nodes_split",
          status: "full",
          reason: [],
          records: [
            {
              path: "records-0001.ndjson",
              format: "ndjson",
              compression: "none",
              record_start: 0,
              record_end: 0
            }
          ],
          nodes: []
        }
      },
      null,
      2
    )
  );
}

async function startServer(dataDir: string, port: number) {
  return startServerWithOptions(dataDir, port, {
    allowUnauthInternal: true
  });
}

type ServerOptions = {
  apiKey?: string;
  tenantId?: string;
  internalApiKey?: string;
  allowUnauthInternal?: boolean;
};

async function startServerWithOptions(dataDir: string, port: number, options: ServerOptions) {
  if (!existsSync(uiDist)) {
    throw new Error(`UI dist not found at ${uiDist}. Run npm --prefix crates/rulemorph_ui/ui run build first.`);
  }
  if (!existsSync(apiRulesDir)) {
    throw new Error(`api rules not found at ${apiRulesDir}.`);
  }
  const args = [
    "run",
    "-p",
    "rulemorph_server",
    "--",
    "--api-mode",
    "rules",
    "--rules-dir",
    apiRulesDir,
    "--data-dir",
    dataDir,
    "--ui-dir",
    uiDist,
    "--ssrf-allow-private",
    "--port",
    String(port)
  ];
  if (options.allowUnauthInternal ?? false) {
    args.push("--allow-unauth-internal");
  }
  if (options.apiKey) {
    args.push("--api-key", options.apiKey);
  }
  if (options.tenantId) {
    args.push("--tenant-id", options.tenantId);
  }
  if (options.internalApiKey) {
    args.push("--internal-api-key", options.internalApiKey);
  }
  if (options.apiKey) {
    args.push("--ssrf-allow-any");
  }

  const child = spawn(
    "cargo",
    args,
    {
      cwd: repoRoot,
      env: { ...process.env, RUST_LOG: "info" },
      stdio: ["ignore", "pipe", "pipe"]
    }
  );

  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error("server did not start in time"));
    }, 20_000);
    const onData = (data: Buffer) => {
      const text = data.toString();
      if (text.includes("listening on 127.0.0.1")) {
        clearTimeout(timeout);
        child.stdout?.off("data", onData);
        resolve();
      }
    };
    child.stdout?.on("data", onData);
    child.on("exit", (code) => {
      clearTimeout(timeout);
      reject(new Error(`server exited early: ${code}`));
    });
  });

  return child;
}

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
  execFileSync("zip", ["-r", zipPath, "traces"], { cwd: bundleDir });

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
  execFileSync("zip", ["-r", zipPath, "traces"], { cwd: bundleDir });

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
