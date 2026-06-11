import { mkdirSync, readFileSync, writeFileSync } from "fs";
import path from "path";
import { fileURLToPath } from "url";

const fixturePath = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../../src/__fixtures__/server_trace_responses.json"
);
const serverTraceResponses = JSON.parse(readFileSync(fixturePath, "utf8")) as {
  trace: unknown;
  records: unknown[];
  nodes: unknown[];
  finalize: unknown;
};

function writeNdjson(filePath: string, rows: unknown[]) {
  writeFileSync(filePath, `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`);
}

export function writeDemoTrace(dataDir: string) {
  const traceDir = path.join(dataDir, "traces", "2026", "02", "03", "demo-001");
  mkdirSync(traceDir, { recursive: true });
  writeNdjson(path.join(traceDir, "records-0001.ndjson"), serverTraceResponses.records);
  writeNdjson(path.join(traceDir, "nodes-0001.ndjson"), serverTraceResponses.nodes);
  writeFileSync(path.join(traceDir, "finalize.json"), JSON.stringify(serverTraceResponses.finalize));
  writeFileSync(
    path.join(traceDir, "trace.json"),
    JSON.stringify(serverTraceResponses.trace, null, 2)
  );
}

export function writeErrorTrace(dataDir: string) {
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
