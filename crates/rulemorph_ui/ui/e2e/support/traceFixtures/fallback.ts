import { mkdirSync, writeFileSync } from "fs";
import path from "path";

export function writeBasicTrace(dataDir: string) {
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

export function writeBrokenTrace(dataDir: string) {
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
