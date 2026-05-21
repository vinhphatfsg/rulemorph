import { mkdirSync, writeFileSync } from "fs";
import path from "path";

export function writeInlineNodesTrace(dataDir: string) {
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

export function writeLegacyInlineNodesTrace(dataDir: string) {
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
