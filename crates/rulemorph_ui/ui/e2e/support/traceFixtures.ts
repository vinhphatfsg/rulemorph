import { mkdirSync, writeFileSync } from "fs";
import path from "path";

export function writeDemoTrace(dataDir: string) {
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
