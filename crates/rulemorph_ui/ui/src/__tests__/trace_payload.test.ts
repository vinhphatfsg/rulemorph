import { describe, expect, it } from "vitest";

import serverTraceResponses from "../__fixtures__/server_trace_responses.json";
import {
  mergeNodesIntoRecords,
  normalizeInlineNodeValue,
  normalizeInlineNodes,
  normalizeRecord,
  normalizeRecordIndex,
  normalizeTracePayload,
  parseNumericIndex,
  type TracePayload,
  type TraceNodeChunkEntry,
  type TraceRecord
} from "../api/trace_payload";

describe("trace payload helpers", () => {
  it("keeps the server trace fixture assignable to UI response types", () => {
    const response: {
      trace: TracePayload;
      records: TraceRecord[];
      nodes: TraceNodeChunkEntry[];
      finalize: NonNullable<TracePayload["finalize"]>;
    } = serverTraceResponses;

    expect(response.trace.trace_id).toBe("demo-001");
    expect(response.nodes[0].node.children?.[0].args).toEqual({ amount: 1 });
  });

  it("parses finite numeric record indexes", () => {
    expect(parseNumericIndex(2)).toBe(2);
    expect(parseNumericIndex("3")).toBe(3);
    expect(parseNumericIndex(Number.NaN)).toBeNull();
    expect(parseNumericIndex(Number.POSITIVE_INFINITY)).toBeNull();
    expect(parseNumericIndex("not-a-number")).toBeNull();
    expect(normalizeRecordIndex("4", 9)).toBe(4);
    expect(normalizeRecordIndex("missing", 9)).toBe(9);
  });

  it("normalizes inline node values with existing fallback behavior", () => {
    expect(normalizeInlineNodeValue("label text", "fallback")).toMatchObject({
      id: "fallback",
      kind: "value",
      label: "label text",
      value: "label text"
    });

    expect(normalizeInlineNodeValue({ id: "", kind: "", label: "", value: 7 }, "fallback")).toMatchObject({
      id: "fallback",
      kind: "value",
      label: "value",
      value: 7
    });

    expect(normalizeInlineNodeValue({ id: "node", kind: "op", label: "trim" }, "fallback")).toMatchObject({
      id: "node",
      kind: "op",
      label: "trim"
    });
  });

  it("normalizes inline node collections", () => {
    expect(normalizeInlineNodes(null, "record-0")).toEqual([]);
    expect(normalizeInlineNodes(undefined, "record-0")).toEqual([]);
    expect(normalizeInlineNodes("single", "record-0")).toEqual([
      { id: "record-0-0", kind: "value", label: "single", value: "single" }
    ]);
    expect(normalizeInlineNodes([{ label: "A" }, { id: "b", kind: "step" }], "record-0")).toEqual([
      { id: "record-0-0", kind: "value", label: "A" },
      { id: "b", kind: "step", label: "step" }
    ]);
  });

  it("normalizes record indexes and node prefixes", () => {
    const record = normalizeRecord(
      { index: "bad" as unknown as number, nodes: [{ label: "node" }] },
      6
    );
    expect(record.index).toBe(6);
    expect(record.nodes).toEqual([{ id: "record-6-0", kind: "value", label: "node" }]);
  });

  it("normalizes traces only when records are present", () => {
    expect(normalizeTracePayload(null)).toBeNull();
    const traceWithoutRecords: TracePayload = { trace_id: "trace-a" };
    expect(normalizeTracePayload(traceWithoutRecords)).toBe(traceWithoutRecords);

    const normalized = normalizeTracePayload({
      trace_id: "trace-b",
      records: [{ index: "1" as unknown as number, nodes: "inline" as unknown as TraceRecord["nodes"] }]
    });
    expect(normalized?.records?.[0]).toMatchObject({
      index: 1,
      nodes: [{ id: "record-1-0", kind: "value", label: "inline", value: "inline" }]
    });
  });

  it("merges chunk nodes into normalized records", () => {
    const chunkNodes = new Map([
      [
        2,
        [
          {
            id: "chunk-node",
            kind: "op",
            label: "chunk"
          }
        ]
      ]
    ]);

    const merged = mergeNodesIntoRecords(
      [
        { index: "2" as unknown as number, nodes: [{ label: "inline" }] },
        { index: "bad" as unknown as number }
      ],
      chunkNodes
    );

    expect(merged[0]).toMatchObject({
      index: 2,
      nodes: [
        { id: "record-2-0", kind: "value", label: "inline" },
        { id: "chunk-node", kind: "op", label: "chunk" }
      ]
    });
    expect(merged[1]).toEqual({ index: 1 });
  });
});
