import type { TraceSummary } from "../trace_list/trace_list_helpers";

export type TraceNode = {
  id: string;
  kind: string;
  label: string;
  status?: string;
  duration_us?: number;
  duration_ms?: number;
  input?: unknown;
  output?: unknown;
  pipe_value?: unknown;
  args?: unknown;
  pipe_steps?: { index: number; label: string; input?: unknown; output?: unknown }[];
  children?: TraceNode[];
  child_trace?: TracePayload;
  error?: { code?: string; message?: string; path?: string };
  meta?: Record<string, unknown>;
};

export type TraceRecord = {
  index: number;
  status?: string;
  duration_us?: number;
  duration_ms?: number;
  input?: unknown;
  output?: unknown;
  nodes?: TraceNode[];
  error?: { code?: string; message?: string; path?: string };
};

export type EndpointSpec = {
  method: string;
  path: string;
  steps: { rule: string }[];
  reply?: { status?: number; body?: string };
};

export type EndpointRule = {
  version: number;
  type: "endpoint";
  endpoints: EndpointSpec[];
};

export type TraceChunkRef = {
  path: string;
  format: string;
  compression: string;
  record_start?: number;
  record_end?: number;
  node_start?: number;
  node_end?: number;
  bytes?: number;
};

export type TraceDetailRef = {
  layout: string;
  status: string;
  reason?: string[];
  records?: TraceChunkRef[];
  nodes?: TraceChunkRef[];
  finalize?: TraceChunkRef;
};

export type TraceManifest = {
  trace_schema_version: number;
  trace_id: string;
  timestamp?: string;
  status?: string;
  rule?: { name?: string; path?: string; type?: string; version?: number };
  input_format?: string;
  summary?: TraceSummary;
  max_chunk_bytes_uncompressed?: number;
  detail?: TraceDetailRef;
  masking?: { enabled: boolean; rules?: string[] };
  rule_source?: EndpointRule;
};

export type TraceNodeChunkEntry = {
  record_index: number;
  node: TraceNode;
};

export type TracePayload = {
  trace_schema_version?: number;
  trace_id?: string;
  timestamp?: string;
  status?: string;
  rule?: { name?: string; path?: string; type?: string; version?: number };
  rule_source?: EndpointRule;
  detail?: TraceDetailRef;
  max_chunk_bytes_uncompressed?: number;
  masking?: { enabled: boolean; rules?: string[] };
  records?: TraceRecord[];
  finalize?: {
    nodes?: TraceNode[];
    input?: unknown;
    output?: unknown;
    status?: string;
    duration_us?: number;
    duration_ms?: number;
  };
  summary?: TraceSummary;
  input_format?: string;
};

export function parseNumericIndex(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value);
    if (!Number.isNaN(parsed)) return parsed;
  }
  return null;
}

export function normalizeRecordIndex(value: unknown, fallback: number) {
  const parsed = parseNumericIndex(value);
  return parsed ?? fallback;
}

export function normalizeInlineNodeValue(value: unknown, fallbackId: string): TraceNode {
  const node =
    value && typeof value === "object" && !Array.isArray(value)
      ? { ...(value as Record<string, unknown>) }
      : { value };
  if (typeof node.id !== "string" || node.id.length === 0) {
    node.id = fallbackId;
  }
  if (typeof node.kind !== "string" || node.kind.length === 0) {
    node.kind = "value";
  }
  if (typeof node.label !== "string" || node.label.length === 0) {
    node.label =
      typeof node.value === "string" ? node.value : node.kind ?? "value";
  }
  return node as TraceNode;
}

export function normalizeInlineNodes(value: unknown, prefix: string): TraceNode[] {
  if (Array.isArray(value)) {
    return value.map((node, index) =>
      normalizeInlineNodeValue(node, `${prefix}-${index}`)
    );
  }
  if (value === null || value === undefined) {
    return [];
  }
  return [normalizeInlineNodeValue(value, `${prefix}-0`)];
}

export function normalizeRecord(record: TraceRecord, fallbackIndex: number): TraceRecord {
  const index = normalizeRecordIndex(record.index, fallbackIndex);
  const nodes =
    record.nodes === undefined
      ? undefined
      : normalizeInlineNodes(record.nodes as unknown, `record-${index}`);
  return { ...record, index, nodes };
}

export function normalizeTracePayload(trace: TracePayload | null): TracePayload | null {
  if (!trace?.records) return trace;
  return {
    ...trace,
    records: trace.records.map((record, index) => normalizeRecord(record, index))
  };
}

export function mergeNodesIntoRecords(
  records: TraceRecord[],
  nodesByRecord: Map<number, TraceNode[]>
) {
  return records.map((record, position) => {
    const recordIndex = normalizeRecordIndex(record.index, position);
    const nodes = nodesByRecord.get(recordIndex);
    if (!nodes || nodes.length === 0) {
      return { ...record, index: recordIndex };
    }
    const existing = normalizeInlineNodes(record.nodes as unknown, `record-${recordIndex}`);
    return {
      ...record,
      index: recordIndex,
      nodes: [...existing, ...nodes]
    };
  });
}
