import { getApiKey, getInternalKey } from "./auth";
import { getTenantId } from "./tenant";
import {
  normalizeRecord,
  parseNumericIndex,
  type TraceDetailRef,
  type TraceNode,
  type TraceNodeChunkEntry,
  type TracePayload,
  type TraceRecord
} from "./trace_payload";

export const API_BASE = "/api";
export const INTERNAL_BASE = "/internal";

export type FetchAuth = "api" | "internal";

export function buildHeaders(auth: FetchAuth): Record<string, string> {
  const headers: Record<string, string> = { "x-rulemorph-ui": "1" };
  if (auth === "internal") {
    const internalKey = getInternalKey();
    if (internalKey) {
      headers["x-api-key"] = internalKey;
    }
    const tenantId = getTenantId();
    if (tenantId) {
      headers["x-tenant-id"] = tenantId;
    }
    return headers;
  }
  const apiKey = getApiKey();
  if (apiKey) {
    headers["x-api-key"] = apiKey;
  }
  return headers;
}

export async function fetchJson<T>(path: string, auth: FetchAuth = "api"): Promise<T | null> {
  try {
    const headers = buildHeaders(auth);
    const res = await fetch(path, { headers });
    if (!res.ok) return null;
    return (await res.json()) as T;
  } catch (err) {
    console.error("fetch failed", err);
    return null;
  }
}

export async function loadRecordChunks(
  traceId: string,
  detail: TraceDetailRef
): Promise<TraceRecord[]> {
  const chunks = detail.records ?? [];
  const records: TraceRecord[] = [];
  for (let i = 0; i < chunks.length; i += 1) {
    const payload = await fetchJson<{ records: TraceRecord[] }>(
      `${API_BASE}/traces/${traceId}/records/${i}`
    );
    if (!payload) {
      throw new Error(`record chunk ${i} load failed`);
    }
    const offset = records.length;
    const normalized = payload.records.map((record, index) =>
      normalizeRecord(record, offset + index)
    );
    records.push(...normalized);
  }
  return records;
}

export async function loadNodeChunks(
  traceId: string,
  detail: TraceDetailRef
): Promise<Map<number, TraceNode[]>> {
  const chunks = detail.nodes ?? [];
  const nodesByRecord = new Map<number, TraceNode[]>();
  for (let i = 0; i < chunks.length; i += 1) {
    const payload = await fetchJson<{ nodes: TraceNodeChunkEntry[] }>(
      `${API_BASE}/traces/${traceId}/nodes/${i}`
    );
    if (!payload) {
      throw new Error(`node chunk ${i} load failed`);
    }
    payload.nodes.forEach((entry) => {
      const recordIndex = parseNumericIndex(entry.record_index);
      if (recordIndex == null) return;
      const list = nodesByRecord.get(recordIndex) ?? [];
      list.push(entry.node);
      nodesByRecord.set(recordIndex, list);
    });
  }
  return nodesByRecord;
}

export async function loadFinalize(traceId: string) {
  const payload = await fetchJson<{ finalize: TracePayload["finalize"] }>(
    `${API_BASE}/traces/${traceId}/finalize`
  );
  if (!payload) {
    throw new Error("finalize chunk load failed");
  }
  return payload.finalize ?? null;
}
