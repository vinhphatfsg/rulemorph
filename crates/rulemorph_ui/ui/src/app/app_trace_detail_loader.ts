import {
  API_BASE,
  fetchJson,
  loadFinalize,
  loadNodeChunks,
  loadRecordChunks
} from "../api/api_client";
import {
  mergeNodesIntoRecords,
  normalizeTracePayload,
  type TraceManifest,
  type TracePayload
} from "../api/trace_payload";

type TraceSetter = (
  value: TracePayload | null | ((prev: TracePayload | null) => TracePayload | null)
) => void;

type LoadSelectedTraceDetailArgs = {
  selectedId: string;
  isMounted: () => boolean;
  setTrace: TraceSetter;
  setTraceManifest: (manifest: TraceManifest | null) => void;
  setDetailLoading: (loading: boolean) => void;
  setDetailError: (message: string | null) => void;
};

function buildBaseTrace(manifest: TraceManifest): TracePayload {
  return {
    trace_id: manifest.trace_id,
    timestamp: manifest.timestamp,
    status: manifest.status,
    rule: manifest.rule,
    rule_source: manifest.rule_source,
    records: [],
    finalize: undefined,
    summary: manifest.summary,
    input_format: manifest.input_format
  };
}

export async function loadSelectedTraceDetail({
  selectedId,
  isMounted,
  setTrace,
  setTraceManifest,
  setDetailLoading,
  setDetailError
}: LoadSelectedTraceDetailArgs): Promise<void> {
  const manifestResult = await fetchJson<{ manifest: TraceManifest }>(
    `${API_BASE}/traces/${selectedId}/manifest`
  );
  if (!isMounted()) return;
  if (!manifestResult?.manifest) {
    const result = await fetchJson<{ trace: TracePayload }>(`${API_BASE}/traces/${selectedId}`);
    if (!isMounted()) return;
    setTrace(normalizeTracePayload(result?.trace ?? null));
    return;
  }
  const manifest = manifestResult.manifest;
  setTraceManifest(manifest);
  const baseTrace = buildBaseTrace(manifest);
  setTrace(baseTrace);
  const detail = manifest.detail;
  if (!detail || detail.status !== "full") {
    return;
  }
  setDetailLoading(true);
  try {
    const records = await loadRecordChunks(selectedId, detail);
    if (!isMounted()) return;
    let nextTrace: TracePayload = { ...baseTrace, records };
    setTrace(nextTrace);
    if (detail.layout === "records_nodes_split" && (detail.nodes?.length ?? 0) > 0) {
      const nodesByRecord = await loadNodeChunks(selectedId, detail);
      if (!isMounted()) return;
      const mergedRecords = mergeNodesIntoRecords(records, nodesByRecord);
      nextTrace = { ...nextTrace, records: mergedRecords };
      setTrace(nextTrace);
    }
    if (detail.finalize) {
      const finalize = await loadFinalize(selectedId);
      if (!isMounted()) return;
      if (finalize) {
        setTrace((prev) => (prev ? { ...prev, finalize } : { ...nextTrace, finalize }));
      }
    }
  } catch (err) {
    if (!isMounted()) return;
    const fallback = await fetchJson<{ trace: TracePayload }>(`${API_BASE}/traces/${selectedId}`);
    if (!isMounted()) return;
    if (fallback?.trace) {
      setTraceManifest(null);
      setTrace(normalizeTracePayload(fallback.trace));
      setDetailError(null);
      return;
    }
    setDetailError("trace detail load failed");
  } finally {
    if (isMounted()) {
      setDetailLoading(false);
    }
  }
}
