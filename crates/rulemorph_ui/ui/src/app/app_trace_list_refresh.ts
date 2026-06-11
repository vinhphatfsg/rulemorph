import { Dispatch, SetStateAction } from "react";
import { API_BASE, fetchJson, INTERNAL_BASE } from "../api/api_client";
import type { TraceListItem } from "../trace_list/trace_list_helpers";

type TraceIdSetter = Dispatch<SetStateAction<string | null>>;
type TraceListSetter = Dispatch<SetStateAction<TraceListItem[]>>;

type LoadTraceListArgs = {
  preserveSelection: boolean;
  setTraces: TraceListSetter;
  setSelectedId: TraceIdSetter;
};

export async function loadTraceList({
  preserveSelection,
  setTraces,
  setSelectedId
}: LoadTraceListArgs): Promise<void> {
  const list = await fetchJson<{ traces: TraceListItem[] }>(`${API_BASE}/traces`);
  const data = list?.traces?.length ? list.traces : [];
  setTraces(data);
  setSelectedId((prev) => {
    if (preserveSelection && prev && data.some((item) => item.trace_id === prev)) {
      return prev;
    }
    return data[0]?.trace_id ?? null;
  });
}

type ReconcileFilteredTraceSelectionArgs = {
  selectedId: string | null;
  filteredTraces: TraceListItem[];
  setSelectedId: TraceIdSetter;
};

export function reconcileFilteredTraceSelection({
  selectedId,
  filteredTraces,
  setSelectedId
}: ReconcileFilteredTraceSelectionArgs): void {
  if (!selectedId) {
    if (filteredTraces.length > 0) {
      setSelectedId(filteredTraces[0].trace_id ?? null);
    }
    return;
  }
  if (!filteredTraces.some((item) => item.trace_id === selectedId)) {
    setSelectedId(filteredTraces[0]?.trace_id ?? null);
  }
}

type SubscribeTraceListRefreshArgs = {
  internalKey: string | null;
  loadTraces: (preserveSelection: boolean) => void;
};

export function subscribeTraceListRefresh({
  internalKey,
  loadTraces
}: SubscribeTraceListRefreshArgs): () => void {
  const usePolling = API_BASE.startsWith("/api");
  if (usePolling || internalKey) {
    const timer = window.setInterval(() => {
      loadTraces(true);
    }, 5000);
    return () => {
      window.clearInterval(timer);
    };
  }
  const source = new EventSource(`${INTERNAL_BASE}/stream`);
  const onUpdate = () => {
    loadTraces(true);
  };
  source.addEventListener("traces", onUpdate);
  source.onerror = () => {
    // keep EventSource alive; browser will retry automatically
  };
  return () => {
    source.removeEventListener("traces", onUpdate);
    source.close();
  };
}
