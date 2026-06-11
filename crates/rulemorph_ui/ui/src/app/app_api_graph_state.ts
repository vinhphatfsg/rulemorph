import type { Dispatch, SetStateAction } from "react";
import { API_BASE, fetchJson } from "../api/api_client";
import type { ApiGraphNode, ApiGraphOp, ApiGraphResponse } from "../trace_graph/trace_graph";

type LoadApiGraphArgs = {
  setApiGraph: Dispatch<SetStateAction<ApiGraphResponse | null>>;
};

export async function loadApiGraph({ setApiGraph }: LoadApiGraphArgs): Promise<void> {
  const data = await fetchJson<ApiGraphResponse>(`${API_BASE}/api-graph`);
  if (data) {
    setApiGraph(data);
  }
}

type ResetApiGraphSelectionArgs = {
  setSelectedApiNode: Dispatch<SetStateAction<ApiGraphNode | null>>;
  setSelectedApiOp: Dispatch<SetStateAction<ApiGraphOp | null>>;
  setApiExpandedRuleIds: Dispatch<SetStateAction<string[]>>;
  setApiFocusedRuleId: Dispatch<SetStateAction<string | null>>;
  setInspectorOpen: Dispatch<SetStateAction<boolean>>;
};

export function resetApiGraphSelection({
  setSelectedApiNode,
  setSelectedApiOp,
  setApiExpandedRuleIds,
  setApiFocusedRuleId,
  setInspectorOpen
}: ResetApiGraphSelectionArgs): void {
  setSelectedApiNode(null);
  setSelectedApiOp(null);
  setApiExpandedRuleIds([]);
  setApiFocusedRuleId(null);
  setInspectorOpen(false);
}
