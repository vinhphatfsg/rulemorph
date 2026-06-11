import type { Dispatch, SetStateAction } from "react";
import { useEffect, useState } from "react";
import type { ApiGraphNode, ApiGraphOp, ApiGraphResponse } from "../trace_graph/trace_graph";
import { loadApiGraph, resetApiGraphSelection } from "./app_api_graph_state";
import type { ViewMode } from "./view_mode";

type NodePositions = Record<string, { x: number; y: number }>;

type ApiGraphStateArgs = {
  viewMode: ViewMode;
  setInspectorOpen: Dispatch<SetStateAction<boolean>>;
};

export function useApiGraphState({ viewMode, setInspectorOpen }: ApiGraphStateArgs) {
  const [apiGraph, setApiGraph] = useState<ApiGraphResponse | null>(null);
  const [selectedApiNode, setSelectedApiNode] = useState<ApiGraphNode | null>(null);
  const [selectedApiOp, setSelectedApiOp] = useState<ApiGraphOp | null>(null);
  const [apiExpandedRuleIds, setApiExpandedRuleIds] = useState<string[]>([]);
  const [apiFocusedRuleId, setApiFocusedRuleId] = useState<string | null>(null);
  const [apiPinnedPositions, setApiPinnedPositions] = useState<NodePositions>({});

  useEffect(() => {
    if (viewMode !== "api") return;
    void loadApiGraph({ setApiGraph });
  }, [viewMode]);

  useEffect(() => {
    if (viewMode !== "api") return;
    resetApiGraphSelection({
      setSelectedApiNode,
      setSelectedApiOp,
      setApiExpandedRuleIds,
      setApiFocusedRuleId,
      setInspectorOpen
    });
  }, [setInspectorOpen, viewMode]);

  return {
    apiGraph,
    selectedApiNode,
    setSelectedApiNode,
    selectedApiOp,
    setSelectedApiOp,
    apiExpandedRuleIds,
    setApiExpandedRuleIds,
    apiFocusedRuleId,
    setApiFocusedRuleId,
    apiPinnedPositions,
    setApiPinnedPositions
  };
}
