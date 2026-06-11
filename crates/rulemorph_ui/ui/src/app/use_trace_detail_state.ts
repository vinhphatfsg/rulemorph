import type { Dispatch, SetStateAction } from "react";
import { useEffect, useState } from "react";
import type { TraceManifest, TraceNode, TracePayload } from "../api/trace_payload";
import { loadTraceDetailForSelection } from "./app_trace_detail_state";

type NodePositions = Record<string, { x: number; y: number }>;

type TraceDetailStateArgs = {
  selectedId: string | null;
  setInspectorOpen: Dispatch<SetStateAction<boolean>>;
};

export function useTraceDetailState({ selectedId, setInspectorOpen }: TraceDetailStateArgs) {
  const [trace, setTrace] = useState<TracePayload | null>(null);
  const [traceManifest, setTraceManifest] = useState<TraceManifest | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [expandedRuleIds, setExpandedRuleIds] = useState<string[]>([]);
  const [focusedRuleId, setFocusedRuleId] = useState<string | null>(null);
  const [recordIndex, setRecordIndex] = useState(0);
  const [selectedNode, setSelectedNode] = useState<TraceNode | null>(null);
  const [selectedOp, setSelectedOp] = useState<TraceNode | null>(null);
  const [pinnedPositions, setPinnedPositions] = useState<NodePositions>({});

  useEffect(() => {
    return loadTraceDetailForSelection({
      selectedId,
      setTrace,
      setTraceManifest,
      setDetailLoading,
      setDetailError,
      setRecordIndex,
      setSelectedNode,
      setSelectedOp,
      setExpandedRuleIds,
      setFocusedRuleId,
      setInspectorOpen,
      setPinnedPositions
    });
  }, [selectedId, setInspectorOpen]);

  return {
    trace,
    traceManifest,
    detailLoading,
    detailError,
    expandedRuleIds,
    setExpandedRuleIds,
    focusedRuleId,
    setFocusedRuleId,
    recordIndex,
    setRecordIndex,
    selectedNode,
    setSelectedNode,
    selectedOp,
    setSelectedOp,
    pinnedPositions,
    setPinnedPositions
  };
}
