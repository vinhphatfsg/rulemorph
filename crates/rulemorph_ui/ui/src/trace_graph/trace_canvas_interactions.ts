import { Dispatch, SetStateAction } from "react";
import { Node, NodeChange } from "reactflow";
import { type TraceNode } from "../api/trace_payload";
import {
  type ApiDetailEntry,
  type ApiGraphNode,
  type ApiGraphOp,
  type DetailEntry,
  type OverviewGraph
} from "./trace_graph";

type ViewMode = "trace" | "api";

type Position = { x: number; y: number };

export type TraceInspectorSections = {
  finalize: boolean;
  step: boolean;
  opList: boolean;
  opResult: boolean;
};

type PersistSettledNodeMovesArgs = {
  changes: NodeChange[];
  nextNodes: Node[];
  viewMode: ViewMode;
  setPinnedPositions: Dispatch<SetStateAction<Record<string, Position>>>;
  setApiPinnedPositions: Dispatch<SetStateAction<Record<string, Position>>>;
};

export function persistSettledNodeMoves({
  changes,
  nextNodes,
  viewMode,
  setPinnedPositions,
  setApiPinnedPositions
}: PersistSettledNodeMovesArgs) {
  const settledMoves = changes.filter(
    (change) => change.type === "position" && change.dragging === false
  );
  if (settledMoves.length === 0) {
    return;
  }
  const nextById = new Map(nextNodes.map((node) => [node.id, node]));
  if (viewMode === "api") {
    setApiPinnedPositions((prev) => {
      const next = { ...prev };
      settledMoves.forEach((change) => {
        const node = nextById.get(change.id);
        if (!node) return;
        next[change.id] = { ...node.position };
      });
      return next;
    });
    return;
  }
  setPinnedPositions((prev) => {
    const next = { ...prev };
    settledMoves.forEach((change) => {
      const node = nextById.get(change.id);
      if (!node) return;
      next[change.id] = { ...node.position };
    });
    return next;
  });
}

type HandleTraceCanvasNodeClickArgs = {
  viewMode: ViewMode;
  node: Node;
  overviewGraph: OverviewGraph;
  expandedRuleIds: string[];
  setExpandedRuleIds: Dispatch<SetStateAction<string[]>>;
  setFocusedRuleId: Dispatch<SetStateAction<string | null>>;
  setRecordIndex: Dispatch<SetStateAction<number>>;
  setSelectedNode: Dispatch<SetStateAction<TraceNode | null>>;
  setSelectedOp: Dispatch<SetStateAction<TraceNode | null>>;
  setInspectorOpen: Dispatch<SetStateAction<boolean>>;
  setTraceInspectorSections: Dispatch<SetStateAction<TraceInspectorSections>>;
  detailNodeMap: Map<string, DetailEntry>;
  apiGraphNodeMap: Map<string, ApiGraphNode>;
  apiDetailNodeMap: Map<string, ApiDetailEntry>;
  apiExpandedRuleIds: string[];
  setApiExpandedRuleIds: Dispatch<SetStateAction<string[]>>;
  setApiFocusedRuleId: Dispatch<SetStateAction<string | null>>;
  setSelectedApiNode: Dispatch<SetStateAction<ApiGraphNode | null>>;
  setSelectedApiOp: Dispatch<SetStateAction<ApiGraphOp | null>>;
};

export function handleTraceCanvasNodeClick({
  viewMode,
  node,
  overviewGraph,
  expandedRuleIds,
  setExpandedRuleIds,
  setFocusedRuleId,
  setRecordIndex,
  setSelectedNode,
  setSelectedOp,
  setInspectorOpen,
  setTraceInspectorSections,
  detailNodeMap,
  apiGraphNodeMap,
  apiDetailNodeMap,
  apiExpandedRuleIds,
  setApiExpandedRuleIds,
  setApiFocusedRuleId,
  setSelectedApiNode,
  setSelectedApiOp
}: HandleTraceCanvasNodeClickArgs) {
  if (viewMode === "api") {
    const apiNode = apiGraphNodeMap.get(node.id);
    if (apiNode) {
      const alreadyExpanded = apiExpandedRuleIds.includes(node.id);
      if (alreadyExpanded) {
        setApiExpandedRuleIds((prev) => {
          const next = prev.filter((id) => id !== node.id);
          setApiFocusedRuleId(next[next.length - 1] ?? null);
          return next;
        });
        setSelectedApiNode(null);
        setSelectedApiOp(null);
        setInspectorOpen(false);
      } else {
        setApiExpandedRuleIds((prev) => [...prev, node.id]);
        setApiFocusedRuleId(node.id);
        setSelectedApiNode(apiNode);
        setSelectedApiOp(null);
        setInspectorOpen(false);
      }
      return;
    }
    const apiDetail = apiDetailNodeMap.get(node.id);
    if (!apiDetail) return;
    const parent = apiGraphNodeMap.get(apiDetail.ruleId);
    if (parent) {
      setSelectedApiNode(parent);
      setSelectedApiOp(apiDetail.node);
    }
    setInspectorOpen(true);
    return;
  }
  const nextTrace = overviewGraph.traceMap.get(node.id);
  if (nextTrace) {
    const alreadyExpanded = expandedRuleIds.includes(node.id);
    if (alreadyExpanded) {
      setExpandedRuleIds((prev) => {
        const next = prev.filter((id) => id !== node.id);
        setFocusedRuleId(next[next.length - 1] ?? null);
        return next;
      });
      setRecordIndex(0);
      setSelectedNode(null);
      setSelectedOp(null);
      setInspectorOpen(false);
    } else {
      setExpandedRuleIds((prev) => [...prev, node.id]);
      setFocusedRuleId(node.id);
      setRecordIndex(0);
      setSelectedNode(null);
      setSelectedOp(null);
      setInspectorOpen(false);
    }
    return;
  }
  const detailEntry = detailNodeMap.get(node.id);
  if (!detailEntry) return;
  setFocusedRuleId(detailEntry.ruleId);
  if (detailEntry.kind === "op") {
    setSelectedNode(detailEntry.parent ?? null);
    setSelectedOp(detailEntry.node);
    setTraceInspectorSections((prev) => ({ ...prev, opResult: true }));
  } else {
    setSelectedNode(detailEntry.node);
    setSelectedOp(detailEntry.node.children?.find((child) => child.kind === "op") ?? null);
  }
  setInspectorOpen(true);
}
