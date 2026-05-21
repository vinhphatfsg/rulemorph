import { Dispatch, SetStateAction, useCallback, useEffect, useMemo, useRef, useState } from "react";
import ReactFlow, {
  Background,
  Controls,
  Edge,
  Node,
  NodeChange,
  ReactFlowInstance,
  applyNodeChanges,
  useEdgesState,
  useNodesState
} from "reactflow";
import { shouldResetInitialCenter } from "./view_mode";
import { type TraceNode, type TracePayload } from "./trace_payload";
import {
  DetailNode,
  getNodesBounds,
  type ApiDetailEntry,
  type ApiGraphNode,
  type ApiGraphOp,
  type DetailEntry,
  type OverviewGraph
} from "./trace_graph";

const INITIAL_CENTER_X_RATIO = 0.45;
const INITIAL_CENTER_PADDING = 0.22;

type ViewMode = "trace" | "api";

type TraceCanvasProps = {
  viewMode: ViewMode;
  trace: TracePayload | null;
  traceResetKey: string | null;
  activeGraph: { nodes: Node[]; edges: Edge[] };
  hasDetail: boolean;
  apiHasDetail: boolean;
  overviewGraph: OverviewGraph;
  expandedRuleIds: string[];
  setExpandedRuleIds: Dispatch<SetStateAction<string[]>>;
  setFocusedRuleId: Dispatch<SetStateAction<string | null>>;
  setRecordIndex: Dispatch<SetStateAction<number>>;
  setSelectedNode: Dispatch<SetStateAction<TraceNode | null>>;
  setSelectedOp: Dispatch<SetStateAction<TraceNode | null>>;
  setInspectorOpen: Dispatch<SetStateAction<boolean>>;
  setTraceInspectorSections: Dispatch<
    SetStateAction<{
      finalize: boolean;
      step: boolean;
      opList: boolean;
      opResult: boolean;
    }>
  >;
  detailNodeMap: Map<string, DetailEntry>;
  apiGraphNodeMap: Map<string, ApiGraphNode>;
  apiDetailNodeMap: Map<string, ApiDetailEntry>;
  apiExpandedRuleIds: string[];
  setApiExpandedRuleIds: Dispatch<SetStateAction<string[]>>;
  setApiFocusedRuleId: Dispatch<SetStateAction<string | null>>;
  setSelectedApiNode: Dispatch<SetStateAction<ApiGraphNode | null>>;
  setSelectedApiOp: Dispatch<SetStateAction<ApiGraphOp | null>>;
  pinnedPositions: Record<string, { x: number; y: number }>;
  setPinnedPositions: Dispatch<SetStateAction<Record<string, { x: number; y: number }>>>;
  apiPinnedPositions: Record<string, { x: number; y: number }>;
  setApiPinnedPositions: Dispatch<SetStateAction<Record<string, { x: number; y: number }>>>;
};

export function TraceCanvas({
  viewMode,
  trace,
  traceResetKey,
  activeGraph,
  hasDetail,
  apiHasDetail,
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
  setSelectedApiOp,
  pinnedPositions,
  setPinnedPositions,
  apiPinnedPositions,
  setApiPinnedPositions
}: TraceCanvasProps) {
  const [flow, setFlow] = useState<ReactFlowInstance | null>(null);
  const initialCenterAppliedRef = useRef(false);
  const prevViewModeRef = useRef<ViewMode>(viewMode);
  const nodeTypes = useMemo(() => ({ detail: DetailNode }), []);
  const [nodes, setNodes] = useNodesState(activeGraph.nodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(activeGraph.edges);

  const onNodesChange = useCallback(
    (changes: NodeChange[]) => {
      let nextNodes: Node[] = [];
      setNodes((prev) => {
        nextNodes = applyNodeChanges(changes, prev);
        return nextNodes;
      });
      const settledMoves = changes.filter(
        (change) => change.type === "position" && change.dragging === false
      );
      if (settledMoves.length > 0) {
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
        } else {
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
      }
    },
    [setApiPinnedPositions, setNodes, setPinnedPositions, viewMode]
  );

  useEffect(() => {
    setNodes((prev) => {
      const prevById = new Map(prev.map((node) => [node.id, node]));
      return activeGraph.nodes.map((node) => {
        const existing = prevById.get(node.id);
        const isOverview = node.className?.includes("trace-node--overview");
        const pinned = viewMode === "api" ? apiPinnedPositions[node.id] : pinnedPositions[node.id];
        if (existing && isOverview) {
          return { ...node, position: existing.position };
        }
        if (isOverview && pinned) {
          return { ...node, position: pinned };
        }
        return node;
      });
    });
    setEdges(activeGraph.edges);
  }, [activeGraph.nodes, activeGraph.edges, pinnedPositions, apiPinnedPositions, viewMode, setNodes, setEdges]);

  useEffect(() => {
    if (shouldResetInitialCenter(prevViewModeRef.current, viewMode)) {
      initialCenterAppliedRef.current = false;
    }
    prevViewModeRef.current = viewMode;
  }, [viewMode]);

  useEffect(() => {
    initialCenterAppliedRef.current = false;
  }, [traceResetKey]);

  useEffect(() => {
    if (!flow) return;
    if (viewMode === "api") {
      flow.fitView({ padding: INITIAL_CENTER_PADDING });
      return;
    }
    if (!trace) return;
    if (nodes.length === 0) return;
    if (initialCenterAppliedRef.current) return;
    requestAnimationFrame(() => {
      const overviewNodes = nodes.filter((node) =>
        node.className?.includes("trace-node--overview")
      );
      if (overviewNodes.length === 0) return;
      const container = document.querySelector(".trace-canvas");
      if (!container) return;
      const { width, height } = container.getBoundingClientRect();
      if (!width || !height) return;
      const bounds = getNodesBounds(overviewNodes);
      const availableWidth = width * (1 - INITIAL_CENTER_PADDING * 2);
      const availableHeight = height * (1 - INITIAL_CENTER_PADDING * 2);
      const zoom = Math.min(availableWidth / bounds.width, availableHeight / bounds.height);
      const centerX = bounds.minX + bounds.width / 2;
      const centerY = bounds.minY + bounds.height / 2;
      const desiredCenterX = width * INITIAL_CENTER_X_RATIO;
      const desiredCenterY = height * 0.5;
      const x = desiredCenterX - centerX * zoom;
      const y = desiredCenterY - centerY * zoom;
      flow.setViewport({ x, y, zoom }, { duration: 0 });
      initialCenterAppliedRef.current = true;
    });
  }, [flow, trace, viewMode, nodes.length]);

  return (
    <div className="trace-canvas">
      <ReactFlow
        key="canvas"
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitViewOptions={{ padding: 0.22 }}
        nodesDraggable
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onInit={setFlow}
        onNodeClick={(_, node) => {
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
        }}
      >
        <Background gap={hasDetail || apiHasDetail ? 28 : 32} size={1} />
        <Controls />
      </ReactFlow>
    </div>
  );
}
