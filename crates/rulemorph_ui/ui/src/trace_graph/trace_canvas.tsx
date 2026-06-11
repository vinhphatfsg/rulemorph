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
import { shouldResetInitialCenter } from "../app/view_mode";
import { type TraceNode, type TracePayload } from "../api/trace_payload";
import {
  DetailNode,
  getNodesBounds,
  type ApiDetailEntry,
  type ApiGraphNode,
  type ApiGraphOp,
  type DetailEntry,
  type OverviewGraph
} from "./trace_graph";
import {
  handleTraceCanvasNodeClick,
  persistSettledNodeMoves,
  type TraceInspectorSections
} from "./trace_canvas_interactions";

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
  setTraceInspectorSections: Dispatch<SetStateAction<TraceInspectorSections>>;
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
      persistSettledNodeMoves({
        changes,
        nextNodes,
        viewMode,
        setPinnedPositions,
        setApiPinnedPositions
      });
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
        onNodeClick={(_, node) =>
          handleTraceCanvasNodeClick({
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
          })
        }
      >
        <Background gap={hasDetail || apiHasDetail ? 28 : 32} size={1} />
        <Controls />
      </ReactFlow>
    </div>
  );
}
