import { useCallback, useEffect, useMemo, useState } from "react";
import ReactFlow, {
  Background,
  Controls,
  Edge,
  Handle,
  Node,
  NodeChange,
  Position,
  useEdgesState,
  useNodesState,
  ReactFlowInstance,
  applyNodeChanges
} from "reactflow";
import "reactflow/dist/style.css";
import clsx from "clsx";
import dagre from "dagre";
type TraceListItem = {
  trace_id: string;
  status?: string;
  timestamp?: string;
  duration_ms?: number;
  rule?: { name?: string; path?: string; type?: string; version?: number };
  summary?: { record_total?: number; record_success?: number; record_failed?: number };
};

type TraceNode = {
  id: string;
  kind: string;
  label: string;
  status?: string;
  input?: unknown;
  output?: unknown;
  pipe_value?: unknown;
  args?: unknown[];
  pipe_steps?: { index: number; label: string; input?: unknown; output?: unknown }[];
  children?: TraceNode[];
  child_trace?: TracePayload;
  error?: { code?: string; message?: string; path?: string };
  meta?: Record<string, unknown>;
};

type TraceRecord = {
  index: number;
  status?: string;
  duration_ms?: number;
  input?: unknown;
  output?: unknown;
  nodes?: TraceNode[];
  error?: { code?: string; message?: string; path?: string };
};

type EndpointSpec = {
  method: string;
  path: string;
  steps: { rule: string }[];
  reply?: { status?: number; body?: string };
};

type EndpointRule = {
  version: number;
  type: "endpoint";
  endpoints: EndpointSpec[];
};

type TraceNodeData = {
  label: string;
};

function DetailNode({ data }: { data: TraceNodeData }) {
  return (
    <div className="trace-node__body">
      <Handle type="target" position={Position.Top} id="top" />
      <Handle type="source" position={Position.Bottom} id="bottom" />
      <Handle type="source" position={Position.Right} id="right" />
      <span>{data.label}</span>
    </div>
  );
}

type TracePayload = {
  trace_id?: string;
  timestamp?: string;
  rule?: { name?: string; path?: string; type?: string; version?: number };
  rule_source?: EndpointRule;
  records?: TraceRecord[];
  finalize?: { nodes?: TraceNode[]; input?: unknown; output?: unknown; status?: string };
};

const API_BASE = "/internal";

const graphDefaults = {
  rankdir: "LR",
  nodesep: 220,
  ranksep: 80
};


async function fetchJson<T>(path: string): Promise<T | null> {
  try {
    const res = await fetch(path);
    if (!res.ok) return null;
    return (await res.json()) as T;
  } catch {
    return null;
  }
}

function formatTime(value?: string) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function formatDuration(value?: number) {
  if (value == null) return "-";
  return `${value} ms`;
}

type OverviewGraph = {
  nodes: Node[];
  edges: Edge[];
  traceMap: Map<string, TracePayload>;
  endpointEdgeLabels: Map<string, string>;
};

type DetailEntry = {
  kind: "step" | "op";
  node: TraceNode;
  parent?: TraceNode;
  ruleId: string;
};

type DetailBundle = {
  nodes: Node[];
  edges: Edge[];
  map: Map<string, DetailEntry>;
  firstId?: string;
  lastId?: string;
  bounds: { minX: number; maxX: number; minY: number; maxY: number };
  refs: { fromId: string; toRule: string }[];
};

function buildOverviewGraph(trace: TracePayload): OverviewGraph {
  const nodes: Node[] = [];
  const edges: Edge[] = [];
  const traceMap = new Map<string, TracePayload>();
  const endpointEdgeLabels = new Map<string, string>();
  const seen = new Map<string, Node>();
  const edgeKeys = new Set<string>();

  const pushNode = (id: string, label: string) => {
    if (seen.has(id)) return;
    const node: Node = {
      id,
      position: { x: 0, y: 0 },
      data: { label },
      type: "default",
      className: "trace-node trace-node--overview",
      style: { width: 240, height: 80 }
    };
    nodes.push(node);
    seen.set(id, node);
  };

  const pushEdge = (from: string, to: string, label?: string) => {
    const key = `${from}::${to}`;
    if (edgeKeys.has(key)) return;
    edgeKeys.add(key);
    edges.push({
      id: `${from}->${to}-${edges.length}`,
      source: from,
      target: to,
      label,
      labelBgPadding: [6, 4],
      labelBgBorderRadius: 8,
      className: label ? "edge--endpoint" : undefined
    });
  };

  const walk = (current: TracePayload, parentPath?: string) => {
    const currentPath = current.rule?.path ?? parentPath ?? "root";
    const isEndpoint = current.rule?.type === "endpoint";
    const endpointRule = (current as TracePayload & { rule_source?: EndpointRule }).rule_source;
    const endpointPaths = endpointRule?.endpoints?.map((endpoint) => ({
      rule: endpoint.steps?.[0]?.rule,
      label: `${endpoint.method} ${endpoint.path}`
    }));
    traceMap.set(currentPath, current);
    pushNode(currentPath, current.rule?.name ?? currentPath);
    if (parentPath && parentPath !== currentPath) {
      pushEdge(parentPath, currentPath);
    }
    const records = current.records ?? [];
    records.forEach((record) => {
      (record.nodes ?? []).forEach((node) => {
        const ruleRef = node.meta && typeof node.meta["rule_ref"] === "string" ? String(node.meta["rule_ref"]) : undefined;
        const childTrace = node.child_trace;
        const childPath = childTrace?.rule?.path ?? ruleRef;
        if (childPath) {
          pushNode(childPath, childTrace?.rule?.name ?? childPath);
          let label: string | undefined;
          if (isEndpoint && endpointPaths) {
            const match = endpointPaths.find((endpoint) => {
              if (!endpoint.rule) return false;
              const normRule = endpoint.rule.replace(/^\.\//, "rules/");
              return normRule === childPath;
            });
            label = match?.label;
          }
          pushEdge(currentPath, childPath, label);
          if (label) {
            endpointEdgeLabels.set(`${currentPath}::${childPath}`, label);
          }
        }
        if (childTrace) {
          walk(childTrace, currentPath);
        }
      });
    });
  };

  walk(trace);
  const layouted = layoutGraph(nodes, edges, graphDefaults.rankdir as "LR" | "TB");
  return { nodes: layouted.nodes, edges: layouted.edges, traceMap, endpointEdgeLabels };
}

function buildDetailBundle(record: TraceRecord | undefined, ruleId: string): DetailBundle {
  const nodes: Node[] = [];
  const edges: Edge[] = [];
  const map = new Map<string, DetailEntry>();
  const refs: { fromId: string; toRule: string }[] = [];
  const recordNodes = record?.nodes ?? [];
  const spacing = 90;
  const stepWidth = 200;
  const opWidth = 160;
  let cursorY = 0;
  let previousId: string | null = null;

  recordNodes.forEach((node, index) => {
    const stepId = `${ruleId}::step-${index}`;
    const stepNodeId = `detail-${stepId}`;
    nodes.push({
      id: stepNodeId,
      position: { x: 0, y: cursorY },
      data: { label: `${node.kind} · ${node.label}` },
      type: "detail",
      className: "trace-node trace-node--detail",
      sourcePosition: Position.Bottom,
      targetPosition: Position.Top,
      style: { width: stepWidth, height: 64 }
    });
    map.set(stepNodeId, { kind: "step", node, ruleId });
    const ruleRef =
      node.meta && typeof node.meta["rule_ref"] === "string" ? String(node.meta["rule_ref"]) : undefined;
    if (ruleRef) {
      refs.push({ fromId: stepNodeId, toRule: ruleRef });
    }

    if (previousId) {
      edges.push({ id: `${previousId}->${stepNodeId}`, source: previousId, target: stepNodeId });
    }

    let lastId = stepNodeId;
    const ops = (node.children ?? []).filter((child) => child.kind === "op");
    ops.forEach((child, opIndex) => {
      cursorY += spacing;
      const opId = `detail-${stepId}::op-${opIndex}`;
      nodes.push({
        id: opId,
        position: { x: (stepWidth - opWidth) / 2, y: cursorY },
        data: { label: child.label },
        type: "detail",
        className: "trace-node trace-node--op",
        sourcePosition: Position.Bottom,
        targetPosition: Position.Top,
        style: { width: opWidth, height: 48 }
      });
      edges.push({ id: `${lastId}->${opId}`, source: lastId, target: opId });
      map.set(opId, { kind: "op", node: child, parent: node, ruleId });
      lastId = opId;
    });

    previousId = lastId;
    cursorY += spacing;
  });

  const bounds = nodes.reduce(
    (acc, node) => {
      const width = typeof node.style?.width === "number" ? node.style.width : 0;
      const height = typeof node.style?.height === "number" ? node.style.height : 0;
      acc.minX = Math.min(acc.minX, node.position.x);
      acc.maxX = Math.max(acc.maxX, node.position.x + width);
      acc.minY = Math.min(acc.minY, node.position.y);
      acc.maxY = Math.max(acc.maxY, node.position.y + height);
      return acc;
    },
    { minX: Infinity, maxX: -Infinity, minY: Infinity, maxY: -Infinity }
  );

  return {
    nodes,
    edges,
    map,
    firstId: nodes[0]?.id,
    lastId: nodes[nodes.length - 1]?.id,
    bounds,
    refs
  };
}

function buildMergedGraph(
  overview: OverviewGraph,
  bundles: Map<string, DetailBundle>,
  expandedRuleIds: string[],
  pinnedPositions: Record<string, { x: number; y: number }>,
  endpointEdgeLabels: Map<string, string>
) {
  const sizeById = new Map<string, { width: number; height: number }>();
  overview.nodes.forEach((node) => {
    sizeById.set(node.id, { width: 240, height: 80 });
  });
  expandedRuleIds.forEach((ruleId) => {
    const bundle = bundles.get(ruleId);
    if (!bundle) return;
    const { minX, maxX, minY, maxY } = bundle.bounds;
    const padding = 36;
    const width = Math.max(320, maxX - minX + padding * 2);
    const height = Math.max(200, maxY - minY + padding * 2);
    sizeById.set(ruleId, { width, height });
  });

  const overviewNodes = overview.nodes.map((node) => {
    const isExpanded = expandedRuleIds.includes(node.id);
    const size = sizeById.get(node.id) ?? { width: 240, height: 80 };
    const pinned = pinnedPositions[node.id];
    return {
      ...node,
      type: "default",
      className: isExpanded
        ? `${node.className ?? ""} trace-node--overview-expanded`.trim()
        : node.className,
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
      position: pinned ? { ...pinned } : node.position,
      style: { width: size.width, height: size.height }
    };
  });
  const layoutedOverview = layoutGraphWithSizes(
    overviewNodes,
    overview.edges,
    graphDefaults.rankdir as "LR" | "TB",
    sizeById,
    expandedRuleIds.length > 0 ? 240 : graphDefaults.nodesep,
    expandedRuleIds.length > 0 ? 140 : graphDefaults.ranksep
  );
  const nodes = layoutedOverview.nodes.map((node) => {
    const pinned = pinnedPositions[node.id];
    return pinned ? { ...node, position: { ...pinned } } : { ...node };
  });
  let edges = overview.edges.map((edge) => ({ ...edge }));

  expandedRuleIds.forEach((ruleId) => {
    const bundle = bundles.get(ruleId);
    const anchorNode = nodes.find((node) => node.id === ruleId);
    if (!bundle || !anchorNode || bundle.nodes.length === 0) {
      return;
    }

    const { minX, maxX, minY } = bundle.bounds;
    const padding = 36;
    const containerSize = sizeById.get(ruleId);
    const detailWidth = maxX - minX;
    const containerWidth = containerSize?.width ?? detailWidth + padding * 2;
    const innerWidth = Math.max(0, containerWidth - padding * 2);
    const offsetX = padding + (innerWidth - detailWidth) / 2 - minX;
    const positionedDetailNodes = bundle.nodes.map((node, index) => ({
      ...node,
      position: {
        x: node.position.x + offsetX,
        y: node.position.y + padding - minY
      },
      parentId: ruleId,
      extent: "parent",
      className: `${node.className ?? ""} trace-node--reveal`.trim(),
      style: {
        ...(node.style ?? {}),
        zIndex: 10,
        animationDelay: `${index * 40}ms`
      }
    }));

    nodes.push(...positionedDetailNodes);
    edges = [...edges, ...bundle.edges];

  });

  const filteredEdges = edges.filter((edge) => {
    if (expandedRuleIds.includes(edge.source)) {
      return false;
    }
    return true;
  });

  const refEdges: Edge[] = [];
  const refEdgeKeys = new Set<string>();
  expandedRuleIds.forEach((ruleId) => {
    const bundle = bundles.get(ruleId);
    if (!bundle) return;
    bundle.refs.forEach((ref) => {
      const key = `${ref.fromId}::${ref.toRule}`;
      if (refEdgeKeys.has(key)) return;
      refEdgeKeys.add(key);
      const label = endpointEdgeLabels.get(`${ruleId}::${ref.toRule}`);
      refEdges.push({
        id: `${ref.fromId}->${ref.toRule}`,
        source: ref.fromId,
        target: ref.toRule,
        sourceHandle: "right",
        label,
        labelBgPadding: label ? [6, 4] : undefined,
        labelBgBorderRadius: label ? 8 : undefined,
        className: label ? "edge--ref edge--endpoint" : "edge--ref"
      });
    });
  });

  return { nodes, edges: [...filteredEdges, ...refEdges] };
}

function layoutGraph(nodes: Node[], edges: Edge[], direction: "LR" | "TB") {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({ rankdir: direction, nodesep: graphDefaults.nodesep, ranksep: graphDefaults.ranksep });

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, { width: 240, height: 80 });
  });
  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(dagreGraph);

  const layouted = nodes.map((node) => {
    const { x, y } = dagreGraph.node(node.id);
    return { ...node, position: { x: x - 110, y: y - 36 } };
  });

  return { nodes: layouted, edges };
}

function layoutGraphWithSizes(
  nodes: Node[],
  edges: Edge[],
  direction: "LR" | "TB",
  sizes: Map<string, { width: number; height: number }>,
  nodesep: number,
  ranksep: number
) {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({ rankdir: direction, nodesep, ranksep });

  nodes.forEach((node) => {
    const size = sizes.get(node.id) ?? { width: 240, height: 80 };
    dagreGraph.setNode(node.id, { width: size.width, height: size.height });
  });
  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(dagreGraph);

  const layouted = nodes.map((node) => {
    const size = sizes.get(node.id) ?? { width: 240, height: 80 };
    const { x, y } = dagreGraph.node(node.id);
    return { ...node, position: { x: x - size.width / 2, y: y - size.height / 2 } };
  });

  return { nodes: layouted, edges };
}

export default function App() {
  const [traces, setTraces] = useState<TraceListItem[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [trace, setTrace] = useState<TracePayload | null>(null);
  const [expandedRuleIds, setExpandedRuleIds] = useState<string[]>([]);
  const [focusedRuleId, setFocusedRuleId] = useState<string | null>(null);
  const [recordIndex, setRecordIndex] = useState(0);
  const [selectedNode, setSelectedNode] = useState<TraceNode | null>(null);
  const [selectedOp, setSelectedOp] = useState<TraceNode | null>(null);
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [flow, setFlow] = useState<ReactFlowInstance | null>(null);
  const [pinnedPositions, setPinnedPositions] = useState<Record<string, { x: number; y: number }>>({});
  const nodeTypes = useMemo(() => ({ detail: DetailNode }), []);

  const loadTraces = useCallback(
    async (preserveSelection: boolean) => {
      const list = await fetchJson<{ traces: TraceListItem[] }>(`${API_BASE}/traces`);
      const data = list?.traces?.length ? list.traces : [];
      setTraces(data);
      setSelectedId((prev) => {
        if (!preserveSelection || !prev) {
          return data[0]?.trace_id ?? null;
        }
        return data.some((item) => item.trace_id === prev) ? prev : data[0]?.trace_id ?? null;
      });
    },
    []
  );

  useEffect(() => {
    loadTraces(false);
  }, [loadTraces]);

  useEffect(() => {
    const source = new EventSource(`${API_BASE}/stream`);
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
  }, [loadTraces]);

  useEffect(() => {
    if (!selectedId) {
      setTrace(null);
      return;
    }
    let mounted = true;
    (async () => {
      const result = await fetchJson<{ trace: TracePayload }>(`${API_BASE}/traces/${selectedId}`);
      const data = result?.trace ?? null;
      if (mounted) {
        setTrace(data);
        setRecordIndex(0);
        setSelectedNode(null);
        setSelectedOp(null);
        setExpandedRuleIds([]);
        setFocusedRuleId(null);
        setInspectorOpen(false);
        setPinnedPositions({});
      }
    })();
    return () => {
      mounted = false;
    };
  }, [selectedId]);

  const overviewGraph = useMemo(
    () =>
      trace
        ? buildOverviewGraph(trace)
        : { nodes: [], edges: [], traceMap: new Map(), endpointEdgeLabels: new Map() },
    [trace]
  );
  const effectiveFocusedRuleId =
    focusedRuleId ?? expandedRuleIds[expandedRuleIds.length - 1] ?? null;
  const currentTrace = effectiveFocusedRuleId
    ? overviewGraph.traceMap.get(effectiveFocusedRuleId) ?? trace
    : trace;
  const currentRecord = currentTrace?.records?.[recordIndex];
  const bundles = useMemo(() => {
    const map = new Map<string, DetailBundle>();
    expandedRuleIds.forEach((ruleId) => {
      const ruleTrace = overviewGraph.traceMap.get(ruleId);
      const record =
        ruleId === effectiveFocusedRuleId
          ? ruleTrace?.records?.[recordIndex]
          : ruleTrace?.records?.[0];
      map.set(ruleId, buildDetailBundle(record, ruleId));
    });
    return map;
  }, [expandedRuleIds, overviewGraph, recordIndex, effectiveFocusedRuleId]);
  const mergedGraph = useMemo(
    () =>
      buildMergedGraph(
        overviewGraph,
        bundles,
        expandedRuleIds,
        pinnedPositions,
        overviewGraph.endpointEdgeLabels
      ),
    [overviewGraph, bundles, expandedRuleIds, pinnedPositions]
  );
  const [nodes, setNodes] = useNodesState(mergedGraph.nodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(mergedGraph.edges);

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
    },
    [setNodes]
  );

  useEffect(() => {
    setNodes((prev) => {
      const prevById = new Map(prev.map((node) => [node.id, node]));
      return mergedGraph.nodes.map((node) => {
        const existing = prevById.get(node.id);
        const isOverview = node.className?.includes("trace-node--overview");
        const pinned = pinnedPositions[node.id];
        if (existing && isOverview && pinned) {
          return { ...node, position: existing.position };
        }
        return node;
      });
    });
    setEdges(mergedGraph.edges);
  }, [mergedGraph.nodes, mergedGraph.edges, pinnedPositions, setNodes, setEdges]);

  useEffect(() => {
    if (!flow) return;
    flow.fitView({ padding: 0.22 });
  }, [flow, trace]);
  const detailNodeMap = useMemo(() => {
    const map = new Map<string, DetailEntry>();
    bundles.forEach((bundle) => {
      bundle.map.forEach((entry, nodeId) => {
        map.set(nodeId, entry);
      });
    });
    return map;
  }, [bundles]);
  const hasDetail = expandedRuleIds.length > 0;

  return (
    <div className="app">
      <div className="app__glow" />
      <header className="topbar">
        <div className="title-chip">
          <span className="title-chip__dot" />
          <span className="title-chip__label">Rulemorph Trace</span>
          <span className="title-chip__id">{currentTrace?.rule?.path ?? currentTrace?.trace_id ?? "no-trace"}</span>
        </div>
        <div className="topbar__meta">
          <span className="meta-pill">{hasDetail ? "detail" : "overview"}</span>
          <span className="meta-pill">{traces.length} traces</span>
          <span className="meta-pill">record #{currentRecord?.index ?? 0}</span>
        </div>
      </header>

      <main className="stage">
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
              } else {
                setSelectedNode(detailEntry.node);
                setSelectedOp(detailEntry.node.children?.find((child) => child.kind === "op") ?? null);
              }
              setInspectorOpen(true);
            }}
          >
            <Background gap={hasDetail ? 28 : 32} size={1} />
            <Controls />
          </ReactFlow>
        </div>

        <aside className="floating-panel trace-panel">
          <div className="panel__header">
            <h2>Trace一覧</h2>
            <p>最新順</p>
          </div>
          <div className="trace-list">
            {traces.length === 0 && (
              <div className="empty-trace">
                <p>traces が見つかりません。</p>
                <p className="muted">
                  data_dir（既定: ~/.rulemorph）の traces/ に JSON を配置してください。
                </p>
              </div>
            )}
            {traces.map((item) => (
              <button
                key={item.trace_id}
                className={clsx("trace-card", selectedId === item.trace_id && "is-active")}
                onClick={() => setSelectedId(item.trace_id)}
              >
                <div>
                  <span className="chip">{item.status ?? "ok"}</span>
                  <h3>{item.rule?.name ?? item.trace_id}</h3>
                  <p className="muted">{item.rule?.path ?? "(no path)"}</p>
                </div>
                <div className="trace-meta">
                  <div>
                    <span>time</span>
                    <strong>{formatTime(item.timestamp)}</strong>
                  </div>
                  <div>
                    <span>duration</span>
                    <strong>{formatDuration(item.duration_ms)}</strong>
                  </div>
                </div>
              </button>
            ))}
          </div>
        </aside>

        {hasDetail && (
          <>
            <aside className="floating-panel record-panel">
              <div className="panel__header">
                <h2>Records</h2>
                <p>{currentTrace?.records?.length ?? 0} total</p>
              </div>
              <div className="record-list">
                {(currentTrace?.records ?? []).map((record, idx) => (
                  <button
                    key={record.index}
                    className={clsx("record-card", recordIndex === idx && "is-active")}
                    onClick={() => {
                      setRecordIndex(idx);
                      setSelectedNode(null);
                      setSelectedOp(null);
                      setInspectorOpen(false);
                    }}
                  >
                    <span>#{record.index}</span>
                    <span className="muted">{record.status ?? "ok"}</span>
                    <span>{formatDuration(record.duration_ms)}</span>
                  </button>
                ))}
              </div>
            </aside>
          </>
        )}

        <button
          className={clsx("inspector-toggle", inspectorOpen && "is-open")}
          onClick={() => setInspectorOpen((prev) => !prev)}
        >
          {inspectorOpen ? "Inspectorを閉じる" : "Inspectorを見る"}
        </button>

        <aside className={clsx("inspector-drawer", inspectorOpen && "is-open")}>
          <div className="inspector__header">
            <div>
              <h2>Inspector</h2>
              <p>{selectedNode ? selectedNode.label : "ノードを選択して詳細を表示"}</p>
            </div>
            <button className="icon-button" onClick={() => setInspectorOpen(false)}>
              ×
            </button>
          </div>

          <div className="inspector__section inspector__section--oplist">
            <h3>OP一覧</h3>
            <div className="op-list">
              {(selectedNode?.children ?? []).length === 0 && (
                <p className="muted">このノードにOPはありません。</p>
              )}
              {(selectedNode?.children ?? []).map((child) => (
                <button
                  key={child.id}
                  className={clsx("op-item", selectedOp?.id === child.id && "is-active")}
                  onClick={() => setSelectedOp(child)}
                >
                  <span>{child.label}</span>
                  <span className="muted">{child.meta?.op ?? "op"}</span>
                </button>
              ))}
            </div>
          </div>

          <div className="inspector__section inspector__section--opresult">
            <h3>OP結果</h3>
            <div className="inspector__content">
              {(() => {
                const op = selectedOp as any;
                const input = op?.input ?? null;
                const pipe = op?.pipe_value ?? null;
                const args = op?.args ?? null;
                const output = op?.output ?? null;
                const pipeSteps = (op?.pipe_steps ?? []) as {
                  index: number;
                  label: string;
                  input?: unknown;
                  output?: unknown;
                }[];
                const renderBlock = (label: string, value: unknown) => {
                  const hasValue = !(value === null || value === undefined);
                  const isWide = label === "input" || label === "output";
                  return (
                    <div
                      className={clsx("inspector-block", isWide && "inspector-block--wide")}
                      key={label}
                    >
                  <div className="inspector-block__header">
                    <div className="inspector-block__title">
                      <span className="inspector-block__line" />
                      <span className="inspector-block__name">{label}</span>
                    </div>
                    <span className="inspector-block__meta">{hasValue ? "json" : "empty"}</span>
                  </div>
                  <pre className="inspector-block__body">
                        {hasValue ? JSON.stringify(value, null, 2) : "なし"}
                  </pre>
                </div>
              );
            };
                return (
                  <>
                    <div className="inspector-grid">
                      {renderBlock("input", input)}
                      {renderBlock("pipe", pipe)}
                      {renderBlock("args", args)}
                      {renderBlock("output", output)}
                    </div>
                    <div className="pipe-steps">
                      <div className="pipe-steps__header">
                        <h4>ステップ推移</h4>
                        <span className="muted">{pipeSteps.length} steps</span>
                      </div>
                      {pipeSteps.length === 0 ? (
                        <p className="muted">ステップがありません。</p>
                      ) : (
                        <div className="pipe-steps__list">
                          {pipeSteps.map((step) => (
                            <div className="pipe-step" key={step.index}>
                              <div className="pipe-step__title">
                                <span className="pipe-step__index">#{step.index}</span>
                                <span className="pipe-step__label">{step.label}</span>
                              </div>
                              <div className="pipe-step__io">
                                <div className="pipe-step__cell">
                                  <span className="pipe-step__name">input</span>
                                  <pre>{step.input !== undefined ? JSON.stringify(step.input) : "なし"}</pre>
                                </div>
                                <div className="pipe-step__cell">
                                  <span className="pipe-step__name">output</span>
                                  <pre>{step.output !== undefined ? JSON.stringify(step.output) : "なし"}</pre>
                                </div>
                              </div>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  </>
                );
              })()}
            </div>
          </div>
        </aside>
      </main>
    </div>
  );
}
