import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
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
import { shouldResetInitialCenter } from "./view_mode";
type TraceListItem = {
  trace_id: string;
  status?: string;
  timestamp?: string;
  duration_us?: number;
  duration_ms?: number;
  rule?: { name?: string; path?: string; type?: string; version?: number };
  summary?: { record_total?: number; record_success?: number; record_failed?: number };
};

export type TraceNode = {
  id: string;
  kind: string;
  label: string;
  status?: string;
  duration_us?: number;
  duration_ms?: number;
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

export type TraceRecord = {
  index: number;
  status?: string;
  duration_us?: number;
  duration_ms?: number;
  input?: unknown;
  output?: unknown;
  nodes?: TraceNode[];
  error?: { code?: string; message?: string; path?: string };
};

type DurationUnit = "us" | "ms";

export type EndpointSpec = {
  method: string;
  path: string;
  steps: { rule: string }[];
  reply?: { status?: number; body?: string };
};

export type EndpointRule = {
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

export type TracePayload = {
  trace_id?: string;
  timestamp?: string;
  rule?: { name?: string; path?: string; type?: string; version?: number };
  rule_source?: EndpointRule;
  records?: TraceRecord[];
  finalize?: { nodes?: TraceNode[]; input?: unknown; output?: unknown; status?: string };
};

type ApiGraphOp = {
  label: string;
  detail?: string;
  refs?: string[];
};

type ApiGraphNode = {
  id: string;
  label: string;
  kind: string;
  path: string;
  ops: ApiGraphOp[];
};

type ApiGraphEdge = {
  source: string;
  target: string;
  label?: string;
  kind: string;
};

type ApiGraphResponse = {
  nodes: ApiGraphNode[];
  edges: ApiGraphEdge[];
};

const API_BASE = "/internal";

const graphDefaults = {
  rankdir: "LR",
  nodesep: 220,
  ranksep: 80
};

const INITIAL_CENTER_X_RATIO = 0.45;
const INITIAL_CENTER_PADDING = 0.22;

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

function formatDurationParts(valueUs: number | undefined, unit: DurationUnit) {
  if (valueUs == null) return null;
  if (unit === "ms") {
    const valueMs = valueUs / 1000;
    const formatted =
      valueMs >= 100 ? valueMs.toFixed(0) : valueMs >= 10 ? valueMs.toFixed(1) : valueMs.toFixed(2);
    return { value: formatted, unit: "ms" as const };
  }
  return { value: `${valueUs}`, unit: "μs" as const };
}

function formatDuration(valueUs: number | undefined, unit: DurationUnit) {
  const parts = formatDurationParts(valueUs, unit);
  return parts ? `${parts.value} ${parts.unit}` : "-";
}

function resolveDurationUs(durationUs?: number, durationMs?: number) {
  if (typeof durationUs === "number") return durationUs;
  if (typeof durationMs === "number") return durationMs * 1000;
  return undefined;
}

function resolveTraceDurationUs(trace?: TracePayload) {
  if (!trace) return undefined;
  const summary = (trace as TracePayload & {
    summary?: { duration_us?: number; duration_ms?: number };
  }).summary;
  const fromSummary = resolveDurationUs(summary?.duration_us, summary?.duration_ms);
  if (fromSummary !== undefined) return fromSummary;
  const record = trace.records?.[0];
  return resolveDurationUs(record?.duration_us, record?.duration_ms);
}

function isErrorStatus(status?: string) {
  return status?.toLowerCase() === "error";
}

function formatEdgeDurationMs(valueUs: number | undefined) {
  if (valueUs == null) return "-";
  const valueMs = valueUs / 1000;
  const formatted = valueMs >= 100 ? valueMs.toFixed(0) : valueMs >= 10 ? valueMs.toFixed(1) : valueMs.toFixed(2);
  return `${formatted}ms`;
}

function buildEdgeLabel(label: string, duration: string): ReactNode {
  return (
    <>
      <tspan x="0" dy="0">
        {label}
      </tspan>
      <tspan x="0" dy="1.2em">
        {duration}
      </tspan>
    </>
  );
}

type OverviewGraph = {
  nodes: Node[];
  edges: Edge[];
  traceMap: Map<string, TracePayload>;
  endpointEdgeLabels: Map<string, string>;
  edgeDurationMap: Map<string, number>;
  errorRuleIds: Set<string>;
  ruleTypeById: Map<string, string>;
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
  refs: { fromId: string; toRule: string; label?: string }[];
  errorNodeIds: Set<string>;
};

type ApiDetailEntry = {
  kind: "op";
  node: ApiGraphOp;
  ruleId: string;
};

type ApiDetailBundle = {
  nodes: Node[];
  edges: Edge[];
  map: Map<string, ApiDetailEntry>;
  bounds: { minX: number; maxX: number; minY: number; maxY: number };
  refs: { fromId: string; toRule: string }[];
};

type RuleRefEntry = { ref: string; label?: string };

function extractRuleRefs(meta?: Record<string, unknown>): RuleRefEntry[] {
  if (!meta) return [];
  const entries: RuleRefEntry[] = [];
  const push = (ref: unknown, label?: unknown) => {
    if (typeof ref !== "string" || ref.length === 0) return;
    entries.push({
      ref,
      label: typeof label === "string" ? label : undefined
    });
  };
  push(meta["rule_ref"], meta["rule_ref_label"]);
  const refs = Array.isArray(meta["rule_refs"]) ? meta["rule_refs"] : [];
  const labels = Array.isArray(meta["rule_ref_labels"]) ? meta["rule_ref_labels"] : [];
  refs.forEach((ref, index) => push(ref, labels[index]));
  const deduped: RuleRefEntry[] = [];
  const seen = new Set<string>();
  entries.forEach((entry) => {
    const key = `${entry.ref}::${entry.label ?? ""}`;
    if (seen.has(key)) return;
    seen.add(key);
    deduped.push(entry);
  });
  return deduped;
}

function traceNodeHasError(node: TraceNode) {
  if (isErrorStatus(node.status) || node.error) return true;
  return (node.children ?? []).some(traceNodeHasError);
}

function buildVirtualTrace(rulePath: string, ruleName: string, parentNode: TraceNode): TracePayload {
  const durationUs = resolveDurationUs(parentNode.duration_us, parentNode.duration_ms);
  return {
    rule: {
      type: "normal",
      name: ruleName,
      path: rulePath,
      version: 2
    },
    records: [
      {
        index: 0,
        status: "error",
        duration_us: durationUs,
        input: parentNode.input,
        output: parentNode.output,
        nodes: [parentNode],
        error: parentNode.error
      }
    ]
  };
}

export function buildOverviewGraph(trace: TracePayload): OverviewGraph {
  const nodes: Node[] = [];
  const edges: Edge[] = [];
  const traceMap = new Map<string, TracePayload>();
  const endpointEdgeLabels = new Map<string, string>();
  const edgeDurationMap = new Map<string, number>();
  const errorRuleIds = new Set<string>();
  const ruleTypeById = new Map<string, string>();
  const virtualTraceMap = new Map<string, TracePayload>();
  const seen = new Map<string, Node>();
  const edgeKeys = new Set<string>();
  const edgeIndexByKey = new Map<string, number>();

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
    const existingIndex = edgeIndexByKey.get(key);
    if (existingIndex !== undefined) {
      if (label && !edges[existingIndex].label) {
        edges[existingIndex] = {
          ...edges[existingIndex],
          label,
          labelBgPadding: [6, 4],
          labelBgBorderRadius: 8,
          className: "edge--endpoint"
        };
      }
      return;
    }
    edgeKeys.add(key);
    edges.push({
      id: `${from}->${to}-${edges.length}`,
      source: from,
      target: to,
      label,
      labelBgPadding: label ? [6, 4] : undefined,
      labelBgBorderRadius: label ? 8 : undefined,
      className: label ? "edge--endpoint" : undefined
    });
    edgeIndexByKey.set(key, edges.length - 1);
  };

  const walk = (current: TracePayload, parentPath?: string) => {
    const currentPath = current.rule?.path ?? parentPath ?? "root";
    ruleTypeById.set(currentPath, current.rule?.type ?? "normal");
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
    let hasLocalError = false;
    let hasChildError = false;
    const records = current.records ?? [];
    records.forEach((record) => {
      const recordHasError =
        isErrorStatus(record.status) || !!record.error || (record.nodes ?? []).some(traceNodeHasError);
      if (recordHasError) {
        hasLocalError = true;
      }
      (record.nodes ?? []).forEach((node) => {
        const nodeHasError = traceNodeHasError(node);
        if (isErrorStatus(node.status) || node.error) {
          hasLocalError = true;
        }
        const meta = (node.meta ?? {}) as Record<string, unknown>;
        let refs = extractRuleRefs(node.meta);
        if (node.kind === "branch") {
          const branchTaken =
            typeof meta["branch_taken"] === "string" ? String(meta["branch_taken"]) : undefined;
          const chosenRef = typeof meta["rule_ref"] === "string" ? String(meta["rule_ref"]) : undefined;
          if (branchTaken === "none") {
            refs = [];
          } else if (branchTaken === "then" || branchTaken === "else") {
            const labelMatch = `branch: ${branchTaken}`;
            refs = refs.filter((entry) => {
              if (chosenRef && entry.ref === chosenRef) return true;
              return entry.label === labelMatch;
            });
          }
        }
        const childTrace = node.child_trace;
        const primaryRef = refs[0]?.ref;
        const childPath = childTrace?.rule?.path ?? primaryRef;
        if (childPath) {
          pushNode(childPath, childTrace?.rule?.name ?? childPath);
          if (childTrace?.rule?.type) {
            ruleTypeById.set(childPath, childTrace.rule.type);
          }
          let label: string | undefined;
          if (isEndpoint) {
            const match = endpointPaths.find((endpoint) => {
              if (!endpoint.rule) return false;
              const normRule = endpoint.rule.replace(/^\.\//, "rules/");
              return normRule === childPath;
            });
            label = current.rule?.name ?? match?.label;
          }
          if (!label) {
            const match = refs.find((entry) => entry.ref === childPath);
            label = match?.label;
          }
          pushEdge(currentPath, childPath, label);
          if (label) {
            endpointEdgeLabels.set(`${currentPath}::${childPath}`, label);
          }
          const edgeKey = `${currentPath}::${childPath}`;
          if (!edgeDurationMap.has(edgeKey)) {
            const durationUs =
              resolveTraceDurationUs(childTrace) ?? resolveDurationUs(node.duration_us, node.duration_ms);
            if (durationUs !== undefined) {
              edgeDurationMap.set(edgeKey, durationUs);
            }
          }
          if (nodeHasError && !childTrace) {
            if (!traceMap.has(childPath) && !virtualTraceMap.has(childPath)) {
              const virtualTrace = buildVirtualTrace(
                childPath,
                childTrace?.rule?.name ?? childPath,
                node
              );
              virtualTraceMap.set(childPath, virtualTrace);
            }
            errorRuleIds.add(childPath);
            hasChildError = true;
          }
        }
        refs.forEach((entry) => {
          if (!entry.ref || entry.ref === childPath) return;
          pushNode(entry.ref, entry.ref);
          pushEdge(currentPath, entry.ref, entry.label);
          const edgeKey = `${currentPath}::${entry.ref}`;
          if (!edgeDurationMap.has(edgeKey)) {
            const durationUs = resolveDurationUs(node.duration_us, node.duration_ms);
            if (durationUs !== undefined) {
              edgeDurationMap.set(edgeKey, durationUs);
            }
          }
        });
        if (childTrace) {
          const childHasError = walk(childTrace, currentPath);
          if (childHasError) {
            hasChildError = true;
          }
        }
      });
    });
    if ((current.finalize?.nodes ?? []).some((node) => isErrorStatus(node.status) || node.error)) {
      hasLocalError = true;
    }
    if (hasLocalError && !hasChildError) {
      errorRuleIds.add(currentPath);
    }
    return hasLocalError || hasChildError;
  };

  walk(trace);
  virtualTraceMap.forEach((virtualTrace, path) => {
    if (!traceMap.has(path)) {
      traceMap.set(path, virtualTrace);
    }
  });
  const layouted = layoutGraph(nodes, edges, graphDefaults.rankdir as "LR" | "TB");
  return {
    nodes: layouted.nodes,
    edges: layouted.edges,
    traceMap,
    endpointEdgeLabels,
    edgeDurationMap,
    errorRuleIds,
    ruleTypeById
  };
}

function buildApiGraph(
  graph: ApiGraphResponse
): { nodes: Node[]; edges: Edge[]; nodeMap: Map<string, ApiGraphNode>; edgeLabelMap: Map<string, string> } {
  const nodeMap = new Map<string, ApiGraphNode>();
  const edgeLabelMap = new Map<string, string>();
  const nodes: Node[] = graph.nodes.map((node) => {
    nodeMap.set(node.id, node);
    return {
      id: node.id,
      position: { x: 0, y: 0 },
      data: { label: node.label },
      type: "default",
      className: "trace-node trace-node--overview",
      style: { width: 240, height: 80 }
    };
  });
  const edges: Edge[] = graph.edges.map((edge, index) => {
    if (edge.label) {
      edgeLabelMap.set(`${edge.source}::${edge.target}`, edge.label);
    }
    return {
      id: `${edge.source}->${edge.target}-${index}`,
      source: edge.source,
      target: edge.target,
      label: edge.label,
      labelBgPadding: edge.label ? [6, 4] : undefined,
      labelBgBorderRadius: edge.label ? 8 : undefined,
      className: edge.label ? "edge--endpoint" : edge.kind === "ref" ? "edge--ref" : undefined,
      type: "smoothstep",
      style: { strokeWidth: 1.4 }
    };
  });
  const layouted = layoutGraph(nodes, edges, graphDefaults.rankdir as "LR" | "TB");
  return { nodes: layouted.nodes, edges: layouted.edges, nodeMap, edgeLabelMap };
}

function buildApiDetailBundle(rule: ApiGraphNode): ApiDetailBundle {
  const nodes: Node[] = [];
  const edges: Edge[] = [];
  const map = new Map<string, ApiDetailEntry>();
  const refs: { fromId: string; toRule: string }[] = [];
  const spacing = 74;
  const opWidth = 200;
  let cursorY = 0;
  let previousId: string | null = null;

  rule.ops.forEach((op, index) => {
    const opId = `detail-${rule.id}::op-${index}`;
    const node: Node = {
      id: opId,
      position: { x: 0, y: cursorY },
      data: { label: op.label },
      type: "detail",
      className: "trace-node trace-node--op",
      sourcePosition: Position.Bottom,
      targetPosition: Position.Top,
      style: { width: opWidth, height: 48 }
    };
    nodes.push(node);
    map.set(opId, { kind: "op", node: op, ruleId: rule.id });
    (op.refs ?? []).forEach((target) => {
      refs.push({ fromId: opId, toRule: target });
    });
    if (previousId) {
      edges.push({ id: `${previousId}->${opId}`, source: previousId, target: opId });
    }
    previousId = opId;
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

  return { nodes, edges, map, bounds, refs };
}

function buildDetailBundle(record: TraceRecord | undefined, ruleId: string): DetailBundle {
  const nodes: Node[] = [];
  const edges: Edge[] = [];
  const map = new Map<string, DetailEntry>();
  const refs: { fromId: string; toRule: string; label?: string }[] = [];
  const recordNodes = record?.nodes ?? [];
  const spacing = 90;
  const stepWidth = 200;
  const opWidth = 160;
  let cursorY = 0;
  let previousId: string | null = null;
  const errorNodeIds = new Set<string>();
  let errorMarked = false;

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
    if (!errorMarked && (isErrorStatus(node.status) || node.error)) {
      errorNodeIds.add(stepNodeId);
      errorMarked = true;
    }
    extractRuleRefs(node.meta).forEach((entry) => {
      refs.push({ fromId: stepNodeId, toRule: entry.ref, label: entry.label });
    });

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
      if (!errorMarked && (isErrorStatus(child.status) || child.error)) {
        errorNodeIds.add(opId);
        errorMarked = true;
      }
      extractRuleRefs(child.meta).forEach((entry) => {
        refs.push({ fromId: opId, toRule: entry.ref, label: entry.label });
      });
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
    refs,
    errorNodeIds
  };
}

function buildMergedGraph(
  overview: OverviewGraph,
  bundles: Map<string, DetailBundle>,
  expandedRuleIds: string[],
  pinnedPositions: Record<string, { x: number; y: number }>,
  endpointEdgeLabels: Map<string, string>
) {
  const expandedSet = new Set(expandedRuleIds);
  const overviewEdges = overview.edges.filter((edge) => !expandedSet.has(edge.source));
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
    const size = sizeById.get(node.id) ?? { width: 240, height: 80 };
    const pinned = pinnedPositions[node.id];
    const typeName = overview.ruleTypeById.get(node.id) ?? "normal";
    const typeClass = `trace-node--type-${typeName}`;
    const errorClass = overview.errorRuleIds.has(node.id) ? "trace-node--error-path" : "";
    const expandedClass = expandedRuleIds.includes(node.id) ? "trace-node--overview-expanded" : "";
    const className = [node.className, typeClass, expandedClass, errorClass]
      .filter(Boolean)
      .join(" ");
    return {
      ...node,
      type: "default",
      className,
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
      position: pinned ? { ...pinned } : node.position,
      style: { width: size.width, height: size.height }
    };
  });
  const nodes = overviewNodes.map((node) => {
    const pinned = pinnedPositions[node.id];
    return pinned ? { ...node, position: { ...pinned } } : { ...node };
  });
  let edges = overviewEdges.map((edge) => {
    const edgeKey = `${edge.source}::${edge.target}`;
    const baseLabel = typeof edge.label === "string" ? edge.label : undefined;
    const fallbackLabel =
      overview.traceMap.get(edge.target)?.rule?.name ??
      overview.traceMap.get(edge.target)?.rule?.path ??
      edge.target;
    const labelText = baseLabel ?? fallbackLabel;
    const durationText = formatEdgeDurationMs(overview.edgeDurationMap.get(edgeKey));
    const label = buildEdgeLabel(labelText, durationText);
    return {
      ...edge,
      label,
      labelBgPadding: edge.labelBgPadding ?? [6, 6],
      labelBgBorderRadius: edge.labelBgBorderRadius ?? 8,
      type: edge.type ?? "smoothstep",
      style: { strokeWidth: 1.4, ...(edge.style ?? {}) }
    };
  });

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
    const positionedDetailNodes = bundle.nodes.map((node, index) => {
      const isError = bundle.errorNodeIds.has(node.id);
      const className = `${node.className ?? ""} trace-node--reveal${
        isError ? " trace-node--error-path" : ""
      }`.trim();
      return {
        ...node,
        position: {
          x: node.position.x + offsetX,
          y: node.position.y + padding - minY
        },
        parentId: ruleId,
        extent: "parent",
        className,
        style: {
          ...(node.style ?? {}),
          zIndex: 10,
          animationDelay: `${index * 40}ms`
        }
      };
    });

    nodes.push(...positionedDetailNodes);
    edges = [
      ...edges,
      ...bundle.edges.map((edge) => ({
        ...edge,
        type: edge.type ?? "smoothstep",
        style: { strokeWidth: 1.2, ...(edge.style ?? {}) }
      }))
    ];

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
      const fallbackLabel =
        overview.traceMap.get(ref.toRule)?.rule?.name ??
        overview.traceMap.get(ref.toRule)?.rule?.path ??
        ref.toRule;
      const baseLabel =
        ref.label ?? endpointEdgeLabels.get(`${ruleId}::${ref.toRule}`) ?? fallbackLabel;
      const entry = bundle.map.get(ref.fromId);
      const durationUs = entry ? resolveDurationUs(entry.node.duration_us, entry.node.duration_ms) : undefined;
      const durationText = formatEdgeDurationMs(durationUs);
      const label = buildEdgeLabel(baseLabel, durationText);
      const className = "edge--ref edge--endpoint";
      refEdges.push({
        id: `${ref.fromId}->${ref.toRule}`,
        source: ref.fromId,
        target: ref.toRule,
        sourceHandle: "right",
        label,
        labelBgPadding: [6, 6],
        labelBgBorderRadius: 8,
        className,
        type: "smoothstep",
        style: { strokeWidth: 1.4 }
      });
    });
  });

  return { nodes, edges: [...edges, ...refEdges] };
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

function getNodesBounds(nodes: Node[]) {
  const initial = { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity };
  const bounds = nodes.reduce((acc, node) => {
    const width = typeof node.style?.width === "number" ? node.style.width : 240;
    const height = typeof node.style?.height === "number" ? node.style.height : 80;
    acc.minX = Math.min(acc.minX, node.position.x);
    acc.minY = Math.min(acc.minY, node.position.y);
    acc.maxX = Math.max(acc.maxX, node.position.x + width);
    acc.maxY = Math.max(acc.maxY, node.position.y + height);
    return acc;
  }, initial);
  return {
    ...bounds,
    width: Math.max(1, bounds.maxX - bounds.minX),
    height: Math.max(1, bounds.maxY - bounds.minY)
  };
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

function buildMergedApiGraph(
  overview: { nodes: Node[]; edges: Edge[] },
  bundles: Map<string, ApiDetailBundle>,
  expandedRuleIds: string[],
  pinnedPositions: Record<string, { x: number; y: number }>,
  edgeLabelMap: Map<string, string>
) {
  const expandedSet = new Set(expandedRuleIds);
  const overviewEdges = overview.edges.filter((edge) => !expandedSet.has(edge.source));
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
    const size = sizeById.get(node.id) ?? { width: 240, height: 80 };
    const pinned = pinnedPositions[node.id];
    return {
      ...node,
      type: "default",
      className: expandedRuleIds.includes(node.id)
        ? `${node.className ?? ""} trace-node--overview-expanded`.trim()
        : node.className,
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
      position: pinned ? { ...pinned } : node.position,
      style: { width: size.width, height: size.height }
    };
  });

  const nodes = overviewNodes.map((node) => {
    const pinned = pinnedPositions[node.id];
    return pinned ? { ...node, position: { ...pinned } } : { ...node };
  });
  let edges = overviewEdges.map((edge) => ({ ...edge }));

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

  const refEdges: Edge[] = [];
  const refEdgeKeys = new Set<string>();
  expandedRuleIds.forEach((ruleId) => {
    const bundle = bundles.get(ruleId);
    if (!bundle) return;
    bundle.refs.forEach((ref) => {
      const key = `${ref.fromId}::${ref.toRule}`;
      if (refEdgeKeys.has(key)) return;
      refEdgeKeys.add(key);
      const label = edgeLabelMap.get(`${ruleId}::${ref.toRule}`);
      refEdges.push({
        id: `${ref.fromId}->${ref.toRule}`,
        source: ref.fromId,
        target: ref.toRule,
        sourceHandle: "right",
        label,
        labelBgPadding: label ? [6, 4] : undefined,
        labelBgBorderRadius: label ? 8 : undefined,
        className: label ? "edge--ref edge--endpoint" : "edge--ref",
        type: "smoothstep",
        style: { strokeWidth: 1.4 }
      });
    });
  });

  return { nodes, edges: [...edges, ...refEdges] };
}

export default function App() {
  const [viewMode, setViewMode] = useState<"trace" | "api">("trace");
  const [durationUnit, setDurationUnit] = useState<DurationUnit>(() => {
    if (typeof window === "undefined") return "us";
    const stored = window.localStorage.getItem("traceDurationUnit");
    return stored === "ms" ? "ms" : "us";
  });
  const [traces, setTraces] = useState<TraceListItem[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [trace, setTrace] = useState<TracePayload | null>(null);
  const [expandedRuleIds, setExpandedRuleIds] = useState<string[]>([]);
  const [focusedRuleId, setFocusedRuleId] = useState<string | null>(null);
  const [recordIndex, setRecordIndex] = useState(0);
  const [selectedNode, setSelectedNode] = useState<TraceNode | null>(null);
  const [selectedOp, setSelectedOp] = useState<TraceNode | null>(null);
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [traceListOpen, setTraceListOpen] = useState(true);
  const [traceInspectorSections, setTraceInspectorSections] = useState(() => ({
    step: true,
    opList: false,
    opResult: false
  }));
  const [apiInspectorSections, setApiInspectorSections] = useState(() => ({
    opList: false,
    memo: false
  }));
  const [apiGraph, setApiGraph] = useState<ApiGraphResponse | null>(null);
  const [selectedApiNode, setSelectedApiNode] = useState<ApiGraphNode | null>(null);
  const [selectedApiOp, setSelectedApiOp] = useState<ApiGraphOp | null>(null);
  const [apiExpandedRuleIds, setApiExpandedRuleIds] = useState<string[]>([]);
  const [apiFocusedRuleId, setApiFocusedRuleId] = useState<string | null>(null);
  const [flow, setFlow] = useState<ReactFlowInstance | null>(null);
  const initialCenterAppliedRef = useRef(false);
  const prevViewModeRef = useRef<"trace" | "api">(viewMode);
  const [pinnedPositions, setPinnedPositions] = useState<Record<string, { x: number; y: number }>>({});
  const [apiPinnedPositions, setApiPinnedPositions] = useState<Record<string, { x: number; y: number }>>({});
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
    if (viewMode !== "api") return;
    fetchJson<ApiGraphResponse>(`${API_BASE}/api-graph`).then((data) => {
      if (data) {
        setApiGraph(data);
      }
    });
  }, [viewMode]);

  useEffect(() => {
    if (viewMode !== "api") return;
    setSelectedApiNode(null);
    setSelectedApiOp(null);
    setApiExpandedRuleIds([]);
    setApiFocusedRuleId(null);
    setInspectorOpen(false);
  }, [viewMode]);

  useEffect(() => {
    if (shouldResetInitialCenter(prevViewModeRef.current, viewMode)) {
      initialCenterAppliedRef.current = false;
    }
    prevViewModeRef.current = viewMode;
  }, [viewMode]);

  useEffect(() => {
    if (!selectedId) {
      setTrace(null);
      initialCenterAppliedRef.current = false;
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
        initialCenterAppliedRef.current = false;
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
        : {
            nodes: [],
            edges: [],
            traceMap: new Map(),
            endpointEdgeLabels: new Map(),
            edgeDurationMap: new Map(),
            errorRuleIds: new Set(),
            ruleTypeById: new Map()
          },
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
  const apiGraphLayout = useMemo(() => {
    if (!apiGraph) {
      return { nodes: [], edges: [], nodeMap: new Map<string, ApiGraphNode>(), edgeLabelMap: new Map<string, string>() };
    }
    return buildApiGraph(apiGraph);
  }, [apiGraph]);
  const apiBundles = useMemo(() => {
    const map = new Map<string, ApiDetailBundle>();
    apiExpandedRuleIds.forEach((ruleId) => {
      const rule = apiGraphLayout.nodeMap.get(ruleId);
      if (!rule) return;
      map.set(ruleId, buildApiDetailBundle(rule));
    });
    return map;
  }, [apiExpandedRuleIds, apiGraphLayout]);
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
  const apiMergedGraph = useMemo(
    () =>
      buildMergedApiGraph(
        { nodes: apiGraphLayout.nodes, edges: apiGraphLayout.edges },
        apiBundles,
        apiExpandedRuleIds,
        apiPinnedPositions,
        apiGraphLayout.edgeLabelMap
      ),
    [apiGraphLayout, apiBundles, apiExpandedRuleIds, apiPinnedPositions]
  );
  const activeGraph = viewMode === "api" ? apiMergedGraph : mergedGraph;

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
    [setNodes, viewMode]
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
  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem("traceDurationUnit", durationUnit);
  }, [durationUnit]);
  const detailNodeMap = useMemo(() => {
    const map = new Map<string, DetailEntry>();
    bundles.forEach((bundle) => {
      bundle.map.forEach((entry, nodeId) => {
        map.set(nodeId, entry);
      });
    });
    return map;
  }, [bundles]);
  const apiDetailNodeMap = useMemo(() => {
    const map = new Map<string, ApiDetailEntry>();
    apiBundles.forEach((bundle) => {
      bundle.map.forEach((entry, nodeId) => {
        map.set(nodeId, entry);
      });
    });
    return map;
  }, [apiBundles]);
  const hasDetail = viewMode === "trace" && expandedRuleIds.length > 0;
  const apiHasDetail = viewMode === "api" && apiExpandedRuleIds.length > 0;
  const selectedMeta = (selectedNode?.meta ?? {}) as Record<string, unknown>;
  const stepRecordWhen =
    typeof selectedMeta["record_when"] === "boolean" ? selectedMeta["record_when"] : undefined;
  const stepAssertsOk =
    typeof selectedMeta["asserts_ok"] === "boolean" ? selectedMeta["asserts_ok"] : undefined;
  const stepBranchTaken =
    typeof selectedMeta["branch_taken"] === "string" ? String(selectedMeta["branch_taken"]) : undefined;
  const stepDuration = resolveDurationUs(selectedNode?.duration_us, selectedNode?.duration_ms);
  const stepDurationParts = formatDurationParts(stepDuration, durationUnit);
  const traceStepOpen = traceInspectorSections.step;
  const traceOpListOpen = traceInspectorSections.opList;
  const traceOpResultOpen = traceInspectorSections.opResult;
  const apiOpListOpen = apiInspectorSections.opList;
  const apiMemoOpen = apiInspectorSections.memo;

  const renderJsonBlock = (label: string, value: unknown) => {
    const hasValue = !(value === null || value === undefined);
    const isWide = label === "input" || label === "output" || label === "error";
    return (
      <div className={clsx("inspector-block", isWide && "inspector-block--wide")} key={label}>
        <div className="inspector-block__header">
          <div className="inspector-block__title">
            <span className="inspector-block__line" />
            <span className="inspector-block__name">{label}</span>
          </div>
          <span className="inspector-block__meta">{hasValue ? "json" : "empty"}</span>
        </div>
        <pre className="inspector-block__body">{hasValue ? JSON.stringify(value, null, 2) : "なし"}</pre>
      </div>
    );
  };

  return (
    <div className="app">
      <div className="app__glow" />
      <header className="topbar">
        <div className="title-chip">
          <span className="title-chip__dot" />
          <span className="title-chip__label">
            {viewMode === "trace" ? "Rulemorph Trace" : "Rulemorph 構成図"}
          </span>
          <span className="title-chip__id">
            {viewMode === "trace"
              ? currentTrace?.rule?.path ?? currentTrace?.trace_id ?? "no-trace"
              : selectedApiNode?.path ?? "api-graph"}
          </span>
        </div>
        <div className="topbar__meta">
          <div className="meta-tabs">
            <button
              className={clsx("meta-tab", viewMode === "trace" && "is-active")}
              onClick={() => setViewMode("trace")}
            >
              Trace
            </button>
            <button
              className={clsx("meta-tab", viewMode === "api" && "is-active")}
              onClick={() => setViewMode("api")}
            >
              構成図
            </button>
          </div>
          {viewMode === "trace" ? (
            <>
              <span className="meta-pill">{hasDetail ? "detail" : "overview"}</span>
              <span className="meta-pill">{traces.length} traces</span>
              <span className="meta-pill">record #{currentRecord?.index ?? 0}</span>
            </>
          ) : (
            <>
              <span className="meta-pill">{apiGraph?.nodes.length ?? 0} rules</span>
              <span className="meta-pill">{apiGraph?.edges.length ?? 0} edges</span>
            </>
          )}
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
              if (viewMode === "api") {
                const apiNode = apiGraphLayout.nodeMap.get(node.id);
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
                const parent = apiGraphLayout.nodeMap.get(apiDetail.ruleId);
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

        {viewMode === "trace" && (
          <aside className={clsx("floating-panel trace-panel", !traceListOpen && "is-collapsed")}>
            {traceListOpen ? (
              <>
                <div className="panel__header trace-panel__header">
                  <div>
                    <h2>Trace一覧</h2>
                    <p>最新順</p>
                  </div>
                  <div className="trace-panel__actions">
                    <div className="unit-toggle" role="group" aria-label="Duration unit">
                      <button
                        className={clsx("unit-toggle__button", durationUnit === "us" && "is-active")}
                        onClick={() => setDurationUnit("us")}
                      >
                        μs
                      </button>
                      <button
                        className={clsx("unit-toggle__button", durationUnit === "ms" && "is-active")}
                        onClick={() => setDurationUnit("ms")}
                      >
                        ms
                      </button>
                    </div>
                    <button
                      className="icon-button trace-panel__toggle"
                      aria-expanded={traceListOpen}
                      onClick={() => setTraceListOpen(false)}
                    >
                      ×
                    </button>
                  </div>
                </div>
              <div className="trace-list">
                {traces.length === 0 && (
                  <div className="empty-trace">
                    <p>traces が見つかりません。</p>
                    <p className="muted">
                      data_dir（既定: ./.rulemorph）の traces/ に JSON を配置してください。
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
                      <span className={clsx("chip", isErrorStatus(item.status) && "chip--error")}>
                        {item.status ?? "ok"}
                      </span>
                      <h3>{item.rule?.name ?? item.trace_id}</h3>
                      <p className="muted">{item.rule?.path ?? "(no path)"}</p>
                    </div>
                    <div className="trace-meta">
                      <div>
                        <span>time: </span>
                        <strong>{formatTime(item.timestamp)}</strong>
                      </div>
                      <div>
                        <span>duration: </span>
                        <strong>
                          {formatDuration(
                            resolveDurationUs(item.duration_us, item.duration_ms),
                            durationUnit
                          )}
                        </strong>
                      </div>
                    </div>
                  </button>
                ))}
              </div>
              </>
            ) : (
              <button className="trace-panel__chip" onClick={() => setTraceListOpen(true)}>
                <span>Trace一覧</span>
                <span className="trace-panel__chip-count">{traces.length}</span>
              </button>
            )}
          </aside>
        )}

        {hasDetail && viewMode === "trace" && (
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
                    <span
                      className={clsx(
                        "record-status",
                        isErrorStatus(record.status) && "record-status--error"
                      )}
                    >
                      {record.status ?? "ok"}
                    </span>
                    <span>
                      {formatDuration(resolveDurationUs(record.duration_us, record.duration_ms), durationUnit)}
                    </span>
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
              <p>
                {viewMode === "trace"
                  ? selectedNode
                    ? selectedNode.label
                    : "ノードを選択して詳細を表示"
                  : selectedApiNode
                    ? selectedApiNode.label
                    : "ノードを選択して詳細を表示"}
              </p>
            </div>
            <button className="icon-button" onClick={() => setInspectorOpen(false)}>
              ×
            </button>
          </div>

          {viewMode === "api" ? (
            <>
              <div
                className={clsx(
                  "inspector__section inspector__section--oplist",
                  !apiOpListOpen && "is-collapsed"
                )}
              >
                <button
                  className="inspector__section-toggle"
                  aria-expanded={apiOpListOpen}
                  onClick={() =>
                    setApiInspectorSections((prev) => ({ ...prev, opList: !prev.opList }))
                  }
                >
                  <h3>OP一覧</h3>
                  <span className="inspector__chevron">{apiOpListOpen ? "v" : ">"}</span>
                </button>
                {apiOpListOpen && (
                  <div className="op-list">
                    {(selectedApiNode?.ops ?? []).length === 0 && (
                      <p className="muted">このルールにOPはありません。</p>
                    )}
                    {(selectedApiNode?.ops ?? []).map((op, index) => (
                      <div
                        key={`${op.label}-${index}`}
                        className={clsx(
                          "op-item is-static",
                          selectedApiOp?.label === op.label && "is-active"
                        )}
                      >
                        <span>{op.label}</span>
                        <span className="muted">{op.detail ?? selectedApiNode?.kind}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
              <div className={clsx("inspector__section", !apiMemoOpen && "is-collapsed")}>
                <button
                  className="inspector__section-toggle"
                  aria-expanded={apiMemoOpen}
                  onClick={() =>
                    setApiInspectorSections((prev) => ({ ...prev, memo: !prev.memo }))
                  }
                >
                  <h3>処理メモ</h3>
                  <span className="inspector__chevron">{apiMemoOpen ? "v" : ">"}</span>
                </button>
                {apiMemoOpen && (
                  <div className="inspector__content">
                    <p className="muted">
                      実値はありません。ルールファイルに記載された処理内容のみ表示しています。
                    </p>
                  </div>
                )}
              </div>
            </>
          ) : (
            <>
              <div
                className={clsx(
                  "inspector__section inspector__section--opresult",
                  !traceStepOpen && "is-collapsed"
                )}
              >
                <button
                  className="inspector__section-toggle"
                  aria-expanded={traceStepOpen}
                  onClick={() =>
                    setTraceInspectorSections((prev) => ({ ...prev, step: !prev.step }))
                  }
                >
                  <h3>Step結果</h3>
                  <span className="inspector__chevron">{traceStepOpen ? "v" : ">"}</span>
                </button>
                {traceStepOpen && (
                  <div className="inspector__content">
                    {!selectedNode ? (
                      <p className="muted">ノードを選択して詳細を表示してください。</p>
                    ) : (
                      <>
                        <div className="step-badges">
                          <span
                            className={clsx(
                              "chip",
                              isErrorStatus(selectedNode.status) && "chip--error"
                            )}
                          >
                            {selectedNode.status ?? "ok"}
                          </span>
                          {stepDurationParts && (
                            <span className="chip">
                              duration: {stepDurationParts.value}{" "}
                              <span className="chip__unit">{stepDurationParts.unit}</span>
                            </span>
                          )}
                          {stepRecordWhen !== undefined && (
                            <span className="chip">record_when: {String(stepRecordWhen)}</span>
                          )}
                          {stepAssertsOk !== undefined && (
                            <span className="chip">asserts: {String(stepAssertsOk)}</span>
                          )}
                          {stepBranchTaken && <span className="chip">branch: {stepBranchTaken}</span>}
                        </div>
                        <div className="inspector-grid">
                          {renderJsonBlock("input", selectedNode.input ?? null)}
                          {renderJsonBlock("output", selectedNode.output ?? null)}
                          {selectedNode.error && renderJsonBlock("error", selectedNode.error)}
                        </div>
                      </>
                    )}
                  </div>
                )}
              </div>

              <div
                className={clsx(
                  "inspector__section inspector__section--oplist",
                  !traceOpListOpen && "is-collapsed"
                )}
              >
                <button
                  className="inspector__section-toggle"
                  aria-expanded={traceOpListOpen}
                  onClick={() =>
                    setTraceInspectorSections((prev) => ({ ...prev, opList: !prev.opList }))
                  }
                >
                  <h3>OP一覧</h3>
                  <span className="inspector__chevron">{traceOpListOpen ? "v" : ">"}</span>
                </button>
                {traceOpListOpen && (
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
                        <span className="op-item__meta">
                          <span className="muted">{child.meta?.op ?? "op"}</span>
                          {resolveDurationUs(child.duration_us, child.duration_ms) !== undefined && (
                            <span className="op-item__duration">
                              {formatDuration(
                                resolveDurationUs(child.duration_us, child.duration_ms),
                                durationUnit
                              )}
                            </span>
                          )}
                        </span>
                      </button>
                    ))}
                  </div>
                )}
              </div>

              <div
                className={clsx(
                  "inspector__section inspector__section--opresult",
                  !traceOpResultOpen && "is-collapsed"
                )}
              >
                <button
                  className="inspector__section-toggle"
                  aria-expanded={traceOpResultOpen}
                  onClick={() =>
                    setTraceInspectorSections((prev) => ({ ...prev, opResult: !prev.opResult }))
                  }
                >
                  <h3>OP結果</h3>
                  <span className="inspector__chevron">{traceOpResultOpen ? "v" : ">"}</span>
                </button>
                {traceOpResultOpen && (
                  <div className="inspector__content">
                    {(() => {
                      const op = selectedOp as any;
                      const input = op?.input ?? null;
                      const pipe = op?.pipe_value ?? null;
                      const args = op?.args ?? null;
                      const output = op?.output ?? null;
                      const opDuration = resolveDurationUs(op?.duration_us, op?.duration_ms);
                      const opDurationParts = formatDurationParts(opDuration, durationUnit);
                      const pipeSteps = (op?.pipe_steps ?? []) as {
                        index: number;
                        label: string;
                        input?: unknown;
                        output?: unknown;
                      }[];
                      return (
                        <>
                          {opDurationParts && (
                            <div className="step-badges">
                              <span className="chip">
                                duration: {opDurationParts.value}{" "}
                                <span className="chip__unit">{opDurationParts.unit}</span>
                              </span>
                            </div>
                          )}
                          <div className="inspector-grid">
                            {renderJsonBlock("input", input)}
                            {renderJsonBlock("pipe", pipe)}
                            {renderJsonBlock("args", args)}
                            {renderJsonBlock("output", output)}
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
                                        <pre>
                                          {step.input !== undefined
                                            ? JSON.stringify(step.input)
                                            : "なし"}
                                        </pre>
                                      </div>
                                      <div className="pipe-step__cell">
                                        <span className="pipe-step__name">output</span>
                                        <pre>
                                          {step.output !== undefined
                                            ? JSON.stringify(step.output)
                                            : "なし"}
                                        </pre>
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
                )}
              </div>
            </>
          )}
        </aside>
      </main>
    </div>
  );
}
