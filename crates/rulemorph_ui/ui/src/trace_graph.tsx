import { type ReactNode } from "react";
import { Edge, Handle, Node, Position } from "reactflow";
import dagre from "dagre";
import {
  isErrorStatus,
  resolveDurationUs,
  resolveTraceDurationUs
} from "./trace_list_helpers";
import { type EndpointRule, type TraceNode, type TracePayload, type TraceRecord } from "./trace_payload";

type TraceNodeData = {
  label: string;
};

export function DetailNode({ data }: { data: TraceNodeData }) {
  return (
    <div className="trace-node__body">
      <Handle type="target" position={Position.Top} id="top" />
      <Handle type="source" position={Position.Bottom} id="bottom" />
      <Handle type="source" position={Position.Right} id="right" />
      <span>{data.label}</span>
    </div>
  );
}

export type ApiGraphOp = {
  label: string;
  detail?: string;
  refs?: string[];
};

export type ApiGraphNode = {
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

export type ApiGraphResponse = {
  nodes: ApiGraphNode[];
  edges: ApiGraphEdge[];
};

const graphDefaults = {
  rankdir: "LR",
  nodesep: 220,
  ranksep: 80
};

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

export type OverviewGraph = {
  nodes: Node[];
  edges: Edge[];
  traceMap: Map<string, TracePayload>;
  endpointEdgeLabels: Map<string, string>;
  edgeDurationMap: Map<string, number>;
  errorRuleIds: Set<string>;
  ruleTypeById: Map<string, string>;
};

export type DetailEntry = {
  kind: "step" | "op";
  node: TraceNode;
  parent?: TraceNode;
  ruleId: string;
};

export type DetailBundle = {
  nodes: Node[];
  edges: Edge[];
  map: Map<string, DetailEntry>;
  firstId?: string;
  lastId?: string;
  bounds: { minX: number; maxX: number; minY: number; maxY: number };
  refs: { fromId: string; toRule: string; label?: string }[];
  errorNodeIds: Set<string>;
};

export type ApiDetailEntry = {
  kind: "op";
  node: ApiGraphOp;
  ruleId: string;
};

export type ApiDetailBundle = {
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

export function buildApiGraph(
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

export function buildApiDetailBundle(rule: ApiGraphNode): ApiDetailBundle {
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

export function buildDetailBundle(record: TraceRecord | undefined, ruleId: string): DetailBundle {
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

export function buildMergedGraph(
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

export function getNodesBounds(nodes: Node[]) {
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

export function buildMergedApiGraph(
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
