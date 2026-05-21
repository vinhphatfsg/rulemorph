import { Edge, Node, Position } from "reactflow";
import {
  isErrorStatus,
  resolveDurationUs,
  resolveTraceDurationUs
} from "./trace_list_helpers";
import { type EndpointRule, type TraceNode, type TracePayload } from "./trace_payload";
import {
  DetailNode,
  buildApiDetailBundle,
  buildDetailBundle
} from "./trace_graph_detail";
import { extractRuleRefs } from "./trace_graph_refs";
import {
  type ApiDetailBundle,
  type ApiGraphNode,
  type ApiGraphResponse,
  type DetailBundle,
  type OverviewGraph
} from "./trace_graph_types";
import { buildEdgeLabel, formatEdgeDurationMs } from "./trace_graph_edges";
import { graphDefaults, layoutGraph, layoutGraphWithSizes } from "./trace_graph_layout";

export {
  DetailNode,
  buildApiDetailBundle,
  buildDetailBundle
} from "./trace_graph_detail";
export type {
  ApiDetailBundle,
  ApiDetailEntry,
  ApiGraphNode,
  ApiGraphOp,
  ApiGraphResponse,
  DetailBundle,
  DetailEntry,
  OverviewGraph
} from "./trace_graph_types";
export { getNodesBounds } from "./trace_graph_layout";

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
