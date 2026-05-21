import { Edge, Node, Position } from "reactflow";
import { resolveDurationUs } from "./trace_list_helpers";
import {
  DetailNode,
  buildApiDetailBundle,
  buildDetailBundle
} from "./trace_graph_detail";
import {
  type ApiDetailBundle,
  type DetailBundle,
  type OverviewGraph
} from "./trace_graph_types";
import { buildEdgeLabel, formatEdgeDurationMs } from "./trace_graph_edges";
import { layoutGraphWithSizes } from "./trace_graph_layout";

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
export { buildOverviewGraph } from "./trace_graph_overview";
export { buildApiGraph } from "./trace_graph_api";

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
