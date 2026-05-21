import { Edge, Node, Position } from "reactflow";
import type { ApiDetailBundle } from "../trace_graph_types";

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
