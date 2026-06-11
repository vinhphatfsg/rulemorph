import { Edge, Node } from "reactflow";
import dagre from "dagre";

export const graphDefaults = {
  rankdir: "LR",
  nodesep: 220,
  ranksep: 80
};

export function layoutGraph(nodes: Node[], edges: Edge[], direction: "LR" | "TB") {
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

export function layoutGraphWithSizes(
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
