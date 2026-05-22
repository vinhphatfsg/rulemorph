import { Edge, Node } from "reactflow";
import { type ApiGraphNode, type ApiGraphResponse } from "./trace_graph_types";
import { graphDefaults, layoutGraph } from "./trace_graph_layout";

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
