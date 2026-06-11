import { Edge, Handle, Node, Position } from "reactflow";
import { isErrorStatus } from "../trace_list/trace_list_helpers";
import { type TraceNode, type TraceRecord } from "../api/trace_payload";
import {
  type ApiDetailBundle,
  type ApiDetailEntry,
  type ApiGraphNode,
  type DetailBundle,
  type DetailEntry
} from "./trace_graph_types";
import { extractRuleRefs } from "./trace_graph_refs";

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
    const ops = (node.children ?? []).filter((child: TraceNode) => child.kind === "op");
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
