import { type Edge, type Node } from "reactflow";
import { type TraceNode, type TracePayload } from "../api/trace_payload";

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

export type ApiGraphEdge = {
  source: string;
  target: string;
  label?: string;
  kind: string;
};

export type ApiGraphResponse = {
  nodes: ApiGraphNode[];
  edges: ApiGraphEdge[];
};

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

export type RuleRefEntry = { ref: string; label?: string };
