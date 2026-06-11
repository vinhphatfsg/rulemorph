import { Edge, Node } from "reactflow";
import {
  isErrorStatus,
  resolveDurationUs,
  resolveTraceDurationUs
} from "../trace_list/trace_list_helpers";
import { type EndpointRule, type TraceNode, type TracePayload } from "../api/trace_payload";
import { extractRuleRefs } from "./trace_graph_refs";
import { type OverviewGraph } from "./trace_graph_types";
import { graphDefaults, layoutGraph } from "./trace_graph_layout";

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
