import {
  resolveRuleLabel,
  resolveTraceStatus,
  type TraceListItem
} from "./trace_list_helpers";
import type { TraceManifest, TracePayload, TraceRecord } from "./trace_payload";
import {
  buildApiDetailBundle,
  buildApiGraph,
  buildDetailBundle,
  buildOverviewGraph,
  type ApiDetailBundle,
  type ApiDetailEntry,
  type DetailBundle,
  type DetailEntry
} from "./trace_graph";

type OverviewGraph = ReturnType<typeof buildOverviewGraph>;
type ApiGraphLayout = ReturnType<typeof buildApiGraph>;

export function deriveStatusOptions(traces: TraceListItem[]): string[] {
  const set = new Set<string>();
  traces.forEach((item) => {
    set.add(resolveTraceStatus(item));
  });
  return Array.from(set).sort();
}

export function deriveRuleOptions(traces: TraceListItem[]): string[] {
  const set = new Set<string>();
  traces.forEach((item) => {
    const label = resolveRuleLabel(item);
    if (label) {
      set.add(label);
    }
  });
  return Array.from(set).sort();
}

export function emptyOverviewGraph(): OverviewGraph {
  return {
    nodes: [],
    edges: [],
    traceMap: new Map(),
    endpointEdgeLabels: new Map(),
    edgeDurationMap: new Map(),
    errorRuleIds: new Set(),
    ruleTypeById: new Map()
  };
}

export function resolveEffectiveFocusedRuleId(
  focusedRuleId: string | null,
  expandedRuleIds: string[]
): string | null {
  return focusedRuleId ?? expandedRuleIds[expandedRuleIds.length - 1] ?? null;
}

export function resolveCurrentTrace(
  effectiveFocusedRuleId: string | null,
  overviewGraph: OverviewGraph,
  trace: TracePayload | null
): TracePayload | null {
  return effectiveFocusedRuleId
    ? overviewGraph.traceMap.get(effectiveFocusedRuleId) ?? trace
    : trace;
}

export function buildTraceDetailBundles(
  expandedRuleIds: string[],
  overviewGraph: OverviewGraph,
  recordIndex: number,
  effectiveFocusedRuleId: string | null
): Map<string, DetailBundle> {
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
}

export function buildApiDetailBundles(
  apiExpandedRuleIds: string[],
  apiGraphLayout: ApiGraphLayout
): Map<string, ApiDetailBundle> {
  const map = new Map<string, ApiDetailBundle>();
  apiExpandedRuleIds.forEach((ruleId) => {
    const rule = apiGraphLayout.nodeMap.get(ruleId);
    if (!rule) return;
    map.set(ruleId, buildApiDetailBundle(rule));
  });
  return map;
}

export function collectDetailNodeMap(bundles: Map<string, DetailBundle>): Map<string, DetailEntry> {
  const map = new Map<string, DetailEntry>();
  bundles.forEach((bundle) => {
    bundle.map.forEach((entry, nodeId) => {
      map.set(nodeId, entry);
    });
  });
  return map;
}

export function collectApiDetailNodeMap(
  apiBundles: Map<string, ApiDetailBundle>
): Map<string, ApiDetailEntry> {
  const map = new Map<string, ApiDetailEntry>();
  apiBundles.forEach((bundle) => {
    bundle.map.forEach((entry, nodeId) => {
      map.set(nodeId, entry);
    });
  });
  return map;
}

export function resolveDetailStatus(
  traceManifest: TraceManifest | null,
  trace: TracePayload | null
): string | undefined {
  return traceManifest ? traceManifest.detail?.status ?? "basic" : trace?.detail?.status;
}

export function resolveDetailReason(
  traceManifest: TraceManifest | null,
  trace: TracePayload | null
): string[] {
  return traceManifest ? traceManifest.detail?.reason ?? [] : trace?.detail?.reason ?? [];
}

export function resolveRecordLabel(
  isFinalizeSelected: boolean,
  currentRecord: TraceRecord | undefined
): string {
  return isFinalizeSelected ? "finalize" : `record #${currentRecord?.index ?? 0}`;
}

export function resolveDetailLabel(
  detailStatus: string | undefined,
  detailLoading: boolean,
  hasDetail: boolean
): string {
  return detailStatus
    ? detailStatus === "full"
      ? detailLoading
        ? "loading"
        : hasDetail
          ? "detail"
          : "overview"
      : detailStatus
    : hasDetail
      ? "detail"
      : "overview";
}
