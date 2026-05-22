import { useMemo } from "react";
import {
  buildApiDetailBundles,
  buildTraceDetailBundles,
  collectApiDetailNodeMap,
  collectDetailNodeMap,
  emptyOverviewGraph,
  resolveCurrentTrace,
  resolveDetailLabel,
  resolveDetailReason,
  resolveDetailStatus,
  resolveEffectiveFocusedRuleId,
  resolveRecordLabel
} from "./app_derived_state";
import {
  buildApiGraph,
  buildMergedApiGraph,
  buildMergedGraph,
  buildOverviewGraph,
  type ApiGraphNode,
  type ApiGraphResponse
} from "./trace_graph";
import type { TraceManifest, TracePayload } from "./trace_payload";

type ViewMode = "trace" | "api";
type PinnedPositions = Record<string, { x: number; y: number }>;
type ApiGraphLayout = ReturnType<typeof buildApiGraph>;

type AppGraphViewStateArgs = {
  viewMode: ViewMode;
  trace: TracePayload | null;
  traceManifest: TraceManifest | null;
  detailLoading: boolean;
  expandedRuleIds: string[];
  focusedRuleId: string | null;
  recordIndex: number;
  apiGraph: ApiGraphResponse | null;
  apiExpandedRuleIds: string[];
  pinnedPositions: PinnedPositions;
  apiPinnedPositions: PinnedPositions;
};

function emptyApiGraphLayout(): ApiGraphLayout {
  return {
    nodes: [],
    edges: [],
    nodeMap: new Map<string, ApiGraphNode>(),
    edgeLabelMap: new Map<string, string>()
  };
}

export function useAppGraphViewState({
  viewMode,
  trace,
  traceManifest,
  detailLoading,
  expandedRuleIds,
  focusedRuleId,
  recordIndex,
  apiGraph,
  apiExpandedRuleIds,
  pinnedPositions,
  apiPinnedPositions
}: AppGraphViewStateArgs) {
  const overviewGraph = useMemo(
    () => (trace ? buildOverviewGraph(trace) : emptyOverviewGraph()),
    [trace]
  );
  const effectiveFocusedRuleId = resolveEffectiveFocusedRuleId(focusedRuleId, expandedRuleIds);
  const currentTrace = resolveCurrentTrace(effectiveFocusedRuleId, overviewGraph, trace);
  const isFinalizeSelected = recordIndex < 0;
  const currentRecord = recordIndex >= 0 ? currentTrace?.records?.[recordIndex] : undefined;
  const finalizePayload = currentTrace?.finalize ?? null;
  const bundles = useMemo(
    () => buildTraceDetailBundles(expandedRuleIds, overviewGraph, recordIndex, effectiveFocusedRuleId),
    [expandedRuleIds, overviewGraph, recordIndex, effectiveFocusedRuleId]
  );
  const apiGraphLayout = useMemo(
    () => (apiGraph ? buildApiGraph(apiGraph) : emptyApiGraphLayout()),
    [apiGraph]
  );
  const apiBundles = useMemo(
    () => buildApiDetailBundles(apiExpandedRuleIds, apiGraphLayout),
    [apiExpandedRuleIds, apiGraphLayout]
  );
  const mergedGraph = useMemo(
    () =>
      buildMergedGraph(
        overviewGraph,
        bundles,
        expandedRuleIds,
        pinnedPositions,
        overviewGraph.endpointEdgeLabels
      ),
    [overviewGraph, bundles, expandedRuleIds, pinnedPositions]
  );
  const apiMergedGraph = useMemo(
    () =>
      buildMergedApiGraph(
        { nodes: apiGraphLayout.nodes, edges: apiGraphLayout.edges },
        apiBundles,
        apiExpandedRuleIds,
        apiPinnedPositions,
        apiGraphLayout.edgeLabelMap
      ),
    [apiGraphLayout, apiBundles, apiExpandedRuleIds, apiPinnedPositions]
  );
  const activeGraph = viewMode === "api" ? apiMergedGraph : mergedGraph;
  const detailNodeMap = useMemo(() => collectDetailNodeMap(bundles), [bundles]);
  const apiDetailNodeMap = useMemo(() => collectApiDetailNodeMap(apiBundles), [apiBundles]);
  const detailStatus = resolveDetailStatus(traceManifest, trace);
  const detailReason = resolveDetailReason(traceManifest, trace);
  const detailAvailable = detailStatus ? detailStatus === "full" : true;
  const hasDetail = viewMode === "trace" && expandedRuleIds.length > 0 && detailAvailable;
  const apiHasDetail = viewMode === "api" && apiExpandedRuleIds.length > 0;
  const recordLabel = resolveRecordLabel(isFinalizeSelected, currentRecord);
  const detailLabel = resolveDetailLabel(detailStatus, detailLoading, hasDetail);

  return {
    overviewGraph,
    currentTrace,
    isFinalizeSelected,
    finalizePayload,
    activeGraph,
    detailNodeMap,
    apiGraphLayout,
    apiDetailNodeMap,
    detailStatus,
    detailReason,
    hasDetail,
    apiHasDetail,
    recordLabel,
    detailLabel
  };
}
