import { useCallback, useEffect, useMemo, useState } from "react";
import "reactflow/dist/style.css";
import {
  __resetAuthCachesForTest as resetAuthCachesForTest,
  captureAuthSecretsFromLocation,
  getApiKey,
  getInternalKey
} from "./auth";
import { __resetTenantCachesForTest, getTenantId } from "./tenant";
import { API_BASE, fetchJson } from "./api_client";
import {
  applyTraceFilters,
  type DurationUnit,
  type TimeRange,
  type TraceListItem
} from "./trace_list_helpers";
import {
  buildApiDetailBundles,
  buildTraceDetailBundles,
  collectApiDetailNodeMap,
  collectDetailNodeMap,
  deriveRuleOptions,
  deriveStatusOptions,
  emptyOverviewGraph,
  resolveCurrentTrace,
  resolveDetailLabel,
  resolveDetailReason,
  resolveDetailStatus,
  resolveEffectiveFocusedRuleId,
  resolveRecordLabel
} from "./app_derived_state";
import {
  type TraceManifest,
  type TraceNode,
  type TracePayload,
  type TraceRecord
} from "./trace_payload";
import { loadSelectedTraceDetail } from "./app_trace_detail_loader";
import { InspectorDrawer } from "./inspector_drawer";
import { RecordPanel } from "./record_panel";
import { Topbar } from "./topbar";
import { TraceCanvas } from "./trace_canvas";
import {
  buildApiGraph,
  buildMergedApiGraph,
  buildMergedGraph,
  buildOverviewGraph,
  type ApiGraphNode,
  type ApiGraphOp,
  type ApiGraphResponse
} from "./trace_graph";
import { TraceListPanel, ZipImportModal } from "./trace_list_panel";
import { runZipImport } from "./zip_import";
import {
  loadTraceList,
  reconcileFilteredTraceSelection,
  subscribeTraceListRefresh
} from "./app_trace_list_refresh";

export { getApiKey, getInternalKey } from "./auth";
export { __getTenantIdFromQueryOrStorageForTest, resolveTenantId } from "./tenant";
export type { EndpointRule, EndpointSpec, TraceNode, TracePayload, TraceRecord } from "./trace_payload";
export { buildOverviewGraph } from "./trace_graph";

export function __resetAuthCachesForTest(): void {
  resetAuthCachesForTest();
  __resetTenantCachesForTest();
}

export default function App() {
  captureAuthSecretsFromLocation();
  useEffect(() => {
    if (typeof window === "undefined") return;
    const capture = () => captureAuthSecretsFromLocation();
    window.addEventListener("hashchange", capture);
    window.addEventListener("popstate", capture);
    return () => {
      window.removeEventListener("hashchange", capture);
      window.removeEventListener("popstate", capture);
    };
  }, []);
  const [viewMode, setViewMode] = useState<"trace" | "api">("trace");
  const [durationUnit, setDurationUnit] = useState<DurationUnit>(() => {
    if (typeof window === "undefined") return "us";
    const stored = window.localStorage.getItem("traceDurationUnit");
    return stored === "ms" ? "ms" : "us";
  });
  const [traces, setTraces] = useState<TraceListItem[]>([]);
  const [traceFilterStatus, setTraceFilterStatus] = useState("all");
  const [traceFilterRule, setTraceFilterRule] = useState("all");
  const [traceFilterQuery, setTraceFilterQuery] = useState("");
  const [traceFilterRange, setTraceFilterRange] = useState<TimeRange>("all");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [trace, setTrace] = useState<TracePayload | null>(null);
  const [traceManifest, setTraceManifest] = useState<TraceManifest | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [expandedRuleIds, setExpandedRuleIds] = useState<string[]>([]);
  const [focusedRuleId, setFocusedRuleId] = useState<string | null>(null);
  const [recordIndex, setRecordIndex] = useState(0);
  const [selectedNode, setSelectedNode] = useState<TraceNode | null>(null);
  const [selectedOp, setSelectedOp] = useState<TraceNode | null>(null);
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [traceListOpen, setTraceListOpen] = useState(true);
  const [zipModalOpen, setZipModalOpen] = useState(false);
  const [zipFile, setZipFile] = useState<File | null>(null);
  const [zipMessage, setZipMessage] = useState<string | null>(null);
  const [zipUploading, setZipUploading] = useState(false);
  const [traceInspectorSections, setTraceInspectorSections] = useState(() => ({
    finalize: true,
    step: true,
    opList: false,
    opResult: false
  }));
  const [apiInspectorSections, setApiInspectorSections] = useState(() => ({
    opList: false,
    memo: false
  }));
  const [apiGraph, setApiGraph] = useState<ApiGraphResponse | null>(null);
  const [selectedApiNode, setSelectedApiNode] = useState<ApiGraphNode | null>(null);
  const [selectedApiOp, setSelectedApiOp] = useState<ApiGraphOp | null>(null);
  const [apiExpandedRuleIds, setApiExpandedRuleIds] = useState<string[]>([]);
  const [apiFocusedRuleId, setApiFocusedRuleId] = useState<string | null>(null);
  const [pinnedPositions, setPinnedPositions] = useState<Record<string, { x: number; y: number }>>({});
  const [apiPinnedPositions, setApiPinnedPositions] = useState<Record<string, { x: number; y: number }>>({});
  const internalKey = getInternalKey();
  const tenantId = getTenantId();

  const statusOptions = useMemo(() => deriveStatusOptions(traces), [traces]);

  const ruleOptions = useMemo(() => deriveRuleOptions(traces), [traces]);

  const filteredTraces = useMemo(
    () =>
      applyTraceFilters(traces, {
        status: traceFilterStatus,
        rule: traceFilterRule,
        query: traceFilterQuery,
        range: traceFilterRange
      }),
    [traces, traceFilterStatus, traceFilterRule, traceFilterQuery, traceFilterRange]
  );

  const loadTraces = useCallback(
    async (preserveSelection: boolean) => {
      await loadTraceList({
        preserveSelection,
        setTraces,
        setSelectedId
      });
    },
    []
  );

  const handleZipImport = useCallback(async () => {
    await runZipImport({
      zipFile,
      internalKey,
      setZipMessage,
      setZipUploading,
      setZipFile,
      loadTraces
    });
  }, [zipFile, internalKey, loadTraces]);

  useEffect(() => {
    loadTraces(false);
  }, [loadTraces]);

  useEffect(() => {
    reconcileFilteredTraceSelection({
      selectedId,
      filteredTraces,
      setSelectedId
    });
  }, [filteredTraces, selectedId]);

  useEffect(() => {
    return subscribeTraceListRefresh({
      internalKey,
      loadTraces
    });
  }, [loadTraces, internalKey]);

  useEffect(() => {
    if (viewMode !== "api") return;
    fetchJson<ApiGraphResponse>(`${API_BASE}/api-graph`).then((data) => {
      if (data) {
        setApiGraph(data);
      }
    });
  }, [viewMode]);

  useEffect(() => {
    if (viewMode !== "api") return;
    setSelectedApiNode(null);
    setSelectedApiOp(null);
    setApiExpandedRuleIds([]);
    setApiFocusedRuleId(null);
    setInspectorOpen(false);
  }, [viewMode]);

  useEffect(() => {
    if (!selectedId) {
      setTrace(null);
      setTraceManifest(null);
      setDetailLoading(false);
      setDetailError(null);
      return;
    }
    let mounted = true;
    setTrace(null);
    setTraceManifest(null);
    setDetailLoading(false);
    setDetailError(null);
    setRecordIndex(0);
    setSelectedNode(null);
    setSelectedOp(null);
    setExpandedRuleIds([]);
    setFocusedRuleId(null);
    setInspectorOpen(false);
    setPinnedPositions({});
    (async () => {
      await loadSelectedTraceDetail({
        selectedId,
        isMounted: () => mounted,
        setTrace,
        setTraceManifest,
        setDetailLoading,
        setDetailError
      });
    })();
    return () => {
      mounted = false;
    };
  }, [selectedId]);

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
  const apiGraphLayout = useMemo(() => {
    if (!apiGraph) {
      return { nodes: [], edges: [], nodeMap: new Map<string, ApiGraphNode>(), edgeLabelMap: new Map<string, string>() };
    }
    return buildApiGraph(apiGraph);
  }, [apiGraph]);
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

  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem("traceDurationUnit", durationUnit);
  }, [durationUnit]);
  const detailNodeMap = useMemo(() => collectDetailNodeMap(bundles), [bundles]);
  const apiDetailNodeMap = useMemo(() => collectApiDetailNodeMap(apiBundles), [apiBundles]);
  const detailStatus = resolveDetailStatus(traceManifest, trace);
  const detailReason = resolveDetailReason(traceManifest, trace);
  const detailAvailable = detailStatus ? detailStatus === "full" : true;
  const hasDetail = viewMode === "trace" && expandedRuleIds.length > 0 && detailAvailable;
  const apiHasDetail = viewMode === "api" && apiExpandedRuleIds.length > 0;
  const recordLabel = resolveRecordLabel(isFinalizeSelected, currentRecord);
  const detailLabel = resolveDetailLabel(detailStatus, detailLoading, hasDetail);

  return (
    <div className="app">
      <div className="app__glow" />
      <Topbar
        viewMode={viewMode}
        setViewMode={setViewMode}
        traceTitleId={currentTrace?.rule?.path ?? currentTrace?.trace_id ?? "no-trace"}
        apiTitleId={selectedApiNode?.path ?? "api-graph"}
        detailLabel={detailLabel}
        filteredTraceCount={filteredTraces.length}
        traceCount={traces.length}
        recordLabel={recordLabel}
        apiRuleCount={apiGraph?.nodes.length ?? 0}
        apiEdgeCount={apiGraph?.edges.length ?? 0}
      />

      <main className="stage">
        <TraceCanvas
          viewMode={viewMode}
          trace={trace}
          traceResetKey={selectedId}
          activeGraph={activeGraph}
          hasDetail={hasDetail}
          apiHasDetail={apiHasDetail}
          overviewGraph={overviewGraph}
          expandedRuleIds={expandedRuleIds}
          setExpandedRuleIds={setExpandedRuleIds}
          setFocusedRuleId={setFocusedRuleId}
          setRecordIndex={setRecordIndex}
          setSelectedNode={setSelectedNode}
          setSelectedOp={setSelectedOp}
          setInspectorOpen={setInspectorOpen}
          setTraceInspectorSections={setTraceInspectorSections}
          detailNodeMap={detailNodeMap}
          apiGraphNodeMap={apiGraphLayout.nodeMap}
          apiDetailNodeMap={apiDetailNodeMap}
          apiExpandedRuleIds={apiExpandedRuleIds}
          setApiExpandedRuleIds={setApiExpandedRuleIds}
          setApiFocusedRuleId={setApiFocusedRuleId}
          setSelectedApiNode={setSelectedApiNode}
          setSelectedApiOp={setSelectedApiOp}
          pinnedPositions={pinnedPositions}
          setPinnedPositions={setPinnedPositions}
          apiPinnedPositions={apiPinnedPositions}
          setApiPinnedPositions={setApiPinnedPositions}
        />

        {viewMode === "trace" && (
          <TraceListPanel
            traceListOpen={traceListOpen}
            setTraceListOpen={setTraceListOpen}
            internalKey={internalKey}
            durationUnit={durationUnit}
            setDurationUnit={setDurationUnit}
            traceFilterQuery={traceFilterQuery}
            setTraceFilterQuery={setTraceFilterQuery}
            traceFilterStatus={traceFilterStatus}
            setTraceFilterStatus={setTraceFilterStatus}
            traceFilterRule={traceFilterRule}
            setTraceFilterRule={setTraceFilterRule}
            traceFilterRange={traceFilterRange}
            setTraceFilterRange={setTraceFilterRange}
            statusOptions={statusOptions}
            ruleOptions={ruleOptions}
            filteredTraces={filteredTraces}
            traces={traces}
            selectedId={selectedId}
            setSelectedId={setSelectedId}
            detailStatus={detailStatus}
            detailError={detailError}
            detailReason={detailReason}
            setZipMessage={setZipMessage}
            setZipModalOpen={setZipModalOpen}
          />
        )}

        {hasDetail && viewMode === "trace" && (
          <RecordPanel
            currentTrace={currentTrace}
            recordIndex={recordIndex}
            setRecordIndex={setRecordIndex}
            setSelectedNode={setSelectedNode}
            setSelectedOp={setSelectedOp}
            setInspectorOpen={setInspectorOpen}
            finalizePayload={finalizePayload}
            isFinalizeSelected={isFinalizeSelected}
            durationUnit={durationUnit}
          />
        )}

        <InspectorDrawer
          inspectorOpen={inspectorOpen}
          setInspectorOpen={setInspectorOpen}
          viewMode={viewMode}
          selectedNode={selectedNode}
          selectedOp={selectedOp}
          setSelectedOp={setSelectedOp}
          selectedApiNode={selectedApiNode}
          selectedApiOp={selectedApiOp}
          traceInspectorSections={traceInspectorSections}
          setTraceInspectorSections={setTraceInspectorSections}
          apiInspectorSections={apiInspectorSections}
          setApiInspectorSections={setApiInspectorSections}
          isFinalizeSelected={isFinalizeSelected}
          finalizePayload={finalizePayload}
          durationUnit={durationUnit}
        />
        {zipModalOpen && (
          <ZipImportModal
            tenantId={tenantId}
            zipMessage={zipMessage}
            zipUploading={zipUploading}
            zipFile={zipFile}
            setZipFile={setZipFile}
            setZipMessage={setZipMessage}
            setZipModalOpen={setZipModalOpen}
            handleZipImport={handleZipImport}
          />
        )}
      </main>
    </div>
  );
}
