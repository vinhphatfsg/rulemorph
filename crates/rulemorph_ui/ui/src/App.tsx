import { useCallback, useEffect, useMemo, useState } from "react";
import "reactflow/dist/style.css";
import {
  __resetAuthCachesForTest as resetAuthCachesForTest,
  captureAuthSecretsFromLocation,
  getApiKey,
  getInternalKey
} from "./auth";
import { __resetTenantCachesForTest, getTenantId } from "./tenant";
import {
  API_BASE,
  INTERNAL_BASE,
  fetchJson,
  loadFinalize,
  loadNodeChunks,
  loadRecordChunks
} from "./api_client";
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
  mergeNodesIntoRecords,
  normalizeTracePayload,
  type TraceManifest,
  type TraceNode,
  type TracePayload,
  type TraceRecord
} from "./trace_payload";
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
      const list = await fetchJson<{ traces: TraceListItem[] }>(`${API_BASE}/traces`);
      const data = list?.traces?.length ? list.traces : [];
      setTraces(data);
      setSelectedId((prev) => {
        if (preserveSelection && prev && data.some((item) => item.trace_id === prev)) {
          return prev;
        }
        return data[0]?.trace_id ?? null;
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
    if (!selectedId) {
      if (filteredTraces.length > 0) {
        setSelectedId(filteredTraces[0].trace_id ?? null);
      }
      return;
    }
    if (!filteredTraces.some((item) => item.trace_id === selectedId)) {
      setSelectedId(filteredTraces[0]?.trace_id ?? null);
    }
  }, [filteredTraces, selectedId]);

  useEffect(() => {
    const usePolling = API_BASE.startsWith("/api");
    if (usePolling || internalKey) {
      const timer = window.setInterval(() => {
        loadTraces(true);
      }, 5000);
      return () => {
        window.clearInterval(timer);
      };
    }
    const source = new EventSource(`${INTERNAL_BASE}/stream`);
    const onUpdate = () => {
      loadTraces(true);
    };
    source.addEventListener("traces", onUpdate);
    source.onerror = () => {
      // keep EventSource alive; browser will retry automatically
    };
    return () => {
      source.removeEventListener("traces", onUpdate);
      source.close();
    };
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
      const manifestResult = await fetchJson<{ manifest: TraceManifest }>(
        `${API_BASE}/traces/${selectedId}/manifest`
      );
      if (!mounted) return;
      if (!manifestResult?.manifest) {
        const result = await fetchJson<{ trace: TracePayload }>(`${API_BASE}/traces/${selectedId}`);
        if (!mounted) return;
        setTrace(normalizeTracePayload(result?.trace ?? null));
        return;
      }
      const manifest = manifestResult.manifest;
      setTraceManifest(manifest);
      const baseTrace: TracePayload = {
        trace_id: manifest.trace_id,
        timestamp: manifest.timestamp,
        status: manifest.status,
        rule: manifest.rule,
        rule_source: manifest.rule_source,
        records: [],
        finalize: undefined,
        summary: manifest.summary,
        input_format: manifest.input_format
      };
      setTrace(baseTrace);
      const detail = manifest.detail;
      if (!detail || detail.status !== "full") {
        return;
      }
      setDetailLoading(true);
      try {
        const records = await loadRecordChunks(selectedId, detail);
        if (!mounted) return;
        let nextTrace: TracePayload = { ...baseTrace, records };
        setTrace(nextTrace);
        if (detail.layout === "records_nodes_split" && (detail.nodes?.length ?? 0) > 0) {
          const nodesByRecord = await loadNodeChunks(selectedId, detail);
          if (!mounted) return;
          const mergedRecords = mergeNodesIntoRecords(records, nodesByRecord);
          nextTrace = { ...nextTrace, records: mergedRecords };
          setTrace(nextTrace);
        }
        if (detail.finalize) {
          const finalize = await loadFinalize(selectedId);
          if (!mounted) return;
          if (finalize) {
            setTrace((prev) => (prev ? { ...prev, finalize } : { ...nextTrace, finalize }));
          }
        }
      } catch (err) {
        if (!mounted) return;
        const fallback = await fetchJson<{ trace: TracePayload }>(`${API_BASE}/traces/${selectedId}`);
        if (!mounted) return;
        if (fallback?.trace) {
          setTraceManifest(null);
          setTrace(normalizeTracePayload(fallback.trace));
          setDetailError(null);
          return;
        }
        setDetailError("trace detail load failed");
      } finally {
        if (mounted) {
          setDetailLoading(false);
        }
      }
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
