import "reactflow/dist/style.css";
import {
  __resetAuthCachesForTest as resetAuthCachesForTest,
  getApiKey,
  getInternalKey
} from "../api/auth";
import { __resetTenantCachesForTest, getTenantId } from "../api/tenant";
import { type TraceNode, type TracePayload, type TraceRecord } from "../api/trace_payload";
import { Topbar } from "./topbar";
import { useAuthSecretCapture, useStoredDurationUnit } from "./app_runtime";
import { useAppGraphViewState } from "./app_graph_view_state";
import { AppStage } from "./app_stage";
import { useApiGraphState } from "./use_api_graph_state";
import { useAppModeState } from "./use_app_mode_state";
import { useInspectorState } from "./use_inspector_state";
import { useTraceDetailState } from "./use_trace_detail_state";
import { useTraceListState } from "./use_trace_list_state";
import { useZipImportState } from "./use_zip_import_state";

export { getApiKey, getInternalKey } from "../api/auth";
export { __getTenantIdFromQueryOrStorageForTest, resolveTenantId } from "../api/tenant";
export type { EndpointRule, EndpointSpec, TraceNode, TracePayload, TraceRecord } from "../api/trace_payload";
export { buildOverviewGraph } from "../trace_graph/trace_graph";

export function __resetAuthCachesForTest(): void {
  resetAuthCachesForTest();
  __resetTenantCachesForTest();
}

export default function App() {
  useAuthSecretCapture();
  const { viewMode, setViewMode } = useAppModeState();
  const [durationUnit, setDurationUnit] = useStoredDurationUnit();
  const internalKey = getInternalKey();
  const tenantId = getTenantId();
  const {
    inspectorOpen,
    setInspectorOpen,
    traceInspectorSections,
    setTraceInspectorSections,
    apiInspectorSections,
    setApiInspectorSections
  } = useInspectorState();
  const {
    traces,
    traceFilterStatus,
    setTraceFilterStatus,
    traceFilterRule,
    setTraceFilterRule,
    traceFilterQuery,
    setTraceFilterQuery,
    traceFilterRange,
    setTraceFilterRange,
    selectedId,
    setSelectedId,
    traceListOpen,
    setTraceListOpen,
    statusOptions,
    ruleOptions,
    filteredTraces,
    loadTraces
  } = useTraceListState({ internalKey });
  const {
    trace,
    traceManifest,
    detailLoading,
    detailError,
    expandedRuleIds,
    setExpandedRuleIds,
    focusedRuleId,
    setFocusedRuleId,
    recordIndex,
    setRecordIndex,
    selectedNode,
    setSelectedNode,
    selectedOp,
    setSelectedOp,
    pinnedPositions,
    setPinnedPositions
  } = useTraceDetailState({ selectedId, setInspectorOpen });
  const {
    apiGraph,
    selectedApiNode,
    setSelectedApiNode,
    selectedApiOp,
    setSelectedApiOp,
    apiExpandedRuleIds,
    setApiExpandedRuleIds,
    apiFocusedRuleId,
    setApiFocusedRuleId,
    apiPinnedPositions,
    setApiPinnedPositions
  } = useApiGraphState({ viewMode, setInspectorOpen });
  const {
    zipModalOpen,
    setZipModalOpen,
    zipFile,
    setZipFile,
    zipMessage,
    setZipMessage,
    zipUploading,
    handleZipImport
  } = useZipImportState({ internalKey, loadTraces });

  const {
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
  } = useAppGraphViewState({
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
  });

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

      <AppStage
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
        recordIndex={recordIndex}
        setRecordIndex={setRecordIndex}
        selectedNode={selectedNode}
        setSelectedNode={setSelectedNode}
        selectedOp={selectedOp}
        setSelectedOp={setSelectedOp}
        inspectorOpen={inspectorOpen}
        setInspectorOpen={setInspectorOpen}
        traceInspectorSections={traceInspectorSections}
        setTraceInspectorSections={setTraceInspectorSections}
        detailNodeMap={detailNodeMap}
        apiGraphNodeMap={apiGraphLayout.nodeMap}
        apiDetailNodeMap={apiDetailNodeMap}
        apiExpandedRuleIds={apiExpandedRuleIds}
        setApiExpandedRuleIds={setApiExpandedRuleIds}
        setApiFocusedRuleId={setApiFocusedRuleId}
        selectedApiNode={selectedApiNode}
        setSelectedApiNode={setSelectedApiNode}
        selectedApiOp={selectedApiOp}
        setSelectedApiOp={setSelectedApiOp}
        pinnedPositions={pinnedPositions}
        setPinnedPositions={setPinnedPositions}
        apiPinnedPositions={apiPinnedPositions}
        setApiPinnedPositions={setApiPinnedPositions}
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
        currentTrace={currentTrace}
        finalizePayload={finalizePayload}
        isFinalizeSelected={isFinalizeSelected}
        apiInspectorSections={apiInspectorSections}
        setApiInspectorSections={setApiInspectorSections}
        zipModalOpen={zipModalOpen}
        tenantId={tenantId}
        zipMessage={zipMessage}
        zipUploading={zipUploading}
        zipFile={zipFile}
        setZipFile={setZipFile}
        handleZipImport={handleZipImport}
      />
    </div>
  );
}
