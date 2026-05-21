import type { Dispatch, SetStateAction } from "react";
import type { Edge, Node } from "reactflow";
import { InspectorDrawer } from "./inspector_drawer";
import type { ApiInspectorSections, TraceInspectorSections } from "./inspector_section_types";
import { RecordPanel } from "./record_panel";
import { TraceCanvas } from "./trace_canvas";
import type { DurationUnit, TimeRange, TraceListItem } from "./trace_list_helpers";
import { TraceListPanel, ZipImportModal } from "./trace_list_panel";
import type { TraceNode, TracePayload } from "./trace_payload";
import type {
  ApiDetailEntry,
  ApiGraphNode,
  ApiGraphOp,
  DetailEntry,
  OverviewGraph
} from "./trace_graph";

type NodePositions = Record<string, { x: number; y: number }>;

type AppStageProps = {
  viewMode: "trace" | "api";
  trace: TracePayload | null;
  traceResetKey: string | null;
  activeGraph: { nodes: Node[]; edges: Edge[] };
  hasDetail: boolean;
  apiHasDetail: boolean;
  overviewGraph: OverviewGraph;
  expandedRuleIds: string[];
  setExpandedRuleIds: Dispatch<SetStateAction<string[]>>;
  setFocusedRuleId: Dispatch<SetStateAction<string | null>>;
  recordIndex: number;
  setRecordIndex: Dispatch<SetStateAction<number>>;
  selectedNode: TraceNode | null;
  setSelectedNode: Dispatch<SetStateAction<TraceNode | null>>;
  selectedOp: TraceNode | null;
  setSelectedOp: Dispatch<SetStateAction<TraceNode | null>>;
  inspectorOpen: boolean;
  setInspectorOpen: Dispatch<SetStateAction<boolean>>;
  traceInspectorSections: TraceInspectorSections;
  setTraceInspectorSections: Dispatch<SetStateAction<TraceInspectorSections>>;
  detailNodeMap: Map<string, DetailEntry>;
  apiGraphNodeMap: Map<string, ApiGraphNode>;
  apiDetailNodeMap: Map<string, ApiDetailEntry>;
  apiExpandedRuleIds: string[];
  setApiExpandedRuleIds: Dispatch<SetStateAction<string[]>>;
  setApiFocusedRuleId: Dispatch<SetStateAction<string | null>>;
  selectedApiNode: ApiGraphNode | null;
  setSelectedApiNode: Dispatch<SetStateAction<ApiGraphNode | null>>;
  selectedApiOp: ApiGraphOp | null;
  setSelectedApiOp: Dispatch<SetStateAction<ApiGraphOp | null>>;
  pinnedPositions: NodePositions;
  setPinnedPositions: Dispatch<SetStateAction<NodePositions>>;
  apiPinnedPositions: NodePositions;
  setApiPinnedPositions: Dispatch<SetStateAction<NodePositions>>;
  traceListOpen: boolean;
  setTraceListOpen: Dispatch<SetStateAction<boolean>>;
  internalKey: string | null;
  durationUnit: DurationUnit;
  setDurationUnit: Dispatch<SetStateAction<DurationUnit>>;
  traceFilterQuery: string;
  setTraceFilterQuery: Dispatch<SetStateAction<string>>;
  traceFilterStatus: string;
  setTraceFilterStatus: Dispatch<SetStateAction<string>>;
  traceFilterRule: string;
  setTraceFilterRule: Dispatch<SetStateAction<string>>;
  traceFilterRange: TimeRange;
  setTraceFilterRange: Dispatch<SetStateAction<TimeRange>>;
  statusOptions: string[];
  ruleOptions: string[];
  filteredTraces: TraceListItem[];
  traces: TraceListItem[];
  selectedId: string | null;
  setSelectedId: Dispatch<SetStateAction<string | null>>;
  detailStatus?: string;
  detailError: string | null;
  detailReason: string[];
  setZipMessage: Dispatch<SetStateAction<string | null>>;
  setZipModalOpen: Dispatch<SetStateAction<boolean>>;
  currentTrace: TracePayload | null;
  finalizePayload: TracePayload["finalize"] | null;
  isFinalizeSelected: boolean;
  apiInspectorSections: ApiInspectorSections;
  setApiInspectorSections: Dispatch<SetStateAction<ApiInspectorSections>>;
  zipModalOpen: boolean;
  tenantId: string | null;
  zipMessage: string | null;
  zipUploading: boolean;
  zipFile: File | null;
  setZipFile: Dispatch<SetStateAction<File | null>>;
  handleZipImport: () => void;
};

export function AppStage({
  viewMode,
  trace,
  traceResetKey,
  activeGraph,
  hasDetail,
  apiHasDetail,
  overviewGraph,
  expandedRuleIds,
  setExpandedRuleIds,
  setFocusedRuleId,
  recordIndex,
  setRecordIndex,
  selectedNode,
  setSelectedNode,
  selectedOp,
  setSelectedOp,
  inspectorOpen,
  setInspectorOpen,
  traceInspectorSections,
  setTraceInspectorSections,
  detailNodeMap,
  apiGraphNodeMap,
  apiDetailNodeMap,
  apiExpandedRuleIds,
  setApiExpandedRuleIds,
  setApiFocusedRuleId,
  selectedApiNode,
  setSelectedApiNode,
  selectedApiOp,
  setSelectedApiOp,
  pinnedPositions,
  setPinnedPositions,
  apiPinnedPositions,
  setApiPinnedPositions,
  traceListOpen,
  setTraceListOpen,
  internalKey,
  durationUnit,
  setDurationUnit,
  traceFilterQuery,
  setTraceFilterQuery,
  traceFilterStatus,
  setTraceFilterStatus,
  traceFilterRule,
  setTraceFilterRule,
  traceFilterRange,
  setTraceFilterRange,
  statusOptions,
  ruleOptions,
  filteredTraces,
  traces,
  selectedId,
  setSelectedId,
  detailStatus,
  detailError,
  detailReason,
  setZipMessage,
  setZipModalOpen,
  currentTrace,
  finalizePayload,
  isFinalizeSelected,
  apiInspectorSections,
  setApiInspectorSections,
  zipModalOpen,
  tenantId,
  zipMessage,
  zipUploading,
  zipFile,
  setZipFile,
  handleZipImport
}: AppStageProps) {
  return (
    <main className="stage">
      <TraceCanvas
        viewMode={viewMode}
        trace={trace}
        traceResetKey={traceResetKey}
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
        apiGraphNodeMap={apiGraphNodeMap}
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
  );
}
