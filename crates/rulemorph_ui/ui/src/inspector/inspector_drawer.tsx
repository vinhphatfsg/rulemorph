import type { Dispatch, SetStateAction } from "react";
import clsx from "clsx";
import type { DurationUnit } from "../trace_list/trace_list_helpers";
import type { TraceNode, TracePayload } from "../api/trace_payload";
import type { ApiGraphNode, ApiGraphOp } from "../trace_graph/trace_graph";
import { ApiInspectorContent } from "./inspector_api_sections";
import type { ApiInspectorSections, TraceInspectorSections } from "./inspector_section_types";
import { TraceInspectorContent } from "./inspector_trace_sections";

type InspectorDrawerProps = {
  inspectorOpen: boolean;
  setInspectorOpen: Dispatch<SetStateAction<boolean>>;
  viewMode: "trace" | "api";
  selectedNode: TraceNode | null;
  selectedOp: TraceNode | null;
  setSelectedOp: Dispatch<SetStateAction<TraceNode | null>>;
  selectedApiNode: ApiGraphNode | null;
  selectedApiOp: ApiGraphOp | null;
  traceInspectorSections: TraceInspectorSections;
  setTraceInspectorSections: Dispatch<SetStateAction<TraceInspectorSections>>;
  apiInspectorSections: ApiInspectorSections;
  setApiInspectorSections: Dispatch<SetStateAction<ApiInspectorSections>>;
  isFinalizeSelected: boolean;
  finalizePayload: TracePayload["finalize"] | null;
  durationUnit: DurationUnit;
};

export function InspectorDrawer({
  inspectorOpen,
  setInspectorOpen,
  viewMode,
  selectedNode,
  selectedOp,
  setSelectedOp,
  selectedApiNode,
  selectedApiOp,
  traceInspectorSections,
  setTraceInspectorSections,
  apiInspectorSections,
  setApiInspectorSections,
  isFinalizeSelected,
  finalizePayload,
  durationUnit
}: InspectorDrawerProps) {
  return (
    <>
      <button
        className={clsx("inspector-toggle", inspectorOpen && "is-open")}
        onClick={() => setInspectorOpen((prev) => !prev)}
      >
        {inspectorOpen ? "Inspectorを閉じる" : "Inspectorを見る"}
      </button>

      <aside className={clsx("inspector-drawer", inspectorOpen && "is-open")}>
        <div className="inspector__header">
          <div>
            <h2>Inspector</h2>
            <p>
              {viewMode === "trace"
                ? selectedNode
                  ? selectedNode.label
                  : "ノードを選択して詳細を表示"
                : selectedApiNode
                  ? selectedApiNode.label
                  : "ノードを選択して詳細を表示"}
            </p>
          </div>
          <button className="icon-button" onClick={() => setInspectorOpen(false)}>
            ×
          </button>
        </div>

        {viewMode === "api" ? (
          <ApiInspectorContent
            selectedApiNode={selectedApiNode}
            selectedApiOp={selectedApiOp}
            apiInspectorSections={apiInspectorSections}
            setApiInspectorSections={setApiInspectorSections}
          />
        ) : (
          <TraceInspectorContent
            selectedNode={selectedNode}
            selectedOp={selectedOp}
            setSelectedOp={setSelectedOp}
            traceInspectorSections={traceInspectorSections}
            setTraceInspectorSections={setTraceInspectorSections}
            isFinalizeSelected={isFinalizeSelected}
            finalizePayload={finalizePayload}
            durationUnit={durationUnit}
          />
        )}
      </aside>
    </>
  );
}
