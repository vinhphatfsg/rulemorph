import type { Dispatch, SetStateAction } from "react";
import { type DurationUnit } from "../trace_list/trace_list_helpers";
import type { TraceNode, TracePayload } from "../api/trace_payload";
import type { TraceInspectorSections } from "./inspector_section_types";
import { TraceOpResultSection } from "./inspector_trace_op_result";
import {
  TraceFinalizeSection,
  TraceOpListSection,
  TraceStepSection
} from "./inspector_trace_detail_sections";

type TraceInspectorContentProps = {
  selectedNode: TraceNode | null;
  selectedOp: TraceNode | null;
  setSelectedOp: Dispatch<SetStateAction<TraceNode | null>>;
  traceInspectorSections: TraceInspectorSections;
  setTraceInspectorSections: Dispatch<SetStateAction<TraceInspectorSections>>;
  isFinalizeSelected: boolean;
  finalizePayload: TracePayload["finalize"] | null;
  durationUnit: DurationUnit;
};

export function TraceInspectorContent({
  selectedNode,
  selectedOp,
  setSelectedOp,
  traceInspectorSections,
  setTraceInspectorSections,
  isFinalizeSelected,
  finalizePayload,
  durationUnit
}: TraceInspectorContentProps) {
  if (isFinalizeSelected) {
    return (
      <TraceFinalizeSection
        finalizeOpen={traceInspectorSections.finalize}
        setTraceInspectorSections={setTraceInspectorSections}
        finalizePayload={finalizePayload}
        durationUnit={durationUnit}
      />
    );
  }

  return (
    <>
      <TraceStepSection
        selectedNode={selectedNode}
        stepOpen={traceInspectorSections.step}
        setTraceInspectorSections={setTraceInspectorSections}
        durationUnit={durationUnit}
      />
      <TraceOpListSection
        selectedNode={selectedNode}
        selectedOp={selectedOp}
        setSelectedOp={setSelectedOp}
        opListOpen={traceInspectorSections.opList}
        setTraceInspectorSections={setTraceInspectorSections}
        durationUnit={durationUnit}
      />
      <TraceOpResultSection
        selectedOp={selectedOp}
        opResultOpen={traceInspectorSections.opResult}
        setTraceInspectorSections={setTraceInspectorSections}
        durationUnit={durationUnit}
      />
    </>
  );
}
