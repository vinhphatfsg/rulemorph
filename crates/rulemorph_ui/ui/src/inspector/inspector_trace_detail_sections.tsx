import type { Dispatch, SetStateAction } from "react";
import clsx from "clsx";
import {
  formatDuration,
  formatDurationParts,
  isErrorStatus,
  resolveDurationUs,
  type DurationUnit
} from "../trace_list/trace_list_helpers";
import type { TraceNode, TracePayload } from "../api/trace_payload";
import type { TraceInspectorSections } from "./inspector_section_types";
import { renderJsonBlock } from "./inspector_json_block";

type TraceFinalizeSectionProps = {
  finalizeOpen: boolean;
  setTraceInspectorSections: Dispatch<SetStateAction<TraceInspectorSections>>;
  finalizePayload: TracePayload["finalize"] | null;
  durationUnit: DurationUnit;
};

export function TraceFinalizeSection({
  finalizeOpen,
  setTraceInspectorSections,
  finalizePayload,
  durationUnit
}: TraceFinalizeSectionProps) {
  const finalizeDuration = resolveDurationUs(
    finalizePayload?.duration_us,
    finalizePayload?.duration_ms
  );
  const finalizeDurationParts = formatDurationParts(finalizeDuration, durationUnit);
  const finalizeNodes = finalizePayload?.nodes ?? [];

  return (
    <div
      className={clsx(
        "inspector__section inspector__section--finalize",
        !finalizeOpen && "is-collapsed"
      )}
    >
      <button
        className="inspector__section-toggle"
        aria-expanded={finalizeOpen}
        onClick={() =>
          setTraceInspectorSections((prev) => ({ ...prev, finalize: !prev.finalize }))
        }
      >
        <h3>Finalize</h3>
        <span className="inspector__chevron">{finalizeOpen ? "v" : ">"}</span>
      </button>
      {finalizeOpen && (
        <div className="inspector__content">
          {!finalizePayload ? (
            <p className="muted">finalize のデータがありません。</p>
          ) : (
            <>
              <div className="step-badges">
                <span
                  className={clsx("chip", isErrorStatus(finalizePayload.status) && "chip--error")}
                >
                  {finalizePayload.status ?? "ok"}
                </span>
                {finalizeDurationParts && (
                  <span className="chip">
                    duration: {finalizeDurationParts.value}{" "}
                    <span className="chip__unit">{finalizeDurationParts.unit}</span>
                  </span>
                )}
              </div>
              <div className="inspector-grid">
                {renderJsonBlock("input", finalizePayload.input ?? null)}
                {renderJsonBlock("output", finalizePayload.output ?? null)}
              </div>
              <div className="op-list">
                {finalizeNodes.length === 0 ? (
                  <p className="muted">finalize ノードがありません。</p>
                ) : (
                  finalizeNodes.map((node, index) => (
                    <div key={node.id ?? `finalize-${index}`} className="op-item is-static">
                      <span>{node.label ?? node.kind}</span>
                      <span className="op-item__meta">
                        <span className="muted">{node.kind}</span>
                        {resolveDurationUs(node.duration_us, node.duration_ms) !== undefined && (
                          <span className="op-item__duration">
                            {formatDuration(
                              resolveDurationUs(node.duration_us, node.duration_ms),
                              durationUnit
                            )}
                          </span>
                        )}
                      </span>
                    </div>
                  ))
                )}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}

type TraceStepSectionProps = {
  selectedNode: TraceNode | null;
  stepOpen: boolean;
  setTraceInspectorSections: Dispatch<SetStateAction<TraceInspectorSections>>;
  durationUnit: DurationUnit;
};

export function TraceStepSection({
  selectedNode,
  stepOpen,
  setTraceInspectorSections,
  durationUnit
}: TraceStepSectionProps) {
  const selectedMeta = (selectedNode?.meta ?? {}) as Record<string, unknown>;
  const stepRecordWhen =
    typeof selectedMeta["record_when"] === "boolean" ? selectedMeta["record_when"] : undefined;
  const stepAssertsOk =
    typeof selectedMeta["asserts_ok"] === "boolean" ? selectedMeta["asserts_ok"] : undefined;
  const stepBranchTaken =
    typeof selectedMeta["branch_taken"] === "string"
      ? String(selectedMeta["branch_taken"])
      : undefined;
  const stepDuration = resolveDurationUs(selectedNode?.duration_us, selectedNode?.duration_ms);
  const stepDurationParts = formatDurationParts(stepDuration, durationUnit);

  return (
    <div
      className={clsx(
        "inspector__section inspector__section--opresult",
        !stepOpen && "is-collapsed"
      )}
    >
      <button
        className="inspector__section-toggle"
        aria-expanded={stepOpen}
        onClick={() => setTraceInspectorSections((prev) => ({ ...prev, step: !prev.step }))}
      >
        <h3>Step結果</h3>
        <span className="inspector__chevron">{stepOpen ? "v" : ">"}</span>
      </button>
      {stepOpen && (
        <div className="inspector__content">
          {!selectedNode ? (
            <p className="muted">ノードを選択して詳細を表示してください。</p>
          ) : (
            <>
              <div className="step-badges">
                <span className={clsx("chip", isErrorStatus(selectedNode.status) && "chip--error")}>
                  {selectedNode.status ?? "ok"}
                </span>
                {stepDurationParts && (
                  <span className="chip">
                    duration: {stepDurationParts.value}{" "}
                    <span className="chip__unit">{stepDurationParts.unit}</span>
                  </span>
                )}
                {stepRecordWhen !== undefined && (
                  <span className="chip">record_when: {String(stepRecordWhen)}</span>
                )}
                {stepAssertsOk !== undefined && (
                  <span className="chip">asserts: {String(stepAssertsOk)}</span>
                )}
                {stepBranchTaken && <span className="chip">branch: {stepBranchTaken}</span>}
              </div>
              <div className="inspector-grid">
                {renderJsonBlock("input", selectedNode.input ?? null)}
                {renderJsonBlock("output", selectedNode.output ?? null)}
                {selectedNode.error && renderJsonBlock("error", selectedNode.error)}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}

type TraceOpListSectionProps = {
  selectedNode: TraceNode | null;
  selectedOp: TraceNode | null;
  setSelectedOp: Dispatch<SetStateAction<TraceNode | null>>;
  opListOpen: boolean;
  setTraceInspectorSections: Dispatch<SetStateAction<TraceInspectorSections>>;
  durationUnit: DurationUnit;
};

export function TraceOpListSection({
  selectedNode,
  selectedOp,
  setSelectedOp,
  opListOpen,
  setTraceInspectorSections,
  durationUnit
}: TraceOpListSectionProps) {
  return (
    <div
      className={clsx(
        "inspector__section inspector__section--oplist",
        !opListOpen && "is-collapsed"
      )}
    >
      <button
        className="inspector__section-toggle"
        aria-expanded={opListOpen}
        onClick={() => setTraceInspectorSections((prev) => ({ ...prev, opList: !prev.opList }))}
      >
        <h3>OP一覧</h3>
        <span className="inspector__chevron">{opListOpen ? "v" : ">"}</span>
      </button>
      {opListOpen && (
        <div className="op-list">
          {(selectedNode?.children ?? []).length === 0 && (
            <p className="muted">このノードにOPはありません。</p>
          )}
          {(selectedNode?.children ?? []).map((child) => (
            <button
              key={child.id}
              className={clsx("op-item", selectedOp?.id === child.id && "is-active")}
              onClick={() => setSelectedOp(child)}
            >
              <span>{child.label}</span>
              <span className="op-item__meta">
                <span className="muted">{child.meta?.op ?? "op"}</span>
                {resolveDurationUs(child.duration_us, child.duration_ms) !== undefined && (
                  <span className="op-item__duration">
                    {formatDuration(
                      resolveDurationUs(child.duration_us, child.duration_ms),
                      durationUnit
                    )}
                  </span>
                )}
              </span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
