import type { Dispatch, SetStateAction } from "react";
import clsx from "clsx";
import {
  formatDurationParts,
  resolveDurationUs,
  type DurationUnit
} from "./trace_list_helpers";
import type { TraceNode } from "./trace_payload";
import type { TraceInspectorSections } from "./inspector_section_types";
import { renderJsonBlock } from "./inspector_json_block";

type TraceOpResultSectionProps = {
  selectedOp: TraceNode | null;
  opResultOpen: boolean;
  setTraceInspectorSections: Dispatch<SetStateAction<TraceInspectorSections>>;
  durationUnit: DurationUnit;
};

export function TraceOpResultSection({
  selectedOp,
  opResultOpen,
  setTraceInspectorSections,
  durationUnit
}: TraceOpResultSectionProps) {
  return (
    <div
      className={clsx(
        "inspector__section inspector__section--opresult",
        !opResultOpen && "is-collapsed"
      )}
    >
      <button
        className="inspector__section-toggle"
        aria-expanded={opResultOpen}
        onClick={() =>
          setTraceInspectorSections((prev) => ({ ...prev, opResult: !prev.opResult }))
        }
      >
        <h3>OP結果</h3>
        <span className="inspector__chevron">{opResultOpen ? "v" : ">"}</span>
      </button>
      {opResultOpen && (
        <div className="inspector__content">
          {(() => {
            const op = selectedOp as any;
            const input = op?.input ?? null;
            const pipe = op?.pipe_value ?? null;
            const args = op?.args ?? null;
            const output = op?.output ?? null;
            const opDuration = resolveDurationUs(op?.duration_us, op?.duration_ms);
            const opDurationParts = formatDurationParts(opDuration, durationUnit);
            const pipeSteps = (op?.pipe_steps ?? []) as {
              index: number;
              label: string;
              input?: unknown;
              output?: unknown;
            }[];
            return (
              <>
                {opDurationParts && (
                  <div className="step-badges">
                    <span className="chip">
                      duration: {opDurationParts.value}{" "}
                      <span className="chip__unit">{opDurationParts.unit}</span>
                    </span>
                  </div>
                )}
                <div className="inspector-grid">
                  {renderJsonBlock("input", input)}
                  {renderJsonBlock("pipe", pipe)}
                  {renderJsonBlock("args", args)}
                  {renderJsonBlock("output", output)}
                </div>
                <div className="pipe-steps">
                  <div className="pipe-steps__header">
                    <h4>ステップ推移</h4>
                    <span className="muted">{pipeSteps.length} steps</span>
                  </div>
                  {pipeSteps.length === 0 ? (
                    <p className="muted">ステップがありません。</p>
                  ) : (
                    <div className="pipe-steps__list">
                      {pipeSteps.map((step) => (
                        <div className="pipe-step" key={step.index}>
                          <div className="pipe-step__title">
                            <span className="pipe-step__index">#{step.index}</span>
                            <span className="pipe-step__label">{step.label}</span>
                          </div>
                          <div className="pipe-step__io">
                            <div className="pipe-step__cell">
                              <span className="pipe-step__name">input</span>
                              <pre>
                                {step.input !== undefined ? JSON.stringify(step.input) : "なし"}
                              </pre>
                            </div>
                            <div className="pipe-step__cell">
                              <span className="pipe-step__name">output</span>
                              <pre>
                                {step.output !== undefined ? JSON.stringify(step.output) : "なし"}
                              </pre>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </>
            );
          })()}
        </div>
      )}
    </div>
  );
}
