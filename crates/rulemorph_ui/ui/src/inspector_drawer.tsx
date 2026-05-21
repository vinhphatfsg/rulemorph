import type { Dispatch, SetStateAction } from "react";
import clsx from "clsx";
import {
  formatDuration,
  formatDurationParts,
  isErrorStatus,
  resolveDurationUs,
  type DurationUnit
} from "./trace_list_helpers";
import type { TraceNode, TracePayload } from "./trace_payload";
import type { ApiGraphNode, ApiGraphOp } from "./trace_graph";

type TraceInspectorSections = {
  finalize: boolean;
  step: boolean;
  opList: boolean;
  opResult: boolean;
};

type ApiInspectorSections = {
  opList: boolean;
  memo: boolean;
};

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

function renderJsonBlock(label: string, value: unknown) {
  const hasValue = !(value === null || value === undefined);
  const isWide = label === "input" || label === "output" || label === "error";
  return (
    <div className={clsx("inspector-block", isWide && "inspector-block--wide")} key={label}>
      <div className="inspector-block__header">
        <div className="inspector-block__title">
          <span className="inspector-block__line" />
          <span className="inspector-block__name">{label}</span>
        </div>
        <span className="inspector-block__meta">{hasValue ? "json" : "empty"}</span>
      </div>
      <pre className="inspector-block__body">
        {hasValue ? JSON.stringify(value, null, 2) : "なし"}
      </pre>
    </div>
  );
}

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
  const finalizeDuration = resolveDurationUs(
    finalizePayload?.duration_us,
    finalizePayload?.duration_ms
  );
  const finalizeDurationParts = formatDurationParts(finalizeDuration, durationUnit);
  const finalizeNodes = finalizePayload?.nodes ?? [];
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
  const traceFinalizeOpen = traceInspectorSections.finalize;
  const traceStepOpen = traceInspectorSections.step;
  const traceOpListOpen = traceInspectorSections.opList;
  const traceOpResultOpen = traceInspectorSections.opResult;
  const apiOpListOpen = apiInspectorSections.opList;
  const apiMemoOpen = apiInspectorSections.memo;

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
          <>
            <div
              className={clsx(
                "inspector__section inspector__section--oplist",
                !apiOpListOpen && "is-collapsed"
              )}
            >
              <button
                className="inspector__section-toggle"
                aria-expanded={apiOpListOpen}
                onClick={() =>
                  setApiInspectorSections((prev) => ({ ...prev, opList: !prev.opList }))
                }
              >
                <h3>OP一覧</h3>
                <span className="inspector__chevron">{apiOpListOpen ? "v" : ">"}</span>
              </button>
              {apiOpListOpen && (
                <div className="op-list">
                  {(selectedApiNode?.ops ?? []).length === 0 && (
                    <p className="muted">このルールにOPはありません。</p>
                  )}
                  {(selectedApiNode?.ops ?? []).map((op, index) => (
                    <div
                      key={`${op.label}-${index}`}
                      className={clsx(
                        "op-item is-static",
                        selectedApiOp?.label === op.label && "is-active"
                      )}
                    >
                      <span>{op.label}</span>
                      <span className="muted">{op.detail ?? selectedApiNode?.kind}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
            <div className={clsx("inspector__section", !apiMemoOpen && "is-collapsed")}>
              <button
                className="inspector__section-toggle"
                aria-expanded={apiMemoOpen}
                onClick={() =>
                  setApiInspectorSections((prev) => ({ ...prev, memo: !prev.memo }))
                }
              >
                <h3>処理メモ</h3>
                <span className="inspector__chevron">{apiMemoOpen ? "v" : ">"}</span>
              </button>
              {apiMemoOpen && (
                <div className="inspector__content">
                  <p className="muted">
                    実値はありません。ルールファイルに記載された処理内容のみ表示しています。
                  </p>
                </div>
              )}
            </div>
          </>
        ) : (
          <>
            {isFinalizeSelected && (
              <div
                className={clsx(
                  "inspector__section inspector__section--finalize",
                  !traceFinalizeOpen && "is-collapsed"
                )}
              >
                <button
                  className="inspector__section-toggle"
                  aria-expanded={traceFinalizeOpen}
                  onClick={() =>
                    setTraceInspectorSections((prev) => ({ ...prev, finalize: !prev.finalize }))
                  }
                >
                  <h3>Finalize</h3>
                  <span className="inspector__chevron">{traceFinalizeOpen ? "v" : ">"}</span>
                </button>
                {traceFinalizeOpen && (
                  <div className="inspector__content">
                    {!finalizePayload ? (
                      <p className="muted">finalize のデータがありません。</p>
                    ) : (
                      <>
                        <div className="step-badges">
                          <span
                            className={clsx(
                              "chip",
                              isErrorStatus(finalizePayload.status) && "chip--error"
                            )}
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
                              <div
                                key={node.id ?? `finalize-${index}`}
                                className="op-item is-static"
                              >
                                <span>{node.label ?? node.kind}</span>
                                <span className="op-item__meta">
                                  <span className="muted">{node.kind}</span>
                                  {resolveDurationUs(node.duration_us, node.duration_ms) !==
                                    undefined && (
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
            )}
            {!isFinalizeSelected && (
              <>
                <div
                  className={clsx(
                    "inspector__section inspector__section--opresult",
                    !traceStepOpen && "is-collapsed"
                  )}
                >
                  <button
                    className="inspector__section-toggle"
                    aria-expanded={traceStepOpen}
                    onClick={() =>
                      setTraceInspectorSections((prev) => ({ ...prev, step: !prev.step }))
                    }
                  >
                    <h3>Step結果</h3>
                    <span className="inspector__chevron">{traceStepOpen ? "v" : ">"}</span>
                  </button>
                  {traceStepOpen && (
                    <div className="inspector__content">
                      {!selectedNode ? (
                        <p className="muted">ノードを選択して詳細を表示してください。</p>
                      ) : (
                        <>
                          <div className="step-badges">
                            <span
                              className={clsx(
                                "chip",
                                isErrorStatus(selectedNode.status) && "chip--error"
                              )}
                            >
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
                            {stepBranchTaken && (
                              <span className="chip">branch: {stepBranchTaken}</span>
                            )}
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

                <div
                  className={clsx(
                    "inspector__section inspector__section--oplist",
                    !traceOpListOpen && "is-collapsed"
                  )}
                >
                  <button
                    className="inspector__section-toggle"
                    aria-expanded={traceOpListOpen}
                    onClick={() =>
                      setTraceInspectorSections((prev) => ({ ...prev, opList: !prev.opList }))
                    }
                  >
                    <h3>OP一覧</h3>
                    <span className="inspector__chevron">{traceOpListOpen ? "v" : ">"}</span>
                  </button>
                  {traceOpListOpen && (
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
                            {resolveDurationUs(child.duration_us, child.duration_ms) !==
                              undefined && (
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

                <div
                  className={clsx(
                    "inspector__section inspector__section--opresult",
                    !traceOpResultOpen && "is-collapsed"
                  )}
                >
                  <button
                    className="inspector__section-toggle"
                    aria-expanded={traceOpResultOpen}
                    onClick={() =>
                      setTraceInspectorSections((prev) => ({ ...prev, opResult: !prev.opResult }))
                    }
                  >
                    <h3>OP結果</h3>
                    <span className="inspector__chevron">{traceOpResultOpen ? "v" : ">"}</span>
                  </button>
                  {traceOpResultOpen && (
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
                                            {step.input !== undefined
                                              ? JSON.stringify(step.input)
                                              : "なし"}
                                          </pre>
                                        </div>
                                        <div className="pipe-step__cell">
                                          <span className="pipe-step__name">output</span>
                                          <pre>
                                            {step.output !== undefined
                                              ? JSON.stringify(step.output)
                                              : "なし"}
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
              </>
            )}
          </>
        )}
      </aside>
    </>
  );
}
