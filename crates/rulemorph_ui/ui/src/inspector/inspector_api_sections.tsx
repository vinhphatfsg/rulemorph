import type { Dispatch, SetStateAction } from "react";
import clsx from "clsx";
import type { ApiGraphNode, ApiGraphOp } from "../trace_graph/trace_graph";
import type { ApiInspectorSections } from "./inspector_section_types";

type ApiInspectorContentProps = {
  selectedApiNode: ApiGraphNode | null;
  selectedApiOp: ApiGraphOp | null;
  apiInspectorSections: ApiInspectorSections;
  setApiInspectorSections: Dispatch<SetStateAction<ApiInspectorSections>>;
};

export function ApiInspectorContent({
  selectedApiNode,
  selectedApiOp,
  apiInspectorSections,
  setApiInspectorSections
}: ApiInspectorContentProps) {
  const apiOpListOpen = apiInspectorSections.opList;
  const apiMemoOpen = apiInspectorSections.memo;

  return (
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
          onClick={() => setApiInspectorSections((prev) => ({ ...prev, opList: !prev.opList }))}
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
          onClick={() => setApiInspectorSections((prev) => ({ ...prev, memo: !prev.memo }))}
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
  );
}
