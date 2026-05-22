import type { Dispatch, SetStateAction } from "react";
import clsx from "clsx";

type ViewMode = "trace" | "api";

type TopbarProps = {
  viewMode: ViewMode;
  setViewMode: Dispatch<SetStateAction<ViewMode>>;
  traceTitleId: string;
  apiTitleId: string;
  detailLabel: string;
  filteredTraceCount: number;
  traceCount: number;
  recordLabel: string;
  apiRuleCount: number;
  apiEdgeCount: number;
};

export function Topbar({
  viewMode,
  setViewMode,
  traceTitleId,
  apiTitleId,
  detailLabel,
  filteredTraceCount,
  traceCount,
  recordLabel,
  apiRuleCount,
  apiEdgeCount
}: TopbarProps) {
  return (
    <header className="topbar">
      <div className="title-chip">
        <span className="title-chip__dot" />
        <span className="title-chip__label">
          {viewMode === "trace" ? "Rulemorph Trace" : "Rulemorph 構成図"}
        </span>
        <span className="title-chip__id">
          {viewMode === "trace" ? traceTitleId : apiTitleId}
        </span>
      </div>
      <div className="topbar__meta">
        <div className="meta-tabs">
          <button
            className={clsx("meta-tab", viewMode === "trace" && "is-active")}
            onClick={() => setViewMode("trace")}
          >
            Trace
          </button>
          <button
            className={clsx("meta-tab", viewMode === "api" && "is-active")}
            onClick={() => setViewMode("api")}
          >
            構成図
          </button>
        </div>
        {viewMode === "trace" ? (
          <>
            <span className="meta-pill">{detailLabel}</span>
            <span className="meta-pill">
              {filteredTraceCount} / {traceCount} traces
            </span>
            <span className="meta-pill">{recordLabel}</span>
          </>
        ) : (
          <>
            <span className="meta-pill">{apiRuleCount} rules</span>
            <span className="meta-pill">{apiEdgeCount} edges</span>
          </>
        )}
      </div>
    </header>
  );
}
