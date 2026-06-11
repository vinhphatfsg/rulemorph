import clsx from "clsx";
import type { DurationUnit, TimeRange, TraceListItem } from "./trace_list_helpers";
import { TraceListFilters } from "./trace_list_filters";
import { TraceListItems } from "./trace_list_items";
export { ZipImportModal } from "../import/zip_import_modal";

type TraceListPanelProps = {
  traceListOpen: boolean;
  setTraceListOpen: (open: boolean) => void;
  internalKey: string | null;
  durationUnit: DurationUnit;
  setDurationUnit: (unit: DurationUnit) => void;
  traceFilterQuery: string;
  setTraceFilterQuery: (value: string) => void;
  traceFilterStatus: string;
  setTraceFilterStatus: (value: string) => void;
  traceFilterRule: string;
  setTraceFilterRule: (value: string) => void;
  traceFilterRange: TimeRange;
  setTraceFilterRange: (value: TimeRange) => void;
  statusOptions: string[];
  ruleOptions: string[];
  filteredTraces: TraceListItem[];
  traces: TraceListItem[];
  selectedId: string | null;
  setSelectedId: (traceId: string) => void;
  detailStatus?: string;
  detailError: string | null;
  detailReason: string[];
  setZipMessage: (value: string | null) => void;
  setZipModalOpen: (open: boolean) => void;
};

export function TraceListPanel({
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
  setZipModalOpen
}: TraceListPanelProps) {
  return (
    <aside className={clsx("floating-panel trace-panel", !traceListOpen && "is-collapsed")}>
      {traceListOpen ? (
        <>
          <div className="panel__header trace-panel__header">
            <div>
              <h2>Trace一覧</h2>
              <p>最新順</p>
            </div>
            <div className="trace-panel__actions">
              <button
                className="trace-panel__import"
                data-testid="zip-import-button"
                title={internalKey ? "ZIPインポート" : "internal_key 未設定でも試行できます"}
                onClick={() => {
                  setZipMessage(null);
                  setZipModalOpen(true);
                }}
              >
                ZIPインポート
              </button>
              <div className="unit-toggle" role="group" aria-label="Duration unit">
                <button
                  className={clsx("unit-toggle__button", durationUnit === "us" && "is-active")}
                  onClick={() => setDurationUnit("us")}
                >
                  μs
                </button>
                <button
                  className={clsx("unit-toggle__button", durationUnit === "ms" && "is-active")}
                  onClick={() => setDurationUnit("ms")}
                >
                  ms
                </button>
              </div>
              <button
                className="icon-button trace-panel__toggle"
                aria-expanded={traceListOpen}
                onClick={() => setTraceListOpen(false)}
              >
                ×
              </button>
            </div>
          </div>
          <TraceListFilters
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
          />
          <TraceListItems
            durationUnit={durationUnit}
            filteredTraces={filteredTraces}
            traces={traces}
            selectedId={selectedId}
            setSelectedId={setSelectedId}
            detailStatus={detailStatus}
            detailError={detailError}
            detailReason={detailReason}
          />
        </>
      ) : (
        <button className="trace-panel__chip" onClick={() => setTraceListOpen(true)}>
          <span>Trace一覧</span>
          <span className="trace-panel__chip-count">{filteredTraces.length}</span>
        </button>
      )}
    </aside>
  );
}
