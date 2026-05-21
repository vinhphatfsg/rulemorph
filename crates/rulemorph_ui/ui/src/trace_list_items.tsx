import clsx from "clsx";
import {
  formatDuration,
  formatTime,
  isErrorStatus,
  resolveDurationUs,
  type DurationUnit,
  type TraceListItem
} from "./trace_list_helpers";

type TraceListItemsProps = {
  durationUnit: DurationUnit;
  filteredTraces: TraceListItem[];
  traces: TraceListItem[];
  selectedId: string | null;
  setSelectedId: (traceId: string) => void;
  detailStatus?: string;
  detailError: string | null;
  detailReason: string[];
};

export function TraceListItems({
  durationUnit,
  filteredTraces,
  traces,
  selectedId,
  setSelectedId,
  detailStatus,
  detailError,
  detailReason
}: TraceListItemsProps) {
  return (
    <div className="trace-list">
      {(detailStatus && detailStatus !== "full") || detailError ? (
        <div className="trace-panel__note">
          {detailError ? (
            <>
              <p>detail の読み込みに失敗しました。</p>
              <p className="muted">manifest または chunk の取得に失敗しています。</p>
            </>
          ) : (
            <>
              <p>detail は {detailStatus} です。</p>
              {detailReason.length > 0 && (
                <p className="muted">reason: {detailReason.join(", ")}</p>
              )}
            </>
          )}
        </div>
      ) : null}
      {traces.length === 0 && (
        <div className="empty-trace">
          <p>traces が見つかりません。</p>
          <p className="muted">data_dir（既定: ./.rulemorph）の traces/ に JSON を配置してください。</p>
        </div>
      )}
      {traces.length > 0 && filteredTraces.length === 0 && (
        <div className="empty-trace">
          <p>フィルタ条件に一致するトレースがありません。</p>
          <p className="muted">検索語や期間を調整してください。</p>
        </div>
      )}
      {filteredTraces.map((item) => (
        <button
          key={item.trace_id}
          className={clsx("trace-card", selectedId === item.trace_id && "is-active")}
          onClick={() => setSelectedId(item.trace_id)}
        >
          <div>
            <span className={clsx("chip", isErrorStatus(item.status) && "chip--error")}>
              {item.status ?? "ok"}
            </span>
            <h3>{item.rule?.name ?? item.trace_id}</h3>
            <p className="muted">{item.rule?.path ?? "(no path)"}</p>
          </div>
          <div className="trace-meta">
            <div>
              <span>time: </span>
              <strong>{formatTime(item.timestamp)}</strong>
            </div>
            <div>
              <span>duration: </span>
              <strong>
                {formatDuration(resolveDurationUs(item.duration_us, item.duration_ms), durationUnit)}
              </strong>
            </div>
          </div>
        </button>
      ))}
    </div>
  );
}
