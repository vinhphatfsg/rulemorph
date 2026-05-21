import clsx from "clsx";
import {
  TIME_RANGE_OPTIONS,
  formatDuration,
  formatTime,
  isErrorStatus,
  resolveDurationUs,
  type DurationUnit,
  type TimeRange,
  type TraceListItem
} from "./trace_list_helpers";

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

type ZipImportModalProps = {
  tenantId: string | null;
  zipMessage: string | null;
  zipUploading: boolean;
  zipFile: File | null;
  setZipFile: (file: File | null) => void;
  setZipMessage: (value: string | null) => void;
  setZipModalOpen: (open: boolean) => void;
  handleZipImport: () => void;
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
          <div className="trace-panel__filters">
            <input
              className="trace-filter__search"
              type="search"
              aria-label="Trace検索"
              placeholder="trace / rule を検索"
              value={traceFilterQuery}
              onChange={(event) => setTraceFilterQuery(event.target.value)}
            />
            <select
              className="trace-filter__select"
              aria-label="ステータス"
              value={traceFilterStatus}
              onChange={(event) => setTraceFilterStatus(event.target.value)}
            >
              <option value="all">status: all</option>
              {statusOptions.map((status) => (
                <option key={status} value={status}>
                  {status}
                </option>
              ))}
            </select>
            <select
              className="trace-filter__select"
              aria-label="ルール"
              value={traceFilterRule}
              onChange={(event) => setTraceFilterRule(event.target.value)}
            >
              <option value="all">rule: all</option>
              {ruleOptions.map((rule) => (
                <option key={rule} value={rule}>
                  {rule}
                </option>
              ))}
            </select>
            <select
              className="trace-filter__select"
              aria-label="期間"
              value={traceFilterRange}
              onChange={(event) => setTraceFilterRange(event.target.value as TimeRange)}
            >
              {TIME_RANGE_OPTIONS.map((range) => (
                <option key={range.value} value={range.value}>
                  {range.label}
                </option>
              ))}
            </select>
            <button
              className="trace-filter__reset"
              type="button"
              onClick={() => {
                setTraceFilterStatus("all");
                setTraceFilterRule("all");
                setTraceFilterQuery("");
                setTraceFilterRange("all");
              }}
            >
              リセット
            </button>
            <span className="trace-filter__count">
              {filteredTraces.length} / {traces.length}
            </span>
          </div>
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
                <p className="muted">
                  data_dir（既定: ./.rulemorph）の traces/ に JSON を配置してください。
                </p>
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
                      {formatDuration(
                        resolveDurationUs(item.duration_us, item.duration_ms),
                        durationUnit
                      )}
                    </strong>
                  </div>
                </div>
              </button>
            ))}
          </div>
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

export function ZipImportModal({
  tenantId,
  zipMessage,
  zipUploading,
  zipFile,
  setZipFile,
  setZipMessage,
  setZipModalOpen,
  handleZipImport
}: ZipImportModalProps) {
  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal">
        <div className="modal__header">
          <div>
            <h3>ZIPインポート</h3>
            <p className="muted">traces/ と rules/ を含むZIPをアップロードしてください。</p>
          </div>
          <button
            className="icon-button"
            onClick={() => {
              setZipModalOpen(false);
              setZipFile(null);
            }}
          >
            ×
          </button>
        </div>
        <div className="modal__body">
          {tenantId && <p className="muted">tenant: {tenantId}</p>}
          <input
            className="modal__file"
            data-testid="zip-import-file"
            type="file"
            accept=".zip"
            onChange={(event) => {
              const file = event.currentTarget.files?.[0] ?? null;
              setZipFile(file);
              setZipMessage(null);
            }}
          />
          {zipMessage && (
            <div className="modal__message" data-testid="zip-import-message">
              {zipMessage}
            </div>
          )}
        </div>
        <div className="modal__actions">
          <button
            className="modal__button"
            type="button"
            onClick={() => {
              setZipModalOpen(false);
              setZipFile(null);
            }}
          >
            閉じる
          </button>
          <button
            className="modal__button modal__button--primary"
            data-testid="zip-import-submit"
            type="button"
            disabled={zipUploading || !zipFile}
            onClick={handleZipImport}
          >
            {zipUploading ? "アップロード中..." : "インポート"}
          </button>
        </div>
      </div>
    </div>
  );
}
