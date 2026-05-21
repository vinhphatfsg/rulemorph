import {
  TIME_RANGE_OPTIONS,
  type TimeRange,
  type TraceListItem
} from "./trace_list_helpers";

type TraceListFiltersProps = {
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
};

export function TraceListFilters({
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
  traces
}: TraceListFiltersProps) {
  return (
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
  );
}
