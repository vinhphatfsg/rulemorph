export type TraceSummary = {
  record_total?: number;
  record_success?: number;
  record_failed?: number;
  duration_us?: number;
  duration_ms?: number;
};

export type TraceListItem = {
  trace_id: string;
  status?: string;
  timestamp?: string;
  duration_us?: number;
  duration_ms?: number;
  rule?: { name?: string; path?: string; type?: string; version?: number };
  summary?: TraceSummary;
};

export type DurationUnit = "us" | "ms";
export type TimeRange = "all" | "1h" | "24h" | "7d" | "30d";

type DurationSource = {
  duration_us?: number;
  duration_ms?: number;
};

type TraceDurationPayload = {
  summary?: TraceSummary;
  records?: DurationSource[];
};

export function formatTime(value?: string) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

export function formatDurationParts(valueUs: number | undefined, unit: DurationUnit) {
  if (valueUs == null) return null;
  if (unit === "ms") {
    const valueMs = valueUs / 1000;
    const formatted =
      valueMs >= 100 ? valueMs.toFixed(0) : valueMs >= 10 ? valueMs.toFixed(1) : valueMs.toFixed(2);
    return { value: formatted, unit: "ms" as const };
  }
  return { value: `${valueUs}`, unit: "μs" as const };
}

export function formatDuration(valueUs: number | undefined, unit: DurationUnit) {
  const parts = formatDurationParts(valueUs, unit);
  return parts ? `${parts.value} ${parts.unit}` : "-";
}

export function resolveDurationUs(durationUs?: number, durationMs?: number) {
  if (typeof durationUs === "number") return durationUs;
  if (typeof durationMs === "number") return durationMs * 1000;
  return undefined;
}

export function resolveTraceDurationUs(trace?: TraceDurationPayload) {
  if (!trace) return undefined;
  const fromSummary = resolveDurationUs(trace.summary?.duration_us, trace.summary?.duration_ms);
  if (fromSummary !== undefined) return fromSummary;
  const record = trace.records?.[0];
  return resolveDurationUs(record?.duration_us, record?.duration_ms);
}

export const TIME_RANGE_OPTIONS: { value: TimeRange; label: string; ms: number | null }[] = [
  { value: "all", label: "全期間", ms: null },
  { value: "1h", label: "1時間", ms: 60 * 60 * 1000 },
  { value: "24h", label: "24時間", ms: 24 * 60 * 60 * 1000 },
  { value: "7d", label: "7日", ms: 7 * 24 * 60 * 60 * 1000 },
  { value: "30d", label: "30日", ms: 30 * 24 * 60 * 60 * 1000 }
];

export function resolveTraceStatus(item: TraceListItem) {
  return (item.status ?? "ok").toLowerCase();
}

export function resolveRuleLabel(item: TraceListItem) {
  return item.rule?.path ?? item.rule?.name ?? null;
}

export function matchesTraceQuery(item: TraceListItem, query: string) {
  if (!query) return true;
  const lowered = query.trim().toLowerCase();
  if (!lowered) return true;
  const haystack = [
    item.trace_id,
    item.rule?.name,
    item.rule?.path,
    item.status
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return haystack.includes(lowered);
}

export function isWithinTimeRange(item: TraceListItem, range: TimeRange) {
  if (range === "all") return true;
  const threshold = TIME_RANGE_OPTIONS.find((opt) => opt.value === range)?.ms;
  if (!threshold) return true;
  if (!item.timestamp) return false;
  const parsed = Date.parse(item.timestamp);
  if (Number.isNaN(parsed)) return false;
  const now = Date.now();
  const diff = now - parsed;
  if (diff < 0) return true;
  return diff <= threshold;
}

export function applyTraceFilters(
  items: TraceListItem[],
  filters: {
    status: string;
    rule: string;
    query: string;
    range: TimeRange;
  }
) {
  return items.filter((item) => {
    if (filters.status !== "all" && resolveTraceStatus(item) !== filters.status) {
      return false;
    }
    if (filters.rule !== "all") {
      const ruleLabel = resolveRuleLabel(item);
      if (!ruleLabel || ruleLabel !== filters.rule) {
        return false;
      }
    }
    if (!matchesTraceQuery(item, filters.query)) {
      return false;
    }
    if (!isWithinTimeRange(item, filters.range)) {
      return false;
    }
    return true;
  });
}

export function isErrorStatus(status?: string) {
  return status?.toLowerCase() === "error";
}
