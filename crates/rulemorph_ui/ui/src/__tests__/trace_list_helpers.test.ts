import { describe, expect, it, vi } from "vitest";

import {
  applyTraceFilters,
  formatDuration,
  formatDurationParts,
  isErrorStatus,
  isWithinTimeRange,
  matchesTraceQuery,
  resolveDurationUs,
  resolveRuleLabel,
  resolveTraceDurationUs,
  resolveTraceStatus,
  type TraceListItem
} from "../trace_list_helpers";

const traces: TraceListItem[] = [
  {
    trace_id: "trace-a",
    status: "ok",
    timestamp: "2026-05-17T10:30:00.000Z",
    rule: { name: "Alpha", path: "rules/alpha.json" },
    summary: { duration_ms: 2 }
  },
  {
    trace_id: "trace-b",
    status: "ERROR",
    timestamp: "2026-05-16T08:00:00.000Z",
    rule: { name: "Beta", path: "rules/beta.json" },
    duration_us: 500
  },
  {
    trace_id: "trace-c",
    timestamp: "not-a-date",
    rule: { name: "Gamma" }
  }
];

describe("trace list helpers", () => {
  it("resolves display status and rule label defaults", () => {
    expect(resolveTraceStatus(traces[1])).toBe("error");
    expect(resolveTraceStatus(traces[2])).toBe("ok");
    expect(resolveRuleLabel(traces[0])).toBe("rules/alpha.json");
    expect(resolveRuleLabel(traces[2])).toBe("Gamma");
  });

  it("matches trace queries against id, rule, path, and status", () => {
    expect(matchesTraceQuery(traces[0], "alpha")).toBe(true);
    expect(matchesTraceQuery(traces[1], "rules/beta")).toBe(true);
    expect(matchesTraceQuery(traces[1], "error")).toBe(true);
    expect(matchesTraceQuery(traces[2], "trace-c")).toBe(true);
    expect(matchesTraceQuery(traces[2], "missing")).toBe(false);
  });

  it("filters by status, rule, query, and time range", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-05-17T11:00:00.000Z"));

    try {
      expect(
        applyTraceFilters(traces, {
          status: "ok",
          rule: "all",
          query: "",
          range: "all"
        }).map((trace) => trace.trace_id)
      ).toEqual(["trace-a", "trace-c"]);

      expect(
        applyTraceFilters(traces, {
          status: "all",
          rule: "rules/beta.json",
          query: "beta",
          range: "30d"
        }).map((trace) => trace.trace_id)
      ).toEqual(["trace-b"]);

      expect(
        applyTraceFilters(traces, {
          status: "all",
          rule: "all",
          query: "",
          range: "1h"
        }).map((trace) => trace.trace_id)
      ).toEqual(["trace-a"]);

      expect(isWithinTimeRange(traces[0], "1h")).toBe(true);
      expect(isWithinTimeRange(traces[1], "1h")).toBe(false);
      expect(isWithinTimeRange(traces[2], "1h")).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });

  it("formats and resolves durations without changing unit behavior", () => {
    expect(resolveDurationUs(undefined, 2.5)).toBe(2500);
    expect(resolveTraceDurationUs({ summary: { duration_ms: 2 } })).toBe(2000);
    expect(resolveTraceDurationUs({ records: [{ duration_us: 7 }] })).toBe(7);
    expect(formatDurationParts(1234, "us")).toEqual({ value: "1234", unit: "μs" });
    expect(formatDurationParts(1234, "ms")).toEqual({ value: "1.23", unit: "ms" });
    expect(formatDuration(undefined, "ms")).toBe("-");
  });

  it("detects error status case-insensitively", () => {
    expect(isErrorStatus("ERROR")).toBe(true);
    expect(isErrorStatus("error")).toBe(true);
    expect(isErrorStatus("ok")).toBe(false);
    expect(isErrorStatus()).toBe(false);
  });
});
