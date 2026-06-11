import { useCallback, useEffect, useMemo, useState } from "react";
import {
  applyTraceFilters,
  type TimeRange,
  type TraceListItem
} from "../trace_list/trace_list_helpers";
import { deriveRuleOptions, deriveStatusOptions } from "./app_derived_state";
import {
  loadTraceList,
  reconcileFilteredTraceSelection,
  subscribeTraceListRefresh
} from "./app_trace_list_refresh";

type TraceListStateArgs = {
  internalKey: string | null;
};

export function useTraceListState({ internalKey }: TraceListStateArgs) {
  const [traces, setTraces] = useState<TraceListItem[]>([]);
  const [traceFilterStatus, setTraceFilterStatus] = useState("all");
  const [traceFilterRule, setTraceFilterRule] = useState("all");
  const [traceFilterQuery, setTraceFilterQuery] = useState("");
  const [traceFilterRange, setTraceFilterRange] = useState<TimeRange>("all");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [traceListOpen, setTraceListOpen] = useState(true);

  const statusOptions = useMemo(() => deriveStatusOptions(traces), [traces]);

  const ruleOptions = useMemo(() => deriveRuleOptions(traces), [traces]);

  const filteredTraces = useMemo(
    () =>
      applyTraceFilters(traces, {
        status: traceFilterStatus,
        rule: traceFilterRule,
        query: traceFilterQuery,
        range: traceFilterRange
      }),
    [traces, traceFilterStatus, traceFilterRule, traceFilterQuery, traceFilterRange]
  );

  const loadTraces = useCallback(async (preserveSelection: boolean) => {
    await loadTraceList({
      preserveSelection,
      setTraces,
      setSelectedId
    });
  }, []);

  useEffect(() => {
    loadTraces(false);
  }, [loadTraces]);

  useEffect(() => {
    reconcileFilteredTraceSelection({
      selectedId,
      filteredTraces,
      setSelectedId
    });
  }, [filteredTraces, selectedId]);

  useEffect(() => {
    return subscribeTraceListRefresh({
      internalKey,
      loadTraces
    });
  }, [internalKey, loadTraces]);

  return {
    traces,
    traceFilterStatus,
    setTraceFilterStatus,
    traceFilterRule,
    setTraceFilterRule,
    traceFilterQuery,
    setTraceFilterQuery,
    traceFilterRange,
    setTraceFilterRange,
    selectedId,
    setSelectedId,
    traceListOpen,
    setTraceListOpen,
    statusOptions,
    ruleOptions,
    filteredTraces,
    loadTraces
  };
}
