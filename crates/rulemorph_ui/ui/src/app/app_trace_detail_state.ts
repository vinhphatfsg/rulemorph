import type { Dispatch, SetStateAction } from "react";
import { loadSelectedTraceDetail } from "./app_trace_detail_loader";
import type { TraceManifest, TraceNode, TracePayload } from "../api/trace_payload";

type PositionMap = Record<string, { x: number; y: number }>;

type TraceDetailStateArgs = {
  setTrace: Dispatch<SetStateAction<TracePayload | null>>;
  setTraceManifest: Dispatch<SetStateAction<TraceManifest | null>>;
  setDetailLoading: Dispatch<SetStateAction<boolean>>;
  setDetailError: Dispatch<SetStateAction<string | null>>;
  setRecordIndex: Dispatch<SetStateAction<number>>;
  setSelectedNode: Dispatch<SetStateAction<TraceNode | null>>;
  setSelectedOp: Dispatch<SetStateAction<TraceNode | null>>;
  setExpandedRuleIds: Dispatch<SetStateAction<string[]>>;
  setFocusedRuleId: Dispatch<SetStateAction<string | null>>;
  setInspectorOpen: Dispatch<SetStateAction<boolean>>;
  setPinnedPositions: Dispatch<SetStateAction<PositionMap>>;
};

type LoadTraceDetailForSelectionArgs = TraceDetailStateArgs & {
  selectedId: string | null;
};

function resetEmptyTraceDetail({
  setTrace,
  setTraceManifest,
  setDetailLoading,
  setDetailError
}: TraceDetailStateArgs): void {
  setTrace(null);
  setTraceManifest(null);
  setDetailLoading(false);
  setDetailError(null);
}

function resetSelectedTraceDetail(args: TraceDetailStateArgs): void {
  resetEmptyTraceDetail(args);
  args.setRecordIndex(0);
  args.setSelectedNode(null);
  args.setSelectedOp(null);
  args.setExpandedRuleIds([]);
  args.setFocusedRuleId(null);
  args.setInspectorOpen(false);
  args.setPinnedPositions({});
}

export function loadTraceDetailForSelection(
  args: LoadTraceDetailForSelectionArgs
): void | (() => void) {
  const { selectedId } = args;
  if (!selectedId) {
    resetEmptyTraceDetail(args);
    return undefined;
  }

  let mounted = true;
  resetSelectedTraceDetail(args);
  void loadSelectedTraceDetail({
    selectedId,
    isMounted: () => mounted,
    setTrace: args.setTrace,
    setTraceManifest: args.setTraceManifest,
    setDetailLoading: args.setDetailLoading,
    setDetailError: args.setDetailError
  });
  return () => {
    mounted = false;
  };
}
