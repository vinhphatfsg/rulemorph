import clsx from "clsx";
import {
  formatDuration,
  isErrorStatus,
  resolveDurationUs,
  type DurationUnit
} from "./trace_list_helpers";
import { type TraceNode, type TracePayload } from "./trace_payload";

type FinalizePayload = NonNullable<TracePayload["finalize"]>;

type RecordPanelProps = {
  currentTrace: TracePayload | null;
  recordIndex: number;
  setRecordIndex: (index: number) => void;
  setSelectedNode: (node: TraceNode | null) => void;
  setSelectedOp: (node: TraceNode | null) => void;
  setInspectorOpen: (open: boolean) => void;
  finalizePayload: FinalizePayload | null;
  isFinalizeSelected: boolean;
  durationUnit: DurationUnit;
};

export function RecordPanel({
  currentTrace,
  recordIndex,
  setRecordIndex,
  setSelectedNode,
  setSelectedOp,
  setInspectorOpen,
  finalizePayload,
  isFinalizeSelected,
  durationUnit
}: RecordPanelProps) {
  return (
    <aside className="floating-panel record-panel">
      <div className="panel__header">
        <h2>Records</h2>
        <p>{currentTrace?.records?.length ?? 0} total</p>
      </div>
      <div className="record-list">
        {(currentTrace?.records ?? []).map((record, idx) => (
          <button
            key={record.index}
            className={clsx("record-card", recordIndex === idx && "is-active")}
            onClick={() => {
              setRecordIndex(idx);
              setSelectedNode(null);
              setSelectedOp(null);
              setInspectorOpen(false);
            }}
          >
            <span>#{record.index}</span>
            <span
              className={clsx(
                "record-status",
                isErrorStatus(record.status) && "record-status--error"
              )}
            >
              {record.status ?? "ok"}
            </span>
            <span>
              {formatDuration(resolveDurationUs(record.duration_us, record.duration_ms), durationUnit)}
            </span>
          </button>
        ))}
        {finalizePayload && (
          <button
            key="finalize"
            data-testid="record-finalize"
            className={clsx("record-card record-card--finalize", isFinalizeSelected && "is-active")}
            onClick={() => {
              setRecordIndex(-1);
              setSelectedNode(null);
              setSelectedOp(null);
              setInspectorOpen(true);
            }}
          >
            <span>Finalize</span>
            <span
              className={clsx(
                "record-status",
                isErrorStatus(finalizePayload.status) && "record-status--error"
              )}
            >
              {finalizePayload.status ?? "ok"}
            </span>
            <span>
              {formatDuration(
                resolveDurationUs(finalizePayload.duration_us, finalizePayload.duration_ms),
                durationUnit
              )}
            </span>
          </button>
        )}
      </div>
    </aside>
  );
}
