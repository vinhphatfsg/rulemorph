import { type ReactNode } from "react";

export function formatEdgeDurationMs(valueUs: number | undefined) {
  if (valueUs == null) return "-";
  const valueMs = valueUs / 1000;
  const formatted = valueMs >= 100 ? valueMs.toFixed(0) : valueMs >= 10 ? valueMs.toFixed(1) : valueMs.toFixed(2);
  return `${formatted}ms`;
}

export function buildEdgeLabel(label: string, duration: string): ReactNode {
  return (
    <>
      <tspan x="0" dy="0">
        {label}
      </tspan>
      <tspan x="0" dy="1.2em">
        {duration}
      </tspan>
    </>
  );
}
