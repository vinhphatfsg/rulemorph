import { useState } from "react";

export function useInspectorState() {
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [traceInspectorSections, setTraceInspectorSections] = useState(() => ({
    finalize: true,
    step: true,
    opList: false,
    opResult: false
  }));
  const [apiInspectorSections, setApiInspectorSections] = useState(() => ({
    opList: false,
    memo: false
  }));

  return {
    inspectorOpen,
    setInspectorOpen,
    traceInspectorSections,
    setTraceInspectorSections,
    apiInspectorSections,
    setApiInspectorSections
  };
}
