import { useState } from "react";
import type { ViewMode } from "./view_mode";

export function useAppModeState() {
  const [viewMode, setViewMode] = useState<ViewMode>("trace");

  return {
    viewMode,
    setViewMode
  };
}
