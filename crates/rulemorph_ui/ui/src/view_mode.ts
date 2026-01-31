export type ViewMode = "trace" | "api";

export function shouldResetInitialCenter(prev: ViewMode | null, next: ViewMode): boolean {
  return prev === "api" && next === "trace";
}
