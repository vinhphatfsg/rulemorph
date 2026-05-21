import { type RuleRefEntry } from "./trace_graph_types";

export function extractRuleRefs(meta?: Record<string, unknown>): RuleRefEntry[] {
  if (!meta) return [];
  const entries: RuleRefEntry[] = [];
  const push = (ref: unknown, label?: unknown) => {
    if (typeof ref !== "string" || ref.length === 0) return;
    entries.push({
      ref,
      label: typeof label === "string" ? label : undefined
    });
  };
  push(meta["rule_ref"], meta["rule_ref_label"]);
  const refs = Array.isArray(meta["rule_refs"]) ? meta["rule_refs"] : [];
  const labels = Array.isArray(meta["rule_ref_labels"]) ? meta["rule_ref_labels"] : [];
  refs.forEach((ref, index) => push(ref, labels[index]));
  const deduped: RuleRefEntry[] = [];
  const seen = new Set<string>();
  entries.forEach((entry) => {
    const key = `${entry.ref}::${entry.label ?? ""}`;
    if (seen.has(key)) return;
    seen.add(key);
    deduped.push(entry);
  });
  return deduped;
}
