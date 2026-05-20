use super::*;

pub(super) fn apply_offset(records: &mut Vec<JsonValue>, offset: usize) {
    if offset > 0 && offset < records.len() {
        *records = records.split_off(offset);
    } else if offset >= records.len() {
        records.clear();
    }
}

pub(super) fn apply_limit(records: &mut Vec<JsonValue>, limit: usize) {
    if limit < records.len() {
        records.truncate(limit);
    }
}

pub(super) fn apply_offset_traced(
    records: &mut Vec<JsonValue>,
    offset: usize,
    collector: &mut TraceCollector,
) {
    apply_offset(records, offset);
    collector
        .emit(TraceEventKind::FinalizeOffset, TracePhase::Instant)
        .rule_path("finalize.offset")
        .attr_index("offset", offset)
        .finish_with_output(collector, &JsonValue::Array(records.clone()), None);
}

pub(super) fn apply_limit_traced(
    records: &mut Vec<JsonValue>,
    limit: usize,
    collector: &mut TraceCollector,
) {
    apply_limit(records, limit);
    collector
        .emit(TraceEventKind::FinalizeLimit, TracePhase::Instant)
        .rule_path("finalize.limit")
        .attr_count("limit", limit)
        .finish_with_output(collector, &JsonValue::Array(records.clone()), None);
}
