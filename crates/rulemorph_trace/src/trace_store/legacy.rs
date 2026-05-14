use serde_json::{Value, json};

use crate::trace_schema::{TRACE_NODE_COUNT_HARD_MAX, TRACE_RECORD_COUNT_HARD_MAX};

pub(super) fn looks_like_legacy_trace(value: &Value) -> bool {
    value.get("trace_id").is_some() || value.get("records").is_some() || value.get("rule").is_some()
}

pub(super) fn apply_legacy_limits(legacy: &mut Value) {
    apply_legacy_limits_with_thresholds(
        legacy,
        TRACE_RECORD_COUNT_HARD_MAX,
        TRACE_NODE_COUNT_HARD_MAX,
    );
}

pub(super) fn apply_legacy_limits_with_thresholds(
    legacy: &mut Value,
    record_limit: usize,
    node_limit: usize,
) {
    let (record_count, node_count) = legacy_counts(legacy);
    if record_count <= record_limit && node_count <= node_limit {
        return;
    }
    if let Some(obj) = legacy.as_object_mut() {
        obj.insert("records".to_string(), Value::Array(Vec::new()));
        obj.remove("finalize");
        obj.remove("nodes");
        obj.insert(
            "detail".to_string(),
            json!({
                "layout": "records_inline",
                "status": "basic",
                "reason": ["budget_exceeded"],
                "records": [],
                "nodes": []
            }),
        );
    }
}

fn legacy_counts(legacy: &Value) -> (usize, usize) {
    let mut record_count = 0usize;
    let mut node_count = 0usize;
    if let Some(records) = legacy.get("records").and_then(|value| value.as_array()) {
        record_count = records.len();
        for record in records {
            if let Some(nodes) = record.get("nodes").and_then(|value| value.as_array()) {
                node_count = node_count.saturating_add(nodes.len());
            }
        }
    }
    if let Some(nodes) = legacy.get("nodes").and_then(|value| value.as_array()) {
        node_count = node_count.saturating_add(nodes.len());
    }
    (record_count, node_count)
}
