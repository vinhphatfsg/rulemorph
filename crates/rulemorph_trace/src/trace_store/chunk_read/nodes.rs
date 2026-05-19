use serde_json::{Value, json};

pub(in crate::trace_store) struct NodeChunkEntry {
    pub(in crate::trace_store) record_index: Option<u64>,
    pub(in crate::trace_store) record_index_present: bool,
    pub(in crate::trace_store) node: Value,
}

pub(in crate::trace_store) fn parse_node_chunk_entry(value: Value) -> NodeChunkEntry {
    let record_index_value = value.get("record_index");
    let record_index_present = record_index_value.is_some();
    let record_index = record_index_value.and_then(parse_record_index);
    let mut node = match value {
        Value::Object(mut obj) => {
            let has_node = obj.contains_key("node");
            let has_core_fields =
                obj.contains_key("id") || obj.contains_key("kind") || obj.contains_key("status");
            let legacy_wrapper_shape = has_node
                && !has_core_fields
                && obj
                    .keys()
                    .all(|key| matches!(key.as_str(), "node" | "record_index"));
            if legacy_wrapper_shape {
                obj.remove("node").unwrap_or(Value::Null)
            } else {
                obj.remove("record_index");
                Value::Object(obj)
            }
        }
        other => other,
    };
    if !node.is_object() {
        node = json!({ "value": node });
    }
    NodeChunkEntry {
        record_index,
        record_index_present,
        node,
    }
}

pub(in crate::trace_store) fn normalize_inline_nodes_value(nodes_value: &Value) -> Vec<Value> {
    match nodes_value {
        Value::Array(nodes) => nodes.iter().map(normalize_inline_node).collect(),
        Value::Object(_) => vec![normalize_inline_node(nodes_value)],
        other => vec![json!({ "value": other })],
    }
}

fn normalize_inline_node(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(map.clone()),
        other => json!({ "value": other }),
    }
}

pub(in crate::trace_store) fn count_inline_nodes(records: &[Value], max_nodes: usize) -> usize {
    let mut total = 0usize;
    for record in records {
        if let Some(nodes) = record.get("nodes").and_then(|value| value.as_array()) {
            total = total.saturating_add(nodes.len());
            if total > max_nodes {
                break;
            }
        }
    }
    total
}

pub(in crate::trace_store) fn parse_record_index(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}
