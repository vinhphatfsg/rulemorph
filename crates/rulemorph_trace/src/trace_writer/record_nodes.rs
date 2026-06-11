use std::collections::HashSet;

use serde_json::{Value as JsonValue, json};
use tracing::warn;

pub(super) fn normalize_inline_records(records: &[JsonValue]) -> Vec<JsonValue> {
    records
        .iter()
        .map(|record| {
            let mut record_clone = record.clone();
            if let Some(obj) = record_clone.as_object_mut()
                && let Some(nodes_value) = obj.get("nodes").cloned()
            {
                let normalized = normalize_nodes_value(&nodes_value);
                obj.insert("nodes".to_string(), JsonValue::Array(normalized));
            }
            record_clone
        })
        .collect()
}

fn normalize_nodes_value(nodes_value: &JsonValue) -> Vec<JsonValue> {
    match nodes_value {
        JsonValue::Array(nodes) => nodes.iter().map(normalize_node_value).collect(),
        JsonValue::Object(_) => vec![normalize_node_value(nodes_value)],
        other => vec![json!({ "value": other })],
    }
}

fn normalize_node_value(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(map) => JsonValue::Object(map.clone()),
        other => json!({ "value": other }),
    }
}

fn parse_record_index(value: &JsonValue) -> Option<u64> {
    match value {
        JsonValue::Number(num) => num.as_u64(),
        JsonValue::String(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

fn build_node_chunk_entry(node: &JsonValue, record_index: u64) -> JsonValue {
    match node {
        JsonValue::Object(map) if map.contains_key("record_index") => json!({
            "record_index": record_index,
            "node": JsonValue::Object(map.clone()),
        }),
        JsonValue::Object(map) => {
            let mut entry = JsonValue::Object(map.clone());
            if let Some(obj) = entry.as_object_mut() {
                obj.insert("record_index".to_string(), JsonValue::from(record_index));
            }
            entry
        }
        other => json!({
            "record_index": record_index,
            "value": other,
        }),
    }
}

pub(super) fn split_records_and_nodes(records: &[JsonValue]) -> (Vec<JsonValue>, Vec<JsonValue>) {
    let mut records_out = Vec::with_capacity(records.len());
    let mut nodes_out = Vec::new();
    let mut seen_indices: HashSet<u64> = HashSet::new();

    for (index, record) in records.iter().enumerate() {
        let mut record_index = record
            .get("index")
            .and_then(parse_record_index)
            .unwrap_or(index as u64);
        if seen_indices.contains(&record_index) {
            record_index = index as u64;
        }
        if seen_indices.contains(&record_index) {
            let start = record_index;
            loop {
                record_index = record_index.wrapping_add(1);
                if !seen_indices.contains(&record_index) {
                    break;
                }
                if record_index == start {
                    warn!(
                        "record_index space exhausted while deduplicating; using {}",
                        record_index
                    );
                    break;
                }
            }
        }
        seen_indices.insert(record_index);
        if let Some(nodes_value) = record.get("nodes") {
            match nodes_value {
                JsonValue::Array(nodes) => {
                    for node in nodes {
                        nodes_out.push(build_node_chunk_entry(node, record_index));
                    }
                }
                other => {
                    nodes_out.push(build_node_chunk_entry(other, record_index));
                }
            }
        }

        let mut record_clone = record.clone();
        if let Some(obj) = record_clone.as_object_mut() {
            obj.insert("index".to_string(), JsonValue::from(record_index));
            if obj.contains_key("nodes") {
                obj.remove("nodes");
            }
        }
        records_out.push(record_clone);
    }

    (records_out, nodes_out)
}
