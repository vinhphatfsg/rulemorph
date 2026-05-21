use std::collections::HashMap;

use serde_json::{Value, json};

use crate::path_expr::append_path;

#[derive(Default)]
pub(crate) struct PathStats {
    count: usize,
    pub(crate) type_counts: HashMap<&'static str, usize>,
    examples: Vec<Value>,
}

pub(crate) fn analyze_records(
    records: &[Value],
    max_paths: Option<usize>,
) -> HashMap<String, PathStats> {
    let mut stats = HashMap::new();
    for record in records {
        collect_path_stats(record, "", &mut stats, max_paths);
    }
    stats
}

fn collect_path_stats(
    value: &Value,
    prefix: &str,
    stats: &mut HashMap<String, PathStats>,
    max_paths: Option<usize>,
) {
    match value {
        Value::Object(map) => {
            if map.is_empty() {
                record_path_value(stats, prefix, value, max_paths);
                return;
            }
            for (key, child) in map {
                let next = append_path(prefix, key);
                collect_path_stats(child, &next, stats, max_paths);
            }
        }
        Value::Array(_) => {
            record_path_value(stats, prefix, value, max_paths);
        }
        _ => record_path_value(stats, prefix, value, max_paths),
    }
}

fn record_path_value(
    stats: &mut HashMap<String, PathStats>,
    path: &str,
    value: &Value,
    max_paths: Option<usize>,
) {
    let path = if path.is_empty() {
        "$".to_string()
    } else {
        path.to_string()
    };
    if !stats.contains_key(&path) && max_paths.is_some_and(|max| stats.len() >= max) {
        return;
    }
    let entry = stats.entry(path).or_default();
    entry.count += 1;
    let type_name = value_type_name(value);
    *entry.type_counts.entry(type_name).or_insert(0) += 1;
    if entry.examples.len() < 3 && is_primitive(value) && !entry.examples.contains(value) {
        entry.examples.push(value.clone());
    }
}

pub(crate) fn stats_to_json(stats: &HashMap<String, PathStats>) -> Value {
    let mut entries: Vec<(&String, &PathStats)> = stats.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));

    let mut values = Vec::new();
    for (path, stat) in entries {
        let mut types = serde_json::Map::new();
        let mut type_entries: Vec<_> = stat.type_counts.iter().collect();
        type_entries.sort_by(|a, b| a.0.cmp(b.0));
        for (type_name, count) in type_entries {
            types.insert(type_name.to_string(), json!(count));
        }

        let mut obj = json!({
            "path": path,
            "count": stat.count,
            "types": types
        });
        if !stat.examples.is_empty() {
            obj["examples"] = Value::Array(stat.examples.clone());
        }
        values.push(obj);
    }
    Value::Array(values)
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn is_primitive(value: &Value) -> bool {
    matches!(
        value,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
    )
}
