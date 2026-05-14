use std::collections::{HashMap, HashSet};

use serde_json::{Value, json};

use crate::path_expr::{append_path, leaf_from_path};

#[derive(Default)]
pub(crate) struct PathStats {
    count: usize,
    type_counts: HashMap<&'static str, usize>,
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

#[derive(Clone)]
pub(crate) struct InputPathInfo {
    pub(crate) path: String,
    leaf: String,
    tokens: Vec<String>,
    type_counts: HashMap<&'static str, usize>,
}

#[derive(Clone)]
pub(crate) struct Candidate {
    pub(crate) source: String,
    pub(crate) score: f64,
    pub(crate) reason: &'static str,
    pub(crate) confidence: &'static str,
}

pub(crate) fn build_input_paths(stats: &HashMap<String, PathStats>) -> Vec<InputPathInfo> {
    let mut paths = Vec::new();
    for (path, stat) in stats {
        if path == "$" {
            continue;
        }
        let leaf = leaf_from_path(path).unwrap_or_else(|| path.clone());
        let tokens = split_tokens(&leaf);
        paths.push(InputPathInfo {
            path: path.clone(),
            leaf,
            tokens,
            type_counts: stat.type_counts.clone(),
        });
    }
    paths
}

fn split_tokens(value: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            current.push(ch.to_ascii_lowercase());
        } else if !current.is_empty() {
            tokens.push(current.clone());
            current.clear();
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn token_similarity(a: &[String], b: &[String]) -> f64 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let set_a: HashSet<&str> = a.iter().map(String::as_str).collect();
    let set_b: HashSet<&str> = b.iter().map(String::as_str).collect();
    let overlap = set_a.intersection(&set_b).count() as f64;
    let denom = set_a.len().max(set_b.len()) as f64;
    if denom == 0.0 { 0.0 } else { overlap / denom }
}

pub(crate) fn select_candidates(
    target_leaf: &str,
    source_hint: Option<&str>,
    value_type: Option<&str>,
    input_paths: &[InputPathInfo],
    max_candidates: usize,
) -> Vec<Candidate> {
    let mut candidates = Vec::new();
    let target_tokens = split_tokens(target_leaf);
    let source_leaf = source_hint.and_then(leaf_from_path);
    let source_tokens = source_leaf.as_deref().map(split_tokens).unwrap_or_default();

    for input in input_paths {
        let mut score = 0.0;
        let mut reason = None;

        if let Some(source_hint) = source_hint {
            if input.path == source_hint {
                score = 1.0;
                reason = Some("exact_source");
            }
        }

        if reason.is_none() && !target_leaf.is_empty() {
            if input.leaf.eq_ignore_ascii_case(target_leaf) {
                score = 0.8;
                reason = Some("leaf_match");
            }
        }

        if reason.is_none() {
            if let Some(source_leaf) = source_leaf.as_deref() {
                if input.leaf.eq_ignore_ascii_case(source_leaf) {
                    score = 0.75;
                    reason = Some("leaf_match");
                }
            }
        }

        if reason.is_none() {
            let mut similarity = token_similarity(&target_tokens, &input.tokens);
            if !source_tokens.is_empty() {
                similarity = similarity.max(token_similarity(&source_tokens, &input.tokens));
            }
            if similarity > 0.0 {
                score = 0.6 * similarity;
                reason = Some("token_match");
            }
        }

        if let Some(reason) = reason {
            score += type_boost(&input.type_counts, value_type);
            let confidence = confidence_for_score(score);
            candidates.push(Candidate {
                source: input.path.clone(),
                score,
                reason,
                confidence,
            });
        }
    }

    candidates.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.source.cmp(&b.source))
    });
    candidates.truncate(max_candidates);
    candidates
}

fn type_boost(type_counts: &HashMap<&'static str, usize>, value_type: Option<&str>) -> f64 {
    let Some(value_type) = value_type else {
        return 0.0;
    };
    let type_name = match value_type {
        "string" => "string",
        "int" | "float" => "number",
        "bool" => "bool",
        _ => return 0.0,
    };
    if type_counts.contains_key(type_name) {
        0.1
    } else {
        0.0
    }
}

fn confidence_for_score(score: f64) -> &'static str {
    if score >= 0.9 {
        "high"
    } else if score >= 0.7 {
        "medium"
    } else {
        "low"
    }
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
