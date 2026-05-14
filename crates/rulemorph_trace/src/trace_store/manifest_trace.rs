use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::Value;
use tracing::warn;

use super::chunk_read::{
    ChunkBudget, count_inline_nodes, normalize_inline_nodes_value, parse_node_chunk_entry,
    parse_record_index, read_json_chunk, read_ndjson_chunk,
};
use super::manifest_budget::resolve_max_chunk_bytes;
use crate::trace_schema::{TRACE_NODE_COUNT_HARD_MAX, TRACE_RECORD_COUNT_HARD_MAX, TraceManifest};

fn build_trace_from_manifest(manifest: &TraceManifest, base_dir: &Path) -> Result<Value> {
    build_trace_from_manifest_with_budget(manifest, base_dir, ChunkBudget::new())
}

pub(super) fn build_trace_from_manifest_with_budget(
    manifest: &TraceManifest,
    base_dir: &Path,
    mut budget: ChunkBudget,
) -> Result<Value> {
    let mut trace = serde_json::to_value(manifest)?;
    let mut records = Vec::new();
    let max_chunk_bytes = resolve_max_chunk_bytes(manifest);

    let detail = match &manifest.detail {
        Some(detail) => detail,
        None => {
            if let Some(obj) = trace.as_object_mut() {
                obj.insert("records".to_string(), Value::Array(Vec::new()));
            }
            return Ok(trace);
        }
    };

    if detail.status != "full" {
        if let Some(obj) = trace.as_object_mut() {
            obj.insert("records".to_string(), Value::Array(Vec::new()));
            obj.remove("finalize");
            if let Some(detail_obj) = obj
                .get_mut("detail")
                .and_then(|value| value.as_object_mut())
            {
                detail_obj.insert("records".to_string(), Value::Array(Vec::new()));
                detail_obj.insert("nodes".to_string(), Value::Array(Vec::new()));
                detail_obj.remove("finalize");
            }
        }
        return Ok(trace);
    }

    let mut detail_status = detail.status.clone();
    let mut detail_reason = detail.reason.clone();
    let mut chunk_error = false;
    let mut budget_exceeded = false;
    let mut size_exceeded = false;
    let mut remaining_records = TRACE_RECORD_COUNT_HARD_MAX;
    let mut remaining_nodes = TRACE_NODE_COUNT_HARD_MAX;

    for chunk in &detail.records {
        if !budget.consume_chunk() {
            budget_exceeded = true;
            chunk_error = true;
            break;
        }
        let lines = read_ndjson_chunk(base_dir, chunk, max_chunk_bytes, remaining_records)?;
        if lines.had_error {
            chunk_error = true;
        }
        if lines.size_exceeded {
            size_exceeded = true;
            chunk_error = true;
            break;
        }
        if lines.limit_exceeded {
            budget_exceeded = true;
            chunk_error = true;
            break;
        }
        if !budget.consume_bytes(lines.bytes) {
            budget_exceeded = true;
            chunk_error = true;
        }
        if !budget_exceeded {
            remaining_records = remaining_records.saturating_sub(lines.value.len());
            records.extend(lines.value);
        }
        if budget_exceeded {
            break;
        }
    }

    if !chunk_error {
        for record in &mut records {
            if let Some(obj) = record.as_object_mut() {
                if let Some(nodes_value) = obj.get("nodes").cloned() {
                    let normalized = normalize_inline_nodes_value(&nodes_value);
                    obj.insert("nodes".to_string(), Value::Array(normalized));
                }
            }
        }
    }

    let inline_nodes = if !chunk_error {
        count_inline_nodes(&records, TRACE_NODE_COUNT_HARD_MAX)
    } else {
        0
    };
    if inline_nodes > TRACE_NODE_COUNT_HARD_MAX {
        budget_exceeded = true;
        chunk_error = true;
    }
    if detail.layout == "records_nodes_split" && !chunk_error {
        remaining_nodes = remaining_nodes.saturating_sub(inline_nodes);
    }

    if !detail.nodes.is_empty() && detail.layout != "records_nodes_split" {
        warn!(
            "node chunks present but layout is {}; skipping nodes chunk",
            detail.layout
        );
    }
    if !detail.nodes.is_empty() && detail.layout == "records_nodes_split" && !chunk_error {
        let mut nodes_by_record: HashMap<u64, Vec<Value>> = HashMap::new();
        for chunk in &detail.nodes {
            let mut last_record_index: Option<u64> = None;
            if !budget.consume_chunk() {
                budget_exceeded = true;
                chunk_error = true;
                break;
            }
            let lines = read_ndjson_chunk(base_dir, chunk, max_chunk_bytes, remaining_nodes)?;
            if lines.had_error {
                chunk_error = true;
            }
            if lines.size_exceeded {
                size_exceeded = true;
                chunk_error = true;
                break;
            }
            if lines.limit_exceeded {
                budget_exceeded = true;
                chunk_error = true;
                break;
            }
            if !budget.consume_bytes(lines.bytes) {
                budget_exceeded = true;
                chunk_error = true;
            }
            if !budget_exceeded {
                remaining_nodes = remaining_nodes.saturating_sub(lines.value.len());
            }
            for value in lines.value {
                let entry = parse_node_chunk_entry(value);
                if entry.record_index.is_none() && entry.record_index_present {
                    warn!("node chunk entry has invalid record_index; skipping");
                    continue;
                }
                let record_index = match entry.record_index.or(last_record_index) {
                    Some(index) => index,
                    None => {
                        warn!("node chunk entry missing record_index; skipping");
                        continue;
                    }
                };
                if entry.record_index.is_some() {
                    last_record_index = entry.record_index;
                }
                nodes_by_record
                    .entry(record_index)
                    .or_default()
                    .push(entry.node);
            }
            if budget_exceeded {
                break;
            }
        }
        let mut seen_record_indices: HashMap<u64, usize> = HashMap::new();
        let mut used_record_indices: HashMap<u64, usize> = HashMap::new();
        for (position, record) in records.iter_mut().enumerate() {
            let index_value = record.get("index");
            let parsed_index = index_value.and_then(parse_record_index);
            if parsed_index.is_none() && index_value.is_some() {
                warn!(
                    "invalid record_index in trace record; skipping node attach at position {}",
                    position
                );
                continue;
            }
            let record_index = parsed_index.unwrap_or(position as u64);
            if let Some(prev) = seen_record_indices.insert(record_index, position) {
                warn!(
                    "duplicate record_index in trace records: {} (at {} and {})",
                    record_index, prev, position
                );
            }
            let nodes_from_chunk = nodes_by_record.get(&record_index).cloned();
            if let Some(nodes) = nodes_from_chunk {
                if used_record_indices.contains_key(&record_index) {
                    warn!(
                        "node chunk entries already attached for record_index {}; skipping duplicate record",
                        record_index
                    );
                } else {
                    let mut attached = false;
                    if let Some(obj) = record.as_object_mut() {
                        match obj.get_mut("nodes") {
                            Some(existing) => {
                                if let Some(existing_nodes) = existing.as_array_mut() {
                                    if !nodes.is_empty() {
                                        warn!(
                                            "record has inline nodes and node chunk entries; merged record_index={}",
                                            record_index
                                        );
                                        existing_nodes.extend(nodes.clone());
                                        attached = true;
                                    }
                                } else {
                                    let previous =
                                        std::mem::replace(existing, Value::Array(Vec::new()));
                                    let mut combined = Vec::new();
                                    combined.push(previous);
                                    combined.extend(nodes.clone());
                                    *existing = Value::Array(combined);
                                    warn!(
                                        "record has non-array inline nodes and node chunk entries; merged record_index={}",
                                        record_index
                                    );
                                    attached = true;
                                }
                            }
                            None => {
                                obj.insert("nodes".to_string(), Value::Array(nodes.clone()));
                                attached = true;
                            }
                        }
                    } else {
                        warn!(
                            "record is non-object; skipping node attach for record_index={}",
                            record_index
                        );
                    }
                    if attached {
                        *used_record_indices.entry(record_index).or_insert(0) += 1;
                    }
                }
            }
        }
        if !nodes_by_record.is_empty() {
            let orphan_count: usize = nodes_by_record
                .iter()
                .filter(|(key, _)| !used_record_indices.contains_key(key))
                .map(|(_, nodes)| nodes.len())
                .sum();
            if orphan_count > 0 {
                warn!(
                    "node chunk entries not attached to records: {}",
                    orphan_count
                );
            }
        }
    }

    let finalize = if !chunk_error {
        match &detail.finalize {
            Some(chunk) => {
                if !budget.consume_chunk() {
                    budget_exceeded = true;
                    chunk_error = true;
                    None
                } else {
                    let result = read_json_chunk(base_dir, chunk, max_chunk_bytes)?;
                    if result.had_error {
                        chunk_error = true;
                    }
                    if result.size_exceeded {
                        size_exceeded = true;
                        chunk_error = true;
                        None
                    } else if !budget.consume_bytes(result.bytes) {
                        budget_exceeded = true;
                        chunk_error = true;
                        None
                    } else {
                        result.value
                    }
                }
            }
            None => None,
        }
    } else {
        None
    };

    if let Some(obj) = trace.as_object_mut() {
        obj.insert("records".to_string(), Value::Array(records));
        if let Some(finalize_value) = finalize {
            obj.insert("finalize".to_string(), finalize_value);
        }
        if chunk_error {
            if detail_status == "full" {
                detail_status = "basic".to_string();
            }
            if size_exceeded
                && !detail_reason
                    .iter()
                    .any(|reason| reason == "chunk_too_large")
            {
                detail_reason.push("chunk_too_large".to_string());
            }
            if budget_exceeded
                && !detail_reason
                    .iter()
                    .any(|reason| reason == "budget_exceeded")
            {
                detail_reason.push("budget_exceeded".to_string());
            }
            if !detail_reason.iter().any(|reason| reason == "chunk_error") {
                detail_reason.push("chunk_error".to_string());
            }
            obj.insert("records".to_string(), Value::Array(Vec::new()));
            obj.remove("finalize");
            if let Some(detail_obj) = obj
                .get_mut("detail")
                .and_then(|value| value.as_object_mut())
            {
                detail_obj.insert("status".to_string(), Value::String(detail_status));
                detail_obj.insert(
                    "reason".to_string(),
                    Value::Array(detail_reason.into_iter().map(Value::String).collect()),
                );
                detail_obj.insert("records".to_string(), Value::Array(Vec::new()));
                detail_obj.insert("nodes".to_string(), Value::Array(Vec::new()));
                detail_obj.remove("finalize");
            }
        }
    }

    Ok(trace)
}

pub(super) async fn build_trace_from_manifest_async(
    manifest: TraceManifest,
    base_dir: PathBuf,
) -> Result<Value> {
    tokio::task::spawn_blocking(move || build_trace_from_manifest(&manifest, &base_dir))
        .await
        .map_err(|err| anyhow::anyhow!("trace load task failed: {}", err))?
}
