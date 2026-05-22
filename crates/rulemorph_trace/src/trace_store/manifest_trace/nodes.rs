use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use serde_json::Value;
use tracing::warn;

use crate::trace_schema::TraceChunkRef;

use super::super::chunk_read::{
    ChunkBudget, parse_node_chunk_entry, parse_record_index, read_ndjson_chunk,
};

pub(super) struct NodeChunkAttachOutcome {
    pub(super) chunk_error: bool,
    pub(super) budget_exceeded: bool,
    pub(super) size_exceeded: bool,
}

pub(super) fn attach_node_chunks(
    records: &mut [Value],
    chunks: &[TraceChunkRef],
    base_dir: &Path,
    max_chunk_bytes: usize,
    budget: &mut ChunkBudget,
    remaining_nodes: &mut usize,
) -> Result<NodeChunkAttachOutcome> {
    let mut nodes_by_record: HashMap<u64, Vec<Value>> = HashMap::new();
    let mut chunk_error = false;
    let mut budget_exceeded = false;
    let mut size_exceeded = false;

    for chunk in chunks {
        let mut last_record_index: Option<u64> = None;
        if !budget.consume_chunk() {
            budget_exceeded = true;
            chunk_error = true;
            break;
        }
        let lines = read_ndjson_chunk(base_dir, chunk, max_chunk_bytes, *remaining_nodes)?;
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
            *remaining_nodes = remaining_nodes.saturating_sub(lines.value.len());
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

    attach_nodes_by_record(records, &nodes_by_record);

    Ok(NodeChunkAttachOutcome {
        chunk_error,
        budget_exceeded,
        size_exceeded,
    })
}

fn attach_nodes_by_record(records: &mut [Value], nodes_by_record: &HashMap<u64, Vec<Value>>) {
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
