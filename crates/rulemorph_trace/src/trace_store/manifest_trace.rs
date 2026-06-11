use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::Value;
use tracing::warn;

use super::chunk_read::{
    ChunkBudget, count_inline_nodes, normalize_inline_nodes_value, read_json_chunk,
    read_ndjson_chunk,
};
use super::manifest_budget::resolve_max_chunk_bytes;
use crate::trace_schema::{TRACE_NODE_COUNT_HARD_MAX, TRACE_RECORD_COUNT_HARD_MAX, TraceManifest};

mod detail_state;
mod nodes;
use detail_state::{attach_empty_records, downgrade_chunk_error_detail, strip_non_full_detail};
use nodes::attach_node_chunks;

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
            attach_empty_records(&mut trace);
            return Ok(trace);
        }
    };

    if detail.status != "full" {
        strip_non_full_detail(&mut trace);
        return Ok(trace);
    }

    let detail_status = detail.status.clone();
    let detail_reason = detail.reason.clone();
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
            if let Some(obj) = record.as_object_mut()
                && let Some(nodes_value) = obj.get("nodes").cloned()
            {
                let normalized = normalize_inline_nodes_value(&nodes_value);
                obj.insert("nodes".to_string(), Value::Array(normalized));
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
        let outcome = attach_node_chunks(
            &mut records,
            &detail.nodes,
            base_dir,
            max_chunk_bytes,
            &mut budget,
            &mut remaining_nodes,
        )?;
        if outcome.chunk_error {
            chunk_error = true;
        }
        if outcome.size_exceeded {
            size_exceeded = true;
        }
        if outcome.budget_exceeded {
            budget_exceeded = true;
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
            downgrade_chunk_error_detail(
                &mut trace,
                detail_status,
                detail_reason,
                size_exceeded,
                budget_exceeded,
            );
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
