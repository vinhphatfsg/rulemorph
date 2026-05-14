use std::path::PathBuf;

use anyhow::Result;
use serde_json::Value;
use tracing::warn;

use super::TraceNodeChunkEntry;
use super::chunk_read::{parse_node_chunk_entry, read_json_chunk, read_ndjson_chunk};
use super::manifest_budget::resolve_max_chunk_bytes;
use crate::trace_schema::{TRACE_NODE_COUNT_HARD_MAX, TRACE_RECORD_COUNT_HARD_MAX, TraceManifest};

pub(super) async fn get_records_chunk(
    manifest: TraceManifest,
    base_dir: PathBuf,
    chunk_index: usize,
) -> Result<Option<Vec<Value>>> {
    let detail = match manifest.detail.as_ref() {
        Some(detail) => detail,
        None => return Ok(None),
    };
    if detail.status != "full" {
        return Ok(None);
    }
    let chunk = match detail.records.get(chunk_index) {
        Some(chunk) => chunk.clone(),
        None => return Ok(None),
    };
    let max_chunk_bytes = resolve_max_chunk_bytes(&manifest);
    let chunk_path = chunk.path.clone();
    let result = tokio::task::spawn_blocking(move || {
        read_ndjson_chunk(
            &base_dir,
            &chunk,
            max_chunk_bytes,
            TRACE_RECORD_COUNT_HARD_MAX,
        )
    })
    .await
    .map_err(|err| anyhow::anyhow!("trace record chunk task failed: {}", err))??;
    if result.had_error || result.size_exceeded || result.limit_exceeded {
        return Err(anyhow::anyhow!(
            "trace record chunk failed to load: {}",
            chunk_path
        ));
    }
    Ok(Some(result.value))
}

pub(super) async fn get_nodes_chunk(
    manifest: TraceManifest,
    base_dir: PathBuf,
    chunk_index: usize,
) -> Result<Option<Vec<TraceNodeChunkEntry>>> {
    let detail = match manifest.detail.as_ref() {
        Some(detail) => detail,
        None => return Ok(None),
    };
    if detail.status != "full" {
        return Ok(None);
    }
    if detail.layout != "records_nodes_split" {
        warn!(
            "node chunks present but layout is {}; skipping nodes chunk",
            detail.layout
        );
        return Ok(None);
    }
    let chunk = match detail.nodes.get(chunk_index) {
        Some(chunk) => chunk.clone(),
        None => return Ok(None),
    };
    let max_chunk_bytes = resolve_max_chunk_bytes(&manifest);
    let chunk_path = chunk.path.clone();
    let result = tokio::task::spawn_blocking(move || {
        read_ndjson_chunk(
            &base_dir,
            &chunk,
            max_chunk_bytes,
            TRACE_NODE_COUNT_HARD_MAX,
        )
    })
    .await
    .map_err(|err| anyhow::anyhow!("trace node chunk task failed: {}", err))??;
    if result.had_error || result.size_exceeded || result.limit_exceeded {
        return Err(anyhow::anyhow!(
            "trace node chunk failed to load: {}",
            chunk_path
        ));
    }
    let mut entries = Vec::new();
    let mut last_record_index: Option<u64> = None;
    for value in result.value {
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
        entries.push(TraceNodeChunkEntry {
            record_index,
            node: entry.node,
        });
    }
    Ok(Some(entries))
}

pub(super) async fn get_finalize_chunk(
    manifest: TraceManifest,
    base_dir: PathBuf,
) -> Result<Option<Value>> {
    let detail = match manifest.detail.as_ref() {
        Some(detail) => detail,
        None => return Ok(None),
    };
    if detail.status != "full" {
        return Ok(None);
    }
    let chunk = match detail.finalize.as_ref() {
        Some(chunk) => chunk.clone(),
        None => return Ok(None),
    };
    let max_chunk_bytes = resolve_max_chunk_bytes(&manifest);
    let chunk_path = chunk.path.clone();
    let result =
        tokio::task::spawn_blocking(move || read_json_chunk(&base_dir, &chunk, max_chunk_bytes))
            .await
            .map_err(|err| anyhow::anyhow!("trace finalize chunk task failed: {}", err))??;
    if result.had_error || result.size_exceeded || result.limit_exceeded {
        return Err(anyhow::anyhow!(
            "trace finalize chunk failed to load: {}",
            chunk_path
        ));
    }
    Ok(result.value)
}
