use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::Value as JsonValue;

use super::super::chunk_write::{
    max_ndjson_line_bytes, write_finalize_chunk, write_node_chunks, write_record_chunks,
};
use super::super::manifest::count_inline_nodes;
use super::super::options::TraceWriteOptions;
use super::super::record_nodes::{normalize_inline_records, split_records_and_nodes};
use crate::trace_schema::{TRACE_NODE_COUNT_HARD_MAX, TRACE_RECORD_COUNT_HARD_MAX, TraceChunkRef};

pub(super) struct DetailChunkWriteResult {
    pub(super) record_chunks: Vec<TraceChunkRef>,
    pub(super) record_files: Vec<PathBuf>,
    pub(super) node_chunks: Vec<TraceChunkRef>,
    pub(super) node_files: Vec<PathBuf>,
    pub(super) finalize_chunk: Option<TraceChunkRef>,
    pub(super) finalize_file: Option<PathBuf>,
    pub(super) detail_layout: String,
    pub(super) budget_exceeded: bool,
    pub(super) chunk_too_large: bool,
}

pub(super) fn write_full_detail_chunks(
    trace_dir: &Path,
    trace: &JsonValue,
    records: &[JsonValue],
    options: &TraceWriteOptions,
    budget_remaining: &mut u64,
    chunk_budget_remaining: &mut usize,
) -> Result<DetailChunkWriteResult> {
    let mut result = DetailChunkWriteResult {
        record_chunks: Vec::new(),
        record_files: Vec::new(),
        node_chunks: Vec::new(),
        node_files: Vec::new(),
        finalize_chunk: None,
        finalize_file: None,
        detail_layout: "records_inline".to_string(),
        budget_exceeded: false,
        chunk_too_large: false,
    };

    let (records_for_chunks, nodes_for_chunks) = if options.split_nodes {
        result.detail_layout = "records_nodes_split".to_string();
        split_records_and_nodes(records)
    } else {
        (normalize_inline_records(records), Vec::new())
    };

    let total_records = records_for_chunks.len();
    let total_nodes = if options.split_nodes {
        nodes_for_chunks.len()
    } else {
        count_inline_nodes(&records_for_chunks, TRACE_NODE_COUNT_HARD_MAX)
    };
    if total_records > TRACE_RECORD_COUNT_HARD_MAX || total_nodes > TRACE_NODE_COUNT_HARD_MAX {
        result.budget_exceeded = true;
        return Ok(result);
    }

    let max_record_line = max_ndjson_line_bytes(&records_for_chunks, "record")?;
    let max_node_line = max_ndjson_line_bytes(&nodes_for_chunks, "node")?;
    let max_line = max_record_line.max(max_node_line);
    if max_line > options.max_chunk_bytes_uncompressed {
        result.detail_layout = "records_inline".to_string();
        result.chunk_too_large = true;
        return Ok(result);
    }

    let record_result = write_record_chunks(
        trace_dir,
        &records_for_chunks,
        options,
        budget_remaining,
        chunk_budget_remaining,
    )?;
    result.record_chunks = record_result.chunks;
    result.record_files = record_result.files;
    result.budget_exceeded = record_result.budget_exceeded;

    if !result.budget_exceeded && options.split_nodes {
        let node_result = write_node_chunks(
            trace_dir,
            &nodes_for_chunks,
            options,
            budget_remaining,
            chunk_budget_remaining,
        )?;
        result.node_chunks = node_result.chunks;
        result.node_files = node_result.files;
        result.budget_exceeded = node_result.budget_exceeded;
    }

    if !result.budget_exceeded {
        let finalize_result = write_finalize_chunk(
            trace_dir,
            trace,
            options,
            budget_remaining,
            chunk_budget_remaining,
        )?;
        result.finalize_chunk = finalize_result.chunk;
        result.finalize_file = finalize_result.file;
        result.budget_exceeded = finalize_result.budget_exceeded;
        result.chunk_too_large = finalize_result.size_exceeded;
    }

    Ok(result)
}
