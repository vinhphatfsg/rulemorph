use std::path::PathBuf;

use crate::trace_schema::TraceChunkRef;

use super::super::chunk_write::total_chunk_bytes;
use super::super::cleanup::cleanup_detail_files;
use super::super::manifest::blob_total_bytes;

pub(super) fn reset_detail_to_basic(
    record_files: &mut Vec<PathBuf>,
    node_files: &mut Vec<PathBuf>,
    finalize_file: &mut Option<PathBuf>,
    blob_files: &mut Vec<PathBuf>,
    record_chunks: &mut Vec<TraceChunkRef>,
    node_chunks: &mut Vec<TraceChunkRef>,
    finalize_chunk: &mut Option<TraceChunkRef>,
    detail_status: &mut String,
    detail_layout: &mut String,
) {
    cleanup_detail_files(record_files, node_files, finalize_file, blob_files);
    record_chunks.clear();
    node_chunks.clear();
    *finalize_chunk = None;
    *detail_status = "basic".to_string();
    *detail_layout = "records_inline".to_string();
}

pub(super) fn add_detail_reason(detail_reason: &mut Vec<String>, reason: &str) {
    if !detail_reason.iter().any(|existing| existing == reason) {
        detail_reason.push(reason.to_string());
    }
}

pub(super) fn total_detail_bytes(
    record_chunks: &[TraceChunkRef],
    node_chunks: &[TraceChunkRef],
    finalize_chunk: &Option<TraceChunkRef>,
    blob_files: &[PathBuf],
) -> u64 {
    total_chunk_bytes(record_chunks)
        .saturating_add(total_chunk_bytes(node_chunks))
        .saturating_add(
            finalize_chunk
                .as_ref()
                .and_then(|chunk| chunk.bytes)
                .unwrap_or(0),
        )
        .saturating_add(blob_total_bytes(blob_files))
}
