use std::path::PathBuf;

use crate::trace_schema::TraceChunkRef;

use super::super::chunk_write::total_chunk_bytes;
use super::super::cleanup::cleanup_detail_files;
use super::super::manifest::blob_total_bytes;

pub(super) struct DetailReset<'a> {
    pub(super) record_files: &'a mut Vec<PathBuf>,
    pub(super) node_files: &'a mut Vec<PathBuf>,
    pub(super) finalize_file: &'a mut Option<PathBuf>,
    pub(super) blob_files: &'a mut Vec<PathBuf>,
    pub(super) record_chunks: &'a mut Vec<TraceChunkRef>,
    pub(super) node_chunks: &'a mut Vec<TraceChunkRef>,
    pub(super) finalize_chunk: &'a mut Option<TraceChunkRef>,
    pub(super) detail_status: &'a mut String,
    pub(super) detail_layout: &'a mut String,
}

pub(super) fn reset_detail_to_basic(reset: DetailReset<'_>) {
    cleanup_detail_files(
        reset.record_files,
        reset.node_files,
        reset.finalize_file,
        reset.blob_files,
    );
    reset.record_chunks.clear();
    reset.node_chunks.clear();
    *reset.finalize_chunk = None;
    *reset.detail_status = "basic".to_string();
    *reset.detail_layout = "records_inline".to_string();
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
