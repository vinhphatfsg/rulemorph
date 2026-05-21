use crate::trace_schema::{
    TRACE_CHUNK_COUNT_HARD_MAX, TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX,
};

mod io;
mod json;
mod ndjson;
mod nodes;

#[cfg(test)]
pub(super) use io::resolve_chunk_path;
pub(super) use json::read_json_chunk;
pub(super) use ndjson::read_ndjson_chunk;
pub(super) use nodes::{
    count_inline_nodes, normalize_inline_nodes_value, parse_node_chunk_entry, parse_record_index,
};

pub(super) struct ChunkReadResult<T> {
    pub(super) value: T,
    pub(super) had_error: bool,
    pub(super) size_exceeded: bool,
    pub(super) limit_exceeded: bool,
    pub(super) bytes: usize,
}

pub(super) struct ChunkBudget {
    pub(super) remaining_bytes: usize,
    pub(super) remaining_chunks: usize,
}

impl ChunkBudget {
    pub(super) fn new() -> Self {
        Self {
            remaining_bytes: TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX,
            remaining_chunks: TRACE_CHUNK_COUNT_HARD_MAX,
        }
    }

    pub(super) fn consume_chunk(&mut self) -> bool {
        if self.remaining_chunks == 0 {
            return false;
        }
        self.remaining_chunks = self.remaining_chunks.saturating_sub(1);
        true
    }

    pub(super) fn consume_bytes(&mut self, bytes: usize) -> bool {
        if bytes > self.remaining_bytes {
            return false;
        }
        self.remaining_bytes = self.remaining_bytes.saturating_sub(bytes);
        true
    }
}

pub(super) fn is_supported_compression(compression: &str) -> bool {
    matches!(compression, "zstd" | "none")
}
