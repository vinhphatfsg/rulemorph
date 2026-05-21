use anyhow::{Context, Result};
use serde_json::Value as JsonValue;

use crate::trace_schema::TraceChunkRef;

mod finalize;
mod indexed;
pub(super) use finalize::write_finalize_chunk;
pub(super) use indexed::{write_node_chunks, write_record_chunks};

pub(super) fn max_ndjson_line_bytes(items: &[JsonValue], label: &str) -> Result<usize> {
    let mut max_len = 0usize;
    for (index, item) in items.iter().enumerate() {
        let payload = serde_json::to_vec(item)
            .with_context(|| format!("failed to serialize {label} at {index}"))?;
        let line_len = payload.len().saturating_add(1);
        if line_len > max_len {
            max_len = line_len;
        }
    }
    Ok(max_len)
}

pub(super) fn total_chunk_bytes(chunks: &[TraceChunkRef]) -> u64 {
    chunks.iter().filter_map(|chunk| chunk.bytes).sum()
}
