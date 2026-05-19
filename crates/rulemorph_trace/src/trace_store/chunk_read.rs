use std::path::Path;

use anyhow::Result;
use serde_json::{Value, json};
use tracing::warn;

use crate::trace_schema::{
    TRACE_CHUNK_COUNT_HARD_MAX, TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX, TraceChunkRef,
};

mod io;

#[cfg(test)]
pub(super) use io::resolve_chunk_path;
use io::{ChunkSizeExceeded, read_chunk_bytes};

pub(super) struct NodeChunkEntry {
    pub(super) record_index: Option<u64>,
    pub(super) record_index_present: bool,
    pub(super) node: Value,
}

pub(super) fn parse_node_chunk_entry(value: Value) -> NodeChunkEntry {
    let record_index_value = value.get("record_index");
    let record_index_present = record_index_value.is_some();
    let record_index = record_index_value.and_then(parse_record_index);
    let mut node = match value {
        Value::Object(mut obj) => {
            let has_node = obj.contains_key("node");
            let has_core_fields =
                obj.contains_key("id") || obj.contains_key("kind") || obj.contains_key("status");
            let legacy_wrapper_shape = has_node
                && !has_core_fields
                && obj
                    .keys()
                    .all(|key| matches!(key.as_str(), "node" | "record_index"));
            if legacy_wrapper_shape {
                obj.remove("node").unwrap_or(Value::Null)
            } else {
                obj.remove("record_index");
                Value::Object(obj)
            }
        }
        other => other,
    };
    if !node.is_object() {
        node = json!({ "value": node });
    }
    NodeChunkEntry {
        record_index,
        record_index_present,
        node,
    }
}

pub(super) fn normalize_inline_nodes_value(nodes_value: &Value) -> Vec<Value> {
    match nodes_value {
        Value::Array(nodes) => nodes.iter().map(normalize_inline_node).collect(),
        Value::Object(_) => vec![normalize_inline_node(nodes_value)],
        other => vec![json!({ "value": other })],
    }
}

fn normalize_inline_node(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(map.clone()),
        other => json!({ "value": other }),
    }
}

pub(super) fn count_inline_nodes(records: &[Value], max_nodes: usize) -> usize {
    let mut total = 0usize;
    for record in records {
        if let Some(nodes) = record.get("nodes").and_then(|value| value.as_array()) {
            total = total.saturating_add(nodes.len());
            if total > max_nodes {
                break;
            }
        }
    }
    total
}

pub(super) fn parse_record_index(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

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

pub(super) fn read_ndjson_chunk(
    base_dir: &Path,
    chunk: &TraceChunkRef,
    max_bytes: usize,
    max_items: usize,
) -> Result<ChunkReadResult<Vec<Value>>> {
    if max_items == 0 {
        warn!(
            "trace chunk exceeds max item count; skipping chunk {}",
            chunk.path
        );
        return Ok(ChunkReadResult {
            value: Vec::new(),
            had_error: true,
            size_exceeded: false,
            limit_exceeded: true,
            bytes: 0,
        });
    }
    if chunk.format != "ndjson" {
        warn!(
            "unsupported chunk format {}; skipping chunk {}",
            chunk.format, chunk.path
        );
        return Ok(ChunkReadResult {
            value: Vec::new(),
            had_error: true,
            size_exceeded: false,
            limit_exceeded: false,
            bytes: 0,
        });
    }
    if !is_supported_compression(&chunk.compression) {
        warn!(
            "unsupported chunk compression {}; skipping chunk {}",
            chunk.compression, chunk.path
        );
        return Ok(ChunkReadResult {
            value: Vec::new(),
            had_error: true,
            size_exceeded: false,
            limit_exceeded: false,
            bytes: 0,
        });
    }
    let chunk_path = base_dir.join(&chunk.path);
    let raw = match read_chunk_bytes(base_dir, chunk, max_bytes) {
        Ok(raw) => raw,
        Err(err) => {
            let size_exceeded = err.downcast_ref::<ChunkSizeExceeded>().is_some();
            warn!(
                "failed to read/decode ndjson chunk {}; skipping chunk: {}",
                chunk.path, err
            );
            return Ok(ChunkReadResult {
                value: Vec::new(),
                had_error: true,
                size_exceeded,
                limit_exceeded: false,
                bytes: 0,
            });
        }
    };
    let raw_len = raw.len();
    let text = match String::from_utf8(raw) {
        Ok(text) => text,
        Err(err) => {
            warn!(
                "failed to decode ndjson chunk as utf-8 {}; skipping chunk {}",
                err, chunk.path
            );
            return Ok(ChunkReadResult {
                value: Vec::new(),
                had_error: true,
                size_exceeded: false,
                limit_exceeded: false,
                bytes: 0,
            });
        }
    };
    let mut values = Vec::new();
    let mut had_error = false;
    let mut limit_exceeded = false;
    for (line_number, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if values.len() >= max_items {
            warn!(
                "ndjson chunk exceeds max item count {}; stopping at {} for {}",
                max_items,
                values.len(),
                chunk_path.display()
            );
            had_error = true;
            limit_exceeded = true;
            break;
        }
        match serde_json::from_str::<Value>(trimmed) {
            Ok(value) => values.push(value),
            Err(err) => {
                warn!(
                    "skipping malformed ndjson line {} in {}: {}",
                    line_number + 1,
                    chunk_path.display(),
                    err
                );
                had_error = true;
            }
        }
    }
    Ok(ChunkReadResult {
        value: values,
        had_error,
        size_exceeded: false,
        limit_exceeded,
        bytes: raw_len,
    })
}

pub(super) fn read_json_chunk(
    base_dir: &Path,
    chunk: &TraceChunkRef,
    max_bytes: usize,
) -> Result<ChunkReadResult<Option<Value>>> {
    if chunk.format != "json" {
        warn!(
            "unsupported chunk format {}; skipping chunk {}",
            chunk.format, chunk.path
        );
        return Ok(ChunkReadResult {
            value: None,
            had_error: true,
            size_exceeded: false,
            limit_exceeded: false,
            bytes: 0,
        });
    }
    if !is_supported_compression(&chunk.compression) {
        warn!(
            "unsupported chunk compression {}; skipping chunk {}",
            chunk.compression, chunk.path
        );
        return Ok(ChunkReadResult {
            value: None,
            had_error: true,
            size_exceeded: false,
            limit_exceeded: false,
            bytes: 0,
        });
    }
    let raw = match read_chunk_bytes(base_dir, chunk, max_bytes) {
        Ok(raw) => raw,
        Err(err) => {
            let size_exceeded = err.downcast_ref::<ChunkSizeExceeded>().is_some();
            warn!(
                "failed to read/decode json chunk {}; skipping chunk: {}",
                chunk.path, err
            );
            return Ok(ChunkReadResult {
                value: None,
                had_error: true,
                size_exceeded,
                limit_exceeded: false,
                bytes: 0,
            });
        }
    };
    let raw_len = raw.len();
    let value = match serde_json::from_slice(&raw) {
        Ok(value) => value,
        Err(err) => {
            warn!(
                "failed to parse json chunk {}; skipping chunk: {}",
                chunk.path, err
            );
            return Ok(ChunkReadResult {
                value: None,
                had_error: true,
                size_exceeded: false,
                limit_exceeded: false,
                bytes: 0,
            });
        }
    };
    Ok(ChunkReadResult {
        value: Some(value),
        had_error: false,
        size_exceeded: false,
        limit_exceeded: false,
        bytes: raw_len,
    })
}

pub(super) fn is_supported_compression(compression: &str) -> bool {
    matches!(compression, "zstd" | "none")
}
