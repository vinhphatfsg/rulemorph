use std::fmt;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tracing::warn;

use crate::trace_schema::{
    TRACE_CHUNK_BYTES_COMPRESSED_HARD_MAX, TRACE_CHUNK_BYTES_COMPRESSED_OVERHEAD_MAX,
    TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX, TRACE_CHUNK_COUNT_HARD_MAX,
    TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX, TraceChunkRef,
};

const HARD_MAX_CHUNK_BYTES_COMPRESSED: u64 = TRACE_CHUNK_BYTES_COMPRESSED_HARD_MAX as u64;
// Zstd frames produced with default settings expect at least an 8MB window.
const ZSTD_WINDOW_BYTES_MIN: usize = 8 * 1024 * 1024;

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

fn is_supported_compression(compression: &str) -> bool {
    matches!(compression, "zstd" | "none")
}

#[derive(Debug)]
struct ChunkSizeExceeded {
    actual: u64,
    max: u64,
}

impl fmt::Display for ChunkSizeExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "trace chunk exceeds max bytes: {} > {}",
            self.actual, self.max
        )
    }
}

impl std::error::Error for ChunkSizeExceeded {}

pub(super) fn resolve_chunk_path(base_dir: &Path, chunk_path: &str) -> Result<PathBuf> {
    let rel = Path::new(chunk_path);
    if rel.as_os_str().is_empty() {
        return Err(anyhow::anyhow!("trace chunk path is empty"));
    }
    if rel.is_absolute()
        || rel.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(anyhow::anyhow!(
            "trace chunk path must be relative without parent components: {}",
            chunk_path
        ));
    }

    let base_dir = base_dir.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize trace base dir: {}",
            base_dir.display()
        )
    })?;
    let path = base_dir.join(rel);
    let resolved = path.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize trace chunk path: {}",
            path.display()
        )
    })?;
    if !resolved.starts_with(&base_dir) {
        return Err(anyhow::anyhow!(
            "trace chunk path escapes base dir: {}",
            chunk_path
        ));
    }
    Ok(resolved)
}

fn read_chunk_bytes(base_dir: &Path, chunk: &TraceChunkRef, max_bytes: usize) -> Result<Vec<u8>> {
    let path = resolve_chunk_path(base_dir, &chunk.path)?;
    let compressed_bytes = std::fs::metadata(&path)
        .with_context(|| format!("failed to read trace chunk metadata: {}", path.display()))?
        .len();
    let max_compressed_bytes = std::cmp::min(
        HARD_MAX_CHUNK_BYTES_COMPRESSED,
        (max_bytes as u64).saturating_add(TRACE_CHUNK_BYTES_COMPRESSED_OVERHEAD_MAX as u64),
    );
    if compressed_bytes > max_compressed_bytes {
        return Err(anyhow::Error::new(ChunkSizeExceeded {
            actual: compressed_bytes,
            max: max_compressed_bytes,
        }));
    }
    let raw = std::fs::read(&path)
        .with_context(|| format!("failed to read trace chunk: {}", path.display()))?;
    match chunk.compression.as_str() {
        "zstd" => decode_zstd_limited(&raw, max_bytes),
        "none" => {
            if raw.len() > max_bytes {
                return Err(anyhow::Error::new(ChunkSizeExceeded {
                    actual: raw.len() as u64,
                    max: max_bytes as u64,
                }));
            }
            Ok(raw)
        }
        other => Err(anyhow::anyhow!("unsupported compression: {}", other)),
    }
}

fn decode_zstd_limited(raw: &[u8], max_bytes: usize) -> Result<Vec<u8>> {
    let mut decoder = zstd::stream::read::Decoder::new(raw)?;
    decoder.window_log_max(zstd_window_log_max(max_bytes))?;
    let mut limited = decoder.take((max_bytes as u64).saturating_add(1));
    let mut output = Vec::new();
    limited.read_to_end(&mut output)?;
    if output.len() > max_bytes {
        return Err(anyhow::Error::new(ChunkSizeExceeded {
            actual: output.len() as u64,
            max: max_bytes as u64,
        }));
    }
    Ok(output)
}

fn zstd_window_log_max(max_bytes: usize) -> u32 {
    let max_bytes = max_bytes
        .max(ZSTD_WINDOW_BYTES_MIN)
        .min(TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX)
        .max(1) as u64;
    let pow2 = max_bytes.next_power_of_two();
    let log = 63u32.saturating_sub(pow2.leading_zeros());
    log.clamp(20, 31)
}
