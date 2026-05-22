use std::path::Path;

use anyhow::Result;
use serde_json::Value;
use tracing::warn;

use crate::trace_schema::TraceChunkRef;

use super::io::{ChunkSizeExceeded, read_chunk_bytes};
use super::{ChunkReadResult, is_supported_compression};

pub(in crate::trace_store) fn read_ndjson_chunk(
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
