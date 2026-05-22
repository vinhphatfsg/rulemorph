use std::path::Path;

use anyhow::Result;
use serde_json::Value;
use tracing::warn;

use crate::trace_schema::TraceChunkRef;

use super::io::{ChunkSizeExceeded, read_chunk_bytes};
use super::{ChunkReadResult, is_supported_compression};

pub(in crate::trace_store) fn read_json_chunk(
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
