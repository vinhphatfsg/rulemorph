use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::Value as JsonValue;

use crate::trace_schema::TraceChunkRef;

use super::super::{TraceCompression, TraceWriteOptions, reserve_budget, write_atomic};

pub(in crate::trace_writer) struct FinalizeWriteResult {
    pub(in crate::trace_writer) chunk: Option<TraceChunkRef>,
    pub(in crate::trace_writer) file: Option<PathBuf>,
    pub(in crate::trace_writer) budget_exceeded: bool,
    pub(in crate::trace_writer) size_exceeded: bool,
}

pub(in crate::trace_writer) fn write_finalize_chunk(
    trace_dir: &Path,
    trace: &JsonValue,
    options: &TraceWriteOptions,
    budget_remaining: &mut u64,
    chunk_budget_remaining: &mut usize,
) -> Result<FinalizeWriteResult> {
    let finalize = match trace.get("finalize") {
        Some(value) => value,
        None => {
            return Ok(FinalizeWriteResult {
                chunk: None,
                file: None,
                budget_exceeded: false,
                size_exceeded: false,
            });
        }
    };

    if *chunk_budget_remaining == 0 {
        return Ok(FinalizeWriteResult {
            chunk: None,
            file: None,
            budget_exceeded: true,
            size_exceeded: false,
        });
    }

    let filename = format!(
        "finalize.json{}",
        match options.compression {
            TraceCompression::Zstd => ".zst",
            TraceCompression::None => "",
        }
    );
    let path = trace_dir.join(&filename);
    let payload = serde_json::to_vec(finalize)?;
    if payload.len() > options.max_chunk_bytes_uncompressed {
        return Ok(FinalizeWriteResult {
            chunk: None,
            file: None,
            budget_exceeded: false,
            size_exceeded: true,
        });
    }
    let raw_bytes = payload.as_slice();
    let (bytes, payload) = match options.compression {
        TraceCompression::Zstd => {
            let compressed = zstd::stream::encode_all(raw_bytes, 3)?;
            (compressed.len() as u64, compressed)
        }
        TraceCompression::None => (raw_bytes.len() as u64, raw_bytes.to_vec()),
    };
    if !reserve_budget(budget_remaining, bytes) {
        return Ok(FinalizeWriteResult {
            chunk: None,
            file: None,
            budget_exceeded: true,
            size_exceeded: false,
        });
    }
    write_atomic(&path, payload.as_slice())?;
    *chunk_budget_remaining = chunk_budget_remaining.saturating_sub(1);
    let chunk = TraceChunkRef {
        path: filename,
        format: "json".to_string(),
        compression: match options.compression {
            TraceCompression::Zstd => "zstd".to_string(),
            TraceCompression::None => "none".to_string(),
        },
        record_start: None,
        record_end: None,
        node_start: None,
        node_end: None,
        bytes: Some(bytes),
        bytes_uncompressed: Some(raw_bytes.len() as u64),
    };
    Ok(FinalizeWriteResult {
        chunk: Some(chunk),
        file: Some(path),
        budget_exceeded: false,
        size_exceeded: false,
    })
}
