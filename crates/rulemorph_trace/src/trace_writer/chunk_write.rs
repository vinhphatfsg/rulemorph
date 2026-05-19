use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde_json::Value as JsonValue;

use crate::trace_schema::TraceChunkRef;

use super::{TraceCompression, TraceWriteOptions, reserve_budget, write_atomic};

mod finalize;
pub(super) use finalize::write_finalize_chunk;

pub(super) struct ChunkWriteResult {
    pub(super) chunks: Vec<TraceChunkRef>,
    pub(super) files: Vec<PathBuf>,
    pub(super) budget_exceeded: bool,
}

pub(super) fn write_record_chunks(
    trace_dir: &Path,
    records: &[JsonValue],
    options: &TraceWriteOptions,
    budget_remaining: &mut u64,
    chunk_budget_remaining: &mut usize,
) -> Result<ChunkWriteResult> {
    let mut chunks = Vec::new();
    let mut files = Vec::new();
    let mut budget_exceeded = false;

    let mut chunk_index = 0usize;
    let mut record_start = 0usize;
    let mut current_lines: Vec<String> = Vec::new();
    let mut current_bytes: usize = 0;

    let mut flush = |lines: &mut Vec<String>, start: usize, end: usize| -> Result<bool> {
        if lines.is_empty() {
            return Ok(false);
        }
        chunk_index += 1;
        let filename = format!(
            "records-{chunk_index:04}.ndjson{}",
            match options.compression {
                TraceCompression::Zstd => ".zst",
                TraceCompression::None => "",
            }
        );
        let path = trace_dir.join(&filename);
        if *chunk_budget_remaining == 0 {
            return Ok(true);
        }
        let payload = format!("{}\n", lines.join("\n"));
        let raw_bytes = payload.as_bytes();

        let (bytes, payload) = match options.compression {
            TraceCompression::Zstd => {
                let compressed = zstd::stream::encode_all(raw_bytes, 3)?;
                (compressed.len() as u64, compressed)
            }
            TraceCompression::None => (raw_bytes.len() as u64, raw_bytes.to_vec()),
        };
        if !reserve_budget(budget_remaining, bytes) {
            return Ok(true);
        }
        write_atomic(&path, payload.as_slice())?;
        *chunk_budget_remaining = chunk_budget_remaining.saturating_sub(1);
        chunks.push(TraceChunkRef {
            path: filename,
            format: "ndjson".to_string(),
            compression: match options.compression {
                TraceCompression::Zstd => "zstd".to_string(),
                TraceCompression::None => "none".to_string(),
            },
            record_start: Some(start as u64),
            record_end: Some(end as u64),
            node_start: None,
            node_end: None,
            bytes: Some(bytes),
            bytes_uncompressed: Some(raw_bytes.len() as u64),
        });
        files.push(path);
        lines.clear();
        Ok(false)
    };

    for (index, record) in records.iter().enumerate() {
        let line = serde_json::to_string(record)
            .with_context(|| format!("failed to serialize record at {index}"))?;
        let line_len = line.as_bytes().len() + 1; // newline
        let exceeds_record_limit = lines_len_exceeds(&current_lines, options.max_records_per_chunk);
        let exceeds_byte_limit = current_bytes + line_len > options.max_chunk_bytes_uncompressed;
        if !current_lines.is_empty() && (exceeds_record_limit || exceeds_byte_limit) {
            let end = record_start + current_lines.len() - 1;
            if flush(&mut current_lines, record_start, end)? {
                budget_exceeded = true;
                break;
            }
            record_start = index;
            current_bytes = 0;
        }
        current_bytes += line_len;
        current_lines.push(line);
    }

    if !budget_exceeded && !current_lines.is_empty() {
        let end = record_start + current_lines.len() - 1;
        if flush(&mut current_lines, record_start, end)? {
            budget_exceeded = true;
        }
    }

    Ok(ChunkWriteResult {
        chunks,
        files,
        budget_exceeded,
    })
}

pub(super) fn write_node_chunks(
    trace_dir: &Path,
    nodes: &[JsonValue],
    options: &TraceWriteOptions,
    budget_remaining: &mut u64,
    chunk_budget_remaining: &mut usize,
) -> Result<ChunkWriteResult> {
    if nodes.is_empty() {
        return Ok(ChunkWriteResult {
            chunks: Vec::new(),
            files: Vec::new(),
            budget_exceeded: false,
        });
    }

    let mut chunks = Vec::new();
    let mut files = Vec::new();
    let mut budget_exceeded = false;

    let mut chunk_index = 0usize;
    let mut node_start = 0usize;
    let mut current_lines: Vec<String> = Vec::new();
    let mut current_bytes: usize = 0;

    let mut flush = |lines: &mut Vec<String>, start: usize, end: usize| -> Result<bool> {
        if lines.is_empty() {
            return Ok(false);
        }
        chunk_index += 1;
        let filename = format!(
            "nodes-{chunk_index:04}.ndjson{}",
            match options.compression {
                TraceCompression::Zstd => ".zst",
                TraceCompression::None => "",
            }
        );
        let path = trace_dir.join(&filename);
        if *chunk_budget_remaining == 0 {
            return Ok(true);
        }
        let payload = format!("{}\n", lines.join("\n"));
        let raw_bytes = payload.as_bytes();

        let (bytes, payload) = match options.compression {
            TraceCompression::Zstd => {
                let compressed = zstd::stream::encode_all(raw_bytes, 3)?;
                (compressed.len() as u64, compressed)
            }
            TraceCompression::None => (raw_bytes.len() as u64, raw_bytes.to_vec()),
        };
        if !reserve_budget(budget_remaining, bytes) {
            return Ok(true);
        }
        write_atomic(&path, payload.as_slice())?;
        *chunk_budget_remaining = chunk_budget_remaining.saturating_sub(1);
        chunks.push(TraceChunkRef {
            path: filename,
            format: "ndjson".to_string(),
            compression: match options.compression {
                TraceCompression::Zstd => "zstd".to_string(),
                TraceCompression::None => "none".to_string(),
            },
            record_start: None,
            record_end: None,
            node_start: Some(start as u64),
            node_end: Some(end as u64),
            bytes: Some(bytes),
            bytes_uncompressed: Some(raw_bytes.len() as u64),
        });
        files.push(path);
        lines.clear();
        Ok(false)
    };

    for (index, node) in nodes.iter().enumerate() {
        let line = serde_json::to_string(node)
            .with_context(|| format!("failed to serialize node at {index}"))?;
        let line_len = line.as_bytes().len() + 1; // newline
        let exceeds_node_limit = lines_len_exceeds(&current_lines, options.max_nodes_per_chunk);
        let exceeds_byte_limit = current_bytes + line_len > options.max_chunk_bytes_uncompressed;
        if !current_lines.is_empty() && (exceeds_node_limit || exceeds_byte_limit) {
            let end = node_start + current_lines.len() - 1;
            if flush(&mut current_lines, node_start, end)? {
                budget_exceeded = true;
                break;
            }
            node_start = index;
            current_bytes = 0;
        }
        current_bytes += line_len;
        current_lines.push(line);
    }

    if !budget_exceeded && !current_lines.is_empty() {
        let end = node_start + current_lines.len() - 1;
        if flush(&mut current_lines, node_start, end)? {
            budget_exceeded = true;
        }
    }

    Ok(ChunkWriteResult {
        chunks,
        files,
        budget_exceeded,
    })
}

fn lines_len_exceeds(lines: &[String], max: usize) -> bool {
    lines.len() >= max && max > 0
}

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
